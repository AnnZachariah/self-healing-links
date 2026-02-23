import asyncio
import math
import re
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Sequence, Set
from urllib.parse import urlparse

import httpx

from database import DeadLinkDatabase

SuggestionProgressCallback = Callable[[str, int], Awaitable[None]]


@dataclass
class ReplacementSuggestion:
    source_page: str
    dead_url: str
    anchor_text: str
    suggested_url: str
    wayback_snapshot_url: str
    similarity_score: float
    match_reason: str


class ReplacementEngine:
    """Stage 2 engine: discover replacement URLs using Wayback + text similarity."""

    def __init__(
        self,
        database: DeadLinkDatabase,
        concurrency: int = 5,
        timeout_seconds: float = 10.0,
        min_similarity: float = 0.03,
        top_k_per_link: int = 3,
    ) -> None:
        self.database = database
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = httpx.Timeout(timeout_seconds)
        self.min_similarity = min_similarity
        self.top_k_per_link = top_k_per_link

    async def generate_replacements(
        self,
        dead_links: Sequence[Dict[str, str]],
        progress_callback: Optional[SuggestionProgressCallback] = None,
    ) -> int:
        suggestion_count = 0
        async with httpx.AsyncClient(follow_redirects=True, timeout=self.timeout) as client:
            tasks = [self._process_dead_link(client, row) for row in dead_links]
            results = await asyncio.gather(*tasks)

        for suggestions in results:
            if not suggestions:
                continue
            for suggestion in suggestions:
                self.database.insert_replacement_suggestion(
                    source_page=suggestion.source_page,
                    dead_url=suggestion.dead_url,
                    anchor_text=suggestion.anchor_text,
                    suggested_url=suggestion.suggested_url,
                    wayback_snapshot_url=suggestion.wayback_snapshot_url,
                    similarity_score=suggestion.similarity_score,
                    match_reason=suggestion.match_reason,
                )
                suggestion_count += 1
                if progress_callback is not None:
                    await progress_callback(suggestion.dead_url, suggestion_count)

        return suggestion_count

    async def _process_dead_link(
        self,
        client: httpx.AsyncClient,
        row: Dict[str, str],
    ) -> List[ReplacementSuggestion]:
        dead_url = str(row.get("dead_url", "")).strip()
        if not dead_url:
            return []

        source_page = str(row.get("source_page", "")).strip()
        anchor_text = str(row.get("anchor_text", "")).strip()
        context = str(row.get("surrounding_context", "")).strip()

        wayback_snapshot = await self._get_wayback_snapshot(client, dead_url)
        candidates = await self._get_candidate_urls(client, dead_url)

        if wayback_snapshot:
            candidates.insert(0, wayback_snapshot)

        ranked = self._rank_candidates(dead_url, anchor_text, context, candidates)
        if not ranked:
            return []

        suggestions: List[ReplacementSuggestion] = []
        for suggested_url, similarity_score in ranked[: self.top_k_per_link]:
            if similarity_score < self.min_similarity:
                continue
            suggestions.append(
                ReplacementSuggestion(
                    source_page=source_page,
                    dead_url=dead_url,
                    anchor_text=anchor_text,
                    suggested_url=suggested_url,
                    wayback_snapshot_url=wayback_snapshot or "",
                    similarity_score=similarity_score,
                    match_reason="wayback+semantic-match",
                )
            )

        return suggestions

    async def _get_wayback_snapshot(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        endpoint = "https://archive.org/wayback/available"
        try:
            async with self.semaphore:
                response = await client.get(endpoint, params={"url": url})
            response.raise_for_status()
            data = response.json()
            snapshots = data.get("archived_snapshots", {})
            closest = snapshots.get("closest", {})
            snapshot_url = closest.get("url")
            if isinstance(snapshot_url, str):
                return snapshot_url
            return None
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError):
            return None

    async def _get_candidate_urls(self, client: httpx.AsyncClient, dead_url: str) -> List[str]:
        parsed = urlparse(dead_url)
        if not parsed.netloc:
            return []

        cdx_endpoint = "https://web.archive.org/cdx/search/cdx"
        patterns = self._cdx_patterns(dead_url)
        urls: List[str] = []

        for pattern in patterns:
            params = {
                "url": pattern,
                "output": "json",
                "fl": "original,statuscode,mimetype,timestamp",
                "filter": ["statuscode:200", "mimetype:text/html"],
                "collapse": "urlkey",
                "limit": "120",
            }
            try:
                async with self.semaphore:
                    response = await client.get(cdx_endpoint, params=params)
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list) or len(rows) <= 1:
                    continue

                for row in rows[1:]:
                    if isinstance(row, list) and row:
                        original = row[0]
                        if isinstance(original, str) and original.startswith(("http://", "https://")):
                            urls.append(original)
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError):
                continue

        filtered = self._filter_candidates(dead_url, self._unique(urls))
        return filtered

    def _rank_candidates(
        self,
        dead_url: str,
        anchor_text: str,
        surrounding_context: str,
        candidates: Sequence[str],
    ) -> List[tuple]:
        dead_path = urlparse(dead_url).path
        query = f"{anchor_text} {self._candidate_text(dead_url)}".strip()
        if not anchor_text.strip():
            query = f"{query} {surrounding_context[:220]}".strip()
        if not query:
            return []

        ranked: List[tuple] = []
        seen: Set[str] = set()

        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)

            candidate_text = self._candidate_text(candidate)
            if not candidate_text:
                continue

            semantic_score = self._cosine_similarity(query, candidate_text)
            path_bonus = self._path_prefix_bonus(dead_path, urlparse(candidate).path)
            query_penalty = self._query_penalty(candidate)
            score = semantic_score + path_bonus - query_penalty
            ranked.append((candidate, min(score, 1.0)))

        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked

    def _candidate_text(self, candidate_url: str) -> str:
        parsed = urlparse(candidate_url)
        path_text = re.sub(r"[-_/]+", " ", parsed.path)
        host_text = re.sub(r"[.-]+", " ", parsed.netloc)
        return f"{host_text} {path_text}".strip()

    def _cosine_similarity(self, a: str, b: str) -> float:
        vec_a = self._bow_vector(a)
        vec_b = self._bow_vector(b)
        if not vec_a or not vec_b:
            return 0.0

        numerator = sum(vec_a.get(token, 0.0) * vec_b.get(token, 0.0) for token in vec_a)
        denom_a = math.sqrt(sum(value * value for value in vec_a.values()))
        denom_b = math.sqrt(sum(value * value for value in vec_b.values()))
        if denom_a == 0.0 or denom_b == 0.0:
            return 0.0
        return numerator / (denom_a * denom_b)

    def _bow_vector(self, text: str) -> Dict[str, float]:
        tokens = self._tokenize(text)
        if not tokens:
            return {}

        counts: Dict[str, float] = {}
        for token in tokens:
            counts[token] = counts.get(token, 0.0) + 1.0

        total = float(len(tokens))
        return {token: value / total for token, value in counts.items()}

    def _tokenize(self, text: str) -> List[str]:
        return re.findall(r"[a-z0-9]{2,}", text.lower())

    def _path_prefix_bonus(self, dead_path: str, candidate_path: str) -> float:
        dead_parts = [p for p in dead_path.split("/") if p]
        cand_parts = [p for p in candidate_path.split("/") if p]
        if not dead_parts or not cand_parts:
            return 0.0

        shared = 0
        for d_part, c_part in zip(dead_parts, cand_parts):
            if d_part != c_part:
                break
            shared += 1

        token_overlap = self._token_overlap_score(dead_path, candidate_path)
        return min(shared * 0.07, 0.28) + min(token_overlap * 0.2, 0.2)

    def _token_overlap_score(self, a: str, b: str) -> float:
        tokens_a = set(self._tokenize(a))
        tokens_b = set(self._tokenize(b))
        if not tokens_a or not tokens_b:
            return 0.0
        intersection = len(tokens_a.intersection(tokens_b))
        union = len(tokens_a.union(tokens_b))
        return intersection / union

    def _query_penalty(self, url: str) -> float:
        query = urlparse(url).query
        if not query:
            return 0.0
        return 0.08

    def _unique(self, values: Sequence[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    def _cdx_patterns(self, dead_url: str) -> List[str]:
        parsed = urlparse(dead_url)
        netloc = parsed.netloc
        path = parsed.path or "/"
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        patterns = [
            dead_url,
            f"{netloc}{path}*",
            f"{netloc}{parent}/*" if parent else f"{netloc}/*",
            f"{netloc}/*",
        ]
        return self._unique(patterns)

    def _filter_candidates(self, dead_url: str, candidates: Sequence[str]) -> List[str]:
        dead_parsed = urlparse(dead_url)
        dead_host = dead_parsed.netloc
        dead_parent = dead_parsed.path.rsplit("/", 1)[0]

        preferred: List[str] = []
        fallback: List[str] = []
        for candidate in candidates:
            parsed = urlparse(candidate)
            if parsed.netloc != dead_host:
                continue
            if parsed.path in {"", "/"}:
                fallback.append(candidate)
                continue
            if parsed.query:
                # Keep query URLs only as low-priority fallbacks.
                fallback.append(candidate)
                continue
            if dead_parent and parsed.path.startswith(f"{dead_parent}/"):
                preferred.append(candidate)
            else:
                fallback.append(candidate)

        combined = preferred + fallback
        return combined[:200]
