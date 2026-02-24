import re
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from database import DeadLinkDatabase

ClassificationProgressCallback = Callable[[str, int, int], Awaitable[None]]


@dataclass
class ClassifiedSuggestion:
    suggestion_id: int
    dead_url: str
    suggested_url: str
    similarity_score: float
    confidence_score: float
    decision: str
    rationale: str


class SelfHealingClassifier:
    """Stage 3: classify replacement suggestions for auto vs manual handling."""

    def __init__(
        self,
        database: DeadLinkDatabase,
        auto_threshold: float = 0.75,
    ) -> None:
        self.database = database
        self.auto_threshold = auto_threshold

    async def classify(
        self,
        suggestions: Sequence[Dict[str, object]],
        progress_callback: Optional[ClassificationProgressCallback] = None,
    ) -> tuple:
        classified_count = 0
        auto_count = 0

        for row in suggestions:
            classified = self._classify_row(row)
            self.database.insert_classification(
                suggestion_id=classified.suggestion_id,
                dead_url=classified.dead_url,
                suggested_url=classified.suggested_url,
                similarity_score=classified.similarity_score,
                confidence_score=classified.confidence_score,
                decision=classified.decision,
                rationale=classified.rationale,
            )
            classified_count += 1
            if classified.decision == "auto_replace":
                auto_count += 1

            if progress_callback is not None:
                await progress_callback(classified.suggested_url, classified_count, auto_count)

        return classified_count, auto_count

    def _classify_row(self, row: Dict[str, object]) -> ClassifiedSuggestion:
        suggestion_id = int(row.get("id", 0))
        dead_url = str(row.get("dead_url", ""))
        suggested_url = str(row.get("suggested_url", ""))
        similarity_score = float(row.get("similarity_score", 0.0))

        confidence_score, rationale = self._confidence(dead_url, suggested_url, similarity_score)
        decision = "auto_replace" if confidence_score >= self.auto_threshold else "manual_review"

        return ClassifiedSuggestion(
            suggestion_id=suggestion_id,
            dead_url=dead_url,
            suggested_url=suggested_url,
            similarity_score=similarity_score,
            confidence_score=confidence_score,
            decision=decision,
            rationale=rationale,
        )

    def _confidence(self, dead_url: str, suggested_url: str, similarity_score: float) -> tuple:
        dead_parsed = urlparse(dead_url)
        suggested_parsed = urlparse(suggested_url)

        score = similarity_score
        reasons: List[str] = [f"base_similarity={similarity_score:.4f}"]

        same_host = dead_parsed.netloc == suggested_parsed.netloc and dead_parsed.netloc != ""
        if same_host:
            score += 0.15
            reasons.append("same_host")

        if suggested_parsed.scheme == "https":
            score += 0.03
            reasons.append("https")

        shared_prefix = self._shared_prefix_tokens(dead_parsed.path, suggested_parsed.path)
        if shared_prefix > 0:
            path_bonus = min(0.2, 0.05 * shared_prefix)
            score += path_bonus
            reasons.append(f"path_prefix+{path_bonus:.2f}")

        overlap = self._token_overlap(dead_parsed.path, suggested_parsed.path)
        if overlap > 0:
            overlap_bonus = min(0.15, overlap * 0.15)
            score += overlap_bonus
            reasons.append(f"path_overlap+{overlap_bonus:.2f}")

        if suggested_parsed.query:
            score -= 0.1
            reasons.append("query_penalty")

        if suggested_parsed.path in {"", "/"}:
            score -= 0.05
            reasons.append("root_path_penalty")

        score = max(0.0, min(1.0, score))
        return score, ",".join(reasons)

    def _shared_prefix_tokens(self, a: str, b: str) -> int:
        parts_a = [p for p in a.split("/") if p]
        parts_b = [p for p in b.split("/") if p]
        shared = 0
        for part_a, part_b in zip(parts_a, parts_b):
            if part_a != part_b:
                break
            shared += 1
        return shared

    def _token_overlap(self, a: str, b: str) -> float:
        tokens_a = set(re.findall(r"[a-z0-9]{2,}", a.lower()))
        tokens_b = set(re.findall(r"[a-z0-9]{2,}", b.lower()))
        if not tokens_a or not tokens_b:
            return 0.0
        return len(tokens_a.intersection(tokens_b)) / len(tokens_a.union(tokens_b))
