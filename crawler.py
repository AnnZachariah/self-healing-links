import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urldefrag, urljoin, urlparse
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from database import DeadLinkDatabase

StatusValue = Union[int, str]
ProgressCallback = Callable[[str, StatusValue, int], Awaitable[None]]


@dataclass
class LinkContext:
    source_page: str
    dead_url: str
    anchor_text: str
    surrounding_context: str
    status_code: StatusValue


class LinkCrawler:
    def __init__(
        self,
        database: DeadLinkDatabase,
        max_depth: int = 2,
        concurrency: int = 10,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.database = database
        self.max_depth = max_depth
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = httpx.Timeout(timeout_seconds)
        self.dead_count = 0
        self.status_cache: Dict[str, StatusValue] = {}

    async def crawl(
        self,
        target: str,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> int:
        root_pages = await self._resolve_targets(target)
        if not root_pages:
            raise ValueError("No pages found to crawl from the provided input.")

        allowed_hosts = {urlparse(url).netloc for url in root_pages}
        visited_pages: Set[str] = set()
        queue: deque[Tuple[str, int]] = deque((url, 0) for url in root_pages)

        async with httpx.AsyncClient(follow_redirects=True, timeout=self.timeout) as client:
            while queue:
                current_page, depth = queue.popleft()
                if current_page in visited_pages or depth > self.max_depth:
                    continue

                visited_pages.add(current_page)
                html = await self._fetch_html(client, current_page)
                if html is None:
                    continue

                links = self._extract_links(current_page, html)
                if not links:
                    continue

                link_tasks = [
                    self._check_and_record_link(client, link, progress_callback)
                    for link in links
                ]
                child_candidates = await asyncio.gather(*link_tasks)

                if depth < self.max_depth:
                    for candidate in child_candidates:
                        if not candidate:
                            continue
                        parsed = urlparse(candidate)
                        if parsed.netloc in allowed_hosts and candidate not in visited_pages:
                            queue.append((candidate, depth + 1))

        return self.dead_count

    async def _resolve_targets(self, target: str) -> List[str]:
        normalized = self._normalize_url(target)
        if normalized.lower().endswith("sitemap.xml"):
            return await self._parse_sitemap(normalized)
        return [normalized]

    async def _parse_sitemap(self, sitemap_url: str) -> List[str]:
        async with httpx.AsyncClient(follow_redirects=True, timeout=self.timeout) as client:
            content = await self._fetch_text(client, sitemap_url)

        if not content:
            return []

        try:
            root = ElementTree.fromstring(content)
        except ElementTree.ParseError:
            return []

        urls: List[str] = []
        for loc in root.findall(".//{*}loc"):
            if loc.text and loc.text.strip():
                urls.append(self._normalize_url(loc.text.strip()))
        return urls

    async def _fetch_text(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        try:
            async with self.semaphore:
                response = await client.get(url)
            response.raise_for_status()
            return response.text
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RequestError, httpx.HTTPStatusError):
            return None

    async def _fetch_html(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        text = await self._fetch_text(client, url)
        if not text:
            return None
        return text

    def _extract_links(self, page_url: str, html: str) -> List[LinkContext]:
        soup = BeautifulSoup(html, "html.parser")
        links: List[LinkContext] = []

        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "").strip()
            if not href:
                continue

            absolute = self._normalize_url(urljoin(page_url, href))
            if not absolute.startswith(("http://", "https://")):
                continue

            links.append(
                LinkContext(
                    source_page=page_url,
                    dead_url=absolute,
                    anchor_text=anchor.get_text(" ", strip=True)[:300],
                    surrounding_context=self._extract_surrounding_context(anchor),
                    status_code="",
                )
            )

        return links

    async def _check_and_record_link(
        self,
        client: httpx.AsyncClient,
        link: LinkContext,
        progress_callback: Optional[ProgressCallback],
    ) -> Optional[str]:
        status = await self._check_link_status(client, link.dead_url)

        if progress_callback is not None:
            await progress_callback(link.dead_url, status, self.dead_count)

        if self._is_dead_status(status):
            self.dead_count += 1
            link.status_code = status
            self.database.insert_dead_link(
                source_page=link.source_page,
                dead_url=link.dead_url,
                anchor_text=link.anchor_text,
                surrounding_context=link.surrounding_context,
                status_code=str(status),
                discovered_at=datetime.now(timezone.utc).isoformat(),
            )
            if progress_callback is not None:
                await progress_callback(link.dead_url, status, self.dead_count)

        parsed = urlparse(link.dead_url)
        if parsed.scheme in {"http", "https"}:
            return link.dead_url
        return None

    async def _check_link_status(self, client: httpx.AsyncClient, url: str) -> StatusValue:
        if url in self.status_cache:
            return self.status_cache[url]

        try:
            async with self.semaphore:
                response = await client.head(url)
            status: StatusValue = response.status_code
            if response.status_code == 405:
                async with self.semaphore:
                    fallback = await client.get(url)
                status = fallback.status_code
        except httpx.TimeoutException:
            status = "TIMEOUT"
        except httpx.ConnectError:
            status = "CONNECTION_ERROR"
        except httpx.RequestError:
            status = "REQUEST_ERROR"

        self.status_cache[url] = status
        return status

    def _is_dead_status(self, status: StatusValue) -> bool:
        if isinstance(status, int):
            return status in {404, 410}
        return status in {"TIMEOUT", "CONNECTION_ERROR", "REQUEST_ERROR"}

    def _extract_surrounding_context(self, anchor) -> str:
        paragraph = anchor.find_parent("p")
        if paragraph:
            return paragraph.get_text(" ", strip=True)

        parent_text = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""
        if parent_text:
            return parent_text[:400]

        return anchor.get_text(" ", strip=True)[:400]

    def _normalize_url(self, url: str) -> str:
        cleaned, _ = urldefrag(url.strip())
        return cleaned
