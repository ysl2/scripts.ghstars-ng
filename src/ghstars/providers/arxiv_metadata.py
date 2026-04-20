from __future__ import annotations

import asyncio
import html
import xml.etree.ElementTree as ET

import aiohttp

from src.ghstars.models import Paper
from src.ghstars.net.http import RateLimiter, request_text
from src.ghstars.normalize.arxiv import build_arxiv_abs_url, extract_arxiv_id, sanitize_title


ARXIV_NS = {"a": "http://www.w3.org/2005/Atom"}
ARXIV_MIN_INTERVAL = 3.0
ARXIV_TRANSIENT_RETRY_LIMIT = 5


class ArxivMetadataClient:
    def __init__(self, session: aiohttp.ClientSession, *, min_interval: float = 0.5, max_concurrent: int = 1):
        self.session = session
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(max(min_interval, ARXIV_MIN_INTERVAL))

    async def fetch_search_page(
        self,
        *,
        search_query: str,
        start: int = 0,
        max_results: int = 100,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={
                "search_query": search_query,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
                "start": str(start),
                "max_results": str(max_results),
            },
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv metadata query",
        )

    async def fetch_category_page(self, *, category: str, start: int = 0, max_results: int = 100) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await self.fetch_search_page(
            search_query=f"cat:{category}",
            start=start,
            max_results=max_results,
        )

    async def fetch_id_list_feed(self, arxiv_ids: list[str]) -> tuple[int | None, str | None, dict[str, str], str | None]:
        id_list = ",".join(item.strip() for item in arxiv_ids if item and item.strip())
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={"id_list": id_list},
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv metadata id_list query",
            max_retries=ARXIV_TRANSIENT_RETRY_LIMIT,
        )

    async def fetch_listing_page(
        self,
        *,
        category: str,
        period: str,
        skip: int = 0,
        show: int = 2000,
    ) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            f"https://arxiv.org/list/{category}/{period}",
            params={
                "skip": str(skip),
                "show": str(show),
            },
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv listing query",
            max_retries=ARXIV_TRANSIENT_RETRY_LIMIT,
        )

    async def fetch_paper_feed(self, arxiv_id: str) -> tuple[int | None, str | None, dict[str, str], str | None]:
        return await request_text(
            self.session,
            "https://export.arxiv.org/api/query",
            params={"id_list": arxiv_id},
            semaphore=self.semaphore,
            rate_limiter=self.rate_limiter,
            retry_prefix="arXiv paper query",
        )


def parse_papers_from_feed(feed_xml: str) -> list[Paper]:
    if not feed_xml:
        return []

    try:
        root = ET.fromstring(feed_xml)
    except ET.ParseError:
        return []

    papers: list[Paper] = []
    for entry in root.findall("a:entry", ARXIV_NS):
        id_el = entry.find("a:id", ARXIV_NS)
        title_el = entry.find("a:title", ARXIV_NS)
        summary_el = entry.find("a:summary", ARXIV_NS)
        published_el = entry.find("a:published", ARXIV_NS)
        updated_el = entry.find("a:updated", ARXIV_NS)
        if id_el is None or title_el is None or summary_el is None:
            continue

        arxiv_id = extract_arxiv_id((id_el.text or "").strip())
        if not arxiv_id:
            continue

        categories = tuple(_extract_categories(entry))
        primary_category = categories[0] if categories else None
        authors = tuple(_extract_authors(entry))
        comment = _extract_comment(entry)
        title = sanitize_title("".join(title_el.itertext()))
        abstract = sanitize_title("".join(summary_el.itertext()))
        papers.append(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=build_arxiv_abs_url(arxiv_id),
                title=title,
                abstract=abstract,
                published_at=(published_el.text or "").strip()[:10] if published_el is not None and published_el.text else None,
                updated_at=(updated_el.text or "").strip()[:10] if updated_el is not None and updated_el.text else None,
                authors=authors,
                categories=categories,
                comment=comment,
                primary_category=primary_category,
            )
        )
    return papers


def _extract_categories(entry) -> list[str]:
    categories: list[str] = []
    seen: set[str] = set()
    for category in entry.findall("a:category", ARXIV_NS):
        term = (category.attrib.get("term") or "").strip()
        if not term or term in seen:
            continue
        seen.add(term)
        categories.append(term)
    return categories


def _extract_authors(entry) -> list[str]:
    authors: list[str] = []
    for author in entry.findall("a:author", ARXIV_NS):
        name_el = author.find("a:name", ARXIV_NS)
        if name_el is None or not name_el.text:
            continue
        authors.append(" ".join(name_el.text.split()).strip())
    return authors


def _extract_comment(entry) -> str | None:
    for child in entry:
        if child.tag.endswith("comment") and child.text:
            return html.unescape(" ".join(child.text.split())).strip()
    return None
