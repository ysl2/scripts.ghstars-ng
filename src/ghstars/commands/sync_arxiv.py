from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
import uuid

from src.ghstars import cli as cli_module
from src.ghstars.commands._common import (
    _build_sync_owner_id,
    _ensure_resource_lease,
    _heartbeat_resource_lease,
)
from src.ghstars.models import Paper
from src.ghstars.providers.arxiv_metadata import ArxivMetadataClient, parse_papers_from_feed
from src.ghstars.storage.db import Database
from src.ghstars.storage.raw_cache import RawCacheStore


DEFAULT_LATEST_MAX_RESULTS = 100
ARXIV_PAGE_SIZE = 100


class ArxivWindowSyncIncompleteError(RuntimeError):
    pass


async def _run_sync_arxiv(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivMetadataClient,
    categories: tuple[str, ...],
    *,
    max_results: int | None,
    window: cli_module.ArxivSyncWindow,
) -> None:
    owner_id = _build_sync_owner_id()
    for category in categories:
        stream_name = f"arxiv:{category}:{window.describe()}" if window.enabled else f"arxiv:{category}"
        stream_lease = database.try_acquire_resource_lease(
            stream_name,
            owner_id=owner_id,
            lease_token=str(uuid.uuid4()),
            lease_ttl_seconds=cli_module.RESOURCE_LEASE_TTL_SECONDS,
        )
        if stream_lease is None:
            print(f"{category}: skipped (stream sync held by another process)")
            continue

        stop_heartbeat = asyncio.Event()
        heartbeat_task = asyncio.create_task(_heartbeat_resource_lease(database, stream_lease, stop_heartbeat))
        try:
            if window.enabled:
                synced_count, latest_cursor = await _sync_arxiv_category_by_window(
                    database,
                    raw_cache,
                    client,
                    category,
                    window,
                )
                _ensure_resource_lease(database, stream_lease)
                database.set_sync_state(
                    stream_name,
                    latest_cursor,
                    lease_owner_id=stream_lease.owner_id,
                    lease_token=stream_lease.lease_token,
                )
                print(f"{category}: synced {synced_count} papers in {window.describe()}")
                continue

            latest_max_results = max_results or DEFAULT_LATEST_MAX_RESULTS
            search_query = f"cat:{category}"
            status, body, headers, error = await client.fetch_search_page(
                search_query=search_query,
                start=0,
                max_results=latest_max_results,
            )
            if error or body is None or status is None:
                print(f"{category}: {error or 'empty response'}")
                continue
            request_key = f"search={search_query}:start=0:max_results={latest_max_results}"
            raw_entry = _store_arxiv_search_page(
                database,
                raw_cache,
                search_query=search_query,
                request_key=request_key,
                status=status,
                body=body,
                headers=headers,
            )
            papers = parse_papers_from_feed(body)
            _persist_arxiv_papers(database, raw_entry.id, papers, surface="search_feed")
            _ensure_resource_lease(database, stream_lease)
            database.set_sync_state(
                stream_name,
                papers[0].updated_at if papers else None,
                lease_owner_id=stream_lease.owner_id,
                lease_token=stream_lease.lease_token,
            )
            print(f"{category}: synced {len(papers)} papers")
        finally:
            stop_heartbeat.set()
            await heartbeat_task
            database.release_resource_lease(
                stream_name,
                owner_id=stream_lease.owner_id,
                lease_token=stream_lease.lease_token,
            )


async def _sync_arxiv_category_by_window(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivMetadataClient,
    category: str,
    window: cli_module.ArxivSyncWindow,
) -> tuple[int, str | None]:
    start = 0
    synced_count = 0
    latest_cursor = None
    search_query = _build_arxiv_window_search_query(category, window)

    while True:
        status, body, headers, error = await client.fetch_search_page(
            search_query=search_query,
            start=start,
            max_results=ARXIV_PAGE_SIZE,
        )
        if error or body is None or status is None:
            raise ArxivWindowSyncIncompleteError(
                f"{category}: arXiv window sync {window.describe()} incomplete at start={start} "
                f"({error or 'empty response'}); aborting after persisting fetched pages"
            )

        request_key = f"search={search_query}:start={start}:max_results={ARXIV_PAGE_SIZE}"
        raw_entry = _store_arxiv_search_page(
            database,
            raw_cache,
            search_query=search_query,
            request_key=request_key,
            status=status,
            body=body,
            headers=headers,
        )
        papers = parse_papers_from_feed(body)
        if not papers:
            break
        if latest_cursor is None:
            latest_cursor = papers[0].updated_at

        _persist_arxiv_papers(database, raw_entry.id, papers, surface="search_feed")
        synced_count += len(papers)
        if len(papers) < ARXIV_PAGE_SIZE:
            break
        start += ARXIV_PAGE_SIZE

    return synced_count, latest_cursor


def _store_arxiv_search_page(
    database: Database,
    raw_cache: RawCacheStore,
    *,
    search_query: str,
    request_key: str,
    status: int,
    body: str,
    headers: dict[str, str],
):
    path, content_hash = raw_cache.write_body(
        provider="arxiv",
        surface="search_feed",
        request_key=request_key,
        body=body,
        content_type=headers.get("Content-Type"),
    )
    return database.upsert_raw_cache(
        provider="arxiv",
        surface="search_feed",
        request_key=request_key,
        request_url=f"https://export.arxiv.org/api/query?search_query={search_query}",
        content_type=headers.get("Content-Type"),
        status_code=status,
        body_path=path,
        content_hash=content_hash,
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    )


def _persist_arxiv_papers(database: Database, raw_cache_id: int, papers: list[Paper], *, surface: str) -> None:
    for paper in papers:
        database.persist_paper_with_source(
            paper,
            provider="arxiv",
            surface=surface,
            raw_cache_id=raw_cache_id,
            data={
                "title": paper.title,
                "abstract": paper.abstract,
                "categories": list(paper.categories),
                "comment": paper.comment,
            },
        )


def _format_arxiv_datetime(value: date, *, end_of_day: bool) -> str:
    return f"{value.strftime('%Y%m%d')}{'2359' if end_of_day else '0000'}"


def _build_arxiv_window_search_query(category: str, window: cli_module.ArxivSyncWindow) -> str:
    start_date = window.start_date or date(1991, 1, 1)
    end_date = window.end_date or date.today()
    return (
        f"cat:{category} AND submittedDate:[{_format_arxiv_datetime(start_date, end_of_day=False)} "
        f"TO {_format_arxiv_datetime(end_date, end_of_day=True)}]"
    )
