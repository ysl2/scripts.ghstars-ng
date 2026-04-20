from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import uuid

from src.ghstars import cli as cli_module
from src.ghstars.associate.resolver import build_final_links
from src.ghstars.commands._common import (
    _build_sync_owner_id,
    _ensure_paper_lease,
    _heartbeat_paper_sync_lease,
    _list_papers_for_window,
    _persist_raw_response,
    _replace_surface_observations,
)
from src.ghstars.models import Paper, PaperSyncLease
from src.ghstars.providers.alphaxiv_links import (
    AlphaXivLinksClient,
    extract_github_url_from_alphaxiv_html,
    extract_github_url_from_alphaxiv_payload,
)
from src.ghstars.providers.arxiv_links import ArxivLinksClient, extract_github_urls_from_abs_html, extract_github_urls_from_comment
from src.ghstars.providers.huggingface_links import (
    HuggingFaceLinksClient,
    extract_github_url_from_hf_html,
    extract_github_url_from_hf_payload,
)
from src.ghstars.storage.db import Database, LeaseLostError
from src.ghstars.storage.raw_cache import RawCacheStore


LINK_SYNC_TTL = timedelta(days=7)
LINK_SYNC_FOUND = "found"
LINK_SYNC_NOT_FOUND = "not_found"


@dataclass(frozen=True)
class LinkSyncAttempt:
    found: bool
    complete: bool


async def _run_sync_links(
    database: Database,
    raw_cache: RawCacheStore,
    arxiv_links: ArxivLinksClient,
    huggingface: HuggingFaceLinksClient,
    alphaxiv: AlphaXivLinksClient,
    categories: tuple[str, ...],
    *,
    concurrency: int = 1,
    window: cli_module.ArxivSyncWindow | None = None,
) -> None:
    if window is None:
        window = cli_module.ArxivSyncWindow()
    papers = _list_papers_for_window(database, categories, window=window)
    if not papers:
        return
    owner_id = _build_sync_owner_id()
    if concurrency == 1:
        for paper in papers:
            await _sync_links_for_paper(
                database,
                raw_cache,
                arxiv_links,
                huggingface,
                alphaxiv,
                paper,
                owner_id=owner_id,
            )
        return

    queue: asyncio.Queue[Paper | None] = asyncio.Queue()
    for paper in papers:
        queue.put_nowait(paper)
    worker_count = min(concurrency, len(papers))
    for _ in range(worker_count):
        queue.put_nowait(None)

    async def worker() -> None:
        worker_db = Database(database.db_path)
        try:
            while True:
                paper = await queue.get()
                if paper is None:
                    return
                try:
                    await _sync_links_for_paper(
                        worker_db,
                        raw_cache,
                        arxiv_links,
                        huggingface,
                        alphaxiv,
                        paper,
                        owner_id=owner_id,
                    )
                except Exception as exc:
                    print(f"{paper.arxiv_id}: sync links failed ({exc})")
        finally:
            worker_db.close()

    await asyncio.gather(*(worker() for _ in range(worker_count)))


async def _sync_links_for_paper(
    database: Database,
    raw_cache: RawCacheStore,
    arxiv_links: ArxivLinksClient,
    huggingface: HuggingFaceLinksClient,
    alphaxiv: AlphaXivLinksClient,
    paper: Paper,
    *,
    owner_id: str,
) -> None:
    lease = database.try_acquire_paper_sync_lease(
        paper.arxiv_id,
        owner_id=owner_id,
        lease_token=str(uuid.uuid4()),
        lease_ttl_seconds=cli_module.PAPER_SYNC_LEASE_TTL_SECONDS,
    )
    if lease is None:
        print(f"{paper.arxiv_id}: skipped (lease held by another sync)")
        return

    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(_heartbeat_paper_sync_lease(database, lease, stop_heartbeat))
    try:
        previous_links = database.list_paper_repo_links(paper.arxiv_id)
        arxiv_attempt = await _sync_arxiv_link_surfaces(
            database,
            raw_cache,
            arxiv_links,
            paper.arxiv_id,
            paper.comment,
            paper.title,
            lease,
        )
        _ensure_paper_lease(database, lease)
        observations = database.list_repo_observations(paper.arxiv_id)
        if arxiv_attempt.found:
            final_links = build_final_links(paper.arxiv_id, observations)
            database.replace_paper_repo_links(
                paper.arxiv_id,
                final_links,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            database.upsert_paper_link_sync_state(
                paper.arxiv_id,
                LINK_SYNC_FOUND,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            print(f"{paper.arxiv_id}: {len(final_links)} final links")
            return

        link_sync_state = database.get_paper_link_sync_state(paper.arxiv_id)
        if _is_link_sync_state_fresh(link_sync_state.checked_at if link_sync_state is not None else None):
            final_links = build_final_links(paper.arxiv_id, observations)
            database.replace_paper_repo_links(
                paper.arxiv_id,
                final_links,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            print(f"{paper.arxiv_id}: {len(final_links)} final links")
            return

        fallback_attempt = await _sync_fallback_exact_surfaces(
            database,
            raw_cache,
            huggingface,
            alphaxiv,
            paper.arxiv_id,
            paper.title,
            lease,
        )
        _ensure_paper_lease(database, lease)
        if fallback_attempt.found:
            observations = database.list_repo_observations(paper.arxiv_id)
            final_links = build_final_links(paper.arxiv_id, observations)
            database.replace_paper_repo_links(
                paper.arxiv_id,
                final_links,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            database.upsert_paper_link_sync_state(
                paper.arxiv_id,
                LINK_SYNC_FOUND,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            print(f"{paper.arxiv_id}: {len(final_links)} final links")
            return

        if arxiv_attempt.complete and fallback_attempt.complete:
            database.replace_paper_repo_links(
                paper.arxiv_id,
                [],
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            database.upsert_paper_link_sync_state(
                paper.arxiv_id,
                LINK_SYNC_NOT_FOUND,
                lease_owner_id=lease.owner_id,
                lease_token=lease.lease_token,
            )
            print(f"{paper.arxiv_id}: 0 final links")
            return

        print(f"{paper.arxiv_id}: partial refresh failed, kept {len(previous_links)} final links")
    except LeaseLostError:
        print(f"{paper.arxiv_id}: skipped after lease loss")
    finally:
        stop_heartbeat.set()
        await heartbeat_task
        database.release_paper_sync_lease(
            paper.arxiv_id,
            owner_id=lease.owner_id,
            lease_token=lease.lease_token,
        )


async def _sync_arxiv_link_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: ArxivLinksClient,
    arxiv_id: str,
    comment: str | None,
    title: str,
    lease: PaperSyncLease | None = None,
) -> LinkSyncAttempt:
    comment_urls = extract_github_urls_from_comment(comment)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="arxiv",
        surface="comment",
        urls=comment_urls,
        evidence_text=comment,
        raw_cache_id=None,
        lease=lease,
    )

    status, body, headers, error = await client.fetch_abs_html(arxiv_id)
    _ensure_paper_lease(database, lease)
    if error or status is None:
        return LinkSyncAttempt(found=bool(comment_urls), complete=False)

    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="arxiv",
        surface="abs_html",
        request_key=f"abs:{arxiv_id}",
        request_url=f"https://arxiv.org/abs/{arxiv_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    urls = extract_github_urls_from_abs_html(body)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="arxiv",
        surface="abs_html",
        urls=urls,
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )
    return LinkSyncAttempt(found=bool(comment_urls or urls), complete=True)


async def _sync_huggingface_exact_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: HuggingFaceLinksClient,
    arxiv_id: str,
    title: str,
    lease: PaperSyncLease | None = None,
) -> LinkSyncAttempt:
    return await _sync_huggingface_paper_surfaces(
        database,
        raw_cache,
        client,
        source_paper_id=arxiv_id,
        fetch_paper_id=arxiv_id,
        title=title,
        payload_surface="paper_api",
        html_surface="paper_html",
        lease=lease,
    )


async def _sync_huggingface_paper_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: HuggingFaceLinksClient,
    *,
    source_paper_id: str,
    fetch_paper_id: str,
    title: str,
    payload_surface: str,
    html_surface: str,
    lease: PaperSyncLease | None = None,
) -> LinkSyncAttempt:
    payload_complete = True
    status, body, headers, error = await client.fetch_paper_payload(fetch_paper_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="huggingface",
        surface="paper_api",
        request_key=f"paper_api:{fetch_paper_id}",
        request_url=f"https://huggingface.co/api/papers/{fetch_paper_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        payload_complete = False
    else:
        payload_urls = extract_github_url_from_hf_payload(body)
        _replace_surface_observations(
            database,
            arxiv_id=source_paper_id,
            provider="huggingface",
            surface=payload_surface,
            urls=payload_urls,
            evidence_text=body if body is not None else title,
            raw_cache_id=raw_cache_id,
            lease=lease,
        )
        if payload_urls:
            return LinkSyncAttempt(found=True, complete=payload_complete)
        if status == 404:
            return LinkSyncAttempt(found=False, complete=payload_complete)

    status, body, headers, error = await client.fetch_paper_html(fetch_paper_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="huggingface",
        surface="paper_html",
        request_key=f"paper_html:{fetch_paper_id}",
        request_url=f"https://huggingface.co/papers/{fetch_paper_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        return LinkSyncAttempt(found=False, complete=False)

    html_urls = extract_github_url_from_hf_html(body)
    _replace_surface_observations(
        database,
        arxiv_id=source_paper_id,
        provider="huggingface",
        surface=html_surface,
        urls=html_urls,
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )
    return LinkSyncAttempt(found=bool(html_urls), complete=payload_complete)


async def _sync_alphaxiv_link_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    client: AlphaXivLinksClient,
    arxiv_id: str,
    title: str,
    lease: PaperSyncLease | None = None,
) -> LinkSyncAttempt:
    payload_complete = True
    status, body, headers, error = await client.fetch_paper_payload(arxiv_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="alphaxiv",
        surface="paper_api",
        request_key=f"paper_api:{arxiv_id}",
        request_url=f"https://api.alphaxiv.org/papers/v3/{arxiv_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        payload_complete = False
    else:
        payload_urls = extract_github_url_from_alphaxiv_payload(body)
        _replace_surface_observations(
            database,
            arxiv_id=arxiv_id,
            provider="alphaxiv",
            surface="paper_api",
            urls=payload_urls,
            evidence_text=body if body is not None else title,
            raw_cache_id=raw_cache_id,
            lease=lease,
        )
        if payload_urls:
            return LinkSyncAttempt(found=True, complete=payload_complete)
        if status == 404:
            return LinkSyncAttempt(found=False, complete=payload_complete)

    status, body, headers, error = await client.fetch_paper_html(arxiv_id)
    _ensure_paper_lease(database, lease)
    raw_cache_id = _persist_raw_response(
        database,
        raw_cache,
        provider="alphaxiv",
        surface="paper_html",
        request_key=f"paper_html:{arxiv_id}",
        request_url=f"https://www.alphaxiv.org/abs/{arxiv_id}",
        status=status,
        headers=headers,
        body=body,
    )
    _ensure_paper_lease(database, lease)
    if error and status != 404:
        return LinkSyncAttempt(found=False, complete=False)

    html_urls = extract_github_url_from_alphaxiv_html(body)
    _replace_surface_observations(
        database,
        arxiv_id=arxiv_id,
        provider="alphaxiv",
        surface="paper_html",
        urls=html_urls,
        evidence_text=body if body is not None else title,
        raw_cache_id=raw_cache_id,
        lease=lease,
    )
    return LinkSyncAttempt(found=bool(html_urls), complete=payload_complete)


async def _sync_fallback_exact_surfaces(
    database: Database,
    raw_cache: RawCacheStore,
    huggingface: HuggingFaceLinksClient,
    alphaxiv: AlphaXivLinksClient,
    arxiv_id: str,
    title: str,
    lease: PaperSyncLease | None = None,
) -> LinkSyncAttempt:
    huggingface_attempt = await _sync_huggingface_exact_surfaces(
        database,
        raw_cache,
        huggingface,
        arxiv_id,
        title,
        lease,
    )
    if huggingface_attempt.found:
        return huggingface_attempt

    alphaxiv_attempt = await _sync_alphaxiv_link_surfaces(
        database,
        raw_cache,
        alphaxiv,
        arxiv_id,
        title,
        lease,
    )
    return LinkSyncAttempt(
        found=alphaxiv_attempt.found,
        complete=huggingface_attempt.complete and alphaxiv_attempt.complete,
    )


def _is_link_sync_state_fresh(checked_at: str | None) -> bool:
    if not checked_at:
        return False
    try:
        checked = datetime.fromisoformat(checked_at)
    except ValueError:
        return False
    if checked.tzinfo is None:
        checked = checked.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - checked < LINK_SYNC_TTL
