from __future__ import annotations

import asyncio
import os
import uuid

from src.ghstars import cli as cli_module
from src.ghstars.models import Paper, PaperSyncLease
from src.ghstars.storage.db import Database, LeaseLostError
from src.ghstars.storage.raw_cache import RawCacheStore


EXTRACTOR_VERSION = "2"


def _list_papers_for_window(
    database: Database,
    categories: tuple[str, ...],
    *,
    window: cli_module.ArxivSyncWindow,
) -> list[Paper]:
    return database.list_papers_by_categories(
        categories,
        published_from=window.start_date.isoformat() if window.start_date is not None else None,
        published_to=window.end_date.isoformat() if window.end_date is not None else None,
    )

def _persist_raw_response(
    database: Database,
    raw_cache: RawCacheStore,
    *,
    provider: str,
    surface: str,
    request_key: str,
    request_url: str,
    status: int | None,
    headers: dict[str, str],
    body: str | None,
) -> int | None:
    if status is None or body is None:
        return None
    path, content_hash = raw_cache.write_body(
        provider=provider,
        surface=surface,
        request_key=request_key,
        body=body,
        content_type=headers.get("Content-Type"),
    )
    return database.upsert_raw_cache(
        provider=provider,
        surface=surface,
        request_key=request_key,
        request_url=request_url,
        content_type=headers.get("Content-Type"),
        status_code=status,
        body_path=path,
        content_hash=content_hash,
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    ).id


def _replace_surface_observations(
    database: Database,
    *,
    arxiv_id: str,
    provider: str,
    surface: str,
    urls: tuple[str, ...],
    evidence_text: str | None,
    raw_cache_id: int | None,
    empty_status: str = "checked_no_match",
    error_message: str | None = None,
    lease: PaperSyncLease | None = None,
) -> None:
    database.replace_repo_observations(
        arxiv_id=arxiv_id,
        provider=provider,
        surface=surface,
        observations=[
            {
                "status": "found",
                "observed_repo_url": url,
                "normalized_repo_url": url,
                "evidence_text": evidence_text,
                "raw_cache_id": raw_cache_id,
                "extractor_version": EXTRACTOR_VERSION,
            }
            for url in urls
        ]
        or [
            {
                "status": empty_status,
                "observed_repo_url": None,
                "normalized_repo_url": None,
                "evidence_text": evidence_text,
                "raw_cache_id": raw_cache_id,
                "extractor_version": EXTRACTOR_VERSION,
                "error_message": error_message,
            }
        ],
        lease_owner_id=lease.owner_id if lease is not None else None,
        lease_token=lease.lease_token if lease is not None else None,
    )


def _has_found_repo(observations) -> bool:
    return any(observation.status == "found" and observation.normalized_repo_url for observation in observations)


def _build_sync_owner_id() -> str:
    return f"pid-{os.getpid()}-{uuid.uuid4().hex}"


def _ensure_resource_lease(database: Database, lease) -> None:
    if lease is None:
        return
    if not database.validate_resource_lease(
        lease.resource_key,
        owner_id=lease.owner_id,
        lease_token=lease.lease_token,
    ):
        raise LeaseLostError(f"{lease.resource_key}: resource lease lost")


def _ensure_paper_lease(database: Database, lease: PaperSyncLease | None) -> None:
    if lease is None:
        return
    if not database.validate_paper_sync_lease(
        lease.arxiv_id,
        owner_id=lease.owner_id,
        lease_token=lease.lease_token,
    ):
        raise LeaseLostError(f"{lease.arxiv_id}: paper lease lost")


async def _heartbeat_paper_sync_lease(database: Database, lease: PaperSyncLease, stop_event: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cli_module.PAPER_SYNC_LEASE_HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            renewed = database.renew_paper_sync_lease(
                lease.arxiv_id,
                owner_id=lease.owner_id,
                lease_token=lease.lease_token,
                lease_ttl_seconds=cli_module.PAPER_SYNC_LEASE_TTL_SECONDS,
            )
            if not renewed:
                return


async def _heartbeat_resource_lease(database: Database, lease, stop_event: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cli_module.RESOURCE_LEASE_HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            renewed = database.renew_resource_lease(
                lease.resource_key,
                owner_id=lease.owner_id,
                lease_token=lease.lease_token,
                lease_ttl_seconds=cli_module.RESOURCE_LEASE_TTL_SECONDS,
            )
            if not renewed:
                return
