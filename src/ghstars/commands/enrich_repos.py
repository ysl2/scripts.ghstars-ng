from __future__ import annotations

import asyncio
import uuid

from src.ghstars import cli as cli_module
from src.ghstars.commands._common import (
    _build_sync_owner_id,
    _ensure_resource_lease,
    _heartbeat_resource_lease,
    _list_papers_for_window,
)
from src.ghstars.normalize.github import normalize_github_url
from src.ghstars.providers.github import GitHubClient
from src.ghstars.storage.db import Database


async def _run_enrich_repos(
    database: Database,
    github: GitHubClient,
    categories: tuple[str, ...],
    *,
    window: cli_module.ArxivSyncWindow | None = None,
) -> None:
    if window is None:
        window = cli_module.ArxivSyncWindow()
    papers = _list_papers_for_window(database, categories, window=window)
    seen: set[str] = set()
    owner_id = _build_sync_owner_id()
    for paper in papers:
        for link in database.list_paper_repo_links(paper.arxiv_id):
            normalized = normalize_github_url(link.normalized_repo_url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            resource_key = f"repo:{normalized}"
            lease = database.try_acquire_resource_lease(
                resource_key,
                owner_id=owner_id,
                lease_token=str(uuid.uuid4()),
                lease_ttl_seconds=cli_module.RESOURCE_LEASE_TTL_SECONDS,
            )
            if lease is None:
                print(f"{normalized}: skipped (repo enrich held by another process)")
                continue
            stop_heartbeat = asyncio.Event()
            heartbeat_task = asyncio.create_task(_heartbeat_resource_lease(database, lease, stop_heartbeat))
            try:
                metadata, error = await github.fetch_repo_metadata(normalized)
                if error:
                    print(f"{normalized}: {error}")
                    continue
                _ensure_resource_lease(database, lease)
                if metadata is not None:
                    database.upsert_github_repo(metadata)
                    print(f"{normalized}: enriched")
            finally:
                stop_heartbeat.set()
                await heartbeat_task
                database.release_resource_lease(
                    resource_key,
                    owner_id=lease.owner_id,
                    lease_token=lease.lease_token,
                )
