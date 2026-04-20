from __future__ import annotations

import csv
import hashlib
import json
import re
import struct
import tempfile
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
from sqlalchemy import String, cast, delete, func, or_, select, text
from sqlalchemy.orm import Session, selectinload

from src.ghstars.models import Paper as LegacyPaper
from src.ghstars.net.http import RateLimiter, build_timeout, request_text
from src.ghstars.normalize.arxiv import build_arxiv_abs_url, extract_arxiv_id
from src.ghstars.normalize.github import extract_owner_repo
from src.ghstars.providers.alphaxiv_links import (
    AlphaXivLinksClient,
    extract_github_url_from_alphaxiv_html,
    extract_github_url_from_alphaxiv_payload,
)
from src.ghstars.providers.arxiv_links import (
    ArxivLinksClient,
    extract_github_urls_from_abs_html,
    extract_github_urls_from_comment,
)
from src.ghstars.providers.arxiv_metadata import ArxivMetadataClient, parse_papers_from_feed
from src.ghstars.providers.huggingface_links import (
    HuggingFaceLinksClient,
    extract_github_url_from_hf_html,
    extract_github_url_from_hf_payload,
)
from src.ghstars.storage.raw_cache import RawCacheStore
from src.ghstarsv2.config import get_settings
from src.ghstarsv2.models import (
    ArxivArchiveAppearance,
    ArxivSyncWindow,
    ExportRecord,
    GitHubRepo,
    Job,
    JobStatus,
    ObservationStatus,
    Paper,
    PaperRepoState,
    RawFetch,
    RepoObservation,
    RepoStableStatus,
    utc_now,
)
from src.ghstarsv2.scope import (
    month_label,
    month_start,
    resolve_archive_months_from_scope_json,
    resolve_categories_from_scope_json,
    resolve_window_from_scope_json,
)


LINK_TTL = timedelta(days=7)
ARXIV_WINDOW_TTL = timedelta(days=30)
ARXIV_ID_BATCH_SIZE = 100
ARXIV_LIST_PAGE_SIZE = 2000
ARXIV_LIST_ABS_LINK_PATTERN = re.compile(r'href\s*=\s*"/abs/([^"#?]+)"', re.IGNORECASE)
ProgressCallback = Callable[[dict[str, Any]], None]


def ensure_runtime_dirs() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_fetch_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)


def _emit_progress(progress: ProgressCallback | None, stats: dict[str, Any]) -> None:
    if progress is None:
        return
    progress(dict(stats))


def _raw_store() -> RawCacheStore:
    return RawCacheStore(get_settings().raw_fetch_dir)


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _now_utc() -> datetime:
    return utc_now()


def _today_utc() -> date:
    return _now_utc().date()


def _required_scope_categories(scope_json: dict[str, Any]) -> list[str]:
    categories = resolve_categories_from_scope_json(scope_json)
    if not categories:
        raise RuntimeError("categories is required for sync jobs")
    return categories


def _archive_month_bounds(archive_month: date) -> tuple[date, date]:
    start_date = month_start(archive_month)
    end_date = date.fromordinal(_next_month_start(start_date).toordinal() - 1)
    return start_date, end_date


def _is_closed_arxiv_window(start_date: date, end_date: date) -> bool:
    _ = start_date
    return end_date < _today_utc()


def _get_arxiv_sync_window(db: Session, *, category: str, start_date: date, end_date: date) -> ArxivSyncWindow | None:
    return db.get(
        ArxivSyncWindow,
        {
            "category": category,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


def _arxiv_window_sync_due(
    db: Session,
    *,
    category: str,
    start_date: date,
    end_date: date,
    force: bool,
) -> bool:
    if force:
        return True
    if not _is_closed_arxiv_window(start_date, end_date):
        return True

    window = _get_arxiv_sync_window(db, category=category, start_date=start_date, end_date=end_date)
    if window is None or window.last_completed_at is None:
        return True

    last_completed_at = _coerce_utc(window.last_completed_at)
    assert last_completed_at is not None
    return last_completed_at + ARXIV_WINDOW_TTL <= _now_utc()


def _record_arxiv_window_completed(
    db: Session,
    *,
    category: str,
    start_date: date,
    end_date: date,
) -> None:
    window = _get_arxiv_sync_window(db, category=category, start_date=start_date, end_date=end_date)
    if window is None:
        window = ArxivSyncWindow(
            category=category,
            start_date=start_date,
            end_date=end_date,
        )
        db.add(window)
    window.last_completed_at = _now_utc()


def _categories_json_contains_any(categories: list[str]) -> object:
    return or_(*[cast(Paper.categories_json, String).like(f'%"{category}"%') for category in categories])


def _paper_scope_conditions(scope_json: dict[str, Any]) -> list[object]:
    categories = resolve_categories_from_scope_json(scope_json)
    window = resolve_window_from_scope_json(scope_json)
    conditions: list[object] = []
    if categories:
        conditions.append(_categories_json_contains_any(categories))
    if window.start_date is not None:
        conditions.append(Paper.published_at >= window.start_date)
    if window.end_date is not None:
        conditions.append(Paper.published_at <= window.end_date)
    return conditions


def scoped_papers(
    db: Session,
    scope_json: dict[str, Any],
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> list[Paper]:
    stmt = select(Paper).options(selectinload(Paper.repo_state)).where(*_paper_scope_conditions(scope_json))
    stmt = stmt.order_by(Paper.published_at.desc().nullslast(), Paper.arxiv_id.desc())
    if offset:
        stmt = stmt.offset(offset)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.scalars(stmt))


def scoped_repos(db: Session, scope_json: dict[str, Any], *, limit: int | None = None) -> list[GitHubRepo]:
    repo_urls = (
        select(PaperRepoState.primary_repo_url)
        .join(Paper, Paper.arxiv_id == PaperRepoState.arxiv_id)
        .where(PaperRepoState.primary_repo_url.is_not(None), *_paper_scope_conditions(scope_json))
        .distinct()
    )
    stmt = select(GitHubRepo).where(GitHubRepo.normalized_github_url.in_(repo_urls))
    stmt = stmt.order_by(GitHubRepo.stars.desc().nullslast(), GitHubRepo.checked_at.desc().nullslast())
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.scalars(stmt))


def all_papers(db: Session) -> list[Paper]:
    stmt = select(Paper).order_by(Paper.published_at.desc().nullslast(), Paper.arxiv_id.desc())
    return list(db.scalars(stmt))


def get_dashboard_stats(db: Session, scope_json: dict[str, Any]) -> dict[str, Any]:
    paper_count = db.scalar(select(func.count()).select_from(Paper).where(*_paper_scope_conditions(scope_json))) or 0
    counts = {
        "papers": paper_count,
        "found": 0,
        "not_found": 0,
        "ambiguous": 0,
        "unknown": paper_count,
    }
    status_rows = db.execute(
        select(
            PaperRepoState.stable_status,
            func.count(),
        )
        .select_from(Paper)
        .join(PaperRepoState, PaperRepoState.arxiv_id == Paper.arxiv_id)
        .where(*_paper_scope_conditions(scope_json))
        .group_by(PaperRepoState.stable_status)
    ).all()
    for status, value in status_rows:
        key = status.value if isinstance(status, RepoStableStatus) else str(status)
        if key in counts:
            counts[key] = int(value)
            counts["unknown"] -= int(value)

    repo_urls = (
        select(PaperRepoState.primary_repo_url)
        .join(Paper, Paper.arxiv_id == PaperRepoState.arxiv_id)
        .where(PaperRepoState.primary_repo_url.is_not(None), *_paper_scope_conditions(scope_json))
        .distinct()
    )
    counts["repos"] = db.scalar(
        select(func.count()).select_from(GitHubRepo).where(GitHubRepo.normalized_github_url.in_(repo_urls))
    ) or 0
    counts["exports"] = db.scalar(select(func.count()).select_from(ExportRecord)) or 0
    counts["pending_jobs"] = db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.pending)) or 0
    counts["running_jobs"] = db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.running)) or 0
    return counts


def _legacy_paper_to_model(paper: LegacyPaper) -> dict[str, Any]:
    return {
        "arxiv_id": paper.arxiv_id,
        "abs_url": paper.abs_url,
        "title": paper.title,
        "abstract": paper.abstract,
        "published_at": date.fromisoformat(paper.published_at) if paper.published_at else None,
        "updated_at": date.fromisoformat(paper.updated_at) if paper.updated_at else None,
        "authors_json": list(paper.authors),
        "categories_json": list(paper.categories),
        "comment": paper.comment,
        "primary_category": paper.primary_category,
    }


def upsert_paper(db: Session, paper: LegacyPaper) -> None:
    payload = _legacy_paper_to_model(paper)
    existing = db.get(Paper, paper.arxiv_id)
    now = utc_now()
    if existing is None:
        db.add(
            Paper(
                **payload,
                source_first_seen_at=now,
                source_last_seen_at=now,
            )
        )
        return

    for key, value in payload.items():
        setattr(existing, key, value)
    existing.source_last_seen_at = now


def _store_raw_fetch(
    db: Session,
    *,
    provider: str,
    surface: str,
    request_key: str,
    request_url: str,
    status_code: int | None,
    body: str | None,
    headers: dict[str, str],
) -> str | None:
    if status_code is None or body is None:
        return None
    path, content_hash = _raw_store().write_body(
        provider=provider,
        surface=surface,
        request_key=request_key,
        body=body,
        content_type=headers.get("Content-Type"),
    )
    existing = db.scalar(
        select(RawFetch).where(
            RawFetch.provider == provider,
            RawFetch.surface == surface,
            RawFetch.request_key == request_key,
        )
    )
    if existing is None:
        existing = RawFetch(
            provider=provider,
            surface=surface,
            request_key=request_key,
            request_url=request_url,
            status_code=status_code,
            content_type=headers.get("Content-Type"),
            headers_json=dict(headers),
            body_path=str(path),
            content_hash=content_hash,
            etag=headers.get("ETag"),
            last_modified=headers.get("Last-Modified"),
            fetched_at=utc_now(),
        )
        db.add(existing)
    else:
        existing.request_url = request_url
        existing.status_code = status_code
        existing.content_type = headers.get("Content-Type")
        existing.headers_json = dict(headers)
        existing.body_path = str(path)
        existing.content_hash = content_hash
        existing.etag = headers.get("ETag")
        existing.last_modified = headers.get("Last-Modified")
        existing.fetched_at = utc_now()
    db.flush()
    return existing.id


def _hash_lock_key(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()[:8]
    return struct.unpack(">q", digest)[0]


def try_advisory_lock(db: Session, resource_key: str) -> bool:
    if db.bind.dialect.name != "postgresql":
        return True
    lock_id = _hash_lock_key(resource_key)
    return bool(db.scalar(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}))


def release_advisory_lock(db: Session, resource_key: str) -> None:
    if db.bind.dialect.name != "postgresql":
        return
    lock_id = _hash_lock_key(resource_key)
    db.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id})


def _upsert_observations(db: Session, arxiv_id: str, observations: list[dict[str, Any]]) -> None:
    db.execute(delete(RepoObservation).where(RepoObservation.arxiv_id == arxiv_id))
    for item in observations:
        db.add(
            RepoObservation(
                arxiv_id=arxiv_id,
                provider=item["provider"],
                surface=item["surface"],
                status=item["status"],
                observed_repo_url=item.get("observed_repo_url"),
                normalized_repo_url=item.get("normalized_repo_url"),
                evidence_excerpt=item.get("evidence_excerpt"),
                raw_fetch_id=item.get("raw_fetch_id"),
                error_message=item.get("error_message"),
                observed_at=item.get("observed_at") or utc_now(),
            )
        )


def _finalize_repo_urls(observations: list[dict[str, Any]]) -> list[str]:
    grouped: dict[str, tuple[set[str], set[str]]] = {}
    for item in observations:
        if item["status"] != ObservationStatus.found or not item.get("normalized_repo_url"):
            continue
        url = item["normalized_repo_url"]
        providers, surfaces = grouped.setdefault(url, (set(), set()))
        providers.add(item["provider"])
        surfaces.add(f'{item["provider"]}:{item["surface"]}')
    return sorted(
        grouped,
        key=lambda url: (
            -len(grouped[url][0]),
            -len(grouped[url][1]),
            url,
        ),
    )


def _apply_repo_state(
    db: Session,
    paper: Paper,
    *,
    final_urls: list[str],
    complete: bool,
    error_text: str | None,
) -> PaperRepoState:
    state = paper.repo_state or PaperRepoState(arxiv_id=paper.arxiv_id)
    now = utc_now()

    previous_status = state.stable_status
    previous_primary = state.primary_repo_url
    previous_urls = list(state.repo_urls_json or [])
    previous_decided_at = state.stable_decided_at
    previous_refresh_after = state.refresh_after

    if final_urls:
        state.stable_status = RepoStableStatus.ambiguous if len(final_urls) > 1 else RepoStableStatus.found
        state.primary_repo_url = final_urls[0]
        state.repo_urls_json = final_urls
        state.stable_decided_at = now
        state.refresh_after = now + LINK_TTL
    elif complete:
        state.stable_status = RepoStableStatus.not_found
        state.primary_repo_url = None
        state.repo_urls_json = []
        state.stable_decided_at = now
        state.refresh_after = now + LINK_TTL
    elif previous_status in {RepoStableStatus.found, RepoStableStatus.not_found, RepoStableStatus.ambiguous}:
        state.stable_status = previous_status
        state.primary_repo_url = previous_primary
        state.repo_urls_json = previous_urls
        state.stable_decided_at = previous_decided_at
        state.refresh_after = previous_refresh_after
    else:
        state.stable_status = RepoStableStatus.unknown
        state.primary_repo_url = None
        state.repo_urls_json = []
        state.stable_decided_at = None
        state.refresh_after = None

    state.last_attempt_at = now
    state.last_attempt_complete = complete
    state.last_attempt_error = error_text
    db.add(state)
    paper.repo_state = state
    return state


def _link_lookup_due(state: PaperRepoState | None, *, force: bool) -> bool:
    if force or state is None:
        return True
    if state.stable_status == RepoStableStatus.unknown:
        return True
    if state.refresh_after is None:
        return True
    refresh_after = _coerce_utc(state.refresh_after)
    assert refresh_after is not None
    return refresh_after <= utc_now()


def _next_month_start(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _extract_arxiv_ids_from_listing_html(html_text: str) -> list[str]:
    if not html_text:
        return []
    arxiv_ids: list[str] = []
    seen: set[str] = set()
    for raw in ARXIV_LIST_ABS_LINK_PATTERN.findall(html_text):
        arxiv_id = extract_arxiv_id(f"https://arxiv.org/abs/{raw.strip()}") or raw.strip().split("v", 1)[0]
        if not arxiv_id or arxiv_id in seen:
            continue
        seen.add(arxiv_id)
        arxiv_ids.append(arxiv_id)
    return arxiv_ids


def _batch_arxiv_ids(arxiv_ids: list[str], batch_size: int = ARXIV_ID_BATCH_SIZE) -> list[list[str]]:
    return [arxiv_ids[index : index + batch_size] for index in range(0, len(arxiv_ids), batch_size)]


def _record_arxiv_archive_appearances(
    db: Session,
    *,
    category: str,
    archive_month: date,
    arxiv_ids: list[str],
) -> int:
    if not arxiv_ids:
        return 0
    existing = set(
        db.scalars(
            select(ArxivArchiveAppearance.arxiv_id).where(
                ArxivArchiveAppearance.category == category,
                ArxivArchiveAppearance.archive_month == archive_month,
                ArxivArchiveAppearance.arxiv_id.in_(arxiv_ids),
            )
        ).all()
    )
    created = 0
    for arxiv_id in arxiv_ids:
        if arxiv_id in existing:
            continue
        db.add(
            ArxivArchiveAppearance(
                arxiv_id=arxiv_id,
                category=category,
                archive_month=archive_month,
            )
        )
        created += 1
    return created


def _parse_listing_request_key(request_key: str) -> tuple[str, date] | None:
    parts = request_key.split(":")
    if len(parts) != 5 or parts[0] != "list":
        return None
    _, category, period, _skip, _page_size = parts
    try:
        archive_month = date.fromisoformat(f"{period}-01")
    except ValueError:
        return None
    return category, archive_month


def backfill_arxiv_archive_appearances(db: Session) -> dict[str, int]:
    if not try_advisory_lock(db, "arxiv:archive-appearance-backfill"):
        return {"listing_fetches": 0, "appearances_created": 0, "skipped_locked": 1}

    try:
        existing_count = db.scalar(select(func.count()).select_from(ArxivArchiveAppearance)) or 0
        if existing_count > 0:
            return {"listing_fetches": 0, "appearances_created": 0, "skipped_existing": int(existing_count)}

        listing_fetches = list(
            db.scalars(
                select(RawFetch)
                .where(
                    RawFetch.provider == "arxiv",
                    RawFetch.surface == "listing_html",
                )
                .order_by(RawFetch.fetched_at.asc(), RawFetch.id.asc())
            ).all()
        )
        appearances_created = 0

        for raw_fetch in listing_fetches:
            parsed = _parse_listing_request_key(raw_fetch.request_key)
            if parsed is None:
                continue
            category, archive_month = parsed
            body_path = Path(raw_fetch.body_path)
            if not body_path.exists():
                continue
            arxiv_ids = _extract_arxiv_ids_from_listing_html(body_path.read_text(encoding="utf-8"))
            if not arxiv_ids:
                continue
            existing_ids = list(db.scalars(select(Paper.arxiv_id).where(Paper.arxiv_id.in_(arxiv_ids))).all())
            appearances_created += _record_arxiv_archive_appearances(
                db,
                category=category,
                archive_month=archive_month,
                arxiv_ids=existing_ids,
            )

        db.flush()
        return {
            "listing_fetches": len(listing_fetches),
            "appearances_created": appearances_created,
        }
    finally:
        release_advisory_lock(db, "arxiv:archive-appearance-backfill")


async def _sync_arxiv_archive_month(
    db: Session,
    client: ArxivMetadataClient,
    *,
    category: str,
    archive_month: date,
    stats: dict[str, int],
    progress: ProgressCallback | None = None,
) -> None:
    period = month_label(archive_month)
    skip = 0
    while True:
        status, body, headers, error = await client.fetch_listing_page(
            category=category,
            period=period,
            skip=skip,
            show=ARXIV_LIST_PAGE_SIZE,
        )
        if error or body is None or status is None:
            raise RuntimeError(f"{category}: arXiv listing fetch failed for {period} ({error or 'empty response'})")
        stats["pages_fetched"] += 1
        stats["listing_pages_fetched"] += 1
        _store_raw_fetch(
            db,
            provider="arxiv",
            surface="listing_html",
            request_key=f"list:{category}:{period}:{skip}:{ARXIV_LIST_PAGE_SIZE}",
            request_url=f"https://arxiv.org/list/{category}/{period}?skip={skip}&show={ARXIV_LIST_PAGE_SIZE}",
            status_code=status,
            body=body,
            headers=headers,
        )
        _emit_progress(progress, stats)

        arxiv_ids = _extract_arxiv_ids_from_listing_html(body)
        if not arxiv_ids:
            break

        for batch in _batch_arxiv_ids(arxiv_ids):
            batch_key = hashlib.sha1(",".join(batch).encode("utf-8")).hexdigest()[:16]
            feed_status, feed_body, feed_headers, feed_error = await client.fetch_id_list_feed(batch)
            if feed_error or feed_body is None or feed_status is None:
                raise RuntimeError(f"{category}: arXiv metadata batch fetch failed for {period} ({feed_error or 'empty response'})")
            stats["pages_fetched"] += 1
            stats["metadata_batches_fetched"] += 1
            _store_raw_fetch(
                db,
                provider="arxiv",
                surface="id_list_feed",
                request_key=f"id_batch:{category}:{period}:{batch_key}:{len(batch)}",
                request_url=f"https://export.arxiv.org/api/query?id_list_batch={batch_key}&count={len(batch)}",
                status_code=feed_status,
                body=feed_body,
                headers=feed_headers,
            )

            papers = parse_papers_from_feed(feed_body)
            for paper in papers:
                upsert_paper(db, paper)
            _record_arxiv_archive_appearances(
                db,
                category=category,
                archive_month=archive_month,
                arxiv_ids=[paper.arxiv_id for paper in papers],
            )
            db.commit()
            stats["papers_upserted"] += len(papers)
            _emit_progress(progress, stats)

        if len(arxiv_ids) < ARXIV_LIST_PAGE_SIZE:
            break
        skip += ARXIV_LIST_PAGE_SIZE


async def run_sync_arxiv(
    db: Session,
    scope_json: dict[str, Any],
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    settings = get_settings()
    categories = _required_scope_categories(scope_json)
    force = bool(scope_json.get("force"))
    archive_months = resolve_archive_months_from_scope_json(scope_json)
    max_results = int(scope_json.get("max_results") or 100)
    stats = {
        "categories": len(categories),
        "papers_upserted": 0,
        "pages_fetched": 0,
        "listing_pages_fetched": 0,
        "metadata_batches_fetched": 0,
        "categories_skipped_locked": 0,
        "windows_skipped_ttl": 0,
    }
    _emit_progress(progress, stats)

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        client = ArxivMetadataClient(session, min_interval=settings.arxiv_api_min_interval)
        for category in categories:
            if not archive_months:
                lock_key = f"arxiv:{category}:all:{max_results}"
                if not try_advisory_lock(db, lock_key):
                    stats["categories_skipped_locked"] += 1
                    _emit_progress(progress, stats)
                    continue
                try:
                    status, body, headers, error = await client.fetch_category_page(category=category, start=0, max_results=max_results)
                    if error or body is None or status is None:
                        raise RuntimeError(f"{category}: arXiv fetch failed ({error or 'empty response'})")
                    stats["pages_fetched"] += 1
                    _store_raw_fetch(
                        db,
                        provider="arxiv",
                        surface="search_feed",
                        request_key=f"cat:{category}:0:{max_results}",
                        request_url=f"https://export.arxiv.org/api/query?search_query=cat:{category}",
                        status_code=status,
                        body=body,
                        headers=headers,
                    )
                    papers = parse_papers_from_feed(body)
                    for paper in papers:
                        upsert_paper(db, paper)
                    db.commit()
                    stats["papers_upserted"] += len(papers)
                    _emit_progress(progress, stats)
                finally:
                    release_advisory_lock(db, lock_key)
                continue

            for archive_month in archive_months:
                start_date, end_date = _archive_month_bounds(archive_month)
                lock_key = f"arxiv:{category}:{month_label(archive_month)}"
                if not try_advisory_lock(db, lock_key):
                    stats["categories_skipped_locked"] += 1
                    _emit_progress(progress, stats)
                    continue
                try:
                    if not _arxiv_window_sync_due(
                        db,
                        category=category,
                        start_date=start_date,
                        end_date=end_date,
                        force=force,
                    ):
                        stats["windows_skipped_ttl"] += 1
                        _emit_progress(progress, stats)
                        continue

                    await _sync_arxiv_archive_month(
                        db,
                        client,
                        category=category,
                        archive_month=archive_month,
                        stats=stats,
                        progress=progress,
                    )
                    if _is_closed_arxiv_window(start_date, end_date):
                        _record_arxiv_window_completed(
                            db,
                            category=category,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        db.commit()
                finally:
                    release_advisory_lock(db, lock_key)

    return stats


async def _probe_huggingface(
    db: Session,
    client: HuggingFaceLinksClient,
    paper: Paper,
) -> tuple[list[dict[str, Any]], bool]:
    observations: list[dict[str, Any]] = []

    payload_status, payload_body, payload_headers, payload_error = await client.fetch_paper_payload(paper.arxiv_id)
    payload_raw_id = _store_raw_fetch(
        db,
        provider="huggingface",
        surface="paper_api",
        request_key=f"paper_api:{paper.arxiv_id}",
        request_url=f"https://huggingface.co/api/papers/{paper.arxiv_id}",
        status_code=payload_status,
        body=payload_body,
        headers=payload_headers,
    )
    if payload_error and payload_status != 404:
        observations.append(
            {
                "provider": "huggingface",
                "surface": "paper_api",
                "status": ObservationStatus.fetch_failed,
                "error_message": payload_error,
                "raw_fetch_id": payload_raw_id,
            }
        )
        return observations, False

    payload_urls = extract_github_url_from_hf_payload(payload_body)
    if payload_urls:
        for url in payload_urls:
            observations.append(
                {
                    "provider": "huggingface",
                    "surface": "paper_api",
                    "status": ObservationStatus.found,
                    "observed_repo_url": url,
                    "normalized_repo_url": url,
                    "raw_fetch_id": payload_raw_id,
                }
            )
        return observations, True

    observations.append(
        {
            "provider": "huggingface",
            "surface": "paper_api",
            "status": ObservationStatus.checked_no_match,
            "raw_fetch_id": payload_raw_id,
        }
    )
    if payload_status == 404:
        return observations, True

    html_status, html_body, html_headers, html_error = await client.fetch_paper_html(paper.arxiv_id)
    html_raw_id = _store_raw_fetch(
        db,
        provider="huggingface",
        surface="paper_html",
        request_key=f"paper_html:{paper.arxiv_id}",
        request_url=f"https://huggingface.co/papers/{paper.arxiv_id}",
        status_code=html_status,
        body=html_body,
        headers=html_headers,
    )
    if html_error and html_status != 404:
        observations.append(
            {
                "provider": "huggingface",
                "surface": "paper_html",
                "status": ObservationStatus.fetch_failed,
                "error_message": html_error,
                "raw_fetch_id": html_raw_id,
            }
        )
        return observations, False

    html_urls = extract_github_url_from_hf_html(html_body)
    if html_urls:
        for url in html_urls:
            observations.append(
                {
                    "provider": "huggingface",
                    "surface": "paper_html",
                    "status": ObservationStatus.found,
                    "observed_repo_url": url,
                    "normalized_repo_url": url,
                    "raw_fetch_id": html_raw_id,
                }
            )
    else:
        observations.append(
            {
                "provider": "huggingface",
                "surface": "paper_html",
                "status": ObservationStatus.checked_no_match,
                "raw_fetch_id": html_raw_id,
            }
        )
    return observations, True


async def _probe_alphaxiv(
    db: Session,
    client: AlphaXivLinksClient,
    paper: Paper,
) -> tuple[list[dict[str, Any]], bool]:
    observations: list[dict[str, Any]] = []

    payload_status, payload_body, payload_headers, payload_error = await client.fetch_paper_payload(paper.arxiv_id)
    payload_raw_id = _store_raw_fetch(
        db,
        provider="alphaxiv",
        surface="paper_api",
        request_key=f"paper_api:{paper.arxiv_id}",
        request_url=f"https://api.alphaxiv.org/papers/v3/{paper.arxiv_id}",
        status_code=payload_status,
        body=payload_body,
        headers=payload_headers,
    )
    if payload_error and payload_status != 404:
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_api",
                "status": ObservationStatus.fetch_failed,
                "error_message": payload_error,
                "raw_fetch_id": payload_raw_id,
            }
        )
        return observations, False

    payload_urls = extract_github_url_from_alphaxiv_payload(payload_body)
    if payload_urls:
        for url in payload_urls:
            observations.append(
                {
                    "provider": "alphaxiv",
                    "surface": "paper_api",
                    "status": ObservationStatus.found,
                    "observed_repo_url": url,
                    "normalized_repo_url": url,
                    "raw_fetch_id": payload_raw_id,
                }
            )
        return observations, True

    observations.append(
        {
            "provider": "alphaxiv",
            "surface": "paper_api",
            "status": ObservationStatus.checked_no_match,
            "raw_fetch_id": payload_raw_id,
        }
    )
    if payload_status == 404:
        return observations, True

    html_status, html_body, html_headers, html_error = await client.fetch_paper_html(paper.arxiv_id)
    html_raw_id = _store_raw_fetch(
        db,
        provider="alphaxiv",
        surface="paper_html",
        request_key=f"paper_html:{paper.arxiv_id}",
        request_url=f"https://www.alphaxiv.org/abs/{paper.arxiv_id}",
        status_code=html_status,
        body=html_body,
        headers=html_headers,
    )
    if html_error and html_status != 404:
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_html",
                "status": ObservationStatus.fetch_failed,
                "error_message": html_error,
                "raw_fetch_id": html_raw_id,
            }
        )
        return observations, False

    html_urls = extract_github_url_from_alphaxiv_html(html_body)
    if html_urls:
        for url in html_urls:
            observations.append(
                {
                    "provider": "alphaxiv",
                    "surface": "paper_html",
                    "status": ObservationStatus.found,
                    "observed_repo_url": url,
                    "normalized_repo_url": url,
                    "raw_fetch_id": html_raw_id,
                }
            )
    else:
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_html",
                "status": ObservationStatus.checked_no_match,
                "raw_fetch_id": html_raw_id,
            }
        )
    return observations, True


async def run_sync_links(
    db: Session,
    scope_json: dict[str, Any],
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    _required_scope_categories(scope_json)
    papers = scoped_papers(db, scope_json)
    force = bool(scope_json.get("force"))
    due_papers = [paper for paper in papers if _link_lookup_due(paper.repo_state, force=force)]
    stats = {
        "papers_considered": len(papers),
        "papers_processed": 0,
        "papers_skipped_fresh": len(papers) - len(due_papers),
        "found": 0,
        "not_found": 0,
        "ambiguous": 0,
        "unknown": 0,
        "skipped_locked": 0,
    }
    _emit_progress(progress, stats)

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        arxiv_client = ArxivLinksClient(session, min_interval=settings.arxiv_api_min_interval)
        hf_client = HuggingFaceLinksClient(
            session,
            huggingface_token=settings.huggingface_token,
            min_interval=settings.huggingface_min_interval,
        )
        alphaxiv_client = AlphaXivLinksClient(
            session,
            alphaxiv_token=settings.alphaxiv_token,
            min_interval=settings.huggingface_min_interval,
        )
        for paper in due_papers:
            lock_key = f"paper:{paper.arxiv_id}"
            if not try_advisory_lock(db, lock_key):
                stats["skipped_locked"] += 1
                _emit_progress(progress, stats)
                continue
            try:
                observations: list[dict[str, Any]] = []
                errors: list[str] = []
                complete = True

                comment_urls = extract_github_urls_from_comment(paper.comment)
                if comment_urls:
                    for url in comment_urls:
                        observations.append(
                            {
                                "provider": "arxiv",
                                "surface": "comment",
                                "status": ObservationStatus.found,
                                "observed_repo_url": url,
                                "normalized_repo_url": url,
                                "evidence_excerpt": paper.comment,
                            }
                        )
                else:
                    observations.append(
                        {
                            "provider": "arxiv",
                            "surface": "comment",
                            "status": ObservationStatus.checked_no_match,
                        }
                    )

                abs_status, abs_body, abs_headers, abs_error = await arxiv_client.fetch_abs_html(paper.arxiv_id)
                abs_raw_id = _store_raw_fetch(
                    db,
                    provider="arxiv",
                    surface="abs_html",
                    request_key=f"abs:{paper.arxiv_id}",
                    request_url=build_arxiv_abs_url(paper.arxiv_id),
                    status_code=abs_status,
                    body=abs_body,
                    headers=abs_headers,
                )
                if abs_error or abs_status is None:
                    complete = False
                    errors.append(abs_error or "arXiv abs fetch failed")
                    observations.append(
                        {
                            "provider": "arxiv",
                            "surface": "abs_html",
                            "status": ObservationStatus.fetch_failed,
                            "error_message": abs_error or "arXiv abs fetch failed",
                            "raw_fetch_id": abs_raw_id,
                        }
                    )
                else:
                    abs_urls = extract_github_urls_from_abs_html(abs_body)
                    if abs_urls:
                        for url in abs_urls:
                            observations.append(
                                {
                                    "provider": "arxiv",
                                    "surface": "abs_html",
                                    "status": ObservationStatus.found,
                                    "observed_repo_url": url,
                                    "normalized_repo_url": url,
                                    "raw_fetch_id": abs_raw_id,
                                }
                            )
                    else:
                        observations.append(
                            {
                                "provider": "arxiv",
                                "surface": "abs_html",
                                "status": ObservationStatus.checked_no_match,
                                "raw_fetch_id": abs_raw_id,
                            }
                        )

                final_urls = _finalize_repo_urls(observations)
                if not final_urls and settings.huggingface_enabled:
                    hf_observations, hf_complete = await _probe_huggingface(db, hf_client, paper)
                    observations.extend(hf_observations)
                    complete = complete and hf_complete
                    if not hf_complete:
                        errors.append("Hugging Face lookup incomplete")
                    final_urls = _finalize_repo_urls(observations)

                if not final_urls and settings.alphaxiv_enabled:
                    alphaxiv_observations, alphaxiv_complete = await _probe_alphaxiv(db, alphaxiv_client, paper)
                    observations.extend(alphaxiv_observations)
                    complete = complete and alphaxiv_complete
                    if not alphaxiv_complete:
                        errors.append("AlphaXiv lookup incomplete")
                    final_urls = _finalize_repo_urls(observations)

                _upsert_observations(db, paper.arxiv_id, observations)
                state = _apply_repo_state(
                    db,
                    paper,
                    final_urls=final_urls,
                    complete=complete,
                    error_text="; ".join(errors) if errors else None,
                )
                db.commit()
                stats["papers_processed"] += 1
                stats[state.stable_status.value] += 1
                _emit_progress(progress, stats)
            finally:
                release_advisory_lock(db, lock_key)

    return stats


async def _fetch_github_repo(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    semaphore: aiohttp.BaseConnector | Any,
    normalized_url: str,
    existing: GitHubRepo | None,
) -> tuple[str, dict[str, Any] | None, dict[str, str]]:
    settings = get_settings()
    owner_repo = extract_owner_repo(normalized_url)
    if owner_repo is None:
        raise RuntimeError(f"{normalized_url} is not a valid GitHub repository URL")
    owner, repo = owner_repo
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ghstars-v2",
    }
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    if existing is not None:
        if existing.etag:
            headers["If-None-Match"] = existing.etag
        if existing.last_modified:
            headers["If-Modified-Since"] = existing.last_modified

    status, body, response_headers, error = await request_text(
        session,
        f"https://api.github.com/repos/{owner}/{repo}",
        headers=headers,
        semaphore=semaphore,
        rate_limiter=limiter,
        retry_prefix="GitHub API",
        allowed_statuses={304, 404},
    )
    if error:
        raise RuntimeError(error)
    if status == 304:
        return "not_modified", None, response_headers
    if status == 404:
        return "missing", None, response_headers
    if body is None:
        raise RuntimeError("GitHub API returned empty body")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub API returned invalid JSON") from exc
    return "ok", payload, response_headers


def _license_from_payload(payload: dict[str, Any]) -> str | None:
    license_info = payload.get("license") if isinstance(payload.get("license"), dict) else None
    return (license_info or {}).get("spdx_id") or (license_info or {}).get("name")


async def run_enrich(
    db: Session,
    scope_json: dict[str, Any],
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    _required_scope_categories(scope_json)
    papers = scoped_papers(db, scope_json)
    repo_urls: set[str] = set()
    for paper in papers:
        if paper.repo_state is None:
            continue
        repo_urls.update(paper.repo_state.repo_urls_json or [])

    stats = {"repos_considered": len(repo_urls), "updated": 0, "not_modified": 0, "missing": 0, "skipped_locked": 0}
    _emit_progress(progress, stats)
    min_interval = settings.github_min_interval if settings.github_token.strip() else max(settings.github_min_interval, 60.0)
    limiter = RateLimiter(min_interval)

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        import asyncio

        semaphore = asyncio.Semaphore(1)
        for normalized_url in sorted(repo_urls):
            lock_key = f"repo:{normalized_url}"
            if not try_advisory_lock(db, lock_key):
                stats["skipped_locked"] += 1
                _emit_progress(progress, stats)
                continue
            try:
                existing = db.get(GitHubRepo, normalized_url)
                status, payload, headers = await _fetch_github_repo(session, limiter, semaphore, normalized_url, existing)
                now = utc_now()
                if status == "not_modified":
                    if existing is not None:
                        existing.checked_at = now
                    stats["not_modified"] += 1
                    db.commit()
                    _emit_progress(progress, stats)
                    continue
                if status == "missing":
                    if existing is not None:
                        existing.checked_at = now
                    stats["missing"] += 1
                    db.commit()
                    _emit_progress(progress, stats)
                    continue

                assert payload is not None
                repo = existing or GitHubRepo(
                    normalized_github_url=normalized_url,
                    owner=payload.get("owner", {}).get("login") or "",
                    repo=payload.get("name") or "",
                    first_seen_at=now,
                )

                repo.owner = payload.get("owner", {}).get("login") or repo.owner
                repo.repo = payload.get("name") or repo.repo
                if repo.github_id is None:
                    repo.github_id = payload.get("id")
                if repo.created_at is None:
                    repo.created_at = payload.get("created_at")

                repo.stars = payload.get("stargazers_count")
                repo.description = payload.get("description") or ""
                repo.homepage = payload.get("homepage")
                repo.topics_json = payload.get("topics") or []
                repo.license = _license_from_payload(payload)
                repo.archived = bool(payload.get("archived"))
                repo.pushed_at = payload.get("pushed_at")
                repo.checked_at = now
                repo.etag = headers.get("ETag") if headers else repo.etag
                repo.last_modified = headers.get("Last-Modified") if headers else repo.last_modified
                db.add(repo)
                db.commit()
                stats["updated"] += 1
                _emit_progress(progress, stats)
            finally:
                release_advisory_lock(db, lock_key)
    return stats


def run_export(db: Session, scope_json: dict[str, Any]) -> dict[str, Any]:
    ensure_runtime_dirs()
    settings = get_settings()
    export_mode = str(scope_json.get("export_mode") or "").strip()
    if export_mode == "all_papers":
        papers = all_papers(db)
    elif export_mode == "papers_view":
        requested_ids = [str(item).strip() for item in (scope_json.get("paper_ids") or []) if str(item).strip()]
        if not requested_ids:
            raise RuntimeError("Filtered export requires at least one visible paper")
        available = {
            paper.arxiv_id: paper
            for paper in db.scalars(select(Paper).where(Paper.arxiv_id.in_(requested_ids))).all()
        }
        papers = [available[paper_id] for paper_id in requested_ids if paper_id in available]
    else:
        papers = scoped_papers(db, scope_json)
    timestamp = utc_now().strftime("%Y%m%d-%H%M%S")
    categories = "-".join(scope_json.get("categories") or settings.default_categories_list)
    default_name = (
        "papers-all"
        if export_mode == "all_papers"
        else "papers-view"
        if export_mode == "papers_view"
        else categories
    )
    output_name = (scope_json.get("output_name") or f"{default_name}-{timestamp}").strip()
    if not output_name.endswith(".csv"):
        output_name = f"{output_name}.csv"
    final_path = settings.export_dir / output_name

    rows: list[dict[str, Any]] = []
    for paper in papers:
        state = paper.repo_state
        status = state.stable_status.value if state is not None else RepoStableStatus.unknown.value
        primary_url = state.primary_repo_url if state is not None and state.primary_repo_url else ""
        metadata = db.get(GitHubRepo, primary_url) if primary_url else None
        rows.append(
            {
                "arxiv_id": paper.arxiv_id,
                "abs_url": paper.abs_url,
                "title": paper.title,
                "abstract": paper.abstract,
                "published_at": paper.published_at.isoformat() if paper.published_at else "",
                "categories": ", ".join(paper.categories_json or []),
                "primary_category": paper.primary_category or "",
                "github_primary": primary_url,
                "github_all": "; ".join((state.repo_urls_json if state is not None else []) or []),
                "link_status": status,
                "stars": metadata.stars if metadata is not None and metadata.stars is not None else "",
                "created_at": metadata.created_at if metadata is not None and metadata.created_at is not None else "",
                "description": metadata.description if metadata is not None and metadata.description is not None else "",
                "link_refresh_after": state.refresh_after.isoformat() if state is not None and state.refresh_after else "",
                "repo_checked_at": metadata.checked_at.isoformat() if metadata is not None and metadata.checked_at else "",
            }
        )

    final_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=final_path.parent, delete=False) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "arxiv_id",
                "abs_url",
                "title",
                "abstract",
                "published_at",
                "categories",
                "primary_category",
                "github_primary",
                "github_all",
                "link_status",
                "stars",
                "created_at",
                "description",
                "link_refresh_after",
                "repo_checked_at",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
        temp_path = Path(handle.name)
    temp_path.replace(final_path)

    export_record = ExportRecord(
        file_name=final_path.name,
        file_path=str(final_path),
        scope_json=scope_json,
    )
    db.add(export_record)
    db.commit()
    return {"rows": len(rows), "file_name": final_path.name, "export_id": export_record.id, "export_mode": export_mode or "scoped"}
