from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import re
import struct
import tempfile
import time
import weakref
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
from sqlalchemy import String, cast, delete, func, or_, select, text
from sqlalchemy.orm import Session, selectinload

from papertorepo.core.records import Paper as ParsedPaper
from papertorepo.core.http import RateLimiter, build_timeout, request_text
from papertorepo.core.normalize.arxiv import extract_arxiv_id
from papertorepo.core.normalize.github import extract_github_repo_urls, extract_owner_repo
from papertorepo.providers.alphaxiv_links import (
    AlphaXivLinksClient,
    extract_github_url_from_alphaxiv_html,
    extract_github_url_from_alphaxiv_payload,
)
from papertorepo.providers.arxiv_metadata import ArxivMetadataClient, parse_papers_from_feed
from papertorepo.providers.arxiv_metadata import parse_arxiv_ids_from_feed
from papertorepo.providers.huggingface_links import (
    HuggingFaceLinksClient,
    extract_github_url_from_hf_payload,
)
from papertorepo.providers.github import REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS
from papertorepo.storage.raw_fetch_store import RawCacheStore
from papertorepo.core.config import get_settings
from papertorepo.jobs.batches import is_batch_root_job
from papertorepo.jobs.ordering import job_execution_order_by
from papertorepo.db.models import (
    SyncPapersArxivArchiveAppearance,
    SyncPapersArxivDay,
    ExportRecord,
    GitHubRepo,
    Job,
    JobAttemptMode,
    JobItemResumeProgress,
    JobStatus,
    JobType,
    ObservationStatus,
    Paper,
    PaperRepoState,
    RawFetch,
    RepoObservation,
    RepoStableStatus,
    SyncPapersArxivRequestCheckpoint,
    utc_now,
)
from papertorepo.core.scope import (
    build_scope_payload,
    month_label,
    month_start,
    resolve_archive_months_from_scope_json,
    resolve_categories_from_scope_json,
    resolve_window_from_scope_json,
)


SYNC_PAPERS_ARXIV_CATCHUP_MAX_AGE_DAYS = 90
SYNC_PAPERS_ARXIV_MAX_CONCURRENT = 1
SYNC_PAPERS_ARXIV_LIST_ABS_LINK_PATTERN = re.compile(r'href\s*=\s*"/abs/([^"#?]+)"', re.IGNORECASE)
REFRESH_METADATA_GITHUB_GRAPHQL_MAX_CONCURRENT = 1
REFRESH_METADATA_GITHUB_GRAPHQL_TOPICS_FIRST = 20
RESUME_ITEM_STATUS_COMPLETED = "completed"
RESUME_ITEM_KIND_PAPER = "paper"
RESUME_ITEM_KIND_REPO = "repo"
_SYNC_PAPERS_ARXIV_RATE_LIMITERS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[float, RateLimiter]] = (
    weakref.WeakKeyDictionary()
)
ProgressCallback = Callable[[dict[str, Any]], None]
StopCheck = Callable[[], None]


@dataclass(slots=True)
class RawFetchEnvelope:
    provider: str
    surface: str
    request_key: str
    request_url: str
    status_code: int | None
    body: str | None
    headers: dict[str, str]


@dataclass(frozen=True, slots=True)
class SyncPapersCheckpointContext:
    job_id: str | None
    attempt_series_key: str | None
    attempt_mode: JobAttemptMode | str

    @property
    def can_reuse(self) -> bool:
        attempt_mode = self.attempt_mode.value if isinstance(self.attempt_mode, JobAttemptMode) else str(self.attempt_mode)
        return self.attempt_series_key is not None and attempt_mode == JobAttemptMode.repair.value

    @property
    def can_store(self) -> bool:
        return self.attempt_series_key is not None


@dataclass(frozen=True, slots=True)
class ItemResumeContext:
    job_id: str | None
    attempt_series_key: str | None
    attempt_mode: JobAttemptMode | str
    job_type: JobType | str
    item_kind: str
    force: bool = False

    @property
    def job_type_value(self) -> str:
        return self.job_type.value if isinstance(self.job_type, JobType) else str(self.job_type)

    @property
    def can_reuse(self) -> bool:
        attempt_mode = self.attempt_mode.value if isinstance(self.attempt_mode, JobAttemptMode) else str(self.attempt_mode)
        return self.attempt_series_key is not None and attempt_mode == JobAttemptMode.repair.value and not self.force

    @property
    def can_store(self) -> bool:
        return self.attempt_series_key is not None


@dataclass(frozen=True, slots=True)
class SyncPapersArxivResponse:
    status: int | None
    body: str | None
    headers: dict[str, str]
    error: str | None
    reused_checkpoint: bool = False


@dataclass(slots=True)
class LinkLookupTask:
    arxiv_id: str
    comment: str | None
    abstract: str | None


@dataclass(slots=True)
class LinkLookupResult:
    arxiv_id: str
    observations: list[dict[str, Any]]
    complete: bool
    errors: list[str]
    raw_fetches: dict[str, RawFetchEnvelope] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SyncPapersArxivUnit:
    category: str
    transport: str
    requested_start_date: date
    requested_end_date: date
    fetch_month: date | None = None
    fetch_day: date | None = None


def ensure_runtime_dirs() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_fetch_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)


def _emit_progress(progress: ProgressCallback | None, stats: dict[str, Any]) -> None:
    if progress is None:
        return
    progress(dict(stats))


def _run_stop_check(stop_check: StopCheck | None) -> None:
    if stop_check is None:
        return
    stop_check()


def _raw_store() -> RawCacheStore:
    return RawCacheStore(get_settings().raw_fetch_dir)


def _sync_papers_arxiv_rate_limiter(min_interval: float) -> RateLimiter:
    loop = asyncio.get_running_loop()
    normalized_interval = max(0.0, min_interval)
    rate_limiters = _SYNC_PAPERS_ARXIV_RATE_LIMITERS.setdefault(loop, {})
    limiter = rate_limiters.get(normalized_interval)
    if limiter is None:
        limiter = RateLimiter(normalized_interval)
        rate_limiters[normalized_interval] = limiter
    return limiter


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


def _chunked(items: list[Any], size: int) -> Iterable[list[Any]]:
    chunk_size = max(1, size)
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def _merge_nested_metrics(target: dict[str, Any], delta: dict[str, Any]) -> None:
    for key, value in delta.items():
        if isinstance(value, dict):
            existing = target.get(key)
            if not isinstance(existing, dict):
                existing = {}
                target[key] = existing
            _merge_nested_metrics(existing, value)
            continue
        if isinstance(value, (int, float)):
            target[key] = target.get(key, 0) + value
            continue
        target[key] = value


def _update_runtime_stats(
    stats: dict[str, Any],
    *,
    started_at: float,
    processed_key: str,
    throughput_key: str,
) -> None:
    elapsed_seconds = max(0.0, time.perf_counter() - started_at)
    stats["elapsed_seconds"] = round(elapsed_seconds, 3)
    processed = int(stats.get(processed_key) or 0)
    stats[throughput_key] = round((processed * 60.0 / elapsed_seconds), 2) if elapsed_seconds > 0 else 0.0


def _new_find_repos_metrics() -> dict[str, Any]:
    return {
        "provider_counts": {
            "arxiv": {
                "comment_matches": 0,
                "abstract_matches": 0,
            },
            "huggingface": {
                "api_requests": 0,
                "api_failures": 0,
            },
            "alphaxiv": {
                "api_requests": 0,
                "api_failures": 0,
                "html_requests": 0,
                "html_failures": 0,
            },
        },
        "stage_seconds": {
            "huggingface_api": 0.0,
            "alphaxiv_api": 0.0,
            "alphaxiv_html": 0.0,
            "persist": 0.0,
        },
    }


def _new_refresh_metadata_metrics() -> dict[str, Any]:
    return {
        "provider_counts": {
            "github": {
                "graphql_batches": 0,
                "graphql_batch_failures": 0,
                "graphql_repos": 0,
                "graphql_fallbacks": 0,
                "rest_requests": 0,
                "rest_failures": 0,
            }
        },
        "stage_seconds": {
            "github_graphql": 0.0,
            "github_rest": 0.0,
            "persist": 0.0,
        },
    }


def _required_scope_categories(scope_json: dict[str, Any]) -> list[str]:
    categories = resolve_categories_from_scope_json(scope_json)
    if not categories:
        raise RuntimeError("categories is required for sync jobs")
    return categories


def _required_scope_window(scope_json: dict[str, Any]) -> None:
    window = resolve_window_from_scope_json(scope_json)
    if window.start_date is None or window.end_date is None:
        raise RuntimeError("time window is required for sync jobs")


def _archive_month_bounds(archive_month: date) -> tuple[date, date]:
    start_date = month_start(archive_month)
    end_date = date.fromordinal(_next_month_start(start_date).toordinal() - 1)
    return start_date, end_date


def _iter_natural_days(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _ttl_completed_days(start_date: date, end_date: date) -> list[date]:
    last_completed_day = _today_utc() - timedelta(days=1)
    if start_date > end_date or start_date > last_completed_day:
        return []
    effective_end = min(end_date, last_completed_day)
    return list(_iter_natural_days(start_date, effective_end))


def _sync_day_last_completed_at(db: Session, *, category: str, sync_day: date) -> datetime | None:
    record = db.get(SyncPapersArxivDay, {"category": category, "sync_day": sync_day})
    return _coerce_utc(record.last_completed_at) if record is not None else None


def _sync_day_is_stale(db: Session, *, category: str, sync_day: date) -> bool:
    last_completed_at = _sync_day_last_completed_at(db, category=category, sync_day=sync_day)
    if last_completed_at is None:
        return True
    ttl_days = max(1, get_settings().sync_papers_arxiv_ttl_days)
    return last_completed_at + timedelta(days=ttl_days) <= _now_utc()


def _requested_arxiv_window_sync_due(
    db: Session,
    *,
    category: str,
    start_date: date,
    end_date: date,
    force: bool,
) -> bool:
    if force:
        return True
    for sync_day in _iter_natural_days(start_date, end_date):
        if sync_day >= _today_utc():
            return True
        if _sync_day_is_stale(db, category=category, sync_day=sync_day):
            return True
    return False


def _record_arxiv_days_completed(
    db: Session,
    *,
    category: str,
    sync_days: list[date],
) -> None:
    if not sync_days:
        return
    completed_at = _now_utc()
    for sync_day in sync_days:
        record = db.get(SyncPapersArxivDay, {"category": category, "sync_day": sync_day})
        if record is None:
            record = SyncPapersArxivDay(category=category, sync_day=sync_day)
            db.add(record)
        record.last_completed_at = completed_at


def _categories_json_contains_any(categories: list[str]) -> object:
    return or_(*[cast(Paper.categories_json, String).like(f'%"{category}"%') for category in categories])


def _job_sort_timestamp(value: datetime | None) -> float:
    coerced = _coerce_utc(value)
    if coerced is None:
        return float("-inf")
    return coerced.timestamp()


def _pick_current_queue_job(active_jobs: list[Job]) -> Job | None:
    if not active_jobs:
        return None
    return sorted(
        active_jobs,
        key=lambda job: (
            1 if is_batch_root_job(job.job_type, job.parent_job_id) else 0,
            -_job_sort_timestamp(job.locked_at),
            -_job_sort_timestamp(job.started_at),
            -_job_sort_timestamp(job.created_at),
            job.id,
        ),
    )[0]


def get_job_queue_snapshot(db: Session) -> dict[str, Any]:
    active_jobs = list(db.scalars(select(Job).where(Job.status == JobStatus.running)).all())
    current_job = _pick_current_queue_job(active_jobs)
    next_job = db.scalar(select(Job).where(Job.status == JobStatus.pending).order_by(*job_execution_order_by()))
    if current_job is not None:
        state = "active"
    elif next_job is not None:
        state = "waiting"
    else:
        state = "idle"
    return {
        "state": state,
        "current_job_id": current_job.id if current_job is not None else None,
        "next_job_id": next_job.id if next_job is not None else None,
    }


def _paper_scope_conditions(scope_json: dict[str, Any]) -> list[object]:
    categories = resolve_categories_from_scope_json(scope_json)
    window = resolve_window_from_scope_json(scope_json)
    conditions: list[object] = []
    if categories:
        conditions.append(_categories_json_contains_any(categories))
    if window.start_date is not None:
        conditions.append(
            Paper.published_at
            >= datetime.combine(window.start_date, datetime.min.time(), tzinfo=timezone.utc)
        )
    if window.end_date is not None:
        conditions.append(
            Paper.published_at
            < datetime.combine(window.end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        )
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
        "unknown": 0,
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
    known_status_total = 0
    for status, value in status_rows:
        key = status.value if isinstance(status, RepoStableStatus) else str(status)
        if key in {"found", "not_found", "ambiguous"}:
            counts[key] = int(value)
            known_status_total += int(value)

    # "unknown" should include both papers with an explicit unknown repo state and
    # papers that have not created any repo-state row yet. Compute it from the
    # paper total instead of mutating it inside the grouped status loop so the
    # result is stable regardless of SQL row ordering.
    counts["unknown"] = paper_count - known_status_total

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
    counts["stopping_jobs"] = db.scalar(
        select(func.count()).select_from(Job).where(Job.status == JobStatus.running, Job.stop_requested_at.is_not(None))
    ) or 0
    counts["running_jobs"] = db.scalar(
        select(func.count()).select_from(Job).where(Job.status == JobStatus.running, Job.stop_requested_at.is_(None))
    ) or 0
    return counts


def _parsed_paper_to_model(paper: ParsedPaper) -> dict[str, Any]:
    return {
        "arxiv_id": paper.arxiv_id,
        "entry_id": paper.entry_id,
        "abs_url": paper.abs_url,
        "title": paper.title,
        "abstract": paper.abstract,
        "published_at": paper.published_at,
        "updated_at": paper.updated_at,
        "authors_json": list(paper.authors),
        "author_details_json": list(paper.author_details),
        "categories_json": list(paper.categories),
        "category_details_json": list(paper.category_details),
        "links_json": list(paper.links),
        "comment": paper.comment,
        "journal_ref": paper.journal_ref,
        "doi": paper.doi,
        "primary_category": paper.primary_category,
        "primary_category_scheme": paper.primary_category_scheme,
    }


def upsert_paper(db: Session, paper: ParsedPaper) -> None:
    payload = _parsed_paper_to_model(paper)
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


def _record_sync_papers_checkpoint_reuse(stats: dict[str, Any], surface: str) -> None:
    stats["checkpoint_reused"] += 1
    if surface == "id_list_feed":
        stats["checkpoint_metadata_batches_reused"] += 1
    else:
        stats["checkpoint_pages_reused"] += 1


def _completed_resume_item_keys(db: Session, context: ItemResumeContext) -> set[str]:
    if not context.can_reuse:
        return set()
    return set(
        db.scalars(
            select(JobItemResumeProgress.item_key).where(
                JobItemResumeProgress.attempt_series_key == context.attempt_series_key,
                JobItemResumeProgress.job_type == context.job_type_value,
                JobItemResumeProgress.item_kind == context.item_kind,
                JobItemResumeProgress.status == RESUME_ITEM_STATUS_COMPLETED,
            )
        ).all()
    )


def _record_resume_item_completed(db: Session, context: ItemResumeContext, item_key: str) -> None:
    if not context.can_store:
        return
    now = utc_now()
    source_job_id = context.job_id if context.job_id is not None and db.get(Job, context.job_id) is not None else None
    progress = db.scalar(
        select(JobItemResumeProgress).where(
            JobItemResumeProgress.attempt_series_key == context.attempt_series_key,
            JobItemResumeProgress.job_type == context.job_type_value,
            JobItemResumeProgress.item_kind == context.item_kind,
            JobItemResumeProgress.item_key == item_key,
        )
    )
    if progress is None:
        progress = JobItemResumeProgress(
            attempt_series_key=str(context.attempt_series_key),
            job_type=context.job_type_value,
            item_kind=context.item_kind,
            item_key=item_key,
            status=RESUME_ITEM_STATUS_COMPLETED,
            created_at=now,
        )
        db.add(progress)
    progress.status = RESUME_ITEM_STATUS_COMPLETED
    progress.source_job_id = source_job_id
    progress.updated_at = now
    db.flush()


def _load_sync_papers_arxiv_checkpoint(
    db: Session,
    context: SyncPapersCheckpointContext | None,
    *,
    surface: str,
    request_key: str,
) -> SyncPapersArxivResponse | None:
    if context is None or not context.can_reuse:
        return None
    checkpoint = db.scalar(
        select(SyncPapersArxivRequestCheckpoint).where(
            SyncPapersArxivRequestCheckpoint.attempt_series_key == context.attempt_series_key,
            SyncPapersArxivRequestCheckpoint.surface == surface,
            SyncPapersArxivRequestCheckpoint.request_key == request_key,
        )
    )
    if checkpoint is None:
        return None
    body_path = Path(checkpoint.body_path)
    if not body_path.exists():
        return None
    return SyncPapersArxivResponse(
        status=checkpoint.status_code,
        body=body_path.read_text(encoding="utf-8"),
        headers=dict(checkpoint.headers_json or {}),
        error=None,
        reused_checkpoint=True,
    )


def _store_sync_papers_arxiv_checkpoint(
    db: Session,
    context: SyncPapersCheckpointContext | None,
    *,
    surface: str,
    request_key: str,
    request_url: str,
    raw_fetch_id: str | None,
) -> None:
    if context is None or not context.can_store or raw_fetch_id is None:
        return
    raw_fetch = db.get(RawFetch, raw_fetch_id)
    if raw_fetch is None:
        return
    now = utc_now()
    checkpoint = db.scalar(
        select(SyncPapersArxivRequestCheckpoint).where(
            SyncPapersArxivRequestCheckpoint.attempt_series_key == context.attempt_series_key,
            SyncPapersArxivRequestCheckpoint.surface == surface,
            SyncPapersArxivRequestCheckpoint.request_key == request_key,
        )
    )
    if checkpoint is None:
        checkpoint = SyncPapersArxivRequestCheckpoint(
            attempt_series_key=str(context.attempt_series_key),
            surface=surface,
            request_key=request_key,
            created_at=now,
        )
        db.add(checkpoint)
    checkpoint.source_job_id = context.job_id
    checkpoint.request_url = request_url
    checkpoint.status_code = raw_fetch.status_code
    checkpoint.content_type = raw_fetch.content_type
    checkpoint.headers_json = dict(raw_fetch.headers_json or {})
    checkpoint.body_path = raw_fetch.body_path
    checkpoint.content_hash = raw_fetch.content_hash
    checkpoint.raw_fetch_id = raw_fetch.id
    checkpoint.updated_at = now
    db.flush()


async def _fetch_sync_papers_arxiv_request(
    db: Session,
    context: SyncPapersCheckpointContext | None,
    *,
    surface: str,
    request_key: str,
    request_url: str,
    stats: dict[str, Any],
    fetch: Callable[[], Awaitable[tuple[int | None, str | None, dict[str, str], str | None]]],
) -> SyncPapersArxivResponse:
    checkpoint = _load_sync_papers_arxiv_checkpoint(
        db,
        context,
        surface=surface,
        request_key=request_key,
    )
    if checkpoint is not None:
        _record_sync_papers_checkpoint_reuse(stats, surface)
        return checkpoint

    status, body, headers, error = await fetch()
    if error is None and body is not None and status is not None:
        raw_fetch_id = _store_raw_fetch(
            db,
            provider="arxiv",
            surface=surface,
            request_key=request_key,
            request_url=request_url,
            status_code=status,
            body=body,
            headers=headers,
        )
        _store_sync_papers_arxiv_checkpoint(
            db,
            context,
            surface=surface,
            request_key=request_key,
            request_url=request_url,
            raw_fetch_id=raw_fetch_id,
        )
    return SyncPapersArxivResponse(status=status, body=body, headers=headers, error=error)


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
    link_ttl = timedelta(days=max(1, get_settings().find_repos_link_ttl_days))

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
        state.refresh_after = now + link_ttl
    elif complete:
        state.stable_status = RepoStableStatus.not_found
        state.primary_repo_url = None
        state.repo_urls_json = []
        state.stable_decided_at = now
        state.refresh_after = now + link_ttl
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
    for raw in SYNC_PAPERS_ARXIV_LIST_ABS_LINK_PATTERN.findall(html_text):
        arxiv_id = extract_arxiv_id(f"https://arxiv.org/abs/{raw.strip()}") or raw.strip().split("v", 1)[0]
        if not arxiv_id or arxiv_id in seen:
            continue
        seen.add(arxiv_id)
        arxiv_ids.append(arxiv_id)
    return arxiv_ids


def _batch_arxiv_ids(arxiv_ids: list[str]) -> list[list[str]]:
    batch_size = max(1, get_settings().sync_papers_arxiv_id_batch_size)
    return [arxiv_ids[index : index + batch_size] for index in range(0, len(arxiv_ids), batch_size)]


def _record_sync_papers_arxiv_archive_appearances(
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
            select(SyncPapersArxivArchiveAppearance.arxiv_id).where(
                SyncPapersArxivArchiveAppearance.category == category,
                SyncPapersArxivArchiveAppearance.archive_month == archive_month,
                SyncPapersArxivArchiveAppearance.arxiv_id.in_(arxiv_ids),
            )
        ).all()
    )
    created = 0
    for arxiv_id in arxiv_ids:
        if arxiv_id in existing:
            continue
        db.add(
            SyncPapersArxivArchiveAppearance(
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


def backfill_sync_papers_arxiv_archive_appearances(db: Session) -> dict[str, int]:
    if not try_advisory_lock(db, "arxiv:archive-appearance-backfill"):
        return {"listing_fetches": 0, "appearances_created": 0, "skipped_locked": 1}

    try:
        existing_count = db.scalar(select(func.count()).select_from(SyncPapersArxivArchiveAppearance)) or 0
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
            appearances_created += _record_sync_papers_arxiv_archive_appearances(
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


async def _sync_papers_archive_month(
    db: Session,
    client: ArxivMetadataClient,
    *,
    category: str,
    archive_month: date,
    stats: dict[str, Any],
    checkpoint_context: SyncPapersCheckpointContext | None = None,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> None:
    period = month_label(archive_month)
    page_size = max(1, get_settings().sync_papers_arxiv_list_page_size)
    skip = 0
    while True:
        _run_stop_check(stop_check)
        request_key = f"list:{category}:{period}:{skip}:{page_size}"
        request_url = f"https://arxiv.org/list/{category}/{period}?skip={skip}&show={page_size}"
        started = time.perf_counter()
        response = await _fetch_sync_papers_arxiv_request(
            db,
            checkpoint_context,
            surface="listing_html",
            request_key=request_key,
            request_url=request_url,
            stats=stats,
            fetch=lambda: client.fetch_listing_page(
                category=category,
                period=period,
                skip=skip,
                show=page_size,
            ),
        )
        stats["stage_seconds"]["listing_fetch"] += time.perf_counter() - started
        if response.error or response.body is None or response.status is None:
            raise RuntimeError(f"{category}: arXiv listing fetch failed for {period} ({response.error or 'empty response'})")
        if not response.reused_checkpoint:
            stats["pages_fetched"] += 1
            stats["listing_pages_fetched"] += 1
            stats["provider_counts"]["arxiv"]["listing_requests"] += 1
        _emit_progress(progress, stats)

        arxiv_ids = _extract_arxiv_ids_from_listing_html(response.body)
        if arxiv_ids:
            await _hydrate_arxiv_ids(
                db,
                client,
                category=category,
                request_scope_label=period,
                arxiv_ids=arxiv_ids,
                archive_month=archive_month,
                stats=stats,
                checkpoint_context=checkpoint_context,
                progress=progress,
                stop_check=stop_check,
            )

        if len(arxiv_ids) < page_size:
            break
        skip += page_size


def _day_uses_catchup(sync_day: date) -> bool:
    today = _today_utc()
    return sync_day < today and (today - sync_day).days <= SYNC_PAPERS_ARXIV_CATCHUP_MAX_AGE_DAYS


def _ttl_days_for_unit(unit: SyncPapersArxivUnit) -> list[date]:
    if unit.fetch_day is not None:
        return _ttl_completed_days(unit.fetch_day, unit.fetch_day)
    if unit.fetch_month is not None:
        start_date, end_date = _archive_month_bounds(unit.fetch_month)
        return _ttl_completed_days(start_date, end_date)
    return []


def _arxiv_lock_key_for_unit(unit: SyncPapersArxivUnit) -> str:
    if unit.fetch_day is not None:
        return f"arxiv:{unit.category}:{unit.transport}:{unit.fetch_day.isoformat()}"
    assert unit.fetch_month is not None
    return f"arxiv:{unit.category}:{unit.transport}:{month_label(unit.fetch_month)}"


def _plan_sync_papers_arxiv_units(scope_json: dict[str, Any], categories: list[str]) -> list[SyncPapersArxivUnit]:
    scope = build_scope_payload(scope_json)
    units: list[SyncPapersArxivUnit] = []
    if scope.day is not None:
        transport = "catchup_day" if _day_uses_catchup(scope.day) else "submitted_day"
        for category in categories:
            units.append(
                SyncPapersArxivUnit(
                    category=category,
                    transport=transport,
                    requested_start_date=scope.day,
                    requested_end_date=scope.day,
                    fetch_day=scope.day,
                    fetch_month=month_start(scope.day),
                )
            )
        return units

    if scope.month is not None:
        archive_month = date.fromisoformat(f"{scope.month}-01")
        start_date, end_date = _archive_month_bounds(archive_month)
        for category in categories:
            units.append(
                SyncPapersArxivUnit(
                    category=category,
                    transport="list_month",
                    requested_start_date=start_date,
                    requested_end_date=end_date,
                    fetch_month=archive_month,
                )
            )
        return units

    if scope.from_date is not None and scope.to_date is not None:
        for category in categories:
            for archive_month in resolve_archive_months_from_scope_json(scope_json):
                month_start_date, month_end_date = _archive_month_bounds(archive_month)
                units.append(
                    SyncPapersArxivUnit(
                        category=category,
                        transport="list_month",
                        requested_start_date=max(scope.from_date, month_start_date),
                        requested_end_date=min(scope.to_date, month_end_date),
                        fetch_month=archive_month,
                    )
                )
        return units

    return []


async def _hydrate_arxiv_ids(
    db: Session,
    client: ArxivMetadataClient,
    *,
    category: str,
    request_scope_label: str,
    arxiv_ids: list[str],
    archive_month: date | None,
    stats: dict[str, Any],
    checkpoint_context: SyncPapersCheckpointContext | None = None,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> None:
    for batch in _batch_arxiv_ids(arxiv_ids):
        _run_stop_check(stop_check)
        batch_key = hashlib.sha1(",".join(batch).encode("utf-8")).hexdigest()[:16]
        request_key = f"id_batch:{category}:{request_scope_label}:{batch_key}:{len(batch)}"
        request_url = f"https://export.arxiv.org/api/query?id_list_batch={batch_key}&count={len(batch)}"
        started = time.perf_counter()
        response = await _fetch_sync_papers_arxiv_request(
            db,
            checkpoint_context,
            surface="id_list_feed",
            request_key=request_key,
            request_url=request_url,
            stats=stats,
            fetch=lambda: client.fetch_id_list_feed(batch),
        )
        stats["stage_seconds"]["metadata_fetch"] += time.perf_counter() - started
        if response.error or response.body is None or response.status is None:
            raise RuntimeError(
                f"{category}: arXiv metadata batch fetch failed for {request_scope_label} ({response.error or 'empty response'})"
            )
        if not response.reused_checkpoint:
            stats["pages_fetched"] += 1
            stats["metadata_batches_fetched"] += 1
            stats["provider_counts"]["arxiv"]["metadata_requests"] += 1

        papers = parse_papers_from_feed(response.body)
        persist_started = time.perf_counter()
        for paper in papers:
            upsert_paper(db, paper)
        if archive_month is not None:
            _record_sync_papers_arxiv_archive_appearances(
                db,
                category=category,
                archive_month=archive_month,
                arxiv_ids=[paper.arxiv_id for paper in papers],
            )
        db.commit()
        stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
        stats["papers_upserted"] += len(papers)
        _emit_progress(progress, stats)


async def _sync_papers_catchup_day(
    db: Session,
    client: ArxivMetadataClient,
    *,
    category: str,
    sync_day: date,
    stats: dict[str, Any],
    checkpoint_context: SyncPapersCheckpointContext | None = None,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> None:
    _run_stop_check(stop_check)
    request_key = f"catchup:{category}:{sync_day.isoformat()}"
    request_url = f"https://arxiv.org/catchup/{category}/{sync_day.isoformat()}"
    started = time.perf_counter()
    response = await _fetch_sync_papers_arxiv_request(
        db,
        checkpoint_context,
        surface="catchup_html",
        request_key=request_key,
        request_url=request_url,
        stats=stats,
        fetch=lambda: client.fetch_catchup_page(category=category, day=sync_day),
    )
    stats["stage_seconds"]["catchup_fetch"] += time.perf_counter() - started
    if response.error or response.body is None or response.status is None:
        raise RuntimeError(f"{category}: arXiv catchup fetch failed for {sync_day.isoformat()} ({response.error or 'empty response'})")
    if not response.reused_checkpoint:
        stats["pages_fetched"] += 1
        stats["catchup_pages_fetched"] += 1
        stats["provider_counts"]["arxiv"]["catchup_requests"] += 1
    _emit_progress(progress, stats)

    arxiv_ids = _extract_arxiv_ids_from_listing_html(response.body)
    if not arxiv_ids:
        return
    await _hydrate_arxiv_ids(
        db,
        client,
        category=category,
        request_scope_label=sync_day.isoformat(),
        arxiv_ids=arxiv_ids,
        archive_month=month_start(sync_day),
        stats=stats,
        checkpoint_context=checkpoint_context,
        progress=progress,
        stop_check=stop_check,
    )


async def _sync_papers_submitted_day(
    db: Session,
    client: ArxivMetadataClient,
    *,
    category: str,
    sync_day: date,
    stats: dict[str, Any],
    checkpoint_context: SyncPapersCheckpointContext | None = None,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> None:
    page_size = max(1, get_settings().sync_papers_arxiv_list_page_size)
    start = 0
    while True:
        _run_stop_check(stop_check)
        request_key = f"submitted_day:{category}:{sync_day.isoformat()}:{start}:{page_size}"
        request_url = f"https://export.arxiv.org/api/query?search_query=submitted_day:{category}:{sync_day.isoformat()}"
        started = time.perf_counter()
        response = await _fetch_sync_papers_arxiv_request(
            db,
            checkpoint_context,
            surface="submitted_day_feed",
            request_key=request_key,
            request_url=request_url,
            stats=stats,
            fetch=lambda: client.fetch_submitted_day_page(
                category=category,
                day=sync_day,
                start=start,
                max_results=page_size,
            ),
        )
        stats["stage_seconds"]["search_fetch"] += time.perf_counter() - started
        if response.error or response.body is None or response.status is None:
            raise RuntimeError(
                f"{category}: arXiv submittedDate fetch failed for {sync_day.isoformat()} ({response.error or 'empty response'})"
            )
        if not response.reused_checkpoint:
            stats["pages_fetched"] += 1
            stats["search_pages_fetched"] += 1
            stats["provider_counts"]["arxiv"]["search_requests"] += 1
        _emit_progress(progress, stats)

        arxiv_ids = parse_arxiv_ids_from_feed(response.body)
        if not arxiv_ids:
            break
        await _hydrate_arxiv_ids(
            db,
            client,
            category=category,
            request_scope_label=sync_day.isoformat(),
            arxiv_ids=arxiv_ids,
            archive_month=month_start(sync_day),
            stats=stats,
            checkpoint_context=checkpoint_context,
            progress=progress,
            stop_check=stop_check,
        )
        if len(arxiv_ids) < page_size:
            break
        start += page_size


async def run_sync_papers(
    db: Session,
    scope_json: dict[str, Any],
    *,
    job_id: str | None = None,
    attempt_series_key: str | None = None,
    attempt_mode: JobAttemptMode | str = JobAttemptMode.fresh,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    settings = get_settings()
    started_at = time.perf_counter()
    categories = _required_scope_categories(scope_json)
    _required_scope_window(scope_json)
    force = bool(scope_json.get("force"))
    stats = {
        "categories": len(categories),
        "papers_upserted": 0,
        "pages_fetched": 0,
        "search_pages_fetched": 0,
        "listing_pages_fetched": 0,
        "catchup_pages_fetched": 0,
        "metadata_batches_fetched": 0,
        "checkpoint_reused": 0,
        "checkpoint_pages_reused": 0,
        "checkpoint_metadata_batches_reused": 0,
        "categories_skipped_locked": 0,
        "windows_skipped_ttl": 0,
        "provider_counts": {
            "arxiv": {
                "search_requests": 0,
                "listing_requests": 0,
                "catchup_requests": 0,
                "metadata_requests": 0,
            }
        },
        "stage_seconds": {
            "search_fetch": 0.0,
            "listing_fetch": 0.0,
            "catchup_fetch": 0.0,
            "metadata_fetch": 0.0,
            "persist": 0.0,
        },
    }
    _update_runtime_stats(stats, started_at=started_at, processed_key="papers_upserted", throughput_key="papers_per_minute")
    _emit_progress(progress, stats)
    checkpoint_context = SyncPapersCheckpointContext(
        job_id=job_id,
        attempt_series_key=attempt_series_key,
        attempt_mode=attempt_mode,
    )

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        client = ArxivMetadataClient(
            session,
            min_interval=settings.sync_papers_arxiv_min_interval,
            max_concurrent=SYNC_PAPERS_ARXIV_MAX_CONCURRENT,
            rate_limiter=_sync_papers_arxiv_rate_limiter(settings.sync_papers_arxiv_min_interval),
        )

        for unit in _plan_sync_papers_arxiv_units(scope_json, categories):
            _run_stop_check(stop_check)
            lock_key = _arxiv_lock_key_for_unit(unit)
            if not try_advisory_lock(db, lock_key):
                stats["categories_skipped_locked"] += 1
                _emit_progress(progress, stats)
                continue
            try:
                if not _requested_arxiv_window_sync_due(
                    db,
                    category=unit.category,
                    start_date=unit.requested_start_date,
                    end_date=unit.requested_end_date,
                    force=force,
                ):
                    stats["windows_skipped_ttl"] += 1
                    _emit_progress(progress, stats)
                    continue

                if unit.transport == "list_month":
                    assert unit.fetch_month is not None
                    await _sync_papers_archive_month(
                        db,
                        client,
                        category=unit.category,
                        archive_month=unit.fetch_month,
                        stats=stats,
                        checkpoint_context=checkpoint_context,
                        progress=progress,
                        stop_check=stop_check,
                    )
                elif unit.transport == "catchup_day":
                    assert unit.fetch_day is not None
                    await _sync_papers_catchup_day(
                        db,
                        client,
                        category=unit.category,
                        sync_day=unit.fetch_day,
                        stats=stats,
                        checkpoint_context=checkpoint_context,
                        progress=progress,
                        stop_check=stop_check,
                    )
                elif unit.transport == "submitted_day":
                    assert unit.fetch_day is not None
                    await _sync_papers_submitted_day(
                        db,
                        client,
                        category=unit.category,
                        sync_day=unit.fetch_day,
                        stats=stats,
                        checkpoint_context=checkpoint_context,
                        progress=progress,
                        stop_check=stop_check,
                    )
                else:
                    raise RuntimeError(f"Unsupported sync-papers arXiv transport: {unit.transport}")

                _record_arxiv_days_completed(
                    db,
                    category=unit.category,
                    sync_days=_ttl_days_for_unit(unit),
                )
                db.commit()
                _update_runtime_stats(
                    stats,
                    started_at=started_at,
                    processed_key="papers_upserted",
                    throughput_key="papers_per_minute",
                )
                _emit_progress(progress, stats)
            finally:
                release_advisory_lock(db, lock_key)

    _update_runtime_stats(stats, started_at=started_at, processed_key="papers_upserted", throughput_key="papers_per_minute")
    return stats


async def _probe_huggingface(
    client: HuggingFaceLinksClient,
    arxiv_id: str,
    metrics: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool, dict[str, RawFetchEnvelope]]:
    observations: list[dict[str, Any]] = []
    raw_fetches: dict[str, RawFetchEnvelope] = {}
    provider_counts = metrics["provider_counts"]["huggingface"]
    stage_seconds = metrics["stage_seconds"]

    started = time.perf_counter()
    payload_status, payload_body, payload_headers, payload_error = await client.fetch_paper_payload(arxiv_id)
    stage_seconds["huggingface_api"] += time.perf_counter() - started
    provider_counts["api_requests"] += 1
    raw_fetches["huggingface_api"] = RawFetchEnvelope(
        provider="huggingface",
        surface="paper_api",
        request_key=f"paper_api:{arxiv_id}",
        request_url=f"https://huggingface.co/api/papers/{arxiv_id}",
        status_code=payload_status,
        body=payload_body,
        headers=payload_headers,
    )
    if payload_error and payload_status != 404:
        provider_counts["api_failures"] += 1
        observations.append(
            {
                "provider": "huggingface",
                "surface": "paper_api",
                "status": ObservationStatus.fetch_failed,
                "error_message": payload_error,
                "raw_fetch_ref": "huggingface_api",
            }
        )
        return observations, False, raw_fetches

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
                    "raw_fetch_ref": "huggingface_api",
                }
            )
        return observations, True, raw_fetches

    observations.append(
        {
            "provider": "huggingface",
            "surface": "paper_api",
            "status": ObservationStatus.checked_no_match,
            "raw_fetch_ref": "huggingface_api",
        }
    )
    return observations, True, raw_fetches


async def _probe_alphaxiv(
    client: AlphaXivLinksClient,
    arxiv_id: str,
    metrics: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool, dict[str, RawFetchEnvelope]]:
    observations: list[dict[str, Any]] = []
    raw_fetches: dict[str, RawFetchEnvelope] = {}
    provider_counts = metrics["provider_counts"]["alphaxiv"]
    stage_seconds = metrics["stage_seconds"]

    started = time.perf_counter()
    payload_status, payload_body, payload_headers, payload_error = await client.fetch_paper_payload(arxiv_id)
    stage_seconds["alphaxiv_api"] += time.perf_counter() - started
    provider_counts["api_requests"] += 1
    raw_fetches["alphaxiv_api"] = RawFetchEnvelope(
        provider="alphaxiv",
        surface="paper_api",
        request_key=f"paper_api:{arxiv_id}",
        request_url=f"https://api.alphaxiv.org/papers/v3/{arxiv_id}",
        status_code=payload_status,
        body=payload_body,
        headers=payload_headers,
    )
    if payload_error and payload_status != 404:
        provider_counts["api_failures"] += 1
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_api",
                "status": ObservationStatus.fetch_failed,
                "error_message": payload_error,
                "raw_fetch_ref": "alphaxiv_api",
            }
        )
        return observations, False, raw_fetches

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
                    "raw_fetch_ref": "alphaxiv_api",
                }
            )
        return observations, True, raw_fetches

    observations.append(
        {
            "provider": "alphaxiv",
            "surface": "paper_api",
            "status": ObservationStatus.checked_no_match,
            "raw_fetch_ref": "alphaxiv_api",
        }
    )
    if payload_status == 404:
        return observations, True, raw_fetches

    started = time.perf_counter()
    html_status, html_body, html_headers, html_error = await client.fetch_paper_html(arxiv_id)
    stage_seconds["alphaxiv_html"] += time.perf_counter() - started
    provider_counts["html_requests"] += 1
    raw_fetches["alphaxiv_html"] = RawFetchEnvelope(
        provider="alphaxiv",
        surface="paper_html",
        request_key=f"paper_html:{arxiv_id}",
        request_url=f"https://www.alphaxiv.org/abs/{arxiv_id}",
        status_code=html_status,
        body=html_body,
        headers=html_headers,
    )
    if html_error and html_status != 404:
        provider_counts["html_failures"] += 1
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_html",
                "status": ObservationStatus.fetch_failed,
                "error_message": html_error,
                "raw_fetch_ref": "alphaxiv_html",
            }
        )
        return observations, False, raw_fetches

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
                    "raw_fetch_ref": "alphaxiv_html",
                }
            )
    else:
        observations.append(
            {
                "provider": "alphaxiv",
                "surface": "paper_html",
                "status": ObservationStatus.checked_no_match,
                "raw_fetch_ref": "alphaxiv_html",
            }
        )
    return observations, True, raw_fetches


def _resolve_observation_raw_refs(
    observations: list[dict[str, Any]],
    raw_fetch_ids: dict[str, str | None],
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    for item in observations:
        payload = dict(item)
        raw_fetch_ref = payload.pop("raw_fetch_ref", None)
        if raw_fetch_ref is not None:
            payload["raw_fetch_id"] = raw_fetch_ids.get(str(raw_fetch_ref))
        resolved.append(payload)
    return resolved


def _persist_link_lookup_result(
    db: Session,
    *,
    paper: Paper,
    result: LinkLookupResult,
) -> PaperRepoState:
    raw_fetch_ids = {
        ref: _store_raw_fetch(
            db,
            provider=payload.provider,
            surface=payload.surface,
            request_key=payload.request_key,
            request_url=payload.request_url,
            status_code=payload.status_code,
            body=payload.body,
            headers=payload.headers,
        )
        for ref, payload in result.raw_fetches.items()
    }
    observations = _resolve_observation_raw_refs(result.observations, raw_fetch_ids)
    _upsert_observations(db, paper.arxiv_id, observations)
    final_urls = _finalize_repo_urls(observations)
    return _apply_repo_state(
        db,
        paper,
        final_urls=final_urls,
        complete=result.complete,
        error_text="; ".join(result.errors) if result.errors else None,
    )


async def _lookup_links_for_paper(
    settings: Any,
    *,
    hf_client: HuggingFaceLinksClient,
    alphaxiv_client: AlphaXivLinksClient,
    task: LinkLookupTask,
    stop_check: StopCheck | None = None,
) -> LinkLookupResult:
    metrics = _new_find_repos_metrics()
    observations: list[dict[str, Any]] = []
    raw_fetches: dict[str, RawFetchEnvelope] = {}
    errors: list[str] = []
    complete = True

    comment_urls = extract_github_repo_urls(task.comment or "")
    if comment_urls:
        metrics["provider_counts"]["arxiv"]["comment_matches"] += len(comment_urls)
        for url in comment_urls:
            observations.append(
                {
                    "provider": "arxiv",
                    "surface": "comment",
                    "status": ObservationStatus.found,
                    "observed_repo_url": url,
                    "normalized_repo_url": url,
                    "evidence_excerpt": task.comment,
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

    final_urls = _finalize_repo_urls(observations)

    if not final_urls:
        abstract_urls = extract_github_repo_urls(task.abstract or "")
        if abstract_urls:
            metrics["provider_counts"]["arxiv"]["abstract_matches"] += len(abstract_urls)
            for url in abstract_urls:
                observations.append(
                    {
                        "provider": "arxiv",
                        "surface": "abstract",
                        "status": ObservationStatus.found,
                        "observed_repo_url": url,
                        "normalized_repo_url": url,
                        "evidence_excerpt": task.abstract,
                    }
                )
        else:
            observations.append(
                {
                    "provider": "arxiv",
                    "surface": "abstract",
                    "status": ObservationStatus.checked_no_match,
                }
            )
        final_urls = _finalize_repo_urls(observations)

    if not final_urls and settings.find_repos_alphaxiv_enabled:
        _run_stop_check(stop_check)
        alphaxiv_observations, alphaxiv_complete, alphaxiv_raw_fetches = await _probe_alphaxiv(
            alphaxiv_client,
            task.arxiv_id,
            metrics,
        )
        observations.extend(alphaxiv_observations)
        raw_fetches.update(alphaxiv_raw_fetches)
        complete = complete and alphaxiv_complete
        if not alphaxiv_complete:
            errors.append("AlphaXiv lookup incomplete")
        final_urls = _finalize_repo_urls(observations)

    if not final_urls and settings.find_repos_huggingface_enabled:
        _run_stop_check(stop_check)
        hf_observations, hf_complete, hf_raw_fetches = await _probe_huggingface(hf_client, task.arxiv_id, metrics)
        observations.extend(hf_observations)
        raw_fetches.update(hf_raw_fetches)
        complete = complete and hf_complete
        if not hf_complete:
            errors.append("Hugging Face lookup incomplete")

    return LinkLookupResult(
        arxiv_id=task.arxiv_id,
        observations=observations,
        complete=complete,
        errors=errors,
        raw_fetches=raw_fetches,
        metrics=metrics,
    )


async def run_find_repos(
    db: Session,
    scope_json: dict[str, Any],
    *,
    job_id: str | None = None,
    attempt_series_key: str | None = None,
    attempt_mode: JobAttemptMode | str = JobAttemptMode.fresh,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    started_at = time.perf_counter()
    _required_scope_categories(scope_json)
    _required_scope_window(scope_json)
    papers = scoped_papers(db, scope_json)
    force = bool(scope_json.get("force"))
    resume_context = ItemResumeContext(
        job_id=job_id,
        attempt_series_key=attempt_series_key,
        attempt_mode=attempt_mode,
        job_type=JobType.find_repos,
        item_kind=RESUME_ITEM_KIND_PAPER,
        force=force,
    )
    completed_resume_keys = _completed_resume_item_keys(db, resume_context)
    resume_reused_keys = {paper.arxiv_id for paper in papers if paper.arxiv_id in completed_resume_keys}
    candidate_papers = [paper for paper in papers if paper.arxiv_id not in completed_resume_keys]
    due_papers = [paper for paper in candidate_papers if _link_lookup_due(paper.repo_state, force=force)]
    metrics = _new_find_repos_metrics()
    stats = {
        "papers_considered": len(papers),
        "papers_processed": 0,
        "papers_skipped_fresh": len(candidate_papers) - len(due_papers),
        "papers_skipped_no_longer_due": 0,
        "resume_items_reused": len(resume_reused_keys),
        "resume_items_completed": 0,
        "found": 0,
        "not_found": 0,
        "ambiguous": 0,
        "unknown": 0,
        "skipped_locked": 0,
        "provider_counts": metrics["provider_counts"],
        "stage_seconds": metrics["stage_seconds"],
    }
    _update_runtime_stats(stats, started_at=started_at, processed_key="papers_processed", throughput_key="papers_per_minute")
    _emit_progress(progress, stats)
    if not due_papers:
        return stats

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        hf_client = HuggingFaceLinksClient(
            session,
            huggingface_token=settings.huggingface_token,
            min_interval=settings.find_repos_huggingface_min_interval,
            max_concurrent=settings.find_repos_huggingface_max_concurrent,
        )
        alphaxiv_client = AlphaXivLinksClient(
            session,
            alphaxiv_token=settings.alphaxiv_token,
            min_interval=settings.find_repos_alphaxiv_min_interval,
            max_concurrent=settings.find_repos_alphaxiv_max_concurrent,
        )

        worker_count = min(max(1, settings.find_repos_worker_concurrency), len(due_papers))
        request_queue: asyncio.Queue[LinkLookupTask | None] = asyncio.Queue()
        result_queue: asyncio.Queue[LinkLookupResult | Exception] = asyncio.Queue(maxsize=max(1, worker_count * 2))
        for paper in due_papers:
            request_queue.put_nowait(LinkLookupTask(arxiv_id=paper.arxiv_id, comment=paper.comment, abstract=paper.abstract))
        for _ in range(worker_count):
            request_queue.put_nowait(None)

        async def worker() -> None:
            while True:
                task = await request_queue.get()
                if task is None:
                    return
                try:
                    result = await _lookup_links_for_paper(
                        settings,
                        hf_client=hf_client,
                        alphaxiv_client=alphaxiv_client,
                        task=task,
                        stop_check=stop_check,
                    )
                except Exception as exc:
                    await result_queue.put(exc)
                    return
                await result_queue.put(result)

        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        cancelled = False
        try:
            remaining = len(due_papers)
            while remaining:
                _run_stop_check(stop_check)
                item = await result_queue.get()
                if isinstance(item, Exception):
                    raise item

                result = item
                _merge_nested_metrics(stats, result.metrics)
                persist_started = time.perf_counter()
                lock_key = f"paper:{result.arxiv_id}"
                if not try_advisory_lock(db, lock_key):
                    stats["skipped_locked"] += 1
                else:
                    try:
                        paper = db.get(Paper, result.arxiv_id)
                        if paper is not None and _link_lookup_due(paper.repo_state, force=force):
                            state = _persist_link_lookup_result(db, paper=paper, result=result)
                            if result.complete:
                                _record_resume_item_completed(db, resume_context, result.arxiv_id)
                            db.commit()
                            stats["papers_processed"] += 1
                            if result.complete:
                                stats["resume_items_completed"] += 1
                            stats[state.stable_status.value] += 1
                        elif paper is not None:
                            stats["papers_skipped_no_longer_due"] += 1
                    except Exception:
                        db.rollback()
                        raise
                    finally:
                        release_advisory_lock(db, lock_key)

                stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
                remaining -= 1
                _update_runtime_stats(
                    stats,
                    started_at=started_at,
                    processed_key="papers_processed",
                    throughput_key="papers_per_minute",
                )
                _emit_progress(progress, stats)
        except Exception:
            cancelled = True
            for task in workers:
                task.cancel()
            raise
        finally:
            await asyncio.gather(*workers, return_exceptions=True)
            if cancelled:
                db.rollback()

    _update_runtime_stats(stats, started_at=started_at, processed_key="papers_processed", throughput_key="papers_per_minute")
    return stats


def _github_api_headers(
    *,
    github_token: str,
    existing: GitHubRepo | None = None,
) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "papertorepo",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    if existing is not None:
        if existing.etag:
            headers["If-None-Match"] = existing.etag
        if existing.last_modified:
            headers["If-Modified-Since"] = existing.last_modified
    return headers


def _github_graphql_headers(*, github_token: str) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "papertorepo",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    return headers


def _github_graphql_request_key(batch: list[str]) -> str:
    digest = hashlib.sha1(",".join(batch).encode("utf-8")).hexdigest()[:16]
    return f"github_graphql:{digest}:{len(batch)}"


def _build_github_graphql_query(batch: list[tuple[str, str, str]]) -> str:
    selections: list[str] = []
    for index, (_normalized_url, owner, repo) in enumerate(batch):
        selections.append(
            (
                f"repo{index}: repository(owner: {json.dumps(owner)}, name: {json.dumps(repo)}) {{"
                " databaseId"
                " name"
                " owner { login }"
                " stargazerCount"
                " createdAt"
                " description"
                " homepageUrl"
                " isArchived"
                " pushedAt"
                " licenseInfo { spdxId name }"
                f" repositoryTopics(first: {REFRESH_METADATA_GITHUB_GRAPHQL_TOPICS_FIRST}) {{ nodes {{ topic {{ name }} }} }}"
                " }"
            )
        )
    return "query RepoBatch {\n" + "\n".join(selections) + "\n}"


def _normalize_github_graphql_payload(payload: dict[str, Any]) -> dict[str, Any]:
    topics: list[str] = []
    repository_topics = payload.get("repositoryTopics")
    if isinstance(repository_topics, dict):
        for item in repository_topics.get("nodes") or []:
            if not isinstance(item, dict):
                continue
            topic = item.get("topic")
            if not isinstance(topic, dict):
                continue
            name = str(topic.get("name") or "").strip()
            if name:
                topics.append(name)
    license_info = payload.get("licenseInfo") if isinstance(payload.get("licenseInfo"), dict) else None
    owner_info = payload.get("owner") if isinstance(payload.get("owner"), dict) else None
    return {
        "github_id": payload.get("databaseId"),
        "owner": (owner_info or {}).get("login") or "",
        "repo": payload.get("name") or "",
        "stars": payload.get("stargazerCount"),
        "created_at": payload.get("createdAt"),
        "description": payload.get("description") or "",
        "homepage": payload.get("homepageUrl"),
        "topics": topics,
        "license": (license_info or {}).get("spdx_id")
        or (license_info or {}).get("spdxId")
        or (license_info or {}).get("name"),
        "archived": bool(payload.get("isArchived")),
        "pushed_at": payload.get("pushedAt"),
    }


def _normalize_github_rest_payload(payload: dict[str, Any]) -> dict[str, Any]:
    owner_info = payload.get("owner") if isinstance(payload.get("owner"), dict) else None
    return {
        "github_id": payload.get("id"),
        "owner": (owner_info or {}).get("login") or "",
        "repo": payload.get("name") or "",
        "stars": payload.get("stargazers_count"),
        "created_at": payload.get("created_at"),
        "description": payload.get("description") or "",
        "homepage": payload.get("homepage"),
        "topics": payload.get("topics") or [],
        "license": _license_from_payload(payload),
        "archived": bool(payload.get("archived")),
        "pushed_at": payload.get("pushed_at"),
    }


def _upsert_github_repo_from_metadata(
    db: Session,
    *,
    normalized_url: str,
    metadata: dict[str, Any],
    now: datetime,
    headers: dict[str, str] | None = None,
) -> GitHubRepo:
    existing = db.get(GitHubRepo, normalized_url)
    repo = existing or GitHubRepo(
        normalized_github_url=normalized_url,
        owner=str(metadata.get("owner") or ""),
        repo=str(metadata.get("repo") or ""),
        first_seen_at=now,
    )

    repo.owner = str(metadata.get("owner") or repo.owner)
    repo.repo = str(metadata.get("repo") or repo.repo)
    if repo.github_id is None:
        repo.github_id = metadata.get("github_id")
    if repo.created_at is None:
        repo.created_at = metadata.get("created_at")

    repo.stars = metadata.get("stars")
    repo.description = metadata.get("description") or ""
    repo.homepage = metadata.get("homepage")
    repo.topics_json = list(metadata.get("topics") or [])
    repo.license = metadata.get("license")
    repo.archived = bool(metadata.get("archived"))
    repo.pushed_at = metadata.get("pushed_at")
    repo.checked_at = now
    if headers is not None:
        repo.etag = headers.get("ETag") or repo.etag
        repo.last_modified = headers.get("Last-Modified") or repo.last_modified
    db.add(repo)
    return repo


async def _fetch_github_graphql_batch(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    semaphore: asyncio.Semaphore,
    batch: list[str],
) -> tuple[dict[str, dict[str, Any]], list[str], RawFetchEnvelope | None, str | None]:
    settings = get_settings()
    repo_specs: list[tuple[str, str, str]] = []
    fallback_urls: list[str] = []
    for normalized_url in batch:
        owner_repo = extract_owner_repo(normalized_url)
        if owner_repo is None:
            fallback_urls.append(normalized_url)
            continue
        owner, repo = owner_repo
        repo_specs.append((normalized_url, owner, repo))

    if not repo_specs:
        return {}, fallback_urls, None, None

    query = _build_github_graphql_query(repo_specs)
    status, body, response_headers, error = await request_text(
        session,
        "https://api.github.com/graphql",
        method="POST",
        headers=_github_graphql_headers(github_token=settings.github_token),
        json_body={"query": query},
        semaphore=semaphore,
        rate_limiter=limiter,
        retry_prefix="GitHub GraphQL",
    )
    raw_fetch = RawFetchEnvelope(
        provider="github",
        surface="graphql_batch",
        request_key=_github_graphql_request_key(batch),
        request_url="https://api.github.com/graphql",
        status_code=status,
        body=body,
        headers=response_headers,
    )
    if error:
        fallback_urls.extend(normalized_url for normalized_url, _owner, _repo in repo_specs)
        return {}, fallback_urls, raw_fetch, error
    if body is None:
        fallback_urls.extend(normalized_url for normalized_url, _owner, _repo in repo_specs)
        return {}, fallback_urls, raw_fetch, "GitHub GraphQL returned empty body"

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        fallback_urls.extend(normalized_url for normalized_url, _owner, _repo in repo_specs)
        return {}, fallback_urls, raw_fetch, "GitHub GraphQL returned invalid JSON"

    data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else {}
    error_aliases: set[str] = set()
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if isinstance(errors, list):
        for item in errors:
            if not isinstance(item, dict):
                continue
            path = item.get("path")
            if isinstance(path, list) and path and isinstance(path[0], str):
                error_aliases.add(path[0])

    resolved: dict[str, dict[str, Any]] = {}
    for index, (normalized_url, _owner, _repo) in enumerate(repo_specs):
        alias = f"repo{index}"
        if alias in error_aliases:
            fallback_urls.append(normalized_url)
            continue
        node = data.get(alias)
        if not isinstance(node, dict):
            fallback_urls.append(normalized_url)
            continue
        resolved[normalized_url] = _normalize_github_graphql_payload(node)
    return resolved, fallback_urls, raw_fetch, None


async def _fetch_github_repo(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    semaphore: asyncio.Semaphore,
    normalized_url: str,
    existing: GitHubRepo | None,
) -> tuple[str, dict[str, Any] | None, dict[str, str], RawFetchEnvelope | None]:
    settings = get_settings()
    owner_repo = extract_owner_repo(normalized_url)
    if owner_repo is None:
        raise RuntimeError(f"{normalized_url} is not a valid GitHub repository URL")
    owner, repo = owner_repo
    headers = _github_api_headers(github_token=settings.github_token, existing=existing)

    status, body, response_headers, error = await request_text(
        session,
        f"https://api.github.com/repos/{owner}/{repo}",
        headers=headers,
        semaphore=semaphore,
        rate_limiter=limiter,
        retry_prefix="GitHub API",
        allowed_statuses={304, 404},
    )
    raw_fetch = RawFetchEnvelope(
        provider="github",
        surface="repo_api",
        request_key=f"repo_api:{owner}:{repo}",
        request_url=f"https://api.github.com/repos/{owner}/{repo}",
        status_code=status,
        body=body,
        headers=response_headers,
    )
    if error:
        raise RuntimeError(error)
    if status == 304:
        return "not_modified", None, response_headers, raw_fetch
    if status == 404:
        return "missing", None, response_headers, raw_fetch
    if body is None:
        raise RuntimeError("GitHub API returned empty body")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub API returned invalid JSON") from exc
    return "ok", payload, response_headers, raw_fetch


def _license_from_payload(payload: dict[str, Any]) -> str | None:
    license_info = payload.get("license") if isinstance(payload.get("license"), dict) else None
    return (license_info or {}).get("spdx_id") or (license_info or {}).get("name")


async def run_refresh_metadata(
    db: Session,
    scope_json: dict[str, Any],
    *,
    job_id: str | None = None,
    attempt_series_key: str | None = None,
    attempt_mode: JobAttemptMode | str = JobAttemptMode.fresh,
    progress: ProgressCallback | None = None,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    started_at = time.perf_counter()
    _required_scope_categories(scope_json)
    _required_scope_window(scope_json)
    papers = scoped_papers(db, scope_json)
    repo_urls: set[str] = set()
    for paper in papers:
        if paper.repo_state is None:
            continue
        repo_urls.update(paper.repo_state.repo_urls_json or [])
    resume_context = ItemResumeContext(
        job_id=job_id,
        attempt_series_key=attempt_series_key,
        attempt_mode=attempt_mode,
        job_type=JobType.refresh_metadata,
        item_kind=RESUME_ITEM_KIND_REPO,
        force=bool(scope_json.get("force")),
    )
    completed_resume_keys = _completed_resume_item_keys(db, resume_context)
    resume_reused_keys = {url for url in repo_urls if url in completed_resume_keys}
    repo_urls_to_process = repo_urls - resume_reused_keys

    metrics = _new_refresh_metadata_metrics()
    stats = {
        "repos_considered": len(repo_urls),
        "repos_completed": 0,
        "resume_items_reused": len(resume_reused_keys),
        "resume_items_completed": 0,
        "updated": 0,
        "not_modified": 0,
        "missing": 0,
        "skipped_locked": 0,
        "provider_counts": metrics["provider_counts"],
        "stage_seconds": metrics["stage_seconds"],
    }
    _update_runtime_stats(stats, started_at=started_at, processed_key="repos_completed", throughput_key="repos_per_minute")
    _emit_progress(progress, stats)
    if not repo_urls_to_process:
        return stats
    min_interval = (
        settings.refresh_metadata_github_min_interval
        if settings.github_token.strip()
        else max(
            settings.refresh_metadata_github_min_interval,
            REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS,
        )
    )
    rest_limiter = RateLimiter(min_interval)
    graphql_limiter = RateLimiter(settings.refresh_metadata_github_min_interval)

    async with aiohttp.ClientSession(timeout=build_timeout()) as session:
        rest_semaphore = asyncio.Semaphore(max(1, settings.refresh_metadata_github_rest_fallback_max_concurrent))
        graphql_semaphore = asyncio.Semaphore(REFRESH_METADATA_GITHUB_GRAPHQL_MAX_CONCURRENT)
        fallback_urls: list[str] = []

        sorted_urls = sorted(repo_urls_to_process)
        if settings.github_token.strip():
            for batch in _chunked(sorted_urls, settings.refresh_metadata_github_graphql_batch_size):
                _run_stop_check(stop_check)
                started = time.perf_counter()
                resolved, batch_fallbacks, raw_fetch, error = await _fetch_github_graphql_batch(
                    session,
                    graphql_limiter,
                    graphql_semaphore,
                    batch,
                )
                stats["stage_seconds"]["github_graphql"] += time.perf_counter() - started
                stats["provider_counts"]["github"]["graphql_batches"] += 1
                stats["provider_counts"]["github"]["graphql_repos"] += len(batch)
                stats["provider_counts"]["github"]["graphql_fallbacks"] += len(batch_fallbacks)
                if error is not None:
                    stats["provider_counts"]["github"]["graphql_batch_failures"] += 1
                if raw_fetch is not None:
                    _store_raw_fetch(
                        db,
                        provider=raw_fetch.provider,
                        surface=raw_fetch.surface,
                        request_key=raw_fetch.request_key,
                        request_url=raw_fetch.request_url,
                        status_code=raw_fetch.status_code,
                        body=raw_fetch.body,
                        headers=raw_fetch.headers,
                    )
                    db.commit()

                for normalized_url, metadata in resolved.items():
                    _run_stop_check(stop_check)
                    lock_key = f"repo:{normalized_url}"
                    if not try_advisory_lock(db, lock_key):
                        stats["skipped_locked"] += 1
                        continue
                    try:
                        persist_started = time.perf_counter()
                        now = utc_now()
                        _upsert_github_repo_from_metadata(db, normalized_url=normalized_url, metadata=metadata, now=now)
                        _record_resume_item_completed(db, resume_context, normalized_url)
                        db.commit()
                        stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
                        stats["updated"] += 1
                        stats["repos_completed"] += 1
                        stats["resume_items_completed"] += 1
                        _update_runtime_stats(
                            stats,
                            started_at=started_at,
                            processed_key="repos_completed",
                            throughput_key="repos_per_minute",
                        )
                        _emit_progress(progress, stats)
                    finally:
                        release_advisory_lock(db, lock_key)

                fallback_urls.extend(batch_fallbacks)
        else:
            fallback_urls = sorted_urls

        for normalized_url in fallback_urls:
            _run_stop_check(stop_check)
            lock_key = f"repo:{normalized_url}"
            if not try_advisory_lock(db, lock_key):
                stats["skipped_locked"] += 1
                _update_runtime_stats(stats, started_at=started_at, processed_key="repos_completed", throughput_key="repos_per_minute")
                _emit_progress(progress, stats)
                continue
            try:
                existing = db.get(GitHubRepo, normalized_url)
                started = time.perf_counter()
                try:
                    status, payload, headers, raw_fetch = await _fetch_github_repo(
                        session,
                        rest_limiter,
                        rest_semaphore,
                        normalized_url,
                        existing,
                    )
                except Exception:
                    stats["provider_counts"]["github"]["rest_failures"] += 1
                    raise
                stats["provider_counts"]["github"]["rest_requests"] += 1
                stats["stage_seconds"]["github_rest"] += time.perf_counter() - started
                if raw_fetch is not None:
                    _store_raw_fetch(
                        db,
                        provider=raw_fetch.provider,
                        surface=raw_fetch.surface,
                        request_key=raw_fetch.request_key,
                        request_url=raw_fetch.request_url,
                        status_code=raw_fetch.status_code,
                        body=raw_fetch.body,
                        headers=raw_fetch.headers,
                    )

                now = utc_now()
                persist_started = time.perf_counter()
                if status == "not_modified":
                    if existing is not None:
                        existing.checked_at = now
                    _record_resume_item_completed(db, resume_context, normalized_url)
                    stats["not_modified"] += 1
                    stats["repos_completed"] += 1
                    stats["resume_items_completed"] += 1
                    db.commit()
                    stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
                    _update_runtime_stats(
                        stats,
                        started_at=started_at,
                        processed_key="repos_completed",
                        throughput_key="repos_per_minute",
                    )
                    _emit_progress(progress, stats)
                    continue
                if status == "missing":
                    if existing is not None:
                        existing.checked_at = now
                    _record_resume_item_completed(db, resume_context, normalized_url)
                    stats["missing"] += 1
                    stats["repos_completed"] += 1
                    stats["resume_items_completed"] += 1
                    db.commit()
                    stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
                    _update_runtime_stats(
                        stats,
                        started_at=started_at,
                        processed_key="repos_completed",
                        throughput_key="repos_per_minute",
                    )
                    _emit_progress(progress, stats)
                    continue

                assert payload is not None
                metadata = _normalize_github_rest_payload(payload)
                _upsert_github_repo_from_metadata(
                    db,
                    normalized_url=normalized_url,
                    metadata=metadata,
                    now=now,
                    headers=headers,
                )
                _record_resume_item_completed(db, resume_context, normalized_url)
                db.commit()
                stats["stage_seconds"]["persist"] += time.perf_counter() - persist_started
                stats["updated"] += 1
                stats["repos_completed"] += 1
                stats["resume_items_completed"] += 1
                _update_runtime_stats(
                    stats,
                    started_at=started_at,
                    processed_key="repos_completed",
                    throughput_key="repos_per_minute",
                )
                _emit_progress(progress, stats)
            finally:
                release_advisory_lock(db, lock_key)
    _update_runtime_stats(stats, started_at=started_at, processed_key="repos_completed", throughput_key="repos_per_minute")
    return stats


def run_export(
    db: Session,
    scope_json: dict[str, Any],
    *,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    settings = get_settings()
    _run_stop_check(stop_check)
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
        _run_stop_check(stop_check)
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
    _run_stop_check(stop_check)
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
    _run_stop_check(stop_check)

    export_record = ExportRecord(
        file_name=final_path.name,
        file_path=str(final_path),
        scope_json=scope_json,
    )
    db.add(export_record)
    db.commit()
    return {"rows": len(rows), "file_name": final_path.name, "export_id": export_record.id, "export_mode": export_mode or "scoped"}
