from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import os
import socket
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any
from collections.abc import Callable

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from papertorepo.core.config import get_settings
from papertorepo.db.session import get_engine, session_scope
from papertorepo.jobs.batches import (
    batch_child_job_type_for_root,
    batch_root_job_type_for_child,
    is_batch_root_job,
    is_batch_root_job_type,
    planned_child_scope_jsons,
    should_create_batch_root,
)
from papertorepo.jobs.ordering import (
    job_display_order_by,
    job_display_sort_key,
    job_execution_order_by,
    job_scope_window_sort_keys,
)
from papertorepo.jobs.stop import JobStopRequested, mark_job_cancelled, raise_if_job_stop_requested, request_job_stop
from papertorepo.db.models import (
    Job,
    JobAttemptMode,
    JobItemResumeProgress,
    JobStatus,
    JobType,
    SyncPapersArxivRequestCheckpoint,
    utc_now,
)
from papertorepo.core.scope import build_scope_json, build_scope_payload, build_dedupe_key
from papertorepo.api.schemas import ChildSummary, JobRead, ScopePayload, validate_scope_for_job
from papertorepo.services.pipeline import (
    backfill_sync_papers_arxiv_archive_appearances,
    ensure_runtime_dirs,
    run_export,
    run_find_repos,
    run_refresh_metadata,
    run_sync_papers,
)


JOB_QUEUE_INIT_DATABASE_LOCK_ID = 649183502117041921
JOB_QUEUE_REUSED_CHILD_LOCKED_BY = "batch-reuse"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobAttemptMeta:
    attempt_count: int = 1
    attempt_rank: int = 1


def init_database() -> None:
    ensure_runtime_dirs()
    engine = get_engine()
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            conn.exec_driver_sql(f"SELECT pg_advisory_lock({JOB_QUEUE_INIT_DATABASE_LOCK_ID})")
        try:
            from papertorepo.db.migrations import run_database_migrations

            run_database_migrations(conn)
        finally:
            if engine.dialect.name == "postgresql":
                conn.exec_driver_sql(f"SELECT pg_advisory_unlock({JOB_QUEUE_INIT_DATABASE_LOCK_ID})")
    with session_scope() as db:
        backfill_sync_papers_arxiv_archive_appearances(db)


def _fresh_running_after() -> object:
    settings = get_settings()
    return utc_now() - timedelta(seconds=settings.job_queue_running_timeout_seconds)


def _parent_job_clause(parent_job_id: str | None) -> object:
    return Job.parent_job_id.is_(None) if parent_job_id is None else Job.parent_job_id == parent_job_id


def _has_fresh_running_job(db: Session, stale_before: object) -> bool:
    return (
        db.scalar(
            select(Job.id)
            .where(
                Job.status == JobStatus.running,
                or_(Job.locked_at.is_(None), Job.locked_at >= stale_before),
            )
            .limit(1)
        )
        is not None
    )


def _find_active_job(
    db: Session,
    *,
    job_type: JobType,
    dedupe_key: str,
    parent_job_id: str | None,
) -> Job | None:
    return db.scalar(
        select(Job)
        .where(
            Job.job_type == job_type,
            Job.dedupe_key == dedupe_key,
            _parent_job_clause(parent_job_id),
            or_(
                Job.status == JobStatus.pending,
                and_(
                    Job.status == JobStatus.running,
                    or_(Job.locked_at.is_(None), Job.locked_at >= _fresh_running_after()),
                ),
            ),
        )
        .order_by(*job_display_order_by())
    )


def _insert_job_record(
    db: Session,
    job_type: JobType,
    scope_json: dict[str, Any],
    *,
    parent_job_id: str | None = None,
    attempt_mode: JobAttemptMode = JobAttemptMode.fresh,
    attempt_series_key: str | None = None,
) -> Job:
    dedupe_key = build_dedupe_key(job_type.value, scope_json)
    job_id = str(uuid.uuid4())

    job = Job(
        id=job_id,
        parent_job_id=parent_job_id,
        job_type=job_type,
        status=JobStatus.pending,
        attempt_mode=attempt_mode,
        attempt_series_key=attempt_series_key or job_id,
        scope_json=scope_json,
        dedupe_key=dedupe_key,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def _resolve_sync_target_job_type(
    job_type: JobType,
    scope_json: dict[str, Any],
    *,
    parent_job_id: str | None,
) -> JobType:
    target_job_type = job_type
    batch_root_job_type = batch_root_job_type_for_child(job_type)
    if batch_root_job_type is not None and parent_job_id is None and should_create_batch_root(job_type, scope_json):
        target_job_type = batch_root_job_type
    return target_job_type


def create_job(
    db: Session,
    job_type: JobType,
    scope: ScopePayload,
    *,
    parent_job_id: str | None = None,
    attempt_mode: JobAttemptMode = JobAttemptMode.fresh,
    attempt_series_key: str | None = None,
) -> Job:
    validate_scope_for_job(scope, job_type)
    scope_json = build_scope_json(scope)
    return _insert_job_record(
        db,
        job_type,
        scope_json,
        parent_job_id=parent_job_id,
        attempt_mode=attempt_mode,
        attempt_series_key=attempt_series_key,
    )


def launch_sync_job(
    db: Session,
    job_type: JobType,
    scope: ScopePayload,
    *,
    parent_job_id: str | None = None,
    attempt_mode: JobAttemptMode = JobAttemptMode.fresh,
    attempt_series_key: str | None = None,
) -> Job:
    validate_scope_for_job(scope, job_type)
    scope_json = build_scope_json(scope)
    target_job_type = _resolve_sync_target_job_type(job_type, scope_json, parent_job_id=parent_job_id)
    dedupe_key = build_dedupe_key(target_job_type.value, scope_json)
    existing = _find_active_job(
        db,
        job_type=target_job_type,
        dedupe_key=dedupe_key,
        parent_job_id=parent_job_id,
    )
    if existing is not None:
        raise RuntimeError(f"An identical job is already active ({existing.id[:8]}).")
    return _insert_job_record(
        db,
        target_job_type,
        scope_json,
        parent_job_id=parent_job_id,
        attempt_mode=attempt_mode,
        attempt_series_key=attempt_series_key,
    )


def create_sync_job(
    db: Session,
    job_type: JobType,
    scope: ScopePayload,
    *,
    parent_job_id: str | None = None,
    attempt_mode: JobAttemptMode = JobAttemptMode.fresh,
    attempt_series_key: str | None = None,
) -> Job:
    validate_scope_for_job(scope, job_type)
    scope_json = build_scope_json(scope)
    target_job_type = _resolve_sync_target_job_type(job_type, scope_json, parent_job_id=parent_job_id)
    return _insert_job_record(
        db,
        target_job_type,
        scope_json,
        parent_job_id=parent_job_id,
        attempt_mode=attempt_mode,
        attempt_series_key=attempt_series_key,
    )


def create_sync_papers_job(
    db: Session,
    scope: ScopePayload,
    *,
    parent_job_id: str | None = None,
    attempt_mode: JobAttemptMode = JobAttemptMode.fresh,
    attempt_series_key: str | None = None,
) -> Job:
    return create_sync_job(
        db,
        JobType.sync_papers,
        scope,
        parent_job_id=parent_job_id,
        attempt_mode=attempt_mode,
        attempt_series_key=attempt_series_key,
    )


def _latest_jobs_by_dedupe(jobs: list[Job]) -> list[Job]:
    latest: dict[str, Job] = {}
    for job in sorted(jobs, key=job_display_sort_key, reverse=True):
        latest.setdefault(job.dedupe_key, job)
    return list(latest.values())


def _job_is_batch_root(job: Job) -> bool:
    return is_batch_root_job(job.job_type, job.parent_job_id)


def _planned_batch_child_scopes(job: Job) -> list[dict[str, Any]]:
    return planned_child_scope_jsons(job.job_type, job.scope_json)


def _created_at_desc_sort_key(value: datetime | None) -> float:
    if value is None:
        return float("inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return -value.timestamp()


def _scope_window_desc_sort_key(value: str) -> int:
    if not value:
        return 0
    try:
        return -date.fromisoformat(value).toordinal()
    except ValueError:
        return 0


def _batch_child_display_data_by_dedupe_key(parent_job: Job) -> dict[str, tuple[int, str, str]]:
    child_job_type = batch_child_job_type_for_root(parent_job.job_type)
    if child_job_type is None:
        return {}
    result: dict[str, tuple[int, str, str]] = {}
    for index, child_scope_json in enumerate(_planned_batch_child_scopes(parent_job)):
        start_key, end_key = job_scope_window_sort_keys(child_scope_json)
        result[build_dedupe_key(child_job_type.value, child_scope_json)] = (index, start_key, end_key)
    return result


def _batch_child_scope_sort_key(
    child_job: Job,
    display_data_by_dedupe_key: dict[str, tuple[int, str, str]],
) -> tuple[int, int, int, int, float, str]:
    display_data = display_data_by_dedupe_key.get(child_job.dedupe_key)
    if display_data is None:
        start_key, end_key = job_scope_window_sort_keys(child_job.scope_json or {})
        return (
            1,
            _scope_window_desc_sort_key(start_key),
            _scope_window_desc_sort_key(end_key),
            len(display_data_by_dedupe_key),
            _created_at_desc_sort_key(child_job.created_at),
            child_job.id,
        )
    scope_order, start_key, end_key = display_data
    return (
        0,
        _scope_window_desc_sort_key(start_key),
        _scope_window_desc_sort_key(end_key),
        scope_order,
        _created_at_desc_sort_key(child_job.created_at),
        child_job.id,
    )


def _sort_batch_child_jobs(parent_job: Job, child_jobs: list[Job]) -> list[Job]:
    display_data_by_dedupe_key = _batch_child_display_data_by_dedupe_key(parent_job)
    return sorted(
        child_jobs,
        key=lambda child_job: _batch_child_scope_sort_key(child_job, display_data_by_dedupe_key),
    )


def _child_summary_for_batch(job: Job, child_jobs: list[Job]) -> ChildSummary:
    latest_jobs = _latest_jobs_by_dedupe(child_jobs)
    planned_total = len(_planned_batch_child_scopes(job))
    latest_total = len(latest_jobs)
    total = max(planned_total, latest_total)
    counts = {
        "pending": 0,
        "running": 0,
        "stopping": 0,
        "succeeded": 0,
        "failed": 0,
        "cancelled": 0,
    }
    for child in latest_jobs:
        if child.status == JobStatus.running and child.stop_requested_at is not None:
            counts["stopping"] += 1
            continue
        counts[child.status.value] += 1
    missing_total = max(0, total - latest_total)
    if job.stop_requested_at is not None or job.status == JobStatus.cancelled:
        counts["cancelled"] += missing_total
    else:
        counts["pending"] += missing_total
    return ChildSummary(
        total=total,
        pending=counts["pending"],
        running=counts["running"],
        stopping=counts["stopping"],
        succeeded=counts["succeeded"],
        failed=counts["failed"],
        cancelled=counts["cancelled"],
    )


def _batch_state_for_job(job: Job, child_summary: ChildSummary | None) -> str:
    if job.status == JobStatus.running and job.stop_requested_at is not None:
        return "stopping"
    if child_summary is not None and child_summary.stopping > 0:
        return "stopping"
    if child_summary is not None:
        if child_summary.running > 0:
            return "running"
        if child_summary.pending > 0:
            return "queued"
        if child_summary.failed > 0:
            return "failed"
        if child_summary.cancelled > 0:
            return "cancelled"
        return "succeeded"
    if job.status == JobStatus.pending:
        return "queued"
    if job.status == JobStatus.running:
        return "running"
    if job.status == JobStatus.failed:
        return "failed"
    if job.status == JobStatus.cancelled:
        return "cancelled"
    return "succeeded"


def _latest_attempt_in_series(db: Session, attempt_series_key: str) -> Job | None:
    return db.scalar(
        select(Job)
        .where(
            Job.attempt_series_key == attempt_series_key,
        )
        .order_by(*job_display_order_by())
    )


def _batch_root_attempts(db: Session, batch_job: Job) -> list[Job]:
    return list(
        db.scalars(
            select(Job)
            .where(
                Job.attempt_series_key == batch_job.attempt_series_key,
                Job.parent_job_id.is_(None),
            )
            .order_by(*job_display_order_by())
        )
    )


def _latest_batch_root_attempt(db: Session, batch_job: Job) -> Job | None:
    return next(iter(_batch_root_attempts(db, batch_job)), None)


def _latest_batch_lineage_child_attempt(
    db: Session,
    *,
    batch_job: Job,
    child_job_type: JobType,
    child_scope_json: dict[str, Any],
) -> Job | None:
    lineage_parent_ids = [item.id for item in _batch_root_attempts(db, batch_job)]
    if not lineage_parent_ids:
        return None
    child_dedupe_key = build_dedupe_key(child_job_type.value, child_scope_json)
    return db.scalar(
        select(Job)
        .where(
            Job.job_type == child_job_type,
            Job.dedupe_key == child_dedupe_key,
            Job.parent_job_id.in_(lineage_parent_ids),
        )
        .order_by(*job_display_order_by())
    )


def _create_reused_child_record(
    db: Session,
    *,
    child_job_type: JobType,
    child_scope_json: dict[str, Any],
    parent_job_id: str,
    source_job: Job,
    attempt_mode: JobAttemptMode,
) -> Job:
    timestamp = utc_now()
    job = Job(
        id=str(uuid.uuid4()),
        parent_job_id=parent_job_id,
        job_type=child_job_type,
        status=JobStatus.succeeded,
        attempt_mode=attempt_mode,
        attempt_series_key=source_job.attempt_series_key,
        scope_json=child_scope_json,
        dedupe_key=build_dedupe_key(child_job_type.value, child_scope_json),
        stats_json={
            "reused": True,
            "reused_from_job_id": source_job.id,
            "reused_from_parent_job_id": source_job.parent_job_id,
        },
        created_at=timestamp,
        started_at=timestamp,
        finished_at=timestamp,
        locked_by=JOB_QUEUE_REUSED_CHILD_LOCKED_BY,
        locked_at=timestamp,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def list_child_jobs(db: Session, parent_job_id: str) -> list[Job]:
    child_jobs = list(
        db.scalars(select(Job).where(Job.parent_job_id == parent_job_id).order_by(*job_display_order_by()))
    )
    parent_job = db.get(Job, parent_job_id)
    if parent_job is None or not _job_is_batch_root(parent_job):
        return child_jobs
    return _sort_batch_child_jobs(parent_job, child_jobs)


def _serialize_dt(value: object) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _resume_item_kind_for_job_type(job_type: JobType) -> str | None:
    if job_type == JobType.find_repos:
        return "paper"
    if job_type == JobType.refresh_metadata:
        return "repo"
    return None


def _repair_resume_json(db: Session, job: Job) -> dict[str, Any] | None:
    if job.attempt_mode != JobAttemptMode.repair:
        return None

    previous_attempt = db.scalar(
        select(Job)
        .where(
            Job.attempt_series_key == job.attempt_series_key,
            Job.id != job.id,
            Job.created_at <= job.created_at,
        )
        .order_by(Job.created_at.desc(), Job.id.desc())
    )
    if previous_attempt is None:
        return None

    payload: dict[str, Any] = {
        "previous_job_id": previous_attempt.id,
        "previous_status": previous_attempt.status.value,
        "previous_error_text": previous_attempt.error_text,
        "previous_created_at": _serialize_dt(previous_attempt.created_at),
        "previous_started_at": _serialize_dt(previous_attempt.started_at),
        "previous_finished_at": _serialize_dt(previous_attempt.finished_at),
        "previous_stats_json": previous_attempt.stats_json,
    }
    if job.job_type == JobType.sync_papers:
        checkpoint_rows = list(
            db.execute(
                select(SyncPapersArxivRequestCheckpoint.surface, func.count(SyncPapersArxivRequestCheckpoint.id))
                .where(SyncPapersArxivRequestCheckpoint.attempt_series_key == job.attempt_series_key)
                .group_by(SyncPapersArxivRequestCheckpoint.surface)
            ).all()
        )
        by_surface = {str(surface): int(count) for surface, count in checkpoint_rows}
        checkpoint_window = db.execute(
            select(
                func.min(SyncPapersArxivRequestCheckpoint.created_at),
                func.max(SyncPapersArxivRequestCheckpoint.updated_at),
            ).where(SyncPapersArxivRequestCheckpoint.attempt_series_key == job.attempt_series_key)
        ).one()
        payload["checkpoints"] = {
            "total": sum(by_surface.values()),
            "by_surface": by_surface,
            "first_checkpoint_at": _serialize_dt(checkpoint_window[0]),
            "last_checkpoint_at": _serialize_dt(checkpoint_window[1]),
        }
        return payload

    item_kind = _resume_item_kind_for_job_type(job.job_type)
    if item_kind is None:
        return None
    item_rows = list(
        db.execute(
            select(JobItemResumeProgress.status, func.count(JobItemResumeProgress.id))
            .where(
                JobItemResumeProgress.attempt_series_key == job.attempt_series_key,
                JobItemResumeProgress.job_type == job.job_type.value,
                JobItemResumeProgress.item_kind == item_kind,
            )
            .group_by(JobItemResumeProgress.status)
        ).all()
    )
    by_status = {str(status): int(count) for status, count in item_rows}
    payload["resume_items"] = {
        "total": sum(by_status.values()),
        "item_kind": item_kind,
        "by_status": by_status,
    }
    return payload


def _attempt_meta_subquery():
    partition_by = (Job.attempt_series_key,)
    stmt = select(
        Job.id.label("job_id"),
        func.count(Job.id).over(partition_by=partition_by).label("attempt_count"),
        func.row_number().over(
            partition_by=partition_by,
            order_by=job_display_order_by(),
        ).label("attempt_rank"),
    )
    return stmt.subquery()


def get_job_attempt_meta(db: Session, job: Job) -> JobAttemptMeta:
    attempt_meta = _attempt_meta_subquery()
    row = db.execute(
        select(attempt_meta.c.attempt_count, attempt_meta.c.attempt_rank).where(attempt_meta.c.job_id == job.id)
    ).one_or_none()
    if row is None:
        return JobAttemptMeta()
    return JobAttemptMeta(attempt_count=int(row.attempt_count), attempt_rank=int(row.attempt_rank))


def list_jobs_read(
    db: Session,
    *,
    limit: int,
    parent_job_id: str | None = None,
    root_only: bool = False,
    view: str = "all",
) -> list[JobRead]:
    attempt_meta = _attempt_meta_subquery()
    stmt = select(Job, attempt_meta.c.attempt_count, attempt_meta.c.attempt_rank).join(attempt_meta, Job.id == attempt_meta.c.job_id)
    if root_only and parent_job_id is None:
        stmt = stmt.where(Job.parent_job_id.is_(None))
    if parent_job_id is not None:
        stmt = stmt.where(Job.parent_job_id == parent_job_id)
    if view == "latest":
        stmt = stmt.where(attempt_meta.c.attempt_rank == 1)
    parent_job = db.get(Job, parent_job_id) if parent_job_id is not None else None
    if parent_job is not None and _job_is_batch_root(parent_job):
        rows = list(db.execute(stmt).all())
        display_data_by_dedupe_key = _batch_child_display_data_by_dedupe_key(parent_job)
        rows.sort(key=lambda row: _batch_child_scope_sort_key(row[0], display_data_by_dedupe_key))
        rows = rows[:limit]
    else:
        stmt = stmt.order_by(*job_display_order_by()).limit(limit)
        rows = list(db.execute(stmt).all())
    jobs = [row[0] for row in rows]
    attempt_meta_by_id = {
        row[0].id: JobAttemptMeta(attempt_count=int(row.attempt_count), attempt_rank=int(row.attempt_rank)) for row in rows
    }
    return serialize_jobs(db, jobs, attempt_meta_by_id=attempt_meta_by_id)


def list_job_attempts_read(db: Session, job_id: str, *, limit: int = 100) -> list[JobRead]:
    job = db.get(Job, job_id)
    if job is None:
        raise LookupError("Job not found")
    attempt_meta = _attempt_meta_subquery()
    stmt = (
        select(Job, attempt_meta.c.attempt_count, attempt_meta.c.attempt_rank)
        .join(attempt_meta, Job.id == attempt_meta.c.job_id)
        .where(
            Job.attempt_series_key == job.attempt_series_key,
        )
        .order_by(*job_display_order_by())
        .limit(limit)
    )
    rows = list(db.execute(stmt).all())
    jobs = [row[0] for row in rows]
    attempt_meta_by_id = {
        row[0].id: JobAttemptMeta(attempt_count=int(row.attempt_count), attempt_rank=int(row.attempt_rank)) for row in rows
    }
    return serialize_jobs(db, jobs, attempt_meta_by_id=attempt_meta_by_id)


def serialize_job(
    db: Session,
    job: Job,
    *,
    child_jobs: list[Job] | None = None,
    attempt_meta: JobAttemptMeta | None = None,
) -> JobRead:
    batch_state = None
    child_summary = None
    if _job_is_batch_root(job):
        child_jobs = child_jobs if child_jobs is not None else list_child_jobs(db, job.id)
        child_summary = _child_summary_for_batch(job, child_jobs)
        batch_state = _batch_state_for_job(job, child_summary)
    attempt_meta = attempt_meta or JobAttemptMeta()

    return JobRead(
        id=job.id,
        parent_job_id=job.parent_job_id,
        job_type=job.job_type,
        status=job.status,
        attempt_mode=job.attempt_mode,
        attempt_series_key=job.attempt_series_key,
        scope_json=job.scope_json,
        dedupe_key=job.dedupe_key,
        stats_json=job.stats_json,
        repair_resume_json=_repair_resume_json(db, job),
        error_text=job.error_text,
        stop_requested_at=job.stop_requested_at,
        stop_reason=job.stop_reason,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        attempts=job.attempts,
        locked_by=job.locked_by,
        locked_at=job.locked_at,
        batch_state=batch_state,
        child_summary=child_summary,
        attempt_count=attempt_meta.attempt_count,
        attempt_rank=attempt_meta.attempt_rank,
    )


def serialize_jobs(
    db: Session,
    jobs: list[Job],
    *,
    attempt_meta_by_id: dict[str, JobAttemptMeta] | None = None,
) -> list[JobRead]:
    batch_parent_ids = [job.id for job in jobs if _job_is_batch_root(job)]
    child_jobs_by_parent: dict[str, list[Job]] = {}
    if batch_parent_ids:
        child_jobs = list(
            db.scalars(select(Job).where(Job.parent_job_id.in_(batch_parent_ids)).order_by(*job_display_order_by()))
        )
        for child in child_jobs:
            if child.parent_job_id is None:
                continue
            child_jobs_by_parent.setdefault(child.parent_job_id, []).append(child)
    return [
        serialize_job(
            db,
            job,
            child_jobs=child_jobs_by_parent.get(job.id),
            attempt_meta=attempt_meta_by_id.get(job.id) if attempt_meta_by_id is not None else None,
        )
        for job in jobs
    ]


def _batch_root_has_retryable_scopes(child_summary: ChildSummary) -> bool:
    return child_summary.failed > 0 or child_summary.cancelled > 0 or child_summary.pending > 0


def _rerun_batch_root_children_in_place(db: Session, job: Job) -> Job:
    child_job_type = batch_child_job_type_for_root(job.job_type)
    if child_job_type is None:
        raise RuntimeError("Unsupported batch job type")

    stats = {
        "children_total": 0,
        "children_enqueued": 0,
        "children_succeeded": 0,
        "children_existing": 0,
    }
    for child_scope_json in _planned_batch_child_scopes(job):
        stats["children_total"] += 1
        latest_child = _latest_batch_lineage_child_attempt(
            db,
            batch_job=job,
            child_job_type=child_job_type,
            child_scope_json=child_scope_json,
        )
        if latest_child is not None and latest_child.status == JobStatus.succeeded:
            stats["children_succeeded"] += 1
            continue
        if latest_child is not None and latest_child.status in {JobStatus.pending, JobStatus.running}:
            stats["children_existing"] += 1
            continue

        _insert_job_record(
            db,
            child_job_type,
            child_scope_json,
            parent_job_id=job.id,
            attempt_mode=JobAttemptMode.repair,
            attempt_series_key=latest_child.attempt_series_key if latest_child is not None else None,
        )
        stats["children_enqueued"] += 1

    job.stats_json = stats
    db.add(job)
    db.commit()
    db.refresh(job)
    logger.info(
        "Queued batch root rerun child repairs in place",
        extra={"job_id": job.id, "children_enqueued": stats["children_enqueued"], "children_total": stats["children_total"]},
    )
    return job


def _rerun_direct_sync_job(db: Session, job: Job, scope: ScopePayload) -> Job:
    latest_attempt = _latest_attempt_in_series(db, job.attempt_series_key)
    if latest_attempt is None or latest_attempt.id != job.id:
        raise RuntimeError("Only the latest run in this repair chain can be re-run")
    existing = _find_active_job(
        db,
        job_type=job.job_type,
        dedupe_key=job.dedupe_key,
        parent_job_id=job.parent_job_id,
    )
    if existing is not None:
        raise RuntimeError(f"An identical job is already active ({existing.id[:8]}).")
    return create_sync_job(
        db,
        job.job_type,
        scope,
        attempt_mode=JobAttemptMode.repair,
        attempt_series_key=job.attempt_series_key,
    )


def _rerun_batch_child_job(db: Session, job: Job, scope: ScopePayload) -> Job:
    latest_attempt = _latest_attempt_in_series(db, job.attempt_series_key)
    if latest_attempt is None or latest_attempt.id != job.id:
        raise RuntimeError("Only the latest run in this repair chain can be re-run")

    parent_job = db.get(Job, job.parent_job_id) if job.parent_job_id is not None else None
    if parent_job is None or not _job_is_batch_root(parent_job):
        raise RuntimeError("Batch child job is missing its parent batch folder")

    latest_parent = _latest_batch_root_attempt(db, parent_job)
    if latest_parent is None or latest_parent.id != parent_job.id:
        raise RuntimeError("Only child jobs from the latest batch attempt can be re-run")

    parent_child_summary = _child_summary_for_batch(latest_parent, list_child_jobs(db, latest_parent.id))
    parent_batch_state = _batch_state_for_job(latest_parent, parent_child_summary)
    if parent_batch_state == "stopping":
        logger.info(
            "Rejected batch child rerun because parent batch is stopping",
            extra={"job_id": job.id, "parent_job_id": latest_parent.id, "parent_batch_state": parent_batch_state},
        )
        raise RuntimeError("Child jobs from a stopping batch folder cannot be re-run")

    existing = _find_active_job(
        db,
        job_type=job.job_type,
        dedupe_key=job.dedupe_key,
        parent_job_id=latest_parent.id,
    )
    if existing is not None:
        raise RuntimeError(f"An identical job is already active ({existing.id[:8]}).")
    return create_job(
        db,
        job.job_type,
        scope,
        parent_job_id=latest_parent.id,
        attempt_mode=JobAttemptMode.repair,
        attempt_series_key=job.attempt_series_key,
    )


def _rerun_batch_root_job(db: Session, job: Job, scope: ScopePayload) -> Job:
    latest_batch_attempt = _latest_attempt_in_series(db, job.attempt_series_key)
    if latest_batch_attempt is None or latest_batch_attempt.id != job.id:
        raise RuntimeError("Only the latest run in this repair chain can be re-run")

    child_summary = _child_summary_for_batch(job, list_child_jobs(db, job.id))
    if _batch_state_for_job(job, child_summary) in {"queued", "running", "stopping"}:
        raise RuntimeError("Only finished jobs can be re-run")
    if not _batch_root_has_retryable_scopes(child_summary):
        raise RuntimeError("All child scopes already succeeded")

    existing = _find_active_job(
        db,
        job_type=job.job_type,
        dedupe_key=job.dedupe_key,
        parent_job_id=None,
    )
    if existing is not None:
        raise RuntimeError(f"An identical job is already active ({existing.id[:8]}).")
    return _rerun_batch_root_children_in_place(db, job)


def rerun_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise LookupError("Job not found")
    if job.status in {JobStatus.pending, JobStatus.running}:
        raise RuntimeError("Only finished jobs can be re-run")

    scope = build_scope_payload(job.scope_json)
    if _job_is_batch_root(job):
        return _rerun_batch_root_job(db, job, scope)
    if job.job_type in {JobType.sync_papers, JobType.find_repos, JobType.refresh_metadata}:
        if job.parent_job_id is not None:
            return _rerun_batch_child_job(db, job, scope)
        return _rerun_direct_sync_job(db, job, scope)
    raise ValueError("Re-run is only supported for sync jobs")


def _stop_batch_job(db: Session, job: Job) -> Job:
    child_jobs = list_child_jobs(db, job.id)
    batch_state = _batch_state_for_job(job, _child_summary_for_batch(job, child_jobs))
    if batch_state not in {"queued", "running", "stopping"}:
        raise RuntimeError("Only active jobs can be stopped")

    request_job_stop(job)
    if job.status == JobStatus.pending:
        mark_job_cancelled(job, clear_lock=True)

    for child in child_jobs:
        if child.status == JobStatus.pending:
            mark_job_cancelled(child, clear_lock=True)
        elif child.status == JobStatus.running:
            request_job_stop(child)

    db.commit()
    db.refresh(job)
    return job


def stop_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise LookupError("Job not found")

    if _job_is_batch_root(job):
        return _stop_batch_job(db, job)

    if job.status == JobStatus.pending:
        mark_job_cancelled(job, clear_lock=True)
        db.commit()
        db.refresh(job)
        return job
    if job.status == JobStatus.running:
        request_job_stop(job)
        db.commit()
        db.refresh(job)
        return job
    raise RuntimeError("Only active jobs can be stopped")


async def run_batch_root_job(
    db: Session,
    job: Job,
    *,
    progress: Callable[[dict[str, object]], None] | None = None,
    stop_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    child_job_type = batch_child_job_type_for_root(job.job_type)
    if child_job_type is None:
        raise RuntimeError(f"Unsupported batch job type: {job.job_type}")

    child_scopes = _planned_batch_child_scopes(job)
    stats = {
        "children_total": len(child_scopes),
        "children_enqueued": 0,
        "children_reused_success": 0,
        "children_existing": 0,
    }
    if progress is not None:
        progress(dict(stats))

    for child_scope_json in child_scopes:
        if stop_check is not None:
            stop_check()

        child_dedupe_key = build_dedupe_key(child_job_type.value, child_scope_json)
        current_attempt_child = db.scalar(
            select(Job)
            .where(
                Job.job_type == child_job_type,
                Job.dedupe_key == child_dedupe_key,
                Job.parent_job_id == job.id,
                Job.status.in_([JobStatus.pending, JobStatus.running, JobStatus.succeeded]),
            )
            .order_by(*job_display_order_by())
        )
        if current_attempt_child is not None:
            stats["children_existing"] += 1
            if progress is not None:
                progress(dict(stats))
            continue

        latest_child = None
        if job.attempt_mode == JobAttemptMode.repair:
            latest_child = _latest_batch_lineage_child_attempt(
                db,
                batch_job=job,
                child_job_type=child_job_type,
                child_scope_json=child_scope_json,
            )

        if latest_child is not None and latest_child.status == JobStatus.succeeded:
            _create_reused_child_record(
                db,
                child_job_type=child_job_type,
                child_scope_json=child_scope_json,
                parent_job_id=job.id,
                source_job=latest_child,
                attempt_mode=job.attempt_mode,
            )
            stats["children_reused_success"] += 1
        else:
            child_scope = build_scope_payload(child_scope_json)
            child_series_key = latest_child.attempt_series_key if latest_child is not None else None
            _insert_job_record(
                db,
                child_job_type,
                build_scope_json(child_scope),
                parent_job_id=job.id,
                attempt_mode=job.attempt_mode,
                attempt_series_key=child_series_key,
            )
            stats["children_enqueued"] += 1
        if progress is not None:
            progress(dict(stats))
    return stats


def claim_next_job(db: Session, worker_name: str) -> Job | None:
    stale_before = _fresh_running_after()
    if _has_fresh_running_job(db, stale_before):
        return None

    stmt = (
        select(Job)
        .where(
            or_(
                Job.status == JobStatus.pending,
                and_(Job.status == JobStatus.running, Job.locked_at.is_not(None), Job.locked_at < stale_before),
            )
        )
        .order_by(*job_execution_order_by())
    )
    if db.bind.dialect.name == "postgresql":
        stmt = stmt.with_for_update(skip_locked=True)
    else:
        stmt = stmt.with_for_update()

    job = db.scalar(stmt)
    if job is None:
        return None
    if job.status == JobStatus.running and job.stop_requested_at is not None:
        mark_job_cancelled(job)
        db.commit()
        return None
    job.status = JobStatus.running
    if job.started_at is None:
        job.started_at = utc_now()
    job.finished_at = None
    job.error_text = None
    job.locked_by = worker_name
    job.locked_at = utc_now()
    job.attempts += 1
    db.commit()
    db.refresh(job)
    return job


async def process_job(job_id: str) -> None:
    with session_scope() as db:
        job = db.get(Job, job_id)
        if job is None:
            return

        def persist_progress(stats: dict[str, object]) -> None:
            job.stats_json = dict(stats)
            job.locked_at = utc_now()
            db.commit()

        def stop_check() -> None:
            raise_if_job_stop_requested(db, job_id)

        try:
            stop_check()
            if job.job_type == JobType.sync_papers:
                job.stats_json = await run_sync_papers(
                    db,
                    job.scope_json,
                    job_id=job.id,
                    attempt_series_key=job.attempt_series_key,
                    attempt_mode=job.attempt_mode,
                    progress=persist_progress,
                    stop_check=stop_check,
                )
            elif is_batch_root_job_type(job.job_type):
                job.stats_json = await run_batch_root_job(db, job, progress=persist_progress, stop_check=stop_check)
            elif job.job_type == JobType.find_repos:
                job.stats_json = await run_find_repos(
                    db,
                    job.scope_json,
                    job_id=job.id,
                    attempt_series_key=job.attempt_series_key,
                    attempt_mode=job.attempt_mode,
                    progress=persist_progress,
                    stop_check=stop_check,
                )
            elif job.job_type == JobType.refresh_metadata:
                job.stats_json = await run_refresh_metadata(
                    db,
                    job.scope_json,
                    job_id=job.id,
                    attempt_series_key=job.attempt_series_key,
                    attempt_mode=job.attempt_mode,
                    progress=persist_progress,
                    stop_check=stop_check,
                )
            elif job.job_type == JobType.export:
                job.stats_json = run_export(db, job.scope_json, stop_check=stop_check)
            else:
                raise RuntimeError(f"Unsupported job type: {job.job_type}")
            job.status = JobStatus.succeeded
            job.finished_at = utc_now()
            job.locked_at = job.finished_at
        except JobStopRequested:
            mark_job_cancelled(job)
        except Exception as exc:
            job.status = JobStatus.failed
            job.finished_at = utc_now()
            job.locked_at = job.finished_at
            job.error_text = str(exc)


async def run_worker_forever() -> None:
    settings = get_settings()
    worker_name = f"{socket.gethostname()}:{os.getpid()}"
    while True:
        with session_scope() as db:
            job = claim_next_job(db, worker_name)
        if job is None:
            await asyncio.sleep(settings.job_queue_worker_poll_seconds)
            continue
        await process_job(job.id)
