from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import socket
from datetime import timedelta
from typing import Any
from collections.abc import Callable

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from src.ghstarsv2.config import get_settings
from src.ghstarsv2.db import get_engine, session_scope
from src.ghstarsv2.models import Job, JobStatus, JobType, utc_now
from src.ghstarsv2.scope import (
    arxiv_scope_spans_multiple_months,
    build_scope_json,
    build_scope_payload,
    build_dedupe_key,
    expand_arxiv_child_scope_jsons,
)
from src.ghstarsv2.schemas import ChildSummary, JobRead, ScopePayload, validate_scope_for_job
from src.ghstarsv2.services import (
    backfill_arxiv_archive_appearances,
    ensure_runtime_dirs,
    run_enrich,
    run_export,
    run_sync_arxiv,
    run_sync_links,
)


INIT_DATABASE_LOCK_ID = 649183502117041921


@dataclass(frozen=True)
class JobAttemptMeta:
    attempt_count: int = 1
    attempt_rank: int = 1


def init_database() -> None:
    ensure_runtime_dirs()
    engine = get_engine()
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            conn.exec_driver_sql(f"SELECT pg_advisory_lock({INIT_DATABASE_LOCK_ID})")
        try:
            from src.ghstarsv2.migrations import run_database_migrations

            run_database_migrations(conn)
        finally:
            if engine.dialect.name == "postgresql":
                conn.exec_driver_sql(f"SELECT pg_advisory_unlock({INIT_DATABASE_LOCK_ID})")
    with session_scope() as db:
        backfill_arxiv_archive_appearances(db)


def _fresh_running_after() -> object:
    settings = get_settings()
    return utc_now() - timedelta(seconds=settings.job_timeout_seconds)


def _parent_job_clause(parent_job_id: str | None) -> object:
    return Job.parent_job_id.is_(None) if parent_job_id is None else Job.parent_job_id == parent_job_id


def _create_job_record(
    db: Session,
    job_type: JobType,
    scope_json: dict[str, Any],
    *,
    parent_job_id: str | None = None,
) -> tuple[Job, bool]:
    dedupe_key = build_dedupe_key(job_type.value, scope_json)
    existing = db.scalar(
        select(Job).where(
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
    )
    if existing is not None:
        return existing, False

    job = Job(
        parent_job_id=parent_job_id,
        job_type=job_type,
        status=JobStatus.pending,
        scope_json=scope_json,
        dedupe_key=dedupe_key,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job, True


def create_job(
    db: Session,
    job_type: JobType,
    scope: ScopePayload,
    *,
    parent_job_id: str | None = None,
) -> Job:
    validate_scope_for_job(scope, job_type)
    scope_json = build_scope_json(scope)
    job, _created = _create_job_record(db, job_type, scope_json, parent_job_id=parent_job_id)
    return job


def create_sync_arxiv_job(db: Session, scope: ScopePayload, *, parent_job_id: str | None = None) -> Job:
    validate_scope_for_job(scope, JobType.sync_arxiv)
    scope_json = build_scope_json(scope)
    job_type = JobType.sync_arxiv
    if parent_job_id is None and arxiv_scope_spans_multiple_months(scope_json):
        job_type = JobType.sync_arxiv_batch
    job, _created = _create_job_record(db, job_type, scope_json, parent_job_id=parent_job_id)
    return job


def _latest_jobs_by_dedupe(jobs: list[Job]) -> list[Job]:
    latest: dict[str, Job] = {}
    for job in sorted(jobs, key=lambda item: (item.created_at, item.id), reverse=True):
        latest.setdefault(job.dedupe_key, job)
    return list(latest.values())


def _child_summary_for_batch(scope_json: dict[str, Any], child_jobs: list[Job]) -> ChildSummary:
    latest_jobs = _latest_jobs_by_dedupe(child_jobs)
    planned_total = len(expand_arxiv_child_scope_jsons(scope_json))
    latest_total = len(latest_jobs)
    total = max(planned_total, latest_total)
    counts = {
        JobStatus.pending: 0,
        JobStatus.running: 0,
        JobStatus.succeeded: 0,
        JobStatus.failed: 0,
    }
    for job in latest_jobs:
        counts[job.status] += 1
    pending = counts[JobStatus.pending] + max(0, total - latest_total)
    return ChildSummary(
        total=total,
        pending=pending,
        running=counts[JobStatus.running],
        succeeded=counts[JobStatus.succeeded],
        failed=counts[JobStatus.failed],
    )


def _batch_state_for_job(job: Job, child_summary: ChildSummary | None) -> str:
    if job.status == JobStatus.pending:
        return "queued"
    if job.status == JobStatus.running:
        return "running"
    if job.status == JobStatus.failed:
        return "failed"
    if child_summary is None:
        return "succeeded"
    if child_summary.failed > 0:
        return "failed"
    if child_summary.running > 0 or child_summary.pending > 0:
        return "running"
    return "succeeded"


def list_child_jobs(db: Session, parent_job_id: str) -> list[Job]:
    return list(db.scalars(select(Job).where(Job.parent_job_id == parent_job_id).order_by(Job.created_at.desc())))


def _attempt_meta_subquery(*, parent_job_id: str | None = None):
    partition_by = (Job.parent_job_id, Job.dedupe_key)
    stmt = select(
        Job.id.label("job_id"),
        func.count(Job.id).over(partition_by=partition_by).label("attempt_count"),
        func.row_number().over(
            partition_by=partition_by,
            order_by=(Job.created_at.desc(), Job.id.desc()),
        ).label("attempt_rank"),
    )
    if parent_job_id is not None:
        stmt = stmt.where(Job.parent_job_id == parent_job_id)
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
    attempt_meta = _attempt_meta_subquery(parent_job_id=parent_job_id)
    stmt = (
        select(Job, attempt_meta.c.attempt_count, attempt_meta.c.attempt_rank)
        .join(attempt_meta, Job.id == attempt_meta.c.job_id)
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(limit)
    )
    if root_only and parent_job_id is None:
        stmt = stmt.where(Job.parent_job_id.is_(None))
    if view == "latest":
        stmt = stmt.where(attempt_meta.c.attempt_rank == 1)
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
            Job.job_type == job.job_type,
            Job.dedupe_key == job.dedupe_key,
            _parent_job_clause(job.parent_job_id),
        )
        .order_by(Job.created_at.desc(), Job.id.desc())
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
    if job.job_type == JobType.sync_arxiv_batch:
        child_jobs = child_jobs if child_jobs is not None else list_child_jobs(db, job.id)
        child_summary = _child_summary_for_batch(job.scope_json, child_jobs)
        batch_state = _batch_state_for_job(job, child_summary)
    attempt_meta = attempt_meta or JobAttemptMeta()

    return JobRead(
        id=job.id,
        parent_job_id=job.parent_job_id,
        job_type=job.job_type,
        status=job.status,
        scope_json=job.scope_json,
        dedupe_key=job.dedupe_key,
        stats_json=job.stats_json,
        error_text=job.error_text,
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
    batch_parent_ids = [job.id for job in jobs if job.job_type == JobType.sync_arxiv_batch]
    child_jobs_by_parent: dict[str, list[Job]] = {}
    if batch_parent_ids:
        child_jobs = list(
            db.scalars(select(Job).where(Job.parent_job_id.in_(batch_parent_ids)).order_by(Job.created_at.desc()))
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


def _rerun_sync_arxiv_child_job(db: Session, job: Job, scope: ScopePayload) -> Job:
    # Child reruns stay attached to the same batch parent and only enqueue this exact child scope.
    return create_job(db, JobType.sync_arxiv, scope, parent_job_id=job.parent_job_id)


def rerun_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise LookupError("Job not found")
    if job.status in {JobStatus.pending, JobStatus.running}:
        raise RuntimeError("Only finished jobs can be re-run")

    scope = build_scope_payload(job.scope_json)
    if job.job_type == JobType.sync_arxiv_batch:
        child_summary = _child_summary_for_batch(job.scope_json, list_child_jobs(db, job.id))
        if _batch_state_for_job(job, child_summary) in {"queued", "running"}:
            raise RuntimeError("Only finished jobs can be re-run")
        return create_sync_arxiv_job(db, scope)
    if job.job_type == JobType.sync_arxiv:
        latest_attempt = db.scalar(
            select(Job)
            .where(
                Job.job_type == JobType.sync_arxiv,
                Job.dedupe_key == job.dedupe_key,
                _parent_job_clause(job.parent_job_id),
            )
            .order_by(Job.created_at.desc())
        )
        if latest_attempt is not None and latest_attempt.id != job.id and latest_attempt.status in {JobStatus.pending, JobStatus.running}:
            raise RuntimeError("A newer attempt is still active")
        if job.parent_job_id is not None:
            return _rerun_sync_arxiv_child_job(db, job, scope)
        return create_sync_arxiv_job(db, scope)
    raise ValueError("Re-run is only supported for arXiv jobs")


async def run_sync_arxiv_batch(
    db: Session,
    scope_json: dict[str, Any],
    *,
    parent_job_id: str,
    progress: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, Any]:
    child_scopes = expand_arxiv_child_scope_jsons(scope_json)
    stats = {
        "children_total": len(child_scopes),
        "children_created": 0,
        "children_reused": 0,
    }
    if progress is not None:
        progress(dict(stats))

    for child_scope_json in child_scopes:
        child_scope = build_scope_payload(child_scope_json)
        child, created = _create_job_record(
            db,
            JobType.sync_arxiv,
            build_scope_json(child_scope),
            parent_job_id=parent_job_id,
        )
        if created:
            stats["children_created"] += 1
        else:
            stats["children_reused"] += 1
        if progress is not None:
            progress(dict(stats))
        # Keep the latest reference alive for debuggability even when reused.
        _ = child
    return stats


def claim_next_job(db: Session, worker_name: str) -> Job | None:
    stale_before = _fresh_running_after()
    stmt = (
        select(Job)
        .where(
            or_(
                Job.status == JobStatus.pending,
                and_(Job.status == JobStatus.running, Job.locked_at.is_not(None), Job.locked_at < stale_before),
            )
        )
        .order_by(Job.created_at.asc())
    )
    if db.bind.dialect.name == "postgresql":
        stmt = stmt.with_for_update(skip_locked=True)
    else:
        stmt = stmt.with_for_update()

    job = db.scalar(stmt)
    if job is None:
        return None
    job.status = JobStatus.running
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

        try:
            if job.job_type == JobType.sync_arxiv:
                job.stats_json = await run_sync_arxiv(db, job.scope_json, progress=persist_progress)
            elif job.job_type == JobType.sync_arxiv_batch:
                job.stats_json = await run_sync_arxiv_batch(db, job.scope_json, parent_job_id=job.id, progress=persist_progress)
            elif job.job_type == JobType.sync_links:
                job.stats_json = await run_sync_links(db, job.scope_json, progress=persist_progress)
            elif job.job_type == JobType.enrich:
                job.stats_json = await run_enrich(db, job.scope_json, progress=persist_progress)
            elif job.job_type == JobType.export:
                job.stats_json = run_export(db, job.scope_json)
            else:
                raise RuntimeError(f"Unsupported job type: {job.job_type}")
            job.status = JobStatus.succeeded
            job.finished_at = utc_now()
            job.locked_at = job.finished_at
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
            await asyncio.sleep(settings.worker_poll_seconds)
            continue
        await process_job(job.id)
