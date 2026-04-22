from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import configure_database, session_scope
from papertorepo.jobs.queue import create_job, get_job_attempt_meta, init_database, list_jobs_read, process_job, rerun_job, serialize_job, stop_job
from papertorepo.jobs.stop import request_job_stop
from papertorepo.db.models import Job, JobAttemptMode, JobStatus, JobType, utc_now
from papertorepo.api.schemas import ScopePayload
from papertorepo.services.pipeline import get_dashboard_stats, get_job_queue_snapshot


@pytest.fixture()
def job_env(monkeypatch, tmp_path):
    db_path = tmp_path / "papertorepo-test.db"
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    clear_settings_cache()
    configure_database()
    init_database()
    yield
    clear_settings_cache()


def test_stop_pending_job_marks_it_cancelled(job_env):
    with session_scope() as db:
        job = create_job(db, JobType.export, ScopePayload(output_name="papers.csv"))
        stopped = stop_job(db, job.id)
        assert stopped.status == JobStatus.cancelled
        assert stopped.stop_requested_at is not None
        assert stopped.finished_at is not None
        assert stopped.error_text == "Stopped by user."


def test_create_job_defaults_attempt_mode_to_fresh(job_env):
    with session_scope() as db:
        job = create_job(db, JobType.export, ScopePayload(output_name="papers.csv"))

    assert job.attempt_mode == JobAttemptMode.fresh


def test_rerun_job_creates_repair_attempt(job_env):
    with session_scope() as db:
        job = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        job.status = JobStatus.succeeded
        job.finished_at = utc_now()
        db.add(job)

    with session_scope() as db:
        rerun = rerun_job(db, job.id)

    assert rerun.id != job.id
    assert rerun.attempt_mode == JobAttemptMode.repair
    assert rerun.status == JobStatus.pending


def test_same_scope_fresh_runs_do_not_share_attempt_series(job_env):
    with session_scope() as db:
        first = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        second = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))

    assert first.attempt_series_key != second.attempt_series_key

    with session_scope() as db:
        first_meta = get_job_attempt_meta(db, db.get(Job, first.id))
        second_meta = get_job_attempt_meta(db, db.get(Job, second.id))

    assert first_meta.attempt_count == 1
    assert second_meta.attempt_count == 1


def test_rerun_attaches_to_clicked_fresh_series_not_newer_same_scope_fresh(job_env):
    with session_scope() as db:
        first = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        second = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        first.status = JobStatus.succeeded
        first.finished_at = utc_now()
        second.status = JobStatus.succeeded
        second.finished_at = utc_now()
        db.add_all([first, second])

    with session_scope() as db:
        rerun = rerun_job(db, first.id)

    assert rerun.attempt_series_key == first.attempt_series_key
    assert rerun.attempt_series_key != second.attempt_series_key
    assert rerun.attempt_mode == JobAttemptMode.repair

    with session_scope() as db:
        first_meta = get_job_attempt_meta(db, db.get(Job, first.id))
        second_meta = get_job_attempt_meta(db, db.get(Job, second.id))
        rerun_meta = get_job_attempt_meta(db, db.get(Job, rerun.id))

    assert first_meta.attempt_count == 2
    assert first_meta.attempt_rank == 2
    assert rerun_meta.attempt_count == 2
    assert rerun_meta.attempt_rank == 1
    assert second_meta.attempt_count == 1
    assert second_meta.attempt_rank == 1


def test_only_latest_repair_chain_node_can_rerun(job_env):
    with session_scope() as db:
        root = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        root.status = JobStatus.succeeded
        root.finished_at = utc_now()
        db.add(root)

    with session_scope() as db:
        rerun = rerun_job(db, root.id)
        rerun.status = JobStatus.succeeded
        rerun.finished_at = utc_now()
        db.add(rerun)

    with session_scope() as db:
        with pytest.raises(RuntimeError, match="Only the latest run in this repair chain can be re-run"):
            rerun_job(db, root.id)


@pytest.mark.anyio
async def test_process_job_respects_preexisting_stop_request(job_env):
    with session_scope() as db:
        job = create_job(
            db,
            JobType.sync_links,
            ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)),
        )
        job.status = JobStatus.running
        job.started_at = utc_now()
        job.locked_by = "test-worker"
        job.locked_at = utc_now()
        request_job_stop(job)
        db.add(job)

    await process_job(job.id)

    with session_scope() as db:
        stopped = db.get(Job, job.id)
        assert stopped is not None
        assert stopped.status == JobStatus.cancelled
        assert stopped.finished_at is not None
        assert stopped.error_text == "Stopped by user."


def test_stop_batch_job_cascades_to_children_and_updates_batch_state(job_env):
    with session_scope() as db:
        parent = create_job(
            db,
            JobType.sync_arxiv_batch,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 4, 1), "to": date(2026, 5, 31)}),
        )
        parent.status = JobStatus.succeeded
        pending_child = create_job(
            db,
            JobType.sync_arxiv,
            ScopePayload(categories=["cs.CV"], month="2026-04"),
            parent_job_id=parent.id,
        )
        running_child = create_job(
            db,
            JobType.sync_arxiv,
            ScopePayload(categories=["cs.CV"], month="2026-05"),
            parent_job_id=parent.id,
        )
        running_child.status = JobStatus.running
        running_child.started_at = utc_now()
        running_child.locked_by = "worker"
        running_child.locked_at = utc_now()
        db.add(parent)
        db.add(running_child)

    with session_scope() as db:
        stopped_parent = stop_job(db, parent.id)
        stopped_parent_read = serialize_job(db, stopped_parent)
        pending_child_after = db.get(Job, pending_child.id)
        running_child_after = db.get(Job, running_child.id)
        assert stopped_parent_read.batch_state == "stopping"
        assert pending_child_after is not None and pending_child_after.status == JobStatus.cancelled
        assert running_child_after is not None and running_child_after.stop_requested_at is not None

        running_child_after.status = JobStatus.cancelled
        running_child_after.finished_at = utc_now()
        db.add(running_child_after)

    with session_scope() as db:
        refreshed_parent = db.get(Job, parent.id)
        assert refreshed_parent is not None
        refreshed_parent_read = serialize_job(db, refreshed_parent)
        assert refreshed_parent_read.batch_state == "cancelled"


def test_dashboard_stats_split_running_and_stopping_jobs(job_env):
    with session_scope() as db:
        pending = create_job(db, JobType.export, ScopePayload(output_name="pending.csv"))
        running = create_job(db, JobType.export, ScopePayload(output_name="running.csv"))
        running.status = JobStatus.running
        running.started_at = utc_now()
        running.locked_at = utc_now()
        running.locked_by = "worker-a"

        stopping = create_job(db, JobType.export, ScopePayload(output_name="stopping.csv"))
        stopping.status = JobStatus.running
        stopping.started_at = utc_now()
        stopping.locked_at = utc_now()
        stopping.locked_by = "worker-b"
        request_job_stop(stopping)

        db.add_all([pending, running, stopping])

    with session_scope() as db:
        stats = get_dashboard_stats(db, {})
        assert stats["pending_jobs"] == 1
        assert stats["running_jobs"] == 1
        assert stats["stopping_jobs"] == 1


def test_job_queue_snapshot_returns_current_running_and_next_pending_job(job_env):
    with session_scope() as db:
        pending = create_job(db, JobType.export, ScopePayload(output_name="pending.csv"))
        running = create_job(db, JobType.export, ScopePayload(output_name="running.csv"))
        running.status = JobStatus.running
        running.started_at = utc_now()
        running.locked_at = utc_now()
        running.locked_by = "worker-a"
        db.add_all([pending, running])

    with session_scope() as db:
        snapshot = get_job_queue_snapshot(db)
        assert snapshot["state"] == "active"
        assert snapshot["current_job_id"] == running.id
        assert snapshot["next_job_id"] == pending.id


def test_job_queue_snapshot_prefers_child_job_over_running_batch_root(job_env):
    now = utc_now()
    with session_scope() as db:
        parent = create_job(
            db,
            JobType.sync_arxiv_batch,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 4, 1), "to": date(2026, 5, 31)}),
        )
        parent.status = JobStatus.running
        parent.started_at = now
        parent.locked_at = now
        parent.locked_by = "worker-parent"

        child = create_job(
            db,
            JobType.sync_arxiv,
            ScopePayload(categories=["cs.CV"], month="2026-04"),
            parent_job_id=parent.id,
        )
        child.status = JobStatus.running
        child.started_at = now - timedelta(seconds=5)
        child.locked_at = now - timedelta(seconds=5)
        child.locked_by = "worker-child"
        db.add_all([parent, child])

    with session_scope() as db:
        snapshot = get_job_queue_snapshot(db)
        assert snapshot["state"] == "active"
        assert snapshot["current_job_id"] == child.id


def test_job_display_order_prefers_newer_scope_when_created_at_matches(job_env):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        april = create_job(db, JobType.sync_arxiv, ScopePayload(categories=["cs.CV"], month="2026-04"))
        may = create_job(db, JobType.sync_arxiv, ScopePayload(categories=["cs.CV"], month="2026-05"))
        april.created_at = same_created_at
        may.created_at = same_created_at
        db.add_all([april, may])

    with session_scope() as db:
        rows = list_jobs_read(db, limit=10, view="all")

    assert [row.scope_json["month"] for row in rows[:2]] == ["2026-05", "2026-04"]


def test_job_queue_snapshot_prefers_older_scope_when_created_at_matches(job_env):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        april = create_job(db, JobType.sync_arxiv, ScopePayload(categories=["cs.CV"], month="2026-04"))
        may = create_job(db, JobType.sync_arxiv, ScopePayload(categories=["cs.CV"], month="2026-05"))
        april.created_at = same_created_at
        may.created_at = same_created_at
        db.add_all([april, may])

    with session_scope() as db:
        snapshot = get_job_queue_snapshot(db)

    assert snapshot["state"] == "waiting"
    assert snapshot["next_job_id"] == april.id
