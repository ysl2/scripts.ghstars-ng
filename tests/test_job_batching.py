from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from fastapi.testclient import TestClient
import pytest

from papertorepo.api.app import app
from papertorepo.db.session import session_scope
from papertorepo.jobs.queue import (
    claim_next_job,
    create_job,
    create_sync_papers_job,
    create_sync_job,
    list_child_jobs,
    list_jobs_read,
    process_job,
    rerun_job,
    serialize_job,
    stop_job,
)
from papertorepo.jobs.batches import planned_child_scope_jsons
from papertorepo.db.models import Job, JobAttemptMode, JobStatus, JobType
from papertorepo.core.scope import expand_sync_papers_child_scope_jsons
from papertorepo.api.schemas import ScopePayload


def test_create_sync_papers_job_uses_batch_for_multi_month_scope(db_env):
    with session_scope() as db:
        job = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    assert job.job_type == JobType.sync_papers_batch
    assert job.parent_job_id is None


def test_expand_sync_papers_child_scope_jsons_inherit_force_flag():
    child_scopes = expand_sync_papers_child_scope_jsons(
        {
            "categories": ["cs.CV"],
            "from": "2025-03-15",
            "to": "2025-04-10",
            "force": True,
            "day": None,
            "month": None,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        }
    )

    assert len(child_scopes) == 2
    assert all(child_scope["force"] is True for child_scope in child_scopes)


def test_planned_child_scope_jsons_keep_partial_edges_contiguous_for_find_repos():
    child_scopes = planned_child_scope_jsons(
        JobType.find_repos,
        {
            "categories": ["cs.CV"],
            "from": "2025-03-15",
            "to": "2025-06-10",
            "force": True,
            "day": None,
            "month": None,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        },
    )

    assert child_scopes == [
        {
            "categories": ["cs.CV"],
            "day": None,
            "month": None,
            "from": "2025-03-15",
            "to": "2025-03-31",
            "force": True,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        },
        {
            "categories": ["cs.CV"],
            "day": None,
            "month": "2025-04",
            "from": None,
            "to": None,
            "force": True,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        },
        {
            "categories": ["cs.CV"],
            "day": None,
            "month": "2025-05",
            "from": None,
            "to": None,
            "force": True,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        },
        {
            "categories": ["cs.CV"],
            "day": None,
            "month": None,
            "from": "2025-06-01",
            "to": "2025-06-10",
            "force": True,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        },
    ]


def test_create_sync_job_uses_batch_for_multi_window_link_scope(db_env):
    with session_scope() as db:
        job = create_sync_job(
            db,
            JobType.find_repos,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 6, 10)},
            ),
        )

    assert job.job_type == JobType.find_repos_batch
    assert job.parent_job_id is None


def test_create_sync_job_canonicalizes_single_day_range_to_day(db_env):
    with session_scope() as db:
        job = create_sync_job(
            db,
            JobType.find_repos,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 4, 12), "to": date(2025, 4, 12)},
            ),
        )

    assert job.job_type == JobType.find_repos
    assert job.scope_json["day"] == "2025-04-12"
    assert job.scope_json["month"] is None
    assert job.scope_json["from"] is None
    assert job.scope_json["to"] is None


def test_create_sync_job_canonicalizes_full_month_range_to_month(db_env):
    with session_scope() as db:
        job = create_sync_job(
            db,
            JobType.find_repos,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 4, 1), "to": date(2025, 4, 30)},
            ),
        )

    assert job.job_type == JobType.find_repos
    assert job.scope_json["day"] is None
    assert job.scope_json["month"] == "2025-04"
    assert job.scope_json["from"] is None
    assert job.scope_json["to"] is None


def test_create_sync_papers_job_keeps_single_partial_month_range_as_direct_job(db_env):
    with session_scope() as db:
        job = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 3, 20)},
            ),
        )

    assert job.job_type == JobType.sync_papers
    assert job.scope_json["day"] is None
    assert job.scope_json["month"] is None
    assert job.scope_json["from"] == "2025-03-15"
    assert job.scope_json["to"] == "2025-03-20"


@pytest.mark.anyio
async def test_process_batch_job_creates_archive_month_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None
    assert claimed.id == parent.id

    await process_job(parent.id)

    with session_scope() as db:
        refreshed_parent = db.get(Job, parent.id)
        children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())

    assert refreshed_parent is not None
    assert refreshed_parent.status == JobStatus.succeeded
    assert refreshed_parent.job_type == JobType.sync_papers_batch
    assert len(children) == 2
    assert [child.scope_json["month"] for child in children] == ["2025-03", "2025-04"]
    assert [child.scope_json["from"] for child in children] == [None, None]
    assert [child.scope_json["to"] for child in children] == [None, None]
    assert all(child.job_type == JobType.sync_papers for child in children)


@pytest.mark.anyio
async def test_batch_summary_uses_latest_child_attempt_per_scope(db_env):
    with session_scope() as db:
        parent = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None
    await process_job(parent.id)

    with session_scope() as db:
        children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())
        march_job, april_job = children
        march_job.status = JobStatus.failed
        march_job.finished_at = march_job.locked_at
        april_job.status = JobStatus.succeeded
        april_job.finished_at = april_job.locked_at

    with session_scope() as db:
        rerun = rerun_job(db, march_job.id)
        rerun.status = JobStatus.succeeded
        rerun.finished_at = rerun.locked_at

    with session_scope() as db:
        refreshed_parent = db.get(Job, parent.id)
        assert refreshed_parent is not None
        serialized = serialize_job(db, refreshed_parent)

    assert serialized.child_summary is not None
    assert serialized.child_summary.total == 2
    assert serialized.child_summary.succeeded == 2
    assert serialized.child_summary.failed == 0
    assert serialized.batch_state == "succeeded"


@pytest.mark.anyio
async def test_fresh_batch_run_does_not_reuse_previous_successful_children(db_env):
    scope = ScopePayload(
        categories=["cs.CV"],
        **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
    )

    with session_scope() as db:
        first_batch = create_sync_papers_job(db, scope)

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None
    await process_job(first_batch.id)

    with session_scope() as db:
        first_children = list(db.query(Job).filter(Job.parent_job_id == first_batch.id).order_by(Job.created_at.asc()).all())
        for child in first_children:
            child.status = JobStatus.succeeded
            child.finished_at = child.created_at
            db.add(child)

    with session_scope() as db:
        second_batch = create_sync_papers_job(db, scope)

    assert second_batch.id != first_batch.id
    assert second_batch.attempt_mode == JobAttemptMode.fresh

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None
    assert claimed.id == second_batch.id
    await process_job(second_batch.id)

    with session_scope() as db:
        second_children = list(db.query(Job).filter(Job.parent_job_id == second_batch.id).order_by(Job.created_at.asc()).all())

    assert len(second_children) == 2
    assert all(child.attempt_mode == JobAttemptMode.fresh for child in second_children)
    assert all(child.status == JobStatus.pending for child in second_children)
    assert all(child.stats_json.get("reused") is not True for child in second_children)


def test_rerun_api_supports_failed_only_batch_parent_rerun(db_env):
    with session_scope() as db:
        parent = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None

    import asyncio

    asyncio.run(process_job(parent.id))

    with session_scope() as db:
        children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())
        child = children[0] if children else None
        assert child is not None
        child.status = JobStatus.failed
        child.finished_at = child.created_at
        for sibling in children[1:]:
            sibling.status = JobStatus.succeeded
            sibling.finished_at = sibling.created_at
        child_scope = dict(child.scope_json)
        sibling_scopes = [dict(item.scope_json) for item in children[1:]]

    with TestClient(app) as client:
        batch_response = client.post(f"/api/v1/jobs/{parent.id}/rerun")
        child_response = client.post(f"/api/v1/jobs/{child.id}/rerun")

    assert batch_response.status_code == 200
    assert batch_response.json()["id"] == parent.id
    assert batch_response.json()["job_type"] == JobType.sync_papers_batch.value
    assert batch_response.json()["parent_job_id"] is None
    assert batch_response.json()["attempt_mode"] == JobAttemptMode.fresh.value
    assert batch_response.json()["batch_state"] == "queued"
    assert child_response.status_code == 409

    with session_scope() as db:
        rerun_batch = db.get(Job, parent.id)
        assert rerun_batch is not None
        latest_children = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")
        all_children = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="all")

    assert rerun_batch.status == JobStatus.succeeded
    assert len(latest_children) == 2
    assert len(all_children) == 3
    repair_child = next(item for item in latest_children if item.scope_json == child_scope)
    assert repair_child.status == JobStatus.pending
    assert repair_child.attempt_mode == JobAttemptMode.repair
    succeeded_sibling = next(item for item in latest_children if item.scope_json in sibling_scopes)
    assert succeeded_sibling.status == JobStatus.succeeded
    assert succeeded_sibling.attempt_mode == JobAttemptMode.fresh


def test_rerun_api_supports_latest_batch_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None

    import asyncio

    asyncio.run(process_job(parent.id))

    with session_scope() as db:
        children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())
        child = children[0]
        child.status = JobStatus.failed
        child.finished_at = child.created_at
        for sibling in children[1:]:
            sibling.status = JobStatus.succeeded
            sibling.finished_at = sibling.created_at

    with TestClient(app) as client:
        batch_response = client.post(f"/api/v1/jobs/{parent.id}/rerun")

    assert batch_response.status_code == 200
    assert batch_response.json()["id"] == parent.id

    with session_scope() as db:
        latest_batch_children = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")
        latest_child_read = next(item for item in latest_batch_children if item.status == JobStatus.pending)
        latest_child = db.get(Job, latest_child_read.id)
        assert latest_child is not None
        latest_child.status = JobStatus.failed
        latest_child.finished_at = latest_child.created_at
        latest_child_scope = dict(latest_child.scope_json)

    with TestClient(app) as client:
        child_response = client.post(f"/api/v1/jobs/{latest_child.id}/rerun")
        latest_children_response = client.get(f"/api/v1/jobs?parent_id={parent.id}&view=latest&limit=10")
        all_children_response = client.get(f"/api/v1/jobs?parent_id={parent.id}&view=all&limit=10")
        attempts_response = client.get(f"/api/v1/jobs/{latest_child.id}/attempts?limit=10")

    assert child_response.status_code == 200
    assert child_response.json()["job_type"] == JobType.sync_papers.value
    assert child_response.json()["parent_job_id"] == parent.id
    assert child_response.json()["attempt_count"] == 3
    assert child_response.json()["attempt_rank"] == 1

    assert latest_children_response.status_code == 200
    latest_children = latest_children_response.json()
    assert len(latest_children) == 2
    latest_rerun_child = next(item for item in latest_children if item["scope_json"] == latest_child_scope)
    assert latest_rerun_child["id"] == child_response.json()["id"]
    assert latest_rerun_child["attempt_count"] == 3
    assert latest_rerun_child["attempt_rank"] == 1

    assert all_children_response.status_code == 200
    all_children = all_children_response.json()
    assert len(all_children) == 4
    original_child = next(item for item in all_children if item["id"] == latest_child.id)
    assert original_child["attempt_count"] == 3
    assert original_child["attempt_rank"] == 2

    assert attempts_response.status_code == 200
    attempts = attempts_response.json()
    assert len(attempts) == 3
    assert [item["id"] for item in attempts[:2]] == [child_response.json()["id"], latest_child.id]
    assert [item["attempt_rank"] for item in attempts] == [1, 2, 3]
    assert all(item["attempt_count"] == 3 for item in attempts)


def test_rerun_api_rejects_batch_child_when_parent_is_stopping(db_env):
    with session_scope() as db:
        parent, children = _create_batch_with_child_states(
            db,
            JobType.sync_papers_batch,
            JobType.sync_papers,
            [(JobStatus.failed, False), (JobStatus.running, True)],
            parent_stop_requested=True,
        )
        failed_child = children[0]
        assert serialize_job(db, parent).batch_state == "stopping"

    with TestClient(app) as client:
        response = client.post(f"/api/v1/jobs/{failed_child.id}/rerun")

    assert response.status_code == 409
    assert response.json()["detail"] == "Child jobs from a stopping batch folder cannot be re-run"


def test_rerun_api_allows_finished_batch_child_when_parent_is_running(db_env):
    with session_scope() as db:
        parent, children = _create_batch_with_child_states(
            db,
            JobType.sync_papers_batch,
            JobType.sync_papers,
            [(JobStatus.failed, False), (JobStatus.running, False)],
        )
        failed_child = children[0]
        assert serialize_job(db, parent).batch_state == "running"

    with TestClient(app) as client:
        response = client.post(f"/api/v1/jobs/{failed_child.id}/rerun")

    assert response.status_code == 200
    assert response.json()["parent_job_id"] == parent.id
    assert response.json()["attempt_mode"] == JobAttemptMode.repair.value


def test_child_rerun_reactivates_cancelled_batch_until_child_finishes(db_env):
    with session_scope() as db:
        parent, children = _create_batch_with_child_states(
            db,
            JobType.sync_papers_batch,
            JobType.sync_papers,
            [
                (JobStatus.cancelled, False),
                (JobStatus.cancelled, False),
                (JobStatus.succeeded, False),
            ],
            parent_stop_requested=True,
        )
        cancelled_child = children[0]
        assert serialize_job(db, parent).batch_state == "cancelled"

    with TestClient(app) as client:
        response = client.post(f"/api/v1/jobs/{cancelled_child.id}/rerun")

    assert response.status_code == 200
    rerun_child_id = response.json()["id"]
    assert response.json()["attempt_mode"] == JobAttemptMode.repair.value

    with session_scope() as db:
        parent_after_rerun = db.get(Job, parent.id)
        rerun_child = db.get(Job, rerun_child_id)
        assert parent_after_rerun is not None
        assert rerun_child is not None
        assert serialize_job(db, parent_after_rerun).batch_state == "queued"

        rerun_child.status = JobStatus.running
        rerun_child.started_at = rerun_child.created_at
        rerun_child.locked_by = "worker:test"
        rerun_child.locked_at = rerun_child.created_at
        db.add(rerun_child)
        assert serialize_job(db, parent_after_rerun).batch_state == "running"

        rerun_child.status = JobStatus.succeeded
        rerun_child.finished_at = rerun_child.created_at
        rerun_child.locked_at = rerun_child.created_at
        db.add(rerun_child)
        assert serialize_job(db, parent_after_rerun).batch_state == "cancelled"


def test_list_jobs_root_only_excludes_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_papers_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )
        create_job(
            db,
            JobType.find_repos,
            ScopePayload(categories=["cs.CV"], month="2025-03"),
        )

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None

    import asyncio

    asyncio.run(process_job(parent.id))

    with session_scope() as db:
        children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())
        assert len(children) == 2
        child_ids = {child.id for child in children}

    with TestClient(app) as client:
        response = client.get("/api/v1/jobs?view=latest&limit=10&root_only=true")

    assert response.status_code == 200
    rows = response.json()
    ids = {row["id"] for row in rows}

    assert parent.id in ids
    assert child_ids.isdisjoint(ids)
    assert all(row["parent_job_id"] is None for row in rows)


def _scope_label(scope_json: dict[str, object]) -> str:
    return str(scope_json.get("day") or scope_json.get("month") or f"{scope_json.get('from')}..{scope_json.get('to')}")


def _batch_window_for_child_count(child_count: int) -> tuple[date, date]:
    end_dates = {
        2: date(2026, 5, 31),
        3: date(2026, 6, 30),
    }
    return date(2026, 4, 1), end_dates[child_count]


def _create_batch_with_child_states(
    db,
    parent_job_type: JobType,
    child_job_type: JobType,
    child_states: list[tuple[JobStatus, bool]],
    *,
    parent_stop_requested: bool = False,
):
    start_date, end_date = _batch_window_for_child_count(len(child_states))
    parent = create_job(
        db,
        parent_job_type,
        ScopePayload(categories=["cs.CV"], **{"from": start_date, "to": end_date}),
    )
    parent.status = JobStatus.succeeded
    if parent_stop_requested:
        parent.stop_requested_at = parent.created_at

    planned_scopes = planned_child_scope_jsons(parent.job_type, parent.scope_json)
    assert len(planned_scopes) == len(child_states)
    children = []
    for child_scope_json, (status, stop_requested) in zip(planned_scopes, child_states, strict=True):
        child = create_job(
            db,
            child_job_type,
            ScopePayload.model_validate(child_scope_json),
            parent_job_id=parent.id,
        )
        child.status = status
        if status == JobStatus.running:
            child.started_at = child.created_at
            child.locked_by = "worker:test"
            child.locked_at = child.created_at
        elif status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
            child.finished_at = child.created_at
        if stop_requested:
            child.stop_requested_at = child.created_at
        db.add(child)
        children.append(child)
    db.add(parent)
    return parent, children


@pytest.mark.parametrize(
    ("parent_job_type", "child_job_type"),
    [
        (JobType.sync_papers_batch, JobType.sync_papers),
        (JobType.find_repos_batch, JobType.find_repos),
        (JobType.refresh_metadata_batch, JobType.refresh_metadata),
    ],
)
def test_batch_root_rerun_creates_child_repairs_in_place_for_all_batch_types(db_env, parent_job_type, child_job_type):
    with session_scope() as db:
        parent, children = _create_batch_with_child_states(
            db,
            parent_job_type,
            child_job_type,
            [
                (JobStatus.failed, False),
                (JobStatus.cancelled, False),
                (JobStatus.succeeded, False),
            ],
        )
        original_child_by_dedupe = {child.dedupe_key: child for child in children}

        rerun_parent = rerun_job(db, parent.id)
        assert rerun_parent.id == parent.id
        assert serialize_job(db, rerun_parent).batch_state == "queued"

        latest_rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")
        all_rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="all")

    assert len(latest_rows) == 3
    assert len(all_rows) == 5
    assert sum(1 for row in latest_rows if row.status == JobStatus.pending and row.attempt_mode == JobAttemptMode.repair) == 2
    assert sum(1 for row in latest_rows if row.status == JobStatus.succeeded and row.attempt_mode == JobAttemptMode.fresh) == 1

    for row in latest_rows:
        original_child = original_child_by_dedupe[row.dedupe_key]
        if original_child.status in {JobStatus.failed, JobStatus.cancelled}:
            assert row.id != original_child.id
            assert row.attempt_series_key == original_child.attempt_series_key
        else:
            assert row.id == original_child.id


@pytest.mark.parametrize(
    ("parent_job_type", "child_job_type"),
    [
        (JobType.sync_papers_batch, JobType.sync_papers),
        (JobType.find_repos_batch, JobType.find_repos),
        (JobType.refresh_metadata_batch, JobType.refresh_metadata),
    ],
)
def test_batch_root_rerun_creates_missing_child_scopes_in_place(db_env, parent_job_type, child_job_type):
    with session_scope() as db:
        parent = create_job(
            db,
            parent_job_type,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 4, 1), "to": date(2026, 6, 30)}),
        )
        parent.status = JobStatus.succeeded
        parent.stop_requested_at = parent.created_at
        planned_scopes = planned_child_scope_jsons(parent.job_type, parent.scope_json)
        existing_child = create_job(
            db,
            child_job_type,
            ScopePayload.model_validate(planned_scopes[0]),
            parent_job_id=parent.id,
        )
        existing_child.status = JobStatus.succeeded
        existing_child.finished_at = existing_child.created_at
        db.add_all([parent, existing_child])

        rerun_parent = rerun_job(db, parent.id)
        latest_rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")

    assert rerun_parent.id == parent.id
    assert len(latest_rows) == 3
    assert sum(1 for row in latest_rows if row.status == JobStatus.pending and row.attempt_mode == JobAttemptMode.repair) == 2
    assert sum(1 for row in latest_rows if row.status == JobStatus.succeeded and row.id == existing_child.id) == 1


@pytest.mark.parametrize(
    ("parent_job_type", "child_job_type"),
    [
        (JobType.sync_papers_batch, JobType.sync_papers),
        (JobType.find_repos_batch, JobType.find_repos),
        (JobType.refresh_metadata_batch, JobType.refresh_metadata),
    ],
)
@pytest.mark.parametrize(
    ("child_states", "expected_batch_state"),
    [
        ([(JobStatus.running, True), (JobStatus.failed, False)], "stopping"),
        ([(JobStatus.running, False), (JobStatus.failed, False)], "running"),
        ([(JobStatus.pending, False), (JobStatus.failed, False)], "queued"),
        ([(JobStatus.failed, False), (JobStatus.cancelled, False)], "failed"),
        ([(JobStatus.cancelled, False), (JobStatus.succeeded, False)], "cancelled"),
        ([(JobStatus.succeeded, False), (JobStatus.succeeded, False)], "succeeded"),
    ],
)
def test_batch_state_is_child_centric_for_all_batch_types(
    db_env,
    parent_job_type,
    child_job_type,
    child_states,
    expected_batch_state,
):
    with session_scope() as db:
        parent, _ = _create_batch_with_child_states(
            db,
            parent_job_type,
            child_job_type,
            child_states,
            parent_stop_requested=True,
        )
        serialized = serialize_job(db, parent)

    assert serialized.batch_state == expected_batch_state


@pytest.mark.parametrize(
    ("parent_job_type", "child_job_type"),
    [
        (JobType.sync_papers_batch, JobType.sync_papers),
        (JobType.find_repos_batch, JobType.find_repos),
        (JobType.refresh_metadata_batch, JobType.refresh_metadata),
    ],
)
def test_batch_child_jobs_follow_descending_scope_time_for_all_batch_types(db_env, parent_job_type, child_job_type):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        parent = create_job(
            db,
            parent_job_type,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 4, 1), "to": date(2026, 5, 31)}),
        )
        planned_scopes = planned_child_scope_jsons(parent.job_type, parent.scope_json)
        for child_scope_json in reversed(planned_scopes):
            child = create_job(
                db,
                child_job_type,
                ScopePayload.model_validate(child_scope_json),
                parent_job_id=parent.id,
            )
            child.created_at = same_created_at
            db.add(child)
        parent.status = JobStatus.succeeded
        db.add(parent)

    with session_scope() as db:
        children = list_child_jobs(db, parent.id)
        rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")

    expected_labels = ["2026-05", "2026-04"]
    assert [_scope_label(child.scope_json) for child in children] == expected_labels
    assert [_scope_label(row.scope_json) for row in rows] == expected_labels


def test_batch_child_rerun_stays_in_original_scope_position(db_env):
    with session_scope() as db:
        parent = create_job(
            db,
            JobType.sync_papers_batch,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 3, 1), "to": date(2026, 4, 30)}),
        )
        planned_scopes = planned_child_scope_jsons(parent.job_type, parent.scope_json)
        march = create_job(
            db,
            JobType.sync_papers,
            ScopePayload.model_validate(planned_scopes[0]),
            parent_job_id=parent.id,
        )
        april = create_job(
            db,
            JobType.sync_papers,
            ScopePayload.model_validate(planned_scopes[1]),
            parent_job_id=parent.id,
        )
        march.status = JobStatus.failed
        march.finished_at = march.created_at
        april.status = JobStatus.succeeded
        april.finished_at = april.created_at
        parent.status = JobStatus.succeeded
        db.add_all([parent, march, april])

    with session_scope() as db:
        rerun = rerun_job(db, march.id)
        rerun.created_at = march.created_at + timedelta(minutes=10)
        db.add(rerun)

    with session_scope() as db:
        latest_rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="latest")
        all_rows = list_jobs_read(db, parent_job_id=parent.id, limit=10, view="all")

    assert [_scope_label(row.scope_json) for row in latest_rows] == ["2026-04", "2026-03"]
    assert latest_rows[1].id == rerun.id
    assert [_scope_label(row.scope_json) for row in all_rows] == ["2026-04", "2026-03", "2026-03"]
    assert [row.id for row in all_rows[1:]] == [rerun.id, march.id]
