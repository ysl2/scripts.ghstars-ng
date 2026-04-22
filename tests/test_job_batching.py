from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient
import pytest

from papertorepo.api.app import app
from papertorepo.db.session import session_scope
from papertorepo.jobs.queue import (
    claim_next_job,
    create_job,
    create_sync_arxiv_job,
    create_sync_job,
    list_child_jobs,
    process_job,
    rerun_job,
    serialize_job,
)
from papertorepo.jobs.batches import planned_child_scope_jsons
from papertorepo.db.models import Job, JobAttemptMode, JobStatus, JobType
from papertorepo.core.scope import expand_arxiv_child_scope_jsons
from papertorepo.api.schemas import ScopePayload


def test_create_sync_arxiv_job_uses_batch_for_multi_month_scope(db_env):
    with session_scope() as db:
        job = create_sync_arxiv_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )

    assert job.job_type == JobType.sync_arxiv_batch
    assert job.parent_job_id is None


def test_expand_arxiv_child_scope_jsons_inherit_force_flag():
    child_scopes = expand_arxiv_child_scope_jsons(
        {
            "categories": ["cs.CV"],
            "from": "2025-03-15",
            "to": "2025-04-10",
            "force": True,
            "max_results": None,
            "day": None,
            "month": None,
            "export_mode": None,
            "paper_ids": [],
            "output_name": None,
        }
    )

    assert len(child_scopes) == 2
    assert all(child_scope["force"] is True for child_scope in child_scopes)


def test_planned_child_scope_jsons_keep_partial_edges_contiguous_for_sync_links():
    child_scopes = planned_child_scope_jsons(
        JobType.sync_links,
        {
            "categories": ["cs.CV"],
            "from": "2025-03-15",
            "to": "2025-06-10",
            "force": True,
            "max_results": None,
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
            "max_results": None,
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
            "max_results": None,
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
            "max_results": None,
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
            "max_results": None,
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
            JobType.sync_links,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 6, 10)},
            ),
        )

    assert job.job_type == JobType.sync_links_batch
    assert job.parent_job_id is None


def test_create_sync_job_canonicalizes_single_day_range_to_day(db_env):
    with session_scope() as db:
        job = create_sync_job(
            db,
            JobType.sync_links,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 4, 12), "to": date(2025, 4, 12)},
            ),
        )

    assert job.job_type == JobType.sync_links
    assert job.scope_json["day"] == "2025-04-12"
    assert job.scope_json["month"] is None
    assert job.scope_json["from"] is None
    assert job.scope_json["to"] is None


def test_create_sync_job_canonicalizes_full_month_range_to_month(db_env):
    with session_scope() as db:
        job = create_sync_job(
            db,
            JobType.sync_links,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 4, 1), "to": date(2025, 4, 30)},
            ),
        )

    assert job.job_type == JobType.sync_links
    assert job.scope_json["day"] is None
    assert job.scope_json["month"] == "2025-04"
    assert job.scope_json["from"] is None
    assert job.scope_json["to"] is None


def test_create_sync_arxiv_job_keeps_single_partial_month_range_as_direct_job(db_env):
    with session_scope() as db:
        job = create_sync_arxiv_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 3, 20)},
            ),
        )

    assert job.job_type == JobType.sync_arxiv
    assert job.scope_json["day"] is None
    assert job.scope_json["month"] is None
    assert job.scope_json["from"] == "2025-03-15"
    assert job.scope_json["to"] == "2025-03-20"


@pytest.mark.anyio
async def test_process_batch_job_creates_archive_month_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_arxiv_job(
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
    assert refreshed_parent.job_type == JobType.sync_arxiv_batch
    assert len(children) == 2
    assert [child.scope_json["month"] for child in children] == ["2025-03", "2025-04"]
    assert [child.scope_json["from"] for child in children] == [None, None]
    assert [child.scope_json["to"] for child in children] == [None, None]
    assert all(child.job_type == JobType.sync_arxiv for child in children)


@pytest.mark.anyio
async def test_batch_summary_uses_latest_child_attempt_per_scope(db_env):
    with session_scope() as db:
        parent = create_sync_arxiv_job(
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
        first_batch = create_sync_arxiv_job(db, scope)

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
        second_batch = create_sync_arxiv_job(db, scope)

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
        parent = create_sync_arxiv_job(
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
    assert batch_response.json()["job_type"] == JobType.sync_arxiv_batch.value
    assert batch_response.json()["parent_job_id"] is None
    assert batch_response.json()["attempt_mode"] == JobAttemptMode.repair.value
    assert child_response.status_code == 409

    rerun_batch_id = batch_response.json()["id"]

    import asyncio

    asyncio.run(process_job(rerun_batch_id))

    with session_scope() as db:
        rerun_batch = db.get(Job, rerun_batch_id)
        assert rerun_batch is not None
        refreshed_children = list(db.query(Job).filter(Job.parent_job_id == rerun_batch_id).order_by(Job.created_at.asc()).all())

    assert rerun_batch.status == JobStatus.succeeded
    assert len(refreshed_children) == 2
    assert sum(1 for item in refreshed_children if item.scope_json == child_scope and item.status == JobStatus.pending) == 1
    reused_sibling = next(item for item in refreshed_children if item.scope_json in sibling_scopes)
    assert reused_sibling.status == JobStatus.succeeded
    assert reused_sibling.attempt_mode == JobAttemptMode.repair
    assert reused_sibling.stats_json["reused"] is True
    assert reused_sibling.stats_json["reused_from_job_id"] in {item.id for item in children[1:]}


def test_rerun_api_supports_latest_batch_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_arxiv_job(
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
    rerun_batch_id = batch_response.json()["id"]
    asyncio.run(process_job(rerun_batch_id))

    with session_scope() as db:
        latest_batch_children = list(db.query(Job).filter(Job.parent_job_id == rerun_batch_id).order_by(Job.created_at.asc()).all())
        latest_child = next(item for item in latest_batch_children if item.status == JobStatus.pending)
        latest_child.status = JobStatus.failed
        latest_child.finished_at = latest_child.created_at
        latest_child_scope = dict(latest_child.scope_json)

    with TestClient(app) as client:
        child_response = client.post(f"/api/v1/jobs/{latest_child.id}/rerun")
        latest_children_response = client.get(f"/api/v1/jobs?parent_id={rerun_batch_id}&view=latest&limit=10")
        all_children_response = client.get(f"/api/v1/jobs?parent_id={rerun_batch_id}&view=all&limit=10")
        attempts_response = client.get(f"/api/v1/jobs/{latest_child.id}/attempts?limit=10")

    assert child_response.status_code == 200
    assert child_response.json()["job_type"] == JobType.sync_arxiv.value
    assert child_response.json()["parent_job_id"] == rerun_batch_id

    assert latest_children_response.status_code == 200
    latest_children = latest_children_response.json()
    assert len(latest_children) == 2
    latest_rerun_child = next(item for item in latest_children if item["scope_json"] == latest_child_scope)
    assert latest_rerun_child["id"] == child_response.json()["id"]
    assert latest_rerun_child["attempt_count"] == 3
    assert latest_rerun_child["attempt_rank"] == 1

    assert all_children_response.status_code == 200
    all_children = all_children_response.json()
    assert len(all_children) == 3
    original_child = next(item for item in all_children if item["id"] == latest_child.id)
    assert original_child["attempt_count"] == 3
    assert original_child["attempt_rank"] == 2

    assert attempts_response.status_code == 200
    attempts = attempts_response.json()
    assert len(attempts) == 3
    assert [item["id"] for item in attempts[:2]] == [child_response.json()["id"], latest_child.id]
    assert [item["attempt_rank"] for item in attempts] == [1, 2, 3]
    assert all(item["attempt_count"] == 3 for item in attempts)


def test_list_jobs_root_only_excludes_child_jobs(db_env):
    with session_scope() as db:
        parent = create_sync_arxiv_job(
            db,
            ScopePayload(
                categories=["cs.CV"],
                **{"from": date(2025, 3, 15), "to": date(2025, 4, 10)},
            ),
        )
        create_job(
            db,
            JobType.sync_links,
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


def test_batch_child_jobs_default_order_prefers_newer_scope_when_created_at_matches(db_env):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        parent = create_job(
            db,
            JobType.sync_arxiv_batch,
            ScopePayload(categories=["cs.CV"], **{"from": date(2026, 4, 1), "to": date(2026, 5, 31)}),
        )
        april = create_job(
            db,
            JobType.sync_arxiv,
            ScopePayload(categories=["cs.CV"], month="2026-04"),
            parent_job_id=parent.id,
        )
        may = create_job(
            db,
            JobType.sync_arxiv,
            ScopePayload(categories=["cs.CV"], month="2026-05"),
            parent_job_id=parent.id,
        )
        parent.status = JobStatus.succeeded
        april.created_at = same_created_at
        may.created_at = same_created_at
        db.add_all([parent, april, may])

    with session_scope() as db:
        children = list_child_jobs(db, parent.id)

    assert [child.scope_json["month"] for child in children] == ["2026-05", "2026-04"]
