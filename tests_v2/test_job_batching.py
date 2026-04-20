from __future__ import annotations

from datetime import date

from fastapi.testclient import TestClient
import pytest

from src.ghstarsv2.app import app
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.jobs import claim_next_job, create_job, create_sync_arxiv_job, process_job, rerun_job, serialize_job
from src.ghstarsv2.models import Job, JobStatus, JobType
from src.ghstarsv2.scope import expand_arxiv_child_scope_jsons
from src.ghstarsv2.schemas import ScopePayload


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


def test_rerun_api_supports_batch_parent_and_child_jobs(db_env):
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
        child.finished_at = child.locked_at
        child_scope = dict(child.scope_json)
        sibling_scopes = [dict(item.scope_json) for item in children[1:]]

    with TestClient(app) as client:
        batch_response = client.post(f"/api/v1/jobs/{parent.id}/rerun")
        child_response = client.post(f"/api/v1/jobs/{child.id}/rerun")
        latest_children_response = client.get(f"/api/v1/jobs?parent_id={parent.id}&view=latest&limit=10")
        all_children_response = client.get(f"/api/v1/jobs?parent_id={parent.id}&view=all&limit=10")
        attempts_response = client.get(f"/api/v1/jobs/{child.id}/attempts?limit=10")

    assert batch_response.status_code == 200
    assert batch_response.json()["job_type"] == JobType.sync_arxiv_batch.value
    assert batch_response.json()["parent_job_id"] is None

    assert child_response.status_code == 200
    assert child_response.json()["job_type"] == JobType.sync_arxiv.value
    assert child_response.json()["parent_job_id"] == parent.id

    assert latest_children_response.status_code == 200
    latest_children = latest_children_response.json()
    assert len(latest_children) == 2
    latest_rerun_child = next(item for item in latest_children if item["scope_json"] == child_scope)
    assert latest_rerun_child["id"] == child_response.json()["id"]
    assert latest_rerun_child["attempt_count"] == 2
    assert latest_rerun_child["attempt_rank"] == 1

    assert all_children_response.status_code == 200
    all_children = all_children_response.json()
    assert len(all_children) == 3
    original_child = next(item for item in all_children if item["id"] == child.id)
    assert original_child["attempt_count"] == 2
    assert original_child["attempt_rank"] == 2

    assert attempts_response.status_code == 200
    attempts = attempts_response.json()
    assert len(attempts) == 2
    assert [item["id"] for item in attempts] == [child_response.json()["id"], child.id]
    assert [item["attempt_rank"] for item in attempts] == [1, 2]
    assert all(item["attempt_count"] == 2 for item in attempts)

    with session_scope() as db:
        refreshed_children = list(db.query(Job).filter(Job.parent_job_id == parent.id).order_by(Job.created_at.asc()).all())

    assert len(refreshed_children) == 3
    assert sum(1 for item in refreshed_children if item.scope_json == child_scope) == 2
    assert sum(1 for item in refreshed_children if item.scope_json in sibling_scopes) == len(sibling_scopes)

    rerun_child = next(item for item in refreshed_children if item.id == child_response.json()["id"])
    assert rerun_child.scope_json == child_scope


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
