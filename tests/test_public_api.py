from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from papertorepo.api.app import app, create_app
from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import session_scope
from papertorepo.db.models import Job, JobAttemptMode, JobStatus, Paper, PaperRepoState, RepoStableStatus, utc_now
from papertorepo.jobs.queue import claim_next_job, create_sync_arxiv_job, process_job
from papertorepo.api.schemas import ScopePayload


def test_public_papers_returns_summary_rows(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15312",
                abs_url="https://arxiv.org/abs/2604.15312",
                title="Bidirectional Cross-Modal Prompting",
                abstract="Long abstract body",
                published_at=date(2026, 4, 16),
                updated_at=date(2026, 4, 16),
                authors_json=["Alice", "Bob"],
                categories_json=["cs.CV"],
                comment="CVPR 2026",
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15312",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/example/project",
                repo_urls_json=["https://github.com/example/project"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
                last_attempt_error=None,
            )
        )

    with TestClient(app) as client:
        response = client.get("/api/v1/papers?limit=10")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    row = rows[0]
    assert row["arxiv_id"] == "2604.15312"
    assert row["title"] == "Bidirectional Cross-Modal Prompting"
    assert row["primary_repo_url"] == "https://github.com/example/project"
    assert row["link_status"] == "found"
    assert "abstract" not in row
    assert "comment" not in row
    assert "repo_urls" not in row


def test_public_papers_supports_offset_paging(db_env):
    with session_scope() as db:
        for arxiv_id, title, published_at in [
            ("2604.20003", "Paper C", date(2026, 4, 18)),
            ("2604.20002", "Paper B", date(2026, 4, 18)),
            ("2604.20001", "Paper A", date(2026, 4, 17)),
        ]:
            db.add(
                Paper(
                    arxiv_id=arxiv_id,
                    abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                    title=title,
                    abstract=f"Abstract for {title}",
                    published_at=published_at,
                    updated_at=published_at,
                    authors_json=["Alice"],
                    categories_json=["cs.CV"],
                    comment=None,
                    primary_category="cs.CV",
                    source_first_seen_at=utc_now(),
                    source_last_seen_at=utc_now(),
                )
            )

    with TestClient(app) as client:
        first_page = client.get("/api/v1/papers?limit=2&offset=0")
        second_page = client.get("/api/v1/papers?limit=2&offset=2")

    assert first_page.status_code == 200
    assert second_page.status_code == 200

    first_ids = [row["arxiv_id"] for row in first_page.json()]
    second_ids = [row["arxiv_id"] for row in second_page.json()]

    assert first_ids == ["2604.20003", "2604.20002"]
    assert second_ids == ["2604.20001"]


def test_public_paper_detail_returns_full_payload(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15311",
                abs_url="https://arxiv.org/abs/2604.15311",
                title="LeapAlign",
                abstract="Detail abstract",
                published_at=date(2026, 4, 16),
                updated_at=date(2026, 4, 16),
                authors_json=["Carol"],
                categories_json=["cs.CV"],
                comment="Accepted by CVPR 2026",
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15311",
                stable_status=RepoStableStatus.not_found,
                primary_repo_url=None,
                repo_urls_json=[],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
                last_attempt_error=None,
            )
        )

    with TestClient(app) as client:
        response = client.get("/api/v1/papers/2604.15311")
        not_found_response = client.get("/api/v1/papers/missing")

    assert response.status_code == 200
    payload = response.json()
    assert payload["arxiv_id"] == "2604.15311"
    assert payload["abstract"] == "Detail abstract"
    assert payload["comment"] == "Accepted by CVPR 2026"
    assert payload["repo_urls"] == []

    assert not_found_response.status_code == 404
    assert not_found_response.json()["detail"] == "Paper not found"


def test_health_reports_serial_queue_runtime_metadata(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("GITHUB_MIN_INTERVAL", "0.5")
    clear_settings_cache()

    with TestClient(create_app()) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["queue_mode"] == "serial"
    assert payload["github_auth_configured"] is False
    assert payload["effective_github_min_interval_seconds"] == 60.0
    assert payload["step_providers"]["sync_arxiv"] == ["arxiv_listing", "arxiv_export_api"]
    assert payload["step_providers"]["sync_links"] == ["arxiv_abs", "huggingface", "alphaxiv"]
    assert payload["step_providers"]["enrich"] == ["github_api"]

    clear_settings_cache()


def test_public_dashboard_returns_job_queue_summary(db_env):
    from papertorepo.jobs.queue import create_job
    from papertorepo.db.models import JobStatus, JobType
    from papertorepo.api.schemas import ScopePayload

    with session_scope() as db:
        queued_job = create_job(db, JobType.enrich, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        running_job = create_job(db, JobType.sync_links, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        running_job.status = JobStatus.running
        running_job.started_at = utc_now()
        running_job.locked_at = utc_now()
        running_job.locked_by = "worker-a"
        db.add(running_job)

    with TestClient(app) as client:
        response = client.get("/api/v1/dashboard")

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_queue_summary"]["state"] == "active"
    assert payload["job_queue_summary"]["running"] == 1
    assert payload["job_queue_summary"]["pending"] == 1
    assert payload["job_queue_summary"]["stopping"] == 0
    assert payload["job_queue_summary"]["current_job"]["id"] == running_job.id
    assert payload["job_queue_summary"]["current_job"]["job_type"] == "sync_links"
    assert payload["job_queue_summary"]["next_job"]["id"] == queued_job.id
    assert payload["job_queue_summary"]["next_job"]["job_type"] == "enrich"


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/dashboard?categories=cs.CV&month=abcd",
        "/api/v1/papers?categories=cs.CV&month=abcd",
        "/api/v1/repos?categories=cs.CV&month=abcd",
    ],
)
def test_public_scope_endpoints_return_422_for_invalid_month_query(db_env, path):
    with TestClient(create_app()) as client:
        response = client.get(path)

    assert response.status_code == 422
    assert response.json()["detail"] == "month must be YYYY-MM"


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/dashboard?categories=computer%20vision&month=2026-04",
        "/api/v1/papers?categories=computer%20vision&month=2026-04",
        "/api/v1/repos?categories=computer%20vision&month=2026-04",
    ],
)
def test_public_scope_endpoints_return_422_for_invalid_categories_query(db_env, path):
    with TestClient(create_app()) as client:
        response = client.get(path)

    assert response.status_code == 422
    assert response.json()["detail"] == "Enter categories as comma-separated arXiv fields, e.g. cs.CV, cs.LG."


def test_sync_launch_endpoint_blocks_identical_active_scope(db_env):
    payload = {
        "categories": ["cs.CV"],
        "month": "2026-04",
    }

    with TestClient(app) as client:
        first = client.post("/api/v1/jobs/sync-arxiv", json=payload)
        second = client.post("/api/v1/jobs/sync-arxiv", json=payload)

    assert first.status_code == 200
    assert second.status_code == 409
    first_payload = first.json()

    assert first_payload["disposition"] == "created"
    assert first_payload["job"]["attempt_mode"] == JobAttemptMode.fresh.value
    assert "already active" in second.json()["detail"]

    with session_scope() as db:
        jobs = list(db.query(Job).all())

    assert len(jobs) == 1


@pytest.mark.anyio
async def test_sync_launch_endpoint_creates_fresh_batch_after_previous_success_without_reuse(db_env):
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
        first_children = list(db.query(Job).filter(Job.parent_job_id == first_batch.id).all())
        for child in first_children:
            child.status = JobStatus.succeeded
            child.finished_at = utc_now()
            db.add(child)

    payload = {
        "categories": ["cs.CV"],
        "from": "2025-03-15",
        "to": "2025-04-10",
    }
    with TestClient(app) as client:
        response = client.post("/api/v1/jobs/sync-arxiv", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["disposition"] == "created"
    assert body["job"]["attempt_mode"] == JobAttemptMode.fresh.value
    assert body["job"]["id"] != first_batch.id

    second_batch_id = body["job"]["id"]

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")
    assert claimed is not None
    assert claimed.id == second_batch_id
    await process_job(second_batch_id)

    with session_scope() as db:
        second_children = list(db.query(Job).filter(Job.parent_job_id == second_batch_id).all())

    assert len(second_children) == 2
    assert all(child.status == JobStatus.pending for child in second_children)
    assert all(child.attempt_mode == JobAttemptMode.fresh for child in second_children)
    assert all(child.stats_json.get("reused") is not True for child in second_children)


def test_same_scope_fresh_runs_remain_independent_in_attempt_history(db_env):
    with session_scope() as db:
        first = create_sync_arxiv_job(db, ScopePayload(categories=["cs.CV"], month="2026-04"))
        second = create_sync_arxiv_job(db, ScopePayload(categories=["cs.CV"], month="2026-04"))
        first.status = JobStatus.succeeded
        first.finished_at = utc_now()
        second.status = JobStatus.succeeded
        second.finished_at = utc_now()
        db.add_all([first, second])

    with TestClient(app) as client:
        first_attempts = client.get(f"/api/v1/jobs/{first.id}/attempts?limit=10")
        second_attempts = client.get(f"/api/v1/jobs/{second.id}/attempts?limit=10")
        latest_jobs = client.get("/api/v1/jobs?view=latest&root_only=true&limit=20")

    assert first_attempts.status_code == 200
    assert second_attempts.status_code == 200
    assert [row["id"] for row in first_attempts.json()] == [first.id]
    assert [row["id"] for row in second_attempts.json()] == [second.id]

    latest_ids = [row["id"] for row in latest_jobs.json()]
    assert first.id in latest_ids
    assert second.id in latest_ids
