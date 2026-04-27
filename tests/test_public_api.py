from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from papertorepo.api.app import app, create_app
from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import session_scope
from papertorepo.db.models import (
    GitHubRepo,
    Job,
    JobAttemptMode,
    JobItemResumeProgress,
    JobStatus,
    JobType,
    Paper,
    PaperRepoState,
    RepoStableStatus,
    utc_now,
)
from papertorepo.jobs.queue import claim_next_job, create_job, create_sync_papers_job, process_job
from papertorepo.api.schemas import ScopePayload


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def test_public_papers_returns_summary_rows(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15312",
                abs_url="https://arxiv.org/abs/2604.15312",
                title="Bidirectional Cross-Modal Prompting",
                abstract="Long abstract body",
                published_at=at_utc_midnight(date(2026, 4, 16)),
                updated_at=at_utc_midnight(date(2026, 4, 16)),
                authors_json=["Alice", "Bob"],
                categories_json=["cs.CV"],
                comment="CVPR 2026",
                journal_ref="CVPR 2026",
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15312",
                stable_status=RepoStableStatus.found,
                primary_github_url="https://github.com/example/project",
                github_urls_json=["https://github.com/example/project"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
                last_attempt_error=None,
            )
        )
        db.add(
            GitHubRepo(
                github_url="https://github.com/example/project",
                github_id=123,
                name_with_owner="example/project",
                stargazers_count=321,
                size_kb=1536,
                primary_language="TypeScript",
                created_at="2020-01-01T00:00:00Z",
                updated_at="2026-04-19T00:00:00Z",
                description="example project",
                homepage=None,
                topic=None,
                license_spdx_id=None,
                license_name=None,
                is_archived=False,
                pushed_at="2026-04-18T00:00:00Z",
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
    assert row["primary_github_url"] == "https://github.com/example/project"
    assert row["primary_github_stargazers_count"] == 321
    assert row["primary_github_language"] == "TypeScript"
    assert row["primary_github_size_kb"] == 1536
    assert row["primary_github_created_at"] == "2020-01-01T00:00:00Z"
    assert row["primary_github_pushed_at"] == "2026-04-18T00:00:00Z"
    assert row["primary_github_updated_at"] == "2026-04-19T00:00:00Z"
    assert row["primary_github_description"] == "example project"
    assert row["link_status"] == "found"
    assert "abstract" not in row
    assert row["comment"] == "CVPR 2026"
    assert row["journal_ref"] == "CVPR 2026"
    assert "github_urls" not in row


def test_public_papers_returns_null_primary_github_metadata_without_metadata(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15313",
                abs_url="https://arxiv.org/abs/2604.15313",
                title="Missing repo metadata",
                abstract="Long abstract body",
                published_at=at_utc_midnight(date(2026, 4, 16)),
                updated_at=at_utc_midnight(date(2026, 4, 16)),
                authors_json=["Alice"],
                categories_json=["cs.CV"],
                comment=None,
                journal_ref="NeurIPS 2026",
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15313",
                stable_status=RepoStableStatus.found,
                primary_github_url="https://github.com/missing/project",
                github_urls_json=["https://github.com/missing/project"],
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
    row = response.json()[0]
    assert row["primary_github_url"] == "https://github.com/missing/project"
    assert row["primary_github_stargazers_count"] is None
    assert row["primary_github_language"] is None
    assert row["primary_github_size_kb"] is None
    assert row["primary_github_created_at"] is None
    assert row["primary_github_pushed_at"] is None
    assert row["primary_github_updated_at"] is None
    assert row["primary_github_description"] is None
    assert row["journal_ref"] == "NeurIPS 2026"


def test_public_repos_returns_common_github_metadata_only(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15314",
                abs_url="https://arxiv.org/abs/2604.15314",
                title="Repo metadata paper",
                abstract="Long abstract body",
                published_at=at_utc_midnight(date(2026, 4, 16)),
                updated_at=at_utc_midnight(date(2026, 4, 16)),
                authors_json=["Alice"],
                categories_json=["cs.CV"],
                comment=None,
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15314",
                stable_status=RepoStableStatus.found,
                primary_github_url="https://github.com/example/project",
                github_urls_json=["https://github.com/example/project"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
                last_attempt_error=None,
            )
        )
        db.add(
            GitHubRepo(
                github_url="https://github.com/example/project",
                github_id=123,
                node_id="R_123",
                name_with_owner="example/project",
                stargazers_count=321,
                parent_github_url="https://github.com/example/parent",
            )
        )

    with TestClient(app) as client:
        response = client.get("/api/v1/repos?categories=cs.CV&month=2026-04&limit=10")

    assert response.status_code == 200
    row = response.json()[0]
    assert row["github_url"] == "https://github.com/example/project"
    assert row["parent_github_url"] == "https://github.com/example/parent"
    assert "source_github_url" not in row


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
                    published_at=at_utc_midnight(published_at),
                    updated_at=at_utc_midnight(published_at),
                    authors_json=["Alice"],
                    categories_json=["cs.CV"],
                    comment=None,
                    primary_category="cs.CV",
                    source_first_seen_at=utc_now(),
                    source_last_seen_at=utc_now(),
                )
            )
        db.add(
            PaperRepoState(
                arxiv_id="2604.20001",
                stable_status=RepoStableStatus.found,
                primary_github_url="https://github.com/example/page-two",
                github_urls_json=["https://github.com/example/page-two"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
                last_attempt_error=None,
            )
        )
        db.add(
            GitHubRepo(
                github_url="https://github.com/example/page-two",
                stargazers_count=42,
                size_kb=2048,
                primary_language="Python",
                created_at="2021-02-03T00:00:00Z",
                pushed_at="2026-04-20T00:00:00Z",
                updated_at="2026-04-21T00:00:00Z",
                description="second page repo",
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
    assert second_page.json()[0]["primary_github_stargazers_count"] == 42
    assert second_page.json()[0]["primary_github_language"] == "Python"
    assert second_page.json()[0]["primary_github_size_kb"] == 2048
    assert second_page.json()[0]["primary_github_created_at"] == "2021-02-03T00:00:00Z"
    assert second_page.json()[0]["primary_github_pushed_at"] == "2026-04-20T00:00:00Z"
    assert second_page.json()[0]["primary_github_updated_at"] == "2026-04-21T00:00:00Z"
    assert second_page.json()[0]["primary_github_description"] == "second page repo"


def test_public_paper_detail_returns_full_payload(db_env):
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id="2604.15311",
                abs_url="https://arxiv.org/abs/2604.15311",
                title="LeapAlign",
                abstract="Detail abstract",
                published_at=at_utc_midnight(date(2026, 4, 16)),
                updated_at=at_utc_midnight(date(2026, 4, 16)),
                authors_json=["Carol"],
                categories_json=["cs.CV"],
                comment="Accepted by CVPR 2026",
                doi="10.48550/arXiv.2604.15311",
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.15311",
                stable_status=RepoStableStatus.not_found,
                primary_github_url=None,
                github_urls_json=[],
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
    assert payload["doi"] == "10.48550/arXiv.2604.15311"
    assert payload["github_urls"] == []

    assert not_found_response.status_code == 404
    assert not_found_response.json()["detail"] == "Paper not found"


def test_health_reports_serial_queue_runtime_metadata(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("REFRESH_METADATA_GITHUB_MIN_INTERVAL", "0.5")
    clear_settings_cache()

    with TestClient(create_app()) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["queue_mode"] == "serial"
    assert payload["github_auth_configured"] is False
    assert payload["effective_github_min_interval_seconds"] == 60.0
    assert payload["step_providers"]["sync_papers"] == [
        "arxiv_listing",
        "arxiv_catchup",
        "arxiv_submitted_day",
        "arxiv_id_list",
    ]
    assert payload["step_providers"]["find_repos"] == [
        "paper_comment",
        "paper_abstract",
        "alphaxiv_api",
        "alphaxiv_html",
        "huggingface_api",
    ]
    assert payload["step_providers"]["refresh_metadata"] == ["github_api"]

    clear_settings_cache()


def _create_app_with_test_frontend_dist(dist_dir, monkeypatch):
    dist_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("<!doctype html><div id=\"root\"></div>", encoding="utf-8")
    monkeypatch.setenv("FRONTEND_DIST_DIR", str(dist_dir))
    clear_settings_cache()
    return create_app()


def test_frontend_static_missing_asset_does_not_fallback_to_index(db_env, monkeypatch):
    test_app = _create_app_with_test_frontend_dist(db_env / "frontend-dist", monkeypatch)
    with TestClient(test_app) as client:
        response = client.get("/assets/missing-build-chunk.js")

    assert response.status_code == 404


def test_frontend_spa_route_still_falls_back_to_index(db_env, monkeypatch):
    test_app = _create_app_with_test_frontend_dist(db_env / "frontend-dist", monkeypatch)
    with TestClient(test_app) as client:
        response = client.get("/jobs")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert response.headers["cache-control"] == "no-cache"
    assert '<div id="root"></div>' in response.text


def test_public_dashboard_returns_job_queue_summary(db_env):
    from papertorepo.jobs.queue import create_job
    from papertorepo.db.models import JobStatus, JobType
    from papertorepo.api.schemas import ScopePayload

    with session_scope() as db:
        queued_job = create_job(db, JobType.refresh_metadata, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
        running_job = create_job(db, JobType.find_repos, ScopePayload(categories=["cs.CV"], day=date(2026, 4, 21)))
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
    assert payload["job_queue_summary"]["current_job"]["job_type"] == "find_repos"
    assert payload["job_queue_summary"]["next_job"]["id"] == queued_job.id
    assert payload["job_queue_summary"]["next_job"]["job_type"] == "refresh_metadata"


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
        first = client.post("/api/v1/jobs/sync-papers", json=payload)
        second = client.post("/api/v1/jobs/sync-papers", json=payload)

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
        first_batch = create_sync_papers_job(db, scope)

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
        response = client.post("/api/v1/jobs/sync-papers", json=payload)

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
        first = create_sync_papers_job(db, ScopePayload(categories=["cs.CV"], month="2026-04"))
        second = create_sync_papers_job(db, ScopePayload(categories=["cs.CV"], month="2026-04"))
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


def test_public_job_detail_includes_item_resume_summary(db_env):
    scope = ScopePayload(categories=["cs.CV"], month="2026-04")
    with session_scope() as db:
        failed = create_job(db, JobType.refresh_metadata, scope)
        failed.status = JobStatus.failed
        failed.finished_at = utc_now()
        failed_id = failed.id
        db.add(
            JobItemResumeProgress(
                attempt_series_key=failed.attempt_series_key,
                job_type=JobType.refresh_metadata,
                item_kind="repo",
                item_key="https://github.com/foo/bar",
                status="completed",
                source_job_id=failed_id,
            )
        )
        repair = create_job(
            db,
            JobType.refresh_metadata,
            scope,
            attempt_mode=JobAttemptMode.repair,
            attempt_series_key=failed.attempt_series_key,
        )
        repair_id = repair.id

    with TestClient(app) as client:
        response = client.get(f"/api/v1/jobs/{repair_id}")

    assert response.status_code == 200
    resume = response.json()["repair_resume_json"]
    assert resume["previous_job_id"] == failed_id
    assert resume["resume_items"] == {
        "total": 1,
        "item_kind": "repo",
        "by_status": {"completed": 1},
    }
