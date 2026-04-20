from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from src.ghstarsv2.app import app, create_app
from src.ghstarsv2.config import clear_settings_cache
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.models import Paper, PaperRepoState, RepoStableStatus, utc_now


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
