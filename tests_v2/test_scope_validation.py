from __future__ import annotations

from argparse import Namespace

import pytest
from fastapi.testclient import TestClient

from src.ghstarsv2.app import create_app
from src.ghstarsv2.cli import _build_scope, main
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.jobs import create_job
from src.ghstarsv2.models import JobType
from src.ghstarsv2.schemas import ScopePayload
from src.ghstarsv2.services import run_enrich, run_sync_links


def test_create_job_rejects_empty_categories_for_sync_jobs(db_env):
    with session_scope() as db:
        with pytest.raises(ValueError, match="categories is required for sync jobs"):
            create_job(db, JobType.sync_links, ScopePayload())


def test_create_job_allows_categoryless_export(db_env):
    with session_scope() as db:
        job = create_job(
            db,
            JobType.export,
            ScopePayload(
                export_mode="all_papers",
                output_name="all-papers.csv",
            ),
        )

    assert job.scope_json["categories"] == []
    assert job.scope_json["export_mode"] == "all_papers"


@pytest.mark.anyio
@pytest.mark.parametrize("runner", [run_sync_links, run_enrich])
async def test_sync_services_reject_empty_categories(db_env, runner):
    with session_scope() as db:
        with pytest.raises(RuntimeError, match="categories is required for sync jobs"):
            await runner(db, {})


def test_api_returns_422_when_sync_job_categories_are_empty(db_env):
    app = create_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/jobs/sync-links",
            json={
                "categories": "",
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "categories is required for sync jobs"


def test_api_returns_422_when_sync_job_month_is_invalid(db_env):
    app = create_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/jobs/sync-links",
            json={
                "categories": "cs.CV",
                "month": "abcd",
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "month must be YYYY-MM"


def test_api_returns_422_when_sync_job_categories_are_malformed(db_env):
    app = create_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/jobs/sync-links",
            json={
                "categories": "computer vision",
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "Enter categories as comma-separated arXiv fields, e.g. cs.CV, cs.LG."


def test_cli_build_scope_rejects_empty_categories_for_sync_jobs(db_env):
    args = Namespace(
        command="sync-links",
        categories="",
        day=None,
        month=None,
        from_date=None,
        to_date=None,
        max_results=None,
        force=False,
        output=None,
    )

    with pytest.raises(ValueError, match="categories is required for sync jobs"):
        _build_scope(args)


def test_cli_build_scope_rejects_invalid_month(db_env):
    args = Namespace(
        command="sync-links",
        categories="cs.CV",
        day=None,
        month="abcd",
        from_date=None,
        to_date=None,
        max_results=None,
        force=False,
        output=None,
    )

    with pytest.raises(ValueError, match="month must be YYYY-MM"):
        _build_scope(args)


def test_cli_build_scope_rejects_invalid_categories_format(db_env):
    args = Namespace(
        command="sync-links",
        categories="computer vision",
        day=None,
        month=None,
        from_date=None,
        to_date=None,
        max_results=None,
        force=False,
        output=None,
    )

    with pytest.raises(ValueError, match=r"Enter categories as comma-separated arXiv fields, e\.g\. cs\.CV, cs\.LG\."):
        _build_scope(args)


def test_cli_main_exits_cleanly_for_invalid_month(db_env):
    with pytest.raises(SystemExit, match="month must be YYYY-MM"):
        main(["sync-links", "--categories", "cs.CV", "--month", "abcd"])
