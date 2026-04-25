from __future__ import annotations

import json
from datetime import date, datetime
from datetime import timedelta, timezone

import pytest
from sqlalchemy import select

from papertorepo.core.config import clear_settings_cache
from papertorepo.db.session import session_scope
from papertorepo.db.models import (
    SyncPapersArxivArchiveAppearance,
    GitHubRepo,
    JobAttemptMode,
    JobItemResumeProgress,
    JobType,
    ObservationStatus,
    Paper,
    PaperRepoState,
    RawFetch,
    RepoObservation,
    RepoStableStatus,
    utc_now,
)
from papertorepo.services.pipeline import run_refresh_metadata, run_find_repos
from tests.conftest import insert_paper


def at_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _insert_paper(arxiv_id: str, published_at: date, *, abstract: str = "Example abstract") -> None:
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                title=f"Paper {arxiv_id}",
                abstract=abstract,
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


@pytest.mark.anyio
async def test_find_repos_skips_fresh_stable_records(db_env):
    insert_paper()
    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2604.12345",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/bar",
                repo_urls_json=["https://github.com/foo/bar"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now() + timedelta(days=3),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )

    with session_scope() as db:
        stats = await run_find_repos(db, {"categories": ["cs.CV"], "month": "2026-04"})

    assert stats["papers_processed"] == 0
    assert stats["papers_skipped_fresh"] == 1


@pytest.mark.anyio
async def test_find_repos_preserves_previous_found_state_after_incomplete_lookup(db_env, monkeypatch):
    monkeypatch.setenv("FIND_REPOS_ALPHAXIV_ENABLED", "false")
    clear_settings_cache()
    insert_paper()
    expired_refresh_after = utc_now() - timedelta(hours=1)

    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2604.12345",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/bar",
                repo_urls_json=["https://github.com/foo/bar"],
                stable_decided_at=utc_now() - timedelta(days=8),
                refresh_after=expired_refresh_after,
                last_attempt_at=utc_now() - timedelta(days=8),
                last_attempt_complete=True,
            )
        )

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            return 503, None, {}, "hf unavailable"

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            raise AssertionError("alphaxiv is disabled")

        async def fetch_paper_html(self, _arxiv_id):
            raise AssertionError("alphaxiv is disabled")

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            job_id="incomplete-preserve",
            attempt_series_key="incomplete-preserve-series",
            attempt_mode=JobAttemptMode.fresh,
        )
        assert stats["resume_items_completed"] == 0

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        resume_item = db.scalar(
            select(JobItemResumeProgress).where(
                JobItemResumeProgress.attempt_series_key == "incomplete-preserve-series",
                JobItemResumeProgress.item_key == "2604.12345",
            )
        )
        assert state is not None
        assert state.stable_status == RepoStableStatus.found
        assert state.primary_repo_url == "https://github.com/foo/bar"
        assert state.refresh_after is not None
        assert state.refresh_after.replace(tzinfo=timezone.utc) == expired_refresh_after
        assert state.last_attempt_complete is False
        assert "Hugging Face lookup incomplete" in (state.last_attempt_error or "")
        assert resume_item is None


@pytest.mark.anyio
async def test_find_repos_marks_not_found_only_after_complete_lookup(db_env, monkeypatch):
    insert_paper()
    calls: list[str] = []

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            calls.append("hf_api")
            return 404, "", {"Content-Type": "application/json"}, None

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            calls.append("alphaxiv_api")
            return 404, "", {"Content-Type": "application/json"}, None

        async def fetch_paper_html(self, _arxiv_id):
            calls.append("alphaxiv_html")
            return 404, "", {"Content-Type": "text/html"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_find_repos(db, {"categories": ["cs.CV"], "month": "2026-04"})
        assert stats["not_found"] == 1
        assert stats["provider_counts"]["alphaxiv"]["html_requests"] == 1

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        assert state is not None
        assert state.stable_status == RepoStableStatus.not_found
        assert state.refresh_after is not None
        assert state.last_attempt_complete is True

    assert calls == ["alphaxiv_api", "alphaxiv_html", "hf_api"]


@pytest.mark.anyio
async def test_find_repos_alphaxiv_api_404_continues_to_html_and_finds_repo(db_env, monkeypatch):
    insert_paper()
    calls: list[str] = []

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            raise AssertionError("huggingface should be skipped after alphaxiv html finds a repo")

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            calls.append("alphaxiv_api")
            return 404, "", {"Content-Type": "application/json"}, None

        async def fetch_paper_html(self, _arxiv_id):
            calls.append("alphaxiv_html")
            return (
                200,
                'resources:{github:{url:"https://github.com/foo/from-html"}}',
                {"Content-Type": "text/html"},
                None,
            )

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_find_repos(db, {"categories": ["cs.CV"], "month": "2026-04"})

    assert calls == ["alphaxiv_api", "alphaxiv_html"]
    assert stats["found"] == 1
    assert stats["provider_counts"]["alphaxiv"]["api_requests"] == 1
    assert stats["provider_counts"]["alphaxiv"]["html_requests"] == 1

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        observations = list(
            db.scalars(
                select(RepoObservation).where(RepoObservation.arxiv_id == "2604.12345").order_by(RepoObservation.id)
            )
        )

    assert state is not None
    assert state.stable_status == RepoStableStatus.found
    assert state.primary_repo_url == "https://github.com/foo/from-html"
    assert state.refresh_after is not None
    assert state.last_attempt_complete is True
    assert [(item.provider, item.surface, item.status) for item in observations] == [
        ("arxiv", "comment", ObservationStatus.checked_no_match),
        ("arxiv", "abstract", ObservationStatus.checked_no_match),
        ("alphaxiv", "paper_api", ObservationStatus.checked_no_match),
        ("alphaxiv", "paper_html", ObservationStatus.found),
    ]


@pytest.mark.anyio
async def test_find_repos_alphaxiv_api_error_continues_to_html_found_and_records_resume(db_env, monkeypatch):
    insert_paper()
    calls: list[str] = []

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            raise AssertionError("huggingface should be skipped after alphaxiv html finds a repo")

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            calls.append("alphaxiv_api")
            return 500, None, {}, "alphaxiv api unavailable"

        async def fetch_paper_html(self, _arxiv_id):
            calls.append("alphaxiv_html")
            return (
                200,
                'implementation:"https://github.com/foo/html-after-api-error"',
                {"Content-Type": "text/html"},
                None,
            )

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            job_id="alphaxiv-error-html-found",
            attempt_series_key="alphaxiv-error-html-found-series",
            attempt_mode=JobAttemptMode.fresh,
        )

    assert calls == ["alphaxiv_api", "alphaxiv_html"]
    assert stats["found"] == 1
    assert stats["resume_items_completed"] == 1
    assert stats["provider_counts"]["alphaxiv"]["api_failures"] == 1

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        resume_item = db.scalar(
            select(JobItemResumeProgress).where(
                JobItemResumeProgress.attempt_series_key == "alphaxiv-error-html-found-series",
                JobItemResumeProgress.item_key == "2604.12345",
            )
        )
        observations = list(
            db.scalars(
                select(RepoObservation).where(RepoObservation.arxiv_id == "2604.12345").order_by(RepoObservation.id)
            )
        )

    assert state is not None
    assert state.stable_status == RepoStableStatus.found
    assert state.primary_repo_url == "https://github.com/foo/html-after-api-error"
    assert state.refresh_after is not None
    assert state.last_attempt_complete is False
    assert "AlphaXiv lookup incomplete" in (state.last_attempt_error or "")
    assert resume_item is not None
    assert [(item.provider, item.surface, item.status) for item in observations] == [
        ("arxiv", "comment", ObservationStatus.checked_no_match),
        ("arxiv", "abstract", ObservationStatus.checked_no_match),
        ("alphaxiv", "paper_api", ObservationStatus.fetch_failed),
        ("alphaxiv", "paper_html", ObservationStatus.found),
    ]


@pytest.mark.anyio
async def test_find_repos_repair_reuses_completed_paper_items(db_env, monkeypatch):
    monkeypatch.setenv("FIND_REPOS_WORKER_CONCURRENCY", "1")
    monkeypatch.setenv("FIND_REPOS_ALPHAXIV_ENABLED", "false")
    clear_settings_cache()
    _insert_paper("2604.00001", date(2026, 4, 1))
    _insert_paper("2604.00002", date(2026, 4, 2))

    calls: list[tuple[str, str]] = []
    phase = "fresh"

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, arxiv_id):
            calls.append((phase, arxiv_id))
            if phase == "fresh" and arxiv_id == "2604.00001":
                raise RuntimeError("hf exploded")
            return 404, "", {"Content-Type": "application/json"}, None

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        with pytest.raises(RuntimeError, match="hf exploded"):
            await run_find_repos(
                db,
                {"categories": ["cs.CV"], "month": "2026-04"},
                job_id="fresh-find",
                attempt_series_key="find-series",
                attempt_mode=JobAttemptMode.fresh,
            )

    phase = "repair"
    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            job_id="repair-find",
            attempt_series_key="find-series",
            attempt_mode=JobAttemptMode.repair,
        )

    assert calls == [
        ("fresh", "2604.00002"),
        ("fresh", "2604.00001"),
        ("repair", "2604.00001"),
    ]
    assert stats["resume_items_reused"] == 1
    assert stats["resume_items_completed"] == 1
    assert stats["papers_processed"] == 1


@pytest.mark.anyio
async def test_find_repos_repair_does_not_reuse_incomplete_paper_items(db_env, monkeypatch):
    monkeypatch.setenv("FIND_REPOS_WORKER_CONCURRENCY", "1")
    monkeypatch.setenv("FIND_REPOS_ALPHAXIV_ENABLED", "false")
    clear_settings_cache()
    _insert_paper("2604.00501", date(2026, 4, 1))
    _insert_paper("2604.00502", date(2026, 4, 2))

    calls: list[tuple[str, str]] = []
    phase = "fresh"

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, arxiv_id):
            calls.append((phase, arxiv_id))
            if phase == "fresh" and arxiv_id == "2604.00501":
                raise RuntimeError("hf exploded")
            if phase == "fresh":
                return 503, None, {}, "hf unavailable"
            return 404, "", {"Content-Type": "application/json"}, None

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        with pytest.raises(RuntimeError, match="hf exploded"):
            await run_find_repos(
                db,
                {"categories": ["cs.CV"], "month": "2026-04"},
                job_id="fresh-incomplete-find",
                attempt_series_key="find-incomplete-series",
                attempt_mode=JobAttemptMode.fresh,
            )

    with session_scope() as db:
        incomplete_resume_item = db.scalar(
            select(JobItemResumeProgress).where(
                JobItemResumeProgress.attempt_series_key == "find-incomplete-series",
                JobItemResumeProgress.item_key == "2604.00502",
            )
        )
    assert incomplete_resume_item is None

    phase = "repair"
    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            job_id="repair-incomplete-find",
            attempt_series_key="find-incomplete-series",
            attempt_mode=JobAttemptMode.repair,
        )

    assert calls == [
        ("fresh", "2604.00502"),
        ("fresh", "2604.00501"),
        ("repair", "2604.00502"),
        ("repair", "2604.00501"),
    ]
    assert stats["resume_items_reused"] == 0
    assert stats["resume_items_completed"] == 2
    assert stats["papers_processed"] == 2


@pytest.mark.anyio
async def test_find_repos_force_repair_does_not_reuse_completed_paper_items(db_env, monkeypatch):
    monkeypatch.setenv("FIND_REPOS_WORKER_CONCURRENCY", "1")
    monkeypatch.setenv("FIND_REPOS_ALPHAXIV_ENABLED", "false")
    clear_settings_cache()
    _insert_paper("2604.01001", date(2026, 4, 1))
    _insert_paper("2604.01002", date(2026, 4, 2))
    with session_scope() as db:
        db.add(
            JobItemResumeProgress(
                attempt_series_key="force-find-series",
                job_type=JobType.find_repos,
                item_kind="paper",
                item_key="2604.01001",
                status="completed",
            )
        )

    calls: list[str] = []

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, arxiv_id):
            calls.append(arxiv_id)
            return 404, "", {"Content-Type": "application/json"}, None

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr("papertorepo.services.pipeline.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("papertorepo.services.pipeline.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04", "force": True},
            job_id="force-repair-find",
            attempt_series_key="force-find-series",
            attempt_mode=JobAttemptMode.repair,
        )

    assert calls == ["2604.01002", "2604.01001"]
    assert stats["resume_items_reused"] == 0
    assert stats["papers_processed"] == 2


@pytest.mark.anyio
async def test_refresh_metadata_refreshes_dynamic_fields_but_keeps_created_at(db_env, monkeypatch):
    insert_paper()

    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2604.12345",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/bar",
                repo_urls_json=["https://github.com/foo/bar"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now() + timedelta(days=7),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/bar",
                github_id=111,
                owner="foo",
                repo="bar",
                stars=12,
                created_at="2020-01-01T00:00:00Z",
                description="old description",
                homepage=None,
                topics_json=[],
                license=None,
                archived=False,
                pushed_at=None,
                first_seen_at=utc_now(),
                checked_at=utc_now(),
            )
        )

    async def fake_request_text(*_args, **_kwargs):
        payload = {
            "id": 111,
            "name": "bar",
            "owner": {"login": "foo"},
            "stargazers_count": 99,
            "created_at": "2024-02-02T00:00:00Z",
            "description": "new description",
            "homepage": "https://example.com",
            "topics": ["vision", "reconstruction"],
            "license": {"spdx_id": "MIT"},
            "archived": False,
            "pushed_at": "2026-04-18T00:00:00Z",
        }
        return 200, json.dumps(payload), {"ETag": "etag-1"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_refresh_metadata(db, {"categories": ["cs.CV"], "month": "2026-04"})
        assert stats["updated"] == 1

    with session_scope() as db:
        repo = db.get(GitHubRepo, "https://github.com/foo/bar")
        assert repo is not None
        assert repo.created_at == "2020-01-01T00:00:00Z"
        assert repo.stars == 99
        assert repo.description == "new description"
        assert repo.homepage == "https://example.com"
        assert repo.topics_json == ["vision", "reconstruction"]
        assert repo.license == "MIT"


@pytest.mark.anyio
async def test_refresh_metadata_repair_reuses_completed_repo_items(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("REFRESH_METADATA_GITHUB_MIN_INTERVAL", "0")
    clear_settings_cache()
    monkeypatch.setattr("papertorepo.services.pipeline.REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS", 0.0)
    _insert_paper("2604.02001", date(2026, 4, 1))
    _insert_paper("2604.02002", date(2026, 4, 2))
    with session_scope() as db:
        db.add_all(
            [
                PaperRepoState(
                    arxiv_id="2604.02001",
                    stable_status=RepoStableStatus.found,
                    primary_repo_url="https://github.com/foo/a",
                    repo_urls_json=["https://github.com/foo/a"],
                    stable_decided_at=utc_now(),
                    refresh_after=utc_now(),
                    last_attempt_at=utc_now(),
                    last_attempt_complete=True,
                ),
                PaperRepoState(
                    arxiv_id="2604.02002",
                    stable_status=RepoStableStatus.found,
                    primary_repo_url="https://github.com/foo/b",
                    repo_urls_json=["https://github.com/foo/b"],
                    stable_decided_at=utc_now(),
                    refresh_after=utc_now(),
                    last_attempt_at=utc_now(),
                    last_attempt_complete=True,
                ),
            ]
        )

    calls: list[tuple[str, str]] = []
    phase = "fresh"

    async def fake_request_text(_session, url, **_kwargs):
        calls.append((phase, url))
        if phase == "fresh" and url.endswith("/foo/b"):
            raise RuntimeError("github exploded")
        repo_name = url.rsplit("/", 1)[-1]
        payload = {
            "id": 100 if repo_name == "a" else 200,
            "name": repo_name,
            "owner": {"login": "foo"},
            "stargazers_count": 10,
            "created_at": "2020-01-01T00:00:00Z",
            "description": repo_name,
            "homepage": None,
            "topics": [],
            "license": None,
            "archived": False,
            "pushed_at": "2026-04-18T00:00:00Z",
        }
        return 200, json.dumps(payload), {"ETag": f"etag-{repo_name}"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        with pytest.raises(RuntimeError, match="github exploded"):
            await run_refresh_metadata(
                db,
                {"categories": ["cs.CV"], "month": "2026-04"},
                job_id="fresh-refresh",
                attempt_series_key="refresh-series",
                attempt_mode=JobAttemptMode.fresh,
            )

    phase = "repair"
    with session_scope() as db:
        stats = await run_refresh_metadata(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            job_id="repair-refresh",
            attempt_series_key="refresh-series",
            attempt_mode=JobAttemptMode.repair,
        )

    assert calls == [
        ("fresh", "https://api.github.com/repos/foo/a"),
        ("fresh", "https://api.github.com/repos/foo/b"),
        ("repair", "https://api.github.com/repos/foo/b"),
    ]
    assert stats["resume_items_reused"] == 1
    assert stats["resume_items_completed"] == 1
    assert stats["repos_completed"] == 1


@pytest.mark.anyio
async def test_refresh_metadata_force_repair_does_not_reuse_completed_repo_items(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "")
    monkeypatch.setenv("REFRESH_METADATA_GITHUB_MIN_INTERVAL", "0")
    clear_settings_cache()
    monkeypatch.setattr("papertorepo.services.pipeline.REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS", 0.0)
    _insert_paper("2604.02501", date(2026, 4, 1))
    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2604.02501",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/force",
                repo_urls_json=["https://github.com/foo/force"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now(),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            JobItemResumeProgress(
                attempt_series_key="force-refresh-series",
                job_type=JobType.refresh_metadata,
                item_kind="repo",
                item_key="https://github.com/foo/force",
                status="completed",
            )
        )

    calls: list[str] = []

    async def fake_request_text(_session, url, **_kwargs):
        calls.append(url)
        payload = {
            "id": 300,
            "name": "force",
            "owner": {"login": "foo"},
            "stargazers_count": 30,
            "created_at": "2020-01-01T00:00:00Z",
            "description": "force",
            "homepage": None,
            "topics": [],
            "license": None,
            "archived": False,
            "pushed_at": "2026-04-18T00:00:00Z",
        }
        return 200, json.dumps(payload), {"ETag": "etag-force"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_refresh_metadata(
            db,
            {"categories": ["cs.CV"], "month": "2026-04", "force": True},
            job_id="force-repair-refresh",
            attempt_series_key="force-refresh-series",
            attempt_mode=JobAttemptMode.repair,
        )

    assert calls == ["https://api.github.com/repos/foo/force"]
    assert stats["resume_items_reused"] == 0
    assert stats["resume_items_completed"] == 1
    assert stats["repos_completed"] == 1


@pytest.mark.anyio
async def test_find_repos_uses_database_published_at_scope_not_archive_month(db_env, monkeypatch):
    monkeypatch.setenv("FIND_REPOS_HUGGINGFACE_ENABLED", "false")
    monkeypatch.setenv("FIND_REPOS_ALPHAXIV_ENABLED", "false")
    clear_settings_cache()

    _insert_paper("2604.00001", date(2026, 4, 18), abstract="Code: https://github.com/foo/april")
    _insert_paper("2605.00001", date(2026, 5, 2), abstract="Code: https://github.com/foo/may")

    with session_scope() as db:
        db.add(
            SyncPapersArxivArchiveAppearance(
                arxiv_id="2605.00001",
                category="cs.CV",
                archive_month=date(2026, 4, 1),
            )
        )

    with session_scope() as db:
        stats = await run_find_repos(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
        )

    assert stats["papers_considered"] == 1
    assert stats["papers_processed"] == 1
    with session_scope() as db:
        assert db.get(PaperRepoState, "2604.00001") is not None
        assert db.get(PaperRepoState, "2605.00001") is None


@pytest.mark.anyio
async def test_refresh_metadata_uses_database_published_at_scope_not_archive_month(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "")
    clear_settings_cache()
    _insert_paper("2604.00002", date(2026, 4, 18))
    _insert_paper("2605.00002", date(2026, 5, 2))

    with session_scope() as db:
        db.add(
            SyncPapersArxivArchiveAppearance(
                arxiv_id="2605.00002",
                category="cs.CV",
                archive_month=date(2026, 4, 1),
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2604.00002",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/april-paper",
                repo_urls_json=["https://github.com/foo/april-paper"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now() + timedelta(days=7),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            PaperRepoState(
                arxiv_id="2605.00002",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/archive-only",
                repo_urls_json=["https://github.com/foo/archive-only"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now() + timedelta(days=7),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/april-paper",
                github_id=1,
                owner="foo",
                repo="april-paper",
                stars=10,
                created_at="2020-01-01T00:00:00Z",
                first_seen_at=utc_now(),
                checked_at=utc_now(),
            )
        )
        db.add(
            GitHubRepo(
                normalized_github_url="https://github.com/foo/archive-only",
                github_id=2,
                owner="foo",
                repo="archive-only",
                stars=10,
                created_at="2020-01-01T00:00:00Z",
                first_seen_at=utc_now(),
                checked_at=utc_now(),
            )
        )

    requested_urls: list[str] = []

    async def fake_request_text(_session, url, **_kwargs):
        requested_urls.append(url)
        payload = {
            "id": 1,
            "name": "april-paper",
            "owner": {"login": "foo"},
            "stargazers_count": 20,
            "created_at": "2024-02-02T00:00:00Z",
            "description": "updated",
            "homepage": None,
            "topics": [],
            "license": None,
            "archived": False,
            "pushed_at": "2026-04-18T00:00:00Z",
        }
        return 200, json.dumps(payload), {"ETag": "etag-1"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_refresh_metadata(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
        )

    assert stats["repos_considered"] == 1
    assert stats["updated"] == 1
    assert requested_urls == ["https://api.github.com/repos/foo/april-paper"]


@pytest.mark.anyio
async def test_refresh_metadata_uses_github_graphql_batch_with_token(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "token-1")
    clear_settings_cache()
    insert_paper("2604.10001")
    insert_paper("2604.10002")

    with session_scope() as db:
        db.add_all(
            [
                PaperRepoState(
                    arxiv_id="2604.10001",
                    stable_status=RepoStableStatus.found,
                    primary_repo_url="https://github.com/foo/bar",
                    repo_urls_json=["https://github.com/foo/bar"],
                    stable_decided_at=utc_now(),
                    refresh_after=utc_now() + timedelta(days=7),
                    last_attempt_at=utc_now(),
                    last_attempt_complete=True,
                ),
                PaperRepoState(
                    arxiv_id="2604.10002",
                    stable_status=RepoStableStatus.found,
                    primary_repo_url="https://github.com/acme/baz",
                    repo_urls_json=["https://github.com/acme/baz"],
                    stable_decided_at=utc_now(),
                    refresh_after=utc_now() + timedelta(days=7),
                    last_attempt_at=utc_now(),
                    last_attempt_complete=True,
                ),
            ]
        )

    calls: list[str] = []

    async def fake_request_text(_session, url, **kwargs):
        calls.append(url)
        if url != "https://api.github.com/graphql":
            raise AssertionError("REST fallback should not run for successful GraphQL batches")
        assert kwargs.get("method") == "POST"
        query = ((kwargs.get("json_body") or {}).get("query") or "")
        assert "repo0:" in query
        assert "repo1:" in query
        payload = {
            "data": {
                "repo0": {
                    "databaseId": 22,
                    "name": "baz",
                    "owner": {"login": "acme"},
                    "stargazerCount": 456,
                    "createdAt": "2021-02-02T00:00:00Z",
                    "description": "repo baz",
                    "homepageUrl": None,
                    "isArchived": False,
                    "pushedAt": "2026-04-19T00:00:00Z",
                    "licenseInfo": {"spdxId": "Apache-2.0", "name": "Apache-2.0"},
                    "repositoryTopics": {"nodes": [{"topic": {"name": "ml"}}, {"topic": {"name": "cv"}}]},
                },
                "repo1": {
                    "databaseId": 11,
                    "name": "bar",
                    "owner": {"login": "foo"},
                    "stargazerCount": 123,
                    "createdAt": "2020-01-01T00:00:00Z",
                    "description": "repo bar",
                    "homepageUrl": "https://bar.example",
                    "isArchived": False,
                    "pushedAt": "2026-04-20T00:00:00Z",
                    "licenseInfo": {"spdxId": "MIT", "name": "MIT"},
                    "repositoryTopics": {"nodes": [{"topic": {"name": "vision"}}]},
                },
            }
        }
        return 200, json.dumps(payload), {"Content-Type": "application/json"}, None

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_refresh_metadata(db, {"categories": ["cs.CV"], "month": "2026-04"})

    assert stats["updated"] == 2
    assert stats["repos_completed"] == 2
    assert stats["provider_counts"]["github"]["graphql_batches"] == 1
    assert stats["provider_counts"]["github"]["graphql_fallbacks"] == 0
    assert calls == ["https://api.github.com/graphql"]

    with session_scope() as db:
        foo_repo = db.get(GitHubRepo, "https://github.com/foo/bar")
        acme_repo = db.get(GitHubRepo, "https://github.com/acme/baz")
        raw_fetches = list(
            db.scalars(
                select(RawFetch).where(
                    RawFetch.provider == "github",
                    RawFetch.surface == "graphql_batch",
                )
            ).all()
        )

    assert foo_repo is not None
    assert foo_repo.stars == 123
    assert foo_repo.topics_json == ["vision"]
    assert acme_repo is not None
    assert acme_repo.stars == 456
    assert acme_repo.topics_json == ["ml", "cv"]
    assert len(raw_fetches) == 1


@pytest.mark.anyio
async def test_refresh_metadata_graphql_falls_back_to_rest_for_unresolved_repo(db_env, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "token-1")
    clear_settings_cache()
    insert_paper("2604.10003")

    with session_scope() as db:
        db.add(
            PaperRepoState(
                arxiv_id="2604.10003",
                stable_status=RepoStableStatus.found,
                primary_repo_url="https://github.com/foo/fallback",
                repo_urls_json=["https://github.com/foo/fallback"],
                stable_decided_at=utc_now(),
                refresh_after=utc_now() + timedelta(days=7),
                last_attempt_at=utc_now(),
                last_attempt_complete=True,
            )
        )

    calls: list[str] = []

    async def fake_request_text(_session, url, **kwargs):
        calls.append(url)
        if url == "https://api.github.com/graphql":
            payload = {
                "data": {"repo0": None},
                "errors": [{"path": ["repo0"], "message": "not resolved"}],
            }
            return 200, json.dumps(payload), {"Content-Type": "application/json"}, None
        if url == "https://api.github.com/repos/foo/fallback":
            payload = {
                "id": 33,
                "name": "fallback",
                "owner": {"login": "foo"},
                "stargazers_count": 77,
                "created_at": "2020-03-03T00:00:00Z",
                "description": "rest fallback repo",
                "homepage": "https://fallback.example",
                "topics": ["fallback"],
                "license": {"spdx_id": "BSD-3-Clause"},
                "archived": False,
                "pushed_at": "2026-04-18T00:00:00Z",
            }
            return 200, json.dumps(payload), {"ETag": "etag-33"}, None
        raise AssertionError(f"unexpected request: {url}")

    monkeypatch.setattr("papertorepo.services.pipeline.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_refresh_metadata(db, {"categories": ["cs.CV"], "month": "2026-04"})

    assert stats["updated"] == 1
    assert stats["repos_completed"] == 1
    assert stats["provider_counts"]["github"]["graphql_batches"] == 1
    assert stats["provider_counts"]["github"]["graphql_fallbacks"] == 1
    assert stats["provider_counts"]["github"]["rest_requests"] == 1
    assert calls == [
        "https://api.github.com/graphql",
        "https://api.github.com/repos/foo/fallback",
    ]

    with session_scope() as db:
        repo = db.get(GitHubRepo, "https://github.com/foo/fallback")
        raw_fetches = list(
            db.scalars(
                select(RawFetch).where(
                    RawFetch.provider == "github",
                    RawFetch.surface.in_(["graphql_batch", "repo_api"]),
                )
            ).all()
        )

    assert repo is not None
    assert repo.stars == 77
    assert repo.topics_json == ["fallback"]
    assert len(raw_fetches) == 2
