from __future__ import annotations

import json
from datetime import date
from datetime import timedelta, timezone

import pytest

from src.ghstarsv2.config import clear_settings_cache
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.models import ArxivArchiveAppearance, GitHubRepo, Paper, PaperRepoState, RepoStableStatus, utc_now
from src.ghstarsv2.services import run_enrich, run_sync_links
from tests_v2.conftest import insert_paper


def _insert_paper(arxiv_id: str, published_at: date) -> None:
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                title=f"Paper {arxiv_id}",
                abstract="Example abstract",
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


@pytest.mark.anyio
async def test_sync_links_skips_fresh_stable_records(db_env, monkeypatch):
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

    class FailArxivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_abs_html(self, _arxiv_id):
            raise AssertionError("fresh records should not re-fetch arXiv")

    monkeypatch.setattr("src.ghstarsv2.services.ArxivLinksClient", FailArxivClient)

    with session_scope() as db:
        stats = await run_sync_links(db, {"categories": ["cs.CV"]})

    assert stats["papers_processed"] == 0
    assert stats["papers_skipped_fresh"] == 1


@pytest.mark.anyio
async def test_sync_links_preserves_previous_found_state_after_incomplete_lookup(db_env, monkeypatch):
    monkeypatch.setenv("ALPHAXIV_ENABLED", "false")
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

    class FakeArxivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_abs_html(self, _arxiv_id):
            return 200, "<html>No repo here</html>", {"Content-Type": "text/html"}, None

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            return 503, None, {}, "hf unavailable"

        async def fetch_paper_html(self, _arxiv_id):
            raise AssertionError("html fallback should not run after payload failure")

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            raise AssertionError("alphaxiv is disabled")

        async def fetch_paper_html(self, _arxiv_id):
            raise AssertionError("alphaxiv is disabled")

    monkeypatch.setattr("src.ghstarsv2.services.ArxivLinksClient", FakeArxivClient)
    monkeypatch.setattr("src.ghstarsv2.services.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("src.ghstarsv2.services.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        await run_sync_links(db, {"categories": ["cs.CV"]})

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        assert state is not None
        assert state.stable_status == RepoStableStatus.found
        assert state.primary_repo_url == "https://github.com/foo/bar"
        assert state.refresh_after is not None
        assert state.refresh_after.replace(tzinfo=timezone.utc) == expired_refresh_after
        assert state.last_attempt_complete is False
        assert "Hugging Face lookup incomplete" in (state.last_attempt_error or "")


@pytest.mark.anyio
async def test_sync_links_marks_not_found_only_after_complete_lookup(db_env, monkeypatch):
    insert_paper()

    class FakeArxivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_abs_html(self, _arxiv_id):
            return 200, "<html>No repo here</html>", {"Content-Type": "text/html"}, None

    class FakeHuggingFaceClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            return 404, "", {"Content-Type": "application/json"}, None

        async def fetch_paper_html(self, _arxiv_id):
            raise AssertionError("404 payload should short-circuit html fallback")

    class FakeAlphaXivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_paper_payload(self, _arxiv_id):
            return 404, "", {"Content-Type": "application/json"}, None

        async def fetch_paper_html(self, _arxiv_id):
            raise AssertionError("404 payload should short-circuit html fallback")

    monkeypatch.setattr("src.ghstarsv2.services.ArxivLinksClient", FakeArxivClient)
    monkeypatch.setattr("src.ghstarsv2.services.HuggingFaceLinksClient", FakeHuggingFaceClient)
    monkeypatch.setattr("src.ghstarsv2.services.AlphaXivLinksClient", FakeAlphaXivClient)

    with session_scope() as db:
        stats = await run_sync_links(db, {"categories": ["cs.CV"]})
        assert stats["not_found"] == 1

    with session_scope() as db:
        state = db.get(PaperRepoState, "2604.12345")
        assert state is not None
        assert state.stable_status == RepoStableStatus.not_found
        assert state.refresh_after is not None
        assert state.last_attempt_complete is True


@pytest.mark.anyio
async def test_enrich_refreshes_dynamic_fields_but_keeps_created_at(db_env, monkeypatch):
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

    monkeypatch.setattr("src.ghstarsv2.services.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_enrich(db, {"categories": ["cs.CV"]})
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
async def test_sync_links_uses_database_published_at_scope_not_archive_month(db_env, monkeypatch):
    monkeypatch.setenv("HUGGINGFACE_ENABLED", "false")
    monkeypatch.setenv("ALPHAXIV_ENABLED", "false")
    clear_settings_cache()

    _insert_paper("2604.00001", date(2026, 4, 18))
    _insert_paper("2605.00001", date(2026, 5, 2))

    with session_scope() as db:
        db.add(
            ArxivArchiveAppearance(
                arxiv_id="2605.00001",
                category="cs.CV",
                archive_month=date(2026, 4, 1),
            )
        )

    processed_ids: list[str] = []

    class FakeArxivClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_abs_html(self, arxiv_id):
            processed_ids.append(arxiv_id)
            return 200, "<html>No repo here</html>", {"Content-Type": "text/html"}, None

    monkeypatch.setattr("src.ghstarsv2.services.ArxivLinksClient", FakeArxivClient)

    with session_scope() as db:
        stats = await run_sync_links(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
        )

    assert stats["papers_considered"] == 1
    assert stats["papers_processed"] == 1
    assert processed_ids == ["2604.00001"]


@pytest.mark.anyio
async def test_enrich_uses_database_published_at_scope_not_archive_month(db_env, monkeypatch):
    _insert_paper("2604.00002", date(2026, 4, 18))
    _insert_paper("2605.00002", date(2026, 5, 2))

    with session_scope() as db:
        db.add(
            ArxivArchiveAppearance(
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

    monkeypatch.setattr("src.ghstarsv2.services.request_text", fake_request_text)

    with session_scope() as db:
        stats = await run_enrich(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
        )

    assert stats["repos_considered"] == 1
    assert stats["updated"] == 1
    assert requested_urls == ["https://api.github.com/repos/foo/april-paper"]
