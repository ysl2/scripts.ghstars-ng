import asyncio
import csv
from datetime import date
import json
from pathlib import Path
import time

import pytest

from src.ghstars.cli import ArxivSyncWindow, _resolve_sync_links_concurrency
from src.ghstars.commands.audit_parity import _run_audit_parity
from src.ghstars.commands.enrich_repos import _run_enrich_repos
from src.ghstars.commands.export_csv import _run_export_csv
from src.ghstars.commands.sync_links import _run_sync_links
from src.ghstars.models import GitHubRepoMetadata, Paper
from src.ghstars.storage.db import Database
from src.ghstars.storage.raw_cache import RawCacheStore


def _insert_paper(
    db: Database,
    arxiv_id: str = "2603.12345",
    title: str = "Fast3R",
    *,
    published_at: str | None = None,
) -> None:
    db.upsert_paper(
        Paper(
            arxiv_id=arxiv_id,
            abs_url=f"https://arxiv.org/abs/{arxiv_id}",
            title=title,
            abstract="Abstract",
            published_at=published_at,
            updated_at=None,
            authors=(),
            categories=("cs.CV",),
            comment=None,
            primary_category="cs.CV",
        )
    )


def _seed_final_link(db: Database, *, arxiv_id: str, repo_url: str) -> None:
    db.replace_paper_repo_links(
        arxiv_id,
        [
            {
                "normalized_repo_url": repo_url,
                "status": "found",
                "providers": {"arxiv"},
                "surfaces": {"arxiv:abs_html"},
                "provider_count": 1,
                "surface_count": 1,
                "is_primary": True,
            }
        ],
    )


def _seed_surface_observation(
    db: Database,
    raw_cache: RawCacheStore,
    *,
    arxiv_id: str = "2603.12345",
    provider: str,
    surface: str,
    request_key: str,
    request_url: str,
    status_code: int,
    body: str,
    content_type: str,
    status: str,
    normalized_repo_url: str | None = None,
    raw_cache_enabled: bool = True,
    observed_at: str | None = None,
) -> None:
    raw_cache_id = None
    if raw_cache_enabled:
        path, content_hash = raw_cache.write_body(
            provider=provider,
            surface=surface,
            request_key=request_key,
            body=body,
            content_type=content_type,
        )
        raw_cache_id = db.upsert_raw_cache(
            provider=provider,
            surface=surface,
            request_key=request_key,
            request_url=request_url,
            content_type=content_type,
            status_code=status_code,
            body_path=path,
            content_hash=content_hash,
            etag=None,
            last_modified=None,
        ).id
    db.replace_repo_observations(
        arxiv_id=arxiv_id,
        provider=provider,
        surface=surface,
        observations=[
            {
                "status": status,
                "observed_repo_url": normalized_repo_url,
                "normalized_repo_url": normalized_repo_url,
                "evidence_text": body,
                "raw_cache_id": raw_cache_id,
                "extractor_version": "1",
                "observed_at": observed_at,
            }
        ],
    )


def _seed_link_sync_state(
    db: Database,
    *,
    arxiv_id: str = "2603.12345",
    status: str,
    checked_at: str,
) -> None:
    db.upsert_paper_link_sync_state(arxiv_id, status, checked_at=checked_at)


@pytest.mark.anyio
async def test_run_sync_links_stops_after_huggingface_api_hit(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        _insert_paper(db)

        class FakeRawCache:
            def write_body(self, **_kwargs):
                return tmp_path / "cache.json", "hash"

        class FakeRawEntry:
            id = 7

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '{"githubRepo":"https://github.com/foo/bar"}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after API hit")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after HF API hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after HF API hit")

        original = db.upsert_raw_cache

        def fake_upsert_raw_cache(**kwargs):
            original(**kwargs)
            return FakeRawEntry()

        db.upsert_raw_cache = fake_upsert_raw_cache
        await _run_sync_links(
            db,
            FakeRawCache(),
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert len(links) == 1
        assert links[0].normalized_repo_url == "https://github.com/foo/bar"
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_uses_huggingface_html_fallback_and_skips_alphaxiv(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        _insert_paper(db)

        class FakeRawCache:
            def write_body(self, **_kwargs):
                return tmp_path / "cache.json", "hash"

        class FakeRawEntry:
            id = 8

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '{}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '<a href="https://github.com/foo/bar" title="GitHub">GitHub</a>', {"Content-Type": "text/html"}, None

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after HF HTML hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after HF HTML hit")

        original = db.upsert_raw_cache

        def fake_upsert_raw_cache(**kwargs):
            original(**kwargs)
            return FakeRawEntry()

        db.upsert_raw_cache = fake_upsert_raw_cache
        await _run_sync_links(
            db,
            FakeRawCache(),
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert len(links) == 1
        assert links[0].normalized_repo_url == "https://github.com/foo/bar"
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_uses_alphaxiv_api_when_huggingface_misses(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        _insert_paper(db)

        class FakeRawCache:
            def write_body(self, **_kwargs):
                return tmp_path / "cache.json", "hash"

        class FakeRawEntry:
            id = 9

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '{}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, '', {"Content-Type": "text/html"}, None

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '{"paper":{"implementation":"https://github.com/foo/bar"}}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv HTML should not run after API hit")

        original = db.upsert_raw_cache

        def fake_upsert_raw_cache(**kwargs):
            original(**kwargs)
            return FakeRawEntry()

        db.upsert_raw_cache = fake_upsert_raw_cache
        await _run_sync_links(
            db,
            FakeRawCache(),
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert len(links) == 1
        assert links[0].normalized_repo_url == "https://github.com/foo/bar"
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_always_refreshes_arxiv_abs_html(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_surface_observation(
            db,
            raw_cache,
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            status_code=200,
            body='<a href="https://github.com/foo/old">old</a>',
            content_type="text/html",
            status="found",
            normalized_repo_url="https://github.com/foo/old",
        )
        calls: list[str] = []

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                calls.append(arxiv_id)
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv refresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv refresh")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv refresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv refresh")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        assert calls == ["2603.12345"]
        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_skips_fallback_refresh_within_fresh_found_ttl(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_surface_observation(
            db,
            raw_cache,
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            status_code=404,
            body="",
            content_type="text/html",
            status="checked_no_match",
        )
        _seed_link_sync_state(db, status="found", checked_at="2099-04-15T00:00:00+00:00")
        _seed_surface_observation(
            db,
            raw_cache,
            provider="huggingface",
            surface="paper_api",
            request_key="paper_api:2603.12345",
            request_url="https://huggingface.co/api/papers/2603.12345",
            status_code=200,
            body='{"githubRepo":"https://github.com/foo/bar"}',
            content_type="application/json",
            status="found",
            normalized_repo_url="https://github.com/foo/bar",
        )
        _seed_final_link(db, arxiv_id="2603.12345", repo_url="https://github.com/foo/bar")

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF API should be skipped while link TTL is fresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should be skipped while link TTL is fresh")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should be skipped while link TTL is fresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should be skipped while link TTL is fresh")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_skips_fallback_refresh_within_fresh_not_found_ttl(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_link_sync_state(db, status="not_found", checked_at="2099-04-15T00:00:00+00:00")
        _seed_surface_observation(
            db,
            raw_cache,
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            status_code=404,
            body="",
            content_type="text/html",
            status="checked_no_match",
        )
        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF API should be skipped while no-match TTL is fresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should be skipped while no-match TTL is fresh")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should be skipped while no-match TTL is fresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv HTML should be skipped while no-match TTL is fresh")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        assert db.list_paper_repo_links("2603.12345") == []
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_refetches_fallback_after_ttl_expires(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_link_sync_state(db, status="not_found", checked_at="2000-01-01T00:00:00+00:00")
        _seed_surface_observation(
            db,
            raw_cache,
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            status_code=404,
            body="",
            content_type="text/html",
            status="checked_no_match",
            observed_at="2000-01-01T00:00:00+00:00",
        )

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '{"githubRepo":"https://github.com/foo/bar"}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful HF API refresh")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful HF API refresh")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful HF API refresh")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
        state = db.get_paper_link_sync_state("2603.12345")
        assert state is not None
        assert state.status == "found"
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_keeps_previous_links_after_partial_refresh_failure(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_link_sync_state(db, status="found", checked_at="2000-01-01T00:00:00+00:00")
        _seed_final_link(db, arxiv_id="2603.12345", repo_url="https://github.com/foo/old")
        _seed_surface_observation(
            db,
            raw_cache,
            provider="huggingface",
            surface="paper_api",
            request_key="paper_api:2603.12345",
            request_url="https://huggingface.co/api/papers/2603.12345",
            status_code=200,
            body='{"githubRepo":"https://github.com/foo/old"}',
            content_type="application/json",
            status="found",
            normalized_repo_url="https://github.com/foo/old",
        )

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 503, None, {}, "HF API error"

            async def fetch_paper_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "{}", {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 404, "", {"Content-Type": "text/html"}, None

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/old"]
        state = db.get_paper_link_sync_state("2603.12345")
        assert state is not None
        assert state.status == "found"
        assert state.checked_at == "2000-01-01T00:00:00+00:00"
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_retries_surface_after_fetch_failed(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        db.replace_repo_observations(
            arxiv_id="2603.12345",
            provider="arxiv",
            surface="abs_html",
            observations=[
                {
                    "status": "fetch_failed",
                    "observed_repo_url": None,
                    "normalized_repo_url": None,
                    "evidence_text": "Fast3R",
                    "raw_cache_id": None,
                    "extractor_version": "1",
                    "error_message": "timeout",
                }
            ],
        )

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv retry")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv retry")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv retry")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv retry")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_refetches_when_cached_found_body_is_missing(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        _seed_surface_observation(
            db,
            raw_cache,
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            status_code=200,
            body='<a href="https://github.com/foo/old">old</a>',
            content_type="text/html",
            status="found",
            normalized_repo_url="https://github.com/foo/old",
        )
        cached = db.get_raw_cache("arxiv", "abs_html", "abs:2603.12345")
        assert cached is not None
        cached.body_path.unlink()

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                assert arxiv_id == "2603.12345"
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv refetch")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv refetch")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv refetch")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv refetch")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


def test_resolve_sync_links_concurrency_prefers_cli_and_rejects_nonpositive():
    assert _resolve_sync_links_concurrency(None, 4) == 4
    assert _resolve_sync_links_concurrency(2, 4) == 2
    with pytest.raises(ValueError):
        _resolve_sync_links_concurrency(0, 4)


@pytest.mark.anyio
async def test_run_sync_links_processes_multiple_papers_concurrently(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_TTL_SECONDS", 1.0)
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_HEARTBEAT_SECONDS", 0.05)
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db, "2603.12345", title="Paper A")
        _insert_paper(db, "2603.23456", title="Paper B")
        events: list[tuple[str, str]] = []
        entered = asyncio.Event()
        release = asyncio.Event()
        active = 0
        overlap_detected = False

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                nonlocal active, overlap_detected
                events.append((arxiv_id, "arxiv:start"))
                active += 1
                if active >= 2:
                    overlap_detected = True
                    entered.set()
                if active == 1:
                    await asyncio.wait_for(entered.wait(), timeout=1)
                await asyncio.wait_for(release.wait(), timeout=1)
                active -= 1
                events.append((arxiv_id, "arxiv:end"))
                return 404, "", {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                events.append((arxiv_id, "hf_api"))
                return 200, '{"githubRepo":"https://github.com/foo/bar"}', {"Content-Type": "application/json"}, None

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after API hit")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after HF API hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after HF API hit")

        task = asyncio.create_task(
            _run_sync_links(
                db,
                raw_cache,
                FakeArxivClient(),
                FakeHuggingFaceClient(),
                FakeAlphaXivClient(),
                ("cs.CV",),
                concurrency=2,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        release.set()
        await asyncio.wait_for(task, timeout=2)

        assert overlap_detected is True
        for arxiv_id in ("2603.12345", "2603.23456"):
            paper_events = [event for paper_id, event in events if paper_id == arxiv_id]
            assert paper_events == ["arxiv:start", "arxiv:end", "hf_api"]
            links = db.list_paper_repo_links(arxiv_id)
            assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_skips_same_paper_when_another_process_holds_lease(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_TTL_SECONDS", 1.0)
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_HEARTBEAT_SECONDS", 0.05)
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        holder = Database(tmp_path / "ghstars.db")
        lease = holder.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="holder",
            lease_token="token-holder",
            lease_ttl_seconds=1.0,
        )
        assert lease is not None
        calls: list[str] = []

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                calls.append(arxiv_id)
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run when lease is held elsewhere")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run when lease is held elsewhere")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run when lease is held elsewhere")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run when lease is held elsewhere")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        assert calls == []
        assert db.list_paper_repo_links("2603.12345") == []
        holder.release_paper_sync_lease("2603.12345", owner_id="holder", lease_token="token-holder")
        holder.close()
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_reclaims_expired_lease_and_completes(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_TTL_SECONDS", 0.05)
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_HEARTBEAT_SECONDS", 1.0)
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        holder = Database(tmp_path / "ghstars.db")
        lease = holder.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="holder",
            lease_token="token-holder",
            lease_ttl_seconds=0.01,
        )
        assert lease is not None
        time.sleep(0.02)

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv reclaim")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv reclaim")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv reclaim")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv reclaim")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
        )

        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
        holder.close()
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_heartbeat_prevents_same_paper_takeover(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_TTL_SECONDS", 0.05)
    monkeypatch.setattr("src.ghstars.cli.PAPER_SYNC_LEASE_HEARTBEAT_SECONDS", 0.01)
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db)
        entered = asyncio.Event()
        release = asyncio.Event()
        calls: list[str] = []

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                calls.append("first")
                entered.set()
                await asyncio.wait_for(release.wait(), timeout=1)
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv hit")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv hit")

        first = asyncio.create_task(
            _run_sync_links(
                db,
                raw_cache,
                FakeArxivClient(),
                FakeHuggingFaceClient(),
                FakeAlphaXivClient(),
                ("cs.CV",),
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0.1)

        class BlockingArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                calls.append("second")
                return 200, '<a href="https://github.com/foo/baz">code</a>', {"Content-Type": "text/html"}, None

        second = asyncio.create_task(
            _run_sync_links(
                Database(tmp_path / "ghstars.db"),
                raw_cache,
                BlockingArxivClient(),
                FakeHuggingFaceClient(),
                FakeAlphaXivClient(),
                ("cs.CV",),
            )
        )
        await asyncio.sleep(0.05)
        release.set()
        await asyncio.wait_for(first, timeout=2)
        await asyncio.wait_for(second, timeout=2)

        assert calls == ["first"]
        links = db.list_paper_repo_links("2603.12345")
        assert [link.normalized_repo_url for link in links] == ["https://github.com/foo/bar"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_links_filters_papers_by_published_window(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    try:
        _insert_paper(db, "2603.12345", title="March Paper", published_at="2026-03-15")
        _insert_paper(db, "2602.23456", title="February Paper", published_at="2026-02-28")
        _insert_paper(db, "2601.34567", title="Unknown Date")
        calls: list[str] = []

        class FakeArxivClient:
            async def fetch_abs_html(self, arxiv_id):
                calls.append(arxiv_id)
                return 200, '<a href="https://github.com/foo/bar">code</a>', {"Content-Type": "text/html"}, None

        class FakeHuggingFaceClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("HF should not run after successful arXiv hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("HF HTML should not run after successful arXiv hit")

        class FakeAlphaXivClient:
            async def fetch_paper_payload(self, arxiv_id):
                raise AssertionError("AlphaXiv API should not run after successful arXiv hit")

            async def fetch_paper_html(self, arxiv_id):
                raise AssertionError("AlphaXiv should not run after successful arXiv hit")

        await _run_sync_links(
            db,
            raw_cache,
            FakeArxivClient(),
            FakeHuggingFaceClient(),
            FakeAlphaXivClient(),
            ("cs.CV",),
            window=ArxivSyncWindow(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31)),
        )

        assert calls == ["2603.12345"]
        assert [link.normalized_repo_url for link in db.list_paper_repo_links("2603.12345")] == ["https://github.com/foo/bar"]
        assert db.list_paper_repo_links("2602.23456") == []
        assert db.list_paper_repo_links("2601.34567") == []
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_enrich_repos_filters_papers_by_published_window(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        _insert_paper(db, "2603.12345", title="March Paper", published_at="2026-03-15")
        _insert_paper(db, "2602.23456", title="February Paper", published_at="2026-02-28")
        _seed_final_link(db, arxiv_id="2603.12345", repo_url="https://github.com/foo/march")
        _seed_final_link(db, arxiv_id="2602.23456", repo_url="https://github.com/foo/feb")
        calls: list[str] = []

        class FakeGitHubClient:
            async def fetch_repo_metadata(self, normalized_github_url):
                calls.append(normalized_github_url)
                return (
                    GitHubRepoMetadata(
                        normalized_github_url=normalized_github_url,
                        owner="foo",
                        repo=normalized_github_url.rsplit("/", 1)[-1],
                        stars=42,
                        created_at="2024-01-01T00:00:00Z",
                        description="repo",
                    ),
                    None,
                )

        await _run_enrich_repos(
            db,
            FakeGitHubClient(),
            ("cs.CV",),
            window=ArxivSyncWindow(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31)),
        )

        assert calls == ["https://github.com/foo/march"]
        assert db.get_github_repo("https://github.com/foo/march") is not None
        assert db.get_github_repo("https://github.com/foo/feb") is None
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_enrich_repos_skips_when_repo_lease_is_held(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    holder = None
    try:
        _insert_paper(db, "2603.12345", title="March Paper", published_at="2026-03-15")
        _seed_final_link(db, arxiv_id="2603.12345", repo_url="https://github.com/foo/march")
        holder = Database(tmp_path / "ghstars.db")
        lease = holder.try_acquire_resource_lease(
            "repo:https://github.com/foo/march",
            owner_id="holder",
            lease_token="token-holder",
            lease_ttl_seconds=1.0,
        )
        assert lease is not None
        calls: list[str] = []

        class FakeGitHubClient:
            async def fetch_repo_metadata(self, normalized_github_url):
                calls.append(normalized_github_url)
                return None, None

        await _run_enrich_repos(
            db,
            FakeGitHubClient(),
            ("cs.CV",),
            window=ArxivSyncWindow(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31)),
        )

        assert calls == []
        assert db.get_github_repo("https://github.com/foo/march") is None
    finally:
        if holder is not None:
            holder.release_resource_lease(
                "repo:https://github.com/foo/march",
                owner_id="holder",
                lease_token="token-holder",
            )
            holder.close()
        db.close()


def test_run_audit_parity_filters_papers_by_published_window(tmp_path, capsys):
    db = Database(tmp_path / "ghstars.db")
    try:
        _insert_paper(db, "2603.12345", title="March Paper", published_at="2026-03-15")
        _insert_paper(db, "2602.23456", title="February Paper", published_at="2026-02-28")
        for arxiv_id, repo_url in (
            ("2603.12345", "https://github.com/foo/march"),
            ("2602.23456", "https://github.com/foo/feb"),
        ):
            db.replace_repo_observations(
                arxiv_id=arxiv_id,
                provider="arxiv",
                surface="abs_html",
                observations=[
                    {
                        "status": "found",
                        "observed_repo_url": repo_url,
                        "normalized_repo_url": repo_url,
                        "extractor_version": "1",
                    }
                ],
            )
            _seed_final_link(db, arxiv_id=arxiv_id, repo_url=repo_url)

        _run_audit_parity(
            db,
            ("cs.CV",),
            window=ArxivSyncWindow(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31)),
        )

        assert json.loads(capsys.readouterr().out) == {
            "papers": 1,
            "provider_visible_link_papers": 1,
            "final_found_papers": 1,
            "ambiguous_papers": 0,
        }
    finally:
        db.close()


def test_run_export_csv_writes_published_at_and_includes_unresolved_rows(tmp_path, capsys):
    db = Database(tmp_path / "ghstars.db")
    output_path = tmp_path / "papers.csv"
    try:
        _insert_paper(db, "2603.12345", title="March Paper", published_at="2026-03-15")
        _insert_paper(db, "2603.23456", title="March Unresolved", published_at="2026-03-20")
        _insert_paper(db, "2602.23456", title="February Paper", published_at="2026-02-28")
        _seed_final_link(db, arxiv_id="2603.12345", repo_url="https://github.com/foo/march")
        _seed_final_link(db, arxiv_id="2602.23456", repo_url="https://github.com/foo/feb")
        db.upsert_github_repo(
            GitHubRepoMetadata(
                normalized_github_url="https://github.com/foo/march",
                owner="foo",
                repo="march",
                stars=42,
                created_at="2024-01-01T00:00:00Z",
                description="March repo",
            )
        )
        db.upsert_github_repo(
            GitHubRepoMetadata(
                normalized_github_url="https://github.com/foo/feb",
                owner="foo",
                repo="feb",
                stars=13,
                created_at="2024-02-01T00:00:00Z",
                description="February repo",
            )
        )

        _run_export_csv(
            db,
            ("cs.CV",),
            output_path,
            window=ArxivSyncWindow(start_date=date(2026, 3, 1), end_date=date(2026, 3, 31)),
        )

        resolved_output_path = Path(capsys.readouterr().out.strip())
        assert resolved_output_path.parent == output_path.parent
        assert resolved_output_path.name.startswith("papers-")
        assert resolved_output_path.suffix == ".csv"

        with resolved_output_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)

        assert reader.fieldnames is not None
        assert "published_at" in reader.fieldnames
        assert rows == [
            {
                "arxiv_id": "2603.23456",
                "abs_url": "https://arxiv.org/abs/2603.23456",
                "title": "March Unresolved",
                "abstract": "Abstract",
                "published_at": "2026-03-20",
                "categories": "cs.CV",
                "primary_category": "cs.CV",
                "github_primary": "",
                "github_all": "",
                "link_status": "not_found",
                "stars": "",
                "created_at": "",
                "description": "",
            },
            {
                "arxiv_id": "2603.12345",
                "abs_url": "https://arxiv.org/abs/2603.12345",
                "title": "March Paper",
                "abstract": "Abstract",
                "published_at": "2026-03-15",
                "categories": "cs.CV",
                "primary_category": "cs.CV",
                "github_primary": "https://github.com/foo/march",
                "github_all": "https://github.com/foo/march",
                "link_status": "found",
                "stars": "42",
                "created_at": "2024-01-01T00:00:00Z",
                "description": "March repo",
            },
        ]
    finally:
        db.close()
