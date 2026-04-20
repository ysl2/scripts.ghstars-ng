import time

import pytest

from src.ghstars.models import GitHubRepoMetadata, Paper
from src.ghstars.storage.db import Database, LeaseLostError
from src.ghstars.storage.raw_cache import RawCacheStore


def test_database_upserts_paper_and_categories(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at="2026-03-24",
                updated_at="2026-03-25",
                authors=("Alice",),
                categories=("cs.CV", "cs.LG"),
                comment=None,
                primary_category="cs.CV",
            )
        )
        papers = db.list_papers_by_categories(("cs.CV",))
        assert len(papers) == 1
        assert papers[0].arxiv_id == "2603.12345"
        assert papers[0].categories == ("cs.CV", "cs.LG")
    finally:
        db.close()


def test_database_lists_papers_by_categories_with_published_at_filters(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        for arxiv_id, published_at in (
            ("2603.20001", "2026-03-20"),
            ("2603.15001", "2026-03-15"),
            ("2602.28001", "2026-02-28"),
            ("2601.00001", None),
        ):
            db.upsert_paper(
                Paper(
                    arxiv_id=arxiv_id,
                    abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                    title=arxiv_id,
                    abstract="Abstract",
                    published_at=published_at,
                    updated_at=None,
                    authors=(),
                    categories=("cs.CV",),
                    comment=None,
                    primary_category="cs.CV",
                )
            )

        assert [paper.arxiv_id for paper in db.list_papers_by_categories(("cs.CV",))] == [
            "2603.20001",
            "2603.15001",
            "2602.28001",
            "2601.00001",
        ]
        assert [
            paper.arxiv_id
            for paper in db.list_papers_by_categories(
                ("cs.CV",),
                published_from="2026-03-15",
                published_to="2026-03-15",
            )
        ] == ["2603.15001"]
        assert [
            paper.arxiv_id
            for paper in db.list_papers_by_categories(
                ("cs.CV",),
                published_from="2026-03-01",
                published_to="2026-03-31",
            )
        ] == ["2603.20001", "2603.15001"]
        assert [
            paper.arxiv_id
            for paper in db.list_papers_by_categories(
                ("cs.CV",),
                published_from="2026-02-28",
                published_to="2026-03-15",
            )
        ] == ["2603.15001", "2602.28001"]
        assert [
            paper.arxiv_id
            for paper in db.list_papers_by_categories(("cs.CV",), published_from="2026-03-15")
        ] == ["2603.20001", "2603.15001"]
        assert [
            paper.arxiv_id
            for paper in db.list_papers_by_categories(("cs.CV",), published_to="2026-02-28")
        ] == ["2602.28001"]
    finally:
        db.close()


def test_database_replaces_repo_observations_and_links(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at=None,
                updated_at=None,
                authors=(),
                categories=("cs.CV",),
                comment=None,
                primary_category="cs.CV",
            )
        )
        db.replace_repo_observations(
            arxiv_id="2603.12345",
            provider="huggingface",
            surface="paper_api",
            observations=[
                {
                    "status": "found",
                    "observed_repo_url": "https://github.com/foo/bar",
                    "normalized_repo_url": "https://github.com/foo/bar",
                    "extractor_version": "1",
                }
            ],
        )
        db.replace_paper_repo_links(
            "2603.12345",
            [
                {
                    "normalized_repo_url": "https://github.com/foo/bar",
                    "status": "found",
                    "providers": {"huggingface"},
                    "surfaces": {"huggingface:paper_api"},
                    "provider_count": 1,
                    "surface_count": 1,
                    "is_primary": True,
                }
            ],
        )
        observations = db.list_repo_observations("2603.12345")
        links = db.list_paper_repo_links("2603.12345")
        assert len(observations) == 1
        assert observations[0].normalized_repo_url == "https://github.com/foo/bar"
        assert len(links) == 1
        assert links[0].normalized_repo_url == "https://github.com/foo/bar"
    finally:
        db.close()


def test_database_upserts_paper_link_sync_state(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at=None,
                updated_at=None,
                authors=(),
                categories=("cs.CV",),
                comment=None,
                primary_category="cs.CV",
            )
        )
        db.upsert_paper_link_sync_state("2603.12345", "found", checked_at="2026-03-20T00:00:00+00:00")
        state = db.get_paper_link_sync_state("2603.12345")
        assert state is not None
        assert state.status == "found"
        assert state.checked_at == "2026-03-20T00:00:00+00:00"

        db.upsert_paper_link_sync_state("2603.12345", "not_found", checked_at="2026-03-21T00:00:00+00:00")
        state = db.get_paper_link_sync_state("2603.12345")
        assert state is not None
        assert state.status == "not_found"
        assert state.checked_at == "2026-03-21T00:00:00+00:00"
    finally:
        db.close()


def test_database_preserves_repo_created_at_while_refreshing_stars(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_github_repo(
            GitHubRepoMetadata(
                normalized_github_url="https://github.com/foo/bar",
                owner="foo",
                repo="bar",
                stars=10,
                created_at="2024-01-01T00:00:00Z",
                description="first description",
                checked_at="2026-03-20T00:00:00+00:00",
            )
        )
        db.upsert_github_repo(
            GitHubRepoMetadata(
                normalized_github_url="https://github.com/foo/bar",
                owner="foo",
                repo="bar",
                stars=25,
                created_at="2025-02-02T00:00:00Z",
                description="updated description",
                checked_at="2026-03-21T00:00:00+00:00",
            )
        )

        metadata = db.get_github_repo("https://github.com/foo/bar")
        assert metadata is not None
        assert metadata.stars == 25
        assert metadata.created_at == "2024-01-01T00:00:00Z"
        assert metadata.description == "updated description"
        assert metadata.checked_at == "2026-03-21T00:00:00+00:00"
    finally:
        db.close()


def test_raw_cache_store_reads_written_body(tmp_path):
    store = RawCacheStore(tmp_path / "raw")
    db = Database(tmp_path / "ghstars.db")
    try:
        path, content_hash = store.write_body(
            provider="huggingface",
            surface="paper_api",
            request_key="paper_api:2603.12345",
            body='{"githubRepo":"https://github.com/foo/bar"}',
            content_type="application/json",
        )
        entry = db.upsert_raw_cache(
            provider="huggingface",
            surface="paper_api",
            request_key="paper_api:2603.12345",
            request_url="https://huggingface.co/api/papers/2603.12345",
            content_type="application/json",
            status_code=200,
            body_path=path,
            content_hash=content_hash,
            etag=None,
            last_modified=None,
        )

        assert store.read_body(entry) == '{"githubRepo":"https://github.com/foo/bar"}'
    finally:
        db.close()


def test_raw_cache_store_reuses_existing_content_addressed_file(tmp_path):
    store = RawCacheStore(tmp_path / "raw")
    path1, content_hash1 = store.write_body(
        provider="huggingface",
        surface="paper_api",
        request_key="paper_api:2603.12345",
        body='{"githubRepo":"https://github.com/foo/bar"}',
        content_type="application/json",
    )
    path2, content_hash2 = store.write_body(
        provider="huggingface",
        surface="paper_api",
        request_key="paper_api:2603.12345",
        body='{"githubRepo":"https://github.com/foo/bar"}',
        content_type="application/json",
    )

    assert path1 == path2
    assert content_hash1 == content_hash2
    assert path1.read_text(encoding="utf-8") == '{"githubRepo":"https://github.com/foo/bar"}'


def test_database_gets_raw_cache_by_id_and_latest_surface_observation(tmp_path):
    store = RawCacheStore(tmp_path / "raw")
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at=None,
                updated_at=None,
                authors=(),
                categories=("cs.CV",),
                comment=None,
                primary_category="cs.CV",
            )
        )
        path, content_hash = store.write_body(
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            body='<a href="https://github.com/foo/bar">code</a>',
            content_type="text/html",
        )
        entry = db.upsert_raw_cache(
            provider="arxiv",
            surface="abs_html",
            request_key="abs:2603.12345",
            request_url="https://arxiv.org/abs/2603.12345",
            content_type="text/html",
            status_code=200,
            body_path=path,
            content_hash=content_hash,
            etag=None,
            last_modified=None,
        )
        db.replace_repo_observations(
            arxiv_id="2603.12345",
            provider="arxiv",
            surface="abs_html",
            observations=[
                {
                    "status": "found",
                    "observed_repo_url": "https://github.com/foo/bar",
                    "normalized_repo_url": "https://github.com/foo/bar",
                    "raw_cache_id": entry.id,
                    "extractor_version": "1",
                    "observed_at": "2026-04-15T00:00:00+00:00",
                }
            ],
        )

        fetched_entry = db.get_raw_cache_by_id(entry.id)
        latest = db.get_latest_repo_observation("2603.12345", "arxiv", "abs_html")
        surface_rows = db.list_surface_repo_observations("2603.12345", "arxiv", "abs_html")

        assert fetched_entry is not None
        assert fetched_entry.id == entry.id
        assert latest is not None
        assert latest.raw_cache_id == entry.id
        assert len(surface_rows) == 1
        assert surface_rows[0].normalized_repo_url == "https://github.com/foo/bar"
    finally:
        db.close()


def test_database_acquires_and_renews_paper_sync_lease(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at=None,
                updated_at=None,
                authors=(),
                categories=("cs.CV",),
                comment=None,
                primary_category="cs.CV",
            )
        )
        lease = db.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        )
        assert lease is not None
        assert db.validate_paper_sync_lease("2603.12345", owner_id="owner-a", lease_token="token-a") is True
        assert db.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="owner-b",
            lease_token="token-b",
            lease_ttl_seconds=1,
        ) is None
        assert db.renew_paper_sync_lease(
            "2603.12345",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        ) is True
        assert db.release_paper_sync_lease("2603.12345", owner_id="owner-a", lease_token="token-a") is True
        assert db.get_paper_sync_lease("2603.12345") is None
    finally:
        db.close()


def test_database_reclaims_expired_paper_sync_lease_and_fences_stale_writes(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        db.upsert_paper(
            Paper(
                arxiv_id="2603.12345",
                abs_url="https://arxiv.org/abs/2603.12345",
                title="Paper",
                abstract="Abstract",
                published_at=None,
                updated_at=None,
                authors=(),
                categories=("cs.CV",),
                comment=None,
                primary_category="cs.CV",
            )
        )
        first = db.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=0.01,
        )
        assert first is not None
        time.sleep(0.02)
        second = db.try_acquire_paper_sync_lease(
            "2603.12345",
            owner_id="owner-b",
            lease_token="token-b",
            lease_ttl_seconds=1,
        )
        assert second is not None
        assert second.owner_id == "owner-b"
        assert db.renew_paper_sync_lease(
            "2603.12345",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        ) is False
        with pytest.raises(LeaseLostError):
            db.replace_repo_observations(
                arxiv_id="2603.12345",
                provider="arxiv",
                surface="abs_html",
                observations=[
                    {
                        "status": "found",
                        "observed_repo_url": "https://github.com/foo/old",
                        "normalized_repo_url": "https://github.com/foo/old",
                        "extractor_version": "1",
                    }
                ],
                lease_owner_id="owner-a",
                lease_token="token-a",
            )
        with pytest.raises(LeaseLostError):
            db.replace_paper_repo_links(
                "2603.12345",
                [
                    {
                        "normalized_repo_url": "https://github.com/foo/old",
                        "status": "found",
                        "providers": {"arxiv"},
                        "surfaces": {"arxiv:abs_html"},
                        "provider_count": 1,
                        "surface_count": 1,
                        "is_primary": True,
                    }
                ],
                lease_owner_id="owner-a",
                lease_token="token-a",
            )
    finally:
        db.close()


def test_database_acquires_and_fences_resource_lease(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        lease = db.try_acquire_resource_lease(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        )
        assert lease is not None
        assert db.validate_resource_lease(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            owner_id="owner-a",
            lease_token="token-a",
        ) is True
        assert db.try_acquire_resource_lease(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            owner_id="owner-b",
            lease_token="token-b",
            lease_ttl_seconds=1,
        ) is None
        assert db.renew_resource_lease(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        ) is True
        db.set_sync_state(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            "2026-04-15",
            lease_owner_id="owner-a",
            lease_token="token-a",
        )
        assert db.get_sync_state("arxiv:cs.CV:2026-04-01..2026-04-30") == "2026-04-15"
        assert db.release_resource_lease(
            "arxiv:cs.CV:2026-04-01..2026-04-30",
            owner_id="owner-a",
            lease_token="token-a",
        ) is True
        assert db.get_resource_lease("arxiv:cs.CV:2026-04-01..2026-04-30") is None
    finally:
        db.close()


def test_database_reclaims_expired_resource_lease_and_fences_sync_state(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    try:
        first = db.try_acquire_resource_lease(
            "repo:https://github.com/foo/bar",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=0.01,
        )
        assert first is not None
        time.sleep(0.02)
        second = db.try_acquire_resource_lease(
            "repo:https://github.com/foo/bar",
            owner_id="owner-b",
            lease_token="token-b",
            lease_ttl_seconds=1,
        )
        assert second is not None
        assert second.owner_id == "owner-b"
        assert db.renew_resource_lease(
            "repo:https://github.com/foo/bar",
            owner_id="owner-a",
            lease_token="token-a",
            lease_ttl_seconds=1,
        ) is False
        with pytest.raises(LeaseLostError):
            db.set_sync_state(
                "repo:https://github.com/foo/bar",
                "cursor",
                lease_owner_id="owner-a",
                lease_token="token-a",
            )
    finally:
        db.close()
