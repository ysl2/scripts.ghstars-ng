from __future__ import annotations

import pytest

from src.ghstarsv2.jobs import claim_next_job, create_job, process_job
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.models import Job, JobStatus, JobType
from src.ghstarsv2.schemas import ScopePayload
from src.ghstarsv2.services import run_sync_arxiv


def _feed_xml(entries: list[tuple[str, str, str]]) -> str:
    rendered = "".join(
        f"""
        <entry>
          <id>http://arxiv.org/abs/{arxiv_id}v1</id>
          <updated>{published_at}T00:00:00Z</updated>
          <published>{published_at}T00:00:00Z</published>
          <title>{title}</title>
          <summary>{title} abstract</summary>
          <author><name>Alice</name></author>
          <category term="cs.CV" scheme="http://arxiv.org/schemas/atom"/>
        </entry>
        """
        for arxiv_id, published_at, title in entries
    )
    return (
        "<?xml version='1.0' encoding='UTF-8'?>"
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:arxiv="http://arxiv.org/schemas/atom">'
        f"{rendered}</feed>"
    )


@pytest.mark.anyio
async def test_run_sync_arxiv_reports_progress_snapshots(db_env, monkeypatch):
    snapshots: list[dict[str, int]] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_category_page(self, *, category, start=0, max_results=100):
            assert category == "cs.CV"
            assert start == 0
            assert max_results == 100
            return (
                200,
                _feed_xml([("2604.12345", "2026-04-18", "Example paper")]),
                {"Content-Type": "application/atom+xml"},
                None,
            )

    monkeypatch.setattr("src.ghstarsv2.services.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        stats = await run_sync_arxiv(
            db,
            {"categories": ["cs.CV"]},
            progress=lambda current: snapshots.append(dict(current)),
        )

    assert snapshots[0] == {
        "categories": 1,
        "papers_upserted": 0,
        "pages_fetched": 0,
        "listing_pages_fetched": 0,
        "metadata_batches_fetched": 0,
        "categories_skipped_locked": 0,
        "windows_skipped_ttl": 0,
    }
    assert snapshots[-1]["pages_fetched"] == 1
    assert snapshots[-1]["papers_upserted"] == 1
    assert snapshots[-1] == stats


@pytest.mark.anyio
async def test_process_job_failure_keeps_partial_stats(db_env, monkeypatch):
    with session_scope() as db:
        job = create_job(db, JobType.sync_arxiv, ScopePayload(categories=["cs.CV"]))

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")

    assert claimed is not None
    assert claimed.id == job.id

    async def fake_run_sync_arxiv(_db, _scope_json, *, progress=None):
        assert progress is not None
        progress({"categories": 1, "pages_fetched": 3})
        raise RuntimeError("boom")

    monkeypatch.setattr("src.ghstarsv2.jobs.run_sync_arxiv", fake_run_sync_arxiv)

    await process_job(job.id)

    with session_scope() as db:
        refreshed = db.get(Job, job.id)
        assert refreshed is not None
        assert refreshed.status == JobStatus.failed
        assert refreshed.stats_json == {"categories": 1, "pages_fetched": 3}
        assert refreshed.error_text == "boom"
        assert refreshed.locked_at is not None
