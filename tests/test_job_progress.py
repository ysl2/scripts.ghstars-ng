from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from papertorepo.jobs.queue import claim_next_job, create_job, process_job, serialize_job
from papertorepo.db.session import session_scope
from papertorepo.db.models import Job, JobAttemptMode, JobStatus, JobType, SyncPapersArxivRequestCheckpoint
from papertorepo.api.schemas import ScopePayload
from papertorepo.services.pipeline import run_sync_papers


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
async def test_run_sync_papers_reports_progress_snapshots(db_env, monkeypatch):
    snapshots: list[dict[str, int]] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        async def fetch_listing_page(self, *, category, period, skip=0, show=2000):
            assert category == "cs.CV"
            assert period == "2026-04"
            assert skip == 0
            assert show == 2000
            return (
                200,
                '<html><body><a href="/abs/2604.12345">arXiv:2604.12345</a></body></html>',
                {"Content-Type": "text/html"},
                None,
            )

        async def fetch_id_list_feed(self, arxiv_ids):
            assert arxiv_ids == ["2604.12345"]
            return (
                200,
                _feed_xml([("2604.12345", "2026-04-18", "Example paper")]),
                {"Content-Type": "application/atom+xml"},
                None,
            )

    monkeypatch.setattr("papertorepo.services.pipeline.ArxivMetadataClient", FakeClient)

    with session_scope() as db:
        stats = await run_sync_papers(
            db,
            {"categories": ["cs.CV"], "month": "2026-04"},
            progress=lambda current: snapshots.append(dict(current)),
        )

    assert snapshots[0]["categories"] == 1
    assert snapshots[0]["papers_upserted"] == 0
    assert snapshots[0]["pages_fetched"] == 0
    assert snapshots[0]["listing_pages_fetched"] == 0
    assert snapshots[0]["metadata_batches_fetched"] == 0
    assert snapshots[0]["categories_skipped_locked"] == 0
    assert snapshots[0]["windows_skipped_ttl"] == 0
    assert snapshots[-1]["pages_fetched"] == 2
    assert snapshots[-1]["papers_upserted"] == 1
    assert stats["pages_fetched"] == 2
    assert stats["papers_upserted"] == 1


@pytest.mark.anyio
async def test_process_job_failure_keeps_partial_stats(db_env, monkeypatch):
    with session_scope() as db:
        job = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-04"))

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")

    assert claimed is not None
    assert claimed.id == job.id

    async def fake_run_sync_papers(_db, _scope_json, **kwargs):
        progress = kwargs.get("progress")
        stop_check = kwargs.get("stop_check")
        assert progress is not None
        _ = stop_check
        progress({"categories": 1, "pages_fetched": 3})
        raise RuntimeError("boom")

    monkeypatch.setattr("papertorepo.jobs.queue.run_sync_papers", fake_run_sync_papers)

    await process_job(job.id)

    with session_scope() as db:
        refreshed = db.get(Job, job.id)
        assert refreshed is not None
        assert refreshed.status == JobStatus.failed
        assert refreshed.stats_json == {"categories": 1, "pages_fetched": 3}
        assert refreshed.error_text == "boom"
        assert refreshed.locked_at is not None


def test_claim_next_job_waits_while_fresh_running_job_exists(db_env):
    now = datetime.now(timezone.utc)

    with session_scope() as db:
        running = create_job(db, JobType.find_repos, ScopePayload(categories=["cs.CV"], month="2026-04"))
        running.status = JobStatus.running
        running.started_at = now
        running.locked_at = now
        running.locked_by = "worker:old"
        pending = create_job(db, JobType.refresh_metadata, ScopePayload(categories=["cs.CV"], month="2026-05"))
        pending_id = pending.id
        db.add_all([running, pending])

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:new")

    assert claimed is None

    with session_scope() as db:
        pending_after = db.get(Job, pending_id)
        assert pending_after is not None
        assert pending_after.status == JobStatus.pending
        assert pending_after.locked_by is None


def test_claim_next_job_recovers_stale_running_job(db_env):
    now = datetime.now(timezone.utc)

    with session_scope() as db:
        stale = create_job(db, JobType.find_repos, ScopePayload(categories=["cs.CV"], month="2026-04"))
        stale.status = JobStatus.running
        stale.started_at = now - timedelta(seconds=2000)
        stale.locked_at = now - timedelta(seconds=2000)
        stale.locked_by = "worker:old"
        pending = create_job(db, JobType.refresh_metadata, ScopePayload(categories=["cs.CV"], month="2026-05"))
        stale_id = stale.id
        pending_id = pending.id
        db.add_all([stale, pending])

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:new")

    assert claimed is not None
    assert claimed.id == stale_id

    with session_scope() as db:
        stale_after = db.get(Job, stale_id)
        pending_after = db.get(Job, pending_id)
        assert stale_after is not None
        assert stale_after.status == JobStatus.running
        assert stale_after.locked_by == "worker:new"
        assert stale_after.attempts == 1
        assert pending_after is not None
        assert pending_after.status == JobStatus.pending


def test_serialize_pending_sync_papers_repair_includes_resume_summary(db_env):
    scope = ScopePayload(categories=["cs.CV"], month="2025-06", force=True)
    with session_scope() as db:
        failed = create_job(db, JobType.sync_papers, scope)
        failed.status = JobStatus.failed
        failed.stats_json = {
            "pages_fetched": 22,
            "listing_pages_fetched": 2,
            "metadata_batches_fetched": 20,
            "papers_upserted": 2000,
        }
        failed.error_text = "arXiv metadata id_list query error (429)"
        failed.finished_at = datetime(2026, 4, 24, 11, 31, 33, tzinfo=timezone.utc)
        db.add_all(
            [
                SyncPapersArxivRequestCheckpoint(
                    attempt_series_key=failed.attempt_series_key,
                    surface="listing_html",
                    request_key="list:cs.CV:2025-06:0:2000",
                    request_url="https://arxiv.org/list/cs.CV/2025-06?skip=0&show=2000",
                    status_code=200,
                    headers_json={},
                    body_path="/tmp/listing.html",
                    content_hash="listing",
                    created_at=datetime(2026, 4, 24, 11, 30, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 4, 24, 11, 30, tzinfo=timezone.utc),
                ),
                SyncPapersArxivRequestCheckpoint(
                    attempt_series_key=failed.attempt_series_key,
                    surface="id_list_feed",
                    request_key="id_batch:cs.CV:2025-06:abc:100",
                    request_url="https://export.arxiv.org/api/query?id_list_batch=abc&count=100",
                    status_code=200,
                    headers_json={},
                    body_path="/tmp/feed.xml",
                    content_hash="feed",
                    created_at=datetime(2026, 4, 24, 11, 31, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 4, 24, 11, 31, tzinfo=timezone.utc),
                ),
            ]
        )
        repair = create_job(
            db,
            JobType.sync_papers,
            scope,
            attempt_mode=JobAttemptMode.repair,
            attempt_series_key=failed.attempt_series_key,
        )
        db.commit()

        serialized = serialize_job(db, repair)

    assert serialized.stats_json == {}
    assert serialized.repair_resume_json is not None
    assert serialized.repair_resume_json["previous_job_id"] == failed.id
    assert serialized.repair_resume_json["previous_status"] == "failed"
    assert serialized.repair_resume_json["previous_stats_json"]["pages_fetched"] == 22
    assert serialized.repair_resume_json["checkpoints"]["total"] == 2
    assert serialized.repair_resume_json["checkpoints"]["by_surface"] == {
        "id_list_feed": 1,
        "listing_html": 1,
    }


def test_claim_next_job_prefers_older_scope_when_created_at_matches(db_env):
    same_created_at = datetime(2026, 4, 21, 10, 0, 0, 123456, tzinfo=timezone.utc)
    with session_scope() as db:
        april = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-04"))
        may = create_job(db, JobType.sync_papers, ScopePayload(categories=["cs.CV"], month="2026-05"))
        april.created_at = same_created_at
        may.created_at = same_created_at
        db.add_all([april, may])

    with session_scope() as db:
        claimed = claim_next_job(db, "worker:test")

    assert claimed is not None
    assert claimed.id == april.id
