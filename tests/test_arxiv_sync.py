import asyncio
from contextlib import suppress
from datetime import date
from types import SimpleNamespace

import pytest

from src.ghstars.cli import ArxivSyncWindow, _resolve_arxiv_sync_window, async_main
from src.ghstars.commands.sync_arxiv import (
    ARXIV_PAGE_SIZE,
    ArxivWindowSyncIncompleteError,
    _build_arxiv_window_search_query,
    _run_sync_arxiv,
    _sync_arxiv_category_by_window,
)
from src.ghstars.net.http import RateLimiter, request_text
from src.ghstars.storage.db import Database
from src.ghstars.storage.raw_cache import RawCacheStore


def _feed_xml(entries: list[tuple[str, str]]) -> str:
    xml_entries = []
    for arxiv_id, published_at in entries:
        xml_entries.append(
            f"""
            <entry>
              <id>http://arxiv.org/abs/{arxiv_id}v1</id>
              <updated>{published_at}T12:00:00Z</updated>
              <published>{published_at}T00:00:00Z</published>
              <title>{arxiv_id}</title>
              <summary>Abstract</summary>
              <author><name>Alice</name></author>
              <category term='cs.CV' />
            </entry>
            """
        )
    return "<feed xmlns='http://www.w3.org/2005/Atom'>" + "".join(xml_entries) + "</feed>"


def test_resolve_arxiv_sync_window_for_day():
    window = _resolve_arxiv_sync_window(day="2026-04-15", month=None, from_date=None, to_date=None)
    assert window == ArxivSyncWindow(start_date=date(2026, 4, 15), end_date=date(2026, 4, 15))


def test_resolve_arxiv_sync_window_for_month():
    window = _resolve_arxiv_sync_window(day=None, month="2026-04", from_date=None, to_date=None)
    assert window == ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))


def test_resolve_arxiv_sync_window_for_range():
    window = _resolve_arxiv_sync_window(day=None, month=None, from_date="2026-04-01", to_date="2026-04-15")
    assert window == ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 15))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"day": "2026-04-15", "month": "2026-04", "from_date": None, "to_date": None}, "--day cannot be combined"),
        ({"day": None, "month": None, "from_date": "2026-04-16", "to_date": "2026-04-15"}, "--from must be <= --to"),
    ],
)
def test_resolve_arxiv_sync_window_rejects_invalid_input(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _resolve_arxiv_sync_window(**kwargs)


def test_build_arxiv_window_search_query_for_month():
    window = ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))
    assert _build_arxiv_window_search_query("cs.CV", window) == (
        "cat:cs.CV AND submittedDate:[202604010000 TO 202604302359]"
    )


@pytest.mark.anyio
async def test_sync_arxiv_category_by_window_pages_by_query(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    window = ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))

    class FakeClient:
        def __init__(self):
            self.calls = []

        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            self.calls.append((search_query, start, max_results))
            if start == 0:
                return 200, _feed_xml([("2604.15001", "2026-04-15"), ("2604.14001", "2026-04-14")]), {"Content-Type": "application/xml"}, None
            return 200, _feed_xml([]), {"Content-Type": "application/xml"}, None

    client = FakeClient()
    try:
        synced_count, latest_cursor = await _sync_arxiv_category_by_window(db, raw_cache, client, "cs.CV", window)
        assert synced_count == 2
        assert latest_cursor == "2026-04-15"
        assert client.calls == [
            ("cat:cs.CV AND submittedDate:[202604010000 TO 202604302359]", 0, ARXIV_PAGE_SIZE),
        ]
        papers = db.list_papers_by_categories(("cs.CV",))
        assert [paper.arxiv_id for paper in papers] == ["2604.15001", "2604.14001"]
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_arxiv_defaults_to_latest_100_when_max_results_omitted(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")

    class FakeClient:
        def __init__(self):
            self.calls = []

        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            self.calls.append((search_query, start, max_results))
            return 200, _feed_xml([("2604.15001", "2026-04-15")]), {"Content-Type": "application/xml"}, None

    client = FakeClient()
    try:
        await _run_sync_arxiv(db, raw_cache, client, ("cs.CV",), max_results=None, window=ArxivSyncWindow())
        assert client.calls == [("cat:cs.CV", 0, 100)]
        papers = db.list_papers_by_categories(("cs.CV",))
        assert len(papers) == 1
        assert papers[0].arxiv_id == "2604.15001"
    finally:
        db.close()


@pytest.mark.anyio
async def test_sync_arxiv_category_by_window_raises_on_mid_pagination_failure_after_persisting_completed_pages(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    window = ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))

    class FakeClient:
        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            if start == 0:
                entries = [(f"2604.{15000 - i:05d}", "2026-04-15") for i in range(ARXIV_PAGE_SIZE)]
                return 200, _feed_xml(entries), {"Content-Type": "application/xml"}, None
            return 429, None, {"Retry-After": "5"}, "arXiv metadata query error (429)"

    try:
        with pytest.raises(ArxivWindowSyncIncompleteError, match="incomplete"):
            await _sync_arxiv_category_by_window(db, raw_cache, FakeClient(), "cs.CV", window)
        papers = db.list_papers_by_categories(("cs.CV",))
        assert len(papers) == ARXIV_PAGE_SIZE
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_arxiv_window_failure_does_not_set_sync_state(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    window = ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))

    class FakeClient:
        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            return 429, None, {"Retry-After": "5"}, "arXiv metadata query error (429)"

    try:
        with pytest.raises(ArxivWindowSyncIncompleteError):
            await _run_sync_arxiv(db, raw_cache, FakeClient(), ("cs.CV",), max_results=None, window=window)
        assert db.get_sync_state(f"arxiv:cs.CV:{window.describe()}") is None
        assert db.list_papers_by_categories(("cs.CV",)) == []
    finally:
        db.close()


@pytest.mark.anyio
async def test_run_sync_arxiv_window_failure_keeps_already_persisted_pages(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    window = ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30))

    class FakeClient:
        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            if start == 0:
                entries = [(f"2604.{15000 - i:05d}", "2026-04-15") for i in range(ARXIV_PAGE_SIZE)]
                return 200, _feed_xml(entries), {"Content-Type": "application/xml"}, None
            return 429, None, {"Retry-After": "5"}, "arXiv metadata query error (429)"

    try:
        with pytest.raises(ArxivWindowSyncIncompleteError):
            await _run_sync_arxiv(db, raw_cache, FakeClient(), ("cs.CV",), max_results=None, window=window)
        assert db.get_sync_state(f"arxiv:cs.CV:{window.describe()}") is None
        assert len(db.list_papers_by_categories(("cs.CV",))) == ARXIV_PAGE_SIZE
    finally:
        db.close()


@pytest.mark.anyio
async def test_async_main_raises_for_incomplete_window_sync(monkeypatch, tmp_path):
    class FakeClient:
        def __init__(self, session, *, min_interval=0.5, max_concurrent=1):
            pass

        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            return 429, None, {"Retry-After": "5"}, "arXiv metadata query error (429)"

    monkeypatch.setattr("src.ghstars.cli.load_config", lambda: SimpleNamespace(
        db_path=tmp_path / "ghstars.db",
        raw_dir=tmp_path / "raw",
        default_categories=("cs.CV",),
        arxiv_api_min_interval=0.5,
        huggingface_token="",
        huggingface_min_interval=0.5,
        alphaxiv_token="",
        github_token="",
        github_min_interval=0.5,
        sync_links_concurrency=4,
    ))
    monkeypatch.setattr("src.ghstars.cli.ArxivMetadataClient", FakeClient)

    with pytest.raises(ArxivWindowSyncIncompleteError):
        await async_main(["sync", "arxiv", "--month", "2026-04"])


@pytest.mark.anyio
async def test_run_sync_arxiv_skips_when_same_stream_lease_is_held(tmp_path):
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    stream_name = "arxiv:cs.CV:2026-04-01..2026-04-30"
    holder = None
    try:
        holder = Database(tmp_path / "ghstars.db")
        lease = holder.try_acquire_resource_lease(
            stream_name,
            owner_id="holder",
            lease_token="token-holder",
            lease_ttl_seconds=1.0,
        )
        assert lease is not None

        class FakeClient:
            async def fetch_search_page(self, *, search_query, start=0, max_results=100):
                raise AssertionError("arXiv fetch should not run when stream lease is held elsewhere")

        await _run_sync_arxiv(
            db,
            raw_cache,
            FakeClient(),
            ("cs.CV",),
            max_results=None,
            window=ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30)),
        )
        assert db.get_sync_state(stream_name) is None
        assert db.list_papers_by_categories(("cs.CV",)) == []
    finally:
        if holder is not None:
            with suppress(Exception):
                holder.release_resource_lease(stream_name, owner_id="holder", lease_token="token-holder")
            holder.close()
        db.close()


@pytest.mark.anyio
async def test_run_sync_arxiv_heartbeat_prevents_stream_takeover(tmp_path, monkeypatch):
    monkeypatch.setattr("src.ghstars.cli.RESOURCE_LEASE_TTL_SECONDS", 0.05)
    monkeypatch.setattr("src.ghstars.cli.RESOURCE_LEASE_HEARTBEAT_SECONDS", 0.01)
    db = Database(tmp_path / "ghstars.db")
    raw_cache = RawCacheStore(tmp_path / "raw")
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    class HoldingClient:
        async def fetch_search_page(self, *, search_query, start=0, max_results=100):
            calls.append("first")
            entered.set()
            await asyncio.wait_for(release.wait(), timeout=1)
            return 200, _feed_xml([("2604.15001", "2026-04-15")]), {"Content-Type": "application/xml"}, None

    first = asyncio.create_task(
        _run_sync_arxiv(
            db,
            raw_cache,
            HoldingClient(),
            ("cs.CV",),
            max_results=None,
            window=ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30)),
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0.1)

        second_db = Database(tmp_path / "ghstars.db")
        second_raw_cache = RawCacheStore(tmp_path / "raw")
        try:
            class SecondClient:
                async def fetch_search_page(self, *, search_query, start=0, max_results=100):
                    calls.append("second")
                    return 200, _feed_xml([("2604.14001", "2026-04-14")]), {"Content-Type": "application/xml"}, None

            await _run_sync_arxiv(
                second_db,
                second_raw_cache,
                SecondClient(),
                ("cs.CV",),
                max_results=None,
                window=ArxivSyncWindow(start_date=date(2026, 4, 1), end_date=date(2026, 4, 30)),
            )
        finally:
            second_db.close()

        assert calls == ["first"]
    finally:
        release.set()
        await asyncio.wait_for(first, timeout=2)
        db.close()


class _FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._body


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, url, *, headers=None, params=None):
        return self.responses.pop(0)


@pytest.mark.anyio
async def test_request_text_honors_retry_after(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    session = _FakeSession([
        _FakeResponse(429, "busy", {"Retry-After": "3"}),
        _FakeResponse(200, "ok", {}),
    ])

    status, body, headers, error = await request_text(
        session,
        "https://example.com",
        semaphore=asyncio.Semaphore(1),
        rate_limiter=RateLimiter(0),
        retry_prefix="example",
    )

    assert (status, body, error) == (200, "ok", None)
    assert sleeps == [3.0]
