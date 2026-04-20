from __future__ import annotations

import csv
from datetime import date

from src.ghstarsv2.db import session_scope
from src.ghstarsv2.models import ArxivArchiveAppearance, ExportRecord, Paper, utc_now
from src.ghstarsv2.scope import build_scope_json
from src.ghstarsv2.schemas import ScopePayload
from src.ghstarsv2.services import run_export


def _insert_paper(arxiv_id: str, published_at: date, primary_category: str) -> None:
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
                categories_json=[primary_category],
                comment=None,
                primary_category=primary_category,
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )


def test_build_scope_json_keeps_all_papers_export_categoryless(db_env):
    scope_json = build_scope_json(
        ScopePayload(
            export_mode="all_papers",
            output_name="all-papers.csv",
        )
    )

    assert scope_json["categories"] == []
    assert scope_json["export_mode"] == "all_papers"
    assert scope_json["paper_ids"] == []


def test_run_export_all_papers_ignores_default_categories(db_env):
    _insert_paper("2604.00002", date(2026, 4, 18), "cs.CV")
    _insert_paper("2604.00001", date(2026, 4, 17), "cs.LG")

    scope_json = build_scope_json(
        ScopePayload(
            export_mode="all_papers",
            output_name="all-papers.csv",
        )
    )

    with session_scope() as db:
        stats = run_export(db, scope_json)

    assert stats["rows"] == 2
    export_path = db_env / "data" / "exports" / "all-papers.csv"
    with export_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["arxiv_id"] for row in rows] == ["2604.00002", "2604.00001"]

    with session_scope() as db:
        record = db.query(ExportRecord).one()
        assert record.scope_json["export_mode"] == "all_papers"
        assert record.scope_json["categories"] == []


def test_run_export_papers_view_preserves_requested_order(db_env):
    _insert_paper("2604.00003", date(2026, 4, 19), "cs.CV")
    _insert_paper("2604.00002", date(2026, 4, 18), "cs.CV")
    _insert_paper("2604.00001", date(2026, 4, 17), "cs.CV")

    scope_json = build_scope_json(
        ScopePayload(
            export_mode="papers_view",
            paper_ids=["2604.00002", "2604.00003"],
            output_name="filtered-papers.csv",
        )
    )

    with session_scope() as db:
        stats = run_export(db, scope_json)

    assert stats["rows"] == 2
    assert stats["export_mode"] == "papers_view"
    export_path = db_env / "data" / "exports" / "filtered-papers.csv"
    with export_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["arxiv_id"] for row in rows] == ["2604.00002", "2604.00003"]

    with session_scope() as db:
        records = db.query(ExportRecord).order_by(ExportRecord.created_at.asc()).all()
        assert records[-1].scope_json["paper_ids"] == ["2604.00002", "2604.00003"]


def test_run_export_scope_uses_database_published_at_not_archive_month(db_env):
    _insert_paper("2604.00004", date(2026, 4, 20), "cs.CV")
    _insert_paper("2605.00004", date(2026, 5, 2), "cs.CV")

    with session_scope() as db:
        db.add(
            ArxivArchiveAppearance(
                arxiv_id="2605.00004",
                category="cs.CV",
                archive_month=date(2026, 4, 1),
            )
        )

    scope_json = build_scope_json(
        ScopePayload(
            categories=["cs.CV"],
            month="2026-04",
            output_name="april-scope.csv",
        )
    )

    with session_scope() as db:
        stats = run_export(db, scope_json)

    assert stats["rows"] == 1
    export_path = db_env / "data" / "exports" / "april-scope.csv"
    with export_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["arxiv_id"] for row in rows] == ["2604.00004"]
