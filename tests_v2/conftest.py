from __future__ import annotations

from datetime import date

import pytest

from src.ghstarsv2.config import clear_settings_cache
from src.ghstarsv2.db import configure_database, session_scope
from src.ghstarsv2.jobs import init_database
from src.ghstarsv2.models import Paper, utc_now


@pytest.fixture
def db_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'ghstars-v2.db'}")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DEFAULT_CATEGORIES", "cs.CV")
    clear_settings_cache()
    configure_database()
    init_database()
    yield tmp_path
    clear_settings_cache()


def insert_paper(arxiv_id: str = "2604.12345") -> None:
    with session_scope() as db:
        db.add(
            Paper(
                arxiv_id=arxiv_id,
                abs_url=f"https://arxiv.org/abs/{arxiv_id}",
                title=f"Paper {arxiv_id}",
                abstract="Example abstract",
                published_at=date(2026, 4, 18),
                updated_at=date(2026, 4, 18),
                authors_json=["Alice", "Bob"],
                categories_json=["cs.CV"],
                comment=None,
                primary_category="cs.CV",
                source_first_seen_at=utc_now(),
                source_last_seen_at=utc_now(),
            )
        )
