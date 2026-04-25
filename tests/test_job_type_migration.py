from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

from papertorepo.core.scope import build_dedupe_key


def _load_migration_module(file_name: str):
    migration_path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / file_name
    spec = importlib.util.spec_from_file_location(file_name.removesuffix(".py"), migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_alembic_revision_ids_fit_postgresql_version_column():
    version_dir = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    for migration_path in version_dir.glob("*.py"):
        module = _load_migration_module(migration_path.name)
        assert len(module.revision) <= 32, migration_path.name


def test_job_item_resume_progress_migration_creates_and_drops_table(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-resume-items.db'}")
    module = _load_migration_module("0013_job_item_resume_progress.py")

    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE jobs (id TEXT PRIMARY KEY)"))
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(module, "op", operations)

        module.upgrade()

        table_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        index_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'index'")).mappings()
        }
        assert "job_item_resume_progress" in table_names
        assert "ix_job_item_resume_progress_item" in index_names
        assert "ix_job_item_resume_progress_source_job" in index_names

        module.downgrade()

        table_names_after = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        assert "job_item_resume_progress" not in table_names_after


def test_sync_papers_postgresql_enum_rename_does_not_reupdate_old_labels(monkeypatch):
    module = _load_migration_module("0011_rename_sync_papers.py")
    executed_sql: list[str] = []

    class FakeDialect:
        name = "postgresql"

    class FakeBind:
        dialect = FakeDialect()

        def exec_driver_sql(self, sql: str) -> None:
            executed_sql.append(sql)

    monkeypatch.setattr(module.op, "get_bind", lambda: FakeBind())
    monkeypatch.setattr(module, "_postgres_jobtype_labels", lambda: {"sync_arxiv", "sync_arxiv_batch"})

    mappings_requiring_update = module._prepare_postgresql_job_type_mapping(module.OLD_TO_NEW_JOB_TYPES)

    assert mappings_requiring_update == {}
    assert executed_sql == [
        "ALTER TYPE jobtype RENAME VALUE 'sync_arxiv' TO 'sync_papers'",
        "ALTER TYPE jobtype RENAME VALUE 'sync_arxiv_batch' TO 'sync_papers_batch'",
    ]


def test_upgrade_renames_repo_job_types_and_rebuilds_dedupe_keys(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    module = _load_migration_module("0010_rename_repo_job_types.py")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    scope_json TEXT,
                    dedupe_key TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO jobs (id, job_type, scope_json, dedupe_key)
                VALUES
                    ('job-find', 'sync_links', :find_scope, 'old-find'),
                    ('job-find-batch', 'sync_links_batch', :find_batch_scope, 'old-find-batch'),
                    ('job-refresh', 'enrich', :refresh_scope, 'old-refresh'),
                    ('job-refresh-batch', 'enrich_batch', :refresh_batch_scope, 'old-refresh-batch'),
                    ('job-arxiv', 'sync_arxiv', :arxiv_scope, 'keep-me')
                """
            ),
            {
                "find_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-21"}),
                "find_batch_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
                "refresh_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-22"}),
                "refresh_batch_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-05"}),
                "arxiv_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
            },
        )

        monkeypatch.setattr(module.op, "get_bind", lambda: connection)
        module.upgrade()

        rows = {
            row.id: row
            for row in connection.execute(text("SELECT id, job_type, scope_json, dedupe_key FROM jobs")).mappings()
        }

    assert rows["job-find"]["job_type"] == "find_repos"
    assert rows["job-find-batch"]["job_type"] == "find_repos_batch"
    assert rows["job-refresh"]["job_type"] == "refresh_metadata"
    assert rows["job-refresh-batch"]["job_type"] == "refresh_metadata_batch"
    assert rows["job-arxiv"]["job_type"] == "sync_arxiv"

    assert rows["job-find"]["dedupe_key"] == build_dedupe_key("find_repos", json.loads(rows["job-find"]["scope_json"]))
    assert rows["job-find-batch"]["dedupe_key"] == build_dedupe_key(
        "find_repos_batch",
        json.loads(rows["job-find-batch"]["scope_json"]),
    )
    assert rows["job-refresh"]["dedupe_key"] == build_dedupe_key(
        "refresh_metadata",
        json.loads(rows["job-refresh"]["scope_json"]),
    )
    assert rows["job-refresh-batch"]["dedupe_key"] == build_dedupe_key(
        "refresh_metadata_batch",
        json.loads(rows["job-refresh-batch"]["scope_json"]),
    )
    assert rows["job-arxiv"]["dedupe_key"] == "keep-me"


def test_upgrade_renames_sync_papers_job_types_tables_and_dedupe_keys(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-sync-papers.db'}")
    module = _load_migration_module("0011_rename_sync_papers.py")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    scope_json TEXT,
                    dedupe_key TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO jobs (id, job_type, scope_json, dedupe_key)
                VALUES
                    ('job-sync', 'sync_arxiv', :sync_scope, 'old-sync'),
                    ('job-sync-batch', 'sync_arxiv_batch', :sync_batch_scope, 'old-sync-batch'),
                    ('job-find', 'find_repos', :find_scope, 'keep-me')
                """
            ),
            {
                "sync_scope": json.dumps({"categories": ["cs.CV"], "month": "2026-04"}),
                "sync_batch_scope": json.dumps({"categories": ["cs.CV"], "from": "2026-04-01", "to": "2026-05-31"}),
                "find_scope": json.dumps({"categories": ["cs.CV"], "day": "2026-04-21"}),
            },
        )
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_sync_days (
                    category TEXT NOT NULL,
                    sync_day DATE NOT NULL,
                    last_completed_at DATETIME,
                    PRIMARY KEY (category, sync_day)
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX ix_arxiv_sync_days_completed ON arxiv_sync_days (last_completed_at)"))
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_archive_appearances (
                    arxiv_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    archive_month DATE NOT NULL,
                    observed_at DATETIME,
                    PRIMARY KEY (arxiv_id, category, archive_month)
                )
                """
            )
        )
        connection.execute(
            text("CREATE INDEX ix_arxiv_archive_appearances_month_arxiv ON arxiv_archive_appearances (archive_month, arxiv_id)")
        )
        connection.execute(
            text("CREATE INDEX ix_arxiv_archive_appearances_category_month ON arxiv_archive_appearances (category, archive_month)")
        )
        connection.execute(
            text(
                """
                CREATE TABLE arxiv_sync_windows (
                    category TEXT NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    last_completed_at DATETIME,
                    PRIMARY KEY (category, start_date, end_date)
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX ix_arxiv_sync_windows_completed ON arxiv_sync_windows (last_completed_at)"))

        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(module, "op", operations)
        module.upgrade()

        rows = {
            row.id: row
            for row in connection.execute(text("SELECT id, job_type, scope_json, dedupe_key FROM jobs")).mappings()
        }
        table_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).mappings()
        }
        index_names = {
            row.name
            for row in connection.execute(text("SELECT name FROM sqlite_master WHERE type = 'index'")).mappings()
        }

    assert rows["job-sync"]["job_type"] == "sync_papers"
    assert rows["job-sync-batch"]["job_type"] == "sync_papers_batch"
    assert rows["job-find"]["job_type"] == "find_repos"
    assert rows["job-sync"]["dedupe_key"] == build_dedupe_key("sync_papers", json.loads(rows["job-sync"]["scope_json"]))
    assert rows["job-sync-batch"]["dedupe_key"] == build_dedupe_key(
        "sync_papers_batch",
        json.loads(rows["job-sync-batch"]["scope_json"]),
    )
    assert rows["job-find"]["dedupe_key"] == "keep-me"
    assert "sync_papers_arxiv_days" in table_names
    assert "sync_papers_arxiv_archive_appearances" in table_names
    assert "arxiv_sync_windows" not in table_names
    assert "ix_sync_papers_arxiv_days_completed" in index_names
    assert "ix_sync_papers_arxiv_archive_appearances_month_arxiv" in index_names
    assert "ix_sync_papers_arxiv_archive_appearances_category_month" in index_names
