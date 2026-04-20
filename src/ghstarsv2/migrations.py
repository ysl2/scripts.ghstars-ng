from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, inspect

from src.ghstarsv2.db import Base


LEGACY_BASELINE_REVISION = "0001_legacy_baseline"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _alembic_config(connection: Connection) -> Config:
    root = _repo_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("sqlalchemy.url", str(connection.engine.url))
    config.attributes["connection"] = connection
    return config


def run_database_migrations(connection: Connection) -> None:
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    user_tables = {name for name in table_names if name != "alembic_version"}
    config = _alembic_config(connection)

    if not user_tables:
        Base.metadata.create_all(bind=connection)
        command.stamp(config, "head")
        return

    if "alembic_version" not in table_names:
        command.stamp(config, LEGACY_BASELINE_REVISION)

    command.upgrade(config, "head")
