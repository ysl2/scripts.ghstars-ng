"""Add persisted job attempt series key.

Revision ID: 0008_job_attempt_series_key
Revises: 0007_job_attempt_mode
Create Date: 2026-04-23 10:30:00
"""
from __future__ import annotations

from collections.abc import Iterator

from alembic import op
import sqlalchemy as sa


revision = "0008_job_attempt_series_key"
down_revision = "0007_job_attempt_mode"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(index["name"] == index_name for index in inspector.get_indexes(table_name))


def _ordered_jobs(bind: sa.engine.Connection) -> Iterator[sa.RowMapping]:
    jobs = sa.table(
        "jobs",
        sa.column("id", sa.String()),
        sa.column("parent_job_id", sa.String()),
        sa.column("job_type", sa.String()),
        sa.column("attempt_mode", sa.String()),
        sa.column("dedupe_key", sa.String()),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    parent_sort = sa.case((jobs.c.parent_job_id.is_(None), 0), else_=1)
    stmt = (
        sa.select(
            jobs.c.id,
            jobs.c.parent_job_id,
            jobs.c.job_type,
            jobs.c.attempt_mode,
            jobs.c.dedupe_key,
            jobs.c.created_at,
        )
        .order_by(
            jobs.c.created_at.asc(),
            parent_sort.asc(),
            jobs.c.id.asc(),
        )
    )
    return bind.execute(stmt).mappings()


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_table("jobs") or _has_column("jobs", "attempt_series_key"):
        return

    op.add_column("jobs", sa.Column("attempt_series_key", sa.String(length=36), nullable=True))
    if not _has_index("jobs", "ix_jobs_attempt_series_key"):
        op.create_index("ix_jobs_attempt_series_key", "jobs", ["attempt_series_key"], unique=False)

    job_series_by_id: dict[str, str] = {}
    latest_series_by_group: dict[tuple[str, str, str | None], str] = {}
    update_stmt = sa.text("UPDATE jobs SET attempt_series_key = :attempt_series_key WHERE id = :job_id")

    for row in _ordered_jobs(bind):
        parent_id = row["parent_job_id"]
        parent_series_key = job_series_by_id.get(parent_id) if parent_id is not None else None
        group_key = (str(row["job_type"]), str(row["dedupe_key"]), parent_series_key)
        attempt_mode = str(row["attempt_mode"] or "fresh")
        if attempt_mode == "repair":
            attempt_series_key = latest_series_by_group.get(group_key) or str(row["id"])
        else:
            attempt_series_key = str(row["id"])
        bind.execute(update_stmt, {"job_id": str(row["id"]), "attempt_series_key": attempt_series_key})
        job_series_by_id[str(row["id"])] = attempt_series_key
        latest_series_by_group[group_key] = attempt_series_key

    op.alter_column("jobs", "attempt_series_key", nullable=False)


def downgrade() -> None:
    if not _has_table("jobs") or not _has_column("jobs", "attempt_series_key"):
        return

    if _has_index("jobs", "ix_jobs_attempt_series_key"):
        op.drop_index("ix_jobs_attempt_series_key", table_name="jobs")
    op.drop_column("jobs", "attempt_series_key")
