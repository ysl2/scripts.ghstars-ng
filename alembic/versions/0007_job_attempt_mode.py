"""Add persisted job attempt mode.

Revision ID: 0007_job_attempt_mode
Revises: 0006_generic_sync_batches
Create Date: 2026-04-22 22:55:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_job_attempt_mode"
down_revision = "0006_generic_sync_batches"
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


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_table("jobs") or _has_column("jobs", "attempt_mode"):
        return

    attempt_mode_enum = sa.Enum("fresh", "repair", name="jobattemptmode")
    if bind.dialect.name == "postgresql":
        attempt_mode_enum.create(bind, checkfirst=True)

    op.add_column(
        "jobs",
        sa.Column(
            "attempt_mode",
            attempt_mode_enum,
            nullable=True,
            server_default=sa.text("'fresh'"),
        ),
    )
    op.execute("UPDATE jobs SET attempt_mode = 'fresh' WHERE attempt_mode IS NULL")
    op.alter_column("jobs", "attempt_mode", nullable=False, server_default=sa.text("'fresh'"))


def downgrade() -> None:
    bind = op.get_bind()
    if not _has_table("jobs") or not _has_column("jobs", "attempt_mode"):
        return

    op.drop_column("jobs", "attempt_mode")
    if bind.dialect.name == "postgresql":
        sa.Enum("fresh", "repair", name="jobattemptmode").drop(bind, checkfirst=True)
