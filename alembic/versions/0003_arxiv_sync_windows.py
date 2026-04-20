"""Add arXiv sync window TTL state.

Revision ID: 0003_arxiv_sync_windows
Revises: 0002_job_hierarchy
Create Date: 2026-04-20 00:10:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_arxiv_sync_windows"
down_revision = "0002_job_hierarchy"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(index["name"] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _has_table("arxiv_sync_windows"):
        op.create_table(
            "arxiv_sync_windows",
            sa.Column("category", sa.String(length=64), nullable=False),
            sa.Column("start_date", sa.Date(), nullable=False),
            sa.Column("end_date", sa.Date(), nullable=False),
            sa.Column("last_completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("category", "start_date", "end_date"),
        )

    if not _has_index("arxiv_sync_windows", "ix_arxiv_sync_windows_completed"):
        op.create_index("ix_arxiv_sync_windows_completed", "arxiv_sync_windows", ["last_completed_at"], unique=False)


def downgrade() -> None:
    if _has_table("arxiv_sync_windows"):
        if _has_index("arxiv_sync_windows", "ix_arxiv_sync_windows_completed"):
            op.drop_index("ix_arxiv_sync_windows_completed", table_name="arxiv_sync_windows")
        op.drop_table("arxiv_sync_windows")
