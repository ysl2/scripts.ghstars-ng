"""Add arXiv archive appearance tracking.

Revision ID: 0004_arxiv_archive_appearances
Revises: 0003_arxiv_sync_windows
Create Date: 2026-04-20 13:40:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_arxiv_archive_appearances"
down_revision = "0003_arxiv_sync_windows"
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
    if not _has_table("arxiv_archive_appearances"):
        op.create_table(
            "arxiv_archive_appearances",
            sa.Column("arxiv_id", sa.String(length=32), nullable=False),
            sa.Column("category", sa.String(length=64), nullable=False),
            sa.Column("archive_month", sa.Date(), nullable=False),
            sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["arxiv_id"], ["papers.arxiv_id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("arxiv_id", "category", "archive_month"),
        )

    if not _has_index("arxiv_archive_appearances", "ix_arxiv_archive_appearances_month_arxiv"):
        op.create_index(
            "ix_arxiv_archive_appearances_month_arxiv",
            "arxiv_archive_appearances",
            ["archive_month", "arxiv_id"],
            unique=False,
        )

    if not _has_index("arxiv_archive_appearances", "ix_arxiv_archive_appearances_category_month"):
        op.create_index(
            "ix_arxiv_archive_appearances_category_month",
            "arxiv_archive_appearances",
            ["category", "archive_month"],
            unique=False,
        )


def downgrade() -> None:
    if _has_table("arxiv_archive_appearances"):
        if _has_index("arxiv_archive_appearances", "ix_arxiv_archive_appearances_category_month"):
            op.drop_index("ix_arxiv_archive_appearances_category_month", table_name="arxiv_archive_appearances")
        if _has_index("arxiv_archive_appearances", "ix_arxiv_archive_appearances_month_arxiv"):
            op.drop_index("ix_arxiv_archive_appearances_month_arxiv", table_name="arxiv_archive_appearances")
        op.drop_table("arxiv_archive_appearances")
