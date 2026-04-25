"""Add job item resume progress.

Revision ID: 0013_job_item_resume_progress
Revises: 0012_arxiv_checkpoints
Create Date: 2026-04-25 15:41:51
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0013_job_item_resume_progress"
down_revision = "0012_arxiv_checkpoints"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    return index_name in {item["name"] for item in _inspector().get_indexes(table_name)}


def upgrade() -> None:
    if _has_table("job_item_resume_progress"):
        return

    op.create_table(
        "job_item_resume_progress",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("attempt_series_key", sa.String(length=36), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("item_kind", sa.String(length=32), nullable=False),
        sa.Column("item_key", sa.String(length=1024), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_job_id", sa.String(length=36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_job_item_resume_progress_item",
        "job_item_resume_progress",
        ["attempt_series_key", "job_type", "item_kind", "item_key"],
        unique=True,
    )
    op.create_index(
        "ix_job_item_resume_progress_source_job",
        "job_item_resume_progress",
        ["source_job_id"],
        unique=False,
    )


def downgrade() -> None:
    if not _has_table("job_item_resume_progress"):
        return

    if _has_index("job_item_resume_progress", "ix_job_item_resume_progress_source_job"):
        op.drop_index("ix_job_item_resume_progress_source_job", table_name="job_item_resume_progress")
    if _has_index("job_item_resume_progress", "ix_job_item_resume_progress_item"):
        op.drop_index("ix_job_item_resume_progress_item", table_name="job_item_resume_progress")
    op.drop_table("job_item_resume_progress")
