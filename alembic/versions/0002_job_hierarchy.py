"""Add arXiv batch job hierarchy.

Revision ID: 0002_job_hierarchy
Revises: 0001_legacy_baseline
Create Date: 2026-04-19 15:31:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_job_hierarchy"
down_revision = "0001_legacy_baseline"
branch_labels = None
depends_on = None


def _has_index(index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(index["name"] == index_name for index in inspector.get_indexes("jobs"))


def _has_column(column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(column["name"] == column_name for column in inspector.get_columns("jobs"))


def _has_foreign_key(foreign_key_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any((foreign_key.get("name") or "") == foreign_key_name for foreign_key in inspector.get_foreign_keys("jobs"))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "jobs" not in inspector.get_table_names():
        return

    if bind.dialect.name == "postgresql":
        bind.exec_driver_sql("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'sync_arxiv_batch'")

    if not _has_column("parent_job_id"):
        op.add_column("jobs", sa.Column("parent_job_id", sa.String(length=36), nullable=True))

    if not _has_index("ix_jobs_parent_job_id"):
        op.create_index("ix_jobs_parent_job_id", "jobs", ["parent_job_id"], unique=False)

    if not _has_index("ix_jobs_parent_created_at"):
        op.create_index("ix_jobs_parent_created_at", "jobs", ["parent_job_id", "created_at"], unique=False)

    if bind.dialect.name != "sqlite" and not _has_foreign_key("fk_jobs_parent_job_id_jobs"):
        op.create_foreign_key(
            "fk_jobs_parent_job_id_jobs",
            "jobs",
            "jobs",
            ["parent_job_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "jobs" not in inspector.get_table_names():
        return

    if bind.dialect.name != "sqlite" and _has_foreign_key("fk_jobs_parent_job_id_jobs"):
        op.drop_constraint("fk_jobs_parent_job_id_jobs", "jobs", type_="foreignkey")

    if _has_index("ix_jobs_parent_created_at"):
        op.drop_index("ix_jobs_parent_created_at", table_name="jobs")

    if _has_index("ix_jobs_parent_job_id"):
        op.drop_index("ix_jobs_parent_job_id", table_name="jobs")

    if _has_column("parent_job_id"):
        op.drop_column("jobs", "parent_job_id")
