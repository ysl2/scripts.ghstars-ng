"""Add daily arXiv freshness ledger and richer paper feed fields.

Revision ID: 0009_arxiv_daily_feed_fields
Revises: 0008_job_attempt_series_key
Create Date: 2026-04-23 14:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0009_arxiv_daily_feed_fields"
down_revision = "0008_job_attempt_series_key"
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


def _column_type_name(table_name: str, column_name: str) -> str | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for column in inspector.get_columns(table_name):
        if column["name"] == column_name:
            return column["type"].__class__.__name__.lower()
    return None


def upgrade() -> None:
    bind = op.get_bind()

    if _has_table("papers"):
        with op.batch_alter_table("papers") as batch_op:
            if not _has_column("papers", "entry_id"):
                batch_op.add_column(sa.Column("entry_id", sa.String(length=255), nullable=True))
            if not _has_column("papers", "author_details_json"):
                batch_op.add_column(sa.Column("author_details_json", sa.JSON(), nullable=True))
            if not _has_column("papers", "category_details_json"):
                batch_op.add_column(sa.Column("category_details_json", sa.JSON(), nullable=True))
            if not _has_column("papers", "links_json"):
                batch_op.add_column(sa.Column("links_json", sa.JSON(), nullable=True))
            if not _has_column("papers", "journal_ref"):
                batch_op.add_column(sa.Column("journal_ref", sa.Text(), nullable=True))
            if not _has_column("papers", "doi"):
                batch_op.add_column(sa.Column("doi", sa.String(length=255), nullable=True))
            if not _has_column("papers", "primary_category_scheme"):
                batch_op.add_column(sa.Column("primary_category_scheme", sa.String(length=255), nullable=True))

        if bind.dialect.name == "postgresql":
            published_type = _column_type_name("papers", "published_at")
            updated_type = _column_type_name("papers", "updated_at")
            if published_type == "date":
                op.alter_column(
                    "papers",
                    "published_at",
                    type_=sa.DateTime(timezone=True),
                    postgresql_using="CASE WHEN published_at IS NULL THEN NULL ELSE published_at::timestamp AT TIME ZONE 'UTC' END",
                    existing_nullable=True,
                )
            if updated_type == "date":
                op.alter_column(
                    "papers",
                    "updated_at",
                    type_=sa.DateTime(timezone=True),
                    postgresql_using="CASE WHEN updated_at IS NULL THEN NULL ELSE updated_at::timestamp AT TIME ZONE 'UTC' END",
                    existing_nullable=True,
                )

    if not _has_table("arxiv_sync_days"):
        op.create_table(
            "arxiv_sync_days",
            sa.Column("category", sa.String(length=64), nullable=False),
            sa.Column("sync_day", sa.Date(), nullable=False),
            sa.Column("last_completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("category", "sync_day"),
        )

    if not _has_index("arxiv_sync_days", "ix_arxiv_sync_days_completed"):
        op.create_index("ix_arxiv_sync_days_completed", "arxiv_sync_days", ["last_completed_at"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()

    if _has_table("arxiv_sync_days"):
        if _has_index("arxiv_sync_days", "ix_arxiv_sync_days_completed"):
            op.drop_index("ix_arxiv_sync_days_completed", table_name="arxiv_sync_days")
        op.drop_table("arxiv_sync_days")

    if not _has_table("papers"):
        return

    if bind.dialect.name == "postgresql":
        published_type = _column_type_name("papers", "published_at")
        updated_type = _column_type_name("papers", "updated_at")
        if published_type in {"datetime", "timestamp", "timestampwithtimezone"}:
            op.alter_column(
                "papers",
                "published_at",
                type_=sa.Date(),
                postgresql_using="published_at::date",
                existing_nullable=True,
            )
        if updated_type in {"datetime", "timestamp", "timestampwithtimezone"}:
            op.alter_column(
                "papers",
                "updated_at",
                type_=sa.Date(),
                postgresql_using="updated_at::date",
                existing_nullable=True,
            )

    with op.batch_alter_table("papers") as batch_op:
        if _has_column("papers", "primary_category_scheme"):
            batch_op.drop_column("primary_category_scheme")
        if _has_column("papers", "doi"):
            batch_op.drop_column("doi")
        if _has_column("papers", "journal_ref"):
            batch_op.drop_column("journal_ref")
        if _has_column("papers", "links_json"):
            batch_op.drop_column("links_json")
        if _has_column("papers", "category_details_json"):
            batch_op.drop_column("category_details_json")
        if _has_column("papers", "author_details_json"):
            batch_op.drop_column("author_details_json")
        if _has_column("papers", "entry_id"):
            batch_op.drop_column("entry_id")
