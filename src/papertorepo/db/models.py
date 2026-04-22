from __future__ import annotations

import enum
import uuid
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Enum, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from papertorepo.db.session import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobType(str, enum.Enum):
    sync_arxiv = "sync_arxiv"
    sync_arxiv_batch = "sync_arxiv_batch"
    sync_links = "sync_links"
    sync_links_batch = "sync_links_batch"
    enrich = "enrich"
    enrich_batch = "enrich_batch"
    export = "export"


class JobStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobAttemptMode(str, enum.Enum):
    fresh = "fresh"
    repair = "repair"


class ObservationStatus(str, enum.Enum):
    found = "found"
    checked_no_match = "checked_no_match"
    fetch_failed = "fetch_failed"


class RepoStableStatus(str, enum.Enum):
    found = "found"
    not_found = "not_found"
    ambiguous = "ambiguous"
    unknown = "unknown"


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_status_created_at", "status", "created_at"),
        Index("ix_jobs_dedupe_key_status", "dedupe_key", "status"),
        Index("ix_jobs_attempt_series_key", "attempt_series_key"),
        Index("ix_jobs_locked_at", "locked_at"),
        Index("ix_jobs_parent_created_at", "parent_job_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    parent_job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    job_type: Mapped[JobType] = mapped_column(Enum(JobType))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.pending)
    attempt_mode: Mapped[JobAttemptMode] = mapped_column(Enum(JobAttemptMode), default=JobAttemptMode.fresh)
    attempt_series_key: Mapped[str] = mapped_column(String(36))
    scope_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    dedupe_key: Mapped[str] = mapped_column(String(500), index=True)
    stats_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    stop_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stop_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)


class Paper(Base):
    __tablename__ = "papers"

    arxiv_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    abs_url: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    title: Mapped[str] = mapped_column(Text)
    abstract: Mapped[str] = mapped_column(Text)
    published_at: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    updated_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    authors_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    categories_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_category: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    source_last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    observations: Mapped[list["RepoObservation"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    archive_appearances: Mapped[list["ArxivArchiveAppearance"]] = relationship(
        back_populates="paper",
        cascade="all, delete-orphan",
    )
    repo_state: Mapped["PaperRepoState | None"] = relationship(
        back_populates="paper",
        cascade="all, delete-orphan",
        uselist=False,
    )


class RawFetch(Base):
    __tablename__ = "raw_fetches"
    __table_args__ = (
        Index("ix_raw_fetches_request", "provider", "surface", "request_key", unique=True),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    provider: Mapped[str] = mapped_column(String(64))
    surface: Mapped[str] = mapped_column(String(64))
    request_key: Mapped[str] = mapped_column(String(255))
    request_url: Mapped[str] = mapped_column(String(1024))
    status_code: Mapped[int] = mapped_column(Integer)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    headers_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    body_path: Mapped[str] = mapped_column(String(1024))
    content_hash: Mapped[str] = mapped_column(String(128))
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_modified: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class RepoObservation(Base):
    __tablename__ = "repo_observations"
    __table_args__ = (
        Index("ix_repo_observations_paper", "arxiv_id", "provider", "surface"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    arxiv_id: Mapped[str] = mapped_column(ForeignKey("papers.arxiv_id", ondelete="CASCADE"), index=True)
    provider: Mapped[str] = mapped_column(String(64))
    surface: Mapped[str] = mapped_column(String(64))
    status: Mapped[ObservationStatus] = mapped_column(Enum(ObservationStatus))
    observed_repo_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    normalized_repo_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    evidence_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_fetch_id: Mapped[str | None] = mapped_column(ForeignKey("raw_fetches.id", ondelete="SET NULL"), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    paper: Mapped[Paper] = relationship(back_populates="observations")


class ArxivSyncWindow(Base):
    __tablename__ = "arxiv_sync_windows"
    __table_args__ = (
        Index("ix_arxiv_sync_windows_completed", "last_completed_at"),
    )

    category: Mapped[str] = mapped_column(String(64), primary_key=True)
    start_date: Mapped[date] = mapped_column(Date, primary_key=True)
    end_date: Mapped[date] = mapped_column(Date, primary_key=True)
    last_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ArxivArchiveAppearance(Base):
    __tablename__ = "arxiv_archive_appearances"
    __table_args__ = (
        Index("ix_arxiv_archive_appearances_month_arxiv", "archive_month", "arxiv_id"),
        Index("ix_arxiv_archive_appearances_category_month", "category", "archive_month"),
    )

    arxiv_id: Mapped[str] = mapped_column(ForeignKey("papers.arxiv_id", ondelete="CASCADE"), primary_key=True)
    category: Mapped[str] = mapped_column(String(64), primary_key=True)
    archive_month: Mapped[date] = mapped_column(Date, primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    paper: Mapped[Paper] = relationship(back_populates="archive_appearances")


class PaperRepoState(Base):
    __tablename__ = "paper_repo_state"
    __table_args__ = (
        Index("ix_paper_repo_state_status_refresh", "stable_status", "refresh_after"),
    )

    arxiv_id: Mapped[str] = mapped_column(ForeignKey("papers.arxiv_id", ondelete="CASCADE"), primary_key=True)
    stable_status: Mapped[RepoStableStatus] = mapped_column(Enum(RepoStableStatus), default=RepoStableStatus.unknown)
    primary_repo_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    repo_urls_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    stable_decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refresh_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    last_attempt_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    paper: Mapped[Paper] = relationship(back_populates="repo_state")


class GitHubRepo(Base):
    __tablename__ = "github_repos"

    normalized_github_url: Mapped[str] = mapped_column(String(255), primary_key=True)
    github_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    owner: Mapped[str] = mapped_column(String(255))
    repo: Mapped[str] = mapped_column(String(255))
    stars: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    homepage: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    topics_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    license: Mapped[str | None] = mapped_column(String(255), nullable=True)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    pushed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_modified: Mapped[str | None] = mapped_column(String(255), nullable=True)


class ExportRecord(Base):
    __tablename__ = "exports"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    file_name: Mapped[str] = mapped_column(String(255))
    file_path: Mapped[str] = mapped_column(String(1024))
    scope_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
