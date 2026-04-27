from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from papertorepo.db.models import JobAttemptMode, JobStatus, JobType, RepoStableStatus


MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
ARXIV_CATEGORY_PATTERN = re.compile(r"^[a-z]+(?:-[a-z]+)*(?:\.[A-Za-z-]+)?$")
CATEGORIES_FORMAT_ERROR = "Enter categories as comma-separated arXiv fields, e.g. cs.CV, cs.LG."


class HealthRead(BaseModel):
    app_name: str
    api_prefix: str
    default_categories: list[str]
    database_dialect: str
    queue_mode: Literal["serial"]
    github_auth_configured: bool
    effective_github_min_interval_seconds: float
    step_providers: dict[str, list[str]]


class ScopePayload(BaseModel):
    categories: list[str] = Field(default_factory=list)
    day: date | None = None
    month: str | None = None
    from_date: date | None = Field(default=None, alias="from")
    to_date: date | None = Field(default=None, alias="to")
    force: bool = False
    export_mode: Literal["all_papers", "papers_view"] | None = None
    paper_ids: list[str] = Field(default_factory=list)
    output_name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_input(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        value = dict(value)
        categories = value.get("categories")
        if isinstance(categories, str):
            if categories.strip():
                value["categories"] = [item.strip() for item in categories.split(",")]
            else:
                value["categories"] = []
        month = value.get("month")
        if isinstance(month, str):
            value["month"] = month.strip() or None

        day = value.get("day")
        month = value.get("month")
        from_date = value.get("from")
        to_date = value.get("to")
        if day and not month and from_date == day and to_date == day:
            value["from"] = None
            value["to"] = None
        elif month and not day:
            try:
                month_start = date.fromisoformat(f"{month}-01")
            except ValueError:
                return value
            if month_start.month == 12:
                next_month = date(month_start.year + 1, 1, 1)
            else:
                next_month = date(month_start.year, month_start.month + 1, 1)
            month_end = date.fromordinal(next_month.toordinal() - 1)
            if from_date == month_start.isoformat() and to_date == month_end.isoformat():
                value["from"] = None
                value["to"] = None
        return value

    @field_validator("categories")
    @classmethod
    def validate_categories_format(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in value:
            token = raw.strip()
            if not token or not ARXIV_CATEGORY_PATTERN.match(token):
                raise ValueError(CATEGORIES_FORMAT_ERROR)
            normalized.append(token)
        return normalized

    @field_validator("month")
    @classmethod
    def validate_month_format(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if not MONTH_PATTERN.match(normalized):
            raise ValueError("month must be YYYY-MM")
        return normalized

    @model_validator(mode="after")
    def validate_window(self) -> "ScopePayload":
        if self.day and (self.month or self.from_date or self.to_date):
            raise ValueError("day cannot be combined with month/from/to")
        if self.month and (self.day or self.from_date or self.to_date):
            raise ValueError("month cannot be combined with day/from/to")
        if bool(self.from_date) != bool(self.to_date):
            raise ValueError("from and to must be provided together")
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ValueError("from must be <= to")
        if self.export_mode == "papers_view" and not self.paper_ids:
            raise ValueError("papers_view export requires paper_ids")
        return self


SYNC_JOB_TYPES_REQUIRING_CATEGORIES = {
    JobType.sync_papers,
    JobType.sync_papers_batch,
    JobType.find_repos,
    JobType.find_repos_batch,
    JobType.refresh_metadata,
    JobType.refresh_metadata_batch,
}


def normalized_categories(scope: ScopePayload) -> list[str]:
    seen: set[str] = set()
    categories: list[str] = []
    for raw in scope.categories:
        value = raw.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        categories.append(value)
    return categories


def validate_scope_for_job(scope: ScopePayload, job_type: JobType) -> ScopePayload:
    if job_type in SYNC_JOB_TYPES_REQUIRING_CATEGORIES and not normalized_categories(scope):
        raise ValueError("categories is required for sync jobs")
    if job_type in SYNC_JOB_TYPES_REQUIRING_CATEGORIES and not (scope.day or scope.month or (scope.from_date and scope.to_date)):
        raise ValueError("time window is required for sync jobs")
    return scope


class ChildSummary(BaseModel):
    total: int
    pending: int
    running: int
    stopping: int
    succeeded: int
    failed: int
    cancelled: int


class JobRead(BaseModel):
    id: str
    parent_job_id: str | None
    job_type: JobType
    status: JobStatus
    attempt_mode: JobAttemptMode
    attempt_series_key: str
    scope_json: dict[str, Any]
    dedupe_key: str
    stats_json: dict[str, Any]
    repair_resume_json: dict[str, Any] | None = None
    error_text: str | None
    stop_requested_at: datetime | None
    stop_reason: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    attempts: int
    locked_by: str | None
    locked_at: datetime | None
    batch_state: Literal["queued", "running", "stopping", "succeeded", "failed", "cancelled"] | None = None
    child_summary: ChildSummary | None = None
    attempt_count: int = 1
    attempt_rank: int = 1

    model_config = {"from_attributes": True}


class JobLaunchRead(BaseModel):
    disposition: Literal["created"]
    job: JobRead


class JobQueueSummaryRead(BaseModel):
    state: Literal["idle", "waiting", "active"]
    running: int
    pending: int
    stopping: int
    current_job: JobRead | None = None
    next_job: JobRead | None = None


class DashboardStats(BaseModel):
    papers: int
    found: int
    not_found: int
    ambiguous: int
    unknown: int
    repos: int
    exports: int
    pending_jobs: int
    running_jobs: int
    stopping_jobs: int
    job_queue_summary: JobQueueSummaryRead
    recent_jobs: list[JobRead]


class PaperSummaryRead(BaseModel):
    arxiv_id: str
    abs_url: str
    title: str
    published_at: datetime | None
    updated_at: datetime | None
    authors_json: list[str]
    categories_json: list[str]
    primary_category: str | None
    comment: str | None
    journal_ref: str | None
    link_status: RepoStableStatus
    primary_github_url: str | None
    primary_github_stargazers_count: int | None
    primary_github_language: str | None
    primary_github_size_kb: int | None
    primary_github_created_at: str | None
    primary_github_pushed_at: str | None
    primary_github_updated_at: str | None
    primary_github_description: str | None
    stable_decided_at: datetime | None
    refresh_after: datetime | None
    last_attempt_at: datetime | None
    last_attempt_complete: bool
    last_attempt_error: str | None


class PaperRead(PaperSummaryRead):
    abstract: str
    doi: str | None
    github_urls: list[str]


class RepoRead(BaseModel):
    github_url: str
    github_id: int | None
    node_id: str | None
    name_with_owner: str | None
    description: str | None
    homepage: str | None
    stargazers_count: int | None
    forks_count: int | None
    size_kb: int | None
    primary_language: str | None
    topic: str | None
    license_spdx_id: str | None
    license_name: str | None
    default_branch: str | None
    is_private: bool | None
    visibility: str | None
    is_fork: bool | None
    is_archived: bool | None
    is_template: bool | None
    is_disabled: bool | None
    has_issues: bool | None
    has_projects: bool | None
    has_wiki: bool | None
    has_discussions: bool | None
    allow_forking: bool | None
    web_commit_signoff_required: bool | None
    parent_github_url: str | None
    created_at: str | None
    updated_at: str | None
    pushed_at: str | None

    model_config = {"from_attributes": True}


class ExportRead(BaseModel):
    id: str
    file_name: str
    file_path: str
    scope_json: dict[str, Any]
    created_at: datetime

    model_config = {"from_attributes": True}
