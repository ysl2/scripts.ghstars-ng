from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from src.ghstarsv2.config import get_settings
from src.ghstarsv2.db import get_db
from src.ghstarsv2.jobs import (
    create_job,
    create_sync_arxiv_job,
    get_job_attempt_meta,
    list_job_attempts_read,
    list_jobs_read,
    rerun_job,
    serialize_job,
    serialize_jobs,
)
from src.ghstarsv2.models import ExportRecord, GitHubRepo, Job, JobType, Paper, RepoStableStatus
from src.ghstarsv2.schemas import DashboardStats, ExportRead, HealthRead, JobRead, PaperRead, PaperSummaryRead, RepoRead, ScopePayload
from src.ghstarsv2.scope import build_scope_json
from src.ghstarsv2.services import get_dashboard_stats, scoped_papers, scoped_repos


def _scope_from_query(
    *,
    categories: str | None,
    day: date | None,
    month: str | None,
    from_date: date | None,
    to_date: date | None,
    max_results: int | None = None,
    force: bool = False,
    output_name: str | None = None,
) -> ScopePayload:
    return ScopePayload(
        categories=categories or "",
        day=day,
        month=month,
        **{"from": from_date, "to": to_date},
        max_results=max_results,
        force=force,
        output_name=output_name,
    )


def _paper_summary_payload(paper: Paper) -> dict[str, object]:
    state = paper.repo_state
    return {
        "arxiv_id": paper.arxiv_id,
        "abs_url": paper.abs_url,
        "title": paper.title,
        "published_at": paper.published_at,
        "updated_at": paper.updated_at,
        "authors_json": paper.authors_json or [],
        "categories_json": paper.categories_json or [],
        "primary_category": paper.primary_category,
        "link_status": state.stable_status if state is not None else RepoStableStatus.unknown,
        "primary_repo_url": state.primary_repo_url if state is not None else None,
        "stable_decided_at": state.stable_decided_at if state is not None else None,
        "refresh_after": state.refresh_after if state is not None else None,
        "last_attempt_at": state.last_attempt_at if state is not None else None,
        "last_attempt_complete": bool(state.last_attempt_complete) if state is not None else False,
        "last_attempt_error": state.last_attempt_error if state is not None else None,
    }


def serialize_paper_summary(paper: Paper) -> PaperSummaryRead:
    return PaperSummaryRead(**_paper_summary_payload(paper))


def serialize_paper(paper: Paper) -> PaperRead:
    state = paper.repo_state
    return PaperRead(
        **_paper_summary_payload(paper),
        abstract=paper.abstract,
        comment=paper.comment,
        repo_urls=state.repo_urls_json if state is not None else [],
    )


def _enqueue_job(db: Session, job_type: JobType, scope: ScopePayload) -> JobRead:
    try:
        return serialize_job(db, create_job(db, job_type, scope))
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _enqueue_sync_arxiv_job(db: Session, scope: ScopePayload) -> JobRead:
    try:
        return serialize_job(db, create_sync_arxiv_job(db, scope))
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _scope_error_detail(exc: ValueError | ValidationError) -> str:
    if isinstance(exc, ValidationError):
        errors = exc.errors()
        if errors:
            message = str(errors[0].get("msg") or "Invalid scope")
            return message.removeprefix("Value error, ")
        return "Invalid scope"
    return str(exc)


def _scope_http_exception(exc: ValueError | ValidationError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=_scope_error_detail(exc))


def _scope_json_from_query(
    *,
    categories: str | None,
    day: date | None,
    month: str | None,
    from_date: date | None,
    to_date: date | None,
    max_results: int | None = None,
    force: bool = False,
    output_name: str | None = None,
) -> dict[str, object]:
    try:
        return build_scope_json(
            _scope_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
                max_results=max_results,
                force=force,
                output_name=output_name,
            )
        )
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def register_routes(app: FastAPI) -> None:
    settings = get_settings()
    router = APIRouter(prefix=settings.api_prefix)

    @router.get("/health", response_model=HealthRead)
    def health(db: Session = Depends(get_db)) -> HealthRead:
        dialect_name = db.bind.dialect.name
        settings = get_settings()
        github_auth_configured = bool(settings.github_token.strip())
        effective_github_min_interval_seconds = (
            settings.github_min_interval if github_auth_configured else max(settings.github_min_interval, 60.0)
        )
        return HealthRead(
            app_name=settings.app_name,
            api_prefix=settings.api_prefix,
            default_categories=settings.default_categories_list,
            database_dialect=dialect_name,
            queue_mode="serial",
            github_auth_configured=github_auth_configured,
            effective_github_min_interval_seconds=effective_github_min_interval_seconds,
            step_providers={
                "sync_arxiv": ["arxiv_listing", "arxiv_export_api"],
                "sync_links": ["arxiv_abs", "huggingface", "alphaxiv"],
                "enrich": ["github_api"],
            },
        )

    @router.get("/dashboard", response_model=DashboardStats)
    def public_dashboard(
        categories: str | None = Query(default=None),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        db: Session = Depends(get_db),
    ) -> DashboardStats:
        scope = _scope_json_from_query(
            categories=categories,
            day=day,
            month=month,
            from_date=from_date,
            to_date=to_date,
        )
        stats = get_dashboard_stats(db, scope)
        recent_jobs = list(db.scalars(select(Job).order_by(Job.created_at.desc()).limit(12)))
        return DashboardStats(**stats, recent_jobs=serialize_jobs(db, recent_jobs))

    @router.get("/papers", response_model=list[PaperSummaryRead])
    def public_papers(
        categories: str | None = Query(default=None),
        status_filter: RepoStableStatus | None = Query(default=None, alias="status"),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=500, ge=1, le=25000),
        db: Session = Depends(get_db),
    ) -> list[PaperSummaryRead]:
        papers = scoped_papers(
            db,
            _scope_json_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
            ),
            offset=offset,
            limit=limit,
        )
        rows = [serialize_paper_summary(paper) for paper in papers]
        if status_filter is not None:
            rows = [paper for paper in rows if paper.link_status == status_filter]
        return rows

    @router.get("/papers/{arxiv_id}", response_model=PaperRead)
    def public_paper(arxiv_id: str, db: Session = Depends(get_db)) -> PaperRead:
        paper = db.scalar(select(Paper).options(selectinload(Paper.repo_state)).where(Paper.arxiv_id == arxiv_id))
        if paper is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Paper not found")
        return serialize_paper(paper)

    @router.get("/repos", response_model=list[RepoRead])
    def public_repos(
        categories: str | None = Query(default=None),
        day: date | None = Query(default=None),
        month: str | None = Query(default=None),
        from_date: date | None = Query(default=None, alias="from"),
        to_date: date | None = Query(default=None, alias="to"),
        limit: int = Query(default=200, ge=1, le=10000),
        db: Session = Depends(get_db),
    ) -> list[RepoRead]:
        repos = scoped_repos(
            db,
            _scope_json_from_query(
                categories=categories,
                day=day,
                month=month,
                from_date=from_date,
                to_date=to_date,
            ),
            limit=limit,
        )
        return [RepoRead.model_validate(item) for item in repos]

    @router.get("/exports", response_model=list[ExportRead])
    def public_exports(db: Session = Depends(get_db)) -> list[ExportRead]:
        return [ExportRead.model_validate(item) for item in db.scalars(select(ExportRecord).order_by(ExportRecord.created_at.desc())).all()]

    @router.get("/exports/{export_id}/download")
    def download_export(export_id: str, db: Session = Depends(get_db)) -> FileResponse:
        export_record = db.get(ExportRecord, export_id)
        if export_record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export not found")
        if not get_settings().public_export_downloads:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Public downloads disabled")
        path = Path(export_record.file_path)
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export file missing")
        return FileResponse(path, filename=export_record.file_name, media_type="text/csv")

    @router.get("/jobs", response_model=list[JobRead])
    def list_jobs(
        limit: int = Query(default=100, ge=1, le=1000),
        parent_id: str | None = Query(default=None),
        root_only: bool = Query(default=False),
        view: Literal["all", "latest"] = Query(default="all"),
        db: Session = Depends(get_db),
    ) -> list[JobRead]:
        return list_jobs_read(db, limit=limit, parent_job_id=parent_id, root_only=root_only, view=view)

    @router.get("/jobs/{job_id}", response_model=JobRead)
    def get_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        job = db.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        return serialize_job(db, job, attempt_meta=get_job_attempt_meta(db, job))

    @router.get("/jobs/{job_id}/attempts", response_model=list[JobRead])
    def get_job_attempts(
        job_id: str,
        limit: int = Query(default=100, ge=1, le=500),
        db: Session = Depends(get_db),
    ) -> list[JobRead]:
        try:
            return list_job_attempts_read(db, job_id, limit=limit)
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @router.post("/jobs/sync-arxiv", response_model=JobRead)
    def enqueue_sync_arxiv(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_sync_arxiv_job(db, scope)

    @router.post("/jobs/sync-links", response_model=JobRead)
    def enqueue_sync_links(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_job(db, JobType.sync_links, scope)

    @router.post("/jobs/enrich", response_model=JobRead)
    def enqueue_enrich(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_job(db, JobType.enrich, scope)

    @router.post("/jobs/export", response_model=JobRead)
    def enqueue_export(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_job(db, JobType.export, scope)

    @router.post("/jobs/{job_id}/rerun", response_model=JobRead)
    def rerun_existing_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        try:
            return serialize_job(db, rerun_job(db, job_id))
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    app.include_router(router)
