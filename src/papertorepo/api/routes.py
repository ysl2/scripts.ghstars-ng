from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from papertorepo.core.config import get_settings
from papertorepo.db.session import get_db
from papertorepo.jobs.ordering import job_display_order_by
from papertorepo.jobs.queue import (
    create_job,
    get_job_attempt_meta,
    launch_sync_job,
    list_job_attempts_read,
    list_jobs_read,
    rerun_job,
    stop_job,
    serialize_job,
    serialize_jobs,
)
from papertorepo.db.models import ExportRecord, GitHubRepo, Job, JobType, Paper, RepoStableStatus
from papertorepo.api.schemas import (
    DashboardStats,
    ExportRead,
    HealthRead,
    JobLaunchRead,
    JobQueueSummaryRead,
    JobRead,
    PaperRead,
    PaperSummaryRead,
    RepoRead,
    ScopePayload,
)
from papertorepo.core.scope import build_scope_json
from papertorepo.services.pipeline import (
    REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS,
    get_dashboard_stats,
    get_job_queue_snapshot,
    scoped_papers,
    scoped_repos,
)


def _scope_from_query(
    *,
    categories: str | None,
    day: date | None,
    month: str | None,
    from_date: date | None,
    to_date: date | None,
    force: bool = False,
    output_name: str | None = None,
) -> ScopePayload:
    return ScopePayload(
        categories=categories or "",
        day=day,
        month=month,
        **{"from": from_date, "to": to_date},
        force=force,
        output_name=output_name,
    )


def _paper_summary_payload(paper: Paper, *, primary_repo_stars: int | None = None) -> dict[str, object]:
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
        "comment": paper.comment,
        "link_status": state.stable_status if state is not None else RepoStableStatus.unknown,
        "primary_repo_url": state.primary_repo_url if state is not None else None,
        "primary_repo_stars": primary_repo_stars,
        "stable_decided_at": state.stable_decided_at if state is not None else None,
        "refresh_after": state.refresh_after if state is not None else None,
        "last_attempt_at": state.last_attempt_at if state is not None else None,
        "last_attempt_complete": bool(state.last_attempt_complete) if state is not None else False,
        "last_attempt_error": state.last_attempt_error if state is not None else None,
    }


def serialize_paper_summary(paper: Paper, *, primary_repo_stars: int | None = None) -> PaperSummaryRead:
    return PaperSummaryRead(**_paper_summary_payload(paper, primary_repo_stars=primary_repo_stars))


def serialize_paper(paper: Paper, *, primary_repo_stars: int | None = None) -> PaperRead:
    state = paper.repo_state
    return PaperRead(
        **_paper_summary_payload(paper, primary_repo_stars=primary_repo_stars),
        abstract=paper.abstract,
        doi=paper.doi,
        journal_ref=paper.journal_ref,
        repo_urls=state.repo_urls_json if state is not None else [],
    )


def _enqueue_job(db: Session, job_type: JobType, scope: ScopePayload) -> JobRead:
    try:
        return serialize_job(db, create_job(db, job_type, scope))
    except (ValueError, ValidationError) as exc:
        raise _scope_http_exception(exc) from exc


def _launch_sync_job(db: Session, job_type: JobType, scope: ScopePayload) -> JobLaunchRead:
    try:
        job = launch_sync_job(db, job_type, scope)
        return JobLaunchRead(
            disposition="created",
            job=serialize_job(db, job),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
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
            settings.refresh_metadata_github_min_interval
            if github_auth_configured
            else max(
                settings.refresh_metadata_github_min_interval,
                REFRESH_METADATA_GITHUB_ANONYMOUS_REST_MIN_INTERVAL_SECONDS,
            )
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
                "sync_papers": ["arxiv_listing", "arxiv_catchup", "arxiv_submitted_day", "arxiv_id_list"],
                "find_repos": ["paper_comment", "paper_abstract", "alphaxiv_api", "alphaxiv_html", "huggingface_api"],
                "refresh_metadata": ["github_api"],
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
        queue_snapshot = get_job_queue_snapshot(db)
        queue_job_ids = [job_id for job_id in [queue_snapshot.get("current_job_id"), queue_snapshot.get("next_job_id")] if job_id]
        queue_jobs_by_id: dict[str, JobRead] = {}
        if queue_job_ids:
            queue_jobs = list(db.scalars(select(Job).where(Job.id.in_(queue_job_ids))).all())
            queue_jobs_by_id = {job.id: serialize_job(db, job) for job in queue_jobs}
        recent_jobs = list(db.scalars(select(Job).order_by(*job_display_order_by()).limit(12)))
        return DashboardStats(
            **stats,
            job_queue_summary=JobQueueSummaryRead(
                state=str(queue_snapshot["state"]),
                running=stats["running_jobs"],
                pending=stats["pending_jobs"],
                stopping=stats["stopping_jobs"],
                current_job=queue_jobs_by_id.get(str(queue_snapshot["current_job_id"])) if queue_snapshot["current_job_id"] else None,
                next_job=queue_jobs_by_id.get(str(queue_snapshot["next_job_id"])) if queue_snapshot["next_job_id"] else None,
            ),
            recent_jobs=serialize_jobs(db, recent_jobs),
        )

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
        primary_repo_urls = {
            paper.repo_state.primary_repo_url
            for paper in papers
            if paper.repo_state is not None and paper.repo_state.primary_repo_url is not None
        }
        stars_by_url = dict(
            db.execute(
                select(GitHubRepo.normalized_github_url, GitHubRepo.stars).where(
                    GitHubRepo.normalized_github_url.in_(primary_repo_urls)
                )
            ).all()
        )
        rows = [
            serialize_paper_summary(
                paper,
                primary_repo_stars=stars_by_url.get(paper.repo_state.primary_repo_url)
                if paper.repo_state is not None and paper.repo_state.primary_repo_url is not None
                else None,
            )
            for paper in papers
        ]
        if status_filter is not None:
            rows = [paper for paper in rows if paper.link_status == status_filter]
        return rows

    @router.get("/papers/{arxiv_id}", response_model=PaperRead)
    def public_paper(arxiv_id: str, db: Session = Depends(get_db)) -> PaperRead:
        paper = db.scalar(select(Paper).options(selectinload(Paper.repo_state)).where(Paper.arxiv_id == arxiv_id))
        if paper is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Paper not found")
        primary_repo_url = paper.repo_state.primary_repo_url if paper.repo_state is not None else None
        repo = db.get(GitHubRepo, primary_repo_url) if primary_repo_url is not None else None
        return serialize_paper(paper, primary_repo_stars=repo.stars if repo is not None else None)

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

    @router.post("/jobs/sync-papers", response_model=JobLaunchRead)
    def enqueue_sync_papers(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.sync_papers, scope)

    @router.post("/jobs/find-repos", response_model=JobLaunchRead)
    def enqueue_find_repos(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.find_repos, scope)

    @router.post("/jobs/refresh-metadata", response_model=JobLaunchRead)
    def enqueue_refresh_metadata(scope: ScopePayload, db: Session = Depends(get_db)) -> JobLaunchRead:
        return _launch_sync_job(db, JobType.refresh_metadata, scope)

    @router.post("/jobs/export", response_model=JobRead)
    def enqueue_export(scope: ScopePayload, db: Session = Depends(get_db)) -> JobRead:
        return _enqueue_job(db, JobType.export, scope)

    @router.post("/jobs/{job_id}/rerun", response_model=JobRead)
    def rerun_existing_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        try:
            job = rerun_job(db, job_id)
            return serialize_job(db, job, attempt_meta=get_job_attempt_meta(db, job))
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    @router.post("/jobs/{job_id}/stop", response_model=JobRead)
    def stop_existing_job(job_id: str, db: Session = Depends(get_db)) -> JobRead:
        try:
            return serialize_job(db, stop_job(db, job_id))
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    app.include_router(router)
