from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime
from typing import Any

from pydantic import ValidationError
import uvicorn
from sqlalchemy import select

from src.ghstarsv2.api import serialize_paper
from src.ghstarsv2.db import session_scope
from src.ghstarsv2.jobs import init_database, rerun_job, run_worker_forever, serialize_jobs
from src.ghstarsv2.models import ExportRecord, GitHubRepo, Job, JobType
from src.ghstarsv2.schemas import ExportRead, RepoRead, ScopePayload, validate_scope_for_job
from src.ghstarsv2.scope import build_scope_json
from src.ghstarsv2.services import run_enrich, run_export, run_sync_arxiv, run_sync_links, scoped_papers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ghstars")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--reload", action="store_true")

    subparsers.add_parser("migrate")
    subparsers.add_parser("worker")

    sync_arxiv = subparsers.add_parser("sync-arxiv")
    _add_scope_arguments(sync_arxiv, require_categories=True)
    sync_arxiv.add_argument("--max-results", type=int, default=None)
    sync_arxiv.add_argument("--force", action="store_true")

    sync_links = subparsers.add_parser("sync-links")
    _add_scope_arguments(sync_links, require_categories=True)
    sync_links.add_argument("--force", action="store_true")

    enrich = subparsers.add_parser("enrich")
    _add_scope_arguments(enrich, require_categories=True)

    export = subparsers.add_parser("export")
    _add_scope_arguments(export)
    export.add_argument("--output", default=None)

    jobs = subparsers.add_parser("jobs")
    jobs.add_argument("--limit", type=int, default=50)
    jobs.add_argument("action", nargs="?", choices=["rerun"])
    jobs.add_argument("job_id", nargs="?")

    papers = subparsers.add_parser("papers")
    _add_scope_arguments(papers)
    papers.add_argument("--limit", type=int, default=50)

    repos = subparsers.add_parser("repos")
    repos.add_argument("--limit", type=int, default=50)

    subparsers.add_parser("exports")
    return parser


def _add_scope_arguments(parser: argparse.ArgumentParser, *, require_categories: bool = False) -> None:
    parser.add_argument("--categories", required=require_categories, default=None)
    parser.add_argument("--day", default=None)
    parser.add_argument("--month", default=None)
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)


def _scope_error_detail(exc: ValueError | ValidationError) -> str:
    if isinstance(exc, ValidationError):
        errors = exc.errors()
        if errors:
            message = str(errors[0].get("msg") or "Invalid scope")
            return message.removeprefix("Value error, ")
        return "Invalid scope"
    return str(exc)


def _build_scope(args: argparse.Namespace) -> dict[str, Any]:
    try:
        scope = ScopePayload(
            categories=args.categories or "",
            day=args.day,
            month=args.month,
            **{"from": getattr(args, "from_date", None), "to": getattr(args, "to_date", None)},
            max_results=getattr(args, "max_results", None),
            force=bool(getattr(args, "force", False)),
            output_name=getattr(args, "output", None),
        )
        job_type = {
            "sync-arxiv": JobType.sync_arxiv,
            "sync-links": JobType.sync_links,
            "enrich": JobType.enrich,
            "export": JobType.export,
        }.get(getattr(args, "command", ""))
        if job_type is not None:
            validate_scope_for_job(scope, job_type)
        return build_scope_json(scope)
    except (ValueError, ValidationError) as exc:
        raise ValueError(_scope_error_detail(exc)) from exc


def _build_scope_or_exit(args: argparse.Namespace) -> dict[str, Any]:
    try:
        return _build_scope(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=_json_default))


async def async_main_from_args(args: argparse.Namespace) -> int:
    init_database()

    if args.command == "worker":
        await run_worker_forever()
        return 0

    if args.command == "migrate":
        return 0

    if args.command == "sync-arxiv":
        with session_scope() as db:
            _print_json(await run_sync_arxiv(db, _build_scope_or_exit(args)))
        return 0

    if args.command == "sync-links":
        with session_scope() as db:
            _print_json(await run_sync_links(db, _build_scope_or_exit(args)))
        return 0

    if args.command == "enrich":
        with session_scope() as db:
            _print_json(await run_enrich(db, _build_scope_or_exit(args)))
        return 0

    if args.command == "export":
        with session_scope() as db:
            _print_json(run_export(db, _build_scope_or_exit(args)))
        return 0

    if args.command == "jobs":
        with session_scope() as db:
            if args.action == "rerun":
                if not args.job_id:
                    raise SystemExit("jobs rerun requires a job_id")
                _print_json(serialize_jobs(db, [rerun_job(db, args.job_id)])[0].model_dump(mode="json"))
                return 0
            rows = [item.model_dump(mode="json") for item in serialize_jobs(db, list(db.scalars(select(Job).order_by(Job.created_at.desc()).limit(args.limit)).all()))]
        _print_json(rows)
        return 0

    if args.command == "papers":
        with session_scope() as db:
            rows = [serialize_paper(item).model_dump(mode="json") for item in scoped_papers(db, _build_scope_or_exit(args), limit=args.limit)]
        _print_json(rows)
        return 0

    if args.command == "repos":
        with session_scope() as db:
            stmt = select(GitHubRepo).order_by(GitHubRepo.stars.desc().nullslast(), GitHubRepo.checked_at.desc().nullslast()).limit(args.limit)
            rows = [RepoRead.model_validate(item).model_dump(mode="json") for item in db.scalars(stmt).all()]
        _print_json(rows)
        return 0

    if args.command == "exports":
        with session_scope() as db:
            rows = [ExportRead.model_validate(item).model_dump(mode="json") for item in db.scalars(select(ExportRecord).order_by(ExportRecord.created_at.desc())).all()]
        _print_json(rows)
        return 0

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        uvicorn.run("src.ghstarsv2.main:app", host=args.host, port=args.port, reload=args.reload)
        return 0
    return asyncio.run(async_main_from_args(args))
