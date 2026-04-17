from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import aiohttp

from src.ghstars.config import load_config, resolve_categories
from src.ghstars.net.http import build_timeout
from src.ghstars.providers.alphaxiv_links import AlphaXivLinksClient
from src.ghstars.providers.arxiv_links import ArxivLinksClient
from src.ghstars.providers.arxiv_metadata import ArxivMetadataClient
from src.ghstars.providers.github import GitHubClient
from src.ghstars.providers.huggingface_links import HuggingFaceLinksClient
from src.ghstars.storage.db import Database
from src.ghstars.storage.raw_cache import RawCacheStore


PAPER_SYNC_LEASE_TTL_SECONDS = 30.0
PAPER_SYNC_LEASE_HEARTBEAT_SECONDS = 10.0
RESOURCE_LEASE_TTL_SECONDS = 30.0
RESOURCE_LEASE_HEARTBEAT_SECONDS = 10.0


@dataclass(frozen=True)
class ArxivSyncWindow:
    start_date: date | None = None
    end_date: date | None = None

    @property
    def enabled(self) -> bool:
        return self.start_date is not None or self.end_date is not None

    def contains(self, published_date: date | None) -> bool:
        if published_date is None:
            return False
        if self.start_date is not None and published_date < self.start_date:
            return False
        if self.end_date is not None and published_date > self.end_date:
            return False
        return True

    def describe(self) -> str:
        if not self.enabled:
            return "latest"
        if self.start_date == self.end_date and self.start_date is not None:
            return self.start_date.isoformat()
        if self.start_date is None:
            return f"<= {self.end_date.isoformat()}"
        if self.end_date is None:
            return f">= {self.start_date.isoformat()}"
        return f"{self.start_date.isoformat()}..{self.end_date.isoformat()}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scripts.ghstars-ng")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    sync_arxiv = sync_subparsers.add_parser("arxiv")
    sync_arxiv.add_argument("--categories", default=None)
    sync_arxiv.add_argument("--max-results", type=int, default=None)
    sync_arxiv.add_argument("--from", dest="from_date", default=None)
    sync_arxiv.add_argument("--to", dest="to_date", default=None)
    sync_arxiv.add_argument("--day", default=None)
    sync_arxiv.add_argument("--month", default=None)

    sync_links = sync_subparsers.add_parser("links")
    sync_links.add_argument("--categories", default=None)
    sync_links.add_argument("--concurrency", type=int, default=None)
    sync_links.add_argument("--from", dest="from_date", default=None)
    sync_links.add_argument("--to", dest="to_date", default=None)
    sync_links.add_argument("--day", default=None)
    sync_links.add_argument("--month", default=None)

    audit_parser = subparsers.add_parser("audit")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)
    audit_parity = audit_subparsers.add_parser("parity")
    audit_parity.add_argument("--categories", default=None)
    audit_parity.add_argument("--from", dest="from_date", default=None)
    audit_parity.add_argument("--to", dest="to_date", default=None)
    audit_parity.add_argument("--day", default=None)
    audit_parity.add_argument("--month", default=None)

    enrich_parser = subparsers.add_parser("enrich")
    enrich_subparsers = enrich_parser.add_subparsers(dest="enrich_command", required=True)
    enrich_repos = enrich_subparsers.add_parser("repos")
    enrich_repos.add_argument("--categories", default=None)
    enrich_repos.add_argument("--from", dest="from_date", default=None)
    enrich_repos.add_argument("--to", dest="to_date", default=None)
    enrich_repos.add_argument("--day", default=None)
    enrich_repos.add_argument("--month", default=None)

    export_parser = subparsers.add_parser("export")
    export_subparsers = export_parser.add_subparsers(dest="export_command", required=True)
    export_csv = export_subparsers.add_parser("csv")
    export_csv.add_argument("--categories", default=None)
    export_csv.add_argument("--output", required=True)
    export_csv.add_argument("--from", dest="from_date", default=None)
    export_csv.add_argument("--to", dest="to_date", default=None)
    export_csv.add_argument("--day", default=None)
    export_csv.add_argument("--month", default=None)

    return parser


def _resolve_arxiv_sync_window(*, day: str | None, month: str | None, from_date: str | None, to_date: str | None) -> ArxivSyncWindow:
    specified = sum(bool(value) for value in (day, month, from_date, to_date))
    if day and (month or from_date or to_date):
        raise ValueError("--day cannot be combined with --month/--from/--to")
    if month and (day or from_date or to_date):
        raise ValueError("--month cannot be combined with --day/--from/--to")
    if specified == 0:
        return ArxivSyncWindow()
    if day:
        parsed_day = date.fromisoformat(day)
        return ArxivSyncWindow(start_date=parsed_day, end_date=parsed_day)
    if month:
        month_start = datetime.strptime(month, "%Y-%m").date()
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return ArxivSyncWindow(start_date=month_start, end_date=next_month - timedelta(days=1))
    start_date = date.fromisoformat(from_date) if from_date else None
    end_date = date.fromisoformat(to_date) if to_date else None
    if start_date is None and end_date is None:
        return ArxivSyncWindow()
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("--from must be <= --to")
    return ArxivSyncWindow(start_date=start_date, end_date=end_date)


def _resolve_sync_links_concurrency(cli_value: int | None, default: int) -> int:
    value = cli_value if cli_value is not None else default
    if value <= 0:
        raise ValueError("--concurrency must be >= 1")
    return value


async def async_main(argv: list[str] | None = None) -> int:
    from src.ghstars.commands.audit_parity import _run_audit_parity
    from src.ghstars.commands.enrich_repos import _run_enrich_repos
    from src.ghstars.commands.export_csv import _run_export_csv
    from src.ghstars.commands.sync_arxiv import _run_sync_arxiv
    from src.ghstars.commands.sync_links import _run_sync_links

    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config()
    categories = resolve_categories(getattr(args, "categories", None), config.default_categories)

    database = Database(config.db_path)
    raw_cache = RawCacheStore(config.raw_dir)
    try:
        async with aiohttp.ClientSession(timeout=build_timeout()) as session:
            arxiv_metadata = ArxivMetadataClient(session, min_interval=config.arxiv_api_min_interval)
            arxiv_links = ArxivLinksClient(session, min_interval=config.arxiv_api_min_interval)
            huggingface = HuggingFaceLinksClient(
                session,
                huggingface_token=config.huggingface_token,
                min_interval=config.huggingface_min_interval,
            )
            alphaxiv = AlphaXivLinksClient(
                session,
                alphaxiv_token=config.alphaxiv_token,
                min_interval=0.5,
            )
            github = GitHubClient(session, github_token=config.github_token, min_interval=config.github_min_interval)

            if args.command == "sync" and args.sync_command == "arxiv":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_sync_arxiv(
                    database,
                    raw_cache,
                    arxiv_metadata,
                    categories,
                    max_results=args.max_results,
                    window=window,
                )
                return 0
            if args.command == "sync" and args.sync_command == "links":
                concurrency = _resolve_sync_links_concurrency(args.concurrency, config.sync_links_concurrency)
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_sync_links(
                    database,
                    raw_cache,
                    arxiv_links,
                    huggingface,
                    alphaxiv,
                    categories,
                    concurrency=concurrency,
                    window=window,
                )
                return 0
            if args.command == "audit" and args.audit_command == "parity":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                _run_audit_parity(database, categories, window=window)
                return 0
            if args.command == "enrich" and args.enrich_command == "repos":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await _run_enrich_repos(database, github, categories, window=window)
                return 0
            if args.command == "export" and args.export_command == "csv":
                window = _resolve_arxiv_sync_window(
                    day=args.day,
                    month=args.month,
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                _run_export_csv(database, categories, Path(args.output), window=window)
                return 0
    finally:
        database.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))
