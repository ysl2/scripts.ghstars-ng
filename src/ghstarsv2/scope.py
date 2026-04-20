from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from src.ghstarsv2.schemas import ScopePayload


@dataclass(frozen=True)
class ScopeWindow:
    start_date: date | None = None
    end_date: date | None = None

    @property
    def enabled(self) -> bool:
        return self.start_date is not None or self.end_date is not None


def resolve_categories(scope: ScopePayload) -> list[str]:
    if scope.categories:
        return sorted({item.strip() for item in scope.categories if item.strip()})
    return []


def normalize_paper_ids(scope: ScopePayload) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in scope.paper_ids:
        value = raw.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def resolve_window(scope: ScopePayload) -> ScopeWindow:
    if scope.day:
        return ScopeWindow(start_date=scope.day, end_date=scope.day)
    if scope.month:
        month_start = datetime.strptime(scope.month, "%Y-%m").date()
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return ScopeWindow(start_date=month_start, end_date=next_month - timedelta(days=1))
    return ScopeWindow(start_date=scope.from_date, end_date=scope.to_date)


def build_scope_payload(scope_json: dict[str, object]) -> ScopePayload:
    return ScopePayload.model_validate(scope_json)


def resolve_window_from_scope_json(scope_json: dict[str, object]) -> ScopeWindow:
    return resolve_window(build_scope_payload(scope_json))


def resolve_categories_from_scope_json(scope_json: dict[str, object]) -> list[str]:
    payload = build_scope_payload(scope_json)
    return resolve_categories(payload)


def _next_month_start(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def month_label(value: date) -> str:
    return value.strftime("%Y-%m")


def month_windows_between(start_date: date, end_date: date) -> list[ScopeWindow]:
    current = month_start(start_date)
    windows: list[ScopeWindow] = []
    while current <= end_date:
        month_start = current
        month_end = _next_month_start(month_start) - timedelta(days=1)
        windows.append(
            ScopeWindow(
                start_date=max(start_date, month_start),
                end_date=min(end_date, month_end),
            )
        )
        current = _next_month_start(month_start)
    return windows


def resolve_archive_months(scope: ScopePayload) -> list[date]:
    window = resolve_window(scope)
    if window.start_date is None or window.end_date is None:
        return []
    current = month_start(window.start_date)
    months: list[date] = []
    while current <= window.end_date:
        months.append(current)
        current = _next_month_start(current)
    return months


def resolve_archive_months_from_scope_json(scope_json: dict[str, object]) -> list[date]:
    return resolve_archive_months(build_scope_payload(scope_json))


def arxiv_scope_spans_multiple_months(scope_json: dict[str, object]) -> bool:
    return len(resolve_archive_months_from_scope_json(scope_json)) > 1


def expand_arxiv_child_scope_jsons(scope_json: dict[str, object]) -> list[dict[str, Any]]:
    categories = resolve_categories_from_scope_json(scope_json)
    archive_months = resolve_archive_months_from_scope_json(scope_json)
    if not categories or not archive_months:
        return []

    max_results = scope_json.get("max_results")
    force = bool(scope_json.get("force"))
    child_scopes: list[dict[str, Any]] = []
    for category in categories:
        for archive_month in archive_months:
            child_scopes.append(
                build_scope_json(
                    ScopePayload(
                        categories=[category],
                        month=month_label(archive_month),
                        max_results=max_results if isinstance(max_results, int) else None,
                        force=force,
                    )
                )
            )
    return child_scopes


def build_scope_json(scope: ScopePayload) -> dict[str, object]:
    categories = resolve_categories(scope)
    window = resolve_window(scope)
    paper_ids = normalize_paper_ids(scope)
    return {
        "categories": categories,
        "day": scope.day.isoformat() if scope.day else None,
        "month": scope.month,
        "from": None if scope.day or scope.month else window.start_date.isoformat() if window.start_date else None,
        "to": None if scope.day or scope.month else window.end_date.isoformat() if window.end_date else None,
        "max_results": scope.max_results,
        "force": scope.force,
        "export_mode": scope.export_mode,
        "paper_ids": paper_ids,
        "output_name": scope.output_name,
    }


def build_dedupe_key(job_type: str, scope_json: dict[str, object]) -> str:
    payload = json.dumps({"job_type": job_type, "scope": scope_json}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
