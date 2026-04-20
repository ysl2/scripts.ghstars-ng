from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.ghstars.models import (
    GitHubRepoMetadata,
    Paper,
    PaperLinkSyncState,
    PaperRepoLink,
    PaperSyncLease,
    RawCacheEntry,
    RepoObservation,
    ResourceLease,
)

SCHEMA_VERSION = 4


class LeaseLostError(RuntimeError):
    pass


class Database:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self._configure_connection()
        self._initialize_schema()

    def close(self) -> None:
        self.connection.close()

    def upsert_raw_cache(
        self,
        *,
        provider: str,
        surface: str,
        request_key: str,
        request_url: str,
        content_type: str | None,
        status_code: int,
        body_path: Path,
        content_hash: str,
        etag: str | None,
        last_modified: str | None,
    ) -> RawCacheEntry:
        fetched_at = _utc_now()
        with self._write_transaction():
            self.connection.execute(
                """
                INSERT INTO raw_cache (
                    provider, surface, request_key, request_url, content_type,
                    status_code, body_path, content_hash, fetched_at, etag, last_modified
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, surface, request_key) DO UPDATE SET
                    request_url = excluded.request_url,
                    content_type = excluded.content_type,
                    status_code = excluded.status_code,
                    body_path = excluded.body_path,
                    content_hash = excluded.content_hash,
                    fetched_at = excluded.fetched_at,
                    etag = excluded.etag,
                    last_modified = excluded.last_modified
                """,
                (
                    provider,
                    surface,
                    request_key,
                    request_url,
                    content_type,
                    status_code,
                    str(body_path),
                    content_hash,
                    fetched_at,
                    etag,
                    last_modified,
                ),
            )
        row = self.connection.execute(
            "SELECT * FROM raw_cache WHERE provider = ? AND surface = ? AND request_key = ?",
            (provider, surface, request_key),
        ).fetchone()
        return _to_raw_cache_entry(row)

    def get_raw_cache(self, provider: str, surface: str, request_key: str) -> RawCacheEntry | None:
        row = self._fetchone(
            "SELECT * FROM raw_cache WHERE provider = ? AND surface = ? AND request_key = ?",
            (provider, surface, request_key),
        )
        return _to_raw_cache_entry(row)

    def get_raw_cache_by_id(self, raw_cache_id: int) -> RawCacheEntry | None:
        row = self._fetchone(
            "SELECT * FROM raw_cache WHERE id = ?",
            (raw_cache_id,),
        )
        return _to_raw_cache_entry(row)

    def upsert_paper(self, paper: Paper) -> None:
        with self._write_transaction():
            self._upsert_paper_record(paper)

    def upsert_paper_source(
        self,
        *,
        arxiv_id: str,
        provider: str,
        surface: str,
        raw_cache_id: int | None,
        data: dict[str, Any],
    ) -> None:
        with self._write_transaction():
            self._insert_paper_source(
                arxiv_id=arxiv_id,
                provider=provider,
                surface=surface,
                raw_cache_id=raw_cache_id,
                data=data,
            )

    def persist_paper_with_source(
        self,
        paper: Paper,
        *,
        provider: str,
        surface: str,
        raw_cache_id: int | None,
        data: dict[str, Any],
    ) -> None:
        with self._write_transaction():
            self._upsert_paper_record(paper)
            self._insert_paper_source(
                arxiv_id=paper.arxiv_id,
                provider=provider,
                surface=surface,
                raw_cache_id=raw_cache_id,
                data=data,
            )

    def replace_repo_observations(
        self,
        *,
        arxiv_id: str,
        provider: str,
        surface: str,
        observations: list[dict[str, Any]],
        lease_owner_id: str | None = None,
        lease_token: str | None = None,
    ) -> None:
        with self._write_transaction():
            self._require_active_paper_sync_lease(
                arxiv_id,
                owner_id=lease_owner_id,
                lease_token=lease_token,
            )
            self.connection.execute(
                "DELETE FROM repo_observations WHERE arxiv_id = ? AND provider = ? AND surface = ?",
                (arxiv_id, provider, surface),
            )
            self.connection.executemany(
                """
                INSERT INTO repo_observations (
                    arxiv_id, provider, surface, status, observed_repo_url,
                    normalized_repo_url, evidence_text, raw_cache_id,
                    extractor_version, error_message, observed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        arxiv_id,
                        provider,
                        surface,
                        item["status"],
                        item.get("observed_repo_url"),
                        item.get("normalized_repo_url"),
                        item.get("evidence_text"),
                        item.get("raw_cache_id"),
                        item.get("extractor_version") or "1",
                        item.get("error_message"),
                        item.get("observed_at") or _utc_now(),
                    )
                    for item in observations
                ],
            )

    def list_repo_observations(self, arxiv_id: str) -> list[RepoObservation]:
        rows = self._fetchall(
            "SELECT * FROM repo_observations WHERE arxiv_id = ? ORDER BY provider, surface, id",
            (arxiv_id,),
        )
        return [_to_repo_observation(row) for row in rows]

    def list_surface_repo_observations(self, arxiv_id: str, provider: str, surface: str) -> list[RepoObservation]:
        rows = self._fetchall(
            """
            SELECT *
            FROM repo_observations
            WHERE arxiv_id = ? AND provider = ? AND surface = ?
            ORDER BY id
            """,
            (arxiv_id, provider, surface),
        )
        return [_to_repo_observation(row) for row in rows]

    def get_latest_repo_observation(self, arxiv_id: str, provider: str, surface: str) -> RepoObservation | None:
        rows = self.list_surface_repo_observations(arxiv_id, provider, surface)
        return rows[-1] if rows else None

    def replace_paper_repo_links(
        self,
        arxiv_id: str,
        links: list[dict[str, Any]],
        *,
        lease_owner_id: str | None = None,
        lease_token: str | None = None,
    ) -> None:
        with self._write_transaction():
            self._require_active_paper_sync_lease(
                arxiv_id,
                owner_id=lease_owner_id,
                lease_token=lease_token,
            )
            self.connection.execute("DELETE FROM paper_repo_links WHERE arxiv_id = ?", (arxiv_id,))
            self.connection.executemany(
                """
                INSERT INTO paper_repo_links (
                    arxiv_id, normalized_repo_url, status, providers_json, surfaces_json,
                    provider_count, surface_count, is_primary, resolved_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        arxiv_id,
                        item["normalized_repo_url"],
                        item["status"],
                        json.dumps(sorted(item["providers"]), ensure_ascii=False),
                        json.dumps(sorted(item["surfaces"]), ensure_ascii=False),
                        item["provider_count"],
                        item["surface_count"],
                        1 if item.get("is_primary") else 0,
                        item.get("resolved_at") or _utc_now(),
                    )
                    for item in links
                ],
            )

    def upsert_paper_link_sync_state(
        self,
        arxiv_id: str,
        status: str,
        *,
        checked_at: str | None = None,
        lease_owner_id: str | None = None,
        lease_token: str | None = None,
    ) -> None:
        with self._write_transaction():
            self._require_active_paper_sync_lease(
                arxiv_id,
                owner_id=lease_owner_id,
                lease_token=lease_token,
            )
            self.connection.execute(
                """
                INSERT INTO paper_link_sync_state (arxiv_id, status, checked_at)
                VALUES (?, ?, ?)
                ON CONFLICT(arxiv_id) DO UPDATE SET
                    status = excluded.status,
                    checked_at = excluded.checked_at
                """,
                (arxiv_id, status, checked_at or _utc_now()),
            )

    def get_paper_link_sync_state(self, arxiv_id: str) -> PaperLinkSyncState | None:
        row = self._fetchone(
            "SELECT * FROM paper_link_sync_state WHERE arxiv_id = ?",
            (arxiv_id,),
        )
        if row is None:
            return None
        return _to_paper_link_sync_state(row)

    def list_paper_repo_links(self, arxiv_id: str) -> list[PaperRepoLink]:
        rows = self._fetchall(
            "SELECT * FROM paper_repo_links WHERE arxiv_id = ? ORDER BY is_primary DESC, normalized_repo_url ASC",
            (arxiv_id,),
        )
        return [_to_paper_repo_link(row) for row in rows]

    def upsert_github_repo(self, metadata: GitHubRepoMetadata) -> None:
        with self._write_transaction():
            self.connection.execute(
                """
                INSERT INTO github_repos (
                    normalized_github_url, owner, repo, stars, created_at, description, checked_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(normalized_github_url) DO UPDATE SET
                    owner = excluded.owner,
                    repo = excluded.repo,
                    stars = excluded.stars,
                    created_at = COALESCE(github_repos.created_at, excluded.created_at),
                    description = excluded.description,
                    checked_at = excluded.checked_at
                """,
                (
                    metadata.normalized_github_url,
                    metadata.owner,
                    metadata.repo,
                    metadata.stars,
                    metadata.created_at,
                    metadata.description,
                    metadata.checked_at or _utc_now(),
                ),
            )

    def get_github_repo(self, normalized_github_url: str) -> GitHubRepoMetadata | None:
        row = self._fetchone(
            "SELECT * FROM github_repos WHERE normalized_github_url = ?",
            (normalized_github_url,),
        )
        if row is None:
            return None
        return GitHubRepoMetadata(
            normalized_github_url=row["normalized_github_url"],
            owner=row["owner"],
            repo=row["repo"],
            stars=row["stars"],
            created_at=row["created_at"],
            description=row["description"],
            checked_at=row["checked_at"],
        )

    def list_papers_by_categories(
        self,
        categories: tuple[str, ...],
        *,
        published_from: str | None = None,
        published_to: str | None = None,
    ) -> list[Paper]:
        filters: list[str] = []
        params: list[str] = []
        join_clause = ""
        distinct_prefix = ""

        if categories:
            placeholders = ",".join("?" for _ in categories)
            join_clause = "JOIN paper_categories c ON c.arxiv_id = p.arxiv_id"
            filters.append(f"c.category IN ({placeholders})")
            params.extend(categories)
            distinct_prefix = "DISTINCT "

        if published_from is not None:
            filters.append("p.published_at IS NOT NULL")
            filters.append("p.published_at >= ?")
            params.append(published_from)
        if published_to is not None:
            filters.append("p.published_at IS NOT NULL")
            filters.append("p.published_at <= ?")
            params.append(published_to)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = self._fetchall(
            f"""
            SELECT {distinct_prefix}p.*
            FROM papers p
            {join_clause}
            {where_clause}
            ORDER BY p.published_at DESC, p.arxiv_id DESC
            """,
            tuple(params),
        )
        return [_to_paper(row) for row in rows]

    def get_paper(self, arxiv_id: str) -> Paper | None:
        row = self._fetchone(
            "SELECT * FROM papers WHERE arxiv_id = ?",
            (arxiv_id,),
        )
        return _to_paper(row)

    def set_sync_state(
        self,
        stream_name: str,
        cursor: str | None,
        *,
        lease_owner_id: str | None = None,
        lease_token: str | None = None,
    ) -> None:
        now = _utc_now()
        with self._write_transaction():
            self._require_active_resource_lease(
                stream_name,
                owner_id=lease_owner_id,
                lease_token=lease_token,
            )
            self.connection.execute(
                """
                INSERT INTO sync_state (stream_name, cursor, last_run_at, last_success_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stream_name) DO UPDATE SET
                    cursor = excluded.cursor,
                    last_run_at = excluded.last_run_at,
                    last_success_at = excluded.last_success_at
                """,
                (stream_name, cursor, now, now),
            )

    def get_sync_state(self, stream_name: str) -> str | None:
        row = self._fetchone(
            "SELECT cursor FROM sync_state WHERE stream_name = ?",
            (stream_name,),
        )
        if row is None:
            return None
        return row["cursor"]

    def try_acquire_paper_sync_lease(
        self,
        arxiv_id: str,
        *,
        owner_id: str,
        lease_token: str,
        lease_ttl_seconds: float,
    ) -> PaperSyncLease | None:
        now = _utc_now()
        expires_at = _utc_now_after(lease_ttl_seconds)
        with self._write_transaction():
            row = self.connection.execute(
                "SELECT * FROM paper_sync_leases WHERE arxiv_id = ?",
                (arxiv_id,),
            ).fetchone()
            if row is None:
                self.connection.execute(
                    """
                    INSERT INTO paper_sync_leases (
                        arxiv_id, owner_id, lease_token, acquired_at, heartbeat_at, lease_expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (arxiv_id, owner_id, lease_token, now, now, expires_at),
                )
            else:
                current = _to_paper_sync_lease(row)
                if current.owner_id == owner_id and current.lease_token == lease_token:
                    self.connection.execute(
                        """
                        UPDATE paper_sync_leases
                        SET heartbeat_at = ?, lease_expires_at = ?
                        WHERE arxiv_id = ? AND owner_id = ? AND lease_token = ?
                        """,
                        (now, expires_at, arxiv_id, owner_id, lease_token),
                    )
                elif _is_lease_expired(current.lease_expires_at):
                    self.connection.execute(
                        """
                        UPDATE paper_sync_leases
                        SET owner_id = ?, lease_token = ?, acquired_at = ?, heartbeat_at = ?, lease_expires_at = ?
                        WHERE arxiv_id = ?
                        """,
                        (owner_id, lease_token, now, now, expires_at, arxiv_id),
                    )
                else:
                    return None
        return self.get_paper_sync_lease(arxiv_id)

    def get_paper_sync_lease(self, arxiv_id: str) -> PaperSyncLease | None:
        row = self.connection.execute(
            "SELECT * FROM paper_sync_leases WHERE arxiv_id = ?",
            (arxiv_id,),
        ).fetchone()
        return _to_paper_sync_lease(row)

    def renew_paper_sync_lease(
        self,
        arxiv_id: str,
        *,
        owner_id: str,
        lease_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        now = _utc_now()
        expires_at = _utc_now_after(lease_ttl_seconds)
        with self._write_transaction():
            row = self.connection.execute(
                "SELECT owner_id, lease_token, lease_expires_at FROM paper_sync_leases WHERE arxiv_id = ?",
                (arxiv_id,),
            ).fetchone()
            if row is None:
                return False
            if row["owner_id"] != owner_id or row["lease_token"] != lease_token:
                return False
            if _is_lease_expired(row["lease_expires_at"]):
                return False
            updated = self.connection.execute(
                """
                UPDATE paper_sync_leases
                SET heartbeat_at = ?, lease_expires_at = ?
                WHERE arxiv_id = ? AND owner_id = ? AND lease_token = ?
                """,
                (now, expires_at, arxiv_id, owner_id, lease_token),
            ).rowcount
        return bool(updated)

    def release_paper_sync_lease(self, arxiv_id: str, *, owner_id: str, lease_token: str) -> bool:
        with self._write_transaction():
            deleted = self.connection.execute(
                "DELETE FROM paper_sync_leases WHERE arxiv_id = ? AND owner_id = ? AND lease_token = ?",
                (arxiv_id, owner_id, lease_token),
            ).rowcount
        return bool(deleted)

    def validate_paper_sync_lease(self, arxiv_id: str, *, owner_id: str, lease_token: str) -> bool:
        row = self.connection.execute(
            "SELECT owner_id, lease_token, lease_expires_at FROM paper_sync_leases WHERE arxiv_id = ?",
            (arxiv_id,),
        ).fetchone()
        if row is None:
            return False
        if row["owner_id"] != owner_id or row["lease_token"] != lease_token:
            return False
        return not _is_lease_expired(row["lease_expires_at"])

    def try_acquire_resource_lease(
        self,
        resource_key: str,
        *,
        owner_id: str,
        lease_token: str,
        lease_ttl_seconds: float,
    ) -> ResourceLease | None:
        now = _utc_now()
        expires_at = _utc_now_after(lease_ttl_seconds)
        with self._write_transaction():
            row = self.connection.execute(
                "SELECT * FROM resource_leases WHERE resource_key = ?",
                (resource_key,),
            ).fetchone()
            if row is None:
                self.connection.execute(
                    """
                    INSERT INTO resource_leases (
                        resource_key, owner_id, lease_token, acquired_at, heartbeat_at, lease_expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (resource_key, owner_id, lease_token, now, now, expires_at),
                )
            else:
                current = _to_resource_lease(row)
                if current.owner_id == owner_id and current.lease_token == lease_token:
                    self.connection.execute(
                        """
                        UPDATE resource_leases
                        SET heartbeat_at = ?, lease_expires_at = ?
                        WHERE resource_key = ? AND owner_id = ? AND lease_token = ?
                        """,
                        (now, expires_at, resource_key, owner_id, lease_token),
                    )
                elif _is_lease_expired(current.lease_expires_at):
                    self.connection.execute(
                        """
                        UPDATE resource_leases
                        SET owner_id = ?, lease_token = ?, acquired_at = ?, heartbeat_at = ?, lease_expires_at = ?
                        WHERE resource_key = ?
                        """,
                        (owner_id, lease_token, now, now, expires_at, resource_key),
                    )
                else:
                    return None
        return self.get_resource_lease(resource_key)

    def get_resource_lease(self, resource_key: str) -> ResourceLease | None:
        row = self.connection.execute(
            "SELECT * FROM resource_leases WHERE resource_key = ?",
            (resource_key,),
        ).fetchone()
        return _to_resource_lease(row)

    def renew_resource_lease(
        self,
        resource_key: str,
        *,
        owner_id: str,
        lease_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        now = _utc_now()
        expires_at = _utc_now_after(lease_ttl_seconds)
        with self._write_transaction():
            row = self.connection.execute(
                "SELECT owner_id, lease_token, lease_expires_at FROM resource_leases WHERE resource_key = ?",
                (resource_key,),
            ).fetchone()
            if row is None:
                return False
            if row["owner_id"] != owner_id or row["lease_token"] != lease_token:
                return False
            if _is_lease_expired(row["lease_expires_at"]):
                return False
            updated = self.connection.execute(
                """
                UPDATE resource_leases
                SET heartbeat_at = ?, lease_expires_at = ?
                WHERE resource_key = ? AND owner_id = ? AND lease_token = ?
                """,
                (now, expires_at, resource_key, owner_id, lease_token),
            ).rowcount
        return bool(updated)

    def release_resource_lease(self, resource_key: str, *, owner_id: str, lease_token: str) -> bool:
        with self._write_transaction():
            deleted = self.connection.execute(
                "DELETE FROM resource_leases WHERE resource_key = ? AND owner_id = ? AND lease_token = ?",
                (resource_key, owner_id, lease_token),
            ).rowcount
        return bool(deleted)

    def validate_resource_lease(self, resource_key: str, *, owner_id: str, lease_token: str) -> bool:
        row = self.connection.execute(
            "SELECT owner_id, lease_token, lease_expires_at FROM resource_leases WHERE resource_key = ?",
            (resource_key,),
        ).fetchone()
        if row is None:
            return False
        if row["owner_id"] != owner_id or row["lease_token"] != lease_token:
            return False
        return not _is_lease_expired(row["lease_expires_at"])

    def _require_active_paper_sync_lease(self, arxiv_id: str, *, owner_id: str | None, lease_token: str | None) -> None:
        if owner_id is None or lease_token is None:
            return
        if not self.validate_paper_sync_lease(arxiv_id, owner_id=owner_id, lease_token=lease_token):
            raise LeaseLostError(f"{arxiv_id}: paper lease lost")

    def _require_active_resource_lease(self, resource_key: str, *, owner_id: str | None, lease_token: str | None) -> None:
        if owner_id is None or lease_token is None:
            return
        if not self.validate_resource_lease(resource_key, owner_id=owner_id, lease_token=lease_token):
            raise LeaseLostError(f"{resource_key}: resource lease lost")

    def _configure_connection(self) -> None:
        self.connection.execute("PRAGMA busy_timeout = 30000")
        try:
            self.connection.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            pass
        try:
            self.connection.execute("PRAGMA synchronous = NORMAL")
        except sqlite3.OperationalError:
            pass

    def _initialize_schema(self) -> None:
        with self.connection:
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            self._create_base_tables()
            current_version = self._get_schema_version()
            if current_version < 2:
                self._migrate_to_v2()
            if current_version < 3:
                self._migrate_to_v3()
            if current_version < 4:
                self._migrate_to_v4()
            self.connection.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(SCHEMA_VERSION),),
            )

    def _create_base_tables(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                arxiv_id TEXT PRIMARY KEY,
                abs_url TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                abstract TEXT NOT NULL,
                published_at TEXT,
                updated_at TEXT,
                authors_json TEXT NOT NULL,
                categories_json TEXT NOT NULL,
                comment TEXT,
                primary_category TEXT,
                source_first_seen_at TEXT NOT NULL,
                source_last_seen_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_categories (
                arxiv_id TEXT NOT NULL,
                category TEXT NOT NULL,
                PRIMARY KEY (arxiv_id, category),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                surface TEXT NOT NULL,
                request_key TEXT NOT NULL,
                request_url TEXT NOT NULL,
                content_type TEXT,
                status_code INTEGER NOT NULL,
                body_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                etag TEXT,
                last_modified TEXT,
                UNIQUE(provider, surface, request_key)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arxiv_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                surface TEXT NOT NULL,
                raw_cache_id INTEGER,
                data_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE,
                FOREIGN KEY (raw_cache_id) REFERENCES raw_cache(id) ON DELETE SET NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS repo_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arxiv_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                surface TEXT NOT NULL,
                status TEXT NOT NULL,
                observed_repo_url TEXT,
                normalized_repo_url TEXT,
                evidence_text TEXT,
                raw_cache_id INTEGER,
                extractor_version TEXT NOT NULL,
                error_message TEXT,
                observed_at TEXT NOT NULL,
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE,
                FOREIGN KEY (raw_cache_id) REFERENCES raw_cache(id) ON DELETE SET NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_repo_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arxiv_id TEXT NOT NULL,
                normalized_repo_url TEXT NOT NULL,
                status TEXT NOT NULL,
                providers_json TEXT NOT NULL,
                surfaces_json TEXT NOT NULL,
                provider_count INTEGER NOT NULL,
                surface_count INTEGER NOT NULL,
                is_primary INTEGER NOT NULL DEFAULT 0,
                resolved_at TEXT NOT NULL,
                UNIQUE (arxiv_id, normalized_repo_url),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS github_repos (
                normalized_github_url TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                repo TEXT NOT NULL,
                stars INTEGER,
                created_at TEXT,
                description TEXT,
                checked_at TEXT
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_link_sync_state (
                arxiv_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                stream_name TEXT PRIMARY KEY,
                cursor TEXT,
                last_run_at TEXT,
                last_success_at TEXT
            )
            """
        )

    def _get_schema_version(self) -> int:
        row = self.connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return 1
        try:
            return int(row["value"])
        except (TypeError, ValueError):
            return 1

    def _migrate_to_v2(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_sync_leases (
                arxiv_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                lease_token TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_sync_leases_expires_at ON paper_sync_leases (lease_expires_at)"
        )

    def _migrate_to_v3(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS resource_leases (
                resource_key TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                lease_token TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_resource_leases_expires_at ON resource_leases (lease_expires_at)"
        )

    def _migrate_to_v4(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_link_sync_state (
                arxiv_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id) ON DELETE CASCADE
            )
            """
        )

    @contextmanager
    def snapshot_reads(self):
        self.connection.execute("BEGIN")
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.rollback()

    @contextmanager
    def _write_transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def _upsert_paper_record(self, paper: Paper) -> None:
        self.connection.execute(
            """
            INSERT INTO papers (
                arxiv_id, abs_url, title, abstract, published_at, updated_at,
                authors_json, categories_json, comment, primary_category,
                source_first_seen_at, source_last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(arxiv_id) DO UPDATE SET
                abs_url = excluded.abs_url,
                title = excluded.title,
                abstract = excluded.abstract,
                published_at = excluded.published_at,
                updated_at = excluded.updated_at,
                authors_json = excluded.authors_json,
                categories_json = excluded.categories_json,
                comment = excluded.comment,
                primary_category = excluded.primary_category,
                source_last_seen_at = excluded.source_last_seen_at
            """,
            (
                paper.arxiv_id,
                paper.abs_url,
                paper.title,
                paper.abstract,
                paper.published_at,
                paper.updated_at,
                json.dumps(list(paper.authors), ensure_ascii=False),
                json.dumps(list(paper.categories), ensure_ascii=False),
                paper.comment,
                paper.primary_category,
                _utc_now(),
                _utc_now(),
            ),
        )
        self.connection.execute(
            "DELETE FROM paper_categories WHERE arxiv_id = ?",
            (paper.arxiv_id,),
        )
        self.connection.executemany(
            "INSERT INTO paper_categories (arxiv_id, category) VALUES (?, ?)",
            [(paper.arxiv_id, category) for category in paper.categories],
        )

    def _insert_paper_source(
        self,
        *,
        arxiv_id: str,
        provider: str,
        surface: str,
        raw_cache_id: int | None,
        data: dict[str, Any],
    ) -> None:
        fetched_at = _utc_now()
        self.connection.execute(
            """
            INSERT INTO paper_sources (arxiv_id, provider, surface, raw_cache_id, data_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (arxiv_id, provider, surface, raw_cache_id, json.dumps(data, ensure_ascii=False), fetched_at),
        )

    def _fetchall(self, query: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        return self.connection.execute(query, params).fetchall()

    def _fetchone(self, query: str, params: tuple[object, ...] = ()) -> sqlite3.Row | None:
        return self.connection.execute(query, params).fetchone()



def _to_raw_cache_entry(row: sqlite3.Row | None) -> RawCacheEntry | None:
    if row is None:
        return None
    return RawCacheEntry(
        id=int(row["id"]),
        provider=row["provider"],
        surface=row["surface"],
        request_key=row["request_key"],
        request_url=row["request_url"],
        content_type=row["content_type"],
        status_code=int(row["status_code"]),
        body_path=Path(row["body_path"]),
        content_hash=row["content_hash"],
        fetched_at=row["fetched_at"],
        etag=row["etag"],
        last_modified=row["last_modified"],
    )


def _to_paper(row: sqlite3.Row | None) -> Paper | None:
    if row is None:
        return None
    return Paper(
        arxiv_id=row["arxiv_id"],
        abs_url=row["abs_url"],
        title=row["title"],
        abstract=row["abstract"],
        published_at=row["published_at"],
        updated_at=row["updated_at"],
        authors=tuple(json.loads(row["authors_json"] or "[]")),
        categories=tuple(json.loads(row["categories_json"] or "[]")),
        comment=row["comment"],
        primary_category=row["primary_category"],
    )


def _to_repo_observation(row: sqlite3.Row) -> RepoObservation:
    return RepoObservation(
        id=int(row["id"]),
        arxiv_id=row["arxiv_id"],
        provider=row["provider"],
        surface=row["surface"],
        status=row["status"],
        observed_repo_url=row["observed_repo_url"],
        normalized_repo_url=row["normalized_repo_url"],
        evidence_text=row["evidence_text"],
        raw_cache_id=row["raw_cache_id"],
        extractor_version=row["extractor_version"],
        error_message=row["error_message"],
        observed_at=row["observed_at"],
    )


def _to_paper_repo_link(row: sqlite3.Row) -> PaperRepoLink:
    providers = tuple(json.loads(row["providers_json"] or "[]"))
    surfaces = tuple(json.loads(row["surfaces_json"] or "[]"))
    return PaperRepoLink(
        id=int(row["id"]),
        arxiv_id=row["arxiv_id"],
        normalized_repo_url=row["normalized_repo_url"],
        status=row["status"],
        providers=providers,
        surfaces=surfaces,
        provider_count=int(row["provider_count"]),
        surface_count=int(row["surface_count"]),
        is_primary=bool(row["is_primary"]),
        resolved_at=row["resolved_at"],
    )


def _to_paper_link_sync_state(row: sqlite3.Row) -> PaperLinkSyncState:
    return PaperLinkSyncState(
        arxiv_id=row["arxiv_id"],
        status=row["status"],
        checked_at=row["checked_at"],
    )


def _to_paper_sync_lease(row: sqlite3.Row | None) -> PaperSyncLease | None:
    if row is None:
        return None
    return PaperSyncLease(
        arxiv_id=row["arxiv_id"],
        owner_id=row["owner_id"],
        lease_token=row["lease_token"],
        acquired_at=row["acquired_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
    )


def _to_resource_lease(row: sqlite3.Row | None) -> ResourceLease | None:
    if row is None:
        return None
    return ResourceLease(
        resource_key=row["resource_key"],
        owner_id=row["owner_id"],
        lease_token=row["lease_token"],
        acquired_at=row["acquired_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _is_lease_expired(expires_at: str) -> bool:
    try:
        expiry = datetime.fromisoformat(expires_at)
    except ValueError:
        return True
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= expiry
