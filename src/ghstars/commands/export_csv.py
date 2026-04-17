from __future__ import annotations

from pathlib import Path

from src.ghstars import cli as cli_module
from src.ghstars.commands._common import _list_papers_for_window
from src.ghstars.export.csv import build_export_row, write_papers_csv
from src.ghstars.storage.db import Database


def _run_export_csv(
    database: Database,
    categories: tuple[str, ...],
    output_path: Path,
    *,
    window: cli_module.ArxivSyncWindow | None = None,
) -> None:
    if window is None:
        window = cli_module.ArxivSyncWindow()
    with database.snapshot_reads():
        papers = _list_papers_for_window(database, categories, window=window)
        rows: list[dict[str, object]] = []
        for paper in papers:
            links = database.list_paper_repo_links(paper.arxiv_id)
            repo_metadata_by_url = {
                link.normalized_repo_url: metadata
                for link in links
                if (metadata := database.get_github_repo(link.normalized_repo_url)) is not None
            }
            rows.append(build_export_row(paper, links, repo_metadata_by_url))
    resolved_output_path = write_papers_csv(rows, output_path)
    print(str(resolved_output_path))
