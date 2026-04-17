from __future__ import annotations

import json

from src.ghstars import cli as cli_module
from src.ghstars.associate.resolver import parity_summary
from src.ghstars.commands._common import _list_papers_for_window
from src.ghstars.storage.db import Database


def _run_audit_parity(
    database: Database,
    categories: tuple[str, ...],
    *,
    window: cli_module.ArxivSyncWindow | None = None,
) -> None:
    if window is None:
        window = cli_module.ArxivSyncWindow()
    with database.snapshot_reads():
        papers = _list_papers_for_window(database, categories, window=window)
        total = len(papers)
        found_provider = 0
        found_final = 0
        ambiguous = 0
        for paper in papers:
            observations = database.list_repo_observations(paper.arxiv_id)
            final_links = database.list_paper_repo_links(paper.arxiv_id)
            summary = parity_summary(observations, final_links)
            if summary["found_any_provider_link"]:
                found_provider += 1
            if summary["final_status"] == "found":
                found_final += 1
            elif summary["final_status"] == "ambiguous":
                ambiguous += 1
    print(
        json.dumps(
            {
                "papers": total,
                "provider_visible_link_papers": found_provider,
                "final_found_papers": found_final,
                "ambiguous_papers": ambiguous,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
