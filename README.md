# scripts.ghstars-ng

Local-first toolkit for building and auditing deterministic arXiv ↔ GitHub associations.

## What it does

`scripts.ghstars-ng` builds a local SQLite dataset from arXiv papers and conservative GitHub link evidence.
It is designed for workflows where you want reproducible, inspectable paper → repo associations instead of fuzzy search results.

The pipeline is centered on a local database:

1. sync arXiv papers into SQLite
2. collect provider-specific GitHub link observations
3. resolve final paper-repo links conservatively
4. enrich linked repositories with GitHub metadata
5. audit parity and export timestamped CSV snapshots

## Key properties

- local-first: all canonical state lives in SQLite
- deterministic exact-match association chain
- conservative resolver: conflicting exact sources stay ambiguous instead of forcing a winner
- multi-process-safe local execution on a single machine
- raw response caching for replay and debugging
- timestamped CSV exports with `published_at`

## Exact-match sources

The live main CLI chain is:

- arXiv comment
- arXiv abstract HTML
- Hugging Face paper API by arXiv id
- Hugging Face paper HTML by arXiv id
- AlphaXiv paper API by arXiv id
- AlphaXiv paper HTML by arXiv id

`uv run main.py sync links` intentionally follows only this paper-scoped exact-match chain.
It does not use title-search fallback against Hugging Face or GitHub.

## Non-goals

This project is intentionally conservative.

It does **not** try to be:

- a multi-machine distributed system
- a fuzzy repo discovery engine
- a GitHub title-search matcher
- a best-effort heuristic linker that guesses when evidence is weak

If an exact source does not provide a trustworthy repo signal, the project prefers `not_found` or `ambiguous` over a risky match.

## Install

```bash
uv sync
cp .env.example .env
```

Then adjust `.env` if needed.

## Configuration

Example `.env`:

```dotenv
DEFAULT_CATEGORIES=cs.CV
GITHUB_TOKEN=
HUGGINGFACE_TOKEN=
ALPHAXIV_TOKEN=
ARXIV_API_MIN_INTERVAL=0.5
HUGGINGFACE_MIN_INTERVAL=0.5
GITHUB_MIN_INTERVAL=0.5
```

Notes:

- `DEFAULT_CATEGORIES` is used when `--categories` is omitted
- tokens are optional, but authenticated access is better for real runs
- the project defaults to local storage under `data/`

## Quick start

### 1. Sync papers from arXiv

```bash
uv run main.py sync arxiv --categories cs.CV --month 2026-01
```

### 2. Sync deterministic GitHub link evidence

```bash
uv run main.py sync links --categories cs.CV --month 2026-01
```

### 3. Enrich resolved repositories with GitHub metadata

```bash
uv run main.py enrich repos --categories cs.CV --month 2026-01
```

### 4. Audit provider-visible vs final links

```bash
uv run main.py audit parity --categories cs.CV --month 2026-01
```

### 5. Export a timestamped CSV snapshot

```bash
uv run main.py export csv --categories cs.CV --month 2026-01 --output output/papers.csv
```

The export command writes to a timestamped file such as:

```text
output/papers-20260416-071922-151537.csv
```

`--output` supplies the base path/name; the actual file written is a timestamped sibling file.
The CSV includes every paper in the selected category/time window, including papers with no resolved GitHub link.

## Example real run

A full real run on `2026-01` for the default `cs.CV` category produced:

- papers: `2444`
- provider-visible link papers: `872`
- final found papers: `872`
- ambiguous papers: `0`
- end-to-end runtime: about `41m 17s`

Treat this as a snapshot of current behavior, not a benchmark guarantee.

## Time window filters

Supported filters:

- `--day YYYY-MM-DD`
- `--month YYYY-MM`
- `--from YYYY-MM-DD --to YYYY-MM-DD`

These filters are supported across the downstream workflow, including:

- `sync arxiv`
- `sync links`
- `enrich repos`
- `audit parity`
- `export csv`

## Storage layout

- SQLite database: `data/ghstars.db`
- raw response cache: `data/raw/`
- exported CSV snapshots: user-specified output directory

## CSV columns

The CSV export currently includes:

- `arxiv_id`
- `abs_url`
- `title`
- `abstract`
- `published_at`
- `categories`
- `primary_category`
- `github_primary`
- `github_all`
- `link_status`
- `stars`
- `created_at`
- `description`

Exports include every paper in the selected category/time window.
For papers without a final resolved repository, `github_primary=""`, `github_all=""`, and `link_status="not_found"`.
For resolved rows, `github_primary` is the primary final repository URL, `github_all` is a semicolon-separated list of final repository URLs, and `link_status` is `found` or `ambiguous`.

## Command overview

```bash
uv run main.py sync arxiv --categories cs.CV
uv run main.py sync links --categories cs.CV
uv run main.py audit parity --categories cs.CV
uv run main.py enrich repos --categories cs.CV
uv run main.py export csv --categories cs.CV --output output/papers.csv
```

To inspect all CLI options:

```bash
uv run main.py --help
```

## Testing

```bash
uv sync --dev
uv run pytest
```

## Design principles

### Database-first

Canonical paper facts, source evidence, final links, sync state, leases, and enriched repo metadata are all stored locally.
The database is the center of the workflow, not an afterthought.

### Conservative resolution

Provider observations are stored first. Final links are derived afterward.
This keeps source evidence inspectable and makes downstream exports reproducible.

### Multi-process local safety

The project is designed for single-machine use with possible overlapping processes.
Shared writes are fenced with SQLite transactions and lease-based coordination.

### Partial work is preserved where it matters

For expensive upstream fetches such as arXiv window sync, already fetched pages are persisted even if the overall run later fails.

## Current fit

`scripts.ghstars-ng` is a good fit for:

- local dataset building
- repeatable monthly or daily paper sync
- auditing exact-source parity
- exporting paper/repo snapshots for later analysis

It is not trying to be a hosted service or a broad fuzzy-discovery platform.
