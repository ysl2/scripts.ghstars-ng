# ghstars

`ghstars` is a shared workspace service for:

1. syncing a batch of arXiv papers into PostgreSQL
2. finding exact GitHub repos for those papers
3. enriching linked repos with GitHub metadata
4. exporting scoped CSV snapshots

This V2 path is intentionally simple:

- one shared workspace
- one PostgreSQL database
- one FastAPI app
- one background worker
- one WebUI
- one flat CLI

No user system is required for the current version. Visitors operate on the same queue and the same dataset.

## Main ideas

### Database first

Papers, current repo observations, stable link state, repo metadata, jobs, raw fetch records, and exports all live in PostgreSQL.

### Cheap where possible, conservative where necessary

`sync-links` only re-checks papers when the repo state is:

- `unknown`
- missing entirely
- past its `refresh_after` TTL
- forced explicitly

If a paper is deterministically `found` or `not_found`, it waits 7 days before the next full repo lookup.

If a lookup is incomplete because of network/provider failure and no trustworthy repo was found, the paper stays `unknown` or keeps its previous stable result. Incomplete fetches do not stamp a trusted `not_found`.

### Metadata refresh policy

`enrich` treats GitHub fields in two groups:

- dynamic: `stars`, `description`, `homepage`, `topics`, `license`, `archived`, `pushed_at`
- stable: `github_id`, `created_at`

Dynamic fields refresh every enrich run. Stable fields are only initialized once or filled when still missing.

## Architecture

Current V2 runtime:

- `main.py`: flat CLI entry
- `src/ghstarsv2/app.py`: FastAPI app
- `src/ghstarsv2/jobs.py`: shared job queue + worker claiming
- `src/ghstarsv2/services.py`: sync/enrich/export pipeline logic
- `frontend/`: React WebUI

The older `src/ghstars/` package is still kept for parser/provider reuse, but it is no longer the main runtime path.

## Queue semantics

The current queue is intentionally serial.

- the default Compose topology starts exactly one `worker`
- that worker claims exactly one pending job at a time
- later jobs wait in FIFO order instead of running in parallel

This is deliberate for now:

- `sync-arxiv` and `sync-links` both consume arXiv capacity
- rate limiting is process-local, not globally coordinated
- later steps currently snapshot the database at start, so same-scope cross-step parallelism can miss newly inserted papers or repos

If throughput needs to increase later, that should be done with a dedicated scheduler redesign instead of simply adding more workers.

## Quick start

### 1. Docker Compose workflow

This is the main runtime path.

```bash
cp .env.example .env
docker compose up --build
```

This starts:

- `db`: PostgreSQL
- `app`: FastAPI + built frontend
- `worker`: background job worker

Compose runtime data is stored in a Docker named volume.
This avoids macOS bind-mount issues and keeps V2 isolated from any older local `./data/` directory or legacy SQLite artifacts.

Open:

```text
http://127.0.0.1:8000
```

Run CLI commands against the same shared workspace:

```bash
docker compose exec app uv run python main.py jobs
docker compose exec app uv run python main.py sync-arxiv --categories cs.CV --month 2026-04
docker compose exec app uv run python main.py sync-links --categories cs.CV --month 2026-04
docker compose exec app uv run python main.py enrich --categories cs.CV --month 2026-04
docker compose exec app uv run python main.py export --categories cs.CV --month 2026-04 --output cv-2026-04.csv
```

### 2. Local Python workflow

This is optional and assumes you already have a PostgreSQL instance reachable from the host.

```bash
uv sync
cp .env.example .env
```

Before starting the app, update `DATABASE_URL` in `.env` so it points to your host-accessible PostgreSQL, for example:

```dotenv
DATABASE_URL=postgresql+psycopg://ghstars:ghstars@127.0.0.1:5432/ghstars
```

Then start the API:

```bash
uv run python main.py serve
```

Start the worker in another terminal:

```bash
uv run python main.py worker
```

Open:

```text
http://127.0.0.1:8000
```

## CLI

The CLI is flat by design:

```bash
uv run python main.py serve
uv run python main.py worker
uv run python main.py sync-arxiv --categories cs.CV --month 2026-04
uv run python main.py sync-links --categories cs.CV --month 2026-04
uv run python main.py enrich --categories cs.CV --month 2026-04
uv run python main.py export --categories cs.CV --month 2026-04 --output cv-2026-04.csv
uv run python main.py jobs
uv run python main.py papers --categories cs.CV --month 2026-04
uv run python main.py repos
uv run python main.py exports
```

CLI command names stay hyphenated for user-facing consistency. Internal job type ids stay underscore-based.

## WebUI

The WebUI is a single shared control plane:

- set scope
- queue `sync-arxiv`
- queue `sync-links`
- queue `enrich`
- queue `export`
- inspect recent jobs
- inspect scoped papers
- inspect enriched repos
- download exports

## Environment

Compose-oriented `.env` example:

```dotenv
DATABASE_URL=postgresql+psycopg://ghstars:ghstars@db:5432/ghstars
DATA_DIR=data

DEFAULT_CATEGORIES=cs.CV

GITHUB_TOKEN=
HUGGINGFACE_TOKEN=
ALPHAXIV_TOKEN=

HUGGINGFACE_ENABLED=true
ALPHAXIV_ENABLED=true

ARXIV_API_MIN_INTERVAL=0.5
HUGGINGFACE_MIN_INTERVAL=0.5
GITHUB_MIN_INTERVAL=0.5

WORKER_POLL_SECONDS=1.0
JOB_TIMEOUT_SECONDS=1800
```

## Testing

```bash
uv sync --dev
uv run pytest
```
