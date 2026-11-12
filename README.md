# Legacy CRM Migrator

A reproducible migration demo that copies customers, contacts, and activities from an awkward legacy CRM API into PostgreSQL. The Go worker normalizes inconsistent fields, validates records, retries transient failures, throttles source requests, writes idempotently, checkpoints each batch, and records invalid rows in a dead-letter table. An Angular dashboard exposes run controls and verification status.

## Run locally

Requirements: Go 1.22+, Node.js 22.22.3+ (or a supported newer LTS release), npm, and PostgreSQL 14+ or Docker Compose.

```sh
# Option A: use an existing local PostgreSQL server (no Docker required)
brew services start postgresql@14
createdb crm

# Option B: run PostgreSQL in Docker
docker compose up -d db

go run ./cmd/legacy-api                 # terminal 1, listens on :8081
go run ./cmd/migrator serve             # terminal 2, status/control API on :8080
cd web && npm install && npm start      # terminal 3, Angular on :4200
```

If `brew services start postgresql@14` reports an error because port 5432 is already in use, start the local cluster on 5433 instead:

```sh
pg_ctl -D /opt/homebrew/var/postgresql@14 -o "-p 5433" -l /opt/homebrew/var/log/postgresql@14.log start
psql -p 5433 -d postgres
```

In the `psql` prompt, create the demo database login and database, then exit:

```sql
CREATE ROLE crm LOGIN PASSWORD 'crm';
CREATE DATABASE crm OWNER crm;
\q
```

Start the Go processes with `DATABASE_URL='postgres://crm:crm@localhost:5433/crm?sslmode=disable'` set in their environment. The `crm`/`crm` credentials are for this local demo only.

Start a migration from the dashboard, or use the CLI:

```sh
go run ./cmd/migrator migrate
go run ./cmd/migrator migrate --resume
```

The worker can also run without the control server. Set `DATABASE_URL`, `LEGACY_API_URL`, and optionally `CONTROL_API_URL` (defaults are in `.env.example`). The source mock API serves deliberately inconsistent JSON, including mixed-case keys, numeric/string IDs, blank fields, and one intentionally invalid customer to demonstrate validation and dead-letter handling. It supports `GET /v1/{customers,contacts,activities}?page=N&limit=N`.

## Endpoints

- `GET /api/status` — current run, counts, progress, and latest verification
- `POST /api/migrations` — start a new run (`{"resume":true}` resumes from checkpoints)
- `GET /api/migrations` — recent runs
- `GET /healthz` — control service health
- Mock source: `GET /healthz`, `GET /v1/customers`, `GET /v1/contacts`, `GET /v1/activities`

The database schema and checkpoint state are initialized automatically by the worker. Re-running a migration is safe because destination rows use stable source IDs and `ON CONFLICT` updates.

## Design notes

Each resource is paged in fixed-size batches. A checkpoint stores the last committed page per resource; data writes, dead-letter entries, and checkpoint updates share a transaction. Transient HTTP and database failures are retried with bounded exponential backoff. Verification compares source totals with destination counts and reports any mismatch. The mock server and worker are separate processes so the source can be replaced without changing the migration pipeline.
