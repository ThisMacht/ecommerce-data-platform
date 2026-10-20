# Repository Guidelines

## Project Structure & Module Organization

Implemented: raw validation, PostgreSQL loading/simulation, orders/customers Bronze/Silver and pinned customer FK checks.

- `src/olist_platform/raw/`: manifests, profiling, contracts.
- `src/olist_platform/source_simulator/`: deterministic batches and transactional apply.
- `src/olist_platform/bronze/`: bounded extraction, durable retry and verified MinIO publication.
- `src/olist_platform/silver/`: Spark normalization, routing and Parquet publication.
- `scripts/`: compatibility wrappers.
- `tests/unit/`: unittest and CLI checks.
- `tests/processing/`: Spark checks with independent expected fixtures.
- `tests/integration/`: disposable PostgreSQL/MinIO tests. Seed tests require canonical CSVs; others use `tests/fixtures/`.
- `config/`: raw manifest and source contract.
- `infrastructure/`: Compose, PostgreSQL migrations, seeds, checks, and runners.
- `data/raw/`: immutable CSVs; ignored `data/generated/` holds batches.
- `docs/`: contracts, profiles, runbooks; reports: `build/reports/`.
- `.github/workflows/ci.yml`: fast checks, source/Bronze and Spark/Silver integration.

## Build, Test, and Development Commands

Use Python 3.10+, uv and Docker Compose. From repository root: `cp .env.example .env`.

- `make sync`: install locked dependencies.
- `make lint` / `make format`: check or format with Ruff.
- `make build`: build wheel/sdist.
- `make test`: Ruff, contracts, unit tests and Compose/shell checks without containers.
- `make verify-raw` / `make profile-source`: verify inputs or regenerate profiles.
- `make seed-source` / `make verify-source`: load PostgreSQL or verify schema/state.
- `make generate-source-batches` / `make validate-source-batch`: generate/verify batches.
- `make apply-source-batch SOURCE_BATCH_DIR=<path>`: apply a batch transactionally.
- `make test-source-integration` / `make test-source-simulator-integration`: test seed or simulator against isolated PostgreSQL.
- `make infrastructure-down`: stop services; retain volumes.
- `make bronze-up` / `make init-bronze`: start MinIO or initialize its bucket.
- `make ingest-orders BRONZE_RUN_ID=<id>`: extract/resume orders.
- `make ingest-customers` / `make process-customers`: publish full customer snapshots.
- `make test-bronze-integration`: test disposable PostgreSQL/MinIO.
- `make sync-processing`: install locked Spark extra; use Java 17.
- `make process-orders BRONZE_PREFIX=<prefix> SILVER_RUN_ID=<id>`: publish/resume Silver.
- Add `CUSTOMER_SNAPSHOT=<prefix>` for FK/deferred routing; `make test-customer-integration` verifies it.
- `make test-processing` / `make test-silver-integration`: Spark or end-to-end slice checks.

## Coding Style & Naming Conventions

Use four spaces, type hints and `snake_case`. Ruff configuration: `pyproject.toml`. Resolve CLI paths from working directory. Shell: POSIX with `set -eu`. SQL: uppercase keywords and Psycopg parameters/identifiers. Migrations: `NNN_descriptive_name.sql`; never edit applied migrations.

## Testing Guidelines

Use `unittest`, `test_*.py` and `test_*` methods. Fast unit tests need no Docker, Spark or CSVs. Cover parsing, drift, failures and replay. Run relevant integration suites after changes. No coverage threshold exists.

## Commit & Pull Request Guidelines

Use imperative commit subjects; describe behavior, validation and contract impacts. Update related docs/locks. Commit completed task changes without asking; preserve unrelated edits. Report hash and subject in final response.

## Data & Configuration Safety

Never commit credentials, CSVs or generated artifacts. Never edit raw snapshots; re-profile new checksums and update manifests. Simulator: `SOURCE_SIMULATOR_DSN`/`PG*`. Compose `.env` is not automatically exported. Default credentials are local-only.

Bronze: `BRONZE_SOURCE_DSN`/`PG*` and AWS credentials. Resume unfinished run IDs; never edit prepared pages. Bronze delivery checkpoint is separate from the planned pipeline watermark.

Use full customer snapshots for FK checks. Snapshot hashes are immutable per Silver run; reprocess with a new run ID when dependencies change. Deferred persistence/expiry remains planned.
