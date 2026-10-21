# Repository Guidelines

## Project Structure & Module Organization

Implemented: core data layers, dbt analytics, deferred retries and receipt-gated local pipeline audit/watermarks.

- `src/olist_platform/raw/`: manifests, profiling, contracts.
- `src/olist_platform/source_simulator/`: deterministic batches and transactional apply.
- `src/olist_platform/bronze/`: bounded extraction, durable retry and verified MinIO publication.
- `src/olist_platform/silver/`: Spark normalization, routing and Parquet publication.
- `src/olist_platform/warehouse/`: verified Silver loading, canonical hashes and version-aware merges.
- `src/olist_platform/deferred/`: quarantine registration, bounded retry/expiry and atomic staging resolution.
- `src/olist_platform/pipeline/`: run/task audit, receipt reconciliation and all-table cursor CAS.
- `dbt/`: staging views, customer Type 1 dimension, order fact and daily GMV/AOV marts.
- `scripts/`: compatibility wrappers and the local dataset runner.
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
- `make dataset`: run available core dataset stages with automatic local configuration; skip missing inputs/unimplemented stages, fail on validation errors.
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
- `make ingest-order-items` / `make ingest-order-payments`: extract composite-key windows.
- `make process-order-items` / `make process-order-payments`: require `ORDER_SNAPSHOT=<full orders Silver prefix>`.
- `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`: full orders parent; default remains incremental.
- `make test-order-children-integration`: verify child datasets and pinned orders dependencies.
- `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> WAREHOUSE_RUN_ID=<id>`: load staging via `WAREHOUSE_DSN`/`PG*`.
- `make test-warehouse-merge-integration` / `make test-warehouse-integration`: transactional merge and verified Silver loading checks.
- `make sync-analytics` / `make dbt-build`: locked dbt dependencies and build/tests; export `PG*`, `DBT_ENV_SECRET_PASSWORD`, `DBT_READER_USER`.
- `make test-dbt-integration`: verify grains, metrics, shopper keys and failure gates in disposable PostgreSQL.
- `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>`: persist deferred rows with immutable limits.
- `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<id>`: retry at most 1,000 pending rows; use a new ID for a new pass/parent.
- `make test-deferred-state-integration` / `make test-deferred-integration`: verify lifecycle transactions and pinned snapshots.
- `make test-pipeline-control-integration` / `make test-pipeline-integration`: atomic controls and full receipt/dbt gates.
- `make pipeline-health`: read-only JSON checks using `PG*`; thresholds: `config/pipeline_health.json`. CLI exit 0 healthy, 1 unhealthy, 2 unavailable; Make fails on either error.
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

Bronze: `BRONZE_SOURCE_DSN`/`PG*` and AWS credentials. Resume unfinished run IDs; never edit prepared pages. Bronze delivery checkpoint is separate from local pipeline control.

Use full customer snapshots for FK checks. Snapshot hashes are immutable per Silver/resolution run; use a new run ID when dependencies change. Deferred limits are immutable per physical UID/rule; terminal states never reopen on replay.

Child FK checks require full orders snapshots; never use a delta as a complete parent key set. Preserve composite grain and decimal money. Products/sellers membership is not yet checked.

Warehouse uses authoritative `source_sequence`, versioned business hashes and atomic data/audit commits. Never overwrite newer records or change an existing load request. Staging success does not promote the pipeline watermark.

Aggregate items/payments separately before joining orders. Customer dimension uses `customer_unique_id`; AOV counts eligible positive-payment orders once. dbt views are not atomic published snapshots; require passing build/tests and avoid concurrent writers.

Count retry inputs separately from source growth. Resolution must commit queue/attempts/staging/audit atomically; expired rows never load. Duplicate pending occurrences must dedupe by full business key/version before merge.

Local runner uses full snapshots for all four tables and audits 19 tasks. Final gate holds staging SHARE locks through fresh dbt build and atomic watermark CAS. Resume with unchanged code/config; never bypass missing receipts. Scheduled incremental/backfill orchestration is not implemented.
