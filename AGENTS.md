# Repository Guidelines

## Project Structure & Module Organization

Raw validation, PostgreSQL loading, and source simulation are implemented. Orchestration, processing layers, warehouse models, and dashboards remain planned.

- `src/olist_platform/raw/`: manifests, profiling, and contracts.
- `src/olist_platform/source_simulator/`: batch artifacts, deterministic fixtures, and transactional apply.
- `scripts/`: compatibility wrappers calling the package.
- `tests/unit/`: unittest and installed CLI checks with temporary fixtures.
- `tests/integration/`: disposable PostgreSQL tests; simulator uses small `tests/fixtures/` SQL inputs, seed tests require canonical CSVs.
- `config/`: versioned raw manifest and source contract.
- `infrastructure/`: Compose, PostgreSQL migrations, seeds, checks, and runners.
- `data/raw/`: immutable local CSV inputs. Generated batches use ignored `data/generated/`.
- `docs/`: contracts, profiles, and runbooks. JSON reports use ignored `build/reports/`.
- `.github/workflows/ci.yml`: Python checks/build and isolated simulator integration, without raw CSVs or secrets.

## Build, Test, and Development Commands

Use Python 3.10+, uv, and Docker Compose. Run from repository root. Initialize configuration: `cp .env.example .env`.

- `make sync`: install the editable package and locked dependencies.
- `make lint` / `make format`: check lint/formatting or format with Ruff.
- `make build`: build wheel and source distribution.
- `make test`: run Ruff, contracts, unit tests, and Compose/shell checks without containers.
- `make verify-raw` / `make profile-source`: verify inputs or regenerate profiles.
- `make seed-source` / `make verify-source`: load PostgreSQL or verify schema/state.
- `make generate-source-batches` / `make validate-source-batch`: publish or verify simulator artifacts.
- `make apply-source-batch SOURCE_BATCH_DIR=<path>`: apply a batch transactionally.
- `make test-source-integration` / `make test-source-simulator-integration`: test seed or simulator against isolated PostgreSQL.
- `make infrastructure-down`: stop services, retaining database volumes.

## Coding Style & Naming Conventions

Use four-space indentation, type hints, and `snake_case`. Configure Ruff in `pyproject.toml`. CLI paths use working directory. Keep shell POSIX-compatible with `set -eu`. Use uppercase SQL and Psycopg parameters/identifiers. Name migrations `NNN_descriptive_name.sql`; never edit applied migrations.

## Testing Guidelines

Use `unittest`, `test_*.py`, and `test_*` methods. Unit tests require neither Docker nor raw datasets. Cover parsing, checksum/contract drift, transaction failures, and replay. Run relevant PostgreSQL integration checks for source changes. No coverage threshold is configured.

## Commit & Pull Request Guidelines

Use imperative commit subjects. Describe changes, validation, and contract impacts. Update documentation and dependency locks together. Commit completed task changes without asking; preserve unrelated edits. Report commit hash and subject in final response.

## Data & Configuration Safety

Never commit credentials, raw CSVs, or generated artifacts. Never edit raw snapshots. Re-profile new checksums and update manifests. Simulator uses `SOURCE_SIMULATOR_DSN` or standard `PG*` process environment; Compose `.env` is not automatically exported. Default credentials are local-only.
