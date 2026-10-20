# Repository Guidelines

## Project Structure & Module Organization

Olist raw-data validation and PostgreSQL loading are implemented. Orchestration, processing layers, warehouse models, and dashboards remain planned.

- `src/olist_platform/raw/`: importable manifest, verification, profiling, and contract modules.
- `scripts/`: compatibility wrappers calling the package.
- `tests/unit/`: unittest and installed CLI checks with temporary fixtures.
- `tests/integration/`: disposable PostgreSQL tests using the canonical snapshot.
- `config/`: versioned raw snapshot manifest and source contract.
- `infrastructure/`: Compose, PostgreSQL migrations, seeds, checks, and shell runners.
- `data/raw/`: immutable CSV inputs; see its README for attribution.
- `docs/`: contracts, profiles, and runbooks. JSON reports use ignored `build/reports/`.

## Build, Test, and Development Commands

Use Python 3.10+, uv, and Docker Compose. Run commands from repository root. Initialize configuration: `cp .env.example .env`.

- `make sync`: install the editable package and development tools from `uv.lock`.
- `make lint` / `make format`: check lint/formatting or format Python with Ruff.
- `make build`: build the wheel and source distribution.
- `make test`: run Ruff, contracts, unit tests, and Compose/shell checks without starting containers.
- `make test-unit`: run Python unit tests only.
- `make verify-raw` / `make profile-source`: verify the snapshot or regenerate profiles.
- `make seed-source` / `make verify-source`: load PostgreSQL or check schema and source state.
- `make test-source-integration`: test schema drift, seed reruns, and rollback in isolated PostgreSQL.
- `make infrastructure-down`: stop services while retaining database volumes.

## Coding Style & Naming Conventions

Use four-space indentation, type hints, and `snake_case`. Share raw-data helpers through `olist_platform.raw.manifest`. Configure Ruff in `pyproject.toml`. CLI defaults use the working directory; pass explicit paths elsewhere. Keep shell runners POSIX-compatible with `set -eu`. Use uppercase SQL keywords. Name migrations `NNN_descriptive_name.sql`; add migrations instead of editing applied files.

## Testing Guidelines

Use `unittest`, `test_*.py` files, and `test_*` methods. Unit tests require neither Docker nor raw datasets. Cover parsing failures, contract drift, and installed CLI behavior. Run database integration checks for migration/loader changes. No coverage threshold is configured.

## Commit & Pull Request Guidelines

Use concise imperative commit subjects. Describe changes, validation, and contract/schema impacts. Update documentation together. Commit `pyproject.toml` and `uv.lock` together when dependencies change.

Commit completed task changes without asking; preserve unrelated edits. Report the commit hash and subject in the final response.

## Data & Configuration Safety

Never commit credentials, raw CSVs, or build artifacts. Never edit raw snapshots in place. Re-profile checksum changes as new snapshots and update the manifest. Default credentials are local-only.
