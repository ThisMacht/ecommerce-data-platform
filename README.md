# Olist E-commerce Data Platform

[![CI](https://github.com/ThisMacht/ecommerce-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/ThisMacht/ecommerce-data-platform/actions/workflows/ci.yml)

A local batch data-engineering platform that takes Olist raw CSVs through a PostgreSQL source simulator, immutable Bronze and PySpark Silver layers on MinIO, version-aware warehouse staging, dbt analytics and Metabase. Eight core datasets support full, incremental and backfill runs with deferred recovery and gated completion watermarks. See the [architecture and ERDs](docs/22_architecture_and_data_model.md), [runbook](docs/runbooks/01_data_processing_runbook.md) and [acceptance checklist](docs/23_project_acceptance.md) for implementation details, verification evidence and closeout decisions.

The local implementation scope is verified: same-run recovery after an OOM completed all 34 tasks and 261 dbt nodes; 52 SQL and 52 saved-dashboard API comparisons passed. The original run retains a runtime alert because elapsed time includes the outage. See the [closeout decision](docs/23_project_acceptance.md#closeout-decision) for evidence and limits; this is not a production deployment certification.

## Current status

| Capability | Status |
| --- | --- |
| Raw snapshot manifest and checksum verification | Implemented |
| Deterministic source profiling | Implemented |
| Versioned source contract | Implemented |
| Dockerized PostgreSQL, migrations and source seed | Implemented |
| Live schema contract checks and isolated source integration tests | Implemented |
| Source simulator mutation batches | Implemented — 4 core tables |
| Synthetic website events | Implemented — standalone deterministic JSONL generator/verification; benchmark measured separately |
| Local event engine benchmark | Implemented — 2M rows, three rounds/engine, independent oracle and Python/JVM resource guards; configuration-specific results |
| Bounded extraction and immutable Bronze on MinIO | Implemented — four core deltas plus full dimension/review snapshots and FK parents |
| Orders PySpark normalization, routing and immutable Silver Parquet | Implemented — local rules and optional customer FK/deferred routing |
| Full customer snapshots and normalized customer Silver | Implemented — parent manifest/hash pinned to orders runs |
| Items/payments Silver with composite grain and decimal money | Implemented — scheduled item v2 pins full order/product/seller snapshots; payments pin orders |
| PostgreSQL raw staging and transactional load audit | Implemented — version-aware merges, canonical hashes and exact load reconciliation |
| Durable deferred lifecycle | Implemented — bounded retry passes, pinned full snapshots, expiry and atomic staging resolution |
| Local pipeline audit and watermarks | Implemented — verified receipts, fresh dbt gate and all-table atomic CAS commit |
| Incremental core pipeline | Implemented — separate cursors, atomic ten-stream freeze, inclusive 48h lookback and full FK dependencies |
| Isolated backfill | Implemented — explicit UTC window, frozen retry, origin-scoped queue and unchanged scheduled cursors |
| Operational health checks | Implemented — read-only audit JSON, configurable thresholds and CLI failure status |
| Operational dashboard | Implemented — standalone HTML health snapshot; manual refresh |
| GitHub Actions CI | Verified at `f2ced4d` — 11 jobs: fast/wheel, Airflow, transactional integration, dbt/catalog, four Spark groups and two benchmark correctness lanes |
| Full products/sellers Bronze/Silver, staging and Type 1 dimensions | Implemented — scheduled in all core modes, frozen full snapshots and item FK v2 |
| dbt analytics | Implemented — 31 views: five dimensions, five facts, ten marts plus staging/intermediates; metric/key/association reconciliation and catalog checks |
| Airflow orchestration | Implemented — paused daily full/incremental and manual backfill DAGs, isolated runtime and stable retry identity |
| Business dashboard / Metabase | Implemented — date-filtered GMV/paid/AOV, product/seller rankings and payment-type charts; restricted reader and publication gate |

## Quick start

Place all nine canonical Olist CSVs in `data/raw/`, then run:

```bash
make dataset
```

[`scripts/make_dataset.sh`](scripts/make_dataset.sh) creates `.env` if missing, syncs dependencies, verifies and seeds PostgreSQL, starts MinIO, and processes customers/products/sellers/geolocation → full orders → order_items/order_payments/order_reviews through Bronze/Silver, PostgreSQL staging and dbt build/tests. Parent snapshots are passed automatically for FK checks. The runner obtains configuration from Compose and writes results to ignored `build/reports/dataset/`; run IDs, prefixes and local credentials require no manual input. Requirements: Python 3.10+, uv, Docker Compose and Java 17 for Silver/staging. JDK 17 is detected under `/usr/lib/jvm` when `JAVA_HOME` is unset.

The runner freezes full snapshots of all eight datasets in the same source snapshot before publication. If any of the nine CSVs is missing, it lists the missing files and skips seeding and downstream work; partial seeding is unsupported. Without Java, it runs Bronze only, reports `PARTIAL` and retains the pending ID for resumption. Invalid checksums, migration errors and processing/load failures stop execution. After failure, rerun `make dataset` with the original code/configuration to resume; after success, the next invocation creates a new run. The runner holds a local file lock and retains services/volumes. Pending runs from the older bounded runner must finish with their original configuration before switching to full mode; see the [pipeline contract](docs/data_contracts/11_pipeline_control.md).

Prerequisites: Python `>=3.10`, [uv](https://docs.astral.sh/uv/getting-started/installation/), Docker Engine and Docker Compose.

```bash
cp .env.example .env
make sync
make test
make seed-source
make verify-source
```

`make seed-source` verifies raw files before starting PostgreSQL, applying migrations and loading 1,550,922 logical records. Repeating the same snapshot is a no-op.

## Project structure

- `src/olist_platform/raw/`: Python manifest helpers, verification, profiling and source contracts.
- `src/olist_platform/source_simulator/`: batch artifacts, deterministic fixtures and transactional apply.
- `src/olist_platform/benchmark/`: standalone event engine workload, independent oracle and resource-guarded measurements.
- `src/olist_platform/bronze/`: eight-dataset extraction, durable frozen retry state and verified MinIO publication.
- `src/olist_platform/silver/`: explicit-schema Spark processing, row routing and Parquet publication.
- `src/olist_platform/warehouse/`: verified staging merges/load audit and Metabase provisioning.
- `src/olist_platform/deferred/`: durable quarantine, bounded retry/expiry and atomic resolution.
- `src/olist_platform/pipeline/`: run/task audit, health/dashboard and receipt/dbt/CAS gates.
- `airflow/`: three local DAG wrappers with an isolated Python 3.12 runtime and lock.
- `dbt/`: staging/intermediate/dimension/fact/mart SQL, schema descriptions and tests.
- `tests/processing/`: Spark tests and independent dirty-fixture expectations, separate from fast unit tests.
- `tests/benchmark/`: optional engine parity/resource checks on tiny fixtures.
- `scripts/`: compatibility wrappers for earlier Python commands; install the package before use.
- `tests/unit/`: `unittest` and small fixtures, including installed CLI checks outside the checkout.
- `tests/integration/`: disposable PostgreSQL tests; simulator tests use small SQL fixtures in `tests/fixtures/`, while source seed tests require the canonical raw snapshot.
- `config/`: versioned raw manifest and source contract.
- `infrastructure/`: Docker Compose, PostgreSQL migrations, seeding and checks.
- `data/raw/`: immutable local CSVs, excluded from Git.
- `docs/`: contracts, generated source profile and runbooks; JSON reports go to ignored `build/reports/`.

## Python development

`pyproject.toml` defines the package, Python version, CLI entry points and development tools. `uv.lock` locks dependencies and must be committed. Source access uses Psycopg 3, object storage uses Boto3, and Ruff belongs to the development group. PySpark is in the optional `processing` extra: use Java 17 and `make sync-processing`. dbt uses the `analytics` extra; the pandas/Polars/Spark benchmark uses `benchmark`. Airflow has a separate Python 3.12 environment and lock under `airflow/`.

```bash
make sync       # uv sync --locked: install the editable package and dev tools in .venv
make lint       # Ruff lint and formatting checks
make format     # Format Python code
make test-unit  # Run unittest without Docker or the full raw snapshot
make build      # Build wheel and source distribution in dist/
```

Make defaults to `uv run --locked python`; override the interpreter with `make test-unit PYTHON=.venv/bin/python`. Add dependencies using `uv add <package>` or `uv add --dev <tool>`, then commit both `pyproject.toml` and `uv.lock`. A separate root `requirements.txt` is not maintained.

The following CLIs are available after syncing:

```bash
uv run --locked olist-verify-raw
uv run --locked olist-profile-source
uv run --locked olist-validate-source-contract
uv run --locked olist-verify-source-schema --catalog build/reports/source_schema.json
```

Run Make and CLI commands from the repository root to use default paths. CLIs resolve relative paths against the working directory, rather than inferring the checkout from the package location. When invoking an installed executable elsewhere, pass absolute paths through `--manifest`, `--raw-dir`, `--contract` and the relevant output options. The wheel contains application code only; configuration, raw CSVs and reports remain external.

## Continuous integration

[GitHub Actions CI](.github/workflows/ci.yml) runs on pushes, pull requests and manual dispatch:

- `Fast checks`: Python 3.10 and 3.12; install dependencies from `uv.lock`, run `make test`, build wheel/sdist and rerun unit tests against the installed wheel.
- `Airflow DAG checks`: import/serialize DAGs in the separate Airflow runtime and verify retry wiring.
- `PostgreSQL and MinIO integration`: simulator, Bronze, warehouse merge, deferred lifecycle and pipeline control/CAS checks using disposable services and small fixtures.
- `dbt models and catalog`: a separate job with a 35-minute timeout, running all of `make test-dbt-integration`, including metric/grain/FK reconciliation, reader grants and catalog descriptions/output columns.
- `Spark`: four independent runners with 30-minute timeouts: normalization/orders; dimensions/staging/deferred; full/incremental/backfill gates; recovery. Each runner executes its Make targets sequentially from `matrix.targets`; one group's failure does not cancel the others.
- `Event benchmark correctness`: Python 3.10/3.12, independent oracle parity and process/resource guards on tiny fixtures; no large-workload performance measurements.

CI requires no `.env`, raw CSVs or repository secrets. Integration suites generate credentials for disposable services and clean up containers/volumes afterward. The full snapshot suite (`make test-source-integration`) runs locally after downloading the canonical CSVs. Spark uses Java 17, a 2 GiB heap and two JVM CPUs per runner.

Reproduce fast checks with `make sync`, `make test` and `make build`. For integration, install the relevant locked extras (`make sync-analytics` or `uv sync --locked --extra processing --extra analytics`), then run the job's Make targets from the workflow. On a single local machine, run Spark groups sequentially and export `PYSPARK_PYTHON="$PWD/.venv/bin/python"` so workers use the correct Python environment.

Hosted [run 40000218387](https://github.com/ThisMacht/ecommerce-data-platform/actions/runs/40000218387) completed successfully with 11/11 jobs at `f2ced4d`; the full Olist baseline and large benchmark are recorded separately in the [acceptance evidence](docs/23_project_acceptance.md).

The workflow pins action commit SHAs and the uv version; update these pins with validation when upgrading tools. Timeouts are CI budgets, not benchmark results. Claim hosted CI success only from an actual workflow run.

## Synthetic events

```bash
make generate-events  # 2M physical rows, seed 42; standalone, no Docker/Spark/CSVs
make verify-events
# A new request uses a new directory:
make generate-events EVENT_ROWS=10000000 EVENT_SEED=43 EVENT_OUTPUT=data/generated/events/10m_seed43
```

JSONL/manifest/marker artifacts go to ignored `data/generated/events/`. The streaming generator produces anonymous, cart-abandonment, checkout-abandonment and purchase sessions, exact duplicates, late arrivals and out-of-order events. All IDs are synthetic and have no Olist core associations. Repeating a request verifies the artifact and returns NOOP; request mismatches or corruption fail. Counts distinguish physical/unique events and the final censored session. See the [event contract](docs/data_contracts/20_synthetic_events.md). The independent engine benchmark is described below; the core pipeline/BI continues to use the existing eight datasets.

## Event engine benchmark

```bash
make sync-benchmark
make test-benchmark  # Linux + Java 17; tiny correctness fixtures
make benchmark-events  # default 3 rounds, 4 CPUs, 4 GiB sampled RSS budget
```

pandas/Polars/PySpark process the same verified JSONL using global event deduplication and UTC day/type aggregates. The parent checks an independent oracle, and each trial runs in a fresh process with both Python and JVM monitoring. The default budget requires at least 5 GiB available RAM; resource guards stop the benchmark workload when resources are insufficient. Engines run sequentially. Reports/logs go to ignored `build/reports/events-benchmark/`; use a new `BENCHMARK_OUTPUT` for each run. See the [benchmark contract](docs/data_contracts/21_event_engine_benchmark.md) for the workload, pinned versions, cache/memory limitations and timing interpretation.

## Documentation

- [Documentation reading order](docs/README.md)
- [Raw dataset manifest](data/raw/README.md)
- [Local infrastructure](infrastructure/README.md)
- [Source simulator contract and usage](docs/data_contracts/03_source_simulator.md)
- [Orders Bronze contract, retry and local execution](docs/data_contracts/04_orders_bronze.md)
- [Orders Silver rules, Parquet and publication](docs/data_contracts/05_orders_silver.md)
- [Customer snapshots and orders dependency routing](docs/data_contracts/06_customer_snapshots.md)
- [Architecture, source ERD and warehouse ERD](docs/22_architecture_and_data_model.md)
- [Project acceptance checklist and verification evidence](docs/23_project_acceptance.md)

## Commands

Run `make help` for the current command interface. `make test` runs Ruff, contract validation, unit tests and Compose/shell syntax checks without starting containers. `make test-source-integration` performs full database seeding and idempotence checks.

## Source integration tests

```bash
make verify-source-schema     # Compare the live schema with the source contract
make verify-source            # Verify schema, row counts and seed audit
make test-source-integration  # Run tests in disposable PostgreSQL with automatic cleanup
```

`make verify-source-schema` exports a catalog through the reader role to `build/reports/source_schema.json`, then checks types (including numeric precision/scale), nullability, technical keys, ordered primary keys, identity and `source_sequence` uniqueness. Nullable additions produce contract warnings; required additions and missing/incompatible columns fail the command.

The integration suite requires Docker and all nine canonical CSVs with pinned checksums. It uses the PostgreSQL image from Compose, creates a separate container without published ports, and creates a new database for each test. The container and anonymous volume are removed afterward; the local Compose database is untouched. Running `make seed-source` first is unnecessary.

Tests verify migration reruns, real schema drift, exact seed row counts, SHA-256 fingerprints of every source field (including metadata), audit rows and sequence state before/after replay. They also check that updates survive shell no-ops and SQL reseeding, inject failures in the final table and audit insert, and verify rollback and successful retry. PostgreSQL `setval` is nontransactional: audit failures may leave sequence gaps. Sequences need not be contiguous, but data and audit must roll back together.

## Source simulator

```bash
make generate-source-batches
make validate-source-batch
make test-source-simulator-integration
```

The generator creates four immutable batches with 16 insert/update operations: same-timestamp records, child-before-parent arrivals, late payments and backdated corrections. Applying a batch commits source changes and `audit.source_batch_runs` together; replaying the same batch/hash is a no-op, while reusing an ID with a different payload fails. Source versions use identity sequences even when timestamps move backward.

After `make seed-source`, configure the connection through the process environment and apply each directory in order:

```bash
export SOURCE_SIMULATOR_DSN='postgresql://olist_loader:local_loader_change_me@127.0.0.1:5432/olist_source'
make apply-source-batch
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/002_children
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/003_parents_updates
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/004_backdated
```

The DSN above illustrates local default credentials only; the CLI also supports standard `PG*` variables. Compose `.env` is not automatically exported to CLIs. Simulator integration tests use a small synthetic baseline and require no raw CSVs; see the [contract](docs/data_contracts/03_source_simulator.md) for expected counts, semantics and limitations.

## Orders to Bronze

The MinIO/orders CLI supports consistent snapshots, composite cursors/lookback, durable retry pages, immutable JSONL objects, manifests/SHA-256 and `_SUCCESS`. Its checkpoint advances only after verified publication; this is a Bronze delivery checkpoint, separate from the pipeline completion watermark.

Run `make bronze-up`, export the database/S3 environment, run `make init-bronze`, then `make ingest-orders BRONZE_RUN_ID=<stable_id>`. Resume the same ID after failure; use a new ID for the next extraction. See the [Bronze contract](docs/data_contracts/04_orders_bronze.md) for commands, the lookback SLA and limitations. `make test-bronze-integration` uses a small fixture and requires no canonical CSVs.

## Orders to Silver

After Bronze, use Java 17, run `make sync-processing`, then `make process-orders BRONZE_PREFIX='<manifest.prefix>' SILVER_RUN_ID=<stable_id>`. Spark normalizes strings/timezones, retains the latest version, routes valid/rejected/duplicate records and publishes immutable Parquet with a manifest and reconciliation. Warning rows are a subset of valid rows.

For customer FK checks, create full customer Bronze/Silver snapshots with `make ingest-customers` and `make process-customers`, then pass `CUSTOMER_SNAPSHOT=<customer Silver prefix>` when processing orders. Orders missing a parent are deferred. Reprocess the original Bronze input with a new snapshot and Silver run ID, or use the [durable deferred lifecycle](docs/data_contracts/10_deferred_lifecycle.md) to resolve frozen pending records. Silver does not promote the end-to-end watermark.

## Order items and payments

Bronze supports composite cursors, decimal-as-string serialization and independent checkpoints for both child tables. Silver uses the complete business key and `decimal(18,2)`, and requires a full orders snapshot for FK checks; missing orders are deferred. Create the parent with `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`, process orders, then pass `ORDER_SNAPSHOT=<orders Silver prefix>` to `make process-order-items` / `make process-order-payments`.

Items v1 checks only that product/seller IDs are present. [V2](docs/data_contracts/17_item_dependency_snapshots.md) checks order/product/seller membership when all three full snapshots are supplied; all three core modes enable v2 automatically. See the [contract and commands](docs/data_contracts/07_order_children.md). Run `make test-order-children-integration` after changes; the suite uses fixtures and disposable services.

Products have a standalone path: `make ingest-products`, `make process-products`, `make load-warehouse SOURCE_TABLE=products`, then `make dbt-build`. See the [product contract and prerequisites](docs/data_contracts/15_product_snapshots.md). Products are refreshed as full snapshots in full/incremental/backfill modes and serve as item FK parents. There is no product delta cursor; full/empty snapshots never imply staging deletes.

Sellers follow the same pattern: `make ingest-sellers`, `make process-sellers`, then `make load-warehouse SOURCE_TABLE=sellers` and `make dbt-build`. See the [seller contract](docs/data_contracts/16_seller_snapshots.md). The core runner schedules both datasets; standalone item FK v2 is enabled with flags from [contract 17](docs/data_contracts/17_item_dependency_snapshots.md).

## Silver to PostgreSQL staging

After migration `006`, run `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX='<prefix>' WAREHOUSE_RUN_ID=<id>`. The CLI accepts `WAREHOUSE_DSN` or `PG*` and the same S3 environment. Only valid rows are copied/merged into `raw_staging`; data and `audit.warehouse_load_runs` commit in one transaction. Newer source sequences win, older versions are ignored, and equal versions with different business hashes fail. Replaying the same ID/input returns `NOOP`.

`make dataset` runs this step automatically and saves `*_warehouse.json`. See the [staging contract](docs/data_contracts/08_warehouse_staging.md); verify with `make test-warehouse-merge-integration` and `make test-warehouse-integration`. Local run/task audit and gated pipeline watermarks are described in the [pipeline contract](docs/data_contracts/11_pipeline_control.md); full/incremental/manual-backfill Airflow wrappers are implemented.

## dbt analytics

After eight staging loads, the core runner builds/tests 31 dbt views: eight staging, three intermediates, five dimensions, five facts and ten marts. These cover order/item/payment/delivery/review analytics and customer value/repeat/status metrics; see the [warehouse ERD and mart grains](docs/22_architecture_and_data_model.md#warehouse-erd). Items/payments are aggregated separately at order grain before joining. Customers use shopper business keys, stable surrogate keys and current Type 1 attributes. Tests reconcile exactly against staging, including business associations, fan-out, canceled orders and zero payments.

For standalone execution, run `make sync-analytics`, export `PG*`, `DBT_ENV_SECRET_PASSWORD` and `DBT_READER_USER`, then run `make dbt-build`. See the [analytics contract](docs/data_contracts/09_analytics_models.md); verify with `make test-dbt-integration`. Views do not provide atomic publication, and standalone dbt builds do not promote end-to-end watermarks. A build is successful only when its tests pass.

## dbt documentation and lineage

After `make dbt-build`, keep the same `PG*`, `DBT_ENV_SECRET_PASSWORD` and `DBT_READER_USER`, then run `make dbt-docs`. The default output is ignored `build/reports/dbt-docs/`: `index.html`, `manifest.json`, `catalog.json` and compiled SQL. Override it with `DBT_DOCS_OUTPUT=<directory>`. Serve it with the locked dbt runtime:

```bash
uv run --locked --extra analytics dbt docs serve --project-dir dbt --profiles-dir dbt --target-path "$PWD/build/reports/dbt-docs" --host 127.0.0.1 --port 8081
```

The docs reflect existing relations and metadata at generation time; the command does not build/test models, promote watermarks or publish an analytics snapshot. Keep artifacts local and ignored. Integration checks cover descriptions for every model/output column, the source catalog and removal of stale column documentation.

The diagram below groups raw staging sources into one node; each source feeds the staging view for its dataset. All other arrows represent direct `ref()` dependencies, rather than physical snapshot boundaries. The full graph and column metadata are available in the generated docs.

```mermaid
flowchart LR
    raw["raw_staging: 8 datasets"] --> sc[stg_customers]
    raw --> so[stg_orders]
    raw --> si[stg_order_items]
    raw --> sp[stg_payments]
    raw --> spr[stg_products]
    raw --> ss[stg_sellers]
    raw --> sg[stg_geolocation]
    raw --> sr[stg_reviews]
    sg --> gz[int_geolocation_by_zip] --> loc[dim_location]
    sc --> dc[dim_customer]
    loc --> dc
    ss --> ds[dim_seller]
    loc --> ds
    spr --> dp[dim_product]
    so --> dd[dim_date]
    sr --> dd
    si --> isum[int_order_item_summary]
    sp --> psum[int_payment_summary]
    sc --> fo[fct_orders]
    so --> fo
    dc --> fo
    dd --> fo
    isum --> fo
    psum --> fo
    si --> fi[fct_order_items]
    fo --> fi
    dp --> fi
    ds --> fi
    sp --> fp[fct_payments]
    fo --> fp
    so --> fd[fct_delivery]
    fo --> fd
    dd --> fd
    sr --> fr[fct_reviews]
    dd --> fr
    fo --> rev[mart_revenue_daily]
    fo --> aov[mart_average_order_value]
    fo --> cv[mart_customer_value]
    dc --> cv
    fo --> rep[mart_repeat_purchase_daily]
    fo --> status[mart_order_status_daily]
    fi --> prod[mart_product_daily]
    fi --> sell[mart_seller_daily]
    fp --> pay[mart_payment_type_daily]
    fd --> delivery[mart_delivery_daily]
    fr --> review[mart_review_daily]
```

## Business dashboard

After a successful pipeline run, execute `make metabase-up` and open [local Metabase](http://127.0.0.1:3000). Dashboard URLs and IDs are stored in ignored `build/metabase/dashboard.json`; generated admin credentials are stored in `build/metabase/credentials.json` (0600). Set `METABASE_PORT` in `.env` to change the host port.

The dashboard includes GMV/gross paid/AOV/eligible-order KPIs, daily trends, status and completeness, with inclusive start/end dates. AOV divides total paid amount by paid orders. The dedicated BI reader cannot access source/raw staging. Cards are gated when the latest pipeline has not succeeded or a newer audited load exists. `make metabase-setup` reconciles managed resources; `make metabase-down` stops the service and retains metadata. See the [business dashboard contract](docs/data_contracts/14_business_dashboard.md). Metabase/H2 metadata is intended for local demos; live views do not provide atomic snapshots across cards.

## Dirty-data verification

`make test-dirty-data` uses Java 17 and Spark to process 44 deterministic synthetic records across four core tables. A handwritten expected-error manifest checks every physical UID, exact route and error/warning/deferred rule; checksums pin input bytes. These fixtures also run in CI through `make test-processing`. See the [fixture guide](tests/fixtures/silver/README.md). The command does not modify raw CSVs or the source database; it does not cover every rule combination or schema failure.

## Deferred retries

`make dataset` registers missing-parent records and resolves them against full parent snapshots after each dependent dataset and before dbt. The queue retains original UIDs/payloads, with default limits of three attempts or seven days. Replaying the same run ID does not increase attempts; expiry is a terminal rejection. Queue transitions, version-aware staging merges and audit commit in one transaction.

Standalone operations: `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>`, then `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<new id>`. See the [contract](docs/data_contracts/10_deferred_lifecycle.md). Each pass processes at most 1,000 rows per table; there is no background retry scheduler. Local watermarks commit only at the final pipeline gate.

## Pipeline completion and watermarks

`make dataset` records run/task audit and reconciles server-side Bronze/Silver/warehouse/deferred receipts. The final gate holds staging SHARE locks during a fresh dbt build. Only after all tasks, tests and counts pass does it atomically commit the four pipeline cursors through CAS with reconciliation audit. Failure preserves previous watermarks; replaying a committed run returns `NOOP`.

See the [pipeline contract](docs/data_contracts/11_pipeline_control.md); run `make test-pipeline-control-integration` and `make test-pipeline-integration` after gate changes. Full mode uses `local_core_v1`; incremental uses `incremental_core_v1` with its own Airflow wrapper. Backfill uses `backfill_core_v1` and preserves scheduled cursors. Atomic analytics publication is not implemented.

Verify recovery with `make test-recovery-integration` and Java 17: incremental/backfill failures after Bronze/Silver/warehouse commits and after dbt/CAS before the final commit. The suite checks frozen bytes, receipts, queue attempts, row counts and watermarks on resumption; see the [failure recovery evidence](docs/data_contracts/11_pipeline_control.md#failure-recovery-verification).

`make pipeline-health` reads audit data through `PG*` (a reader role is sufficient) and emits JSON covering the active/latest run, last success, freshness, runtime, reject/warning rates and deferred backlog. Thresholds are configured in `config/pipeline_health.json`; override with `PIPELINE_HEALTH_CONFIG=<path>`. The `olist-pipeline-health` CLI exits `0` when healthy, `1` for threshold violations/missing success, and `2` when checks cannot be performed; Make returns nonzero on CLI failure. The command does not load `.env`, mutate queues/watermarks or run on a schedule. See [operational health](docs/data_contracts/11_pipeline_control.md#operational-health) for calculations and limitations.

`make pipeline-dashboard` writes `build/reports/pipeline_dashboard.html` from the same health snapshot (export `PG*` as above); open it directly in a browser. Override the path with `PIPELINE_DASHBOARD_OUTPUT=<path>`. It shows run/attempt, issues, observed metrics and thresholds without a web server. The CLI also emits JSON and returns the health exit code; HTML is still generated for unhealthy results. This is a timestamped static snapshot with manual refresh. Database/read/write failures return an error; an older file may remain, so check its timestamp and the command's exit status.

## Item analytics

`fct_order_items` has grain `(order_id, order_item_id)`, joining current product/seller Type 1 keys and order context. `mart_product_daily`/`mart_seller_daily` calculate item counts, group-local distinct orders, GMV and freight by local purchase date. Payments are not joined, and AOV/payout is not allocated to items. Order counts are not additive across products/sellers. See the [metric contract](docs/data_contracts/09_analytics_models.md#item-facts-and-productseller-metrics). Run `make test-dbt-integration`; `make dataset` builds/tests these models automatically. Metabase exposes top-10 product/seller tables and payment-type charts through SELECT-only aggregate mart access; see the [dashboard contract](docs/data_contracts/14_business_dashboard.md).

## Payment analytics

`fct_payments` has grain `(order_id, payment_sequential)` and LEFT JOINs order context. `mart_payment_type_daily` calculates entry counts, distinct eligible orders per payment type/date and observed gross paid. Amounts are not multiplied by installments, items are not joined, and type-level order counts are not used as the AOV denominator. `make dataset` builds/tests these models automatically; standalone execution uses `make dbt-build` with the existing PG/dbt environment. See the [payment metric contract](docs/data_contracts/09_analytics_models.md#payment-facts-and-type-metrics).

## Customer value and purchase behavior

`mart_customer_value` aggregates by shopper, including members without orders: observed eligible GMV/paid value, positive-paid AOV, completeness counts and a repeat flag. `mart_repeat_purchase_daily` counts first/repeat eligible orders in current staged history; late earlier orders and status changes can reclassify previous dates. `mart_order_status_daily` counts current status within purchase cohorts, rather than status transitions. Raw items/payments/reviews are not joined into these metrics, and the models do not infer lifetime value or complete retention. BI grants remain unchanged; see the [metric contract](docs/data_contracts/09_analytics_models.md#customer-value-and-repeat-purchase).

## Dimension keys and missing members

Customer/product/seller/ZIP dimensions use stable MD5 business keys; dates use YYYYMMDD. There are no synthetic unknown members or dummy keys. Missing required parents follow Silver deferred/reject rules and dbt integrity gates; optional locations remain NULL. Missing attributes do not remove real members. Source-backed reconciliation checks keys without fact references and the customer representative; see the [policy](docs/data_contracts/09_analytics_models.md#dimension-keys-and-missing-members).

## Review snapshots

Reviews use full Bronze/Silver/staging snapshots, standalone or in all three core modes, at grain `(review_id, order_id)`; neither `review_id` nor `order_id` is individually unique. Run `make ingest-order-reviews` and `make process-order-reviews` with `ORDER_SNAPSHOT=<full orders Silver prefix>`, followed by warehouse/deferred commands with `SOURCE_TABLE=order_reviews`. Validation covers scores 1–5, UTC/DST, optional comments and bounded retries. The core runner processes reviews automatically, including `fct_reviews` and `mart_review_daily`, without changing BI grants. See the [review contract](docs/data_contracts/19_review_snapshots.md).

Reviews are frozen in the same source transaction, pin full orders and require normal/deferred receipts. Health/dashboard outputs include `review_snapshot_rows` and backlog; no completion cursor or BI grants are added. Bounded modes refresh current reviews beyond the delta window; see the [review contract](docs/data_contracts/19_review_snapshots.md).

The review mart cohorts by review creation date in São Paulo, with review-weighted mean scores, score 1–2 rates and comment-presence rates. Distinct reviewed orders are not additive across dates; combine score sums/counts before dividing instead of averaging daily means. The fact does not join order/item/payment amounts, and review comments remain in staging. The calendar includes review creation dates, including reviews-only backfills. See the [metric policy](docs/data_contracts/09_analytics_models.md#review-facts-and-creation-cohorts).

## Geolocation analytics

`ingest-geolocation` / `process-geolocation` support standalone full observations; all three core modes freeze, publish and load geolocation with the other datasets. For standalone loads, use `SOURCE_TABLE=geolocation`, then `make dbt-build`. `dim_location` has one row per ZIP, with exact coordinate medians and deterministic joint city/state modes. ZIP is not a source key; observations sharing coordinates are retained, and customer/seller addresses are not inferred. Both dimensions have nullable ZIP-based `location_key` values; unmatched ZIPs remain NULL. Full mode audits 34 tasks and bounded modes 38; the four completion cursors and BI grants remain unchanged. See the [geolocation contract](docs/data_contracts/18_geolocation_snapshots.md).

## Calendar dimension

`dim_date` spans every date between the minimum and maximum local purchase, estimated/actual delivery and review creation dates; empty input produces an empty calendar. Four facts have YYYYMMDD purchase-date keys; delivery adds estimated/actual date roles, while reviews use a creation-date key. ISO weeks include the ISO year, weekdays use Monday=1, and holidays are not inferred. `make dbt-build` and the core runners build/test these models automatically; BI grants remain unchanged. See the [calendar contract](docs/data_contracts/09_analytics_models.md#calendar-dimension-and-fact-date-roles).

## Delivery analytics

`fct_delivery` has one row per order, preserving milestones, chronology flags and UTC elapsed hours. Lateness uses São Paulo calendar dates: delivery on the estimated date is on time. Missing/invalid timestamps are preserved, metrics remain NULL, and completeness counts are reported separately. `mart_delivery_daily` uses purchase-date cohorts and valid delivered observations for rates. Undelivered orders are not treated as on time, and overall late rates must not average daily rates. See the [delivery contract](docs/data_contracts/09_analytics_models.md#delivery-facts-and-cohort-metrics). The pipeline builds/tests these models; delivery has no Metabase grants/cards.

## Incremental core pipeline

`make dataset-incremental` bootstraps a separate namespace on its first run, then derives windows from the **pipeline watermark** with an inclusive 48-hour lookback. It atomically freezes four source deltas and six full snapshots (customers/orders/products/sellers/geolocation/order_reviews) in one PostgreSQL snapshot, persisting bytes/bounds before publication. Retries reuse the original upper cursor; the delivery checkpoint does not determine the window. Full mode (`make dataset`) freezes eight full streams.

Orders/items/payments FK checks and deferred retries always use full parents, never deltas as complete key sets. Customer deltas use Silver v2; the full customer parent remains v1. The pipeline audits 38 tasks with the same final receipt/reconciliation/dbt/CAS gates. Bootstrap/after-cursor/lookback, FK scans and retry inputs have separate counters; after-cursor rows do not measure net business growth. Full parent scans still incur processing costs.

```bash
make dataset-incremental
make pipeline-health PIPELINE_NAMESPACE=incremental_core_v1
make pipeline-dashboard PIPELINE_NAMESPACE=incremental_core_v1
```

Resume with the same mode/code/configuration/pending ID; updates outside the 48-hour lookback are not guaranteed to be collected automatically. See the [incremental contract](docs/data_contracts/12_incremental_pipeline.md); verify with `make test-incremental-integration` and Java 17.

## Isolated backfill

Repair records by `source_updated_at` within a UTC **[start, end)** window while preserving full/incremental watermarks. Rerun the same command/bounds to resume after failure:

```bash
make dataset-backfill BACKFILL_START=2020-01-01T00:00:00Z BACKFILL_END=2020-02-01T00:00:00Z
```

Four bounded delta streams and six full inputs are frozen in one transaction. Backfill retries only its own queue entries; merges never overwrite newer source versions. The final gate still runs fresh dbt and reconciliation. Repairs update shared staging/marts; they cannot restore source history that has already been overwritten. See the [backfill contract](docs/data_contracts/13_backfill_pipeline.md); verify with `make test-backfill-integration`. Health/dashboard commands use `PIPELINE_NAMESPACE=backfill_core_v1`.

## Local Airflow orchestration

Install Python 3.12, uv, Docker Compose and Java 17, provide all nine canonical CSVs, then run:

```bash
make sync-airflow test-airflow
make airflow-local
```

The UI binds to `127.0.0.1:8080`; generated local credentials are stored in ignored `build/airflow/`. DAGs `olist_core_full_snapshot` and `olist_core_incremental` start paused, with `@daily` UTC schedules, `catchup=False` and `max_active_runs=1`. Trigger manually for verification before unpausing, and enable only the intended mode's schedule. Each DAG has one runner task; full/incremental executions audit 34/38 business tasks in PostgreSQL. The manual-only `olist_core_backfill` DAG accepts `start`/`end` configuration and passes bounds through the environment; Retry/Clear the same DAG run to resume. Ctrl-C stops standalone components while retaining PostgreSQL/MinIO and metadata.

Retry/Clear the task in the **same DAG run** to retain its pipeline ID and replay receipts or return NOOP after commit. Do not replace an unfinished run with a new trigger or change pinned code/configuration while a run is pending. Airflow fails on missing CSVs/Java instead of skipping work. This is local standalone orchestration with separate SQLite metadata; production deployment is not implemented. See [operations](infrastructure/README.md#local-airflow).

For memory-constrained hosts, run outside the VS Code integrated terminal and use disk-backed temporary files. See [local OOM recovery](docs/runbooks/01_data_processing_runbook.md#recovery-after-a-local-oom) for the environment settings and same-run recovery checks.

## License

Source code is licensed under [LICENSE](LICENSE). The Olist dataset has a separate license documented in [data/raw/README.md](data/raw/README.md).
