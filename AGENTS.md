# Repository Guidelines

## Project Structure & Module Organization

Implemented: core layers, dbt, deferred retries, full/incremental audit, isolated backfill and gated watermarks.

- `src/olist_platform/raw/`: manifests, profiling, contracts.
- `src/olist_platform/benchmark/`: isolated pandas/Polars/Spark event workload, independent oracle, fresh worker measurements and resource guards.
- `src/olist_platform/source_simulator/`: deterministic batches, transactional apply and standalone bounded-memory synthetic events.
- `src/olist_platform/bronze/`: bounded extraction, durable retry and verified MinIO publication.
- `src/olist_platform/silver/`: Spark normalization, routing and Parquet publication.
- `src/olist_platform/warehouse/`: verified Silver loading, canonical hashes, version-aware merges and local Metabase provisioning.
- `src/olist_platform/deferred/`: quarantine registration, bounded retry/expiry and atomic staging resolution.
- `src/olist_platform/pipeline/`: run/task audit, receipt reconciliation and all-table cursor CAS.
- `airflow/`: isolated Python 3.12 runtime/lock and paused full/incremental and manual backfill DAGs.
- `dbt/`: staging views, customer/product/seller Type 1 dimensions, continuous local-calendar dim_date, ZIP dim_location with optional customer/seller keys, order/item/payment/delivery/review facts, daily GMV/AOV, product/seller/payment-type and delivery cohort marts.
- `scripts/`: compatibility wrappers and the local dataset runner.
- `tests/unit/`: unittest and CLI checks.
- `tests/processing/`: Spark checks with independent expected fixtures.
- `tests/benchmark/`: optional-engine parity and process/resource guards; tiny independent fixtures, no performance thresholds.
- `tests/integration/`: disposable PostgreSQL/MinIO tests. Seed tests require canonical CSVs; others use `tests/fixtures/`.
- `config/`: raw manifest and source contract.
- `infrastructure/`: Compose, PostgreSQL migrations, seeds, checks, and runners.
- `data/raw/`: immutable CSVs; ignored `data/generated/` holds batches and synthetic workloads.
- `docs/`: contracts, profiles, runbooks; reports: `build/reports/`.
- `.github/workflows/ci.yml`: fast/Airflow checks, transactional integration, independent dbt/catalog job and four Spark fixture groups. Keep every existing suite when regrouping jobs; run Spark targets sequentially within each runner.

## Build, Test, and Development Commands

Use Python 3.10+, uv and Docker Compose. From repository root: `cp .env.example .env`.

- `make sync`: install locked dependencies.
- `make metabase-up` / `make metabase-setup` / `make metabase-down`: start/reconcile/stop local BI after pipeline SUCCESS; retain metadata and private credentials in `build/metabase/`.
- `make dataset`: run available core dataset stages with automatic local configuration; skip missing inputs/unimplemented stages, fail on validation errors. Spark driver heap defaults to 2 GiB; preserve explicit `PYSPARK_SUBMIT_ARGS`.
- `make dataset-incremental`: freeze four deltas/full FK inputs with 48h lookback; strict prerequisites. Resume in the same mode.
- `make dataset-backfill BACKFILL_START=<offset-aware ISO> BACKFILL_END=<offset-aware ISO>`: repair source-updated-at [start,end); resume unchanged bounds, preserve scheduled cursors.
- `make test-backfill-integration`: range/empty-window, frozen retries, queue/cursor isolation and independent full reload equivalence.
- `make lint` / `make format`: check or format with Ruff.
- `make build`: build wheel/sdist.
- `make test`: Ruff, contracts, unit tests and Compose/shell checks without containers.
- `make verify-raw` / `make profile-source`: verify inputs or regenerate profiles.
- `make seed-source` / `make verify-source`: load PostgreSQL or verify schema/state.
- `make generate-source-batches` / `make validate-source-batch`: generate/verify batches.
- `make generate-events` / `make verify-events`: standalone immutable JSONL workload (2M physical rows, seed 42); override `EVENT_ROWS`, `EVENT_SEED`, `EVENT_OUTPUT`. No database/Spark/CSVs; artifacts stay in ignored `data/generated/events/`.
- `make sync-benchmark` / `make test-benchmark` / `make benchmark-events`: locked optional engines, tiny correctness checks or sequential fresh-worker trials; Linux/Java 17. Defaults 3 rounds/4 CPUs/4 GiB sampled RSS guard, require another 1 GiB available reserve. Use a new `BENCHMARK_OUTPUT` per run; do not change code/deps/input mid-run.
- `make apply-source-batch SOURCE_BATCH_DIR=<path>`: apply a batch transactionally.
- `make test-source-integration` / `make test-source-simulator-integration`: test seed or simulator against isolated PostgreSQL.
- `make infrastructure-down`: stop services; retain volumes.
- `make bronze-up` / `make init-bronze`: start MinIO or initialize its bucket.
- `make ingest-orders BRONZE_RUN_ID=<id>`: extract/resume orders.
- `make ingest-order-reviews` / `make process-order-reviews`: full reviews standalone or frozen in every core mode; require `ORDER_SNAPSHOT=<full orders Silver prefix>`, then staging/deferred with `SOURCE_TABLE=order_reviews`; `make test-review-integration`.
- `make ingest-geolocation` / `make process-geolocation`: full observations standalone or frozen in every core mode; load with `SOURCE_TABLE=geolocation`, then dbt; use `make test-geolocation-integration`.
- `make ingest-sellers` / `make process-sellers`: standalone full seller snapshots, then staging/dbt; location warnings preserve NULLs and ZIP leading zeros.
- `make test-seller-integration`: frozen retries, dirty location/ID rules, version-aware merges, corruption, empty snapshots and cursor isolation.
- `make ingest-products` / `make process-products`: standalone full product snapshots; load with `SOURCE_TABLE=products`, then dbt. Core modes also publish/load these full snapshots and pin them to items v2.
- `make test-product-integration`: frozen retry, replay, dirty/null handling, version-aware staging and cursor isolation.
- `make ingest-customers` / `make process-customers`: publish full customer snapshots.
- `make ingest-order-items` / `make ingest-order-payments`: extract composite-key windows.
- `make process-order-items` / `make process-order-payments`: require `ORDER_SNAPSHOT=<full orders Silver prefix>`.
- `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`: full orders parent; default remains incremental.
- `make test-order-children-integration`: verify child datasets and pinned orders dependencies.
- `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> WAREHOUSE_RUN_ID=<id>`: load staging via `WAREHOUSE_DSN`/`PG*`.
- `make test-warehouse-merge-integration` / `make test-warehouse-integration`: transactional merge and verified Silver loading checks.
- `make dbt-docs`: generate local catalog/lineage under ignored `build/reports/dbt-docs/` after build; same PG/dbt environment, override `DBT_DOCS_OUTPUT`. Docs do not build/test or publish.
- `make sync-analytics` / `make dbt-build`: locked dbt dependencies and build/tests; export `PG*`, `DBT_ENV_SECRET_PASSWORD`, `DBT_READER_USER`.
- `make test-dbt-integration`: verify grains, metrics, shopper keys and failure gates in disposable PostgreSQL.
- `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>`: persist deferred rows with immutable limits.
- `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<id>`: retry at most 1,000 pending rows; use a new ID for a new pass/parent.
- `make test-deferred-state-integration` / `make test-deferred-integration`: verify lifecycle transactions and pinned snapshots.
- `make test-pipeline-control-integration` / `make test-pipeline-integration`: atomic controls and full receipt/dbt gates.
- `make test-recovery-integration`: test committed Bronze/Silver/warehouse failures and rollback after real dbt/CAS; incremental/backfill resume without duplicate loads/retries.
- `make test-incremental-integration`: test atomic freeze, inclusive lookback, late/stale updates, retry and real dbt gates using Java 17.
- `make pipeline-health`: read-only JSON checks using `PG*`; thresholds: `config/pipeline_health.json`. CLI exit 0 healthy, 1 unhealthy, 2 unavailable; Make fails on either error.
- `make pipeline-dashboard`: write a static HTML health snapshot to `build/reports/pipeline_dashboard.html`; inspect its timestamp and command exit status.
- Set `PIPELINE_NAMESPACE=incremental_core_v1` for incremental health/dashboard; default is `local_core_v1`.
- `make sync-airflow` / `make test-airflow` / `make airflow-local`: install isolated Airflow, check real DAG import/serialization or start local standalone with paused DAG.
- `make test-bronze-integration`: test disposable PostgreSQL/MinIO.
- `make sync-processing`: install locked Spark extra; use Java 17.
- `make process-orders BRONZE_PREFIX=<prefix> SILVER_RUN_ID=<id>`: publish/resume Silver.
- Add `CUSTOMER_SNAPSHOT=<prefix>` for FK/deferred routing; `make test-customer-integration` verifies it.
- `make test-dirty-data`: compare 44 deterministic physical inputs with independent per-record routes/rules; Java 17, no Docker/CSVs; also included in processing CI.
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

Child FK checks require full orders snapshots; never use a delta as a complete parent key set. Preserve composite grain and decimal money. Core modes require item v2 with full order/product/seller pins; standalone v1 remains available when extra flags are omitted.

Warehouse uses authoritative `source_sequence`, versioned business hashes and atomic data/audit commits. Never overwrite newer records or change an existing load request. Staging success does not promote the pipeline watermark.

Aggregate items/payments separately before joining orders. Customer dimension uses `customer_unique_id`; AOV counts eligible positive-payment orders once. dbt views are not atomic published snapshots; require passing build/tests and avoid concurrent writers.

Count retry inputs separately from source growth. Resolution must commit queue/attempts/staging/audit atomically; expired rows never load. Duplicate pending occurrences must dedupe by full business key/version before merge.

Full runner freezes eight full streams and audits 34 tasks; incremental runner audits 38 with separate `incremental_core_v1` controls. Freeze four deltas plus six full inputs (customer/order parents, three dimensions and reviews) atomically before publication; bounds use pipeline cursors, never Bronze delivery cursors. Customer delta Silver v2 is not an FK parent. Source bootstrap/after-cursor/lookback, auxiliary FK scans and deferred retry counts are separate; after-cursor rows are not business growth. Final gate holds staging SHARE locks through fresh dbt and atomic CAS. Resume with unchanged code/config/mode; never bypass receipts. Backfill uses `backfill_core_v1`, the same 38-task freeze/gates, explicit UTC [start,end) bounds and origin-scoped deferred retries. Never promote any pipeline cursors on backfill. Staging/marts remain shared; mutable source cannot restore overwritten historical versions.

Airflow retries must retain original DAG/run context; clear the same task instance, never replace an unfinished run. Missing CSV/Java fails orchestration. Standalone SQLite metadata is local-only; don't install Airflow into the root environment.

Business BI uses `olist_metabase_reader` inheriting `olist_bi_reader`: only SELECT on
six business models (fct_orders, mart_revenue_daily, mart_average_order_value, mart_product_daily, mart_seller_daily, mart_payment_type_daily) plus publication-status view. Keep dbt grants through rebuild.
Managed Metabase content comes from `config/metabase_dashboard.json`; never commit
credentials/metadata, expose admin/source credentials as datasource, average daily
AOV, or treat live views as atomic snapshots. H2 metadata is local-only. A latest
non-successful run or newer audited load hides metrics; untracked writers are not detected.

Products: standalone full snapshots, nullable attributes warn, negative/overflow measurements reject. Preserve source `lenght` spelling until dbt aliases. Type 1 by `product_id`; full/empty snapshots never imply deletes. Core gate locks/audits eight datasets; dbt declares all eight sources; only the four delta tables have completion cursors. Products/sellers use dimension_snapshot_rows, never source growth. A standalone products load hides BI metrics until a new successful core gate.

Sellers use Type 1 by `seller_id`; location rules match customers. Blank locations become NULL with warnings; preserve invalid ZIP/state values with flags. Sellers/products stay full-only in every core mode; no deletes or new delta cursors; item v2 checks membership. New standalone loads require a fresh successful core gate to restore BI publication.

Item v2 requires full order/product/seller snapshots from the same contract; pin all manifest hashes. Missing FKs are evaluated together; rejects/duplicates never defer. Register one `missing_item_dependencies` queue row per physical item, keep all missing rule IDs in immutable payload. Retry all three memberships atomically; v1/v2 resolvers must select their own rule scopes, including backlog counts. Never use missing product/seller flags to downgrade a v2 retry. Run `make test-item-dependencies-integration` after changes. All core modes require eight dataset receipts and item v2 scope/pins. Legacy missing_order item queue entries are not auto-migrated or consumed by v2 retries; inspect backlog separately.

Item analytics: `fct_order_items` grain is `(order_id, order_item_id)`; LEFT JOIN
order context and current Type 1 product/seller keys, fail missing parents via dbt.
Daily product/seller marts use eligible source item price/freight, never joined
payments or allocated payout/AOV. Distinct order counts are local to each group,
not additive across products/sellers. Reconcile against raw staging; BI grants
are limited to those six published models and status view; no item/payment facts, dimensions, staging or source.

Payment analytics: `fct_payments` grain is `(order_id, payment_sequential)`, not
installment grain. Never multiply payment_value by payment_installments or join
items into payment metrics. Daily type mart sums eligible observed BRL payments;
distinct eligible orders include zero-paid orders and are not additive across
types. AOV stays at positive-payment order grain; BI access to the aggregate type mart stays SELECT-only.

Dashboard payment-type series must match the five Silver-validated types. Daily
trend pivots to one row/date to avoid Metabase's observed 2,000-row truncation;
recheck limits when extending history or allowed types. Preserve managed IDs,
inclusive date mappings and publication gates when changing cards.

Delivery analytics: one order per `fct_delivery`, no item/payment joins. Preserve
all milestones; flag carrier-before-purchase, actual-before-purchase/carrier.
Observe delivery only for delivered status + actual timestamp + valid chronology.
Lateness uses Sao Paulo calendar dates; durations use UTC elapsed hours, not local
wall-clock subtraction. Incomplete/invalid/non-delivered metrics stay NULL.
Daily mart uses purchase-date cohorts; expose observation counts, keep undefined
rates/means NULL and never average daily rates. No new BI grants for these models.

Calendar: `dim_date` spans observed local purchase/estimated/actual and review creation dates including
unused intervening days; no dates means no rows. Keys are integer YYYYMMDD;
ISO weeks require iso_year, weekdays Monday=1. Purchase keys propagate through
all four purchase-based facts; actual delivery keys stay NULL only without a timestamp,
independent of delivery observation eligibility. Preserve explicit Sao Paulo
conversion, empty behavior and existing BI grants when extending date roles.

Geolocation is full-only standalone and in all core modes, keyed by positive CSV
geolocation_row_id; ZIP is not a source key. Preserve exact coordinates, ZIP
leading zeros and physical observation frequency. Reject invalid ZIP/coordinates,
warn missing labels. Aggregate before joining: exact numeric medians and joint
city/state mode, ties by C-collated state/city. Coordinate representatives are not
addresses. Freeze/verify geolocation with the other streams; audit its load and
dimension_snapshot_rows, never add a completion cursor or treat it as source growth.
Customer/seller location_key is a nullable LEFT JOIN by source ZIP to dim_location;
missing/invalid/unmatched ZIP stays NULL, never drop rows or infer source city/state.
Dimension business hashes keep their existing source-attribute contract; location
membership is checked separately by source reconciliation. Preserve item v2's
three-parent scope and existing BI grants. Full/empty snapshots imply no deletes;
current staging may include prior observations and newer versions. Backfill
refreshes current geography, not geography as of its historical end bound.

Reviews are full-only standalone and in all core modes, keyed by (review_id, order_id), never review_id
or order_id alone. Preserve nonblank comment text/case/newlines; empty/all-space
comments become NULL without warning. Score integer 1–5; local business times use
Sao Paulo → UTC and reject DST gaps/folds; answer-before-creation warns. Require
pinned full orders from the same contract; missing order defers only locally valid
selected rows. Generic retry must preserve both string keys, frozen payload,
limits and atomic queue/attempt/staging/audit behavior. Core freezes reviews with the other inputs, pins same-run full orders and requires
normal/deferred receipts through the eight-table lock/dbt/CAS gate. Bounded reviews
use the hashed parent stream ID; backfill origin filtering must match that Silver
ID, while load/registration/resolution receipts use the owner pipeline ID. Keep
review_snapshot_rows separate from dimensions/delta growth; health includes reviews
backlog. No new completion cursors or BI grants. Standalone review loads
still hide BI until a fresh core gate; backfill refreshes current reviews, not
review state as of its historical end bound.

Review analytics: `fct_reviews` keeps (review_id, order_id), all observed scores and
UTC creation/answer timestamps; comment text stays in staging, fact exposes only
presence flags. No order/item/payment joins or one-review-per-order selection.
`mart_review_daily` cohorts by Sao Paulo review creation date across all statuses.
Mean/rates are review-weighted: aggregate score_sum/counts before dividing; distinct
reviewed orders are local to each date and not additive across dates. Score 1–2
means low score, not inferred sentiment. Chronology warnings retain scores.
`dim_date` includes review creation dates, including reviews-only backfill when
orders are outside the staging window; Silver full-order membership remains the
parent gate. Empty review input yields zero mart rows. Keep eight dbt sources,
all-table receipt locks and existing six-model BI grants; no review BI exposure.

Dimension keys: preserve MD5 mappings for shopper/product/seller/ZIP and integer
YYYYMMDD dates; hashes are identifiers, not anonymization. No synthetic unknown
members or dummy FK keys. Missing required parents follow deferred/reject rules
and dbt integrity checks; optional location keys stay NULL. Missing attributes
do not remove real members. Customer representative uses source_sequence DESC, customer_id DESC.
`dimension_key_reconciliation` checks raw-staging membership, unreferenced MD5 keys
and latest_customer_id; location/date retain their source-backed gates. Empty
staging yields empty dimensions; empty source snapshots still never imply deletes.

Customer marts: `mart_customer_value` has one row per current dim_customer shopper,
including no-order shoppers; aggregate order-grain facts, never raw item/payment
joins. Counts/GMV/paid amounts use existing eligibility and positive-paid AOV policy.
Repeat means at least two observed eligible orders, including unpaid/zero-paid;
not verified retention or predicted lifetime value. `mart_repeat_purchase_daily`
ranks current staged history by UTC purchase instant then order_id, groups by Sao
Paulo purchase date. Late earlier orders/status changes can reclassify history.
Combine repeat counts before dividing rates; distinct shoppers are not additive.
`mart_order_status_daily` counts current status in purchase cohorts, not transitions.
Preserve all eight dbt sources, core gates and existing six-model BI grants.

Document every exported model column in dbt/models/schema.yml, preserving tests and
source semantics. After output/schema changes run the real docs catalog integration
check; source/model descriptions and exact output-column membership must match
catalog.json. Keep catalog/manifest/compiled SQL local and ignored. README lineage
is a grouped view of direct ref dependencies, not proof of atomic publication.

Fact integrity: exact order reconciliation covers source customer_id/shopper/key,
status, UTC purchase instant, local purchase date and eligibility, as well as
independent item/payment counts and BRL sums. A foreign key that exists can still
be the wrong business association; keep valid-but-wrong fixtures alongside generic
FK/not-null tests. Preserve Silver-normalized local dates; views compare source
attributes rather than silently correcting staging or assigning dummy parents.

Synthetic events: `syn_*` IDs have no Olist FK/PII meaning. Only purchases have order IDs. Preserve versioned deterministic bytes, arrival ordering, adjacent exact duplicates, separate unique/physical counts and the censored final session. Existing artifacts require full verification for NOOP; never overwrite another request or manually reseal modified files. Keep this workload outside the eight-table core and its cursors/grants. Benchmark engines separately; generator timings are not engine results.

Event benchmark: preserve independent fixture/oracle parity, explicit string/null/UTC semantics and exact duplicate scope. Report startup/work/wall separately and sampled whole-worker-tree RSS including JVM; never label it PSS/whole-host RAM or cold-cache performance. Pin versions, input/source hashes, CPU affinity and all trials; failed rounds must not disappear into passing medians. Keep large performance runs out of CI; benchmark CI runs tiny correctness fixtures on Python 3.10/3.12.
