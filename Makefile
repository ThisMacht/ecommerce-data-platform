UV ?= uv
PYTHON ?= $(UV) run --locked python
COMPOSE ?= docker compose

RAW_DIR ?= data/raw
RAW_MANIFEST ?= config/raw_dataset_manifest.json
SOURCE_CONTRACT ?= config/source_contract.json
SOURCE_PROFILE_JSON ?= build/reports/source_profile.json
SOURCE_PROFILE_MARKDOWN ?= docs/data_contracts/00_source_profile.md
SOURCE_SCHEMA_REPORT ?= build/reports/source_schema.json
COMPOSE_FILE ?= infrastructure/docker-compose.yml
SOURCE_BATCH_ROOT ?= data/generated/source_batches/reliability_v1
SOURCE_BATCH_DIR ?= $(SOURCE_BATCH_ROOT)/001_initial
SOURCE_SIMULATOR_SEED ?= 42
EVENT_ROWS ?= 2000000
EVENT_SEED ?= 42
EVENT_OUTPUT ?= data/generated/events/scale_v1
BENCHMARK_PYTHON ?= $(UV) run --locked --extra benchmark python
BENCHMARK_OUTPUT ?= build/reports/events-benchmark
BENCHMARK_REPEATS ?= 3
BENCHMARK_CPUS ?= 4
BENCHMARK_MEMORY_MIB ?= 4096
BRONZE_RUN_ID ?=
BRONZE_PAGE_SIZE ?= 1000
PROCESSING_PYTHON ?= $(UV) run --locked --extra processing python
BRONZE_PREFIX ?=
SILVER_RUN_ID ?=
CUSTOMER_SNAPSHOT ?=
PRODUCT_SNAPSHOT ?=
SELLER_SNAPSHOT ?=
ORDER_SNAPSHOT ?=
BRONZE_FULL_SNAPSHOT ?=
SILVER_PREFIX ?=
WAREHOUSE_RUN_ID ?=
SOURCE_TABLE ?= orders
PARENT_SNAPSHOT ?=
DEFERRED_RUN_ID ?=
MAX_DEFERRED_ATTEMPTS ?= 3
MAX_DEFERRED_AGE_HOURS ?= 168
ANALYTICS_PYTHON ?= $(UV) run --locked --extra analytics python
DBT_DOCS_OUTPUT ?= build/reports/dbt-docs
PIPELINE_PYTHON ?= $(UV) run --locked --extra processing --extra analytics python
PIPELINE_HEALTH_CONFIG ?= config/pipeline_health.json
PIPELINE_DASHBOARD_OUTPUT ?= build/reports/pipeline_dashboard.html
PIPELINE_NAMESPACE ?= local_core_v1
BACKFILL_START ?=
BACKFILL_END ?=

.DEFAULT_GOAL := help

.PHONY: generate-events verify-events sync-benchmark benchmark-events test-benchmark

sync-benchmark:
	$(UV) sync --locked --extra benchmark

benchmark-events:
	$(BENCHMARK_PYTHON) -m olist_platform.benchmark.events run --input-dir "$(EVENT_OUTPUT)" \
		--output-dir "$(BENCHMARK_OUTPUT)" --repeats "$(BENCHMARK_REPEATS)" \
		--cpus "$(BENCHMARK_CPUS)" --memory-mib "$(BENCHMARK_MEMORY_MIB)"

test-benchmark:
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(BENCHMARK_PYTHON) -m unittest discover -s tests/benchmark -p 'test_*.py' -v

generate-events:
	$(PYTHON) -m olist_platform.source_simulator.events generate --rows "$(EVENT_ROWS)" \
		--seed "$(EVENT_SEED)" --output-dir "$(EVENT_OUTPUT)"

verify-events:
	$(PYTHON) -m olist_platform.source_simulator.events verify --output-dir "$(EVENT_OUTPUT)"

.PHONY: help dataset sync lint format build verify-raw profile-source validate-source-contract test-raw-tools \
	test-unit test-infrastructure-config test infrastructure-up \
	infrastructure-status infrastructure-logs infrastructure-down migrate \
	seed-source verify-source verify-source-schema test-source-integration \
	generate-source-batches validate-source-batch apply-source-batch test-source-simulator-integration \
	bronze-up init-bronze ingest-orders test-bronze-integration \
	sync-processing process-orders test-processing test-dirty-data test-silver-integration \
	ingest-customers process-customers test-customer-integration \
	ingest-products process-products test-product-integration \
	ingest-sellers process-sellers test-seller-integration \
	ingest-order-payments ingest-order-items process-order-payments process-order-items test-order-children-integration test-item-dependencies-integration \
	load-warehouse test-warehouse-merge-integration test-warehouse-integration sync-analytics dbt-build dbt-docs test-dbt-integration \
	register-deferred resolve-deferred test-deferred-integration test-deferred-state-integration \
	test-pipeline-control-integration test-pipeline-integration pipeline-health pipeline-dashboard \
	sync-airflow airflow-local test-airflow dataset-incremental test-incremental-integration dataset-backfill test-backfill-integration test-recovery-integration metabase-up metabase-setup metabase-down

metabase-up: migrate
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --wait --wait-timeout 360 metabase
	$(MAKE) metabase-setup

metabase-setup:
	$(PYTHON) -m olist_platform.warehouse.metabase

metabase-down:
	$(COMPOSE) -f $(COMPOSE_FILE) stop metabase

help:
	@echo "Available targets:"
	@echo "  metabase-up / metabase-setup / metabase-down Local business dashboard, provisioning and stop"
	@echo "  dataset                   Prepare local source and supported Bronze/Silver datasets automatically"
	@echo "  sync                      Install the package and locked development tools"
	@echo "  lint                      Check Python lint and formatting"
	@echo "  format                    Format Python files"
	@echo "  build                     Build the Python wheel and source distribution"
	@echo "  verify-raw                Verify raw files, checksums and headers"
	@echo "  profile-source            Profile keys, types and source relationships"
	@echo "  validate-source-contract  Check the contract against the raw manifest"
	@echo "  infrastructure-up         Start the PostgreSQL source service"
	@echo "  infrastructure-status     Show Compose service and health status"
	@echo "  infrastructure-logs       Follow PostgreSQL logs"
	@echo "  migrate                   Apply checksum-protected database migrations"
	@echo "  seed-source               Verify and idempotently load the Olist snapshot"
	@echo "  verify-source             Verify loaded source counts and metadata"
	@echo "  verify-source-schema      Compare live PostgreSQL schema with the contract"
	@echo "  test-source-integration   Test schema, seed reruns and rollback in an isolated container"
	@echo "  generate-source-batches   Publish deterministic simulator fixture batches"
	@echo "  generate-events / verify-events Generate or verify standalone synthetic JSONL events (default 2M)"
	@echo "  sync-benchmark / benchmark-events Install locked engines / compare event workload in fresh sequential workers"
	@echo "  test-benchmark            Check engine/oracle parity and resource/failure guards on small fixtures"
	@echo "  validate-source-batch     Verify a simulator batch without database access"
	@echo "  apply-source-batch        Apply a batch using SOURCE_SIMULATOR_DSN or PG* environment"
	@echo "  test-source-simulator-integration Test simulator transactions in disposable PostgreSQL"
	@echo "  test                       Run lint and fast tests without starting infrastructure"
	@echo "  infrastructure-down       Stop containers without deleting database data"
	@echo "  bronze-up                 Build and start MinIO, waiting for readiness"
	@echo "  init-bronze               Create the Bronze bucket using AWS environment credentials"
	@echo "  ingest-orders             Extract/resume orders; requires BRONZE_RUN_ID and connection environment"
	@echo "  test-bronze-integration   Build MinIO and test orders extraction with disposable services"
	@echo "  sync-processing           Install locked Spark processing dependencies (Java 17 required)"
	@echo "  process-orders            Publish Silver; requires BRONZE_PREFIX and SILVER_RUN_ID"
	@echo "  test-dirty-data           Compare all four dirty fixtures with per-record expected rules/routes"
	@echo "  test-processing           Run Spark normalization, routing and deduplication tests"
	@echo "  test-silver-integration   Test PostgreSQL -> Bronze -> Spark -> Silver with disposable services"
	@echo "  ingest-sellers / process-sellers Full sellers Bronze/Silver snapshots"
	@echo "  ingest-order-reviews / process-order-reviews Full reviews with pinned orders"
	@echo "  test-review-integration   Test review routing, replay, merges and deferred retries"
	@echo "  ingest-geolocation / process-geolocation Full geolocation observations; load with SOURCE_TABLE=geolocation"
	@echo "  test-geolocation-integration Test full snapshots, exact coordinates and version-aware staging"
	@echo "  test-seller-integration   Test sellers publication, replay, routing and staging merges"
	@echo "  ingest-products / process-products Full products Bronze/Silver snapshots"
	@echo "  test-product-integration  Test products publication, replay, routing and staging merges"
	@echo "  ingest-customers          Extract/resume a full customer snapshot"
	@echo "  process-customers         Publish a full validated customer Silver snapshot"
	@echo "  test-customer-integration Test customer snapshots and orders dependency routing"
	@echo "  ingest-order-payments / ingest-order-items Extract composite-key child windows"
	@echo "  process-order-payments / process-order-items Publish Silver; ORDER_SNAPSHOT must be full"
	@echo "  test-item-dependencies-integration Test three-parent item FK scope and atomic retries"
	@echo "  test-order-children-integration Test items/payments publication, replay and FK routing"
	@echo "  load-warehouse            Merge Silver valid; set SOURCE_TABLE, SILVER_PREFIX and WAREHOUSE_RUN_ID"
	@echo "  test-warehouse-merge-integration Test PostgreSQL versions, atomic counters and replay"
	@echo "  test-warehouse-integration Test verified Silver -> PostgreSQL staging"
	@echo "  sync-analytics / dbt-build Install locked dbt dependencies / build models and tests"
	@echo "  dbt-docs                  Generate local catalog, descriptions and lineage (requires built relations)"
	@echo "  test-dbt-integration      Test marts and repeatable customer keys in disposable PostgreSQL"
	@echo "  register-deferred         Persist deferred Silver; set SOURCE_TABLE, SILVER_PREFIX, DEFERRED_RUN_ID"
	@echo "  resolve-deferred          Retry pending rows; set SOURCE_TABLE, PARENT_SNAPSHOT, DEFERRED_RUN_ID"
	@echo "  test-deferred-state-integration / test-deferred-integration Test lifecycle transactions / verified snapshots"
	@echo "  test-pipeline-control-integration Test atomic pipeline control and CAS failures"
	@echo "  test-pipeline-integration Test all core receipts and dbt-gated watermark commit"
	@echo "  pipeline-health           Check audit freshness, runtime, quality and deferred backlog (requires PG*)"
	@echo "  pipeline-dashboard        Write a standalone HTML health snapshot (requires PG*)"
	@echo "  sync-airflow / airflow-local Install isolated Airflow / start local standalone (paused DAG)"
	@echo "  test-airflow              Import/serialize the real DAG and check retry wiring"
	@echo "  dataset-incremental       Process frozen deltas with 48h lookback and full FK snapshots"
	@echo "  test-incremental-integration Test frozen windows, late updates, replay and final gates"
	@echo "  test-recovery-integration Test committed-stage failures and final transaction rollback/resume"
	@echo "  dataset-backfill          Repair a UTC [BACKFILL_START,BACKFILL_END) window without cursor promotion"
	@echo "  test-backfill-integration Test range isolation, full reload equivalence and retry gates"

test-recovery-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PIPELINE_PYTHON) -m unittest discover -s tests/integration -p 'test_pipeline_recovery.py' -v

dataset-backfill:
	@test -n "$(BACKFILL_START)" && test -n "$(BACKFILL_END)" || (echo "Set BACKFILL_START and BACKFILL_END (offset-aware timestamps)" >&2; exit 2)
	DATASET_MODE=backfill BACKFILL_START="$(BACKFILL_START)" BACKFILL_END="$(BACKFILL_END)" sh scripts/make_dataset.sh

test-backfill-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PIPELINE_PYTHON) -m unittest discover -s tests/integration -p 'test_backfill*.py' -v

dataset-incremental:
	DATASET_MODE=incremental sh scripts/make_dataset.sh

sync-airflow:
	$(UV) sync --project airflow --locked

airflow-local:
	sh scripts/airflow.sh standalone

test-airflow:
	AIRFLOW_HOME="$(abspath build/airflow-test)" AIRFLOW__CORE__LOAD_EXAMPLES=False \
		$(UV) run --project airflow --locked python -m unittest discover -s tests/orchestration -p 'test_*.py' -v

pipeline-dashboard:
	$(PYTHON) -m olist_platform.pipeline.health --pipeline "$(PIPELINE_NAMESPACE)" --thresholds "$(PIPELINE_HEALTH_CONFIG)" --html-output "$(PIPELINE_DASHBOARD_OUTPUT)"

pipeline-health:
	$(PYTHON) -m olist_platform.pipeline.health --pipeline "$(PIPELINE_NAMESPACE)" --thresholds "$(PIPELINE_HEALTH_CONFIG)"

test-incremental-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PIPELINE_PYTHON) -m unittest discover -s tests/integration -p 'test_incremental*.py' -v

sync:
	$(UV) sync --locked

dataset:
	sh scripts/make_dataset.sh

lint:
	$(UV) run --locked ruff check .
	$(UV) run --locked ruff format --check .

format:
	$(UV) run --locked ruff format .

build:
	$(UV) build

verify-raw:
	$(PYTHON) -m olist_platform.raw.verification \
		--manifest $(RAW_MANIFEST) \
		--raw-dir $(RAW_DIR)

profile-source: verify-raw
	$(PYTHON) -m olist_platform.raw.profiling \
		--manifest $(RAW_MANIFEST) \
		--raw-dir $(RAW_DIR) \
		--json-output $(SOURCE_PROFILE_JSON) \
		--markdown-output $(SOURCE_PROFILE_MARKDOWN) \
		--skip-verify

validate-source-contract:
	$(PYTHON) -m olist_platform.raw.contracts \
		--contract $(SOURCE_CONTRACT) \
		--manifest $(RAW_MANIFEST)

test-raw-tools:
	$(PYTHON) -m unittest discover -s tests/unit -p 'test_raw_data_tools.py' -v

test-unit:
	$(PYTHON) -m unittest discover -s tests/unit -p 'test_*.py' -v

test-infrastructure-config:
	$(COMPOSE) -f $(COMPOSE_FILE) config --quiet
	sh -n infrastructure/postgres/scripts/run_migrations.sh
	sh -n infrastructure/postgres/scripts/run_source_seed.sh
	sh -n scripts/make_dataset.sh
	sh -n scripts/airflow.sh

test: lint validate-source-contract test-unit test-infrastructure-config

infrastructure-up:
	$(COMPOSE) -f $(COMPOSE_FILE) up -d postgres

infrastructure-status:
	$(COMPOSE) -f $(COMPOSE_FILE) ps

infrastructure-logs:
	$(COMPOSE) -f $(COMPOSE_FILE) logs --follow postgres

migrate: infrastructure-up
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm migrate

seed-source: verify-raw validate-source-contract migrate
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm source-loader

verify-source: verify-source-schema
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm source-check

verify-source-schema: validate-source-contract infrastructure-up
	@mkdir -p "$(dir $(SOURCE_SCHEMA_REPORT))"
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm -T --entrypoint psql source-check \
		--no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align \
		--file=/checks/export_source_schema.sql > "$(SOURCE_SCHEMA_REPORT)"
	$(PYTHON) -m olist_platform.raw.schema \
		--contract "$(SOURCE_CONTRACT)" --catalog "$(SOURCE_SCHEMA_REPORT)"

test-source-integration:
	OLIST_TEST_RAW_DIR="$(abspath $(RAW_DIR))" \
	OLIST_TEST_MANIFEST="$(abspath $(RAW_MANIFEST))" \
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_source_layer.py' -v

infrastructure-down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

generate-source-batches:
	$(PYTHON) -m olist_platform.source_simulator.cli --contract "$(SOURCE_CONTRACT)" generate \
		--output-dir "$(SOURCE_BATCH_ROOT)" --seed "$(SOURCE_SIMULATOR_SEED)"

validate-source-batch:
	$(PYTHON) -m olist_platform.source_simulator.cli --contract "$(SOURCE_CONTRACT)" validate \
		--batch-dir "$(SOURCE_BATCH_DIR)"

apply-source-batch:
	$(PYTHON) -m olist_platform.source_simulator.cli --contract "$(SOURCE_CONTRACT)" apply \
		--batch-dir "$(SOURCE_BATCH_DIR)"

test-source-simulator-integration:
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_source_simulator.py' -v

bronze-up:
	$(COMPOSE) -f $(COMPOSE_FILE) up --build -d --wait minio

init-bronze:
	$(PYTHON) -m olist_platform.bronze.cli init

ingest-orders:
	@test -n "$(BRONZE_RUN_ID)" || (echo "Set BRONZE_RUN_ID to a stable run identifier" >&2; exit 2)
	$(PYTHON) -m olist_platform.bronze.cli --contract "$(SOURCE_CONTRACT)" extract \
		--run-id "$(BRONZE_RUN_ID)" --page-size "$(BRONZE_PAGE_SIZE)" \
		$(if $(filter 1,$(BRONZE_FULL_SNAPSHOT)),--full-snapshot,)

test-bronze-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_bronze_orders.py' -v

sync-processing:
	$(UV) sync --locked --extra processing

process-orders:
	@test -n "$(BRONZE_PREFIX)" -a -n "$(SILVER_RUN_ID)" || (echo "Set BRONZE_PREFIX and SILVER_RUN_ID" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.silver.cli \
		--contract "$(SOURCE_CONTRACT)" process --bronze-prefix "$(BRONZE_PREFIX)" --run-id "$(SILVER_RUN_ID)" \
		$(if $(CUSTOMER_SNAPSHOT),--customer-snapshot "$(CUSTOMER_SNAPSHOT)",)

test-dirty-data:
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/processing -p 'test_dirty_manifest.py' -v

test-processing:
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/processing -p 'test_*.py' -v

test-silver-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" \
	$(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_silver_orders.py' -v

ingest-customers ingest-products ingest-sellers ingest-geolocation:
	@test -n "$(BRONZE_RUN_ID)" || (echo "Set BRONZE_RUN_ID" >&2; exit 2)
	$(PYTHON) -m olist_platform.bronze.cli --contract "$(SOURCE_CONTRACT)" --table $(@:ingest-%=%) extract \
		--run-id "$(BRONZE_RUN_ID)" --page-size "$(BRONZE_PAGE_SIZE)"

process-customers process-products process-sellers process-geolocation:
	@test -n "$(BRONZE_PREFIX)" && test -n "$(SILVER_RUN_ID)" || (echo "Set BRONZE_PREFIX and SILVER_RUN_ID" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.silver.cli \
		--contract "$(SOURCE_CONTRACT)" --table $(@:process-%=%) process --bronze-prefix "$(BRONZE_PREFIX)" --run-id "$(SILVER_RUN_ID)"

test-customer-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" \
	$(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_customers.py' -v

ingest-order-payments ingest-order-items ingest-order-reviews:
	@test -n "$(BRONZE_RUN_ID)" || (echo "Set BRONZE_RUN_ID" >&2; exit 2)
	$(PYTHON) -m olist_platform.bronze.cli --contract "$(SOURCE_CONTRACT)" --table $(subst -,_,$(@:ingest-%=%)) extract \
		--run-id "$(BRONZE_RUN_ID)" --page-size "$(BRONZE_PAGE_SIZE)" \
		$(if $(filter 1,$(BRONZE_FULL_SNAPSHOT)),--full-snapshot,)

process-order-payments process-order-items process-order-reviews:
	@test -n "$(BRONZE_PREFIX)" && test -n "$(SILVER_RUN_ID)" && test -n "$(ORDER_SNAPSHOT)" || (echo "Set BRONZE_PREFIX, SILVER_RUN_ID and ORDER_SNAPSHOT" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.silver.cli \
		--contract "$(SOURCE_CONTRACT)" --table $(subst -,_,$(@:process-%=%)) process \
		--bronze-prefix "$(BRONZE_PREFIX)" --run-id "$(SILVER_RUN_ID)" --order-snapshot "$(ORDER_SNAPSHOT)" \
		$(if $(PRODUCT_SNAPSHOT),--product-snapshot "$(PRODUCT_SNAPSHOT)",) \
		$(if $(SELLER_SNAPSHOT),--seller-snapshot "$(SELLER_SNAPSHOT)",)

test-order-children-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" \
	$(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_order_children.py' -v

load-warehouse:
	@test -n "$(SILVER_PREFIX)" && test -n "$(WAREHOUSE_RUN_ID)" || (echo "Set SILVER_PREFIX and WAREHOUSE_RUN_ID" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.warehouse.cli \
		--contract "$(SOURCE_CONTRACT)" --table "$(SOURCE_TABLE)" \
		--silver-prefix "$(SILVER_PREFIX)" --run-id "$(WAREHOUSE_RUN_ID)"

test-warehouse-merge-integration:
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_warehouse_merge.py' -v

test-warehouse-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" \
	$(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_warehouse_silver.py' -v

sync-analytics:
	$(UV) sync --locked --extra analytics

dbt-build:
	DBT_SEND_ANONYMOUS_USAGE_STATS=false $(UV) run --locked --extra analytics dbt build --project-dir dbt --profiles-dir dbt

dbt-docs:
	DBT_SEND_ANONYMOUS_USAGE_STATS=false $(UV) run --locked --extra analytics dbt docs generate --project-dir dbt --profiles-dir dbt --target-path "$(abspath $(DBT_DOCS_OUTPUT))"

test-dbt-integration:
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(ANALYTICS_PYTHON) -m unittest discover -s tests/integration -p 'test_dbt_models.py' -v

register-deferred:
	@test -n "$(SILVER_PREFIX)" && test -n "$(DEFERRED_RUN_ID)" || (echo "Set SILVER_PREFIX and DEFERRED_RUN_ID" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.deferred.cli \
		--contract "$(SOURCE_CONTRACT)" --table "$(SOURCE_TABLE)" register --silver-prefix "$(SILVER_PREFIX)" \
		--run-id "$(DEFERRED_RUN_ID)" --max-attempts "$(MAX_DEFERRED_ATTEMPTS)" --max-deferred-age-hours "$(MAX_DEFERRED_AGE_HOURS)"

resolve-deferred:
	@test -n "$(PARENT_SNAPSHOT)" && test -n "$(DEFERRED_RUN_ID)" || (echo "Set PARENT_SNAPSHOT and DEFERRED_RUN_ID" >&2; exit 2)
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m olist_platform.deferred.cli \
		--contract "$(SOURCE_CONTRACT)" --table "$(SOURCE_TABLE)" resolve --parent-snapshot "$(PARENT_SNAPSHOT)" --run-id "$(DEFERRED_RUN_ID)" \
		$(if $(PRODUCT_SNAPSHOT),--product-snapshot "$(PRODUCT_SNAPSHOT)",) \
		$(if $(SELLER_SNAPSHOT),--seller-snapshot "$(SELLER_SNAPSHOT)",)

test-deferred-state-integration:
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_deferred_state.py' -v

test-deferred-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_deferred_snapshots.py' -v

test-pipeline-control-integration:
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	$(PYTHON) -m unittest discover -s tests/integration -p 'test_pipeline_control.py' -v

test-pipeline-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PIPELINE_PYTHON) -m unittest discover -s tests/integration -p 'test_pipeline_gate.py' -v

test-product-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_products.py' -v

test-seller-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_sellers.py' -v

test-item-dependencies-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_item_dependencies.py' -v

test-geolocation-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_geolocation.py' -v

.PHONY: ingest-geolocation process-geolocation test-geolocation-integration

.PHONY: ingest-order-reviews process-order-reviews test-review-integration

test-review-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_reviews.py' -v
