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
BRONZE_RUN_ID ?=
BRONZE_PAGE_SIZE ?= 1000
PROCESSING_PYTHON ?= $(UV) run --locked --extra processing python
BRONZE_PREFIX ?=
SILVER_RUN_ID ?=

.DEFAULT_GOAL := help

.PHONY: help sync lint format build verify-raw profile-source validate-source-contract test-raw-tools \
	test-unit test-infrastructure-config test infrastructure-up \
	infrastructure-status infrastructure-logs infrastructure-down migrate \
	seed-source verify-source verify-source-schema test-source-integration \
	generate-source-batches validate-source-batch apply-source-batch test-source-simulator-integration \
	bronze-up init-bronze ingest-orders test-bronze-integration \
	sync-processing process-orders test-processing test-silver-integration

help:
	@echo "Available targets:"
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
	@echo "  test-processing           Run Spark normalization, routing and deduplication tests"
	@echo "  test-silver-integration   Test PostgreSQL -> Bronze -> Spark -> Silver with disposable services"

sync:
	$(UV) sync --locked

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
		--run-id "$(BRONZE_RUN_ID)" --page-size "$(BRONZE_PAGE_SIZE)"

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
		--contract "$(SOURCE_CONTRACT)" process --bronze-prefix "$(BRONZE_PREFIX)" --run-id "$(SILVER_RUN_ID)"

test-processing:
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" $(PROCESSING_PYTHON) -m unittest discover -s tests/processing -p 'test_*.py' -v

test-silver-integration:
	$(COMPOSE) -f $(COMPOSE_FILE) build minio
	OLIST_TEST_CONTRACT="$(abspath $(SOURCE_CONTRACT))" \
	OLIST_TEST_COMPOSE_FILE="$(abspath $(COMPOSE_FILE))" \
	SPARK_LOCAL_IP="$${SPARK_LOCAL_IP:-127.0.0.1}" \
	$(PROCESSING_PYTHON) -m unittest discover -s tests/integration -p 'test_silver_orders.py' -v
