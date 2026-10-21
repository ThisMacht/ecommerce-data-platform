# Olist E-commerce Data Platform

Batch data-engineering project xây từ Olist raw CSV đến PostgreSQL source simulator, immutable Bronze/Silver layers, warehouse models và dashboards. Hiện đã có source validation, PostgreSQL source layer/simulator và bốn core datasets Bronze/PySpark Silver trên MinIO; các phase sau được theo dõi trong [Pipeline Implementation Plan](ecommerce_data_warehouse_pipeline_plan_v4.md).

## Current status

| Capability | Status |
| --- | --- |
| Raw snapshot manifest and checksum verification | Implemented |
| Deterministic source profiling | Implemented |
| Versioned source contract | Implemented |
| Dockerized PostgreSQL, migrations and source seed | Implemented |
| Live schema contract checks and isolated source integration tests | Implemented |
| Source simulator mutation batches | Implemented — 4 core tables |
| Bounded extraction and immutable Bronze on MinIO | Implemented — all four core datasets; full parent snapshots |
| Orders PySpark normalization, routing and immutable Silver Parquet | Implemented — local rules and optional customer FK/deferred routing |
| Full customer snapshots and normalized customer Silver | Implemented — parent manifest/hash pinned to orders runs |
| Items/payments Silver with composite grain and decimal money | Implemented — pinned full orders FK checks; product/seller membership remains planned |
| PostgreSQL raw staging and transactional load audit | Implemented — version-aware merges, canonical hashes and exact load reconciliation |
| GitHub Actions CI: fast checks, package build and simulator integration | Implemented |
| dbt customer dimension, order fact and daily GMV/AOV views | Implemented — exact reconciliation and relationship tests |
| Airflow and Metabase | TODO — later phases |

## Quick start

Đặt đủ 9 CSV Olist canonical vào `data/raw/`, rồi chạy:

```bash
make dataset
```

[`scripts/make_dataset.sh`](scripts/make_dataset.sh) tự tạo `.env` nếu thiếu, sync dependencies, verify/seed PostgreSQL, start MinIO và chạy customers → full orders → order_items → order_payments qua Bronze/Silver rồi PostgreSQL staging và dbt build/tests, tự truyền parent snapshots cho FK checks. Không cần nhập run IDs, prefixes hay export local credentials; runner lấy cấu hình từ Compose và lưu kết quả trong ignored `build/reports/dataset/`. Cần Python 3.10+, uv, Docker Compose; Java 17 cho Silver/staging (tự nhận diện JDK 17 trong `/usr/lib/jvm` nếu chưa đặt `JAVA_HOME`).

Runner xử lý cả 4 core datasets. Thiếu bất kỳ CSV nào trong snapshot 9 files thì seed/downstream được bỏ qua và liệt kê file thiếu; loader hiện không hỗ trợ partial seed. Thiếu Java thì chỉ chạy Bronze. CSV có sẵn nhưng sai checksum, migration hoặc processing/load lỗi sẽ dừng, không bị nuốt lỗi. Sau lỗi, gọi lại `make dataset` để resume run ID đã lưu; sau success, lần gọi tiếp tạo run mới. Chạy một runner tại một thời điểm; services/volumes được giữ lại. Pending orders run từ phiên bản runner cũ phải hoàn tất bằng config cũ trước khi chuyển sang full mode; xem [contract](docs/data_contracts/07_order_children.md).

Prerequisites: Python `>=3.10`, [uv](https://docs.astral.sh/uv/getting-started/installation/), Docker Engine và Docker Compose.

```bash
cp .env.example .env
make sync
make test
make seed-source
make verify-source
```

`make seed-source` xác minh raw files trước khi start PostgreSQL, apply migration và load 1,550,922 logical records. Lần chạy lại cùng snapshot là no-op.

## Project structure

- `src/olist_platform/raw/`: package Python chứa manifest helpers, verification, profiling và source contracts.
- `src/olist_platform/source_simulator/`: batch artifacts, deterministic fixtures và transactional apply.
- `src/olist_platform/bronze/`: bounded orders extraction, durable retry state và verified MinIO publication.
- `src/olist_platform/silver/`: explicit-schema Spark processing, row routing và Parquet publication.
- `tests/processing/`: Spark tests và independent dirty fixture expectations; tách khỏi fast unit tests.
- `scripts/`: compatibility wrappers cho các Python commands trước đây; cần cài package trước khi dùng.
- `tests/unit/`: `unittest` và fixture nhỏ, gồm kiểm tra installed CLI từ thư mục ngoài checkout.
- `tests/integration/`: PostgreSQL tests dùng container tạm; simulator dùng small SQL fixture trong `tests/fixtures/`, source seed dùng canonical raw snapshot.
- `config/`: raw manifest và source contract versioned.
- `infrastructure/`: Docker Compose cùng PostgreSQL migrations, seed và checks.
- `data/raw/`: immutable CSV local, không commit vào Git.
- `docs/`: contracts, generated source profile và runbooks; JSON reports nằm trong `build/reports/` đã ignore.

Chỉ thêm folders ingestion, Airflow, Spark và dbt khi có implementation tương ứng.

## Python development

`pyproject.toml` khai báo package, Python version, CLI entry points và development tools. `uv.lock` khóa dependencies và phải được commit. Source dùng Psycopg 3, object storage dùng Boto3, Ruff thuộc development group. PySpark nằm trong optional extra `processing`: dùng Java 17 và `make sync-processing`. Airflow/dbt runtime sẽ được quản lý khi triển khai.

```bash
make sync       # uv sync --locked: cài editable package và dev tools vào .venv
make lint       # Ruff lint và formatting checks
make format     # Format Python code
make test-unit  # Chạy unittest, không cần Docker hoặc full raw snapshot
make build      # Build wheel và source distribution vào dist/
```

Make dùng `uv run --locked python` mặc định; có thể override interpreter bằng `make test-unit PYTHON=.venv/bin/python`. Khi thêm dependencies, dùng `uv add <package>` hoặc `uv add --dev <tool>` và commit cả `pyproject.toml` lẫn `uv.lock`. Không duy trì một root `requirements.txt` riêng.

Các CLI sau khả dụng sau khi sync:

```bash
uv run --locked olist-verify-raw
uv run --locked olist-profile-source
uv run --locked olist-validate-source-contract
uv run --locked olist-verify-source-schema --catalog build/reports/source_schema.json
```

Chạy Make và CLI từ repository root để dùng default paths. CLI resolve relative paths theo working directory, không suy ra checkout từ vị trí package. Khi gọi executable đã cài từ thư mục khác, truyền absolute paths qua `--manifest`, `--raw-dir`, `--contract` và các output options phù hợp. Wheel chỉ chứa application code; config, raw CSV và reports vẫn nằm ngoài package.

## Continuous integration

[GitHub Actions CI](.github/workflows/ci.yml) chạy trên push, pull request và manual dispatch:

- `Fast checks`: Python 3.10 và 3.12, cài dependencies từ `uv.lock`, chạy `make test`, build wheel/sdist và chạy lại unit tests với wheel đã cài.
- `PostgreSQL and MinIO integration`: Python 3.12, chạy simulator tests và `make test-bronze-integration` với disposable services và small SQL fixtures.
- `Orders Spark Silver`: Python 3.12/Java 17, cài locked processing extra, chạy `make test-processing`, `make test-silver-integration` và `make test-customer-integration`.

CI không cần `.env`, raw CSV hay repository secrets. Integration suites tự tạo credentials cho services tạm và cleanup container/volume khi kết thúc. Full snapshot suite (`make test-source-integration`) vẫn chạy local sau khi tải canonical CSV; dbt checks đã có; DAG checks còn planned.

Tái hiện CI từ checkout sạch: `make sync`, `make test`, `make build`, `make test-source-simulator-integration` và `make test-bronze-integration`. Workflow pin action commit SHAs và uv version; cập nhật các pins cùng validation khi nâng tools.

Spark job dùng Java 17: `make sync-processing`, `make test-processing`, `make test-silver-integration`.

## Documentation

- [Documentation reading order](docs/README.md)
- [Raw dataset manifest](data/raw/README.md)
- [Local infrastructure](infrastructure/README.md)
- [Source simulator contract and usage](docs/data_contracts/03_source_simulator.md)
- [Orders Bronze contract, retry and local execution](docs/data_contracts/04_orders_bronze.md)
- [Orders Silver rules, Parquet and publication](docs/data_contracts/05_orders_silver.md)
- [Customer snapshots and orders dependency routing](docs/data_contracts/06_customer_snapshots.md)
- [Pipeline implementation plan](ecommerce_data_warehouse_pipeline_plan_v4.md)

## Commands

Chạy `make help` để xem interface hiện tại. `make test` chạy Ruff, contract validation, unit tests và Compose/shell syntax checks, không start container. `make test-source-integration` thực thi full database seed và idempotence check.

## Source integration tests

```bash
make verify-source-schema     # So sánh live schema với source contract
make verify-source            # Kiểm tra schema, row counts và seed audit
make test-source-integration  # Chạy tests trong PostgreSQL tạm, tự cleanup
```

`make verify-source-schema` export catalog bằng reader role vào `build/reports/source_schema.json`, rồi kiểm tra types (gồm numeric precision/scale), nullability, technical keys, ordered primary keys, identity và uniqueness của `source_sequence`. Nullable additions tạo warning theo contract; required additions hoặc missing/incompatible columns làm command fail.

Integration suite cần Docker và toàn bộ 9 CSV canonical đã khóa checksum. Suite dùng cùng PostgreSQL image từ Compose, tạo container riêng không publish port và database mới cho từng test. Container cùng anonymous volume được xóa khi kết thúc; local Compose database không được dùng. Không cần chạy `make seed-source` trước.

Các tests xác minh migration rerun, real schema drift, exact seed row counts, SHA-256 fingerprints của mọi source field (gồm metadata), audit rows và sequence state trước/sau rerun. Suite còn kiểm tra updates được giữ nguyên qua shell no-op lẫn SQL reseed, chèn lỗi tại table cuối và audit insert, rồi xác minh rollback cùng successful retry. PostgreSQL `setval` không transactional: audit failure có thể để lại sequence gaps; sequence không cần liên tục, nhưng data và audit phải rollback.

## Source simulator

```bash
make generate-source-batches
make validate-source-batch
make test-source-simulator-integration
```

Generator tạo 4 immutable batches với 16 insert/update operations: same-timestamp records, child-before-parent arrivals, late payment và backdated corrections. Apply trong transaction, audit ở `audit.source_batch_runs`; cùng batch/hash là no-op, cùng ID khác payload bị reject. Source versions dùng identity sequence, kể cả khi timestamp đi lùi.

Sau `make seed-source`, cấu hình connection qua process environment rồi apply từng directory theo thứ tự:

```bash
export SOURCE_SIMULATOR_DSN='postgresql://olist_loader:local_loader_change_me@127.0.0.1:5432/olist_source'
make apply-source-batch
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/002_children
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/003_parents_updates
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/004_backdated
```

DSN ở trên chỉ minh họa default local credentials; CLI cũng hỗ trợ standard `PG*` environment. Compose `.env` không tự được export vào CLI. Simulator integration tests dùng small synthetic baseline, không yêu cầu raw CSV; xem [contract](docs/data_contracts/03_source_simulator.md) cho expected counts, semantics và limitations.

## Orders to Bronze

MinIO và orders extractor đã có CLI slice với consistent snapshot, composite cursor/lookback, durable retry pages, immutable JSONL objects, manifest/SHA-256 và `_SUCCESS`. Checkpoint chỉ tiến sau verified publication; đây là Bronze delivery checkpoint, chưa phải watermark của toàn pipeline.

Chạy `make bronze-up`, export database/S3 environment, `make init-bronze`, rồi `make ingest-orders BRONZE_RUN_ID=<stable_id>`. Resume cùng ID khi lỗi; dùng ID mới cho extraction tiếp theo. Xem [Bronze contract](docs/data_contracts/04_orders_bronze.md) cho đầy đủ commands, lookback SLA và limitations. `make test-bronze-integration` dùng small fixture, không cần canonical CSV.

## Orders to Silver

Sau Bronze, dùng Java 17, `make sync-processing`, rồi `make process-orders BRONZE_PREFIX='<manifest.prefix>' SILVER_RUN_ID=<stable_id>`. Spark chuẩn hóa strings/timezone, giữ version mới nhất, phân loại valid/rejected/duplicates và publish Parquet immutable có manifest/reconciliation. Warning là subset của valid.

Muốn kiểm tra customer FK, tạo full customer Bronze/Silver bằng `make ingest-customers` và `make process-customers`, rồi truyền `CUSTOMER_SNAPSHOT=<customer Silver prefix>` khi process orders. Orders thiếu parent vào deferred; dùng snapshot mới và Silver run ID mới để reprocess cùng Bronze input. Xem [customer contract](docs/data_contracts/06_customer_snapshots.md). Persistent deferred/quarantine lifecycle vẫn planned; Silver không promote end-to-end watermark.

## Order items and payments

Bronze hỗ trợ composite cursors, decimal-as-string và checkpoint độc lập cho hai bảng con. Silver dùng toàn bộ business key, `decimal(18,2)` và bắt buộc full orders snapshot để kiểm tra FK; missing orders vào deferred. Tạo parent bằng `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`, rồi process orders và truyền `ORDER_SNAPSHOT=<orders Silver prefix>` cho `make process-order-items` / `make process-order-payments`.

Items hiện chỉ kiểm tra product/seller ID không thiếu; membership chưa được xác minh. Xem [contract và commands](docs/data_contracts/07_order_children.md). Chạy `make test-order-children-integration` sau thay đổi; suite dùng fixtures và disposable services.

## Silver to PostgreSQL staging

Sau migration `006`, dùng `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX='<prefix>' WAREHOUSE_RUN_ID=<id>`. CLI nhận `WAREHOUSE_DSN` hoặc `PG*` và cùng S3 environment. Chỉ valid rows được COPY/merge vào `raw_staging`; data và `audit.warehouse_load_runs` commit cùng transaction. Source sequence mới thắng, version cũ bị bỏ qua, cùng version khác business hash làm fail; retry cùng ID/input trả `NOOP`.

`make dataset` tự chạy bước này và lưu `*_warehouse.json`. Xem [staging contract](docs/data_contracts/08_warehouse_staging.md); kiểm tra bằng `make test-warehouse-merge-integration` và `make test-warehouse-integration`. DAG audit và end-to-end watermark vẫn planned.

## dbt analytics

Sau staging, `make dataset` tự chạy dbt build/tests cho `dim_customer`, `fct_orders` và daily GMV/AOV views. Items/payments aggregate riêng theo order trước khi join. Customer dùng shopper business key, surrogate key ổn định và current Type 1 attributes. Metric tests đối soát exact với staging, kể cả many-to-many fan-out, canceled orders và zero payments.

Chạy riêng bằng `make sync-analytics`, export `PG*`, `DBT_ENV_SECRET_PASSWORD`, `DBT_READER_USER`, rồi `make dbt-build`. Xem [analytics contract](docs/data_contracts/09_analytics_models.md); kiểm thử bằng `make test-dbt-integration`. Views chưa có atomic publication hoặc end-to-end watermark; chỉ coi build thành công sau khi tests pass.

## License

Source code sử dụng license trong [LICENSE](LICENSE). Olist dataset có license riêng được ghi tại [data/raw/README.md](data/raw/README.md).
