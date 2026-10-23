# Olist E-commerce Data Platform

Batch data-engineering project xây từ Olist raw CSV đến PostgreSQL source simulator, immutable Bronze/Silver layers, warehouse models và dashboards. Hiện đã có source validation, PostgreSQL source layer/simulator và tám core datasets ở Bronze/PySpark Silver trên MinIO; các phase sau được theo dõi trong [Pipeline Implementation Plan](ecommerce_data_warehouse_pipeline_plan_v4.md).

## Current status

| Capability | Status |
| --- | --- |
| Raw snapshot manifest and checksum verification | Implemented |
| Deterministic source profiling | Implemented |
| Versioned source contract | Implemented |
| Dockerized PostgreSQL, migrations and source seed | Implemented |
| Live schema contract checks and isolated source integration tests | Implemented |
| Source simulator mutation batches | Implemented — 4 core tables |
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
| GitHub Actions CI: fast checks, package build and simulator integration | Implemented |
| Full products/sellers Bronze/Silver, staging and Type 1 dimensions | Implemented — scheduled in all core modes, frozen full snapshots and item FK v2 |
| dbt calendar/Type 1 dimensions, order/item/payment/delivery facts and daily marts | Implemented — GMV/AOV, product/seller/payment-type and delivery cohort metrics; exact reconciliation and relationship tests |
| Airflow orchestration | Implemented — paused daily full/incremental and manual backfill DAGs, isolated runtime and stable retry identity |
| Business dashboard / Metabase | Implemented — date-filtered GMV/paid/AOV, product/seller rankings and payment-type charts; restricted reader and publication gate |

## Quick start

Đặt đủ 9 CSV Olist canonical vào `data/raw/`, rồi chạy:

```bash
make dataset
```

[`scripts/make_dataset.sh`](scripts/make_dataset.sh) tự tạo `.env` nếu thiếu, sync dependencies, verify/seed PostgreSQL, start MinIO và chạy customers/products/sellers/geolocation → full orders → order_items/order_payments/order_reviews qua Bronze/Silver rồi PostgreSQL staging và dbt build/tests, tự truyền parent snapshots cho FK checks. Không cần nhập run IDs, prefixes hay export local credentials; runner lấy cấu hình từ Compose và lưu kết quả trong ignored `build/reports/dataset/`. Cần Python 3.10+, uv, Docker Compose; Java 17 cho Silver/staging (tự nhận diện JDK 17 trong `/usr/lib/jvm` nếu chưa đặt `JAVA_HOME`).

Runner xử lý full snapshots cả 8 datasets, freeze cùng source snapshot trước publication. Thiếu bất kỳ CSV nào trong snapshot 9 files thì seed/downstream được bỏ qua và liệt kê file thiếu; loader hiện không hỗ trợ partial seed. Thiếu Java thì chỉ chạy Bronze, báo `PARTIAL` và giữ pending ID để resume. CSV có sẵn nhưng sai checksum, migration hoặc processing/load lỗi sẽ dừng. Sau lỗi, gọi lại `make dataset` với code/config cũ để resume; sau success, lần gọi tiếp tạo run mới. Runner giữ local file lock; services/volumes được giữ lại. Pending runs từ bounded runner cũ phải hoàn tất bằng config cũ trước khi chuyển sang full mode; xem [pipeline contract](docs/data_contracts/11_pipeline_control.md).

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

`pyproject.toml` khai báo package, Python version, CLI entry points và development tools. `uv.lock` khóa dependencies và phải được commit. Source dùng Psycopg 3, object storage dùng Boto3, Ruff thuộc development group. PySpark nằm trong optional extra `processing`: dùng Java 17 và `make sync-processing`. dbt dùng extra `analytics`. Airflow có Python 3.12 environment và lock riêng trong `airflow/`.

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

CI không cần `.env`, raw CSV hay repository secrets. Integration suites tự tạo credentials cho services tạm và cleanup container/volume khi kết thúc. Full snapshot suite (`make test-source-integration`) vẫn chạy local sau khi tải canonical CSV; Job orchestration import/serialize DAG bằng runtime Airflow thật, độc lập với fast checks.

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

Muốn kiểm tra customer FK, tạo full customer Bronze/Silver bằng `make ingest-customers` và `make process-customers`, rồi truyền `CUSTOMER_SNAPSHOT=<customer Silver prefix>` khi process orders. Orders thiếu parent vào deferred; dùng snapshot mới và Silver run ID mới để reprocess cùng Bronze input, hoặc dùng [durable deferred lifecycle](docs/data_contracts/10_deferred_lifecycle.md) để resolve frozen pending records. Silver không promote end-to-end watermark.

## Order items and payments

Bronze hỗ trợ composite cursors, decimal-as-string và checkpoint độc lập cho hai bảng con. Silver dùng toàn bộ business key, `decimal(18,2)` và bắt buộc full orders snapshot để kiểm tra FK; missing orders vào deferred. Tạo parent bằng `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`, rồi process orders và truyền `ORDER_SNAPSHOT=<orders Silver prefix>` cho `make process-order-items` / `make process-order-payments`.

Items v1 chỉ kiểm tra product/seller ID không thiếu. [V2](docs/data_contracts/17_item_dependency_snapshots.md) kiểm tra đủ order/product/seller FK khi cấp ba full snapshots; cả ba core modes tự bật v2. Xem [contract và commands](docs/data_contracts/07_order_children.md). Chạy `make test-order-children-integration` sau thay đổi; suite dùng fixtures và disposable services.

Products có vertical slice riêng: `make ingest-products`, `make process-products`,
`make load-warehouse SOURCE_TABLE=products`, rồi `make dbt-build`. Xem
[product contract và prerequisites](docs/data_contracts/15_product_snapshots.md).
Products được full refresh trong full/incremental/backfill và là FK parent của items.
Không có product delta cursor; full/empty snapshots không xóa staging rows.

Sellers có luồng tương tự: `make ingest-sellers`, `make process-sellers`, rồi
`make load-warehouse SOURCE_TABLE=sellers` và `make dbt-build`. Xem
[seller contract](docs/data_contracts/16_seller_snapshots.md). Hai slice được
schedule bởi core runner; standalone item FK v2 bật bằng flags theo [contract 17](docs/data_contracts/17_item_dependency_snapshots.md).

## Silver to PostgreSQL staging

Sau migration `006`, dùng `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX='<prefix>' WAREHOUSE_RUN_ID=<id>`. CLI nhận `WAREHOUSE_DSN` hoặc `PG*` và cùng S3 environment. Chỉ valid rows được COPY/merge vào `raw_staging`; data và `audit.warehouse_load_runs` commit cùng transaction. Source sequence mới thắng, version cũ bị bỏ qua, cùng version khác business hash làm fail; retry cùng ID/input trả `NOOP`.

`make dataset` tự chạy bước này và lưu `*_warehouse.json`. Xem [staging contract](docs/data_contracts/08_warehouse_staging.md); kiểm tra bằng `make test-warehouse-merge-integration` và `make test-warehouse-integration`. Local run/task audit và gated pipeline watermarks đã có ở [pipeline contract](docs/data_contracts/11_pipeline_control.md); full/incremental/manual-backfill Airflow wrappers đã có.

## dbt analytics

Sau staging, `make dataset` tự chạy dbt build/tests cho `dim_customer`, `fct_orders` và daily GMV/AOV views. Items/payments aggregate riêng theo order trước khi join. Customer dùng shopper business key, surrogate key ổn định và current Type 1 attributes. Metric tests đối soát exact với staging, kể cả many-to-many fan-out, canceled orders và zero payments.

Chạy riêng bằng `make sync-analytics`, export `PG*`, `DBT_ENV_SECRET_PASSWORD`, `DBT_READER_USER`, rồi `make dbt-build`. Xem [analytics contract](docs/data_contracts/09_analytics_models.md); kiểm thử bằng `make test-dbt-integration`. Views chưa có atomic publication hoặc end-to-end watermark; chỉ coi build thành công sau khi tests pass.

## Business dashboard

Sau một pipeline SUCCESS, chạy `make metabase-up`, rồi mở
[Metabase local](http://127.0.0.1:3000). URL dashboard và IDs nằm trong ignored
`build/metabase/dashboard.json`; admin login được tạo trong
`build/metabase/credentials.json` (0600). `METABASE_PORT` đổi host port trong `.env`.

Dashboard có GMV/gross paid/AOV/eligible-order KPIs, daily trends, status và
completeness; start/end date inclusive. AOV dùng tổng paid amount / paid orders.
Dedicated BI reader không có quyền đọc source/raw staging. Cards bị chặn khi latest
pipeline chưa SUCCESS hoặc có audited load mới hơn. `make metabase-setup` reconcile
managed resources; `make metabase-down` stop và giữ metadata. Xem
[business dashboard contract](docs/data_contracts/14_business_dashboard.md).
Metabase/H2 metadata dành cho local demo; live views chưa atomic xuyên các cards.

## Dirty-data verification

`make test-dirty-data` với Java 17 chạy Spark trên 44 deterministic synthetic
records của bốn core tables. Handwritten expected-error manifest kiểm tra từng
physical UID, exact route và error/warning/deferred rules; checksums pin input
bytes. Các fixtures cũng nằm trong `make test-processing` của CI. Xem
[fixture guide](tests/fixtures/silver/README.md). Command không sửa raw CSV hoặc
source database; không đại diện cho toàn bộ rule combinations/schema failures.

## Deferred retries

`make dataset` tự register missing-parent records, rồi resolve theo full parent snapshot sau từng dependent dataset và trước dbt. Queue lưu original UID/payload, default ba attempts hoặc bảy ngày. Retry cùng run ID không tăng attempts; expiry là terminal reject. Queue transitions, version-aware staging merge và audit commit cùng transaction.

Standalone operations: `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>`, rồi `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<new id>`. Xem [contract](docs/data_contracts/10_deferred_lifecycle.md). Mỗi pass tối đa 1.000 rows/bảng; chưa có background scheduler; local watermarks chỉ được commit ở final pipeline gate.

## Pipeline completion and watermarks

`make dataset` ghi run/task audit và đối chiếu server-side Bronze/Silver/warehouse/deferred receipts. Final gate khóa ghi staging trong lúc chạy fresh dbt build; chỉ sau tất cả tasks/tests/counts đạt mới commit bốn pipeline cursors bằng CAS cùng reconciliation audit. Lỗi giữ watermark cũ; committed replay trả `NOOP`.

Xem [pipeline contract](docs/data_contracts/11_pipeline_control.md); chạy `make test-pipeline-control-integration` và `make test-pipeline-integration` sau thay đổi gates. Full mode dùng `local_core_v1`; incremental dùng `incremental_core_v1`, có Airflow wrapper riêng. Backfill dùng `backfill_core_v1`, giữ nguyên scheduled cursors. Atomic analytics publication chưa triển khai.

Kiểm thử recovery bằng `make test-recovery-integration` với Java 17: lỗi sau
Bronze/Silver/warehouse commit và sau dbt/CAS trước final commit, cho incremental
và backfill. Suite xác minh frozen bytes, receipts, queue attempts, row counts và
watermarks khi resume; xem [failure recovery evidence](docs/data_contracts/11_pipeline_control.md#failure-recovery-verification).

`make pipeline-health` đọc audit qua `PG*` (có thể dùng reader role), xuất JSON về run đang chạy/gần nhất, lần thành công cuối, freshness, runtime, reject/warning rates và deferred backlog. Ngưỡng nằm trong `config/pipeline_health.json`; đổi bằng `PIPELINE_HEALTH_CONFIG=<path>`. CLI `olist-pipeline-health` trả exit `0` healthy, `1` vượt ngưỡng/thiếu success, `2` không thể kiểm tra; Make trả nonzero khi CLI thất bại. Command không tự đọc `.env`, không thay đổi queue/watermark và chưa tự được lập lịch. Chi tiết cách tính và giới hạn ở [operational health](docs/data_contracts/11_pipeline_control.md#operational-health).

`make pipeline-dashboard` tạo `build/reports/pipeline_dashboard.html` từ cùng health
snapshot (export `PG*` như trên), mở trực tiếp bằng browser. Override path bằng
`PIPELINE_DASHBOARD_OUTPUT=<path>`. Dashboard hiển thị run/attempt, issues, actual
metrics và thresholds; không cần web server. CLI vẫn xuất JSON và trả health exit
code; HTML vẫn được tạo khi unhealthy. Đây là snapshot tĩnh có timestamp, không tự
refresh. Nếu không đọc được database hoặc ghi file thất bại, command báo lỗi;
file cũ có thể còn tồn tại nên cần kiểm tra timestamp và exit code.

## Item analytics

`fct_order_items` giữ grain `(order_id, order_item_id)`, join current product/seller
Type 1 keys và order context. `mart_product_daily`/`mart_seller_daily` tính item
counts, distinct orders trong từng group, GMV và freight theo local purchase date.
Không join payments hoặc phân bổ AOV/payout xuống items. Order counts không cộng
được giữa products/sellers. Xem [metric contract](docs/data_contracts/09_analytics_models.md#item-facts-and-productseller-metrics).
Chạy `make test-dbt-integration`; `make dataset` tự build/test các models mới.
Metabase có top-10 product/seller tables và payment-type charts, chỉ SELECT trên
aggregate marts; xem [dashboard contract](docs/data_contracts/14_business_dashboard.md).

## Payment analytics

`fct_payments` giữ grain `(order_id, payment_sequential)`, LEFT JOIN order context.
`mart_payment_type_daily` tính entry counts, distinct eligible orders trong từng
payment type/date và observed gross paid. Không nhân amount với installments,
không join items và không dùng type order counts làm AOV denominator. `make dataset`
tự build/test; standalone dùng `make dbt-build` với PG/dbt environment hiện có.
Xem [payment metric contract](docs/data_contracts/09_analytics_models.md#payment-facts-and-type-metrics).

## Review snapshots

Reviews chạy full Bronze/Silver/staging standalone hoặc trong cả ba core modes ở grain `(review_id, order_id)`;
`review_id` hoặc `order_id` riêng lẻ không unique. Dùng `make ingest-order-reviews`,
`make process-order-reviews` với `ORDER_SNAPSHOT=<full orders Silver prefix>`, rồi
warehouse/deferred commands với `SOURCE_TABLE=order_reviews`. Score 1–5, UTC/DST,
optional comments và bounded retry được kiểm tra; core runner tự xử lý reviews,
chưa có review marts hoặc BI grants. Xem [review contract](docs/data_contracts/19_review_snapshots.md).

Reviews được freeze cùng source transaction, pin full orders và có normal/deferred
receipts bắt buộc. `review_snapshot_rows` và backlog xuất hiện trong health/dashboard;
không thêm cursor hoặc dbt/BI model. Bounded modes refresh current reviews ngoài delta
window; xem [review contract](docs/data_contracts/19_review_snapshots.md).

## Geolocation analytics

`ingest-geolocation` / `process-geolocation` chạy full observations standalone;
cả ba core modes tự freeze, publish và load geolocation cùng các datasets khác.
Standalone load với `SOURCE_TABLE=geolocation`, sau đó `make dbt-build`. `dim_location` có một
row/ZIP với exact coordinate medians và deterministic joint city/state mode.
Không dùng ZIP làm source key, không drop observations có tọa độ trùng hoặc suy
đoán địa chỉ customer/seller. Hai dimensions có nullable `location_key` theo ZIP;
unmatched ZIP giữ NULL. Full runner có 34 tasks, bounded modes có 38; bốn cursors
và BI grants giữ nguyên. Xem [geolocation contract](docs/data_contracts/18_geolocation_snapshots.md).

## Calendar dimension

`dim_date` chứa liên tục các ngày từ min/max local purchase, estimated và actual
delivery dates; dữ liệu rỗng cho lịch rỗng. Bốn facts có purchase date key YYYYMMDD;
delivery thêm estimated/actual role keys. ISO week dùng cùng ISO year, weekday
Monday=1; không tự suy diễn holiday calendar. `make dbt-build` hoặc các core runners
tự build/test; BI grants giữ nguyên. Xem [calendar contract](docs/data_contracts/09_analytics_models.md#calendar-dimension-and-fact-date-roles).

## Delivery analytics

`fct_delivery` giữ một row/order với milestones, chronology flags và UTC elapsed
hours. Lateness dùng ngày local Sao Paulo: giao cùng ngày hẹn là on-time. Missing/
invalid timestamps giữ nguyên, metrics NULL và có completeness counts riêng.
`mart_delivery_daily` theo purchase-date cohort, rate chỉ dùng valid delivered
observations; không coi undelivered là on-time hoặc average daily rates để lấy
late rate toàn kỳ. Xem [delivery contract](docs/data_contracts/09_analytics_models.md#delivery-facts-and-cohort-metrics).
Models tự build/test trong pipeline, chưa cấp quyền/cards Metabase cho delivery.

## Incremental core pipeline

`make dataset-incremental` bootstrap namespace riêng lần đầu, sau đó lấy window từ
**pipeline watermark** với lookback 48 giờ inclusive. Freeze chốt đồng thời bốn
source deltas và sáu full snapshots (customers/orders/products/sellers/geolocation/order_reviews) trong một PostgreSQL snapshot, lưu
bytes/bounds trước publication. Retry không lấy lại upper mới; delivery checkpoint
không điều khiển window. Full mode `make dataset` freeze tám full streams.

Orders/items/payments FK và deferred retries luôn dùng full parents, không dùng
delta làm key set. Customer delta là Silver v2, full customer parent vẫn v1.
Pipeline audits 38 tasks, dùng cùng final receipt/reconciliation/dbt/CAS gates.
Bootstrap/after-cursor/lookback, FK scans và retry inputs có counters riêng;
after-cursor không phải net business growth. Full parent scans vẫn có chi phí.

```bash
make dataset-incremental
make pipeline-health PIPELINE_NAMESPACE=incremental_core_v1
make pipeline-dashboard PIPELINE_NAMESPACE=incremental_core_v1
```

Giữ cùng mode/code/config/pending ID khi resume; updates ngoài 48 giờ chưa được bảo
đảm tự thu thập. Xem [incremental contract](docs/data_contracts/12_incremental_pipeline.md),
kiểm tra bằng `make test-incremental-integration` với Java 17.

## Isolated backfill

Repair theo `source_updated_at` trong khoảng UTC **[start, end)**, giữ nguyên
watermark full/incremental. Chạy lại cùng command/bounds để resume sau lỗi:

```bash
make dataset-backfill BACKFILL_START=2020-01-01T00:00:00Z BACKFILL_END=2020-02-01T00:00:00Z
```

Bốn deltas và sáu full inputs được freeze cùng transaction. Backfill chỉ
retry queue thuộc run đó; merge không ghi đè newer source versions. Final gate
vẫn chạy fresh dbt và reconciliation. Dữ liệu sửa đi vào staging/marts dùng chung;
đây không phải historical restore của source versions đã bị overwrite.
Xem [backfill contract](docs/data_contracts/13_backfill_pipeline.md), kiểm tra bằng
`make test-backfill-integration`. Health/dashboard dùng `PIPELINE_NAMESPACE=backfill_core_v1`.

## Local Airflow orchestration

Cài Python 3.12, uv, Docker Compose, Java 17 và đủ 9 CSV canonical, rồi chạy:

```bash
make sync-airflow test-airflow
make airflow-local
```

UI bind `127.0.0.1:8080`; credentials local được tạo trong ignored
`build/airflow/`. Hai DAG `olist_core_full_snapshot` và `olist_core_incremental` mặc định paused, schedule `@daily`
UTC, `catchup=False`, `max_active_runs=1`. Trigger thủ công để kiểm tra trước khi
unpause. Chỉ bật schedule của mode muốn dùng. Mỗi DAG có một task gọi runner; full/incremental audit 19/24 business tasks trong PostgreSQL.
DAG `olist_core_backfill` manual-only nhận conf `start`/`end`, truyền bounds qua environment; Retry/Clear cùng DAG run để resume.
Dừng standalone bằng Ctrl-C; PostgreSQL/MinIO và metadata được giữ lại.

Retry/Clear task của **cùng DAG run** giữ pipeline ID, replay receipts hoặc NOOP
sau commit. Không trigger run mới để thay thế unfinished run và không sửa code/config
trong khi run đang pending. Airflow thiếu CSV/Java sẽ fail thay vì skip. Đây là
local standalone orchestration dùng SQLite riêng; deployment production còn planned. Xem [operations](infrastructure/README.md#local-airflow).

## License

Source code sử dụng license trong [LICENSE](LICENSE). Olist dataset có license riêng được ghi tại [data/raw/README.md](data/raw/README.md).
