# Local Infrastructure

Infrastructure triển khai PostgreSQL source layer và MinIO cho orders Bronze. Database/object storage là stateful services; migrations, source loader và source verification là ephemeral jobs.

## Implemented services

| Service | Lifecycle | Purpose |
| --- | --- | --- |
| `postgres` | Stateful service | Lưu `olist_source`, source metadata và migration/seed audit |
| `minio` | Stateful service | Lưu immutable orders Bronze/Silver, manifests và publication markers |
| `migrate` | Ephemeral job | Apply versioned migrations và kiểm tra checksum |
| `source-loader` | Ephemeral job | COPY raw CSV read-only vào source tables theo transaction |
| `source-check` | Ephemeral job | Đọc bằng read-only role để export schema catalog và xác minh source state |

PostgreSQL dùng image `postgres:17.10-alpine3.23`. Tag khóa cả PostgreSQL minor version và Alpine base version. Named volume `postgres_data` giữ dữ liệu khi `make infrastructure-down`; command này không xóa volume.

## Quick start

```bash
cp .env.example .env
make sync
make test
make seed-source
make verify-source
make infrastructure-status
```

`.env` đã được ignore. Default credentials chỉ dành cho máy local; đổi password nếu port được truy cập bởi người dùng khác.

Host-side Python commands sử dụng editable package từ `src/olist_platform` qua uv. Cài Python `>=3.10` và uv, rồi chạy `make sync` từ repository root. PostgreSQL jobs tiếp tục dùng `psql` trong pinned image, độc lập với môi trường Python trên host.

## Operations

```bash
make infrastructure-up
make migrate
make seed-source
make verify-source
make infrastructure-logs
make infrastructure-down
```

Raw CSV được mount `read-only`; PostgreSQL và MinIO ghi vào named volumes riêng. PostgreSQL bind `127.0.0.1:5432`, đổi port qua `POSTGRES_PORT` trong `.env`.

## Source verification and integration tests

`make verify-source` kiểm tra cả live schema và source state. Có thể chỉ kiểm tra schema bằng `make verify-source-schema`; JSON catalog nằm trong ignored `build/reports/source_schema.json`. Catalog query dùng reader role và được đối chiếu với `config/source_contract.json` trên host.

`make test-source-integration` dùng PostgreSQL image khai báo trong Compose để start container riêng có tên ngẫu nhiên, không expose port và không dùng local `postgres_data`. Mỗi test có database mới và chạy production migration/seed runners; container và anonymous data volume được cleanup kể cả khi test fail. Tests yêu cầu full canonical raw snapshot, được verify trước khi tạo container. `RAW_DIR`, `RAW_MANIFEST`, `SOURCE_CONTRACT` và `COMPOSE_FILE` được truyền từ Make vào runner; seed hiện vẫn chỉ hỗ trợ snapshot Olist v1.

Readiness check dùng TCP để không nhận nhầm temporary initialization server của PostgreSQL image. Suite kiểm tra schema drift, migration no-op, seed no-op với full-row fingerprints/metadata/sequence state, preservation of source updates, rollback khi table/audit insert lỗi và retry sau failure. `setval` không rollback theo PostgreSQL semantics; gaps sau failed transaction được chấp nhận.

## Source simulator

Simulator host CLI dùng Psycopg qua `SOURCE_SIMULATOR_DSN` hoặc standard `PG*` environment. Chạy `make migrate`/`make seed-source` trước để apply migration `002` và cấp audit grants. Compose `.env` không tự được export vào CLI. `make test-source-simulator-integration` dùng container tạm riêng có ephemeral localhost port và small reference fixture; không cần raw CSV. Xem [simulator contract](../docs/data_contracts/03_source_simulator.md).

## MinIO and orders Bronze

`make bronze-up` build và start MinIO, đợi `/minio/health/ready`. Release `2025-10-15` được build từ upstream commit/checksummed archive trong `minio/Dockerfile`; lần đầu cần Internet và vài phút compile. API/console bind localhost ports 9000/9001; named volume `minio_data` được giữ khi down. `.env.example` có local credentials và port overrides.

Sau source seed/migration `003`, export database/S3 credentials và dùng `make init-bronze`, `make ingest-orders BRONZE_RUN_ID=<id>`. Host CLI không tự load `.env`. Migration `003` tạo Bronze-only control/run/page state; không sửa source contract hoặc end-to-end watermark. Xem [orders Bronze contract](../docs/data_contracts/04_orders_bronze.md).

`make test-bronze-integration` build cùng image, tạo PostgreSQL/MinIO tạm với ephemeral localhost ports và anonymous volumes; không dùng local services hoặc CSV. Cleanup chỉ nhắm container/volume riêng của suite.

## Orders Silver runtime

Spark hiện chạy trên host bằng optional `processing` extra, không có placeholder service. Dùng Java 17, `make sync-processing` và `make process-orders BRONZE_PREFIX=<prefix> SILVER_RUN_ID=<id>`. Silver dùng cùng S3 environment/bucket, không cần PostgreSQL connection; xem [contract](../docs/data_contracts/05_orders_silver.md). `make test-silver-integration` kiểm tra toàn slice bằng local Spark và disposable services.

Migration `004` thêm customer snapshot control/run/page tables riêng. `make ingest-customers` đọc full snapshot, không dùng orders lookback; `make process-customers` publish customer Silver để orders dùng qua `CUSTOMER_SNAPSHOT`. `make test-customer-integration` kiểm tra late parents và pinned snapshot recovery. Xem [contract](../docs/data_contracts/06_customer_snapshots.md).

Migration `005` tạo Bronze control/run/page tables riêng cho `order_items` và `order_payments`. Hai bảng dùng composite cursors và cùng publication/recovery boundary. `make test-order-children-integration` kiểm tra child Silver với pinned full orders snapshots; xem [contract](../docs/data_contracts/07_order_children.md). Migrations cũ giữ nguyên checksum.

## TODO

- `TODO(phase-1)`: thêm Airflow và dbt với executable workflows/models.
- `TODO(phase-3)`: thêm Metabase sau khi marts và read-only grants ổn định.
- `TODO(security)`: dùng secret manager và network policy khi có deployment ngoài local.

Không tạo container placeholder cho service chưa có executable code hoặc integration test.
