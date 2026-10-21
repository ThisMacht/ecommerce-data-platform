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

Migration `006` thêm bốn bảng `raw_staging` và `audit.warehouse_load_runs`, dùng existing loader/reader roles. `make load-warehouse` đọc verified Silver và merge theo `source_sequence`; retry giữ nguyên run/input. `make test-warehouse-merge-integration` chỉ cần disposable PostgreSQL; `make test-warehouse-integration` thêm MinIO/Spark. Xem [contract](../docs/data_contracts/08_warehouse_staging.md).

Migration `007` tạo `analytics` schema do loader role sở hữu, cấp reader SELECT qua default privileges và dbt grants. dbt chạy trên host bằng `analytics` extra; không cần placeholder service. Xem [analytics contract](../docs/data_contracts/09_analytics_models.md).

Migration `008` thêm deferred quarantine/registration/resolution/attempt tables. Existing loader có write permissions cần thiết, reader chỉ SELECT. `make register-deferred` và `make resolve-deferred` chạy trên host; `make dataset` tự nối vào flow trước dbt. Xem [lifecycle contract](../docs/data_contracts/10_deferred_lifecycle.md).

Migration `009` thêm run/task/dataset audit và pipeline control riêng cho manual full-snapshot namespace `local_core_v1`. Final coordinator giữ staging SHARE locks qua fresh dbt build và atomic cursor CAS. Bronze delivery state không đổi. Xem [pipeline contract](../docs/data_contracts/11_pipeline_control.md).

## Local Airflow

Airflow chạy trên host, **không** mount Docker socket vào container. Host cần
Python 3.12, uv, Java 17, Docker Compose và quyền chạy local pipeline như trước.
`airflow/pyproject.toml` pin Airflow 3.1.8, `airflow/uv.lock` khóa transitive packages;
`airflow/.venv` tách khỏi Spark/dbt `.venv`. Không cài Airflow vào root environment.

```bash
make sync-airflow
make test-airflow
make airflow-local
```

Wrapper `scripts/airflow.sh` pin metadata vào SQLite trong ignored `build/airflow`,
executor `LocalExecutor`, parallelism 1, DAG folder của checkout, tắt example DAGs
và bind API/UI vào `127.0.0.1:8080`. Standalone khởi tạo metadata/auth local và
chạy các components; Ctrl-C dừng components nhưng giữ metadata và data services.
Không commit password files/logs. Không dùng deployment này ngoài máy local.

DAGs `olist_core_full_snapshot` và `olist_core_incremental`: daily UTC, paused upon creation, no catchup,
max active runs 1; task `run_core_pipeline` retries 2 lần, delay 1 phút, timeout
1 giờ. Một task dùng `BashOperator` gọi runner; business logic/audit 19 stages
không sao chép vào DAG. Incremental wrapper thêm source freeze/full FK stages (24 tasks). UI cho thấy execution tổng, PostgreSQL audit cho thấy stages.
Chưa có stage-level Airflow graph, callbacks hoặc notification integration.

Trigger thủ công qua UI, kiểm tra audit/report trước khi bật schedule. Thiếu raw
snapshot hoặc Java làm task fail; không chấp nhận local skip/PARTIAL làm success.
Runner giữ file lock và namespace hiện có; manual runner và DAG không được chạy
đồng thời. Hai identity context được truyền qua operator `env`, không interpolate
vào shell. Pipeline ID là `airflow_` + SHA-256 của JSON `[dag_id, run_id]`, không
phụ thuộc task try number. Context được pin trong pipeline request.

Retry/Clear failed task của **cùng DAG run** để resume. Không tạo DAG run mới,
không xóa pending file, không đổi hashes để vượt conflict. `airflow dags test`
tạo run mới nên không phải recovery command. Nếu cần chạy runner trực tiếp,
export `OLIST_AIRFLOW_DAG_ID` và `OLIST_AIRFLOW_RUN_ID` đúng original context rồi
`make dataset`; default manual call không có context sẽ bị configuration gate
reject. Resume phải giữ code/config cũ. Completed run retry trả NOOP kể cả khi
pending file đã được dọn. Crash có thể để audit RUNNING tới lần resume.

Full DAG giữ `local_core_v1`/manual. Incremental DAG đặt `DATASET_MODE=incremental`,
dùng `incremental_core_v1`/incremental và pipeline-owned 48h windows. Cả hai không
sử dụng logical date làm backfill bounds; chỉ unpause một mode trên local checkout.
Isolated backfill và PostgreSQL Airflow metadata còn planned. Chi tiết:
[incremental contract](../docs/data_contracts/12_incremental_pipeline.md).

Khi chủ động nâng Airflow, resolve lại với official constraints rồi commit cả lock:

```bash
uv add --project airflow --python 3.12 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt 'apache-airflow==3.1.8'
```

Sau nâng version chạy fast tests, DAG import/serialization và local smoke.
Tham khảo [Apache Airflow quick start](https://airflow.apache.org/docs/apache-airflow/3.1.8/start.html)
và [BashOperator env guidance](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html).

## TODO

- `TODO(orchestration)`: isolated backfill và deployment metadata PostgreSQL.
- `TODO(phase-3)`: thêm Metabase sau khi marts và read-only grants ổn định.
- `TODO(security)`: dùng secret manager và network policy khi có deployment ngoài local.

Không tạo container placeholder cho service chưa có executable code hoặc integration test.
