# Local Infrastructure

Infrastructure hiện triển khai PostgreSQL source layer tối thiểu. Database service chạy liên tục trong Docker; migration, source loader và source verification chạy dưới dạng ephemeral container jobs dùng cùng image đã pin.

## Implemented services

| Service | Lifecycle | Purpose |
| --- | --- | --- |
| `postgres` | Stateful service | Lưu `olist_source`, source metadata và migration/seed audit |
| `migrate` | Ephemeral job | Apply versioned migrations và kiểm tra checksum |
| `source-loader` | Ephemeral job | COPY raw CSV read-only vào source tables theo transaction |
| `source-check` | Ephemeral job | Đọc bằng read-only role và xác minh source state |

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

Raw CSV được mount `read-only`; chỉ named volume của PostgreSQL có quyền ghi. Port mặc định bind vào `127.0.0.1:5432` và có thể đổi qua `POSTGRES_PORT` trong `.env`.

## TODO

- `TODO(phase-0)`: thêm MinIO sau khi source simulator có deterministic batches.
- `TODO(phase-1)`: thêm Airflow, Spark và dbt cùng vertical slice đầu tiên.
- `TODO(phase-3)`: thêm Metabase sau khi marts và read-only grants ổn định.
- `TODO(security)`: dùng secret manager và network policy khi có deployment ngoài local.

Không tạo container placeholder cho service chưa có executable code hoặc integration test.
