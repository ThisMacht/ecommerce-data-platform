# Olist E-commerce Data Platform

Batch data-engineering project xây từ Olist raw CSV đến PostgreSQL source simulator, immutable Bronze/Silver layers, warehouse models và dashboards. Repository hiện đã triển khai source validation cùng PostgreSQL source layer tối thiểu; các phase sau được theo dõi trong [Pipeline Implementation Plan](ecommerce_data_warehouse_pipeline_plan_v4.md).

## Current status

| Capability | Status |
| --- | --- |
| Raw snapshot manifest and checksum verification | Implemented |
| Deterministic source profiling | Implemented |
| Versioned source contract | Implemented |
| Dockerized PostgreSQL, migrations and source seed | Implemented |
| Live schema contract checks and isolated source integration tests | Implemented |
| Source simulator mutation batches | TODO — Phase 0 |
| MinIO, Airflow, Spark, dbt and Metabase | TODO — later phases |

## Quick start

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
- `scripts/`: compatibility wrappers cho các Python commands trước đây; cần cài package trước khi dùng.
- `tests/unit/`: `unittest` và fixture nhỏ, gồm kiểm tra installed CLI từ thư mục ngoài checkout.
- `tests/integration/`: PostgreSQL tests dùng container tạm và canonical raw snapshot.
- `config/`: raw manifest và source contract versioned.
- `infrastructure/`: Docker Compose cùng PostgreSQL migrations, seed và checks.
- `data/raw/`: immutable CSV local, không commit vào Git.
- `docs/`: contracts, generated source profile và runbooks; JSON reports nằm trong `build/reports/` đã ignore.

Chỉ thêm folders simulator, ingestion, Airflow, Spark và dbt khi có implementation tương ứng.

## Python development

`pyproject.toml` khai báo package, Python version, CLI entry points và development tools. `uv.lock` khóa dependencies và phải được commit. Phase 0 chưa cần third-party runtime dependencies; Ruff thuộc development group. Dependencies của Airflow, Spark và dbt sẽ được quản lý theo runtime/service khi triển khai.

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

## Documentation

- [Documentation reading order](docs/README.md)
- [Raw dataset manifest](data/raw/README.md)
- [Local infrastructure](infrastructure/README.md)
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

## License

Source code sử dụng license trong [LICENSE](LICENSE). Olist dataset có license riêng được ghi tại [data/raw/README.md](data/raw/README.md).
