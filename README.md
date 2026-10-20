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
| Source simulator mutation batches | Implemented — 4 core tables |
| GitHub Actions CI: fast checks, package build and simulator integration | Implemented |
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
- `src/olist_platform/source_simulator/`: batch artifacts, deterministic fixtures và transactional apply.
- `scripts/`: compatibility wrappers cho các Python commands trước đây; cần cài package trước khi dùng.
- `tests/unit/`: `unittest` và fixture nhỏ, gồm kiểm tra installed CLI từ thư mục ngoài checkout.
- `tests/integration/`: PostgreSQL tests dùng container tạm; simulator dùng small SQL fixture trong `tests/fixtures/`, source seed dùng canonical raw snapshot.
- `config/`: raw manifest và source contract versioned.
- `infrastructure/`: Docker Compose cùng PostgreSQL migrations, seed và checks.
- `data/raw/`: immutable CSV local, không commit vào Git.
- `docs/`: contracts, generated source profile và runbooks; JSON reports nằm trong `build/reports/` đã ignore.

Chỉ thêm folders ingestion, Airflow, Spark và dbt khi có implementation tương ứng.

## Python development

`pyproject.toml` khai báo package, Python version, CLI entry points và development tools. `uv.lock` khóa dependencies và phải được commit. Simulator dùng Psycopg 3 với binary adapter; Ruff thuộc development group. Dependencies của Airflow, Spark và dbt sẽ được quản lý theo runtime/service khi triển khai.

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
- `PostgreSQL source simulator`: Python 3.12, chạy `make test-source-simulator-integration` với PostgreSQL image từ Compose và disposable database cho từng test.

CI không cần `.env`, raw CSV hay repository secrets. Simulator suite tự tạo credentials cho database tạm và cleanup container/volume khi kết thúc. Full snapshot suite (`make test-source-integration`) vẫn chạy local sau khi tải canonical CSV; chưa có DAG, Spark hoặc dbt checks trong CI.

Tái hiện CI từ checkout sạch: `make sync`, `make test`, `make build` và `make test-source-simulator-integration`. Workflow pin action commit SHAs và uv version; cập nhật các pins cùng validation khi nâng tools.

## Documentation

- [Documentation reading order](docs/README.md)
- [Raw dataset manifest](data/raw/README.md)
- [Local infrastructure](infrastructure/README.md)
- [Source simulator contract and usage](docs/data_contracts/03_source_simulator.md)
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

## License

Source code sử dụng license trong [LICENSE](LICENSE). Olist dataset có license riêng được ghi tại [data/raw/README.md](data/raw/README.md).
