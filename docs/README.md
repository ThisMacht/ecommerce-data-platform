# Documentation

Thư mục này chứa tài liệu kỹ thuật và vận hành của project. Prefix hai chữ số trong filename thể hiện thứ tự đọc đề xuất trên toàn bộ `docs/`.

## Reading order

| Order | Document | Purpose | Status |
| ---: | --- | --- | --- |
| 00 | [Olist Source Profile](data_contracts/00_source_profile.md) | Kết quả profiling, key validation, null statistics và source relationships | Generated — Active |
| 01 | [Batch Data Processing Runbook](runbooks/01_data_processing_runbook.md) | Quy trình vận hành, retry, backfill, reconciliation và incident recovery | Draft — Pre-implementation |
| 02 | [Source Contracts](data_contracts/02_source_contracts.md) | Source boundary, grains, keys, types, simulator metadata và seed semantics | Active |
| 03 | [Source Simulator](data_contracts/03_source_simulator.md) | Batch format, deterministic fixtures, transactional apply và replay semantics | Active — four-table MVP |
| 04 | [Orders Bronze](data_contracts/04_orders_bronze.md) | Bounded extraction, durable snapshot retry, MinIO publication và Bronze-only checkpoint | Active — orders CLI slice |
| 05 | [Orders Silver](data_contracts/05_orders_silver.md) | Spark normalization, routing, deduplication, Parquet và replay | Active — orders local-rule slice |
| 06 | [Customer Snapshots](data_contracts/06_customer_snapshots.md) | Full customers Bronze/Silver, orders FK routing và pinned dependency replay | Active |

## Related documents

| Document | Purpose |
| --- | --- |
| [Pipeline Implementation Plan](../ecommerce_data_warehouse_pipeline_plan_v4.md) | Kiến trúc, phases, contracts và implementation roadmap |
| [Raw Dataset Manifest](../data/raw/README.md) | Nguồn dữ liệu, file inventory, checksums và raw-data handling rules |
| [Local Infrastructure](../infrastructure/README.md) | Docker services, jobs, configuration và local operations |

## Naming convention

- Sử dụng format `NN_descriptive_name.md`, trong đó `NN` là thứ tự đọc gồm hai chữ số.
- `docs/README.md` là entrypoint theo convention nên không có numeric prefix.
- Giữ Markdown headings, table headers, status values và technical keys bằng tiếng Anh; phần giải thích được viết bằng tiếng Việt.
- Generated document phải được đổi tên tại generator và Make target, không rename riêng output file.
- Khi thay đổi thứ tự hoặc filename, phải cập nhật `Reading order` cùng mọi local links trong cùng một change.
