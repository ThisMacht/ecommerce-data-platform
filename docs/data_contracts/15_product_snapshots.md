# Product Snapshots and Dimension

## Scope and Grain

`olist.products` → full Bronze → Silver Parquet → `raw_staging.products` →
`analytics.stg_products` → `analytics.dim_product`. Một dòng hiện tại trên
`product_id`; dimension Type 1 dùng `MD5(product_id)` làm stable surrogate key.
Snapshot v1 độc lập với bốn bảng core: chưa có task/cursor products trong
`make dataset`, incremental, backfill hay Airflow. Product/seller membership
của order items vẫn chưa được kiểm tra.

## Extraction and Replay

Bronze luôn quét full source trong một REPEATABLE READ transaction, kể cả các
record cũ hơn 48 giờ. Migration `013_product_snapshots.sql` tạo delivery state
riêng. Pages được lưu bền vững trước publication; retry cùng run ID dùng nguyên
bytes, không đọc lại source đã thay đổi. Manifest và `_SUCCESS` được xác minh
trước Silver/staging. Dùng ID mới để đọc một snapshot mới; không sửa manifest.

## Silver Rules

Giữ tên source `product_name_lenght`, `product_description_lenght` trong Bronze,
Silver và staging; dbt đổi alias thành `product_name_length` và
`product_description_length`.

- Trim ID/category/batch; category lowercase. ID trống → `missing_product_id`.
- Sequence phải dương; operation thuộc INSERT/UPDATE, batch không trống,
  metadata timestamps parse được và có timezone offset. Sai → REJECT.
- Bảy numeric attributes phải là integer trong [0, 2147483647] hoặc NULL.
  Số âm/overflow → `invalid_<column>` và REJECT, không tự sửa giá trị.
- Category hoặc numeric attribute NULL → `missing_<column>` và WARNING.
  Weight/length/height/width bằng 0 → `zero_<column>` và WARNING; giữ nguyên 0.
- Chọn `source_sequence` lớn nhất theo normalized product ID. Các phiên bản cũ
  vào duplicates; cùng key/version khác raw payload hoặc lặp physical UID làm
  cả batch fail. Raw payload được giữ để đối chiếu.

Chưa join category translation, chưa sinh nhãn English/unknown member hoặc
áp dụng ngưỡng đo bất thường suy đoán. `relationship_checks=NOT_APPLICABLE`
chỉ thể hiện slice này không có required parent snapshot.

## Warehouse and Analytics

Dùng merge/hash `business-v1` hiện có: stale versions không ghi đè, cùng version
khác business hash làm rollback, replay không tăng số dòng. Nullable attributes
được giữ NULL. Full/empty snapshot không xóa record từng có trong staging;
source deletes chưa được hỗ trợ.

`created_at` là lần đầu warehouse quan sát; `updated_at` là lần quan sát được
chấp nhận gần nhất, gồm cả unchanged replay. Hash gồm toàn bộ business attributes,
không gồm metadata/lineage. Type 1 không lưu lịch sử category/measurements.

Full dbt build kiểm tra cả products, dù core chỉ có receipts/cursors của bốn bảng.
Final gate giữ SHARE locks trên cả sáu staging sources (gồm sellers) và kiểm tra artifact source
set. Gate không chứng nhận freshness hay extraction receipt của products. BI role
vẫn chỉ đọc ba business models và status view; không tự cấp quyền dimension.
Load products độc lập khiến BI publication gate unavailable như mọi audited load
mới; chạy thành công core pipeline với fresh dbt gate để công bố lại.

## Commands and Validation

Chạy từ repository root với PostgreSQL/MinIO đã seed, Java 17 và credentials môi
trường như các slice hiện có; Compose `.env` không tự export cho Python CLI.

```sh
make migrate
make ingest-products BRONZE_RUN_ID=products_v1
# Lấy prefix chính xác từ output manifest, không suy đoán ingestion_date.
make process-products BRONZE_PREFIX=<published-prefix> SILVER_RUN_ID=products_v1
make load-warehouse SOURCE_TABLE=products \
  SILVER_PREFIX=silver/products/run_id=products_v1 WAREHOUSE_RUN_ID=products_v1
make dbt-build
```

`make test-processing` kiểm tra từng rule, normalization, dedupe và conflict.
`make test-product-integration` dùng disposable PostgreSQL/MinIO và Spark: frozen
retry, replay, routing/count reconciliation, stale/conflict rollback, corruption,
empty snapshots và cursor isolation. `make test-dbt-integration` kiểm tra Type 1,
NULLs, stable keys/hash/timestamps và core metrics. Không cần raw CSV cho các suite.

## Local Verification

Smoke test ngày 2026-10-22 trên seeded local source: 32,951 input = 32,951 valid,
0 reject/deferred/duplicate; 615 valid records có warning. Trong đó 610 records
thiếu category; mỗi cột weight/length/height/width có 2 NULL; 4 records có weight 0.
Các nhóm có thể chồng lấp. Load đầu insert 32,951 rows, replay cùng request trả NOOP và mọi pipeline
cursor giữ nguyên trong standalone slice. Chi tiết runtime nằm trong ignored
`build/reports/products-smoke.json`; đây không phải benchmark.
