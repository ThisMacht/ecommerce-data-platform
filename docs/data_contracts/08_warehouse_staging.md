# Warehouse Staging

## Scope

Status: **Active — transactional Silver valid loading for four core datasets**.
Migration `006` thêm `raw_staging.customers`, `orders`, `order_items`,
`order_payments` và `audit.warehouse_load_runs`. Source schema `olist` không đổi.
Loader version là `warehouse-v1`; business hash version là `business-v1`.

Đây là current-state staging, chưa phải dimensional warehouse hoặc dbt marts.
Rejects/deferred/duplicates vẫn giữ trong Silver, không được load vào staging.
Full snapshot không có nghĩa xóa target keys vắng trong snapshot; source policy
vẫn chỉ có INSERT/UPDATE. Chưa có delete propagation hoặc cross-table transaction.

## Input boundary

Loader nhận Silver prefix, table và warehouse run ID. Trước khi load, verify
`_SUCCESS`, exact manifest SHA-256, tất cả object checksums, counters và schema
fingerprint bằng Silver verifier. Source contract hash và processor version phải
khớp dataset. Chỉ valid Parquet được đọc; physical schema phải khớp manifest.
Valid rows/warning rows đọc thực tế phải khớp counters với tolerance bằng zero.

Spark chạy local, UTC và Java 17; timestamp được chuyển thành UTC text có offset
trước khi đưa sang Python/Psycopg, tránh phụ thuộc timezone máy chạy. Numeric money
giữ Decimal → PostgreSQL `numeric(18,2)`; không qua float. Giữ leading zeros của ZIP.
Customer location có warning được phép NULL, như Silver contract.

COPY vào temporary table có business-key uniqueness và physical UID uniqueness;
duplicate candidates làm fail batch thay vì âm thầm drop. Loader yêu cầu selected
valid records đã được Silver dedupe. Giữ business columns, source metadata, toàn bộ
lineage và warning flags; orders giữ thêm `order_purchase_date` theo business timezone.

## Canonical business hash

`record_hash = SHA256(encode(["business-v1", table, ordered_business_values]))`.
`encode` là canonical JSON helper hiện có: UTF-8, compact separators, không escape
Unicode và một newline cuối. Thứ tự values theo `columns` của source table trong
`config/source_contract.json`; source technical metadata, lineage, warnings và
derived purchase date không tham gia hash.

- NULL dùng JSON `null`, khác string `"null"`; strings giữ nguyên normalized value.
- Integer dùng JSON number; money dùng string fixed-scale hai chữ số, zero là `"0.00"`.
- Timestamp dùng string UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- Hash schema version được lưu trên từng target row; đổi semantics cần migration/version mới.

## Version-aware merge

Version authoritative là **`source_sequence`**, không phải timestamp. Correction
có timestamp lùi nhưng sequence mới vẫn được update.

| Incoming record | Outcome |
| --- | --- |
| Business key chưa có | `inserted` |
| Sequence lớn hơn, business hash khác | `updated` |
| Sequence lớn hơn, hash giống | `unchanged`; advance source metadata |
| Sequence bằng, hash giống | `unchanged` |
| Sequence nhỏ hơn | `stale_ignored`; không sửa target |
| Sequence bằng, hash khác | Fail toàn batch |

Transaction dùng `READ COMMITTED`, lock riêng target table ở
`SHARE ROW EXCLUSIVE` để classification không race với writers khác. Temporary
COPY, classification, upsert và audit insert cùng transaction. Bất kỳ lỗi nào đều
rollback cả data và audit; retry cùng request an toàn. Lock timeout 15 giây,
statement timeout 120 giây; writer bận lâu có thể khiến caller phải retry.

`first_seen_run_id`/`first_seen_at` giữ lần insert đầu; `last_seen_run_id`/
`last_seen_at` cùng physical lineage cập nhật khi accepted occurrence được xử lý.
Stale occurrences không thay các fields này. Newer unchanged records vẫn advance
source metadata. Mỗi accepted row được upsert một lần; không đếm theo số SQL updates.

## Audit and replay

Audit key là `(source_table, run_id)`. Request pin loader/hash versions, source
contract hash, Silver prefix và exact manifest hash. Retry cùng request trả `NOOP`
cùng original counters; không cộng thêm audit row hoặc đổi target metadata. Cùng ID
với input khác bị reject. Run khác đọc lại business versions cũ có thể được đếm
`unchanged` hoặc `stale_ignored`; đó là load occurrences, không phải source growth.

```text
load_candidate_rows = silver_counts.valid_rows
load_candidate_rows = warehouse_inserted_rows + warehouse_updated_rows
                    + warehouse_unchanged_rows + warehouse_stale_ignored_rows
```

Audit lưu Silver routing counters và committed merge counters. Chưa thay thế
planned DAG-level audit, failure-attempt lifecycle hoặc deferred resolution state.
Không promote end-to-end watermark: dbt/reconciliation gates toàn pipeline chưa có.
Staging không thêm FK tới current target tables: dependency membership vẫn được
đánh giá theo pinned Silver snapshots, có thể khác current staging state.
Product/seller membership vẫn chưa được kiểm tra.

## Commands and validation

```bash
make migrate
make sync-processing
# Export WAREHOUSE_DSN hoặc standard PG*; AWS/MinIO environment như Bronze.
make load-warehouse SOURCE_TABLE=order_payments \
  SILVER_PREFIX='silver/order_payments/run_id=<silver_id>' WAREHOUSE_RUN_ID=load_001
make test-warehouse-merge-integration
make test-warehouse-integration
```

`make dataset` tự load từng bảng sau Silver theo customers → orders → items →
payments và ghi thêm `*_warehouse.json` trong `build/reports/dataset/`. Dùng cùng
local database/loader role; DSN từ shell không được dùng để redirect local runner.
Thiếu Java thì Silver/staging đều skip. Loader CLI riêng dùng `WAREHOUSE_DSN` hoặc
`PG*`, không tự export Compose `.env`. Loader role được SELECT/INSERT/UPDATE staging
và SELECT/INSERT audit; reader chỉ SELECT, không cấp DELETE.

Tests gồm independent canonical hash expectations; all four version outcomes;
same-version conflicts; replay/concurrency; duplicate candidates; zero-row batches;
rollback khi audit lỗi; corrupt Silver/schema/counts; decimal/timezone preservation;
CLI ngoài checkout và runner retry. Integration services đều disposable.
