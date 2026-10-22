# Seller Snapshots and Dimension

## Scope and Grain

`olist.sellers` → full Bronze → Silver Parquet → `raw_staging.sellers` →
`analytics.stg_sellers` → `analytics.dim_seller`. Một dòng hiện tại trên
`seller_id`; Type 1 dùng `MD5(seller_id)` làm stable surrogate key. Hai seller
cùng location vẫn là hai records. Không tạo lịch sử địa chỉ từ snapshot.

Sellers chạy standalone hoặc full refresh trong core full/incremental/backfill
và Airflow. [Item v2](17_item_dependency_snapshots.md) kiểm tra membership bằng
pinned full product/seller snapshots. Không có seller delta/completion cursor.

## Extraction and Replay

Migration `014_seller_snapshots.sql` tạo delivery state riêng và staging table.
Bronze luôn full scan trong một REPEATABLE READ transaction, không dùng lookback
để loại record cũ. Pages được lưu bền vững trước publication; retry cùng run ID
phải dùng nguyên bytes/config đã chuẩn bị. ID mới đọc source snapshot mới.
Silver/staging xác minh manifest, `_SUCCESS`, bytes/schema và reconciliation.

## Silver Rules

Policy location giống customers, không suy đoán tọa độ hoặc sửa mã ZIP/state:

- Trim seller ID, city, state, ZIP và batch; chuỗi trống thành NULL.
- City lowercase; state uppercase; ZIP giữ dạng text và leading zeros.
- ID thiếu → `missing_seller_id` và REJECT.
- Sequence phải dương, operation thuộc INSERT/UPDATE, batch không trống;
  metadata timestamps phải parse được và có timezone offset. Sai → REJECT.
- City NULL → `missing_seller_city`; ZIP NULL/không đủ 5 chữ số →
  `invalid_seller_zip_code_prefix`; state NULL/ngoài Brazil state codes →
  `invalid_seller_state`. Đây là WARNING; NULL hoặc giá trị sai được giữ nguyên.
- Chọn sequence lớn nhất theo normalized seller ID. Phiên bản cũ vào duplicates;
  cùng key/version khác raw payload hoặc lặp physical UID làm cả batch fail.

Source contract giữ các location columns NOT NULL theo PostgreSQL schema thực tế.
Silver/staging cho phép NULL sau normalization; đây là chuyển đổi có warning,
không phải thay đổi source contract. Raw payload vẫn được giữ để đối chiếu.
`relationship_checks=NOT_APPLICABLE` vì slice này không có required parent.

## Warehouse and Analytics

Dùng `business-v1` hash và version-aware merge hiện có: replay không nhân bản,
stale versions không ghi đè, cùng version khác business hash làm rollback.
Full/empty snapshots không xóa các record đã có; source DELETE chưa hỗ trợ.

Dimension giữ `created_at` là lần đầu warehouse quan sát và `updated_at` là lần
quan sát được chấp nhận gần nhất, kể cả unchanged replay. Hash gồm bốn business
columns, không gồm metadata hoặc lineage. Địa chỉ hiện tại không được coi là địa
chỉ tại thời điểm giao dịch trong quá khứ.

Full dbt build hiện kiểm tra sáu staging sources; final core gate giữ SHARE locks
trên cả sáu đến khi fresh dbt và cursor CAS commit. Core audits sáu datasets, promote chỉ bốn core cursors và xác minh frozen seller
extraction/Silver/load receipts cùng item pins.
Standalone seller load làm BI publication unavailable cho đến core run thành công.
BI role vẫn chỉ đọc ba business models và status view, không được thêm dimension.

## Commands and Validation

Cần PostgreSQL/MinIO đã seed, Java 17, `PG*`/`WAREHOUSE_DSN`, S3 credentials và dbt
environment theo các slice hiện có. Compose `.env` không tự export cho Python.

```sh
make migrate
make ingest-sellers BRONZE_RUN_ID=sellers_v1
# Dùng prefix thực tế từ manifest output của lệnh ingest.
make process-sellers BRONZE_PREFIX=<published-prefix> SILVER_RUN_ID=sellers_v1
make load-warehouse SOURCE_TABLE=sellers \
  SILVER_PREFIX=silver/sellers/run_id=sellers_v1 WAREHOUSE_RUN_ID=sellers_v1
make dbt-build
```

`make test-processing`: routes/rules, ZIP leading zeros, NULLs, shared locations,
source versions/conflicts và UTC. `make test-seller-integration`: disposable
PostgreSQL/MinIO/Spark, frozen retry, unchanged/stale/conflict merges, corruption,
response loss, empty snapshots và unchanged core cursors. `make test-dbt-integration`:
stable keys, Type 1, hashes/timestamps, NULLs và core metric regression. Không cần
raw CSV cho các suite này.

## Local Verification

Smoke test ngày 2026-10-22 trên seeded local source: 3,095 input = 3,095 valid =
3,095 inserted; 0 reject/deferred/duplicate/warning theo rules hiện tại. Replay
cùng request trả NOOP và mọi pipeline cursor giữ nguyên trong standalone load.
Chi tiết runtime: ignored `build/reports/sellers-smoke.json`; không phải benchmark
hoặc xác minh tính đúng đắn địa lý của từng địa chỉ.

Full core run trước tích hợp orchestration item v2 đã SUCCESS: 19 tasks, 61 dbt model/test nodes;
`dim_seller` có 3,095 rows và BI publication available. Core gate vẫn không thêm
sellers vào extraction receipts hoặc scheduled cursor set.
