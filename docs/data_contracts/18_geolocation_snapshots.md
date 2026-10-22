# Geolocation Snapshots and Location Dimension

## Scope and Grain

`olist.geolocation` → full Bronze → Silver → `raw_staging.geolocation` →
`stg_geolocation` → `int_geolocation_by_zip` → `dim_location`.
Source grain là một CSV observation; `geolocation_row_id` là vị trí data row
one-based của immutable snapshot, không phải ZIP hoặc global address ID.
Không đổi thứ tự/re-index CSV rồi coi đó là updates của cùng source snapshot.

Geolocation chạy standalone hoặc full snapshot trong core full/incremental/backfill.
Migration `019` bổ sung owned full/parent streams. Full runner freeze bảy streams,
29 tasks; bounded modes freeze bốn deltas cùng năm full dimension/FK snapshots,
33 tasks. Final gate bắt buộc bảy dataset load receipts từ cùng source snapshot,
pin manifest hashes, exact counters và khóa cả bảy staging tables qua fresh dbt/CAS.
Chỉ bốn core tables có completion cursors; geolocation dùng dimension_snapshot_rows.
Airflow wrappers dùng cùng runner và giữ original DAG/run identity khi retry.

`dim_customer` và `dim_seller` thêm nullable `location_key` bằng LEFT JOIN theo
source ZIP vào một-row/ZIP dimension. NULL/invalid/unmatched ZIP giữ key NULL;
không mất customer/seller, không tạo unknown member hoặc ghi đè source city/state.
Customer chọn current representative theo source_sequence trước location lookup.
Hai dimensions giữ hash contract cũ; optional location key được reconcile riêng.
Không đưa location keys trực tiếp vào facts hay mở rộng BI grants.

## Extraction and Replay

Migration `018_geolocation_snapshots.sql` tạo delivery state riêng và staging.
Bronze luôn full scan REPEATABLE READ, kể cả rows cũ hơn lookback. Prepared pages
lưu bền trước MinIO publication; resume cùng run ID/config giữ nguyên bytes.
Silver xác minh source technical key, schema, contract/manifest hashes và lineage.
Owned geolocation streams chỉ được FULL: full snapshot hoặc bounded parent role;
không chấp nhận geolocation delta. Pending run cần resume nguyên mode/code/config.

## Silver and Warehouse Rules

- Trim ZIP/city/state/coordinates/batch; chuỗi trống thành NULL. City lowercase,
  state uppercase; giữ accents và ZIP leading zeros, không tự pad ZIP.
- Row ID và source sequence phải dương; batch/operation/offset timestamps phải
  hợp lệ. ZIP phải đúng 5 ASCII digits. Sai → REJECT.
- Latitude trong [-90,90], longitude trong [-180,180], numeric finite. Dùng Decimal
  để validate và giữ nguyên numeric text trong Parquet; không round qua float
  hoặc Spark fixed-scale decimal. Không clip theo Brazil hoặc suy đoán địa chỉ.
- Missing city hoặc missing/invalid state chỉ WARN; valid coordinates vẫn được
  giữ. Blank labels thành NULL; invalid nonblank state được giữ kèm flag.
- Dedupe theo row ID/source version; observations khác row ID vẫn giữ dù tọa độ
  giống hệt. Conflicting payload/version hoặc physical UID trùng làm batch fail.

Warehouse kiểm tra lại key/ZIP/coordinate bounds và canonicalize numeric text
bằng exact Decimal, không dùng context rounding. Business-v1 hash gồm business
columns theo source contract; technical row ID là merge key, không phải business
attribute trong hash. Source versions/lineage không đổi business hash.
Large route reads repartition toward roughly 10,000 rows before `toLocalIterator`
để tránh materialize cả triệu rows trong một driver heap; đây không phải byte
limit và vẫn cần đủ memory cho một row/Parquet read task.
Version-aware merge và audit commit atomically; stale không overwrite newer,
replay NOOP. Full/empty snapshots không suy ra deletes, nên staging/dimension là
current accumulated observations, không phải exact replacement of latest file.

## ZIP Aggregation and Type 1 Policy

Một dòng mỗi ZIP, key `MD5(zip_code_prefix)` ổn định. Latitude/longitude là
independent exact NUMERIC medians: odd lấy giữa, even trung bình hai giá trị giữa.
Median tính bằng `(low + high) * 0.5`, rồi `TRIM_SCALE` để giữ đủ precision và
hash ổn định khi source chỉ đổi trailing zeros. `AVG` hoặc numeric `/ 2` có thể
round chữ số cuối với input high-scale. Coordinate pair đại diện có thể chưa từng xuất hiện
trong source và không phải vị trí thực của customer/seller.

City/state chọn **joint observed mode** trên labels có city và valid Brazilian
state; tie-break state rồi city bằng C collation. Không chọn hai modes độc lập để
tránh ghép city/state chưa từng tồn tại. Không có valid labels → cả hai NULL.
`sample_count` đếm mọi observation; `modal_sample_count` là tần suất cặp thắng
(zero nếu không có labels), không phải số người hoặc unique locations.

Type 1: attributes/counts thay đổi theo current staging; không dựng lịch sử.
`created_at`/`updated_at` lấy min first-seen/max last-seen. `record_hash` loại
metadata timestamps. Source-based reconciliation dùng ordered numeric arrays để
kiểm tra median, labels, counts, hash và exact ZIP coverage độc lập với model ranks.

## Current Staging and Backfill Limits

Full snapshots không xóa observations đã load; version-aware staging có thể giữ
prior rows hoặc newer standalone versions. Receipts chứng minh input frozen và
load outcomes, không chứng nhận exact replacement của current staging. Dimension
và customer/seller keys là current Type 1 lookup. Backfill vẫn dùng full current
geography, không khôi phục geography tại window end. Source ZIP vắng geolocation
là nullable enrichment, không phải Silver rejection hoặc deferred dependency.
`location_key_reconciliation` đối chiếu key với current source staging; relationship
checks không đủ phát hiện valid key nhưng trỏ sai ZIP. Không bypass old receipts.

## Operations and Validation

```bash
make migrate
# Export BRONZE_SOURCE_DSN/PG*, AWS credentials and dbt reader/password as usual.
make ingest-geolocation BRONZE_RUN_ID=geo_001 BRONZE_PAGE_SIZE=10000
make process-geolocation BRONZE_PREFIX='<Bronze prefix>' SILVER_RUN_ID=geo_001
make load-warehouse SOURCE_TABLE=geolocation SILVER_PREFIX='<Silver prefix>' WAREHOUSE_RUN_ID=geo_001
make dbt-build
make test-geolocation-integration
make test-dbt-integration
```

Dataset runner mặc định cấp Spark driver heap 2 GiB cho full geolocation, giữ
nguyên `PYSPARK_SUBMIT_ARGS` nếu caller đã đặt. Standalone CLI dùng Spark defaults;
khi gặp `Java heap space`, kiểm tra RAM rồi đặt biến sau trước khi chạy lại
đúng command với cùng IDs:

```bash
export PYSPARK_SUBMIT_ARGS='--driver-memory 2g pyspark-shell'
```

Giữ nguyên code/config/mode và frozen inputs; không tạo ID mới chỉ để retry OOM.
Heap này là runtime allocation, không phải giới hạn memory tổng của pipeline.
Warehouse statement timeout là 600 giây; vẫn giữ lock timeout 15 giây và atomic
rollback, kể cả accepted unchanged rows cần refresh physical lineage.

Standalone warehouse load mới làm BI unavailable theo existing publication gate.
Sau khi checks pass, chạy core pipeline thành công mới để mở lại BI; không bypass
status hoặc thay receipts. Resume cùng IDs nếu mất response sau commit.

Fixtures kiểm tra exact precision, out-of-range/NaN/malformed inputs, ZIP/metadata,
dirty labels, frozen retry, stale/conflicting versions, corruption, empty input,
odd/even medians, duplicate observations, deterministic mode ties và Type 1 keys.

## Previous Standalone Verification (2026-10-22)

Standalone `geolocation_f411e5cd6a0640378ee034074046d692` ngày 2026-10-22:
1,000,163 source/Bronze rows, 101 pages; Silver valid=1,000,163 và rejected,
warning, duplicate, deferred đều zero. Warehouse insert đúng 1,000,163 rows;
core controls không đổi. Lần load đầu bị Java heap OOM đã rollback; cùng Silver/
run ID load thành công sau khi chia nhỏ partition handoff, giữ default Spark
heap.

Locked geolocation preflight PASS=22: 19,015 dimension rows/unique ZIPs;
SUM(sample_count)=1,000,163, 17,972 ZIPs có nhiều observations, không thiếu labels.
Core run `dataset_ab25c7556bee4779933524caf04472b9` bị source reconciliation chặn
28 ZIPs khi median/hash còn làm tròn hoặc phụ thuộc numeric scale. Failed run
không promote cursors; regression test high-scale tái hiện được lỗi. Công thức
exact và scale normalization đã pass cả fixture mới lẫn toàn bộ geolocation data.

Evidence ignored: `build/reports/geolocation-local.json`,
`build/reports/geolocation_preflight_dbt/run_results.json`,
`build/reports/geolocation-preflight.json`, `build/reports/geolocation-superseded-run.json`.

Full core run `dataset_ab4312da46b646f8a75886df10a63c5d` ngày 2026-10-22 SUCCESS:
26 tasks, sáu audited core datasets, 189 dbt nodes (25 views, 164 data tests),
không error/skip. Current code hash khớp request; geolocation vẫn standalone,
không có seventh core receipt. Fact rows giữ nguyên: orders/delivery 99,424,
items 112,621, payments 103,869. Health HEALTHY, BI available; dedicated BI
reader bị từ chối cả ba geolocation models. Run lỗi cũ vẫn FAILED.

121 unit, 3 processing và 34 distinct integration cases đã pass: 16 dbt, 2
geolocation, 5 item dependencies, 1 full gate, 6 incremental, 2 backfill và 2
recovery. Sau sửa median precision/scale, rerun geolocation modeling case và
full gate đều pass, cùng preflight dữ liệu thật và fresh full core run. Ruff,
contract/config/shell checks và package build thành công. Chưa chạy CI trên GitHub.

Final evidence ignored: `build/reports/geolocation-final.json`,
`build/reports/geolocation-health.json`, run-specific pipeline/dbt reports trong
`build/reports/dataset/`. Snapshot HTML health tại `build/reports/pipeline_dashboard.html`;
luôn kiểm tra timestamp trước khi dùng.

## Core Integration Verification (2026-10-23)

Run `dataset_8af5a27253f34d51b47c62a239d0e7f7` attempt 4 SUCCESS: 29 tasks,
bảy reconciled dataset audits, cùng source snapshot với 1,451,627 rows. Code hash
khớp request; bốn completion cursors cùng advance đúng một version bởi run này,
không thêm geolocation cursor. dbt PASS=192: 25 views, 167 tests, zero error/skip.

Geolocation load có 1,000,163 unchanged rows, không insert/update/stale;
`dim_location` có 19,015 ZIPs, SUM(sample_count)=1,000,163. Customer dimension giữ
96,096 shoppers: 95,827 matched keys, 269 NULL; sellers giữ 3,095 rows: 3,088
matched, 7 NULL. Source-based location reconciliation pass. Fact counts không đổi:
orders/delivery 99,424, items 112,621, payments 103,869.

BI publication available; dedicated reader không được SELECT dim_location,
dim_customer hoặc dim_seller. Không thêm grants. Health UNHEALTHY chỉ vì
`pipeline.runtime_exceeded`: original run elapsed 3,174.68 giây gồm guard failure,
host restart, chủ động ngắt và chờ resume; threshold 1,800 giây giữ nguyên.
Attempt 4 audited tasks chạy 755.85 giây, dùng lại frozen inputs, không phải timing
của một fresh full extraction. Metabase service vẫn tắt để giữ tài nguyên.

122 unit tests, 46 distinct integration cases (17 dbt, 10 pipeline control,
1 full gate, 6 incremental, 2 backfill, 2 recovery, 8 warehouse merge), 3 Airflow
import/serialization checks, Ruff, contract/config/shell checks và build đã pass.
Fixtures kiểm tra atomic seven/nine-stream freeze, frozen retry sau source writes/
deletes, corrupt/missing receipts, staging locks, nullable/unmatched ZIPs, no fan-out
và phát hiện valid key nhưng sai ZIP. Chưa chạy GitHub CI.

Run cũ `dataset_f34b6e14a0da4785ad61aae5daafc51e` giữ FAILED sau default-heap OOM
và SQL timeout 120 giây; không promote cursors. Owned PREPARED streams được publish
từ original pages/config trước fresh freeze, không sửa requests hoặc bypass gates.
Final runner mặc định heap 2 GiB, giữ caller overrides; warehouse statement timeout
600 giây giữ atomic rollback/lineage semantics. Fresh attempt 2 bị host restart;
attempt 3 ngắt bằng SIGINT để nhường user task; attempt 4 resume cùng code/ID thành
công với affinity tối đa tám cores, nice=10, Spark vẫn local[2]. Năm resource samples
ở attempt 4 còn 8.20–9.73 GiB MemAvailable; swap tăng 1.03→1.95 GiB. Đây là samples,
không phải continuous peak measurement hoặc benchmark khi chạy task nặng đồng thời.

Evidence ignored: `build/reports/location-core-local.json`,
`build/reports/location-core-health.json`, `build/reports/location-core-resources.jsonl`,
`build/reports/location-core-superseded-run.json`, run-specific dbt/pipeline reports.
Health HTML: `build/reports/pipeline_dashboard.html`; luôn kiểm tra timestamp.
