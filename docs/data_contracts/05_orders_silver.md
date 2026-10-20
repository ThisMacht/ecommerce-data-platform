# Orders Silver Contract

## Scope and runtime

Status: **Active — orders-only local PySpark slice**, processor/schema `orders-silver-v1`. Input là một [orders Bronze batch](04_orders_bronze.md) có `_SUCCESS`, đúng source contract `1.0.0`, schema, lineage và checksums. Output là immutable Parquet trên cùng MinIO bucket, chia thành valid, rejected và duplicates.

Runtime: Python 3.10+, Java 17 và PySpark `4.0.2` trong optional extra `processing`. `make test` không cần Spark/Java. Spark chạy local với temporary disk staging; chưa hỗ trợ distributed workers vì input/output temporary paths thuộc host. Cần đủ local disk cho cả input JSONL và output Parquet.

Đây là validation trong một orders batch. **Chưa kiểm tra customer foreign keys, chưa có deferred lifecycle, quarantine database, warehouse merge hoặc end-to-end watermark.** Manifest ghi `relationship_checks=NOT_IMPLEMENTED`, `deferred_rows=0`. Silver valid chỉ có nghĩa pass các rules dưới đây; chưa bảo đảm referential integrity.

## Input validation and lineage

Job xác minh Bronze marker, schema fingerprint, source contract hash và mọi object SHA-256 trước khi Spark đọc. Missing/extra columns, incompatible physical JSON types, unknown schema versions hoặc sai lineage làm fail batch. Null/malformed business values giữ nguyên trong `_raw_payload` để Spark phân loại từng row. Download kiểm tra lại SHA-256, tránh dùng bytes khác với input đã xác minh.

Mỗi physical `_record_uid` phải duy nhất và đúng source run/object/one-based row ordinal. `_logical_record_uid` phải khớp raw order ID/source sequence, hoặc null khi key/version bị thiếu. Blank keys/nonpositive versions vẫn giữ Bronze hash rồi bị row-level reject. Original lineage được giữ nguyên qua mọi routes; Silver run ID riêng nằm trong manifest. `_raw_payload` chứa nguyên business/source metadata dưới dạng JSON string để replay/điều tra.

## Normalization and rules

Spark dùng explicit schema, built-in SQL expressions và session timezone UTC; không dùng Python UDF hoặc schema inference. Trim spaces của order/customer IDs, batch ID; trim/lowercase status. Business timestamps giữ nguyên tên cột nhưng được chuyển từ naive `America/Sao_Paulo` sang UTC, ghi Parquet `TIMESTAMP_MICROS`. `order_purchase_date` giữ ngày local trước conversion. Source technical timestamps parse theo offset gốc.

| Severity | Rules |
| --- | --- |
| Reject | Missing/blank order or customer ID; unknown status; null/nonpositive source sequence; missing batch ID; operation khác INSERT/UPDATE |
| Reject | Required purchase/estimated timestamp null; non-null business timestamp sai format hoặc chứa offset; Sao Paulo DST gap/ambiguous wall time |
| Reject | Source updated/loaded timestamp null, malformed hoặc thiếu offset |
| Reject | Purchase sau Bronze ingestion timestamp; estimated delivery trước purchase |
| Warning | Delivered status thiếu actual delivery timestamp; delivery trễ hơn estimate trên 30 ngày; approval trước purchase |

Statuses được chấp nhận: created, approved, invoiced, processing, shipped, delivered, canceled, unavailable. Canceled/unavailable vẫn là source records hợp lệ; metric eligibility thuộc warehouse contract sau này. Future-date rule dùng immutable Bronze ingestion timestamp, không dùng wall clock của retry. Nullable timestamp null không tự tạo lỗi.

DST checks áp dụng transitions một giờ của São Paulo trong phạm vi Olist: reject cả nonexistent và repeated local times, thay vì ngầm chọn một offset. Fixtures kiểm tra summer UTC−02, winter UTC−03, gap và fold năm 2018.

## Deduplication and reconciliation

Trong mỗi normalized order ID có key/version hợp lệ, giữ source sequence lớn nhất; cùng version/payload dùng `_record_uid` lớn nhất làm deterministic tie-breaker. Cùng key/version nhưng raw payload khác nhau làm fail batch. Key/version không hợp lệ được reject độc lập, không gom tất cả null keys thành một record.

Dedupe diễn ra trước routing: version mới nhất không hợp lệ vẫn là reject, không lấy lại version cũ. Những physical rows bị thay thế đi vào duplicates, giữ evidence/UID. Các record còn lại đi vào rejected nếu có error rules, hoặc valid với warning flags. `rule_violations` đếm từng rule trên selected records; `warning_rows` chỉ đếm valid records có warning.

```text
bronze_rows_read = input_rows
input_rows = valid_rows + rejected_rows + duplicates_rows
0 <= warning_rows <= valid_rows
deferred_rows = 0
```

Writer đọc lại Parquet materialization và đối chiếu exact row counts cho từng route trước upload. Manifest ghi counters, rule hits, input manifest hash, processor/Spark versions, output schema/fingerprint và object hashes/sizes.

## Publication and retry

```text
silver/orders/run_id=<silver_run_id>/
  request.json
  attempt=<uuid>/valid/part-*.parquet
  attempt=<uuid>/rejected/part-*.parquet
  attempt=<uuid>/duplicates/part-*.parquet
  manifest.json
  _SUCCESS
```

`request.json` được tạo bằng conditional write, khóa Bronze prefix/hash, source contract hash và processor/Spark versions cho run ID. Spark tạo physical file names khác nhau giữa attempts; mọi attempt có namespace riêng và không overwrite objects.

Sau khi upload đủ Parquet và verify bytes/counts, conditional write seal `manifest.json`; `_SUCCESS` chứa hash của exact manifest bytes. Concurrent identical requests dùng manifest hoàn chỉnh được seal đầu tiên. Marker là publication boundary cho tất cả routes, không có per-route success giả.

Lỗi trước manifest: retry có thể compute lại cùng immutable Bronze, tạo attempt mới; partial/orphan objects không được đọc. Lỗi sau manifest: resume dùng lại sealed files, không compute/rewrite. Replay đã publish trả `NOOP` sau verification. Run ID với input/config khác bị reject. Chưa có orphan retention/garbage collection.

CLI `verify` kiểm tra marker, manifest/request/schema hashes, object checksums/sizes, route presence và declared reconciliation mà không cần Spark. Physical Parquet row counts được kiểm tra tại materialization; consumer đọc schema từ manifest, đặc biệt với batch rỗng không có data objects. Silver không thay đổi Bronze checkpoint hoặc bất kỳ source data nào.

## Commands and tests

```bash
export JAVA_HOME=/path/to/jdk17
make sync-processing
# Reuse the S3 environment from Bronze setup; no PostgreSQL DSN is required.
make process-orders BRONZE_PREFIX='<bronze manifest.prefix>' SILVER_RUN_ID=orders_silver_001
uv run --locked olist-silver-orders verify --prefix 'silver/orders/run_id=orders_silver_001'
make test-processing
make test-silver-integration
```

`tests/fixtures/silver/orders_dirty.json` và independent `expected.json` khóa 13 inputs → 4 valid, 7 rejected, 2 duplicates, 1 valid warning. Processing tests kiểm tra rules, UTC/local date semantics, latest-invalid version và conflict detection. Disposable PostgreSQL/MinIO tests cover source → Bronze → Spark → Silver, exact replay, partial upload, sealed recovery, concurrent publication, corruption và empty batches. Không cần raw CSV, `.env` hoặc secrets; GitHub Actions có Spark job riêng.
