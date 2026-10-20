# Orders Bronze Contract

## Scope and prerequisites

Status: **Active — orders-only extraction slice, format version 1**. Python CLI đọc PostgreSQL và publish JSON Lines vào MinIO. Source contract `1.0.0`, migrations qua `003` và successful seed của đúng `dataset_snapshot_id` là bắt buộc. Schema `orders` phải khớp chính xác tên, types và nullability; additive columns cũng cần cập nhật contract trước khi extract ở slice này.

Đã có [orders PySpark/Silver slice](05_orders_silver.md); Airflow, warehouse và end-to-end watermark còn planned. `audit.bronze_orders_control` chỉ xác nhận delivery vào Bronze; không thay thế `audit.pipeline_control` dự kiến commit sau downstream reconciliation. Bronze thành công chưa có nghĩa toàn pipeline thành công.

## Extraction and cursor

Cursor có thứ tự `(source_updated_at, source_sequence, order_id)`. Trong một PostgreSQL `REPEATABLE READ` transaction, extractor đọc checkpoint `W`, chụp maximum cursor `U`, tính lower bound `W.updated_at - 48 hours` và keyset paginate mọi row từ lower bound tới `U`. Initial load không có lower bound; page size từ 1–10,000, mặc định 1,000. Count query độc lập trong cùng snapshot phải bằng tổng extracted rows. Candidate là `max(W, U)`; table rỗng giữ nguyên `W`.

`source_sequence` là authoritative source version. Backdated corrections trong lookback vẫn được extract; corrections ngoài SLA 48 giờ không được hứa tự động phát hiện. Lookback cố ý đọc lại một số logical versions giữa các runs. Mỗi batch có tối đa một current row cho mỗi `order_id`; downstream sẽ deduplicate theo key/version. Đây là polling current state, không phải CDC và không giữ mọi intermediate update.

## Durable preparation and retry

Source rows, bounds, serialized page bytes và manifest được seal atomically trong `audit.bronze_orders_runs`/`audit.bronze_orders_parts` trước bất kỳ S3 write nào. Chuẩn bị thất bại thì transaction rollback, không có remote objects; lần thử sau được chụp snapshot mới. Sau khi `PREPARED` commit, retry cùng run ID dùng lại chính bytes/bounds đó, kể cả khi source đổi hoặc process restart.

Session advisory lock chặn overlapping orders ingestion. Chỉ một `PREPARED` run được tồn tại; phải resume run đó trước khi tạo run mới. Cùng run ID với contract, page size, bucket hoặc endpoint khác bị reject. Nâng `extractor_version` khi thay đổi serialization/publication semantics. Publication retries giữ nguyên record IDs, ingestion timestamps và attempt path.

Spool giữ bounded client memory nhưng tăng PostgreSQL storage. Hiện giữ lại pages để retry/audit, chưa có retention job. Không sửa/xóa state của unfinished run. Thay spool bằng durable extraction service là hướng mở rộng khi tăng quy mô.

## Objects and lineage

```text
bronze/orders/ingestion_date=YYYY-MM-DD/run_id=<run_id>/
  attempt=0001/part-000001.jsonl
  attempt=0001/part-000002.jsonl
  manifest.json
  _SUCCESS
```

JSONL UTF-8 giữ nguyên column names, strings và nulls. Business timestamps serialize ISO không offset, giữ semantics `America/Sao_Paulo` của source; technical timestamps dùng UTC có offset. Chưa chuyển business timestamps sang UTC hoặc cast business values. Parquet dành cho processing slice sau.

Mỗi row thêm `_record_uid` (SHA-256 của run/object/row ordinal), `_logical_record_uid` (source/order ID/source sequence), `_ingested_at`, `_run_id`, `_run_attempt`, `_source_name`, `_source_file`, `_source_row_number` và `_schema_version`. Row ordinal bắt đầu từ 1 trong từng object.

Manifest chứa source/schema versions, schema specification/fingerprint, config/hash, bounds/checkpoint version, source/extracted row counts, page count và object keys/sizes/SHA-256/counts. Credentials không nằm trong manifest. Run ID gồm letters, digits, `_`, `-`, dài 1–96 ký tự, bắt đầu bằng letter/digit.

## Publication and checkpoint

Uploader dùng conditional `PutObject` với `If-None-Match: *`: existing identical bytes được chấp nhận, khác bytes làm run fail. Không dùng ETag thay SHA-256. Upload đủ pages/manifest và xác minh remote page hashes trước khi tạo `_SUCCESS` chứa SHA-256 của exact manifest bytes. Marker là publication boundary; consumers phải gọi `verify_published` hoặc CLI `verify`, không đọc object listing trực tiếp.

Verification kiểm tra marker, manifest checksum/version/location, schema fingerprint, object paths, sizes, checksums, JSON parsing và source/page/batch counts. Sau verification, một PostgreSQL transaction compare-and-swap checkpoint version và chuyển run sang `COMMITTED`. Replay đã commit là `NOOP` sau khi xác minh lại remote batch.

S3 và PostgreSQL không có distributed transaction: crash sau marker nhưng trước checkpoint có thể để published batch cùng `PREPARED` state. Resume cùng run hoàn tất checkpoint mà không rewrite objects. Checkpoint conflict phải fail để điều tra. Immutability được enforce bằng application conditional writes; local root credentials vẫn có thể overwrite ngoài CLI, và verification sẽ phát hiện corruption.

## Local execution

```bash
make seed-source                  # Requires canonical local CSVs
make bronze-up                    # Build pinned, checksummed upstream MinIO source
export BRONZE_SOURCE_DSN='postgresql://olist_loader:local_loader_change_me@127.0.0.1:5432/olist_source'
export AWS_ACCESS_KEY_ID='olist_minio_admin'
export AWS_SECRET_ACCESS_KEY='local_minio_change_me'
export BRONZE_S3_ENDPOINT='http://127.0.0.1:9000'
export BRONZE_S3_BUCKET='olist-bronze'
make init-bronze
make ingest-orders BRONZE_RUN_ID=orders_demo_001
make ingest-orders BRONZE_RUN_ID=orders_demo_001  # NOOP or resume
```

Credentials minh họa defaults local; điều chỉnh nếu `.env` khác. CLI đọc process environment, không tự load `.env`; database cũng hỗ trợ standard `PG*` variables. Bootstrap dùng loader role để đọc source và ghi Bronze state; reader không được mutate checkpoint/spool. MinIO root account chỉ dùng local bootstrap; scoped ingestion roles là phần hardening tiếp theo.

Lấy `manifest.prefix` từ JSON output để verify:

```bash
uv run --locked olist-bronze-orders verify --prefix '<manifest.prefix>'
make test-bronze-integration
```

Integration suite dùng disposable PostgreSQL/MinIO, ephemeral localhost ports, database/bucket riêng và SQL fixture hai orders. Không cần CSV, `.env`, secrets hoặc local persistent volumes. Tests cover same-timestamp pagination, inserts/updates, lookback trong/ngoài SLA, concurrent source changes, partial upload, retry, rollback, checkpoint failure/conflict, immutability, corruption, empty table và CLI.
