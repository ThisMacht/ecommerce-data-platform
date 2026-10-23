# Incremental Core Pipeline

## Scope and bootstrap

Migration `010` thêm namespace `incremental_core_v1`, `run_type=incremental` và
scoped Bronze delivery streams. `local_core_v1` giữ full-run controls riêng.
Không copy watermark full-run/standalone Bronze sang namespace mới: lần đầu
incremental bootstrap toàn source hiện có, sau đó dùng committed pipeline cursors.
Hai modes cùng ghi version-aware staging; chỉ một pipeline run được RUNNING trên
cả hai namespaces. Runner vẫn giữ cùng filesystem lock/pending file.

```bash
make dataset-incremental
make pipeline-health PIPELINE_NAMESPACE=incremental_core_v1
make pipeline-dashboard PIPELINE_NAMESPACE=incremental_core_v1
```

Raw CSV/seed preflight vẫn như full runner; thiếu CSV hoặc Java không được skip
trong incremental mode. Dùng Java 17. Có thể gọi trực tiếp
`DATASET_MODE=incremental make dataset`; không đổi mode khi resume pending run.

## Frozen source snapshot

Sau audit begin, `source.freeze` chốt **mười** streams trong cùng PostgreSQL
`REPEATABLE READ` transaction: bốn deltas và full customers/orders/products/sellers/geolocation/order_reviews để kiểm tra FK, cập nhật dimensions và reviews.
Mỗi stream dùng schema validation và durable pages của Bronze hiện có; transaction
commit tất cả manifests/pages hoặc rollback tất cả. Không giữ source snapshot mở
trong lúc upload/Spark/dbt. Manifest lưu `source_snapshot` để gate đối chiếu.
PostgreSQL semantics tham khảo [transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html#XACT-REPEATABLE-READ).

Một record source commit giữa các lần đọc bảng hoặc sau freeze không thay đổi
batch đã seal. Retry không truy vấn lại live source để tạo upper mới. Nếu crash
trước transaction freeze commit, chưa có batch sealed và lần retry có thể chụp
snapshot mới. Scoped publication bị chặn nếu streams chưa được freeze.

Giữ fixed page size 1.000, composite cursor
`(source_updated_at, source_sequence, primary-key components)` và keyset paging.
Delta bounds cho từng table:

```text
before = pipeline_runs.control_before[table].committed_cursor
lower = before.updated_at - 48 hours     # inclusive timestamp; null at bootstrap
upper = max composite source cursor in the frozen snapshot
window = source_updated_at >= lower AND composite_cursor <= upper
```

Lookback neo vào source cursor timestamp, không vào wall clock. Lần đầu `before`
null sẽ đọc toàn source. Empty source giữ null/before; candidate luôn là max của
before và upper, không lùi. Mỗi bảng có fixed bounds riêng trong cùng source snapshot.
Delivery cursor không được chọn bounds cho incremental; delivery cursor có thể đi
trước sau failure và được giữ monotonic riêng bằng `delivery_watermark_candidate`.

## Full FK dependencies

Sáu full streams có run ID `fk_` + SHA-256 của pipeline ID, delivery controls riêng
và `snapshot_kind=FULL`. Orders parent Silver pin full customer parent. Orders
delta pin full customer parent; items/payments deltas pin full orders parent;
items v2 còn pin full products/sellers; deferred resolution pin cùng đủ ba parents. Không dùng delta làm parent key set.

Customers/orders parents chỉ phục vụ FK, không load như deltas. Products/sellers/geolocation parents đồng thời là full dimension load inputs, load receipt dùng owner pipeline ID; không thêm delta cursor. Full customers
Silver tiếp tục `customers-silver-v1`; customer delta dùng
`customers-silver-v2`, `snapshot_kind=INCREMENTAL`, chỉ được dùng làm load input.
Các delta Silver khác giữ processor version hiện có và khai báo INCREMENTAL.
Full parent scans vẫn tốn I/O/CPU; pipeline không tuyên bố đã loại bỏ full scans.

Reviews là full child snapshot, pin full orders trong cùng freeze; load/registration/
resolution dùng owner pipeline ID. Không coi reviews là dimension hoặc delta.

## Accounting and completion

Mỗi attempt có **38 tasks**: 21 core/deferred/dbt tasks, source freeze, 12 full-stream publication tasks và 4 full snapshot load tasks.
Final gate kiểm tra mười committed Bronze/Silver stream receipts và tám dataset load receipts, cùng source snapshot,
pipeline baseline/version, exact bounds và dependency hashes. Warehouse/deferred
receipts, staging locks, fresh dbt build và four-table CAS giữ nguyên như contract 11.
Failure không promote cursor; staging của các stages đã commit vẫn replay an toàn.

Với bốn core deltas, counters partition **source versions trong window**, không phải net source growth:

```text
source_rows_extracted = source_bootstrap_rows + source_after_cursor_rows + source_lookback_rows
processing_input_rows = source_rows_extracted + deferred_retry_input_rows
```

Bootstrap rows chỉ xuất hiện khi before null. After-cursor rows có composite cursor
lớn hơn before; lookback rows có cursor nhỏ hơn hoặc bằng before trong window.
After-cursor có thể là UPDATE, không được gọi là số business inserts. Các source
rows được đọc lại không cộng vào source growth. `full_parent_source_rows` ghi đúng
hai auxiliary scans, tại customers/orders; không cộng vào delta processing inputs.
Products/sellers/geolocation ghi `dimension_snapshot_rows` riêng, không cộng vào growth hay lặp lại trong `full_parent_source_rows`. Routing/load equations của contract 11 áp dụng riêng từng dataset; core deltas cộng retry occurrences.
Reviews ghi `review_snapshot_rows` riêng và có deferred retry counts, không có
bootstrap/after-cursor/lookback hoặc completion cursor. Gate khóa tám staging
tables, còn dbt source set vẫn là bảy nguồn đã model. Health JSON/HTML hiển thị các counters này; old full runs có field chưa có sẽ hiện
Unavailable. Queue metrics vẫn đọc backlog chung của các modes.

## Airflow and recovery

DAG `olist_core_incremental` daily UTC, paused mặc định, no catchup và max active
runs 1; gọi cùng runner bằng `DATASET_MODE=incremental`. Chỉ unpause mode muốn
vận hành; không bật đồng thời full và incremental daily schedules trên cùng local
checkout. Database guard/file lock chặn overlap, nhưng task cạnh tranh có thể fail.
Airflow retry/Clear **cùng DAG run** giữ identity/context/mode; không tạo run mới
để thay pending run. Manual failure gọi lại `make dataset-incremental`.

Giữ code/config/contract và tất cả prepared pages khi resume. Đổi contract hoặc
dependencies cần hoàn tất pending run bằng version cũ trước. Code hash pin cả DAGs
và runtime lock. Không sửa hashes, controls hoặc pending file để ép success.

## Verification and limits

`make test-incremental-integration` dùng PostgreSQL/MinIO disposable và Spark/dbt
thật. Fixtures kiểm tra bootstrap/empty source, inclusive boundary, newer sequence
với older timestamp, ngoài SLA, source writes giữa tables/sau freeze, atomic freeze
rollback, delivery cursor đi trước, dbt failure, retry manifests và stale merges.

Updates quá 48 giờ không được bảo đảm tự thu thập; full FK presence không đồng
nghĩa row ngoài window đã vào staging. Repair ngoài SLA dùng [isolated backfill](13_backfill_pipeline.md). Chưa có
delete propagation hoặc CDC. Analytics vẫn là views, không atomic published
snapshots. SQLite Airflow deployment vẫn local-only; chưa có stage-level DAG graph.
