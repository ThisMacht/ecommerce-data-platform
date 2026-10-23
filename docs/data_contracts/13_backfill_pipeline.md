# Isolated Backfill Pipeline

## Scope and execution

Migration `011` thêm `backfill_core_v1`, `run_type=backfill` và Bronze delivery
streams riêng. Backfill sửa staging/marts dùng chung bằng version-aware merge;
không sửa committed cursor, cursor version hoặc timestamp của full/incremental
controls. Chỉ một pipeline run được RUNNING trên cả ba namespaces.

```bash
make dataset-backfill BACKFILL_START=2020-01-01T00:00:00Z BACKFILL_END=2020-02-01T00:00:00Z
make pipeline-health PIPELINE_NAMESPACE=backfill_core_v1
make pipeline-dashboard PIPELINE_NAMESPACE=backfill_core_v1
```

Bounds bắt buộc có UTC offset, được chuẩn hóa UTC; start phải nhỏ hơn end.
Window là **`source_updated_at >= start AND source_updated_at < end`**, không
phải order purchase date. End exclusive giúp chia các khoảng liền nhau.
Raw CSV/seed, Docker Compose và Java 17 là prerequisites như incremental runner.

## Frozen inputs and dependencies

Tái sử dụng transaction `source.freeze` của contract 12: bốn bounded deltas cùng
sáu full customers/orders/products/sellers/geolocation/order_reviews inputs được chốt atomically, sau đó mới publish.
Window và code/config nằm trong immutable pipeline request. Retry sau freeze giữ
nguyên bytes, upper bounds và parent hashes dù live source đã thay đổi.

Delta streams là `backfill_<table>_delta_v1`; full FK streams là
`backfill_<table>_parent_v1`. Object paths giữ layout Bronze/Silver hiện có, với
run ID duy nhất và hashed parent ID; không ghi đè artifacts của mode khác.
Customer delta không được dùng làm full parent. Customers/orders full parents chỉ kiểm tra FK, không tự load records ngoài window.
Products/sellers/geolocation luôn full refresh và load Type 1, kể cả window rỗng; đây là current
dimension snapshots, không phải ảnh chụp lịch sử tại end bound.

Reviews cũng full refresh ngoài window, pin current full orders trong cùng source
transaction. `review_snapshot_rows` ghi riêng; không thêm review completion cursor
hoặc BI grant. Review fact/mart dùng creation cohort và không join bounded order staging.
Đây không phải review state lịch sử tại end bound.

## Reconciliation and completion

38 audited tasks và fresh dbt/receipt gates như incremental. Bốn core deltas ghi
`source_backfill_rows`; bootstrap/after-cursor/lookback bằng 0. Auxiliary parent
scans được đếm riêng; retry input không phải source growth.

```text
source_rows_extracted = source_backfill_rows
# Four bounded core tables only; dimensions/reviews have separate snapshot counters.
processing_input_rows = source_backfill_rows + deferred_retry_input_rows
```

Resolver pin `origin_run_id` bằng original Silver ID (pipeline ID cho deltas,
hashed parent ID cho reviews): chỉ retry/expire pending records
có original Silver `_run_id` tương ứng. Queue của các run khác không bị consume
hoặc tăng attempts. Giới hạn 1.000 pending rows/bảng/pass vẫn áp dụng; kiểm tra
`remaining_pending_rows`. Scheduled/standalone resolver tiếp tục xử lý global queue.

Staging commit theo dataset; newer `source_sequence` thắng, older version ghi
`stale_ignored`, equal-version/different-payload fail. Final gate giữ staging SHARE
locks qua dbt, ghi candidate và counters vào dataset audit, giữ watermark_after
bằng watermark_before. Backfill core controls luôn null/version 0; dimensions không có completion controls và audit watermarks đều null. Dbt failure không
rollback staging đã commit; resume cùng ID replay receipts.

## Recovery and Airflow

Sau lỗi gọi lại đúng command với bounds, code/config và pending ID cũ. Không đổi
window khi resume. Run SUCCESS replay NOOP; chạy một pass mới cần run ID mới.
DAG `olist_core_backfill` manual-only, paused mặc định, không catchup; trigger với
JSON conf `{"start":"2020-01-01T00:00:00Z","end":"2020-02-01T00:00:00Z"}`.
Bounds/context truyền qua environment, không interpolate vào shell. Retry/Clear
cùng task instance giữ identity; logical date không quyết định window.

## Verification and limits

`make test-backfill-integration` dùng disposable PostgreSQL/MinIO, Spark/dbt thật:
start inclusive/end exclusive, empty window, writes sau freeze, stale merge,
dbt failure/resume, unrelated queue/cursor isolation và exact staging/mart
comparison với full reload trong database độc lập.

Đây là repair của **current mutable source**, không tái dựng historical versions
đã bị source overwrite và không xử lý hard deletes. Full reload equivalence áp
dụng khi window bao phủ current source cần đối chiếu. Analytics vẫn là views dùng
chung, chưa có atomic snapshot publication; full parent scans vẫn có chi phí.
