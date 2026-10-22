# Durable Deferred Lifecycle

## Scope

Status: **Active — missing-customer/order retries, persisted limits and atomic staging resolution**.
Migration `008` thêm shared `audit.quarantine_records`, registration/resolution runs
và per-record attempts. Legacy rules là `missing_customer` và `missing_order`.
Migration `015` thêm composite queue rule cho [item v2](17_item_dependency_snapshots.md); generic rejects/warnings quarantine và scheduled DAG audit còn planned; local run/task audit đã có theo [pipeline contract](11_pipeline_control.md).
CLI dùng Spark extra, Java 17, `WAREHOUSE_DSN`/`PG*` và S3 credentials như warehouse.

## Registration and identity

Registration chỉ nhận verified dependency-aware Silver: `orders-silver-v2`,
`order_items-silver-v1/v2`, `order_payments-silver-v1`. Kiểm tra marker, manifest/hash,
source contract, all object bytes, physical Parquet schema và deferred row count.
Legacy row có đúng một missing-parent rule; item v2 giữ mọi missing rule nhưng chỉ tạo một queue entry. Không nhận local errors. Raw Bronze payload,
normalized typed payload, original physical/logical UID, source key/version và
origin Silver prefix/hash được giữ nguyên; không đọc lại source PostgreSQL.

Quarantine unique key là `(record_uid, rule_id)`. Re-register cùng payload/policy
không tạo thêm row hoặc reset first-seen time, attempts hay terminal state. Payload,
contract hoặc limits khác cho cùng identity làm fail. Registration run pin exact
request; replay cùng run ID trả `NOOP`, request khác bị reject.

Limits được freeze trên từng quarantine row: default `max_attempts=3`,
`max_deferred_age_hours=168`; configurable trong khoảng 1–100 và 1–8760.
Age bắt đầu từ lần registration commit, không từ business/source timestamp.
Không sửa policy hoặc reopen terminal records bằng cách tái dùng run ID.

## Resolution state machine

```text
PENDING -> RESOLVED: dependency có trong pinned full parent snapshot
PENDING -> EXPIRED: đạt max age hoặc hết số lần thử
```

Orders dùng full customer Silver; items/payments dùng full orders Silver. Snapshot
phải đúng dataset/version, cùng source contract, đã verify và `snapshot_kind=FULL`.
Parent key set chỉ gồm valid unique nonblank keys, count phải khớp manifest.
Prefix và manifest SHA-256 được pin trong resolution request; đổi parent cần
resolution run ID mới. Không dùng delta làm toàn bộ parent key set.

Mỗi committed pass tăng attempts đúng một lần trên selected records; replay không
tăng. Age được kiểm tra trước dependency: hết age thì expire kể cả parent đã tới.
Parent có thể resolve tại lần thử cuối được phép; nếu vẫn thiếu ở lần đó thì
expire với `terminal_reason=max_attempts`. EXPIRED là terminal reject của resolution
run, không load staging. RESOLVED/EXPIRED không được retry trong pass tiếp theo.

Mỗi pass chọn tối đa 1.000 PENDING rows/bảng, theo first-seen và UID; result ghi
`remaining_pending_rows`. Các snapshots/selected payloads hiện xử lý trong memory
của local MVP; cần distributed processing trước khi mở rộng quy mô.

Backfill resolver thêm `--origin-run-id <original payload _run_id>` (trùng owning pipeline ID trong backfill): selection và
`remaining_pending_rows` chỉ tính queue có `run_id` đó. Filter được pin trong
resolution request; thay origin cần resolution run ID mới. Backfill final gate
bắt buộc origin bằng owning pipeline ID, giữ queue của run khác nguyên trạng.
Không truyền origin filter thì xử lý global queue trong cùng dependency scope: legacy và item v2 không chọn lẫn nhau.
Health/dashboard luôn đo backlog chung. Xem [contract 13](13_backfill_pipeline.md).

## Duplicate occurrences and version safety

Các Bronze runs khác nhau có thể tạo nhiều physical UIDs cho cùng business key/
version. Giữ audit của từng occurrence; trước merge, chọn source sequence mới nhất
trên toàn bộ business key, tie-break theo UID. Các occurrence còn lại được ghi
`duplicates_rows` và `duplicate_record_uids`. Queue state của chúng là RESOLVED
(dependency đã có), không có nghĩa mỗi UID tạo một warehouse row.

Cùng key/version khác business hash làm fail toàn resolution transaction. Warehouse
merge vẫn bỏ qua stale versions; queue có thể RESOLVED nhưng load outcome là
`stale_ignored` nếu staging đã có version mới hơn. Original lineage của selected
winner được giữ; warning flags chỉ đếm trên valid selected winners.

## Atomicity and reconciliation

Cooperating lifecycle writers dùng một PostgreSQL transaction advisory lock.
Selection, attempts, queue transitions, version-aware staging merge và cả hai
audit receipts cùng transaction. Lỗi merge, count mismatch hoặc audit insert làm
rollback tất cả. Retry sau failure có thể chọn lại queue hiện tại; trước commit
không có external publication hoặc attempt nào được coi là hoàn thành.

Resolution run pin request; replay đã commit trả original result với `NOOP`.
Warehouse load ID nội bộ là `deferred_<SHA256([table,resolution_run_id])>`, tách khỏi
normal dataset load IDs. `audit.warehouse_load_runs.silver_counts` trong mode này
là routing counters của retry pass (request ghi `origin=DEFERRED_RETRY`), không phải
một Silver manifest mới. Original Silver artifacts luôn bất biến.

```text
source_rows_extracted = bronze_rows_read = 0
deferred_retry_input_rows = valid_rows + rejected_rows + deferred_rows + duplicates_rows
load_candidate_rows = valid_rows
load_candidate_rows = inserted + updated + unchanged + stale_ignored
```

Không tính retry inputs thành source growth. Terminal rejects được lưu bằng
resolution audit/attempts và quarantine reason; không sửa rejected Parquet của
original run. Không publish thêm Silver resolution batch; local watermarks được commit ở final [pipeline gate](11_pipeline_control.md), không ở resolution pass.

## Commands and operation

```bash
make migrate
make register-deferred SOURCE_TABLE=orders SILVER_PREFIX='<FK-aware Silver prefix>' \
  DEFERRED_RUN_ID=register_001 MAX_DEFERRED_ATTEMPTS=3 MAX_DEFERRED_AGE_HOURS=168
make resolve-deferred SOURCE_TABLE=orders PARENT_SNAPSHOT='<full customer Silver prefix>' \
  DEFERRED_RUN_ID=resolve_001
make test-deferred-state-integration
make test-deferred-integration
```

`make dataset` tự register và resolve sau mỗi dependent Silver/staging load, trước
dbt. Customers chạy trước orders; full orders trước items/payments. Reports thêm
`*_deferred_registration.json`, `*_deferred_resolution.json` dưới ignored build
directory. Failure giữ pending dataset run ID; retry dùng lại IDs để không double
count attempts. Standalone Silver/warehouse CLIs không tự register: dùng commands
ở trên nếu không chạy runner. Chưa có Airflow scheduler; pass tự chạy khi runner
được gọi, không phải background retry service.

Independent tests cover late/backdated parents, final-attempt success, age/attempt
expiry, physical duplicates, same-version conflicts, corrupt payloads/snapshots,
concurrency, atomic rollback, decimal preservation, CLI và original lineage.
