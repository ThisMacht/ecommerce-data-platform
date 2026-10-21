# Local Pipeline Audit and Watermarks

## Scope

Status: **Active — local manual full-snapshot pipeline, receipt reconciliation and atomic control commit**.
Migration `009` thêm `audit.pipeline_runs`, `pipeline_task_runs`,
`pipeline_dataset_runs` và `pipeline_control`. Namespace là `local_core_v1`;
đây chưa phải scheduled Airflow pipeline hoặc backfill interface.

Runner local dùng full snapshots cho cả bốn core tables. Full mode được bổ sung
cho items/payments; default standalone extraction của orders/items/payments vẫn
bounded incremental. Bootstrap không được lấy một delta sau Bronze checkpoints cũ
làm bằng chứng rằng toàn source snapshot đã được xử lý.

## Run lifecycle and concurrency

`make dataset` giữ filesystem lock trong `build/reports/dataset/runner.lock` cho
toàn runner. PostgreSQL advisory transaction lock serialize control operations;
chỉ một run ID được `RUNNING` trong namespace. Pending run ID nằm trong file cũ;
không tự đổi ID để bỏ qua unfinished Bronze/state.

Begin pin SHA-256 của source/scripts/config/dbt/lockfile/Makefile cùng contract và
public connection identity, endpoint/bucket, reader role. Không lưu password hoặc
DSN. Cùng ID nhưng code/config khác bị reject. Resume tăng run attempt, giữ baseline
control và original start time; old running tasks được đánh dấu failed. Task của
attempt cũ không được ghi vào attempt mới.

Mỗi attempt có 19 tasks: Bronze/Silver/staging cho bốn tables, registration/
resolution cho ba dependent tables và `dbt.build`. Stage failures ghi `FAILED`,
giữ pending ID và không promote watermark. Process crash có thể để `RUNNING`;
resume cùng ID tạo attempt mới, replay immutable data-plane receipts.

Local file lock bảo vệ cùng checkout; không phải distributed execution lease.
Không chạy cùng run ID từ nhiều hosts/checkouts. Airflow `max_active_runs=1` hoặc
distributed ownership vẫn cần được triển khai trước khi mở rộng orchestration.

## Receipt verification

Final gate kiểm tra server-side receipts, không tin counters trong local JSON reports:

- Mọi Bronze run đã `COMMITTED`, all remote bytes/marker/hash/counts hợp lệ, full
  snapshot, đúng contract/table/run ID và không có lower bound.
- Silver full snapshots đúng version, pin đúng Bronze manifest hash và đúng parent
  snapshots của **cùng run**. Customer → orders → items/payments là dependency order.
- Normal warehouse audit phải pin đúng Silver manifest và routing counts.
- Deferred registration/resolution phải pin đúng Silver/parent hashes; retry
  staging receipt phải khớp resolution counters và resolution run ID.
- Tất cả tasks của current attempt phải `SUCCESS` trước commit.

Raw verification, seed/migration và dependency setup là preflight, trước pipeline
begin. Thiếu CSV thì skip như trước; thiếu Java chỉ hoàn thành Bronze, giữ pending
ID và báo `PARTIAL`. Không tạo pipeline success hoặc watermark cho partial flow.

## dbt consistency gate

Coordinator giữ `SHARE` locks trên bốn staging tables, chạy **fresh dbt build** và
giữ locks tới khi audit/control transaction commit. Normal staging writers sẽ bị
chặn trong đoạn này; lock timeout là 15 giây, dbt process timeout là 600 giây.

`run_results.json` phải bao phủ mọi enabled model/test trong cùng manifest, không
duplicate IDs, skipped/failed/error nodes. Invocation ID phải khớp; build phải bắt
đầu sau committed stage receipts. Kiểm tra source/model database và schema đúng
target; hash artifacts được lưu vào dataset evidence. Code fingerprint phải giữ
nguyên trong build. Không nhận một old successful `dbt run` làm fresh build proof.

Models hiện vẫn là views. Shared staging locks giữ consistent source cho final
build/gate, **không** tạo atomic publication cho analytics readers. Failed build
không rollback mọi views đã tạo. Chỉ dùng run `SUCCESS` để xác nhận execution đã đạt.

## Exact counters and promotion

Dataset audit lưu source/Silver/retry/load counters cùng original watermark,
candidate và after. Processing counters cộng **processing occurrences** của source
pass và retry pass; cùng original UID có thể được đánh giá ở cả hai passes.

```text
source_rows_extracted = bronze_rows_written = bronze_rows_read
processing_input_rows = bronze_rows_read + deferred_retry_input_rows
processing_input_rows = silver_valid_rows + rejected_rows + deferred_rows + duplicate_dropped_rows
load_candidate_rows = silver_valid_rows
load_candidate_rows = inserted + updated + unchanged + stale_ignored
```

Full snapshot rows không có nghĩa source growth; không có `source_new_rows` metric
hoặc incremental lookback accounting trong slice này. Source rule hits ghi riêng
`source_rule_violation_count`; không nhầm rule hits với số rows.

Candidate mỗi table là max của pipeline cursor trước run và full snapshot upper
cursor, theo `(source_updated_at, source_sequence, primary key components)`.
Empty/backdated upper không làm cursor lùi. Sau mọi gates, compare-and-swap cả bốn
cursor versions, insert dataset reconciliation rows và mark pipeline `SUCCESS`
trong **một PostgreSQL transaction**. Một CAS conflict làm rollback toàn bộ.

Reject/deferred records đã được giữ và đối soát không bắt buộc phải thành valid
trước promotion; original rejected Silver và durable deferred queue vẫn giữ
evidence/recovery path. Bronze delivery checkpoint vẫn độc lập và có thể đi trước
pipeline control. Pipeline watermark này chưa được dùng làm extraction cursor;
runner tiếp tục full snapshots. Scheduled incremental discovery và isolated
backfills còn planned.

## Commands and recovery

```bash
make dataset
make test-pipeline-control-integration
make test-pipeline-integration
```

CLI `olist-pipeline --run-id <id> begin|task|fail|finish` phục vụ runner; dùng `PG*`
(không tự đọc `.env`/`WAREHOUSE_DSN`), AWS/S3 và dbt environment hiện có. `finish`
cần `--attempt <n> --dbt-target <directory>` và tự chạy dbt; không bypass proof bằng
local report. Reports thêm `<run_id>_pipeline.json` và run-specific dbt artifacts.

Sau lỗi, giữ pending ID và gọi lại `make dataset` bằng code/config cũ. Nếu run đã
commit nhưng mất local response, begin trả `NOOP`, giữ counters/control versions
cũ và dọn pending file. Khi nâng cấp từ bounded runner cũ, hoàn tất unfinished runs
với config cũ trước; không sửa prepared pages, checkpoints hoặc request hashes.
