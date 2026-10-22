# Local Pipeline Audit and Watermarks

## Scope

Status: **Active — local manual full-snapshot pipeline, receipt reconciliation and atomic control commit**.
Migration `009` thêm `audit.pipeline_runs`, `pipeline_task_runs`,
`pipeline_dataset_runs` và `pipeline_control`. Namespace là `local_core_v1`;
Airflow có daily full wrapper dùng cùng namespace. Incremental dùng namespace riêng
theo [contract 12](12_incremental_pipeline.md); isolated backfill theo [contract 13](13_backfill_pipeline.md).

Runner local freeze sáu full snapshots trong cùng source transaction: bốn core tables và products/sellers. Migration `016` thêm owned full/FK delivery streams; chỉ bốn core tables có completion cursors. Full mode được bổ sung
cho items/payments; default standalone extraction của orders/items/payments vẫn
bounded incremental. Bootstrap không được lấy một delta sau Bronze checkpoints cũ
làm bằng chứng rằng toàn source snapshot đã được xử lý.

## Run lifecycle and concurrency

`make dataset` giữ filesystem lock trong `build/reports/dataset/runner.lock` cho
toàn runner. PostgreSQL advisory transaction lock serialize control operations;
chỉ một run ID được `RUNNING` trên cả ba namespaces. Pending run ID nằm trong file cũ;
không tự đổi ID để bỏ qua unfinished Bronze/state.

Begin pin SHA-256 của source/scripts/config/dbt/lockfile/Makefile cùng Airflow DAG/
runtime lock, contract và public connection identity, endpoint/bucket, reader role.
Airflow DAG/run context cũng được pin nếu có. Không lưu password hoặc
DSN. Cùng ID nhưng code/config khác bị reject. Resume tăng run attempt, giữ baseline
control và original start time; old running tasks được đánh dấu failed. Task của
attempt cũ không được ghi vào attempt mới.

Mỗi full attempt có 26 tasks: source freeze, Bronze/Silver/staging cho sáu tables,
registration/resolution cho ba dependent tables và `dbt.build`. Stage failures ghi `FAILED`,
giữ pending ID và không promote watermark. Process crash có thể để `RUNNING`;
resume cùng ID tạo attempt mới, replay immutable data-plane receipts.

Local file lock bảo vệ cùng checkout; không phải distributed execution lease.
Không chạy cùng run ID từ nhiều hosts/checkouts. DAG local đặt `max_active_runs=1`;
distributed ownership vẫn cần được triển khai trước khi mở rộng orchestration.

## Receipt verification

Final gate kiểm tra server-side receipts, không tin counters trong local JSON reports:

- Mọi Bronze run đã `COMMITTED`, all remote bytes/marker/hash/counts hợp lệ, full
  snapshot, đúng contract/table/run ID và không có lower bound.
- Silver full snapshots đúng version, pin đúng Bronze manifest hash và đúng parent
  snapshots của **cùng run**. Customers/products/sellers → orders → items/payments là dependency order. Items v2 pin đủ ba parents; legacy item v1 không được core gate chấp nhận.
- Normal warehouse audit phải pin đúng Silver manifest và routing counts.
- Deferred registration/resolution phải pin đúng Silver/parent hashes; retry
  staging receipt phải khớp resolution counters và resolution run ID.
- Tất cả tasks của current attempt phải `SUCCESS` trước commit.

Raw verification, seed/migration và dependency setup là preflight, trước pipeline
begin. Thiếu CSV thì skip như trước; thiếu Java chỉ hoàn thành Bronze, giữ pending
ID và báo `PARTIAL`. Partial flow cũng begin/freeze owned inputs để resume với Java
không đổi request; run chưa SUCCESS và không promote watermark.

## dbt consistency gate

Coordinator giữ `SHARE` locks trên sáu staging tables, chạy **fresh dbt build** và
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

Candidate mỗi core cursor table là max của pipeline cursor trước run và full snapshot upper
cursor, theo `(source_updated_at, source_sequence, primary key components)`.
Empty/backdated upper không làm cursor lùi. Products/sellers có dataset receipts và `dimension_snapshot_rows`; watermark before/candidate/after đều null, không tạo cursor mới. Sau mọi gates, compare-and-swap cả bốn
cursor versions, insert dataset reconciliation rows và mark pipeline `SUCCESS`
trong **một PostgreSQL transaction**. Một CAS conflict làm rollback toàn bộ.

Reject/deferred records đã được giữ và đối soát không bắt buộc phải thành valid
trước promotion; original rejected Silver và durable deferred queue vẫn giữ
evidence/recovery path. Bronze delivery checkpoint vẫn độc lập và có thể đi trước
pipeline control. Pipeline watermark này chưa được dùng làm extraction cursor;
full runner tiếp tục full snapshots. Incremental namespace dùng cursors riêng theo
[contract 12](12_incremental_pipeline.md); isolated backfills theo [contract 13](13_backfill_pipeline.md).

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

## Airflow retry identity

DAG `olist_core_full_snapshot` gọi runner bằng một BashOperator task. Context
được truyền qua environment và hash thành valid run ID; try number không tham gia
identity. Cùng DAG run retry/Clear sẽ replay cùng receipts hoặc NOOP sau success,
kể cả khi pending file không còn. Pending của run khác bị reject trước seed/load.
Thiếu CSV/Java fail thay vì skip. Manual runner giữ hành vi skip/PARTIAL cũ.

Không gọi `make dataset` thiếu original Airflow context để resume một Airflow run;
dùng Retry/Clear trong cùng execution. Namespace/control/run_type vẫn là local
full snapshot, không dùng Airflow logical date làm bounds. Default DAG paused,
daily UTC, catchup off, max active runs 1. Xem [local deployment/recovery](../../infrastructure/README.md#local-airflow).

## Operational health

`make pipeline-health` hoặc `olist-pipeline-health --thresholds <path>` xuất JSON
qua stdout; dùng `PG*` và chỉ cần reader role. Một transaction `REPEATABLE READ,
READ ONLY` đọc run/dataset audit và quarantine, statement timeout 15 giây. Không
đọc payload, không tải source/S3, không sửa retry status, attempts hoặc cursors.

`config/pipeline_health.json` có `max_run_seconds` cho toàn pipeline và sáu
dataset policies: `max_freshness_hours`, `max_reject_rate`, `max_warning_rate`,
`max_pending_age_hours`. Mọi key/dataset bắt buộc phải có; rates thuộc `[0,1]`,
time limits phải dương và hữu hạn. Defaults 30 phút/run, 24 giờ freshness,
1% reject, 5% warning, 168 giờ pending là **ngưỡng vận hành local khởi đầu**,
không phải SLA đã được chứng minh bằng benchmark.

- Run đang `RUNNING` được ưu tiên; nếu không có, lấy execution hoàn tất gần nhất.
  `FAILED` luôn tạo issue dù vẫn có một success cũ. Elapsed runtime tính từ
  original begin tới completion/server time, gồm cả thời gian chờ resume.
- Freshness là tuổi của **lần pipeline SUCCESS cuối**, không phải tuổi business
  timestamps, source change lag hoặc bằng chứng source không còn thay đổi.
- Reject/warning rates dùng counters của success đó: `rejected_rows` hoặc
  `warning_rows` chia `processing_input_rows`, gồm source và retry occurrences.
  Empty input có rate 0; warnings có thể trùng valid/rejected/deferred rows.
- Backlog dùng trạng thái queue **hiện tại**: pending/expired occurrences và tuổi
  pending lâu nhất. Đây không phải số distinct business keys. Pending đã hết
  persisted attempts/age tạo `pending_resolution_due` kể cả khi operational
  age threshold còn cho phép; report không tự expire hoặc retry rows.

JSON ghi actual metrics, thresholds, run IDs và issue codes. Thiếu run/success
hoặc dataset receipt là unhealthy, không thay bằng giá trị giả. Ngưỡng vượt khi
actual **lớn hơn** limit; persisted expiry dùng `>=` theo lifecycle. Exit code `0`
là `HEALTHY`, `1` là `UNHEALTHY`, `2` là lỗi config/connection/schema và chỉ ghi
thông báo rút gọn vào stderr. Chưa có notification; command
không phải publication gate và không thu hồi watermark đã commit.

### HTML dashboard

`make pipeline-dashboard` ghi `build/reports/pipeline_dashboard.html`; override bằng
`PIPELINE_DASHBOARD_OUTPUT=<path>`. Tương đương CLI
`olist-pipeline-health --html-output <path>`. JSON stdout và health exit codes giữ
nguyên; unhealthy vẫn tạo HTML. Renderer dùng cùng snapshot/thresholds, escape
audit text, không JavaScript, CDN, network requests hoặc raw record payload.
File UTF-8 được replace atomically bằng helper hiện có; parent folders được tạo.

Dashboard hiển thị run/attempt/failure/runtime, last success ID, issue codes,
dataset metrics và ngưỡng kèm định nghĩa metric. Đây là **snapshot tĩnh**, không
phải live monitoring. Khi read/write lỗi, CLI trả `2`; file cũ có thể còn tồn tại.
Luôn kiểm tra `Snapshot checked at` và exit code trước khi dùng làm evidence.
Chưa có historical trends, scheduler hoặc notifications.

## Failure recovery verification

`make test-recovery-integration` dùng fixture nhỏ và PostgreSQL/MinIO/Spark/dbt
thật cho incremental và backfill. Mỗi mode đi qua cùng bốn failure boundaries rồi
resume cùng run ID ở attempt 5:

| Failure boundary | Durable state | Required recovery behavior |
| --- | --- | --- |
| After Bronze commit, before task success | Remote objects/marker, delivery checkpoint | Pipeline controls giữ nguyên; reuse sealed pages và manifest |
| After Silver publication, before task success | Verified Parquet/manifest/marker | Chưa load orders staging; artifacts giữ nguyên bytes khi retry |
| After warehouse commit, before task success | Staging rows and load receipt | Replay cùng load ID; không thêm rows hoặc load audit |
| After dbt and CAS writes, before outer transaction commit | Dataset loads and deferred receipts | Rollback mọi control/dataset-audit writes; run FAILED; fresh dbt bắt buộc khi resume |

Tests thay đổi live source sau lần lỗi đầu, đối chiếu tám frozen manifests/pages,
remote object hashes, staging grains/metrics và deferred queue/attempt count.
Final incremental success advance controls đúng một lần; backfill giữ nguyên
controls. Replay committed run trả NOOP. Warehouse tests riêng dùng batch ID mới
với cùng source versions để xác minh unchanged counts cho cả bốn grains.

`tests/unit/test_make_dataset.py` kiểm tra actual runner `run/main` với subprocess
responses giả lập: exact failed task, giữ pending ID, reset in-memory context và
resume cùng ID. Integration inject controlled exceptions ở production stage
functions; chưa mô phỏng OS process kill, mất điện hoặc network partition.
Dữ liệu staging đã commit trước final gate được giữ; dbt views vẫn chưa atomic
publication. Tests không coi failed final gate là rollback toàn bộ data plane.
