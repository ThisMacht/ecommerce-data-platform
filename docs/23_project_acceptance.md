# Project Acceptance Checklist

Last reviewed: 2026-10-24. Checklist này ghi implementation và evidence đã có;
không xem một command tồn tại hoặc test fixture PASS là bằng chứng live demo.
Architecture và ERDs nằm tại [data model](22_architecture_and_data_model.md);
commands/recovery nằm tại [runbook](runbooks/01_data_processing_runbook.md).

Báo cáo kỹ thuật dùng audit, logs, CI và measurements làm evidence chính; ảnh/video
là minh họa tùy chọn. Không cần chụp thêm ảnh để hoàn thiện tài liệu, nhưng vẫn phải
ghi rõ live verification nào chưa hoàn tất.

## Implemented and Verified

- [x] Nine-file raw manifest/checksums, source contracts, idempotent PostgreSQL seed
  và deterministic four-table mutation simulator.
- [x] Tám core datasets Bronze/Silver/staging; verified immutable publication,
  explicit normalization/routing, composite grains, decimal money và pinned full parents.
- [x] Version-aware atomic warehouse merges; bounded deferred retry/expiry,
  immutable payload/limits và atomic queue/attempt/staging/audit resolution.
- [x] Full/incremental/backfill runners, frozen snapshots, receipt reconciliation,
  fresh dbt gate, all-table CAS và backfill cursor isolation.
- [x] Recovery suites cho failures sau data-plane commits và rollback tại final gate;
  frozen retry không nhân đôi loads/deferred attempts. Controlled exceptions,
  chưa phải OS-kill/network-failure drills.
- [x] 31 dbt views: 5 dimensions, 5 facts, 10 marts cùng staging/intermediates;
  grain/metric/key/association tests và real catalog descriptions/output-column checks.
- [x] Local Airflow full/incremental/manual-backfill DAGs với import/serialization
  checks và stable retry context; DAGs paused mặc định.
- [x] Read-only health JSON/static HTML và managed local Metabase dashboard;
  sáu business-model grants cùng audited publication gate.
- [x] Standalone synthetic events generator, artifact verification và
  pandas/Polars/PySpark benchmark đối chiếu independent oracle.
- [x] Source/warehouse ERDs, direct dbt lineage và operating procedures được mô tả
  theo implementation; xem [documentation index](README.md).

## Hosted CI Evidence

Source revision `f2ced4dd993608ec9fc6135ece71078ee9f2596c` đã **SUCCESS 11/11 jobs**:
[GitHub Actions run 40000218387](https://github.com/ThisMacht/ecommerce-data-platform/actions/runs/40000218387).
Bao gồm fast/wheel trên Python 3.10/3.12, Airflow DAG checks, transactional
PostgreSQL/MinIO, dbt/catalog, bốn Spark groups và hai benchmark correctness lanes.
CI dùng fixtures; không chạy full Olist CSV hoặc large performance benchmark.
Evidence gắn revision nêu trên, không tự chứng nhận các source changes về sau.

## Full Olist Baseline

Baseline 2026-10-23 tại source `14f193a`, run
`dataset_54ab35e89b4d48e48a2649bf88b5b862`, attempt 1:

| Check | Observed result |
| --- | --- |
| Raw verification | 9 canonical CSV checksums PASS |
| Atomic freeze | 8 full streams, 1,550,851 source rows |
| Pipeline audit | 34/34 tasks SUCCESS |
| Fresh dbt gate | 261/261 nodes PASS: 31 models + 230 tests |
| `fct_orders` / `fct_delivery` | 99,424 rows mỗi model |
| `fct_order_items` | 112,621 rows |
| `fct_payments` | 103,869 rows |
| `fct_reviews` | 99,197 rows |
| Warehouse normal loads | 1,550,761 candidates unchanged; inserted/updated/stale ignored = 0 |
| Deferred retry input | 168, hạch toán riêng khỏi source rows |
| Controls | 4 local versions tăng đúng một lần, cursor values giữ nguyên; incremental/backfill không đổi |
| Health / BI | HEALTHY; publication available; six-model reader grants + status view verified |
| Runtime | Pipeline audit 1,355.07 s; toàn bộ `make dataset` 1,364.20 s |

Source/staging counts không đổi; merge refresh lineage metadata. GMV đối soát
BRL 13,491,218.65; eligible gross paid BRL 15,736,441.59; global positive-paid AOV
BRL 160.27 trên 98,190 paid orders. Giữ nguyên rejection/deferred/expiry policies,
không xóa terminal queue để làm đẹp kết quả.

Đây là warm full rerun trên staging/volumes có sẵn, không phải cold initial load.
Java 17, JVM tối đa 8 CPUs, Spark heap 2 GiB, workloads tuần tự; chưa đo peak RAM
cả stack. Thời gian này thuộc revision baseline, không suy ra cho revision/máy khác.
Số source rows core không gồm 71 category translations; tổng seed 9 tables là 1,550,922.

Evidence local được ignore: `build/reports/olist-baseline-{before,after,health}.json`,
`olist-baseline-full.log`, `olist-baseline-time.txt`, `olist-baseline-verification.log`,
run-specific dbt artifacts trong `build/reports/dataset/` và PostgreSQL audit tables.
Không commit CSV, report chứa dữ liệu hoặc credentials để chứng minh acceptance.

## Synthetic Workload and Benchmark

- Generator 2026-10-24: 2,000,000 physical = 1,980,000 unique + 20,000 duplicate
  rows; independent verification PASS. Method, payload hash và generator-only
  measurements nằm trong [event contract](data_contracts/20_synthetic_events.md).
- Benchmark 2026-10-24: 9/9 trials PASS, 3 rounds/engine, cùng 165 oracle groups,
  CPU affinity 4 CPUs và sampled process-tree RSS guard 4 GiB.
  [Benchmark results](data_contracts/21_event_engine_benchmark.md#local-results-2026-10-24)
  giữ versions, startup/work/wall timings, per-engine memory và evidence paths.
- Results chỉ áp dụng workload/config đã đo. Filesystem cache được verification
  làm ấm; sampled RSS gồm JVM, không phải PSS hoặc RAM toàn host. Chưa có 10M,
  cold-cache, Parquet hoặc distributed-cluster measurements; không bắt buộc cho scope này.

## Interrupted Live Demo (2026-10-24)

Lần demo mới đã bắt đầu bằng DAG `olist_core_full_snapshot`, DAG run
`scheduled__2026-10-23T00:00:00+00:00`, pipeline run
`airflow_7bdc420ca7a6d2c20fecd65007d91bb22fc877113b246d88579707ab8ef2c7b1`.

Lúc **02:00:28 +07:00**, `systemd-oomd` kill VS Code application scope do memory
pressure; Airflow và runner chạy trong terminal thuộc scope đó cũng dừng.
Kiểm tra sau incident xác nhận không còn Airflow/runner processes, port 8080 đóng.
Audit dừng tại `geolocation.silver.process`; geolocation Bronze đã SUCCESS.
Run/task vẫn ghi `RUNNING` do process mất trước khi cập nhật failure, không phải
bằng chứng còn thực thi. Không sửa audit thủ công để tạo trạng thái hoàn tất.

Đối soát sau incident: **0 completion controls** mang run ID này và **0 final
dataset receipts**. Final dbt/CAS gate chưa hoàn tất; chưa có SUCCESS cho lần demo
này. Các data-plane commits/artifacts trước đó được giữ để resume, không chạy lại
từ đầu hoặc thay thế bằng run ID mới. Schedule đã được pause; manual run đang
queued chưa được dùng làm bằng chứng execution.

Metabase health trả HTTP 200, nhưng publication status của run này là
`available=false`. Ảnh dashboard còn chọn cả hai date filters là 2050-01-01;
cần xóa filters và refresh sau successful gate. Ảnh Airflow SUCCESS ngày 21/10
thuộc run cũ, không chứng nhận revision hiện tại.

CI, full Olist baseline và benchmark nêu trên vẫn là evidence tại đúng revision
đã ghi. Trạng thái interrupted ở trên được ghi tại thời điểm incident; kết quả
recovery tiếp theo nằm trong mục dưới, không dùng ảnh cũ thay evidence mới.

## Verified Recovery (2026-10-24)

Cùng original DAG run/pipeline ID đã được resume qua Airflow API, không tạo
replacement run. `code_sha256` và original request hash giữ nguyên; source được
kiểm chứng tương ứng implementation `f2ced4d` (HEAD lúc recovery `9b1056a`, các
commits sau implementation chỉ sửa tài liệu).

Attempt 2 được dừng có kiểm soát khi phát hiện Silver spool ghi vào `/tmp` tmpfs.
Hai local download caches của đúng run, đã xác minh không còn process dùng,
được dọn; published objects/prepared pages/audit giữ nguyên. Attempt 3 đặt
`TMPDIR`/`SPARK_LOCAL_DIRS` trên disk dưới ignored `build/tmp/closeout/`, chạy
systemd user service tách khỏi VS Code: CPU affinity 4 CPUs, driver heap 2 GiB,
MemoryHigh 4 GiB, MemoryMax 5 GiB. Thuật toán và semantic request không thay đổi.

| Check | Observed result |
| --- | --- |
| Same-run Airflow task / DAG | SUCCESS, task try 3 |
| Pipeline / dataset gate | SUCCESS, attempt 3; 34/34 tasks, 8 dataset receipts PASS |
| Fresh dbt | 261/261 nodes PASS: 31 views + 230 tests; WARN/ERROR/SKIP = 0 |
| Source/staging counts | Không đổi trên cả tám tables; frozen source snapshot được giữ |
| Original committed loads | 3 receipts trước recovery giữ nguyên hash/committed_at |
| Completion controls | 4 local versions tăng đúng một lần; cursor values và namespaces khác không đổi |
| Committed replay | Real `begin` branch trả NOOP; controls không đổi |
| BI publication / reader | Available; đúng sáu business-model SELECT grants + status; denied source/staging/dimensions/other facts, không write privileges |
| Pending run | Đã được runner dọn sau SUCCESS |
| Runtime | Original audit elapsed 2,792.56 s, gồm downtime; tổng task times của successful attempt 992.56 s |
| Resource measurement | Peak service cgroup 4,306,272,256 bytes (~4.01 GiB), OOM kills = 0 |

Cgroup measurement gồm Airflow/children và charged file cache, không gồm Docker
services hoặc RAM toàn host. Tổng task times không phải cold-run benchmark.
Geolocation Silver retry trên disk hoàn tất trong khoảng 97 giây. Unstarted
manual duplicate đã được cancel qua API, không dùng làm success evidence;
Airflow service tạm đã stop và daily schedule paused sau kiểm chứng.

Operational health là **UNHEALTHY**, chỉ có `pipeline.runtime_exceeded`: tổng
elapsed gồm downtime vượt ngưỡng 1,800 s. Không đổi thresholds, reset start time
hoặc chạy pipeline mới chỉ để làm health xanh. Pipeline completion SUCCESS và
operational runtime alert là hai kết quả riêng; BI gate đã available.

Evidence local ignored: `build/reports/closeout/core-recovery-verification.json`,
`before-recovery.json`, `recovery-task-clear*.json`, `resource-adjustment.json`,
`runtime-profile.json`, `health-after-recovery.json`, `finish-check.log`,
run-specific Airflow/dbt logs và refreshed `build/reports/pipeline_dashboard.html`.

## Headless BI Verification (2026-10-24)

Saved dashboard đã được reconcile sau core SUCCESS, giữ nguyên dashboard/card
IDs, private credential file và datasource `olist_metabase_reader`. Kiểm chứng
được thực hiện tuần tự qua PostgreSQL và REST API, không cần browser UI.

| Check | Observed result |
| --- | --- |
| Managed content | 13 saved cards + policy text; original IDs và start/end mappings giữ nguyên |
| SQL reference | 52/52 comparisons PASS: 13 queries × 4 windows |
| Saved dashboard API | 52/52 comparisons PASS với independent raw-staging SQL |
| Date windows | All history; 2018-01-01–2018-12-31; single day 2018-01-01; empty 2050-01-01 |
| Full-history money | GMV BRL 13,491,218.65; gross paid BRL 15,736,441.59; weighted AOV BRL 160.27 |
| Eligible orders | 98,191; positive-paid denominator khác eligible-order count |
| Daily outputs | Revenue/AOV 634 rows; payment-type pivot 613 rows; API không truncated |
| Unavailable gate | Khi original run RUNNING: 12 metric cards rỗng, status card giữ đúng run/available=false; 13/13 API checks PASS |
| Operational CLI | Exit 1 đúng với `pipeline.runtime_exceeded`; completion SUCCESS, không đổi thresholds |

Expected metrics tính từ raw staging, aggregate items/payments riêng theo order
trước khi join; không lấy chính facts/marts đang kiểm tra làm expected. Checks
bao gồm inclusive date bounds, weighted AOV, zero/NULL empty-window behavior,
completeness, status, product/seller rankings và payment-type totals/daily pivot.
Các saved queries cũng được đối chiếu với managed definition; no-filter defaults
giữ NULL để xem toàn bộ lịch sử. Không đổi card IDs hoặc mở rộng reader grants.

Evidence local ignored: `build/reports/closeout/bi-sql-reference.json`,
`bi-api-complete.json`, `bi-api-results.json`, `live-unavailable-metabase-api.json`,
`health-cli.json`, `headless-closeout.json`, `bi-api-verification.log` và
`metabase-reconcile.log`. Các scripts kiểm chứng tạm nằm cùng ignored directory;
không đóng gói thêm công cụ/test framework vào project cho lần vận hành này.

## Closeout Decision

- [x] Same-run Airflow recovery và actual core execution qua API đã SUCCESS;
  receipts/dbt/CAS/source counts và health status đã được đối soát riêng.
- [x] Metabase sau successful gate: managed cards/IDs, inclusive date filters,
  exact SQL/API metrics, restricted reader và live unavailable state đều PASS.
- [x] README, runbook, source/warehouse ERDs và acceptance evidence đồng bộ.
- Release/tag: không nằm trong closeout hiện tại theo yêu cầu người dùng;
  không tạo GitHub Release hoặc dùng release làm prerequisite cho báo cáo.

**Đủ để đóng scope local batch implementation và báo cáo kỹ thuật.** Không cần
thêm screenshots, UI session hoặc workload mới để chứng minh các checks trên.
Ruff/format và 138 unit tests cũng PASS tại lần closeout. Không chạy lại large
engine benchmark hoặc toàn bộ disposable integration suites vì source không đổi
so với implementation đã được hosted CI kiểm chứng; full-data recovery/live BI
đã chạy thực tế như mô tả.

Runtime alert vẫn được giữ và báo rõ; không tuyên bố mọi operational indicators
đều HEALTHY hoặc deployment production-ready. Browser rendering chưa được kiểm
tra lại, screenshots là minh họa tùy chọn. Airflow service tạm đã dừng, mọi DAGs
paused, pending pipeline ID đã dọn; Metabase vẫn chạy local để có thể xem khi cần.

## Known Boundaries

Đây là local batch platform: current-state polling, insert/update-only và Type 1;
không giữ mọi intermediate source update hoặc phục hồi overwritten history.
Incremental lookback 48h không bảo đảm lấy correction backdated xa hơn; dùng bounded
backfill phù hợp. Frozen full FK inputs vẫn có chi phí dù delta nhỏ.

Airflow standalone SQLite, Metabase H2, local file lock và application-enforced
object immutability chưa phải shared production deployment. dbt live views cùng
publication gate không cung cấp atomic multi-card snapshots hoặc phát hiện untracked
writers. Health HTML cần refresh thủ công; chưa có alerting, retention jobs hay
stage-level Airflow task graph. Các giới hạn này không được coi là đã triển khai.
