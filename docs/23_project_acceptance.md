# Project Acceptance Checklist

Last reviewed: 2026-10-24. Checklist này ghi implementation và evidence đã có;
không xem một command tồn tại hoặc test fixture PASS là bằng chứng live demo.
Architecture và ERDs nằm tại [data model](22_architecture_and_data_model.md);
commands/recovery nằm tại [runbook](runbooks/01_data_processing_runbook.md).

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

## Remaining Closeout Evidence

- [ ] Live demo current revision: trigger một DAG thích hợp, lưu đúng DAG/run ID,
  đối chiếu pipeline SUCCESS, receipts/dbt results và health timestamp. Import tests
  hoặc CLI baseline không thay thế bằng chứng DAG thực thi.
- [ ] Kiểm tra Metabase sau successful core gate: managed cards, inclusive date
  filters, counts/GMV/paid/AOV đối chiếu SQL, restricted reader và unavailable state;
  lưu screenshots hoặc video đã che credentials ngoài Git data artifacts.
- [ ] Gắn release/tag và ghi release notes sau khi demo/evidence được kiểm tra.
  Chưa có bằng chứng release hoàn tất trong checklist này.

README/runbook/ERDs đã được đồng bộ; ba mục trên là phần closeout còn lại, không
phải yêu cầu bổ sung công nghệ. Ngay trước demo cần chạy health mới, kiểm tra pending
run và services; kết quả HEALTHY lịch sử không chứng minh trạng thái hiện tại.

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
