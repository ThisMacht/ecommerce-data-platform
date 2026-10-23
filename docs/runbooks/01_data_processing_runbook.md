# Batch Data Processing Runbook

| Field | Value |
| --- | --- |
| System | Olist E-commerce Data Platform |
| Owner | Data Engineering |
| Status | Active — core full/incremental/backfill and local Airflow wrappers |
| Last reviewed | 2026-10-24 |
| Processing mode | Batch |

Runbook này định nghĩa quy trình vận hành hiện tại để xử lý snapshot nguồn Olist qua Bronze, Silver, các model warehouse PostgreSQL và analytics marts. Tài liệu mô tả các execution gates, cách xử lý lỗi, rerun và backfill. Runbook phải được cập nhật khi implementation details hoặc operational commands thay đổi.

Tài liệu này là hướng dẫn vận hành bổ sung cho:

- [Architecture and ERDs](../22_architecture_and_data_model.md)
- [Project acceptance checklist](../23_project_acceptance.md)
- [Raw dataset manifest](../../data/raw/README.md)
- [Source contracts](../data_contracts/02_source_contracts.md)

Tám core datasets đã chạy Bronze/Silver/staging: customers, orders, order_items, order_payments, products, sellers, geolocation và order_reviews. Full runner audit 34 tasks; incremental/backfill audit 38. Airflow có ba local wrappers; mục 13–30 mô tả operations hiện có. Evidence lịch sử và live demo còn thiếu được phân biệt trong acceptance checklist.

## 1. Scope

Runbook bao gồm:

- Kiểm tra và load lần đầu Kaggle CSV snapshot.
- Scheduled incremental runs từ PostgreSQL source simulator.
- Manual reruns và backfill theo khoảng ngày được cô lập.
- Quy tắc publish Bronze và Silver.
- Data-quality routing ở cấp record.
- Version-aware warehouse merges.
- Thực thi và kiểm thử dbt models.
- Source-to-target reconciliation.
- Cursor commit, retry và incident recovery.

Runbook không bao gồm các phần optional như streaming, Kafka, ClickHouse hoặc cloud deployment.

## 2. Data flow

```text
Raw CSV snapshot
      │
      ▼
PostgreSQL olist_source + source simulator
      │
      ▼
Atomic full / bounded extraction freeze
      │
      ▼
MinIO Bronze ── manifest + checksum + _SUCCESS
      │
      ▼
PySpark validation, normalization and routing
      │
      ├── Silver rejected / duplicate audit (no staging load)
      ├── Silver valid, including warnings ────────────┐
      └── Silver deferred → bounded retry / expiry ────┤
                                                      ▼
PostgreSQL raw_staging ◀── normal valid + resolved candidates
      │                   version-aware merge
      ▼
dbt staging → intermediate → dimensions/facts → marts
                │
                ▼
Exact receipts + fresh dbt under staging locks → atomic completion gate
                │
                ▼
Health JSON / static HTML; Metabase publication gate (live views)
```

## 3. Run types

| Mode | Entry point | Namespace | Completion controls |
| --- | --- | --- | --- |
| Full (`manual` audit type) | `make dataset` | `local_core_v1` | CAS cả bốn controls sau final gate |
| Incremental | `make dataset-incremental` | `incremental_core_v1` | Pipeline-owned cursors, inclusive 48h lookback; bootstrap riêng |
| Backfill | `make dataset-backfill BACKFILL_START=<ISO> BACKFILL_END=<ISO>` | `backfill_core_v1` | Explicit UTC `[start,end)` source-updated-at; không promote cursors |

Airflow full/incremental DAGs có daily schedule nhưng paused mặc định; backfill
DAG manual-only. Trigger từ CLI hay UI không tự thay đổi mode/cursor policy.
Task retries giữ cùng pipeline ID, frozen inputs, code/config và bounds.

## 4. Required run context

Runner ghi `audit.pipeline_runs` với `run_id`, `pipeline_name`, `run_type`,
`request`, `request_sha256`, `control_before` và status trước data-plane work.
Request pin mode, `code_sha256`, `contract_sha256`, database identity, bucket/endpoint,
dbt reader và original Airflow DAG/run context nếu có; backfill pin thêm UTC bounds.
Không tạo/chỉnh audit thủ công để bỏ qua gate.

Source freeze lưu rows, serialized pages, bounds và parent stream IDs atomically
trước publication. `audit.pipeline_dataset_runs` là completion evidence được ghi
tại final gate, không phải nơi tự ghi extraction bounds trước run.

Cursor ordering là `(source_updated_at, source_sequence, <primary-key columns>)`.
`source_sequence` có thẩm quyền khi merge; primary-key columns giữ deterministic
pagination. Bronze delivery cursors không điều khiển incremental pipeline windows.

## 5. Preflight procedure

Trước khi bắt đầu scheduled hoặc backfill run:

1. Xác nhận Docker Compose, PostgreSQL, MinIO, uv và Java 17 khả dụng; Airflow chỉ cần khi chạy qua DAG, không phải prerequisite của CLI runner.
2. Kiểm tra pending run trong `build/reports/dataset/pending-run.txt`; nếu có, giữ nguyên code/config/mode/bounds để resume trước khi đổi revision.
3. Xác nhận credentials của source, warehouse và object storage được cung cấp qua environment hoặc secret configuration, không nằm trong các file được commit.
4. Không chạy pipeline từ nhiều checkout/hosts hoặc cùng standalone writers. Runner giữ local file lock; DAG dùng `max_active_runs=1`; PostgreSQL guards/CAS không thay thế distributed orchestration.
5. Với lần load raw source đầu tiên, kiểm tra checksum của từng CSV theo [raw dataset manifest](../../data/raw/README.md).
6. Xác nhận database migrations đang ở version mong đợi.
7. Gọi runner đúng mode; runner tự begin/resume audit và pin configuration. Không tự INSERT audit rows.

Operator cần hoàn tất prerequisites trước khi chạy pipeline. Riêng CLI full runner
có thể skip downstream khi thiếu CSV hoặc báo PARTIAL khi thiếu Java và giữ pending
ID; đó không phải pipeline SUCCESS. Incremental/backfill và Airflow wrappers fail
khi thiếu prerequisites. Xem reports/exit status, không suy ra completion từ việc
một số Bronze tasks đã chạy.

## 6. Standard processing procedure

### 6.1. Validate the raw snapshot

Step này áp dụng cho lần load source đầu tiên hoặc khi chủ động tiếp nhận một CSV snapshot mới.

1. Kiểm tra SHA-256 checksums.
2. Parse CSV với UTF-8 support, quoted fields và embedded newlines.
3. Sử dụng `utf-8-sig` hoặc cơ chế tương đương cho category translation file.
4. So sánh logical row counts, column counts và headers với manifest.
5. Làm validation fail nếu thiếu file bắt buộc, checksum khác hoặc schema không tương thích.
6. Không chỉnh sửa raw file để ép validation pass.

### 6.2. Load or seed `olist_source`

1. Apply source-schema migrations.
2. Load CSV records vào source tables bằng quy trình bulk load có tính idempotent.
3. Giữ nguyên source values và column names tại boundary này.
4. Verify row counts, schema/types, primary keys và seed audit. Source không enforce business FKs; Silver kiểm tra dependencies qua full pinned parents, gồm child-before-parent fixtures.
5. Khởi tạo các trường deterministic `source_updated_at`, `source_sequence` và `source_operation` cho simulator.
6. Ghi source-seed manifest chứa row counts, checksums và load timestamp.

### 6.3. Capture bounded extraction parameters

1. Full mode freeze tám full streams trong một source snapshot.
2. Incremental đọc bốn pipeline controls `W` và versions; nếu đã có cursor, lấy
   lower timestamp `W.updated_at - 48h` inclusive, đồng thời chụp upper cursors `U`.
   Chưa có cursor thì bootstrap, không tạo sentinel giả.
3. Backfill dùng explicit offset-aware bounds, chuẩn hóa UTC `[start,end)` theo
   `source_updated_at`; scheduled controls giữ nguyên.
4. Bounded modes freeze bốn delta/bounded streams và sáu full inputs trong cùng
   transaction: customers/orders FK parents, products/sellers/geolocation/reviews.
5. Durable prepared pages và pipeline request pin bytes/bounds trước S3 publication;
   retry không đọc lại mutable source. Keyset page cursor tách khỏi completion cursor.
6. Empty source không tạo watermark giả hoặc ngụ ý xóa staging members.

### 6.4. Extract and publish Bronze

1. Đọc các page theo thứ tự giữa fixed lower bound và upper cursor.
2. Giữ nguyên source business values, không trim, normalize hoặc cast business type.
3. Bổ sung physical record ID, logical record ID, run metadata, source metadata và schema version theo frozen publication request.
4. Ghi objects vào run-attempt path mà downstream tasks không thể consume.
5. Sinh `manifest.json` chứa bounds, row count, schema fingerprint, file list và checksums.
6. Chỉ publish batch sau khi mọi object cùng manifest đã hoàn tất.
7. Ghi `_SUCCESS` sau cùng.
8. Consumer verify marker, manifest, checksums và counts; không đọc trực tiếp object listing hoặc attempt chưa publish.

Gate bắt buộc:

```text
source_rows_extracted = bronze_rows_written
```

### 6.5. Validate and publish Silver

Xử lý customers/products/sellers/geolocation trước full orders, rồi dependent
items/payments/reviews. Bounded customer/order deltas không thay full FK parents.
Core item v2 phải pin đủ full orders/products/sellers từ cùng frozen request.

1. Verify Bronze marker, manifest, checksums, counts và schema fingerprint.
2. Parse explicit schema; normalize theo table contract, money NUMERIC/Decimal BRL,
   source business time America/Sao_Paulo → UTC và giữ local business dates.
3. Validate local rules và deterministic key/version selection; rejected/duplicate
   records không được đưa vào deferred chỉ vì thiếu parent.
4. Selected locally valid rows thiếu required parent đi deferred; các missing item
   dependencies được đánh giá cùng nhau và lưu một immutable queue payload.
5. Warning rows vẫn valid, có `warning_rule_ids`; không cộng warnings thêm một lần.
6. Publish valid/rejected/deferred Parquet cùng duplicate counters, immutable
   manifest và `_SUCCESS`. Retry phải giữ original inputs và parent hashes.

### 6.6. Retry deferred records

1. Register immutable Silver deferred records vào `audit.quarantine_records`, key
   `(record_uid, rule_id)`; replay không nhân đôi occurrence hoặc reset limits.
2. Mỗi pass chọn tối đa 1,000 eligible pending rows/table, pin verified full parent
   snapshots; backfill chỉ chọn queue thuộc origin của run đó.
3. Giữ original physical UID, full business key/version và payload. Item v2 resolver
   kiểm tra cả ba memberships; không consume legacy item v1 scope.
4. Dedupe candidates theo full key/version trước merge; retry inputs không phải
   source growth. Chưa resolve thì giữ PENDING hoặc EXPIRED theo immutable limits.
5. Commit queue transitions, attempts, staging merges và resolution audit trong
   cùng transaction. Terminal states không reopen; expired rows không được load.
6. Replay cùng resolution ID không tăng attempts; pass mới/parent mới cần ID mới.
   Không sửa frozen payload hoặc xóa queue để làm gate PASS.

### 6.7. Merge into `raw_staging`

1. Chỉ đọc các Silver valid batch đã thành công.
2. Thu gọn input về source version mới nhất trên mỗi source key.
3. Thực thi merge và cập nhật dataset audit trong cùng một PostgreSQL transaction.
4. Phân loại từng candidate thành inserted, updated, unchanged hoặc stale ignored.
5. Advance source-version metadata đã lưu khi version mới hơn có cùng business hash.
6. Không bao giờ dùng incoming version cũ hơn để ghi đè target version mới hơn.
7. Làm run fail nếu cùng một source version có hai business hash khác nhau.
8. Roll back cả data lẫn counters nếu transaction thất bại.

### 6.8. Build and test analytics models

1. Dùng locked analytics environment và dbt connection/reader configuration hiện có.
2. Final coordinator giữ SHARE locks trên tám staging tables và chạy fresh `dbt build` toàn bộ models/tests; standalone build không thay completion gate.
3. Yêu cầu các tests về not-null, unique, relationship, accepted-value, grain, fan-out và metric invariant phải pass.
4. Không promote source cursor khi dbt build hoặc tests của nó thất bại.

MVP không cung cấp atomic schema swapping cho toàn bộ marts. Một dbt build thất bại có thể để lại analytical models chỉ được refresh một phần; cần đánh dấu pipeline failed, xem business dashboards là unavailable cho run đó và chạy lại dbt trước khi tuyên bố đã recovery. Shadow-schema publication là phần mở rộng theo hướng production.

### 6.9. Reconcile and commit

Mọi dataset run thành công phải thỏa các phương trình sau với tolerance bằng 0:

```text
source_rows_extracted = bronze_rows_written
bronze_rows_written = bronze_rows_read

processing_input_rows = bronze_rows_read + deferred_retry_input_rows
processing_input_rows =
    silver_valid_rows
    + rejected_rows
    + deferred_rows
    + duplicate_dropped_rows

load_candidate_rows = silver_valid_rows
load_candidate_rows =
    warehouse_inserted_rows
    + warehouse_updated_rows
    + warehouse_unchanged_rows
    + warehouse_stale_ignored_rows
```

Warning rows là tập con của valid rows và không được cộng thêm vào các phương trình. Data-quality violation counts đo số rule hits, không phải số distinct routed records.

Final dataset counters ở trên cộng normal Silver routing với deferred resolution
routing, và cộng normal load với retry load. Từng receipt được verify riêng trước
khi tổng hợp; không dùng normal Silver manifest làm retry output hoặc tính retry
inputs là source growth. Warning rows cũng được tổng hợp theo cùng phạm vi.

Sau khi remote artifacts, tám dataset receipts, fresh dbt và counters đều hợp lệ,
coordinator ghi per-dataset audit, all-table cursor CAS và run SUCCESS trong cùng
transaction. Full/incremental dùng candidates không lùi so với `W`; backfill không
promote bất kỳ completion cursor nào. Conflict/rollback giữ controls cũ. Sau đó
chạy health/dashboard mới; status gate cho phép BI nhưng views vẫn là live data.

## 7. Failure and recovery matrix

| Failure point | Expected state | Recovery action | Cursor action |
| --- | --- | --- | --- |
| Raw checksum or schema validation | Chưa load source | Khôi phục snapshot mong đợi hoặc chủ động tiếp nhận và profile lại snapshot mới | Không thay đổi |
| Source extraction | Bronze attempt có thể chưa hoàn chỉnh | Retry với cùng run bounds; bỏ qua các attempt chưa publish | Không thay đổi |
| Bronze manifest verification | Silver chưa được publish | Giữ artifacts; điều tra corruption/config mismatch, không sửa manifest hoặc request để ép resume | Không thay đổi |
| Spark validation job | Silver attempt có thể chưa hoàn chỉnh | Retry từ Bronze đã publish | Không thay đổi |
| Warehouse merge | PostgreSQL transaction đã rollback | Sửa nguyên nhân và chạy lại cùng Silver input | Không thay đổi |
| dbt build or test | Staging có thể đã load; marts có thể chỉ hoàn tất một phần | Đọc failed nodes, khôi phục runtime/input hợp lệ và resume cùng pinned code/config; code fixes phải tuân change control | Không thay đổi |
| Reconciliation mismatch | Data vẫn audit được nhưng run đã failed | So sánh record IDs và manifests theo từng stage; không bỏ qua row loss | Không thay đổi |
| Cursor compare-and-swap conflict | Data có thể đã được load idempotently | Điều tra overlapping runs; không sửa control_before/bounds hoặc tạo ID mới để bypass unfinished run | Không ghi đè |

Không bao giờ recovery incident bằng cách chỉnh sửa thủ công raw CSV, xóa quarantine records, thay đổi audit counters hoặc advance cursor mà không có investigation được tài liệu hóa.

## 8. Idempotent rerun procedure

1. Giữ lại audit rows, manifests và logs của failed run.
2. Xác định gate thất bại sớm nhất.
3. Tái sử dụng immutable upstream artifact thành công gần nhất thay vì đọc lại source cũ khi không cần thiết.
4. CLI gọi lại đúng Make command/mode/bounds với code/config cũ; Airflow Retry/Clear task trong cùng DAG run, không trigger replacement run.
5. Kỳ vọng warehouse rerun phân loại phần lớn record đã load thành unchanged hoặc stale ignored.
6. Thực thi lại dbt tests và mọi reconciliation equation.
7. Xác nhận warehouse row counts không tăng chỉ vì rerun.
8. Chỉ commit cursor sau khi rerun thành công hoàn toàn.

## 9. Backfill procedure

1. Cấp `BACKFILL_START` và `BACKFILL_END` offset-aware ISO; runner pin normalized bounds trong `request.backfill_window`. Ghi reason/operator vào incident notes khi cần, không giả định có audit columns tương ứng.
2. Xác nhận range có giới hạn và không vô tình chọn toàn bộ source history.
3. Sử dụng `run_type=backfill`, control/delivery streams riêng và unique object run IDs.
4. Đọc range cụ thể mà không thay đổi scheduled extraction bounds.
5. Áp dụng cùng validation, merge, dbt và reconciliation gates như scheduled run.
6. Giữ scheduled cursor không đổi; backfill implementation không hỗ trợ promotion.
7. So sánh kết quả với full-reload fixture tương đương hoặc aggregate invariants.
8. Ghi affected row counts và đính kèm reconciliation report vào backfill run.

## 10. Operational evidence retained per run

Giữ lại hoặc tham chiếu các artifacts sau:

- Pipeline, task và dataset audit rows.
- Code version và configuration version.
- Cursor before, lower bound, upper cursor, candidate và committed cursor after.
- Bronze và Silver manifests kèm checksums.
- Inserted, updated, unchanged và stale-ignored counts.
- Rejected, deferred, warning và duplicate counts.
- Data-quality rule results và quarantine state transitions.
- dbt build/test results.
- Reconciliation status và mismatch details.
- Failure logs và recovery notes khi áp dụng.

## 11. Command interface

Các source/development commands sau đã được triển khai; `make help` là interface đầy đủ:

```bash
make sync
make lint
make test-unit
make build
make verify-raw
make profile-source
make validate-source-contract
make test-raw-tools
make test
make infrastructure-up
make migrate
make seed-source
make verify-source-schema
make verify-source
make test-source-integration
make generate-source-batches
make validate-source-batch
make test-source-simulator-integration
make infrastructure-status
make infrastructure-logs
make infrastructure-down
```

Chạy commands từ repository root. `make sync` cài package `src/olist_platform` cùng development tools theo `uv.lock`; `make test` bao gồm Ruff lint/format, contract validation, unit tests và infrastructure configuration checks, không start containers. Các CLI `olist-verify-raw`, `olist-profile-source` và `olist-validate-source-contract` dùng working-directory-relative defaults; khi gọi từ thư mục khác, truyền explicit paths cho config, raw và output.

`make verify-raw` kiểm tra filenames, sizes, SHA-256 checksums và headers. `make profile-source` chạy verification trước, sau đó sinh `docs/data_contracts/00_source_profile.md` và machine-readable report `build/reports/source_profile.json` đã được Git ignore. `make seed-source` start PostgreSQL, apply migrations và idempotently load snapshot. `make verify-source` đối chiếu live schema với contract trước khi kiểm tra source counts/audit bằng reader role; catalog được lưu ở `build/reports/source_schema.json`.

`make test-source-integration` dùng full verified raw snapshot cùng một disposable PostgreSQL container riêng, không dùng local Compose database. Tests so sánh source data/metadata fingerprints, audit và sequence state trước/sau rerun, bảo vệ source updates, chèn lỗi ở late table/audit insert và kiểm tra rollback cùng retry. Container và anonymous volume được cleanup khi suite kết thúc. Sequence gaps sau failed `setval` được chấp nhận vì PostgreSQL sequences không transactional; source rows và audit vẫn phải rollback.

Simulator commands và batch format được mô tả trong [Source Simulator Contract](../data_contracts/03_source_simulator.md). `make apply-source-batch SOURCE_BATCH_DIR=<directory>` dùng `SOURCE_SIMULATOR_DSN` hoặc standard `PG*` process environment, apply atomic source changes cùng audit, và trả `NOOP` khi replay cùng batch/hash. Predecessor batches phải commit trước; batch ID conflict làm fail. Simulator dùng cùng advisory transaction lock với seed. `make test-source-simulator-integration` kiểm tra protocol trên small fixture, không yêu cầu raw CSV. Applied migrations `001` không được sửa; `002` thêm batch audit.

Core runners và operational commands hiện tại:

```bash
make dataset
make dataset-incremental
make pipeline-health
make pipeline-dashboard
make sync-airflow test-airflow
make airflow-local
make metabase-up
make metabase-setup
make metabase-down
```

Health/dashboard và standalone dbt dùng exported `PG*`; Compose `.env` không tự
được export. Metabase setup dùng local configuration, cần fresh successful core
publication gate. Không bật BI reader bằng admin/source credentials.

Backfill interface đã triển khai (source update timestamps, end exclusive):

```bash
make dataset-backfill BACKFILL_START=2020-01-01T00:00:00Z BACKFILL_END=2020-02-01T00:00:00Z
```

## 12. Definition of a successful run

Một run chỉ thành công khi toàn bộ điều kiện sau là đúng:

- Các services và migrations bắt buộc đã pass preflight checks.
- Bronze và Silver outputs có manifests hợp lệ cùng `_SUCCESS` markers.
- Schema và row-level validation đã hoàn tất theo contract version mong đợi.
- Warehouse merge đã commit atomically.
- dbt models và tests đã pass.
- Mọi reconciliation equation khớp chính xác.
- Rejected/deferred/expired records và warning rates được audit đúng policy; health thresholds là read-only operational evaluation, không tự sửa pipeline status hoặc queue.
- Cursor compare-and-swap thành công cho scheduled run, hoặc cursor giữ nguyên với isolated backfill.
- Audit tables chứa final counters, timestamps và status.

## 13. Implemented orders Bronze operations

Orders CLI slice và end-to-end core runner đều đã hoạt động; phần này mô tả standalone orders ingestion. Sau source seed và migration `003`, chạy `make bronze-up`, export `BRONZE_SOURCE_DSN` cùng AWS credentials, rồi `make init-bronze` và `make ingest-orders BRONZE_RUN_ID=<stable_id>`.

Nếu run lỗi, gọi lại cùng run ID và config. `PREPARED` state giữ snapshot bytes/bounds, kể cả khi source đổi. Không tạo run mới để bỏ qua unfinished run, không sửa spool/control state. Marker đã có nhưng run còn `PREPARED` là recovery case hợp lệ: resume để hoàn tất checkpoint. Nếu checkpoint CAS conflict hoặc immutable-object conflict, giữ nguyên evidence để điều tra.

Dùng `olist-bronze-orders verify --prefix '<manifest.prefix>'` trước khi đọc batch. Checkpoint `audit.bronze_orders_control` chỉ xác nhận Bronze delivery; chưa thể dùng để kết luận warehouse hoặc toàn pipeline thành công. Xem [contract và commands](../data_contracts/04_orders_bronze.md). `make test-bronze-integration` kiểm chứng recovery với disposable services và không cần raw CSV.

## 14. Implemented orders Silver operations

Sau verified Bronze publication, dùng Java 17, `make sync-processing` và `make process-orders BRONZE_PREFIX='<manifest.prefix>' SILVER_RUN_ID=<stable_id>`. Job đọc immutable input từ MinIO, không cần source database connection. Xác minh bằng `olist-silver-orders verify --prefix 'silver/orders/run_id=<id>'`.

Retry dùng cùng run ID/input/config. Nếu chưa có sealed manifest, retry có thể compute một attempt mới; partial objects không được consumer đọc. Nếu manifest đã có, resume dùng lại sealed Parquet và hoàn tất marker; replay sau success trả `NOOP`. Không xóa/sửa request hoặc manifest để reuse ID với input khác. Customer FK/deferred routing đã có ở mục 15; persisted deferred lifecycle đã có ở mục 19; local watermark promotion đã có ở mục 20; staging đã có ở mục 17. Xem [Silver contract](../data_contracts/05_orders_silver.md); chạy `make test-processing` và `make test-silver-integration` sau thay đổi rules/publication.

## 15. Implemented customer snapshots and FK routing

Chạy migration `004`, `make ingest-customers BRONZE_RUN_ID=<id>` và `make process-customers BRONZE_PREFIX=<customer Bronze prefix> SILVER_RUN_ID=<id>`. Đây là full snapshot; không dùng customer delta làm toàn bộ parent key set. Khi process orders, truyền `CUSTOMER_SNAPSHOT=<customer Silver prefix>` để bật v2 FK/deferred routing.

Nếu parent tới muộn, tạo full customer snapshot mới và reprocess **original orders Bronze prefix** bằng **Silver run ID mới**. Không thay dependency của run đã seal và không sửa old deferred outputs. Parent hash/contract/counts phải được verify; locally invalid orders vẫn reject trước missing-parent routing. Đây là explicit reprocessing; persisted deferred queue/expiry đã có ở mục 19. Xem [customer contract](../data_contracts/06_customer_snapshots.md); chạy `make test-customer-integration` sau thay đổi.

## 16. Implemented order items and payments

Sau migration `005`, dùng `make ingest-order-items` / `make ingest-order-payments` với stable `BRONZE_RUN_ID`. Tạo full orders parent bằng `make ingest-orders BRONZE_RUN_ID=<id> BRONZE_FULL_SNAPSHOT=1`, rồi process orders. Default incremental orders không đủ làm toàn bộ parent set.

Truyền `ORDER_SNAPSHOT=<full orders Silver prefix>` vào `make process-order-items` / `make process-order-payments`, cùng `BRONZE_PREFIX` và `SILVER_RUN_ID`. Runner `make dataset` tự thực hiện tám datasets theo dependency order. Thiếu order vào deferred; locally invalid records vẫn reject. Scheduled core yêu cầu item v2 với ba full parents. Standalone item v2 bật bằng `PRODUCT_SNAPSHOT` và `SELLER_SNAPSHOT`; retry cần đủ ba parents, theo [contract 17](../data_contracts/17_item_dependency_snapshots.md).

Retry giữ nguyên run/input/config. Parent mới cần Silver ID mới và original child Bronze; không sửa old outputs. Nếu có pending orders run của runner cũ dùng incremental mode, resume bằng config cũ trước khi chuyển full mode. Xem [contract](../data_contracts/07_order_children.md); chạy `make test-order-children-integration` sau thay đổi.

## 17. Implemented PostgreSQL staging loads

Apply migration `006`; dùng Java 17, `WAREHOUSE_DSN` hoặc `PG*`, cùng S3 credentials như Bronze. Chạy `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> WAREHOUSE_RUN_ID=<id>`. `make dataset` tự load sau từng Silver dataset và lưu `*_warehouse.json` cạnh reports Bronze/Silver.

Đối chiếu `audit.warehouse_load_runs`: load candidates bằng Silver valid; inserted + updated + unchanged + stale ignored phải bằng candidates. Source sequence quyết định version, không dùng timestamp. Same-version khác hash là integrity failure; không sửa target để ép pass. Retry cùng input/run ID sau lỗi sẽ rollback/replay an toàn; nếu đã commit thì trả `NOOP` với counters cũ. Parent snapshot khác cần Silver ID mới và warehouse load ID mới.

Data/audit commit cùng transaction theo từng dataset; tám staging loads không phải một transaction chung. dbt và final watermark gate ở mục 18/20; incremental/backfill và Airflow wrappers ở mục 21/22 cùng phần Airflow execution. Run ID conflict, checksum/schema mismatch hoặc conflict version phải giữ evidence để điều tra. Xem [staging contract](../data_contracts/08_warehouse_staging.md); chạy cả hai warehouse integration suites sau thay đổi.

## 18. Implemented dbt analytics

Sau migration `007`, dùng `make sync-analytics`, export `PG*`, `DBT_ENV_SECRET_PASSWORD` và `DBT_READER_USER`, rồi `make dbt-build`. Runner `make dataset` tự chạy sau tám staging loads; build/test failure giữ pending run để retry cùng IDs. Build artifacts nằm tại `build/reports/dataset/<run_id>_dbt`.

Chỉ coi analytics build đạt khi `run_results.json` không có failed/error/skipped nodes và command exit code bằng zero. Kiểm tra `orders_without_items`/`orders_without_payments` khi đọc GMV/AOV. Không so sánh GMV trực tiếp với gross paid amount như hai giá trị phải bằng nhau. AOV NULL khi không có eligible positive-payment orders.

Models hiện là views, không phải atomic published snapshot; failed build không rollback mọi views. Tránh concurrent source/staging writers khi cần consistent analytics. dbt standalone không promote watermark; local final gate ở mục 20 mới thực hiện việc đó. Xem [analytics contract](../data_contracts/09_analytics_models.md); chạy `make test-dbt-integration` sau thay đổi grains, joins hoặc metrics.

## 19. Implemented deferred lifecycle

Sau migration `008`, runner tự register và resolve frozen missing-parent records sau dependent Silver/staging loads, trước dbt. Standalone: `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>` và `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<new id>`.

Mỗi pass tối đa 1.000 pending rows/bảng; kiểm tra `remaining_pending_rows`, routing counters và `audit.deferred_attempts`. Default expiry là ba committed attempts hoặc 168 giờ từ first registration. Replay cùng ID không tăng attempts. Parent mới hoặc pass mới cần ID mới; không reset terminal state hoặc sửa frozen payload/limits để ép pass.

Resolution data/queue/audit atomic. Terminal rejects ghi vào resolution audit, giữ original Silver bất biến. Retry inputs không phải source growth; duplicates được ghi riêng trước merge. Xem [contract](../data_contracts/10_deferred_lifecycle.md). Chạy hai deferred integration suites sau thay đổi; chưa có background scheduler; local watermark promotion thuộc final gate ở mục 20.

## 20. Implemented local pipeline completion

Sau migration `009`, `make dataset` ghi `pipeline_runs`, task attempts và per-dataset reconciliation. Local runner dùng freeze tám full snapshots và audit 34 tasks và giữ file lock. Khi thiếu Java, chỉ hoàn tất Bronze, báo PARTIAL và giữ pending ID; không đánh dấu pipeline success.

Final coordinator verify remote markers/hashes cùng server-side receipts, giữ staging SHARE locks, chạy fresh dbt build và đối soát exact counters. Chỉ sau mọi gates đạt mới CAS cả bốn controls cùng dataset audit và run SUCCESS trong một transaction. Một lỗi, missing receipt hoặc CAS conflict giữ controls cũ. Source scan counts của full snapshot không được gọi là new-source growth.

Sau failure/crash, giữ pending ID và code/config cũ rồi gọi lại `make dataset`. Task attempt cũ được giữ, resume tạo attempt mới; data-plane và deferred retries replay cùng IDs. Không chạy cùng run từ nhiều hosts/checkouts. Local namespace là manual `local_core_v1`; incremental/backfill ở mục 21–22; atomic analytics publication chưa triển khai. Xem [pipeline contract](../data_contracts/11_pipeline_control.md).

### Recovery verification

Chạy `make test-recovery-integration` với Java 17 sau thay đổi orchestration/gates.
Suite kiểm tra lỗi sau Bronze/Silver/warehouse commit và sau dbt/CAS trước final
transaction commit cho incremental/backfill. Recovery phải dùng cùng run ID,
frozen bytes và dependency hashes; warehouse/deferred receipts không được ghi
lặp, retry attempts không tăng khi replay. Chi tiết/bằng chứng tại
[pipeline contract](../data_contracts/11_pipeline_control.md#failure-recovery-verification).

Failure sau load không rollback staging đã commit. Sau lỗi gọi lại đúng command
với mode/bounds/code/config cũ; đọc failed task và counters trước khi đổi gì khác.
Tests dùng controlled exceptions, chưa thay thế OS-kill/network-failure drills.

### Operational health checks

Sau khi export `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` cho reader
role, chạy `make pipeline-health`. CLI không tự export Compose `.env`. Để lưu
JSON thuần: `uv run --locked olist-pipeline-health > build/reports/pipeline_health.json`.
Thư mục output phải tồn tại trước khi redirect.

Ngưỡng: `config/pipeline_health.json`; override bằng
`make pipeline-health PIPELINE_HEALTH_CONFIG=<path>`. CLI exit `0` healthy, `1`
unhealthy, `2` kiểm tra không thực hiện được; Make trả nonzero cho cả hai lỗi.
Với CLI exit `1`, đọc `issues` cùng
actual metrics: resume failed run bằng code/config cũ; kiểm tra nguồn reject/
warning ở immutable Silver; xử lý pending bằng resolver với run ID mới khi
thực hiện pass mới. Report không sửa queue hay cursors. Không chạy lại pipeline
chỉ để xóa một issue khi chưa hiểu nguyên nhân.

Freshness đo từ lần SUCCESS cuối; runtime gồm thời gian chờ resume. Defaults là
operational limits local, chưa phải benchmark/SLA. Xem [contract](../data_contracts/11_pipeline_control.md#operational-health)
trước khi đổi ngưỡng. Command chưa được tự lập lịch hoặc gửi thông báo.

Chạy `make pipeline-dashboard` để tạo HTML tại
`build/reports/pipeline_dashboard.html`, rồi mở bằng browser. `PG*`/thresholds như
health CLI; `PIPELINE_DASHBOARD_OUTPUT=<path>` đổi vị trí. HTML vẫn được ghi khi
unhealthy. File là snapshot tĩnh: kiểm tra timestamp và command exit status;
nếu database/ghi file lỗi, file cũ có thể vẫn tồn tại. Không dùng file cũ làm bằng
chứng cho lần kiểm tra vừa thất bại.

### Airflow execution and recovery

`make sync-airflow test-airflow`, sau đó `make airflow-local` chạy standalone local
với metadata riêng trong `build/airflow/`. DAG `olist_core_full_snapshot` paused
mặc định; trigger thủ công và đối chiếu PostgreSQL audit trước khi unpause daily UTC.
Mỗi DAG có một task gọi runner; PostgreSQL audit ghi 34 full hoặc 38 incremental/backfill tasks, không tách thành stage-level Airflow tasks.

Thiếu CSV/Java fail; retry hai lần cách nhau một phút, tối đa một active DAG run.
Task failure sau audit begin ghi failure hiện có; crash/kill có thể để RUNNING
đến khi resume. Retry/Clear task trong **cùng DAG run** giữ identity. Không tạo run
mới, không dùng `dags test` để resume, không xóa pending ID và không sửa code/config
đã pin. Nếu pending thuộc manual run, hoàn tất manual run trước khi trigger DAG.

Local file lock và PostgreSQL guard vẫn áp dụng; không chạy cùng run từ nhiều
checkout. UI/API bind loopback, SQLite metadata và standalone chỉ dành cho local.
Không coi logical date là extraction window; incremental/backfill dùng explicit mode/bounds theo mục 21–22. Chi tiết [local Airflow](../../infrastructure/README.md#local-airflow).

### Recovery after a local OOM

OOM có thể kill cả VS Code application scope cùng Airflow/runner trong integrated
terminal. Audit còn `RUNNING` không chứng minh process vẫn sống: kiểm tra PID,
heartbeat và API health trước khi Retry/Clear **cùng task trong original DAG run**.
Giữ pending ID, frozen inputs và pinned code/config; không trigger replacement run.

Trên máy có `/tmp` là tmpfs, Silver download spool có thể chiếm thêm nhiều GiB RAM.
Kiểm tra filesystem và đặt Python/Spark temporary files trên ổ đĩa đủ dung lượng:

```bash
df -h /tmp build
mkdir -p build/tmp/spark
env TMPDIR="$PWD/build/tmp/spark" SPARK_LOCAL_DIRS="$PWD/build/tmp/spark" make airflow-local
```

Chạy trong terminal ngoài VS Code hoặc service riêng để tách process lifetime.
Xác nhận `build/tmp/spark` nằm trên disk, không phải tmpfs. Các biến này đổi vị trí
file tạm, không thay input/routing rules; vẫn phải giữ đúng original request/hash.
Chỉ dọn local cache của đúng run khi đã xác minh không còn process sử dụng; không
xóa Bronze/Silver published objects, prepared pages hoặc audit để ép resume.

Theo dõi RAM khả dụng và peak của toàn process group, gồm JVM; giảm workloads chạy
đồng thời trước khi retry. Health runtime tính từ original run start, gồm cả thời
gian downtime: một recovery SUCCESS vẫn có thể báo `pipeline.runtime_exceeded`.
Giữ nguyên thresholds và ghi riêng completion status cùng operational alert.

## 21. Incremental execution and recovery

Chạy `make dataset-incremental` với Java 17 và raw/Compose config hiện có. Namespace
`incremental_core_v1` bootstrap riêng lần đầu, không lấy cursor từ `local_core_v1`
hoặc Bronze checkpoints. Sau đó mỗi window bắt đầu từ committed pipeline source
timestamp trừ 48 giờ inclusive, có fixed upper và durable bytes.

`source.freeze` chốt bốn deltas cùng sáu full inputs trong một transaction;
38 audited tasks xử lý/publish/load/resolve trước fresh dbt/CAS. Customers/orders full parents chỉ phục vụ FK; products/sellers/geolocation còn được full load Type 1; không coi chi phí đọc parent là source growth hay delta processing.
Xem [contract](../data_contracts/12_incremental_pipeline.md) để đọc counters.

Sau lỗi, gọi lại **cùng command/mode**; giữ pending ID và code/config/contract.
Không sửa source pages, cursor/hash để ép chạy. Scoped publication đòi đã freeze.
Airflow dùng DAG `olist_core_incremental`, paused mặc định; Retry/Clear cùng DAG
run giữ context, không dùng run mới hoặc logical date khác để thay batch.
Chỉ bật schedule của một mode trên local checkout; guards chặn overlap nhưng run
cạnh tranh có thể fail. Health/dashboard thêm `PIPELINE_NAMESPACE=incremental_core_v1`;
queue metrics vẫn là backlog chung.

Kiểm tra source_after_cursor/lookback/bootstrap/retry/auxiliary-FK counters và
reconciliation status, rồi xác minh cả bốn controls chỉ tiến sau success. Updates
ngoài SLA cần đối chiếu và repair bằng isolated backfill ở mục 22.
Full FK snapshot không chứng minh row ngoài window đã được load vào warehouse.

## 22. Isolated backfill and recovery

Chạy `make dataset-backfill BACKFILL_START=<ISO-with-offset> BACKFILL_END=<ISO-with-offset>`.
Window dùng `source_updated_at`, start inclusive/end exclusive; không dùng purchase date.
Migration `011` và namespace `backfill_core_v1` tách controls/delivery khỏi scheduled modes.
Bốn deltas và sáu full inputs được freeze cùng transaction, 38 tasks và dbt gates.

Sau lỗi giữ pending ID/bounds/code/config, gọi lại cùng command; không đổi khoảng thời gian.
Kiểm tra `source_backfill_rows`, `full_parent_source_rows`, retry/remaining-pending counters,
load outcomes, reconciliation và xác minh full/incremental controls không thay đổi.
Backfill resolver chỉ retry/expire queue có original Silver run ID tương ứng; queue run
khác giữ nguyên. Staging dùng chung, newer source sequence thắng; failed final gate
không rollback dataset loads đã commit. Không sửa watermark/hash để ép success.

Airflow: trigger paused/manual-only `olist_core_backfill` với JSON conf `start`/`end`
(offset-aware ISO), Retry/Clear cùng DAG run. Không dùng logical date làm bounds.
Health/dashboard thêm `PIPELINE_NAMESPACE=backfill_core_v1`; backlog vẫn là global.
Xem [backfill contract](../data_contracts/13_backfill_pipeline.md) và chạy
`make test-backfill-integration` sau thay đổi. Mutable source không phục hồi historical
versions đã overwrite; analytics views chưa atomic publish.

## 23. Local business dashboard

Sau pipeline SUCCESS, chạy `make metabase-up`; bootstrap tự tạo local private
credentials và managed dashboard. Đọc login ở `build/metabase/credentials.json`
(0600), URL ở `dashboard.json`. Không commit/share credentials. `make metabase-setup`
reconcile definition; `make metabase-down` stop và giữ named volume/H2 metadata.

Date filters inclusive theo local purchase date. Kiểm tra publication-status card
và completeness trước khi dùng metrics; non-success/newer audited load chặn cards.
AOV toàn kỳ dùng tổng paid amount / paid orders. Không chạy pipeline chỉ để xóa
status cảnh báo khi chưa xác định nguyên nhân. Live views chưa atomic giữa các cards;
H2 metadata dành cho local demo. Xem [contract](../data_contracts/14_business_dashboard.md).

## 24. Item analytics

`make dataset` và các bounded modes tự build/test `fct_order_items`,
`mart_product_daily`, `mart_seller_daily` sau staging. Standalone modeling dùng
`make dbt-build` với PG/dbt environment như mục 18; chạy `make test-dbt-integration`
để kiểm tra grains, money, missing dimensions và corruption gates.

Fact giữ một `(order_id, order_item_id)` và current product/seller keys. Marts dùng
local purchase date, eligible item GMV/freight; không phân bổ payments/AOV/payout.
Order counts distinct trong từng group, không cộng qua products/sellers. Missing
parent làm build fail, không drop rows để ép success. Profile local dùng một dbt
thread để tránh deadlock khi replace views có dependencies chung.

Metabase đọc product/seller aggregate marts qua SELECT-only grants; top-10
tables dùng cùng date filters và publication gate. Không cấp quyền facts/dimensions. Đối chiếu status SUCCESS và source-based reconciliation trước khi
sử dụng. Xem [metric contract](../data_contracts/09_analytics_models.md#item-facts-and-productseller-metrics).

## 25. Payment analytics

`make dataset` và bounded modes tự build/test `fct_payments` và
`mart_payment_type_daily`; standalone dùng `make dbt-build` với environment ở mục 18.
`make test-dbt-integration` kiểm tra mixed types, nhiều entries/parent items,
installments, zero amount, canceled/unavailable, empty input và corruption gates.

Giữ `(order_id, payment_sequential)` làm key; `payment_value` đã là amount của
entry, không nhân với installments hoặc join items. Type/date order counts không
cộng được qua types, gồm zero-paid orders và không thay AOV denominator. Missing
parent làm build fail; giữ pending run để resume. Metabase đọc aggregate type mart qua SELECT-only grants; không cấp quyền payment fact.
Xem [payment metrics](../data_contracts/09_analytics_models.md#payment-facts-and-type-metrics).

## 26. Aggregate BI permissions and cards

Chạy `make migrate` rồi `make metabase-setup` khi latest pipeline SUCCESS và marts
đã tồn tại. Migration `017` cấp SELECT cho ba aggregate marts mới; dbt grants giữ
quyền qua rebuild. Trên database mới, chạy pipeline để tạo marts trước setup.
Giữ SELECT đúng sáu business models hiện có, gồm `fct_orders`, cùng status view; không cấp source/staging/dimension hoặc các fact khác, không tạo datasource admin.

Dashboard có 13 saved cards và một policy text: rankings top 10 products/sellers,
payment-type amounts/trends dùng cùng inclusive date filters và publication gate.
Chạy setup lại giữ IDs, không duplicate cards. Kiểm tra `build/metabase/dashboard.json`
và [BI contract](../data_contracts/14_business_dashboard.md); không commit credentials.

## 27. Delivery cohorts

Pipeline tự build/test `fct_delivery` và `mart_delivery_daily`; standalone dùng
`make dbt-build` với environment ở mục 18. Missing/invalid milestones không làm
mất order row; kiểm tra delivered-missing và chronology counts trước duration/rate.
Derived chronology policy không sửa Silver routing hoặc persisted source payloads.

Lateness so local Sao Paulo actual/estimated dates; durations lấy UTC elapsed.
Observation cần status delivered + actual + chronology hợp lệ. Transit cần carrier.
Không dùng wall clock để đo overdue; không điền NULL bằng zero hoặc average daily
rates/means để lấy whole-period metrics. Query fact hoặc weight bằng observation
counts. Hai models không được thêm vào BI reader; xem [delivery contract](../data_contracts/09_analytics_models.md#delivery-facts-and-cohort-metrics).

## 28. Calendar and date keys

Pipeline tự build/test `dim_date` và date keys trong bốn facts. Standalone dùng
`make dbt-build` sau khi export PG/dbt environment ở mục 18; chạy
`make test-dbt-integration` để kiểm tra dates/ISO/grain và failure gates.
Join purchase keys hoặc estimated/actual delivery role keys với `dim_date.date_key`;
ISO grouping cần `iso_year, iso_week`, không dùng calendar year cho ISO week.

Dimension bao phủ local observed dates, gồm gaps; rỗng khi inputs rỗng. NULL actual
key nghĩa là thiếu timestamp, không đồng nghĩa delivery chưa hợp lệ. Giữ nguyên
local date columns cho date filters hiện có; không mở thêm quyền BI/dimension.
Xem [calendar contract](../data_contracts/09_analytics_models.md#calendar-dimension-and-fact-date-roles).

## 29. Geolocation snapshots

Chạy `make migrate`, `make ingest-geolocation BRONZE_RUN_ID=<id>`,
`make process-geolocation BRONZE_PREFIX=<prefix> SILVER_RUN_ID=<id>` và
`make load-warehouse SOURCE_TABLE=geolocation SILVER_PREFIX=<prefix> WAREHOUSE_RUN_ID=<id>`.
Dùng PG/AWS/dbt environment như các snapshots khác; tiếp theo `make dbt-build`.
`make test-geolocation-integration` kiểm tra retries/merge; dbt integration kiểm
tra medians/modes, grain và corruption. Resume các IDs cũ sau mất response.

Kiểm tra rejected/warning counts trước khi dùng location dimension. Một ZIP có
nhiều observations; chỉ join dimension đã aggregate, không join source geolocation
vào facts. Customer/seller dimensions có nullable location_key theo ZIP; unmatched ZIP giữ
NULL và source location giữ nguyên. Core freeze/receipts gồm tám datasets;
full có 34 tasks, incremental/backfill có 38 tasks, vẫn chỉ bốn completion cursors. Standalone load làm
BI unavailable cho tới core SUCCESS mới; không cập nhật status thủ công.
Dataset runner mặc định Spark heap 2 GiB, giữ explicit runtime settings.
Nếu standalone geolocation gặp Java heap OOM, kiểm tra RAM rồi export
`PYSPARK_SUBMIT_ARGS='--driver-memory 2g pyspark-shell'` và chạy lại command cùng IDs.
Xem [geolocation contract](../data_contracts/18_geolocation_snapshots.md).

Khi một manual local run đã FAILED và bug fix làm đổi code hash, giữ audit/
receipts cũ; request của ID đó vẫn immutable. Xác nhận không còn RUNNING và
cursors bằng control_before. Nếu còn owned Bronze PREPARED, publish đúng frozen
bytes với original owner ID/mode/page size qua Bronze CLI trước khi đổi pending ID;
không xóa prepared pages hoặc dùng source live để thay bytes. Đây chỉ commit
Bronze delivery, không đổi pipeline FAILED hoặc completion cursors. Archive
`build/reports/dataset/pending-run.txt` cùng failed ID/evidence, rồi bắt đầu manual
ID mới để tạo đủ receipts/gate mới. Không
đổi request, copy proofs hoặc gán SUCCESS thủ công. Retry không đổi code vẫn dùng
pending ID như cũ; Airflow retries tiếp tục tuân theo original DAG/run context.

## 30. Review snapshots

Chạy `make migrate`, `make ingest-order-reviews BRONZE_RUN_ID=<id>` và
`make process-order-reviews BRONZE_PREFIX=<prefix> SILVER_RUN_ID=<id> ORDER_SNAPSHOT=<full orders prefix>`.
Load staging bằng `SOURCE_TABLE=order_reviews`, rồi register/resolve deferred với
cùng generic commands. Resume cùng IDs, dùng new resolution ID khi parent đổi.
`make test-review-integration` kiểm tra frozen retry, dirty routing, composite keys,
expiry và atomic resolution. Core runner tự freeze/publish/load/register/resolve
reviews full-only, pin same-run full orders và bắt buộc receipts; không có review
completion cursor hoặc BI grant mới. Bounded reviews Silver dùng hashed
parent ID; backfill retry lọc theo ID đó, còn receipts dùng owner ID. Health/dashboard
gồm review backlog và `review_snapshot_rows`. Standalone load làm publication
unavailable cho tới fresh core SUCCESS.
Xem [review contract](../data_contracts/19_review_snapshots.md).

Review analytics được build/test trong cùng core gate: `fct_reviews` giữ composite
grain và UTC/local creation date; `mart_review_daily` theo review creation cohorts.
Không join reviews vào payment/item amounts. Whole-period mean/rates phải gộp sums/
counts trước khi chia; distinct orders không additive qua ngày. `dim_date` bao gồm
review creation dates và không yêu cầu bounded order staging phải có full parents.
Comment text không được đưa vào fact/BI. Sau sửa models chạy `make test-dbt-integration`
và các core gate/backfill/recovery suites; không đổi code trong lúc run đang pending.

Dimension policy giữ stable business-key hashes và không tạo unknown members.
Khi `dimension_key_reconciliation` fail, đối chiếu current staging, exact MD5 key và
customer representative theo source_sequence/customer_id; không chèn dummy row
hoặc sửa source để bypass. Optional location NULL khác required parent missing.
Chạy `make test-dbt-integration` sau thay đổi keys/membership, gồm cả empty staging
và members chưa có fact. Xem [key policy](../data_contracts/09_analytics_models.md#dimension-keys-and-missing-members).

Customer marts build/test trong cùng core dbt gate. Customer value dùng shopper
identity và order-grain sums; first/repeat dùng observed eligible history, UTC
purchase instant/order_id tie-break và local purchase cohorts. Kiểm tra counts/
completeness trước khi dùng AOV/repeat rate; late earlier orders/status updates
có thể thay đổi ngày cũ. Status mart không phải transition log. Không thêm BI grants.
Chạy `make test-dbt-integration` và backfill equivalence sau sửa các marts này.
Xem [customer metric policy](../data_contracts/09_analytics_models.md#customer-value-and-repeat-purchase).

Sau successful dbt build, dùng `make dbt-docs` với cùng environment để generate
catalog/manifest/viewer tại `build/reports/dbt-docs/`. Command không sửa staging,
build relations hoặc promote watermark; phải kiểm tra build/test status riêng.
Khi output columns đổi, bổ sung descriptions trong `dbt/models/schema.yml`, cập nhật
README lineage nếu refs đổi, chạy docs catalog integration và giữ artifacts ignored.
Xem [docs workflow](../data_contracts/09_analytics_models.md#documentation-catalog-and-lineage).

Khi `fact_reconciliation` fail dù generic FK tests PASS, đối chiếu cả source
customer/order identity, current status/eligibility và purchase UTC/local-date
attributes, không chỉ amounts/counts. Key tồn tại vẫn có thể là association sai.
BRL sums giữ NUMERIC, optional delivery dates/review full-parent scope giữ policy
hiện có; xem [fact acceptance](../data_contracts/09_analytics_models.md#fact-integrity-acceptance).

## 31. Change control

Update runbook này trong cùng pull request khi bất kỳ nội dung sau thay đổi:

- Command, path, service hoặc schema name.
- Cursor, lookback, retry hoặc backfill semantics.
- Data-quality routing hoặc thresholds.
- Reconciliation equations.
- Warehouse conflict-resolution behavior.
- Operational evidence hoặc recovery procedure bắt buộc.

Giữ runbook, [architecture/ERDs](../22_architecture_and_data_model.md), README và [acceptance evidence](../23_project_acceptance.md) đồng bộ trong cùng change. Không sửa pinned code/config khi còn pending run; giữ evidence và xử lý unfinished run trước khi nâng revision. Checklist phải phân biệt fixture tests, historical full-data run, live demo và release, không đánh dấu hoàn tất khi chưa có evidence.
