# Batch Data Processing Runbook

| Field | Value |
| --- | --- |
| System | Olist E-commerce Data Platform |
| Owner | Data Engineering |
| Status | Active — core full/incremental/backfill and local Airflow wrappers |
| Last reviewed | 2026-10-22 |
| Processing mode | Batch |

Runbook này định nghĩa quy trình vận hành dự kiến để xử lý snapshot nguồn Olist qua Bronze, Silver, các model warehouse PostgreSQL và analytics marts. Tài liệu mô tả các execution gates, cách xử lý lỗi, rerun và backfill. Runbook phải được cập nhật khi implementation details hoặc operational commands thay đổi.

Tài liệu này là hướng dẫn vận hành bổ sung cho:

- [Kế hoạch triển khai pipeline](../../ecommerce_data_warehouse_pipeline_plan_v4.md)
- [Raw dataset manifest](../../data/raw/README.md)
- [Source contracts](../data_contracts/02_source_contracts.md)

PostgreSQL source, bốn core Bronze/Silver datasets, staging, dbt và deferred lifecycle đã được triển khai. Các mục 13–20 mô tả operations hiện có; local Airflow full/incremental/manual-backfill wrappers đã có; các commands đánh dấu **planned** vẫn là target interface.

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
Bounded incremental extraction
      │
      ▼
MinIO Bronze ── manifest + checksum + _SUCCESS
      │
      ▼
PySpark validation, normalization and routing
      │
      ├── Silver valid, including warning flags
      ├── Silver rejected
      └── Silver deferred
                │
                ▼
PostgreSQL raw_staging ── version-aware merge
                │
                ▼
dbt staging → intermediate → dimensions/facts → marts
                │
                ▼
Exact reconciliation → cursor commit → dashboards
```

## 3. Run types

| Run type | Purpose | Reads committed cursor | May advance scheduled cursor |
| --- | --- | --- | --- |
| `scheduled` | Xử lý incremental thông thường | Có | Có, sau khi mọi gate thành công |
| `manual` | Retry hoặc diagnostic run do operator kích hoạt | Có thể cấu hình | Chỉ khi được cấu hình rõ ràng |
| `backfill` | Xử lý lại một khoảng ngày lịch sử cụ thể | Không; sử dụng bounds được cung cấp | Mặc định không |

Task retries vẫn thuộc cùng một pipeline run và tái sử dụng run ID, lower bound cùng upper cursor của run đó.

## 4. Required run context

Mỗi run phải ghi lại các trường sau trước khi extraction bắt đầu:

```text
run_id
run_type
logical_interval_start
logical_interval_end
code_version
config_version
source_table
committed_cursor_before
extract_lower_bound
run_upper_cursor
lookback_interval
started_at
```

Đối với source simulator, cursor ordering là:

```text
(source_updated_at, source_sequence, <primary-key columns>)
```

`source_sequence` là source version có thẩm quyền để giải quyết merge conflict. Các primary-key columns bảo đảm pagination ordering có tính deterministic.

## 5. Preflight procedure

Trước khi bắt đầu scheduled hoặc backfill run:

1. Xác nhận PostgreSQL, MinIO và các Airflow services bắt buộc đều healthy.
2. Xác nhận configuration và code version mong đợi đã được deploy.
3. Xác nhận credentials của source, warehouse và object storage được cung cấp qua environment hoặc secret configuration, không nằm trong các file được commit.
4. Xác nhận không có scheduled run khác đang giữ pipeline lock. MVP sử dụng `max_active_runs=1` kết hợp cursor compare-and-swap protection.
5. Với lần load raw source đầu tiên, kiểm tra checksum của từng CSV theo [raw dataset manifest](../../data/raw/README.md).
6. Xác nhận database migrations đang ở version mong đợi.
7. Tạo record trong `audit.pipeline_runs` với status `RUNNING`.

Dừng trước extraction nếu bất kỳ preflight check nào thất bại.

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
4. Assert row counts, primary keys và foreign keys mong đợi.
5. Khởi tạo các trường deterministic `source_updated_at`, `source_sequence` và `source_operation` cho simulator.
6. Ghi source-seed manifest chứa row counts, checksums và load timestamp.

### 6.3. Capture bounded extraction parameters

Với mỗi source table:

1. Đọc committed cursor `W` và `cursor_version` của nó từ `audit.pipeline_control`.
2. Tính `extract_lower_bound = W.updated_at - lookback_interval`.
3. Mở một consistent source snapshot và chụp maximum cursor `U`.
4. Lưu `W`, lower bound và `U` vào `audit.pipeline_dataset_runs` trước khi đọc các page.
5. Sử dụng page cursor riêng cho keyset pagination.
6. Tái sử dụng cùng bounds cho mọi task retry trong run này.

Initial load sử dụng sentinel cursor nhỏ hơn mọi source timestamp. Source table rỗng giữ nguyên committed cursor hiện tại.

### 6.4. Extract and publish Bronze

1. Đọc các page theo thứ tự giữa fixed lower bound và upper cursor.
2. Giữ nguyên source business values, không trim, normalize hoặc cast business type.
3. Bổ sung physical record ID, logical record ID, run metadata, source metadata và schema version.
4. Ghi objects vào run-attempt path mà downstream tasks không thể consume.
5. Sinh `manifest.json` chứa bounds, row count, schema fingerprint, file list và checksums.
6. Chỉ publish batch sau khi mọi object cùng manifest đã hoàn tất.
7. Ghi `_SUCCESS` sau cùng.
8. Bỏ qua attempt paths không có `_SUCCESS`.

Gate bắt buộc:

```text
source_rows_extracted = bronze_rows_written
```

### 6.5. Validate and publish Silver

Xử lý parent datasets trước dependent datasets. Dependency order của full model thường là customers, products và sellers trước orders, sau đó đến order items, payments và reviews. Geolocation được xử lý độc lập trước location modeling.

Với mỗi Bronze batch:

1. Kiểm tra manifest checksums, row count và schema fingerprint.
2. Parse và cast values theo source contract.
3. Normalize strings, decimal values và timestamps theo BRL cùng timezone policies đã được tài liệu hóa.
4. Đánh giá mọi data-quality rule áp dụng được.
5. Gán chính xác một routing outcome theo precedence sau:

   ```text
   REJECT > DEFERRED > WARNING > INFO
   ```

6. Xem warning records là valid và gắn `warning_rule_ids`.
7. Deduplicate theo logical record identity và giữ source version cao nhất.
8. Ghi valid, rejected và deferred outputs thành immutable Silver batches.
9. Publish từng output với manifest và `_SUCCESS` marker.

### 6.6. Retry deferred records

1. Đọc các record `PENDING` đủ điều kiện từ shared quarantine table.
2. Giữ nguyên physical record ID ban đầu của từng record.
3. Resolve dependencies dựa trên valid parents của current run và parent state đã load trước đó.
4. Chuyển các record đã resolve sang valid output.
5. Giữ các record chưa resolve ở trạng thái `PENDING` và tăng attempt metadata.
6. Đánh dấu record là `EXPIRED` khi đạt `max_attempts` hoặc `max_deferred_age`.
7. Xem expired records là terminal rejects của resolution run.
8. Upsert quarantine state theo `record_uid + rule_id`; không append duplicate retry entries.

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

1. Chạy dbt dependencies và seeds khi cần.
2. Thực thi `dbt build` cho các staging, intermediate, dimension, fact và mart models được chọn trong run.
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

Sau khi toàn bộ source tables, dbt tests và reconciliations thành công:

1. Tính candidate cursor bằng `max(W, U)`.
2. Promote cursor candidates bằng compare-and-swap trên `cursor_version` trước đó.
3. Làm run fail khi có cursor version conflict; không ghi đè cursor của run khác.
4. Đánh dấu dataset runs và pipeline run là `SUCCESS`.
5. Refresh operational metrics và cho phép consumers nhìn thấy successful run.

## 7. Failure and recovery matrix

| Failure point | Expected state | Recovery action | Cursor action |
| --- | --- | --- | --- |
| Raw checksum or schema validation | Chưa load source | Khôi phục snapshot mong đợi hoặc chủ động tiếp nhận và profile lại snapshot mới | Không thay đổi |
| Source extraction | Bronze attempt có thể chưa hoàn chỉnh | Retry với cùng run bounds; bỏ qua các attempt chưa publish | Không thay đổi |
| Bronze manifest verification | Silver chưa được publish | Sửa code/configuration và replay immutable Bronze | Không thay đổi |
| Spark validation job | Silver attempt có thể chưa hoàn chỉnh | Retry từ Bronze đã publish | Không thay đổi |
| Warehouse merge | PostgreSQL transaction đã rollback | Sửa nguyên nhân và chạy lại cùng Silver input | Không thay đổi |
| dbt build or test | Staging có thể đã load; marts có thể chỉ hoàn tất một phần | Sửa model/test, chạy lại dbt rồi reconciliation | Không thay đổi |
| Reconciliation mismatch | Data vẫn audit được nhưng run đã failed | So sánh record IDs và manifests theo từng stage; không bỏ qua row loss | Không thay đổi |
| Cursor compare-and-swap conflict | Data có thể đã được load idempotently | Điều tra overlapping runs và chạy lại từ current committed cursor | Không ghi đè |

Không bao giờ recovery incident bằng cách chỉnh sửa thủ công raw CSV, xóa quarantine records, thay đổi audit counters hoặc advance cursor mà không có investigation được tài liệu hóa.

## 8. Idempotent rerun procedure

1. Giữ lại audit rows, manifests và logs của failed run.
2. Xác định gate thất bại sớm nhất.
3. Tái sử dụng immutable upstream artifact thành công gần nhất thay vì đọc lại source cũ khi không cần thiết.
4. Retry tasks trong cùng Airflow run khi phù hợp.
5. Kỳ vọng warehouse rerun phân loại phần lớn record đã load thành unchanged hoặc stale ignored.
6. Thực thi lại dbt tests và mọi reconciliation equation.
7. Xác nhận warehouse row counts không tăng chỉ vì rerun.
8. Chỉ commit cursor sau khi rerun thành công hoàn toàn.

## 9. Backfill procedure

1. Ghi `start_date`, `end_date`, reason và operator được yêu cầu vào audit run.
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

Các Phase 0 commands sau đã được triển khai và kiểm thử:

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

Các pipeline commands sau vẫn ở trạng thái **planned** và chỉ trở thành authoritative sau khi Make targets cùng automated tests tương ứng được triển khai:

```bash
make run-pipeline
```

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
- Quality rates nằm trong configured policy hoặc tạo ra data-quality failure state được phê duyệt rõ ràng.
- Cursor compare-and-swap thành công cho scheduled run, hoặc cursor giữ nguyên với isolated backfill.
- Audit tables chứa final counters, timestamps và status.

## 13. Implemented orders Bronze operations

Orders CLI slice đã hoạt động; phần end-to-end phía trên vẫn là planned workflow. Sau source seed và migration `003`, chạy `make bronze-up`, export `BRONZE_SOURCE_DSN` cùng AWS credentials, rồi `make init-bronze` và `make ingest-orders BRONZE_RUN_ID=<stable_id>`.

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

Truyền `ORDER_SNAPSHOT=<full orders Silver prefix>` vào `make process-order-items` / `make process-order-payments`, cùng `BRONZE_PREFIX` và `SILVER_RUN_ID`. Runner `make dataset` tự thực hiện sáu datasets theo dependency order. Thiếu order vào deferred; locally invalid records vẫn reject. Scheduled core yêu cầu item v2 với ba full parents. Standalone item v2 bật bằng `PRODUCT_SNAPSHOT` và `SELLER_SNAPSHOT`; retry cần đủ ba parents, theo [contract 17](../data_contracts/17_item_dependency_snapshots.md).

Retry giữ nguyên run/input/config. Parent mới cần Silver ID mới và original child Bronze; không sửa old outputs. Nếu có pending orders run của runner cũ dùng incremental mode, resume bằng config cũ trước khi chuyển full mode. Xem [contract](../data_contracts/07_order_children.md); chạy `make test-order-children-integration` sau thay đổi.

## 17. Implemented PostgreSQL staging loads

Apply migration `006`; dùng Java 17, `WAREHOUSE_DSN` hoặc `PG*`, cùng S3 credentials như Bronze. Chạy `make load-warehouse SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> WAREHOUSE_RUN_ID=<id>`. `make dataset` tự load sau từng Silver dataset và lưu `*_warehouse.json` cạnh reports Bronze/Silver.

Đối chiếu `audit.warehouse_load_runs`: load candidates bằng Silver valid; inserted + updated + unchanged + stale ignored phải bằng candidates. Source sequence quyết định version, không dùng timestamp. Same-version khác hash là integrity failure; không sửa target để ép pass. Retry cùng input/run ID sau lỗi sẽ rollback/replay an toàn; nếu đã commit thì trả `NOOP` với counters cũ. Parent snapshot khác cần Silver ID mới và warehouse load ID mới.

Data/audit commit cùng transaction, theo từng dataset; toàn bộ bốn datasets chưa atomic như một pipeline. dbt đã có ở mục 18; local watermark gate đã có ở mục 20; scheduled incremental orchestration chưa triển khai. Run ID conflict, checksum/schema mismatch hoặc conflict version phải giữ evidence để điều tra. Xem [staging contract](../data_contracts/08_warehouse_staging.md); chạy cả hai warehouse integration suites sau thay đổi.

## 18. Implemented dbt analytics

Sau migration `007`, dùng `make sync-analytics`, export `PG*`, `DBT_ENV_SECRET_PASSWORD` và `DBT_READER_USER`, rồi `make dbt-build`. Runner `make dataset` tự chạy sau sáu staging loads; build/test failure giữ pending run để retry cùng IDs. Build artifacts nằm tại `build/reports/dataset/<run_id>_dbt`.

Chỉ coi analytics build đạt khi `run_results.json` không có failed/error/skipped nodes và command exit code bằng zero. Kiểm tra `orders_without_items`/`orders_without_payments` khi đọc GMV/AOV. Không so sánh GMV trực tiếp với gross paid amount như hai giá trị phải bằng nhau. AOV NULL khi không có eligible positive-payment orders.

Models hiện là views, không phải atomic published snapshot; failed build không rollback mọi views. Tránh concurrent source/staging writers khi cần consistent analytics. dbt standalone không promote watermark; local final gate ở mục 20 mới thực hiện việc đó. Xem [analytics contract](../data_contracts/09_analytics_models.md); chạy `make test-dbt-integration` sau thay đổi grains, joins hoặc metrics.

## 19. Implemented deferred lifecycle

Sau migration `008`, runner tự register và resolve frozen missing-parent records sau dependent Silver/staging loads, trước dbt. Standalone: `make register-deferred SOURCE_TABLE=<table> SILVER_PREFIX=<prefix> DEFERRED_RUN_ID=<id>` và `make resolve-deferred SOURCE_TABLE=<table> PARENT_SNAPSHOT=<full prefix> DEFERRED_RUN_ID=<new id>`.

Mỗi pass tối đa 1.000 pending rows/bảng; kiểm tra `remaining_pending_rows`, routing counters và `audit.deferred_attempts`. Default expiry là ba committed attempts hoặc 168 giờ từ first registration. Replay cùng ID không tăng attempts. Parent mới hoặc pass mới cần ID mới; không reset terminal state hoặc sửa frozen payload/limits để ép pass.

Resolution data/queue/audit atomic. Terminal rejects ghi vào resolution audit, giữ original Silver bất biến. Retry inputs không phải source growth; duplicates được ghi riêng trước merge. Xem [contract](../data_contracts/10_deferred_lifecycle.md). Chạy hai deferred integration suites sau thay đổi; chưa có background scheduler; local watermark promotion thuộc final gate ở mục 20.

## 20. Implemented local pipeline completion

Sau migration `009`, `make dataset` ghi `pipeline_runs`, task attempts và per-dataset reconciliation. Local runner dùng freeze sáu full snapshots và audit 26 tasks và giữ file lock. Khi thiếu Java, chỉ hoàn tất Bronze, báo PARTIAL và giữ pending ID; không đánh dấu pipeline success.

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
Một task gọi runner, không tách 19 business stages thành Airflow tasks.

Thiếu CSV/Java fail; retry hai lần cách nhau một phút, tối đa một active DAG run.
Task failure sau audit begin ghi failure hiện có; crash/kill có thể để RUNNING
đến khi resume. Retry/Clear task trong **cùng DAG run** giữ identity. Không tạo run
mới, không dùng `dags test` để resume, không xóa pending ID và không sửa code/config
đã pin. Nếu pending thuộc manual run, hoàn tất manual run trước khi trigger DAG.

Local file lock và PostgreSQL guard vẫn áp dụng; không chạy cùng run từ nhiều
checkout. UI/API bind loopback, SQLite metadata và standalone chỉ dành cho local.
Không coi logical date là extraction window; incremental/backfill dùng explicit mode/bounds theo mục 21–22. Chi tiết [local Airflow](../../infrastructure/README.md#local-airflow).

## 21. Incremental execution and recovery

Chạy `make dataset-incremental` với Java 17 và raw/Compose config hiện có. Namespace
`incremental_core_v1` bootstrap riêng lần đầu, không lấy cursor từ `local_core_v1`
hoặc Bronze checkpoints. Sau đó mỗi window bắt đầu từ committed pipeline source
timestamp trừ 48 giờ inclusive, có fixed upper và durable bytes.

`source.freeze` chốt bốn deltas cùng bốn full FK inputs trong một transaction;
30 audited tasks xử lý/publish/load/resolve trước fresh dbt/CAS. Customers/orders full parents chỉ phục vụ FK; products/sellers còn được full load Type 1; không coi chi phí đọc parent là source growth hay delta processing.
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
Bốn deltas và bốn full FK inputs được freeze cùng transaction, 30 tasks và dbt gates.

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

Các models mới dành cho analytics reader hiện có; Metabase chưa được cấp quyền
hoặc thêm cards. Đối chiếu status SUCCESS và source-based reconciliation trước khi
sử dụng. Xem [metric contract](../data_contracts/09_analytics_models.md#item-facts-and-productseller-metrics).

## 25. Payment analytics

`make dataset` và bounded modes tự build/test `fct_payments` và
`mart_payment_type_daily`; standalone dùng `make dbt-build` với environment ở mục 18.
`make test-dbt-integration` kiểm tra mixed types, nhiều entries/parent items,
installments, zero amount, canceled/unavailable, empty input và corruption gates.

Giữ `(order_id, payment_sequential)` làm key; `payment_value` đã là amount của
entry, không nhân với installments hoặc join items. Type/date order counts không
cộng được qua types, gồm zero-paid orders và không thay AOV denominator. Missing
parent làm build fail; giữ pending run để resume. Không thêm quyền/cards Metabase.
Xem [payment metrics](../data_contracts/09_analytics_models.md#payment-facts-and-type-metrics).

## 26. Change control

Update runbook này trong cùng pull request khi bất kỳ nội dung sau thay đổi:

- Command, path, service hoặc schema name.
- Cursor, lookback, retry hoặc backfill semantics.
- Data-quality routing hoặc thresholds.
- Reconciliation equations.
- Warehouse conflict-resolution behavior.
- Operational evidence hoặc recovery procedure bắt buộc.

Sau khi pipeline được triển khai, chuyển document status từ `Draft — pre-implementation` sang `Active`, thay planned commands bằng tested commands và bổ sung links tới final data contracts cùng monitoring dashboards.
