# E-commerce Data Warehouse & Pipeline

## Kế hoạch triển khai Data Engineering Portfolio Project

**Dataset nền:** Olist Brazilian E-commerce

**Phần mở rộng tùy chọn:** Synthetic website events, API emulator và ClickHouse benchmark

---

## 1. Mục tiêu dự án

Dự án xây dựng một batch data platform hoàn chỉnh từ nguồn dữ liệu giao dịch đến data warehouse và dashboard. Hệ thống sẽ thu thập dữ liệu từ PostgreSQL và CSV, lưu bản dữ liệu gần nguyên trạng trong MinIO, chuẩn hóa và kiểm tra dữ liệu bằng PySpark, sau đó load vào PostgreSQL warehouse theo cơ chế incremental và idempotent. dbt chịu trách nhiệm xây dựng dimensional model, kiểm tra tính toàn vẹn và tạo các business marts phục vụ Metabase.

Trọng tâm của dự án không phải là sử dụng càng nhiều công nghệ càng tốt, mà là chứng minh pipeline có tính đúng đắn, có thể chạy lại an toàn, có khả năng phát hiện mất dữ liệu, xử lý late-arriving records, thực hiện backfill và cung cấp đầy đủ audit information. Sau khi hoàn thành phạm vi mục tiêu, repository phải cho thấy một quy trình engineering có thể kiểm thử, giải thích và tái lập.

---

## 2. Kiến trúc tổng thể

```text
PostgreSQL source / CSV / Optional API emulator
                    │
                    ▼
             Airflow ingestion
       Orchestration, scheduling, retry
                    │
                    ▼
               MinIO Bronze
    Raw logical records + ingestion metadata
                    │
                    ▼
                 PySpark
  Parse, schema validation, normalization,
   deduplication and row classification
                    │
                    ▼
               MinIO Silver
       Immutable validated batch outputs
                    │
                    ▼
       PostgreSQL warehouse staging
          Idempotent upsert by source key
                    │
                    ▼
                   dbt
       ├── staging models
       ├── intermediate models
       ├── dimensions and facts
       └── business marts
                    │
                    ▼
                Metabase
       Business and operational dashboards
```

### 2.1. Ranh giới trách nhiệm

**Airflow** chỉ chịu trách nhiệm lập lịch, điều phối task, truyền `run_id` và execution context, cấu hình retry, thực hiện backfill và gọi các job ingestion, PySpark hoặc dbt. Lifecycle và failure context được callback ghi vào project-owned audit tables; business counters do từng job ghi sau khi transaction dữ liệu hoàn tất. Không đọc trực tiếp Airflow metadata database để tránh phụ thuộc vào internal schema có thể thay đổi giữa các phiên bản. Airflow không chứa business transformation phức tạp.

**Bronze** lưu dữ liệu gần với biểu diễn nguồn nhất có thể và phải đủ để replay mà không cần đọc lại source. Với CSV, file gốc được copy bất biến trước khi parse; với PostgreSQL, có thể ghi Parquet nhưng chưa normalize, trim, cast business values hoặc loại bỏ cột nguồn. Mỗi record được bổ sung `_record_uid`, `_logical_record_uid`, `_ingested_at`, `_run_id`, `_run_attempt`, `_source_name`, `_source_file`, `_source_row_number` và `_schema_version` để phục vụ lineage và audit.

`_record_uid` nhận diện một physical input occurrence và được tạo từ source, run, object/page cùng row ordinal; ID này không đổi qua task retry hoặc các layer của cùng run và được dùng cho row-count reconciliation. `_logical_record_uid` nhận diện source table + canonical source key + source version khi key hợp lệ, được dùng cho deduplication và version tracking. Vì các physical duplicates có `_record_uid` khác nhau nhưng `_logical_record_uid` giống nhau, pipeline vẫn đếm chính xác số input rows đã drop.

Mỗi Bronze batch có `manifest.json` chứa source table, extract bounds, committed cursor trước run, row count, schema fingerprint, danh sách object, checksum, code version và config version. Task ghi vào attempt path tạm thời và chỉ publish batch sau khi toàn bộ object cùng manifest hoàn tất; marker `_SUCCESS` cho biết batch có thể được downstream đọc. Retry sử dụng lại extract bounds của run và không coi object chưa có `_SUCCESS` là input hợp lệ.

**PySpark** chịu trách nhiệm kiểm tra schema, parse kiểu dữ liệu, chuẩn hóa string và timestamp, phát hiện duplicate trong batch và phân loại lỗi ở cấp record. Routing outputs gồm valid, rejected và deferred; warning là flags gắn trên valid records, không phải output chồng lặp. PySpark không tạo star schema hoặc business marts.

**Silver** lưu các batch đã được chuẩn hóa dưới dạng immutable output theo `run_id`. Không cập nhật trực tiếp các Parquet partition cũ trong MVP. Việc hợp nhất phiên bản mới nhất của mỗi record được thực hiện trong warehouse staging bằng upsert.

**dbt** chịu trách nhiệm chuẩn hóa tên cột trong warehouse, join các bảng, ngăn fan-out, tạo intermediate models, dimensions, facts và business marts. dbt cũng thực hiện warehouse-level integrity tests, sinh documentation và lineage graph. dbt không đọc trực tiếp Bronze.

**Metabase** chỉ đọc business marts và các bảng audit hoặc data quality. Dashboard không query trực tiếp Bronze, Silver hay raw staging.

---

## 3. Data contracts

Data contracts phải được chốt trước khi triển khai pipeline và lưu trong cấu trúc sau:

```text
docs/data_contracts/
├── source_contracts.md
├── schema_evolution.md
├── dimensional_model.md
├── metrics_contract.md
├── data_quality_rules.md
└── operational_contracts.md
```

### 3.1. Business key của customer

Olist sử dụng `customer_id` cho customer record gắn với một order, trong khi `customer_unique_id` dùng để nhận diện cùng một khách hàng qua nhiều lần mua. Vì vậy:

```text
dim_customer.business_key = customer_unique_id
```

`customer_unique_id` được dùng cho repeat purchase rate, historical customer value, customer order history và customer segmentation. `customer_id` vẫn được giữ như source identifier để audit và trace dữ liệu về nguồn, nhưng không được dùng làm business key cho các phân tích nhiều đơn hàng.

### 3.2. Grain của fact tables

| Table             | Grain                                                |
| ----------------- | ---------------------------------------------------- |
| `fct_orders`      | Một dòng trên mỗi `order_id`                       |
| `fct_order_items` | Một dòng trên mỗi `order_id + order_item_id`       |
| `fct_payments`    | Một dòng trên mỗi `order_id + payment_sequential`  |
| `fct_delivery`    | Một dòng trên mỗi `order_id`                       |

Không được join trực tiếp `orders × order_items × payments`, vì một order có thể có nhiều item và nhiều payment record. Phép join này tạo fan-out và làm tăng sai các tổng giá trị. Trước khi xây `fct_orders`, cần aggregate `order_items` và `payments` về một dòng trên mỗi order thông qua `int_order_item_summary` và `int_payment_summary`.

### 3.3. Dimension strategy

Các dimension trong MVP sử dụng **Slowly Changing Dimension Type 1**. Khi thuộc tính thay đổi, dimension lưu giá trị mới nhất và không giữ lịch sử phiên bản.

```text
dim_customer: SCD Type 1
dim_product:  SCD Type 1
dim_seller:   SCD Type 1
dim_location: SCD Type 1
```

Trade-off này phải được ghi trong `ADR-004`. Chẳng hạn, nếu `customer_city` hoặc `customer_state` thay đổi giữa các order của cùng một `customer_unique_id`, giá trị cũ sẽ bị ghi đè. Điều đó làm giảm độ chính xác của phân tích lịch sử theo địa lý, nhưng được chấp nhận có chủ đích để giữ phạm vi MVP vừa phải. SCD Type 2 được xem là hướng mở rộng, không phải yêu cầu bắt buộc.

Mỗi dimension có surrogate key ổn định, business key unique, `created_at`, `updated_at` và `record_hash`. Fact giữ source key để trace nhưng join tới dimension bằng surrogate key. Missing dependency bắt buộc như customer, order hoặc product được xử lý theo deferred policy thay vì âm thầm gán sai dimension. Unknown member `-1` chỉ dùng cho thuộc tính thực sự không biết nhưng được contract cho phép, không dùng để che lỗi referential integrity.

### 3.4. Metric contracts

Một order được coi là hợp lệ cho các metric khi có `order_id` hợp lệ, có `order_purchase_timestamp` parse được và không ở trạng thái `canceled` hoặc `unavailable`. Policy mặc định bao gồm `delivered`, `shipped`, `invoiced`, `processing`, `approved` và `created`. Dashboard phải hiển thị rõ policy này.

**GMV** được định nghĩa là:

```text
GMV = SUM(order_item.price)
```

GMV chỉ tính valid orders, không bao gồm `freight_value`, được gán theo `order_purchase_timestamp` và bắt đầu từ grain order item.

**Gross paid amount** được định nghĩa là:

```text
Gross paid amount = SUM(payment_value)
```

Payment records phải được aggregate về một dòng trên mỗi order trước khi sử dụng. Metric này không được gọi là doanh thu thực nhận vì dataset không có đầy đủ thông tin về refund, phí nền tảng hoặc seller payout.

**Freight amount** được tính bằng:

```text
Freight amount = SUM(order_item.freight_value)
```

**Average order value** được tính bằng:

```text
AOV =
SUM(order-level paid amount)
/
COUNT(DISTINCT paid order_id)
```

AOV sử dụng cùng status policy với GMV. Một order có nhiều payment vẫn chỉ được đếm một lần.

Trong project này, **customer lifetime value** chỉ mang nghĩa historical customer value, không phải predicted lifetime value:

```text
Historical customer value =
SUM(valid order paid amount)
GROUP BY customer_unique_id
```

Có thể bổ sung các trường như `customer_order_count`, `customer_active_days`, `average_order_value`, `first_purchase_date` và `latest_purchase_date`.

### 3.5. Kiểu dữ liệu, currency và timezone

Các metric tiền tệ sử dụng BRL. Giá trị tiền được xử lý bằng decimal và lưu dưới dạng `NUMERIC(18, 2)` trong MVP; không dùng binary floating point cho phép tính business. Contract phải nêu rõ cách làm tròn khi có phép chia và không mặc định rằng GMV bằng gross paid amount.

Timestamp nguồn Olist được diễn giải theo timezone `America/Sao_Paulo`. Warehouse lưu timestamp chuẩn hóa theo UTC và giữ local business date khi cần phân tích theo ngày mua hàng. Parsing, hashing và dbt models phải dùng cùng một timezone policy.

### 3.6. Source delete policy

MVP mô phỏng và xử lý `INSERT` cùng `UPDATE`, chưa hỗ trợ hard delete vì polling một bảng mutable không thể phát hiện delete đáng tin cậy nếu không có CDC, tombstone hoặc periodic snapshot comparison. Giới hạn này phải xuất hiện trong data contract và known limitations. Nếu mở rộng, simulator sinh `DELETE` tombstone với `source_deleted_at`; staging giữ tombstone để audit và marts loại record theo policy, thay vì xóa vật lý mất dấu vết.

---

## 4. Source simulator

Olist là dataset tĩnh nên cần một source simulator để mô phỏng nguồn transactional có insert và update. Mỗi record được bổ sung `source_updated_at`, `source_operation` và `source_sequence`. `source_sequence` là số tăng đơn điệu theo từng source table và là source version authoritative trong simulator; nó được dùng để phân xử khi nhiều operation có cùng timestamp và để ngăn phiên bản cũ ghi đè phiên bản mới.

```text
source_operation ∈ {INSERT, UPDATE}
```

Simulator cần mô phỏng các tình huống thực tế như thêm order mới, order chuyển từ `shipped` sang `delivered`, cập nhật `order_delivered_customer_date`, sửa location của customer, payment đến muộn, order item đến sau order hoặc customer record xuất hiện sau order. Fixture reliability còn phải có nhiều record cùng `source_updated_at`, update đến sai thứ tự, update nằm trong lookback và update cũ hơn lookback SLA.

Dữ liệu được chia thành batch ngày hoặc tuần. Mỗi lần chạy, simulator đọc batch tiếp theo, insert record mới, update một số record cũ, gắn timestamp nguồn và sinh manifest:

```json
{
  "batch_id": "batch_2018_01_08",
  "inserted_orders": 325,
  "updated_orders": 21,
  "late_payments": 7,
  "late_customers": 3
}
```

---

## 5. Data quality policy

Không phải mọi lỗi đều dẫn đến cùng một hành động. Hệ thống sử dụng bốn severity levels:

| Severity   | Ý nghĩa                                         | Cách xử lý                            |
| ---------- | ----------------------------------------------- | ------------------------------------- |
| `REJECT`   | Record không thể sử dụng                        | Không đưa vào Silver valid            |
| `DEFERRED` | Record có thể hợp lệ nhưng thiếu dependency     | Giữ lại để retry                      |
| `WARNING`  | Record đáng ngờ nhưng vẫn có thể sử dụng        | Đưa vào Silver valid và gắn cờ        |
| `INFO`     | Thông tin audit                                 | Không ảnh hưởng dữ liệu               |

Các lỗi cấp record như primary key null, timestamp value không parse được hoặc giá trị tiền âm thuộc nhóm `REJECT`. Các record tham chiếu tới customer, product, order hoặc dependency chưa xuất hiện thuộc nhóm `DEFERRED`. Các trường hợp như category null, location null, delivery delay quá lớn hoặc product dimensions bất thường thuộc nhóm `WARNING`.

Thiếu cả required column hoặc type của column thay đổi là lỗi contract cấp file/batch, không phải hàng loạt row-level rejects. Policy schema evolution mặc định là: additive nullable column được chấp nhận và ghi warning; missing required column, incompatible type change hoặc unknown schema version làm batch fail trước Silver. Mọi exception phải được version hóa trong source contract.

Một record có thể vi phạm nhiều rules nhưng chỉ có một routing outcome theo thứ tự ưu tiên `REJECT > DEFERRED > WARNING > INFO`. `warning_rows` là tập con của valid rows. Row counts đếm distinct `_record_uid`, còn `data_quality_results` đếm từng rule violation.

Dự án sử dụng một bảng quarantine chung:

```text
audit.quarantine_records
```

Bảng này gồm `record_uid`, `logical_record_uid`, `run_id`, `source_table`, `source_primary_key`, `source_version`, `rule_id`, `severity`, `error_reason`, `raw_payload JSONB`, `detected_at`, `first_seen_at`, `last_attempt_at`, `attempt_count`, `retry_status`, `resolved_at` và `terminal_reason`. Retry giữ nguyên original `_record_uid`; quarantine upsert dùng unique key `record_uid + rule_id`. Với record thiếu primary key, `logical_record_uid` để null nhưng physical `_record_uid` vẫn được tạo từ source name, source file hoặc extract batch, run ID và source row number. Không tạo một bảng quarantine riêng cho từng source table trong MVP.

### 5.1. Deferred lifecycle

Deferred records sử dụng state machine:

```text
PENDING ──dependency xuất hiện──> RESOLVED
    └────max age/attempts───────> EXPIRED
```

`EXPIRED` là trạng thái terminal và record không được load vào warehouse; tại resolution run nó được ghi nhận như một terminal reject. Mỗi scheduled run xử lý parent tables trước, sau đó kiểm tra dependency trên valid records của current batch cộng với parent snapshot đã load từ các run trước. Cuối run có một resolution pass cho các record đang `PENDING`, nhờ đó parent và child đến trong cùng batch vẫn có thể được giải quyết.

Retry phải idempotent, không tạo thêm quarantine row cho cùng `record_uid + rule_id`, và có cấu hình `max_attempts` cùng `max_deferred_age`. Original run ghi nhận record là deferred; các lần thử sau giữ original record UID và được ghi riêng vào `deferred_retry_input_rows`, tránh tính chúng như source rows mới.

### 5.2. Deterministic dirty fixture

Script inject lỗi phải sử dụng seed cố định và configuration rõ ràng:

```yaml
random_seed: 42

orders:
  duplicate_rate: 0.01
  invalid_fk_rate: 0.005
  future_timestamp_rate: 0.002

order_items:
  negative_price_rate: 0.003
  null_product_id_rate: 0.002

products:
  null_category_rate: 0.01
  invalid_weight_rate: 0.002
```

Không inject lỗi vào cột không tồn tại trong Olist. Mỗi lần inject phải sinh manifest để pipeline so sánh expected errors với detected errors:

```json
{
  "fixture_id": "dirty_fixture_v1",
  "injected_duplicates": 100,
  "invalid_foreign_keys": 50,
  "negative_prices": 30,
  "future_timestamps": 20
}
```

---

## 6. Audit và reconciliation

Audit layer gồm các bảng:

```text
audit.pipeline_runs
audit.pipeline_task_runs
audit.pipeline_dataset_runs
audit.data_quality_results
audit.quarantine_records
audit.pipeline_control
```

`audit.pipeline_runs` lưu trạng thái toàn DAG: `run_id`, `dag_id`, `run_type`, logical interval, `started_at`, `completed_at`, `status`, `duration_seconds`, `code_version`, `config_version` và `error_message`. `run_type` phân biệt `scheduled`, `manual` và `backfill`; task retry vẫn thuộc cùng pipeline run và được phân biệt bằng attempt number.

`audit.pipeline_dataset_runs` lưu counters và cursor theo từng `run_id + source_table`, tối thiểu gồm:

```text
run_id
source_table
run_attempt
extract_lower_bound
extract_upper_cursor
source_rows_extracted
bronze_rows_written
bronze_rows_read
deferred_retry_input_rows
processing_input_rows
silver_valid_rows
rejected_rows
deferred_rows
duplicate_dropped_rows
warning_rows
data_quality_violation_count
lookback_reprocessed_rows
load_candidate_rows
warehouse_inserted_rows
warehouse_updated_rows
warehouse_unchanged_rows
warehouse_stale_ignored_rows
watermark_before
watermark_candidate
watermark_after
reconciliation_status
```

`audit.pipeline_task_runs` lưu trạng thái từng task attempt. Callback chỉ ghi lifecycle và error context; business counters được job ghi sau khi transaction dữ liệu hoàn tất để tránh callback race hoặc đếm hai lần khi retry.

### 6.1. Reconciliation equations

Mọi successful dataset run phải thỏa các phương trình sau với tolerance bằng `0`:

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

Các routing outcomes ở vế phải không chồng lặp và đếm distinct `_record_uid`. Warning records vẫn nằm trong `silver_valid_rows`; `warning_rows` vì vậy là một subset, không cộng vào reconciliation. `data_quality_violation_count` đếm rule hits và có thể lớn hơn số records. `deferred_retry_input_rows` không phải source rows mới; record hết hạn trong retry được tính vào `rejected_rows` của resolution run.

`lookback_reprocessed_rows` là tập con của `source_rows_extracted`. Chúng thường trở thành unchanged, updated hoặc stale ignored khi load. Dashboard phải tách source rows mới, lookback rows và deferred retry input để reprocessing không bị hiểu nhầm là tăng trưởng dữ liệu.

Reconciliation được thực thi theo từng source table trước khi tổng hợp lên pipeline run. Mismatch một row cũng làm dataset run fail và ngăn commit watermark. Threshold chỉ áp dụng cho quality rates như warning hoặc quarantine rate; không dùng threshold để che row loss. Mỗi Bronze manifest còn được kiểm tra row count, file checksum và schema fingerprint trước khi Spark đọc.

### 6.2. Operational service levels

MVP phải cấu hình được schedule, freshness threshold, expected maximum runtime, maximum deferred age, warning-rate threshold và reject-rate threshold cho từng dataset. Operational dashboard hiển thị actual so với threshold. Failure notification qua Slack hoặc email vẫn optional, nhưng failure state và nguyên nhân phải xuất hiện trong Airflow cùng audit tables.

---

## 7. Incremental extraction design

### 7.1. Cursor và source version

Mỗi source table sử dụng một ordered cursor thay vì chỉ dựa vào `updated_at`:

```text
(source_updated_at, source_sequence, <primary-key columns>)
```

`source_sequence` phân xử nhiều operations có cùng timestamp; primary-key columns bảo đảm total ordering ổn định. Không nối composite key thành một string vì có thể collision hoặc sai sort order. Nếu source thực tế không cung cấp sequence, fallback `(source_updated_at, <primary-key columns>)` phải được ghi trong contract cùng giới hạn về timestamp resolution.

Tie-breaker columns được cấu hình theo từng bảng:

| Source table       | Primary-key columns                    |
| ------------------ | -------------------------------------- |
| `orders`           | `order_id`                             |
| `customers`        | `customer_id`                          |
| `order_items`      | `order_id`, `order_item_id`            |
| `order_payments`   | `order_id`, `payment_sequential`       |
| `products`         | `product_id`                           |
| `sellers`          | `seller_id`                            |

Source version dùng cho warehouse conflict resolution trong simulator là `source_sequence`; `source_updated_at` phục vụ discovery/lookback và primary-key columns chỉ giúp pagination có total order. Vì sequence tăng theo operation, một correction có timestamp cũ nhưng sequence mới vẫn được phép update target. Với source không có sequence, `source_updated_at` là fallback version và contract phải thừa nhận không thể phân xử hai updates cùng key/cùng timestamp một cách tuyệt đối.

### 7.2. Bounded extraction với lookback

Khi một scheduled run bắt đầu cho từng source table:

1. Đọc committed cursor `W` từ `audit.pipeline_control`.
2. Tính `extract_lower_bound = W.updated_at - lookback_interval`.
3. Trong consistent source snapshot, chụp maximum cursor làm `run_upper_cursor = U`.
4. Đọc mọi row có `source_updated_at >= extract_lower_bound` và cursor `<= U`, sử dụng một `page_cursor` riêng để keyset pagination.
5. Giữ nguyên `W`, `extract_lower_bound` và `U` cho mọi task retry của run.

Query pagination khái niệm có dạng:

```sql
WHERE source_updated_at >= :extract_lower_bound
  AND (source_updated_at, source_sequence, <pk_columns>) > (:page_cursor)
  AND (source_updated_at, source_sequence, <pk_columns>) <= (:run_upper_cursor)
ORDER BY source_updated_at, source_sequence, <pk_columns>
LIMIT :page_size
```

MVP sử dụng PostgreSQL `REPEATABLE READ` cho extract của từng table, hoặc một snapshot/high-water mechanism tương đương, để pagination không nhìn thấy một tập dữ liệu thay đổi giữa các page. Bronze manifest lưu `W`, lower bound, `U`, page count và extracted row count.

Ở initial load, `W` là một sentinel nhỏ hơn mọi source timestamp. Nếu source table trống thì `U` là null và watermark candidate giữ nguyên `W`. Sau một run thành công, candidate là `max(W, U)` theo cursor ordering, nên một update có timestamp lùi trong lookback vẫn được xử lý mà không làm committed cursor đi lùi.

Trong mỗi processing run, record cùng source key được dedupe bằng cách lấy source version lớn nhất; `_record_uid` là tie-breaker cuối cùng để kết quả deterministic. Một event có business event time cũ nhưng `source_updated_at` mới vẫn được cursor bắt bình thường. Một update giữ `source_updated_at` cũ chỉ được bắt nếu nằm trong lookback; MVP đặt lateness SLA mặc định là 48 giờ và phải có test chứng minh record cũ hơn SLA không được hứa là tự động phát hiện. Periodic full-source reconciliation là hướng mở rộng để giảm giới hạn này.

### 7.3. Cursor storage và atomic commit

Local full-snapshot namespace `local_core_v1` đã triển khai run/task audit, exact receipt reconciliation và atomic four-table watermark CAS qua fresh dbt gate; Airflow daily wrapper gọi cùng runner, paused mặc định, một task/cùng retry identity. Xem `docs/data_contracts/11_pipeline_control.md`. Pipeline-owned incremental windows/48h lookback đã có trong contract 12. Isolated backfills và distributed/production orchestration vẫn là mục tiêu của phần dưới.

`audit.pipeline_control` có một row cho mỗi `pipeline_name + source_table`, gồm `committed_cursor JSONB`, `cursor_version`, `updated_by_run_id` và `updated_at`. Cursor ví dụ:

```json
{
  "updated_at": "2018-01-10T12:30:00-02:00",
  "source_sequence": 18492,
  "primary_key": {
    "order_id": "abc",
    "order_item_id": 2
  }
}
```

Trong audit fields, tên `watermark_before/candidate/after` chính là serialized committed/candidate cursor tương ứng; plan giữ thuật ngữ watermark cho dashboard nhưng cursor luôn là ordered tuple, không phải chỉ một timestamp.

`watermark_candidate` được lưu ở dataset run nhưng chưa thay đổi committed cursor. Scheduled run chỉ promote toàn bộ candidates sau khi Bronze publish, Silver processing, warehouse transaction, dbt build và reconciliation đều thành công. Commit sử dụng compare-and-swap trên `cursor_version`; nếu version đã đổi, run fail thay vì ghi đè cursor của run khác.

Airflow đặt `max_active_runs=1` cho scheduled DAG trong MVP. Backfill dùng namespace/run type riêng và mặc định không thay đổi scheduled cursor. Nếu bất kỳ bước nào thất bại, cursor cũ được giữ nguyên; rerun sử dụng lại immutable Bronze batch hoặc cùng bounds thay vì chụp một upper cursor mới.

---

## 8. Warehouse loading strategy

Silver valid được load vào schema `raw_staging`, chẳng hạn:

```text
raw_staging.orders
raw_staging.customers
raw_staging.order_items
raw_staging.order_payments
```

Mỗi staging table giữ source key, source version, `record_hash`, `_record_uid`, first/last seen run ID và ingestion timestamps. Trước merge, Silver input được dedupe về một phiên bản mới nhất trên mỗi source key.

PostgreSQL sử dụng `INSERT ... ON CONFLICT (...) DO UPDATE` hoặc `MERGE` trong một transaction. Conflict resolution bắt buộc tuân theo thứ tự sau:

| Điều kiện                                                        | Kết quả                                                                 |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Target chưa có source key                                        | `inserted`                                                              |
| Incoming version lớn hơn và business hash khác                   | `updated`; cập nhật business values cùng source version                 |
| Incoming version lớn hơn nhưng business hash giống               | `unchanged`; chỉ advance source-version metadata                        |
| Incoming version bằng và hash giống                              | `unchanged`                                                             |
| Incoming version nhỏ hơn                                         | `stale_ignored`; tuyệt đối không ghi đè target                           |
| Incoming version bằng nhưng hash khác                            | Fail integrity check vì cùng source version có hai payload khác nhau    |

Nhờ version comparison, lookback, backfill hoặc out-of-order delivery không thể làm phiên bản cũ ghi đè dữ liệu mới. Việc chạy lại cùng một batch không làm tăng row count.

`record_hash` được tạo từ danh sách business columns có thứ tự và có `hash_schema_version`. Canonical serialization phải quy định explicit null marker, UTF-8 strings, UTC timestamp format và fixed-scale decimal. Ingestion metadata, run ID và source version không nằm trong business hash.

Data mutation, classification counts và ghi `audit.pipeline_dataset_runs` được thực hiện trong cùng PostgreSQL transaction. dbt staging models đọc `raw_staging`; marts chỉ được publish hoặc đánh dấu successful sau khi `dbt build` cùng warehouse reconciliation hoàn tất.

---

## 9. Kế hoạch triển khai theo phase

Các checklist dưới đây được giữ lại vì chúng dùng trực tiếp để theo dõi tiến độ triển khai.

### PHASE 0 — Contracts, fixtures và infrastructure

**Thời lượng dự kiến:** 3–5 ngày

Phase 0 tập trung vào việc hiểu đúng dataset, chốt data contracts, xây source simulator và dựng hạ tầng local. Trước khi kết thúc phase, source ERD, primary keys, foreign keys, fact grains, metric definitions và SCD policy phải được ghi thành tài liệu.

- [ ] Đọc data dictionary và vẽ source ERD.
- [ ] Phân biệt `customer_id` với `customer_unique_id`.
- [ ] Kiểm tra grain và quan hệ của `orders`, `order_items`, `payments` và geolocation.
- [ ] Viết source, schema-evolution, dimensional, metrics, data-quality và operational contracts.
- [ ] Chốt valid-order policy, fact grains, surrogate keys, unknown-member policy và SCD Type 1.
- [ ] Chốt schema evolution, BRL/decimal, timezone, delete policy và 48-hour lateness SLA.
- [ ] Load Olist vào `olist_source`.
- [x] Xây source simulator có insert, update, source sequence và batch manifest cho 4 core tables.
- [x] Sinh fixture có same-timestamp, out-of-order timestamps và late updates trong/ngoài SLA.
- [x] Tái sử dụng dirty inputs bốn core tables; per-record expected-error manifest pin 44 physical inputs bằng SHA-256, exact route/error/warning/deferred rules. Xem `tests/fixtures/silver/README.md`.
- [x] Unit tests kiểm tra deterministic injection, checksum/oracle identities, raw evidence và không mutate legacy inputs.
- [ ] Dựng PostgreSQL, MinIO, Airflow LocalExecutor và Metabase bằng Docker Compose có health checks.
- [ ] Pin image/dependency versions, tạo `.env.example` và database migrations ban đầu.
- [x] Tạo CI skeleton chạy lint, contracts, unit tests, wheel/sdist build và simulator integration.
- [x] Bổ sung DAG import/serialization checks với isolated pinned Airflow runtime; local full-snapshot DAG có max active runs 1, retry identity và strict prerequisites.

Một PostgreSQL instance có thể chứa `olist_source`, `olist_warehouse` và `airflow_metadata` trong môi trường local. Đây là lựa chọn đơn giản hóa cho portfolio, không phải kiến trúc production.

**Milestone 0:** Data contracts cùng known limitations được chốt, source simulator tạo được deterministic versions và edge cases, dirty fixture tái lập được, migrations chạy thành công và toàn bộ infrastructure healthy.

---

### PHASE 1 — End-to-end vertical slice

**Thời lượng dự kiến:** 6–10 ngày

Phase này chỉ sử dụng `orders`, `customers`, `order_items` và `order_payments` để hoàn thiện một luồng dọc từ source đến dashboard trước khi mở rộng phạm vi.

- [x] Local Airflow incremental wrapper extract pipeline-owned 48h windows từ PostgreSQL, freeze deltas/full FK inputs trước publication; manual backfill nhận explicit UTC bounds; xem contract 13.
- [x] Implement orders CLI slice: bounded PostgreSQL snapshot → immutable JSONL Bronze trên MinIO, manifest/checksum/lineage và retry-safe Bronze-only checkpoint.
- [x] Implement orders PySpark/Silver slice: timezone normalization, local validation, version dedupe, Parquet routes và immutable publication/replay.
- [x] Add full customers Bronze/Silver and orders customer FK/deferred routing with pinned snapshot hashes; explicit late-parent reprocessing preserves original lineage. Durable deferred lifecycle and staging are now implemented in contracts `08` and `10`.
- [x] Add `order_items`/`order_payments` Bronze/Silver: composite keys, decimal money, full orders snapshots and pinned FK checks; scheduled item v2 now checks product/seller membership with frozen parents. See `docs/data_contracts/07_order_children.md`.
- [x] Ghi Bronze immutable theo `ingestion_date`, `run_id` và attempt path cho bốn core datasets.
- [x] Tạo Bronze manifest, checksum, schema fingerprint và `_SUCCESS` marker.
- [x] Bổ sung physical `_record_uid`, `_logical_record_uid`, source version và ingestion metadata.
- [x] PySpark parse, cast, normalize, validate và dedupe trong batch cho bốn core datasets.
- [x] Phân loại valid, rejected, deferred và warning; durable deferred lifecycle đã có theo contract `10`.
- [x] Ghi Silver immutable theo `run_id`.
- [x] Upsert Silver valid vào bốn bảng `raw_staging` bằng transactional version-aware loader; xem `docs/data_contracts/08_warehouse_staging.md`.
- [x] Tính canonical `record_hash` và ghi inserted, updated, unchanged, stale-ignored counts cùng data transaction trong `audit.warehouse_load_runs`. Planned DAG-level audit vẫn chưa triển khai.
- [x] Tạo `stg_orders`, `stg_customers`, `stg_order_items`, `stg_payments`.
- [x] Tạo `dim_customer` có surrogate key ổn định theo shopper ID, Type 1 attributes, `int_order_item_summary`, `int_payment_summary` và `fct_orders`.
- [x] Tạo `mart_revenue_daily` và `mart_average_order_value` cùng exact dbt reconciliation; views chưa atomic publish. Xem `docs/data_contracts/09_analytics_models.md`.
- [x] Local Metabase dashboard: GMV/gross paid/AOV/eligible-order KPIs, daily trends, status/completeness, inclusive date filters, dedicated BI grants và publication gate. Xem contract 14.
- [ ] Viết README với architecture diagram, source ERD, grain, metric definitions và screenshots.

Bronze path có dạng:

```text
bronze/orders/
  ingestion_date=YYYY-MM-DD/
    run_id=<run_id>/
      manifest.json
      _SUCCESS
      attempt=0001/part-*.jsonl
```

Silver output có dạng:

Core datasets hiện dùng layout dưới đây cùng run-level request/manifest/marker; deferred chỉ xuất hiện khi có dependency routing. Xem contracts `05`–`07` trong `docs/data_contracts/`.

```text
silver/<table>/run_id=<id>/request.json
silver/<table>/run_id=<id>/manifest.json
silver/<table>/run_id=<id>/_SUCCESS
silver/<table>/run_id=<id>/attempt=<uuid>/<valid|rejected|deferred|duplicates>/part-*.parquet
```

Mỗi Silver run cũng có manifest và chỉ được downstream đọc sau `_SUCCESS`. Warning records nằm trong valid output với danh sách `warning_rule_ids`, không tạo một bản sao ở path riêng.

**Milestone 1:** Pipeline chạy hoàn chỉnh từ source đến Metabase và không xảy ra fan-out khi kết hợp order items với payments.

---

### PHASE 2 — Data quality, audit và reconciliation

**Thời lượng dự kiến:** 6–10 ngày

Phase này biến pipeline từ một luồng ETL thông thường thành một hệ thống có thể kiểm chứng. PySpark xử lý row-level validation và routing, trong khi dbt kiểm tra integrity sau khi dữ liệu đã vào warehouse.

- [ ] Xây rule registry và gắn `rule_id`, severity cho từng lỗi.
- [ ] Tách schema/file-level failures khỏi row-level routing.
- [ ] Ghi rejected, deferred và warning records.
- [x] `make test-dirty-data` so sánh từng UID/rule/route với handwritten manifest trên Spark; 44 cases, gồm reject priority, FK deferred, warnings và duplicates.
- [x] Load missing-customer/order deferred metadata vào `audit.quarantine_records`; generic rejects/warnings quarantine còn planned.
- [x] Cài state machine, idempotent retry, resolve và expire deferred records; pin full parent snapshot, freeze limits, atomic queue/staging/audit, bounded 1,000-record passes. Xem `docs/data_contracts/10_deferred_lifecycle.md`.
- [ ] Thêm dbt tests: `not_null`, `unique`, `relationships`, `accepted_values`.
- [ ] Thêm custom tests cho negative values, fact grain, fan-out và metric invariants.
- [x] Tạo run-, task- và dataset-level audit tables cùng `audit.pipeline_control` cho local manual namespace; Airflow callbacks còn planned.
- [x] Ghi local full-snapshot receipts/counters và watermark before/candidate/after; bounded metadata nằm trong Bronze receipts.
- [x] Thực thi source-to-Bronze, processing-routing và warehouse-load reconciliations trong final local gate.
- [x] Làm local pipeline fail nếu reconciliation lệch một row.
- [x] Cấu hình freshness, runtime, deferred-age và reject/warning-rate thresholds qua read-only `make pipeline-health`; freshness là tuổi pipeline SUCCESS, chưa đo source change lag. Scheduler/notification còn planned.
- [x] Dựng static HTML operational health dashboard từ audit schema qua `make pipeline-dashboard`; source/lookback/backfill/retry accounting đã có; live refresh và historical trends còn planned.

Operational dashboard cần hiển thị pipeline status, duration, freshness, quarantine rate, warning rate, insert/update/unchanged/stale counts, source rows mới, lookback rows, deferred retry input, task failures và reconciliation failures.

**Milestone 2:** Có thể giải thích pipeline có mất dữ liệu hay không, record nào bị reject hoặc deferred, batch nào thất bại và mỗi run đã insert, update hoặc đọc lại bao nhiêu record.

---

### PHASE 3 — Incremental reliability

**Thời lượng dự kiến:** 6–10 ngày

Đây là phase mục tiêu chính của portfolio. Pipeline phải chứng minh rằng nó có thể chạy incremental, chạy lại an toàn và xử lý late-arriving updates.

- [x] Cấu hình ordered cursor `(source_updated_at, source_sequence, primary-key columns)` cho bốn core tables.
- [x] Tách pipeline committed cursor, Bronze delivery cursor, page cursor và frozen run upper cursor.
- [x] Freeze bốn source deltas và hai full FK inputs cùng repeatable-read snapshot; durable bytes/fixed bounds giữ nguyên khi retry.
- [x] Test pagination với nhiều record có cùng `source_updated_at`.
- [x] Lưu committed cursor cùng compare-and-swap version trong JSONB control table; full/incremental namespaces độc lập.
- [x] Promote full/incremental watermark candidates sau receipt/dbt gates; xem contracts 11–12.
- [x] Giữ local watermark cũ nếu dbt/reconciliation fail; failure injection và CAS rollback đã có tests.
- [x] Áp dụng inclusive lookback 48 giờ; fixtures kiểm tra trong/đúng biên/ngoài SLA và source writes sau freeze.
- [x] Tách source bootstrap/after-cursor/lookback, full FK scans và deferred retry inputs; after-cursor không phải business growth.
- [x] Dedupe về latest source version trước warehouse merge.
- [x] Xác minh out-of-order version trở thành `stale_ignored` và không ghi đè target.
- [x] Làm equal-version/different-payload fail integrity check.
- [x] Chạy cùng batch hai lần và xác minh số dòng không đổi.
- [x] Warehouse integration: batch ID mới với cùng source versions ghi toàn bộ `warehouse_unchanged_rows`, không insert/update và giữ row counts cho cả bốn grains.
- [x] Incremental/backfill recovery matrix: lỗi sau committed Bronze, Silver, warehouse và sau dbt/CAS trước transaction commit; cùng run ID resume ở attempt 5, giữ frozen bytes/receipts và không double-count retry.
- [x] Chặn overlapping local full/incremental runs qua shared file/database guard; test cursor CAS conflict. Distributed execution leases còn planned.
- [x] Hỗ trợ isolated backfill bằng `BACKFILL_START`/`BACKFILL_END` offset-aware, source-updated-at [start,end); manual Airflow DAG.
- [x] So sánh exact business staging/hashes và marts với full reload database độc lập bằng integration fixtures.
- [x] Xác minh backfill giữ nguyên full/incremental cursors/versions/timestamps, cô lập queue của run khác; dbt failure/retry và empty-window tests.
- [x] Cấu hình local Airflow wrapper retry 2 lần, delay 1 phút; failure callback riêng vẫn planned, runner giữ audit hiện có.
- [x] Ghi business-stage failures vào `audit.pipeline_task_runs` qua runner; Airflow crash/kill có thể giữ RUNNING tới resume.

**Verification evidence (local core):**

| Requirement | Automated evidence |
| --- | --- |
| Same-timestamp pagination, immutable publication/replay | `tests/integration/test_bronze_orders.py` |
| Latest-version dedupe and conflicting versions | `tests/processing/test_orders_silver.py` |
| All-grain stale/equal-version handling, unchanged batch and audit rollback | `tests/integration/test_warehouse_merge.py` |
| Frozen windows, late updates and atomic freeze | `tests/integration/test_incremental_pipeline.py` |
| Four failure boundaries, frozen retry, deferred attempts and final CAS rollback | `tests/integration/test_pipeline_recovery.py` |
| Range repair, scheduled/queue isolation and full reload comparison | `tests/integration/test_backfill_pipeline.py` |
| Runner pending identity and exact failure-stage audit | `tests/unit/test_make_dataset.py` |
| Shared execution guard and all-table CAS conflicts | `tests/integration/test_pipeline_control.py` |

Recovery integration dùng PostgreSQL/MinIO/Spark/dbt thật với controlled exceptions,
không phải SIGKILL hoặc network-partition tests. Runner subprocess dispatch được
kiểm tra bằng unit tests. Distributed leases, Airflow failure callbacks và atomic
analytics snapshot publication vẫn ngoài local implementation hiện tại. Checklist
không khẳng định CI đã chạy trên GitHub hoặc các phases trước đã hoàn tất.

**Milestone 3:** Pipeline incremental, idempotent và version-aware; bounded lookback, out-of-order data, retry, failure recovery, concurrency guard và backfill đều được chứng minh bằng automated tests, không chỉ bằng happy-path demo.

---

### PHASE 4 — Mở rộng dimensional model

**Thời lượng dự kiến:** 5–8 ngày

Sau khi pipeline lõi ổn định, mở rộng ingestion và modeling sang products, sellers, reviews và geolocation. Geolocation phải được aggregate về một dòng trên ZIP prefix trước khi join để tránh fan-out.

- [x] Products standalone full Bronze/Silver → version-aware staging → `dim_product` Type 1; replay/dirty/reconciliation checks. See `docs/data_contracts/15_product_snapshots.md`.
- [x] Add standalone item v2 with pinned full order/product/seller FKs and atomic deferred retry; see `docs/data_contracts/17_item_dependency_snapshots.md`.
- [x] Integrate products/sellers parents and item v2 into audited full/incremental/backfill freeze/scheduling/receipt gates; six dataset audits and four completion cursors.
- [x] Sellers standalone full Bronze/Silver → version-aware staging → `dim_seller` Type 1; location warnings and replay/reconciliation checks. See `docs/data_contracts/16_seller_snapshots.md`.
- [x] Standalone full geolocation Bronze/Silver/staging with technical row IDs, exact coordinates and retry/version gates.
- [ ] Ingest reviews.
- [ ] Integrate geolocation into scheduled freeze/receipts and customer/seller location FK joins.
- [x] `int_geolocation_by_zip`: exact numeric coordinate medians and deterministic joint city/state mode.
- [ ] Hoàn thiện dimensions với surrogate keys, unique business keys và explicit unknown-member policy.
- [x] Current Type 1 `dim_customer`, `dim_product`, `dim_seller`; continuous `dim_date` with local date role keys in all facts, ISO/leap/empty/corruption checks.
- [x] Standalone Type 1 `dim_location` at ZIP grain; stable key, counts, source reconciliation and no new BI grants.
- [x] Add `fct_order_items` at composite item grain with order/customer/product/seller keys, exact source reconciliation and no payment fan-out.
- [x] Add daily product/seller GMV, freight and item/group-order counts; independent fixtures and failure gates.
- [x] Add `fct_payments` at composite payment-entry grain and daily payment-type observed amounts/counts; exact source reconciliation without installment multiplication or item fan-out.
- [x] Extend Metabase with product/seller top-10 tables and payment-type charts; SELECT-only aggregate grants, date filters and publication gates.
- [x] Add order-grain `fct_delivery` and daily purchase-cohort mart; explicit chronology/completeness, local-calendar lateness, UTC elapsed durations, DST and source reconciliation checks.
- [ ] Kiểm tra fact foreign keys, BRL numeric precision và UTC/local-date semantics.
- [ ] Tạo các marts về revenue, customer value, repeat purchase, product, seller, delivery delay và order status.
- [ ] Viết descriptions cho models và columns.
- [ ] Chạy `dbt docs generate`.
- [ ] Thêm lineage graph vào README.

`fct_delivery` sử dụng các timestamp giao hàng trong `orders`, không dùng `order_items` như shipment table.

**Milestone 4:** Dimensional model đầy đủ, grain rõ ràng và metric semantics nhất quán.

---

### PHASE 5 — Testing và Continuous Integration

**Thời lượng dự kiến:** 3–6 ngày

Unit tests tập trung vào source simulator, dirty-data injector, cursor ordering/serialization, parsing, row classification, canonical record hashing, source-version comparison và reconciliation logic. Integration tests sử dụng một deterministic fixture nhỏ với expected results được chốt độc lập, không tái sử dụng chính logic production để sinh expected output.

- [ ] Chạy cùng batch hai lần và xác minh row count không đổi.
- [x] Update một order và xác minh record được update thay vì insert mới.
- [x] Xác minh newer-same-hash được đếm unchanged nhưng source version vẫn advance.
- [x] Xác minh older version bị stale-ignore và equal-version/different-hash làm run fail.
- [x] Kiểm tra same-timestamp pagination không thiếu hoặc lặp record.
- [x] Kiểm tra customer/order FK được deferred đúng policy; product/seller membership có trong scheduled item v2 và atomic full parent freeze.
- [x] Kiểm tra deferred record được resolve khi dependency xuất hiện, giữ original lineage và source-version safety.
- [x] Kiểm tra deferred record expire đúng max age/attempts, terminal states và replay không double count.
- [x] So sánh backfill với full reload.
- [x] Kiểm tra duplicate không đi vào warehouse.
- [ ] Kiểm tra schema evolution matrix, manifest checksum và incomplete batch marker.
- [x] Failure-inject tại từng stage và xác minh watermark không commit sớm.
- [x] Kiểm tra concurrent-run guard và cursor compare-and-swap.
- [x] Chạy DAG import test và một MinIO → Spark → PostgreSQL vertical-slice smoke test.
- [x] Chạy dbt grain tests, fan-out tests và reconciliation tests.

CI skeleton hiện có trong `.github/workflows/ci.yml`: fast checks/build và installed-wheel tests trên Python 3.10/3.12; simulator integration trên Python 3.12 với disposable PostgreSQL, không cần raw CSV hoặc secrets. CI hiện có DAG import/serialization, Spark, dbt, full/incremental gates và failure recovery suites; raw CSV không bắt buộc cho các fixture suites.

GitHub Actions mục tiêu tách thành hai jobs. Fast job chạy trên mọi pull request:

```text
1. Install dependencies từ lockfile
2. Run formatting/lint checks và unit tests
3. Import/parse Airflow DAGs
4. Start PostgreSQL service và apply migrations
5. Load deterministic fixture
6. Run integration tests không cần Spark cluster
7. Run dbt deps
8. Run dbt seed
9. Run dbt build
```

Integration smoke job chạy trên pull request quan trọng hoặc manual workflow:

```text
1. Start PostgreSQL và MinIO bằng pinned images
2. Wait for explicit health checks
3. Apply migrations và load small fixture
4. Run one-table Bronze → Spark → Silver → staging slice
5. Run dbt build cho selected downstream models
6. Assert manifests, reconciliations và idempotent rerun
7. Collect logs/artifacts khi fail
```

Đây là **Continuous Integration**, không gọi là CI/CD vì chưa có deployment tự động.

**Milestone 5:** Repository có independent expected fixtures, unit/integration/failure-path tests, DAG validation, dbt tests, end-to-end smoke coverage và passing CI badge.

---

### PHASE 6 — Optional scale-up

Phase 6 chỉ được triển khai sau khi Phase 0–3 ổn định. Synthetic website events có thể được sinh ở quy mô 2–10 triệu dòng với các event như `page_view`, `product_view`, `add_to_cart`, `checkout_started` và `purchase`. Chỉ purchase event mới có thể chứa `order_id`. Data generator cần tạo cả anonymous sessions, cart abandonment, duplicate events, late-arriving events và out-of-order events.

Phần mở rộng này giúp tạo lý do thực tế hơn cho PySpark. ADR phải ghi rõ rằng Olist MVP không bắt buộc cần distributed processing; Spark được dùng để xây abstraction có thể mở rộng và synthetic events được dùng để đánh giá scale-up behavior.

Có thể benchmark pandas, Polars và PySpark theo execution time, peak memory, rows per second, startup overhead và số partition. API emulator chỉ nên được thêm nếu mô phỏng pagination, `updated_since`, authentication, retryable failures, timeout, rate limit và schema version. ClickHouse chỉ được benchmark sau khi có event data đủ lớn; không thêm chỉ để tăng số lượng công nghệ.

---

## 10. Repository structure

Repository core chỉ chứa batch MVP đã được triển khai. Không tạo trước các thư mục Kafka/streaming rỗng; chúng chỉ được thêm trong Phase 6 cùng code và tests thực tế.

```text
ecommerce-data-platform/
├── airflow/
│   ├── dags/
│   │   └── batch/
│   └── callbacks/
├── spark/
│   ├── jobs/
│   ├── rules/
│   └── schemas/
├── src/
│   └── olist_platform/
│       ├── raw/
│       ├── ingestion/
│       ├── source_simulator/
│       ├── data_quality/
│       └── audit/
├── data/
│   ├── sample/
│   ├── fixtures/
│   └── generated/
│       └── source_batches/
├── dbt/
├── infrastructure/
│   ├── docker-compose.yml
│   ├── postgres/
│   │   └── migrations/
│   └── minio/
├── config/
│   ├── batch/
│   └── common/
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
│   ├── data_contracts/
│   ├── decisions/
│   ├── diagrams/
│   └── runbooks/
├── scripts/
├── .github/workflows/
├── .env.example
├── .gitignore
├── README.md
├── Makefile
├── pyproject.toml
└── uv.lock
```

Package `olist_platform.source_simulator`, four-batch reliability fixture và isolated transactional tests đã được triển khai; xem `docs/data_contracts/03_source_simulator.md`. CI skeleton đã có fast checks, package build và simulator integration. Dirty-data injector, DAG checks và end-to-end smoke CI vẫn chưa triển khai.

Package `olist_platform.raw` và uv workflow đã được triển khai. `scripts/` chỉ giữ compatibility wrappers; reusable Python logic nằm trong `src/olist_platform`. Các folders khác trong sơ đồ là target layout và chỉ được tạo khi có code/tests tương ứng. CLI defaults dùng working directory; config và raw data không được bundle vào wheel.

### 10.1. Reproducibility và basic security

Docker images và application dependencies phải pin version; database schema thay đổi qua versioned migrations. `.env.example` chỉ chứa tên biến và giá trị local không nhạy cảm. `.env`, credentials, generated data, Spark checkpoints và MinIO volumes không được commit.

Local stack vẫn tách role theo trách nhiệm: ingestion đọc source bằng read-only role, warehouse loader chỉ ghi `raw_staging` cùng audit tables cần thiết, dbt transformer quản lý analytics schemas và Metabase chỉ có read access tới marts/audit views. Services chỉ expose port cần thiết trên localhost. Olist là dữ liệu đã anonymize, nhưng repository vẫn ghi rõ rằng production PII cần masking, retention và access-control policy riêng.

---

## 11. Architecture Decision Records

Các quyết định quan trọng được lưu trong `docs/decisions/`:

```text
ADR-001-use-postgresql-as-warehouse.md
ADR-002-use-minio-for-bronze-and-silver.md
ADR-003-separate-pyspark-and-dbt-responsibilities.md
ADR-004-use-scd-type-1.md
ADR-005-use-immutable-silver-batches.md
ADR-006-use-bounded-composite-cursors.md
ADR-007-use-pyspark-on-small-olist-dataset.md
ADR-008-use-project-owned-audit-tables.md
ADR-009-use-bronze-manifests-and-atomic-publish.md
ADR-010-use-expiring-deferred-record-lifecycle.md
ADR-011-do-not-support-hard-delete-in-mvp.md
ADR-012-use-version-aware-conflict-resolution.md
```

Mỗi ADR trình bày `Context`, `Decision`, `Alternatives considered`, `Consequences` và `Known limitations`.

---

## 12. Phạm vi và thứ tự ưu tiên

Phạm vi bắt buộc gồm source simulator có ordered insert/update versions; bounded Airflow ingestion; immutable Bronze có manifest/checksum/atomic publish; immutable Silver; PySpark validation; valid/reject/deferred/warning classification; expiring deferred lifecycle; PostgreSQL staging với version-aware idempotent merge; dbt models; grain, schema, money/timezone và metric contracts; bounded composite cursors; lookback SLA; atomic watermark commit; isolated backfill; dataset-level audit và exact reconciliation; business/operational dashboards; deterministic fixtures; basic secret/role discipline và GitHub Actions CI.

API emulator, synthetic events, Polars/Spark benchmark, ClickHouse, Slack/email alert, SCD Type 2, streaming pipeline và cloud deployment đều là optional. Không bắt đầu phần optional trước khi Phase 0–3 hoạt động ổn định.

Thứ tự ưu tiên thực tế là:

1. Hoàn thành Phase 0 và CI skeleton để khóa contracts, fixtures và nền tảng tái lập.
2. Hoàn thành Phase 1 để có end-to-end vertical slice.
3. Hoàn thành Phase 2 để có data quality và audit.
4. Hoàn thành Phase 3 để chứng minh reliability.
5. Bổ sung phần lõi của Phase 4 và harden CI theo Phase 5.
6. Chỉ làm Phase 6 khi các phần trên đã hoàn chỉnh.

---

## 13. Điểm dừng hợp lý

| Phase hoàn thành | Giá trị portfolio                                                    |
| ---------------- | ---------------------------------------------------------------------- |
| Phase 1          | Có end-to-end batch pipeline nhưng chưa nổi bật                         |
| Phase 2          | Tốt cho Data Engineer Intern                                            |
| **Phase 3**      | **Cân bằng tốt nhất giữa công sức và giá trị tuyển dụng**               |
| Phase 4          | Dimensional model và business layer hoàn chỉnh                         |
| Phase 5          | Portfolio rất mạnh, thể hiện engineering discipline                   |
| Phase 6          | Phần mở rộng về scale và performance                                   |

Mục tiêu thực tế là hoàn thành Phase 0–3, phần lõi của Phase 4, fast CI và ít nhất một vertical-slice smoke test từ Phase 5. Không cần hoàn thành Phase 6 trước khi đưa dự án vào CV.

---

## 14. Timeline thực tế

| Phạm vi                  | Thời gian part-time dự kiến |
| ------------------------ | ----------------------------: |
| Phase 0                  |                     3–5 ngày |
| Phase 1                  |                    6–10 ngày |
| Phase 2                  |                    6–10 ngày |
| Phase 3                  |                    6–10 ngày |
| Core Phase 4             |                     4–7 ngày |
| Phase 5                  |                     3–6 ngày |
| README, diagrams và demo |                     2–4 ngày |

Tổng thời gian hợp lý là khoảng **6–10 tuần làm part-time**. Nếu Airflow, dbt và PySpark đều là công nghệ mới, nên dự trù **8–12 tuần** hoặc dừng ở Phase 3 với chất lượng kiểm thử tốt trước khi mở rộng model.

---

## 15. Deliverables cuối cùng

Khi đạt phạm vi mục tiêu, repository cần có source ERD, warehouse ERD, architecture diagram, data contracts, schema-evolution matrix, data quality rule catalog, sample Bronze manifest, Airflow screenshots, dbt lineage graph, business dashboard, operational dashboard, source-to-target reconciliation report, failure-recovery evidence, automated test results, passing CI badge, ADRs, runbook, known limitations và video demo ngắn.

Quy trình chạy nên được rút gọn thành một số lệnh dễ nhớ:

```bash
make infrastructure-up
make seed-source
make run-pipeline
make test
make infrastructure-down
```

---

## 16. Nội dung có thể đưa vào CV

> Built an incremental e-commerce data platform using Airflow, PySpark, MinIO, PostgreSQL, and dbt, processing source inserts and updates through immutable Bronze and Silver layers into dimensional models and analytics marts.

> Implemented bounded composite cursors, fixed run upper bounds, version-aware idempotent merges, lookback extraction, late-arriving record handling, isolated backfills, and exact source-to-target reconciliation for safe pipeline reruns.

> Designed row-level data quality rules with reject, deferred, and warning policies, deterministic error fixtures, shared quarantine storage, and operational dashboards for pipeline freshness, data quality, and run-level audit metrics.

> Modeled order, item, payment, customer, and delivery data with explicit grains and intermediate aggregates to prevent join fan-out, then documented business metrics and lineage using dbt tests and dbt Docs.
