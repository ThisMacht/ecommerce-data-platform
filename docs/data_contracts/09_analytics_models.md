# Analytics Models and Metrics

## Scope and execution

Status: **Active — eight modeled staging datasets**.
`dbt/` dùng locked `analytics` extra, PostgreSQL adapter và schema `analytics`
được tạo bởi migration `007`. Existing loader role sở hữu schema; reader chỉ SELECT. Migration `012` thêm
`olist_bi_reader` NOLOGIN group chỉ đọc fct_orders, hai order daily marts, ba
product/seller/payment-type marts và publication-status view; dbt giữ grants qua rebuild.
Không dùng dbt packages hoặc remote model dependencies. Local profile dùng một thread:
rebuild song song các views có dependencies chung đã gây PostgreSQL deadlock
trong integration test; final gate vẫn giữ staging locks và không bypass lỗi.

```bash
make migrate
make sync-analytics
# Export PGHOST, PGPORT, PGDATABASE, PGUSER; password dùng secret variable bên dưới.
export DBT_ENV_SECRET_PASSWORD='<loader password>'
export DBT_READER_USER='<reader role>'
make dbt-build
make test-dbt-integration
```

Profile đọc process environment, không tự load Compose `.env` hoặc `WAREHOUSE_DSN`.
Password dùng `DBT_ENV_SECRET_*` để dbt scrub trong logs. Không ghi credentials vào
profile. `make dataset` tự cấu hình local credentials, chạy dbt sau tám staging loads,
và giữ run pending khi build/tests fail. Retry cùng run IDs để staging trả `NOOP`.
Artifacts nằm trong ignored `dbt/target`, hoặc `build/reports/dataset/<run_id>_dbt`
khi chạy runner. Analytics dependency sync có thể bỏ Spark extra; dùng
`uv sync --locked --extra processing --extra analytics` nếu cần cả hai.

## Model grains

| Model | Grain and behavior |
| --- | --- |
| `stg_customers`, `stg_orders`, `stg_order_items`, `stg_payments` | Giữ source business grain, đã được Silver/staging normalize |
| `int_order_item_summary` | Một order: item count, SUM(price), SUM(freight) |
| `int_payment_summary` | Một order: payment count, SUM(payment_value) |
| `int_geolocation_by_zip`, `dim_location` | Một ZIP với exact medians, joint city/state mode và stable Type 1 location key; full-only observations |
| `dim_date` | Một ngày liên tục giữa min/max observed local purchase/estimated/actual/review-creation dates; integer YYYYMMDD key |
| `dim_customer` | Một `customer_unique_id`; current attributes, stable surrogate key |
| `fct_orders` | Một order; join hai summaries để tránh items × payments fan-out |
| `fct_order_items` | Một `(order_id, order_item_id)`; order/customer/product/seller keys, price và freight ở item grain |
| `mart_product_daily`, `mart_seller_daily` | Một local purchase date × product/seller; item/order counts, GMV và freight |
| `fct_payments` | Một `(order_id, payment_sequential)`; observed payment amount, type, installments và order/customer context |
| `mart_payment_type_daily` | Một local purchase date × payment type; entry counts, group distinct orders và gross paid |
| `fct_delivery` | Một order với UTC milestones, local delivery dates, chronology/completeness flags và durations |
| `stg_reviews`, `fct_reviews` | Một `(review_id, order_id)`; score, UTC timestamps, local creation date/key và comment/chronology flags; fact không chứa comment text |
| `mart_review_daily` | Một local review creation date; review-weighted scores/rates và group-distinct reviewed orders |
| `mart_delivery_daily` | Purchase-date cohort: observed delivery counts, late rate và mean elapsed hours |
| `mart_revenue_daily` | Một local purchase date: GMV, freight, gross paid và completeness counts |
| `mart_average_order_value` | Một local purchase date: paid amount, paid order count và AOV |
| `mart_customer_value` | Một current shopper kể cả chưa có order; observed eligible value/counts, paid AOV, first/last purchase và repeat flag |
| `mart_repeat_purchase_daily` | Một local eligible purchase date; first/repeat order counts theo observed shopper history |
| `mart_order_status_daily` | Một local purchase date × current status; order counts và group-distinct shoppers |

`fct_orders` giữ `customer_id`, shopper ID và `customer_key`. Missing customer không được
inner join để âm thầm drop order hoặc gán unknown member: relationship/not-null
tests làm fail build. Orphan item/payment rows cũng làm fail relationship tests. Reviews giữ Silver full-parent
membership gate; không yêu cầu order phải nằm trong bounded staging window.
Đây là integrity gate cho staging; persisted deferred lifecycle ở [contract 10](10_deferred_lifecycle.md).

## Customer identity and Type 1 policy

Business key là `customer_unique_id`, không phải order-scoped `customer_id`.
Surrogate key là MD5 hex của shopper ID, ổn định qua rebuild hoặc thay location;
unique tests phát hiện collision. Đây là identifier, không phải security hash.

Với nhiều customer records cùng shopper, chọn record có `source_sequence` lớn nhất;
tie-break theo `customer_id`. Current attributes ghi đè lịch sử phân tích location
(Type 1), không giữ SCD2 versions. `created_at` là first staging observation nhỏ nhất;
`updated_at` là last staging observation của representative row. Dimension
`record_hash` là MD5 của JSONB array `[city,state,zip]`, khác warehouse business-v1
SHA-256 hash ở source-row grain. Không dùng dimension hash thay warehouse hash.

Quyết định Type 1 được ghi tại [ADR-004](../adr/ADR-004-use-scd-type-1.md).

## Dimension Keys and Missing Members

Dimensions giữ exact business-key membership từ current `raw_staging`; không thêm
synthetic unknown row, không dùng `-1`, `0` hoặc một hash cố định để thay missing FK.
Đây là chính sách có chủ đích của MVP, không có nghĩa dữ liệu không thể thiếu.

| Dimension | Business key | Surrogate key | Missing-member behavior |
| --- | --- | --- | --- |
| `dim_customer` | `customer_unique_id` | MD5 của business key | Required shopper FK; Silver defer locally valid missing customer, dbt fail nếu staging bị orphan |
| `dim_product` | `product_id` | MD5 của business key | Required item v2 membership; missing product không tạo placeholder |
| `dim_seller` | `seller_id` | MD5 của business key | Required item v2 membership; missing seller không tạo placeholder |
| `dim_location` | Valid source ZIP prefix | MD5 của ZIP, giữ leading zeros | Optional customer/seller LEFT JOIN; absent/invalid/unmatched ZIP giữ location_key NULL |
| `dim_date` | Local calendar date | Integer YYYYMMDD | Required purchase/review creation roles; optional milestone key NULL khi timestamp thiếu, không tạo day 0 |

MD5 chỉ là deterministic identifier, không phải security/anonymization hash.
Business/surrogate uniqueness và not-null tests vẫn bắt buộc, kể cả collision.
Keys giữ nguyên qua Type 1 attribute updates. Customer representative lấy source
sequence lớn nhất, tie theo customer_id DESC; `latest_customer_id` phải đúng current
source record, không chỉ là một customer ID còn tồn tại.

Null product attributes hoặc source city/state là missing attributes của một member
có thật; giữ row/key và warning semantics. Optional location không được suy diễn
địa chỉ hoặc dùng unknown ZIP. Actual delivery date key vẫn giữ khi timestamp có
nhưng chronology/status làm metric không eligible. Reviews pin full orders ở Silver;
order có thể nằm ngoài bounded staging window, nên review fact không invent order
context hoặc áp required warehouse-order relationship trong trường hợp đó.

`dimension_key_reconciliation` FULL JOIN raw staging và ba MD5 entity dimensions,
đối chiếu exact member sets/key mapping, kể cả members chưa được fact tham chiếu,
và customer representative. Location/date source-backed gates giữ trong
`geolocation_reconciliation`, `location_key_reconciliation` và
`date_dimension_reconciliation`. Không dùng generic relationships thay cho việc
kiểm tra business meaning của key.

Empty staging sinh empty dimensions, không còn default unknown row. Full/empty
source snapshots không xóa staging cũ: retained valid staging members vẫn là
source-backed members, không coi chúng là synthetic rows hoặc suy luận deletes.
Missing required parents tiếp tục deferred/rejected theo contract và expiry rules;
không sửa source hoặc gán dummy key để làm tests pass.

## Metric policy

Mọi số tiền là **BRL, exact PostgreSQL NUMERIC**. Aggregates không narrow về
`numeric(18,2)` để tránh overflow khi SUM nhiều rows. Không chuyển sang float.
Purchase date lấy từ Silver theo `America/Sao_Paulo`, không dùng UTC date.

- Eligible statuses: `delivered`, `shipped`, `invoiced`, `processing`, `approved`, `created`.
  Loại `canceled` và `unavailable`; fact vẫn giữ các orders này để trace.
- GMV = eligible item price total; không gồm freight.
- Freight amount = eligible item freight total.
- Gross paid amount = eligible payment value total; không phải net revenue hoặc seller payout.
- Paid order cho AOV phải eligible và có tổng payment **lớn hơn zero**.
  AOV = paid amount / paid order count, làm tròn hai chữ số bằng PostgreSQL
  `ROUND(numeric, 2)` (half away from zero). Không có paid orders thì AOV NULL.

Thiếu child records: fact giữ amount NULL và count zero. Daily marts cộng các
amount quan sát được, ghi `orders_without_items`/`orders_without_payments` để biểu
diễn completeness. Tổng ngày không có contributions là zero, không chứng minh
source đã đầy đủ. Zero-valued payment khác missing payment; không tính zero order
vào AOV denominator. GMV không được giả định bằng gross paid amount.

## Validation and limits

`dbt build` chạy key uniqueness, not-null, customer/order relationships, composite
grains, fact counts/money reconciliation và independent source-based daily metric
reconciliation. Tests yêu cầu exact equality; không dùng tolerance che row loss.
Integration fixture có hai items và hai payments cùng order, shared shopper,
canceled/zero/unpaid orders, Type 1 update, empty inputs và failure injection.

Các models hiện là **views**: rebuild deterministic nhưng không tạo immutable
published snapshot. dbt build không atomic toàn DAG; failed tests không rollback
toàn bộ views đã tạo. Chỉ coi artifacts thành công khi build exit code bằng zero;
không chạy writer/reader pipeline đồng thời khi cần một consistent business snapshot.
Atomic mart publication vẫn planned. Core jobs dùng [item Silver v2](17_item_dependency_snapshots.md) với ba frozen full parent snapshots. Local Airflow full/incremental/backfill wrappers đã có;
Metabase business dashboard, dedicated reader grants và audit-based publication
gate theo [contract 14](14_business_dashboard.md). Local run/task audit và watermark gate đã có theo [pipeline contract](11_pipeline_control.md). Deferred retry/expiry đã có theo [lifecycle contract](10_deferred_lifecycle.md).

Products/sellers dùng Type 1 dimensions trong standalone và cả ba core modes;
xem [products](15_product_snapshots.md), [sellers](16_seller_snapshots.md).

## Item Facts and Product/Seller Metrics

`fct_order_items` LEFT JOIN order fact và hai Type 1 dimensions; không join payments.
Giữ source composite key, product/seller business IDs, surrogate keys và mọi order
statuses. Missing order/customer/product/seller để NULL và fail not-null/relationship
hoặc reconciliation tests; không silently drop item hay tự gán unknown member.
Type 1 attributes là hiện tại, không phải category/location tại ngày giao dịch.

Marts group theo `order_purchase_date` (America/Sao_Paulo) và product/seller ID/key:

- `total_item_count`: mọi item rows quan sát được, gồm canceled/unavailable.
- `eligible_item_count`: item rows thuộc eligible statuses như order metrics.
  Một item row là một đơn vị; không dùng `order_item_id` như quantity.
- `eligible_order_count`: distinct orders **trong mỗi group**. Một order có thể
  thuộc nhiều products/sellers, nên không cộng counts giữa groups để lấy global orders.
- `gmv = SUM(price)` và `freight_amount = SUM(freight_value)` trên eligible items,
  exact NUMERIC/BRL. Groups chỉ có ineligible items giữ counts và amounts bằng zero
  cho eligible metrics. Không có items thì không sinh group giả.

Không phân bổ payment, AOV, net revenue hoặc seller payout xuống item/product/seller.
Mỗi item chỉ đóng góp một lần; tổng GMV/freight qua product hoặc seller groups
khớp eligible source item totals và order daily metrics khi integrity tests đạt.
Metabase đọc hai aggregate marts này qua SELECT grants; không được đọc item fact
hoặc dimensions. Xem [BI contract](14_business_dashboard.md).

`item_model_grains`, `item_fact_reconciliation`, `item_mart_reconciliation` đối
chiếu exact keys, mapping, money, statuses, dates và grouped counts với raw staging.
Independent integration fixtures kiểm tra multiple items/payments, nhiều products/
sellers trong cùng order, nhiều dates, canceled/unavailable, empty inputs,
missing dimensions và cố tình làm sai GMV để xác minh test fail.

## Payment Facts and Type Metrics

`fct_payments` giữ mọi payment entry và order statuses, LEFT JOIN order context;
missing order/customer làm integrity tests fail thay vì drop rows. Không join items
hoặc phân bổ payment tới product/seller. `payment_sequential` là thứ tự payment
entry trong order; không phải số installment. Nhiều entries có thể cùng type.

`payment_value` là observed amount của một entry (BRL, exact NUMERIC); không nhân
với `payment_installments` và không suy ra lịch trả góp/cash settlement. Dữ liệu
không đại diện net revenue, refunds hay seller payout.

`mart_payment_type_daily` group theo local purchase date và payment type:

- `total_payment_count`: mọi entries trong group, gồm canceled/unavailable.
- `eligible_payment_count`: entries thuộc eligible order statuses.
- `eligible_order_count`: distinct eligible orders trong type/date, gồm zero-paid
  orders; không phải denominator AOV và không cộng qua types để lấy global orders.
- `gross_paid_amount`: SUM eligible `payment_value`, zero nếu group chỉ có
  ineligible entries. Không có input thì không sinh group giả.

Order có mixed types được tính một lần trong mỗi type có payment entry; order
có nhiều entries cùng type vẫn chỉ tính một order trong group. Amounts cộng qua
types/dates khớp gross paid của order mart khi integrity tests đạt. AOV vẫn lấy
order-grain positive payment total ở `mart_average_order_value`.

`payment_model_grains`, `payment_fact_reconciliation` và
`payment_type_reconciliation` kiểm tra keys, customer mapping, dates, status,
installments, exact money và grouped counts bằng raw staging độc lập. BI reader có SELECT trên aggregate type mart, cùng năm business models khác và
publication-status view; không được đọc payment fact.

## Delivery Facts and Cohort Metrics

`fct_delivery` giữ **một row cho mọi order**, đọc milestones từ orders và LEFT JOIN
order/customer context. Không dùng items như shipment table, không join payments.
Giữ approved/carrier/actual/estimated timestamps nguyên bản; missing customer làm
integrity gate fail. Không sửa hoặc loại source rows chỉ để làm đẹp delivery metrics.

Policy derived fields (không thay Silver routing hoặc warning rules):

- `has_delivery_timestamp`: actual customer timestamp tồn tại, bất kể status.
- `has_invalid_delivery_chronology`: carrier trước purchase, actual trước purchase
  hoặc actual trước carrier. Missing optional milestone không tự bị coi là invalid.
  Approval chronology giữ nguyên ở Silver warning, không nằm trong delivery flag.
- `is_delivery_observed`: status delivered, actual tồn tại và không có chronology
  lỗi ở trên. Actual timestamp của shipped/canceled/unavailable vẫn giữ để trace,
  không vào completed-delivery metrics. Delivered thiếu actual không được suy diễn.
- `delivery_delay_days`: signed difference giữa actual và estimated **local calendar
  dates** tại America/Sao_Paulo. Early âm, cùng ngày hẹn bằng 0, ngày sau hẹn dương.
  Không so actual instant với estimated midnight để kết luận trễ trong cùng ngày.
- `delivery_elapsed_hours`: elapsed UTC purchase → actual; `carrier_to_delivery_hours`:
  UTC carrier → actual. Dùng EXTRACT(EPOCH)/3600 dạng NUMERIC, không làm tròn trước
  AVG; qua DST, một ngày local có thể chỉ 23 giờ. Transit cần carrier timestamp.
- Delay/late flag/durations là NULL nếu không có valid delivered observation;
  không clamp negative input durations thành 0 hoặc dùng CURRENT_TIMESTAMP để đo
  overdue. Local dates dùng explicit AT TIME ZONE, không phụ thuộc DB session TZ.

`mart_delivery_daily` group theo **purchase date**, không phải delivery-day throughput:

- Counts: total orders, delivered statuses, delivered thiếu actual, invalid chronology,
  valid observed deliveries, transit observations, late và on-time deliveries.
- `late_delivery_rate = late_delivery_count / observed_delivery_count` (ratio 0..1).
  Empty denominator cho NULL; không biến undelivered/incomplete thành on-time.
- `avg_delivery_hours` chỉ dùng observed deliveries; transit mean chỉ dùng observations
  có carrier, denominator công khai ở `transit_observation_count`. Không cộng/average
  daily rates/means để lấy toàn kỳ: cần dùng counts hoặc fact-level durations.

Counts lỗi/completeness vẫn nhìn thấy dù bị loại khỏi duration/rate denominator.
Marts là current mutable cohort, không reconstructed historical snapshots; không
suy đoán refund, carrier SLA hay shipping events ngoài timestamps có trong orders.
Hai models dành cho analytics reader; không mở rộng sáu-model BI grant scope.

`delivery_fact_reconciliation` và `delivery_metric_reconciliation` đối chiếu exact
milestones, mappings, NULLs, chronology, local days, numeric durations và daily
counts/rates với raw staging. Independent fixtures kiểm tra same-day midnight,
early/late, Sao Paulo DST, session timezone, missing carrier/actual, canceled actual,
invalid ordering, empty inputs và cố tình corrupt duration để xác minh gates fail.

## Location dimension

Geolocation chạy full-only standalone hoặc core; `dim_location` aggregate trước mọi join.
Customer/seller dimensions LEFT JOIN theo source ZIP để thêm nullable location_key;
unmatched ZIP giữ NULL, source city/state và business hash contract giữ nguyên.
Không drop rows hoặc thêm location key vào facts. Xem [geolocation contract](18_geolocation_snapshots.md) về exact
medians, deterministic joint modes, completeness và giới hạn core receipts/BI.

## Calendar dimension and fact date roles

`dim_date` là continuous calendar từ min đến max của purchase date đã normalize
và estimated/actual delivery, review creation dates chuyển rõ bằng `America/Sao_Paulo`. Bao gồm
ngày không có orders; không có source dates thì dimension rỗng. Dùng integer day
offsets, không dùng timestamp series hoặc session timezone. Không tự thêm future
dates, holiday/fiscal calendar hay unknown member.

`date_key` là integer YYYYMMDD, ổn định qua rebuild. Attributes gồm calendar
năm/quý/tháng/ngày, `iso_year`, `iso_week`, `iso_day_of_week` (Monday=1) và
`is_weekend` (Saturday/Sunday). ISO week phải ghép với ISO year: 2019-12-30 thuộc
2020-W01; 2021-01-01 thuộc 2020-W53.

Bốn purchase-based facts giữ nguyên date columns và thêm `order_purchase_date_key`. Items/payments
kế thừa purchase key từ order; delivery thêm `estimated_delivery_date_key` và
`delivered_date_key`. Actual key NULL khi timestamp thiếu; chronology/status không
xóa actual date key dù observation bị loại khỏi metrics. Đây là role-playing date
joins, không thêm rows, phân bổ tiền hoặc đổi eligibility. Không cấp BI quyền đọc
dimension. Review fact dùng `review_creation_date_key` riêng. Lịch là view theo current staging: sửa source bounds có thể đổi phạm vi.

`date_dimension_reconciliation` đối chiếu source bounds, từng ngày và attributes;
`fact_date_reconciliation` so date keys với raw staging, phát hiện cả valid key
nhưng trỏ sai ngày. Generic tests kiểm tra uniqueness, not-null và relationships.
Fixtures kiểm tra giao năm ISO, leap day, gaps, delivery dates ngoài purchase range,
NULL actual, session timezone, empty sources và corruption failure gates.

## Local Verification

Full run `dataset_57c523f030ca4fe3b9bb88710412096f` ngày 2026-10-22 SUCCESS:
26 audited tasks; dbt PASS=100 (17 view models, 83 data tests), không skip/error.
`fct_order_items` có 112,621 rows và 112,621 unique composite keys. Tổng GMV
13,491,218.65 BRL và freight 2,240,547.03 BRL khớp exact giữa product/seller/order
marts. Health HEALTHY cho sáu datasets; BI publication available.

Evidence ignored: `build/reports/dataset/<run_id>_pipeline.json`, run-specific
`<run_id>_dbt/run_results.json`, `build/reports/item-models-metrics.txt` và
`build/reports/item-models-health.json`. Automated checks: 117 unit và 20 disposable
integration tests (9 dbt, full gate, incremental, backfill, committed-stage recovery);
package build thành công. Chưa thêm product/seller cards hoặc grants vào Metabase.

### Payment Expansion

Full run `dataset_e0a5a4b13c8f4e3f81662efec1854712` ngày 2026-10-22 SUCCESS:
26 tasks, 122 dbt nodes (19 views, 103 data tests), không error/skip. Payment fact
có 103,869 rows và unique composite keys. Gross paid 15,736,441.59 BRL khớp exact
giữa payment-type và order marts. Health HEALTHY, BI publication available.

Evidence ignored: run-specific pipeline/dbt reports ở `build/reports/dataset/`,
`build/reports/payment-models-metrics.txt`, `build/reports/payment-models-health.json`.
117 unit tests và 22 distinct integration cases đã kiểm chứng: 11 dbt cases
(orphan-payment case được rerun sau khi sửa test để kiểm tra view đã tồn tại),
11 full/incremental/backfill/recovery cases; package build thành công. Mixed-type
fixture giữ gross paid 43.25 BRL; cố tình nhân installment làm cả hai payment
reconciliation tests fail. Không mở thêm quyền hoặc cards Metabase.

### Delivery Expansion

Local locked dbt build ngày 2026-10-22: PASS=149 (21 views, 128 data tests), không
error/skip. Build dùng staging SHARE locks; verification read-only đối chiếu fact
và daily counts, giữ nguyên mọi pipeline control. Không chạy source extraction
hoặc tạo một pipeline SUCCESS mới chỉ cho thay đổi modeling này.

99,424 order rows/unique IDs được giữ. Có 189 chronology flags, 8 delivered rows
thiếu actual và 6 non-delivered rows có actual. Policy cho 96,266 observed
valid deliveries: 6,529 late, 89,737 on-time; late ratio
0.06782249184551139551. Transit duration có 96,265 observations. Counts của fact
và mart khớp source; các rows có lỗi/thiếu timestamp không bị xóa khỏi fact.

117 unit và 24 distinct integration cases đã kiểm chứng (13 dbt, full gate,
6 incremental, 2 backfill, 2 recovery); timeline case được rerun sau khi thêm
session `Asia/Tokyo` và actual-before-purchase assertions. Corrupt duration làm
cả hai delivery reconciliation tests fail. Package build thành công.

Evidence ignored: `build/reports/delivery_dbt/run_results.json`,
`build/reports/delivery-models-local.json`, `build/reports/delivery-models-health.json`.
Health HEALTHY, BI available; dedicated BI reader vẫn bị từ chối đọc hai delivery
models. Modeling build không chứng nhận historical source versions hoặc carrier SLA.

### Calendar Expansion

Local locked dbt build ngày 2026-10-22: PASS=167 (22 views, 145 data tests), không
error/skip. Lịch có 800 ngày liên tục từ 2016-09-04 đến 2018-11-12. Purchase keys
map đúng cho 99,424 orders, 112,621 items, 103,869 payments và 99,424 delivery rows;
không làm thay đổi các grains hoặc pipeline controls. Có 99,424 estimated keys,
96,460 actual keys; 194 actual timestamps không đủ điều kiện valid observation
vẫn giữ date key để truy vết.

Build giữ staging SHARE locks; không extract hoặc tạo pipeline SUCCESS mới cho
thay đổi modeling. Health HEALTHY, BI available; dedicated BI reader bị từ chối
đọc `dim_date`. Evidence ignored: `build/reports/calendar_dbt/run_results.json`,
`build/reports/calendar-models-local.json`, `build/reports/calendar-models-health.json`.

117 unit và 26 distinct integration cases pass: 15 dbt, 1 full gate, 6 incremental,
2 backfill và 2 recovery. Calendar boundary case được rerun sau khi thêm actual/
estimated dates nằm ngoài purchase bounds. Valid-but-wrong date key vẫn qua
relationship test nhưng fail source reconciliation; corrupt ISO year cũng fail.
Ruff, contract/config/shell checks và package build thành công.

## References

Profile và environment theo [dbt profiles](https://docs.getdbt.com/docs/local/profiles.yml);
execution/tests theo [dbt build](https://docs.getdbt.com/reference/commands/build).

## Review facts and creation cohorts

`stg_reviews` đọc current version-aware staging ở grain `(review_id, order_id)`.
`fct_reviews` giữ đầy đủ composite reviews, không chọn một review/order, không
join orders/items/payments hoặc mang doanh thu vào review grain. Scope là observed
valid reviews qua mọi order statuses, không suy diễn mỗi order có review.

Fact giữ UTC creation/answer timestamps và explicit São Paulo creation date/key.
`dim_date` thêm review creation dates vào observed bounds, kể cả review-only inputs;
calendar rỗng chỉ khi không có observed order hoặc review dates. Đây không phải
purchase/delivery cohort. Answer-before-creation giữ flag và score, không suy diễn
customer-service response SLA. Comment title/message chỉ nằm ở protected staging;
fact giữ hai presence flags, không phân tích sentiment từ text.

`mart_review_daily` có một row/ngày thực sự có reviews:

- `review_count`: số composite reviews, không phải số distinct review IDs.
- `reviewed_order_count`: COUNT DISTINCT order_id trong ngày; một order có reviews
  ở nhiều ngày xuất hiện trong nhiều cohorts, không cộng daily counts thành total.
- `score_sum`, `average_score`: SUM(score), AVG(score), mỗi review có weight bằng 1.
  Whole-period mean = SUM(score_sum)/SUM(review_count), không AVG(daily averages).
- `low_score_count/rate`: số/tỷ lệ score 1–2 trên toàn review_count; đây là ngưỡng
  score do project định nghĩa, không khẳng định sentiment hoặc order-level rating.
- `commented_review_count/comment_rate`: nonblank title OR message, đếm một lần
  nếu có cả hai. Aggregate counts trước khi chia rates qua nhiều ngày.

Empty input sinh zero fact/mart rows; calendar không tạo synthetic zero-review
observations. Full reviews snapshots không xóa staging cũ. Backfill có thể có valid
reviews mà order tương ứng nằm ngoài bounded staging window; Silver đã pin full
orders trong cùng source transaction. Fact không drop rows hoặc invent order/customer
context trong trường hợp đó. Review analytics không đổi GMV/AOV hoặc BI grants.

Source-grain, composite fact grain, exact fact attributes/date/flags và daily
metrics được đối soát trực tiếp raw staging, độc lập với upstream aggregates.
Ngày/key hợp lệ nhưng sai, valid score bị đổi hoặc coherent wrong daily averages
phải làm dbt tests fail. Core gate yêu cầu đủ tám declared dbt sources.

## Review Analytics Verification (2026-10-23)

Locked standalone dbt build trên canonical staging, baseline core run
`dataset_7f462f5e178f448d88480321352623c3`: PASS=226 (28 views, 198 tests),
WARN/ERROR/SKIP=0 và đúng tám declared sources. Tám staging SHARE locks được giữ
qua build và đối soát; current code hash được kiểm tra trước/sau. Đây là analytics
validation, không tạo core run mới hoặc promote completion cursors.

`fct_reviews`: 99,197 rows, 98,383 distinct review IDs, 98,646 distinct orders,
score_sum=405,361, mean=4.0864239845963084, low_score_count=14,571 và
commented_review_count=42,683. Không có answer-before-creation trong valid staging
của dataset này. Fact không chứa comment text.

`mart_review_daily`: 636 cohorts từ 2016-10-02 đến 2018-08-31. Tổng review_count,
score_sum, low-score/comment counts và weighted mean khớp raw staging. Sum daily
distinct orders=99,040 khác whole-period distinct orders=98,646; không sử dụng
SUM(daily reviewed_order_count) làm số orders toàn kỳ.

Existing facts giữ counts orders/delivery=99,424, items=112,621, payments=103,869.
Pipeline controls, deferred statuses/attempts và số warehouse load receipts không
thay đổi. Health HEALTHY, BI publication vẫn available; business reader không đọc
được staging/fact/mart reviews. Metabase UI/cards không được mở rộng trong task này.

Evidence ignored: `build/reports/review-analytics-local.json`,
`review-analytics-local-build.log`, `review-analytics-health.json` và artifacts tại
`build/reports/review-analytics-local-dbt/`. Không chạy lại toàn bộ ingestion trên
Olist hoặc GitHub CI; full/incremental/backfill/recovery gates được kiểm tra bằng
fixtures riêng.

### Regression Coverage

126 fast unit tests, 21 unique analytics integration cases và 11 core pipeline
cases đã được xác nhận PASS. Analytics suite gồm grain, source reconciliation,
weighted metrics, local/DST dates, reviews-only/empty behavior, no revenue fan-out,
BI denial và valid-but-wrong fact/mart corruption. Hai copied-view corruption
fixtures được rerun sau khi cấp SELECT cho test loader; giữ cả initial suite log
và targeted recheck log để phân biệt lỗi fixture với kết quả cuối cùng.

Pipeline verification gồm full gate (1), incremental (6), isolated backfill (2)
và recovery (2), với fresh 226-node builds và source set tám tables. Backfill so
sánh cả review fact/mart với independent full reload; recovery giữ rollback/CAS,
immutable receipts và original retry attempts. Lint, formatter và wheel/sdist
build được kiểm tra trong cùng thay đổi.

Logs ignored: `build/reports/review-analytics-fast.log`, `review-analytics-dbt.log`,
`review-analytics-dbt-recheck.log`, `review-analytics-gates.log` và
`review-analytics-final-checks.log`.

## Dimension Policy Verification (2026-10-23)

126 fast unit tests, 7 targeted analytics integration cases và full pipeline gate
(1 case) đã PASS. Fixtures xác nhận wrong-but-unique unused customer/product/seller
keys, wrong current customer representative và synthetic unknown rows đều làm
`dimension_key_reconciliation` fail trong khi generic constraints vẫn pass.
Equal-sequence customer tie chọn customer_id DESC. Existing strict missing-parent,
optional location NULL, all-empty dimensions và reviews-only calendar cases vẫn PASS.
Full fixture gate build đủ 227 nodes (28 views, 199 tests), không skip/error.

Trên Olist, chỉ chạy selected `dimension_key_reconciliation` dưới tám staging SHARE
locks; PASS với current code hash, không rebuild views hoặc tạo pipeline execution.
Counts đọc tại cùng lần kiểm tra: customer=96,096, product=32,951, seller=3,095,
location=19,015 và date=800. Đây không phải full 227-node rebuild trên Olist.
Controls, deferred statuses/attempts và số warehouse load receipts giữ nguyên.

Logs/artifacts ignored: `build/reports/dimension-policy-fast.log`,
`dimension-policy-integration.log`, `dimension-policy-gate.log`,
`dimension-policy-local.json`, `dimension-policy-local.log`,
`dimension-policy-local-dbt/` và `dimension-policy-final-checks.log`.
Ruff/format và wheel/sdist build PASS. Không chạy lại toàn bộ ingestion,
incremental/backfill/recovery suites hoặc GitHub CI trong thay đổi này; các modes
cùng sử dụng manifest-based final gate, node-count expectations đã cập nhật 227.

## Customer Value and Repeat Purchase

`mart_customer_value` bắt đầu từ `dim_customer` ở shopper grain
`customer_unique_id`/`customer_key`, LEFT JOIN `fct_orders` đã aggregate items và
payments riêng. Một shopper có nhiều order-scoped customer IDs vẫn chỉ một row.
Shoppers chưa có order giữ counts/amounts=0, AOV và first/last timestamps=NULL,
repeat flag=false. Cancel-only shoppers có total_order_count nhưng không eligible.

- `total_order_count` gồm mọi status; eligible/paid counts dùng shared eligible
  statuses, paid order phải có summed payment > 0. Không coi missing payment như
  observed zero payment: giữ riêng orders_without_payments/items.
- GMV/freight/gross paid là observed eligible sums, BRL exact NUMERIC không narrow
  aggregate precision. Paid AOV chia positive-paid sum cho paid_order_count, ROUND
  hai decimals; không average order/customer/day means để gộp AOV.
- First/last eligible purchase là min/max observed UTC instants, không khẳng định
  acquisition date hoặc lịch sử lifetime đã đầy đủ. Đây là observed value, không
  phải predicted customer lifetime value, net revenue hoặc profit.
- `is_repeat_customer` là ít nhất hai eligible orders hiện có trong staging, kể cả
  unpaid/zero-paid. Không đưa canceled/unavailable orders vào repeat denominator.

`mart_repeat_purchase_daily` rank eligible orders theo UTC purchase timestamp rồi
order_id trong mỗi shopper; thứ tự tie chỉ phục vụ deterministic classification,
không khẳng định thứ tự nghiệp vụ giữa các simultaneous orders. First order là
first **observed eligible** order, các order còn lại là repeat. Một ngày có thể
chứa cả first và repeat orders của cùng shopper.

Mart group theo São Paulo purchase date từ Silver, không theo source update date.
`first_order_count + repeat_order_count = eligible_order_count`;
repeat_order_rate = repeat_order_count / eligible_order_count. Gộp counts trước
khi chia; purchasing/repeat_customer_count distinct trong ngày không additive qua
ngày. Không có eligible orders thì mart rỗng, không tạo zero-observation cohorts.

Late earlier orders hoặc current status changes có thể đổi first/repeat classification
và các ngày cũ khi view rebuild. Backfill dùng shared current staging, không tái dựng
lịch sử còn thiếu hoặc một point-in-time retention cohort. No fixed observation
window, acquisition/retention/churn rate, RFM hoặc survival model được suy diễn.

`mart_order_status_daily` giữ tất cả observed current statuses theo local purchase
cohort, mỗi order một lần, distinct shoppers chỉ trong status/date. Không dùng nó
như status-transition history hoặc cộng customer_count qua groups thành total.
Không join review/item/payment rows vào các status counts.

Các marts dùng existing reader role; không thêm grants/cards cho business BI.
Source-based reconciliations tính expected từ raw orders/customers và independent
per-order item/payment sums, bắt wrong grain, extra/missing groups, money/rate sai.
Empty input, canceled-only/no-order shoppers, DST local dates, late earlier orders,
zero/missing payments và aggregate sums vượt source precision có fixtures riêng.

## Customer Marts Verification (2026-10-23)

126 fast unit tests, 8 targeted analytics cases và 3 full/backfill pipeline cases
đã PASS. Fixtures bao phủ shopper có nhiều customer IDs, no-order/canceled-only
members, zero/missing payments, exact BRL aggregate vượt source-row precision,
São Paulo DST dates, late earlier order reclassification, empty input, unchanged
BI grants và wrong-but-valid metrics. Dimension corruption expectations được cập
nhật để customer reconciliation cùng bắt wrong keys/synthetic members. Backfill
đối chiếu cả ba marts với independent full reload, giữ scheduled cursors/foreign queue.

Locked standalone build trên Olist PASS=261 (31 views, 230 tests), đúng tám sources,
WARN/ERROR/SKIP=0. Tám staging SHARE locks giữ qua fresh build/đối soát; current code
hash được kiểm tra trước/sau. Đây không phải ingestion hoặc core run mới.

- Customer value có 96,096 shoppers; 17 chưa có staged orders, 2,888 repeat shoppers,
  1,123 NULL AOV. Total orders=99,424, eligible=98,191, positive-paid=98,190.
- Observed eligible GMV=BRL 13,491,218.65, freight=2,240,547.03 và gross paid=
  15,736,441.59; khớp existing daily marts, không gọi gross paid là net revenue.
- Repeat mart có 614 ngày từ 2016-09-04 đến 2018-09-03: first orders=94,974,
  repeat orders=3,217, tổng=98,191; aggregate repeat-order rate=
  0.03276267682374148344. Không phải tỷ lệ retention toàn bộ lifetime.
- Status counts: delivered=96,462, shipped=1,107, invoiced=314, processing=301,
  created=5, approved=2, canceled=625, unavailable=608; tổng=99,424.

Existing fact counts giữ nguyên orders/delivery=99,424, items=112,621,
payments=103,869. Pipeline controls, deferred statuses/attempts và số warehouse
load receipts không đổi. Health HEALTHY, publication available; business reader
không đọc được ba marts mới. Không thêm Metabase cards hoặc mở service.

Evidence ignored: `build/reports/customer-marts-fast.log`,
`customer-marts-integration.log`, `customer-marts-gates.log`,
`customer-marts-local-build.log`, `customer-marts-local.json`,
`customer-marts-health.json`, `customer-marts-local-dbt/` và
`customer-marts-final-checks.log`. Ruff/format và package build PASS. Không chạy lại
full ingestion, toàn bộ analytics/incremental/recovery suites hoặc GitHub CI trong
lượt này; các node-count expectations cho core gates cùng cập nhật 261.

## Documentation Catalog and Lineage

`dbt/models/schema.yml` mô tả grains, units, time semantics, null behavior và metric
limits cho 31 models/304 output columns, cùng 8 raw staging sources. Giữ tests hiện
có khi cập nhật descriptions. Source catalog có thêm technical lineage columns;
coverage 304 columns nói về exported analytics models, không khẳng định mọi source
technical column đều có column description riêng.

Sau successful build, dùng cùng PG/dbt environment:

```bash
make dbt-docs
# Optional local viewer, port independent of Metabase:
uv run --locked --extra analytics dbt docs serve --project-dir dbt --profiles-dir dbt --target-path "$PWD/build/reports/dbt-docs" --host 127.0.0.1 --port 8081
```

`DBT_DOCS_OUTPUT` đổi target directory; default ignored `build/reports/dbt-docs/`.
`index.html` là docs viewer, manifest chứa model descriptions/SQL/ref dependencies,
catalog chứa database relation/column metadata. Cần relations hiện có để catalog
đối chiếu đúng; docs generation không chạy data tests, build/rebuild relations,
register load receipts hoặc promote completion cursors. Không dùng docs SUCCESS
thay cho receipt/dbt completion gate. Artifacts giữ local, không commit hoặc publish.

`test_docs_catalog_covers_all_models_columns_and_sources` thực thi build và docs
trong disposable PostgreSQL, kiểm tra same invocation, empty catalog errors, đủ
sources/models và exact column membership/descriptions. Test này được chạy qua
`make test-dbt-integration` trong existing CI job. README có grouped lineage; graph
chi tiết lấy từ manifest/docs, không coi arrows là immutable publication boundaries.

### Documentation Verification (2026-10-23)

126 unit tests và real docs integration case PASS. Integration build đủ 261 nodes
(31 views/230 tests) trên fixture trước docs generation; catalog có đủ 31 models,
304 model columns với nonblank descriptions và 8 described sources, không catalog
errors hoặc stale/missing documented columns. Viewer index được tạo thành công.

`make dbt-docs` chạy trên existing Olist relations dưới tám staging SHARE locks:
metadata coverage khớp các counts trên. Manifest model SQL checksums và dbt node IDs
không thay đổi so với successful customer-marts build. Definitions của analytics
views, controls, deferred statuses/attempts và warehouse load receipt count giữ nguyên.
Không chạy data tests/rebuild Olist relations trong docs command.

README Mermaid được đối chiếu với generated manifest: đủ 31 model nodes và đúng
39 direct model-ref edges; raw sources được gom thành một node. Đây là structural
validation, không tuyên bố đã visual-review browser UI. Generated artifacts ở
`build/reports/dbt-docs/`; evidence: ignored `build/reports/dbt-docs-local.json`,
`dbt-docs-local.log`, `dbt-docs-integration.log` và `dbt-docs-final-checks.log`.
Không public-host docs, rerun ingestion/pipeline integration suites hoặc GitHub CI.

## Fact Integrity Acceptance

Required FK, BRL và time behavior được kiểm tra theo grain thay vì chỉ kiểm tra
một surrogate key có tồn tại. Generic not-null/relationships bảo vệ membership;
raw-source reconciliation bảo vệ đúng business association và payload.

| Fact | Required relationships | Exact amounts and time semantics |
| --- | --- | --- |
| `fct_orders` | Shopper key và purchase date key; source customer/order mapping giữ nguyên | Independent per-order item/payment sums; UTC purchase instant, normalized local date, current status và eligibility khớp staging |
| `fct_order_items` | Order, current product/seller/shopper và purchase date | Composite item grain; BRL price/freight và UTC shipping deadline khớp source, không phân bổ payments |
| `fct_payments` | Order/shopper và purchase date | Composite entry grain; BRL payment_value không nhân installments; order context khớp source |
| `fct_delivery` | Order/shopper và purchase/estimated date; actual date key khi timestamp có | UTC elapsed hours, São Paulo calendar lateness; chronology/status không xóa actual date key hoặc biến missing observation thành on-time |
| `fct_reviews` | Review creation date; full-order membership pin tại Silver | Composite review grain, UTC timestamps và local creation date; không yêu cầu bounded order staging có full parent set |

`fact_reconciliation` đối chiếu source customer_id, customer_unique_id và MD5
shopper key, status, purchase timestamp/date và is_metric_eligible cùng counts/
amounts. Một valid key của shopper khác hoặc timestamp vẫn hợp lệ nhưng sai phải
fail dù generic FK/not-null tests PASS. Không sửa SQL model để che sai staging.
Local purchase date là Silver-normalized source attribute; review/delivery dates
được derive với explicit timezone, không dựa session timezone.

Source money giữ NUMERIC(18,2); SUM/derived analytics dùng exact NUMERIC không
narrow về source precision hoặc float. Fixture có hai entries cùng order với
9,000,000,000,000,000.01 BRL mỗi entry: aggregate vượt source typmod vẫn exact,
paid-order AOV round hai decimals và zero-paid denominator giữ NULL. Quy mô đó
là boundary fixture, không phải số liệu kinh doanh Olist.

Acceptance tests bao gồm missing required parents mà không drop child rows,
valid-but-wrong FK/date/identity attributes, composite grain, no child fan-out,
UTC/São Paulo/DST behavior, actual-key completeness và reviews-only backfill.
SQL model algorithms, output schema, BI grants và source data giữ nguyên trong
đợt củng cố acceptance này; chỉ tăng độ đầy đủ của raw-source order fact gate.

### Fact Integrity Verification (2026-10-23)

126 unit tests, 10 targeted analytics cases và full pipeline gate (1 case) PASS.
Order-fact corruption fixture cài từng customer_id, shopper ID/key, status,
purchase UTC instant/local date và eligibility sai; cả bảy generic order checks
vẫn PASS nhưng raw `fact_reconciliation` FAIL đúng yêu cầu. Fixtures missing
customer/order/product/seller, exact large BRL aggregates/NULL AOV, valid-but-wrong
date keys, DST delivery, ISO/leap calendar, review grain/local dates và reviews-only
empty order staging đều PASS. Full fixture gate build đủ 261 nodes, zero skip/error.

Trên Olist, selected test run dưới tám staging SHARE locks PASS cả sáu gates:
order/item/payment/delivery/review fact reconciliation và fact date reconciliation.
Current code hash được đối chiếu; views không rebuild, controls, deferred attempts
và warehouse load receipt count giữ nguyên. Existing fact counts là orders/delivery
99,424, items 112,621, payments 103,869, reviews 99,197.

Metadata check xác nhận sáu money columns của ba money facts là NUMERIC; ba
order-aggregate amounts không có narrowed numeric typmod. Purchase date so với
UTC timestamp chuyển rõ São Paulo có zero mismatches trên Olist. Đây là kiểm tra
selected integrity/schema, không phải full 261-node Olist rebuild hoặc ingestion mới.

Logs/artifacts ignored: `build/reports/fact-integrity-fast.log`,
`fact-integrity-integration.log`, `fact-integrity-gate.log`, `fact-integrity-local.log`,
`fact-integrity-local.json`, `fact-integrity-local-dbt/` và
`fact-integrity-final-checks.log`. Ruff/format, wheel/sdist PASS. Không chạy lại
incremental/backfill/recovery suites hoặc GitHub CI trong thay đổi này; không đổi
SQL model algorithms, output columns, BI grants hoặc dbt node counts.
