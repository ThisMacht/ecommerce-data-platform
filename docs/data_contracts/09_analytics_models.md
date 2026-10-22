# Analytics Models and Metrics

## Scope and execution

Status: **Active — six core staging datasets plus standalone geolocation**.
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
profile. `make dataset` tự cấu hình local credentials, chạy dbt sau sáu staging loads,
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
| `int_geolocation_by_zip`, `dim_location` | Một ZIP với exact medians, joint city/state mode và stable Type 1 location key; standalone observations |
| `dim_date` | Một ngày liên tục giữa min/max observed local purchase/estimated/actual dates; integer YYYYMMDD key |
| `dim_customer` | Một `customer_unique_id`; current attributes, stable surrogate key |
| `fct_orders` | Một order; join hai summaries để tránh items × payments fan-out |
| `fct_order_items` | Một `(order_id, order_item_id)`; order/customer/product/seller keys, price và freight ở item grain |
| `mart_product_daily`, `mart_seller_daily` | Một local purchase date × product/seller; item/order counts, GMV và freight |
| `fct_payments` | Một `(order_id, payment_sequential)`; observed payment amount, type, installments và order/customer context |
| `mart_payment_type_daily` | Một local purchase date × payment type; entry counts, group distinct orders và gross paid |
| `fct_delivery` | Một order với UTC milestones, local delivery dates, chronology/completeness flags và durations |
| `mart_delivery_daily` | Purchase-date cohort: observed delivery counts, late rate và mean elapsed hours |
| `mart_revenue_daily` | Một local purchase date: GMV, freight, gross paid và completeness counts |
| `mart_average_order_value` | Một local purchase date: paid amount, paid order count và AOV |

`fct_orders` giữ `customer_id`, shopper ID và `customer_key`. Missing customer không được
inner join để âm thầm drop order hoặc gán unknown member: relationship/not-null
tests làm fail build. Orphan child rows cũng làm fail relationship tests.
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

Geolocation chạy standalone; `dim_location` aggregate observations trước mọi join.
ZIP không phải source key; không gắn current location vào customer/seller/facts
trong bước này. Xem [geolocation contract](18_geolocation_snapshots.md) về exact
medians, deterministic joint modes, completeness và giới hạn core receipts/BI.

## Calendar dimension and fact date roles

`dim_date` là continuous calendar từ min đến max của purchase date đã normalize
và estimated/actual delivery dates chuyển rõ bằng `America/Sao_Paulo`. Bao gồm
ngày không có orders; không có source dates thì dimension rỗng. Dùng integer day
offsets, không dùng timestamp series hoặc session timezone. Không tự thêm future
dates, holiday/fiscal calendar hay unknown member.

`date_key` là integer YYYYMMDD, ổn định qua rebuild. Attributes gồm calendar
năm/quý/tháng/ngày, `iso_year`, `iso_week`, `iso_day_of_week` (Monday=1) và
`is_weekend` (Saturday/Sunday). ISO week phải ghép với ISO year: 2019-12-30 thuộc
2020-W01; 2021-01-01 thuộc 2020-W53.

Bốn facts giữ nguyên date columns và thêm `order_purchase_date_key`. Items/payments
kế thừa purchase key từ order; delivery thêm `estimated_delivery_date_key` và
`delivered_date_key`. Actual key NULL khi timestamp thiếu; chronology/status không
xóa actual date key dù observation bị loại khỏi metrics. Đây là role-playing date
joins, không thêm rows, phân bổ tiền hoặc đổi eligibility. Không cấp BI quyền đọc
dimension. Lịch là view theo current staging: sửa source bounds có thể đổi phạm vi.

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
