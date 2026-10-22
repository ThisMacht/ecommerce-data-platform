# Analytics Models and Metrics

## Scope and execution

Status: **Active — dbt views for six staging datasets**.
`dbt/` dùng locked `analytics` extra, PostgreSQL adapter và schema `analytics`
được tạo bởi migration `007`. Existing loader role sở hữu schema; reader chỉ SELECT. Migration `012` thêm
`olist_bi_reader` NOLOGIN group chỉ đọc fct_orders, hai daily marts và publication-status view; dbt giữ grants của ba models qua rebuild.
Không dùng dbt packages hoặc remote model dependencies.

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
| `dim_customer` | Một `customer_unique_id`; current attributes, stable surrogate key |
| `fct_orders` | Một order; join hai summaries để tránh items × payments fan-out |
| `mart_revenue_daily` | Một local purchase date: GMV, freight, gross paid và completeness counts |
| `mart_average_order_value` | Một local purchase date: paid amount, paid order count và AOV |

Fact giữ `customer_id`, shopper ID và `customer_key`. Missing customer không được
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
Atomic mart publication vẫn planned. Product/seller membership đã có trong [Silver v2 standalone](17_item_dependency_snapshots.md); core jobs chưa bật checks này. Local Airflow full/incremental/backfill wrappers đã có;
Metabase business dashboard, dedicated reader grants và audit-based publication
gate theo [contract 14](14_business_dashboard.md). Local run/task audit và watermark gate đã có theo [pipeline contract](11_pipeline_control.md). Deferred retry/expiry đã có theo [lifecycle contract](10_deferred_lifecycle.md).

## References

Profile và environment theo [dbt profiles](https://docs.getdbt.com/docs/local/profiles.yml);
execution/tests theo [dbt build](https://docs.getdbt.com/reference/commands/build).

Products có [standalone Type 1 slice](15_product_snapshots.md); full dbt build
bao gồm `stg_products` và `dim_product`. Items chưa join product dimension;
membership có trong [Silver v2 standalone](17_item_dependency_snapshots.md).

Sellers có [standalone Type 1 slice](16_seller_snapshots.md), giữ grain `seller_id`,
location warnings và current attributes; chưa join seller dimension vào items.
