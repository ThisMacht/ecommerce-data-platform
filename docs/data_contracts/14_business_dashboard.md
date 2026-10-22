# Local Business Dashboard

## Scope and startup

Metabase local đọc `mart_revenue_daily`, `mart_average_order_value`, `fct_orders`
và `bi_publication_status` qua dedicated reader. Không query trực tiếp source,
Bronze/Silver hoặc raw staging. Migration `012` tạo NOLOGIN group `olist_bi_reader`
và publication-status view; dbt giữ grants trên ba business models qua rebuild.

Sau khi `make dataset` hoặc incremental/backfill đã SUCCESS:

```bash
make metabase-up
# Reconcile the managed dashboard after changing its definition:
make metabase-setup
# Stop only Metabase; keep metadata and credentials:
make metabase-down
```

Compose tự đọc `.env`; `METABASE_PORT` mặc định `3000`, chỉ bind `127.0.0.1`.
Metabase image được pin digest; runtime hiện tại báo version `v0.63.18.5`.
`make metabase-up` apply migrations, chờ healthcheck và gọi bootstrap REST API.
Không tự chạy lại dataset để refresh dashboard hoặc sửa freshness warnings.

## Credentials and managed resources

Bootstrap tạo random admin/reader passwords trong ignored
`build/metabase/credentials.json`, mode `0600`; lấy `email`/`password` ở file này
để đăng nhập UI. Không commit, đưa credentials vào README hoặc chia sẻ file.
`dashboard.json` chứa URL/card IDs/query outputs, `verification.json` là local
smoke evidence nếu đã thu thập; các files này không phải published data snapshots.

Login `olist_metabase_reader` chỉ inherit `olist_bi_reader`, mặc định read-only.
Group chỉ SELECT bốn views kể trên; không có schema USAGE ở `olist`, `raw_staging`
hoặc `audit`, và không được đọc `analytics.stg_*`. Existing broad source reader
không được dùng làm datasource Metabase.

Collection `Olist Core Analytics`, datasource `Olist Analytics Reader` và dashboard
`Olist Business Overview` thuộc bootstrap. Chạy lại reconcile cards/layout/filters,
giữ resource IDs và không tạo duplicate names. Không sửa tay managed cards; đặt
các dashboard tự tạo ở collection khác. Setup từ chối duplicate names hoặc
unmanaged card/collection collisions. Lỗi API không in response/password ra logs.

Giữ file credentials cùng named volume `metabase_data` khi reboot/backup. Không
xóa credentials để reset một Metabase instance đã initialized; restore file hoặc
cập nhật đúng admin password sau khi đổi trong UI. Không dùng `down -v` để stop.

## Metrics and filters

Dashboard có bốn KPI cards, daily revenue/AOV trends, order-status breakdown,
completeness table và current publication status. Policy hiển thị ngay trên dashboard:

- Tiền là BRL; dates là local purchase date `America/Sao_Paulo`.
- Eligible: delivered, shipped, invoiced, processing, approved, created.
- Canceled/unavailable không vào money KPIs, nhưng vẫn hiện trong status breakdown.
- GMV không gồm freight. Gross paid là observed payments, không phải net revenue.
- AOV toàn khoảng = `ROUND(SUM(paid_amount)/NULLIF(SUM(paid_order_count),0),2)`;
  không average daily AOV. Chỉ eligible positive-payment orders vào denominator.
- Start/end date đều inclusive; để trống hiển thị toàn bộ lịch sử.
- Không có paid orders: AOV NULL. Empty valid window: additive KPIs zero.
- Missing items/payments được đếm riêng; zero amount không chứng minh source đầy đủ.

`config/metabase_dashboard.json` version SQL, visualizations và layout; amounts
aggregate bằng PostgreSQL NUMERIC, không tính metric qua Python float.

## Publication gate and limits

`bi_publication_status.available` chỉ true khi run gần nhất SUCCESS và không có
warehouse load audit mới hơn run đó. Nếu unavailable hoặc chưa có pipeline run,
eight metric cards trả no results; status card giải thích run/status/available.
Bootstrap mới cũng từ chối khi gate chưa đạt. Setup không promote watermark.

Gate áp dụng lúc query; refresh dashboard để cập nhật sau pipeline run, không có push invalidation của dữ liệu đã render.
Gate đọc audit state, không verify lại S3 receipts cho mỗi dashboard request.
Các dbt models vẫn là live views, không atomic snapshots xuyên các cards. Untracked
writers bỏ qua load audit không được phát hiện. H2 metadata trên named volume chỉ
phục vụ local demo; chuyển sang PostgreSQL metadata trước shared deployment và
backup metadata khi Metabase đã stop trước khi nâng version/digest.

## Verification

`make test` kiểm tra private/stable credentials, native date tags, ownership
ambiguity và sanitized HTTP errors. `make test-dbt-integration` kiểm tra exact
weighted AOV, inclusive date filters, empty windows, status/completeness grains,
reader denials, grants sau dbt rebuild và unavailable/newer-load gates.

Local smoke đã query saved dashboard qua API, so sánh date-filtered KPI với SQL
trực tiếp bằng reader và xác minh chạy lại giữ dashboard/card IDs. Pipeline gate
integration kiểm tra fresh dbt vẫn đạt với grants mới; không coi SQL status fixtures
trong dashboard tests là bằng chứng đã chạy full receipt pipeline.

## References

- [Metabase Docker setup](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker).
- [Metabase REST setup and dashboard API](https://www.metabase.com/articles/set-up-metabase-from-code-with-the-api).
- [Analytics metric contract](09_analytics_models.md).
