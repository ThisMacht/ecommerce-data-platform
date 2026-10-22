# Item Dependency Snapshots

## Scope and Versions

`order_items-silver-v2` kiểm tra ba direct FKs: order, product và seller. Bật v2
bằng cách cấp đủ `ORDER_SNAPSHOT`, `PRODUCT_SNAPSHOT`, `SELLER_SNAPSHOT`. Hai flags
product/seller phải đi cùng nhau và chỉ dùng cho items. Mỗi parent phải là full
verified Silver, đúng dataset/version và cùng source contract; chỉ valid unique
nonblank keys được dùng. Empty full snapshots là hợp lệ, không đồng nghĩa tắt FK.

Không cấp product/seller flags thì vẫn là v1, chỉ kiểm tra order và khai báo hai
unchecked relationships. Core runner/Airflow hiện vẫn chạy v1; v2 là standalone
slice, chưa được core receipt gate chấp nhận. Các staging rows đã load bằng v1
không tự được kiểm tra lại hoặc xóa. Chưa thêm product/seller keys vào fact models.

## Routing and Pinned Evidence

Local reject/duplicate giữ priority. Với selected locally valid items, đánh giá
đồng thời cả ba memberships; `deferred_rule_ids` chứa mọi rule thiếu theo thứ tự
`missing_order`, `missing_product`, `missing_seller`. Không dừng sau rule đầu tiên.

Request pin prefix và SHA-256 của cả ba manifests. Output khai báo
`relationship_checks=ORDER_PRODUCT_SELLER_SNAPSHOTS`, `unchecked_relationships=[]`.
Đổi bất kỳ parent/input nào cần Silver run ID mới. Replay kiểm tra lại inputs,
parents và sealed output; resume missing `_SUCCESS` dùng sealed manifest.

Snapshots hiện được capture độc lập; không phải atomic cross-table source snapshot.
Core integration cần bổ sung freeze và audit cho product/seller parents trước khi
chuyển scheduled jobs sang v2. Thay đổi key sets có thể cần retry với snapshots mới.

## Deferred Identity and Atomic Retry

Migration `015_item_dependency_scope.sql` thêm queue rule `missing_item_dependencies`
cho items. Một physical UID có một queue entry cho v2, dù payload ghi nhiều missing
rules. `input_rows`, `inserted_rows`, retry counters và giới hạn 1,000 đo physical
rows; rule-hit counts không được cộng thành processing inputs.

Registration v2 pin `dependency_scope=ORDER_PRODUCT_SELLER_SNAPSHOTS`. Retry v2
pin đủ ba parents, đánh giá lại cả ba FKs từ frozen normalized payload và không
đọc lại source. Parent đến từng phần vẫn PENDING; result `missing_dependencies`
ghi các rule hiện còn thiếu. Chỉ khi đủ cả ba mới RESOLVED và merge staging.

Queue update, attempts, staging merge và audit commit trong một transaction.
Giữ limits/first-seen/lineage ban đầu, expiry policy và dedupe theo full composite
key/version. Re-register không reset attempts hoặc reopen terminal state. Đổi
parent cần resolution run ID mới; replay cùng request không tăng attempts.

Resolver v1 chỉ chọn legacy rule; resolver v2 chỉ chọn composite rule. Cả selection
và `remaining_pending_rows` dùng cùng scope/origin filter. Origin là payload
`_run_id` ban đầu (Bronze identity), có thể khác Silver processing run ID. Thiếu flags không được
làm v2 rows resolve theo policy yếu hơn. Legacy và v2 có thể có cùng physical UID
nhưng khác rule identity; không chuyển hoặc rewrite legacy queue tự động.

## Commands and Validation

Cần migration, Java 17, PG/S3 environment như các slice hiện có. Chuẩn bị full
orders/products/sellers Silver snapshots trước khi chạy:

```sh
make process-order-items BRONZE_PREFIX=<items-bronze> SILVER_RUN_ID=items_fk_v2 \
  ORDER_SNAPSHOT=<orders-full> PRODUCT_SNAPSHOT=<products-full> SELLER_SNAPSHOT=<sellers-full>
make load-warehouse SOURCE_TABLE=order_items \
  SILVER_PREFIX=silver/order_items/run_id=items_fk_v2 WAREHOUSE_RUN_ID=items_fk_v2
make register-deferred SOURCE_TABLE=order_items \
  SILVER_PREFIX=silver/order_items/run_id=items_fk_v2 DEFERRED_RUN_ID=items_fk_v2
make resolve-deferred SOURCE_TABLE=order_items DEFERRED_RUN_ID=items_retry_v2 \
  PARENT_SNAPSHOT=<orders-full> PRODUCT_SNAPSHOT=<products-full> SELLER_SNAPSHOT=<sellers-full>
```

Warehouse/deferred loads độc lập làm BI publication unavailable cho đến fresh core
gate SUCCESS. Publish Silver đơn thuần không load staging hay promote core cursors.
`make test-item-dependencies-integration` dùng disposable services, fixtures độc lập,
real Spark/merge: nhiều FKs thiếu, partial arrivals, rejected parent keys, corruption,
sealed retry, atomic rollback, expiry, lineage, scope isolation và CLI. Spark/unit
cases nằm trong `make test-processing`/`make test`; không cần raw CSV cho các suite.

## Local Verification

Smoke test ngày 2026-10-22 publish 112,650 source items bằng v2: 112,621 valid,
23 deferred (`missing_order`), 6 rejected (`invalid_shipping_limit_date`), không
có duplicate/warning hoặc missing product/seller trong các snapshots đã pin.
Replay trả NOOP. Chỉ publish Silver: warehouse loads và toàn bộ core controls giữ
nguyên; BI vẫn available. Runtime evidence: ignored `build/reports/item-fk-smoke.json`.
