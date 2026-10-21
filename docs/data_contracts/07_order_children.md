# Order Items and Payments

## Scope and grain

Status: **Active — Bronze windows, Silver rules and pinned orders FK routing**.
Source contract giữ `1.0.0`. Artifacts dùng `order_items-bronze-v1`,
`order_payments-bronze-v1` và các Silver versions tương ứng `*-silver-v1`.

| Dataset | Business key | Money fields |
| --- | --- | --- |
| `order_items` | `(order_id, order_item_id)` | `price`, `freight_value` |
| `order_payments` | `(order_id, payment_sequential)` | `payment_value` |

Không deduplicate chỉ theo `order_id`: một order có nhiều items/payments.
Không join hai bảng trực tiếp để tính doanh thu; aggregate từng bảng về order grain trước.

## Bronze boundary

Migration `005` thêm control/run/page tables riêng cho từng dataset. Extraction dùng
`REPEATABLE READ`, keyset `(source_updated_at, source_sequence, <business key components>)`,
lookback theo source contract và upper cursor cố định. Integer keys giữ JSON integer;
PostgreSQL decimals được serialize thành string, không qua float.

Logical UID là SHA-256 của `[source, [order_id, ordinal], source_sequence]` theo
canonical JSON serializer hiện có. Physical UID vẫn theo `[run_id, object_key, ordinal_in_page]`.
Durable prepared pages, conditional object writes, counts/checksums, `_SUCCESS` và
checkpoint CAS dùng chung engine với orders/customers. Retry cùng run ID giữ nguyên bytes;
không reuse ID với config khác. Checkpoint này chỉ xác nhận Bronze delivery.

## Silver rules

Silver giữ raw payload/lineage, trim IDs, normalize payment type và chọn source version
mới nhất theo **toàn bộ business key**. Cùng normalized key/version khác payload làm fail
batch; repeated physical UID cũng fail. Selected record lỗi không được thay bằng version cũ.

| Outcome | Rules |
| --- | --- |
| Reject | Order ID thiếu; ordinal không dương; source sequence/operation/batch/technical timestamps không hợp lệ |
| Reject | Money thiếu, âm, quá 16 chữ số phần nguyên, quá hai chữ số thập phân hoặc malformed; không âm thầm round |
| Reject items | Product/seller ID thiếu; shipping timestamp thiếu, malformed, có offset hoặc rơi vào DST gap/fold |
| Reject payments | Payment type ngoài `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined`; installments thiếu hoặc âm |
| Warning payments | Type `not_defined`, installments bằng zero hoặc value bằng zero; giữ record nếu các rules khác hợp lệ |
| Deferred | Selected locally valid record thiếu order trong pinned parent snapshot: `missing_order` |

Tiền dùng Spark `decimal(18,2)`, BRL, không dùng double. `shipping_limit_date` chuyển
từ naive `America/Sao_Paulo` sang UTC; technical timestamps phải có offset. Zero
price/freight được chấp nhận. Không suy luận tổng payment phải bằng item total ở cấp row.

## Parent snapshots and replay

Items/payments **bắt buộc** truyền `--order-snapshot` / `ORDER_SNAPSHOT`. Parent phải
là verified `orders-silver-v1` hoặc `orders-silver-v2`, `snapshot_kind=FULL` và cùng
source contract hash. Tạo bằng orders Bronze `--full-snapshot`, rồi process orders
như bình thường. Default orders extraction vẫn incremental và không được dùng làm
toàn bộ parent key set, kể cả lần đầu tình cờ đọc đủ rows.

Full orders đọc mọi current row, không dùng lower bound; flag nằm trong sealed Bronze
config. Silver truyền tiếp `snapshot_kind=FULL`. Parent **valid** keys phải duy nhất,
nonblank và có count khớp manifest. Reject/deferred orders không thành parent hợp lệ.
Các snapshots nhất quán riêng, không phải cross-table transaction cùng thời điểm.

Child request pin parent prefix và manifest SHA-256; đổi parent cần Silver run ID mới.
Local rejects/duplicates giữ priority, không chuyển sang deferred. Manifest ghi
`relationship_checks=ORDER_SNAPSHOT`; items còn ghi
`unchecked_relationships=["product_id", "seller_id"]`: chỉ kiểm tra nonblank,
**chưa xác minh membership** vì chưa có products/sellers Silver snapshots.

```text
input_rows = valid_rows + rejected_rows + deferred_rows + duplicates_rows
0 <= warning_rows <= valid_rows
```

Parent tới muộn: tạo full orders snapshot mới và reprocess **original child Bronze**
bằng Silver ID mới. Original UIDs giữ nguyên; old outputs bất biến. Durable deferred
retry/expiry đã có theo [lifecycle contract](10_deferred_lifecycle.md); local watermark chỉ được promote sau final [pipeline gate](11_pipeline_control.md). Warehouse staging đã có theo [contract](08_warehouse_staging.md).

## Commands and validation

```bash
make migrate
make ingest-orders BRONZE_RUN_ID=orders_full_001 BRONZE_FULL_SNAPSHOT=1
make process-orders BRONZE_PREFIX='<full orders Bronze prefix>' SILVER_RUN_ID=orders_full_001
make ingest-order-payments BRONZE_RUN_ID=payments_001
make process-order-payments BRONZE_PREFIX='<payments Bronze prefix>' \
  SILVER_RUN_ID=payments_001 ORDER_SNAPSHOT='silver/orders/run_id=orders_full_001'
make ingest-order-items BRONZE_RUN_ID=items_001
make process-order-items BRONZE_PREFIX='<items Bronze prefix>' \
  SILVER_RUN_ID=items_001 ORDER_SNAPSHOT='silver/orders/run_id=orders_full_001'
make test-processing
make test-order-children-integration
```

Các commands dùng database/S3 environment như orders; `make dataset` tự nối cả bốn
datasets và customer/orders snapshots. Nếu có pending orders run từ trước khi bật
full mode, hoàn tất run đó bằng config cũ trước; không sửa audit state để đổi config.

Independent dirty fixtures kiểm tra composite grain, decimal precision, metadata,
timezone/DST và exact routing/rule counts. Disposable PostgreSQL/MinIO tests kiểm tra
pagination, replay, partial publication, isolated checkpoints, missing/late parents,
empty snapshots, corruption, pinned hash và CLI. Không cần canonical CSV cho các suites này.
