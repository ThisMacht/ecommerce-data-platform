# Source Contracts

| Field | Value |
| --- | --- |
| System | Olist E-commerce Data Platform |
| Contract version | `1.0.0` |
| Dataset snapshot | `olist-kaggle-local-v1` |
| Status | Active |
| Owner | Data Engineering |
| Last reviewed | 2026-07-28 |
| Machine-readable contract | [`config/source_contract.json`](../../config/source_contract.json) |

Tài liệu này định nghĩa boundary giữa raw Olist CSV và PostgreSQL source simulator. Contract máy đọc được là nguồn chuẩn cho filename, table mapping, grain, key, PostgreSQL type, nullability và các policy dùng chung. Tài liệu này giải thích các quyết định cần con người đọc.

## 1. Scope

Contract áp dụng cho:

- Snapshot Kaggle đã được khóa bằng checksum trong [`config/raw_dataset_manifest.json`](../../config/raw_dataset_manifest.json).
- Initial seed vào database `olist_source`, schema `olist`.
- Metadata phục vụ source simulator và bounded incremental extraction.
- Schema validation, type policy, relationship validation và idempotent seed.

Contract chưa định nghĩa Bronze/Silver physical schema, dimensional model hoặc business metrics. Những contract đó được bổ sung ở phase tương ứng.

## 2. Physical boundary

| Key | Value |
| --- | --- |
| `database` | `olist_source` |
| `source_schema` | `olist` |
| `audit_schema` | `audit` |
| `migration_table` | `audit.schema_migrations` |
| `seed_audit_table` | `audit.source_seed_runs` |
| `source_loader_role` | `olist_loader` by default |
| `source_reader_role` | `olist_reader` by default |

`olist_loader` chỉ có quyền `SELECT`, `INSERT`, `UPDATE` trên source tables và quyền tối thiểu với seed audit. `olist_reader` chỉ có quyền đọc. Admin credentials chỉ được migration job sử dụng.

Raw CSV được mount read-only tại `/data/raw` trong source-loader container. Loader không sửa, đổi tên hoặc chuẩn hóa file gốc.

## 3. Dataset mapping

| Raw file | Physical table | Grain | Primary key |
| --- | --- | --- | --- |
| `olist_customers_dataset.csv` | `olist.customers` | Một `customer_id` gắn với một order identity | `customer_id` |
| `olist_geolocation_dataset.csv` | `olist.geolocation` | Một logical CSV row | `geolocation_row_id` |
| `olist_order_items_dataset.csv` | `olist.order_items` | Một item position trong order | `order_id`, `order_item_id` |
| `olist_order_payments_dataset.csv` | `olist.order_payments` | Một payment sequence trong order | `order_id`, `payment_sequential` |
| `olist_order_reviews_dataset.csv` | `olist.order_reviews` | Một cặp review và order | `review_id`, `order_id` |
| `olist_orders_dataset.csv` | `olist.orders` | Một order | `order_id` |
| `olist_products_dataset.csv` | `olist.products` | Một product | `product_id` |
| `olist_sellers_dataset.csv` | `olist.sellers` | Một seller | `seller_id` |
| `product_category_name_translation.csv` | `olist.product_category_name_translation` | Một Portuguese category | `product_category_name` |

`geolocation_zip_code_prefix` không unique. `geolocation_row_id` là technical key bắt đầu từ 1 và được suy ra từ vị trí logical data row trong CSV, không phải physical line number. Cách này giữ được quoted multiline records ở dataset khác và giúp seed lặp lại cho kết quả ổn định.

`review_id` riêng lẻ không unique trong snapshot. Grain hợp lệ của reviews là composite key `review_id + order_id`.

## 4. Customer identity

| Key | Meaning | Usage |
| --- | --- | --- |
| `customer_id` | Order-scoped customer identifier | Join trực tiếp `orders` với `customers`; source primary key |
| `customer_unique_id` | Stable shopper identifier trong dataset | Gom nhiều order identities về cùng business customer ở dimensional layer |

Không dùng `customer_id` để tính repeat-customer metrics. Dimensional contract sau này phải định nghĩa surrogate key dựa trên `customer_unique_id`, đồng thời vẫn giữ lineage về `customer_id`.

## 5. Type policy

| Source category | PostgreSQL type | Policy |
| --- | --- | --- |
| Identifier and ZIP prefix | `text` | Giữ leading zero và không suy diễn UUID/integer semantics |
| Count and sequence | `integer` or `bigint` | Parse strict; invalid representation làm seed transaction fail |
| Money | `numeric(18,2)` | Currency là BRL; không dùng binary floating point |
| Coordinates | `numeric` | Giữ decimal representation từ source; outlier được xử lý ở DQ layer |
| Business timestamp | `timestamp without time zone` | Giữ local wall-clock value từ Olist |
| Simulator timestamp | `timestamp with time zone` | So sánh cursor theo instant; session vận hành dùng UTC |
| Free text | `text` | PostgreSQL CSV parser xử lý quote và embedded newline |

Các timestamp nghiệp vụ Olist được diễn giải theo `America/Sao_Paulo` khi chuyển sang Silver. Source layer chưa chuyển timezone để giữ source boundary rõ ràng. Warehouse lưu UTC và giữ local business date khi cần.

Tên cột typo có sẵn như `product_name_lenght` và `product_description_lenght` được giữ nguyên ở source boundary. Tên chuẩn hóa chỉ xuất hiện từ Silver hoặc dbt staging.

## 6. Nullability policy

Nullability được khóa theo profile thực tế:

- `orders.order_approved_at`, `orders.order_delivered_carrier_date` và `orders.order_delivered_customer_date` có thể `NULL`.
- `order_reviews.review_comment_title` và `order_reviews.review_comment_message` có thể `NULL`.
- Mọi product attribute ngoài `product_id` có thể `NULL` theo snapshot đã profile.
- Các source key luôn `NOT NULL`.
- Các cột còn lại là `NOT NULL` trong contract version `1.0.0`.

Empty string trong nullable text được chuyển thành `NULL` khi seed. Whitespace hoặc business normalization khác chưa được thực hiện tại source boundary.

## 7. Relationship policy

| Relationship | Validation | Physical FK |
| --- | --- | --- |
| `orders.customer_id → customers.customer_id` | Required at initial seed | No |
| `order_items.order_id → orders.order_id` | Required at initial seed | No |
| `order_items.product_id → products.product_id` | Required at initial seed | No |
| `order_items.seller_id → sellers.seller_id` | Required at initial seed | No |
| `order_payments.order_id → orders.order_id` | Required at initial seed | No |
| `order_reviews.order_id → orders.order_id` | Required at initial seed | No |
| `products.product_category_name → translation.product_category_name` | Warning only | No |

Initial seed fail toàn bộ transaction nếu một required relationship bị orphan. Physical foreign keys chủ ý chưa được tạo vì simulator cần sinh late-parent và out-of-order fixtures để kiểm tra `DEFERRED` routing. Downstream DQ contract chịu trách nhiệm phân loại những event này.

Snapshot hiện có 13 product rows thuộc 2 category values không có English translation; đây là warning đã biết, không phải seed failure.

## 8. Simulator metadata

Mỗi source table có cùng metadata:

| Key | Type | Rule |
| --- | --- | --- |
| `source_updated_at` | `timestamp with time zone` | Discovery/lookback timestamp |
| `source_sequence` | `bigint` | Authoritative version, unique và tăng đơn điệu theo table |
| `source_operation` | `text` | Chỉ nhận `INSERT` hoặc `UPDATE` trong MVP |
| `source_batch_id` | `text` | Batch tạo ra phiên bản hiện tại |
| `source_loaded_at` | `timestamp with time zone` | Operational load timestamp |

Incremental cursor có thứ tự:

```text
(source_updated_at, source_sequence, <primary-key columns>)
```

Initial snapshot dùng một deterministic timestamp `2020-01-01T00:00:00Z`. `source_sequence` dùng one-based logical CSV row position trong từng table. Việc nhiều records có cùng timestamp là có chủ ý để cursor implementation bắt buộc dùng sequence và key tie-breaker.

## 9. Seed semantics

`make seed-source` thực hiện lần lượt:

1. Kiểm tra filename, size, SHA-256 và header của mọi raw CSV.
2. Kiểm tra machine-readable source contract khớp raw manifest.
3. Start PostgreSQL và apply checksum-protected migrations.
4. COPY toàn bộ CSV vào temporary text staging tables trong một transaction.
5. Kiểm tra row counts, source keys và required relationships.
6. Cast sang contract types và insert những source keys chưa tồn tại.
7. Kiểm tra mọi source key đã tồn tại ở target bằng `EXCEPT`; cách này tránh anti-join plans phụ thuộc statistics chưa có của target mới load.
8. Đồng bộ identity sequences và ghi `audit.source_seed_runs`.

Khi cùng `snapshot_id + manifest_sha256` đã có trạng thái `SUCCESS`, loader trả về thành công mà không đọc lại CSV. Nếu cùng `snapshot_id` có manifest checksum khác, loader fail thay vì âm thầm tiếp nhận snapshot mới.

Existing source keys không bị seed ghi đè. Policy này bảo vệ các updates mà simulator đã áp dụng nếu operator vô tình chạy lại seed command.

Seed rerun thành công phải giữ nguyên source payload, metadata, audit rows và identity sequence state. Integration tests so sánh full-row fingerprints cùng sequence state trước/sau shell no-op và xác minh source update được giữ nguyên cả khi gọi lại SQL seed trực tiếp.

Nếu transaction seed fail, source rows cùng metadata và seed audit phải rollback; retry sau khi bỏ nguyên nhân lỗi phải thành công. PostgreSQL `setval` không transactional, nên lỗi xảy ra sau bước đồng bộ sequences có thể để lại sequence gaps. Contract yêu cầu version ordering và uniqueness, không yêu cầu sequence liên tục.

## 10. Schema evolution

| Change | Outcome |
| --- | --- |
| Additive nullable column | `WARN`; cần contract version mới trước khi sử dụng downstream |
| Missing required column | `FAIL` before source load |
| Incompatible type change | `FAIL` and rollback seed transaction |
| Unknown schema version | `FAIL` |
| Raw checksum change | `FAIL`; cần deliberate snapshot adoption |
| Rename or remove column | Breaking change; migration và major contract review bắt buộc |

Applied migration không được sửa tại chỗ. Migration runner lưu SHA-256 và fail nếu checksum của một version đã apply thay đổi.

## 11. Delete policy

MVP chỉ hỗ trợ `INSERT` và `UPDATE`. Hard delete không được mô phỏng hoặc phát hiện vì polling mutable tables không cung cấp tombstone đáng tin cậy. Một phase sau chỉ được thêm delete khi contract định nghĩa `DELETE` event, `source_deleted_at`, retention và downstream tombstone behavior.

## 12. Known limitations and TODO

| TODO | Impact | Planned phase |
| --- | --- | --- |
| `TODO(source-simulator)` | Chưa có batch generator cho updates, late parents và out-of-order events | Phase 0 |
| `TODO(dirty-fixture)` | Chưa có deterministic invalid-record manifest | Phase 0 |
| `TODO(contract-loader-generation)` | Seed SQL đang explicit theo snapshot v1, chưa generate từ JSON contract | Phase 0 or 1 |
| `TODO(hard-delete)` | Không phát hiện source hard delete | Post-MVP |
| `TODO(production-secrets)` | Local defaults không thay thế secret manager | Deployment phase |

Các TODO trên không ngăn initial snapshot seed và source read path hoạt động. Chúng ngăn tuyên bố Phase 0 hoàn tất cho tới khi simulator fixtures và tests tương ứng được bổ sung.

## 13. Verification commands

```bash
make validate-source-contract
make seed-source
make verify-source-schema
make verify-source
make test-source-integration
```

`make verify-source-schema` dùng PostgreSQL catalog report từ read-only role để đối chiếu physical source schema với machine-readable contract: tables, business/technical/metadata columns, types, nullability, ordered primary keys và `source_sequence` BY DEFAULT identity cùng unique constraint. Missing hoặc incompatible declarations fail; additive nullable columns warn theo schema-evolution policy. `make verify-source` chạy schema check trước source-state checks.

`make test-source-integration` verify full canonical raw snapshot, rồi chạy production migration/seed runners trong disposable PostgreSQL container riêng. Mỗi test dùng database mới. Suite kiểm tra exact canonical counts, no-op data/metadata/audit/sequence state, preservation of updates, live schema drift và rollback/retry khi chèn lỗi ở late table insert hoặc seed audit insert. Local Compose data không được dùng.
