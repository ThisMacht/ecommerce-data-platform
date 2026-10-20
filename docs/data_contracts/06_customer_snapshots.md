# Customer Snapshots and Orders Dependencies

## Scope and versions

Status: **Active — full customers Bronze/Silver và optional orders FK routing**. Source contract vẫn là `1.0.0`. Customer artifacts dùng `customers-bronze-v1` / `customers-silver-v1`; orders có parent snapshot dùng `orders-silver-v2`. Orders Bronze windows và `orders-silver-v1` local-rule mode giữ nguyên contract/replay behavior.

Slice này thêm parent membership checks và explicit reprocessing khi parent xuất hiện. Persistent deferred queue, attempt/age limits, quarantine database, warehouse merge và end-to-end watermark vẫn planned.

## Full customer snapshots

`make ingest-customers` đọc **mọi current customer row** trong một `REPEATABLE READ` transaction, theo keyset pages `(source_updated_at, source_sequence, customer_id)`. Không áp dụng incremental lower bound: customer không đổi hoặc backdated vẫn có mặt trong parent snapshot, kể cả ngoài orders lookback SLA.

Bronze manifest ghi `snapshot_kind=FULL`, `extract_lower_bound=null`, schema/hash, source/extracted counts và upper cursor. Migration `004` tạo `audit.bronze_customers_control`, `bronze_customers_runs`, `bronze_customers_parts`. State, locks và run IDs tách biệt với orders; hai datasets có thể dùng cùng run ID. Shared engine giữ nguyên durable prepared bytes, conditional publication, checkpoint CAS và exact replay.

Customer Silver chỉ nhận full customer Bronze có source contract khớp. Giữ version mới nhất theo normalized **`customer_id`**, không theo `customer_unique_id`: nhiều order-scoped customer IDs có thể cùng một shopper. Cùng key/version khác payload làm fail batch; replaced occurrences nằm trong duplicates.

| Outcome | Rules |
| --- | --- |
| Reject | Customer/shopper ID thiếu hoặc blank; source sequence/operation/batch không hợp lệ; source technical timestamps malformed hoặc thiếu offset |
| Warning | City thiếu; ZIP prefix không đủ năm chữ số; state code thiếu/không hợp lệ |
| Normalize | Trim IDs/location, lowercase city, uppercase state; giữ ZIP dạng text và leading zeros |

Giữ original Bronze lineage/raw payload. Customer bị reject không nằm trong parent key set; location warnings không loại parent hợp lệ. Customer Silver ghi `snapshot_kind=FULL`, valid/rejected/duplicates counters và immutable Parquet. Full chỉ toàn bộ input snapshot; rejected rows vẫn được đối chiếu ngoài valid key set.

## Orders dependency routing

Truyền `CUSTOMER_SNAPSHOT=<customer Silver prefix>` vào `make process-orders`, hoặc `--customer-snapshot` vào CLI. Dependency phải là `customers-silver-v1`, full, đã publish/verify và dùng cùng source contract hash với orders.

Orders request seal exact customer manifest SHA-256 và prefix. Thay snapshot của cùng Silver run ID bị reject. Parent Parquet được kiểm tra lại bytes khi download; valid key count phải khớp manifest, keys phải là non-null strings và duy nhất. Snapshot không có valid records được xử lý như empty key set.

Local orders validation/dedupe chạy trước. Selected locally valid order thiếu normalized `customer_id` trong parent snapshot đi vào **deferred**, với `deferred_rule_ids=["missing_customer"]`. Local rejects giữ priority; duplicates giữ classification cũ. Warnings là flags, không tạo bản sao hoặc join fan-out.

```text
input_rows = valid_rows + rejected_rows + deferred_rows + duplicates_rows
0 <= warning_rows <= valid_rows
```

Manifest `orders-silver-v2` ghi `relationship_checks=CUSTOMER_SNAPSHOT`. Deferred Parquet dùng chung run-level atomic manifest/marker. Mỗi physical input UID được đếm đúng một lần. Silver không đọc source database hoặc thay đổi Bronze checkpoints.

Orders/customer snapshots nhất quán riêng, không phải một cross-table transaction cùng thời điểm. Membership được đánh giá theo parent snapshot caller chọn; hash ghi lại lựa chọn đó để review/replay. Caller chịu trách nhiệm chọn snapshot đủ mới.

## Late parent reprocessing

Khi parent tới muộn, tạo **full customer snapshot mới**, rồi process **original orders Bronze prefix** bằng **Silver run ID mới** và customer Silver prefix mới. Original physical UIDs/source versions giữ nguyên; old Silver/deferred artifacts bất biến. Order deferred có thể thành valid mà không đọc lại orders từ PostgreSQL.

Đây là explicit batch re-evaluation, chưa phải automatic deferred lifecycle. Mỗi output ghi full input/routing counters của riêng run; cross-run source-growth/deferred-retry accounting và persisted resolution state còn planned. Không cộng reprocessing rows thành source records mới hoặc reuse old Silver ID với parent hash khác.

## Commands

```bash
make migrate                 # Apply 004 sau khi source snapshot đã được seed
make ingest-customers BRONZE_RUN_ID=customers_001
export JAVA_HOME=/path/to/jdk17
make sync-processing
make process-customers BRONZE_PREFIX='<customer Bronze manifest.prefix>' SILVER_RUN_ID=customers_001
make process-orders BRONZE_PREFIX='<orders Bronze manifest.prefix>' \
  SILVER_RUN_ID=orders_fk_001 CUSTOMER_SNAPSHOT='silver/customers/run_id=customers_001'
uv run --locked olist-silver-source verify --prefix 'silver/orders/run_id=orders_fk_001'
make test-customer-integration
```

Dùng process database/S3 environment của Bronze. Source CLIs hỗ trợ `--table orders|customers`; generic aliases là `olist-bronze-source`, `olist-silver-source`. Orders aliases cũ vẫn hoạt động. Silver chỉ cần S3 credentials, không cần PostgreSQL DSN.

## Validation evidence

Independent fixture khóa 6 customer inputs → 3 valid, 2 rejected, 1 duplicate; shared shopper IDs vẫn tách biệt. Spark tests cover normalization, warnings, FK routing và reject priority. Disposable PostgreSQL/MinIO tests cover full snapshots, consistent pagination, backdated parents, sealed retries, dataset isolation, empty/invalid parents, pinned hashes, lineage-preserving reprocessing và CLI flags. CI chạy suite này không cần raw CSV hoặc repository secrets.
