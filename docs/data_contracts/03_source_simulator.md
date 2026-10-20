# Source Simulator Contract

| Field | Value |
| --- | --- |
| Status | Active — four-table MVP |
| Batch format version | `1` |
| Source contract version | `1.0.0` |
| Supported tables | `customers`, `orders`, `order_items`, `order_payments` |
| Successful batch audit | `audit.source_batch_runs` |

## 1. Artifact format

`make generate-source-batches` publish bốn batch vào ignored `data/generated/source_batches/reliability_v1/`. Mỗi directory có:

- `operations.json`: `format_version`, `batch_id`, `predecessor_batch_id`, logical `applied_at` và ordered `operations`.
- `manifest.json`: SHA-256 của exact payload bytes, canonical source-contract hash, operation count, per-table `INSERT`/`UPDATE` counts và scenario expectations.

Mỗi operation khai báo `table`, `operation`, `key`, `values` và timezone-aware `source_updated_at`. `key` phải có đúng các primary-key columns. `INSERT` khai báo toàn bộ business columns ngoài key, kể cả nullable fields bằng explicit null; `UPDATE` chứa một subset không rỗng của business columns ngoài key. Không được tự cấp simulator metadata hoặc thay primary key.

Money dùng decimal strings, không dùng JSON floats; integer types loại trừ booleans. Numeric overflow hoặc scale vượt `numeric(18,2)` bị reject trước transaction. Business timestamps là local values không timezone; source timestamps có timezone và được chuyển UTC.

Generator dùng seed `42` mặc định. Cùng seed/contract cho output bytes giống nhau; generation lại artifact giống hệt là no-op. Artifact khác tại cùng đường dẫn bị từ chối. Batch directory được publish bằng rename sau khi payload và manifest hoàn tất; loader đọc payload bytes một lần rồi kiểm tra checksum trên đúng bytes đó. `fixture.json` ghi thứ tự các batch.

## 2. Apply protocol

Apply dùng Psycopg 3, source-loader role và idle autocommit connection. Transaction dùng explicit `READ COMMITTED` để audit read sau khi chờ lock nhìn thấy batch vừa commit. Một transaction bao gồm:

1. Lấy transaction advisory lock `hashtext('olist.seed.olist-kaggle-local-v1')`, dùng chung với seed runner. Các cooperating source writers được serialize.
2. Xác nhận `audit.source_seed_runs` có `SUCCESS` cho snapshot của contract; nếu chưa seed thì fail trước mutation để tránh sequence collision với initial load.
3. Nếu batch ID đã commit với cùng payload/contract hashes, trả `NOOP` cùng original results trước khi cấp sequence hoặc sửa source. Cùng ID khác hash hoặc predecessor chưa commit đều fail.
4. Apply operations theo thứ tự. New `INSERT` trùng key hoặc `UPDATE` thiếu key làm fail toàn batch; không dùng upsert để che lỗi input.
5. Cấp fresh identity `source_sequence` cho cả insert và update. Gán operation, batch ID, source timestamp; operational `source_loaded_at` dùng database clock.
6. Ghi counts và per-operation key/version/timestamp vào `audit.source_batch_runs`; chỉ trả `APPLIED` sau commit.

Source rows, metadata và audit commit/rollback cùng nhau. Failed attempts không tạo success audit. PostgreSQL identity allocations không transactional nên rollback có thể để lại gaps; version phải unique và tăng theo operation, không cần liên tục. Replay không cấp sequence và không sửa operational timestamps.

Audit loader chỉ có `SELECT`/`INSERT`; reader chỉ có `SELECT`. Migration `002` tạo bảng audit và grants; migration `001` được giữ nguyên. Lock timeout là 15 giây, statement timeout 60 giây. Các writer ngoài protocol không được bảo vệ bởi advisory lock.

## 3. Reliability fixture

Business attributes và entity IDs của fixture là synthetic. Item references dùng product `4244733e06e7ecb4970a6e2683c13e61` và seller `48436dade18ac8b2bce089ec2a041202` từ canonical snapshot. Seed local source trước để có references này. Integration tests tạo hai reference rows và seed audit cho một test snapshot ID riêng, không giả lập canonical snapshot đã load và không cần raw CSV.

| Directory | Operations | Scenario | Fixture rows after: customers / orders / items / payments |
| --- | ---: | --- | --- |
| `001_initial` | 7 | Hai orders cùng source timestamp, hai order-scoped customers cùng shopper | 2 / 2 / 2 / 1 |
| `002_children` | 2 | Item/payment đến trước order thứ ba | 2 / 2 / 3 / 2 |
| `003_parents_updates` | 5 | Parent xuất hiện, order delivered, customer correction và late payment | 3 / 3 / 3 / 3 |
| `004_backdated` | 2 | Newer source versions có timestamp cũ trong/ngoài lookback 48 giờ | 3 / 3 / 3 / 3 |

Counts này chỉ tính fixture-owned rows, không cộng canonical snapshot. Physical foreign keys vẫn không được enforce để mô phỏng late parents.

`source_sequence` là authoritative version. Batch cuối có thể đưa `source_updated_at` lùi; đây là correction mới với timestamp cũ, không phải apply một source version cũ. Một correction quá lookback không được hứa tự động phát hiện bằng polling. MVP giữ current source state; không cung cấp CDC log hoặc mọi intermediate payload. Original operations, batch audit và generated expectations phục vụ kiểm tra reliability sau này.

## 4. Commands and connection

```bash
make sync
make seed-source
make verify-source-schema
make generate-source-batches
make validate-source-batch

# Local example using the default development loader credentials:
export SOURCE_SIMULATOR_DSN='postgresql://olist_loader:local_loader_change_me@127.0.0.1:5432/olist_source'
make apply-source-batch
make apply-source-batch # NOOP
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/002_children
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/003_parents_updates
make apply-source-batch SOURCE_BATCH_DIR=data/generated/source_batches/reliability_v1/004_backdated
```

CLI đọc `SOURCE_SIMULATOR_DSN` hoặc standard `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` từ process environment. `.env` của Compose không tự được export vào host CLI; credentials không được truyền qua argv. API yêu cầu migrations/schema contract hiện hành trước apply. `applied_at` là logical fixture time; `committed_at`/`source_loaded_at` là operational database times và không deterministic giữa hai database mới.

`make test-source-simulator-integration` dùng PostgreSQL tạm, ephemeral port chỉ bind localhost và small SQL fixture ở `tests/fixtures/source_simulator/`. Tests bao gồm full fixture, no-op, payload conflict, predecessor ordering, concurrency, rollback/retry, composite updates, SQL parameterization và role permissions. `make test-source-integration` tiếp tục dùng full canonical raw snapshot để kiểm tra seed compatibility.
