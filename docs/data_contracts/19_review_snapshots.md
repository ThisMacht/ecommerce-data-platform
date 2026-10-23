# Review Snapshots

## Scope and Grain

Reviews chạy standalone full Bronze → Silver → version-aware staging. Grain là
`(review_id, order_id)`: cùng review ID có thể thuộc nhiều orders và một order có
thể có nhiều reviews. Không chọn một review/order hoặc dùng review_id riêng lẻ làm
PK. Source contract giữ nguyên; migration `020` thêm delivery state, staging và
quarantine scope. Không thêm scheduled streams, completion cursors, dbt models
hoặc BI grants; core vẫn bảy datasets, 29/33 tasks và bốn completion cursors.

## Extraction and Replay

Bronze full scan source trong REPEATABLE READ, kể cả rows cũ hơn delivery cursor.
Composite keyset paging và physical/logical lineage dùng đủ hai keys. Prepared
pages, bounds và bytes giữ nguyên khi retry cùng ID/config. Không đổi source để
resume; new snapshot phải dùng ID mới. CLI từ chối owned core reviews streams.
Full hoặc empty snapshot không có nghĩa xóa rows đã load.

## Validation and Time Semantics

- Trim keys/batch ID; thiếu key hoặc metadata bắt buộc thì reject.
- review_score phải là integer 1–5; ngoài range hoặc NULL thì reject.
- Comment title/message optional: empty/all-space thành NULL, không cảnh báo chỉ
  vì không có comment. Giữ case, Unicode, newline và nonblank text nguyên trạng.
- Hai business timestamps là local São Paulo theo source contract, chuyển UTC.
  Reject malformed/NULL, explicit offset trong local input, DST gap hoặc fold;
  không tự chọn một instant khi local time không xác định duy nhất. Metadata
  timestamps phải có offset. Giữ microsecond precision.
- Answer trước creation phát warning `review_answer_before_creation`, giữ row.
  Không suy diễn creation/answer là purchase/delivery timestamps hoặc sentiment.
- Shared source-version dedupe giữ latest source_sequence trên đủ composite key;
  repeated physical UID hoặc same-version conflicting payload làm fail batch.

## Parent Membership and Deferred Retry

`process-order-reviews` bắt buộc full orders Silver từ cùng source contract;
pin manifest hash, không dùng delta làm toàn bộ key set. Locally valid selected
rows thiếu order đi deferred `missing_order`; rejects/duplicates không defer.
Original payload/comment/lineage và retry limits giữ immutable. Generic bounded
resolver dùng tối đa 1,000 pending rows/pass; queue, attempts, staging và audit
commit/rollback cùng transaction. Expired rows không load hoặc reopen. Retry dùng
đủ composite key/version; nhiều reviews cùng order không bị collapse.

Staging dùng authoritative source_sequence và business-v1 hash. Newer versions
không bị stale replay ghi đè; same-version/different-hash fail cả batch. Accepted
unchanged versions refresh lineage; cùng load request replay NOOP. Loader kiểm tra
score và typed keys, không áp positive numeric ordinal cho text order_id.

## Commands

Dùng Java 17, locked processing extra và PG/AWS environment như các snapshots khác:

```bash
make migrate
make ingest-order-reviews BRONZE_RUN_ID=reviews_001
make process-order-reviews BRONZE_PREFIX='<reviews Bronze prefix>' SILVER_RUN_ID=reviews_001 ORDER_SNAPSHOT='<full orders Silver prefix>'
make load-warehouse SOURCE_TABLE=order_reviews SILVER_PREFIX='<reviews Silver prefix>' WAREHOUSE_RUN_ID=reviews_001
make register-deferred SOURCE_TABLE=order_reviews SILVER_PREFIX='<reviews Silver prefix>' DEFERRED_RUN_ID=reviews_register_001
make resolve-deferred SOURCE_TABLE=order_reviews PARENT_SNAPSHOT='<full orders Silver prefix>' DEFERRED_RUN_ID=reviews_retry_001
make test-review-integration
```

Source orders parent có thể là standalone v1 hoặc dependency-aware v2, nhưng luôn
FULL. Dùng new retry ID khi parent thay đổi; replay giữ đúng original parent.
Standalone warehouse load vẫn làm BI unavailable theo existing global publication
gate cho tới fresh core SUCCESS, dù reviews chưa thuộc analytics. Không bypass
gate. Core health/dashboard chưa theo dõi reviews backlog; kiểm tra quarantine
riêng. Không expose review comments lên business BI.

## Validation Evidence

Unit checks bao phủ composite text keys, scores, UTC boundary và full-only scope.
Spark fixtures có repeated review IDs, nhiều reviews/order, optional comments,
Unicode/newline, DST gap/fold, chronology warning và reject/deferred precedence.
Disposable integration kiểm tra frozen retry, pinned full parents, corruption,
response loss, empty snapshots, merge versions/conflicts, two-review resolution,
expiry và rollback sau staging nhưng trước resolution commit.

## Local Verification (2026-10-23)

Run `reviews_3919d134e81e43fb97a353064f928ae8`: source/Bronze 99,224 rows,
98,410 distinct review IDs, 100 pages, FULL và không lower bound. Pinned parent
là full orders từ core SUCCESS `dataset_8af5a27253f34d51b47c62a239d0e7f7`;
standalone snapshots không được tuyên bố cùng source transaction với parent cũ.

Silver valid=99,197, rejected=11, deferred=16, duplicates=0, warning=0. Cả 11
rejected rows có ambiguous review_answer_timestamp trong DST fold; independent
PostgreSQL timezone check xác nhận 11 folds/0 gaps. Không chọn fold hoặc sửa time
để làm đẹp kết quả. 16 deferred rows không có order trong pinned valid snapshot;
không tự xem source order tồn tại là đủ để bypass membership.

Warehouse insert 99,197 rows, 98,383 distinct review IDs, zero update/stale;
composite grain giữ multiple reviews/order và repeated review IDs qua orders.
Register 16 physical inputs; bounded retry giữ 16 PENDING, valid/rejected=0.
Bronze/Silver/load/register/resolve replay NOOP; không double load hoặc tăng thêm
retry attempts. Tất cả pipeline cursors giữ nguyên. BI publication unavailable
sau standalone load, reader bị từ chối SELECT raw_staging.order_reviews; Metabase
không được mở lại hoặc bypass gate.

125 unit, 2 targeted Spark processing tests, 28 integration cases (3 reviews,
8 merge, 8 deferred state, 6 existing children, 1 full gate, 2 backfill) đã pass,
cùng Ruff, contracts/config/shell checks và package build. Full/backfill fixture
gates vẫn PASS=192 dbt nodes. Chưa chạy GitHub CI hoặc tích hợp reviews vào core.
Evidence ignored: `build/reports/reviews-local.json`, `reviews-local.log`,
`reviews-local-replay.log`, `reviews-processing.log`, `reviews-integration.log`
và `reviews-regression.log` trong cùng `build/reports/`.
