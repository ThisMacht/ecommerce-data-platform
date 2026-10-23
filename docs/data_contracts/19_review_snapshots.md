# Review Snapshots

## Scope and Grain

Reviews chạy full standalone và trong core full/incremental/backfill: Bronze → Silver → version-aware staging. Grain là
`(review_id, order_id)`: cùng review ID có thể thuộc nhiều orders và một order có
thể có nhiều reviews. Không chọn một review/order hoặc dùng review_id riêng lẻ làm
PK. Source contract giữ nguyên; migration `020` thêm delivery state, staging và
quarantine scope. Migration `021` thêm owned full review streams. Core có tám
datasets, 34/38 tasks và bốn completion cursors; có review fact/creation-cohort
mart trong dbt, giữ nguyên BI grants.

## Extraction and Replay

Bronze full scan source trong REPEATABLE READ, kể cả rows cũ hơn delivery cursor.
Composite keyset paging và physical/logical lineage dùng đủ hai keys. Prepared
pages, bounds và bytes giữ nguyên khi retry cùng ID/config. Không đổi source để
resume; new snapshot phải dùng ID mới. Core freeze tám full streams hoặc bốn deltas
và sáu full inputs atomically; reviews luôn FULL, kể cả bounded/empty-window runs.
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

## Core Scheduling and Completion

`make dataset`, `make dataset-incremental` và `make dataset-backfill` tự publish/load/
register/resolve reviews. Pin full orders từ cùng source transaction, không dùng delta.
Full mode dùng owner ID cho source/Silver; bounded modes dùng hashed parent ID.
Load/registration/resolution receipts luôn dùng owner ID. Backfill resolver lọc
origin bằng original full Silver ID, giữ nguyên queue các run khác.

Final gate kiểm đủ tám dataset receipts, reviews parent/deferred hashes và khóa tám
staging tables qua fresh dbt/CAS. dbt artifact source set đủ tám nguồn, bao gồm
review fact/metric reconciliation trên raw staging.
Chỉ bốn delta tables có completion cursors. Full/empty review snapshots không xóa
staging cũ; backfill refreshes current reviews, không khôi phục lịch sử tại end bound.

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
gate cho tới fresh core SUCCESS. Không bypass
gate. Core health/dashboard theo dõi review quality/backlog và `review_snapshot_rows`
riêng; không cộng full scan hoặc retry vào delta growth. Không expose review
comments lên business BI.

## Analytics Semantics

`stg_reviews` → `fct_reviews` → `mart_review_daily`; xem đầy đủ metric policy tại
[analytics contract](09_analytics_models.md#review-facts-and-creation-cohorts).
Fact giữ composite grain, UTC timestamps, local São Paulo creation date/key và
comment/chronology flags; không copy comment text hoặc join orders/items/payments.
Mart review-weighted qua mọi statuses, low score = 1–2, comment presence = title OR
message. Distinct orders không additive qua dates; aggregate score_sum/counts trước
khi chia average/rates. Chronology warnings không loại score khỏi denominator.
Calendar bao gồm review creation dates; empty input không sinh synthetic mart rows.
No new BI grants/cards; full-parent validation vẫn ở Silver cho reviews nằm ngoài
bounded order staging window.

## Validation Evidence

Unit checks bao phủ composite text keys, scores, UTC boundary và full-only scope.
Spark fixtures có repeated review IDs, nhiều reviews/order, optional comments,
Unicode/newline, DST gap/fold, chronology warning và reject/deferred precedence.
Disposable integration kiểm tra frozen retry, pinned full parents, corruption,
response loss, empty snapshots, merge versions/conflicts, two-review resolution,
expiry và rollback sau staging nhưng trước resolution commit.

## Standalone Verification (2026-10-23)

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
gates vẫn PASS=192 dbt nodes. Đây là bằng chứng standalone trước core integration; chưa chạy GitHub CI.
Evidence ignored: `build/reports/reviews-local.json`, `reviews-local.log`,
`reviews-local-replay.log`, `reviews-processing.log`, `reviews-integration.log`
và `reviews-regression.log` trong cùng `build/reports/`.

## Core Integration Validation (2026-10-23)

126 unit tests, 29 disposable integration cases và 3 real Airflow import/
serialization checks đã PASS, cùng Ruff, contracts/config/shell và wheel/sdist
build. Integration gồm 10 control, 1 full gate, 6 incremental, 2 backfill,
2 recovery, 5 item dependencies và 3 standalone reviews cases.

Core fixtures giữ repeated review IDs qua orders và nhiều reviews/order. Full
gate từ chối missing load/deferred receipts, corrupted marker, wrong parent hash
và concurrent review staging writes; rejects không đi vào deferred. Freeze lỗi ở
review stream cuối rollback toàn bộ 8/10 streams; replay giữ bytes sau source
writes/deletes. Backfill kiểm tra current reviews ngoài delta window, own-Silver
origin filtering và unchanged foreign queues/scheduled cursors. Recovery giữ
immutable receipts, queue/attempts và fresh dbt sau rollback ở final gate.

Evidence ignored: `build/reports/reviews-core-fast.log`, `reviews-core-gates.log`,
`reviews-core-integration.log` và `reviews-core-final-checks.log`. GitHub CI chưa
được chạy; integration dùng fixtures, bằng chứng full Olist run ghi riêng bên dưới.

## Core Ingestion Verification (2026-10-23, Before Analytics)

Full run `dataset_7f462f5e178f448d88480321352623c3`, attempt 1 SUCCESS: 34 tasks,
8 reconciled dataset audits, 1,550,851 source rows trong cùng frozen transaction.
Current code hash được đối chiếu sau completion. Bốn completion cursors advance
đúng một lần; review audit watermarks đều NULL. Fresh dbt PASS=192 (25 views,
167 tests), WARN/ERROR/SKIP=0. Chỉ full mode chạy trên canonical Olist ở lượt này;
incremental/backfill được kiểm tra bằng disposable fixtures ở trên.

Review Silver: 99,224 input, 99,197 valid, 11 invalid answer timestamps, 16 missing
orders deferred, zero duplicates/warnings. Pin full orders của chính run này.
Warehouse nhận 99,197 unchanged rows, zero insert/update/stale; staging giữ 98,383
distinct review IDs và 98,646 distinct orders, không collapse composite grain.

Register 16 new physical occurrences, retry 32 occurrences từ run này và backlog
standalone trước đó; cả 32 vẫn thiếu parent, không có retry load. Vì vậy review
processing_input_rows=99,256 và deferred_rows=48 gồm 16 source + 32 retry routing
occurrences, không phải 48 distinct reviews hoặc source growth. 16 occurrences
của run mới đều attempt_count=1. Retry limits/expiry tiếp tục giữ nguyên.

Health HEALTHY, không có issue; HTML snapshot đã refresh. BI publication available
trở lại sau fresh core gate, nhưng business reader vẫn bị từ chối đọc source/
staging reviews. Metabase service vẫn dừng; đây là kiểm tra database publication
và quyền đọc, không phải xác minh UI. Fact counts giữ nguyên: orders/delivery
99,424, items 112,621, payments 103,869.

Evidence ignored: `build/reports/reviews-core-local.json`, `reviews-core-health.json`,
`reviews-core-verify.log`, `reviews-core-dataset.log` và refreshed
`build/reports/pipeline_dashboard.html`. Chưa triển khai review analytics hoặc chạy
GitHub CI; các rows thiếu parent không được bỏ qua validation để đưa vào staging.

## Analytics Verification (2026-10-23)

Locked standalone build trên staging Olist đã PASS 226 nodes (28 views, 198 tests),
đủ tám sources. Review fact có 99,197 composite rows, score_sum=405,361 và mean
4.0864239845963084; mart có 636 local creation cohorts, tổng sums/counts khớp raw
staging. Business reader bị từ chối cả ba review models; fact không chứa comment
text. Controls, deferred attempts và warehouse load receipts giữ nguyên.

Đây là kiểm tra analytics trên baseline core SUCCESS trước đó, không phải một core
execution mới. Xem [chi tiết số liệu và giới hạn](09_analytics_models.md#review-analytics-verification-2026-10-23)
và ignored `build/reports/review-analytics-local.json`. Không chạy lại ingestion
hoặc GitHub CI trong lượt này.
