# Synthetic Website Events

Status: **Active — standalone generator, synthetic-events-v1**. Đây là workload
cho Phase 6, độc lập với tám Olist datasets, staging/dbt, deferred queues và
completion cursors. Chưa có ingestion, event marts hoặc engine benchmark.

## Commands

```bash
make generate-events
make verify-events
make generate-events EVENT_ROWS=10000000 EVENT_SEED=43 EVENT_OUTPUT=data/generated/events/10m_seed43
olist-synthetic-events generate --rows 100 --seed 42 --output-dir /tmp/events-fixture
olist-synthetic-events verify --output-dir /tmp/events-fixture
```

Mặc định: 2.000.000 **physical rows**, seed 42, output
`data/generated/events/scale_v1/`. Rows nhận 1–10.000.000; seed là unsigned
32-bit integer. Chạy từ working directory; không cần CSV, Docker, Java hoặc Spark.
Generator chỉ dùng standard library và canonical serializer hiện có.

## Event Schema

| Field | Type | Semantics |
|---|---|---|
| `event_id` | string | Logical event identity, seed + source ordinal; duplicates giữ ID/payload |
| `session_id` | string | Session identity trong workload của seed |
| `anonymous_id` | string | Visitor identity, hai sessions liên tiếp dùng chung visitor |
| `customer_unique_id` | nullable string | Chỉ có ở identified sessions; không phải Olist shopper FK |
| `event_type` | string | `page_view`, `product_view`, `add_to_cart`, `checkout_started`, `purchase` |
| `event_time` | UTC timestamp string | Synthetic occurrence instant, ISO seconds với `Z` |
| `received_at` | UTC timestamp string | Synthetic arrival instant, không phải thời gian chạy generator |
| `product_id` | nullable string | NULL cho page view; một synthetic product cố định mỗi session |
| `order_id` | nullable string | Chỉ purchase có synthetic order ID; không phải Olist order FK |

Tất cả IDs có prefix `syn_*`; seed/ordinal không phải thông tin định danh người
thật. Dữ liệu là mô phỏng, không dùng để suy luận conversion, retention hoặc
revenue của Olist. Các kích thước cùng seed chia sẻ logical IDs; mỗi artifact
là một workload riêng, không phải batch mới để append vào core pipeline.

## Session and Arrival Rules

Bốn session templates lặp theo thứ tự: anonymous browse (2 events), identified
cart abandonment (3), identified checkout abandonment (4), identified purchase
(5). Mỗi chu kỳ có 14 unique events. Session bắt đầu cách nhau 5 giây từ
`2024-01-01T00:00:00Z`; events trong session cách nhau 1 giây. SHA-256 của
version/seed/session quyết định product và jitter 0–3 giây. Tỷ lệ scenarios cố
định để tái lập workload, không phải phân phối hành vi đã được quan sát.

Mỗi source ordinal thứ 20 trễ 72 giờ; nếu không thuộc nhóm đó, mỗi ordinal thứ
7 trễ 20 giây; các events khác trễ 1 giây. Jitter được cộng sau delay. File được
sắp theo `(received_at, event_id)` bằng heap; source-time watermark cho phép flush
sớm. Buffer phụ thuộc delay/rate cố định, không tăng theo tổng số rows.

Mỗi physical row thứ 100 là bản sao chính xác của row trước, kể cả timestamps.
Vì vậy `unique_events = rows - floor(rows/100)`. Generator drain các late arrivals
ở cuối logical prefix. Một session cuối theo logical source prefix có thể chưa đủ events: manifest ghi
`partial_session_id`, loại session này khỏi `completed_by_scenario`; không gán
nó thành abandonment. Artifact nhỏ có thể chưa chứa đủ mọi scenario/delay.

Manifest event-type/anonymous/late/out-of-order counts tính trên **unique events**.
Late nghĩa là arrival delay >48 giờ; out-of-order nghĩa là `event_time` nhỏ hơn
maximum occurrence instant đã thấy trong arrival order. Hai metric khác nhau.
`cart_abandonment` trong scenario counts chỉ session dừng sau add-to-cart;
`checkout_abandonment` là scenario riêng, không có purchase.

## Publication and Verification

Output có đúng `events.jsonl`, `manifest.json`, `_SUCCESS`. Payload canonical
UTF-8 JSONL, không có wall-clock timestamps hoặc machine paths trong artifact.
Manifest pin generator version, request, schema, byte size, SHA-256, counts và
session summary. Marker pin SHA-256 của manifest bytes. Publish bằng rename
một temporary directory cùng parent sau khi đóng đủ files; lỗi cleanup phần tạm.
Đây là atomic visibility trên local filesystem, không phải cam kết power-loss
recovery. Concurrent publishers không có locking; không replace artifact đã có.

Cùng request trả NOOP sau full verification và giữ bytes/mtimes. Request khác,
missing marker, extra files, symlinks, metadata sai hoặc corruption đều fail;
dùng output directory mới khi đổi config/version. Verify tái tạo stream theo
version/seed, so sánh từng canonical record và kiểm tra manifest/marker. Việc
này giữ RAM bounded, bắt được cả payload bị sửa rồi tính lại checksum, nhưng
chi phí CPU tương tự generation. Verify chỉ chấp nhận artifact của generator
v1, không phải validator cho dữ liệu website bên ngoài.

## Validation

`tests/unit/test_synthetic_events.py` dùng independent 100-row expectations cho
counts/profiles, kiểm tra UTC/delays/arrival order, small prefixes, replay,
corruption/forged hashes, failure cleanup và lazy source consumption. CLI được
kiểm tra từ ngoài checkout, gồm installed-wheel tests trong fast CI. Generated
artifacts ở ignored `data/generated/`; không commit dữ liệu hoặc kết quả benchmark
chưa đo. Engine benchmark là bước tiếp theo.

## Local Acceptance (2026-10-24)

Default seed 42 đã sinh và verify đủ 2.000.000 physical rows → 1.980.000 unique
events, 20.000 duplicate rows, 99.000 late và 572.116 out-of-order unique events.
Có 282.858 anonymous unique events; 565.714 completed sessions và một censored
session. Payload 717.712.624 bytes, SHA-256
`066a4855eeee2d54bed189be01693a3651ca1aef8915d4ce99e61d7dfe50c814`.

Generation/verification lần lượt 19,62/19,50 giây; `/usr/bin/time` ghi max process
RSS 25.856/25.916 KiB. Đây là số đo local của generator/verifier, không phải peak
RAM toàn stack, cold-cache benchmark hoặc kết quả pandas/Polars/Spark.
Independent streaming audit đọc toàn bộ payload mà không import generator:
kiểm tra ordinal coverage, identities/schema, templates, UTC/delays/arrival order,
exact duplicates, unique counts và SHA-256 đều PASS.

135 fast tests PASS trên Python 3.10 và 3.12; 135 installed-wheel tests PASS trên
Python 3.10, wheel/sdist build PASS. Artifact 2.000 rows từ Python 3.10 installed
wheel và Python 3.12 có bytes giống nhau cho cả ba files. Evidence local ở ignored
`build/reports/events-*`; chưa xác minh hosted CI cho thay đổi generator này.
