# Dirty Fixture and Expected Errors

`*_dirty.json` là các patches tổng hợp, được merge với valid defaults tại
`tests/processing/silver_fixture.py`. `dirty_rows(table)` tạo mới inputs mỗi lần,
giữ lỗi money/timestamp/metadata dưới dạng raw values và lưu `_raw_payload`.
Không sửa canonical CSV hoặc ghi invalid records vào PostgreSQL source.

`expected_error_manifest.json` là oracle viết độc lập với Spark validation:

- `input_sha256` pin deterministic JSONL bytes của từng bảng, gồm cả fixture metadata.
- Mỗi physical `_record_uid` có một `case`, `_routing` và exact error/warning/deferred rule lists.
- `parent_keys` là toàn bộ synthetic key set cho FK scenario tương ứng, không phải dataset thật.
- Orders giữ nguyên legacy inputs, trừ customer của `winter` đổi thành `late_customer`.
- UID trong fixture chỉ có phạm vi từng bảng; không thay thế production Bronze lineage hashes.

| Table | Input | Valid | Rejected | Deferred | Duplicates | Valid with warnings |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| customers | 6 | 3 | 2 | 0 | 1 | 1 |
| orders | 13 | 3 | 7 | 1 | 2 | 1 |
| order_items | 13 | 2 | 9 | 1 | 1 | 0 |
| order_payments | 12 | 2 | 8 | 1 | 1 | 1 |

Chạy `make test-dirty-data` với Java 17; `make test-processing` cũng chạy cùng check
trong CI. Tests so sánh từng UID/rule/route, giữ nguyên raw payload, đối soát counts
và in actual summary. Unit tests kiểm tra deterministic bytes, checksum, đầy đủ
oracle identities và injection không mutate inputs của lần gọi khác.

Khi thêm case, tự xác định expected rules từ contract và review riêng; không lấy
Spark output để tự tạo expected results. Cập nhật checksum khi chủ động đổi inputs.
Fixture này chưa bao phủ toàn bộ rule combinations hoặc schema/file-level failures;
các lỗi checksum, schema và publication thuộc integration suites riêng.
