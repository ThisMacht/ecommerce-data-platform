# ADR-007: Use Local Spark and Separate Scale-up Evidence

## Status

Accepted for the batch MVP and Phase 6 workload preparation.

## Context

Olist MVP hiện có khoảng 1,55 triệu source rows trong tám streams, phần lớn là
geolocation. Full rerun đã hoàn tất bằng local Spark; correctness, transactional
merges và audit gates là yêu cầu chính. Kích thước này không tự chứng minh cần
một distributed cluster. Tại thời điểm khởi tạo Phase 6.1 chưa có kết quả so sánh pandas/Polars/PySpark.

## Decision

Giữ local PySpark cho normalization/routing và abstraction xử lý dữ liệu hiện có.
Olist MVP không bắt buộc cần distributed processing. Không thêm cluster hoặc
ClickHouse chỉ để mở rộng danh sách công nghệ.

Phase 6.1 sinh standalone synthetic website events bằng bounded-memory standard
library generator. IDs không tham chiếu Olist; artifact không mở rộng core
contracts/cursors/grants. JSONL pin schema, request và checksums để các engine
sau này có cùng input có thể tái lập.

## Consequences

Generator throughput/RSS và full-pipeline warm rerun không phải bằng chứng engine
speedup. Benchmark tiếp theo phải dùng cùng công việc, input và correctness
oracle; ghi startup overhead, CPU/memory limits, partitions, cache conditions và
execution environment. Chỉ quyết định scale-up dựa trên measurements đã thu thập.
Streaming, event marts và cloud deployment chưa được triển khai bởi quyết định này.

## Follow-up Evidence

Phase 6.2 đã đo local JSONL workload 2M rows, 3 fresh-process trials/engine, cùng
4 CPU affinity và independent oracle. [Benchmark contract](../data_contracts/21_event_engine_benchmark.md)
ghi actual results và limitations; đây không phải evidence cho distributed
scale-up hoặc full Olist speedup. Quyết định giữ cluster/ClickHouse ngoài MVP
vẫn áp dụng.
