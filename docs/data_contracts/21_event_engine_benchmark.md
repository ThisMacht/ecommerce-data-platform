# Event Engine Benchmark

Status: **Active — local correctness and measurement harness**. Benchmark dùng
artifact `synthetic-events-v1` đã verify, độc lập với PostgreSQL/MinIO, Olist
staging/dbt, BI grants và completion cursors. Cần Linux, Java 17, uv và optional
extra `benchmark`; không có distributed cluster hoặc ClickHouse.

## Commands

```bash
make sync-benchmark
# JAVA_HOME phải trỏ tới JDK 17 trên máy đang chạy.
make test-benchmark
make benchmark-events
# Mỗi lần chạy giữ evidence trong một directory mới:
make benchmark-events BENCHMARK_OUTPUT=build/reports/events-benchmark-r2
```

Input mặc định: `EVENT_OUTPUT=data/generated/events/scale_v1` (chạy
`make generate-events` trước nếu chưa có). Defaults: 3 rounds, 4 CPUs,
`BENCHMARK_MEMORY_MIB=4096`. Override bằng `BENCHMARK_REPEATS`, `BENCHMARK_CPUS`,
`BENCHMARK_MEMORY_MIB`; CPU phải fit affinity hiện có và không quá 8. Root vẫn
hỗ trợ Python 3.10: pandas 2.3.3 cho 3.10, pandas 3.1.0 cho 3.11+; Polars 2.0.0,
PySpark 4.0.2 và psutil 7.2.2 được lock. Report ghi versions thực tế, không gộp
measurements giữa hai Python/pandas versions.

## Workload and Oracle

Mỗi engine đọc JSONL với explicit string/null schema, project sáu columns cần
thiết, global deduplicate theo `event_id`, parse UTC, derive occurrence date rồi
aggregate theo `(event_day, event_type)`. Duplicate payload phải identical; artifact
verification xác minh điều này trước trial. Workload không phân tích session
abandonment và không tính tiền hoặc join Olist.

| Output | Definition |
|---|---|
| `event_count` | Unique events trong group |
| `anonymous_count` | `customer_unique_id IS NULL`; empty string không phải NULL |
| `late_count` | `received_at - event_time > 48h`; đúng 48h không late |
| `purchase_count` | Type purchase và order_id không NULL |
| `lag_seconds_sum` | Tổng integer elapsed seconds, timestamps v1 có second precision |

Oracle standard library đọc actual records, global dedup bằng set và aggregate
độc lập; không lấy expected groups từ generator counts. Expected rows được đối
chiếu đầy đủ sau sort/canonicalization. Fixture thủ công ở
`tests/fixtures/benchmark/` có non-adjacent duplicates, UTC midnight, NULL/empty
string và 48h boundaries; expected groups được chốt riêng. Oracle/input verify
chạy ngoài timed worker và có làm ấm filesystem cache.

## Implementations and Resource Controls

- pandas đọc chunks 100.000 rows, Python-backed nullable strings, concat rồi
  dedup/aggregate eager. Đây là bounded parsing, không phải toàn workload streaming.
- Polars dùng lazy NDJSON scan và streaming collect; optimizer có thể fuse/project.
- Spark SQL local dùng `local[N]`, driver heap 2 GiB, shuffle partitions `2*N`, AQE
  tắt; report ghi input partitions thực tế. Không collect raw events vào Python.

Các implementation thực hiện cùng logical task, không dùng cùng physical plan
hoặc memory strategy; đây không phải exhaustive tuning của từng engine.

Mỗi trial dùng process mới và cùng Linux CPU affinity subset; thread env được
pin, descendants/JVM kế thừa affinity. Chạy tuần tự, rotate engine order mỗi
round. Implementation hash pin benchmark/Spark factory/generator helpers;
worker version/affinity/hash và input stat/hash phải khớp. Không sửa code/deps hoặc
input giữa trials. Report giữ từng kết quả, log và oracle fingerprint.

Admission cần RAM khả dụng >= RSS budget + 1 GiB, kiểm tra lại trước mỗi trial.
RSS sampling mỗi 50ms gồm Python và recursive descendants/JVM; vượt budget,
host available <512 MiB hoặc timeout 900s sẽ terminate process group riêng của
worker. Đây là sampled soft guard, không phải cgroup/hard memory quota. Cleanup
chỉ tác động worker/descendants được theo dõi. Không kill task khác hoặc evict OS
caches. Preflight/source/input drift abort và giữ evidence đã có; dùng output
mới để chạy lại. Worker failure/correctness mismatch được ghi FAIL và trả exit 1.

## Measurements and Interpretation

`report.json` và `report.md` ở ignored output directory; không overwrite report
cũ. JSON giữ CPU model/mask, host memory/swap snapshot, platform/Python/dependency
versions, config, source/input hashes và từng trial. CI chỉ chạy small correctness
fixtures/guards trên Python 3.10/3.12, không có speed thresholds hoặc large benchmark.

- Startup: library import và SparkSession initialization trong worker.
- Work: JSON read, dedup, timestamp transforms, aggregation và materialize/sort
  output nhỏ; fused engines không tách timing read/compute giả định.
- Wall: parent từ process launch qua exit/descendant cleanup, gồm interpreter và
  teardown. Median startup + median work không nhất thiết bằng median wall.
- Throughput: physical rows từ verified manifest / median work seconds.
- Memory: maximum **sampled sum of tree RSS** qua trials, có thể double count
  shared pages và bỏ lỡ spikes; không phải PSS hoặc RAM toàn host. Oracle/monitor
  memory ở parent không được tính là engine RSS.

Chỉ xuất medians khi mọi round của engine đều PASS; failed/partial engine không
được lặng lẽ loại khỏi comparison. Trial logs/result JSON được giữ để điều tra.
Filesystem cache không được reset; không gọi đây là cold-cache/cold-load benchmark.
Host vẫn có ứng dụng khác, CPU frequency/load không được kiểm soát độc quyền.
Results chỉ áp dụng cho input, workload, versions và cấu hình đã đo; không suy
rộng thành engine speedup chung hoặc end-to-end Olist performance.

## Primary References

API đọc/streaming: [pandas read_json](https://pandas.pydata.org/pandas-docs/version/2.3/reference/api/pandas.read_json.html),
[Polars scan_ndjson](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_ndjson.html),
[Polars collect](https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html).
Dedup/monitoring: [Spark dropDuplicates](https://spark.apache.org/docs/4.0.2/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html),
[psutil](https://psutil.io/). Timings trong report phải có nguồn từ actual trials,
không phải từ các tài liệu API này.

## Local Results (2026-10-24)

2.000.000 physical rows / 1.980.000 unique events; chín trials đều PASS và trả
cùng 165 groups đúng với oracle (99.000 late, 141.428 purchases). Ba rounds,
4 CPUs affinity `[0,1,2,3]`, RSS guard 4096 MiB, Spark 6 input / 8 shuffle
partitions, AQE off. Polars thực tế 4 threads; pandas Python string storage.
Host Intel Core i5-1235U, Python 3.12.13, pandas 3.1.0, Polars 2.0.0, Spark
4.0.2, Java 17.0.20.1. Tổng harness 155,93 giây gồm untimed input/oracle checks.

| Engine | Startup median (s) | Work median (s) | Wall median (s) | Max sampled tree RSS (MiB) |
|---|---:|---:|---:|---:|
| pandas | 0.167 | 17.076 | 17.358 | 1607.0 |
| polars | 0.060 | 2.086 | 2.376 | 1564.5 |
| spark | 5.695 | 14.369 | 21.098 | 1822.7 |

Polars có work/wall median thấp nhất trong cấu hình/workload này. Spark work
median thấp hơn pandas nhưng wall cao hơn vì startup/teardown. Đây không phải
kết luận chung cho mọi dataset hoặc distributed scale; chưa chạy 10M, cold-cache,
Parquet hoặc tuning variants. Max RSS là estimator đã mô tả phía trên.

Attempt đầu dừng preflight khi RAM còn khoảng 2,8 GiB, chưa có engine trial.
Sau khi task khác hoàn tất, lượt r2 được admit với RAM đủ reserve và hoàn thành
đủ 9/9 trials. Không loại bỏ failed engine trials khỏi medians.

Evidence: ignored `build/reports/events-benchmark-2m-r2/report.{json,md}`,
worker logs/result JSON, `benchmark-2m-r2.log`, `benchmark-2m-r2-time.txt` và
`benchmark-java-version.txt`. Input SHA-256 pin trong report. Implementation fingerprint:
`b79ba7079f16a1b2b4960ead8e530e3f89b8d36e6bbb2f3a9d6f14dfb917668e`.

Validation: 138 fast tests trên Python 3.10/3.12, 138 installed-wheel tests trên
Python 3.10, 5 engine/guard tests trên cả hai versions, wheel/sdist build và
actionlint PASS. Installed wheel implementation hash khớp measured source.
Hosted CI mới chưa được xác minh cho thay đổi benchmark; các lane mới chỉ chạy
small correctness fixtures, không đo large performance trong CI.
