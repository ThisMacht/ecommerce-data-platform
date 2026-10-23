"""Compare local event engines with an independent oracle and fresh processes."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from olist_platform.raw.manifest import sha256_file
from olist_platform.raw.profiling import atomic_write_text
from olist_platform.source_simulator.batches import canonical_bytes, source_time
from olist_platform.source_simulator.events import SCHEMA, verify

ENGINES = ("pandas", "polars", "spark")
KEYS = ("event_day", "event_type")
METRICS = (
    "event_count",
    "anonymous_count",
    "late_count",
    "purchase_count",
    "lag_seconds_sum",
)
COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "received_at",
    "customer_unique_id",
    "order_id",
)


def implementation_hash() -> str:
    package = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for name in (
        "benchmark/events.py",
        "silver/orders.py",
        "source_simulator/events.py",
        "source_simulator/batches.py",
    ):
        digest.update(name.encode() + b"\0" + (package / name).read_bytes())
    return digest.hexdigest()


def normalized(rows: list[dict]) -> list[dict]:
    return sorted(
        [
            {**{k: str(r[k]) for k in KEYS}, **{k: int(r[k]) for k in METRICS}}
            for r in rows
        ],
        key=lambda r: tuple(r[k] for k in KEYS),
    )


def oracle(path: Path) -> list[dict]:
    """Read actual records; do not derive expected groups from generator counts."""
    seen = set()
    groups = defaultdict(lambda: dict.fromkeys(METRICS, 0))
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            if event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            occurred = source_time(event["event_time"])
            lag = int((source_time(event["received_at"]) - occurred).total_seconds())
            counts = groups[(occurred.date().isoformat(), event["event_type"])]
            counts["event_count"] += 1
            counts["anonymous_count"] += event["customer_unique_id"] is None
            counts["late_count"] += lag > 172800
            counts["purchase_count"] += (
                event["event_type"] == "purchase" and event["order_id"] is not None
            )
            counts["lag_seconds_sum"] += lag
    return normalized([dict(zip(KEYS, key)) | value for key, value in groups.items()])


def _pandas(pd, path: Path) -> list[dict]:
    with pd.read_json(
        path,
        lines=True,
        chunksize=100_000,
        convert_dates=False,
        dtype={name: pd.StringDtype(storage="python") for name in SCHEMA},
    ) as reader:
        frame = pd.concat((chunk[list(COLUMNS)] for chunk in reader), ignore_index=True)
    frame = frame.drop_duplicates("event_id")
    occurred = pd.to_datetime(
        frame["event_time"], format="%Y-%m-%dT%H:%M:%SZ", utc=True
    )
    received = pd.to_datetime(
        frame["received_at"], format="%Y-%m-%dT%H:%M:%SZ", utc=True
    )
    frame["event_day"] = occurred.dt.strftime("%Y-%m-%d")
    frame["lag_seconds_sum"] = (received - occurred).dt.total_seconds().astype("int64")
    frame["event_count"] = 1
    frame["anonymous_count"] = frame["customer_unique_id"].isna().astype("int64")
    frame["late_count"] = (frame["lag_seconds_sum"] > 172800).astype("int64")
    frame["purchase_count"] = (
        (frame["event_type"] == "purchase") & frame["order_id"].notna()
    ).astype("int64")
    return (
        frame.groupby(list(KEYS), dropna=False, sort=False)[list(METRICS)]
        .sum()
        .reset_index()
        .to_dict("records")
    )


def _polars(pl, path: Path) -> list[dict]:
    frame = (
        pl.scan_ndjson(path, schema={name: pl.String for name in SCHEMA})
        .select(COLUMNS)
        .unique(subset="event_id", keep="any")
        .with_columns(
            [
                pl.col(name).str.to_datetime(
                    "%Y-%m-%dT%H:%M:%SZ", time_zone="UTC", time_unit="us"
                )
                for name in ("event_time", "received_at")
            ]
        )
        .with_columns(pl.col("event_time").dt.strftime("%Y-%m-%d").alias("event_day"))
    )
    lag = (pl.col("received_at") - pl.col("event_time")).dt.total_seconds()
    return (
        frame.group_by(list(KEYS))
        .agg(
            pl.len().cast(pl.Int64).alias("event_count"),
            pl.col("customer_unique_id")
            .is_null()
            .sum()
            .cast(pl.Int64)
            .alias("anonymous_count"),
            (lag > 172800).sum().cast(pl.Int64).alias("late_count"),
            ((pl.col("event_type") == "purchase") & pl.col("order_id").is_not_null())
            .sum()
            .cast(pl.Int64)
            .alias("purchase_count"),
            lag.sum().alias("lag_seconds_sum"),
        )
        .collect(engine="streaming")
        .to_dicts()
    )


def _spark(spark, path: Path) -> tuple[list[dict], int]:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    frame = (
        spark.read.schema(
            T.StructType([T.StructField(name, T.StringType(), True) for name in SCHEMA])
        )
        .option("mode", "FAILFAST")
        .json(str(path))
        .select(*COLUMNS)
    )
    input_partitions = frame.rdd.getNumPartitions()
    frame = frame.dropDuplicates(["event_id"])
    for name in ("event_time", "received_at"):
        frame = frame.withColumn(name, F.to_timestamp(name, "yyyy-MM-dd'T'HH:mm:ssX"))
    frame = frame.withColumn("event_day", F.date_format("event_time", "yyyy-MM-dd"))
    lag = F.col("received_at").cast("long") - F.col("event_time").cast("long")
    rows = (
        frame.groupBy(*KEYS)
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.col("customer_unique_id").isNull().cast("long")).alias(
                "anonymous_count"
            ),
            F.sum((lag > 172800).cast("long")).alias("late_count"),
            F.sum(
                (
                    (F.col("event_type") == "purchase") & F.col("order_id").isNotNull()
                ).cast("long")
            ).alias("purchase_count"),
            F.sum(lag).alias("lag_seconds_sum"),
        )
        .collect()
    )
    return [r.asDict() for r in rows], input_partitions


def execute(engine: str, path: Path, cpus: int) -> dict:
    started = time.perf_counter()
    spark = None
    try:
        if engine == "pandas":
            import pandas as library
        elif engine == "polars":
            import polars as library
        elif engine == "spark":
            from olist_platform.silver.orders import spark_session

            spark = spark_session(f"local[{cpus}]")
            spark.conf.set("spark.sql.shuffle.partitions", cpus * 2)
            spark.conf.set("spark.sql.adaptive.enabled", "false")
        else:
            raise ValueError("Unsupported benchmark engine")
        ready = time.perf_counter()
        partitions = None
        if engine == "pandas":
            rows = _pandas(library, path)
        elif engine == "polars":
            rows = _polars(library, path)
        else:
            rows, partitions = _spark(spark, path)
        rows = normalized(rows)
        finished = time.perf_counter()
        return {
            "engine": engine,
            "version": importlib.metadata.version(
                "pyspark" if engine == "spark" else engine
            ),
            "startup_seconds": ready - started,
            "work_seconds": finished - ready,
            "input_partitions": partitions,
            "affinity": sorted(os.sched_getaffinity(0)),
            "implementation_sha256": implementation_hash(),
            "effective_threads": library.thread_pool_size()
            if engine == "polars"
            else spark.sparkContext.defaultParallelism
            if engine == "spark"
            else None,
            "rows": rows,
        }
    finally:
        if spark is not None:
            spark.stop()


def _stop(child: subprocess.Popen) -> None:
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()


def supervise(
    command: list[str], log: Path, env: dict, memory_mib: int, timeout: float = 900
) -> dict:
    import psutil

    started = time.perf_counter()
    peak = samples = 0
    descendants = {}
    reason = None
    with log.open("w") as stream:
        child = subprocess.Popen(
            command,
            stdout=stream,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        try:
            while child.poll() is None:
                try:
                    parent = psutil.Process(child.pid)
                    children = parent.children(recursive=True)
                    descendants.update({p.pid: p for p in children})
                    rss = 0
                    for process in [parent, *children]:
                        try:
                            rss += process.memory_info().rss
                        except (psutil.NoSuchProcess, psutil.ZombieProcess):
                            pass
                    peak = max(peak, rss)
                    samples += 1
                except psutil.NoSuchProcess:
                    pass
                if peak > memory_mib * 1024**2:
                    reason = "sampled RSS budget exceeded"
                elif psutil.virtual_memory().available < 512 * 1024**2:
                    reason = "host available memory below 512 MiB"
                elif time.perf_counter() - started > timeout:
                    reason = "worker timeout"
                if reason:
                    _stop(child)
                    break
                time.sleep(0.05)
            child.wait()
        except BaseException:
            _stop(child)
            raise
        finally:
            _, alive = psutil.wait_procs(list(descendants.values()), timeout=5)
            for process in alive:
                try:
                    process.kill()
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(alive, timeout=3)
    return {
        "wall_seconds": time.perf_counter() - started,
        "peak_tree_rss_bytes": peak if samples else None,
        "rss_samples": samples,
        "returncode": child.returncode,
        "failure": reason,
    }


def run(
    directory: Path,
    output: Path,
    repeats: int = 3,
    cpus: int = 4,
    memory_mib: int = 4096,
) -> dict:
    import psutil

    if not hasattr(os, "sched_getaffinity"):
        raise ValueError("The benchmark requires Linux CPU affinity")
    if type(repeats) is not int or not 1 <= repeats <= 10:
        raise ValueError("Repeats must be an integer from 1 to 10")
    if type(cpus) is not int or not 1 <= cpus <= min(8, len(os.sched_getaffinity(0))):
        raise ValueError("CPUs must fit the process affinity and be between 1 and 8")
    if type(memory_mib) is not int or memory_mib < 1024:
        raise ValueError("RSS budget must be at least 1024 MiB")
    if output.exists():
        raise ValueError(
            "Use a new benchmark output directory to preserve existing results"
        )
    required_memory = (memory_mib + 1024) * 1024**2
    if psutil.virtual_memory().available < required_memory:
        raise ValueError(
            "Not enough available RAM for the RSS budget plus 1 GiB reserve"
        )
    versions = {
        name: importlib.metadata.version(name)
        for name in ("pandas", "polars", "pyspark", "psutil", "numpy", "py4j")
    }
    code = implementation_hash()
    manifest = verify(directory)
    path = (directory / "events.jsonl").resolve()
    expected = oracle(path)
    expected_hash = hashlib.sha256(canonical_bytes(expected)).hexdigest()
    identity = path.stat()
    affinity = sorted(os.sched_getaffinity(0))[:cpus]
    env = os.environ.copy()
    env.update(
        {
            name: str(cpus)
            for name in (
                "POLARS_MAX_THREADS",
                "OMP_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "MKL_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
            )
        }
    )
    env.update(
        JAVA_TOOL_OPTIONS=f"-XX:ActiveProcessorCount={cpus}",
        PYSPARK_SUBMIT_ARGS="--driver-memory 2g pyspark-shell",
        PYSPARK_PYTHON=sys.executable,
        SPARK_LOCAL_IP="127.0.0.1",
    )
    memory_before = psutil.virtual_memory()
    if memory_before.available < required_memory:
        raise ValueError("Not enough available RAM after input verification/oracle")
    output.mkdir(parents=True)
    swap_before = psutil.swap_memory()
    trials = []
    for repeat in range(repeats):
        order = ENGINES[repeat % 3 :] + ENGINES[: repeat % 3]
        for engine in order:
            if implementation_hash() != code:
                raise ValueError("Benchmark implementation changed")
            if psutil.virtual_memory().available < required_memory:
                raise ValueError(
                    "Not enough available RAM for the RSS budget plus 1 GiB reserve"
                )
            actual = path.stat()
            if (actual.st_size, actual.st_mtime_ns, actual.st_ino) != (
                identity.st_size,
                identity.st_mtime_ns,
                identity.st_ino,
            ):
                raise ValueError("Benchmark input changed")
            result_path = output / f"{repeat + 1}_{engine}.json"
            log = output / f"{repeat + 1}_{engine}.log"
            print(
                f"[RUN] round {repeat + 1}/{repeats}: {engine}",
                file=sys.stderr,
                flush=True,
            )
            metrics = supervise(
                [
                    sys.executable,
                    "-m",
                    "olist_platform.benchmark.events",
                    "_worker",
                    "--engine",
                    engine,
                    "--input-path",
                    str(path),
                    "--result-path",
                    str(result_path.resolve()),
                    "--cpus",
                    str(cpus),
                ],
                log,
                env,
                memory_mib,
            )
            trial = {"engine": engine, "round": repeat + 1, **metrics, "status": "FAIL"}
            if (
                metrics["returncode"] == 0
                and not metrics["failure"]
                and metrics["rss_samples"] > 0
                and metrics["peak_tree_rss_bytes"]
                and result_path.is_file()
            ):
                measured = json.loads(result_path.read_text())
                identity_matches = (
                    measured.get("implementation_sha256") == code
                    and measured.get("affinity") == affinity
                    and measured.get("version")
                    == versions["pyspark" if engine == "spark" else engine]
                )
                if measured["rows"] == expected and identity_matches:
                    trial.update({k: v for k, v in measured.items() if k != "rows"})
                    trial.update(status="PASS", output_sha256=expected_hash)
                elif measured["rows"] != expected:
                    trial["failure"] = "result differs from independent oracle"
                else:
                    trial["failure"] = "worker identity/configuration drift"
            elif not trial["failure"]:
                trial["failure"] = "worker failed; inspect its log/result file"
            trials.append(trial)
            print(
                f"[{trial['status']}] {engine}: {trial['wall_seconds']:.3f}s wall",
                file=sys.stderr,
                flush=True,
            )
    if sha256_file(path) != manifest["event_file"]["sha256"]:
        raise ValueError("Input checksum changed during the benchmark")
    if implementation_hash() != code:
        raise ValueError("Benchmark implementation changed")
    summary = {}
    for engine in ENGINES:
        group = [t for t in trials if t["engine"] == engine]
        row = {
            "status": "PASS" if all(t["status"] == "PASS" for t in group) else "FAIL"
        }
        if row["status"] == "PASS":
            row.update(
                {
                    name: statistics.median(t[name] for t in group)
                    for name in ("startup_seconds", "work_seconds", "wall_seconds")
                }
            )
            row["max_peak_tree_rss_bytes"] = max(
                t["peak_tree_rss_bytes"] for t in group
            )
            row["physical_rows_per_work_second"] = (
                manifest["counts"]["physical_rows"] / row["work_seconds"]
            )
        summary[engine] = row
    report = {
        "status": "PASS"
        if all(r["status"] == "PASS" for r in summary.values())
        else "FAIL",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "workload": "event-dedup-utc-day-type-v1",
        "input": manifest,
        "oracle_sha256": expected_hash,
        "implementation_sha256": code,
        "oracle_rows": expected,
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "versions": versions,
            "affinity": affinity,
            "cpu_model": next(
                (
                    line.partition(":")[2].strip()
                    for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")
                ),
                platform.processor(),
            ),
            "logical_cpu_count": psutil.cpu_count(),
            "total_memory_bytes": memory_before.total,
            "available_before_trials_bytes": memory_before.available,
            "swap_used_before_trials_bytes": swap_before.used,
        },
        "settings": {
            "repeats": repeats,
            "cpus": cpus,
            "rss_budget_mib": memory_mib,
            "sample_interval_seconds": 0.05,
            "worker_timeout_seconds": 900,
            "spark_heap": "2g",
            "spark_shuffle_partitions": cpus * 2,
            "spark_aqe": False,
            "pandas_chunksize": 100000,
            "pandas_string_storage": "python",
            "polars_engine": "streaming",
            "cache_policy": "verified input; filesystem cache uncontrolled/warmed, fresh worker processes",
        },
        "trials": trials,
        "summary": summary,
    }
    atomic_write_text(output / "report.json", json.dumps(report, indent=2) + "\n")
    atomic_write_text(output / "report.md", markdown(report))
    return report


def markdown(report: dict) -> str:
    lines = [
        "# Event Engine Benchmark",
        "",
        f"Status: **{report['status']}**; input: {report['input']['counts']['physical_rows']:,} physical rows.",
        "",
        "| Engine | Status | Startup median (s) | Work median (s) | Wall median (s) | Max sampled tree RSS (MiB) |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for engine, row in report["summary"].items():
        if row["status"] == "PASS":
            lines.append(
                f"| {engine} | PASS | {row['startup_seconds']:.3f} | {row['work_seconds']:.3f} | {row['wall_seconds']:.3f} | {row['max_peak_tree_rss_bytes'] / 1024**2:.1f} |"
            )
        else:
            lines.append(f"| {engine} | FAIL | — | — | — | — |")
    lines += [
        "",
        "Cùng workload và independent oracle; mỗi trial dùng process mới, thứ tự engine được rotate.",
        "Filesystem cache đã được verification/oracle làm ấm nhưng không được kiểm soát hoặc evict.",
        "Work gồm đọc JSONL, global dedup, parse UTC, group/aggregate và collect kết quả; wall gồm process startup/teardown.",
        "RSS là sampled sum của Python/descendants gồm JVM, có thể đếm trùng shared pages và bỏ lỡ peaks; không phải PSS hoặc RAM toàn host.",
        "Xem report.json để biết versions, CPU affinity, cấu hình, từng trial và kết quả oracle. Không suy rộng thành engine speedup cho workloads khác.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True, metavar="{run}")
    runner = commands.add_parser(
        "run", help="Compare all three engines on a verified event artifact"
    )
    runner.add_argument(
        "--input-dir", type=Path, default=Path("data/generated/events/scale_v1")
    )
    runner.add_argument(
        "--output-dir", type=Path, default=Path("build/reports/events-benchmark")
    )
    runner.add_argument("--repeats", type=int, default=3)
    runner.add_argument("--cpus", type=int, default=4)
    runner.add_argument("--memory-mib", type=int, default=4096)
    worker = commands.add_parser("_worker")
    worker.add_argument("--engine", choices=ENGINES, required=True)
    worker.add_argument("--input-path", type=Path, required=True)
    worker.add_argument("--result-path", type=Path, required=True)
    worker.add_argument("--cpus", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "_worker":
            os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[: args.cpus])
            result = execute(args.engine, args.input_path, args.cpus)
            atomic_write_text(args.result_path, json.dumps(result, indent=2) + "\n")
            return 0
        report = run(
            args.input_dir, args.output_dir, args.repeats, args.cpus, args.memory_mib
        )
    except (
        OSError,
        ValueError,
        ImportError,
        importlib.metadata.PackageNotFoundError,
    ) as error:
        print(f"Event benchmark: FAIL: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": report["status"],
                "report": str(args.output_dir / "report.json"),
                "summary": report["summary"],
            },
            indent=2,
        )
    )
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
