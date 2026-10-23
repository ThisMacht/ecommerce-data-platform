"""Read-only operational checks over committed local pipeline audit records."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from olist_platform.raw.profiling import atomic_write_text

from .control import ANALYTICS_SOURCES, BACKFILL, INCREMENTAL, PIPELINE
from .dashboard import render


def load_thresholds(path: Path) -> dict:
    limits = json.loads(path.read_text(encoding="utf-8"))
    keys = {
        "max_freshness_hours",
        "max_reject_rate",
        "max_warning_rate",
        "max_pending_age_hours",
    }
    if (
        not isinstance(limits, dict)
        or set(limits) != {"max_run_seconds", "datasets"}
        or not isinstance(limits["datasets"], dict)
        or set(limits["datasets"]) != set(ANALYTICS_SOURCES)
    ):
        raise ValueError(
            "Expected runtime limit and thresholds for all pipeline datasets"
        )
    for dataset in limits["datasets"].values():
        if not isinstance(dataset, dict) or set(dataset) != keys:
            raise ValueError("Dataset threshold keys are invalid")
    for group in [
        {"max_run_seconds": limits["max_run_seconds"]},
        *limits["datasets"].values(),
    ]:
        for name, value in group.items():
            if (
                type(value) not in (int, float)
                or (type(value) is float and not math.isfinite(value))
                or (not 0 <= value <= 1 if name.endswith("_rate") else value <= 0)
            ):
                raise ValueError(f"Invalid numeric threshold: {name}")
    return limits


def evaluate(now, latest, success, counters, queue, limits, pipeline=PIPELINE) -> dict:
    issues = []
    run = None
    if latest is None:
        issues.append("pipeline.missing_run")
    else:
        runtime = (
            (latest["completed_at"] or now) - latest["started_at"]
        ).total_seconds()
        run = {
            "run_id": latest["run_id"],
            "status": latest["status"],
            "attempt": latest["attempt"],
            "failed_task": latest["failed_task"],
            "elapsed_seconds": runtime,
        }
        if latest["status"] == "FAILED":
            issues.append("pipeline.latest_run_failed")
        if runtime > limits["max_run_seconds"]:
            issues.append("pipeline.runtime_exceeded")
    freshness = (
        (now - success["completed_at"]).total_seconds() / 3600 if success else None
    )
    datasets = {}
    for table in ANALYTICS_SOURCES:
        policy = limits["datasets"][table]
        counts = counters.get(table)
        pending = queue.get(table, {})
        metrics = {
            "freshness_hours": freshness,
            "processing_input_rows": counts["processing_input_rows"]
            if counts
            else None,
            "reject_rate": None,
            "warning_rate": None,
            "pending_rows": pending.get("pending_rows", 0),
            "expired_rows": pending.get("expired_rows", 0),
            "oldest_pending_age_hours": pending.get("oldest_pending_age_hours"),
            "due_pending_rows": pending.get("due_pending_rows", 0),
        }
        for key in (
            "dimension_snapshot_rows",
            "review_snapshot_rows",
            "source_rows_extracted",
            "source_bootstrap_rows",
            "source_backfill_rows",
            "source_after_cursor_rows",
            "source_lookback_rows",
            "deferred_retry_input_rows",
            "full_parent_source_rows",
        ):
            metrics[key] = counts.get(key) if counts is not None else None
        if success is None:
            issues.append(f"{table}.missing_success")
        else:
            if freshness > policy["max_freshness_hours"]:
                issues.append(f"{table}.freshness_exceeded")
            if counts is None:
                issues.append(f"{table}.missing_counters")
            else:
                for metric, count in (
                    ("reject_rate", "rejected_rows"),
                    ("warning_rate", "warning_rows"),
                ):
                    denominator = counts["processing_input_rows"]
                    metrics[metric] = (
                        counts[count] / denominator if denominator else 0.0
                    )
                    if metrics[metric] > policy[f"max_{metric}"]:
                        issues.append(f"{table}.{metric}_exceeded")
        age = metrics["oldest_pending_age_hours"]
        if age is not None and age > policy["max_pending_age_hours"]:
            issues.append(f"{table}.pending_age_exceeded")
        if metrics["due_pending_rows"]:
            issues.append(f"{table}.pending_resolution_due")
        datasets[table] = metrics
    return {
        "status": "UNHEALTHY" if issues else "HEALTHY",
        "checked_at": now.isoformat(),
        "pipeline_name": pipeline,
        "latest_run": run,
        "last_success_run_id": success["run_id"] if success else None,
        "thresholds": limits,
        "datasets": datasets,
        "issues": issues,
    }


def report(connection, limits: dict, pipeline=PIPELINE) -> dict:
    if pipeline not in (PIPELINE, INCREMENTAL, BACKFILL):
        raise ValueError("Unsupported pipeline namespace")
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.execute("SET LOCAL statement_timeout='15s'")
        with connection.cursor(row_factory=dict_row) as reader:
            now = reader.execute("SELECT CURRENT_TIMESTAMP AS now").fetchone()["now"]
            latest = reader.execute(
                "SELECT run_id,status,attempt,failed_task,started_at,completed_at FROM audit.pipeline_runs WHERE pipeline_name=%s ORDER BY (status='RUNNING') DESC,COALESCE(completed_at,started_at) DESC,run_id DESC LIMIT 1",
                (pipeline,),
            ).fetchone()
            success = reader.execute(
                "SELECT run_id,completed_at FROM audit.pipeline_runs WHERE pipeline_name=%s AND status='SUCCESS' ORDER BY completed_at DESC,run_id DESC LIMIT 1",
                (pipeline,),
            ).fetchone()
            counters = {}
            if success:
                counters = {
                    row["source_table"]: row["counters"]
                    for row in reader.execute(
                        "SELECT source_table,counters FROM audit.pipeline_dataset_runs WHERE run_id=%s",
                        (success["run_id"],),
                    ).fetchall()
                }
            queue = {
                row["source_table"]: row
                for row in reader.execute(
                    """SELECT source_table,
                        COUNT(*) FILTER (WHERE retry_status='PENDING') AS pending_rows,
                        COUNT(*) FILTER (WHERE retry_status='EXPIRED') AS expired_rows,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(first_seen_at)
                            FILTER (WHERE retry_status='PENDING')))::double precision / 3600
                            AS oldest_pending_age_hours,
                        COUNT(*) FILTER (WHERE retry_status='PENDING' AND
                            (attempt_count >= max_attempts OR first_seen_at +
                            max_deferred_age_hours * INTERVAL '1 hour' <= CURRENT_TIMESTAMP))
                            AS due_pending_rows
                    FROM audit.quarantine_records GROUP BY source_table"""
                ).fetchall()
            }
    return evaluate(now, latest, success, counters, queue, limits, pipeline)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--thresholds", type=Path, default=Path("config/pipeline_health.json")
    )
    parser.add_argument(
        "--html-output", type=Path, help="Write a standalone HTML snapshot"
    )
    parser.add_argument(
        "--pipeline", choices=(PIPELINE, INCREMENTAL, BACKFILL), default=PIPELINE
    )
    args = parser.parse_args()
    try:
        limits = load_thresholds(args.thresholds)
        with psycopg.connect(
            "",
            autocommit=True,
            connect_timeout=10,
            application_name="olist-pipeline-health",
        ) as connection:
            result = report(connection, limits, args.pipeline)
        if args.html_output:
            atomic_write_text(args.html_output, render(result))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["status"] == "HEALTHY" else 1
    except (OSError, ValueError, psycopg.Error) as error:
        print(
            f"Pipeline health unavailable ({type(error).__name__}); check thresholds, PG environment and audit migrations",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
