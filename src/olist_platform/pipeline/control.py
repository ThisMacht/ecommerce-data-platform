"""Receipt-gated audit for the local full-snapshot pipeline."""

from __future__ import annotations

import json
import re
from pathlib import Path

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from olist_platform.bronze.extraction import cursor_tuple, state_sql
from olist_platform.bronze.storage import BronzeError, encode, sha256, verify_published
from olist_platform.silver.batch import validate_manifest, verify_silver

PIPELINE = "local_core_v1"
INCREMENTAL = "incremental_core_v1"
TABLES = ("customers", "orders", "order_items", "order_payments")
TASKS = (
    tuple(
        f"{t}.{stage}"
        for t in TABLES
        for stage in ("bronze.extract", "silver.process", "warehouse.load")
    )
    + tuple(
        f"{t}.deferred.{stage}" for t in TABLES[1:] for stage in ("register", "resolve")
    )
    + ("dbt.build",)
)
INCREMENTAL_TASKS = (
    TASKS
    + ("source.freeze",)
    + tuple(
        f"{table}.parent.{stage}"
        for table in TABLES[:2]
        for stage in ("bronze.extract", "silver.process")
    )
)


def parent_run_id(run_id: str) -> str:
    return "fk_" + sha256(encode(run_id))


def required_tasks(row: dict) -> tuple:
    return INCREMENTAL_TASKS if row["pipeline_name"] == INCREMENTAL else TASKS


def code_hash(root: Path) -> str:
    paths = [root / "pyproject.toml", root / "uv.lock", root / "Makefile"]
    paths.extend(
        p
        for p in (root / "airflow/pyproject.toml", root / "airflow/uv.lock")
        if p.exists()
    )
    for directory, extensions in (
        ("src", {".py"}),
        ("scripts", {".py", ".sh"}),
        ("config", {".json"}),
        ("dbt", {".sql", ".yml"}),
        ("airflow/dags", {".py"}),
    ):
        paths.extend(
            p
            for p in (root / directory).rglob("*")
            if p.is_file()
            and p.suffix in extensions
            and not any(
                x in ("target", "logs", "dbt_packages")
                for x in p.relative_to(root / directory).parts
            )
        )
    return sha256(
        encode(
            [[str(p.relative_to(root)), sha256(p.read_bytes())] for p in sorted(paths)]
        )
    )


def one(connection, query, parameters=()):
    with connection.cursor(row_factory=dict_row) as reader:
        return reader.execute(query, parameters).fetchone()


def lock(connection):
    connection.execute("SET LOCAL lock_timeout='15s'")
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtext('olist.pipeline.local_core_v1'))"
    )


def current(connection, run_id: str, attempt: int | None = None) -> dict:
    row = one(
        connection, "SELECT * FROM audit.pipeline_runs WHERE run_id=%s", (run_id,)
    )
    if row is None or (
        attempt is not None
        and (row["attempt"] != attempt or row["status"] != "RUNNING")
    ):
        raise BronzeError("Pipeline run/attempt is not active")
    return row


def begin(connection, run_id: str, request: dict) -> dict:
    if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id) is None:
        raise BronzeError("Invalid pipeline run ID")
    digest = sha256(encode(request))
    mode = request.get("mode", "full")
    if mode not in ("full", "incremental"):
        raise BronzeError("Unsupported pipeline mode")
    pipeline = INCREMENTAL if mode == "incremental" else PIPELINE
    with connection.transaction():
        lock(connection)
        previous = one(
            connection, "SELECT * FROM audit.pipeline_runs WHERE run_id=%s", (run_id,)
        )
        if previous and previous["request_sha256"] != digest:
            raise BronzeError("Pipeline run ID request/configuration conflict")
        if previous and previous["status"] == "SUCCESS":
            return {
                "status": "NOOP",
                "attempt": previous["attempt"],
                "result": previous["result"],
            }
        active = one(
            connection,
            "SELECT run_id FROM audit.pipeline_runs WHERE status='RUNNING'",
        )
        if active and active["run_id"] != run_id:
            raise BronzeError("Resume the active pipeline run before starting another")
        before = {}
        for table in TABLES:
            connection.execute(
                "INSERT INTO audit.pipeline_control (pipeline_name,source_table) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (pipeline, table),
            )
            row = one(
                connection,
                "SELECT committed_cursor,cursor_version FROM audit.pipeline_control WHERE pipeline_name=%s AND source_table=%s",
                (pipeline, table),
            )
            before[table] = row
        if previous:
            if previous["control_before"] != before:
                raise BronzeError("Pipeline control changed since this run began")
            attempt = previous["attempt"] + 1
            connection.execute(
                "UPDATE audit.pipeline_task_runs SET status='FAILED',completed_at=clock_timestamp() WHERE run_id=%s AND status='RUNNING'",
                (run_id,),
            )
            connection.execute(
                "UPDATE audit.pipeline_runs SET status='RUNNING',attempt=%s,completed_at=NULL,failed_task=NULL WHERE run_id=%s",
                (attempt, run_id),
            )
        else:
            attempt = 1
            connection.execute(
                "INSERT INTO audit.pipeline_runs (run_id,pipeline_name,run_type,request,request_sha256,control_before,status) VALUES (%s,%s,%s,%s,%s,%s,'RUNNING')",
                (
                    run_id,
                    pipeline,
                    "incremental" if mode == "incremental" else "manual",
                    Jsonb(request),
                    digest,
                    Jsonb(before),
                ),
            )
        return {"status": "STARTED", "attempt": attempt}


def task(connection, run_id: str, attempt: int, name: str, status: str) -> None:
    if status not in ("RUNNING", "SUCCESS", "FAILED"):
        raise BronzeError("Unsupported pipeline task/status")
    with connection.transaction():
        lock(connection)
        row = current(connection, run_id, attempt)
        if name not in required_tasks(row):
            raise BronzeError("Unsupported pipeline task/status")
        if status == "RUNNING":
            connection.execute(
                "INSERT INTO audit.pipeline_task_runs (run_id,run_attempt,task_name,status) VALUES (%s,%s,%s,'RUNNING')",
                (run_id, attempt, name),
            )
        else:
            changed = connection.execute(
                "UPDATE audit.pipeline_task_runs SET status=%s,completed_at=clock_timestamp() WHERE run_id=%s AND run_attempt=%s AND task_name=%s AND status='RUNNING'",
                (status, run_id, attempt, name),
            ).rowcount
            if changed != 1:
                raise BronzeError("Pipeline task is not running")


def fail(connection, run_id: str, attempt: int, name: str) -> None:
    if name not in INCREMENTAL_TASKS and name != "verification":
        raise BronzeError("Unsupported pipeline failure stage")
    with connection.transaction():
        lock(connection)
        row = current(connection, run_id)
        if row["attempt"] != attempt or row["status"] == "SUCCESS":
            raise BronzeError("Cannot fail a completed or superseded pipeline attempt")
        connection.execute(
            "UPDATE audit.pipeline_task_runs SET status='FAILED',completed_at=clock_timestamp() WHERE run_id=%s AND run_attempt=%s AND status='RUNNING'",
            (run_id, attempt),
        )
        connection.execute(
            "UPDATE audit.pipeline_runs SET status='FAILED',completed_at=clock_timestamp(),failed_task=%s WHERE run_id=%s",
            (name, run_id),
        )


def evidence(connection, store, contract: dict, run_id: str) -> dict:
    from datetime import timedelta

    row = current(connection, run_id)
    incremental = row["pipeline_name"] == INCREMENTAL

    def stream(table, identity, kind):
        receipt = one(
            connection,
            state_sql("SELECT status,manifest FROM {runs} WHERE run_id=%s", table),
            (identity,),
        )
        if not receipt or receipt["status"] != "COMMITTED":
            raise BronzeError("Missing committed Bronze receipt")
        bronze = verify_published(store, receipt["manifest"]["prefix"])
        validate_manifest(bronze, contract, table)
        silver = verify_silver(store, f"silver/{table}/run_id={identity}")
        version = (
            "orders-silver-v2"
            if table == "orders"
            else (
                "customers-silver-v2"
                if table == "customers" and kind == "INCREMENTAL"
                else f"{table}-silver-v1"
            )
        )
        bh = sha256(store.read(f"{bronze['prefix']}/manifest.json"))
        sh = sha256(store.read(f"{silver['prefix']}/manifest.json"))
        if (
            bronze != receipt["manifest"]
            or bronze.get("snapshot_kind") != kind
            or silver.get("snapshot_kind") != kind
            or silver["schema_version"] != version
            or bronze["source"] != f"postgresql.{table}"
            or bronze["run_id"] != identity
        ):
            raise BronzeError("Pipeline snapshot kind, identity or version mismatch")
        if (
            bronze["config"]["contract_sha256"] != sha256(encode(contract))
            or silver["request"]["contract_sha256"] != sha256(encode(contract))
            or silver["request"]["bronze_manifest_sha256"] != bh
            or silver["request"]["bronze_prefix"] != bronze["prefix"]
            or silver["request"]["run_id"] != identity
        ):
            raise BronzeError("Pipeline manifest dependency/contract mismatch")
        expected_lower = None
        if incremental:
            baseline = row["control_before"][table]
            before = baseline["committed_cursor"]
            if kind == "INCREMENTAL" and before is not None:
                expected_lower = (
                    cursor_tuple(before, table)[0] - timedelta(hours=48)
                ).isoformat()
            role = "delta" if kind == "INCREMENTAL" else "parent"
            if (
                bronze["watermark_before"] != before
                or bronze["pipeline_cursor_version_before"]
                != baseline["cursor_version"]
                or bronze["config"].get("pipeline_run_id") != run_id
                or bronze["config"]["pipeline"] != f"incremental_{table}_{role}_v1"
                or bronze["config"]["lookback_hours"] != 48
            ):
                raise BronzeError("Incremental receipt does not use pipeline control")
        if bronze.get("extract_lower_bound") != expected_lower:
            raise BronzeError("Pipeline extraction bounds mismatch")
        return {
            "bronze": bronze,
            "silver": silver,
            "bronze_sha256": bh,
            "silver_sha256": sh,
        }

    parents = (
        {t: stream(t, parent_run_id(run_id), "FULL") for t in TABLES[:2]}
        if incremental
        else {}
    )
    if incremental:
        pin = {
            "prefix": parents["customers"]["silver"]["prefix"],
            "manifest_sha256": parents["customers"]["silver_sha256"],
        }
        if parents["orders"]["silver"]["request"].get("customer_snapshot") != pin:
            raise BronzeError("Full order parent does not pin the full customer parent")
    result = {}
    for table in TABLES:
        item = stream(table, run_id, "INCREMENTAL" if incremental else "FULL")
        silver, sh = item["silver"], item["silver_sha256"]
        load = one(
            connection,
            "SELECT * FROM audit.warehouse_load_runs WHERE source_table=%s AND run_id=%s",
            (table, run_id),
        )
        if (
            not load
            or load["request"].get("silver_manifest_sha256") != sh
            or load["request"].get("silver_prefix") != silver["prefix"]
            or load["silver_counts"] != silver["counts"]
        ):
            raise BronzeError("Missing or mismatched warehouse receipt")
        result[table] = item | {"load": load}
        if incremental:
            result[table]["full_parent_source_rows"] = (
                parents.get(table, {}).get("bronze", {}).get("row_count", 0)
            )
            result[table]["parents"] = {
                t: {
                    "prefix": p["silver"]["prefix"],
                    "manifest_sha256": p["silver_sha256"],
                }
                for t, p in parents.items()
            }
    if (
        incremental
        and len(
            {
                x["bronze"]["source_snapshot"]
                for x in [*result.values(), *parents.values()]
            }
        )
        != 1
    ):
        raise BronzeError("Incremental source snapshots differ")
    for table in TABLES[1:]:
        parent = "customers" if table == "orders" else "orders"
        dependency = "customer_snapshot" if table == "orders" else "order_snapshot"
        pin = {
            "prefix": (parents or result)[parent]["silver"]["prefix"],
            "manifest_sha256": (parents or result)[parent]["silver_sha256"],
        }
        if result[table]["silver"]["request"].get(dependency) != pin:
            raise BronzeError("Pipeline Silver does not pin this run parent snapshot")
        registration = one(
            connection,
            "SELECT * FROM audit.deferred_registration_runs WHERE source_table=%s AND run_id=%s",
            (table, run_id),
        )
        resolution = one(
            connection,
            "SELECT * FROM audit.deferred_resolution_runs WHERE source_table=%s AND run_id=%s",
            (table, run_id),
        )
        if (
            not registration
            or not resolution
            or registration["request"]["silver_manifest_sha256"]
            != result[table]["silver_sha256"]
            or registration["input_rows"]
            != result[table]["silver"]["counts"]["deferred_rows"]
            or resolution["request"].get("parent_snapshot") != pin["prefix"]
            or resolution["request"].get("parent_manifest_sha256")
            != pin["manifest_sha256"]
        ):
            raise BronzeError("Missing or mismatched deferred receipts")
        retry = resolution["result"]
        retry_load = one(
            connection,
            "SELECT * FROM audit.warehouse_load_runs WHERE source_table=%s AND run_id=%s",
            (table, retry["warehouse_run_id"]),
        )
        if (
            not retry_load
            or retry_load["silver_counts"] != retry["routing_counts"]
            or retry_load["request"].get("resolution_run_id") != run_id
        ):
            raise BronzeError("Missing deferred staging receipt")
        result[table].update(
            registration=registration, resolution=resolution, retry_load=retry_load
        )
    return result


def reconcile(item: dict) -> dict:
    bronze, silver, load = item["bronze"], item["silver"], item["load"]
    sc = silver["counts"]
    retry = item.get("resolution", {}).get("result", {})
    rc = retry.get("routing_counts", dict.fromkeys(sc, 0))
    loads = [load] + ([item["retry_load"]] if "retry_load" in item else [])
    counts = {
        "source_rows_extracted": bronze["source_row_count"],
        "bronze_rows_written": bronze["row_count"],
        "bronze_rows_read": sc["input_rows"],
        "deferred_retry_input_rows": retry.get("deferred_retry_input_rows", 0),
        "processing_input_rows": sc["input_rows"] + rc["input_rows"],
        "silver_valid_rows": sc["valid_rows"] + rc["valid_rows"],
        "rejected_rows": sc["rejected_rows"] + rc["rejected_rows"],
        "deferred_rows": sc["deferred_rows"] + rc["deferred_rows"],
        "duplicate_dropped_rows": sc["duplicates_rows"] + rc["duplicates_rows"],
        "warning_rows": sc["warning_rows"] + rc["warning_rows"],
        "source_rule_violation_count": sum(silver["rule_violations"].values()),
    }
    if bronze.get("snapshot_kind") == "INCREMENTAL":
        for key in (
            "source_bootstrap_rows",
            "source_after_cursor_rows",
            "source_lookback_rows",
        ):
            counts[key] = bronze[key]
        if (
            any(
                type(counts[k]) is not int or counts[k] < 0
                for k in (
                    "source_bootstrap_rows",
                    "source_after_cursor_rows",
                    "source_lookback_rows",
                )
            )
            or sum(
                counts[k]
                for k in (
                    "source_bootstrap_rows",
                    "source_after_cursor_rows",
                    "source_lookback_rows",
                )
            )
            != counts["source_rows_extracted"]
        ):
            raise BronzeError("Incremental source accounting mismatch")
        counts["full_parent_source_rows"] = item["full_parent_source_rows"]
    for name in (
        "load_candidate_rows",
        "warehouse_inserted_rows",
        "warehouse_updated_rows",
        "warehouse_unchanged_rows",
        "warehouse_stale_ignored_rows",
    ):
        counts[name] = sum(receipt[name] for receipt in loads)
    if (
        counts["source_rows_extracted"] != counts["bronze_rows_written"]
        or counts["bronze_rows_written"] != counts["bronze_rows_read"]
        or counts["deferred_retry_input_rows"] != rc["input_rows"]
        or counts["processing_input_rows"]
        != sum(
            counts[k]
            for k in (
                "silver_valid_rows",
                "rejected_rows",
                "deferred_rows",
                "duplicate_dropped_rows",
            )
        )
        or counts["load_candidate_rows"] != counts["silver_valid_rows"]
        or counts["load_candidate_rows"]
        != sum(counts[k] for k in counts if k.startswith("warehouse_"))
    ):
        raise BronzeError("Pipeline exact reconciliation failed")
    return counts


def dbt_evidence(directory: Path, earliest, database: str) -> dict:
    from olist_platform.warehouse.loading import utc_time

    rb = (directory / "run_results.json").read_bytes()
    mb = (directory / "manifest.json").read_bytes()
    results = json.loads(rb)
    manifest = json.loads(mb)
    expected = {
        k
        for k, v in manifest["nodes"].items()
        if v["resource_type"] in ("model", "test") and v["config"].get("enabled", True)
    }
    actual = results["results"]
    ids = [r["unique_id"] for r in actual]
    sources = manifest["sources"].values()
    if (
        {node["name"] for node in sources} != set(TABLES)
        or any(
            node["database"] != database or node["schema"] != "raw_staging"
            for node in sources
        )
        or any(
            node["database"] != database or node["schema"] != "analytics"
            for node in manifest["nodes"].values()
            if node["resource_type"] == "model"
        )
    ):
        raise BronzeError("dbt build used another database/schema")
    if (
        not expected
        or len(ids) != len(set(ids))
        or set(ids) != expected
        or any(r["status"] not in ("success", "pass") for r in actual)
        or results["args"].get("which") != "build"
        or results["metadata"]["invocation_id"] != manifest["metadata"]["invocation_id"]
        or manifest["metadata"]["project_name"] != "olist_analytics"
        or utc_time(results["metadata"]["invocation_started_at"]) < earliest
    ):
        raise BronzeError("dbt build is incomplete, stale or failed")
    return {
        "run_results_sha256": sha256(rb),
        "manifest_sha256": sha256(mb),
        "invocation_id": results["metadata"]["invocation_id"],
        "node_count": len(ids),
    }


def promote(connection, run_id: str, attempt: int, items: dict, dbt: dict) -> dict:
    with connection.transaction():
        lock(connection)
        row = current(connection, run_id, attempt)
        tasks = dict(
            connection.execute(
                "SELECT task_name,status FROM audit.pipeline_task_runs WHERE run_id=%s AND run_attempt=%s",
                (run_id, attempt),
            ).fetchall()
        )
        if (
            set(tasks) != set(required_tasks(row))
            or any(value != "SUCCESS" for value in tasks.values())
            or set(items) != set(TABLES)
        ):
            raise BronzeError("Pipeline tasks or datasets are incomplete")
        counters = {table: reconcile(item) for table, item in items.items()}
        for table, item in items.items():
            previous = row["control_before"][table]
            before = previous["committed_cursor"]
            upper = item["bronze"]["run_upper_cursor"]
            candidate = before
            if upper is not None and (
                before is None
                or cursor_tuple(upper, table) > cursor_tuple(before, table)
            ):
                candidate = upper
            changed = connection.execute(
                "UPDATE audit.pipeline_control SET committed_cursor=%s,cursor_version=cursor_version+1,updated_by_run_id=%s,updated_at=clock_timestamp() WHERE pipeline_name=%s AND source_table=%s AND cursor_version=%s",
                (
                    Jsonb(candidate),
                    run_id,
                    row["pipeline_name"],
                    table,
                    previous["cursor_version"],
                ),
            ).rowcount
            if changed != 1:
                raise BronzeError("Pipeline watermark compare-and-swap conflict")
            proof = {
                "bronze_prefix": item["bronze"]["prefix"],
                "bronze_sha256": item["bronze_sha256"],
                "silver_prefix": item["silver"]["prefix"],
                "silver_sha256": item["silver_sha256"],
                "dbt": dbt,
                "parents": item.get("parents", {}),
            }
            connection.execute(
                "INSERT INTO audit.pipeline_dataset_runs (run_id,source_table,evidence,counters,watermark_before,watermark_candidate,watermark_after,reconciliation_status) VALUES (%s,%s,%s,%s,%s,%s,%s,'PASS')",
                (
                    run_id,
                    table,
                    Jsonb(proof),
                    Jsonb(counters[table]),
                    Jsonb(before),
                    Jsonb(candidate),
                    Jsonb(candidate),
                ),
            )
        result = {
            "status": "SUCCESS",
            "run_id": run_id,
            "attempt": attempt,
            "datasets": counters,
            "dbt": dbt,
        }
        connection.execute(
            "UPDATE audit.pipeline_runs SET status='SUCCESS',completed_at=clock_timestamp(),result=%s WHERE run_id=%s",
            (Jsonb(result), run_id),
        )
        return result
