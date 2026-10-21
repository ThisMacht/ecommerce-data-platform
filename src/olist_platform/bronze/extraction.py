"""Shared bounded extraction and full parent snapshots for core source tables."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .storage import BronzeError, ObjectStore, encode, sha256, verify_published

KEYS = {
    "orders": ("order_id",),
    "customers": ("customer_id",),
    "order_payments": ("order_id", "payment_sequential"),
    "order_items": ("order_id", "order_item_id"),
}
PIPELINE = "bronze_orders_v1"
LOCK = "olist.bronze.orders.v1"
CURSOR = "(source_updated_at, source_sequence, order_id)"


def state_sql(template: str, table: str) -> sql.Composed:
    if table not in KEYS:
        raise BronzeError("Unsupported extraction table")
    return sql.SQL(template).format(
        **{
            name: sql.Identifier("audit", f"bronze_{table}_{name}")
            for name in ("control", "runs", "parts")
        }
    )


def cursor_value(row: dict | None, table: str = "orders") -> dict | None:
    if row is None:
        return None
    return {
        "updated_at": row["source_updated_at"].isoformat(),
        "source_sequence": row["source_sequence"],
        "primary_key": {key: row[key] for key in KEYS[table]},
    }


def cursor_tuple(value: dict, table: str = "orders") -> tuple:
    return (
        datetime.fromisoformat(value["updated_at"]),
        value["source_sequence"],
        *(value["primary_key"][key] for key in KEYS[table]),
    )


def configuration(
    contract: dict,
    store: ObjectStore,
    endpoint: str,
    page_size: int,
    table: str = "orders",
    full_snapshot: bool = False,
    pipeline_run_id: str | None = None,
    pipeline_parent: bool = False,
    pipeline_mode: str = "incremental",
) -> dict:
    if table not in KEYS:
        raise BronzeError("Unsupported extraction table")
    if contract["contract_version"] != "1.0.0":
        raise BronzeError("Unsupported source contract version")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise BronzeError("page_size must be an integer between 1 and 10000")
    location = urlsplit(endpoint)
    if (
        location.scheme not in ("http", "https")
        or not location.hostname
        or location.username is not None
        or location.password is not None
        or location.query
        or location.fragment
    ):
        raise BronzeError(
            "S3 endpoint must be an HTTP(S) URL without credentials or query parameters"
        )
    lookback = contract["policies"]["lateness_sla_hours"]
    if type(lookback) is not int or not 0 <= lookback <= 24 * 365:
        raise BronzeError("Invalid contract lookback hours")
    if full_snapshot and table == "customers":
        raise BronzeError("Customers already use full snapshots")
    config = {
        "format_version": 1,
        "extractor_version": f"{table}-bronze-v1",
        "pipeline": f"bronze_{table}_v1",
        "contract_sha256": sha256(encode(contract)),
        "endpoint": endpoint,
        "bucket": store.bucket,
        "page_size": page_size,
        "lookback_hours": lookback,
    }
    if full_snapshot:
        config["snapshot_kind"] = "FULL"
    if pipeline_mode not in ("incremental", "backfill") or (
        pipeline_mode == "backfill" and pipeline_run_id is None
    ):
        raise BronzeError("Invalid pipeline stream mode")
    if pipeline_parent and pipeline_run_id is None:
        raise BronzeError("Parent stream requires a pipeline run")
    if pipeline_run_id is not None:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", pipeline_run_id):
            raise BronzeError("Invalid owning pipeline run ID")
        if full_snapshot or (pipeline_parent and table not in ("customers", "orders")):
            raise BronzeError("Invalid incremental stream configuration")
        config.update(
            pipeline=f"{pipeline_mode}_{table}_{'parent' if pipeline_parent else 'delta'}_v1",
            pipeline_run_id=pipeline_run_id,
            snapshot_kind="FULL" if pipeline_parent else "INCREMENTAL",
        )
    return config


def prepare(
    connection: psycopg.Connection,
    run_id: str,
    contract: dict,
    config: dict,
    table: str = "orders",
    *,
    snapshot_transaction: bool = False,
) -> dict:
    """One transaction seals bounds and all bytes before any object is uploaded."""
    pipeline = config["pipeline"]
    cursor = "(source_updated_at, source_sequence, " + ", ".join(KEYS[table]) + ")"
    placeholders = "(" + ", ".join(["%s"] * (2 + len(KEYS[table]))) + ")"
    config_hash = sha256(encode(config))
    with connection.transaction():
        if not snapshot_transaction:
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        previous = connection.execute(
            state_sql(
                "SELECT config_sha256, manifest FROM {runs} WHERE run_id = %s", table
            ),
            (run_id,),
        ).fetchone()
        if previous:
            if previous[0] != config_hash:
                raise BronzeError("Run ID already exists with different configuration")
            return previous[1]
        if config.get("pipeline_run_id") and not snapshot_transaction:
            raise BronzeError("Freeze all incremental streams before publishing")
        unfinished = connection.execute(
            state_sql(
                "SELECT run_id FROM {runs} WHERE pipeline = %s AND status = 'PREPARED'",
                table,
            ),
            (pipeline,),
        ).fetchone()
        if unfinished:
            raise BronzeError(f"Resume unfinished run first: {unfinished[0]}")
        seeded = connection.execute(
            "SELECT 1 FROM audit.source_seed_runs WHERE snapshot_id = %s AND status = 'SUCCESS'",
            (contract["dataset_snapshot_id"],),
        ).fetchone()
        if not seeded:
            raise BronzeError("Source snapshot must be seeded before extraction")
        connection.execute(
            state_sql(
                "INSERT INTO {control} (pipeline) VALUES (%s) ON CONFLICT DO NOTHING",
                table,
            ),
            (pipeline,),
        )
        before, version = connection.execute(
            state_sql(
                "SELECT cursor_value, cursor_version FROM {control} WHERE pipeline = %s",
                table,
            ),
            (pipeline,),
        ).fetchone()
        delivery_before = before
        pipeline_version = None
        if config.get("pipeline_run_id"):
            from olist_platform.pipeline.control import (
                BACKFILL,
                INCREMENTAL,
                current,
                parent_run_id,
            )

            owner = current(connection, config["pipeline_run_id"])
            expected = (
                parent_run_id(owner["run_id"])
                if config["snapshot_kind"] == "FULL"
                else owner["run_id"]
            )
            if (
                owner["pipeline_name"] not in (INCREMENTAL, BACKFILL)
                or run_id != expected
                or config["pipeline"]
                != f"{owner['request']['mode']}_{table}_{'parent' if config['snapshot_kind'] == 'FULL' else 'delta'}_v1"
            ):
                raise BronzeError("Incremental extraction requires its owning pipeline")
            baseline = owner["control_before"][table]
            before = baseline["committed_cursor"]
            pipeline_version = baseline["cursor_version"]
        target = sql.Identifier(contract["database"]["source_schema"], table)
        specification = next(t for t in contract["tables"] if t["table"] == table)
        expected = specification["columns"] + contract["metadata_columns"]
        actual = connection.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull "
            "FROM pg_attribute a WHERE a.attrelid = %s::regclass AND a.attnum > 0 "
            "AND NOT a.attisdropped ORDER BY a.attnum",
            (f"{contract['database']['source_schema']}.{table}",),
        ).fetchall()
        specs = {c["name"]: (c["postgres_type"], c["nullable"]) for c in expected}
        if {name: (kind, nullable) for name, kind, nullable in actual} != specs:
            raise BronzeError(f"{table} source schema differs from the contract")
        window = (
            owner["request"]["backfill_window"]
            if config.get("pipeline_run_id")
            and owner["pipeline_name"] == BACKFILL
            and config["snapshot_kind"] != "FULL"
            else None
        )
        with connection.cursor(row_factory=dict_row) as reader:
            reader.execute(
                sql.SQL(
                    "SELECT * FROM {} "
                    + (
                        "WHERE source_updated_at >= %s AND source_updated_at < %s "
                        if window
                        else ""
                    )
                    + "ORDER BY "
                    + cursor
                    + " DESC LIMIT 1"
                ).format(target),
                (window["start"], window["end"]) if window else (),
            )
            upper = cursor_value(reader.fetchone(), table)
        candidate = before
        if upper and (
            before is None or cursor_tuple(upper, table) > cursor_tuple(before, table)
        ):
            candidate = upper
        lower = (
            cursor_tuple(before, table)[0] - timedelta(hours=config["lookback_hours"])
            if before
            and config.get("snapshot_kind") != "FULL"
            and (table != "customers" or config.get("pipeline_run_id"))
            else None
        )
        if window:
            lower = datetime.fromisoformat(window["start"])
        started = datetime.now(timezone.utc)
        prefix = f"bronze/{table}/ingestion_date={started.date()}/run_id={run_id}"
        source_count = 0
        if upper:
            count_filter = sql.SQL(cursor + " <= " + placeholders)
            count_parameters = list(cursor_tuple(upper, table))
            if lower is not None:
                count_filter += sql.SQL(" AND source_updated_at >= %s")
                count_parameters.append(lower)
            source_count = connection.execute(
                sql.SQL("SELECT COUNT(*) FROM {} WHERE {}").format(
                    target, count_filter
                ),
                count_parameters,
            ).fetchone()[0]
        manifest = {
            "format_version": 1,
            "schema_version": contract["contract_version"],
            "source": f"postgresql.{table}",
            "run_id": run_id,
            "prefix": prefix,
            "prepared_at": started.isoformat(),
            "config": config,
            "config_sha256": config_hash,
            "schema_fingerprint": sha256(encode(expected)),
            "source_schema": expected,
            "watermark_before": before,
            "extract_lower_bound": lower.isoformat() if lower else None,
            **({"extract_end_bound": window["end"]} if window else {}),
            "run_upper_cursor": upper,
            "watermark_candidate": candidate,
            "cursor_version_before": version,
            "row_count": 0,
            "source_row_count": source_count,
            "page_count": 0,
            "objects": [],
        }
        if config.get("pipeline_run_id"):
            delivery_candidate = candidate
            if delivery_before is not None and (
                delivery_candidate is None
                or cursor_tuple(delivery_before, table)
                > cursor_tuple(delivery_candidate, table)
            ):
                delivery_candidate = delivery_before
            manifest.update(
                snapshot_kind=config["snapshot_kind"],
                delivery_watermark_before=delivery_before,
                delivery_watermark_candidate=delivery_candidate,
                pipeline_cursor_version_before=pipeline_version,
                source_backfill_rows=0,
                source_bootstrap_rows=0,
                source_after_cursor_rows=0,
                source_lookback_rows=0,
                source_snapshot=connection.execute(
                    "SELECT pg_current_snapshot()::text"
                ).fetchone()[0],
            )
        elif table == "customers" or config.get("snapshot_kind") == "FULL":
            manifest["snapshot_kind"] = "FULL"
        connection.execute(
            state_sql(
                "INSERT INTO {runs} (run_id, pipeline, status, config_sha256, cursor_version_before, manifest) VALUES (%s, %s, 'PREPARED', %s, %s, %s)",
                table,
            ),
            (run_id, pipeline, config_hash, version, Jsonb(manifest)),
        )
        page_cursor = None
        while upper is not None:
            predicates = [sql.SQL(cursor + " <= " + placeholders)]
            parameters = list(cursor_tuple(upper, table))
            if lower is not None:
                predicates.append(sql.SQL("source_updated_at >= %s"))
                parameters.append(lower)
            if page_cursor is not None:
                predicates.append(sql.SQL(cursor + " > " + placeholders))
                parameters.extend(cursor_tuple(page_cursor, table))
            parameters.append(config["page_size"])
            with connection.cursor(row_factory=dict_row) as reader:
                reader.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE {} ORDER BY " + cursor + " LIMIT %s"
                    ).format(target, sql.SQL(" AND ").join(predicates)),
                    parameters,
                )
                rows = reader.fetchall()
            if not rows:
                break
            number = manifest["page_count"] + 1
            key = f"{prefix}/attempt=0001/part-{number:06d}.jsonl"
            lines = []
            for ordinal, row in enumerate(rows, 1):
                if config.get("pipeline_run_id"):
                    category = (
                        "source_backfill_rows"
                        if window
                        else "source_bootstrap_rows"
                        if before is None
                        else "source_after_cursor_rows"
                        if cursor_tuple(cursor_value(row, table), table)
                        > cursor_tuple(before, table)
                        else "source_lookback_rows"
                    )
                    manifest[category] += 1
                logical = [
                    f"postgresql.{table}",
                    row[KEYS[table][0]]
                    if len(KEYS[table]) == 1
                    else [row[k] for k in KEYS[table]],
                    row["source_sequence"],
                ]
                serialized = {
                    name: value.isoformat()
                    if isinstance(value, datetime)
                    else str(value)
                    if isinstance(value, Decimal)
                    else value
                    for name, value in row.items()
                }
                serialized.update(
                    _record_uid=sha256(encode([run_id, key, ordinal])),
                    _logical_record_uid=sha256(encode(logical)),
                    _ingested_at=started.isoformat(),
                    _run_id=run_id,
                    _run_attempt=1,
                    _source_name=f"postgresql.{table}",
                    _source_file=key,
                    _source_row_number=ordinal,
                    _schema_version=contract["contract_version"],
                )
                lines.append(encode(serialized))
            body = b"".join(lines)
            connection.execute(
                state_sql(
                    "INSERT INTO {parts} (run_id, page_number, body) VALUES (%s, %s, %s)",
                    table,
                ),
                (run_id, number, body),
            )
            manifest["objects"].append(
                {
                    "key": key,
                    "sha256": sha256(body),
                    "size_bytes": len(body),
                    "row_count": len(rows),
                }
            )
            manifest["row_count"] += len(rows)
            manifest["page_count"] = number
            page_cursor = cursor_value(rows[-1], table)
        if manifest["row_count"] != source_count:
            raise BronzeError("Source-to-Bronze row count reconciliation failed")
        connection.execute(
            state_sql("UPDATE {runs} SET manifest = %s WHERE run_id = %s", table),
            (Jsonb(manifest), run_id),
        )
    return manifest


def ingest_source(
    connection: psycopg.Connection,
    store: ObjectStore,
    contract: dict,
    *,
    run_id: str,
    endpoint: str,
    page_size: int = 1000,
    table: str = "orders",
    full_snapshot: bool = False,
    pipeline_run_id: str | None = None,
    pipeline_parent: bool = False,
    pipeline_mode: str = "incremental",
) -> dict:
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise BronzeError(
            "Invalid run_id: use 1-96 letters, digits, underscores or hyphens"
        )
    config = configuration(
        contract,
        store,
        endpoint,
        page_size,
        table,
        full_snapshot,
        pipeline_run_id,
        pipeline_parent,
        pipeline_mode,
    )
    if (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise BronzeError("Ingestion requires an idle autocommit connection")
    pipeline = config["pipeline"]
    lock = f"olist.bronze.{table}.v1"
    acquired = connection.execute(
        "SELECT pg_try_advisory_lock(hashtext(%s))", (lock,)
    ).fetchone()[0]
    if not acquired:
        raise BronzeError(f"Another {table} ingestion is active")
    try:
        manifest = prepare(connection, run_id, contract, config, table)
        prefix = manifest["prefix"]
        status = connection.execute(
            state_sql("SELECT status FROM {runs} WHERE run_id = %s", table), (run_id,)
        ).fetchone()[0]
        if status == "COMMITTED":
            if verify_published(store, prefix) != manifest:
                raise BronzeError("Published manifest differs from audit state")
            return {"status": "NOOP", "manifest": manifest}
        for number, part in enumerate(manifest["objects"], 1):
            body = bytes(
                connection.execute(
                    state_sql(
                        "SELECT body FROM {parts} WHERE run_id = %s AND page_number = %s",
                        table,
                    ),
                    (run_id, number),
                ).fetchone()[0]
            )
            if sha256(body) != part["sha256"]:
                raise BronzeError("Prepared page checksum mismatch")
            store.put_immutable(part["key"], body)
        manifest_body = encode(manifest)
        store.put_immutable(f"{prefix}/manifest.json", manifest_body)
        # Validate every remote byte before making this batch visible to readers.
        for part in manifest["objects"]:
            if sha256(store.read(part["key"])) != part["sha256"]:
                raise BronzeError("Uploaded page checksum mismatch")
        store.put_immutable(
            f"{prefix}/_SUCCESS", encode({"manifest_sha256": sha256(manifest_body)})
        )
        if verify_published(store, prefix) != manifest:
            raise BronzeError("Published manifest differs from audit state")
        with connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            changed = connection.execute(
                state_sql(
                    "UPDATE {control} SET cursor_value = %s, cursor_version = cursor_version + 1, updated_by_run_id = %s, updated_at = clock_timestamp() WHERE pipeline = %s AND cursor_version = %s",
                    table,
                ),
                (
                    Jsonb(
                        manifest.get(
                            "delivery_watermark_candidate",
                            manifest["watermark_candidate"],
                        )
                    ),
                    run_id,
                    pipeline,
                    manifest["cursor_version_before"],
                ),
            ).rowcount
            if changed != 1:
                raise BronzeError("Bronze checkpoint compare-and-swap conflict")
            connection.execute(
                state_sql(
                    "UPDATE {runs} SET status = 'COMMITTED', committed_at = clock_timestamp() WHERE run_id = %s",
                    table,
                ),
                (run_id,),
            )
        return {"status": "PUBLISHED", "manifest": manifest}
    finally:
        connection.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock,))
