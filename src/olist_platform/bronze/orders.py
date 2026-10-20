"""Prepare a stable snapshot, publish it, then CAS the Bronze-only checkpoint."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .storage import BronzeError, ObjectStore, encode, sha256, verify_published

PIPELINE = "bronze_orders_v1"
LOCK = "olist.bronze.orders.v1"
CURSOR = "(source_updated_at, source_sequence, order_id)"


def cursor_value(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "updated_at": row["source_updated_at"].isoformat(),
        "source_sequence": row["source_sequence"],
        "primary_key": {"order_id": row["order_id"]},
    }


def cursor_tuple(value: dict) -> tuple:
    return (
        datetime.fromisoformat(value["updated_at"]),
        value["source_sequence"],
        value["primary_key"]["order_id"],
    )


def configuration(
    contract: dict, store: ObjectStore, endpoint: str, page_size: int
) -> dict:
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
    return {
        "format_version": 1,
        "extractor_version": "orders-bronze-v1",
        "pipeline": PIPELINE,
        "contract_sha256": sha256(encode(contract)),
        "endpoint": endpoint,
        "bucket": store.bucket,
        "page_size": page_size,
        "lookback_hours": lookback,
    }


def prepare(
    connection: psycopg.Connection, run_id: str, contract: dict, config: dict
) -> dict:
    """One transaction seals bounds and all bytes before any object is uploaded."""
    config_hash = sha256(encode(config))
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        previous = connection.execute(
            "SELECT config_sha256, manifest FROM audit.bronze_orders_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        if previous:
            if previous[0] != config_hash:
                raise BronzeError("Run ID already exists with different configuration")
            return previous[1]
        unfinished = connection.execute(
            "SELECT run_id FROM audit.bronze_orders_runs WHERE pipeline = %s AND status = 'PREPARED'",
            (PIPELINE,),
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
            "INSERT INTO audit.bronze_orders_control (pipeline) VALUES (%s) ON CONFLICT DO NOTHING",
            (PIPELINE,),
        )
        before, version = connection.execute(
            "SELECT cursor_value, cursor_version FROM audit.bronze_orders_control WHERE pipeline = %s",
            (PIPELINE,),
        ).fetchone()
        target = sql.Identifier(contract["database"]["source_schema"], "orders")
        table = next(t for t in contract["tables"] if t["table"] == "orders")
        expected = table["columns"] + contract["metadata_columns"]
        actual = connection.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull "
            "FROM pg_attribute a WHERE a.attrelid = %s::regclass AND a.attnum > 0 "
            "AND NOT a.attisdropped ORDER BY a.attnum",
            (f"{contract['database']['source_schema']}.orders",),
        ).fetchall()
        specs = {c["name"]: (c["postgres_type"], c["nullable"]) for c in expected}
        if {name: (kind, nullable) for name, kind, nullable in actual} != specs:
            raise BronzeError("Orders source schema differs from the contract")
        with connection.cursor(row_factory=dict_row) as reader:
            reader.execute(
                sql.SQL("SELECT * FROM {} ORDER BY " + CURSOR + " DESC LIMIT 1").format(
                    target
                )
            )
            upper = cursor_value(reader.fetchone())
        candidate = before
        if upper and (before is None or cursor_tuple(upper) > cursor_tuple(before)):
            candidate = upper
        lower = (
            cursor_tuple(before)[0] - timedelta(hours=config["lookback_hours"])
            if before
            else None
        )
        started = datetime.now(timezone.utc)
        prefix = f"bronze/orders/ingestion_date={started.date()}/run_id={run_id}"
        source_count = 0
        if upper:
            count_filter = sql.SQL(CURSOR + " <= (%s, %s, %s)")
            count_parameters = list(cursor_tuple(upper))
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
            "source": "postgresql.orders",
            "run_id": run_id,
            "prefix": prefix,
            "prepared_at": started.isoformat(),
            "config": config,
            "config_sha256": config_hash,
            "schema_fingerprint": sha256(encode(expected)),
            "source_schema": expected,
            "watermark_before": before,
            "extract_lower_bound": lower.isoformat() if lower else None,
            "run_upper_cursor": upper,
            "watermark_candidate": candidate,
            "cursor_version_before": version,
            "row_count": 0,
            "source_row_count": source_count,
            "page_count": 0,
            "objects": [],
        }
        connection.execute(
            "INSERT INTO audit.bronze_orders_runs "
            "(run_id, pipeline, status, config_sha256, cursor_version_before, manifest) "
            "VALUES (%s, %s, 'PREPARED', %s, %s, %s)",
            (run_id, PIPELINE, config_hash, version, Jsonb(manifest)),
        )
        page_cursor = None
        while upper is not None:
            predicates = [sql.SQL(CURSOR + " <= (%s, %s, %s)")]
            parameters = list(cursor_tuple(upper))
            if lower is not None:
                predicates.append(sql.SQL("source_updated_at >= %s"))
                parameters.append(lower)
            if page_cursor is not None:
                predicates.append(sql.SQL(CURSOR + " > (%s, %s, %s)"))
                parameters.extend(cursor_tuple(page_cursor))
            parameters.append(config["page_size"])
            with connection.cursor(row_factory=dict_row) as reader:
                reader.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE {} ORDER BY " + CURSOR + " LIMIT %s"
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
                logical = ["postgresql.orders", row["order_id"], row["source_sequence"]]
                serialized = {
                    name: value.isoformat() if isinstance(value, datetime) else value
                    for name, value in row.items()
                }
                serialized.update(
                    _record_uid=sha256(encode([run_id, key, ordinal])),
                    _logical_record_uid=sha256(encode(logical)),
                    _ingested_at=started.isoformat(),
                    _run_id=run_id,
                    _run_attempt=1,
                    _source_name="postgresql.orders",
                    _source_file=key,
                    _source_row_number=ordinal,
                    _schema_version=contract["contract_version"],
                )
                lines.append(encode(serialized))
            body = b"".join(lines)
            connection.execute(
                "INSERT INTO audit.bronze_orders_parts (run_id, page_number, body) VALUES (%s, %s, %s)",
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
            page_cursor = cursor_value(rows[-1])
        if manifest["row_count"] != source_count:
            raise BronzeError("Source-to-Bronze row count reconciliation failed")
        connection.execute(
            "UPDATE audit.bronze_orders_runs SET manifest = %s WHERE run_id = %s",
            (Jsonb(manifest), run_id),
        )
    return manifest


def ingest_orders(
    connection: psycopg.Connection,
    store: ObjectStore,
    contract: dict,
    *,
    run_id: str,
    endpoint: str,
    page_size: int = 1000,
) -> dict:
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise BronzeError(
            "Invalid run_id: use 1-96 letters, digits, underscores or hyphens"
        )
    config = configuration(contract, store, endpoint, page_size)
    if (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise BronzeError("Ingestion requires an idle autocommit connection")
    acquired = connection.execute(
        "SELECT pg_try_advisory_lock(hashtext(%s))", (LOCK,)
    ).fetchone()[0]
    if not acquired:
        raise BronzeError("Another orders ingestion is active")
    try:
        manifest = prepare(connection, run_id, contract, config)
        prefix = manifest["prefix"]
        status = connection.execute(
            "SELECT status FROM audit.bronze_orders_runs WHERE run_id = %s", (run_id,)
        ).fetchone()[0]
        if status == "COMMITTED":
            if verify_published(store, prefix) != manifest:
                raise BronzeError("Published manifest differs from audit state")
            return {"status": "NOOP", "manifest": manifest}
        for number, part in enumerate(manifest["objects"], 1):
            body = bytes(
                connection.execute(
                    "SELECT body FROM audit.bronze_orders_parts WHERE run_id = %s AND page_number = %s",
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
                "UPDATE audit.bronze_orders_control SET cursor_value = %s, "
                "cursor_version = cursor_version + 1, updated_by_run_id = %s, "
                "updated_at = clock_timestamp() WHERE pipeline = %s AND cursor_version = %s",
                (
                    Jsonb(manifest["watermark_candidate"]),
                    run_id,
                    PIPELINE,
                    manifest["cursor_version_before"],
                ),
            ).rowcount
            if changed != 1:
                raise BronzeError("Bronze checkpoint compare-and-swap conflict")
            connection.execute(
                "UPDATE audit.bronze_orders_runs SET status = 'COMMITTED', "
                "committed_at = clock_timestamp() WHERE run_id = %s",
                (run_id,),
            )
        return {"status": "PUBLISHED", "manifest": manifest}
    finally:
        connection.execute("SELECT pg_advisory_unlock(hashtext(%s))", (LOCK,))
