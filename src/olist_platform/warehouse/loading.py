"""Verified Silver inputs, canonical business hashes and atomic staging merges."""

from __future__ import annotations

import json
import re
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import LINEAGE, source_columns, verify_silver
from olist_platform.silver.geolocation import coordinate_value

LOADER_VERSION = "warehouse-v1"
HASH_VERSION = "business-v1"
OUTCOMES = ("inserted", "updated", "unchanged", "stale_ignored")


def staging_columns(contract: dict, table: str) -> list[str]:
    return (
        [c["name"] for c in source_columns(contract, table)]
        + (["order_purchase_date"] if table == "orders" else [])
        + list(LINEAGE)
        + [
            "warning_rule_ids",
            "record_hash",
            "hash_schema_version",
            "first_seen_run_id",
            "last_seen_run_id",
        ]
    )


def utc_time(value: object) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise BronzeError("Warehouse timestamps require explicit timezone offsets")
    return value.astimezone(timezone.utc)


def canonical_record(row: dict, contract: dict, table: str, run_id: str) -> dict:
    """Hash ordered business values; metadata and physical lineage never affect it."""
    record = {}
    business = next(t["columns"] for t in contract["tables"] if t["table"] == table)
    canonical = []
    for column in source_columns(contract, table):
        name, kind = column["name"], column["postgres_type"]
        value = row[name]
        if value is None:
            nullable = column["nullable"] or name in (
                "customer_city",
                "customer_state",
                "customer_zip_code_prefix",
                "seller_city",
                "seller_state",
                "seller_zip_code_prefix",
                "geolocation_city",
                "geolocation_state",
            )
            if not nullable:
                raise BronzeError(f"Null required staging value: {name}")
            normalized = None
        elif kind.startswith("timestamp"):
            value = utc_time(value)
            normalized = value.isoformat(timespec="microseconds").replace("+00:00", "Z")
        elif kind == "numeric(18,2)":
            if (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or abs(value) >= Decimal("1e16")
                or value != value.quantize(Decimal("0.01"))
                or value < 0
            ):
                raise BronzeError(f"Invalid staging decimal: {name}")
            value = value.quantize(Decimal("0.01"))
            if value == 0:
                value = Decimal("0.00")
            normalized = format(value, ".2f")
        elif kind == "numeric":
            value = coordinate_value(value, 90 if name == "geolocation_lat" else 180)
            if value is None:
                raise BronzeError(f"Invalid staging coordinate: {name}")
            normalized = format(value, "f")
            if "." in normalized:
                normalized = normalized.rstrip("0").rstrip(".")
            if value == 0:
                normalized = "0"
        elif kind in ("integer", "bigint"):
            bits = 32 if kind == "integer" else 64
            if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (
                bits - 1
            ):
                raise BronzeError(f"Invalid staging integer: {name}")
            if table == "products" and column in business and value < 0:
                raise BronzeError(f"Invalid staging product measurement: {name}")
            normalized = value
        else:
            if not isinstance(value, str) or "\x00" in value:
                raise BronzeError(f"Invalid staging text: {name}")
            normalized = value
        record[name] = value
        if column in business:
            canonical.append(normalized)
    if any(
        record[key] is None
        or (
            isinstance(record[key], str)
            and (not record[key] or record[key] != record[key].strip())
        )
        for key in KEYS[table]
    ):
        raise BronzeError("Invalid normalized staging business key")
    if table == "geolocation" and (
        record["geolocation_row_id"] <= 0
        or re.fullmatch(r"[0-9]{5}", record["geolocation_zip_code_prefix"]) is None
    ):
        raise BronzeError("Invalid staging geolocation identity/ZIP")
    if len(KEYS[table]) > 1 and record[KEYS[table][1]] <= 0:
        raise BronzeError("Invalid staging business key ordinal")
    if record["source_sequence"] <= 0 or record["source_operation"] not in (
        "INSERT",
        "UPDATE",
    ):
        raise BronzeError("Invalid staging source version/operation")
    for name in LINEAGE:
        value = row[name]
        if name == "_ingested_at":
            value = utc_time(value)
        elif name in ("_run_attempt", "_source_row_number"):
            if type(value) is not int or value <= 0:
                raise BronzeError("Invalid staging physical lineage ordinal")
        elif not isinstance(value, str) or not value or "\x00" in value:
            raise BronzeError("Invalid staging physical lineage text")
        record[name] = value
    if (
        record["_source_name"] != f"postgresql.{table}"
        or record["_schema_version"] != "1.0.0"
    ):
        raise BronzeError("Invalid staging source lineage")
    for name in ("_record_uid", "_logical_record_uid"):
        if re.fullmatch(r"[0-9a-f]{64}", record[name]) is None:
            raise BronzeError("Invalid staging lineage hash")
    if row.get("error_rule_ids") != [] or row.get("deferred_rule_ids", []) != []:
        raise BronzeError("Only valid Silver records may enter staging")
    warnings = row["warning_rule_ids"]
    if not isinstance(warnings, list) or any(
        not isinstance(w, str) or not w or "\x00" in w for w in warnings
    ):
        raise BronzeError("Invalid Silver warning flags")
    if table == "orders":
        value = row["order_purchase_date"]
        if not isinstance(value, str):
            raise BronzeError("Order purchase date requires ISO date text")
        record["order_purchase_date"] = date.fromisoformat(value)
        if (
            record["order_purchase_date"]
            != record["order_purchase_timestamp"]
            .astimezone(ZoneInfo("America/Sao_Paulo"))
            .date()
        ):
            raise BronzeError("Order purchase date differs from business timezone")
    record.update(
        warning_rule_ids=warnings,
        record_hash=sha256(encode([HASH_VERSION, table, canonical])),
        hash_schema_version=HASH_VERSION,
        first_seen_run_id=run_id,
        last_seen_run_id=run_id,
    )
    return record


def merge_rows(
    connection: psycopg.Connection,
    contract: dict,
    *,
    table: str,
    run_id: str,
    rows: Iterable[dict],
    request: dict,
    silver_counts: dict,
) -> dict:
    if (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise BronzeError("Warehouse load requires an idle autocommit connection")
    return _merge_rows(
        connection,
        contract,
        table=table,
        run_id=run_id,
        rows=rows,
        request=request,
        silver_counts=silver_counts,
    )


def _merge_rows(
    connection: psycopg.Connection,
    contract: dict,
    *,
    table: str,
    run_id: str,
    rows: Iterable[dict],
    request: dict,
    silver_counts: dict,
) -> dict:
    """Internal merge also accepts an outer transaction for atomic deferred resolution."""
    if table not in KEYS or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise BronzeError("Invalid warehouse dataset/run ID")
    idle = connection.info.transaction_status == TransactionStatus.IDLE
    required_counts = {
        "input_rows",
        "valid_rows",
        "rejected_rows",
        "deferred_rows",
        "duplicates_rows",
        "warning_rows",
    }
    if (
        set(silver_counts) != required_counts
        or any(type(n) is not int or n < 0 for n in silver_counts.values())
        or silver_counts["input_rows"]
        != sum(
            silver_counts[f"{r}_rows"]
            for r in ("valid", "rejected", "deferred", "duplicates")
        )
        or silver_counts["warning_rows"] > silver_counts["valid_rows"]
    ):
        raise BronzeError("Invalid warehouse Silver reconciliation counters")
    request_hash = sha256(encode(request))
    target = sql.Identifier("raw_staging", table)
    columns = staging_columns(contract, table)
    names = sql.SQL(", ").join(map(sql.Identifier, columns))
    keys = sql.SQL(", ").join(map(sql.Identifier, KEYS[table]))
    with connection.transaction():
        if idle:
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SET LOCAL lock_timeout = '15s'")
        # Full geolocation refreshes update lineage for over one million rows.
        connection.execute("SET LOCAL statement_timeout = '600s'")
        # Lock the target, including against writers outside this loader.
        connection.execute(
            sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(target)
        )
        previous = connection.execute(
            "SELECT request_sha256, load_candidate_rows, warehouse_inserted_rows, warehouse_updated_rows, warehouse_unchanged_rows, warehouse_stale_ignored_rows FROM audit.warehouse_load_runs WHERE source_table = %s AND run_id = %s",
            (table, run_id),
        ).fetchone()
        if previous:
            if previous[0] != request_hash:
                raise BronzeError(
                    "Warehouse run ID already committed with another request"
                )
            return {
                "status": "NOOP",
                "table": table,
                "run_id": run_id,
                "counts": dict(
                    zip(
                        ("load_candidate_rows",)
                        + tuple(f"warehouse_{o}_rows" for o in OUTCOMES),
                        previous[1:],
                    )
                ),
            }
        connection.execute(
            sql.SQL(
                "CREATE TEMP TABLE warehouse_input (LIKE {} INCLUDING ALL) ON COMMIT DROP"
            ).format(target)
        )
        connection.execute("CREATE UNIQUE INDEX ON warehouse_input (_record_uid)")
        total = warnings = 0
        with connection.cursor().copy(
            sql.SQL("COPY warehouse_input ({}) FROM STDIN").format(names)
        ) as copy:
            for row in rows:
                record = canonical_record(row, contract, table, run_id)
                copy.write_row([record[name] for name in columns])
                total += 1
                warnings += bool(record["warning_rule_ids"])
        if (
            total != silver_counts["valid_rows"]
            or warnings != silver_counts["warning_rows"]
        ):
            raise BronzeError("Silver-to-staging row/warning count mismatch")
        connection.execute(
            sql.SQL("""
            CREATE TEMP TABLE warehouse_classified ON COMMIT DROP AS
            SELECT i.*, CASE
                WHEN t.source_sequence IS NULL THEN 'inserted'
                WHEN i.source_sequence < t.source_sequence THEN 'stale_ignored'
                WHEN i.source_sequence = t.source_sequence AND i.record_hash <> t.record_hash THEN 'conflict'
                WHEN i.record_hash = t.record_hash THEN 'unchanged'
                ELSE 'updated' END AS outcome
            FROM warehouse_input i LEFT JOIN {} t USING ({})
        """).format(target, keys)
        )
        classified = dict(
            connection.execute(
                "SELECT outcome, COUNT(*) FROM warehouse_classified GROUP BY outcome"
            ).fetchall()
        )
        if classified.get("conflict", 0):
            raise BronzeError(
                "Conflicting business payload for the same source version"
            )
        counts = {
            f"warehouse_{outcome}_rows": classified.get(outcome, 0)
            for outcome in OUTCOMES
        }
        if total != sum(counts.values()):
            raise BronzeError("Warehouse merge reconciliation mismatch")
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(name), sql.Identifier(name)
            )
            for name in columns
            if name not in (*KEYS[table], "first_seen_run_id")
        )
        changed = connection.execute(
            sql.SQL("""
            INSERT INTO {} ({}) SELECT {} FROM warehouse_classified
            WHERE outcome <> 'stale_ignored'
            ON CONFLICT ({}) DO UPDATE SET {}, last_seen_at = clock_timestamp()
        """).format(target, names, names, keys, assignments)
        ).rowcount
        if changed != total - counts["warehouse_stale_ignored_rows"]:
            raise BronzeError("Warehouse applied row count mismatch")
        connection.execute(
            """
            INSERT INTO audit.warehouse_load_runs
            (source_table, run_id, request, request_sha256, silver_counts, load_candidate_rows,
             warehouse_inserted_rows, warehouse_updated_rows, warehouse_unchanged_rows, warehouse_stale_ignored_rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                table,
                run_id,
                Jsonb(request),
                request_hash,
                Jsonb(silver_counts),
                total,
                *(counts[f"warehouse_{o}_rows"] for o in OUTCOMES),
            ),
        )
        return {
            "status": "LOADED",
            "table": table,
            "run_id": run_id,
            "counts": {"load_candidate_rows": total, **counts},
        }


def load_silver(
    spark,
    connection: psycopg.Connection,
    store: ObjectStore,
    contract: dict,
    *,
    silver_prefix: str,
    run_id: str,
    table: str = "orders",
) -> dict:
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Warehouse reading requires a UTC Spark session")
    manifest = verify_silver(store, silver_prefix)
    versions = (f"{table}-silver-v1",) + (
        (f"{table}-silver-v2",)
        if table in ("orders", "customers", "order_items")
        else ()
    )
    if (
        table not in KEYS
        or contract["contract_version"] != "1.0.0"
        or manifest["schema_version"] not in versions
    ):
        raise BronzeError("Unsupported warehouse Silver dataset/version")
    body = store.read(f"{silver_prefix}/manifest.json")
    if json.loads(body) != manifest or json.loads(
        store.read(f"{silver_prefix}/_SUCCESS")
    ) != {"manifest_sha256": sha256(body)}:
        raise BronzeError("Silver manifest changed while preparing warehouse load")
    if manifest["request"]["contract_sha256"] != sha256(encode(contract)):
        raise BronzeError("Warehouse source contract hash mismatch")
    request = {
        "loader_version": LOADER_VERSION,
        "hash_schema_version": HASH_VERSION,
        "silver_prefix": silver_prefix,
        "silver_manifest_sha256": sha256(body),
        "contract_sha256": sha256(encode(contract)),
        "table": table,
    }
    with tempfile.TemporaryDirectory(prefix="olist-warehouse-") as directory:
        rows = read_route(spark, store, manifest, Path(directory), "valid")
        return merge_rows(
            connection,
            contract,
            table=table,
            run_id=run_id,
            rows=rows,
            request=request,
            silver_counts=manifest["counts"],
        )


def read_route(spark, store: ObjectStore, manifest: dict, directory: Path, route: str):
    """Read one route with physical schema and byte checks; normalize timestamps to UTC text."""
    from pyspark.sql import functions as F

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver route reading requires a UTC Spark session")
    paths = []
    for number, obj in enumerate(manifest["objects"]):
        if obj["route"] != route:
            continue
        content = store.read(obj["key"])
        if sha256(content) != obj["sha256"]:
            raise BronzeError("Silver object changed while downloading")
        path = directory / f"part-{number}.parquet"
        path.write_bytes(content)
        paths.append(str(path))
    rows = ()
    if paths:
        frame = spark.read.parquet(*paths)
        if frame.schema.jsonValue() != manifest["output_schema"]:
            raise BronzeError("Silver physical schema differs from manifest")
        # toLocalIterator materializes a whole partition; large route files need bounded handoff batches.
        count = manifest["counts"][f"{route}_rows"]
        if count > 10000:
            frame = frame.repartition((count + 9999) // 10000)
        for field in frame.schema.fields:
            if field.dataType.simpleString() == "timestamp":
                frame = frame.withColumn(
                    field.name,
                    F.date_format(field.name, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                )
            elif field.dataType.simpleString() == "date":
                frame = frame.withColumn(field.name, F.col(field.name).cast("string"))
        rows = (row.asDict() for row in frame.toLocalIterator())
    yield from rows
