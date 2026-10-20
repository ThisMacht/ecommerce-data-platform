"""Apply validated batches atomically using the restricted source-loader role."""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from .batches import BatchError, load_batch, source_time, typed_value


def apply_batch(
    connection: psycopg.Connection, directory: Path, contract: dict
) -> dict:
    batch = load_batch(directory, contract)
    payload, manifest = batch.payload, batch.manifest
    if (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise BatchError("apply_batch requires an idle autocommit connection")
    tables = {table["table"]: table for table in contract["tables"]}
    schema = contract["database"]["source_schema"]
    audit = sql.Identifier(contract["database"]["audit_schema"], "source_batch_runs")
    with connection.transaction():
        # Reads after waiting on a writer must see its newly committed audit row.
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SET LOCAL lock_timeout = '15s'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        # Shared with the seed runner: all cooperating source writers serialize.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            ("olist.seed.olist-kaggle-local-v1",),
        )
        seeded = connection.execute(
            sql.SQL(
                "SELECT 1 FROM {} WHERE snapshot_id = %s AND status = 'SUCCESS'"
            ).format(
                sql.Identifier(contract["database"]["audit_schema"], "source_seed_runs")
            ),
            (contract["dataset_snapshot_id"],),
        ).fetchone()
        if seeded is None:
            raise BatchError("Source snapshot must be seeded before applying batches")
        previous = connection.execute(
            sql.SQL(
                "SELECT payload_sha256, contract_sha256, operation_counts, operation_results FROM {} WHERE batch_id = %s"
            ).format(audit),
            (payload["batch_id"],),
        ).fetchone()
        if previous is not None:
            if (
                previous[0] != manifest["payload_sha256"]
                or previous[1] != manifest["contract_sha256"]
            ):
                raise BatchError(
                    "batch_id already exists with different payload or contract"
                )
            result = {
                "status": "NOOP",
                "batch_id": payload["batch_id"],
                "operation_counts": previous[2],
                "operation_results": previous[3],
            }
        else:
            predecessor = payload["predecessor_batch_id"]
            if (
                predecessor is not None
                and connection.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE batch_id = %s").format(audit),
                    (predecessor,),
                ).fetchone()
                is None
            ):
                raise BatchError(f"Predecessor batch has not committed: {predecessor}")
            results = []
            for operation in payload["operations"]:
                table = tables[operation["table"]]
                columns = {column["name"]: column for column in table["columns"]}
                values = {
                    name: typed_value(value, columns[name])
                    for name, value in operation["values"].items()
                }
                key = {
                    name: typed_value(operation["key"][name], columns[name])
                    for name in table["primary_key"]
                }
                metadata = {
                    "source_updated_at": source_time(operation["source_updated_at"]),
                    "source_operation": operation["operation"],
                    "source_batch_id": payload["batch_id"],
                }
                target = sql.Identifier(schema, operation["table"])
                if operation["operation"] == "INSERT":
                    record = key | values | metadata
                    query = sql.SQL(
                        "INSERT INTO {} ({}) VALUES ({}) RETURNING source_sequence"
                    ).format(
                        target,
                        sql.SQL(", ").join(map(sql.Identifier, record)),
                        sql.SQL(", ").join(sql.Placeholder() for _ in record),
                    )
                    cursor = connection.execute(query, list(record.values()))
                else:
                    record = values | metadata
                    assignments = [
                        sql.SQL("{} = %s").format(sql.Identifier(name))
                        for name in record
                    ]
                    assignments.extend(
                        [
                            sql.SQL("source_sequence = DEFAULT"),
                            sql.SQL("source_loaded_at = clock_timestamp()"),
                        ]
                    )
                    predicate = sql.SQL(" AND ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(name)) for name in key
                    )
                    cursor = connection.execute(
                        sql.SQL(
                            "UPDATE {} SET {} WHERE {} RETURNING source_sequence"
                        ).format(target, sql.SQL(", ").join(assignments), predicate),
                        list(record.values()) + list(key.values()),
                    )
                row = cursor.fetchone()
                if row is None:
                    raise BatchError(
                        f"{operation['operation']} did not affect a source row: {operation['table']} {operation['key']}"
                    )
                results.append(
                    {
                        "table": operation["table"],
                        "operation": operation["operation"],
                        "key": operation["key"],
                        "source_sequence": row[0],
                        "source_updated_at": metadata["source_updated_at"].isoformat(),
                    }
                )
            connection.execute(
                sql.SQL(
                    "INSERT INTO {} (batch_id, payload_sha256, contract_sha256, predecessor_batch_id, applied_at, operation_count, operation_counts, operation_results) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                ).format(audit),
                (
                    payload["batch_id"],
                    manifest["payload_sha256"],
                    manifest["contract_sha256"],
                    predecessor,
                    source_time(payload["applied_at"]),
                    len(results),
                    Jsonb(manifest["operation_counts"]),
                    Jsonb(results),
                ),
            )
            result = {
                "status": "APPLIED",
                "batch_id": payload["batch_id"],
                "operation_counts": manifest["operation_counts"],
                "operation_results": results,
            }
    return result
