"""PostgreSQL quarantine registration and atomic dependency resolution."""

from __future__ import annotations

import json
import re
import tempfile
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import BronzeError, encode, sha256
from olist_platform.silver.batch import load_parent_keys, verify_silver
from olist_platform.warehouse.loading import _merge_rows, canonical_record, read_route

DEPENDENCIES = {
    "orders": ("customers", "customer_id", "missing_customer"),
    "order_items": ("orders", "order_id", "missing_order"),
    "order_payments": ("orders", "order_id", "missing_order"),
}


def validate_policy(attempts: int, hours: int) -> None:
    if (
        type(attempts) is not int
        or not 1 <= attempts <= 100
        or type(hours) is not int
        or not 1 <= hours <= 8760
    ):
        raise BronzeError(
            "Deferred policy requires 1-100 attempts and 1-8760 age hours"
        )


@contextmanager
def locked(connection, table: str, run_id: str):
    if table not in DEPENDENCIES or not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id
    ):
        raise BronzeError("Invalid deferred table/run ID")
    if (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise BronzeError("Deferred lifecycle requires an idle autocommit connection")
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SET LOCAL lock_timeout = '15s'")
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext('olist.deferred.lifecycle.v1'))"
        )
        yield


def thaw(payload: dict, contract: dict, table: str) -> dict:
    row = dict(payload)
    columns = next(t["columns"] for t in contract["tables"] if t["table"] == table)
    for column in columns:
        if (
            column["postgres_type"] == "numeric(18,2)"
            and row[column["name"]] is not None
        ):
            row[column["name"]] = Decimal(row[column["name"]])
    row["deferred_rule_ids"] = []
    return row


def register_rows(
    connection,
    contract: dict,
    *,
    table: str,
    run_id: str,
    rows,
    request: dict,
    expected_count: int,
) -> dict:
    validate_policy(request["max_attempts"], request["max_deferred_age_hours"])
    request_hash = sha256(encode(request))
    with locked(connection, table, run_id):
        previous = connection.execute(
            "SELECT request_sha256, input_rows, inserted_rows FROM audit.deferred_registration_runs WHERE source_table=%s AND run_id=%s",
            (table, run_id),
        ).fetchone()
        if previous:
            if previous[0] != request_hash:
                raise BronzeError("Deferred registration run ID request conflict")
            return {
                "status": "NOOP",
                "input_rows": previous[1],
                "inserted_rows": previous[2],
            }
        total = inserted = 0
        seen = set()
        for row in rows:
            if (
                row.get("deferred_rule_ids") != [DEPENDENCIES[table][2]]
                or row.get("error_rule_ids") != []
            ):
                raise BronzeError("Unsupported deferred routing rules")
            payload = {
                name: str(value) if isinstance(value, Decimal) else value
                for name, value in row.items()
            }
            canonical_record(thaw(payload, contract, table), contract, table, run_id)
            uid, rule = row["_record_uid"], DEPENDENCIES[table][2]
            if uid in seen:
                raise BronzeError("Repeated deferred physical UID")
            seen.add(uid)
            digest = sha256(encode(payload))
            existing = connection.execute(
                "SELECT payload_sha256, contract_sha256, max_attempts, max_deferred_age_hours FROM audit.quarantine_records WHERE record_uid=%s AND rule_id=%s",
                (uid, rule),
            ).fetchone()
            if existing:
                if existing != (
                    digest,
                    request["contract_sha256"],
                    request["max_attempts"],
                    request["max_deferred_age_hours"],
                ):
                    raise BronzeError("Deferred immutable payload/policy conflict")
            else:
                connection.execute(
                    """INSERT INTO audit.quarantine_records
                    (record_uid, rule_id, logical_record_uid, run_id, source_table, source_primary_key,
                     source_version, raw_payload, payload, payload_sha256, contract_sha256,
                     origin_silver_prefix, origin_manifest_sha256, max_attempts, max_deferred_age_hours)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        uid,
                        rule,
                        row["_logical_record_uid"],
                        row["_run_id"],
                        table,
                        Jsonb({key: row[key] for key in KEYS[table]}),
                        row["source_sequence"],
                        Jsonb(json.loads(row["_raw_payload"])),
                        Jsonb(payload),
                        digest,
                        request["contract_sha256"],
                        request["silver_prefix"],
                        request["silver_manifest_sha256"],
                        request["max_attempts"],
                        request["max_deferred_age_hours"],
                    ),
                )
                inserted += 1
            total += 1
        if type(expected_count) is not int or total != expected_count:
            raise BronzeError("Deferred registration row count mismatch")
        connection.execute(
            "INSERT INTO audit.deferred_registration_runs (source_table, run_id, request_sha256, request, input_rows, inserted_rows) VALUES (%s,%s,%s,%s,%s,%s)",
            (table, run_id, request_hash, Jsonb(request), total, inserted),
        )
        return {"status": "REGISTERED", "input_rows": total, "inserted_rows": inserted}


def resolve_rows(
    connection,
    contract: dict,
    *,
    table: str,
    run_id: str,
    parent_keys: set[str],
    request: dict,
) -> dict:
    request_hash = sha256(encode(request))
    with locked(connection, table, run_id):
        previous = connection.execute(
            "SELECT request_sha256, result FROM audit.deferred_resolution_runs WHERE source_table=%s AND run_id=%s",
            (table, run_id),
        ).fetchone()
        if previous:
            if previous[0] != request_hash:
                raise BronzeError("Deferred resolution run ID request conflict")
            return previous[1] | {"status": "NOOP"}
        now = connection.execute("SELECT clock_timestamp()").fetchone()[0]
        origin = request.get("origin_run_id")
        pending = connection.execute(
            "SELECT record_uid, rule_id, payload, payload_sha256, contract_sha256, first_seen_at, attempt_count, max_attempts, max_deferred_age_hours FROM audit.quarantine_records WHERE source_table=%s AND retry_status='PENDING' AND (%s::text IS NULL OR run_id=%s) ORDER BY first_seen_at, record_uid LIMIT 1000 FOR UPDATE",
            (table, origin, origin),
        ).fetchall()
        resolved = []
        version_hashes = {}
        counts = {
            "input_rows": len(pending),
            "valid_rows": 0,
            "rejected_rows": 0,
            "deferred_rows": 0,
            "duplicates_rows": 0,
            "warning_rows": 0,
        }
        for (
            uid,
            rule,
            payload,
            payload_hash,
            contract_hash,
            first_seen,
            attempts,
            limit,
            hours,
        ) in pending:
            if (
                contract_hash != sha256(encode(contract))
                or sha256(encode(payload)) != payload_hash
            ):
                raise BronzeError("Deferred payload/contract checksum mismatch")
            attempt = attempts + 1
            expired_age = now - first_seen >= timedelta(hours=hours)
            present = payload[DEPENDENCIES[table][1]] in parent_keys
            reason = (
                "max_deferred_age"
                if expired_age
                else (
                    "max_attempts"
                    if attempts >= limit or (not present and attempt >= limit)
                    else None
                )
            )
            state = "EXPIRED" if reason else ("RESOLVED" if present else "PENDING")
            if state == "RESOLVED":
                row = thaw(payload, contract, table)
                record = canonical_record(row, contract, table, run_id)
                version_key = tuple(row[key] for key in KEYS[table]) + (
                    row["source_sequence"],
                )
                digest = record["record_hash"]
                if (
                    version_key in version_hashes
                    and version_hashes[version_key] != digest
                ):
                    raise BronzeError(
                        "Conflicting deferred business payloads for the same source version"
                    )
                version_hashes[version_key] = digest
                resolved.append(row)
            else:
                counts["rejected_rows" if state == "EXPIRED" else "deferred_rows"] += 1
            connection.execute(
                "UPDATE audit.quarantine_records SET retry_status=%s, attempt_count=%s, last_attempt_at=%s, resolved_at=%s, terminal_reason=%s WHERE record_uid=%s AND rule_id=%s",
                (
                    state,
                    attempt,
                    now,
                    now if state == "RESOLVED" else None,
                    reason,
                    uid,
                    rule,
                ),
            )
            connection.execute(
                "INSERT INTO audit.deferred_attempts (source_table,run_id,record_uid,rule_id,attempt_number,outcome,terminal_reason) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (table, run_id, uid, rule, attempt, state, reason),
            )
        latest = {}
        for row in resolved:
            key = tuple(row[name] for name in KEYS[table])
            previous = latest.get(key)
            if previous is None or (row["source_sequence"], row["_record_uid"]) > (
                previous["source_sequence"],
                previous["_record_uid"],
            ):
                latest[key] = row
        winners = list(latest.values())
        selected_uids = {row["_record_uid"] for row in winners}
        duplicate_uids = sorted(
            row["_record_uid"]
            for row in resolved
            if row["_record_uid"] not in selected_uids
        )
        counts["valid_rows"] = len(winners)
        counts["duplicates_rows"] = len(duplicate_uids)
        counts["warning_rows"] = sum(bool(row["warning_rule_ids"]) for row in winners)
        if len(pending) != sum(
            counts[name]
            for name in (
                "valid_rows",
                "rejected_rows",
                "deferred_rows",
                "duplicates_rows",
            )
        ):
            raise BronzeError("Deferred resolution reconciliation mismatch")
        load_run_id = "deferred_" + sha256(encode([table, run_id]))
        loaded = _merge_rows(
            connection,
            contract,
            table=table,
            run_id=load_run_id,
            rows=winners,
            request=request | {"origin": "DEFERRED_RETRY", "resolution_run_id": run_id},
            silver_counts=counts,
        )
        result = {
            "status": "COMMITTED",
            "table": table,
            "run_id": run_id,
            "source_rows_extracted": 0,
            "bronze_rows_read": 0,
            "deferred_retry_input_rows": len(pending),
            "routing_counts": counts,
            "duplicate_record_uids": duplicate_uids,
            "warehouse_run_id": load_run_id,
            "warehouse_counts": loaded["counts"],
            "remaining_pending_rows": connection.execute(
                "SELECT COUNT(*) FROM audit.quarantine_records WHERE source_table=%s AND retry_status='PENDING' AND (%s::text IS NULL OR run_id=%s)",
                (table, origin, origin),
            ).fetchone()[0],
        }
        connection.execute(
            "INSERT INTO audit.deferred_resolution_runs (source_table,run_id,request_sha256,request,result) VALUES (%s,%s,%s,%s,%s)",
            (table, run_id, request_hash, Jsonb(request), Jsonb(result)),
        )
        return result


def verified(store, prefix: str, contract: dict) -> tuple[dict, str]:
    manifest = verify_silver(store, prefix)
    body = store.read(f"{prefix}/manifest.json")
    digest = sha256(body)
    if json.loads(body) != manifest or json.loads(store.read(f"{prefix}/_SUCCESS")) != {
        "manifest_sha256": digest
    }:
        raise BronzeError("Silver changed while preparing deferred operation")
    if (
        manifest["request"]["contract_sha256"] != sha256(encode(contract))
        or contract["contract_version"] != "1.0.0"
    ):
        raise BronzeError("Deferred source contract mismatch")
    return manifest, digest


def register_silver(
    spark,
    connection,
    store,
    contract: dict,
    *,
    table: str,
    run_id: str,
    silver_prefix: str,
    max_attempts=3,
    max_deferred_age_hours=168,
) -> dict:
    manifest, digest = verified(store, silver_prefix, contract)
    expected_version = "orders-silver-v2" if table == "orders" else f"{table}-silver-v1"
    if table not in DEPENDENCIES or manifest["schema_version"] != expected_version:
        raise BronzeError("Deferred registration requires dependency-aware Silver")
    request = {
        "processor_version": "deferred-register-v1",
        "silver_prefix": silver_prefix,
        "silver_manifest_sha256": digest,
        "contract_sha256": sha256(encode(contract)),
        "max_attempts": max_attempts,
        "max_deferred_age_hours": max_deferred_age_hours,
    }
    with tempfile.TemporaryDirectory(prefix="olist-deferred-") as directory:
        rows = read_route(spark, store, manifest, Path(directory), "deferred")
        return register_rows(
            connection,
            contract,
            table=table,
            run_id=run_id,
            rows=rows,
            request=request,
            expected_count=manifest["counts"]["deferred_rows"],
        )


def resolve_snapshot(
    spark,
    connection,
    store,
    contract: dict,
    *,
    table: str,
    run_id: str,
    parent_snapshot: str,
    origin_run_id: str | None = None,
) -> dict:
    if table not in DEPENDENCIES:
        raise BronzeError("Unsupported deferred dependency table")
    parent_table, key, _ = DEPENDENCIES[table]
    parent, digest = verified(store, parent_snapshot, contract)
    versions = (f"{parent_table}-silver-v1",) + (
        ("orders-silver-v2",) if parent_table == "orders" else ()
    )
    if (
        parent["schema_version"] not in versions
        or parent.get("snapshot_kind") != "FULL"
    ):
        raise BronzeError(
            "Deferred resolution requires a full verified parent snapshot"
        )
    request = {
        "processor_version": "deferred-resolve-v1",
        "batch_limit": 1000,
        "table": table,
        "parent_snapshot": parent_snapshot,
        "parent_manifest_sha256": digest,
        "contract_sha256": sha256(encode(contract)),
    }
    if origin_run_id is not None:
        if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", origin_run_id) is None:
            raise BronzeError("Invalid deferred origin run ID")
        request["origin_run_id"] = origin_run_id
    with tempfile.TemporaryDirectory(prefix="olist-deferred-parent-") as directory:
        frame = load_parent_keys(
            spark, store, parent, Path(directory) / "keys", key=key
        )
        try:
            # shortcut: local snapshot key set in memory; use a distributed join before scaling beyond local MVP.
            keys = {row[key] for row in frame.toLocalIterator()}
            return resolve_rows(
                connection,
                contract,
                table=table,
                run_id=run_id,
                parent_keys=keys,
                request=request,
            )
        finally:
            frame.unpersist()
