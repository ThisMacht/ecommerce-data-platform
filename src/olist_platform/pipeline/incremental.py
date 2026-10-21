"""Freeze all incremental source bytes and full FK inputs in one source snapshot."""

from olist_platform.bronze.extraction import configuration, prepare
from olist_platform.bronze.storage import BronzeError

from .control import INCREMENTAL, TABLES, current, lock, parent_run_id


def freeze(connection, store, contract, run_id, attempt, endpoint):
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        lock(connection)
        row = current(connection, run_id, attempt)
        if row["pipeline_name"] != INCREMENTAL:
            raise BronzeError("Only incremental pipelines freeze source streams")
        if contract["policies"]["lateness_sla_hours"] != 48:
            raise BronzeError("Incremental core v1 requires the 48-hour contract")
        for table in TABLES:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"olist.bronze.{table}.v1",),
            )
        streams = [(table, False) for table in TABLES] + [
            (table, True) for table in TABLES[:2]
        ]
        manifests = {}
        for table, parent in streams:
            identity = parent_run_id(run_id) if parent else run_id
            config = configuration(
                contract,
                store,
                endpoint,
                1000,
                table,
                pipeline_run_id=run_id,
                pipeline_parent=parent,
            )
            manifests[f"{table}.{'parent' if parent else 'delta'}"] = prepare(
                connection,
                identity,
                contract,
                config,
                table,
                snapshot_transaction=True,
            )
        if len({m["source_snapshot"] for m in manifests.values()}) != 1:
            raise BronzeError("Incremental streams were not frozen together")
    return {"status": "FROZEN", "parent_run_id": parent_run_id(run_id)}
