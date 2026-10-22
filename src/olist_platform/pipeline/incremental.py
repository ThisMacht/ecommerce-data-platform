"""Freeze all owned source bytes and full FK inputs in one source snapshot."""

from olist_platform.bronze.extraction import configuration, prepare
from olist_platform.bronze.storage import BronzeError

from .control import ANALYTICS_SOURCES, PARENTS, TABLES, current, lock, parent_run_id


def freeze(connection, store, contract, run_id, attempt, endpoint):
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        lock(connection)
        row = current(connection, run_id, attempt)
        if contract["policies"]["lateness_sla_hours"] != 48:
            raise BronzeError("Pipeline requires the 48-hour contract")
        for table in ANALYTICS_SOURCES:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"olist.bronze.{table}.v1",),
            )
        streams = (
            [(table, False) for table in TABLES] + [(table, True) for table in PARENTS]
            if row["request"].get("mode", "full") != "full"
            else [(table, False) for table in ANALYTICS_SOURCES]
        )
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
                pipeline_mode=row["request"].get("mode", "full"),
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
            raise BronzeError("Pipeline streams were not frozen together")
    return {"status": "FROZEN", "parent_run_id": parent_run_id(run_id)}
