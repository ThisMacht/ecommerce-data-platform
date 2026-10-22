"""Recover committed stages and a rolled-back final gate using real data services."""

from __future__ import annotations

import os
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pipeline_case
from pipeline_case import CorePipelineCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import state_sql
from olist_platform.bronze.storage import sha256
from olist_platform.pipeline.cli import configuration, finish
from olist_platform.pipeline.control import (
    ANALYTICS_SOURCES,
    DIMENSIONS,
    TABLES,
    begin,
    fail,
    parent_run_id,
    promote,
)


class InjectedFailure(RuntimeError):
    pass


class PipelineRecoveryTest(CorePipelineCase):
    def controls(self, c):
        return c.execute(
            "SELECT * FROM audit.pipeline_control ORDER BY pipeline_name,source_table"
        ).fetchall()

    def frozen_inputs(self, c, identity):
        result = {}
        for table in ANALYTICS_SOURCES:
            for run_id in (
                (identity, parent_run_id(identity))
                if table in TABLES[:2]
                else (
                    (parent_run_id(identity),) if table in DIMENSIONS else (identity,)
                )
            ):
                manifest = c.execute(
                    state_sql("SELECT manifest FROM {runs} WHERE run_id=%s", table),
                    (run_id,),
                ).fetchone()[0]
                pages = c.execute(
                    state_sql(
                        "SELECT body FROM {parts} WHERE run_id=%s ORDER BY page_number",
                        table,
                    ),
                    (run_id,),
                ).fetchall()
                result[table, run_id] = (manifest, [sha256(bytes(p[0])) for p in pages])
        return result

    def objects(self):
        return {
            item["Key"]: sha256(self.store.read(item["Key"]))
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.store.bucket
            )
            for item in page.get("Contents", [])
        }

    def assert_failed(self, c, identity, attempt, stage, before):
        self.assertEqual(self.controls(c), before)
        self.assertEqual(
            c.execute(
                "SELECT status,attempt,failed_task FROM audit.pipeline_runs WHERE run_id=%s",
                (identity,),
            ).fetchone(),
            ("FAILED", attempt, stage),
        )
        self.assertEqual(
            c.execute(
                "SELECT status FROM audit.pipeline_task_runs WHERE run_id=%s AND run_attempt=%s AND task_name=%s",
                (identity, attempt, stage),
            ).fetchone(),
            ("FAILED",),
        )
        self.assertEqual(
            c.execute(
                "SELECT COUNT(*) FROM audit.pipeline_dataset_runs WHERE run_id=%s",
                (identity,),
            ).fetchone()[0],
            0,
        )

    def recover(self, mode):
        identity = f"recovery_{mode}"
        with (
            self.environment(mode),
            patch.dict(
                os.environ,
                {
                    "BACKFILL_START": "2020-01-01T00:00:00Z",
                    "BACKFILL_END": "2020-01-11T00:00:00Z",
                },
            ),
            self.connection() as c,
            tempfile.TemporaryDirectory() as directory,
        ):
            # One frozen orphan also checks that replay never consumes another retry.
            self.psql("""INSERT INTO olist.orders
                (order_id,customer_id,order_status,order_purchase_timestamp,
                 order_estimated_delivery_date,source_sequence,source_updated_at,
                 source_operation,source_batch_id)
                VALUES ('orphan','absent','created','2018-01-01','2018-01-10',100,
                        '2020-01-10T00:00:00Z','INSERT','recovery_fixture');""")
            request = configuration(REPOSITORY_ROOT, self.contract)
            self.assertEqual(begin(c, identity, request)["attempt"], 1)
            before = self.controls(c)
            self.frozen(c, identity, 1)
            frozen = self.frozen_inputs(c, identity)
            self.assertEqual(len(frozen), 8)
            self.assertEqual(len({m["source_snapshot"] for m, _ in frozen.values()}), 1)

            earlier_objects = {}
            for attempt, (function, stage) in enumerate(
                (
                    ("ingest_source", "orders.bronze.extract"),
                    ("process_batch", "orders.silver.process"),
                    ("load_silver", "orders.warehouse.load"),
                ),
                1,
            ):
                with self.subTest(mode=mode, stage=stage):
                    if attempt > 1:
                        self.assertEqual(
                            begin(c, identity, request)["attempt"], attempt
                        )
                        self.frozen(c, identity, attempt)
                    original = getattr(pipeline_case, function)

                    def lose_response(*args, **kwargs):
                        result = original(*args, **kwargs)
                        if (
                            kwargs.get("table") == "orders"
                            and kwargs["run_id"] == identity
                        ):
                            fail(c, identity, attempt, stage)
                            raise InjectedFailure(stage)
                        return result

                    with patch.object(
                        pipeline_case, function, side_effect=lose_response
                    ):
                        with self.assertRaisesRegex(InjectedFailure, stage):
                            self.process(c, identity, attempt, mode)
                    self.assert_failed(c, identity, attempt, stage, before)
                    self.assertEqual(self.frozen_inputs(c, identity), frozen)
                    preserved = self.objects()
                    if function == "ingest_source":
                        self.assertEqual(
                            c.execute(
                                "SELECT COUNT(*) FROM raw_staging.orders"
                            ).fetchone()[0],
                            0,
                        )
                        self.psql("""UPDATE olist.orders SET order_status='shipped',source_sequence=500,
                            source_updated_at='2020-01-12T00:00:00Z' WHERE order_id='order_a';
                            DELETE FROM olist.products; DELETE FROM olist.sellers;""")
                    elif function == "process_batch":
                        self.assertIn(
                            f"silver/orders/run_id={identity}/_SUCCESS", preserved
                        )
                        self.assertEqual(
                            c.execute(
                                "SELECT COUNT(*) FROM raw_staging.orders"
                            ).fetchone()[0],
                            0,
                        )
                    else:
                        self.assertEqual(
                            c.execute(
                                "SELECT COUNT(*) FROM raw_staging.orders"
                            ).fetchone()[0],
                            2,
                        )
                    for key, digest in earlier_objects.items():
                        self.assertEqual(preserved[key], digest)
                    earlier_objects = preserved

            self.assertEqual(begin(c, identity, request)["attempt"], 4)
            self.frozen(c, identity, 4)
            published = self.process(c, identity, 4, mode)
            failed_builds = []

            def rollback_after_dbt(*args):
                result = promote(*args)
                self.assertEqual(result["dbt"]["node_count"], 100)
                failed_builds.append(result["dbt"]["invocation_id"])
                raise InjectedFailure("after dbt and CAS, before transaction commit")

            with patch(
                "olist_platform.pipeline.cli.promote", side_effect=rollback_after_dbt
            ):
                with self.assertRaisesRegex(InjectedFailure, "after dbt"):
                    finish(
                        c,
                        self.store,
                        self.contract,
                        REPOSITORY_ROOT,
                        identity,
                        4,
                        request,
                        Path(directory) / "failed",
                    )
            self.assertEqual(len(failed_builds), 1)
            self.assert_failed(c, identity, 4, "dbt.build", before)
            preserved = self.objects()
            for key, digest in earlier_objects.items():
                self.assertEqual(preserved[key], digest)
            receipts = c.execute(
                "SELECT * FROM audit.warehouse_load_runs ORDER BY source_table,run_id"
            ).fetchall()
            queue = c.execute(
                "SELECT * FROM audit.quarantine_records ORDER BY record_uid,rule_id"
            ).fetchall()
            self.assertEqual(len(queue), 1)
            self.assertEqual(
                c.execute(
                    "SELECT attempt_count FROM audit.quarantine_records"
                ).fetchone()[0],
                1,
            )

            self.assertEqual(begin(c, identity, request)["attempt"], 5)
            self.frozen(c, identity, 5)
            self.assertEqual(self.process(c, identity, 5, mode), published)
            completed = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                identity,
                5,
                request,
                Path(directory) / "success",
            )
            self.assertEqual(completed["status"], "SUCCESS")
            self.assertEqual(completed["dbt"]["node_count"], 100)
            self.assertNotEqual(completed["dbt"]["invocation_id"], failed_builds[0])
            self.assertEqual(self.frozen_inputs(c, identity), frozen)
            self.assertEqual(self.objects(), preserved)
            self.assertEqual(
                c.execute(
                    "SELECT * FROM audit.warehouse_load_runs ORDER BY source_table,run_id"
                ).fetchall(),
                receipts,
            )
            self.assertEqual(
                c.execute(
                    "SELECT * FROM audit.quarantine_records ORDER BY record_uid,rule_id"
                ).fetchall(),
                queue,
            )
            self.assertEqual(
                c.execute(
                    "SELECT order_status,source_sequence FROM raw_staging.orders WHERE order_id='order_a'"
                ).fetchone(),
                ("created", 1),
            )
            for table in TABLES:
                self.assertEqual(
                    c.execute(f"SELECT COUNT(*) FROM raw_staging.{table}").fetchone()[
                        0
                    ],
                    2,
                )
            self.assertEqual(
                c.execute(
                    "SELECT COUNT(*),SUM(gross_paid_amount),SUM(item_amount) FROM analytics.fct_orders"
                ).fetchone(),
                (2, Decimal("11.75"), Decimal("15.50")),
            )
            self.assertEqual(
                sum(x["source_rows_extracted"] for x in completed["datasets"].values()),
                11,
            )
            self.assertEqual(
                sum(
                    x["warehouse_inserted_rows"] for x in completed["datasets"].values()
                ),
                10,
            )
            self.assertEqual(
                sum(
                    x["deferred_retry_input_rows"]
                    for x in completed["datasets"].values()
                ),
                1,
            )
            if mode == "backfill":
                self.assertEqual(self.controls(c), before)
            else:
                self.assertEqual(
                    c.execute(
                        "SELECT MIN(cursor_version),MAX(cursor_version) FROM audit.pipeline_control"
                    ).fetchone(),
                    (1, 1),
                )
            for dimension in DIMENSIONS:
                self.assertEqual(
                    completed["datasets"][dimension]["dimension_snapshot_rows"], 1
                )
                self.assertEqual(
                    c.execute(
                        f"SELECT COUNT(*) FROM raw_staging.{dimension}"
                    ).fetchone()[0],
                    1,
                )
            self.assertEqual(
                c.execute(
                    "SELECT COUNT(*) FROM audit.pipeline_control WHERE source_table IN ('products','sellers')"
                ).fetchone()[0],
                0,
            )
            after = self.controls(c)
            self.assertEqual(begin(c, identity, request)["status"], "NOOP")
            self.assertEqual(
                finish(
                    c,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    identity,
                    5,
                    request,
                    Path(directory) / "noop",
                )["status"],
                "NOOP",
            )
            self.assertEqual(self.controls(c), after)
            self.assertEqual(
                c.execute(
                    "SELECT COUNT(*),BOOL_AND(status='SUCCESS') FROM audit.pipeline_task_runs WHERE run_id=%s AND run_attempt=5",
                    (identity,),
                ).fetchone(),
                (30, True),
            )

    def test_incremental_recovers_each_committed_stage_and_final_transaction(self):
        self.recover("incremental")

    def test_backfill_recovers_each_committed_stage_without_promoting_controls(self):
        self.recover("backfill")
