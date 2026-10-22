from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from pipeline_case import CorePipelineCase
from postgres_case import REPOSITORY_ROOT
from psycopg.types.json import Jsonb

from olist_platform.bronze.extraction import ingest_source, prepare
from olist_platform.bronze.storage import BronzeError
from olist_platform.pipeline.cli import configuration, finish
from olist_platform.pipeline.control import (
    ANALYTICS_SOURCES,
    TABLES,
    begin,
    fail,
    parent_run_id,
)
from olist_platform.pipeline.incremental import freeze
from olist_platform.warehouse.loading import load_silver


class IncrementalPipelineTest(CorePipelineCase):
    def test_late_windows_frozen_retry_delivery_independence_and_dbt_gate(self):
        with (
            self.environment(),
            self.connection() as c,
            tempfile.TemporaryDirectory() as directory,
        ):
            request = configuration(REPOSITORY_ROOT, self.contract)
            begin(c, "bootstrap", request)
            self.frozen(c, "bootstrap", 1)
            baseline = self.process(c, "bootstrap", 1)
            first = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "bootstrap",
                1,
                request,
                Path(directory) / "first",
            )
            self.assertEqual(first["datasets"]["customers"]["source_bootstrap_rows"], 2)
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "2"
            )
            self.psql(
                (
                    REPOSITORY_ROOT / "tests/fixtures/bronze/incremental_updates.sql"
                ).read_text()
            )
            begin(c, "late", request)
            self.frozen(c, "late", 1)
            self.psql("""INSERT INTO olist.orders
                (order_id,customer_id,order_status,order_purchase_timestamp,order_estimated_delivery_date,
                 source_sequence,source_updated_at,source_operation,source_batch_id)
                VALUES ('post_freeze','customer_a','created','2018-01-01','2018-01-10',104,
                        '2020-01-12T00:00:00Z','INSERT','post_freeze');""")
            fail(c, "late", 1, "orders.bronze.extract")
            delivery = ingest_source(
                c,
                self.store,
                self.contract,
                run_id="delivery_ahead",
                endpoint=self.endpoint,
                full_snapshot=True,
            )["manifest"]
            self.assertTrue(
                delivery["run_upper_cursor"]["updated_at"].startswith("2020-01-12")
            )
            self.assertEqual(begin(c, "late", request)["attempt"], 2)
            self.frozen(c, "late", 2)
            published = self.process(c, "late", 2)
            for table, expected in (
                ("customers", (3, 2)),
                ("orders", (3, 2)),
                ("order_items", (5, 3)),
                ("order_payments", (5, 3)),
            ):
                bronze = published[table][0]
                self.assertEqual(bronze["row_count"], expected[0])
                self.assertEqual(
                    bronze["source_after_cursor_rows"], 1 if table in TABLES[:2] else 2
                )
                self.assertEqual(bronze["source_lookback_rows"], expected[1])
                self.assertTrue(bronze["extract_lower_bound"].startswith("2020-01-08"))
                self.assertTrue(
                    bronze["run_upper_cursor"]["updated_at"].startswith("2020-01-11")
                )
                self.assertFalse(
                    any(
                        "outside" in str(r) or "post_freeze" in str(r)
                        for r in self.rows(bronze)
                    )
                )
            self.assertNotIn(
                "order_old", {r["order_id"] for r in self.rows(published["orders"][0])}
            )
            self.assertNotIn(
                "customer_old",
                {r["customer_id"] for r in self.rows(published["customers"][0])},
            )
            self.assertEqual(published["orders"][1]["counts"]["deferred_rows"], 0)
            self.assertEqual(published["order_items"][1]["counts"]["deferred_rows"], 0)
            with patch(
                "olist_platform.pipeline.cli.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, "dbt"),
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    finish(
                        c,
                        self.store,
                        self.contract,
                        REPOSITORY_ROOT,
                        "late",
                        2,
                        request,
                        Path(directory) / "failed",
                    )
            self.assertEqual(
                self.psql(
                    "SELECT MAX(cursor_version) FROM audit.pipeline_control WHERE pipeline_name='incremental_core_v1'"
                ),
                "1",
            )
            self.assertEqual(begin(c, "late", request)["attempt"], 3)
            self.frozen(c, "late", 3)
            replayed = self.process(c, "late", 3)
            self.assertEqual(published, replayed)
            final = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "late",
                3,
                request,
                Path(directory) / "final",
            )
            self.assertEqual(final["status"], "SUCCESS")
            self.assertEqual(final["dbt"]["node_count"], 189)
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "4"
            )
            self.assertEqual(
                self.psql(
                    "SELECT customer_city FROM raw_staging.customers WHERE customer_id='customer_a'"
                ),
                "campinas",
            )
            self.assertEqual(
                self.psql(
                    "SELECT COUNT(*) FROM raw_staging.orders WHERE order_id IN ('order_outside','post_freeze')"
                ),
                "0",
            )
            self.assertEqual(
                self.psql(
                    "SELECT MIN(cursor_version) FROM audit.pipeline_control WHERE pipeline_name='incremental_core_v1'"
                ),
                "2",
            )
            stale = load_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                table="orders",
                run_id="stale_replay",
                silver_prefix=baseline["orders"][1]["prefix"],
            )
            self.assertEqual(stale["counts"]["warehouse_stale_ignored_rows"], 1)
            self.assertEqual(
                self.psql(
                    "SELECT order_status FROM raw_staging.orders WHERE order_id='order_a'"
                ),
                "approved",
            )
            self.assertEqual(begin(c, "late", request)["status"], "NOOP")

    def test_atomic_freeze_and_unfrozen_publication_rejected(self):
        with self.environment(), self.connection() as c:
            begin(c, "atomic", configuration(REPOSITORY_ROOT, self.contract))
            with self.assertRaisesRegex(BronzeError, "Freeze all"):
                ingest_source(
                    c,
                    self.store,
                    self.contract,
                    table="orders",
                    run_id="atomic",
                    endpoint=self.endpoint,
                    pipeline_run_id="atomic",
                )

            def injected(*args, **kwargs):
                if args[4] == "sellers" and args[1] == parent_run_id("atomic"):
                    raise RuntimeError("failed last stream")
                return prepare(*args, **kwargs)

            with patch(
                "olist_platform.pipeline.incremental.prepare", side_effect=injected
            ):
                with self.assertRaisesRegex(RuntimeError, "last stream"):
                    freeze(c, self.store, self.contract, "atomic", 1, self.endpoint)
            for table in ANALYTICS_SOURCES:
                self.assertEqual(
                    self.psql(f"SELECT COUNT(*) FROM audit.bronze_{table}_runs"), "0"
                )
            freeze(c, self.store, self.contract, "atomic", 1, self.endpoint)
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM audit.bronze_customers_runs"), "2"
            )

    def test_full_freeze_rolls_back_six_streams_and_reuses_dimension_bytes(self):
        with self.environment(mode="full"), self.connection() as c:
            begin(c, "full_freeze", configuration(REPOSITORY_ROOT, self.contract))

            def injected(*args, **kwargs):
                if args[4] == "sellers":
                    raise RuntimeError("last full stream")
                return prepare(*args, **kwargs)

            with patch(
                "olist_platform.pipeline.incremental.prepare", side_effect=injected
            ):
                with self.assertRaisesRegex(RuntimeError, "last full stream"):
                    freeze(
                        c, self.store, self.contract, "full_freeze", 1, self.endpoint
                    )
            for table in ANALYTICS_SOURCES:
                self.assertEqual(
                    self.psql(f"SELECT COUNT(*) FROM audit.bronze_{table}_runs"), "0"
                )
            self.frozen(c, "full_freeze", 1)
            self.psql("DELETE FROM olist.products; DELETE FROM olist.sellers;")
            freeze(c, self.store, self.contract, "full_freeze", 1, self.endpoint)
            for table in ("products", "sellers"):
                bronze = ingest_source(
                    c,
                    self.store,
                    self.contract,
                    table=table,
                    run_id="full_freeze",
                    endpoint=self.endpoint,
                    pipeline_run_id="full_freeze",
                    pipeline_mode="full",
                )["manifest"]
                self.assertEqual(bronze["row_count"], 1)
                self.assertEqual(bronze["snapshot_kind"], "FULL")
                self.assertEqual(bronze["source_bootstrap_rows"], 0)

    def test_source_writes_between_tables_do_not_split_the_snapshot(self):
        with self.environment(), self.connection() as c:
            begin(c, "concurrent_source", configuration(REPOSITORY_ROOT, self.contract))

            def concurrent(*args, **kwargs):
                result = prepare(*args, **kwargs)
                if args[4] == "customers" and args[1] == "concurrent_source":
                    self.psql(
                        "UPDATE olist.orders SET order_status='shipped', source_sequence=101, source_updated_at='2020-01-12T00:00:00Z' WHERE order_id='order_a'"
                    )
                return result

            with patch(
                "olist_platform.pipeline.incremental.prepare", side_effect=concurrent
            ):
                freeze(
                    c, self.store, self.contract, "concurrent_source", 1, self.endpoint
                )
            bronze = ingest_source(
                c,
                self.store,
                self.contract,
                table="orders",
                run_id="concurrent_source",
                endpoint=self.endpoint,
                pipeline_run_id="concurrent_source",
            )["manifest"]
            self.assertEqual(self.rows(bronze)[0]["order_status"], "created")
            self.assertTrue(
                bronze["run_upper_cursor"]["updated_at"].startswith("2020-01-10")
            )
            self.assertEqual(
                self.psql(
                    "SELECT order_status FROM olist.orders WHERE order_id='order_a'"
                ),
                "shipped",
            )

    def test_ahead_delivery_cursor_never_selects_or_regresses_the_pipeline_window(self):
        with self.environment(), self.connection() as c:
            begin(c, "ahead", configuration(REPOSITORY_ROOT, self.contract))
            ahead = {
                "updated_at": "2020-01-20T00:00:00+00:00",
                "source_sequence": 500,
                "primary_key": {"order_id": "future"},
            }
            c.execute(
                "INSERT INTO audit.bronze_orders_control (pipeline,cursor_value) VALUES ('incremental_orders_delta_v1',%s)",
                (Jsonb(ahead),),
            )
            freeze(c, self.store, self.contract, "ahead", 1, self.endpoint)
            bronze = ingest_source(
                c,
                self.store,
                self.contract,
                run_id="ahead",
                endpoint=self.endpoint,
                pipeline_run_id="ahead",
            )["manifest"]
            self.assertIsNone(bronze["watermark_before"])
            self.assertEqual(bronze["source_bootstrap_rows"], 2)
            self.assertEqual(bronze["delivery_watermark_candidate"], ahead)
            self.assertEqual(
                c.execute(
                    "SELECT cursor_value FROM audit.bronze_orders_control WHERE pipeline='incremental_orders_delta_v1'"
                ).fetchone()[0],
                ahead,
            )

    def test_empty_bootstrap_can_complete_all_gates(self):
        for table in ANALYTICS_SOURCES:
            self.psql(f"DELETE FROM olist.{table}")
        with (
            self.environment(),
            self.connection() as c,
            tempfile.TemporaryDirectory() as directory,
        ):
            request = configuration(REPOSITORY_ROOT, self.contract)
            begin(c, "empty", request)
            self.frozen(c, "empty", 1)
            self.process(c, "empty", 1)
            result = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "empty",
                1,
                request,
                Path(directory),
            )
            self.assertEqual(result["status"], "SUCCESS")
            self.assertTrue(
                all(
                    x["source_rows_extracted"] == 0 for x in result["datasets"].values()
                )
            )
            self.assertEqual(
                self.psql(
                    "SELECT COUNT(*) FROM audit.pipeline_control WHERE committed_cursor='null'::jsonb AND cursor_version=1"
                ),
                "4",
            )
