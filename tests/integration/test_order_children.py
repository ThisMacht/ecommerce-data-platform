from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError, encode, verify_published
from olist_platform.silver.batch import process_batch, verify_silver
from olist_platform.silver.orders import spark_session


class OrderChildrenIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        self.psql(
            (REPOSITORY_ROOT / "tests/fixtures/bronze/order_children.sql").read_text()
        )

    def bronze(self, table, run_id="first", store=None, full_snapshot=False):
        with self.connection() as connection:
            return ingest_source(
                connection,
                store or self.store,
                self.contract,
                table=table,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=1,
                full_snapshot=full_snapshot,
            )

    def silver(self, table, bronze, parent=None, run_id="first", store=None):
        return process_batch(
            self.spark,
            store or self.store,
            self.contract,
            table=table,
            bronze_prefix=bronze["prefix"],
            order_snapshot=parent,
            run_id=run_id,
        )

    def parent(self, run_id="first", full=True):
        bronze = self.bronze("orders", run_id, full_snapshot=full)["manifest"]
        return self.silver("orders", bronze, run_id=run_id)["manifest"]

    def route_rows(self, manifest, route="valid"):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for number, obj in enumerate(manifest["objects"]):
                if obj["route"] == route:
                    path = Path(directory) / f"{number}.parquet"
                    path.write_bytes(self.store.read(obj["key"]))
                    paths.append(str(path))
            return self.spark.read.parquet(*paths).collect() if paths else []

    def test_both_datasets_publish_composite_pages_exact_money_and_replay(self):
        parent = self.parent()
        self.assertEqual(parent["snapshot_kind"], "FULL")
        for table, ordinal, money in (
            ("order_payments", "payment_sequential", "payment_value"),
            ("order_items", "order_item_id", "price"),
        ):
            with self.subTest(table=table):
                bronze = self.bronze(table)["manifest"]
                self.assertEqual((bronze["row_count"], bronze["page_count"]), (4, 4))
                self.assertEqual(verify_published(self.store, bronze["prefix"]), bronze)
                self.assertEqual(self.bronze(table)["status"], "NOOP")
                values = self.rows(bronze)
                self.assertTrue(
                    all(
                        type(r[ordinal]) is int and type(r[money]) is str
                        for r in values
                    )
                )
                self.assertEqual(len({r["_logical_record_uid"] for r in values}), 4)
                manifest = self.silver(table, bronze, parent["prefix"])["manifest"]
                self.assertEqual(
                    manifest["counts"],
                    {
                        "input_rows": 4,
                        "valid_rows": 2,
                        "deferred_rows": 1,
                        "rejected_rows": 1,
                        "duplicates_rows": 0,
                        "warning_rows": 0,
                    },
                )
                valid = sorted(self.route_rows(manifest), key=lambda r: r[ordinal])
                self.assertEqual(
                    [(r["order_id"], r[ordinal]) for r in valid],
                    [("order_a", 1), ("order_a", 2)],
                )
                self.assertEqual(valid[0][money], Decimal("10.50"))
                self.assertEqual(
                    self.route_rows(manifest, "deferred")[0]["deferred_rule_ids"],
                    ["missing_order"],
                )
                self.assertEqual(
                    self.route_rows(manifest, "rejected")[0]["deferred_rule_ids"], []
                )
                self.assertEqual(
                    manifest["unchecked_relationships"],
                    ["product_id", "seller_id"] if table == "order_items" else [],
                )
                self.assertEqual(
                    self.silver(table, bronze, parent["prefix"])["status"], "NOOP"
                )
                self.assertEqual(
                    verify_silver(self.store, manifest["prefix"]), manifest
                )

    def test_prepared_child_retries_keep_bytes_and_checkpoints_independent(self):
        for table in ("order_payments", "order_items"):
            with self.subTest(table=table):
                with self.assertRaises(RuntimeError):
                    self.bronze(table, store=self.failure_store("_SUCCESS"))
                self.assertEqual(
                    self.psql(
                        f"SELECT cursor_version FROM audit.bronze_{table}_control"
                    ),
                    "0",
                )
                self.psql(
                    f"UPDATE olist.{table} SET source_sequence = DEFAULT, source_updated_at = '2020-01-05T00:00:00Z' WHERE order_id = 'order_a'"
                )
                original = self.bronze(table)["manifest"]
                self.assertEqual(
                    [r["source_sequence"] for r in self.rows(original)], [1, 2, 3, 4]
                )
                latest = self.bronze(table, "second")["manifest"]
                self.assertEqual(latest["row_count"], 4)
                self.assertNotEqual(
                    [r["_logical_record_uid"] for r in self.rows(original)],
                    [r["_logical_record_uid"] for r in self.rows(latest)],
                )
                self.assertEqual(
                    self.psql(
                        f"SELECT cursor_version FROM audit.bronze_{table}_control"
                    ),
                    "2",
                )
        self.assertEqual(self.state(), "")

    def test_late_backdated_order_resolves_original_children_with_new_full_snapshot(
        self,
    ):
        parent = self.parent()
        inputs = {
            t: self.bronze(t)["manifest"] for t in ("order_payments", "order_items")
        }
        first = {
            t: self.silver(t, b, parent["prefix"])["manifest"]
            for t, b in inputs.items()
        }
        self.psql("""
            INSERT INTO olist.orders (order_id, customer_id, order_status, order_purchase_timestamp,
                order_estimated_delivery_date, source_updated_at, source_operation, source_batch_id)
            VALUES ('late_order', 'customer_late', 'approved', '2018-01-01',
                '2018-01-10', '2019-01-01T00:00:00Z', 'INSERT', 'late_parent');
        """)
        parent2 = self.parent("second")
        self.assertEqual(parent2["counts"]["valid_rows"], 3)
        for table, bronze in inputs.items():
            with self.subTest(table=table):
                with self.assertRaisesRegex(BronzeError, "conflict"):
                    self.silver(table, bronze, parent2["prefix"])
                result = self.silver(table, bronze, parent2["prefix"], "resolved")[
                    "manifest"
                ]
                self.assertEqual(
                    (result["counts"]["valid_rows"], result["counts"]["deferred_rows"]),
                    (3, 0),
                )
                self.assertEqual(
                    {r["_record_uid"] for r in self.route_rows(result)},
                    {
                        r["_record_uid"]
                        for route in ("valid", "deferred")
                        for r in self.route_rows(first[table], route)
                    },
                )
                self.assertEqual(
                    verify_silver(self.store, first[table]["prefix"]), first[table]
                )

    def test_dependencies_require_full_orders_and_detect_corruption(self):
        delta = self.parent(full=False)
        parent = self.parent("full")
        for table in ("order_payments", "order_items"):
            bronze = self.bronze(table)["manifest"]
            with (
                self.subTest(table=table),
                self.assertRaisesRegex(BronzeError, "configuration"),
            ):
                self.silver(table, bronze)
            with (
                self.subTest(table=table),
                self.assertRaisesRegex(BronzeError, "full verified snapshot"),
            ):
                self.silver(table, bronze, delta["prefix"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=parent["objects"][0]["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.silver(
                "order_items", self.bronze("order_items")["manifest"], parent["prefix"]
            )

    def test_sealed_silver_resumes_missing_marker_and_empty_parent_defers(self):
        self.psql("DELETE FROM olist.orders")
        parent = self.parent()
        for table in ("order_payments", "order_items"):
            bronze = self.bronze(table)["manifest"]
            with self.assertRaises(RuntimeError):
                self.silver(
                    table,
                    bronze,
                    parent["prefix"],
                    store=self.failure_store("_SUCCESS"),
                )
            manifest = self.silver(table, bronze, parent["prefix"])["manifest"]
            self.assertEqual(
                (manifest["counts"]["valid_rows"], manifest["counts"]["deferred_rows"]),
                (0, 3),
            )
            self.assertEqual(
                self.silver(table, bronze, parent["prefix"])["status"], "NOOP"
            )

    def test_cli_extract_and_process_both_child_tables(self):
        parent = self.parent()
        env = os.environ.copy()
        env.pop("BRONZE_SOURCE_DSN", None)
        env.update(
            PGHOST="127.0.0.1",
            PGPORT=str(self.port),
            PGDATABASE=self.database,
            PGUSER="olist_test_loader",
            PGPASSWORD="isolated_test_loader",
            AWS_ACCESS_KEY_ID="bronze_test_admin",
            AWS_SECRET_ACCESS_KEY="isolated_bronze_password",
        )
        with tempfile.TemporaryDirectory() as directory:
            contract = Path(directory) / "contract.json"
            contract.write_bytes(encode(self.contract))
            for table in ("order_payments", "order_items"):
                options = [
                    "--contract",
                    str(contract),
                    "--endpoint",
                    self.endpoint,
                    "--bucket",
                    self.store.bucket,
                    "--table",
                    table,
                ]

                def cli(module, args):
                    result = subprocess.run(
                        [sys.executable, "-m", module] + options + args,
                        env=env,
                        cwd=directory,
                        capture_output=True,
                        text=True,
                        timeout=120,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    return json.loads(result.stdout)["manifest"]

                bronze = cli(
                    "olist_platform.bronze.cli", ["extract", "--run-id", "cli"]
                )
                manifest = cli(
                    "olist_platform.silver.cli",
                    [
                        "process",
                        "--run-id",
                        "cli",
                        "--bronze-prefix",
                        bronze["prefix"],
                        "--order-snapshot",
                        parent["prefix"],
                    ],
                )
                self.assertEqual(manifest["schema_version"], f"{table}-silver-v1")
                self.assertEqual(manifest["counts"]["deferred_rows"], 1)
