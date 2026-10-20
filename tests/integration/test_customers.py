from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError, encode, verify_published
from olist_platform.silver.batch import process_customers, process_orders, verify_silver
from olist_platform.silver.orders import spark_session


class CustomerIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        fixture = REPOSITORY_ROOT / "tests/fixtures/bronze/customers.sql"
        self.psql(fixture.read_text())
        self.contract["dataset_snapshot_id"] = "customers-and-orders-fixture-v1"
        digest = hashlib.sha256(
            fixture.read_bytes()
            + (REPOSITORY_ROOT / "tests/fixtures/bronze/orders.sql").read_bytes()
        ).hexdigest()
        self.psql(
            f"UPDATE audit.source_seed_runs SET snapshot_id = 'customers-and-orders-fixture-v1', manifest_sha256 = '{digest}', row_counts = '{{\"orders\":2,\"customers\":1}}';"
        )

    def customer_bronze(self, run_id="first", store=None):
        with self.connection() as connection:
            return ingest_source(
                connection,
                store or self.store,
                self.contract,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=1,
                table="customers",
            )

    def customer_silver(self, bronze_prefix, run_id="customers"):
        return process_customers(
            self.spark,
            self.store,
            self.contract,
            bronze_prefix=bronze_prefix,
            run_id=run_id,
        )

    def fk_orders(self, bronze_prefix, snapshot, run_id="fk"):
        return process_orders(
            self.spark,
            self.store,
            self.contract,
            bronze_prefix=bronze_prefix,
            run_id=run_id,
            customer_snapshot=snapshot,
        )

    def route_rows(self, manifest, route="valid"):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for number, obj in enumerate(manifest["objects"]):
                if obj["route"] != route:
                    continue
                path = Path(directory) / f"{number}.parquet"
                path.write_bytes(self.store.read(obj["key"]))
                paths.append(str(path))
            return self.spark.read.parquet(*paths).collect() if paths else []

    def add_late_customer(self):
        self.psql("""
            INSERT INTO olist.customers
                (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city,
                 customer_state, source_updated_at, source_operation, source_batch_id)
            VALUES ('customer_b', 'shared_shopper', '00999', 'Rio de Janeiro', 'RJ',
                    '2019-01-01T00:00:00Z', 'INSERT', 'late_parent');
        """)

    def test_full_customer_snapshot_normalizes_location_and_replays(self):
        bronze = self.customer_bronze()["manifest"]
        orders = self.ingest()["manifest"]
        self.assertNotEqual(bronze["prefix"], orders["prefix"])
        self.assertEqual(bronze["snapshot_kind"], "FULL")
        self.assertIsNone(bronze["extract_lower_bound"])
        self.assertEqual(bronze["row_count"], 1)
        self.assertEqual(self.customer_bronze()["status"], "NOOP")
        manifest = self.customer_silver(bronze["prefix"])["manifest"]
        row = self.route_rows(manifest)[0]
        self.assertEqual(
            (
                row["customer_id"],
                row["customer_city"],
                row["customer_state"],
                row["customer_zip_code_prefix"],
            ),
            ("customer_a", "são paulo", "SP", "00123"),
        )
        self.assertEqual(manifest["snapshot_kind"], "FULL")
        self.assertEqual(self.customer_silver(bronze["prefix"])["status"], "NOOP")
        self.assertEqual(verify_silver(self.store, manifest["prefix"]), manifest)

    def test_late_parent_resolves_on_reprocessing_with_a_new_pinned_snapshot(self):
        parent = self.customer_silver(self.customer_bronze()["manifest"]["prefix"])[
            "manifest"
        ]
        bronze = self.ingest()["manifest"]
        first = self.fk_orders(bronze["prefix"], parent["prefix"])["manifest"]
        self.assertEqual(
            (first["counts"]["valid_rows"], first["counts"]["deferred_rows"]), (1, 1)
        )
        pending = self.route_rows(first, "deferred")[0]
        self.assertEqual(
            (pending["order_id"], pending["deferred_rule_ids"]),
            ("order_b", ["missing_customer"]),
        )
        self.assertEqual(
            self.fk_orders(bronze["prefix"], parent["prefix"])["status"], "NOOP"
        )
        self.add_late_customer()
        full = self.customer_bronze("second")["manifest"]
        self.assertEqual(
            full["row_count"], 2
        )  # Includes the 2019 parent, outside lookback.
        self.assertIsNone(full["extract_lower_bound"])
        updated = self.customer_silver(full["prefix"], "customers_second")["manifest"]
        self.assertEqual(
            updated["counts"]["valid_rows"], 2
        )  # Shared shopper is not deduped.
        with self.assertRaisesRegex(BronzeError, "conflict"):
            self.fk_orders(bronze["prefix"], updated["prefix"])
        resolved = self.fk_orders(bronze["prefix"], updated["prefix"], "resolved")[
            "manifest"
        ]
        self.assertEqual(
            (resolved["counts"]["valid_rows"], resolved["counts"]["deferred_rows"]),
            (2, 0),
        )
        self.assertEqual(
            {r["_record_uid"] for r in self.route_rows(resolved)},
            {
                r["_record_uid"]
                for route in ("valid", "deferred")
                for r in self.route_rows(first, route)
            },
        )
        self.assertNotEqual(
            first["request"]["customer_snapshot"]["manifest_sha256"],
            resolved["request"]["customer_snapshot"]["manifest_sha256"],
        )
        self.assertEqual(verify_silver(self.store, first["prefix"]), first)

    def test_customer_partial_publish_is_independent_and_retry_reuses_snapshot(self):
        with self.assertRaises(RuntimeError):
            self.customer_bronze(store=self.failure_store("_SUCCESS"))
        self.ingest()  # An unfinished customer snapshot does not block orders.
        checkpoint = self.state()
        self.psql(
            "UPDATE olist.customers SET customer_city = 'Rio', source_sequence = DEFAULT, source_updated_at = '2019-01-01';"
        )
        original = self.customer_bronze()["manifest"]
        self.assertEqual(self.rows(original)[0]["customer_city"], " São Paulo ")
        latest = self.customer_bronze("second")["manifest"]
        self.assertEqual(self.rows(latest)[0]["customer_city"], "Rio")
        self.assertEqual(self.state(), checkpoint)

    def test_customer_pages_share_a_consistent_snapshot(self):
        self.add_late_customer()
        original_encode = encode
        changed = False

        def update_after_first(value):
            nonlocal changed
            if (
                isinstance(value, dict)
                and value.get("_source_file", "").endswith("part-000001.jsonl")
                and not changed
            ):
                changed = True
                self.psql(
                    "UPDATE olist.customers SET customer_city = 'Changed', source_sequence = DEFAULT WHERE customer_id = 'customer_a';"
                )
            return original_encode(value)

        with patch(
            "olist_platform.bronze.extraction.encode", side_effect=update_after_first
        ):
            bronze = self.customer_bronze()["manifest"]
        self.assertTrue(changed)
        self.assertEqual(
            {r["customer_id"]: r["source_sequence"] for r in self.rows(bronze)},
            {"customer_b": 2, "customer_a": 1},
        )
        self.assertEqual(verify_published(self.store, bronze["prefix"]), bronze)

    def test_reject_takes_precedence_over_missing_parent(self):
        self.psql(
            "UPDATE olist.customers SET customer_unique_id = ''; UPDATE olist.orders SET order_status = 'unknown' WHERE order_id = 'order_a';"
        )
        parent = self.customer_silver(self.customer_bronze()["manifest"]["prefix"])[
            "manifest"
        ]
        self.assertEqual(parent["counts"]["rejected_rows"], 1)
        bronze = self.ingest()["manifest"]
        manifest = self.fk_orders(bronze["prefix"], parent["prefix"])["manifest"]
        self.assertEqual(
            (manifest["counts"]["rejected_rows"], manifest["counts"]["deferred_rows"]),
            (1, 1),
        )
        self.assertEqual(
            self.route_rows(manifest, "rejected")[0]["deferred_rule_ids"], []
        )

    def test_empty_parent_snapshot_defers_all_locally_valid_orders(self):
        self.psql("DELETE FROM olist.customers;")
        parent = self.customer_silver(self.customer_bronze()["manifest"]["prefix"])[
            "manifest"
        ]
        bronze = self.ingest()["manifest"]
        manifest = self.fk_orders(bronze["prefix"], parent["prefix"])["manifest"]
        self.assertEqual(
            (manifest["counts"]["valid_rows"], manifest["counts"]["deferred_rows"]),
            (0, 2),
        )

    def test_dependency_kind_and_corruption_are_rejected(self):
        parent = self.customer_silver(self.customer_bronze()["manifest"]["prefix"])[
            "manifest"
        ]
        bronze = self.ingest()["manifest"]
        local = process_orders(
            self.spark,
            self.store,
            self.contract,
            bronze_prefix=bronze["prefix"],
            run_id="local",
        )["manifest"]
        with self.assertRaisesRegex(BronzeError, "full verified snapshot"):
            self.fk_orders(bronze["prefix"], local["prefix"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=parent["objects"][0]["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.fk_orders(bronze["prefix"], parent["prefix"])

    def test_cli_customer_snapshot_and_orders_fk_flags(self):
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
            options = [
                "--contract",
                str(contract),
                "--endpoint",
                self.endpoint,
                "--bucket",
                self.store.bucket,
            ]

            def cli(module, args):
                result = subprocess.run(
                    [sys.executable, "-m", module] + options + args,
                    env=env,
                    cwd=directory,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)["manifest"]

            bronze = cli(
                "olist_platform.bronze.cli",
                ["--table", "customers", "extract", "--run-id", "cli_customers"],
            )
            parent = cli(
                "olist_platform.silver.cli",
                [
                    "--table",
                    "customers",
                    "process",
                    "--bronze-prefix",
                    bronze["prefix"],
                    "--run-id",
                    "cli_customer_silver",
                ],
            )
            orders = self.ingest()["manifest"]
            manifest = cli(
                "olist_platform.silver.cli",
                [
                    "process",
                    "--bronze-prefix",
                    orders["prefix"],
                    "--run-id",
                    "cli_fk",
                    "--customer-snapshot",
                    parent["prefix"],
                ],
            )
        self.assertEqual(manifest["schema_version"], "orders-silver-v2")
        self.assertEqual(manifest["counts"]["deferred_rows"], 1)
