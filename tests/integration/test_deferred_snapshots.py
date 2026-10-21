from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.silver.batch import process_batch, verify_silver
from olist_platform.silver.orders import spark_session


class DeferredSnapshotsTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        for name in ("customers.sql", "order_children.sql"):
            self.psql((REPOSITORY_ROOT / "tests/fixtures/bronze" / name).read_text())

    def publish(self, table="customers", run_id="first", parent=None, full=True):
        with self.connection() as connection:
            bronze = ingest_source(
                connection,
                self.store,
                self.contract,
                table=table,
                run_id=run_id,
                endpoint=self.endpoint,
                full_snapshot=(table == "orders" and full),
            )["manifest"]
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            table=table,
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
            **(
                {"customer_snapshot": parent}
                if table == "orders"
                else {"order_snapshot": parent}
                if table != "customers"
                else {}
            ),
        )["manifest"]

    def register(self, manifest, table="orders", run_id="register", attempts=3):
        with self.connection() as connection:
            return register_silver(
                self.spark,
                connection,
                self.store,
                self.contract,
                table=table,
                run_id=run_id,
                silver_prefix=manifest["prefix"],
                max_attempts=attempts,
            )

    def resolve(self, parent, table="orders", run_id="retry"):
        with self.connection() as connection:
            return resolve_snapshot(
                self.spark,
                connection,
                self.store,
                self.contract,
                table=table,
                run_id=run_id,
                parent_snapshot=parent["prefix"],
            )

    def test_late_customer_resolves_frozen_orders_with_original_lineage(self):
        parent = self.publish()
        original = self.publish("orders", parent=parent["prefix"])
        self.assertEqual(original["counts"]["deferred_rows"], 1)
        self.assertEqual(self.register(original)["inserted_rows"], 1)
        self.assertEqual(self.register(original)["status"], "NOOP")
        before = self.store.read(f"{original['prefix']}/manifest.json")
        first = self.resolve(parent)
        self.assertEqual(first["routing_counts"]["deferred_rows"], 1)
        self.psql("""INSERT INTO olist.customers (customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state, source_updated_at, source_operation, source_batch_id)
            VALUES ('customer_b','shopper_b','00123','Rio','RJ','2019-01-01T00:00:00Z','INSERT','late');""")
        updated = self.publish(run_id="second")
        with self.assertRaisesRegex(BronzeError, "request conflict"):
            self.resolve(updated)
        result = self.resolve(updated, run_id="resolved")
        self.assertEqual(
            (
                result["deferred_retry_input_rows"],
                result["routing_counts"]["valid_rows"],
            ),
            (1, 1),
        )
        self.assertEqual(self.resolve(updated, run_id="resolved")["status"], "NOOP")
        self.assertEqual(self.store.read(f"{original['prefix']}/manifest.json"), before)
        self.assertEqual(verify_silver(self.store, original["prefix"]), original)
        self.assertEqual(
            self.psql("SELECT order_id || '|' || _run_id FROM raw_staging.orders"),
            "order_b|first",
        )
        self.assertEqual(
            self.psql(
                "SELECT q.record_uid=t._record_uid FROM audit.quarantine_records q JOIN raw_staging.orders t ON q.source_primary_key->>'order_id'=t.order_id"
            ),
            "t",
        )

    def test_late_order_resolves_item_and_payment_money_without_source_reread(self):
        parent = self.publish("orders")
        originals = {
            table: self.publish(table, parent=parent["prefix"])
            for table in ("order_items", "order_payments")
        }
        for table, original in originals.items():
            self.assertEqual(self.register(original, table)["input_rows"], 1)
            self.assertEqual(
                self.resolve(parent, table)["routing_counts"]["deferred_rows"], 1
            )
        self.psql("""INSERT INTO olist.orders (order_id,customer_id,order_status,order_purchase_timestamp,
            order_estimated_delivery_date,source_updated_at,source_operation,source_batch_id)
            VALUES ('late_order','customer_a','approved','2018-01-01','2018-01-10','2019-01-01T00:00:00Z','INSERT','late');""")
        updated = self.publish("orders", run_id="second")
        # Original child source can disappear after registration; retry uses frozen rows.
        self.psql("DELETE FROM olist.order_items; DELETE FROM olist.order_payments")
        for table in originals:
            result = self.resolve(updated, table, "resolved")
            self.assertEqual(result["warehouse_counts"]["warehouse_inserted_rows"], 1)
            money = "price" if table == "order_items" else "payment_value"
            self.assertEqual(
                self.psql(f"SELECT {money}::text FROM raw_staging.{table}"), "20.00"
            )
            self.assertEqual(
                self.psql(
                    f"SELECT retry_status FROM audit.quarantine_records WHERE source_table='{table}'"
                ),
                "RESOLVED",
            )

    def test_delta_wrong_parent_and_corrupt_input_are_rejected(self):
        parent = self.publish("orders")
        original = self.publish("order_items", parent=parent["prefix"])
        self.register(original, "order_items")
        delta = self.publish("orders", run_id="delta", full=False)
        with self.assertRaisesRegex(BronzeError, "full verified parent"):
            self.resolve(delta, "order_items")
        with self.assertRaisesRegex(BronzeError, "full verified parent"):
            self.resolve(self.publish(), "order_items")
        self.client.put_object(
            Bucket=self.store.bucket, Key=parent["objects"][0]["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.resolve(parent, "order_items")
        self.client.put_object(
            Bucket=self.store.bucket,
            Key=next(o["key"] for o in original["objects"] if o["route"] == "deferred"),
            Body=b"corrupt",
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.register(original, "order_items", "corrupt")
        self.assertEqual(
            self.psql("SELECT SUM(attempt_count) FROM audit.quarantine_records"), "0"
        )

    def test_cli_register_and_terminal_expiry_from_another_directory(self):
        parent = self.publish("orders")
        original = self.publish("order_payments", parent=parent["prefix"])
        env = os.environ.copy()
        env.pop("WAREHOUSE_DSN", None)
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
            base = [
                sys.executable,
                "-m",
                "olist_platform.deferred.cli",
                "--contract",
                str(contract),
                "--endpoint",
                self.endpoint,
                "--bucket",
                self.store.bucket,
                "--table",
                "order_payments",
            ]
            for args in (
                [
                    "register",
                    "--silver-prefix",
                    original["prefix"],
                    "--run-id",
                    "cli",
                    "--max-attempts",
                    "1",
                ],
                ["resolve", "--parent-snapshot", parent["prefix"], "--run-id", "cli"],
            ):
                output = subprocess.run(
                    base + args,
                    env=env,
                    cwd=directory,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                self.assertEqual(output.returncode, 0, output.stderr)
                result = json.loads(output.stdout)
        self.assertEqual(result["routing_counts"]["rejected_rows"], 1)
        self.assertEqual(
            self.psql("SELECT retry_status FROM audit.quarantine_records"), "EXPIRED"
        )
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM raw_staging.order_payments"), "0"
        )
