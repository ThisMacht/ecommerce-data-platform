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
from olist_platform.bronze.storage import BronzeError, encode, sha256
from olist_platform.silver.batch import process_batch
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import load_silver


class WarehouseSilverTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        for name in ("customers.sql", "order_children.sql"):
            self.psql((REPOSITORY_ROOT / "tests/fixtures/bronze" / name).read_text())

    def publish(self, table="orders", parent=None, run_id="first"):
        with self.connection() as connection:
            bronze = ingest_source(
                connection,
                self.store,
                self.contract,
                table=table,
                run_id=run_id,
                endpoint=self.endpoint,
                full_snapshot=table == "orders",
            )["manifest"]
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            table=table,
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
            order_snapshot=parent,
        )["manifest"]

    def load(self, manifest, table="orders", run_id="first"):
        with self.connection() as connection:
            return load_silver(
                self.spark,
                connection,
                self.store,
                self.contract,
                silver_prefix=manifest["prefix"],
                run_id=run_id,
                table=table,
            )

    def seal(self, manifest):
        body = encode(manifest)
        for key, content in (
            (f"{manifest['prefix']}/manifest.json", body),
            (
                f"{manifest['prefix']}/_SUCCESS",
                encode({"manifest_sha256": sha256(body)}),
            ),
        ):
            self.client.put_object(Bucket=self.store.bucket, Key=key, Body=content)

    def test_four_tables_from_source_through_silver_to_staging_and_replay(self):
        parents = {}
        for table, expected in (
            ("customers", 1),
            ("orders", 2),
            ("order_items", 2),
            ("order_payments", 2),
        ):
            with self.subTest(table=table):
                manifest = self.publish(
                    table, parents.get("orders") if table.startswith("order_") else None
                )
                parents[table] = manifest["prefix"]
                before = self.state()
                result = self.load(manifest, table)
                self.assertEqual(
                    result["counts"],
                    {
                        "load_candidate_rows": expected,
                        "warehouse_inserted_rows": expected,
                        "warehouse_updated_rows": 0,
                        "warehouse_unchanged_rows": 0,
                        "warehouse_stale_ignored_rows": 0,
                    },
                )
                self.assertEqual(self.load(manifest, table)["status"], "NOOP")
                self.assertEqual(
                    self.psql(f"SELECT COUNT(*) FROM raw_staging.{table}"),
                    str(expected),
                )
                self.assertEqual(self.state(), before)
                audit = json.loads(
                    self.psql(
                        f"SELECT row_to_json(r) FROM audit.warehouse_load_runs r WHERE source_table = '{table}'"
                    )
                )
                self.assertEqual(audit["silver_counts"], manifest["counts"])
                self.assertEqual(
                    audit["request"]["silver_manifest_sha256"], sha256(encode(manifest))
                )
        self.assertEqual(
            self.psql(
                "SET TIME ZONE 'UTC'; SELECT order_purchase_timestamp::text || '|' || order_purchase_date::text FROM raw_staging.orders WHERE order_id = 'order_a'"
            ),
            "2018-01-01 14:00:00+00|2018-01-01",
        )
        self.assertEqual(
            self.psql(
                "SELECT price::text || '|' || freight_value::text FROM raw_staging.order_items WHERE order_id='order_a' AND order_item_id=1"
            ),
            "10.50|1.25",
        )
        self.assertEqual(
            self.psql("SELECT customer_zip_code_prefix FROM raw_staging.customers"),
            "00123",
        )

    def test_corrupt_bytes_physical_schema_and_row_counts_fail_before_commit(self):
        manifest = self.publish()
        obj = manifest["objects"][0]
        original = self.store.read(obj["key"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=obj["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.load(manifest)
        self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=original)
        manifest["counts"]["valid_rows"] += 1
        manifest["counts"]["input_rows"] += 1
        self.seal(manifest)
        with self.assertRaisesRegex(BronzeError, "row/warning count"):
            self.load(manifest)
        manifest["counts"]["valid_rows"] -= 1
        manifest["counts"]["input_rows"] -= 1
        field = next(
            f for f in manifest["output_schema"]["fields"] if f["name"] == "order_id"
        )
        field["type"] = "long"
        manifest["schema_fingerprint"] = sha256(encode(manifest["output_schema"]))
        self.seal(manifest)
        with self.assertRaisesRegex(BronzeError, "physical schema"):
            self.load(manifest)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.warehouse_load_runs"), "0"
        )
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.orders"), "0")

    def test_wrong_dataset_contract_run_conflict_and_empty_valid_routes(self):
        self.psql("UPDATE olist.orders SET order_status='invalid'")
        manifest = self.publish()
        self.assertEqual(manifest["counts"]["valid_rows"], 0)
        with self.assertRaisesRegex(BronzeError, "dataset/version"):
            self.load(manifest, "customers")
        manifest["request"]["contract_sha256"] = "0" * 64
        manifest["request_sha256"] = sha256(encode(manifest["request"]))
        self.seal(manifest)
        with self.assertRaisesRegex(BronzeError, "contract hash"):
            self.load(manifest)
        manifest["request"]["contract_sha256"] = sha256(encode(self.contract))
        manifest["request_sha256"] = sha256(encode(manifest["request"]))
        self.seal(manifest)
        self.assertEqual(self.load(manifest)["counts"]["load_candidate_rows"], 0)
        manifest2 = self.publish(run_id="second")
        with self.assertRaisesRegex(BronzeError, "another request"):
            self.load(manifest2)

    def test_warehouse_cli_uses_process_environment_and_runs_outside_checkout(self):
        manifest = self.publish("customers")
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
            command = [
                sys.executable,
                "-m",
                "olist_platform.warehouse.cli",
                "--contract",
                str(contract),
                "--endpoint",
                self.endpoint,
                "--bucket",
                self.store.bucket,
                "--table",
                "customers",
                "--silver-prefix",
                manifest["prefix"],
                "--run-id",
                "cli",
            ]
            result = subprocess.run(
                command,
                cwd=directory,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                json.loads(result.stdout)["counts"]["warehouse_inserted_rows"], 1
            )
