from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from botocore.exceptions import ClientError
from bronze_case import BronzeFixtureCase
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import process_orders, verify_silver
from olist_platform.silver.orders import spark_session, transform_orders
from tests.processing.silver_fixture import FIXTURES, publish_bronze


class SilverOrdersIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def process(self, bronze_prefix, run_id="silver", store=None):
        return process_orders(
            self.spark,
            store or self.store,
            self.contract,
            bronze_prefix=bronze_prefix,
            run_id=run_id,
        )

    def test_postgres_bronze_to_parquet_silver_and_exact_replay(self):
        bronze = self.ingest()["manifest"]
        checkpoint = self.state()
        result = self.process(bronze["prefix"])
        manifest = result["manifest"]
        self.assertEqual(result["status"], "PUBLISHED")
        self.assertEqual(
            manifest["counts"],
            {
                "input_rows": 2,
                "valid_rows": 2,
                "rejected_rows": 0,
                "duplicates_rows": 0,
                "deferred_rows": 0,
                "warning_rows": 0,
            },
        )
        self.assertEqual(self.state(), checkpoint)
        self.assertEqual(verify_silver(self.store, manifest["prefix"]), manifest)
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for number, obj in enumerate(manifest["objects"]):
                path = Path(directory) / f"{number}.parquet"
                path.write_bytes(self.store.read(obj["key"]))
                paths.append(str(path))
            frame = self.spark.read.parquet(*paths)
            rows = (
                frame.select(
                    "order_id",
                    F.date_format(
                        "order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"
                    ).alias("utc"),
                )
                .orderBy("order_id")
                .collect()
            )
            self.assertEqual(
                [(r["order_id"], r["utc"]) for r in rows],
                [
                    ("order_a", "2018-01-01 14:00:00"),
                    ("order_b", "2018-01-02 14:00:00"),
                ],
            )
        with patch(
            "olist_platform.silver.orders.transform_orders",
            side_effect=AssertionError("must not recompute"),
        ):
            self.assertEqual(self.process(bronze["prefix"])["status"], "NOOP")

    def test_dirty_fixture_counts_and_resume_after_failure_before_marker(self):
        prefix = publish_bronze(self.store, self.contract)
        expected = json.loads((FIXTURES / "expected.json").read_text())
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.process(prefix, store=self.failure_store("_SUCCESS"))
        output_prefix = "silver/orders/run_id=silver"
        with self.assertRaises(ClientError):
            verify_silver(self.store, output_prefix)
        sealed = self.store.read(f"{output_prefix}/manifest.json")
        with patch(
            "olist_platform.silver.orders.transform_orders",
            side_effect=AssertionError("must reuse sealed files"),
        ):
            manifest = self.process(prefix)["manifest"]
        self.assertEqual(manifest["counts"], expected["counts"])
        self.assertEqual(manifest["rule_violations"], expected["rule_violations"])
        self.assertEqual(self.store.read(f"{output_prefix}/manifest.json"), sealed)
        self.assertEqual(manifest["relationship_checks"], "NOT_IMPLEMENTED")

    def test_partial_parquet_upload_can_recompute_without_publishing_partial_batch(
        self,
    ):
        prefix = publish_bronze(self.store, self.contract)

        class FailSecondPart(ObjectStore):
            count = 0

            def put_immutable(self, key, body):
                if key.endswith(".parquet"):
                    self.count += 1
                    if self.count == 2:
                        raise RuntimeError("injected second upload failure")
                return super().put_immutable(key, body)

        with self.assertRaisesRegex(RuntimeError, "second upload"):
            self.process(prefix, store=FailSecondPart(self.client, self.store.bucket))
        with self.assertRaises(ClientError):
            verify_silver(self.store, "silver/orders/run_id=silver")
        manifest = self.process(prefix)["manifest"]
        self.assertEqual(manifest["counts"]["input_rows"], 13)
        self.assertEqual(
            len(
                {
                    obj["key"].split("/attempt=")[1].split("/")[0]
                    for obj in manifest["objects"]
                }
            ),
            1,
        )

    def test_corrupt_input_and_output_are_rejected(self):
        bronze = self.ingest()["manifest"]
        manifest = self.process(bronze["prefix"])["manifest"]
        key = manifest["objects"][0]["key"]
        self.client.put_object(
            Bucket=self.store.bucket, Key=key, Body=b"corrupted parquet"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            verify_silver(self.store, manifest["prefix"])
        with self.assertRaises(BronzeError):
            self.process(bronze["prefix"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=bronze["objects"][0]["key"], Body=b"{}\n"
        )
        with self.assertRaises(BronzeError):
            self.process(bronze["prefix"], run_id="corrupt_input")

    def test_run_id_cannot_be_reused_for_another_bronze_batch(self):
        first = self.ingest()["manifest"]
        manifest = self.process(first["prefix"])["manifest"]
        second = self.ingest("second")["manifest"]
        with self.assertRaisesRegex(BronzeError, "conflict"):
            self.process(second["prefix"])
        self.assertEqual(verify_silver(self.store, manifest["prefix"]), manifest)

    def test_request_binds_exact_bronze_manifest_bytes(self):
        bronze = self.ingest()["manifest"]
        body = json.dumps(bronze, indent=2).encode()
        self.client.put_object(
            Bucket=self.store.bucket, Key=f"{bronze['prefix']}/manifest.json", Body=body
        )
        self.client.put_object(
            Bucket=self.store.bucket,
            Key=f"{bronze['prefix']}/_SUCCESS",
            Body=encode({"manifest_sha256": sha256(body)}),
        )
        manifest = self.process(bronze["prefix"])["manifest"]
        self.assertEqual(manifest["request"]["bronze_manifest_sha256"], sha256(body))

    def test_empty_bronze_publishes_a_typed_empty_batch(self):
        self.psql("DELETE FROM olist.orders;")
        bronze = self.ingest()["manifest"]
        manifest = self.process(bronze["prefix"])["manifest"]
        self.assertEqual(manifest["counts"]["input_rows"], 0)
        self.assertEqual(manifest["objects"], [])
        self.assertTrue(manifest["output_schema"]["fields"])

    def test_concurrent_identical_requests_share_one_sealed_publication(self):
        bronze = self.ingest()["manifest"]
        ready = Barrier(2)

        def start_together(frame):
            ready.wait(timeout=30)
            return transform_orders(frame)

        with patch(
            "olist_platform.silver.orders.transform_orders", side_effect=start_together
        ):
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = list(
                    workers.map(lambda _: self.process(bronze["prefix"]), range(2))
                )
        self.assertEqual(results[0]["manifest"], results[1]["manifest"])
        self.assertEqual(
            verify_silver(self.store, results[0]["manifest"]["prefix"]),
            results[0]["manifest"],
        )

    def test_verify_cli_uses_s3_environment_without_spark_startup(self):
        bronze = self.ingest()["manifest"]
        manifest = self.process(bronze["prefix"])["manifest"]
        env = os.environ.copy()
        env.update(
            AWS_ACCESS_KEY_ID="bronze_test_admin",
            AWS_SECRET_ACCESS_KEY="isolated_bronze_password",
        )
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "olist_platform.silver.cli",
                    "--endpoint",
                    self.endpoint,
                    "--bucket",
                    self.store.bucket,
                    "verify",
                    "--prefix",
                    manifest["prefix"],
                ],
                env=env,
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "VERIFIED")
