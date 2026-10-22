from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT
from psycopg.errors import LockNotAvailable

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.pipeline.cli import configuration, finish
from olist_platform.pipeline.control import TABLES, TASKS, begin, task
from olist_platform.silver.batch import process_batch
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import load_silver


class PipelineGateTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def test_full_receipts_corrupt_marker_failure_resume_and_real_dbt_commit(self):
        for name in ("customers.sql", "order_children.sql"):
            self.psql((REPOSITORY_ROOT / "tests/fixtures/bronze" / name).read_text())
        env = {
            "PGHOST": "127.0.0.1",
            "PGPORT": str(self.port),
            "PGDATABASE": self.database,
            "PGUSER": "olist_test_loader",
            "PGPASSWORD": "isolated_test_loader",
            "DBT_ENV_SECRET_PASSWORD": "isolated_test_loader",
            "DBT_READER_USER": "olist_test_reader",
            "BRONZE_S3_ENDPOINT": self.endpoint,
            "BRONZE_S3_BUCKET": self.store.bucket,
        }
        with (
            patch.dict(os.environ, env),
            tempfile.TemporaryDirectory() as directory,
            self.connection() as c,
        ):
            request = configuration(REPOSITORY_ROOT, self.contract)
            self.assertEqual(begin(c, "gate", request)["attempt"], 1)
            snapshots = {}
            for table in TABLES:
                name = f"{table}.bronze.extract"
                task(c, "gate", 1, name, "RUNNING")
                bronze = ingest_source(
                    c,
                    self.store,
                    self.contract,
                    table=table,
                    run_id="gate",
                    endpoint=self.endpoint,
                    full_snapshot=table != "customers",
                )["manifest"]
                task(c, "gate", 1, name, "SUCCESS")
                dependencies = (
                    {"customer_snapshot": snapshots["customers"]}
                    if table == "orders"
                    else {"order_snapshot": snapshots["orders"]}
                    if table != "customers"
                    else {}
                )
                name = f"{table}.silver.process"
                task(c, "gate", 1, name, "RUNNING")
                silver = process_batch(
                    self.spark,
                    self.store,
                    self.contract,
                    table=table,
                    run_id="gate",
                    bronze_prefix=bronze["prefix"],
                    **dependencies,
                )["manifest"]
                snapshots[table] = silver["prefix"]
                task(c, "gate", 1, name, "SUCCESS")
                name = f"{table}.warehouse.load"
                task(c, "gate", 1, name, "RUNNING")
                load_silver(
                    self.spark,
                    c,
                    self.store,
                    self.contract,
                    table=table,
                    run_id="gate",
                    silver_prefix=silver["prefix"],
                )
                task(c, "gate", 1, name, "SUCCESS")
                if table != "customers":
                    name = f"{table}.deferred.register"
                    task(c, "gate", 1, name, "RUNNING")
                    register_silver(
                        self.spark,
                        c,
                        self.store,
                        self.contract,
                        table=table,
                        run_id="gate",
                        silver_prefix=silver["prefix"],
                    )
                    task(c, "gate", 1, name, "SUCCESS")
                    name = f"{table}.deferred.resolve"
                    task(c, "gate", 1, name, "RUNNING")
                    resolve_snapshot(
                        self.spark,
                        c,
                        self.store,
                        self.contract,
                        table=table,
                        run_id="gate",
                        parent_snapshot=snapshots[
                            "customers" if table == "orders" else "orders"
                        ],
                    )
                    task(c, "gate", 1, name, "SUCCESS")
            marker = f"{snapshots['orders']}/_SUCCESS"
            body = self.store.read(marker)
            self.client.delete_object(Bucket=self.store.bucket, Key=marker)
            with self.assertRaises(ClientError):
                finish(
                    c,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    "gate",
                    1,
                    request,
                    Path(directory),
                )
            self.assertEqual(
                self.psql("SELECT status FROM audit.pipeline_runs WHERE run_id='gate'"),
                "FAILED",
            )
            self.assertEqual(
                self.psql("SELECT MAX(cursor_version) FROM audit.pipeline_control"), "0"
            )
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "0"
            )
            self.client.put_object(Bucket=self.store.bucket, Key=marker, Body=body)
            self.assertEqual(begin(c, "gate", request)["attempt"], 2)
            # Retry reuses immutable data-plane receipts; stage calls are replay-safe.
            for name in TASKS:
                if name != "dbt.build":
                    task(c, "gate", 2, name, "RUNNING")
                    task(c, "gate", 2, name, "SUCCESS")

            def failed_dbt(*args, **kwargs):
                # The new standalone source must stay stable through the core dbt gate.
                with self.connection() as writer:
                    writer.execute("SET lock_timeout='100ms'")
                    for table in ("products", "sellers"):
                        with (
                            self.subTest(table=table),
                            self.assertRaises(LockNotAvailable),
                        ):
                            writer.execute(
                                f"UPDATE raw_staging.{table} SET source_sequence=source_sequence"
                            )
                raise subprocess.CalledProcessError(1, "dbt")

            with patch(
                "olist_platform.pipeline.cli.subprocess.run",
                side_effect=failed_dbt,
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    finish(
                        c,
                        self.store,
                        self.contract,
                        REPOSITORY_ROOT,
                        "gate",
                        2,
                        request,
                        Path(directory),
                    )
            self.assertEqual(
                self.psql("SELECT MAX(cursor_version) FROM audit.pipeline_control"), "0"
            )
            self.assertEqual(begin(c, "gate", request)["attempt"], 3)
            for name in TASKS:
                if name != "dbt.build":
                    task(c, "gate", 3, name, "RUNNING")
                    task(c, "gate", 3, name, "SUCCESS")
            completed = finish(
                c,
                self.store,
                self.contract,
                REPOSITORY_ROOT,
                "gate",
                3,
                request,
                Path(directory),
            )
            self.assertEqual(completed["status"], "SUCCESS")
            self.assertEqual(completed["dbt"]["node_count"], 61)
            self.assertEqual(
                self.psql("SELECT MIN(cursor_version) FROM audit.pipeline_control"), "1"
            )
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "4"
            )
            for values in completed["datasets"].values():
                self.assertEqual(
                    values["source_rows_extracted"], values["bronze_rows_read"]
                )
                self.assertEqual(
                    values["processing_input_rows"],
                    values["bronze_rows_read"] + values["deferred_retry_input_rows"],
                )
            self.assertEqual(
                finish(
                    c,
                    self.store,
                    self.contract,
                    REPOSITORY_ROOT,
                    "gate",
                    3,
                    request,
                    Path(directory),
                )["status"],
                "NOOP",
            )
            self.assertEqual(
                self.psql("SELECT MAX(cursor_version) FROM audit.pipeline_control"), "1"
            )
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "1"
            )
            with self.assertRaisesRegex(BronzeError, "configuration"):
                begin(c, "gate", request | {"changed": True})
