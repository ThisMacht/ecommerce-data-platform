from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import psycopg
from botocore.exceptions import ClientError
from pipeline_case import CorePipelineCase
from postgres_case import REPOSITORY_ROOT
from psycopg.errors import LockNotAvailable
from psycopg.types.json import Jsonb

from olist_platform.bronze.storage import BronzeError
from olist_platform.pipeline.cli import configuration, finish
from olist_platform.pipeline.control import TASKS, begin, evidence, task


class PipelineGateTest(CorePipelineCase):
    def test_full_receipts_corrupt_marker_failure_resume_and_real_dbt_commit(self):
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
            published = self.process(c, "gate", 1, mode="full")
            snapshots = {t: s["prefix"] for t, (b, s) in published.items()}
            receipts = evidence(c, self.store, self.contract, "gate")
            self.assertEqual(
                len({x["bronze"]["source_snapshot"] for x in receipts.values()}), 1
            )
            with psycopg.connect(
                host="127.0.0.1",
                port=self.port,
                dbname=self.database,
                user="olist_test_admin",
                password="isolated_test_admin",
                autocommit=True,
            ) as auditor:
                for parent in ("products", "sellers"):
                    with auditor.transaction(force_rollback=True):
                        auditor.execute(
                            "DELETE FROM audit.warehouse_load_runs WHERE source_table=%s AND run_id='gate'",
                            (parent,),
                        )
                        with self.assertRaisesRegex(BronzeError, "warehouse receipt"):
                            evidence(auditor, self.store, self.contract, "gate")
                with auditor.transaction(force_rollback=True):
                    wrong = dict(receipts["order_items"]["resolution"]["request"])
                    wrong["product_snapshot"] = wrong["seller_snapshot"]
                    auditor.execute(
                        "UPDATE audit.deferred_resolution_runs SET request=%s WHERE source_table='order_items' AND run_id='gate'",
                        (Jsonb(wrong),),
                    )
                    with self.assertRaisesRegex(BronzeError, "product/seller parents"):
                        evidence(auditor, self.store, self.contract, "gate")
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
            self.assertEqual(completed["dbt"]["node_count"], 122)
            self.assertEqual(
                self.psql("SELECT MIN(cursor_version) FROM audit.pipeline_control"), "1"
            )
            self.assertEqual(
                self.psql("SELECT COUNT(*) FROM audit.pipeline_dataset_runs"), "6"
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
                self.psql("SELECT COUNT(*) FROM analytics.fct_orders"), "2"
            )
            with self.assertRaisesRegex(BronzeError, "configuration"):
                begin(c, "gate", request | {"changed": True})
