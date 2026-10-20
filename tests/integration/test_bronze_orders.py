from __future__ import annotations

import json
import os
import subprocess
import sys

import psycopg
from botocore.exceptions import ClientError
from bronze_case import BronzeFixtureCase

from olist_platform.bronze.orders import LOCK
from olist_platform.bronze.storage import (
    BronzeError,
    encode,
    verify_published,
)


class BronzeOrdersIntegrationTest(BronzeFixtureCase):
    def test_initial_load_same_timestamp_pages_and_exact_replay(self):
        result = self.ingest()
        manifest = result["manifest"]
        self.assertEqual(result["status"], "PUBLISHED")
        self.assertEqual((manifest["row_count"], manifest["page_count"]), (2, 2))
        rows = self.rows(manifest)
        self.assertEqual([row["order_id"] for row in rows], ["order_a", "order_b"])
        self.assertEqual([row["source_sequence"] for row in rows], [1, 2])
        self.assertEqual(rows[0]["order_purchase_timestamp"], "2018-01-01T12:00:00")
        self.assertIsNone(rows[0]["order_approved_at"])
        self.assertEqual(len({r["_record_uid"] for r in rows}), 2)
        self.assertEqual(verify_published(self.store, manifest["prefix"]), manifest)
        before = self.state()
        object_before = self.client.head_object(
            Bucket=self.store.bucket, Key=manifest["objects"][0]["key"]
        )
        self.assertEqual(self.ingest()["status"], "NOOP")
        self.assertEqual(self.state(), before)
        self.assertEqual(
            self.client.head_object(
                Bucket=self.store.bucket, Key=manifest["objects"][0]["key"]
            )["LastModified"],
            object_before["LastModified"],
        )

    def test_incremental_lookback_and_backdated_versions(self):
        initial = self.ingest()["manifest"]
        self.psql("""
            UPDATE olist.orders SET order_status = 'delivered', source_sequence = DEFAULT,
                source_updated_at = '2020-01-03T12:00:00Z', source_operation = 'UPDATE'
            WHERE order_id = 'order_a';
        """)
        current = self.ingest("second")["manifest"]
        rows = self.rows(current)
        self.assertEqual([r["order_id"] for r in rows], ["order_a", "order_b"])
        self.assertEqual(rows[0]["source_sequence"], 3)
        self.assertEqual(rows[0]["order_status"], "delivered")
        self.assertEqual(current["watermark_candidate"], initial["watermark_candidate"])
        self.assertEqual(current["extract_lower_bound"], "2020-01-02T00:00:00+00:00")
        self.psql("""
            UPDATE olist.orders SET source_sequence = DEFAULT,
                source_updated_at = '2020-01-01T00:00:00Z' WHERE order_id = 'order_a';
        """)
        outside = self.ingest("third")["manifest"]
        self.assertEqual([r["order_id"] for r in self.rows(outside)], ["order_b"])

    def test_new_rows_and_same_timestamp_update_are_extracted(self):
        self.ingest()
        self.psql("""
            UPDATE olist.orders SET source_sequence = DEFAULT, order_status = 'shipped'
                WHERE order_id = 'order_a';
            INSERT INTO olist.orders
                (order_id, customer_id, order_status, order_purchase_timestamp,
                 order_estimated_delivery_date, source_updated_at, source_operation, source_batch_id)
            VALUES ('order_c', 'customer_c', 'created', '2018-01-03', '2018-01-12',
                    '2020-01-05', 'INSERT', 'next');
        """)
        manifest = self.ingest("next")["manifest"]
        self.assertEqual(
            [r["order_id"] for r in self.rows(manifest)],
            ["order_b", "order_a", "order_c"],
        )
        self.assertEqual(manifest["watermark_candidate"]["source_sequence"], 4)

    def test_partial_upload_is_invisible_and_retry_reuses_sealed_snapshot(self):
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.ingest(store=self.failure_store("part-000002.jsonl"))
        state = self.state()
        manifest = json.loads(
            self.psql("SELECT manifest FROM audit.bronze_orders_runs;")
        )
        with self.assertRaises(ClientError):
            verify_published(self.store, manifest["prefix"])
        self.psql(
            "UPDATE olist.orders SET source_sequence = DEFAULT, order_status = 'canceled' WHERE order_id = 'order_a';"
        )
        with self.assertRaisesRegex(BronzeError, "Resume unfinished"):
            self.ingest("other")
        self.assertEqual(self.state(), state)
        resumed = self.ingest()["manifest"]
        self.assertEqual(resumed, manifest)
        self.assertEqual(self.rows(resumed)[0]["order_status"], "created")
        self.assertEqual(
            self.rows(self.ingest("later")["manifest"])[-1]["order_status"], "canceled"
        )

    def test_failure_before_marker_does_not_advance_checkpoint(self):
        with self.assertRaises(RuntimeError):
            self.ingest(store=self.failure_store("_SUCCESS"))
        self.assertIn('"cursor_version":0', self.state().replace(" ", ""))
        self.assertEqual(self.ingest()["status"], "PUBLISHED")

    def test_checkpoint_failure_after_publish_rolls_back_then_recovers(self):
        self.psql("""
            CREATE FUNCTION audit.fail_bronze_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END $$;
            CREATE TRIGGER fail_commit BEFORE UPDATE ON audit.bronze_orders_control
                FOR EACH ROW EXECUTE FUNCTION audit.fail_bronze_commit();
        """)
        with self.assertRaises(psycopg.Error):
            self.ingest()
        manifest = json.loads(
            self.psql("SELECT manifest FROM audit.bronze_orders_runs;")
        )
        self.assertEqual(verify_published(self.store, manifest["prefix"]), manifest)
        self.assertEqual(
            self.psql("SELECT status FROM audit.bronze_orders_runs;"), "PREPARED"
        )
        self.assertIn('"cursor_version":0', self.state().replace(" ", ""))
        self.psql("DROP TRIGGER fail_commit ON audit.bronze_orders_control;")
        self.assertEqual(self.ingest()["status"], "PUBLISHED")
        self.assertEqual(self.ingest()["status"], "NOOP")

    def test_checkpoint_compare_and_swap_conflict_is_detected(self):
        with self.assertRaises(RuntimeError):
            self.ingest(store=self.failure_store("_SUCCESS"))
        self.psql("UPDATE audit.bronze_orders_control SET cursor_version = 99;")
        with self.assertRaisesRegex(BronzeError, "compare-and-swap"):
            self.ingest()
        self.assertEqual(
            self.psql("SELECT status FROM audit.bronze_orders_runs;"), "PREPARED"
        )
        self.assertEqual(
            self.psql("SELECT cursor_version FROM audit.bronze_orders_control;"), "99"
        )

    def test_immutable_conflicts_and_corruption_are_rejected(self):
        manifest = self.ingest()["manifest"]
        key = manifest["objects"][0]["key"]
        with self.assertRaisesRegex(BronzeError, "Immutable object conflict"):
            self.store.put_immutable(key, b"different\n")
        self.client.put_object(Bucket=self.store.bucket, Key=key, Body=b"{}\n")
        with self.assertRaisesRegex(BronzeError, "checksum"):
            verify_published(self.store, manifest["prefix"])
        with self.assertRaises(BronzeError):
            self.ingest()

    def test_empty_table_publishes_zero_objects_and_keeps_null_cursor(self):
        self.psql("DELETE FROM olist.orders;")
        manifest = self.ingest()["manifest"]
        self.assertEqual((manifest["row_count"], manifest["objects"]), (0, []))
        self.assertIsNone(manifest["run_upper_cursor"])
        self.assertIsNone(manifest["watermark_candidate"])
        self.assertEqual(verify_published(self.store, manifest["prefix"]), manifest)

    def test_unseeded_source_and_invalid_run_ids_do_not_create_state(self):
        self.psql("DELETE FROM audit.source_seed_runs;")
        with self.assertRaisesRegex(BronzeError, "seeded"):
            self.ingest()
        for run_id in ("../unsafe", "", "x" * 97):
            with self.subTest(run_id=run_id), self.assertRaises(BronzeError):
                self.ingest(run_id)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.bronze_orders_runs;"), "0"
        )

    def test_prepare_failure_rolls_back_pages_and_publishes_nothing(self):
        from unittest.mock import patch

        original_encode = encode

        def fail_second_page(value):
            if isinstance(value, dict) and value.get("_source_file", "").endswith(
                "part-000002.jsonl"
            ):
                raise RuntimeError("injected serialization failure")
            return original_encode(value)

        with patch("olist_platform.bronze.orders.encode", side_effect=fail_second_page):
            with self.assertRaisesRegex(RuntimeError, "serialization failure"):
                self.ingest()
        for table in ("runs", "parts", "control"):
            self.assertEqual(
                self.psql(f"SELECT COUNT(*) FROM audit.bronze_orders_{table};"), "0"
            )
        self.assertEqual(
            self.client.list_objects_v2(Bucket=self.store.bucket)["KeyCount"], 0
        )
        self.assertEqual(self.ingest()["status"], "PUBLISHED")

    def test_configuration_drift_schema_drift_and_concurrent_guard(self):
        with self.connection() as held:
            held.execute("SELECT pg_advisory_lock(hashtext(%s))", (LOCK,))
            with self.assertRaisesRegex(BronzeError, "active"):
                self.ingest()
        self.ingest()
        with self.assertRaisesRegex(BronzeError, "different configuration"):
            self.ingest(page_size=2)
        self.psql("ALTER TABLE olist.orders ADD COLUMN unexpected text;")
        with self.assertRaisesRegex(BronzeError, "schema"):
            self.ingest("drift")
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.bronze_orders_runs;"), "1"
        )

    def test_consistent_snapshot_ignores_updates_between_pages(self):
        original_encode = encode
        from unittest.mock import patch

        changed = False

        def change_after_first_page(value):
            nonlocal changed
            if (
                isinstance(value, dict)
                and value.get("_source_file", "").endswith("part-000001.jsonl")
                and not changed
            ):
                changed = True
                self.psql(
                    "UPDATE olist.orders SET source_sequence = DEFAULT, order_status = 'canceled' WHERE order_id = 'order_b';"
                )
            return original_encode(value)

        with patch(
            "olist_platform.bronze.orders.encode", side_effect=change_after_first_page
        ):
            manifest = self.ingest()["manifest"]
        self.assertTrue(changed)
        self.assertEqual(
            [(r["order_id"], r["source_sequence"]) for r in self.rows(manifest)],
            [("order_a", 1), ("order_b", 2)],
        )
        self.assertEqual(
            self.rows(self.ingest("next")["manifest"])[-1]["source_sequence"], 3
        )

    def test_cli_publishes_and_verifies_using_environment(self):
        env = os.environ.copy()
        env.update(
            PGHOST="127.0.0.1",
            PGPORT=str(self.port),
            PGDATABASE=self.database,
            PGUSER="olist_test_loader",
            PGPASSWORD="isolated_test_loader",
            AWS_ACCESS_KEY_ID="bronze_test_admin",
            AWS_SECRET_ACCESS_KEY="isolated_bronze_password",
        )
        env.pop("BRONZE_SOURCE_DSN", None)
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            contract = Path(directory) / "contract.json"
            contract.write_bytes(encode(self.contract))
            arguments = [
                sys.executable,
                "-m",
                "olist_platform.bronze.cli",
                "--contract",
                str(contract),
                "--endpoint",
                self.endpoint,
                "--bucket",
                self.store.bucket,
            ]
            result = subprocess.run(
                arguments + ["extract", "--run-id", "cli"],
                env=env,
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads(result.stdout)["manifest"]
            result = subprocess.run(
                arguments + ["verify", "--prefix", manifest["prefix"]],
                env=env,
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], "VERIFIED")
