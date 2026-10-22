from __future__ import annotations

from unittest.mock import patch

from bronze_case import BronzeFixtureCase

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError
from olist_platform.silver.batch import process_batch, verify_silver
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import load_silver


class SellerIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        self.psql("""
            INSERT INTO olist.sellers
                (seller_id,seller_zip_code_prefix,seller_city,seller_state,
                 source_updated_at,source_operation,source_batch_id)
            VALUES ('a',' 00123 ',' São Paulo ',' sp ','2020-01-04','INSERT','fixture'),
                   ('missing',' ',' ',' ','2020-01-01','INSERT','fixture'),
                   ('','00123','city','SP','2020-01-02','INSERT','fixture');
            UPDATE audit.source_seed_runs SET row_counts = row_counts || '{"sellers":3}'::jsonb;
        """)

    def bronze(self, run_id="sellers", store=None):
        with self.connection() as c:
            return ingest_source(
                c,
                store or self.store,
                self.contract,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=1,
                table="sellers",
            )

    def silver(self, bronze, run_id="sellers"):
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
            table="sellers",
        )

    def load(self, silver, run_id="sellers"):
        with self.connection() as c:
            return load_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                silver_prefix=silver["prefix"],
                run_id=run_id,
                table="sellers",
            )

    def test_full_snapshot_frozen_retry_replay_versions_and_reconciliation(self):
        controls = self.psql(
            "SELECT row_to_json(c) FROM audit.pipeline_control c ORDER BY pipeline_name,source_table"
        )
        with self.assertRaises(RuntimeError):
            self.bronze(store=self.failure_store("_SUCCESS"))
        self.psql(
            "UPDATE olist.sellers SET seller_city='rio', source_sequence=DEFAULT, source_updated_at='2020-01-05' WHERE seller_id='a'"
        )
        bronze = self.bronze()["manifest"]
        self.assertEqual(bronze["snapshot_kind"], "FULL")
        self.assertIsNone(bronze["extract_lower_bound"])
        self.assertEqual(bronze["row_count"], 3)
        self.assertEqual(
            next(r for r in self.rows(bronze) if r["seller_id"] == "a")["seller_city"],
            " São Paulo ",
        )
        self.assertEqual(self.bronze()["status"], "NOOP")
        silver = self.silver(bronze)["manifest"]
        self.assertEqual(
            silver["counts"],
            dict(
                input_rows=3,
                valid_rows=2,
                rejected_rows=1,
                duplicates_rows=0,
                deferred_rows=0,
                warning_rows=1,
            ),
        )
        self.assertEqual(silver["snapshot_kind"], "FULL")
        self.assertEqual(silver["relationship_checks"], "NOT_APPLICABLE")
        self.assertEqual(verify_silver(self.store, silver["prefix"]), silver)
        self.assertEqual(self.silver(bronze)["status"], "NOOP")
        self.assertEqual(self.load(silver)["counts"]["warehouse_inserted_rows"], 2)
        self.assertEqual(
            self.psql(
                "SELECT seller_city || '|' || seller_state || '|' || seller_zip_code_prefix FROM raw_staging.sellers WHERE seller_id='a'"
            ),
            "são paulo|SP|00123",
        )
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.assertEqual(
            self.load(silver, "replay")["counts"]["warehouse_unchanged_rows"], 2
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.sellers WHERE seller_city IS NULL AND seller_state IS NULL AND seller_zip_code_prefix IS NULL"
            ),
            "1",
        )
        self.assertEqual(
            self.psql(
                "SELECT silver_counts->>'rejected_rows' FROM audit.warehouse_load_runs WHERE source_table='sellers' AND run_id='sellers'"
            ),
            "1",
        )
        # A new full snapshot retains old-dated sellers beyond the 48-hour lookback.
        self.psql(
            "UPDATE olist.sellers SET source_updated_at='2020-02-01' WHERE seller_id='a'"
        )
        latest = self.silver(self.bronze("latest")["manifest"], "latest")["manifest"]
        self.assertEqual(latest["counts"]["input_rows"], 3)
        self.assertEqual(
            self.load(latest, "latest")["counts"]["warehouse_updated_rows"], 1
        )
        self.assertEqual(
            self.load(silver, "stale")["counts"]["warehouse_stale_ignored_rows"], 1
        )
        self.assertEqual(
            self.psql(
                "SELECT seller_city FROM raw_staging.sellers WHERE seller_id='a'"
            ),
            "rio",
        )
        # Same source version with another business payload must roll back the whole load.
        self.psql("UPDATE olist.sellers SET seller_city='conflict' WHERE seller_id='a'")
        conflict = self.silver(self.bronze("conflict")["manifest"], "conflict")[
            "manifest"
        ]
        with self.assertRaisesRegex(BronzeError, "Conflicting business payload"):
            self.load(conflict, "conflict")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.warehouse_load_runs WHERE run_id='conflict'"
            ),
            "0",
        )
        self.assertEqual(
            self.psql(
                "SELECT row_to_json(c) FROM audit.pipeline_control c ORDER BY pipeline_name,source_table"
            ),
            controls,
        )

    def test_empty_snapshot_is_nondestructive_and_corruption_cannot_commit(self):
        silver = self.silver(self.bronze()["manifest"])["manifest"]
        obj = silver["objects"][0]
        original = self.store.read(obj["key"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=obj["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.load(silver)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM audit.warehouse_load_runs"), "0"
        )
        self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=original)
        # Exercise a committed load whose client loses the response, then safely resume.
        from olist_platform.warehouse.loading import merge_rows

        def fail_after_commit(*args, **kwargs):
            merge_rows(*args, **kwargs)
            raise RuntimeError("response lost")

        with patch(
            "olist_platform.warehouse.loading.merge_rows", side_effect=fail_after_commit
        ):
            with self.assertRaises(RuntimeError):
                self.load(silver)
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.psql("DELETE FROM olist.sellers")
        empty = self.silver(self.bronze("empty")["manifest"], "empty")["manifest"]
        self.assertEqual(empty["counts"]["input_rows"], 0)
        self.assertEqual(self.load(empty, "empty")["counts"]["load_candidate_rows"], 0)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.sellers"), "2")
