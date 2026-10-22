from __future__ import annotations

from unittest.mock import patch

from bronze_case import BronzeFixtureCase

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError
from olist_platform.silver.batch import process_batch, verify_silver
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import load_silver, merge_rows


class GeolocationIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        self.psql("""INSERT INTO olist.geolocation
            (geolocation_row_id,geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,
             geolocation_city,geolocation_state,source_updated_at,source_operation,source_batch_id)
            VALUES (1,' 00123 ',-23.500000000000000001,-46.500000000000000001,' São Paulo ',' sp ','2020-01-04','INSERT','fixture'),
                   (2,'00123',-24,-47,' ',' ','2020-01-01','INSERT','fixture'),
                   (3,'00123',91,-47,'city','SP','2020-01-02','INSERT','fixture'),
                   (4,'123',-24,-47,'city','SP','2020-01-02','INSERT','fixture'),
                   (5,'99999',45,120,'Other','xx','2020-01-01','INSERT','fixture'),
                   (0,'00123',-24,-47,'city','SP','2020-01-02','INSERT','fixture');""")

    def bronze(self, run_id="geolocation", store=None):
        with self.connection() as c:
            return ingest_source(
                c,
                store or self.store,
                self.contract,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=2,
                table="geolocation",
            )

    def silver(self, bronze, run_id="geolocation"):
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
            table="geolocation",
        )

    def load(self, silver, run_id="geolocation"):
        with self.connection() as c:
            return load_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                silver_prefix=silver["prefix"],
                run_id=run_id,
                table="geolocation",
            )

    def test_frozen_full_snapshot_replay_versions_conflict_and_cursor_isolation(self):
        controls = self.psql(
            "SELECT row_to_json(c) FROM audit.pipeline_control c ORDER BY pipeline_name,source_table"
        )
        with self.assertRaises(RuntimeError):
            self.bronze(store=self.failure_store("_SUCCESS"))
        self.psql(
            "UPDATE olist.geolocation SET geolocation_city='rio',source_sequence=DEFAULT,source_updated_at='2020-01-05' WHERE geolocation_row_id=1"
        )
        bronze = self.bronze()["manifest"]
        self.assertEqual(bronze["snapshot_kind"], "FULL")
        self.assertEqual(bronze["row_count"], 6)
        self.assertIsNone(bronze["extract_lower_bound"])
        self.assertEqual(
            next(r for r in self.rows(bronze) if r["geolocation_row_id"] == 1)[
                "geolocation_city"
            ],
            " São Paulo ",
        )
        self.assertEqual(self.bronze()["status"], "NOOP")
        silver = self.silver(bronze)["manifest"]
        self.assertEqual(
            silver["counts"],
            dict(
                input_rows=6,
                valid_rows=3,
                rejected_rows=3,
                duplicates_rows=0,
                deferred_rows=0,
                warning_rows=2,
            ),
        )
        self.assertEqual(silver["snapshot_kind"], "FULL")
        self.assertEqual(silver["relationship_checks"], "NOT_APPLICABLE")
        self.assertEqual(verify_silver(self.store, silver["prefix"]), silver)
        self.assertEqual(self.silver(bronze)["status"], "NOOP")
        self.assertEqual(self.load(silver)["counts"]["warehouse_inserted_rows"], 3)
        self.assertEqual(
            self.psql(
                "SELECT geolocation_city,geolocation_state,geolocation_zip_code_prefix,geolocation_lat,geolocation_lng FROM raw_staging.geolocation WHERE geolocation_row_id=1"
            ),
            "são paulo|SP|00123|-23.500000000000000001|-46.500000000000000001",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.geolocation WHERE geolocation_zip_code_prefix='00123'"
            ),
            "2",
        )
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.assertEqual(
            self.load(silver, "replay")["counts"]["warehouse_unchanged_rows"], 3
        )
        self.psql(
            "UPDATE olist.geolocation SET source_updated_at='2020-02-01' WHERE geolocation_row_id=1"
        )
        latest = self.silver(self.bronze("latest")["manifest"], "latest")["manifest"]
        self.assertEqual(latest["counts"]["input_rows"], 6)
        self.assertEqual(
            self.load(latest, "latest")["counts"]["warehouse_updated_rows"], 1
        )
        self.assertEqual(
            self.load(silver, "stale")["counts"]["warehouse_stale_ignored_rows"], 1
        )
        self.assertEqual(
            self.psql(
                "SELECT geolocation_city FROM raw_staging.geolocation WHERE geolocation_row_id=1"
            ),
            "rio",
        )
        self.psql(
            "UPDATE olist.geolocation SET geolocation_lat=-23.1 WHERE geolocation_row_id=1"
        )
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

    def test_corruption_response_loss_and_empty_snapshot_are_nondestructive(self):
        silver = self.silver(self.bronze()["manifest"])["manifest"]
        obj = next(o for o in silver["objects"] if o["route"] == "valid")
        body = self.store.read(obj["key"])
        self.client.put_object(
            Bucket=self.store.bucket, Key=obj["key"], Body=b"corrupt"
        )
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.load(silver)
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.warehouse_load_runs WHERE source_table='geolocation'"
            ),
            "0",
        )
        self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=body)

        def response_lost(*args, **kwargs):
            merge_rows(*args, **kwargs)
            raise RuntimeError("response lost")

        with patch(
            "olist_platform.warehouse.loading.merge_rows", side_effect=response_lost
        ):
            with self.assertRaisesRegex(RuntimeError, "response lost"):
                self.load(silver)
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.psql("DELETE FROM olist.geolocation")
        empty = self.silver(self.bronze("empty")["manifest"], "empty")["manifest"]
        self.assertEqual(empty["counts"]["input_rows"], 0)
        self.assertEqual(self.load(empty, "empty")["counts"]["load_candidate_rows"], 0)
        self.assertEqual(self.psql("SELECT COUNT(*) FROM raw_staging.geolocation"), "3")
