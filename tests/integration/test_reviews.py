from __future__ import annotations

from unittest.mock import patch

from bronze_case import BronzeFixtureCase
from postgres_case import REPOSITORY_ROOT

from olist_platform.bronze.extraction import ingest_source
from olist_platform.bronze.storage import BronzeError
from olist_platform.deferred.lifecycle import register_silver, resolve_snapshot
from olist_platform.silver.batch import process_batch, verify_silver
from olist_platform.silver.orders import spark_session
from olist_platform.warehouse.loading import _merge_rows, load_silver, merge_rows


class ReviewsIntegrationTest(BronzeFixtureCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)

    def setUp(self):
        super().setUp()
        self.psql(
            (REPOSITORY_ROOT / "tests/fixtures/bronze/order_reviews.sql").read_text()
        )

    def bronze(self, run_id="reviews", store=None, table="order_reviews", full=False):
        with self.connection() as c:
            return ingest_source(
                c,
                store or self.store,
                self.contract,
                table=table,
                run_id=run_id,
                endpoint=self.endpoint,
                page_size=2,
                full_snapshot=full,
            )

    def parent(self, run_id="parent", full=True):
        bronze = self.bronze(run_id, table="orders", full=full)["manifest"]
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            table="orders",
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
        )["manifest"]

    def silver(self, bronze, parent, run_id="reviews"):
        return process_batch(
            self.spark,
            self.store,
            self.contract,
            table="order_reviews",
            bronze_prefix=bronze["prefix"],
            run_id=run_id,
            order_snapshot=parent["prefix"] if parent else None,
        )

    def load(self, silver, run_id="reviews"):
        with self.connection() as c:
            return load_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_reviews",
                silver_prefix=silver["prefix"],
                run_id=run_id,
            )

    def register(self, silver, run_id="register"):
        with self.connection() as c:
            return register_silver(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_reviews",
                run_id=run_id,
                silver_prefix=silver["prefix"],
                max_attempts=2,
            )

    def resolve(self, parent, run_id="retry"):
        with self.connection() as c:
            return resolve_snapshot(
                self.spark,
                c,
                self.store,
                self.contract,
                table="order_reviews",
                run_id=run_id,
                parent_snapshot=parent["prefix"],
            )

    def controls(self):
        return self.psql(
            "SELECT row_to_json(c) FROM audit.pipeline_control c ORDER BY pipeline_name,source_table"
        )

    def test_frozen_full_retry_composite_grain_versions_and_cursor_isolation(self):
        controls = self.controls()
        parent = self.parent()
        with self.assertRaises(RuntimeError):
            self.bronze(store=self.failure_store("_SUCCESS"))
        self.psql(
            "UPDATE olist.order_reviews SET review_score=1,source_sequence=DEFAULT,source_updated_at='2020-02-01' WHERE review_id='r' AND order_id='order_a'"
        )
        bronze = self.bronze()["manifest"]
        self.assertEqual(
            (bronze["row_count"], bronze["page_count"], bronze["snapshot_kind"]),
            (8, 4, "FULL"),
        )
        self.assertIsNone(bronze["extract_lower_bound"])
        self.assertEqual(
            next(
                r["review_score"]
                for r in self.rows(bronze)
                if (r["review_id"], r["order_id"]) == ("r", "order_a")
            ),
            5,
        )
        self.assertEqual(self.bronze()["status"], "NOOP")
        silver = self.silver(bronze, parent)["manifest"]
        self.assertEqual(
            silver["counts"],
            dict(
                input_rows=8,
                valid_rows=4,
                rejected_rows=1,
                deferred_rows=3,
                duplicates_rows=0,
                warning_rows=1,
            ),
        )
        self.assertEqual(silver["relationship_checks"], "ORDER_SNAPSHOT")
        self.assertEqual(silver["unchecked_relationships"], [])
        self.assertEqual(silver["snapshot_kind"], "FULL")
        self.assertEqual(verify_silver(self.store, silver["prefix"]), silver)
        self.assertEqual(self.silver(bronze, parent)["status"], "NOOP")
        self.assertEqual(self.load(silver)["counts"]["warehouse_inserted_rows"], 4)
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_reviews WHERE review_id='r'"
            ),
            "2",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_reviews WHERE order_id='order_a'"
            ),
            "3",
        )
        self.assertEqual(
            self.psql(
                "SELECT review_comment_message=E' Ótimo!\\nCHEGOU rápido. ',review_creation_date='2020-01-01T03:00:00Z',review_answer_timestamp='2020-01-02T13:15:00.123456Z' FROM raw_staging.order_reviews WHERE review_id='r' AND order_id='order_a'"
            ),
            "t|t|t",
        )
        self.assertEqual(
            self.psql(
                "SELECT review_comment_title IS NULL,review_comment_message IS NULL FROM raw_staging.order_reviews WHERE review_id='second'"
            ),
            "t|t",
        )
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.assertEqual(
            self.load(silver, "replay")["counts"]["warehouse_unchanged_rows"], 4
        )
        latest_bronze = self.bronze("latest")["manifest"]
        self.assertEqual(latest_bronze["row_count"], 8)
        self.assertIsNone(latest_bronze["extract_lower_bound"])
        latest = self.silver(latest_bronze, parent, "latest")["manifest"]
        self.assertEqual(
            self.load(latest, "latest")["counts"]["warehouse_updated_rows"], 1
        )
        self.assertEqual(
            self.load(silver, "stale")["counts"]["warehouse_stale_ignored_rows"], 1
        )
        self.psql(
            "UPDATE olist.order_reviews SET review_score=4 WHERE review_id='r' AND order_id='order_a'"
        )
        conflict = self.silver(self.bronze("conflict")["manifest"], parent, "conflict")[
            "manifest"
        ]
        with self.assertRaisesRegex(BronzeError, "Conflicting business payload"):
            self.load(conflict, "conflict")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.warehouse_load_runs WHERE source_table='order_reviews' AND run_id='conflict'"
            ),
            "0",
        )
        self.assertEqual(self.controls(), controls)
        self.assertEqual(
            self.psql(
                "SELECT has_table_privilege('olist_bi_reader','raw_staging.order_reviews','SELECT')"
            ),
            "f",
        )

    def test_parent_contract_corruption_response_loss_and_empty_snapshot(self):
        parent = self.parent()
        bronze = self.bronze()["manifest"]
        with self.assertRaisesRegex(BronzeError, "dependency configuration"):
            self.silver(bronze, None)
        delta = self.parent("delta", full=False)
        with self.assertRaisesRegex(BronzeError, "full verified snapshot"):
            self.silver(bronze, delta)
        silver = self.silver(bronze, parent)["manifest"]
        with self.assertRaisesRegex(BronzeError, "Immutable object conflict"):
            self.silver(bronze, self.parent("other"))
        obj = next(o for o in silver["objects"] if o["route"] == "valid")
        body = self.store.read(obj["key"])
        self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=b"bad")
        with self.assertRaisesRegex(BronzeError, "checksum"):
            self.load(silver)
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.warehouse_load_runs WHERE source_table='order_reviews'"
            ),
            "0",
        )
        self.client.put_object(Bucket=self.store.bucket, Key=obj["key"], Body=body)

        def lost(*args, **kwargs):
            merge_rows(*args, **kwargs)
            raise RuntimeError("response lost")

        with (
            patch("olist_platform.warehouse.loading.merge_rows", side_effect=lost),
            self.assertRaisesRegex(RuntimeError, "response lost"),
        ):
            self.load(silver)
        self.assertEqual(self.load(silver)["status"], "NOOP")
        self.psql("DELETE FROM olist.order_reviews")
        empty = self.silver(self.bronze("empty")["manifest"], parent, "empty")[
            "manifest"
        ]
        self.assertEqual(empty["counts"]["input_rows"], 0)
        self.assertEqual(self.load(empty, "empty")["counts"]["load_candidate_rows"], 0)
        self.assertEqual(
            self.psql("SELECT COUNT(*) FROM raw_staging.order_reviews"), "4"
        )

    def test_deferred_composite_keys_atomic_resolution_expiry_and_frozen_payload(self):
        controls = self.controls()
        parent = self.parent()
        silver = self.silver(self.bronze()["manifest"], parent)["manifest"]
        self.load(silver)
        self.assertEqual(self.register(silver)["inserted_rows"], 3)
        self.assertEqual(self.register(silver)["status"], "NOOP")
        self.assertEqual(self.resolve(parent)["routing_counts"]["deferred_rows"], 3)
        self.psql("""INSERT INTO olist.orders (order_id,customer_id,order_status,order_purchase_timestamp,order_estimated_delivery_date,source_updated_at,source_operation,source_batch_id)
            VALUES ('missing','customer','created','2020-01-01','2020-01-02','2020-01-01','INSERT','late');
            UPDATE olist.order_reviews SET review_score=1,review_comment_message='changed',source_sequence=DEFAULT WHERE review_id='late1';""")
        late = self.parent("late")
        with self.assertRaisesRegex(BronzeError, "request conflict"):
            self.resolve(late)

        def rollback(*args, **kwargs):
            _merge_rows(*args, **kwargs)
            raise RuntimeError("after staging")

        with (
            patch(
                "olist_platform.deferred.lifecycle._merge_rows", side_effect=rollback
            ),
            self.assertRaisesRegex(RuntimeError, "after staging"),
        ):
            self.resolve(late, "resolved")
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_reviews WHERE order_id='missing'"
            ),
            "0",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.quarantine_records WHERE source_table='order_reviews' AND retry_status='PENDING' AND attempt_count=1"
            ),
            "3",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM audit.deferred_resolution_runs WHERE source_table='order_reviews' AND run_id='resolved'"
            ),
            "0",
        )
        result = self.resolve(late, "resolved")
        self.assertEqual(
            result["routing_counts"],
            dict(
                input_rows=3,
                valid_rows=2,
                rejected_rows=1,
                deferred_rows=0,
                duplicates_rows=0,
                warning_rows=0,
            ),
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_reviews WHERE order_id='missing'"
            ),
            "2",
        )
        self.assertEqual(
            self.psql(
                "SELECT review_score,review_comment_message FROM raw_staging.order_reviews WHERE review_id='late1'"
            ),
            "2|frozen",
        )
        self.assertEqual(self.resolve(late, "resolved")["status"], "NOOP")
        self.assertEqual(self.register(silver, "again")["inserted_rows"], 0)
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FILTER(WHERE retry_status='RESOLVED'),COUNT(*) FILTER(WHERE retry_status='EXPIRED') FROM audit.quarantine_records WHERE source_table='order_reviews'"
            ),
            "2|1",
        )
        self.assertEqual(
            self.psql(
                "SELECT COUNT(*) FROM raw_staging.order_reviews WHERE review_id='expire'"
            ),
            "0",
        )
        self.assertEqual(self.controls(), controls)
