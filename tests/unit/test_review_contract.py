import json
import unittest
from datetime import datetime
from pathlib import Path

from olist_platform.bronze.extraction import KEYS, configuration, cursor_tuple
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.raw.contracts import source_columns
from olist_platform.silver.batch import validate_manifest
from olist_platform.warehouse.loading import canonical_record
from tests.warehouse_fixture import valid_row


class ReviewContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_composite_string_keys_and_standalone_scope(self):
        self.assertEqual(KEYS["order_reviews"], ("review_id", "order_id"))
        config = configuration(
            self.contract,
            ObjectStore(None, "bucket"),
            "http://localhost:9000",
            2,
            "order_reviews",
        )
        self.assertEqual(config["pipeline"], "bronze_order_reviews_v1")
        cursor = {
            "updated_at": "2020-01-01T00:00:00Z",
            "source_sequence": 1,
            "primary_key": {"review_id": "r", "order_id": "order"},
        }
        self.assertEqual(cursor_tuple(cursor, "order_reviews")[2:], ("r", "order"))
        for mode in ("full", "incremental", "backfill"):
            with (
                self.subTest(mode=mode),
                self.assertRaisesRegex(BronzeError, "standalone"),
            ):
                configuration(
                    self.contract,
                    ObjectStore(None, "bucket"),
                    "http://localhost:9000",
                    2,
                    "order_reviews",
                    pipeline_run_id="owner",
                    pipeline_mode=mode,
                )

    def test_review_manifest_requires_full_input_without_a_lower_bound(self):
        manifest = dict(
            schema_version="1.0.0",
            source="postgresql.order_reviews",
            source_schema=source_columns(self.contract, "order_reviews"),
            config={"contract_sha256": sha256(encode(self.contract))},
            snapshot_kind="FULL",
            extract_lower_bound=None,
        )
        validate_manifest(manifest, self.contract, "order_reviews")
        for changes in (
            {"snapshot_kind": "INCREMENTAL"},
            {"extract_lower_bound": "2020-01-01T00:00:00Z"},
        ):
            with self.subTest(changes=changes), self.assertRaises(BronzeError):
                validate_manifest(manifest | changes, self.contract, "order_reviews")

    def test_warehouse_score_bounds_text_and_key_hash_contract(self):
        row = valid_row("order_reviews", "r")

        def record(changes=None):
            return canonical_record(
                row | (changes or {}), self.contract, "order_reviews", "load"
            )

        first = record()
        self.assertEqual(first["order_id"], "order")
        self.assertEqual(first["review_comment_message"], "Ótimo!\nEntrega rápida.")
        self.assertEqual(
            first["record_hash"], record({"source_sequence": 2})["record_hash"]
        )
        self.assertNotEqual(
            first["record_hash"], record({"order_id": "other"})["record_hash"]
        )
        for changes in (
            {"review_score": 0},
            {"review_score": 6},
            {"review_score": True},
            {"review_score": None},
            {"review_id": ""},
            {"order_id": " "},
            {"review_answer_timestamp": datetime(2020, 1, 1)},
        ):
            with self.subTest(changes=changes), self.assertRaises(BronzeError):
                record(changes)
