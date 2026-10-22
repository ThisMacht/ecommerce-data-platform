import json
import unittest
from pathlib import Path

from olist_platform.bronze.extraction import configuration
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import (
    source_columns,
    validate_manifest,
    verify_objects,
)
from olist_platform.warehouse.loading import canonical_record
from tests.warehouse_fixture import valid_row


class SellerContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_seller_extraction_is_independent_of_core_pipeline(self):
        store = ObjectStore(None, "bucket")
        config = configuration(
            self.contract, store, "http://localhost:9000", 2, "sellers"
        )
        self.assertEqual(config["pipeline"], "bronze_sellers_v1")
        with self.assertRaisesRegex(BronzeError, "not part of the core"):
            configuration(
                self.contract,
                store,
                "http://localhost:9000",
                2,
                "sellers",
                pipeline_run_id="core",
            )

    def test_sellers_require_full_bronze_and_silver(self):
        manifest = {
            "schema_version": "1.0.0",
            "source": "postgresql.sellers",
            "source_schema": source_columns(self.contract, "sellers"),
            "config": {"contract_sha256": sha256(encode(self.contract))},
            "snapshot_kind": "FULL",
            "extract_lower_bound": None,
        }
        validate_manifest(manifest, self.contract, "sellers")
        for change in (
            {"snapshot_kind": "INCREMENTAL"},
            {"extract_lower_bound": "2020-01-01Z"},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                validate_manifest(manifest | change, self.contract, "sellers")
        with self.assertRaisesRegex(BronzeError, "full snapshot"):
            verify_objects(
                ObjectStore(None, "bucket"),
                {
                    "prefix": "silver/sellers/run_id=one",
                    "format_version": 1,
                    "schema_version": "sellers-silver-v1",
                    "request": {},
                    "request_sha256": sha256(encode({})),
                    "snapshot_kind": "INCREMENTAL",
                },
            )

    def test_seller_hash_preserves_location_nulls_and_business_grain(self):
        row = valid_row("sellers")

        def record(value):
            return canonical_record(value, self.contract, "sellers", "load")

        first = record(row)
        self.assertEqual(
            first["record_hash"],
            record(row | {"source_sequence": 2, "warning_rule_ids": ["location"]})[
                "record_hash"
            ],
        )
        for field, value in (
            ("seller_city", "rio"),
            ("seller_state", "RJ"),
            ("seller_zip_code_prefix", "123"),
            ("seller_id", "other"),
        ):
            with self.subTest(field=field):
                self.assertNotEqual(
                    first["record_hash"], record(row | {field: value})["record_hash"]
                )
        nulls = record(
            row
            | {
                "seller_city": None,
                "seller_state": None,
                "seller_zip_code_prefix": None,
            }
        )
        self.assertIsNone(nulls["seller_city"])
        self.assertNotEqual(
            nulls["record_hash"], record(row | {"seller_city": "null"})["record_hash"]
        )
        for key in (None, "", " a "):
            with self.subTest(key=key), self.assertRaises(BronzeError):
                record(row | {"seller_id": key})
