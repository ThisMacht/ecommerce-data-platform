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


class ProductContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_product_extraction_is_independent_of_core_pipeline(self):
        store = ObjectStore(None, "bucket")
        config = configuration(
            self.contract, store, "http://localhost:9000", 2, "products"
        )
        self.assertEqual(config["pipeline"], "bronze_products_v1")
        with self.assertRaisesRegex(BronzeError, "not part of the core"):
            configuration(
                self.contract,
                store,
                "http://localhost:9000",
                2,
                "products",
                pipeline_run_id="core",
            )

    def test_products_require_full_bronze_and_silver(self):
        manifest = {
            "schema_version": "1.0.0",
            "source": "postgresql.products",
            "source_schema": source_columns(self.contract, "products"),
            "config": {"contract_sha256": sha256(encode(self.contract))},
            "snapshot_kind": "FULL",
            "extract_lower_bound": None,
        }
        validate_manifest(manifest, self.contract, "products")
        for change in (
            {"snapshot_kind": "INCREMENTAL"},
            {"extract_lower_bound": "2020-01-01Z"},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                validate_manifest(manifest | change, self.contract, "products")
        with self.assertRaisesRegex(BronzeError, "full snapshot"):
            verify_objects(
                ObjectStore(None, "bucket"),
                {
                    "prefix": "silver/products/run_id=one",
                    "format_version": 1,
                    "schema_version": "products-silver-v1",
                    "request": {},
                    "request_sha256": sha256(encode({})),
                    "snapshot_kind": "INCREMENTAL",
                },
            )

    def test_product_hash_preserves_nulls_and_rejects_invalid_measurements(self):
        row = valid_row("products")

        def record(values):
            return canonical_record(values, self.contract, "products", "load")

        first = record(row)
        self.assertEqual(
            first["record_hash"], record(row | {"source_sequence": 2})["record_hash"]
        )
        self.assertNotEqual(
            first["record_hash"],
            record(row | {"product_category_name": None})["record_hash"],
        )
        self.assertIsNone(record(row | {"product_weight_g": None})["product_weight_g"])
        for value in (-1, True, 2**31):
            with self.subTest(value=value), self.assertRaises(BronzeError):
                record(row | {"product_weight_g": value})
