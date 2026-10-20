from __future__ import annotations

import json
import unittest
from copy import deepcopy
from unittest.mock import Mock

from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import (
    validate_manifest,
    validate_record,
    verify_silver,
)
from tests.processing.silver_fixture import ROOT, publish_bronze


class SilverContractTest(unittest.TestCase):
    def setUp(self):
        self.contract = json.loads((ROOT / "config/source_contract.json").read_text())
        self.objects = {}
        self.store = Mock(spec=ObjectStore)
        self.store.put_immutable.side_effect = self.objects.__setitem__
        self.store.read.side_effect = self.objects.__getitem__
        self.prefix = publish_bronze(self.store, self.contract)
        self.manifest = json.loads(self.objects[f"{self.prefix}/manifest.json"])
        self.key = self.manifest["objects"][0]["key"]
        self.row = json.loads(self.objects[self.key].splitlines()[0])

    def test_source_contract_and_physical_lineage_are_required(self):
        validate_manifest(self.manifest, self.contract)
        valid = validate_record(self.row, self.contract, self.manifest, self.key, 1)
        self.assertIn("_raw_payload", valid)
        mutations = [
            dict(self.row, _record_uid="different"),
            dict(self.row, _source_row_number=True),
            dict(self.row, _logical_record_uid="different"),
            dict(self.row, source_sequence=True),
            dict(self.row, extra="unknown"),
        ]
        missing = dict(self.row)
        missing.pop("order_status")
        mutations.append(missing)
        for row in mutations:
            with self.subTest(row=row), self.assertRaises(BronzeError):
                validate_record(row, self.contract, self.manifest, self.key, 1)

    def test_bad_row_values_are_preserved_for_spark_routing(self):
        row = dict(self.row, customer_id=None, order_purchase_timestamp="invalid")
        validated = validate_record(row, self.contract, self.manifest, self.key, 1)
        self.assertIsNone(json.loads(validated["_raw_payload"])["customer_id"])
        blank_key = dict(self.row, order_id="   ", source_sequence=0)
        blank_key["_logical_record_uid"] = sha256(
            encode(["postgresql.orders", "   ", 0])
        )
        self.assertEqual(
            validate_record(blank_key, self.contract, self.manifest, self.key, 1)[
                "order_id"
            ],
            "   ",
        )

    def test_input_schema_and_contract_hash_drift_fail_batch(self):
        manifest = deepcopy(self.manifest)
        manifest["schema_version"] = "unknown"
        with self.assertRaises(BronzeError):
            validate_manifest(manifest, self.contract)
        manifest = deepcopy(self.manifest)
        manifest["config"]["contract_sha256"] = "0" * 64
        with self.assertRaises(BronzeError):
            validate_manifest(manifest, self.contract)

    def test_empty_silver_manifest_checks_counts_schema_and_marker(self):
        prefix = "silver/orders/run_id=empty"
        manifest = {
            "prefix": prefix,
            "format_version": 1,
            "schema_version": "orders-silver-v1",
            "request": {},
            "request_sha256": sha256(encode({})),
            "counts": {
                "input_rows": 0,
                "valid_rows": 0,
                "rejected_rows": 0,
                "duplicates_rows": 0,
                "deferred_rows": 0,
                "warning_rows": 0,
            },
            "output_schema": {},
            "schema_fingerprint": sha256(encode({})),
            "objects": [],
        }

        def seal():
            body = encode(manifest)
            self.objects[f"{prefix}/manifest.json"] = body
            self.objects[f"{prefix}/_SUCCESS"] = encode(
                {"manifest_sha256": sha256(body)}
            )

        with self.assertRaises(KeyError):
            verify_silver(self.store, prefix)
        seal()
        self.assertEqual(verify_silver(self.store, prefix), manifest)
        manifest["counts"]["input_rows"] = 1
        seal()
        with self.assertRaisesRegex(BronzeError, "reconciliation"):
            verify_silver(self.store, prefix)
        manifest["counts"]["input_rows"] = False
        seal()
        with self.assertRaisesRegex(BronzeError, "counters"):
            verify_silver(self.store, prefix)

    def test_silver_object_corruption_is_detected(self):
        prefix = "silver/orders/run_id=fixture"
        key = f"{prefix}/attempt=fixture/valid/part.parquet"
        self.objects[key] = b"original"
        manifest = {
            "prefix": prefix,
            "format_version": 1,
            "schema_version": "orders-silver-v1",
            "request": {},
            "request_sha256": sha256(encode({})),
            "counts": {
                "input_rows": 1,
                "valid_rows": 1,
                "rejected_rows": 0,
                "duplicates_rows": 0,
                "deferred_rows": 0,
                "warning_rows": 0,
            },
            "output_schema": {},
            "schema_fingerprint": sha256(encode({})),
            "objects": [
                {
                    "key": key,
                    "route": "valid",
                    "sha256": sha256(b"original"),
                    "size_bytes": 8,
                }
            ],
        }
        self.objects[f"{prefix}/manifest.json"] = encode(manifest)
        self.objects[f"{prefix}/_SUCCESS"] = encode(
            {"manifest_sha256": sha256(encode(manifest))}
        )
        self.assertEqual(verify_silver(self.store, prefix), manifest)
        self.objects[key] = b"corruption"
        with self.assertRaisesRegex(BronzeError, "checksum"):
            verify_silver(self.store, prefix)
