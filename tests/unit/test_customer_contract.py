import json
import unittest
from pathlib import Path

from olist_platform.bronze.extraction import (
    configuration,
    cursor_tuple,
    cursor_value,
    state_sql,
)
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import (
    source_columns,
    validate_manifest,
    verify_objects,
)


class CustomerContractTest(unittest.TestCase):
    def setUp(self):
        self.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_customers_use_their_own_cursor_and_pipeline(self):
        from datetime import datetime, timezone

        now = datetime(2020, 1, 1, tzinfo=timezone.utc)
        cursor = cursor_value(
            {"customer_id": "a", "source_sequence": 1, "source_updated_at": now},
            "customers",
        )
        self.assertEqual(cursor["primary_key"], {"customer_id": "a"})
        self.assertEqual(cursor_tuple(cursor, "customers"), (now, 1, "a"))
        config = configuration(
            self.contract,
            ObjectStore(None, "bucket"),
            "http://localhost:9000",
            1,
            "customers",
        )
        self.assertEqual(config["pipeline"], "bronze_customers_v1")
        with self.assertRaises(BronzeError):
            state_sql("SELECT 1", "customers; DROP SCHEMA audit")

    def test_customer_silver_requires_a_full_snapshot(self):
        manifest = {
            "schema_version": "1.0.0",
            "source": "postgresql.customers",
            "source_schema": source_columns(self.contract, "customers"),
            "config": {"contract_sha256": sha256(encode(self.contract))},
            "snapshot_kind": "FULL",
            "extract_lower_bound": None,
        }
        validate_manifest(manifest, self.contract, "customers")
        for change in (
            {"snapshot_kind": "DELTA"},
            {"extract_lower_bound": "2020-01-01T00:00:00Z"},
            {"source": "postgresql.orders"},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                validate_manifest(manifest | change, self.contract, "customers")

    def test_customer_delta_requires_pipeline_scope_and_full_flag_remains_strict(self):
        manifest = {
            "schema_version": "1.0.0",
            "source": "postgresql.customers",
            "source_schema": source_columns(self.contract, "customers"),
            "config": {"contract_sha256": sha256(encode(self.contract))},
            "snapshot_kind": "INCREMENTAL",
            "extract_lower_bound": "2020-01-08T00:00:00Z",
        }
        with self.assertRaisesRegex(BronzeError, "full Bronze"):
            validate_manifest(manifest, self.contract, "customers")
        manifest["config"]["pipeline_run_id"] = "incremental"
        validate_manifest(manifest, self.contract, "customers")
        manifest["snapshot_kind"] = "FULL"
        with self.assertRaisesRegex(BronzeError, "lower bound"):
            validate_manifest(manifest, self.contract, "customers")

    def test_fk_manifest_requires_a_pinned_parent_and_full_customer_kind(self):
        manifest = {
            "prefix": "silver/orders/run_id=one",
            "format_version": 1,
            "schema_version": "orders-silver-v2",
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
            "relationship_checks": "CUSTOMER_SNAPSHOT",
        }
        with self.assertRaisesRegex(BronzeError, "pinned customer"):
            verify_objects(ObjectStore(None, "bucket"), manifest)
        manifest["request"] = {
            "customer_snapshot": {
                "prefix": "silver/customers/run_id=parent",
                "manifest_sha256": "0" * 64,
            }
        }
        manifest["request_sha256"] = sha256(encode(manifest["request"]))
        verify_objects(ObjectStore(None, "bucket"), manifest)
        manifest["schema_version"] = "customers-silver-v1"
        manifest["snapshot_kind"] = "DELTA"
        with self.assertRaisesRegex(BronzeError, "full snapshot"):
            verify_objects(ObjectStore(None, "bucket"), manifest)
