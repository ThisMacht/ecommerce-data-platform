from __future__ import annotations

import json
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from olist_platform.bronze.extraction import configuration, cursor_tuple, cursor_value
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.silver.batch import (
    ITEM_SCOPE,
    LINEAGE,
    source_columns,
    validate_record,
    verify_objects,
)


class OrderChildrenContractTest(unittest.TestCase):
    def setUp(self):
        self.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_composite_cursor_preserves_numeric_order_and_both_key_components(self):
        now = datetime(2020, 1, 1, tzinfo=timezone.utc)
        for table, ordinal in (
            ("order_payments", "payment_sequential"),
            ("order_items", "order_item_id"),
        ):
            row = {
                "source_updated_at": now,
                "source_sequence": 1,
                "order_id": "a",
                ordinal: 2,
            }
            cursor = cursor_value(row, table)
            self.assertEqual(cursor["primary_key"], {"order_id": "a", ordinal: 2})
            self.assertEqual(cursor_tuple(cursor, table), (now, 1, "a", 2))
            larger = cursor_value(row | {ordinal: 10}, table)
            self.assertLess(cursor_tuple(cursor, table), cursor_tuple(larger, table))

    def test_full_orders_snapshot_is_opt_in_and_changes_sealed_configuration(self):
        args = (self.contract, ObjectStore(None, "bucket"), "http://localhost:9000", 1)
        self.assertNotIn("snapshot_kind", configuration(*args))
        self.assertEqual(
            configuration(*args, full_snapshot=True)["snapshot_kind"], "FULL"
        )
        for table in ("order_items", "order_payments"):
            self.assertEqual(
                configuration(*args, table=table, full_snapshot=True)["snapshot_kind"],
                "FULL",
            )
        for table in ("customers",):
            with self.assertRaises(BronzeError):
                configuration(*args, table=table, full_snapshot=True)

    def test_child_physical_types_and_logical_lineage_require_composite_keys(self):
        for table, ordinal in (
            ("order_payments", "payment_sequential"),
            ("order_items", "order_item_id"),
        ):
            columns = source_columns(self.contract, table)
            row = {
                c["name"]: 1 if c["postgres_type"] in ("bigint", "integer") else "value"
                for c in columns
            }
            row.update({name: None for name in LINEAGE})
            key = f"bronze/{table}/part.jsonl"
            row.update(
                order_id="a",
                _record_uid=sha256(encode(["one", key, 1])),
                _source_file=key,
                _source_row_number=1,
                _run_attempt=1,
                _run_id="one",
                _source_name=f"postgresql.{table}",
                _schema_version="1.0.0",
                _ingested_at="2020-01-01T00:00:00Z",
                _logical_record_uid=sha256(
                    encode([f"postgresql.{table}", ["a", 1], 1])
                ),
            )
            manifest = {"run_id": "one", "prepared_at": row["_ingested_at"]}
            validate_record(row, self.contract, manifest, key, 1, table)
            for change in (
                {ordinal: True},
                {ordinal: "1"},
                {ordinal: 2},
                {
                    "_logical_record_uid": sha256(
                        encode([f"postgresql.{table}", "a", 1])
                    )
                },
            ):
                with (
                    self.subTest(table=table, change=change),
                    self.assertRaises(BronzeError),
                ):
                    validate_record(
                        row | change, self.contract, manifest, key, 1, table
                    )
            money = "price" if table == "order_items" else "payment_value"
            with self.assertRaisesRegex(BronzeError, "physical type"):
                validate_record(
                    row | {money: 1.25}, self.contract, manifest, key, 1, table
                )

    def test_child_manifest_requires_pinned_parent_and_explicit_unchecked_fks(self):
        for table in ("order_items", "order_payments"):
            request = {
                "order_snapshot": {
                    "prefix": "silver/orders/run_id=one",
                    "manifest_sha256": "0" * 64,
                }
            }
            manifest = {
                "prefix": f"silver/{table}/run_id=one",
                "format_version": 1,
                "schema_version": f"{table}-silver-v1",
                "request": request,
                "request_sha256": sha256(encode(request)),
                "counts": dict.fromkeys(
                    (
                        "input_rows",
                        "valid_rows",
                        "deferred_rows",
                        "rejected_rows",
                        "duplicates_rows",
                        "warning_rows",
                    ),
                    0,
                ),
                "output_schema": {},
                "schema_fingerprint": sha256(encode({})),
                "objects": [],
                "relationship_checks": "ORDER_SNAPSHOT",
                "unchecked_relationships": ["product_id", "seller_id"]
                if table == "order_items"
                else [],
            }
            verify_objects(ObjectStore(None, "bucket"), manifest)
            bad = deepcopy(manifest)
            bad["request"] = {}
            bad["request_sha256"] = sha256(encode({}))
            with self.assertRaisesRegex(BronzeError, "pinned order"):
                verify_objects(ObjectStore(None, "bucket"), bad)
            bad = deepcopy(manifest)
            bad.pop("unchecked_relationships")
            with self.assertRaisesRegex(BronzeError, "relationship scope"):
                verify_objects(ObjectStore(None, "bucket"), bad)

    def test_item_v2_requires_all_three_pins_and_no_unchecked_membership(self):
        request = {
            f"{name}_snapshot": {
                "prefix": f"silver/{name}s/one",
                "manifest_sha256": "a" * 64,
            }
            for name in ("order", "product", "seller")
        }
        manifest = {
            "prefix": "silver/order_items/one",
            "format_version": 1,
            "schema_version": "order_items-silver-v2",
            "request": request,
            "request_sha256": sha256(encode(request)),
            "output_schema": {},
            "schema_fingerprint": sha256(encode({})),
            "objects": [],
            "relationship_checks": ITEM_SCOPE,
            "unchecked_relationships": [],
            "counts": dict.fromkeys(
                (
                    "input_rows",
                    "valid_rows",
                    "deferred_rows",
                    "rejected_rows",
                    "duplicates_rows",
                    "warning_rows",
                ),
                0,
            ),
        }
        verify_objects(ObjectStore(None, "bucket"), manifest)
        for name in ("order", "product", "seller"):
            bad = deepcopy(manifest)
            del bad["request"][f"{name}_snapshot"]
            bad["request_sha256"] = sha256(encode(bad["request"]))
            with self.subTest(name=name), self.assertRaises(BronzeError):
                verify_objects(ObjectStore(None, "bucket"), bad)
        for change in (
            {"relationship_checks": "ORDER_SNAPSHOT"},
            {"unchecked_relationships": ["seller_id"]},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                verify_objects(ObjectStore(None, "bucket"), manifest | change)
