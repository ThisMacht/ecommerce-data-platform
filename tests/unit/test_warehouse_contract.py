import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from olist_platform.bronze.storage import BronzeError
from olist_platform.warehouse.loading import canonical_record
from tests.warehouse_fixture import valid_row


class WarehouseContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def record(self, row, table="customers"):
        return canonical_record(row, self.contract, table, "warehouse_fixture")

    def test_hash_ignores_source_versions_lineage_and_warnings(self):
        row = valid_row()
        actual = self.record(row)
        self.assertEqual(
            actual["record_hash"],
            "e09d3411e7bbfb07994d5a076051077944dbae1fdf52bad43c18d503707ad937",
        )
        changed = row | {
            "source_sequence": 100,
            "source_batch_id": "another",
            "_record_uid": "b" * 64,
            "warning_rule_ids": ["location_warning"],
        }
        self.assertEqual(self.record(changed)["record_hash"], actual["record_hash"])
        self.assertNotEqual(
            self.record(row | {"customer_city": "rio"})["record_hash"],
            actual["record_hash"],
        )
        self.assertNotEqual(
            self.record(row | {"customer_city": None})["record_hash"],
            self.record(row | {"customer_city": "null"})["record_hash"],
        )

    def test_timestamp_and_decimal_canonicalization(self):
        row = valid_row("order_items")
        first = self.record(row, "order_items")
        changed = row | {
            "shipping_limit_date": datetime(
                2019, 12, 31, 21, tzinfo=timezone(timedelta(hours=-3))
            ),
            "price": Decimal("10.500"),
        }
        self.assertEqual(
            self.record(changed, "order_items")["record_hash"], first["record_hash"]
        )
        self.assertEqual(first["price"], Decimal("10.50"))
        self.assertEqual(
            self.record(row | {"price": Decimal("-0.00")}, "order_items")[
                "record_hash"
            ],
            self.record(row | {"price": Decimal("0.00")}, "order_items")["record_hash"],
        )
        for change in (
            {"price": 10.5},
            {"price": Decimal("1.001")},
            {"price": Decimal("NaN")},
            {"price": Decimal("1e16")},
            {"price": Decimal("-1")},
            {"shipping_limit_date": datetime(2020, 1, 1)},
            {"order_item_id": True},
            {"order_item_id": 2**31},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                self.record(row | change, "order_items")

    def test_invalid_valid_route_rows_fail_before_merge(self):
        row = valid_row()
        for change in (
            {"customer_id": " a "},
            {"customer_id": None},
            {"customer_city": "a\x00b"},
            {"source_sequence": 0},
            {"source_operation": "DELETE"},
            {"error_rule_ids": ["invalid"]},
            {"deferred_rule_ids": ["missing_parent"]},
            {"_record_uid": "fake"},
            {"_source_name": "postgresql.orders"},
            {"_source_row_number": True},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                self.record(row | change)
        with self.assertRaisesRegex(BronzeError, "business timezone"):
            self.record(
                valid_row("orders") | {"order_purchase_date": "2020-01-01"}, "orders"
            )
