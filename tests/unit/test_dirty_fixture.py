"""Check deterministic injection and complete oracle identities without Spark."""

import json
import unittest

from olist_platform.bronze.storage import encode, sha256
from tests.processing.silver_fixture import FIXTURES, dirty_rows, rows


class DirtyFixtureTest(unittest.TestCase):
    def test_fixture_bytes_and_expected_physical_identities_are_pinned(self):
        manifest = json.loads((FIXTURES / "expected_error_manifest.json").read_text())
        self.assertEqual(manifest["format_version"], 1)
        self.assertEqual(
            set(manifest["tables"]),
            {"customers", "orders", "order_items", "order_payments"},
        )
        for table, specification in manifest["tables"].items():
            with self.subTest(table=table):
                first = dirty_rows(table)
                body = b"".join(encode(row) for row in first)
                self.assertEqual(
                    b"".join(encode(row) for row in dirty_rows(table)), body
                )
                self.assertEqual(sha256(body), specification["input_sha256"])
                cases = specification["cases"]
                self.assertEqual(len(cases), len(first))
                self.assertEqual(len({c["_record_uid"] for c in cases}), len(cases))
                self.assertEqual(
                    {c["_record_uid"] for c in cases}, {r["_record_uid"] for r in first}
                )
                for case in cases:
                    self.assertTrue(case["case"])
                    self.assertIn(
                        case["_routing"],
                        ("valid", "rejected", "deferred", "duplicates"),
                    )
                    for field in (
                        "error_rule_ids",
                        "warning_rule_ids",
                        "deferred_rule_ids",
                    ):
                        rules = case[field]
                        self.assertEqual(len(rules), len(set(rules)))
                        self.assertTrue(
                            all(isinstance(rule, str) and rule for rule in rules)
                        )
                    if case["_routing"] == "rejected":
                        self.assertTrue(case["error_rule_ids"])
                        self.assertEqual(case["deferred_rule_ids"], [])
                    elif case["_routing"] == "deferred":
                        self.assertFalse(case["error_rule_ids"])
                        self.assertTrue(case["deferred_rule_ids"])

    def test_injection_preserves_raw_evidence_and_does_not_mutate_legacy_inputs(self):
        legacy = rows()
        before = b"".join(encode(row) for row in legacy)
        injected = dirty_rows("orders")
        winter = next(r for r in injected if r["order_id"] == "winter")
        self.assertEqual(winter["customer_id"], "late_customer")
        self.assertEqual(
            json.loads(winter["_raw_payload"])["customer_id"], "late_customer"
        )
        self.assertEqual(
            next(r for r in legacy if r["order_id"] == "winter")["customer_id"],
            "customer_a",
        )
        self.assertEqual(b"".join(encode(row) for row in rows()), before)
        for table in ("customers", "orders", "order_items", "order_payments"):
            for row in dirty_rows(table):
                raw = json.loads(row["_raw_payload"])
                self.assertEqual(raw, {key: row[key] for key in raw})
        invalid = next(
            r for r in dirty_rows("order_items") if r["order_id"] == "bad_money"
        )
        self.assertEqual(invalid["price"], "1.001")
        invalid["price"] = "changed"
        self.assertEqual(
            next(r for r in dirty_rows("order_items") if r["order_id"] == "bad_money")[
                "price"
            ],
            "1.001",
        )
        with self.assertRaises(ValueError):
            dirty_rows("unsupported")
