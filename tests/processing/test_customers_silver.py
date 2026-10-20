from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import FIXTURES, ROOT
from silver_fixture import rows as order_rows

from olist_platform.bronze.storage import encode
from olist_platform.silver.customers import (
    check_customer_dependencies,
    transform_customers,
)
from olist_platform.silver.orders import input_schema, spark_session, transform_orders


class CustomerSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def frame(self, values, table="customers"):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "input.jsonl"
        path.write_bytes(b"".join(encode(row) for row in values))
        return self.spark.read.schema(input_schema(self.contract, table)).json(
            str(path)
        )

    def customer_rows(self):
        result = []
        base = {
            "customer_unique_id": "shared_shopper",
            "customer_zip_code_prefix": "00123",
            "customer_city": " São Paulo ",
            "customer_state": "sp",
            "source_updated_at": "2020-01-01T00:00:00+00:00",
            "source_loaded_at": "2020-01-01T00:00:00+00:00",
            "source_operation": "INSERT",
            "source_batch_id": "fixture",
        }
        for number, value in enumerate(
            json.loads((FIXTURES / "customers_dirty.json").read_text()), 1
        ):
            row = base | value
            row.update(_record_uid=f"uid-{number}", _raw_payload=encode(row).decode())
            result.append(row)
        return result

    def test_independent_customer_fixture_grain_normalization_and_rules(self):
        output = transform_customers(self.frame(self.customer_rows())).cache()
        self.addCleanup(output.unpersist)
        expected = json.loads((FIXTURES / "customers_expected.json").read_text())
        counts = {
            r["_routing"]: r["count"]
            for r in output.groupBy("_routing").count().collect()
        }
        self.assertEqual(counts, {"valid": 3, "rejected": 2, "duplicates": 1})
        valid = output.filter("_routing = 'valid'")
        self.assertEqual(
            sorted(r["customer_id"] for r in valid.select("customer_id").collect()),
            expected["valid_customer_ids"],
        )
        latest = valid.filter("customer_id = 'customer_a'").first()
        self.assertEqual(
            (
                latest["source_sequence"],
                latest["customer_city"],
                latest["customer_state"],
                latest["customer_zip_code_prefix"],
            ),
            (2, "rio", "SP", "00123"),
        )
        violations = {
            r["rule"]: r["count"]
            for r in output.filter("_routing != 'duplicates'")
            .select(
                F.explode(F.concat("error_rule_ids", "warning_rule_ids")).alias("rule")
            )
            .groupBy("rule")
            .count()
            .collect()
        }
        self.assertEqual(violations, expected["rule_violations"])

    def test_fk_routing_preserves_input_counts_and_reject_priority(self):
        values = order_rows()
        for row in values:
            if row["order_id"] == "winter":
                row["customer_id"] = "late_customer"
        orders = transform_orders(self.frame(values, "orders"))
        parent = (
            transform_customers(self.frame(self.customer_rows()))
            .filter("_routing = 'valid'")
            .select("customer_id")
        )
        output = check_customer_dependencies(orders, parent).cache()
        self.addCleanup(output.unpersist)
        counts = {
            r["_routing"]: r["count"]
            for r in output.groupBy("_routing").count().collect()
        }
        self.assertEqual(
            counts, {"valid": 3, "deferred": 1, "rejected": 7, "duplicates": 2}
        )
        self.assertEqual(sum(counts.values()), 13)
        self.assertEqual(
            output.filter("_routing = 'deferred'").first()["order_id"], "winter"
        )
        self.assertEqual(
            output.filter(
                "_routing = 'rejected' AND size(deferred_rule_ids) > 0"
            ).count(),
            0,
        )
