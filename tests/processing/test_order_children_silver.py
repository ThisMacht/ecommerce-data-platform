from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import FIXTURES, ROOT, child_rows

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.order_children import (
    check_order_dependencies,
    transform_order_children,
)
from olist_platform.silver.orders import input_schema, spark_session


class OrderChildrenSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def frame(self, values, table):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "input.jsonl"
        path.write_bytes(b"".join(encode(row) for row in values))
        return self.spark.read.schema(input_schema(self.contract, table)).json(
            str(path)
        )

    def test_independent_fixtures_composite_grain_money_timezone_and_routing(self):
        expected = json.loads((FIXTURES / "order_children_expected.json").read_text())
        for table, ordinal, money in (
            ("order_payments", "payment_sequential", "payment_value"),
            ("order_items", "order_item_id", "price"),
        ):
            with self.subTest(table=table):
                local = transform_order_children(
                    self.frame(child_rows(table), table), table
                )
                parent = self.spark.createDataFrame([("a",)], "order_id string")
                output = check_order_dependencies(local, parent).cache()
                self.addCleanup(output.unpersist)
                counts = {
                    r["_routing"]: r["count"]
                    for r in output.groupBy("_routing").count().collect()
                }
                self.assertEqual(counts, expected[table]["counts"])
                valid = output.filter("_routing = 'valid'").orderBy(ordinal).collect()
                self.assertEqual(
                    [[r["order_id"], r[ordinal]] for r in valid],
                    expected[table]["valid_keys"],
                )
                self.assertEqual(
                    [r[money] for r in valid], [Decimal("11.25"), Decimal("0.00")]
                )
                self.assertEqual(
                    output.schema[money].dataType.simpleString(), "decimal(18,2)"
                )
                self.assertEqual(valid[0]["source_sequence"], 2)
                if table == "order_items":
                    self.assertEqual(
                        output.filter("_routing = 'valid' AND order_item_id = 1")
                        .select(
                            F.date_format("shipping_limit_date", "yyyy-MM-dd HH:mm:ss")
                        )
                        .first()[0],
                        "2018-01-01 14:00:00",
                    )
                    self.assertEqual(
                        (valid[0]["product_id"], valid[0]["seller_id"]),
                        ("product", "seller"),
                    )
                rules = {
                    r["rule"]: r["count"]
                    for r in output.filter("_routing != 'duplicates'")
                    .select(
                        F.explode(
                            F.concat(
                                "error_rule_ids",
                                "warning_rule_ids",
                                "deferred_rule_ids",
                            )
                        ).alias("rule")
                    )
                    .groupBy("rule")
                    .count()
                    .collect()
                }
                self.assertEqual(rules, expected[table]["rule_violations"])
                self.assertEqual(
                    output.filter(
                        "_routing = 'rejected' AND size(deferred_rule_ids) > 0"
                    ).count(),
                    0,
                )

    def test_composite_conflicts_and_repeated_physical_uid_fail(self):
        for table in ("order_payments", "order_items"):
            values = child_rows(table)[1:3]
            conflicting = deepcopy(values[0])
            conflicting["_record_uid"] = "conflict"
            conflicting["_raw_payload"] = "different source payload"
            with (
                self.subTest(table=table),
                self.assertRaisesRegex(BronzeError, "Conflicting"),
            ):
                transform_order_children(
                    self.frame(values + [conflicting], table), table
                )
            with (
                self.subTest(table=table),
                self.assertRaisesRegex(BronzeError, "Repeated"),
            ):
                transform_order_children(self.frame(values + [values[0]], table), table)

    def test_empty_parent_defers_only_locally_valid_selected_rows(self):
        local = transform_order_children(
            self.frame(child_rows("order_payments"), "order_payments"), "order_payments"
        )
        output = check_order_dependencies(
            local, self.spark.createDataFrame([], "order_id string")
        )
        self.assertEqual(output.filter("_routing = 'deferred'").count(), 3)
