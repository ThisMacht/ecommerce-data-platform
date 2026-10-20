from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import FIXTURES, ROOT, rows

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.orders import input_schema, spark_session, transform_orders


class OrdersSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def frame(self, values):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "input.jsonl"
        path.write_bytes(b"".join(encode(row) for row in values))
        return self.spark.read.schema(input_schema(self.contract)).json(str(path))

    def test_independent_dirty_fixture_routing_timezone_and_lineage(self):
        values = rows()
        output = transform_orders(self.frame(values)).cache()
        self.addCleanup(output.unpersist)
        expected = json.loads((FIXTURES / "expected.json").read_text())
        counts = {
            r["_routing"]: r["count"]
            for r in output.groupBy("_routing").count().collect()
        }
        self.assertEqual(counts, {"valid": 4, "rejected": 7, "duplicates": 2})
        valid = output.filter("_routing = 'valid'")
        self.assertEqual(
            sorted(r["order_id"] for r in valid.select("order_id").collect()),
            expected["valid_order_ids"],
        )
        times = {
            r["order_id"]: r["utc"]
            for r in valid.select(
                "order_id",
                F.date_format("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss").alias(
                    "utc"
                ),
            ).collect()
        }
        self.assertEqual(times["dup"], expected["summer_purchase_utc"])
        self.assertEqual(times["winter"], expected["winter_purchase_utc"])
        self.assertEqual(
            {r["_record_uid"] for r in output.select("_record_uid").collect()},
            {r["_record_uid"] for r in values},
        )
        self.assertEqual(valid.filter("order_id = 'dup'").first()["source_sequence"], 2)
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

    def test_duplicate_physical_ids_and_conflicting_versions_fail_batch(self):
        value = rows()[0]
        with self.assertRaisesRegex(BronzeError, "physical"):
            transform_orders(self.frame([value, value]))
        conflicting = dict(
            value, _record_uid="other", _raw_payload='{"different":true}'
        )
        with self.assertRaisesRegex(BronzeError, "Conflicting"):
            transform_orders(self.frame([value, conflicting]))

    def test_latest_invalid_version_does_not_revive_an_older_good_row(self):
        old = rows()[0]
        new = dict(
            old,
            source_sequence=20,
            customer_id=None,
            _record_uid="new",
            _raw_payload='{"version":20}',
        )
        output = transform_orders(self.frame([old, new])).select("_routing").collect()
        self.assertEqual(
            sorted(r["_routing"] for r in output), ["duplicates", "rejected"]
        )

    def test_invalid_source_metadata_and_offset_business_time_are_rejected(self):
        value = dict(
            rows()[0],
            source_sequence=0,
            source_updated_at="2020-01-01T00:00:00",
            source_operation="DELETE",
            order_purchase_timestamp="2018-01-01T12:00:00-02:00",
        )
        output = transform_orders(self.frame([value])).first()
        self.assertEqual(output["_routing"], "rejected")
        self.assertTrue(
            {
                "invalid_source_sequence",
                "invalid_source_updated_at",
                "invalid_source_operation",
                "invalid_order_purchase_timestamp",
            }
            <= set(output["error_rule_ids"])
        )

    def test_empty_frame_retains_typed_schema(self):
        output = transform_orders(self.frame([]))
        self.assertEqual(output.count(), 0)
        self.assertEqual(
            output.schema["order_purchase_timestamp"].dataType.simpleString(),
            "timestamp",
        )
