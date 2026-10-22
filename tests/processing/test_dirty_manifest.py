"""Compare every synthetic physical record to a handwritten routing/rule oracle."""

import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import FIXTURES, ROOT, dirty_rows

from olist_platform.bronze.storage import encode, sha256
from olist_platform.silver.customers import (
    check_customer_dependencies,
    transform_customers,
)
from olist_platform.silver.order_children import (
    check_order_dependencies,
    transform_order_children,
)
from olist_platform.silver.orders import input_schema, spark_session, transform_orders


class DirtyManifestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def test_every_record_has_exact_route_rules_and_unchanged_payload(self):
        manifest = json.loads((FIXTURES / "expected_error_manifest.json").read_text())
        self.assertEqual(
            manifest["source_contract_version"], self.contract["contract_version"]
        )
        summaries = {}
        for table, specification in manifest["tables"].items():
            with self.subTest(table=table), tempfile.TemporaryDirectory() as directory:
                values = dirty_rows(table)
                body = b"".join(encode(row) for row in values)
                self.assertEqual(sha256(body), specification["input_sha256"])
                path = Path(directory) / "input.jsonl"
                path.write_bytes(body)
                frame = self.spark.read.schema(input_schema(self.contract, table)).json(
                    str(path)
                )
                if table == "customers":
                    output = transform_customers(frame).withColumn(
                        "deferred_rule_ids", F.array().cast("array<string>")
                    )
                elif table == "orders":
                    parents = self.spark.createDataFrame(
                        [(k,) for k in specification["parent_keys"]],
                        "customer_id string",
                    )
                    output = check_customer_dependencies(
                        transform_orders(frame), parents
                    )
                else:
                    parents = self.spark.createDataFrame(
                        [(k,) for k in specification["parent_keys"]], "order_id string"
                    )
                    output = check_order_dependencies(
                        transform_order_children(frame, table), parents
                    )
                actual = output.select(
                    "_record_uid",
                    "_routing",
                    "error_rule_ids",
                    "warning_rule_ids",
                    "deferred_rule_ids",
                    "_raw_payload",
                ).collect()
                self.assertEqual(len(actual), len(values))
                expected = {
                    case["_record_uid"]: case for case in specification["cases"]
                }
                self.assertEqual({r["_record_uid"] for r in actual}, set(expected))
                self.assertEqual(
                    {r["_record_uid"]: r["_raw_payload"] for r in actual},
                    {r["_record_uid"]: r["_raw_payload"] for r in values},
                )
                for row in actual:
                    case = expected[row["_record_uid"]]
                    with self.subTest(
                        table=table, case=case["case"], uid=row["_record_uid"]
                    ):
                        self.assertEqual(row["_routing"], case["_routing"])
                        for field in (
                            "error_rule_ids",
                            "warning_rule_ids",
                            "deferred_rule_ids",
                        ):
                            self.assertEqual(
                                sorted(row[field]), sorted(case[field]), field
                            )
                counts = Counter(r["_routing"] for r in actual)
                self.assertEqual(
                    counts, Counter(c["_routing"] for c in expected.values())
                )
                summaries[table] = {
                    "input_rows": len(values),
                    **dict(sorted(counts.items())),
                }
        print(
            json.dumps(
                {"fixture_id": manifest["fixture_id"], "actual_counts": summaries},
                sort_keys=True,
            )
        )
