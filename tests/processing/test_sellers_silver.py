from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import ROOT

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.orders import input_schema, spark_session
from olist_platform.silver.sellers import transform_sellers


class SellerSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def row(self, uid, **changes):
        row = (
            dict(
                seller_id=uid,
                seller_city=" São Paulo ",
                seller_state=" sp ",
                seller_zip_code_prefix=" 00123 ",
                source_sequence=1,
                source_operation="INSERT",
                source_batch_id=" fixture ",
                source_updated_at="2020-01-01T00:00:00-03:00",
                source_loaded_at="2020-01-01T03:00:00Z",
            )
            | changes
        )
        return row | {"_record_uid": uid, "_raw_payload": encode(row).decode()}

    def frame(self, rows):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "rows.jsonl"
        path.write_bytes(b"".join(encode(row) for row in rows))
        return self.spark.read.schema(input_schema(self.contract, "sellers")).json(
            str(path)
        )

    def test_location_normalization_warning_rules_and_version_selection(self):
        values = [
            self.row("old", seller_id=" s "),
            self.row("latest", seller_id=" s ", source_sequence=2),
            self.row("same_location"),
            self.row(
                "blank", seller_city=" ", seller_state=" ", seller_zip_code_prefix=None
            ),
            self.row(
                "invalid_location", seller_state=" xx ", seller_zip_code_prefix=" 123 "
            ),
            self.row("missing_id", seller_id=" "),
            self.row(
                "metadata",
                source_sequence=0,
                source_batch_id=" ",
                source_operation="DELETE",
                source_updated_at="2020-01-01T00:00:00",
                source_loaded_at="invalid",
            ),
            self.row("offset", source_updated_at="2020-01-01T06:00:00+03:00"),
        ]
        actual = {
            r["_record_uid"]: r.asDict()
            for r in transform_sellers(self.frame(values))
            .withColumn(
                "source_updated_at",
                F.date_format("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
            .collect()
        }
        self.assertEqual(len(actual), 8)
        self.assertEqual(actual["old"]["_routing"], "duplicates")
        for uid in ("latest", "same_location", "blank", "invalid_location", "offset"):
            self.assertEqual(actual[uid]["_routing"], "valid")
            self.assertEqual(actual[uid]["error_rule_ids"], [])
        self.assertEqual(
            tuple(
                actual["latest"][k]
                for k in (
                    "seller_id",
                    "seller_city",
                    "seller_state",
                    "seller_zip_code_prefix",
                )
            ),
            ("s", "são paulo", "SP", "00123"),
        )
        self.assertEqual(
            set(actual["blank"]["warning_rule_ids"]),
            {
                "missing_seller_city",
                "invalid_seller_state",
                "invalid_seller_zip_code_prefix",
            },
        )
        self.assertIsNone(actual["blank"]["seller_city"])
        self.assertIsNone(actual["blank"]["seller_state"])
        self.assertEqual(
            set(actual["invalid_location"]["warning_rule_ids"]),
            {"invalid_seller_state", "invalid_seller_zip_code_prefix"},
        )
        self.assertEqual(
            (
                actual["invalid_location"]["seller_state"],
                actual["invalid_location"]["seller_zip_code_prefix"],
            ),
            ("XX", "123"),
        )
        self.assertEqual(actual["missing_id"]["_routing"], "rejected")
        self.assertEqual(actual["missing_id"]["error_rule_ids"], ["missing_seller_id"])
        self.assertEqual(actual["metadata"]["_routing"], "rejected")
        self.assertEqual(
            set(actual["metadata"]["error_rule_ids"]),
            {
                "invalid_source_sequence",
                "missing_source_batch_id",
                "invalid_source_operation",
                "invalid_source_updated_at",
                "invalid_source_loaded_at",
            },
        )
        self.assertEqual(actual["offset"]["source_updated_at"], "2020-01-01T03:00:00Z")
        self.assertEqual(actual["blank"]["_raw_payload"], values[3]["_raw_payload"])

    def test_same_version_duplicates_conflicts_physical_ids_and_timezone(self):
        result = transform_sellers(
            self.frame(
                [self.row("one", seller_id="same"), self.row("two", seller_id="same")]
            )
        ).collect()
        self.assertEqual(
            {r["_record_uid"]: r["_routing"] for r in result},
            {"one": "duplicates", "two": "valid"},
        )
        for rows in (
            [self.row("one"), self.row("two", seller_id="one", seller_city="Rio")],
            [self.row("one"), self.row("one")],
        ):
            with self.subTest(rows=rows), self.assertRaises(BronzeError):
                transform_sellers(self.frame(rows))
        self.spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        try:
            with self.assertRaisesRegex(BronzeError, "UTC"):
                transform_sellers(self.frame([self.row("one")]))
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
