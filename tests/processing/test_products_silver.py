from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import ROOT

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.orders import input_schema, spark_session
from olist_platform.silver.products import transform_products


class ProductSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def row(self, uid, **changes):
        row = (
            dict(
                product_id=uid,
                product_category_name=" CAMA_MESA_BANHO ",
                product_name_lenght=20,
                product_description_lenght=100,
                product_photos_qty=1,
                product_weight_g=200,
                product_length_cm=10,
                product_height_cm=5,
                product_width_cm=8,
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
        return self.spark.read.schema(input_schema(self.contract, "products")).json(
            str(path)
        )

    def test_product_routes_rules_nulls_and_version_selection(self):
        fields = (
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        values = [
            self.row("old", product_id=" p ", source_sequence=1),
            self.row("latest", product_id=" p ", source_sequence=2),
            self.row("missing", product_category_name=" ", **dict.fromkeys(fields)),
            self.row("zero", product_weight_g=0, product_length_cm=0),
            self.row("key", product_id=" "),
            self.row(
                "metadata",
                source_sequence=0,
                source_batch_id=" ",
                source_operation="DELETE",
                source_updated_at="2020-01-01T00:00:00",
                source_loaded_at="not-a-time",
            ),
        ]
        for field in fields:
            values.append(self.row(field, **{field: -1}))
        values.append(self.row("overflow", product_width_cm=2**31))
        actual = {
            r["_record_uid"]: r.asDict()
            for r in transform_products(self.frame(values))
            .withColumn(
                "source_updated_at",
                F.date_format("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
            .collect()
        }
        self.assertEqual(len(actual), 14)
        self.assertEqual(actual["old"]["_routing"], "duplicates")
        self.assertEqual(
            (actual["latest"]["product_id"], actual["latest"]["product_category_name"]),
            ("p", "cama_mesa_banho"),
        )
        for uid in ("latest", "missing", "zero"):
            self.assertEqual(actual[uid]["_routing"], "valid")
            self.assertEqual(actual[uid]["error_rule_ids"], [])
        self.assertEqual(
            set(actual["missing"]["warning_rule_ids"]),
            {"missing_" + name for name in ("product_category_name",) + fields},
        )
        self.assertIsNone(actual["missing"]["product_weight_g"])
        self.assertEqual(
            set(actual["zero"]["warning_rule_ids"]),
            {"zero_product_weight_g", "zero_product_length_cm"},
        )
        for uid in fields + ("overflow", "key", "metadata"):
            self.assertEqual(actual[uid]["_routing"], "rejected")
        for field in fields:
            self.assertEqual(actual[field]["error_rule_ids"], ["invalid_" + field])
        self.assertEqual(
            actual["overflow"]["error_rule_ids"], ["invalid_product_width_cm"]
        )
        self.assertEqual(actual["key"]["error_rule_ids"], ["missing_product_id"])
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
        self.assertEqual(actual["latest"]["source_updated_at"], "2020-01-01T03:00:00Z")
        self.assertEqual(actual["missing"]["_raw_payload"], values[2]["_raw_payload"])

    def test_conflicting_versions_and_repeated_physical_ids_fail(self):
        for rows in (
            [self.row("one"), self.row("two", product_id="one", product_weight_g=10)],
            [self.row("one"), self.row("one")],
        ):
            with self.subTest(rows=rows), self.assertRaises(BronzeError):
                transform_products(self.frame(rows))
        self.spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        try:
            with self.assertRaisesRegex(BronzeError, "UTC"):
                transform_products(self.frame([self.row("one")]))
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
