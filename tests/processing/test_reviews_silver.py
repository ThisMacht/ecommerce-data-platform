from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from silver_fixture import ROOT

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.order_children import check_order_dependencies
from olist_platform.silver.orders import input_schema, spark_session
from olist_platform.silver.reviews import transform_reviews


class ReviewsSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def row(self, uid, **changes):
        row = (
            dict(
                review_id=uid,
                order_id="a",
                review_score=5,
                review_comment_title=None,
                review_comment_message=" Ótimo!\nCHEGOU rápido. ",
                review_creation_date="2020-01-01T00:00:00",
                review_answer_timestamp="2020-01-02T10:15:00.123456",
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
        path.write_bytes(b"".join(encode(r) for r in rows))
        return self.spark.read.schema(
            input_schema(self.contract, "order_reviews")
        ).json(str(path))

    def test_routes_composite_grain_timestamps_optional_text_and_fk_precedence(self):
        values = [
            self.row("old", review_id=" r "),
            self.row("latest", review_id=" r ", source_sequence=2),
            self.row("same_review_other_order", review_id="r", order_id="b"),
            self.row("blank", review_comment_title="  ", review_comment_message=" "),
            self.row("missing", order_id="missing"),
            self.row("low", review_score=0, order_id="missing"),
            self.row("high", review_score=6),
            self.row("null", review_score=None),
            self.row("bad_id", review_id=" "),
            self.row("bad_order", order_id=" "),
            self.row("clock", review_answer_timestamp="2019-12-31T10:00:00"),
            self.row(
                "metadata",
                source_sequence=0,
                source_batch_id=" ",
                source_operation="DELETE",
                source_updated_at="2020-01-01T00:00:00",
                source_loaded_at="bad",
            ),
        ]
        expected = {
            uid: (route, rules)
            for uid, route, rules in [
                ("old", "duplicates", []),
                ("latest", "valid", []),
                ("same_review_other_order", "valid", []),
                ("blank", "valid", []),
                ("missing", "deferred", []),
                ("low", "rejected", ["invalid_review_score"]),
                ("high", "rejected", ["invalid_review_score"]),
                ("null", "rejected", ["invalid_review_score"]),
                ("bad_id", "rejected", ["missing_review_id"]),
                ("bad_order", "rejected", ["missing_order_id"]),
                ("clock", "valid", []),
                (
                    "metadata",
                    "rejected",
                    [
                        "invalid_source_sequence",
                        "missing_source_batch_id",
                        "invalid_source_operation",
                        "invalid_source_updated_at",
                        "invalid_source_loaded_at",
                    ],
                ),
            ]
        }
        for field in ("review_creation_date", "review_answer_timestamp"):
            for tag, value in [
                ("gap", "2018-11-04T00:30:00"),
                ("ambiguous", "2018-02-17T23:30:00"),
                ("offset", "2020-01-01T00:00:00Z"),
                ("malformed", "bad"),
                ("null", None),
            ]:
                uid = field + "_" + tag
                values.append(self.row(uid, **{field: value}))
                expected[uid] = ("rejected", ["invalid_" + field])
        output = check_order_dependencies(
            transform_reviews(self.frame(values)),
            self.spark.createDataFrame([("a",), ("b",)], "order_id string"),
        )
        for name in (
            "review_creation_date",
            "review_answer_timestamp",
            "source_updated_at",
        ):
            output = output.withColumn(
                name, F.date_format(name, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
            )
        actual = {r["_record_uid"]: r.asDict() for r in output.collect()}
        self.assertEqual(set(actual), set(expected))
        for uid, (route, rules) in expected.items():
            with self.subTest(uid=uid):
                self.assertEqual(actual[uid]["_routing"], route)
                self.assertEqual(set(actual[uid]["error_rule_ids"]), set(rules))
                self.assertEqual(
                    actual[uid]["deferred_rule_ids"],
                    ["missing_order"] if uid == "missing" else [],
                )
        self.assertEqual(actual["latest"]["review_id"], "r")
        self.assertEqual(
            actual["latest"]["review_creation_date"], "2020-01-01T03:00:00.000000Z"
        )
        self.assertEqual(
            actual["latest"]["review_answer_timestamp"], "2020-01-02T13:15:00.123456Z"
        )
        self.assertEqual(
            actual["latest"]["source_updated_at"], "2020-01-01T03:00:00.000000Z"
        )
        self.assertEqual(
            actual["latest"]["review_comment_message"], " Ótimo!\nCHEGOU rápido. "
        )
        self.assertIsNone(actual["blank"]["review_comment_title"])
        self.assertIsNone(actual["blank"]["review_comment_message"])
        self.assertEqual(actual["blank"]["warning_rule_ids"], [])
        self.assertEqual(
            actual["clock"]["warning_rule_ids"], ["review_answer_before_creation"]
        )

    def test_conflicts_repeated_uids_and_non_utc_session_fail(self):
        for rows in (
            [self.row("one"), self.row("two", review_id="one", review_score=4)],
            [self.row("one"), self.row("one")],
        ):
            with self.subTest(rows=rows), self.assertRaises(BronzeError):
                transform_reviews(self.frame(rows))
        self.spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        try:
            with self.assertRaisesRegex(BronzeError, "UTC"):
                transform_reviews(self.frame([self.row("one")]))
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
