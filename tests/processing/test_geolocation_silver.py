from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from silver_fixture import ROOT

from olist_platform.bronze.storage import BronzeError, encode
from olist_platform.silver.geolocation import transform_geolocation
from olist_platform.silver.orders import input_schema, spark_session


class GeolocationSilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.addClassCleanup(cls.spark.stop)
        cls.contract = json.loads((ROOT / "config/source_contract.json").read_text())

    def row(self, uid, **changes):
        row = (
            dict(
                geolocation_row_id=1,
                geolocation_zip_code_prefix=" 00123 ",
                geolocation_lat=" -23.500000000000000000000000000000000001 ",
                geolocation_lng="-46.5",
                geolocation_city=" São Paulo ",
                geolocation_state=" sp ",
                source_sequence=1,
                source_operation="INSERT",
                source_batch_id=" fixture ",
                source_updated_at="2020-01-01T00:00:00Z",
                source_loaded_at="2020-01-01T03:00:00+03:00",
            )
            | changes
        )
        return row | dict(_record_uid=uid, _raw_payload=encode(row).decode())

    def frame(self, rows):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "rows.jsonl"
        path.write_bytes(b"".join(encode(r) for r in rows))
        return self.spark.read.schema(input_schema(self.contract, "geolocation")).json(
            str(path)
        )

    def test_exact_coordinates_locations_versions_and_independent_routes(self):
        values = [
            self.row("old"),
            self.row("new", source_sequence=2),
            self.row("another_observation", geolocation_row_id=2),
            self.row(
                "blank",
                geolocation_row_id=3,
                geolocation_city=" ",
                geolocation_state=" ",
            ),
            self.row("unknown_state", geolocation_row_id=4, geolocation_state="xx"),
            self.row(
                "boundary",
                geolocation_row_id=5,
                geolocation_lat="90",
                geolocation_lng="-180",
            ),
            self.row(
                "bad_coordinates",
                geolocation_row_id=6,
                geolocation_lat="90.0000000000000000000000000000001",
                geolocation_lng="NaN",
            ),
            self.row(
                "malformed",
                geolocation_row_id=7,
                geolocation_lat="abc",
                geolocation_lng=None,
            ),
            self.row(
                "bad_identity", geolocation_row_id=0, geolocation_zip_code_prefix="123"
            ),
            self.row(
                "bad_metadata",
                geolocation_row_id=8,
                source_sequence=0,
                source_batch_id=" ",
                source_operation="DELETE",
                source_updated_at="invalid",
                source_loaded_at="2020-01-01T00:00:00",
            ),
        ]
        actual = {
            r["_record_uid"]: r.asDict()
            for r in transform_geolocation(self.frame(values)).collect()
        }
        self.assertEqual(actual["old"]["_routing"], "duplicates")
        for uid in ("new", "another_observation", "blank", "unknown_state", "boundary"):
            self.assertEqual(actual[uid]["_routing"], "valid")
        self.assertEqual(
            actual["new"]["geolocation_lat"], "-23.500000000000000000000000000000000001"
        )
        self.assertEqual(
            tuple(
                actual["new"][c]
                for c in (
                    "geolocation_zip_code_prefix",
                    "geolocation_city",
                    "geolocation_state",
                )
            ),
            ("00123", "são paulo", "SP"),
        )
        self.assertIsNone(actual["blank"]["geolocation_city"])
        self.assertEqual(
            set(actual["blank"]["warning_rule_ids"]),
            {"missing_geolocation_city", "invalid_geolocation_state"},
        )
        for uid in ("bad_coordinates", "malformed"):
            self.assertEqual(actual[uid]["_routing"], "rejected")
            self.assertEqual(
                set(actual[uid]["error_rule_ids"]),
                {"invalid_geolocation_lat", "invalid_geolocation_lng"},
            )
        self.assertEqual(
            set(actual["bad_identity"]["error_rule_ids"]),
            {"invalid_geolocation_row_id", "invalid_geolocation_zip_code_prefix"},
        )
        self.assertEqual(
            set(actual["bad_metadata"]["error_rule_ids"]),
            {
                "invalid_source_sequence",
                "missing_source_batch_id",
                "invalid_source_operation",
                "invalid_source_updated_at",
                "invalid_source_loaded_at",
            },
        )
        self.assertEqual(actual["new"]["_raw_payload"], values[1]["_raw_payload"])

    def test_conflicts_duplicate_uids_and_non_utc_sessions_fail(self):
        for rows in (
            [self.row("one"), self.row("two", geolocation_city="Rio")],
            [self.row("one"), self.row("one")],
        ):
            with self.subTest(rows=rows), self.assertRaises(BronzeError):
                transform_geolocation(self.frame(rows))
        self.spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
        try:
            with self.assertRaisesRegex(BronzeError, "UTC"):
                transform_geolocation(self.frame([self.row("one")]))
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
