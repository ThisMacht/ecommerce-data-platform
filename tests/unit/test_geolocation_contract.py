import json
import unittest
from decimal import Decimal
from pathlib import Path

from olist_platform.bronze.extraction import configuration
from olist_platform.bronze.storage import BronzeError, ObjectStore, encode, sha256
from olist_platform.raw.contracts import source_columns
from olist_platform.silver.batch import validate_manifest, verify_objects
from olist_platform.silver.geolocation import coordinate_value
from olist_platform.warehouse.loading import canonical_record
from tests.warehouse_fixture import valid_row


class GeolocationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config/source_contract.json"
            ).read_text()
        )

    def test_technical_key_and_standalone_full_scope(self):
        columns = source_columns(self.contract, "geolocation")
        self.assertEqual(
            columns[0],
            dict(name="geolocation_row_id", postgres_type="bigint", nullable=False),
        )
        self.assertEqual(len({c["name"] for c in columns}), len(columns))
        config = configuration(
            self.contract,
            ObjectStore(None, "bucket"),
            "http://localhost:9000",
            2,
            "geolocation",
        )
        self.assertEqual(config["pipeline"], "bronze_geolocation_v1")
        for mode in ("full", "incremental", "backfill"):
            with (
                self.subTest(mode=mode),
                self.assertRaisesRegex(BronzeError, "standalone"),
            ):
                configuration(
                    self.contract,
                    ObjectStore(None, "bucket"),
                    "http://localhost:9000",
                    2,
                    "geolocation",
                    pipeline_run_id="owner",
                    pipeline_mode=mode,
                )

    def test_manifest_requires_full_snapshot_without_lower_bound(self):
        manifest = dict(
            schema_version="1.0.0",
            source="postgresql.geolocation",
            source_schema=source_columns(self.contract, "geolocation"),
            config={"contract_sha256": sha256(encode(self.contract))},
            snapshot_kind="FULL",
            extract_lower_bound=None,
        )
        validate_manifest(manifest, self.contract, "geolocation")
        for change in (
            {"snapshot_kind": "INCREMENTAL"},
            {"extract_lower_bound": "2020-01-01Z"},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                validate_manifest(manifest | change, self.contract, "geolocation")
        with self.assertRaisesRegex(BronzeError, "full snapshot"):
            verify_objects(
                ObjectStore(None, "bucket"),
                dict(
                    prefix="silver/geolocation/run_id=one",
                    format_version=1,
                    schema_version="geolocation-silver-v1",
                    request={},
                    request_sha256=sha256(encode({})),
                    snapshot_kind="INCREMENTAL",
                ),
            )

    def test_coordinate_precision_boundaries_and_lexical_validation(self):
        for text, bound in (
            ("-23.5000000000000000000000000000000000001", 90),
            ("1e-100", 180),
            ("+90", 90),
            ("-180", 180),
            ("-0", 90),
        ):
            with self.subTest(text=text):
                self.assertEqual(coordinate_value(text, bound), Decimal(text))
        for value in (
            None,
            True,
            0.0,
            "NaN",
            "Infinity",
            "1_0",
            "",
            " 1",
            "1x",
            "90.0000000000000000000000000000001",
        ):
            with self.subTest(value=value):
                self.assertIsNone(coordinate_value(value, 90))

    def test_numeric_hash_is_exact_stable_and_checks_staging_boundary(self):
        row = valid_row("geolocation", "1")

        def record(changes=None):
            return canonical_record(
                row | (changes or {}), self.contract, "geolocation", "load"
            )

        first = record()
        self.assertEqual(first["geolocation_zip_code_prefix"], "00123")
        self.assertEqual(
            first["record_hash"],
            record(
                {
                    "geolocation_lat": "-23.5000000000000000010",
                    "geolocation_lng": "-4.6500000000000000001e1",
                    "source_sequence": 2,
                }
            )["record_hash"],
        )
        self.assertNotEqual(
            first["record_hash"],
            record({"geolocation_lat": "-23.500000000000000002"})["record_hash"],
        )
        self.assertIsNone(
            record({"geolocation_city": None, "geolocation_state": None})[
                "geolocation_city"
            ]
        )
        for change in (
            {"geolocation_row_id": 0},
            {"geolocation_lat": "NaN"},
            {"geolocation_lng": "180.000000000000000000001"},
            {"geolocation_zip_code_prefix": "123"},
            {"geolocation_lat": -23.5},
        ):
            with self.subTest(change=change), self.assertRaises(BronzeError):
                record(change)
