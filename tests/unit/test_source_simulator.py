from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from olist_platform.raw.contracts import load_contract
from olist_platform.source_simulator.batches import (
    BatchError,
    load_batch,
    source_time,
    typed_value,
    validate_payload,
    write_batch,
)
from olist_platform.source_simulator.fixtures import (
    fixture_batches,
    fixture_ids,
    write_fixture,
)

CONTRACT = Path(__file__).resolve().parents[2] / "config/source_contract.json"


class SourceSimulatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract(CONTRACT)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.batches = fixture_batches()

    def test_fixture_is_deterministic_and_existing_artifacts_are_unchanged(
        self,
    ) -> None:
        first = write_fixture(self.directory / "first", self.contract)
        before = {
            str(path.relative_to(first[0].parent)): (
                path.read_bytes(),
                path.stat().st_mtime_ns,
            )
            for path in (self.directory / "first").rglob("*.json")
        }
        write_fixture(self.directory / "first", self.contract)
        after = {
            str(path.relative_to(first[0].parent)): (
                path.read_bytes(),
                path.stat().st_mtime_ns,
            )
            for path in (self.directory / "first").rglob("*.json")
        }
        self.assertEqual(before, after)
        second = write_fixture(self.directory / "second", self.contract)
        self.assertEqual(
            [load_batch(path, self.contract).manifest for path in first],
            [load_batch(path, self.contract).manifest for path in second],
        )
        self.assertEqual(
            [
                load_batch(path, self.contract).manifest["operation_count"]
                for path in first
            ],
            [7, 2, 5, 2],
        )

    def test_seed_changes_keys_and_batch_ids(self) -> None:
        self.assertNotEqual(fixture_ids(42), fixture_ids(43))
        self.assertNotEqual(
            fixture_batches(42)[0][1]["batch_id"], fixture_batches(43)[0][1]["batch_id"]
        )
        self.assertEqual(len(set(fixture_ids().values())), 8)

    def test_same_timestamp_and_backdated_versions_are_explicit(self) -> None:
        initial = self.batches[0][1]["operations"]
        orders = [operation for operation in initial if operation["table"] == "orders"]
        self.assertEqual(len(orders), 2)
        self.assertEqual(orders[0]["source_updated_at"], orders[1]["source_updated_at"])
        self.assertEqual(
            [entry[2]["fixture_rows_after"] for entry in self.batches],
            [
                {"customers": 2, "orders": 2, "order_items": 2, "order_payments": 1},
                {"customers": 2, "orders": 2, "order_items": 3, "order_payments": 2},
                {"customers": 3, "orders": 3, "order_items": 3, "order_payments": 3},
                {"customers": 3, "orders": 3, "order_items": 3, "order_payments": 3},
            ],
        )
        reference = source_time(self.batches[-1][2]["lookback_reference"])
        timestamps = [
            source_time(op["source_updated_at"])
            for op in self.batches[-1][1]["operations"]
        ]
        self.assertLess((reference - timestamps[0]).total_seconds(), 48 * 3600)
        self.assertGreater((reference - timestamps[1]).total_seconds(), 48 * 3600)

    def test_checksum_detects_byte_changes_even_when_json_is_unchanged(self) -> None:
        path = write_fixture(self.directory / "fixture", self.contract)[0]
        payload = path / "operations.json"
        payload.write_bytes(payload.read_bytes() + b"\n")
        with self.assertRaisesRegex(BatchError, "payload_sha256 mismatch"):
            load_batch(path, self.contract)

    def test_manifest_counts_and_contract_hash_are_checked(self) -> None:
        path = write_fixture(self.directory / "fixture", self.contract)[0]
        changed_contract = deepcopy(self.contract)
        changed_contract["policies"]["lateness_sla_hours"] = 24
        with self.assertRaisesRegex(BatchError, "contract_sha256 mismatch"):
            load_batch(path, changed_contract)
        manifest_path = path / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["operation_count"] = 8
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(BatchError, "operation_count mismatch"):
            load_batch(path, self.contract)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        path = write_fixture(self.directory / "fixture", self.contract)[0]
        (path / "manifest.json").write_text('{"format_version":1,"format_version":1}')
        with self.assertRaisesRegex(BatchError, "Duplicate JSON key"):
            load_batch(path, self.contract)

    def test_existing_different_fixture_is_never_overwritten(self) -> None:
        path = write_fixture(self.directory / "fixture", self.contract)[0]
        before = (path / "operations.json").read_bytes()
        with self.assertRaisesRegex(BatchError, "Refusing to overwrite"):
            write_fixture(self.directory / "fixture", self.contract, seed=43)
        self.assertEqual(before, (path / "operations.json").read_bytes())

    def test_metadata_primary_keys_unknown_tables_and_versions_are_rejected(
        self,
    ) -> None:
        payload = self.batches[0][1]
        cases = [
            ("format", lambda p: p.update(format_version=2)),
            ("table", lambda p: p["operations"][0].update(table="products")),
            ("action", lambda p: p["operations"][0].update(operation="DELETE")),
            ("key", lambda p: p["operations"][0]["key"].clear()),
            (
                "metadata",
                lambda p: p["operations"][0]["values"].update(source_sequence=999),
            ),
            (
                "immutable key",
                lambda p: p["operations"][0]["values"].update(customer_id="changed"),
            ),
            (
                "missing insert field",
                lambda p: p["operations"][0]["values"].pop("customer_city"),
            ),
            (
                "naive timestamp",
                lambda p: p["operations"][0].update(
                    source_updated_at="2020-01-01T00:00:00"
                ),
            ),
        ]
        for label, mutate in cases:
            with self.subTest(case=label):
                changed = deepcopy(payload)
                mutate(changed)
                with self.assertRaises(BatchError):
                    validate_payload(changed, self.contract)

    def test_update_requires_nonempty_business_values(self) -> None:
        payload = deepcopy(self.batches[-1][1])
        payload["operations"][0]["values"] = {}
        with self.assertRaises(BatchError):
            validate_payload(payload, self.contract)

    def test_unknown_source_contract_version_is_rejected(self) -> None:
        self.contract["contract_version"] = "2.0.0"
        with self.assertRaisesRegex(BatchError, "Unsupported source contract version"):
            validate_payload(self.batches[0][1], self.contract)

    def test_money_and_integers_are_exact_and_bounded(self) -> None:
        money = {"name": "price", "postgres_type": "numeric(18,2)", "nullable": False}
        self.assertEqual(typed_value("10.50", money), Decimal("10.50"))
        for value in [10.5, True, "NaN", "Infinity", "1.234", "10000000000000000.00"]:
            with self.subTest(value=value), self.assertRaises(BatchError):
                typed_value(value, money)
        integer = {"name": "item_id", "postgres_type": "integer", "nullable": False}
        for value in [True, 1.0, "1", 2**31]:
            with self.subTest(value=value), self.assertRaises(BatchError):
                typed_value(value, integer)

    def test_timestamp_and_null_policies(self) -> None:
        self.assertEqual(source_time("2020-01-01T07:00:00+07:00").hour, 0)
        self.assertEqual(source_time("2020-01-01T00:00:00Z").tzinfo, timezone.utc)
        timestamp = {
            "name": "approved_at",
            "postgres_type": "timestamp without time zone",
            "nullable": True,
        }
        self.assertIsNone(typed_value(None, timestamp))
        with self.assertRaises(BatchError):
            typed_value("2020-01-01T00:00:00+00:00", timestamp)
        with self.assertRaises(BatchError):
            typed_value(
                None, {"name": "id", "postgres_type": "text", "nullable": False}
            )

    def test_manifest_boolean_counts_do_not_equal_integer_counts(self) -> None:
        name, payload, expected = self.batches[1]
        path = self.directory / name
        write_batch(path, payload, self.contract, expected)
        manifest_path = path / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["operation_counts"]["order_items"]["INSERT"] = True
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(BatchError, "operation_counts mismatch"):
            load_batch(path, self.contract)


if __name__ == "__main__":
    unittest.main()
