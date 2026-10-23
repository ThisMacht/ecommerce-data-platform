from __future__ import annotations

import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from botocore.exceptions import ClientError

from olist_platform.bronze.orders import configuration, cursor_tuple, cursor_value
from olist_platform.bronze.storage import (
    BronzeError,
    ObjectStore,
    encode,
    sha256,
    verify_published,
)


class BronzeOrdersTest(unittest.TestCase):
    def test_cursor_orders_instants_then_sequence_then_key(self):
        row = {
            "source_updated_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "source_sequence": 2,
            "order_id": "b",
        }
        cursor = cursor_value(row)
        self.assertEqual(cursor_tuple(cursor), (row["source_updated_at"], 2, "b"))
        equal_instant = dict(cursor, updated_at="2019-12-31T21:00:00-03:00")
        self.assertEqual(cursor_tuple(cursor), cursor_tuple(equal_instant))
        zulu = dict(cursor, updated_at="2020-01-01T00:00:00Z")
        self.assertEqual(cursor_tuple(cursor), cursor_tuple(zulu))
        with self.assertRaises(ValueError):
            cursor_tuple(dict(cursor, updated_at="not-a-timestamp"))
        self.assertLess(
            cursor_tuple(dict(cursor, source_sequence=1)), cursor_tuple(cursor)
        )
        self.assertLess(
            cursor_tuple(dict(cursor, primary_key={"order_id": "a"})),
            cursor_tuple(cursor),
        )
        self.assertIsNone(cursor_value(None))

    def test_configuration_rejects_invalid_pages_and_unknown_versions(self):
        store = ObjectStore(Mock(), "fixture")
        contract = {"contract_version": "1.0.0", "policies": {"lateness_sla_hours": 48}}
        for size in (True, 0, -1, 10001, 1.5):
            with self.subTest(size=size), self.assertRaises(BronzeError):
                configuration(contract, store, "http://localhost:9000", size)
        with self.assertRaises(BronzeError):
            configuration(dict(contract, contract_version="2.0.0"), store, "local", 1)
        for endpoint in (
            "file:///tmp/data",
            "http://user:password@localhost:9000",
            "http://localhost?token=secret",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(BronzeError):
                configuration(contract, store, endpoint, 1)

    def test_conditional_put_accepts_only_identical_existing_bytes(self):
        client = Mock()
        client.put_object.side_effect = ClientError(
            {"Error": {"Code": "PreconditionFailed"}}, "PutObject"
        )
        client.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"same\n")}
        store = ObjectStore(client, "bucket")
        store.put_immutable("part", b"same\n")
        self.assertEqual(client.put_object.call_args.kwargs["IfNoneMatch"], "*")
        with self.assertRaisesRegex(BronzeError, "conflict"):
            store.put_immutable("part", b"different\n")

    def test_storage_errors_are_not_mistaken_for_conflicts(self):
        client = Mock()
        client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "PutObject"
        )
        with self.assertRaises(ClientError):
            ObjectStore(client, "bucket").put_immutable("part", b"bytes")
        client.get_object.assert_not_called()

    def test_missing_marker_and_corrupt_manifest_objects_or_counts_fail(self):
        prefix = "bronze/orders/ingestion_date=2020-01-01/run_id=fixture"
        body = encode({"order_id": "a"})
        manifest = {
            "prefix": prefix,
            "format_version": 1,
            "row_count": 1,
            "source_row_count": 1,
            "source_schema": [],
            "schema_fingerprint": sha256(encode([])),
            "page_count": 1,
            "objects": [
                {
                    "key": f"{prefix}/attempt=0001/part-000001.jsonl",
                    "sha256": sha256(body),
                    "size_bytes": len(body),
                    "row_count": 1,
                }
            ],
        }
        objects = {manifest["objects"][0]["key"]: body}
        store = Mock()
        store.read.side_effect = objects.__getitem__
        with self.assertRaises(KeyError):
            verify_published(store, prefix)
        objects[f"{prefix}/manifest.json"] = encode(manifest)
        objects[f"{prefix}/_SUCCESS"] = encode(
            {"manifest_sha256": sha256(encode(manifest))}
        )
        self.assertEqual(verify_published(store, prefix), manifest)
        objects[f"{prefix}/manifest.json"] += b" "
        with self.assertRaisesRegex(BronzeError, "manifest checksum"):
            verify_published(store, prefix)
        objects[f"{prefix}/manifest.json"] = encode(manifest)
        objects[manifest["objects"][0]["key"]] = b"corrupted"
        with self.assertRaisesRegex(BronzeError, "object checksum"):
            verify_published(store, prefix)
        objects[manifest["objects"][0]["key"]] = body
        manifest["row_count"] = 2
        objects[f"{prefix}/manifest.json"] = encode(manifest)
        objects[f"{prefix}/_SUCCESS"] = encode(
            {"manifest_sha256": sha256(encode(manifest))}
        )
        with self.assertRaisesRegex(BronzeError, "batch count"):
            verify_published(store, prefix)

    def test_lineage_serialization_preserves_unicode_nulls_and_literal_strings(self):
        value = {"text": "São Paulo\nquoted ' \"", "nullable": None, "postal": "00123"}
        encoded = encode(value)
        self.assertEqual(json.loads(encoded), value)
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertEqual(len(encoded.splitlines()), 1)
