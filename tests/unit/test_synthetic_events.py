from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from unittest.mock import patch

from olist_platform.source_simulator.batches import BatchError, canonical_bytes
from olist_platform.source_simulator.events import generate, iter_events, verify


class SyntheticEventsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def records(self, directory):
        return [
            json.loads(line)
            for line in (directory / "events.jsonl").read_bytes().splitlines()
        ]

    def seal(self, directory, manifest):
        body = canonical_bytes(manifest)
        (directory / "manifest.json").write_bytes(body)
        (directory / "_SUCCESS").write_bytes(
            canonical_bytes({"manifest_sha256": hashlib.sha256(body).hexdigest()})
        )

    def test_independent_counts_profiles_utc_delays_and_ordering(self):
        directory = self.root / "events"
        manifest = generate(directory, 100)["manifest"]
        # Seven complete 14-event cycles and one anonymous page-view prefix.
        self.assertEqual(
            manifest["counts"],
            {
                "physical_rows": 100,
                "unique_events": 99,
                "duplicate_rows": 1,
                "late_events": 4,
                "out_of_order_events": 25,
                "anonymous_events": 15,
                "event_types": {
                    "page_view": 29,
                    "product_view": 28,
                    "add_to_cart": 21,
                    "checkout_started": 14,
                    "purchase": 7,
                },
            },
        )
        self.assertEqual(
            manifest["sessions"],
            {
                "completed_by_scenario": {
                    "anonymous_browse": 7,
                    "cart_abandonment": 7,
                    "checkout_abandonment": 7,
                    "purchase": 7,
                },
                "partial_session_id": "syn_session_0000002a_000000000028",
            },
        )
        rows = self.records(directory)
        self.assertEqual(len(rows), 100)
        self.assertEqual(rows[98], rows[99])
        unique = {row["event_id"]: row for row in rows}
        self.assertEqual(len(unique), 99)
        self.assertEqual(
            Counter(r["event_type"] for r in unique.values()),
            manifest["counts"]["event_types"],
        )
        self.assertEqual(
            rows, sorted(rows, key=lambda r: (r["received_at"], r["event_id"]))
        )
        profiles = [
            ["page_view", "product_view"],
            ["page_view", "product_view", "add_to_cart"],
            ["page_view", "product_view", "add_to_cart", "checkout_started"],
            [
                "page_view",
                "product_view",
                "add_to_cart",
                "checkout_started",
                "purchase",
            ],
        ]
        sessions = defaultdict(list)
        for row in unique.values():
            self.assertEqual(
                set(row),
                {
                    "event_id",
                    "session_id",
                    "anonymous_id",
                    "customer_unique_id",
                    "event_type",
                    "event_time",
                    "received_at",
                    "product_id",
                    "order_id",
                },
            )
            ordinal = int(row["event_id"].rsplit("_", 1)[1])
            session = int(row["session_id"].rsplit("_", 1)[1])
            event_time = datetime.fromisoformat(
                row["event_time"].replace("Z", "+00:00")
            )
            received = datetime.fromisoformat(row["received_at"].replace("Z", "+00:00"))
            delay = (received - event_time).total_seconds()
            if ordinal % 20 == 19:
                self.assertTrue(259200 <= delay <= 259203)
            elif ordinal % 7 == 6:
                self.assertTrue(20 <= delay <= 23)
            else:
                self.assertTrue(1 <= delay <= 4)
            step = profiles[session % 4].index(row["event_type"])
            self.assertEqual(
                event_time,
                datetime(2024, 1, 1, tzinfo=timezone.utc)
                + timedelta(seconds=session * 5 + step),
            )
            self.assertEqual(row["customer_unique_id"] is None, session % 4 == 0)
            self.assertEqual(
                row["order_id"] is not None, row["event_type"] == "purchase"
            )
            self.assertEqual(
                row["product_id"] is None, row["event_type"] == "page_view"
            )
            for key in (
                "event_id",
                "session_id",
                "anonymous_id",
                "customer_unique_id",
                "order_id",
                "product_id",
            ):
                if row[key] is not None:
                    self.assertTrue(row[key].startswith("syn_"))
            sessions[session].append(row)
        for session in range(28):
            actual = [
                r["event_type"]
                for r in sorted(sessions[session], key=lambda r: r["event_time"])
            ]
            self.assertEqual(actual, profiles[session % 4])
        self.assertEqual(verify(directory), manifest)

    def test_deterministic_bytes_replay_and_refusal_to_overwrite(self):
        first, second = self.root / "first", self.root / "second"
        generate(first, 200)
        generate(second, 200)
        files = {
            p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in first.iterdir()
        }
        self.assertEqual(
            {p.name: p.read_bytes() for p in second.iterdir()},
            {k: v[0] for k, v in files.items()},
        )
        self.assertEqual(generate(first, 200)["status"], "NOOP")
        for rows, seed in ((201, 42), (200, 43)):
            with self.assertRaisesRegex(BatchError, "overwrite"):
                generate(first, rows, seed)
        self.assertEqual(
            {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in first.iterdir()},
            files,
        )
        third = self.root / "other-seed"
        generate(third, 200, 43)
        self.assertNotEqual(
            (first / "events.jsonl").read_bytes(), (third / "events.jsonl").read_bytes()
        )

    def test_exact_physical_prefixes_and_censored_sessions(self):
        for count in (1, 14, 99, 100, 101, 200):
            with self.subTest(count=count):
                directory = self.root / str(count)
                manifest = generate(directory, count, 2**32 - 1)["manifest"]
                rows = self.records(directory)
                self.assertEqual(len(rows), count)
                self.assertEqual(
                    len({r["event_id"] for r in rows}), count - count // 100
                )
                self.assertEqual(verify(directory), manifest)
                if count == 14:
                    self.assertIsNone(manifest["sessions"]["partial_session_id"])
                    self.assertEqual(
                        set(manifest["sessions"]["completed_by_scenario"].values()), {1}
                    )

    def test_invalid_parameters_do_not_create_artifacts(self):
        for rows, seed in [(n, 42) for n in (True, 0, -1, 10_000_001, 1.5, None)] + [
            (100, n) for n in (True, -1, 2**32, 1.5, None)
        ]:
            with self.subTest(rows=rows, seed=seed), self.assertRaises(BatchError):
                generate(self.root / "invalid", rows, seed)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_incomplete_corrupt_and_forged_artifacts_fail(self):
        cases = (
            "missing-marker",
            "missing-payload",
            "extra-file",
            "bad-marker",
            "append",
            "truncate",
            "forged-payload",
            "counts",
            "schema",
            "version",
            "request",
            "metadata-size",
            "payload-symlink",
            "noncanonical-manifest",
        )
        for case in cases:
            with self.subTest(case=case):
                directory = self.root / case
                manifest = generate(directory, 100)["manifest"]
                payload = directory / "events.jsonl"
                if case == "missing-marker":
                    (directory / "_SUCCESS").unlink()
                elif case == "missing-payload":
                    payload.unlink()
                elif case == "extra-file":
                    (directory / "unexpected").write_text("partial")
                elif case == "bad-marker":
                    (directory / "_SUCCESS").write_bytes(b"{}\n")
                elif case == "append":
                    with payload.open("ab") as stream:
                        stream.write(b"\n")
                elif case == "truncate":
                    payload.write_bytes(payload.read_bytes()[:-1])
                elif case == "metadata-size":
                    (directory / "_SUCCESS").write_bytes(b"x" * 65537)
                elif case == "payload-symlink":
                    external = self.root / "external.jsonl"
                    external.write_bytes(payload.read_bytes())
                    payload.unlink()
                    payload.symlink_to(external)
                elif case == "noncanonical-manifest":
                    body = json.dumps(manifest, indent=2).encode()
                    (directory / "manifest.json").write_bytes(body)
                    (directory / "_SUCCESS").write_bytes(
                        canonical_bytes(
                            {"manifest_sha256": hashlib.sha256(body).hexdigest()}
                        )
                    )
                else:
                    if case == "forged-payload":
                        data = payload.read_bytes().replace(
                            b'"event_type":"page_view"', b'"event_type":"fake_view"', 1
                        )
                        payload.write_bytes(data)
                        manifest["event_file"].update(
                            size_bytes=len(data),
                            sha256=hashlib.sha256(data).hexdigest(),
                        )
                    elif case == "counts":
                        manifest["counts"]["duplicate_rows"] = 0
                    elif case == "schema":
                        del manifest["schema"]["order_id"]
                    elif case == "version":
                        manifest["format_version"] = True
                    elif case == "request":
                        manifest["request"]["rows"] = True
                    self.seal(directory, manifest)
                with self.assertRaises((OSError, ValueError)):
                    verify(directory)
                with self.assertRaises((OSError, ValueError)):
                    generate(directory, 100)

    def test_failures_before_publication_clean_up_temporary_files(self):
        def partial_failure(*args):
            yield next(iter_events(*args))
            raise OSError("injected")

        for target in ("iter_events", "Path.rename"):
            directory = self.root / target.replace(".", "-") / "events"
            with patch(
                "olist_platform.source_simulator.events." + target,
                side_effect=partial_failure
                if target == "iter_events"
                else OSError("injected"),
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    generate(directory, 100)
            self.assertFalse(directory.exists())
            self.assertEqual(list(directory.parent.iterdir()), [])
            self.assertEqual(generate(directory, 100)["status"], "GENERATED")

    def test_large_generation_does_not_eagerly_materialize_the_source(self):
        from olist_platform.source_simulator.events import _logical_events

        def bounded(total, seed):
            for index, event in enumerate(_logical_events(total, seed)):
                if index >= 100:
                    raise AssertionError("must not prefetch the full source")
                yield event

        with patch(
            "olist_platform.source_simulator.events._logical_events",
            side_effect=bounded,
        ):
            self.assertEqual(len(list(islice(iter_events(2_000_000, 42), 10))), 10)

    def test_arrivals_flush_delayed_events_before_future_source_events(self):
        from olist_platform.source_simulator.events import _arrivals

        source = [
            {
                "event_id": "late",
                "event_time": "2024-01-01T00:00:00Z",
                "received_at": "2024-01-04T00:00:00Z",
            },
            {
                "event_id": "normal",
                "event_time": "2024-01-01T00:00:01Z",
                "received_at": "2024-01-01T00:00:02Z",
            },
            {
                "event_id": "future",
                "event_time": "2024-01-05T00:00:00Z",
                "received_at": "2024-01-05T00:00:01Z",
            },
        ]
        with patch(
            "olist_platform.source_simulator.events._logical_events",
            return_value=iter(source),
        ):
            actual = list(_arrivals(3, 42))
        self.assertEqual([r["event_id"] for r in actual], ["normal", "late", "future"])


if __name__ == "__main__":
    unittest.main()
