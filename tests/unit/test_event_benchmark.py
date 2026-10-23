from __future__ import annotations

import json
import unittest
from pathlib import Path

from olist_platform.benchmark.events import normalized, oracle

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/benchmark"


class EventBenchmarkOracleTest(unittest.TestCase):
    def test_oracle_matches_independent_expected_groups(self):
        expected = json.loads((FIXTURES / "expected.json").read_text())
        self.assertEqual(oracle(FIXTURES / "events.jsonl"), expected)
        self.assertEqual(sum(r["event_count"] for r in expected), 7)

    def test_normalization_has_stable_order_and_integer_metrics(self):
        expected = json.loads((FIXTURES / "expected.json").read_text())
        strings = [{k: str(v) for k, v in row.items()} for row in reversed(expected)]
        self.assertEqual(normalized(strings), expected)
