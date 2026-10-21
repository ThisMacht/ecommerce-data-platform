import unittest

from olist_platform.bronze.storage import BronzeError
from olist_platform.pipeline.control import backfill_window, reconcile
from tests.unit.test_pipeline_contract import dataset_evidence


class BackfillContractTest(unittest.TestCase):
    def test_window_normalizes_offsets_and_rejects_missing_naive_or_inverted_bounds(
        self,
    ):
        self.assertEqual(
            backfill_window("2020-01-01T07:00:00+07:00", "2020-01-02T00:00:00Z"),
            {"start": "2020-01-01T00:00:00+00:00", "end": "2020-01-02T00:00:00+00:00"},
        )
        for start, end in (
            (None, None),
            ("2020-01-01", "2020-01-02"),
            ("bad", "2020-01-02T00:00:00Z"),
            ("2020-01-02T00:00:00Z", "2020-01-01T00:00:00Z"),
            ("2020-01-02T00:00:00Z", "2020-01-02T00:00:00Z"),
        ):
            with self.subTest(start=start, end=end), self.assertRaises(BronzeError):
                backfill_window(start, end)

    def test_backfill_inputs_are_not_bootstrap_lookback_or_source_growth(self):
        item = dataset_evidence(3)
        item["bronze"].update(
            snapshot_kind="INCREMENTAL",
            source_backfill_rows=3,
            source_bootstrap_rows=0,
            source_after_cursor_rows=0,
            source_lookback_rows=0,
        )
        item["full_parent_source_rows"] = 10
        result = reconcile(item)
        self.assertEqual(result["source_backfill_rows"], 3)
        self.assertEqual(result["processing_input_rows"], 3)
        item["bronze"]["source_backfill_rows"] = 2
        with self.assertRaisesRegex(BronzeError, "accounting"):
            reconcile(item)
