import json
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from olist_platform.bronze.storage import BronzeError
from olist_platform.pipeline.control import (
    DBT_SOURCES,
    code_hash,
    dbt_evidence,
    reconcile,
)


def dataset_evidence(rows=2):
    return {
        "bronze": {
            "source_row_count": rows,
            "row_count": rows,
            "prefix": "bronze/fixture",
            "run_upper_cursor": None,
        },
        "silver": {
            "counts": {
                "input_rows": rows,
                "valid_rows": rows,
                "rejected_rows": 0,
                "deferred_rows": 0,
                "duplicates_rows": 0,
                "warning_rows": 0,
            },
            "rule_violations": {},
            "prefix": "silver/fixture",
        },
        "load": {
            "load_candidate_rows": rows,
            "warehouse_inserted_rows": rows,
            "warehouse_updated_rows": 0,
            "warehouse_unchanged_rows": 0,
            "warehouse_stale_ignored_rows": 0,
        },
        "bronze_sha256": "a" * 64,
        "silver_sha256": "b" * 64,
    }


class PipelineContractTest(unittest.TestCase):
    def test_review_snapshot_is_separate_from_dimensions_and_delta_growth(self):
        item = dataset_evidence(rows=3)
        item["bronze"].update(source="postgresql.order_reviews", snapshot_kind="FULL")
        counts = reconcile(item)
        self.assertEqual(counts["review_snapshot_rows"], 3)
        for key in (
            "dimension_snapshot_rows",
            "source_after_cursor_rows",
            "source_backfill_rows",
        ):
            self.assertNotIn(key, counts)

    def test_incremental_accounting_separates_window_categories_and_parent_scan(self):
        item = dataset_evidence(rows=3)
        item["bronze"].update(
            snapshot_kind="INCREMENTAL",
            source_bootstrap_rows=0,
            source_after_cursor_rows=1,
            source_lookback_rows=2,
        )
        item["full_parent_source_rows"] = 10
        counts = reconcile(item)
        self.assertEqual(counts["source_rows_extracted"], 3)
        self.assertEqual(counts["source_lookback_rows"], 2)
        self.assertEqual(counts["full_parent_source_rows"], 10)
        item["bronze"]["source_lookback_rows"] = 3
        with self.assertRaisesRegex(BronzeError, "accounting"):
            reconcile(item)

    def test_fingerprint_pins_airflow_code_and_lock_but_excludes_runtime_artifacts(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for filename in (
                "pyproject.toml",
                "uv.lock",
                "Makefile",
                "airflow/pyproject.toml",
                "airflow/uv.lock",
                "airflow/dags/core.py",
            ):
                path = root / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("original")
            original = code_hash(root)
            runtime = root / "airflow/.venv/lib/runtime.py"
            runtime.parent.mkdir(parents=True)
            runtime.write_text("not part of application source")
            self.assertEqual(code_hash(root), original)
            for filename in ("airflow/dags/core.py", "airflow/uv.lock"):
                path = root / filename
                path.write_text("changed")
                self.assertNotEqual(code_hash(root), original)
                path.write_text("original")

    def test_exact_reconciliation_includes_retry_without_inflating_source(self):
        item = dataset_evidence()
        retry = {
            "input_rows": 3,
            "valid_rows": 1,
            "rejected_rows": 1,
            "deferred_rows": 1,
            "duplicates_rows": 0,
            "warning_rows": 0,
        }
        item["resolution"] = {
            "result": {"deferred_retry_input_rows": 3, "routing_counts": retry}
        }
        item["retry_load"] = dict(
            item["load"], load_candidate_rows=1, warehouse_inserted_rows=1
        )
        counts = reconcile(item)
        self.assertEqual(
            (
                counts["source_rows_extracted"],
                counts["processing_input_rows"],
                counts["silver_valid_rows"],
            ),
            (2, 5, 3),
        )
        for target, key in (
            ("bronze", "row_count"),
            ("load", "warehouse_inserted_rows"),
            ("retry_load", "load_candidate_rows"),
        ):
            bad = deepcopy(item)
            bad[target][key] += 1
            with self.assertRaisesRegex(BronzeError, "reconciliation"):
                reconcile(bad)

    def test_dbt_gate_rejects_partial_failed_stale_and_wrong_database_builds(self):
        earliest = datetime(2020, 1, 1, tzinfo=timezone.utc)
        manifest = {
            "metadata": {"invocation_id": "one", "project_name": "olist_analytics"},
            "nodes": {
                "model.one": {
                    "resource_type": "model",
                    "config": {},
                    "database": "fixture",
                    "schema": "analytics",
                },
                "test.one": {"resource_type": "test", "config": {}},
            },
            "sources": {
                table: {"name": table, "database": "fixture", "schema": "raw_staging"}
                for table in DBT_SOURCES
            },
        }
        results = {
            "metadata": {
                "invocation_id": "one",
                "invocation_started_at": "2020-01-01T00:00:01Z",
            },
            "args": {"which": "build"},
            "results": [
                {"unique_id": "model.one", "status": "success"},
                {"unique_id": "test.one", "status": "pass"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def verify(m, r):
                (root / "manifest.json").write_text(json.dumps(m))
                (root / "run_results.json").write_text(json.dumps(r))
                return dbt_evidence(root, earliest, "fixture")

            self.assertEqual(verify(manifest, results)["node_count"], 2)
            for change in (
                "missing",
                "skipped",
                "failed",
                "stale",
                "invocation",
                "database",
                "missing_source",
                "missing_seller_source",
                "missing_geolocation_source",
                "extra_source",
                "not_build",
            ):
                m = deepcopy(manifest)
                r = deepcopy(results)
                if change == "missing":
                    r["results"].pop()
                elif change == "skipped":
                    r["results"][1]["status"] = "skipped"
                elif change == "failed":
                    r["results"][1]["status"] = "fail"
                elif change == "stale":
                    r["metadata"]["invocation_started_at"] = "2019-01-01T00:00:00Z"
                elif change == "invocation":
                    r["metadata"]["invocation_id"] = "other"
                elif change == "database":
                    m["sources"]["orders"]["database"] = "another"
                elif change == "missing_source":
                    del m["sources"]["products"]
                elif change == "missing_seller_source":
                    del m["sources"]["sellers"]
                elif change == "missing_geolocation_source":
                    del m["sources"]["geolocation"]
                elif change == "extra_source":
                    m["sources"]["reviews"] = m["sources"]["products"] | {
                        "name": "reviews"
                    }
                else:
                    r["args"]["which"] = "run"
                with self.subTest(change=change), self.assertRaises(BronzeError):
                    verify(m, r)
