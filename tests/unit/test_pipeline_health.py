import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import psycopg

from olist_platform.pipeline.control import TABLES
from olist_platform.pipeline.dashboard import render
from olist_platform.pipeline.health import evaluate, load_thresholds, main

CONFIG = Path(__file__).resolve().parents[2] / "config/pipeline_health.json"


class PipelineHealthTest(unittest.TestCase):
    def setUp(self):
        self.limits = load_thresholds(CONFIG)
        self.now = datetime(2026, 1, 2, tzinfo=timezone.utc)
        self.success = {
            "run_id": "good",
            "status": "SUCCESS",
            "attempt": 1,
            "failed_task": None,
            "started_at": self.now - timedelta(minutes=10),
            "completed_at": self.now - timedelta(minutes=5),
        }
        self.counts = {
            table: {"processing_input_rows": 100, "rejected_rows": 1, "warning_rows": 5}
            for table in TABLES
        }

    def check(self, latest=None, queue=None):
        return evaluate(
            self.now,
            latest or self.success,
            self.success,
            self.counts,
            queue or {},
            self.limits,
        )

    def test_healthy_boundaries_and_zero_inputs(self):
        self.assertEqual(self.check()["status"], "HEALTHY")
        self.counts["orders"] = dict.fromkeys(self.counts["orders"], 0)
        self.assertEqual(self.check()["datasets"]["orders"]["reject_rate"], 0)

    def test_failed_or_stuck_latest_run_cannot_hide_behind_old_success(self):
        latest = self.success | {
            "run_id": "bad",
            "status": "FAILED",
            "attempt": 2,
            "failed_task": "dbt.build",
        }
        result = self.check(latest)
        self.assertIn("pipeline.latest_run_failed", result["issues"])
        self.assertEqual(result["last_success_run_id"], "good")
        latest.update(
            status="RUNNING",
            completed_at=None,
            started_at=self.now - timedelta(hours=1),
        )
        self.assertIn("pipeline.runtime_exceeded", self.check(latest)["issues"])

    def test_stale_quality_and_pending_checks_use_distinct_metrics(self):
        self.success["completed_at"] = self.now - timedelta(days=2)
        self.counts["orders"]["rejected_rows"] = 2
        self.counts["orders"]["warning_rows"] = 6
        result = self.check(
            queue={
                "order_items": {
                    "pending_rows": 4,
                    "oldest_pending_age_hours": 169,
                    "due_pending_rows": 1,
                }
            }
        )
        for issue in (
            "customers.freshness_exceeded",
            "orders.reject_rate_exceeded",
            "orders.warning_rate_exceeded",
            "order_items.pending_age_exceeded",
            "order_items.pending_resolution_due",
        ):
            self.assertIn(issue, result["issues"])
        self.assertEqual(result["datasets"]["orders"]["reject_rate"], 0.02)

    def test_missing_success_and_incomplete_receipts_are_unhealthy(self):
        result = evaluate(self.now, None, None, {}, {}, self.limits)
        self.assertEqual(result["status"], "UNHEALTHY")
        self.assertIn("pipeline.missing_run", result["issues"])
        self.assertIn("orders.missing_success", result["issues"])
        del self.counts["orders"]
        self.assertIn("orders.missing_counters", self.check()["issues"])

    def test_cli_exit_codes_and_database_errors_do_not_expose_connection_details(self):
        with patch("sys.argv", ["olist-pipeline-health", "--thresholds", str(CONFIG)]):
            for status, expected in (("HEALTHY", 0), ("UNHEALTHY", 1)):
                output = io.StringIO()
                with (
                    patch("olist_platform.pipeline.health.psycopg.connect"),
                    patch(
                        "olist_platform.pipeline.health.report",
                        return_value={"status": status},
                    ),
                    redirect_stdout(output),
                ):
                    self.assertEqual(main(), expected)
                    self.assertEqual(json.loads(output.getvalue())["status"], status)
            error = io.StringIO()
            with (
                patch(
                    "olist_platform.pipeline.health.psycopg.connect",
                    side_effect=psycopg.OperationalError("password=private"),
                ),
                redirect_stderr(error),
            ):
                self.assertEqual(main(), 2)
            self.assertNotIn("private", error.getvalue())

    def test_dashboard_escapes_audit_values_and_distinguishes_missing_from_zero(self):
        self.counts["customers"]["rejected_rows"] = 0
        result = self.check()
        result["latest_run"]["run_id"] = '<script>alert("audit")</script>'
        result["issues"] = ['<img src=x onerror="alert(1)">']
        html = render(result)
        self.assertNotIn("<script", html)
        self.assertNotIn("<img", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("Unavailable", html)
        self.assertIn("0.000%", html)
        self.assertIn("1.000%", html)
        self.assertIn("5.000%", html)
        self.assertIn("100", html)
        self.assertIn("Static report", html)
        missing = evaluate(self.now, None, None, {}, {}, self.limits)
        self.assertIn("orders.missing_success", render(missing))
        self.assertIn("UNHEALTHY", render(missing))

    def test_cli_writes_unhealthy_dashboard_and_preserves_previous_on_read_failure(
        self,
    ):
        result = evaluate(self.now, None, None, {}, {}, self.limits)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "nested" / "report.html"
            with (
                patch(
                    "sys.argv",
                    [
                        "health",
                        "--thresholds",
                        str(CONFIG),
                        "--html-output",
                        str(target),
                    ],
                ),
                patch("olist_platform.pipeline.health.psycopg.connect"),
                patch("olist_platform.pipeline.health.report", return_value=result),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main(), 1)
                original = target.read_text()
                self.assertIn("UNHEALTHY", original)
                with (
                    patch(
                        "olist_platform.pipeline.health.report",
                        side_effect=psycopg.OperationalError("private"),
                    ),
                    redirect_stderr(io.StringIO()),
                ):
                    self.assertEqual(main(), 2)
                self.assertEqual(target.read_text(), original)
                with (
                    patch(
                        "olist_platform.pipeline.health.atomic_write_text",
                        side_effect=OSError("private"),
                    ),
                    redirect_stderr(io.StringIO()),
                ):
                    self.assertEqual(main(), 2)

    def test_thresholds_reject_missing_unknown_boolean_nonfinite_and_bad_ranges(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "limits.json"
            for value in (True, "24", None, -1, 0, float("nan"), float("inf")):
                bad = deepcopy(self.limits)
                bad["datasets"]["orders"]["max_freshness_hours"] = value
                path.write_text(json.dumps(bad))
                with self.subTest(value=value), self.assertRaises(ValueError):
                    load_thresholds(path)
            for mutation in ("missing", "unknown", "rate", "runtime", "list"):
                bad = deepcopy(self.limits)
                if mutation == "missing":
                    del bad["datasets"]["customers"]
                elif mutation == "unknown":
                    bad["datasets"]["orders"]["typo"] = 1
                elif mutation == "rate":
                    bad["datasets"]["orders"]["max_reject_rate"] = 1.1
                elif mutation == "runtime":
                    bad["max_run_seconds"] = False
                else:
                    bad = []
                path.write_text(json.dumps(bad))
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    load_thresholds(path)
