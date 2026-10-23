from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from olist_platform.benchmark.events import ENGINES, execute, run, supervise
from olist_platform.source_simulator.events import generate

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/benchmark"


class EventEnginesTest(unittest.TestCase):
    def test_each_engine_matches_independent_fixture(self):
        expected = json.loads((FIXTURES / "expected.json").read_text())
        for engine in ENGINES:
            with self.subTest(engine=engine):
                result = execute(engine, FIXTURES / "events.jsonl", 2)
                self.assertEqual(result["rows"], expected)
                self.assertGreaterEqual(result["startup_seconds"], 0)
                self.assertGreater(result["work_seconds"], 0)
                if engine == "spark":
                    self.assertGreater(result["input_partitions"], 0)

    def test_invalid_resource_requests_and_existing_reports_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for repeats, cpus, memory in (
                (0, 1, 4096),
                (True, 1, 4096),
                (1, 0, 4096),
                (1, 9, 4096),
                (1, 1, 0),
            ):
                with (
                    self.subTest(repeats=repeats, cpus=cpus, memory=memory),
                    self.assertRaises(ValueError),
                ):
                    run(root / "missing", root / "out", repeats, cpus, memory)
            with self.assertRaisesRegex(ValueError, "preserve"):
                run(root / "missing", root, 1, 1, 4096)

    def test_supervisor_measures_own_tree_and_stops_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "child.log"
            normal = supervise(
                [
                    sys.executable,
                    "-c",
                    "import subprocess,sys; subprocess.run([sys.executable,'-c','import time; payload=bytearray(32*1024**2); time.sleep(0.3)'],check=True)",
                ],
                log,
                os.environ.copy(),
                1024,
            )
            self.assertEqual(normal["returncode"], 0)
            self.assertIsNone(normal["failure"])
            self.assertGreater(normal["peak_tree_rss_bytes"], 32 * 1024**2)
            timed = supervise(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                log,
                os.environ.copy(),
                1024,
                timeout=0.1,
            )
            self.assertNotEqual(timed["returncode"], 0)
            self.assertEqual(timed["failure"], "worker timeout")
            limited = supervise(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                log,
                os.environ.copy(),
                1,
            )
            self.assertNotEqual(limited["returncode"], 0)
            self.assertEqual(limited["failure"], "sampled RSS budget exceeded")
            with patch("psutil.virtual_memory") as available:
                available.return_value.available = 0
                stopped = supervise(
                    [sys.executable, "-c", "import time; time.sleep(30)"],
                    log,
                    os.environ.copy(),
                    1024,
                )
            self.assertNotEqual(stopped["returncode"], 0)
            self.assertIn("host available", stopped["failure"])

    def test_harness_rejects_wrong_engine_result_and_keeps_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "input"
            generate(artifact, 100)

            def incorrect(command, log, env, memory):
                destination = Path(command[command.index("--result-path") + 1])
                destination.write_text(json.dumps({"rows": []}))
                return dict(
                    returncode=0,
                    failure=None,
                    wall_seconds=1,
                    peak_tree_rss_bytes=1,
                    rss_samples=1,
                )

            with patch(
                "olist_platform.benchmark.events.supervise", side_effect=incorrect
            ):
                result = run(
                    artifact, root / "output", repeats=1, cpus=1, memory_mib=1024
                )
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(len(result["trials"]), 3)
            self.assertTrue(
                all(
                    t["failure"] == "result differs from independent oracle"
                    for t in result["trials"]
                )
            )
            self.assertTrue((root / "output/report.json").is_file())
            self.assertTrue(
                all("work_seconds" not in s for s in result["summary"].values())
            )

    def test_low_host_memory_fails_before_verification_or_output_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch("psutil.virtual_memory") as memory,
                patch("olist_platform.benchmark.events.verify") as validate,
            ):
                memory.return_value.available = 0
                with self.assertRaisesRegex(ValueError, "available RAM"):
                    run(root / "missing", root / "output", 1, 1, 1024)
            validate.assert_not_called()
            self.assertFalse((root / "output").exists())
