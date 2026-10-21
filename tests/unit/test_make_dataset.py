from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.make_dataset import main


class MakeDatasetTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        previous = Path.cwd()
        self.addCleanup(os.chdir, previous)
        os.chdir(directory.name)
        Path("config").mkdir()
        Path("data/raw").mkdir(parents=True)
        self.names = [
            f"olist_{table}_dataset.csv"
            for table in (
                "customers",
                "orders",
                "order_items",
                "order_payments",
                "products",
                "sellers",
                "geolocation",
                "order_reviews",
            )
        ]
        self.names.append("product_category_name_translation.csv")
        Path("config/raw_dataset_manifest.json").write_text(
            json.dumps({"files": [{"name": name} for name in self.names]})
        )
        for name in self.names:
            Path("data/raw", name).touch()
        Path(".env.example").write_text("POSTGRES_PASSWORD=example\n")
        environment = patch.dict(
            os.environ,
            {
                "JAVA_HOME": "/fake/jdk17",
                "BRONZE_SOURCE_DSN": "unrelated-database",
                "WAREHOUSE_DSN": "unrelated-warehouse",
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        self.commands = []

    def command(self, *args, capture=False):
        self.commands.append(args)
        if args[:2] == ("docker", "compose"):
            self.assertIn("--profile", args, "Job services are hidden without profiles")
            self.assertEqual(args[args.index("--profile") + 1], "*")
            return json.dumps(
                {
                    "services": {
                        "postgres": {
                            "environment": {"POSTGRES_DB": "db"},
                            "ports": [{"published": "5544"}],
                        },
                        "source-loader": {
                            "environment": {
                                "SOURCE_LOADER_USER": "loader",
                                "SOURCE_LOADER_PASSWORD": "space $quote'",
                            }
                        },
                        "source-check": {"environment": {"PGUSER": "reader"}},
                        "minio": {
                            "environment": {
                                "MINIO_ROOT_USER": "access",
                                "MINIO_ROOT_PASSWORD": "secret",
                            },
                            "ports": [{"published": "9900"}],
                        },
                    }
                }
            )
        if "extract" in args or "process" in args:
            table = args[args.index("--table") + 1]
            layer = "bronze" if "extract" in args else "silver"
            return json.dumps({"manifest": {"prefix": f"{layer}/{table}/run_id=test"}})
        if "olist_platform.warehouse.cli" in args:
            return json.dumps(
                {"status": "LOADED", "counts": {"load_candidate_rows": 1}}
            )
        return ""

    def invoke(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            main()
        return output.getvalue()

    def test_missing_files_skip_without_starting_services_or_creating_env(self):
        Path("data/raw", self.names[-1]).unlink()
        with patch("scripts.make_dataset.run") as command:
            output = self.invoke()
        command.assert_not_called()
        self.assertIn(self.names[-1], output)
        self.assertIn("[FOUND] order_items", output)
        self.assertFalse(Path(".env").exists())

    def test_shell_entrypoint_resolves_repository_from_another_directory(self):
        source = Path(__file__).resolve().parents[2] / "scripts"
        Path("scripts").mkdir()
        for name in ("make_dataset.sh", "make_dataset.py"):
            shutil.copyfile(source / name, Path("scripts") / name)
        Path("data/raw", self.names[0]).unlink()
        script = Path("scripts/make_dataset.sh").resolve()
        result = subprocess.run(
            ["sh", str(script)], cwd="/tmp", capture_output=True, text=True, timeout=10
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("[SKIP] Source seed", result.stdout)

    def test_full_run_wires_parent_prefix_and_uses_compose_credentials(self):
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            output = self.invoke()
        processing = [args for args in self.commands if "process" in args]
        self.assertEqual(
            [args[args.index("--table") + 1] for args in processing],
            ["customers", "orders", "order_items", "order_payments"],
        )
        self.assertEqual(
            processing[1][-2:], ("--customer-snapshot", "silver/customers/run_id=test")
        )
        for args in processing[2:]:
            self.assertEqual(
                args[-2:], ("--order-snapshot", "silver/orders/run_id=test")
            )
        orders = next(
            args for args in self.commands if "extract" in args and "orders" in args
        )
        self.assertIn("--full-snapshot", orders)
        self.assertEqual(os.environ["PGPORT"], "5544")
        self.assertEqual(os.environ["PGPASSWORD"], "space $quote'")
        self.assertNotIn("BRONZE_SOURCE_DSN", os.environ)
        self.assertNotIn("WAREHOUSE_DSN", os.environ)
        loads = [
            args for args in self.commands if "olist_platform.warehouse.cli" in args
        ]
        self.assertEqual(
            [args[args.index("--table") + 1] for args in loads],
            ["customers", "orders", "order_items", "order_payments"],
        )
        for args in loads:
            table = args[args.index("--table") + 1]
            self.assertEqual(
                args[args.index("--silver-prefix") + 1], f"silver/{table}/run_id=test"
            )
        self.assertEqual(len(list(Path("build/reports/dataset").glob("*.json"))), 12)
        self.assertFalse(Path("build/reports/dataset/pending-run.txt").exists())
        self.assertIn("[DONE]", output)
        self.assertEqual(os.environ["DBT_READER_USER"], "reader")
        self.assertTrue(
            any("dbt" in args and "build" in args for args in self.commands)
        )

    def test_absent_java_skips_silver_but_extracts_all_four_tables(self):
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value=None),
        ):
            output = self.invoke()
        self.assertEqual(len([args for args in self.commands if "extract" in args]), 4)
        self.assertFalse(any("process" in args for args in self.commands))
        self.assertFalse(
            any("olist_platform.warehouse.cli" in args for args in self.commands)
        )
        self.assertIn("[SKIP] Silver", output)

    def test_failed_processing_preserves_run_id_for_retry(self):
        def fail(*args, **kwargs):
            if "process" in args:
                raise subprocess.CalledProcessError(1, "processing")
            return self.command(*args, **kwargs)

        with (
            patch("scripts.make_dataset.run", side_effect=fail),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                self.invoke()
        pending = Path("build/reports/dataset/pending-run.txt")
        saved = pending.read_text().strip()
        self.commands.clear()
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        for args in self.commands:
            if "--run-id" in args:
                self.assertEqual(args[args.index("--run-id") + 1], saved)
        self.assertFalse(pending.exists())

    def test_failed_warehouse_load_preserves_same_run_for_retry(self):
        def fail(*args, **kwargs):
            if "olist_platform.warehouse.cli" in args:
                raise subprocess.CalledProcessError(1, "warehouse")
            return self.command(*args, **kwargs)

        with (
            patch("scripts.make_dataset.run", side_effect=fail),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                self.invoke()
        pending = Path("build/reports/dataset/pending-run.txt")
        run_id = pending.read_text().strip()
        self.commands.clear()
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        for args in self.commands:
            if "--run-id" in args:
                self.assertEqual(args[args.index("--run-id") + 1], run_id)
        self.assertFalse(pending.exists())

    def test_failed_dbt_build_keeps_run_pending_and_retry_reuses_load_ids(self):
        def fail(*args, **kwargs):
            if "dbt" in args and "build" in args:
                raise subprocess.CalledProcessError(1, "dbt")
            return self.command(*args, **kwargs)

        with (
            patch("scripts.make_dataset.run", side_effect=fail),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                self.invoke()
        pending = Path("build/reports/dataset/pending-run.txt")
        run_id = pending.read_text().strip()
        self.commands.clear()
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        for args in self.commands:
            if "--run-id" in args:
                self.assertEqual(args[args.index("--run-id") + 1], run_id)
        self.assertFalse(pending.exists())
