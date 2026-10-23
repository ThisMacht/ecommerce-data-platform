from __future__ import annotations

import contextlib
import fcntl
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import make_dataset
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
        if "olist_platform.pipeline.cli" in args:
            if "freeze" in args:
                return json.dumps({"parent_run_id": "fk_fixture"})
            return json.dumps(
                {"status": "STARTED" if "begin" in args else "SUCCESS", "attempt": 1}
            )
        if "extract" in args or "process" in args:
            table = args[args.index("--table") + 1]
            layer = "bronze" if "extract" in args else "silver"
            return json.dumps({"manifest": {"prefix": f"{layer}/{table}/run_id=test"}})
        if "olist_platform.warehouse.cli" in args:
            return json.dumps(
                {"status": "LOADED", "counts": {"load_candidate_rows": 1}}
            )
        if "olist_platform.deferred.cli" in args:
            return json.dumps(
                {"status": "COMMITTED", "routing_counts": {"input_rows": 0}}
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

    def test_incremental_freezes_first_and_pins_full_parents_for_delta_and_retry(self):
        def command(*args, capture=False):
            if "extract" in args or "process" in args:
                self.commands.append(args)
                table = args[args.index("--table") + 1]
                identity = args[args.index("--run-id") + 1]
                layer = "bronze" if "extract" in args else "silver"
                return json.dumps(
                    {"manifest": {"prefix": f"{layer}/{table}/run_id={identity}"}}
                )
            return self.command(*args, capture=capture)

        with (
            patch.dict(os.environ, {"DATASET_MODE": "incremental"}),
            patch("scripts.make_dataset.run", side_effect=command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        freezes = [a for a in self.commands if "freeze" in a]
        self.assertEqual(len(freezes), 1)
        extracts = [a for a in self.commands if "extract" in a]
        self.assertEqual(len(extracts), 10)
        self.assertLess(
            self.commands.index(freezes[0]), self.commands.index(extracts[0])
        )
        self.assertTrue(all("--pipeline-parent" in a for a in extracts[:4]))
        self.assertTrue(all("--pipeline-run-id" in a for a in extracts))
        self.assertFalse(any("--full-snapshot" in a for a in extracts))
        loads = [a for a in self.commands if "olist_platform.warehouse.cli" in a]
        self.assertEqual(len(loads), 8)
        self.assertFalse(any("fk_fixture" in a for a in loads))
        dependent = [
            a
            for a in self.commands
            if "--customer-snapshot" in a
            or "--order-snapshot" in a
            or "--parent-snapshot" in a
        ]
        self.assertTrue(dependent)
        self.assertTrue(
            all(any("run_id=fk_fixture" in value for value in a) for a in dependent)
        )

    def test_backfill_requires_bounds_before_starting_services(self):
        with (
            patch.dict(
                os.environ,
                {"DATASET_MODE": "backfill", "BACKFILL_START": "", "BACKFILL_END": ""},
            ),
            patch("scripts.make_dataset.run") as command,
        ):
            with self.assertRaisesRegex(ValueError, "BACKFILL_START and BACKFILL_END"):
                self.invoke()
        command.assert_not_called()
        self.assertFalse(Path("build/reports/dataset/pending-run.txt").exists())

    def test_backfill_wires_scoped_streams_and_original_silver_retry_filter(self):
        with (
            patch.dict(
                os.environ,
                {
                    "DATASET_MODE": "backfill",
                    "BACKFILL_START": "2020-01-01T00:00:00Z",
                    "BACKFILL_END": "2020-01-02T00:00:00Z",
                },
            ),
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        extracts = [a for a in self.commands if "extract" in a]
        self.assertEqual(len(extracts), 10)
        self.assertTrue(
            all(a[a.index("--pipeline-mode") + 1] == "backfill" for a in extracts)
        )
        resolvers = [a for a in self.commands if "resolve" in a]
        self.assertEqual(len(resolvers), 4)
        for args in resolvers:
            self.assertEqual(
                args[args.index("--origin-run-id") + 1],
                "fk_fixture"
                if "order_reviews" in args
                else args[args.index("--run-id") + 1],
            )

    def test_incremental_missing_java_fails_without_airflow_context(self):
        with (
            patch.dict(os.environ, {"DATASET_MODE": "incremental"}),
            patch("scripts.make_dataset.shutil.which", return_value=None),
            patch("scripts.make_dataset.run") as command,
        ):
            with self.assertRaisesRegex(ValueError, "requires Java"):
                self.invoke()
            command.assert_not_called()

    def test_airflow_identity_is_stable_scoped_and_not_shell_input(self):
        with patch.dict(
            os.environ,
            {
                "OLIST_AIRFLOW_DAG_ID": "olist_core_full_snapshot",
                "OLIST_AIRFLOW_RUN_ID": "manual__2026-01-01 '$(touch bad)'",
            },
        ):
            first = make_dataset.airflow_run_id()
            self.assertEqual(first, make_dataset.airflow_run_id())
            self.assertRegex(first, r"^airflow_[0-9a-f]{64}$")
            os.environ["OLIST_AIRFLOW_DAG_ID"] = "other"
            self.assertNotEqual(first, make_dataset.airflow_run_id())
            os.environ["OLIST_AIRFLOW_RUN_ID"] = ""
            with self.assertRaisesRegex(ValueError, "Both Airflow"):
                make_dataset.airflow_run_id()

    def test_airflow_missing_csv_or_java_fails_before_services(self):
        with (
            patch.dict(
                os.environ,
                {
                    "OLIST_AIRFLOW_DAG_ID": "olist_core_full_snapshot",
                    "OLIST_AIRFLOW_RUN_ID": "test",
                },
            ),
            patch("scripts.make_dataset.run") as command,
        ):
            Path("data/raw", self.names[0]).unlink()
            with self.assertRaisesRegex(ValueError, "complete raw"):
                self.invoke()
            Path("data/raw", self.names[0]).touch()
            with patch("scripts.make_dataset.shutil.which", return_value=None):
                with self.assertRaisesRegex(ValueError, "requires Java"):
                    self.invoke()
            command.assert_not_called()

    def test_airflow_cannot_adopt_unrelated_pending_run(self):
        pending = Path("build/reports/dataset/pending-run.txt")
        pending.parent.mkdir(parents=True)
        pending.write_text("dataset_unfinished\n")
        with (
            patch.dict(
                os.environ,
                {
                    "OLIST_AIRFLOW_DAG_ID": "olist_core_full_snapshot",
                    "OLIST_AIRFLOW_RUN_ID": "test",
                },
            ),
            patch("scripts.make_dataset.run") as command,
        ):
            with self.assertRaisesRegex(ValueError, "pending dataset"):
                self.invoke()
            command.assert_not_called()
        self.assertEqual(pending.read_text(), "dataset_unfinished\n")

    def test_airflow_retry_after_commit_reuses_identity_and_noops(self):
        with (
            patch.dict(
                os.environ,
                {
                    "OLIST_AIRFLOW_DAG_ID": "olist_core_full_snapshot",
                    "OLIST_AIRFLOW_RUN_ID": "test",
                },
            ),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            identity = make_dataset.airflow_run_id()
            with patch("scripts.make_dataset.run", side_effect=self.command):
                self.invoke()
            self.assertTrue(
                all(
                    args[args.index("--run-id") + 1] == identity
                    for args in self.commands
                    if "--run-id" in args
                )
            )
            self.commands.clear()

            def replay(*args, capture=False):
                if "olist_platform.pipeline.cli" in args and "begin" in args:
                    self.commands.append(args)
                    return '{"status":"NOOP"}'
                return self.command(*args, capture=capture)

            with patch("scripts.make_dataset.run", side_effect=replay):
                self.assertIn("already committed", self.invoke())
            self.assertFalse(any("extract" in args for args in self.commands))
            self.assertFalse(Path("build/reports/dataset/pending-run.txt").exists())

    def test_airflow_failed_attempt_resumes_same_identity(self):
        with (
            patch.dict(
                os.environ,
                {
                    "OLIST_AIRFLOW_DAG_ID": "olist_core_full_snapshot",
                    "OLIST_AIRFLOW_RUN_ID": "retry",
                },
            ),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            identity = make_dataset.airflow_run_id()

            def fail_load(*args, capture=False):
                if "olist_platform.warehouse.cli" in args:
                    raise subprocess.CalledProcessError(1, args)
                return self.command(*args, capture=capture)

            with patch("scripts.make_dataset.run", side_effect=fail_load):
                with self.assertRaises(subprocess.CalledProcessError):
                    self.invoke()
            pending = Path("build/reports/dataset/pending-run.txt")
            self.assertEqual(pending.read_text().strip(), identity)
            with patch("scripts.make_dataset.run", side_effect=self.command):
                self.invoke()
            self.assertFalse(pending.exists())
            self.assertTrue(
                all(
                    args[args.index("--run-id") + 1] == identity
                    for args in self.commands
                    if "--run-id" in args
                )
            )

    def test_committed_stage_response_failures_audit_and_resume_pending_run(self):
        for module, verb, stage in (
            ("bronze", "extract", "orders.bronze.extract"),
            ("silver", "process", "orders.silver.process"),
            ("warehouse", None, "orders.warehouse.load"),
            ("pipeline", "finish", "dbt.build"),
        ):
            with self.subTest(stage=stage):
                self.commands.clear()
                failed = False

                def respond(args, **kwargs):
                    nonlocal failed
                    body = self.command(
                        *args, capture=kwargs.get("stdout") == subprocess.PIPE
                    )
                    selected = (
                        f"olist_platform.{module}.cli" in args
                        and (verb is None or verb in args)
                        and (
                            module == "pipeline"
                            or args[args.index("--table") + 1] == "orders"
                        )
                        and "--pipeline-parent" not in args
                        and args[args.index("--run-id") + 1] != "fk_fixture"
                    )
                    if selected and not failed:
                        failed = True
                        raise subprocess.CalledProcessError(1, args)
                    return subprocess.CompletedProcess(args, 0, stdout=body)

                with (
                    patch.dict(os.environ, {"DATASET_MODE": "incremental"}),
                    patch("scripts.make_dataset.subprocess.run", side_effect=respond),
                    patch("scripts.make_dataset.shutil.which", return_value="java"),
                ):
                    with self.assertRaises(subprocess.CalledProcessError):
                        self.invoke()
                    pending = Path("build/reports/dataset/pending-run.txt")
                    identity = pending.read_text().strip()
                    failures = [a for a in self.commands if "fail" in a]
                    self.assertEqual(len(failures), 1)
                    self.assertEqual(
                        failures[0][failures[0].index("--name") + 1], stage
                    )
                    self.assertEqual(
                        failures[0][failures[0].index("--run-id") + 1], identity
                    )
                    self.assertIsNone(make_dataset.RUN_CONTEXT)
                    self.assertIsNone(make_dataset.CURRENT_TASK)
                    self.invoke()
                    self.assertFalse(pending.exists())
                    begins = [a for a in self.commands if "begin" in a]
                    self.assertEqual(len(begins), 2)
                    self.assertTrue(
                        all(a[a.index("--run-id") + 1] == identity for a in begins)
                    )

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
            [
                "customers",
                "products",
                "sellers",
                "geolocation",
                "orders",
                "order_items",
                "order_payments",
                "order_reviews",
            ],
        )
        self.assertEqual(
            processing[4][-2:], ("--customer-snapshot", "silver/customers/run_id=test")
        )
        for args in processing[5:]:
            self.assertEqual(
                args[args.index("--order-snapshot") + 1], "silver/orders/run_id=test"
            )
        orders = next(
            args for args in self.commands if "extract" in args and "orders" in args
        )
        self.assertIn("--pipeline-run-id", orders)
        self.assertEqual(orders[orders.index("--pipeline-mode") + 1], "full")
        items = processing[5]
        for parent in ("product", "seller"):
            self.assertEqual(
                items[items.index(f"--{parent}-snapshot") + 1],
                f"silver/{parent}s/run_id=test",
            )
        self.assertEqual(os.environ["PGPORT"], "5544")
        self.assertEqual(os.environ["PGPASSWORD"], "space $quote'")
        self.assertNotIn("BRONZE_SOURCE_DSN", os.environ)
        self.assertNotIn("WAREHOUSE_DSN", os.environ)
        loads = [
            args for args in self.commands if "olist_platform.warehouse.cli" in args
        ]
        self.assertEqual(
            [args[args.index("--table") + 1] for args in loads],
            [
                "customers",
                "products",
                "sellers",
                "geolocation",
                "orders",
                "order_items",
                "order_payments",
                "order_reviews",
            ],
        )
        for args in loads:
            table = args[args.index("--table") + 1]
            self.assertEqual(
                args[args.index("--silver-prefix") + 1], f"silver/{table}/run_id=test"
            )
        self.assertEqual(len(list(Path("build/reports/dataset").glob("*.json"))), 33)
        retries = [
            args for args in self.commands if "olist_platform.deferred.cli" in args
        ]
        self.assertEqual(len(retries), 8)
        resolutions = [args for args in retries if "resolve" in args]
        self.assertEqual(
            [args[args.index("--parent-snapshot") + 1] for args in resolutions],
            [
                "silver/customers/run_id=test",
                "silver/orders/run_id=test",
                "silver/orders/run_id=test",
                "silver/orders/run_id=test",
            ],
        )
        self.assertFalse(Path("build/reports/dataset/pending-run.txt").exists())
        self.assertIn("[DONE]", output)
        self.assertEqual(os.environ["DBT_READER_USER"], "reader")
        self.assertTrue(
            any(
                "olist_platform.pipeline.cli" in args and "finish" in args
                for args in self.commands
            )
        )

    def test_spark_heap_default_preserves_explicit_runtime_settings(self):
        for configured in (None, "--driver-memory 3g pyspark-shell"):
            with (
                self.subTest(configured=configured),
                patch.dict(os.environ),
                patch("scripts.make_dataset.run", side_effect=self.command),
                patch("scripts.make_dataset.shutil.which", return_value="java"),
            ):
                os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
                if configured is not None:
                    os.environ["PYSPARK_SUBMIT_ARGS"] = configured
                self.invoke()
                self.assertEqual(
                    os.environ["PYSPARK_SUBMIT_ARGS"],
                    configured or "--driver-memory 2g pyspark-shell",
                )

    def test_absent_java_skips_silver_but_extracts_all_eight_tables(self):
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value=None),
        ):
            output = self.invoke()
        self.assertEqual(len([args for args in self.commands if "extract" in args]), 8)
        self.assertFalse(any("process" in args for args in self.commands))
        self.assertFalse(
            any("olist_platform.warehouse.cli" in args for args in self.commands)
        )
        self.assertIn("[SKIP] Silver", output)
        self.assertTrue(Path("build/reports/dataset/pending-run.txt").exists())
        self.assertTrue(any("freeze" in args for args in self.commands))
        self.assertFalse(any("finish" in args for args in self.commands))
        extracts = [args for args in self.commands if "extract" in args]
        self.assertTrue(all("--pipeline-run-id" in args for args in extracts))
        identity = Path("build/reports/dataset/pending-run.txt").read_text().strip()
        with (
            patch("scripts.make_dataset.run", side_effect=self.command),
            patch("scripts.make_dataset.shutil.which", return_value="java"),
        ):
            self.invoke()
        begins = [args for args in self.commands if "begin" in args]
        self.assertEqual(len(begins), 2)
        self.assertTrue(
            all(args[args.index("--run-id") + 1] == identity for args in begins)
        )
        self.assertFalse(Path("build/reports/dataset/pending-run.txt").exists())

    def test_local_file_lock_prevents_a_second_runner(self):
        root = Path("build/reports/dataset")
        root.mkdir(parents=True)
        with (root / "runner.lock").open("a") as guard:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with (
                patch("scripts.make_dataset.run") as command,
                self.assertRaisesRegex(RuntimeError, "Another local"),
            ):
                self.invoke()
            command.assert_not_called()

    def test_stage_execution_records_begin_and_success_without_auditing_audit_calls(
        self,
    ):
        with (
            patch.object(
                make_dataset, "RUN_CONTEXT", {"run_id": "fixture", "attempt": 2}
            ),
            patch.object(make_dataset, "CURRENT_TASK", None),
            patch("scripts.make_dataset.audit") as audit,
            patch(
                "scripts.make_dataset.subprocess.run",
                return_value=SimpleNamespace(stdout="{}"),
            ),
        ):
            make_dataset.run(
                "python",
                "-m",
                "olist_platform.bronze.cli",
                "--table",
                "orders",
                "extract",
                capture=True,
            )
            self.assertEqual(
                [call.args[-1] for call in audit.call_args_list], ["RUNNING", "SUCCESS"]
            )
            self.assertTrue(
                all(
                    "orders.bronze.extract" in call.args
                    for call in audit.call_args_list
                )
            )
            audit.reset_mock()
            make_dataset.run(
                "python",
                "-m",
                "olist_platform.pipeline.cli",
                "--run-id",
                "fixture",
                "task",
                capture=True,
            )
            audit.assert_not_called()

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
            if "olist_platform.pipeline.cli" in args and "finish" in args:
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

    def test_failed_resolution_keeps_run_pending_and_retry_reuses_ids(self):
        def fail(*args, **kwargs):
            if "olist_platform.deferred.cli" in args and "resolve" in args:
                raise subprocess.CalledProcessError(1, "resolve")
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
