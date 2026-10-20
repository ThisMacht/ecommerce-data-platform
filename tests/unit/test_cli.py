from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class InstalledCliTest(unittest.TestCase):
    """Exercise installed commands from a directory outside the checkout."""

    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.workspace = Path(temporary_directory.name)
        raw_dir = self.workspace / "data" / "raw"
        raw_dir.mkdir(parents=True)
        self.csv_path = raw_dir / "fixture.csv"
        payload = b'id,amount,note\n1,10.50,"hello\nworld"\n2,0.00,ok\n'
        self.csv_path.write_bytes(payload)
        manifest = {
            "dataset": {
                "name": "CLI fixture",
                "snapshot_id": "cli-fixture-v1",
                "validated_on": "2026-10-20",
            },
            "files": [
                {
                    "name": "fixture.csv",
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "size_bytes": len(payload),
                    "row_count": 2,
                    "encoding": "utf-8",
                    "columns": ["id", "amount", "note"],
                    "key": ["id"],
                    "numeric_columns": {"amount": "decimal"},
                }
            ],
            "relationships": [],
        }
        config_dir = self.workspace / "config"
        config_dir.mkdir()
        (config_dir / "raw_dataset_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )

    def run_command(self, name: str, *arguments: str) -> subprocess.CompletedProcess:
        suffix = ".exe" if os.name == "nt" else ""
        command = Path(sys.executable).parent / f"{name}{suffix}"
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        return subprocess.run(
            [str(command), *arguments],
            cwd=self.workspace,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def test_verify_uses_working_directory_defaults(self) -> None:
        result = self.run_command("olist-verify-raw")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Verified 1 files successfully.", result.stdout)

    def test_verify_rejects_modified_snapshot(self) -> None:
        self.csv_path.write_bytes(
            self.csv_path.read_bytes().replace(b"10.50", b"99.99")
        )
        result = self.run_command("olist-verify-raw")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("checksum mismatch", result.stdout)

    def test_profile_writes_deterministic_reports_outside_checkout(self) -> None:
        result = self.run_command("olist-profile-source")
        self.assertEqual(result.returncode, 0, result.stderr)
        json_path = self.workspace / "build" / "reports" / "source_profile.json"
        markdown_path = (
            self.workspace / "docs" / "data_contracts" / "00_source_profile.md"
        )
        profile = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(profile["summary"]["status"], "PASS")
        self.assertEqual(profile["summary"]["total_logical_rows"], 2)
        self.assertEqual(
            profile["files"]["fixture.csv"]["numeric_profiles"]["amount"]["maximum"],
            "10.50",
        )
        first_json = json_path.read_bytes()
        first_markdown = markdown_path.read_bytes()
        result = self.run_command("olist-profile-source")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json_path.read_bytes(), first_json)
        self.assertEqual(markdown_path.read_bytes(), first_markdown)
        self.assertIn(b"`build/reports/source_profile.json`", first_markdown)

    def test_contract_accepts_explicit_paths_from_another_directory(self) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        result = self.run_command(
            "olist-validate-source-contract",
            "--contract",
            str(repository_root / "config" / "source_contract.json"),
            "--manifest",
            str(repository_root / "config" / "raw_dataset_manifest.json"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Source contract: PASS", result.stdout)

    def test_schema_cli_rejects_missing_tables(self) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        catalog_path = self.workspace / "catalog.json"
        catalog_path.write_text('{"schemas": {"olist": {}}}', encoding="utf-8")
        result = self.run_command(
            "olist-verify-source-schema",
            "--contract",
            str(repository_root / "config" / "source_contract.json"),
            "--catalog",
            str(catalog_path),
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("Missing source table: olist.orders", result.stderr)

    def test_schema_cli_rejects_invalid_catalog_json(self) -> None:
        repository_root = Path(__file__).resolve().parents[2]
        catalog_path = self.workspace / "catalog.json"
        catalog_path.write_text("{", encoding="utf-8")
        result = self.run_command(
            "olist-verify-source-schema",
            "--contract",
            str(repository_root / "config" / "source_contract.json"),
            "--catalog",
            str(catalog_path),
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Source schema report is invalid", result.stderr)


if __name__ == "__main__":
    unittest.main()
