import json
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from olist_platform.warehouse.metabase import API, credentials, find, native_card

ROOT = Path(__file__).resolve().parents[2]


class MetabaseTest(unittest.TestCase):
    def test_credentials_are_private_stable_and_never_reset_an_existing_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "credentials.json"
            with self.assertRaisesRegex(RuntimeError, "restore"):
                credentials(path, True)
            self.assertFalse(path.exists())
            value = credentials(path, False)
            self.assertGreater(len(value["password"]), 24)
            self.assertGreater(len(value["reader_password"]), 24)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(credentials(path, True), value)
            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "0600"):
                credentials(path, True)

    def test_native_cards_have_stable_date_tags_and_do_not_mutate_definitions(self):
        definition = json.loads((ROOT / "config/metabase_dashboard.json").read_text())
        original = deepcopy(definition)
        self.assertEqual(len({c["key"] for c in definition["cards"]}), 13)
        for spec in definition["cards"]:
            first = native_card(spec, 12, 3)
            self.assertEqual(native_card(spec, 12, 3), first)
            query = first["dataset_query"]
            self.assertEqual(query["database"], 12)
            tags = query["native"]["template-tags"]
            self.assertEqual(
                set(tags),
                set() if spec["key"] == "publication" else {"start_date", "end_date"},
            )
            if spec["key"] != "publication":
                self.assertIn(
                    "analytics.bi_publication_status", query["native"]["query"]
                )
        self.assertEqual(definition, original)

    def test_module_help_does_not_touch_docker_or_create_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, "-m", "olist_platform.warehouse.metabase", "--help"],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Provision the local Metabase", result.stdout)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_duplicate_names_are_rejected_instead_of_silently_repurposed(self):
        item = {"name": "Managed", "id": 1}
        self.assertEqual(find([item], "Managed"), item)
        self.assertIsNone(find([item], "Absent"))
        with self.assertRaisesRegex(RuntimeError, "Duplicate"):
            find([item, dict(item, id=2)], "Managed")

    def test_http_errors_do_not_expose_response_or_credentials(self):
        error = HTTPError(
            "http://127.0.0.1:3000/api/database",
            400,
            "secret",
            {},
            BytesIO(b"password=private"),
        )
        with patch("olist_platform.warehouse.metabase.urlopen", side_effect=error):
            with self.assertRaises(RuntimeError) as caught:
                API("http://127.0.0.1:3000").call(
                    "POST", "/api/database", {"password": "private"}
                )
        self.assertIn("HTTP 400", str(caught.exception))
        self.assertNotIn("private", str(caught.exception))
