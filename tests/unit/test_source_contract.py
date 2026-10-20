from __future__ import annotations

import unittest
from copy import deepcopy
from pathlib import Path

from olist_platform.raw.contracts import load_contract, validate_contract
from olist_platform.raw.manifest import load_manifest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class SourceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = load_manifest(
            REPOSITORY_ROOT / "config" / "raw_dataset_manifest.json"
        )
        cls.contract = load_contract(
            REPOSITORY_ROOT / "config" / "source_contract.json"
        )

    def test_repository_contract_matches_raw_manifest(self) -> None:
        self.assertEqual(validate_contract(self.contract, self.manifest), [])

    def test_contract_rejects_column_drift(self) -> None:
        changed_contract = deepcopy(self.contract)
        changed_contract["tables"][0]["columns"][0]["name"] = "renamed_id"

        errors = validate_contract(changed_contract, self.manifest)

        self.assertTrue(any("Column order mismatch" in error for error in errors))

    def test_contract_rejects_snapshot_drift(self) -> None:
        changed_contract = deepcopy(self.contract)
        changed_contract["dataset_snapshot_id"] = "unexpected-snapshot"

        errors = validate_contract(changed_contract, self.manifest)

        self.assertTrue(
            any("dataset_snapshot_id mismatch" in error for error in errors)
        )


if __name__ == "__main__":
    unittest.main()
