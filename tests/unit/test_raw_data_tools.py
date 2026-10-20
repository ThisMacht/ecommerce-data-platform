from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from olist_platform.raw.manifest import iter_csv_rows, verify_snapshot
from olist_platform.raw.profiling import profile_file


class RawDataToolsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.raw_dir = Path(self.temporary_directory.name)

    def write_csv(self, name: str, content: str) -> Path:
        path = self.raw_dir / name
        path.write_text(content, encoding="utf-8", newline="")
        return path

    @staticmethod
    def manifest_for(path: Path, row_count: int) -> dict:
        payload = path.read_bytes()
        return {
            "dataset": {"name": "fixture", "snapshot_id": "fixture-v1"},
            "files": [
                {
                    "name": path.name,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "size_bytes": len(payload),
                    "row_count": row_count,
                    "encoding": "utf-8-sig",
                    "columns": ["id", "amount", "event_time", "note"],
                    "key": ["id"],
                    "numeric_columns": {"amount": "decimal"},
                    "timestamp_columns": ["event_time"],
                    "profiled_value_columns": ["note"],
                }
            ],
            "relationships": [],
        }

    def test_verify_snapshot_accepts_matching_file(self) -> None:
        path = self.write_csv(
            "fixture.csv",
            "id,amount,event_time,note\n"
            '1,10.50,2020-01-01 10:00:00,"hello\nworld"\n'
            "2,0.00,2020-01-02 11:30:00,ok\n",
        )
        manifest = self.manifest_for(path, row_count=2)

        results, errors = verify_snapshot(manifest, self.raw_dir)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["errors"], [])

    def test_verify_snapshot_rejects_checksum_change(self) -> None:
        path = self.write_csv(
            "fixture.csv",
            "id,amount,event_time,note\n1,10.50,2020-01-01 10:00:00,ok\n",
        )
        manifest = self.manifest_for(path, row_count=1)
        path.write_text(
            "id,amount,event_time,note\n1,99.99,2020-01-01 10:00:00,ok\n",
            encoding="utf-8",
            newline="",
        )

        _, errors = verify_snapshot(manifest, self.raw_dir)

        self.assertTrue(any("checksum mismatch" in error for error in errors))

    def test_logical_csv_reader_supports_embedded_newline(self) -> None:
        path = self.write_csv(
            "fixture.csv",
            "id,amount,event_time,note\n"
            '1,10.50,2020-01-01 10:00:00,"hello\nworld"\n'
            "2,0.00,2020-01-02 11:30:00,ok\n",
        )

        rows = list(
            iter_csv_rows(
                path,
                "utf-8-sig",
                ["id", "amount", "event_time", "note"],
            )
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1][3], "hello\nworld")

    def test_profile_file_measures_keys_types_and_values(self) -> None:
        path = self.write_csv(
            "fixture.csv",
            "id,amount,event_time,note\n"
            "1,10.50,2020-01-01 10:00:00,ok\n"
            "2,-2.25,2020-01-02 11:30:00,ok\n",
        )
        spec = self.manifest_for(path, row_count=2)["files"][0]

        profile = profile_file(self.raw_dir, spec)

        self.assertEqual(profile["status"], "PASS")
        self.assertEqual(profile["row_count"], 2)
        self.assertEqual(profile["key"]["duplicate_rows"], 0)
        self.assertEqual(profile["numeric_profiles"]["amount"]["minimum"], "-2.25")
        self.assertEqual(profile["numeric_profiles"]["amount"]["negative_count"], 1)
        self.assertEqual(
            profile["value_profiles"]["note"]["distinct_non_null_count"], 1
        )


if __name__ == "__main__":
    unittest.main()
