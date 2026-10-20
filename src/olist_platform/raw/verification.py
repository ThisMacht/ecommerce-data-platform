#!/usr/bin/env python3
"""Verify the local Olist raw snapshot against its committed manifest."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .manifest import ManifestError, load_manifest, verify_snapshot

DEFAULT_MANIFEST = Path("config/raw_dataset_manifest.json")
DEFAULT_RAW_DIR = Path("data/raw")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify raw Olist CSV files by name, size, SHA-256 and header."
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument(
        "--allow-extra-csv",
        action="store_true",
        help="Do not fail when the raw directory contains unmanifested CSV files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = load_manifest(args.manifest)
    except ManifestError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        return 2

    results, errors = verify_snapshot(
        manifest, args.raw_dir, allow_extra_csv=args.allow_extra_csv
    )
    dataset = manifest["dataset"]
    print(f"Dataset: {dataset['name']} ({dataset['snapshot_id']})")

    for result in results:
        if result["errors"]:
            print(f"[FAIL] {result['name']}")
            for error in result["errors"]:
                print(f"       - {error}")
        else:
            short_hash = str(result["actual_sha256"])[:12]
            print(
                f"[OK]   {result['name']}: "
                f"{result['actual_size']} bytes, sha256={short_hash}…"
            )

    if errors:
        file_error_count = sum(len(result["errors"]) for result in results)
        global_error_count = len(errors) - file_error_count
        print(
            f"Raw snapshot verification failed with {len(errors)} error(s) "
            f"({file_error_count} file, {global_error_count} directory).",
            file=sys.stderr,
        )
        for error in errors[:global_error_count]:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"Verified {len(results)} files successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
