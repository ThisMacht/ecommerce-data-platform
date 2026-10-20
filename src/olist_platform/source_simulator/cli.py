"""Generate, validate, and apply deterministic Olist source batches."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

from olist_platform.raw.contracts import ContractError, load_contract

from .apply import apply_batch
from .batches import BatchError, load_batch
from .fixtures import write_fixture


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract", type=Path, default=Path("config/source_contract.json")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser(
        "generate", help="Publish four deterministic reliability batches"
    )
    generate.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/generated/source_batches/reliability_v1"),
    )
    generate.add_argument("--seed", type=int, default=42)
    validate = commands.add_parser(
        "validate", help="Verify a batch without connecting to PostgreSQL"
    )
    validate.add_argument("--batch-dir", type=Path, required=True)
    apply = commands.add_parser(
        "apply",
        help="Apply using SOURCE_SIMULATOR_DSN or standard PG* environment variables",
    )
    apply.add_argument("--batch-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        contract = load_contract(args.contract)
        if args.command == "generate":
            directories = write_fixture(args.output_dir, contract, args.seed)
            result = {
                "status": "GENERATED",
                "batch_directories": [str(path) for path in directories],
            }
        elif args.command == "validate":
            batch = load_batch(args.batch_dir, contract)
            result = {"status": "VALID", **batch.manifest}
        else:
            # Validate before opening a connection; credentials never enter argv.
            load_batch(args.batch_dir, contract)
            with psycopg.connect(
                os.environ.get("SOURCE_SIMULATOR_DSN", ""),
                autocommit=True,
                connect_timeout=10,
                application_name="olist-source-simulator",
            ) as connection:
                result = apply_batch(connection, args.batch_dir, contract)
    except (BatchError, ContractError, OSError, psycopg.Error, ValueError) as exc:
        print(f"Source simulator: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
