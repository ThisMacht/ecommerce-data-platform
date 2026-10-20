#!/usr/bin/env python3
"""Validate the versioned source contract against the immutable raw manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .manifest import ManifestError, load_manifest


class ContractError(ValueError):
    """Raised when the source contract is missing or internally inconsistent."""


def load_contract(path: Path) -> dict[str, Any]:
    """Load a source contract JSON document."""

    try:
        with path.open(encoding="utf-8") as handle:
            contract = json.load(handle)
    except FileNotFoundError as exc:
        raise ContractError(f"Source contract not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ContractError(
            f"Source contract is not valid JSON: {path}: {exc}"
        ) from exc

    if not isinstance(contract, dict):
        raise ContractError("Source contract must be a JSON object")
    return contract


def validate_contract(contract: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    """Return every contract violation found; an empty list means valid."""

    errors: list[str] = []
    required_top_level = {
        "contract_version",
        "status",
        "dataset_snapshot_id",
        "database",
        "policies",
        "seed",
        "metadata_columns",
        "tables",
    }
    missing = sorted(required_top_level - contract.keys())
    if missing:
        errors.append(f"Missing top-level fields: {', '.join(missing)}")

    expected_snapshot = manifest.get("dataset", {}).get("snapshot_id")
    if contract.get("dataset_snapshot_id") != expected_snapshot:
        errors.append(
            "dataset_snapshot_id mismatch: "
            f"expected {expected_snapshot!r}, got {contract.get('dataset_snapshot_id')!r}"
        )

    tables = contract.get("tables")
    if not isinstance(tables, list):
        errors.append("tables must be a list")
        return errors

    contract_by_file: dict[str, dict[str, Any]] = {}
    physical_names: set[str] = set()
    for index, table in enumerate(tables):
        if not isinstance(table, dict):
            errors.append(f"tables[{index}] must be an object")
            continue
        filename = table.get("file")
        table_name = table.get("table")
        if not isinstance(filename, str):
            errors.append(f"tables[{index}].file must be a string")
            continue
        if filename in contract_by_file:
            errors.append(f"Duplicate file mapping: {filename}")
        else:
            contract_by_file[filename] = table
        if not isinstance(table_name, str) or not table_name:
            errors.append(f"Missing physical table name for {filename}")
        elif table_name in physical_names:
            errors.append(f"Duplicate physical table name: {table_name}")
        else:
            physical_names.add(table_name)

    manifest_by_file = {spec["name"]: spec for spec in manifest["files"]}
    missing_mappings = sorted(manifest_by_file.keys() - contract_by_file.keys())
    extra_mappings = sorted(contract_by_file.keys() - manifest_by_file.keys())
    if missing_mappings:
        errors.append(f"Missing file mappings: {', '.join(missing_mappings)}")
    if extra_mappings:
        errors.append(f"Unknown file mappings: {', '.join(extra_mappings)}")

    for filename in sorted(manifest_by_file.keys() & contract_by_file.keys()):
        spec = manifest_by_file[filename]
        table = contract_by_file[filename]
        columns = table.get("columns")
        if not isinstance(columns, list):
            errors.append(f"columns must be a list for {filename}")
            continue
        column_names: list[str] = []
        for column_index, column in enumerate(columns):
            if not isinstance(column, dict):
                errors.append(
                    f"columns[{column_index}] must be an object for {filename}"
                )
                continue
            column_name = column.get("name")
            column_names.append(column_name)
            if not isinstance(column.get("postgres_type"), str):
                errors.append(f"Missing postgres_type for {filename}.{column_name}")
            if not isinstance(column.get("nullable"), bool):
                errors.append(f"Missing nullable boolean for {filename}.{column_name}")

        if column_names != spec["columns"]:
            errors.append(
                f"Column order mismatch for {filename}: "
                f"expected {spec['columns']}, got {column_names}"
            )
        if table.get("source_key") != spec["key"]:
            errors.append(
                f"source_key mismatch for {filename}: "
                f"expected {spec['key']}, got {table.get('source_key')}"
            )
        primary_key = table.get("primary_key")
        if not isinstance(primary_key, list) or not primary_key:
            errors.append(f"primary_key must be a non-empty list for {filename}")
        known_physical_columns = set(column_names)
        technical_key = table.get("technical_key")
        if isinstance(technical_key, dict) and isinstance(
            technical_key.get("name"), str
        ):
            known_physical_columns.add(technical_key["name"])
        if isinstance(primary_key, list):
            unknown_key_columns = sorted(set(primary_key) - known_physical_columns)
            if unknown_key_columns:
                errors.append(
                    f"Unknown primary-key columns for {filename}: "
                    f"{', '.join(unknown_key_columns)}"
                )

    metadata = contract.get("metadata_columns")
    required_metadata = {
        "source_updated_at",
        "source_sequence",
        "source_operation",
        "source_batch_id",
        "source_loaded_at",
    }
    if not isinstance(metadata, list):
        errors.append("metadata_columns must be a list")
    else:
        metadata_names = {
            item.get("name") for item in metadata if isinstance(item, dict)
        }
        missing_metadata = sorted(required_metadata - metadata_names)
        if missing_metadata:
            errors.append(
                f"Missing simulator metadata columns: {', '.join(missing_metadata)}"
            )

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("config/source_contract.json"),
        help="Path to the versioned source contract",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("config/raw_dataset_manifest.json"),
        help="Path to the immutable raw snapshot manifest",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        contract = load_contract(args.contract)
        manifest = load_manifest(args.manifest)
    except (ContractError, ManifestError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    errors = validate_contract(contract, manifest)
    if errors:
        print("Source contract: FAIL", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(
        "Source contract: PASS "
        f"({contract['contract_version']}, {len(contract['tables'])} tables)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
