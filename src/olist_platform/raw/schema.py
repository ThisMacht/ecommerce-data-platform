"""Compare a PostgreSQL catalog report with the versioned source contract."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from .contracts import ContractError, load_contract


def normalize_type(postgres_type: str) -> str:
    """Ignore formatting differences such as numeric(18, 2) vs numeric(18,2)."""

    normalized = " ".join(postgres_type.lower().split())
    return re.sub(r"\s*([(),])\s*", r"\1", normalized)


def validate_database_schema(
    contract: dict[str, Any], catalog: dict[str, Any]
) -> tuple[list[str], list[str]]:
    """Return blocking errors and additive-nullable warnings from a live schema."""

    errors: list[str] = []
    warnings: list[str] = []
    schema_name = contract["database"]["source_schema"]
    tables = catalog["schemas"].get(schema_name)
    if tables is None:
        return [f"Missing source schema: {schema_name}"], warnings

    expected_names = {table["table"] for table in contract["tables"]}
    for name in sorted(tables.keys() - expected_names):
        errors.append(f"Unexpected source table: {schema_name}.{name}")

    for table in contract["tables"]:
        name = table["table"]
        qualified_name = f"{schema_name}.{name}"
        if name not in tables:
            errors.append(f"Missing source table: {qualified_name}")
            continue
        actual = tables[name]
        expected_columns = table["columns"] + contract["metadata_columns"]
        technical_key = table.get("technical_key")
        if technical_key:
            expected_columns = [dict(technical_key, nullable=False)] + expected_columns
        expected = {column["name"]: column for column in expected_columns}
        columns = {column["name"]: column for column in actual["columns"]}

        for column_name, specification in expected.items():
            qualified_column = f"{qualified_name}.{column_name}"
            if column_name not in columns:
                errors.append(f"Missing column: {qualified_column}")
                continue
            column = columns[column_name]
            if normalize_type(column["postgres_type"]) != normalize_type(
                specification["postgres_type"]
            ):
                errors.append(
                    f"Type mismatch for {qualified_column}: "
                    f"expected {specification['postgres_type']}, "
                    f"got {column['postgres_type']}"
                )
            if column["nullable"] != specification["nullable"]:
                errors.append(
                    f"Nullability mismatch for {qualified_column}: "
                    f"expected nullable={specification['nullable']}, "
                    f"got {column['nullable']}"
                )

        additive_policy = contract["policies"]["schema_evolution"][
            "additive_nullable_column"
        ]
        for column_name in sorted(columns.keys() - expected.keys()):
            message = f"Unexpected column: {qualified_name}.{column_name}"
            if columns[column_name]["nullable"] and additive_policy == "WARN":
                warnings.append(message)
            else:
                errors.append(message)

        if actual["primary_key"] != table["primary_key"]:
            errors.append(
                f"Primary-key mismatch for {qualified_name}: "
                f"expected {table['primary_key']}, got {actual['primary_key']}"
            )
        if "source_sequence" in columns:
            if columns["source_sequence"].get("identity") != "d":
                errors.append(
                    f"{qualified_name}.source_sequence must be a BY DEFAULT identity"
                )
            if ["source_sequence"] not in actual["unique_keys"]:
                errors.append(
                    f"{qualified_name}.source_sequence must have a unique constraint"
                )

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract", type=Path, default=Path("config/source_contract.json")
    )
    parser.add_argument("--catalog", type=Path, required=True)
    args = parser.parse_args()
    try:
        contract = load_contract(args.contract)
        catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
        errors, warnings = validate_database_schema(contract, catalog)
    except (
        ContractError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
    ) as exc:
        print(f"Source schema report is invalid: {exc}", file=sys.stderr)
        return 2

    for warning in warnings:
        print(f"[WARN] {warning}")
    if errors:
        print("Source schema: FAIL", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(f"Source schema: PASS ({len(contract['tables'])} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
