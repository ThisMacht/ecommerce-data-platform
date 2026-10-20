#!/usr/bin/env python3
"""Profile the raw Olist source and emit deterministic JSON and Markdown reports."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .manifest import (
    ManifestError,
    file_specs_by_name,
    iter_csv_rows,
    load_manifest,
    verify_snapshot,
)

DEFAULT_MANIFEST = Path("config/raw_dataset_manifest.json")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_JSON_OUTPUT = Path("build/reports/source_profile.json")
DEFAULT_MARKDOWN_OUTPUT = Path("docs/data_contracts/00_source_profile.md")
TOP_VALUE_LIMIT = 10
SAMPLE_LIMIT = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile Olist CSV rows, keys, values, types and relationships."
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN_OUTPUT)
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip checksum verification when it already ran in the same workflow.",
    )
    return parser.parse_args()


def atomic_write_text(path: Path, content: str) -> None:
    """Atomically replace a generated report."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(content)
            temporary_name = handle.name
        os.replace(temporary_name, path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def decimal_text(value: Decimal | int | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, int):
        return str(value)
    return format(value, "f")


def update_numeric_profile(
    stats: dict[str, Any], value: str, numeric_type: str
) -> None:
    if value == "":
        stats["null_count"] += 1
        return

    try:
        parsed: Decimal | int
        if numeric_type == "integer":
            parsed = int(value)
        elif numeric_type == "decimal":
            parsed = Decimal(value)
        else:
            raise ValueError(f"Unsupported numeric type: {numeric_type}")
    except (ValueError, InvalidOperation):
        stats["parse_error_count"] += 1
        if len(stats["parse_error_samples"]) < SAMPLE_LIMIT:
            stats["parse_error_samples"].append(value)
        return

    stats["non_null_count"] += 1
    stats["negative_count"] += parsed < 0
    stats["zero_count"] += parsed == 0
    if stats["minimum_value"] is None or parsed < stats["minimum_value"]:
        stats["minimum_value"] = parsed
    if stats["maximum_value"] is None or parsed > stats["maximum_value"]:
        stats["maximum_value"] = parsed


def update_timestamp_profile(stats: dict[str, Any], value: str) -> None:
    if value == "":
        stats["null_count"] += 1
        return

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        stats["parse_error_count"] += 1
        if len(stats["parse_error_samples"]) < SAMPLE_LIMIT:
            stats["parse_error_samples"].append(value)
        return

    stats["non_null_count"] += 1
    if stats["minimum_value"] is None or parsed < stats["minimum_value"]:
        stats["minimum_value"] = parsed
    if stats["maximum_value"] is None or parsed > stats["maximum_value"]:
        stats["maximum_value"] = parsed


def sorted_top_values(counter: Counter[str]) -> list[dict[str, Any]]:
    values = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [
        {"value": value, "count": count} for value, count in values[:TOP_VALUE_LIMIT]
    ]


def profile_file(raw_dir: Path, spec: dict[str, Any]) -> dict[str, Any]:
    """Profile one CSV file in a single logical-record pass."""

    columns: list[str] = spec["columns"]
    column_indexes = {column: index for index, column in enumerate(columns)}
    null_counts = {column: 0 for column in columns}
    key_columns: list[str] = spec["key"]
    seen_keys: set[tuple[str, ...]] = set()
    duplicate_key_rows = 0
    null_key_rows = 0
    duplicate_key_samples: list[list[str]] = []

    numeric_stats: dict[str, dict[str, Any]] = {}
    for column, numeric_type in spec.get("numeric_columns", {}).items():
        numeric_stats[column] = {
            "type": numeric_type,
            "non_null_count": 0,
            "null_count": 0,
            "parse_error_count": 0,
            "parse_error_samples": [],
            "negative_count": 0,
            "zero_count": 0,
            "minimum_value": None,
            "maximum_value": None,
        }

    timestamp_stats: dict[str, dict[str, Any]] = {}
    for column in spec.get("timestamp_columns", []):
        timestamp_stats[column] = {
            "non_null_count": 0,
            "null_count": 0,
            "parse_error_count": 0,
            "parse_error_samples": [],
            "minimum_value": None,
            "maximum_value": None,
        }

    value_counters = {
        column: Counter() for column in spec.get("profiled_value_columns", [])
    }
    row_count = 0
    path = raw_dir / spec["name"]

    for _, row in iter_csv_rows(path, spec["encoding"], columns):
        row_count += 1
        for column, index in column_indexes.items():
            if row[index] == "":
                null_counts[column] += 1

        if key_columns:
            key = tuple(row[column_indexes[column]] for column in key_columns)
            if any(value == "" for value in key):
                null_key_rows += 1
            if key in seen_keys:
                duplicate_key_rows += 1
                if len(duplicate_key_samples) < SAMPLE_LIMIT:
                    duplicate_key_samples.append(list(key))
            else:
                seen_keys.add(key)

        for column, stats in numeric_stats.items():
            update_numeric_profile(stats, row[column_indexes[column]], stats["type"])
        for column, stats in timestamp_stats.items():
            update_timestamp_profile(stats, row[column_indexes[column]])
        for column, counter in value_counters.items():
            value = row[column_indexes[column]]
            if value != "":
                counter[value] += 1

    errors: list[str] = []
    if row_count != spec["row_count"]:
        errors.append(
            f"row count mismatch: expected {spec['row_count']}, got {row_count}"
        )
    if key_columns and null_key_rows:
        errors.append(f"key contains null values in {null_key_rows} row(s)")
    if key_columns and duplicate_key_rows:
        errors.append(f"key is duplicated in {duplicate_key_rows} row(s)")

    for column, stats in numeric_stats.items():
        if stats["parse_error_count"]:
            errors.append(
                f"numeric parse failed for {column} in "
                f"{stats['parse_error_count']} row(s)"
            )
        stats["minimum"] = decimal_text(stats.pop("minimum_value"))
        stats["maximum"] = decimal_text(stats.pop("maximum_value"))

    for column, stats in timestamp_stats.items():
        if stats["parse_error_count"]:
            errors.append(
                f"timestamp parse failed for {column} in "
                f"{stats['parse_error_count']} row(s)"
            )
        minimum = stats.pop("minimum_value")
        maximum = stats.pop("maximum_value")
        stats["minimum"] = minimum.isoformat(sep=" ") if minimum else None
        stats["maximum"] = maximum.isoformat(sep=" ") if maximum else None

    value_profiles: dict[str, dict[str, Any]] = {}
    for column, counter in value_counters.items():
        value_profiles[column] = {
            "distinct_non_null_count": len(counter),
            "maximum_frequency": max(counter.values(), default=0),
            "top_values": sorted_top_values(counter),
        }

    return {
        "row_count": row_count,
        "expected_row_count": spec["row_count"],
        "column_count": len(columns),
        "columns": columns,
        "null_counts": null_counts,
        "key": {
            "columns": key_columns,
            "unique_count": len(seen_keys) if key_columns else None,
            "null_key_rows": null_key_rows if key_columns else None,
            "duplicate_rows": duplicate_key_rows if key_columns else None,
            "duplicate_samples": duplicate_key_samples,
        },
        "numeric_profiles": numeric_stats,
        "timestamp_profiles": timestamp_stats,
        "value_profiles": value_profiles,
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
    }


def load_key_set(
    raw_dir: Path, spec: dict[str, Any], key_columns: list[str]
) -> set[tuple[str, ...]]:
    columns = spec["columns"]
    indexes = [columns.index(column) for column in key_columns]
    keys: set[tuple[str, ...]] = set()
    for _, row in iter_csv_rows(raw_dir / spec["name"], spec["encoding"], columns):
        keys.add(tuple(row[index] for index in indexes))
    return keys


def profile_relationships(
    raw_dir: Path, manifest: dict[str, Any]
) -> list[dict[str, Any]]:
    specs = file_specs_by_name(manifest)
    target_cache: dict[tuple[str, tuple[str, ...]], set[tuple[str, ...]]] = {}
    profiles: list[dict[str, Any]] = []

    for relationship in manifest.get("relationships", []):
        target_cache_key = (
            relationship["to_file"],
            tuple(relationship["to_columns"]),
        )
        if target_cache_key not in target_cache:
            target_cache[target_cache_key] = load_key_set(
                raw_dir,
                specs[relationship["to_file"]],
                relationship["to_columns"],
            )
        target_keys = target_cache[target_cache_key]

        source_spec = specs[relationship["from_file"]]
        source_columns = source_spec["columns"]
        source_indexes = [
            source_columns.index(column) for column in relationship["from_columns"]
        ]
        orphan_rows = 0
        null_key_rows = 0
        orphan_keys: set[tuple[str, ...]] = set()

        for _, row in iter_csv_rows(
            raw_dir / source_spec["name"],
            source_spec["encoding"],
            source_columns,
        ):
            key = tuple(row[index] for index in source_indexes)
            if any(value == "" for value in key):
                null_key_rows += 1
                if relationship.get("ignore_null", False):
                    continue
            if key not in target_keys:
                orphan_rows += 1
                if len(orphan_keys) < SAMPLE_LIMIT:
                    orphan_keys.add(key)

        enforced = relationship.get("enforced", True)
        if orphan_rows == 0:
            status = "PASS"
        elif enforced:
            status = "FAIL"
        else:
            status = "WARNING"

        profiles.append(
            {
                "name": relationship["name"],
                "from": {
                    "file": relationship["from_file"],
                    "columns": relationship["from_columns"],
                },
                "to": {
                    "file": relationship["to_file"],
                    "columns": relationship["to_columns"],
                },
                "enforced": enforced,
                "null_key_rows": null_key_rows,
                "orphan_rows": orphan_rows,
                "orphan_key_samples": [list(key) for key in sorted(orphan_keys)],
                "status": status,
            }
        )

    return profiles


def build_profile(raw_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    files: dict[str, dict[str, Any]] = {}
    total_rows = 0
    total_null_values = 0

    for spec in manifest["files"]:
        file_profile = profile_file(raw_dir, spec)
        files[spec["name"]] = file_profile
        total_rows += file_profile["row_count"]
        total_null_values += sum(file_profile["null_counts"].values())
        print(
            f"[{file_profile['status']}] {spec['name']}: "
            f"{file_profile['row_count']} logical rows"
        )

    relationships = profile_relationships(raw_dir, manifest)
    file_failures = sum(profile["status"] == "FAIL" for profile in files.values())
    relationship_failures = sum(
        relationship["status"] == "FAIL" for relationship in relationships
    )
    warnings = sum(
        relationship["status"] == "WARNING" for relationship in relationships
    )
    status = "PASS" if file_failures == 0 and relationship_failures == 0 else "FAIL"

    return {
        "profile_version": 1,
        "dataset": manifest["dataset"],
        "summary": {
            "status": status,
            "file_count": len(files),
            "total_logical_rows": total_rows,
            "total_size_bytes": sum(spec["size_bytes"] for spec in manifest["files"]),
            "total_null_values": total_null_values,
            "file_failures": file_failures,
            "relationship_failures": relationship_failures,
            "warnings": warnings,
        },
        "files": files,
        "relationships": relationships,
    }


def markdown_escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def percent(count: int, total: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{count / total:.2%}"


def render_markdown(profile: dict[str, Any], json_output: Path) -> str:
    dataset = profile["dataset"]
    summary = profile["summary"]
    try:
        json_output_display = json_output.resolve().relative_to(Path.cwd())
    except ValueError:
        json_output_display = json_output
    lines: list[str] = [
        "# Olist Source Profile",
        "",
        "> Được tự động sinh bởi `make profile-source`. Không chỉnh sửa thủ công các giá trị đo lường.",
        "",
        "## Snapshot",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Dataset | {markdown_escape(dataset['name'])} |",
        f"| Snapshot ID | `{markdown_escape(dataset['snapshot_id'])}` |",
        f"| Manifest validation date | {markdown_escape(dataset['validated_on'])} |",
        f"| Profile status | **{summary['status']}** |",
        f"| Files | {summary['file_count']:,} |",
        f"| Logical rows | {summary['total_logical_rows']:,} |",
        f"| Source bytes | {summary['total_size_bytes']:,} |",
        f"| Null field values | {summary['total_null_values']:,} |",
        f"| Non-enforced warnings | {summary['warnings']:,} |",
        "",
        "Machine-readable report được sinh tại "
        f"`{markdown_escape(json_output_display.as_posix())}` và đã được loại khỏi Git.",
        "",
        "## File and key validation",
        "",
        "| File | Rows | Columns | Key | Null key rows | Duplicate key rows | Status |",
        "| --- | ---: | ---: | --- | ---: | ---: | --- |",
    ]

    for filename, file_profile in profile["files"].items():
        key = file_profile["key"]
        key_text = " + ".join(f"`{column}`" for column in key["columns"])
        if not key_text:
            key_text = "Not declared"
        null_key_rows = (
            "—" if key["null_key_rows"] is None else f"{key['null_key_rows']:,}"
        )
        duplicate_rows = (
            "—" if key["duplicate_rows"] is None else f"{key['duplicate_rows']:,}"
        )
        lines.append(
            f"| `{filename}` | {file_profile['row_count']:,} | "
            f"{file_profile['column_count']} | {key_text} | {null_key_rows} | "
            f"{duplicate_rows} | {file_profile['status']} |"
        )

    lines.extend(
        [
            "",
            "## Null values",
            "",
            "Chỉ liệt kê các column có ít nhất một source value null hoặc rỗng.",
            "",
            "| File | Column | Null rows | Null rate |",
            "| --- | --- | ---: | ---: |",
        ]
    )
    null_rows_written = 0
    for filename, file_profile in profile["files"].items():
        for column, null_count in file_profile["null_counts"].items():
            if null_count:
                null_rows_written += 1
                lines.append(
                    f"| `{filename}` | `{column}` | {null_count:,} | "
                    f"{percent(null_count, file_profile['row_count'])} |"
                )
    if null_rows_written == 0:
        lines.append("| — | — | 0 | 0.00% |")

    lines.extend(
        [
            "",
            "## Numeric profiles",
            "",
            "| File | Column | Type | Non-null | Parse errors | Min | Max | Negative |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for filename, file_profile in profile["files"].items():
        for column, stats in file_profile["numeric_profiles"].items():
            lines.append(
                f"| `{filename}` | `{column}` | {stats['type']} | "
                f"{stats['non_null_count']:,} | {stats['parse_error_count']:,} | "
                f"{markdown_escape(stats['minimum'])} | {markdown_escape(stats['maximum'])} | "
                f"{stats['negative_count']:,} |"
            )

    lines.extend(
        [
            "",
            "## Timestamp profiles",
            "",
            "Source timestamps được profile dưới dạng naive values. Timezone interpretation được áp dụng ở downstream theo data contract.",
            "",
            "| File | Column | Non-null | Null | Parse errors | Min | Max |",
            "| --- | --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for filename, file_profile in profile["files"].items():
        for column, stats in file_profile["timestamp_profiles"].items():
            lines.append(
                f"| `{filename}` | `{column}` | {stats['non_null_count']:,} | "
                f"{stats['null_count']:,} | {stats['parse_error_count']:,} | "
                f"{markdown_escape(stats['minimum'])} | {markdown_escape(stats['maximum'])} |"
            )

    lines.extend(
        [
            "",
            "## Relationship validation",
            "",
            "| Relationship | From | To | Null key rows | Orphan rows | Enforcement | Status |",
            "| --- | --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for relationship in profile["relationships"]:
        from_text = (
            f"`{relationship['from']['file']}` "
            f"({', '.join(relationship['from']['columns'])})"
        )
        to_text = (
            f"`{relationship['to']['file']}` "
            f"({', '.join(relationship['to']['columns'])})"
        )
        enforcement = "Required" if relationship["enforced"] else "Warning only"
        lines.append(
            f"| `{relationship['name']}` | {from_text} | {to_text} | "
            f"{relationship['null_key_rows']:,} | {relationship['orphan_rows']:,} | "
            f"{enforcement} | {relationship['status']} |"
        )

    lines.extend(
        [
            "",
            "## Selected value profiles",
            "",
            "| File | Column | Distinct non-null | Maximum frequency | Top values |",
            "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for filename, file_profile in profile["files"].items():
        for column, stats in file_profile["value_profiles"].items():
            top_values = ", ".join(
                f"{markdown_escape(item['value'])}={item['count']:,}"
                for item in stats["top_values"]
            )
            lines.append(
                f"| `{filename}` | `{column}` | "
                f"{stats['distinct_non_null_count']:,} | "
                f"{stats['maximum_frequency']:,} | {top_values} |"
            )

    warnings = [
        relationship
        for relationship in profile["relationships"]
        if relationship["status"] == "WARNING"
    ]
    lines.extend(["", "## Non-enforced findings", ""])
    if warnings:
        for relationship in warnings:
            samples = ", ".join(
                " + ".join(key) for key in relationship["orphan_key_samples"]
            )
            lines.append(
                f"- `{relationship['name']}` có "
                f"{relationship['orphan_rows']:,} orphan row(s). "
                f"Sample keys: {markdown_escape(samples) or 'none'}."
            )
    else:
        lines.append("- Không có.")

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Profile `PASS` nghĩa là snapshot khớp với row counts, key constraints, parse expectations và enforced relationships đã khai báo.",
            "- Relationship `WARNING` được thiết kế là non-blocking nhưng phải được xử lý bởi downstream data-quality policy.",
            "- Profile này mô tả clean source snapshot. Dirty fixtures và simulator batches sử dụng expected-error manifests riêng.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    try:
        manifest = load_manifest(args.manifest)
        if not args.skip_verify:
            _, verification_errors = verify_snapshot(manifest, args.raw_dir)
            if verification_errors:
                print("Raw snapshot verification failed:", file=sys.stderr)
                for error in verification_errors:
                    print(f"- {error}", file=sys.stderr)
                return 1
        profile = build_profile(args.raw_dir, manifest)
    except (ManifestError, OSError, UnicodeError, csv.Error, ValueError) as exc:
        print(f"Source profiling failed: {exc}", file=sys.stderr)
        return 2

    json_content = json.dumps(profile, ensure_ascii=False, indent=2) + "\n"
    markdown_content = render_markdown(profile, args.json_output)
    atomic_write_text(args.json_output, json_content)
    atomic_write_text(args.markdown_output, markdown_content)

    print(f"JSON report: {args.json_output}")
    print(f"Markdown report: {args.markdown_output}")
    print(f"Source profile status: {profile['summary']['status']}")
    return 0 if profile["summary"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
