"""Shared helpers for raw Olist snapshot verification and profiling."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterator

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ManifestError(ValueError):
    """Raised when the raw dataset manifest is invalid."""


def load_manifest(path: Path) -> dict[str, Any]:
    """Load and validate the machine-readable raw dataset manifest."""

    try:
        with path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except FileNotFoundError as exc:
        raise ManifestError(f"Manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Manifest is not valid JSON: {path}: {exc}") from exc

    if not isinstance(manifest.get("dataset"), dict):
        raise ManifestError("Manifest must contain a dataset object")
    if not isinstance(manifest.get("files"), list) or not manifest["files"]:
        raise ManifestError("Manifest must contain a non-empty files list")
    if not isinstance(manifest.get("relationships", []), list):
        raise ManifestError("Manifest relationships must be a list")

    file_specs: dict[str, dict[str, Any]] = {}
    for index, spec in enumerate(manifest["files"]):
        if not isinstance(spec, dict):
            raise ManifestError(f"files[{index}] must be an object")

        required = {
            "name",
            "sha256",
            "size_bytes",
            "row_count",
            "encoding",
            "columns",
            "key",
        }
        missing = sorted(required - spec.keys())
        if missing:
            raise ManifestError(
                f"File specification at index {index} is missing: {', '.join(missing)}"
            )

        name = spec["name"]
        if not isinstance(name, str) or Path(name).name != name:
            raise ManifestError(f"Invalid raw filename: {name!r}")
        if name in file_specs:
            raise ManifestError(f"Duplicate file specification: {name}")
        if not SHA256_PATTERN.fullmatch(str(spec["sha256"])):
            raise ManifestError(f"Invalid SHA-256 for {name}")
        if not isinstance(spec["size_bytes"], int) or spec["size_bytes"] < 0:
            raise ManifestError(f"Invalid size_bytes for {name}")
        if not isinstance(spec["row_count"], int) or spec["row_count"] < 0:
            raise ManifestError(f"Invalid row_count for {name}")
        if not isinstance(spec["columns"], list) or not spec["columns"]:
            raise ManifestError(f"Columns must be a non-empty list for {name}")
        if len(spec["columns"]) != len(set(spec["columns"])):
            raise ManifestError(f"Duplicate column names in {name}")
        if not isinstance(spec["key"], list):
            raise ManifestError(f"Key must be a list for {name}")

        known_columns = set(spec["columns"])
        configured_columns = set(spec["key"])
        configured_columns.update(spec.get("numeric_columns", {}).keys())
        configured_columns.update(spec.get("timestamp_columns", []))
        configured_columns.update(spec.get("profiled_value_columns", []))
        unknown = sorted(configured_columns - known_columns)
        if unknown:
            raise ManifestError(
                f"Unknown configured columns in {name}: {', '.join(unknown)}"
            )
        file_specs[name] = spec

    for relationship in manifest.get("relationships", []):
        required = {"name", "from_file", "from_columns", "to_file", "to_columns"}
        missing = sorted(required - relationship.keys())
        if missing:
            raise ManifestError(f"Relationship is missing fields: {', '.join(missing)}")
        from_file = relationship["from_file"]
        to_file = relationship["to_file"]
        if from_file not in file_specs or to_file not in file_specs:
            raise ManifestError(
                f"Relationship {relationship['name']} references an unknown file"
            )
        from_columns = relationship["from_columns"]
        to_columns = relationship["to_columns"]
        if not from_columns or len(from_columns) != len(to_columns):
            raise ManifestError(
                f"Relationship {relationship['name']} has incompatible key columns"
            )
        if not set(from_columns).issubset(file_specs[from_file]["columns"]):
            raise ManifestError(
                f"Relationship {relationship['name']} has unknown source columns"
            )
        if not set(to_columns).issubset(file_specs[to_file]["columns"]):
            raise ManifestError(
                f"Relationship {relationship['name']} has unknown target columns"
            )

    return manifest


def file_specs_by_name(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return manifest file specifications keyed by filename."""

    return {spec["name"]: spec for spec in manifest["files"]}


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate a file SHA-256 without loading the whole file into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_header(path: Path, encoding: str) -> list[str]:
    """Read one CSV header with BOM and newline handling controlled by the manifest."""

    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.reader(handle)
        try:
            return next(reader)
        except StopIteration as exc:
            raise ValueError("CSV file is empty") from exc


def iter_csv_rows(
    path: Path, encoding: str, expected_columns: list[str]
) -> Iterator[tuple[int, list[str]]]:
    """Yield logical CSV records while enforcing the expected header and row width."""

    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV file is empty: {path}") from exc
        if header != expected_columns:
            raise ValueError(
                f"Header mismatch in {path.name}: expected {expected_columns}, got {header}"
            )

        expected_width = len(expected_columns)
        for logical_row_number, row in enumerate(reader, start=2):
            if len(row) != expected_width:
                raise ValueError(
                    f"Row width mismatch in {path.name} at logical row "
                    f"{logical_row_number}: expected {expected_width}, got {len(row)}"
                )
            yield logical_row_number, row


def verify_snapshot(
    manifest: dict[str, Any], raw_dir: Path, allow_extra_csv: bool = False
) -> tuple[list[dict[str, Any]], list[str]]:
    """Verify expected files, sizes, checksums and headers for a raw snapshot."""

    results: list[dict[str, Any]] = []
    all_errors: list[str] = []
    expected_names = {spec["name"] for spec in manifest["files"]}

    if not raw_dir.is_dir():
        return results, [f"Raw data directory not found: {raw_dir}"]

    actual_csv_names = {path.name for path in raw_dir.glob("*.csv") if path.is_file()}
    unexpected = sorted(actual_csv_names - expected_names)
    if unexpected and not allow_extra_csv:
        all_errors.append(f"Unexpected CSV files: {', '.join(unexpected)}")

    for spec in manifest["files"]:
        path = raw_dir / spec["name"]
        file_errors: list[str] = []
        actual_size: int | None = None
        actual_sha256: str | None = None

        if not path.is_file():
            file_errors.append("file is missing")
        else:
            actual_size = path.stat().st_size
            if actual_size != spec["size_bytes"]:
                file_errors.append(
                    f"size mismatch: expected {spec['size_bytes']}, got {actual_size}"
                )

            actual_sha256 = sha256_file(path)
            if actual_sha256 != spec["sha256"]:
                file_errors.append(
                    f"checksum mismatch: expected {spec['sha256']}, got {actual_sha256}"
                )

            try:
                header = read_csv_header(path, spec["encoding"])
                if header != spec["columns"]:
                    file_errors.append(
                        f"header mismatch: expected {spec['columns']}, got {header}"
                    )
            except (OSError, UnicodeError, csv.Error, ValueError) as exc:
                file_errors.append(f"header read failed: {exc}")

        result = {
            "name": spec["name"],
            "expected_rows": spec["row_count"],
            "actual_size": actual_size,
            "actual_sha256": actual_sha256,
            "errors": file_errors,
        }
        results.append(result)
        all_errors.extend(f"{spec['name']}: {error}" for error in file_errors)

    return results, all_errors
