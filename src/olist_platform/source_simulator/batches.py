"""Versioned, checksum-protected simulator batch artifacts."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

CORE_TABLES = ("customers", "orders", "order_items", "order_payments")
BATCH_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$")


class BatchError(ValueError):
    """A batch artifact or source mutation violates the simulator contract."""


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def contract_hash(contract: dict) -> str:
    return hashlib.sha256(canonical_bytes(contract)).hexdigest()


def source_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise BatchError("Source timestamps must be ISO-8601 strings with a timezone")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BatchError(f"Invalid source timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BatchError("Source timestamps require a timezone")
    return parsed.astimezone(timezone.utc)


def typed_value(value: Any, column: dict) -> Any:
    name = column["name"]
    if value is None:
        if column["nullable"]:
            return None
        raise BatchError(f"{name} cannot be null")
    kind = column["postgres_type"]
    if kind == "text":
        if not isinstance(value, str) or (not value and not column["nullable"]):
            raise BatchError(f"{name} must be a non-empty string")
        if "\x00" in value:
            raise BatchError(f"{name} cannot contain a NUL character")
        return value
    if kind == "integer":
        if type(value) is not int or not -(2**31) <= value < 2**31:
            raise BatchError(f"{name} must be a 32-bit integer")
        return value
    if kind == "numeric(18,2)":
        if not isinstance(value, str):
            raise BatchError(f"{name} must be decimal text, never a JSON float")
        try:
            number = Decimal(value)
            if (
                not number.is_finite()
                or abs(number) >= Decimal("1e16")
                or number != number.quantize(Decimal("0.01"))
            ):
                raise InvalidOperation
        except InvalidOperation as exc:
            raise BatchError(f"{name} does not fit numeric(18,2)") from exc
        return number
    if kind == "timestamp without time zone":
        if not isinstance(value, str):
            raise BatchError(f"{name} must be a local timestamp string")
        try:
            timestamp = datetime.fromisoformat(value)
        except ValueError as exc:
            raise BatchError(f"Invalid local timestamp for {name}") from exc
        if timestamp.tzinfo is not None:
            raise BatchError(f"{name} must not include a timezone")
        return timestamp
    raise BatchError(f"Unsupported simulator type: {kind}")


def operation_counts(operations: list[dict]) -> dict:
    counts = {name: {"INSERT": 0, "UPDATE": 0} for name in CORE_TABLES}
    for operation in operations:
        counts[operation["table"]][operation["operation"]] += 1
    return counts


def validate_payload(payload: dict, contract: dict) -> None:
    if contract.get("contract_version") != "1.0.0":
        raise BatchError("Unsupported source contract version")
    if not isinstance(payload, dict) or set(payload) != {
        "format_version",
        "batch_id",
        "predecessor_batch_id",
        "applied_at",
        "operations",
    }:
        raise BatchError("Unexpected or missing batch payload fields")
    if type(payload["format_version"]) is not int or payload["format_version"] != 1:
        raise BatchError("Unsupported batch format_version")
    if not isinstance(payload["batch_id"], str) or not BATCH_ID.fullmatch(
        payload["batch_id"]
    ):
        raise BatchError("Invalid batch_id")
    predecessor = payload["predecessor_batch_id"]
    if predecessor is not None and (
        not isinstance(predecessor, str)
        or not BATCH_ID.fullmatch(predecessor)
        or predecessor == payload["batch_id"]
    ):
        raise BatchError("Invalid predecessor_batch_id")
    source_time(payload["applied_at"])
    operations = payload["operations"]
    if not isinstance(operations, list) or not operations:
        raise BatchError("A batch must contain operations")
    tables = {table["table"]: table for table in contract["tables"]}
    for index, operation in enumerate(operations):
        if not isinstance(operation, dict) or set(operation) != {
            "table",
            "operation",
            "key",
            "values",
            "source_updated_at",
        }:
            raise BatchError(f"Invalid fields for operation {index}")
        if operation["table"] not in CORE_TABLES or operation["operation"] not in (
            "INSERT",
            "UPDATE",
        ):
            raise BatchError(f"Unsupported table or operation at index {index}")
        table = tables[operation["table"]]
        columns = {column["name"]: column for column in table["columns"]}
        key = operation["key"]
        values = operation["values"]
        if not isinstance(key, dict) or set(key) != set(table["primary_key"]):
            raise BatchError(f"Operation {index} requires the exact primary key")
        writable = columns.keys() - set(table["primary_key"])
        if not isinstance(values, dict) or not values or values.keys() - writable:
            raise BatchError(f"Invalid writable columns for operation {index}")
        if operation["operation"] == "INSERT" and set(values) != writable:
            raise BatchError(f"INSERT operation {index} requires all business columns")
        for name, value in (key | values).items():
            typed_value(value, columns[name])
        source_time(operation["source_updated_at"])


@dataclass(frozen=True)
class Batch:
    payload: dict
    manifest: dict


def read_json(path: Path, data: bytes | None = None) -> dict:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict:
        result = {}
        for key, value in pairs:
            if key in result:
                raise BatchError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_bytes() if data is None else data, object_pairs_hook=unique_object
        )
    except (OSError, ValueError) as exc:
        raise BatchError(f"Cannot read {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise BatchError(f"{path.name} must be a JSON object")
    return value


def load_batch(directory: Path, contract: dict) -> Batch:
    manifest = read_json(directory / "manifest.json")
    payload_path = directory / "operations.json"
    try:
        payload_bytes = payload_path.read_bytes()
    except OSError as exc:
        raise BatchError(f"Cannot read operations.json: {exc}") from exc
    payload = read_json(payload_path, payload_bytes)
    validate_payload(payload, contract)
    if (
        type(manifest.get("format_version")) is not int
        or manifest["format_version"] != 1
        or manifest.get("payload_file") != "operations.json"
    ):
        raise BatchError("Unsupported manifest format or payload filename")
    expected = {
        "batch_id": payload["batch_id"],
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "contract_sha256": contract_hash(contract),
        "operation_count": len(payload["operations"]),
        "operation_counts": operation_counts(payload["operations"]),
    }
    for name, value in expected.items():
        if canonical_bytes(manifest.get(name)) != canonical_bytes(value):
            raise BatchError(f"Manifest {name} mismatch")
    return Batch(payload, manifest)


def write_batch(directory: Path, payload: dict, contract: dict, expected: dict) -> None:
    """Publish a complete directory; existing identical artifacts are a no-op."""
    validate_payload(payload, contract)
    data = canonical_bytes(payload)
    manifest = canonical_bytes(
        {
            "format_version": 1,
            "batch_id": payload["batch_id"],
            "payload_file": "operations.json",
            "payload_sha256": hashlib.sha256(data).hexdigest(),
            "contract_sha256": contract_hash(contract),
            "operation_count": len(payload["operations"]),
            "operation_counts": operation_counts(payload["operations"]),
            "expected": expected,
        }
    )
    if directory.exists():
        if (directory / "operations.json").read_bytes() == data and (
            directory / "manifest.json"
        ).read_bytes() == manifest:
            return
        raise BatchError(f"Refusing to overwrite a different batch: {directory}")
    directory.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{directory.name}.", dir=directory.parent)
    )
    try:
        (temporary / "operations.json").write_bytes(data)
        (temporary / "manifest.json").write_bytes(manifest)
        temporary.rename(directory)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
