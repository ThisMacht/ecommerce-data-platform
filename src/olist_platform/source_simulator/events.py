"""Deterministic, bounded-memory synthetic website-event artifacts."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Iterator

from .batches import BatchError, canonical_bytes, source_time

VERSION = "synthetic-events-v1"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
TEMPLATES = (
    ("page_view", "product_view"),
    ("page_view", "product_view", "add_to_cart"),
    ("page_view", "product_view", "add_to_cart", "checkout_started"),
    ("page_view", "product_view", "add_to_cart", "checkout_started", "purchase"),
)
SCENARIOS = ("anonymous_browse", "cart_abandonment", "checkout_abandonment", "purchase")
SCHEMA = {
    "event_id": "string",
    "session_id": "string",
    "anonymous_id": "string",
    "customer_unique_id": "string|null",
    "event_type": "string",
    "event_time": "UTC timestamp string",
    "received_at": "UTC timestamp string",
    "product_id": "string|null",
    "order_id": "string|null",
}


def _request(rows: int, seed: int) -> dict:
    if type(rows) is not int or not 1 <= rows <= 10_000_000:
        raise BatchError("Event rows must be an integer between 1 and 10000000")
    if type(seed) is not int or not 0 <= seed < 2**32:
        raise BatchError("Event seed must be a 32-bit unsigned integer")
    return {"rows": rows, "seed": seed}


def _identifier(kind: str, seed: int, number: int) -> str:
    return f"syn_{kind}_{seed:08x}_{number:012d}"


def _logical_events(total: int, seed: int) -> Iterator[dict]:
    ordinal = session = 0
    while ordinal < total:
        template = TEMPLATES[session % 4]
        jitter = hashlib.sha256(f"{VERSION}:{seed}:{session}".encode()).digest()
        visitor = session // 2
        for step, event_type in enumerate(template):
            if ordinal == total:
                return
            event_time = START + timedelta(seconds=session * 5 + step)
            if ordinal % 20 == 19:
                delay = 72 * 3600
            elif ordinal % 7 == 6:
                delay = 20
            else:
                delay = 1
            yield {
                "event_id": _identifier("event", seed, ordinal),
                "session_id": _identifier("session", seed, session),
                "anonymous_id": _identifier("visitor", seed, visitor),
                "customer_unique_id": None
                if session % 4 == 0
                else _identifier("customer", seed, visitor),
                "event_type": event_type,
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "received_at": (
                    event_time + timedelta(seconds=delay + jitter[step] % 4)
                )
                .isoformat()
                .replace("+00:00", "Z"),
                "product_id": None
                if event_type == "page_view"
                else _identifier(
                    "product", seed, int.from_bytes(jitter[:4], "big") % 50000
                ),
                "order_id": _identifier("order", seed, session)
                if event_type == "purchase"
                else None,
            }
            ordinal += 1
        session += 1


def _arrivals(total: int, seed: int) -> Iterator[dict]:
    pending = []
    for event in _logical_events(total, seed):
        heapq.heappush(pending, (event["received_at"], event["event_id"], event))
        # Future events cannot arrive before the current source-time watermark.
        while pending and pending[0][0] <= event["event_time"]:
            yield heapq.heappop(pending)[2]
    while pending:
        yield heapq.heappop(pending)[2]


def iter_events(rows: int, seed: int) -> Iterator[dict]:
    _request(rows, seed)
    arrivals = _arrivals(rows - rows // 100, seed)
    previous = None
    for position in range(1, rows + 1):
        event = previous if position % 100 == 0 else next(arrivals)
        yield event
        previous = event


def _sessions(unique: int, seed: int) -> dict:
    cycles, remaining = divmod(unique, 14)
    completed = cycles * 4
    for size in (2, 3, 4, 5):
        if remaining < size:
            break
        completed += 1
        remaining -= size
    return {
        "completed_by_scenario": {
            name: (completed + 3 - index) // 4 for index, name in enumerate(SCENARIOS)
        },
        "partial_session_id": _identifier("session", seed, completed)
        if remaining
        else None,
    }


def _manifest(stream: BinaryIO, rows: int, seed: int, *, verify: bool) -> dict:
    digest = hashlib.sha256()
    size = 0
    latest = ""
    counts = {
        "physical_rows": rows,
        "unique_events": rows - rows // 100,
        "duplicate_rows": rows // 100,
        "late_events": 0,
        "out_of_order_events": 0,
        "anonymous_events": 0,
        "event_types": dict.fromkeys(TEMPLATES[-1], 0),
    }
    for position, event in enumerate(iter_events(rows, seed), 1):
        data = canonical_bytes(event)
        if verify:
            if stream.read(len(data)) != data:
                raise BatchError(f"Event payload differs at physical row {position}")
        else:
            stream.write(data)
        digest.update(data)
        size += len(data)
        if position % 100:
            counts["event_types"][event["event_type"]] += 1
            counts["anonymous_events"] += event["customer_unique_id"] is None
            counts["out_of_order_events"] += event["event_time"] < latest
            latest = max(latest, event["event_time"])
            counts["late_events"] += (
                source_time(event["received_at"]) - source_time(event["event_time"])
            ).total_seconds() > 48 * 3600
    if verify and stream.read(1):
        raise BatchError("Unexpected trailing event payload")
    return {
        "format_version": 1,
        "generator_version": VERSION,
        "request": _request(rows, seed),
        "schema": SCHEMA.copy(),
        "event_file": {
            "name": "events.jsonl",
            "size_bytes": size,
            "sha256": digest.hexdigest(),
        },
        "counts": counts,
        "sessions": _sessions(counts["unique_events"], seed),
    }


def _load_manifest(directory: Path) -> dict:
    if directory.is_symlink() or not directory.is_dir():
        raise BatchError("Expected an immutable event artifact directory")
    names = {"events.jsonl", "manifest.json", "_SUCCESS"}
    if {p.name for p in directory.iterdir()} != names or any(
        (directory / name).is_symlink() or not (directory / name).is_file()
        for name in names
    ):
        raise BatchError("Incomplete or unexpected event artifact files")
    if any(
        (directory / name).stat().st_size > 65536
        for name in ("manifest.json", "_SUCCESS")
    ):
        raise BatchError("Event metadata is too large")
    body = (directory / "manifest.json").read_bytes()
    manifest = json.loads(body)
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("format_version")) is not int
        or manifest["format_version"] != 1
        or manifest.get("generator_version") != VERSION
        or not isinstance(manifest.get("request"), dict)
        or set(manifest["request"]) != {"rows", "seed"}
    ):
        raise BatchError("Unsupported event manifest or request")
    _request(**manifest["request"])
    expected = canonical_bytes({"manifest_sha256": hashlib.sha256(body).hexdigest()})
    if (directory / "_SUCCESS").read_bytes() != expected:
        raise BatchError("Event manifest checksum/marker mismatch")
    return manifest


def verify(directory: Path) -> dict:
    manifest = _load_manifest(directory)
    with (directory / "events.jsonl").open("rb") as stream:
        expected = _manifest(stream, **manifest["request"], verify=True)
    if (directory / "manifest.json").read_bytes() != canonical_bytes(expected):
        raise BatchError("Event manifest counts/schema/checksum mismatch")
    return expected


def generate(directory: Path, rows: int = 2_000_000, seed: int = 42) -> dict:
    request = _request(rows, seed)
    if directory.exists() or directory.is_symlink():
        if _load_manifest(directory)["request"] != request:
            raise BatchError("Refusing to overwrite events with another request")
        return {"status": "NOOP", "manifest": verify(directory)}
    directory.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{directory.name}.", dir=directory.parent)
    )
    try:
        with (temporary / "events.jsonl").open("wb") as stream:
            manifest = _manifest(stream, rows, seed, verify=False)
        body = canonical_bytes(manifest)
        (temporary / "manifest.json").write_bytes(body)
        (temporary / "_SUCCESS").write_bytes(
            canonical_bytes({"manifest_sha256": hashlib.sha256(body).hexdigest()})
        )
        temporary.rename(directory)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return {"status": "GENERATED", "manifest": manifest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser("generate")
    generate_parser.add_argument("--rows", type=int, default=2_000_000)
    generate_parser.add_argument("--seed", type=int, default=42)
    for command in (generate_parser, commands.add_parser("verify")):
        command.add_argument(
            "--output-dir", type=Path, default=Path("data/generated/events/scale_v1")
        )
    args = parser.parse_args()
    try:
        result = (
            generate(args.output_dir, args.rows, args.seed)
            if args.command == "generate"
            else {"status": "VALID", "manifest": verify(args.output_dir)}
        )
    except (OSError, ValueError) as error:
        print(f"Synthetic events: FAIL: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
