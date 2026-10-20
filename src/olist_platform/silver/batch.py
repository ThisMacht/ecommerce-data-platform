"""Validate immutable inputs and seal Spark Parquet outputs with a final marker."""

from __future__ import annotations

import json
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from botocore.exceptions import ClientError

from olist_platform.bronze.storage import (
    BronzeError,
    ObjectStore,
    encode,
    sha256,
    verify_published,
)

ROUTES = ("valid", "rejected", "duplicates")
if TYPE_CHECKING:
    from pyspark.sql import SparkSession

PROCESSOR_VERSION = "orders-silver-v1"
LINEAGE = (
    "_record_uid",
    "_logical_record_uid",
    "_ingested_at",
    "_run_id",
    "_run_attempt",
    "_source_name",
    "_source_file",
    "_source_row_number",
    "_schema_version",
)


def optional_read(store: ObjectStore, key: str) -> bytes | None:
    try:
        return store.read(key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            raise
        return None


def source_columns(contract: dict) -> list[dict]:
    table = next(t for t in contract["tables"] if t["table"] == "orders")
    return table["columns"] + contract["metadata_columns"]


def validate_manifest(manifest: dict, contract: dict) -> None:
    if contract["contract_version"] != "1.0.0" or manifest["schema_version"] != "1.0.0":
        raise BronzeError("Unsupported Silver input schema version")
    if manifest["source"] != "postgresql.orders" or manifest[
        "source_schema"
    ] != source_columns(contract):
        raise BronzeError("Bronze schema does not match orders contract")
    if manifest["config"]["contract_sha256"] != sha256(encode(contract)):
        raise BronzeError("Bronze source contract hash mismatch")
    if contract["policies"]["source_timezone"] != "America/Sao_Paulo":
        raise BronzeError("Unsupported business timezone")


def validate_record(
    row: dict, contract: dict, manifest: dict, key: str, ordinal: int
) -> dict:
    columns = source_columns(contract)
    if not isinstance(row, dict) or set(row) != {c["name"] for c in columns} | set(
        LINEAGE
    ):
        raise BronzeError("Bronze row columns differ from declared schema")
    for column in columns:
        value = row[column["name"]]
        expected = int if column["name"] == "source_sequence" else str
        if value is not None and type(value) is not expected:
            raise BronzeError(f"Bronze physical type mismatch: {column['name']}")
    expected_uid = sha256(encode([manifest["run_id"], key, ordinal]))
    if (
        row["_record_uid"] != expected_uid
        or row["_source_file"] != key
        or type(row["_source_row_number"]) is not int
        or row["_source_row_number"] != ordinal
        or type(row["_run_attempt"]) is not int
        or row["_run_attempt"] != 1
        or row["_run_id"] != manifest["run_id"]
        or row["_source_name"] != "postgresql.orders"
        or row["_schema_version"] != "1.0.0"
        or row["_ingested_at"] != manifest["prepared_at"]
    ):
        raise BronzeError("Invalid Bronze physical lineage")
    instant = datetime.fromisoformat(row["_ingested_at"].replace("Z", "+00:00"))
    if instant.utcoffset() is None:
        raise BronzeError("Bronze ingestion timestamp requires an offset")
    logical = None
    if row["order_id"] is not None and row["source_sequence"] is not None:
        logical = sha256(
            encode(["postgresql.orders", row["order_id"], row["source_sequence"]])
        )
    if row["_logical_record_uid"] != logical:
        raise BronzeError("Invalid Bronze logical lineage")
    return dict(
        row,
        _raw_payload=encode({c["name"]: row[c["name"]] for c in columns})
        .decode()
        .rstrip("\n"),
    )


def verify_objects(store: ObjectStore, manifest: dict) -> None:
    prefix = manifest["prefix"]
    if (
        manifest["format_version"] != 1
        or manifest["schema_version"] != PROCESSOR_VERSION
    ):
        raise BronzeError("Unsupported Silver manifest version")
    if sha256(encode(manifest["request"])) != manifest["request_sha256"]:
        raise BronzeError("Silver request checksum mismatch")
    counts = manifest["counts"]
    if set(counts) != {
        "input_rows",
        "valid_rows",
        "rejected_rows",
        "duplicates_rows",
        "deferred_rows",
        "warning_rows",
    } or any(type(count) is not int or count < 0 for count in counts.values()):
        raise BronzeError("Invalid Silver counters")
    if (
        counts["input_rows"] != sum(counts[f"{route}_rows"] for route in ROUTES)
        or counts["deferred_rows"] != 0
    ):
        raise BronzeError("Silver routing reconciliation mismatch")
    if not 0 <= counts["warning_rows"] <= counts["valid_rows"]:
        raise BronzeError("Silver warning count mismatch")
    if sha256(encode(manifest["output_schema"])) != manifest["schema_fingerprint"]:
        raise BronzeError("Silver schema fingerprint mismatch")
    keys = set()
    for obj in manifest["objects"]:
        if (
            obj["route"] not in ROUTES
            or not obj["key"].startswith(f"{prefix}/attempt=")
            or not obj["key"].endswith(".parquet")
            or obj["key"] in keys
        ):
            raise BronzeError("Invalid Silver object location")
        keys.add(obj["key"])
        body = store.read(obj["key"])
        if sha256(body) != obj["sha256"] or len(body) != obj["size_bytes"]:
            raise BronzeError("Silver object checksum/size mismatch")
    for route in ROUTES:
        if counts[f"{route}_rows"] > 0 and not any(
            obj["route"] == route for obj in manifest["objects"]
        ):
            raise BronzeError("Missing Silver route objects")


def verify_silver(store: ObjectStore, prefix: str) -> dict:
    marker = json.loads(store.read(f"{prefix}/_SUCCESS"))
    body = store.read(f"{prefix}/manifest.json")
    if marker != {"manifest_sha256": sha256(body)}:
        raise BronzeError("Silver manifest checksum mismatch")
    manifest = json.loads(body)
    if manifest["prefix"] != prefix:
        raise BronzeError("Silver manifest location mismatch")
    verify_objects(store, manifest)
    return manifest


def process_orders(
    spark: SparkSession,
    store: ObjectStore,
    contract: dict,
    *,
    bronze_prefix: str,
    run_id: str,
) -> dict:
    from .orders import input_schema, transform_orders

    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise BronzeError("Invalid Silver run_id")
    bronze = verify_published(store, bronze_prefix)
    validate_manifest(bronze, contract)
    bronze_body = store.read(f"{bronze_prefix}/manifest.json")
    if json.loads(bronze_body) != bronze or json.loads(
        store.read(f"{bronze_prefix}/_SUCCESS")
    ) != {"manifest_sha256": sha256(bronze_body)}:
        raise BronzeError("Bronze manifest changed while preparing Silver")
    request = {
        "processor_version": PROCESSOR_VERSION,
        "spark_version": spark.version,
        "run_id": run_id,
        "bronze_prefix": bronze_prefix,
        "bronze_manifest_sha256": sha256(bronze_body),
        "contract_sha256": sha256(encode(contract)),
    }
    prefix = f"silver/orders/run_id={run_id}"
    request_hash = sha256(encode(request))
    store.put_immutable(f"{prefix}/request.json", encode(request))
    existing = optional_read(store, f"{prefix}/manifest.json")
    if existing is not None:
        manifest = json.loads(existing)
        if manifest["request_sha256"] != request_hash:
            raise BronzeError("Silver run ID already sealed with another request")
        verify_objects(store, manifest)
        marker_existed = optional_read(store, f"{prefix}/_SUCCESS") is not None
        store.put_immutable(
            f"{prefix}/_SUCCESS", encode({"manifest_sha256": sha256(existing)})
        )
        return {
            "status": "NOOP" if marker_existed else "PUBLISHED",
            "manifest": verify_silver(store, prefix),
        }
    with tempfile.TemporaryDirectory(prefix="olist-silver-") as directory:
        root = Path(directory)
        inputs = root / "input.jsonl"
        with inputs.open("wb") as handle:
            for part in bronze["objects"]:
                body = store.read(part["key"])
                if sha256(body) != part["sha256"]:
                    raise BronzeError("Bronze changed while downloading")
                for ordinal, line in enumerate(body.splitlines(), 1):
                    handle.write(
                        encode(
                            validate_record(
                                json.loads(line), contract, bronze, part["key"], ordinal
                            )
                        )
                    )
        frame = (
            spark.read.schema(input_schema(contract))
            .option("mode", "FAILFAST")
            .json(str(inputs))
        )
        classified = transform_orders(frame).persist()
        try:
            from pyspark.sql import functions as F

            counts = {f"{route}_rows": 0 for route in ROUTES}
            for row in classified.groupBy("_routing").count().collect():
                counts[f"{row['_routing']}_rows"] = row["count"]
            counts["input_rows"] = sum(counts.values())
            counts["deferred_rows"] = 0
            counts["warning_rows"] = classified.filter(
                (F.col("_routing") == "valid") & (F.size("warning_rule_ids") > 0)
            ).count()
            if counts["input_rows"] != bronze["row_count"]:
                raise BronzeError("Bronze-to-Silver row count mismatch")
            selected = classified.filter(F.col("_routing") != "duplicates")
            violations = {
                row["rule"]: row["count"]
                for row in selected.select(
                    F.explode(F.concat("error_rule_ids", "warning_rule_ids")).alias(
                        "rule"
                    )
                )
                .groupBy("rule")
                .count()
                .collect()
            }
            output = root / "output"
            classified.repartition(2, "_routing").sortWithinPartitions(
                "order_id", "_record_uid"
            ).write.partitionBy("_routing").mode("error").parquet(str(output))
            parquet_paths = sorted(output.glob("_routing=*/*.parquet"))
            output_schema = classified.drop("_routing").schema.jsonValue()
            if parquet_paths:
                persisted = spark.read.parquet(str(output))
                physical_counts = {
                    row["_routing"]: row["count"]
                    for row in persisted.groupBy("_routing").count().collect()
                }
                if any(
                    physical_counts.get(route, 0) != counts[f"{route}_rows"]
                    for route in ROUTES
                ):
                    raise BronzeError("Parquet materialization row count mismatch")
                output_schema = persisted.drop("_routing").schema.jsonValue()
            objects = []
            attempt = uuid4().hex
            for path in parquet_paths:
                route = path.parent.name.removeprefix("_routing=")
                body = path.read_bytes()
                key = f"{prefix}/attempt={attempt}/{route}/{path.name}"
                store.put_immutable(key, body)
                objects.append(
                    {
                        "key": key,
                        "route": route,
                        "sha256": sha256(body),
                        "size_bytes": len(body),
                    }
                )
            manifest = {
                "format_version": 1,
                "schema_version": "orders-silver-v1",
                "run_id": run_id,
                "prefix": prefix,
                "request": request,
                "request_sha256": request_hash,
                "counts": counts,
                "rule_violations": violations,
                "relationship_checks": "NOT_IMPLEMENTED",
                "output_schema": output_schema,
                "objects": objects,
            }
            manifest["schema_fingerprint"] = sha256(encode(manifest["output_schema"]))
            verify_objects(store, manifest)
            body = encode(manifest)
            try:
                store.put_immutable(f"{prefix}/manifest.json", body)
            except BronzeError:
                # Another identical request can win publication with different
                # Spark physical file names; its complete sealed output wins.
                body = store.read(f"{prefix}/manifest.json")
                manifest = json.loads(body)
                if manifest["request_sha256"] != request_hash:
                    raise BronzeError("Concurrent Silver request conflict") from None
                verify_objects(store, manifest)
            store.put_immutable(
                f"{prefix}/_SUCCESS", encode({"manifest_sha256": sha256(body)})
            )
            return {"status": "PUBLISHED", "manifest": verify_silver(store, prefix)}
        finally:
            classified.unpersist()
