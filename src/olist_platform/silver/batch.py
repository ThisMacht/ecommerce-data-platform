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

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import (
    BronzeError,
    ObjectStore,
    encode,
    sha256,
    verify_published,
)

ROUTES = ("valid", "rejected", "duplicates")
CHILDREN = ("order_items", "order_payments")
VERSIONS = ("orders-silver-v1", "orders-silver-v2", "customers-silver-v1") + tuple(
    f"{table}-silver-v1" for table in CHILDREN
)
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


def source_columns(contract: dict, table: str = "orders") -> list[dict]:
    specification = next(t for t in contract["tables"] if t["table"] == table)
    return specification["columns"] + contract["metadata_columns"]


def validate_manifest(manifest: dict, contract: dict, table: str = "orders") -> None:
    if contract["contract_version"] != "1.0.0" or manifest["schema_version"] != "1.0.0":
        raise BronzeError("Unsupported Silver input schema version")
    if manifest["source"] != f"postgresql.{table}" or manifest[
        "source_schema"
    ] != source_columns(contract, table):
        raise BronzeError(f"Bronze schema does not match {table} contract")
    if manifest["config"]["contract_sha256"] != sha256(encode(contract)):
        raise BronzeError("Bronze source contract hash mismatch")
    if table == "customers" and manifest.get("snapshot_kind") != "FULL":
        raise BronzeError("Customer Silver requires a full Bronze snapshot")
    if (
        manifest.get("snapshot_kind") == "FULL"
        and manifest.get("extract_lower_bound") is not None
    ):
        raise BronzeError("Full snapshots cannot have an extraction lower bound")
    if contract["policies"]["source_timezone"] != "America/Sao_Paulo":
        raise BronzeError("Unsupported business timezone")


def validate_record(
    row: dict,
    contract: dict,
    manifest: dict,
    key: str,
    ordinal: int,
    table: str = "orders",
) -> dict:
    columns = source_columns(contract, table)
    if not isinstance(row, dict) or set(row) != {c["name"] for c in columns} | set(
        LINEAGE
    ):
        raise BronzeError("Bronze row columns differ from declared schema")
    for column in columns:
        value = row[column["name"]]
        expected = int if column["postgres_type"] in ("bigint", "integer") else str
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
        or row["_source_name"] != f"postgresql.{table}"
        or row["_schema_version"] != "1.0.0"
        or row["_ingested_at"] != manifest["prepared_at"]
    ):
        raise BronzeError("Invalid Bronze physical lineage")
    instant = datetime.fromisoformat(row["_ingested_at"].replace("Z", "+00:00"))
    if instant.utcoffset() is None:
        raise BronzeError("Bronze ingestion timestamp requires an offset")
    logical = None
    if (
        all(row[k] is not None for k in KEYS[table])
        and row["source_sequence"] is not None
    ):
        logical = sha256(
            encode(
                [
                    f"postgresql.{table}",
                    row[KEYS[table][0]]
                    if len(KEYS[table]) == 1
                    else [row[k] for k in KEYS[table]],
                    row["source_sequence"],
                ]
            )
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
    if manifest["format_version"] != 1 or manifest["schema_version"] not in VERSIONS:
        raise BronzeError("Unsupported Silver manifest version")
    if sha256(encode(manifest["request"])) != manifest["request_sha256"]:
        raise BronzeError("Silver request checksum mismatch")
    if manifest["schema_version"] == "orders-silver-v2":
        dependency = manifest["request"].get("customer_snapshot", {})
        if (
            manifest.get("relationship_checks") != "CUSTOMER_SNAPSHOT"
            or not isinstance(dependency.get("prefix"), str)
            or re.fullmatch(r"[0-9a-f]{64}", dependency.get("manifest_sha256", ""))
            is None
        ):
            raise BronzeError("Invalid pinned customer dependency")
    child = manifest["schema_version"] in tuple(
        f"{table}-silver-v1" for table in CHILDREN
    )
    if child:
        dependency = manifest["request"].get("order_snapshot", {})
        if (
            manifest.get("relationship_checks") != "ORDER_SNAPSHOT"
            or not isinstance(dependency.get("prefix"), str)
            or re.fullmatch(r"[0-9a-f]{64}", dependency.get("manifest_sha256", ""))
            is None
            or manifest.get("unchecked_relationships")
            != (
                ["product_id", "seller_id"]
                if manifest["schema_version"] == "order_items-silver-v1"
                else []
            )
        ):
            raise BronzeError("Invalid pinned order dependency/relationship scope")
    if (
        manifest["schema_version"] == "customers-silver-v1"
        and manifest.get("snapshot_kind") != "FULL"
    ):
        raise BronzeError("Customer Silver must declare a full snapshot")
    routes = ROUTES + (
        ("deferred",)
        if manifest["schema_version"] == "orders-silver-v2" or child
        else ()
    )
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
    if counts["input_rows"] != sum(counts[f"{route}_rows"] for route in routes) or (
        "deferred" not in routes and counts["deferred_rows"] != 0
    ):
        raise BronzeError("Silver routing reconciliation mismatch")
    if not 0 <= counts["warning_rows"] <= counts["valid_rows"]:
        raise BronzeError("Silver warning count mismatch")
    if sha256(encode(manifest["output_schema"])) != manifest["schema_fingerprint"]:
        raise BronzeError("Silver schema fingerprint mismatch")
    keys = set()
    for obj in manifest["objects"]:
        if (
            obj["route"] not in routes
            or not obj["key"].startswith(f"{prefix}/attempt=")
            or not obj["key"].endswith(".parquet")
            or obj["key"] in keys
        ):
            raise BronzeError("Invalid Silver object location")
        keys.add(obj["key"])
        body = store.read(obj["key"])
        if sha256(body) != obj["sha256"] or len(body) != obj["size_bytes"]:
            raise BronzeError("Silver object checksum/size mismatch")
    for route in routes:
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


def process_batch(
    spark: SparkSession,
    store: ObjectStore,
    contract: dict,
    *,
    bronze_prefix: str,
    run_id: str,
    table: str = "orders",
    customer_snapshot: str | None = None,
    order_snapshot: str | None = None,
) -> dict:
    from .customers import check_customer_dependencies, transform_customers
    from .order_children import check_order_dependencies, transform_order_children
    from .orders import input_schema, transform_orders

    if (
        table not in KEYS
        or (table != "orders" and customer_snapshot is not None)
        or (table not in CHILDREN and order_snapshot is not None)
        or (table in CHILDREN and order_snapshot is None)
    ):
        raise BronzeError("Unsupported Silver dataset/dependency configuration")
    processor = f"{table}-silver-v1"
    if table == "orders" and customer_snapshot is not None:
        processor = "orders-silver-v2"
    routes = ROUTES + (
        ("deferred",)
        if customer_snapshot is not None or order_snapshot is not None
        else ()
    )

    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise BronzeError("Invalid Silver run_id")
    bronze = verify_published(store, bronze_prefix)
    validate_manifest(bronze, contract, table)
    bronze_body = store.read(f"{bronze_prefix}/manifest.json")
    if json.loads(bronze_body) != bronze or json.loads(
        store.read(f"{bronze_prefix}/_SUCCESS")
    ) != {"manifest_sha256": sha256(bronze_body)}:
        raise BronzeError("Bronze manifest changed while preparing Silver")
    request = {
        "processor_version": processor,
        "spark_version": spark.version,
        "run_id": run_id,
        "bronze_prefix": bronze_prefix,
        "bronze_manifest_sha256": sha256(bronze_body),
        "contract_sha256": sha256(encode(contract)),
    }
    parent = None
    dependency_prefix = customer_snapshot or order_snapshot
    dependency_name = "customer" if customer_snapshot is not None else "order"
    if dependency_prefix is not None:
        parent = verify_silver(store, dependency_prefix)
        if (
            parent["schema_version"]
            not in (
                ("customers-silver-v1",)
                if customer_snapshot is not None
                else ("orders-silver-v1", "orders-silver-v2")
            )
            or parent.get("snapshot_kind") != "FULL"
            or parent["request"]["contract_sha256"] != request["contract_sha256"]
        ):
            raise BronzeError(
                f"{dependency_name.title()} dependency requires a full verified snapshot from the same source contract"
            )
        parent_body = store.read(f"{dependency_prefix}/manifest.json")
        if json.loads(parent_body) != parent or json.loads(
            store.read(f"{dependency_prefix}/_SUCCESS")
        ) != {"manifest_sha256": sha256(parent_body)}:
            raise BronzeError("Parent snapshot changed while preparing Silver")
        request[f"{dependency_name}_snapshot"] = {
            "prefix": dependency_prefix,
            "manifest_sha256": sha256(parent_body),
        }
    prefix = f"silver/{table}/run_id={run_id}"
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
                                json.loads(line),
                                contract,
                                bronze,
                                part["key"],
                                ordinal,
                                table,
                            )
                        )
                    )
        frame = (
            spark.read.schema(input_schema(contract, table))
            .option("mode", "FAILFAST")
            .json(str(inputs))
        )
        if table == "orders":
            classified = transform_orders(frame)
        elif table == "customers":
            classified = transform_customers(frame)
        else:
            classified = transform_order_children(frame, table)
        parent_keys = None
        if parent is not None:
            parent_keys = load_parent_keys(
                spark, store, parent, root / "parent", key=f"{dependency_name}_id"
            )
            classified = (
                check_customer_dependencies
                if customer_snapshot is not None
                else check_order_dependencies
            )(classified, parent_keys)
        classified = classified.persist()
        try:
            from pyspark.sql import functions as F

            counts = {f"{route}_rows": 0 for route in routes}
            for row in classified.groupBy("_routing").count().collect():
                counts[f"{row['_routing']}_rows"] = row["count"]
            counts["input_rows"] = sum(counts.values())
            counts.setdefault("deferred_rows", 0)
            counts["warning_rows"] = classified.filter(
                (F.col("_routing") == "valid") & (F.size("warning_rule_ids") > 0)
            ).count()
            if counts["input_rows"] != bronze["row_count"]:
                raise BronzeError("Bronze-to-Silver row count mismatch")
            selected = classified.filter(F.col("_routing") != "duplicates")
            violations = {
                row["rule"]: row["count"]
                for row in selected.select(
                    F.explode(
                        F.concat(
                            "error_rule_ids",
                            "warning_rule_ids",
                            *(["deferred_rule_ids"] if parent is not None else []),
                        )
                    ).alias("rule")
                )
                .groupBy("rule")
                .count()
                .collect()
            }
            output = root / "output"
            classified.repartition(2, "_routing").sortWithinPartitions(
                *KEYS[table], "_record_uid"
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
                    for route in routes
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
                "schema_version": processor,
                "run_id": run_id,
                "prefix": prefix,
                "request": request,
                "request_sha256": request_hash,
                "counts": counts,
                "rule_violations": violations,
                "relationship_checks": f"{dependency_name.upper()}_SNAPSHOT"
                if parent is not None
                else ("NOT_APPLICABLE" if table == "customers" else "NOT_IMPLEMENTED"),
                "output_schema": output_schema,
                "objects": objects,
            }
            if table in CHILDREN:
                manifest["unchecked_relationships"] = (
                    ["product_id", "seller_id"] if table == "order_items" else []
                )
            if bronze.get("snapshot_kind") == "FULL":
                manifest["snapshot_kind"] = "FULL"
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
            if parent_keys is not None:
                parent_keys.unpersist()


def process_orders(
    spark: SparkSession,
    store: ObjectStore,
    contract: dict,
    *,
    bronze_prefix: str,
    run_id: str,
    customer_snapshot: str | None = None,
) -> dict:
    return process_batch(
        spark,
        store,
        contract,
        bronze_prefix=bronze_prefix,
        run_id=run_id,
        customer_snapshot=customer_snapshot,
    )


def process_customers(
    spark: SparkSession,
    store: ObjectStore,
    contract: dict,
    *,
    bronze_prefix: str,
    run_id: str,
) -> dict:
    return process_batch(
        spark,
        store,
        contract,
        bronze_prefix=bronze_prefix,
        run_id=run_id,
        table="customers",
    )


def load_parent_keys(
    spark: SparkSession,
    store: ObjectStore,
    manifest: dict,
    directory: Path,
    *,
    key: str = "customer_id",
):
    from pyspark.sql import functions as F

    directory.mkdir()
    paths = []
    for number, obj in enumerate(manifest["objects"]):
        if obj["route"] != "valid":
            continue
        body = store.read(obj["key"])
        if sha256(body) != obj["sha256"]:
            raise BronzeError("Parent snapshot changed while downloading")
        path = directory / f"part-{number}.parquet"
        path.write_bytes(body)
        paths.append(str(path))
    if paths:
        data = spark.read.parquet(*paths)
        if data.schema[key].dataType.simpleString() != "string":
            raise BronzeError("Invalid parent snapshot key type")
        keys = data.select(key).cache()
    else:
        path = directory / "empty.jsonl"
        path.touch()
        keys = spark.read.schema(f"{key} string").json(str(path)).cache()
    try:
        if (
            keys.count() != manifest["counts"]["valid_rows"]
            or keys.filter(
                F.col(key).isNull()
                | (F.length(F.trim(key)) == 0)
                | (F.col(key) != F.trim(key))
            )
            .limit(1)
            .count()
            or keys.groupBy(key).count().filter("count > 1").limit(1).count()
        ):
            raise BronzeError("Invalid parent snapshot key set/count")
    except BaseException:
        keys.unpersist()
        raise
    return keys
