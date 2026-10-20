"""Explicit-schema Spark normalization, version deduplication and row routing."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from olist_platform.bronze.storage import BronzeError

BUSINESS_TIMES = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)
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
STATUSES = (
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable",
)


def spark_session(master: str = "local[2]") -> SparkSession:
    if master != "local" and not master.startswith("local["):
        raise BronzeError("Silver temporary staging supports local Spark masters only")
    spark = (
        SparkSession.builder.master(master)
        .appName("olist-orders-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def input_schema(contract: dict) -> T.StructType:
    table = next(t for t in contract["tables"] if t["table"] == "orders")
    fields = []
    for column in table["columns"] + contract["metadata_columns"]:
        kind = T.LongType() if column["name"] == "source_sequence" else T.StringType()
        fields.append(T.StructField(column["name"], kind))
    fields.extend(
        T.StructField(
            name,
            T.LongType()
            if name in ("_run_attempt", "_source_row_number")
            else T.StringType(),
        )
        for name in LINEAGE
    )
    fields.append(T.StructField("_raw_payload", T.StringType()))
    return T.StructType(fields)


def transform_orders(
    frame: DataFrame, timezone: str = "America/Sao_Paulo"
) -> DataFrame:
    """Return one classified row for each distinct physical input occurrence."""
    if timezone != "America/Sao_Paulo":
        raise BronzeError("Unsupported business timezone")
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    if frame.groupBy("_record_uid").count().filter("count > 1").limit(1).count():
        raise BronzeError("Repeated physical record UID in Bronze input")
    raw = frame
    for name in ("order_id", "customer_id", "order_status", "source_batch_id"):
        trimmed = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(trimmed) > 0, trimmed))
    frame = frame.withColumn("order_status", F.lower("order_status"))
    errors = [
        F.when(F.col("order_id").isNull(), "missing_order_id"),
        F.when(F.col("customer_id").isNull(), "missing_customer_id"),
        F.when(
            F.col("order_status").isNull() | ~F.col("order_status").isin(*STATUSES),
            "invalid_order_status",
        ),
        F.when(
            F.col("source_sequence").isNull() | (F.col("source_sequence") <= 0),
            "invalid_source_sequence",
        ),
        F.when(F.col("source_batch_id").isNull(), "missing_source_batch_id"),
        F.when(
            F.col("source_operation").isNull()
            | ~F.col("source_operation").isin("INSERT", "UPDATE"),
            "invalid_source_operation",
        ),
    ]
    warnings = []
    for name in BUSINESS_TIMES:
        raw_name = f"_raw_{name}"
        frame = frame.withColumn(raw_name, F.col(name))
        local = F.try_to_timestamp(
            F.col(raw_name), F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
        )
        # Offset-bearing source business timestamps violate their naive-time contract.
        local = F.when(
            F.col(raw_name).rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?$"),
            local,
        )
        frame = frame.withColumn(f"_local_{name}", local)
        local_col = F.col(f"_local_{name}")
        utc = F.to_utc_timestamp(local_col, timezone)
        # Sao Paulo's relevant DST transitions shift by one hour. Reject both
        # gaps and repeated wall-clock times rather than silently choosing a fold.
        ambiguous = (
            F.from_utc_timestamp(utc + F.expr("INTERVAL 1 HOUR"), timezone) == local_col
        ) | (
            F.from_utc_timestamp(utc - F.expr("INTERVAL 1 HOUR"), timezone) == local_col
        )
        gap = F.from_utc_timestamp(utc, timezone) != local_col
        invalid = F.col(raw_name).isNotNull() & (local_col.isNull() | gap | ambiguous)
        if name in ("order_purchase_timestamp", "order_estimated_delivery_date"):
            invalid = invalid | F.col(raw_name).isNull()
        errors.append(F.when(invalid, f"invalid_{name}"))
        frame = frame.withColumn(name, utc)
    frame = frame.withColumn(
        "order_purchase_date", F.to_date("_local_order_purchase_timestamp")
    )
    for name in ("source_updated_at", "source_loaded_at"):
        raw_name = f"_raw_{name}"
        frame = frame.withColumn(raw_name, F.col(name))
        parsed = F.try_to_timestamp(raw_name)
        errors.append(
            F.when(
                F.col(raw_name).isNull()
                | ~F.col(raw_name).rlike(r"(Z|[+-]\d{2}:\d{2})$")
                | parsed.isNull(),
                f"invalid_{name}",
            )
        )
        frame = frame.withColumn(name, parsed)
    ingested = F.to_timestamp("_ingested_at")
    errors.extend(
        [
            F.when(
                F.col("order_purchase_timestamp") > ingested,
                "future_purchase_timestamp",
            ),
            F.when(
                F.col("order_estimated_delivery_date")
                < F.col("order_purchase_timestamp"),
                "estimated_before_purchase",
            ),
        ]
    )
    warnings.extend(
        [
            F.when(
                (F.col("order_status") == "delivered")
                & F.col("order_delivered_customer_date").isNull(),
                "delivered_without_timestamp",
            ),
            F.when(
                F.col("order_delivered_customer_date")
                > F.col("order_estimated_delivery_date") + F.expr("INTERVAL 30 DAYS"),
                "delivery_delay_over_30d",
            ),
            F.when(
                F.col("order_approved_at") < F.col("order_purchase_timestamp"),
                "approval_before_purchase",
            ),
        ]
    )
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn("warning_rule_ids", F.array_compact(F.array(*warnings)))
    eligible = F.col("order_id").isNotNull() & (F.col("source_sequence") > 0)
    conflicts = (
        frame.filter(eligible)
        .groupBy("order_id", "source_sequence")
        .agg(F.countDistinct(F.sha2("_raw_payload", 256)).alias("versions"))
        .filter("versions > 1")
        .limit(1)
        .count()
    )
    if conflicts:
        raise BronzeError("Conflicting payloads for the same order/source version")
    frame = frame.withColumn(
        "_dedupe_key",
        F.when(eligible, F.concat(F.lit("key:"), F.col("order_id"))).otherwise(
            F.concat(F.lit("uid:"), F.col("_record_uid"))
        ),
    )
    window = Window.partitionBy("_dedupe_key").orderBy(
        F.col("source_sequence").desc_nulls_last(), F.col("_record_uid").desc()
    )
    frame = frame.withColumn("_rank", F.row_number().over(window))
    frame = frame.withColumn(
        "_routing",
        F.when(F.col("_rank") > 1, "duplicates")
        .when(F.size("error_rule_ids") > 0, "rejected")
        .otherwise("valid"),
    )
    columns = raw.columns + [
        "order_purchase_date",
        "error_rule_ids",
        "warning_rule_ids",
        "_routing",
    ]
    return frame.select(*columns)
