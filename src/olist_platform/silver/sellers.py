"""Full seller snapshots with warning-only location quality checks."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError

from .customers import STATES
from .validation import classify_versions


def transform_sellers(frame: DataFrame) -> DataFrame:
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    for name in (
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "source_batch_id",
    ):
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    frame = frame.withColumn("seller_city", F.lower("seller_city"))
    frame = frame.withColumn("seller_state", F.upper("seller_state"))
    errors = [
        F.when(F.col("seller_id").isNull(), "missing_seller_id"),
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
    for name in ("source_updated_at", "source_loaded_at"):
        raw = f"_raw_{name}"
        frame = frame.withColumn(raw, F.col(name))
        parsed = F.try_to_timestamp(raw)
        errors.append(
            F.when(
                F.col(raw).isNull()
                | ~F.col(raw).rlike(r"(Z|[+-]\d{2}:\d{2})$")
                | parsed.isNull(),
                f"invalid_{name}",
            )
        )
        frame = frame.withColumn(name, parsed)
    warnings = [
        F.when(F.col("seller_city").isNull(), "missing_seller_city"),
        F.when(
            F.col("seller_zip_code_prefix").isNull()
            | ~F.col("seller_zip_code_prefix").rlike(r"^\d{5}$"),
            "invalid_seller_zip_code_prefix",
        ),
        F.when(
            F.col("seller_state").isNull() | ~F.col("seller_state").isin(*STATES),
            "invalid_seller_state",
        ),
    ]
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn("warning_rule_ids", F.array_compact(F.array(*warnings)))
    return classify_versions(frame, "seller_id").select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )
