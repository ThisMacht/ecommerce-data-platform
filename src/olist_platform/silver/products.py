"""Full product snapshots: nullable attributes, nonnegative measurements, Type 1 grain."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError

from .validation import classify_versions

MEASUREMENTS = (
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)


def transform_products(frame: DataFrame) -> DataFrame:
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    for name in ("product_id", "product_category_name", "source_batch_id"):
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    frame = frame.withColumn("product_category_name", F.lower("product_category_name"))
    errors = [
        F.when(F.col("product_id").isNull(), "missing_product_id"),
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
    warnings = [
        F.when(F.col("product_category_name").isNull(), "missing_product_category_name")
    ]
    for name in MEASUREMENTS:
        value = F.col(name)
        errors.append(F.when((value < 0) | (value > 2147483647), f"invalid_{name}"))
        warnings.append(F.when(value.isNull(), f"missing_{name}"))
        if name in MEASUREMENTS[3:]:
            warnings.append(F.when(value == 0, f"zero_{name}"))
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
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn("warning_rule_ids", F.array_compact(F.array(*warnings)))
    return classify_versions(frame, "product_id").select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )
