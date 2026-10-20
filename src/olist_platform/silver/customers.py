"""Customer snapshot normalization; shopper IDs are not the customer row grain."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError

from .validation import classify_versions

STATES = (
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
)


def transform_customers(frame: DataFrame) -> DataFrame:
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    for name in (
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "source_batch_id",
    ):
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    frame = frame.withColumn("customer_city", F.lower("customer_city"))
    frame = frame.withColumn("customer_state", F.upper("customer_state"))
    errors = [
        F.when(F.col("customer_id").isNull(), "missing_customer_id"),
        F.when(F.col("customer_unique_id").isNull(), "missing_customer_unique_id"),
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
        F.when(F.col("customer_city").isNull(), "missing_customer_city"),
        F.when(
            F.col("customer_zip_code_prefix").isNull()
            | ~F.col("customer_zip_code_prefix").rlike(r"^\d{5}$"),
            "invalid_customer_zip_code_prefix",
        ),
        F.when(
            F.col("customer_state").isNull() | ~F.col("customer_state").isin(*STATES),
            "invalid_customer_state",
        ),
    ]
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn("warning_rule_ids", F.array_compact(F.array(*warnings)))
    return classify_versions(frame, "customer_id").select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )


def check_customer_dependencies(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Only locally valid selected orders can become dependency-deferred."""
    keys = customers.select(F.col("customer_id").alias("_parent_customer_id"))
    joined = orders.join(keys, orders.customer_id == keys._parent_customer_id, "left")
    missing = (F.col("_routing") == "valid") & F.col("_parent_customer_id").isNull()
    return (
        joined.withColumn(
            "deferred_rule_ids",
            F.when(missing, F.array(F.lit("missing_customer"))).otherwise(
                F.array().cast("array<string>")
            ),
        )
        .withColumn(
            "_routing", F.when(missing, "deferred").otherwise(F.col("_routing"))
        )
        .drop("_parent_customer_id")
    )
