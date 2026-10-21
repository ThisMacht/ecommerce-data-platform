"""Composite-grain item/payment validation with exact decimal money."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_platform.bronze.extraction import KEYS
from olist_platform.bronze.storage import BronzeError

from .validation import classify_versions


def transform_order_children(frame: DataFrame, table: str) -> DataFrame:
    if table not in ("order_items", "order_payments"):
        raise BronzeError("Unsupported order child table")
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    texts = ["order_id", "source_batch_id"] + (
        ["product_id", "seller_id"] if table == "order_items" else ["payment_type"]
    )
    for name in texts:
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    errors = [
        F.when(F.col(name).isNull(), f"missing_{name}")
        for name in texts
        if name != "payment_type"
    ]
    ordinal = KEYS[table][1]
    errors.extend(
        [
            F.when(
                F.col(ordinal).isNull() | (F.col(ordinal) <= 0), f"invalid_{ordinal}"
            ),
            F.when(
                F.col("source_sequence").isNull() | (F.col("source_sequence") <= 0),
                "invalid_source_sequence",
            ),
            F.when(
                F.col("source_operation").isNull()
                | ~F.col("source_operation").isin("INSERT", "UPDATE"),
                "invalid_source_operation",
            ),
        ]
    )
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
    money = ("price", "freight_value") if table == "order_items" else ("payment_value",)
    for name in money:
        raw = f"_raw_{name}"
        frame = frame.withColumn(raw, F.col(name))
        parsed = F.expr(f"try_cast(`{raw}` AS DECIMAL(18,2))")
        # Reject extra precision rather than let decimal casts round money silently.
        invalid = (
            F.col(raw).isNull()
            | ~F.col(raw).rlike(r"^\d{1,16}(\.\d{1,2})?$")
            | parsed.isNull()
        )
        errors.append(F.when(invalid, f"invalid_{name}"))
        frame = frame.withColumn(name, parsed)
    warnings = []
    if table == "order_payments":
        frame = frame.withColumn("payment_type", F.lower("payment_type"))
        errors.extend(
            [
                F.when(
                    F.col("payment_type").isNull()
                    | ~F.col("payment_type").isin(
                        "credit_card", "boleto", "voucher", "debit_card", "not_defined"
                    ),
                    "invalid_payment_type",
                ),
                F.when(
                    F.col("payment_installments").isNull()
                    | (F.col("payment_installments") < 0),
                    "invalid_payment_installments",
                ),
            ]
        )
        warnings = [
            F.when(F.col("payment_type") == "not_defined", "undefined_payment_type"),
            F.when(F.col("payment_installments") == 0, "zero_payment_installments"),
            F.when(F.col("payment_value") == 0, "zero_payment_value"),
        ]
    else:
        raw = "_raw_shipping_limit_date"
        frame = frame.withColumn(raw, F.col("shipping_limit_date"))
        local = F.when(
            F.col(raw).rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?$"),
            F.try_to_timestamp(raw, F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")),
        )
        zone = "America/Sao_Paulo"
        utc = F.to_utc_timestamp(local, zone)
        ambiguous = (
            F.from_utc_timestamp(utc + F.expr("INTERVAL 1 HOUR"), zone) == local
        ) | (F.from_utc_timestamp(utc - F.expr("INTERVAL 1 HOUR"), zone) == local)
        gap = F.from_utc_timestamp(utc, zone) != local
        errors.append(
            F.when(local.isNull() | gap | ambiguous, "invalid_shipping_limit_date")
        )
        frame = frame.withColumn("shipping_limit_date", utc)
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn(
        "warning_rule_ids", F.array_compact(F.array(*warnings)).cast("array<string>")
    )
    return classify_versions(frame, KEYS[table]).select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )


def check_order_dependencies(children: DataFrame, orders: DataFrame) -> DataFrame:
    keys = orders.select(F.col("order_id").alias("_parent_order_id"))
    joined = children.join(keys, children.order_id == keys._parent_order_id, "left")
    missing = (F.col("_routing") == "valid") & F.col("_parent_order_id").isNull()
    return (
        joined.withColumn(
            "deferred_rule_ids",
            F.when(missing, F.array(F.lit("missing_order"))).otherwise(
                F.array().cast("array<string>")
            ),
        )
        .withColumn(
            "_routing", F.when(missing, "deferred").otherwise(F.col("_routing"))
        )
        .drop("_parent_order_id")
    )
