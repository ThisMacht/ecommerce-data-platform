"""Review/order composite grain, bounded scores and explicit local timestamp conversion."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError

from .validation import classify_versions


def transform_reviews(frame: DataFrame) -> DataFrame:
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    texts = ("review_id", "order_id", "source_batch_id")
    for name in texts:
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    for name in ("review_comment_title", "review_comment_message"):
        frame = frame.withColumn(
            name, F.when(F.length(F.trim(F.col(name))) > 0, F.col(name))
        )
    errors = [F.when(F.col(name).isNull(), f"missing_{name}") for name in texts]
    errors.extend(
        [
            F.when(
                F.col("review_score").isNull() | ~F.col("review_score").between(1, 5),
                "invalid_review_score",
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
    zone = "America/Sao_Paulo"
    for name in ("review_creation_date", "review_answer_timestamp"):
        raw = f"_raw_{name}"
        frame = frame.withColumn(raw, F.col(name))
        local = F.when(
            F.col(raw).rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?$"),
            F.try_to_timestamp(raw, F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")),
        )
        utc = F.to_utc_timestamp(local, zone)
        ambiguous = (
            F.from_utc_timestamp(utc + F.expr("INTERVAL 1 HOUR"), zone) == local
        ) | (F.from_utc_timestamp(utc - F.expr("INTERVAL 1 HOUR"), zone) == local)
        gap = F.from_utc_timestamp(utc, zone) != local
        errors.append(F.when(local.isNull() | gap | ambiguous, f"invalid_{name}"))
        frame = frame.withColumn(name, utc)
    frame = frame.withColumn("error_rule_ids", F.array_compact(F.array(*errors)))
    frame = frame.withColumn(
        "warning_rule_ids",
        F.array_compact(
            F.array(
                F.when(
                    F.col("review_answer_timestamp") < F.col("review_creation_date"),
                    "review_answer_before_creation",
                )
            )
        ),
    )
    return classify_versions(frame, ("review_id", "order_id")).select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )
