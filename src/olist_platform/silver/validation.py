"""Shared physical-row and source-version routing invariants."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from olist_platform.bronze.storage import BronzeError


def classify_versions(frame: DataFrame, key: str | tuple[str, ...]) -> DataFrame:
    keys = (key,) if isinstance(key, str) else key
    if frame.groupBy("_record_uid").count().filter("count > 1").limit(1).count():
        raise BronzeError("Repeated physical record UID in Bronze input")
    eligible = F.col("source_sequence") > 0
    for name in keys:
        eligible = eligible & F.col(name).isNotNull()
    conflicts = (
        frame.filter(eligible)
        .groupBy(*keys, "source_sequence")
        .agg(F.countDistinct(F.sha2("_raw_payload", 256)).alias("versions"))
        .filter("versions > 1")
        .limit(1)
        .count()
    )
    if conflicts:
        raise BronzeError("Conflicting payloads for the same key/source version")
    frame = frame.withColumn(
        "_dedupe_key",
        F.when(eligible, F.concat(F.lit("key:"), F.to_json(F.struct(*keys)))).otherwise(
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
    return frame.drop("_dedupe_key", "_rank")
