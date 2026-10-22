"""Full geolocation observations: exact coordinates and physical source-row grain."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def coordinate_value(value: object, bound: int) -> Decimal | None:
    if not isinstance(value, (str, Decimal)) or (
        isinstance(value, str)
        and re.fullmatch(
            r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value
        )
        is None
    ):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() and -bound <= number <= bound else None


def coordinate_errors(latitude: str | None, longitude: str | None) -> list[str]:
    return [
        f"invalid_geolocation_{name}"
        for name, value, bound in (("lat", latitude, 90), ("lng", longitude, 180))
        if coordinate_value(value, bound) is None
    ]


def transform_geolocation(frame: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from olist_platform.bronze.storage import BronzeError

    from .customers import STATES
    from .validation import classify_versions

    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise BronzeError("Silver processing requires a UTC Spark session")
    columns = frame.columns
    for name in (
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
        "geolocation_lat",
        "geolocation_lng",
        "source_batch_id",
    ):
        value = F.trim(F.col(name))
        frame = frame.withColumn(name, F.when(F.length(value) > 0, value))
    frame = frame.withColumn("geolocation_city", F.lower("geolocation_city"))
    frame = frame.withColumn("geolocation_state", F.upper("geolocation_state"))
    errors = [
        F.when(
            F.col("geolocation_row_id").isNull() | (F.col("geolocation_row_id") <= 0),
            "invalid_geolocation_row_id",
        ),
        F.when(
            F.col("geolocation_zip_code_prefix").isNull()
            | ~F.col("geolocation_zip_code_prefix").rlike(r"^[0-9]{5}$"),
            "invalid_geolocation_zip_code_prefix",
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
    # Decimal validation preserves arbitrary source precision instead of rounding to a Spark decimal scale.
    coords = F.udf(coordinate_errors, T.ArrayType(T.StringType(), False))
    frame = frame.withColumn(
        "error_rule_ids",
        F.concat(
            F.array_compact(F.array(*errors)),
            coords("geolocation_lat", "geolocation_lng"),
        ),
    )
    frame = frame.withColumn(
        "warning_rule_ids",
        F.array_compact(
            F.array(
                F.when(F.col("geolocation_city").isNull(), "missing_geolocation_city"),
                F.when(
                    F.col("geolocation_state").isNull()
                    | ~F.col("geolocation_state").isin(*STATES),
                    "invalid_geolocation_state",
                ),
            )
        ),
    )
    return classify_versions(frame, "geolocation_row_id").select(
        *columns, "error_rule_ids", "warning_rule_ids", "_routing"
    )
