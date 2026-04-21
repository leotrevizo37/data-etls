from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    lit,
    sum as spark_sum,
    count,
    when as spark_when,
    unix_timestamp,
    from_unixtime,
    floor)

def stats_calculator(
        base: DataFrame,
        sensor_type: Optional[str] = "Corriente",
        min_expected_value: float = 0,
        max_expected_value: Optional[int] = None,
        ok_ratio: Optional[float] = 0.2
) -> Optional[DataFrame]:

    anom_condition = col("Value") < lit(min_expected_value)
    if max_expected_value is not None:
        anom_condition = anom_condition | (col("Value") > lit(max_expected_value))

    ok_cond = col("Value") >= lit(min_expected_value)
    if max_expected_value is not None:
        ok_cond = ok_cond & (col("Value") <= lit(max_expected_value))

    base = (
        base
        .filter((col("sensor_type") == sensor_type))
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
            ).cast("timestamp")
        ).withColumn(
            "is_anomaly",
            spark_when(
                anom_condition,
                lit(1),
            ).otherwise(lit(0)),
        )
    )

    stats_df = (
        base
        .groupBy("DeviceId", "HourStart", "SensorId", "LocationId", "SensorTyId", "sensor_type")
        .agg(
            avg("Value").alias("Val_dummy"),
            spark_sum(
                spark_when(
                    ok_cond,
                    1
                ).otherwise(0)
            ).alias("is_okay"),
            count("*").alias("total_readings"),
            spark_sum("is_anomaly").alias("Anomalies_raw"),
        )
        .withColumn(
            "StatusCode",
            spark_when(col("is_okay") / col("total_readings") >= ok_ratio, 1).otherwise(3)
        )
        .withColumn("Promedio", col("is_okay") / col("total_readings"))
        .withColumnRenamed("Anomalies_raw", "Anomalies")
        .withColumnRenamed("total_readings", "ReadingsCount")
    )

    stats_current_df = (
        stats_df
        .fillna({"Anomalies": 0})
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if stats_current_df.rdd.isEmpty():
        return None

    return stats_current_df