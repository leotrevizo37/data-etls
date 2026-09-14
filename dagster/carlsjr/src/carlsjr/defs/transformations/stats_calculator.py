from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_list,
    date_trunc,
    avg,
    lit,
    max as spark_max,
    min as spark_min,
    sort_array,
    struct,
    sum as spark_sum,
    count,
    to_json,
    when as spark_when,
    stddev)

def stats_calculator(
        base: DataFrame,
        sensor_type: Optional[str] = "Corriente",
        min_expected_value: Optional[float] = None,
        max_expected_value: Optional[float] = None
) -> Optional[DataFrame]:

    if sensor_type is not None:
        base = base.filter((col("sensor_type") == sensor_type))

    base = base.withColumn("HourStart", date_trunc("hour", col("TimeSpan")))

    agg_exprs = [
        avg("Value").alias("Val_dummy"),
        spark_min("Value").alias("Min"),
        spark_max("Value").alias("Max"),
        count("*").alias("total_readings"),
        stddev("Value").alias("Val_std"),
    ]

    anomaly_cond = None
    if min_expected_value is not None:
        anomaly_cond = (col("Value") < lit(min_expected_value))
    if max_expected_value is not None:
        max_cond = (col("Value") > lit(max_expected_value))
        if anomaly_cond is not None:
            anomaly_cond = anomaly_cond | max_cond
        else:
            anomaly_cond = max_cond

    if anomaly_cond is None:
        anomaly_cond = lit(False)

    reading_data = struct(
        col("TimeSpan").alias("timestamp"),
        col("Value").alias("value"),
    )
    agg_exprs.extend(
        [
            spark_sum(spark_when(anomaly_cond, 1).otherwise(0))
            .cast("integer")
            .alias("Anomalies"),
            to_json(
                sort_array(
                    collect_list(
                        spark_when(~anomaly_cond, reading_data)
                    )
                )
            ).alias("CorrectReadingsJson"),
            to_json(
                sort_array(
                    collect_list(
                        spark_when(anomaly_cond, reading_data)
                    )
                )
            ).alias("AnomalousReadingsJson"),
        ]
    )

    stats_df = (
        base
        .groupBy(
            "DeviceId",
            "HourStart",
            "SensorId",
            "LocationId",
            "SensorTyId",
            "sensor_type",
            "device_type",
            "device_name"
        ).agg(*agg_exprs)
        .withColumnRenamed("total_readings", "ReadingsCount")
    )

    stats_df = stats_df.withColumn(
        "HasAnomalies",
        col("Anomalies") > lit(0),
    )

    stats_current_df = (
        stats_df
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("HourStart", "TimeSpan")
        .withColumnRenamed("Val_std", "MeasurementStdDev")
    )

    if len(stats_current_df.head(1)) == 0:
        return None

    return stats_current_df
