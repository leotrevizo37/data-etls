from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,
                                   date_trunc,
                                   avg,
                                   sum as spark_sum,
                                   count,
                                   when as spark_when,
                                   unix_timestamp,
                                   max as spark_max,
                                   from_unixtime,
                                   floor,
                                   lit)
from pyspark.sql.window import Window

def freezer_heater(
        base: DataFrame,
) -> Optional[DataFrame]:
    ZERO_GUID = "00000000-0000-0000-0000-000000000000"
    w5 = Window.partitionBy("DeviceId", "5MinuteStart")
    base = (
        base
        .filter(
            (col("_sensor_type") == "Corriente")
        )
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
            ).cast("timestamp")
        )
        .withColumn(
            "_defrost_on_5m",
            spark_max(
                spark_when(
                    (
                            (col("_sensor_name") == "Resistencia de deshielo") |
                            (col("_device_type") == "Resitencia de deshielo") |
                            (col("_device_type") == "Resistencia de deshielo")
                    ) & (col("Value") > lit(1)),
                    lit(1)
                ).otherwise(lit(0))
            ).over(w5)
        )
        .withColumn(
            "_phase_on_5m",
            spark_max(
                spark_when(
                    (
                            (col("_sensor_name") != "Resistencia de deshielo") &
                            (col("_device_type") != "Resitencia de deshielo") &
                            (col("_device_type") != "Resistencia de deshielo")
                    ) & (col("Value") > lit(1)),
                    lit(1)
                ).otherwise(lit(0))
            ).over(w5)
        )
        .withColumn(
            "is_anomaly",
            spark_when(
                (col("_defrost_on_5m") == lit(1)) & (col("_phase_on_5m") == lit(1)),
                lit(1)
            ).otherwise(lit(0))
        )
        .drop("_defrost_on_5m", "_phase_on_5m")
    )

    stats_df = (
        base
        .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
        .agg(
            avg("Value").alias("Val_dummy"),
            spark_sum(
                spark_when(col("is_anomaly") == lit(0), 1).otherwise(0)
            ).alias("is_okay"),
            count("*").alias("total_readings"),
            spark_sum("is_anomaly").alias("Anomalies_raw"),
        )
        .withColumn(
            "StatusCode",
            spark_when(col("is_okay") / col("total_readings") >= 0.50, 1)
            .otherwise(3)
        )
        .withColumn("Promedio", col("is_okay") / col("total_readings"))
        .withColumnRenamed("Anomalies_raw", "Anomalies")
    )

    stats_current_df = (
        stats_df
        .fillna({"Anomalies": 0})
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    stats_current_df = (stats_current_df
                        .withColumn("SensorTyId", lit(ZERO_GUID)))

    if stats_current_df.rdd.isEmpty():
        return None

    return stats_current_df