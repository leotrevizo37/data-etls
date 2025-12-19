from typing import Optional

from dagster import AssetExecutionContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,
                                   date_trunc,
                                   avg,
                                   sum as spark_sum,
                                   count,
                                   lag,
                                   lit,
                                   when as spark_when,
                                   max as spark_max,
                                   unix_timestamp,
                                   from_unixtime,
                                   floor,
                                   min as spark_min,
                                   explode,
                                   sequence,
                                   expr,
                                   coalesce)

from pyspark.sql.window import Window

THRESHOLD_MIN_ROWS = 20
def maquinas_nieve(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (col("_sensor_type") == "Corriente") &
            (
                    (col("_device_name") == "Máquinas de nieve") |
                    (col("_device_name") == "Máquina de nieve")
            )
        )
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
            ).cast("timestamp")
        )
        .withColumn(
            "20MinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / 1200) * 1200
            ).cast("timestamp")
        )
    )

    w = Window.partitionBy("HourStart").orderBy("LocalTimeSpan")
    base = base.withColumn(
        "is_zero",
        (col("Value") <= 1).cast("int")
    )

    prev_is_zero = lag("is_zero", 1).over(w)
    base = base.withColumn(
        "zero_run_start",
        spark_when(
            (col("is_zero") == 1) &
            ((prev_is_zero != 1) | prev_is_zero.isNull()),
            1
        ).otherwise(0)
    )

    base = base.withColumn(
        "zero_group",
        spark_sum("zero_run_start").over(w)
    )
    w_group = Window.partitionBy("HourStart", "zero_group")
    base = base.withColumn(
        "zero_run_len",
        spark_sum("is_zero").over(w_group)
    )

    stats_df = (
        base
        .groupBy("DeviceId", "HourStart")
        .agg(
            avg("Value").alias("Valor_dummy"),
            spark_sum(
                spark_when(col("Value") >= 1, 1).otherwise(0)
            ).alias("amperaje_encendido"),
            count("*").alias("total_readings"),
            spark_max("zero_run_len").alias("max_zero_run_len"),
        )
        .withColumn(
            "Estatus",
            spark_when(col("amperaje_encendido") / col("total_readings") >= 0.50, 1).otherwise(3)
        )
    )

    per_min5_max_df = (
        base
        .groupBy("DeviceId", "5MinuteStart", "SensorId", "LocationId", "SensorTyId")
        .agg(
            spark_max("Value").alias("Value_phase_max")
        )
    )

    per5max_df = per_min5_max_df.withColumn(
        "HourStart",
        date_trunc("hour", col("5MinuteStart"))
    )

    valor_df = (
        per5max_df
        .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId")
        .agg(
            avg("Value_phase_max").alias("Valor"),
            count("*").alias("total_sensors"),
        )
    )

    agg_20 = (
        base
        .groupBy("DeviceId", "LocationId", "HourStart", "20MinuteStart", "SensorTyId")
        .agg(
            count("*").alias("row_count"),
            spark_sum(col("is_zero").cast("int")).alias("zero_count"),
        )
    )

    device_span = (
        base
        .groupBy("DeviceId", "LocationId", "SensorTyId")
        .agg(
            spark_min("20MinuteStart").alias("min_win"),
            spark_max("20MinuteStart").alias("max_win"),
        )
    )

    all_windows = (
        device_span
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            explode(
                sequence("min_win", "max_win", expr("INTERVAL 20 MINUTES"))
            ).alias("20MinuteStart"),
        )
    )

    windows_with_data = (
        all_windows
        .join(
            agg_20,
            on=["DeviceId", "LocationId", "20MinuteStart", "SensorTyId"],
            how="left",
        )
        .withColumn("row_count", coalesce(col("row_count"), lit(0)))
        .withColumn("zero_count", coalesce(col("zero_count"), lit(0)))
    )

    windows_with_data = windows_with_data.withColumn(
        "HourStart",
        date_trunc("hour", col("20MinuteStart"))
    )

    windows_with_anom = (
        windows_with_data
        .withColumn(
            "Anomalia_20min",
            spark_when(
                (col("row_count") == 0) |
                ((col("row_count") > 0) & (col("zero_count") == col("row_count"))) |
                ((col("row_count") < lit(THRESHOLD_MIN_ROWS)) & (col("zero_count") > 0)),
                lit(1),
            ).otherwise(lit(0)),
        )
    )

    anomalies_df = (
        windows_with_anom
        .groupBy("DeviceId", "LocationId", "HourStart", "SensorTyId")
        .agg(
            spark_sum("Anomalia_20min").alias("Anomalias_raw"),
        )
        .withColumn(
            "Anomalias",
            spark_when(col("Anomalias_raw") > 3, lit(3)).otherwise(col("Anomalias_raw"))
        )
        .select("HourStart", "Anomalias", "DeviceId")
    )

    maquinas_nieve_df = (
        valor_df
        .join(stats_df.drop("Valor_dummy"), on=["HourStart", "DeviceId"], how="inner")
        .join(anomalies_df, on=["HourStart", "DeviceId"], how="left")
        .fillna({"Anomalias": 0})
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if maquinas_nieve_df.rdd.isEmpty():
        return None

    return maquinas_nieve_df