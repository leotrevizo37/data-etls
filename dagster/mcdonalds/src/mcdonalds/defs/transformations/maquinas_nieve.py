from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,
                                   date_trunc,
                                   avg,
                                   sum as spark_sum,
                                   count,
                                   lag,
                                   lit,
                                   when as spark_when,
                                   max as spark_max)
from pyspark.sql.window import Window

def maquinas_nieve(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (col("_device_type") == "Corriente") &
            (
                    (col("_device_name") == "Máquinas de nieve") |
                    (col("_device_name") == "Máquina de nieve")
            )
        )
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
    )

    w = Window.partitionBy("HourStart").orderBy("LocalTimeSpan")

    base = base.withColumn(
        "is_zero",
        (col("Value") < 1).cast("int")
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

    """maquinas_nieve_df = (
        base
        .groupBy("HourStart")
        .agg(
            avg("Value").alias("Valor"),
            spark_sum(
                spark_when(col("Value") > 3, 1).otherwise(0)
            ).alias("amperaje_mayor_3"),
            count("*").alias("total_readings"),
            spark_max("zero_run_len").alias("max_zero_run_len"),
        )
        .withColumn(
            "estatus",
            spark_when(col("amperaje_mayor_3") >= 18, 1).otherwise(3)
        )
        .withColumn(
            "Anomalias",
            spark_when(col("max_zero_run_len") >= 12, 1).otherwise(0)
        )
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )"""

    stats_df = (
        base
        .groupBy("HourStart")
        .agg(
            avg("Value").alias("Valor_dummy"),
            spark_sum(
                spark_when(col("Value") > 3, 1).otherwise(0)
            ).alias("amperaje_mayor_3"),
            count("*").alias("total_readings"),
            spark_max("zero_run_len").alias("max_zero_run_len"),
        )
        .withColumn(
            "Estatus",
            spark_when(col("amperaje_mayor_3") >= 18, 1).otherwise(3)
        )
    )

    per5max_df = (
        base
        .groupBy("DeviceId", "LocalTimeSpan", "HourStart")
        .agg(
            spark_max("Value").alias("Value_phase_max")
        )
    )

    valor_df = (
        per5max_df
        .groupBy("HourStart")
        .agg(
            avg("Value_phase_max").alias("Valor")
        )
    )

    anomalies_df = (
        base
        .where(col("zero_run_start") == 1)
        .groupBy("HourStart")
        .agg(
            spark_sum(
                spark_when(col("zero_run_len") >= 12, 1).otherwise(0)
            ).alias("Anomalias_raw")
        )
        .withColumn(
            "Anomalias",
            spark_when(col("Anomalias_raw") > 3, lit(3)).otherwise(col("Anomalias_raw"))
        )
        .select("HourStart", "Anomalias")
    )

    maquinas_nieve_df = (
        valor_df
        .join(stats_df.drop("Valor_dummy"), on="HourStart", how="inner")
        .join(anomalies_df, on="HourStart", how="left")
        .fillna({"Anomalias": 0})
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if maquinas_nieve_df.rdd.isEmpty():
        return None

    return maquinas_nieve_df