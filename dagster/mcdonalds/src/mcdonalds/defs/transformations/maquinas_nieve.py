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
                                   floor)


from pyspark.sql.window import Window

def maquinas_nieve(
        factlecturas_extended_df: DataFrame,
        context: AssetExecutionContext) -> Optional[DataFrame]:
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


    if base is None:
        context.log.info("No rows when filtering for maquinas nieve; skipping write.")
        return None
    context.log.info(
        "entries for maquinas nieve:  "
        + str(base.count())
        + "\n top:   "
        + str(base.first)
    )

    w = Window.partitionBy("HourStart").orderBy("LocalTimeSpan")

    base = base.withColumn(
        "is_zero",
        (col("Value") <= 1).cast("int")
    )
    if base is None:
        context.log.info("No rows when grouping by hour; skipping write.")
        return None
    context.log.info(
        "number of rows after hour grouping:  "
        + str(base.count())
        + "\n top:   "
        + str(base.first)
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

    if base is None:
        context.log.info("No rows when putting 1s for counting anomalies; skipping write.")
        return None
    context.log.info(
        "entries for anomalies fisrt count:  "
        + str(base.count())
        + "\n top:   "
        + str(base.first)
    )

    base = base.withColumn(
        "zero_group",
        spark_sum("zero_run_start").over(w)
    )

    if base is None:
        context.log.info("No rows when counting fron start to finish anomalies; skipping write.")
        return None
    context.log.info(
        "entries for anomalies:  "
        + str(base.count())
        + "\n top:   "
        + str(base.first)
    )

    w_group = Window.partitionBy("HourStart", "zero_group")

    base = base.withColumn(
        "zero_run_len",
        spark_sum("is_zero").over(w_group)
    )

    five_minute_df = (
        base
        .groupBy("DeviceId", "SensorId", "LocationId", "HourStart", "5MinuteStart")
        .agg(
            # Por ejemplo: si al menos una lectura está en cero, el bloque cuenta como cero
            spark_max(col("is_zero").cast("int")).alias("is_zero_bucket")
            # si quieres, también podrías sacar avg(Valor) aquí
        )
    )

    if base is None:
        context.log.info("second timespan for count anomalies; skipping write.")
        return None
    context.log.info(
        "entries for the second amonalies window anomalies:  "
        + str(base.count())
        + "\n top:   "
        + str(base.first)
        + str(base.first())
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
            spark_when(col("amperaje_encendido") >= 18, 1).otherwise(3)
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
            spark_when(col("amperaje_encendido")/col("total_readings")>=0.50, 1).otherwise(3)
        )
    )

    if stats_df is None:
        context.log.info("no rows after amepraje count; skipping write.")
        return None
    context.log.info(
        "entries for the amperaje first etl:  "
        + str(stats_df.count())
        + "\n top:   "
        + str(stats_df.first)
    )

    per_min5_max_df = (
        base
        .groupBy("DeviceId", "5MinuteStart", "SensorId", "LocationId")
        .agg(
            spark_max("Value").alias("Value_phase_max")
        )
    )

    per5max_df = per_min5_max_df.withColumn(
        "HourStart",
        date_trunc("hour", col("5MinuteStart"))
    )




    if per5max_df is None:
        context.log.info("no rows after counting the max amperage pero phases; skipping write.")
        return None
    context.log.info(
        "entries for the biggeet ameprage per phases first etl:  "
        + str(per5max_df.count())
        + "\n top:   "
        + str(per5max_df.first)
    )

    valor_df = (
        per5max_df
        .groupBy("DeviceId", "HourStart", "LocationId")
        .agg(
            avg("Value_phase_max").alias("Valor"),
            count("*").alias("total_sensors"),
        )
    )

    if valor_df is None:
        context.log.info("no rows after establishing the average value from amperage; skipping write.")
        return None
    context.log.info(
        "entries for the avg amperage first etl:  "
        + str(valor_df.count())
        + "\n top:   "
        + str(valor_df.first)
    )

    anomalies_df = (
        base
        .where(col("zero_run_start") == 1)
        .groupBy("DeviceId", "HourStart", "SensorId", "LocationId")
        .agg(
            spark_sum(
                spark_when(col("zero_run_len") >= 12, 1).otherwise(0)
            ).alias("Anomalias_raw")
        )
        .withColumn(
            "Anomalias",
            spark_when(col("Anomalias_raw") > 3, lit(3)).otherwise(col("Anomalias_raw"))
        )
        .select("HourStart", "Anomalias", "DeviceId")
    )

    if anomalies_df is None:
        context.log.info("no rows after setting anomalies into df; skipping write.")
        return None
    context.log.info(
        "entries for the new df with anomalies:  "
        + str(anomalies_df.count())
        + "\n top:   "
        + str(anomalies_df.first)
    )

    maquinas_nieve_df = (
        valor_df
        .join(stats_df.drop("Valor_dummy"), on=["HourStart", "DeviceId"], how="inner")
        .join(anomalies_df, on=["HourStart", "DeviceId"], how="left")
        .fillna({"Anomalias": 0})
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if maquinas_nieve_df is None:
        context.log.info("putting the etl altogether; skipping write.")
        return None
    context.log.info(
        "entries for the the maquina nieve etl output:  "
        + str(maquinas_nieve_df.count())
        + "\n top:   "
        + str(maquinas_nieve_df.first)
    )

    if maquinas_nieve_df.rdd.isEmpty():
        return None

    return maquinas_nieve_df