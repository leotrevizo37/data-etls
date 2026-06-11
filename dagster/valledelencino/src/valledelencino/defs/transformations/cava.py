from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit, col
from pyspark import StorageLevel

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


def cava(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str,
) -> Optional[DataFrame]:
    base = factlecturas_extended_df.filter(col("device_type") == "Cava")
    scaffold_base_df = lecturas_time_hours(
        dimensions_df.filter(col("device_type") == "Cava"), min_hour, max_hour
    )

    if scaffold_base_df is None:
        return None

    if len(base.head(1)) == 0:
        group_with_guid_df = (
            scaffold_base_df
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(1).cast("integer"))
            .withColumn("MeasurementValue", lit(None))
            .withColumn("MeasurementStdDev", lit(None))
        )

        return group_with_guid_df

    base = base.persist(StorageLevel.MEMORY_AND_DISK)

    state_stats_df = stats_calculator(
        base,
        "Temperatura",
        -10,
        60
    )
    if state_stats_df is not None:
        state_stats_df = (
            state_stats_df
            .select(
                "DeviceId",
                "LocationId",
                "SensorTyId",
                "SensorId",
                "LocalTimeSpan",
                "MeasurementValue",
                "MeasurementStdDev",
                "Anomalies",
                "sensor_type",
                "ReadingsCount",
                "device_type",
            )
        )

    temperature_stats_df = stats_calculator(
        base,
        "Humedad",
        2,
        99
    )
    if temperature_stats_df is not None:
        temperature_stats_df = (
            temperature_stats_df
            .select(
                "DeviceId",
                "LocationId",
                "SensorTyId",
                "SensorId",
                "LocalTimeSpan",
                "MeasurementValue",
                "MeasurementStdDev",
                "Anomalies",
                "sensor_type",
                "ReadingsCount",
                "device_type",
            )
        )

    stats_dfs = [
        stats_df
        for stats_df in (state_stats_df, temperature_stats_df)
        if stats_df is not None
    ]

    if not stats_dfs:
        base.unpersist()
        return None

    termostatos_df = stats_dfs[0]
    for stats_df in stats_dfs[1:]:
        termostatos_df = termostatos_df.unionByName(stats_df)

    termostatos_df = (
        termostatos_df
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "MeasurementValue",
            "MeasurementStdDev",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
    )

    base.unpersist()

    scaffold_df = (
        scaffold_base_df
        .join(
            termostatos_df,
            on=[
                "SensorId",
                "DeviceId",
                "LocalTimeSpan",
                "SensorTyId",
                "device_type",
                "LocationId",
                "sensor_type"
            ],
            how="left",
        )
    )

    scaffold_enriched = scaffold_df.fillna({"ReadingsCount": 0, "Anomalies": -1})

    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df
