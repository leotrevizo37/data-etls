from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr
from pyspark import StorageLevel

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


PATRON_DEVICE = r"(?iu)(refr|cong|conserv|fr[ií][ao])"
PATRON_SENSOR = r"(?iu)(temp|hum|cor)"

def cuartos_refrigeracion(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            col("device_type").rlike(PATRON_DEVICE)
        )
    )

    if len(base.head(1)) == 0:
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (
                        col("device_type").rlike(PATRON_DEVICE)
                    ) & (
                        col("sensor_type").rlike(PATRON_SENSOR)
                    )
                ),
                min_hour,
                max_hour
            )
        )
        group_with_guid_df = (
            empty_base_df
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("InRangeReads", lit(0))
            .withColumn("Anomalies", lit(1).cast("integer"))
            .withColumn("MeasurementValue", lit(None))
            .withColumn("MeasurementStdDev", lit(None))
        )

        return group_with_guid_df

    base = base.persist(StorageLevel.MEMORY_AND_DISK)

    temperature_stats_df = stats_calculator(
        base,
        "Temperatura F",
        -10,
        90
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
                "InRangeReads",
                "device_type",
            )
        )

    humidity_stats_df = stats_calculator(
        base,
        "Humedad",
        5,
        99
    )
    if humidity_stats_df is not None:
        humidity_stats_df = (
            humidity_stats_df
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
                "InRangeReads",
                "device_type",
            )
        )

    current_stats_df = stats_calculator(
        base,
        "Corriente",
        None,
        None
    )
    if current_stats_df is not None:
        current_stats_df = (
            current_stats_df
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
                "InRangeReads",
                "device_type",
            )
        )

    stats_dfs = [
        stats_df
        for stats_df in (temperature_stats_df, current_stats_df, humidity_stats_df)
        if stats_df is not None
    ]

    if not stats_dfs:
        base.unpersist()
        return None

    cuartos_refrigeracion_df = stats_dfs[0]
    for stats_df in stats_dfs[1:]:
        cuartos_refrigeracion_df = cuartos_refrigeracion_df.unionByName(stats_df)

    cuartos_refrigeracion_df = (
        cuartos_refrigeracion_df.select(
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
            "InRangeReads",
            "device_type",
        )
    )

    base.unpersist()

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                (
                    col("device_type").rlike(PATRON_DEVICE)
                ) & (
                    col("sensor_type").rlike(PATRON_SENSOR)
                )
            ),
            min_hour,
            max_hour
        )
        .join(
            cuartos_refrigeracion_df,
            on=[
                "SensorId",
                "DeviceId",
                "LocalTimeSpan",
                "SensorTyId",
                "device_type",
                "LocationId",
                "sensor_type"
            ],
            how="left"
        )
    )

    scaffold_enriched = (
        scaffold_df
        .fillna({"ReadingsCount": 0, "InRangeReads": 0, "Anomalies": -1})
    )


    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df
