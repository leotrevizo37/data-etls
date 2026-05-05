from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


def termostatos(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("device_type") == "A/C de Cocina") |
                    (col("device_type") == "A/C de Comedor") |
                    (col("device_type") == "Ambiente playland")
            )
        )
    )

    if base.rdd.isEmpty():
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (
                        (col("device_type") == "A/C de Cocina") |
                        (col("device_type") == "A/C de Comedor") |
                        (col("device_type") == "Ambiente playland")
                    ) &
                    (
                        (col("sensor_type") == "On/Off Termostato") |
                        (col("sensor_type") == "Temperatura")
                    )
                ),
                min_hour,
                max_hour
            )
        )

        empty_base_df = (
            empty_base_df.join(dimensions_df, on=["DeviceId", "SensorId"], how="left")
        )

        group_with_guid_df = (
            empty_base_df
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(None))
            .withColumn("StatusCode", lit(3))
            .withColumn("MeasurementValue", lit(None))
        )

        return group_with_guid_df

    state_stats_df = (
        stats_calculator(
            base,
            "On/Off Termostato",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "Promedio",
            "sensor_type",
            "ReadingsCount",
        )
        .withColumnRenamed("Promedio", "MeasurementValue")
        .withColumn("Anomalies", lit(None))
    )

    temperature_stats_df = (
        stats_calculator(
            base,
            "Temperatura",
            66)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "sensor_type",
            "ReadingsCount",
        )
        .withColumn("Anomalies", lit(None))
    )

    termostatos_df = (
        state_stats_df.unionByName(temperature_stats_df)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
        )
    )

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                (
                    (col("device_type") == "A/C de Cocina") |
                    (col("device_type") == "A/C de Comedor") |
                    (col("device_type") == "Ambiente playland")
                ) &
                (
                    (col("sensor_type") == "On/Off Termostato") |
                    (col("sensor_type") == "Temperatura")
                )
            ),
            min_hour,
            max_hour
        )
        .join(
            termostatos_df,
            on=["SensorId", "DeviceId", "LocalTimeSpan", "SensorTyId"],
            how="left"
        )
    )

    scaffold_enriched = (
        scaffold_df
        .fillna({"ReadingsCount": 0, "StatusCode": 3})
    )

    if scaffold_enriched.rdd.isEmpty():
        return None

    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df