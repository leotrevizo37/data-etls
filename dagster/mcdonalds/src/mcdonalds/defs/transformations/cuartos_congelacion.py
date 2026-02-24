from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator
from .stats_calculator_anomalies import (stats_calculator_anomalies)

def cuartos_congelacion(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            col("_device_type").isin([
                "Cuarto de congelación",
                "Ambiente congelacion",
                "Compresor congelación",
                "Resitencia de deshielo",
                "Resistencia de deshielo"
            ])
        )
    )

    temperature_stats_df = (
        stats_calculator_anomalies(
            base,
            "Temperatura",
            -10,
            0,
            240,
            0.25
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "Anomalies",
            "_sensor_type",
        )
    )

    current_stats_df = (
        stats_calculator(
            base
            .filter(
                (col("_sensor_name") != "Resistencia de deshielo") &
                (col("_device_type") != "Resitencia de deshielo") &
                (col("_device_type") != "Resistencia de deshielo")
            ),
            "Corriente",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "_sensor_type",
        )
        .withColumn("Anomalies", lit(None))
    )

    cuartos_conservacion_df = (
        temperature_stats_df.unionByName(current_stats_df)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "Anomalies",
            "_sensor_type",
        )
    )

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                col("_device_type").isin([
                    "Cuarto de congelación",
                    "Ambiente congelacion",
                    "Compresor congelación",
                    "Resitencia de deshielo",
                    "Resistencia de deshielo"
                ])
            ),
            min_hour,
            max_hour
        )
        .join(
            cuartos_conservacion_df,
            on=["SensorId", "DeviceId", "LocalTimeSpan"],
            how="left"
        )
    )

    scaffold_enriched = (
        scaffold_df
        .fillna({"ReadingsCount": 0, "StatusCode": 3})
    )

    if scaffold_enriched.rdd.isEmpty():
        return None

    group_ids_df = (
        scaffold_enriched
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    _ = group_ids_df.count()

    group_with_guid_df = (
        scaffold_enriched
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df