from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator
from .stats_calculator_anomalies import (stats_calculator_anomalies)


def cuartos_refrigeracion(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            col("device_type").isin([
                "Cuarto de conservación",
                "Ambiente conservacion",
                "Compresor conservación"
            ])
        )
    )

    if base.rdd.isEmpty():
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (
                        col("device_type").isin([
                            "Cuarto de conservación",
                            "Ambiente conservacion",
                            "Compresor conservación"]
                        )
                    ) & (
                        (col("sensor_type") == "Corriente") |
                        (col("sensor_type") == "Temperatura") |
                        (col("sensor_type") == "Humedad")
                    )
                ),
                min_hour,
                max_hour
            )
        )
        group_ids_df = (
            empty_base_df
            .select("DeviceId", "LocalTimeSpan")
            .distinct()
            .withColumn("OperationId", expr("uuid()"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        _ = group_ids_df.count()

        group_with_guid_df = (
            empty_base_df
            .join(group_ids_df, on=["SensorId", "DeviceId", "LocalTimeSpan", "SensorTyId"], how="left")
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(None))
            .withColumn("StatusCode", lit(3))
            .withColumn("MeasurementValue", lit(None))
        )

        return group_with_guid_df

    temperature_stats_df = (
        stats_calculator_anomalies(
            base,
            "Temperatura",
            34,
            40,
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
            "sensor_type",
            "ReadingsCount",
        )
    )

    humidity_stats_df = (
        stats_calculator_anomalies(
            base,
            "Humedad",
            10,
            90,
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
            "sensor_type",
            "ReadingsCount",
        )
    )

    current_stats_df = (
        stats_calculator(
            base,
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
            "sensor_type",
            "ReadingsCount",
        )
        .withColumn("Anomalies", lit(None))
    )

    cuartos_refrigeracion_df = (
        temperature_stats_df
        .unionByName(current_stats_df)
        .unionByName(humidity_stats_df)
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
                    col("device_type").isin([
                        "Cuarto de conservación",
                        "Ambiente conservacion",
                        "Compresor conservación"]
                    )
                ) & (
                    (col("sensor_type") == "Corriente") |
                    (col("sensor_type") == "Temperatura") |
                    (col("sensor_type") == "Humedad")
                )
            ),
            min_hour,
            max_hour
        )
        .join(
            cuartos_refrigeracion_df,
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