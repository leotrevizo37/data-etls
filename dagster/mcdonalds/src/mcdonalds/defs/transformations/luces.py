from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


def luces(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(col("_device_type") == "Luminaria")
    )

    if base.rdd.isEmpty():
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (
                        (col("_device_type") == "Luminaria") &
                        (col("_sensor_type") == "Encendido")
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

    states_stats_df = (
        stats_calculator(
            base,
            "Encendido",
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
            "ReadingsCount",
        )
        .withColumn("Anomalies", lit(None))
    )

    luces_df = (
        states_stats_df
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
            "ReadingsCount",
        )
    )

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                (col("_device_type") == "Luminaria") &
                (col("_sensor_type") == "Encendido")
            ),
            min_hour,
            max_hour
        )
        .join(
            luces_df,
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