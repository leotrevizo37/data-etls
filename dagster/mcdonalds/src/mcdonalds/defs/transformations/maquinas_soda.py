from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
    expr)

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator
from .stats_calculator_anomalies import (stats_calculator_anomalies)


def maquinas_soda(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("_device_type") == "Máquina de sodas")
            )
        )
    )

    if base.rdd.isEmpty():
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (col("_device_type") == "Máquina de sodas") &
                    (
                        (col("_sensor_type") == "Corriente") |
                        (col("_sensor_type") == "Presión")
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

    current_stats_df = (
        stats_calculator_anomalies(
            base,
            "Corriente",
            0.3,
            None,
            240,
            0.09
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
            "ReadingsCount",
        )
        .withColumn("LocalTimeSpan", date_trunc("hour", col("LocalTimeSpan")))
    )

    pressure_stats_df = (
        stats_calculator(
            base,
            "Presión",
            20)
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

    maquinas_soda_df = (
        current_stats_df.unionByName(pressure_stats_df)
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
                (col("_device_type") == "Máquina de sodas")&
                (
                    (col("_sensor_type") == "Corriente") |
                    (col("_sensor_type") == "Presión")
                )
            ),
            min_hour,
            max_hour
        )
        .join(
            maquinas_soda_df,
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