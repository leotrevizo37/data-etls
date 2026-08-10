from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
    expr,
    from_utc_timestamp)
from pyspark import StorageLevel

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


def maquinas_soda(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("device_type") == "Máquina de sodas")
            )
        )
    )

    if len(base.head(1)) == 0:
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (col("device_type") == "Máquina de sodas") &
                    (
                        (col("sensor_type") == "Corriente") |
                        (col("sensor_type") == "Presión")
                    )
                ),
                min_hour,
                max_hour
            )
        )
        group_with_guid_df = (
            empty_base_df
            .withColumn(
                "LocalTimeSpan",
                from_utc_timestamp(col("TimeSpan"), col("TimeZone"))
            )
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(TimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(TimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(1).cast("integer"))
            .withColumn("MeasurementValue", lit(None))
            .withColumn("MeasurementStdDev", lit(None))
        )

        return group_with_guid_df

    base = base.persist(StorageLevel.MEMORY_AND_DISK)

    current_stats_df = (
        stats_calculator(
            base,
            "Corriente",
            None,
            13
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "TimeSpan",
            "MeasurementValue",
            "MeasurementStdDev",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
        .withColumn("TimeSpan", date_trunc("hour", col("TimeSpan")))
    )

    pressure_stats_df = (
        stats_calculator(
            base,
            "Presión",
            5,
            40
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "TimeSpan",
            "MeasurementValue",
            "MeasurementStdDev",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
    )

    maquinas_soda_df = (
        current_stats_df.unionByName(pressure_stats_df)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "TimeSpan",
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
        lecturas_time_hours(
            dimensions_df.filter(
                (col("device_type") == "Máquina de sodas") &
                (
                    (col("sensor_type") == "Corriente") |
                    (col("sensor_type") == "Presión")
                )
            ),
            min_hour,
            max_hour
        )
        .join(
            maquinas_soda_df,
            on=[
                "SensorId",
                "DeviceId",
                "TimeSpan",
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
        .fillna({"ReadingsCount": 0, "Anomalies": -1})
    )


    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "LocalTimeSpan",
            from_utc_timestamp(col("TimeSpan"), col("TimeZone"))
        )
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(TimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(TimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df
