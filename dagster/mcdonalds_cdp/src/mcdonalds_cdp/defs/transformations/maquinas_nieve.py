from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
    expr,
    from_utc_timestamp)

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import (stats_calculator)

def maquinas_nieve(
        factlecturas_extended_df: DataFrame,
        dimensions_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("device_type") == "Máquina de nieve")
            )
        )
    )

    if len(base.head(1)) == 0:
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (col("device_type") == "Máquina de nieve") &
                    (col("sensor_type") == "Corriente")
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
            .withColumn("Sales", lit(0).cast("integer"))
        )

        return group_with_guid_df

    maquinas_nieve_df = (
        stats_calculator(
            base,
            "Corriente",
            None,
            13,
            7,
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
            "Sales",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
        .withColumn("TimeSpan", date_trunc("hour", col("TimeSpan")))
    )

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                (col("device_type") == "Máquina de nieve") &
                (col("sensor_type") == "Corriente")
            ),
            min_hour,
            max_hour
        )
        .join(
            maquinas_nieve_df,
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
        .fillna({"ReadingsCount": 0, "Anomalies":-1, "Sales": 0})
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
