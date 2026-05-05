from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
    expr)

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator_anomalies import (stats_calculator_anomalies)

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

    if base.rdd.isEmpty():
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
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(None))
            .withColumn("StatusCode", lit(3))
            .withColumn("MeasurementValue", lit(None))
        )

        return group_with_guid_df

    maquinas_nieve_df = (
        stats_calculator_anomalies(
            base,
            "Corriente",
            0.28,
            None,
            240,
            0.06
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
        .withColumn("LocalTimeSpan", date_trunc("hour", col("LocalTimeSpan")))
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