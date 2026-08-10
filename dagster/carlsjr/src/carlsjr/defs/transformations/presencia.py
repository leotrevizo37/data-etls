from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr, from_utc_timestamp

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


PATRON_DEVICE = r"(?iu)(mostrador|monitoreo)"
PATRON_SENSOR = r"(?iu)(presencia|encendido|estado)"

def presencia(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(col("device_type").rlike(PATRON_DEVICE))
    )

    scaffold_base_df = lecturas_time_hours(
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

    if scaffold_base_df is None:
        return None

    if len(base.head(1)) == 0:
        group_with_guid_df = (
            scaffold_base_df
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

    states_stats_df = (
        stats_calculator(
            base,
            "Camara de presencia",
            None,
            None
        )
    )

    if states_stats_df is None:
        return None

    luces_df = (
        states_stats_df.select(
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

    scaffold_df = (
        scaffold_base_df.join(
            luces_df,
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
