from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator_anomalies import (stats_calculator_anomalies)

def maquinas_nieve(
        factlecturas_extended_df: DataFrame,
        min_hour: str,
        max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("_device_type") == "Máquina de nieve")
            )
        )
    )

    maquinas_nieve_df = (
        stats_calculator_anomalies(
            base,
            "Corriente",
            0.3,
            None,
            180,
            0.35
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
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
            base,
            min_hour,
            max_hour
        ).join(
            maquinas_nieve_df, on=["DeviceId", "LocalTimeSpan"], how="left" )
    )

    if scaffold_df.rdd.isEmpty():
        return None



    group_ids_df = (
        scaffold_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        scaffold_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df