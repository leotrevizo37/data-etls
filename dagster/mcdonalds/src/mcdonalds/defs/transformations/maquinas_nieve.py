from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

from .stats_calculator_time_window import (stats_calculator_time_window)

def maquinas_nieve(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("_device_name") == "Máquinas de nieve") |
                    (col("_device_name") == "Máquina de nieve")
            )
        )
    )

    maquinas_nieve_df = (
        stats_calculator_time_window(
            base,
            "Corriente",
            1,
            None,
            20
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "Estatus",
            "Valor",
            "Anomalias",
            "_sensor_type",
        )
    )

    if maquinas_nieve_df.rdd.isEmpty():
        return None

    group_ids_df = (
        maquinas_nieve_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        maquinas_nieve_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df