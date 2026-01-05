from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

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

    return maquinas_nieve_df