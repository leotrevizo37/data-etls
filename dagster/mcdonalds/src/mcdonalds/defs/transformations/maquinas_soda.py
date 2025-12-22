from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from .stats_calculator_time_window import stats_calculator_time_window
from .stats_calculator import stats_calculator

def maquinas_soda(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("_device_name") == "Máquina de Sodas") |
                    (col("_device_name") == "Máquina de sodas") |
                    (col("_device_name") == "Máquinas de Sodas") |
                    (col("_device_name") == "Máquinas de sodas") |
                    (col("_device_name") == "Máquinas de Soda") |
                    (col("_device_name") == "Máquinas de soda") |
                    (col("_device_name") == "Máquina de Soda") |
                    (col("_device_name") == "Máquina de soda")
            )
        )
    )


    current_stats_df = (
        stats_calculator_time_window(
            base,
            "Corriente",
            1,
            None,
            20)
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

    pressure_stats_df = (
        stats_calculator(
            base,
            "Presión",
            20)
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

    maquinas_soda_df = (
        current_stats_df.unionByName(pressure_stats_df)
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

    if maquinas_soda_df.rdd.isEmpty():
        return None

    return maquinas_soda_df