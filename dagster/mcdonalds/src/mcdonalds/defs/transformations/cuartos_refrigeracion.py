from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from .stats_calculator import stats_calculator

def cuartos_refrigeracion(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter((col("_device_name") == "Cuarto de conservación"))
    )

    temperature_stats_df = (
        stats_calculator(
            base,
            "Temperatura",
            34,
            40)
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

    cuartos_refrigeracion_df = (
        temperature_stats_df
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

    if cuartos_refrigeracion_df.rdd.isEmpty():
        return None

    return cuartos_refrigeracion_df