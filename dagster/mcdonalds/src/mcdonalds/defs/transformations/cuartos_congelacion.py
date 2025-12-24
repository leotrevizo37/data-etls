from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from .stats_calculator import stats_calculator

def cuartos_conservacion(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter((col("_device_name") == "Cuarto de congelación"))
    )

    temperature_stats_df = (
        stats_calculator(
            base,
            "Temperatura",
            -10,
            0)
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

    current_stats_df = (
        stats_calculator(
            base,
            "Corriente",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "Estatus",
            "Valor",
            "_sensor_type",
        )
        .withColumn("Anomalias", lit(None))
    )

    cuartos_conservacion_df = (
        temperature_stats_df.unionByName(current_stats_df)
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

    if cuartos_conservacion_df.rdd.isEmpty():
        return None

    return cuartos_conservacion_df