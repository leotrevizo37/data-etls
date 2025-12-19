from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..resources import SqlServerResource, SparkResource

def factlecturas_extended(
        spark_res: SparkResource,
        sqlserver: SqlServerResource,
        raw_dimensions: DataFrame,
        fetch_factlecturas_from: str) -> Optional[DataFrame]:
    spark = spark_res.get_spark()

    if fetch_factlecturas_from is None:
        dwh_factlecturas_df = (
            spark.read.format("jdbc")
            .option("dbtable", "dwh.FactLecturas")
            .options(**sqlserver.get_basic_options())
            .load()
        )
    else:
        dbtable = (
            "(SELECT * "
            " FROM dwh.FactLecturas "
            f" WHERE LocalTimeSpan >= '{fetch_factlecturas_from}') AS src"
        )
        dwh_factlecturas_df =(
            spark.read.format("jdbc")
            .option("dbtable", dbtable)
            .options(**sqlserver.get_basic_options())
            .load()
        )

    dwh_factlecturas_extended_df = (
        dwh_factlecturas_df
        .alias("fl")
        .join(
            raw_dimensions.alias("dim"), on="SensorId", how="left"
        )
        .select(
            col("fl.ReadId"),
            col("fl.TimeSpan"),
            col("fl.LocalTimeSpan"),
            col("fl.Value"),
            col("dim.SensorId"),
            col("dim.SensorTyId"),
            col("dim.DeviceId"),
            col("dim._sensor_name"),
            col("dim._sensor_type"),
            col("dim.DeviceTyId"),
            col("dim.SubLocationId"),
            col("dim._device_name"),
            col("dim._device_type"),
            col("dim.SubLocationTyId"),
            col("dim._sublocation_name"),
            col("dim.LocationId"),
            col("dim._sublocation_type")
        )
    )

    if dwh_factlecturas_extended_df.rdd.isEmpty():
        return None

    return dwh_factlecturas_extended_df
