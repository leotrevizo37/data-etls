from pyspark.sql.functions import col

from dagster import (asset,
                     AssetKey,
                     MaterializeResult,
                     AssetExecutionContext)

from ..resources import SparkResource, SqlServerResource, SqlDimServerResource
from ..transformations import raw_dimensions, factlecturas_extended, maquinas_nieve

@asset(
    non_argument_deps={
        AssetKey("dbo_sensors"),
        AssetKey("dbo_sensor_tys"),
        AssetKey("dbo_devices"),
        AssetKey("dbo_device_tys"),
        AssetKey("dbo_sublocations"),
        AssetKey("dbo_sublocation_tys"),
    },
    required_resource_keys={
        "spark",
        "sqlserver",
        "dim_sqlserver"
    },
)
def factoperations(context: AssetExecutionContext) -> MaterializeResult:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver

    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    factlecturas_extended_df = factlecturas_extended(spark_res, sqlserver, raw_dims_df)

    if factlecturas_extended_df is None:
        context.log.info("No rows in extended factLecturas; skipping write.")
        return None

    maquinas_nieve_df = maquinas_nieve(factlecturas_extended_df)
    if maquinas_nieve_df is None:
        return None

    maquinas_nieve_df = (
        maquinas_nieve_df
        .select(
            col("SensorId"),
            col("LocationId"),
            col("LocalTimeSpan"),
            col("Estatus"),
            col("Valor"),
            col("Anomalias"),
        )
    )
    return MaterializeResult(
        metadata={"row_count": maquinas_nieve_df.count()}
    )
