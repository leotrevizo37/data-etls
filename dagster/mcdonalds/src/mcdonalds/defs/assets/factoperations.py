from datetime import timedelta

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

    max_op_ts = sqlserver.fetch_scalar(
        "SELECT MAX(LocalTimeSpan) FROM dwh.factOperaciones"
    )
    fetch_from_ts = max_op_ts - timedelta(hours=4)
    delete_from_ts = max_op_ts - timedelta(hours=2)

    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    factlecturas_extended_df = factlecturas_extended(spark_res, sqlserver, raw_dims_df, fetch_from_ts)

    if factlecturas_extended_df is None:
        context.log.info("No new rows detected in factLecturas; skipping write.")
        return None

    context.log.info("Deleting las period rows from FactOperaciones.")
    deleted_rows = sqlserver.execute_query(
        "DELETE FROM dwh.factOperaciones WHERE LocalTimeSpan >= ?",
        delete_from_ts,
    )
    if deleted_rows is None or deleted_rows == 0:
        context.log.info("No new rows deleted from factOperaciones "
                         "for this period; skipping write.")
    else:
        context.log.info("Deleted" + str(deleted_rows) + "from FactOperaciones.")

    context.log.info("Materializing maquinas de nieve ETL")
    maquinas_nieve_df = maquinas_nieve(factlecturas_extended_df, context)

    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
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

    context.log.info("Writing " + str(maquinas_nieve_df.count()) + " rows into factOperaciones.")
    inserted = spark_res.write_fact_operations_incremental(maquinas_nieve_df)
    context.log.info(str(inserted) + " rows correctly written into factOperaciones.")

    return MaterializeResult(
        metadata={"row_count": inserted},
    )
