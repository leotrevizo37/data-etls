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
    context.log.info(
        "fact Lecturas join dims row count:  "
        + str(factlecturas_extended_df.count())
        + "\n top:   "
        + str(factlecturas_extended_df.first)
    )

    filter_expr = (
            (col("_device_type") == "Corriente")
            & (
                    (col("_device_name") == "Máquinas de nieve")
                    | (col("_device_name") == "Máquina de nieve")
            )
    )

    filtered_df = factlecturas_extended_df.filter(filter_expr)

    match_count = filtered_df.count()

    if match_count == 0:
        context.log.info(
            "factLecturas_extended: 0 rows matching devices 'Corriente' + 'Máquina(s) de nieve'."
        )
    else:
        first_match = filtered_df.first()
        context.log.info(
            "factLecturas_extended matches: "
            + str(match_count)
            + "\nfirst match: "
            + str(first_match)
        )

    maquinas_nieve_df = maquinas_nieve(factlecturas_extended_df, context)

    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
        return None
    context.log.info(
        "maquinas nieve etl rows count:  "
        + str(maquinas_nieve_df.count())
        + "\n top:   "
        + str(maquinas_nieve_df.first())
    )

    maquinas_nieve_df = (
        maquinas_nieve_df
        .select(
            col("DeviceId"),
            col("LocationId"),
            col("LocalTimeSpan"),
            col("Estatus"),
            col("Valor"),
            col("Anomalias"),
        )
    )

    context.log.info(
        "maquinas nieve df top"
        + str(maquinas_nieve_df.first)
        + str(maquinas_nieve_df.first())
        + str(maquinas_nieve_df.count())
    )

    inserted = spark_res.write_fact_operations_incremental(maquinas_nieve_df)
    context.log.info(inserted)

    return MaterializeResult(
        metadata={"row_count": inserted},
    )
