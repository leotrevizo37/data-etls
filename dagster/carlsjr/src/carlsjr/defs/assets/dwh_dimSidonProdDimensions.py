from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext)
from time import perf_counter

from ..resources import (SparkResource,
                         SqlDimServerResource)
from ..transformations import raw_dimensions

@asset(
    non_argument_deps={
        AssetKey(["carlsjr", "dbo_sensors"]),
        AssetKey(["carlsjr", "dbo_sensor_tys"]),
        AssetKey(["carlsjr", "dbo_devices"]),
        AssetKey(["carlsjr", "dbo_device_tys"]),
        AssetKey(["carlsjr", "dbo_sublocations"]),
        AssetKey(["carlsjr", "dbo_sublocation_tys"])
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver"
    },
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_dimsidonproddimensions(
    context: AssetExecutionContext) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver

    context.log.info("Changes detected, materializing dimensions...")
    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    context.log.info("Inserting new entries to stg.dimSidonProdDimensions...")
    _inserted = spark_res.write_dim_prod_dimensions(raw_dims_df)

    return MaterializeResult(
        metadata={
            "row_count": _inserted,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        },
    )
