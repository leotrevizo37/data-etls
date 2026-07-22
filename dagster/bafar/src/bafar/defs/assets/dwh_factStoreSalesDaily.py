from datetime import datetime
from typing import Any, Dict
from zoneinfo import ZoneInfo

from dagster import In, MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op
from pyspark.sql import DataFrame
from pyspark.sql.functions import max as spark_max

from ..resources import SparkResource, SqlDimServerResource, SqlServerResource
from ..transformations.fact_store_sales_daily import transform_fact_store_sales_daily


FACT_STORE_SALES_DAILY_TIMEZONE = ZoneInfo("America/Ciudad_Juarez")
FACT_STORE_SALES_DAILY_BOOTSTRAP_DATE = datetime(2026, 5, 1)
FACT_STORE_SALES_DAILY_MAX_RETRIES = 3
FACT_STORE_SALES_DAILY_RETRY_DELAY_SECONDS = 60
FACT_STORE_SALES_DAILY_RETRY_POLICY = RetryPolicy(
    max_retries=FACT_STORE_SALES_DAILY_MAX_RETRIES,
    delay=FACT_STORE_SALES_DAILY_RETRY_DELAY_SECONDS,
)


def _read_sales_window(
    spark_resource: SparkResource,
    source_sqlserver: SqlDimServerResource,
    from_value: str,
    to_value: str,
) -> DataFrame:
    return spark_resource.read_fact_store_sales_daily_window(
        source_sqlserver,
        from_value,
        to_value,
    )


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=FACT_STORE_SALES_DAILY_RETRY_POLICY,
)
def get_fact_store_sales_daily_window_op(context) -> Dict[str, Any]:
    sqlserver: SqlServerResource = context.resources.sqlserver
    last_loaded_at = (
        sqlserver.get_watermark("FactStoreSalesDaily")
        or FACT_STORE_SALES_DAILY_BOOTSTRAP_DATE
    )
    from_date = last_loaded_at.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date = datetime.now(FACT_STORE_SALES_DAILY_TIMEZONE).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )

    return {
        "last_loaded_at": last_loaded_at,
        "from_value": from_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "to_value": to_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
    }


@op(
    required_resource_keys={"spark", "dim_sqlserver"},
    ins={"window": In(Dict[str, Any])},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=FACT_STORE_SALES_DAILY_RETRY_POLICY,
)
def load_fact_store_sales_daily_window_op(
    context,
    window: Dict[str, Any],
) -> DataFrame:
    spark_resource: SparkResource = context.resources.spark
    source_sqlserver: SqlDimServerResource = context.resources.dim_sqlserver
    return _read_sales_window(
        spark_resource,
        source_sqlserver,
        window["from_value"],
        window["to_value"],
    )


@op(
    required_resource_keys={"spark", "sqlserver"},
    ins={
        "window": In(Dict[str, Any]),
        "sales_df": In(DataFrame),
    },
    out=Out(Dict[str, Any]),
    retry_policy=FACT_STORE_SALES_DAILY_RETRY_POLICY,
)
def process_fact_store_sales_daily_op(
    context,
    window: Dict[str, Any],
    sales_df: DataFrame,
) -> Dict[str, Any]:
    spark_resource: SparkResource = context.resources.spark
    sqlserver: SqlServerResource = context.resources.sqlserver
    last_loaded_at: datetime = window["last_loaded_at"]
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)

    try:
        sqlserver.mark_run_started("FactStoreSalesDaily")
        max_sale_date = sales_df.agg(
            spark_max("Fecha").alias("MaxFecha")
        ).first().MaxFecha
        new_last_loaded_at = (
            max_sale_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if max_sale_date is not None
            else last_loaded_at
        )
        result_df = transform_fact_store_sales_daily(sales_df)

        sqlserver.clear_fact_store_sales_daily_stage(run_id)
        evaluated_rows = spark_resource.write_fact_store_sales_daily_stage(
            result_df,
            run_id,
        )
        affected_rows = sqlserver.merge_fact_store_sales_daily(run_id)
        sqlserver.clear_fact_store_sales_daily_stage(run_id)
    except Exception as exc:
        try:
            sqlserver.clear_fact_store_sales_daily_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean factStoreSalesDaily staging rows before retry: %s",
                cleanup_exc,
            )
        if retry_number < FACT_STORE_SALES_DAILY_MAX_RETRIES:
            raise RetryRequested(
                max_retries=FACT_STORE_SALES_DAILY_MAX_RETRIES,
                seconds_to_wait=FACT_STORE_SALES_DAILY_RETRY_DELAY_SECONDS,
            ) from exc
        sqlserver.mark_run_completed(
            ingestion_name="FactStoreSalesDaily",
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(exc)[:4000],
        )
        raise

    return {
        "new_last_loaded_at": new_last_loaded_at,
        "evaluated_rows": evaluated_rows,
        "affected_rows": affected_rows,
        "from_value": window["from_value"],
        "to_value": window["to_value"],
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=FACT_STORE_SALES_DAILY_RETRY_POLICY,
)
def finalize_fact_store_sales_daily_run_op(
    context,
    result: Dict[str, Any],
):
    sqlserver: SqlServerResource = context.resources.sqlserver
    new_last_loaded_at: datetime = result["new_last_loaded_at"]

    sqlserver.mark_run_completed(
        ingestion_name="FactStoreSalesDaily",
        new_last_loaded_at=new_last_loaded_at,
        succeeded=True,
        error_message=None,
    )

    return MaterializeResult(
        metadata={
            "evaluated_rows": result["evaluated_rows"],
            "affected_rows": result["affected_rows"],
            "from_sales_date": result["from_value"],
            "to_sales_date": result["to_value"],
        }
    )


@graph_asset(
    name="dwh_factstoresalesdaily",
    key_prefix=["bafar"],
    group_name="bafar",
)
def dwh_factstoresalesdaily():
    window = get_fact_store_sales_daily_window_op()
    sales_df = load_fact_store_sales_daily_window_op(window)
    result = process_fact_store_sales_daily_op(window, sales_df)
    return finalize_fact_store_sales_daily_run_op(result)
