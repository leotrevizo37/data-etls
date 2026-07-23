from datetime import datetime
from typing import Any, Dict

from dagster import RetryPolicy, daily_partitioned_config, job, op

from ..resources import SparkResource, SqlDimServerResource, SqlServerResource
from ..transformations.fact_out_of_hours_sales import transform_fact_out_of_hours_sales
from ..transformations.fact_store_sales_daily import transform_fact_store_sales_daily


USER_CODE = "bafar"
BAFAR_SALES_BACKFILL_START_DATE = datetime(2026, 5, 1)
BAFAR_SALES_BACKFILL_TIMEZONE = "America/Ciudad_Juarez"
BAFAR_SALES_BACKFILL_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@daily_partitioned_config(
    start_date=BAFAR_SALES_BACKFILL_START_DATE,
    timezone=BAFAR_SALES_BACKFILL_TIMEZONE,
)
def fact_out_of_hours_sales_backfill_daily_config(
    start: datetime,
    end: datetime,
):
    return {
        "ops": {
            "backfill_fact_out_of_hours_sales_daily_op": {
                "config": {
                    "from_value": start.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    "to_value": end.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                }
            }
        }
    }


@daily_partitioned_config(
    start_date=BAFAR_SALES_BACKFILL_START_DATE,
    timezone=BAFAR_SALES_BACKFILL_TIMEZONE,
)
def fact_store_sales_daily_backfill_daily_config(
    start: datetime,
    end: datetime,
):
    return {
        "ops": {
            "backfill_fact_store_sales_daily_op": {
                "config": {
                    "from_value": start.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    "to_value": end.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                }
            }
        }
    }


@op(
    required_resource_keys={"spark", "sqlserver", "dim_sqlserver"},
    config_schema={"from_value": str, "to_value": str},
    retry_policy=BAFAR_SALES_BACKFILL_RETRY_POLICY,
)
def backfill_fact_out_of_hours_sales_daily_op(context) -> Dict[str, Any]:
    spark_resource: SparkResource = context.resources.spark
    source_sqlserver: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    from_value = context.op_config["from_value"]
    to_value = context.op_config["to_value"]
    run_id = context.run_id

    try:
        sqlserver.clear_fact_out_of_hours_sales_stage(run_id)
        sales_df = spark_resource.read_fact_out_of_hours_sales_window(
            source_sqlserver,
            from_value,
            to_value,
        )
        schedules_df = spark_resource.read_store_schedules(sqlserver)
        result_df = transform_fact_out_of_hours_sales(sales_df, schedules_df)
        evaluated_rows = spark_resource.write_fact_out_of_hours_sales_stage(
            result_df,
            run_id,
        )
        affected_rows = sqlserver.merge_fact_out_of_hours_sales(run_id)
        sqlserver.clear_fact_out_of_hours_sales_stage(run_id)
    except Exception:
        try:
            sqlserver.clear_fact_out_of_hours_sales_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean factOutOfHoursSales staging rows before retry: %s",
                cleanup_exc,
            )
        raise

    context.log.info(
        "FactOutOfHoursSales backfill evaluated %s rows and affected %s rows.",
        evaluated_rows,
        affected_rows,
    )
    return {
        "from_value": from_value,
        "to_value": to_value,
        "evaluated_rows": evaluated_rows,
        "affected_rows": affected_rows,
    }


@op(
    required_resource_keys={"spark", "sqlserver", "dim_sqlserver"},
    config_schema={"from_value": str, "to_value": str},
    retry_policy=BAFAR_SALES_BACKFILL_RETRY_POLICY,
)
def backfill_fact_store_sales_daily_op(context) -> Dict[str, Any]:
    spark_resource: SparkResource = context.resources.spark
    source_sqlserver: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    from_value = context.op_config["from_value"]
    to_value = context.op_config["to_value"]
    run_id = context.run_id

    try:
        sqlserver.clear_fact_store_sales_daily_stage(run_id)
        sales_df = spark_resource.read_fact_store_sales_daily_window(
            source_sqlserver,
            from_value,
            to_value,
        )
        result_df = transform_fact_store_sales_daily(sales_df)
        evaluated_rows = spark_resource.write_fact_store_sales_daily_stage(
            result_df,
            run_id,
        )
        affected_rows = sqlserver.merge_fact_store_sales_daily(run_id)
        sqlserver.clear_fact_store_sales_daily_stage(run_id)
    except Exception:
        try:
            sqlserver.clear_fact_store_sales_daily_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean factStoreSalesDaily staging rows before retry: %s",
                cleanup_exc,
            )
        raise

    context.log.info(
        "FactStoreSalesDaily backfill evaluated %s rows and affected %s rows.",
        evaluated_rows,
        affected_rows,
    )
    return {
        "from_value": from_value,
        "to_value": to_value,
        "evaluated_rows": evaluated_rows,
        "affected_rows": affected_rows,
    }


@job(
    name="022_bafar_out_of_hours_sales_backfill_daily",
    config=fact_out_of_hours_sales_backfill_daily_config,
    tags={
        "job_type": "ventas",
        "user_code": USER_CODE,
        "backfill": "fact_out_of_hours_sales_daily",
    },
)
def fact_out_of_hours_sales_backfill_daily_job():
    backfill_fact_out_of_hours_sales_daily_op()


@job(
    name="023_bafar_store_sales_daily_backfill_daily",
    config=fact_store_sales_daily_backfill_daily_config,
    tags={
        "job_type": "ventas",
        "user_code": USER_CODE,
        "backfill": "fact_store_sales_daily",
    },
)
def fact_store_sales_daily_backfill_daily_job():
    backfill_fact_store_sales_daily_op()
