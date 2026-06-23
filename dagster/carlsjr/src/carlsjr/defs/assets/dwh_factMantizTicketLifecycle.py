from datetime import datetime
from typing import Any, Dict, Optional

from dagster import In, Out, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import MantizMySqlResource, SparkResource, SqlServerResource
from ..transformations.mantiz_tickets import build_mantiz_ticket_lifecycle_df
from .mantiz_common import (
    LIFECYCLE_INGESTION_NAME,
    MANTIZ_MAX_RETRIES,
    MANTIZ_RETRY_DELAY_SECONDS,
    MANTIZ_RETRY_POLICY,
    changed_lifecycle_tickets_query,
    finalize_run,
    read_mysql_query,
    read_mysql_table,
    start_run,
    validate_mantiz_config,
)


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_RETRY_POLICY,
)
def start_mantiz_lifecycle_run_and_get_range_op(context) -> Dict[str, Any]:
    return start_run(context, LIFECYCLE_INGESTION_NAME)


@op(
    required_resource_keys={"sqlserver", "spark", "mantiz_mysql"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_RETRY_POLICY,
)
def transform_and_load_mantiz_lifecycle_op(
    context,
    window: Dict[str, Any],
) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    mysql: MantizMySqlResource = context.resources.mantiz_mysql
    validate_mantiz_config(mysql)

    from_utc: datetime = window["from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)

    succeeded = False
    error_message: Optional[str] = None
    max_source_changed_at: Optional[datetime] = None
    row_count = 0
    merged_rows = 0
    result_df: Optional[DataFrame] = None

    try:
        if retry_number > 0:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)

        tickets_df = read_mysql_query(
            spark_res,
            mysql,
            changed_lifecycle_tickets_query(from_utc, to_utc, mysql.organization_id),
            "changed_mantiz_tickets",
        )

        if len(tickets_df.head(1)) == 0:
            context.log.info("No Mantiz lifecycle tickets found in window.")
            succeeded = True
        else:
            result_df = build_mantiz_ticket_lifecycle_df(
                tickets_df=tickets_df,
                branches_df=read_mysql_table(spark_res, mysql, "Branch"),
                clients_df=read_mysql_table(spark_res, mysql, "Client"),
                equipment_instances_df=read_mysql_table(spark_res, mysql, "EquipmentInstance"),
                equipment_catalog_df=read_mysql_table(spark_res, mysql, "EquipmentCatalog"),
                failure_types_df=read_mysql_table(spark_res, mysql, "FailureType"),
                suppliers_df=read_mysql_table(spark_res, mysql, "Supplier"),
                organization_id=mysql.organization_id,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            row_count = result_df.count()
            if row_count > 0:
                max_source_changed_at = spark_res.get_max_time_span(
                    result_df,
                    "SourceLastChangedAt",
                )
                sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
                spark_res.write_fact_mantiz_ticket_lifecycle_stage(result_df, run_id)
                merged_rows = sql.merge_fact_mantiz_ticket_lifecycle(run_id)

            context.log.info(
                str(row_count)
                + " Mantiz lifecycle rows prepared; "
                + str(merged_rows)
                + " merged into dwh.factMantizTicketLifecycle."
            )
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading factMantizTicketLifecycle window: " + error_message)
        try:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
        except Exception:
            pass
        if retry_number < MANTIZ_MAX_RETRIES:
            raise RetryRequested(
                max_retries=MANTIZ_MAX_RETRIES,
                seconds_to_wait=MANTIZ_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if result_df is not None:
            result_df.unpersist()
        try:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
        except Exception:
            pass

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "max_source_changed_at": max_source_changed_at,
        "succeeded": succeeded,
        "error_message": error_message,
        "row_count": row_count,
        "merged_rows": merged_rows,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=MANTIZ_RETRY_POLICY,
)
def finalize_mantiz_lifecycle_run_op(context, result: Dict[str, Any]):
    return finalize_run(context, LIFECYCLE_INGESTION_NAME, result)


@graph_asset(
    name="dwh_factmantizticketlifecycle",
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_factmantizticketlifecycle():
    window = start_mantiz_lifecycle_run_and_get_range_op()
    result = transform_and_load_mantiz_lifecycle_op(window)
    return finalize_mantiz_lifecycle_run_op(result)
