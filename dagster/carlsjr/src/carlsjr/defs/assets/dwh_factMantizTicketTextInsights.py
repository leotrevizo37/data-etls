from datetime import datetime
import os
from time import perf_counter
from typing import Any, Dict, Optional

from dagster import In, MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import MantizMySqlResource, SparkResource, SqlServerResource
from ..transformations.mantiz_tickets import (
    build_mantiz_source_texts_from_context_df,
    build_mantiz_text_insight_review_queue_df,
    build_mantiz_ticket_context_df,
    build_mantiz_ticket_text_insight_detail_from_sources_df,
    build_mantiz_ticket_text_insights_fact_from_prepared_df,
    empty_mantiz_classifier_predictions_df,
)
from ..transformations.mantiz_text_classifier import (
    load_mantiz_model_bytes,
    predict_mantiz_text,
)


MANTIZ_TEXT_INSIGHTS_MAX_RETRIES = 3
MANTIZ_TEXT_INSIGHTS_RETRY_DELAY_SECONDS = 60
MANTIZ_TEXT_INSIGHTS_RETRY_POLICY = RetryPolicy(
    max_retries=MANTIZ_TEXT_INSIGHTS_MAX_RETRIES,
    delay=MANTIZ_TEXT_INSIGHTS_RETRY_DELAY_SECONDS,
)
TEXT_INSIGHTS_INGESTION_NAME = "factMantizTicketTextInsights"


def _mysql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _mysql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _read_mantiz_text_insights_mysql_query(
    spark_res: SparkResource,
    mysql: MantizMySqlResource,
    query: str,
    alias: str,
) -> DataFrame:
    spark = spark_res.get_spark()
    return (
        spark.read
        .format("jdbc")
        .option("dbtable", "(" + query + ") AS " + alias)
        .options(**mysql.get_basic_options())
        .load()
    )


def _changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    from_str = _mysql_datetime(from_utc)
    to_str = _mysql_datetime(to_utc)
    org = _mysql_literal(organization_id)
    return (
        "SELECT DISTINCT t.* "
        "FROM Ticket t "
        "LEFT JOIN Branch b ON b.id = t.branchId "
        "LEFT JOIN Client c ON c.id = b.clientId "
        "WHERE c.organizationId = " + org + " "
        "AND ("
        "(COALESCE(t.updatedAt, t.createdAt) >= '" + from_str + "' "
        "AND COALESCE(t.updatedAt, t.createdAt) < '" + to_str + "') "
        "OR EXISTS ("
        "SELECT 1 FROM TicketComment tc "
        "WHERE tc.ticketId = t.id "
        "AND COALESCE(tc.updatedAt, tc.createdAt) >= '" + from_str + "' "
        "AND COALESCE(tc.updatedAt, tc.createdAt) < '" + to_str + "'"
        ")"
        ")"
    )


def _branches_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT b.id, b.clientId, b.name "
        "FROM Branch b "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.branchId = b.id"
    )


def _clients_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    org = _mysql_literal(organization_id)
    return (
        "SELECT DISTINCT c.id, c.organizationId "
        "FROM Client c "
        "INNER JOIN Branch b ON b.clientId = c.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.branchId = b.id "
        "WHERE c.organizationId = " + org
    )


def _equipment_instances_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ei.id, ei.catalogId, ei.location, ei.customAssetId, ei.serialNumber "
        "FROM EquipmentInstance ei "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.equipmentId = ei.id"
    )


def _equipment_catalog_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ec.id, ec.name "
        "FROM EquipmentCatalog ec "
        "INNER JOIN EquipmentInstance ei ON ei.catalogId = ec.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.equipmentId = ei.id"
    )


def _failure_types_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ft.id, ft.name "
        "FROM FailureType ft "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.failureId = ft.id"
    )


def _suppliers_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT s.id, s.name "
        "FROM Supplier s "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.supplierId = s.id"
    )


def _ticket_spare_parts_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT tsp.* "
        "FROM TicketSparePart tsp "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.id = tsp.ticketId"
    )


def _spare_parts_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    org = _mysql_literal(organization_id)
    return (
        "SELECT DISTINCT sp.* "
        "FROM SparePart sp "
        "INNER JOIN TicketSparePart tsp ON tsp.sparePartId = sp.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.id = tsp.ticketId "
        "WHERE sp.organizationId = " + org
    )


def _comments_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = _changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT tc.* "
        "FROM TicketComment tc "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON tc.ticketId = changed_tickets.id"
    )


def _validate_mantiz_text_insights_config(mysql: MantizMySqlResource) -> None:
    if not mysql.organization_id:
        raise ValueError("MANTIZ_ORGANIZATION_ID is required")


def _get_mantiz_text_insights_start_date() -> datetime:
    start_date = os.getenv("MANTIZ_START_DATE", "2026-01-01")
    return datetime.strptime(start_date, "%Y-%m-%d")


def _start_mantiz_text_insights_run(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(TEXT_INSIGHTS_INGESTION_NAME)
    if last_loaded_at is None:
        last_loaded_at = _get_mantiz_text_insights_start_date()

    from_utc = last_loaded_at.replace(microsecond=0)
    to_utc = datetime.utcnow().replace(microsecond=0)
    sql.mark_run_started(TEXT_INSIGHTS_INGESTION_NAME)

    context.log.info(
        TEXT_INSIGHTS_INGESTION_NAME
        + " run window from "
        + str(from_utc)
        + " to "
        + str(to_utc)
    )

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": last_loaded_at,
        "start_time": start_time,
    }


def _finalize_mantiz_text_insights_run(context, result: Dict[str, Any]):
    sql: SqlServerResource = context.resources.sqlserver

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_source_changed_at: Optional[datetime] = result["max_source_changed_at"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
    elif max_source_changed_at is None:
        new_watermark = to_utc
    else:
        new_watermark = max(max_source_changed_at, original_last_loaded_at)

    sql.mark_run_completed(
        ingestion_name=TEXT_INSIGHTS_INGESTION_NAME,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )

    start_time = result.get("start_time")
    elapsed = round(perf_counter() - start_time, 3) if start_time else 0.0

    return MaterializeResult(
        metadata={
            "row_count": result.get("row_count", 0),
            "merged_rows": result.get("merged_rows", 0),
            "elapsed_seconds": elapsed,
        }
    )


def _load_active_mantiz_text_model_payloads(
    context,
    sql: SqlServerResource,
    spark_res: SparkResource,
) -> Dict[str, tuple[bytes, str]]:
    active_models = sql.get_active_mantiz_text_classifier_models()
    loaded = {}
    for insight_type, model_info in active_models.items():
        try:
            content = spark_res.read_mantiz_text_model_artifact(model_info["artifact_path"])
            load_mantiz_model_bytes(content)
            loaded[insight_type] = (
                content,
                str(model_info["model_version"]),
            )
        except Exception as exc:
            context.log.warning(
                "Could not load active Mantiz text classifier for "
                + str(insight_type)
                + ": "
                + str(exc)
            )
    return loaded


def _build_mantiz_classifier_predictions_df(
    context,
    spark_res: SparkResource,
    sql: SqlServerResource,
    sources_df: DataFrame,
) -> DataFrame:
    spark = spark_res.get_spark()
    model_payloads = _load_active_mantiz_text_model_payloads(context, sql, spark_res)
    if not model_payloads:
        return empty_mantiz_classifier_predictions_df(spark)

    threshold = float(os.getenv("MANTIZ_TEXT_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.75"))
    payloads_bc = spark.sparkContext.broadcast(model_payloads)
    prediction_rows = (
        sources_df
        .select(
            "OrganizationId",
            "TicketId",
            "SourceType",
            "SourceId",
            "NormalizedText",
        )
        .rdd
        .mapPartitions(
            lambda rows: _predict_mantiz_text_partition(
                rows,
                payloads_bc.value,
                threshold,
            )
        )
    )

    return spark.createDataFrame(
        prediction_rows,
        "OrganizationId string, TicketId string, SourceType string, SourceId string, "
        "InsightType string, CanonicalCode string, CanonicalName string, "
        "ClassifierModelVersion string, ClassifierConfidence double",
    )


def _predict_mantiz_text_partition(rows, model_payloads, threshold):
    models = None
    for row in rows:
        if models is None:
            models = [
                (insight_type, load_mantiz_model_bytes(content), model_version)
                for insight_type, (content, model_version) in model_payloads.items()
            ]
        for insight_type, model, model_version in models:
            prediction = predict_mantiz_text(
                model,
                row.NormalizedText,
                model_version,
                threshold,
            )
            if prediction is None:
                continue
            yield (
                row.OrganizationId,
                row.TicketId,
                row.SourceType,
                row.SourceId,
                insight_type,
                prediction.label,
                prediction.label.replace("_", " "),
                prediction.model_version,
                prediction.confidence,
            )


def _materialize_mantiz_text_insights_df(df: DataFrame) -> DataFrame:
    return df.localCheckpoint(
        eager=True,
        storageLevel=StorageLevel.MEMORY_AND_DISK,
    )


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_TEXT_INSIGHTS_RETRY_POLICY,
)
def start_mantiz_text_insights_run_and_get_range_op(context) -> Dict[str, Any]:
    return _start_mantiz_text_insights_run(context)


@op(
    required_resource_keys={"sqlserver", "spark", "mantiz_mysql"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_TEXT_INSIGHTS_RETRY_POLICY,
)
def transform_and_load_mantiz_text_insights_op(
    context,
    window: Dict[str, Any],
) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    mysql: MantizMySqlResource = context.resources.mantiz_mysql
    _validate_mantiz_text_insights_config(mysql)

    from_utc: datetime = window["from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)

    succeeded = False
    error_message: Optional[str] = None
    max_source_changed_at: Optional[datetime] = None
    row_count = 0
    changed_ticket_count = 0
    merged_rows = 0
    review_queue_rows = 0
    review_queue_merged_rows = 0
    detail_row_count = 0
    result_df: Optional[DataFrame] = None
    detail_df: Optional[DataFrame] = None
    changed_keys_df: Optional[DataFrame] = None
    context_df: Optional[DataFrame] = None
    sources_df: Optional[DataFrame] = None
    review_queue_df: Optional[DataFrame] = None

    try:
        if retry_number > 0:
            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)

        tickets_df = _read_mantiz_text_insights_mysql_query(
            spark_res,
            mysql,
            _changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
            "changed_mantiz_text_tickets",
        )

        if len(tickets_df.head(1)) == 0:
            context.log.info("No Mantiz text insight tickets found in window.")
            succeeded = True
        else:
            branches_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _branches_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_branches",
            )
            clients_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _clients_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_clients",
            )
            equipment_instances_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _equipment_instances_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_equipment_instances",
            )
            equipment_catalog_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _equipment_catalog_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_equipment_catalog",
            )
            failure_types_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _failure_types_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_failure_types",
            )
            suppliers_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _suppliers_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_suppliers",
            )
            ticket_spare_parts_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _ticket_spare_parts_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_ticket_spare_parts",
            )
            spare_parts_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _spare_parts_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_spare_parts",
            )
            comments_df = _read_mantiz_text_insights_mysql_query(
                spark_res,
                mysql,
                _comments_for_changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_ticket_comments",
            )

            context_df = build_mantiz_ticket_context_df(
                tickets_df=tickets_df,
                branches_df=branches_df,
                clients_df=clients_df,
                equipment_instances_df=equipment_instances_df,
                equipment_catalog_df=equipment_catalog_df,
                failure_types_df=failure_types_df,
                suppliers_df=suppliers_df,
                organization_id=mysql.organization_id,
            )
            context_df = _materialize_mantiz_text_insights_df(context_df)
            changed_keys_df = _materialize_mantiz_text_insights_df(
                context_df
                .select("OrganizationId", "TicketId")
                .dropDuplicates(["OrganizationId", "TicketId"])
            )
            sources_df = build_mantiz_source_texts_from_context_df(
                context_df=context_df,
                ticket_comments_df=comments_df,
            )
            sources_df = _materialize_mantiz_text_insights_df(sources_df)
            classifier_predictions_df = _build_mantiz_classifier_predictions_df(
                context,
                spark_res,
                sql,
                sources_df,
            )

            detail_df = build_mantiz_ticket_text_insight_detail_from_sources_df(
                sources_df=sources_df,
                classifier_predictions_df=classifier_predictions_df,
            )
            detail_df = _materialize_mantiz_text_insights_df(detail_df)
            result_df = build_mantiz_ticket_text_insights_fact_from_prepared_df(
                context_df=context_df,
                sources_df=sources_df,
                insight_detail_df=detail_df,
                ticket_spare_parts_df=ticket_spare_parts_df,
                spare_parts_df=spare_parts_df,
            )
            result_df = _materialize_mantiz_text_insights_df(result_df)

            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)
            changed_ticket_count = spark_res.write_mantiz_ticket_text_insight_changed_tickets_stage(
                changed_keys_df,
                run_id,
            )
            max_source_changed_at = spark_res.get_max_time_span(
                result_df,
                "SourceLastChangedAt",
            )
            row_count = spark_res.write_fact_mantiz_ticket_text_insights_stage(result_df, run_id)
            result_df.unpersist()
            result_df = None
            sources_df.unpersist()
            sources_df = None
            context_df.unpersist()
            context_df = None
            detail_row_count = spark_res.write_mantiz_ticket_text_insight_detail_stage(
                detail_df,
                run_id,
            )
            if detail_row_count > 0:
                review_queue_df = _materialize_mantiz_text_insights_df(
                    build_mantiz_text_insight_review_queue_df(detail_df)
                )
                review_queue_rows = spark_res.write_mantiz_text_insight_review_queue_stage(
                    review_queue_df,
                    run_id,
                )
                review_queue_df.unpersist()
                review_queue_df = None
            detail_df.unpersist()
            detail_df = None
            if row_count == 0:
                max_source_changed_at = to_utc
            merged_rows = sql.replace_fact_mantiz_ticket_text_insights(run_id)
            review_queue_merged_rows = sql.merge_mantiz_text_insight_review_queue(run_id)

            context.log.info(
                str(row_count)
                + " Mantiz ticket text insight rows prepared from "
                + str(changed_ticket_count)
                + " changed tickets and "
                + str(detail_row_count)
                + " detail rows; "
                + str(merged_rows)
                + " inserted into dwh.factMantizTicketTextInsights; "
                + str(review_queue_merged_rows)
                + " review queue rows merged."
            )
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading factMantizTicketTextInsights window: " + error_message)
        try:
            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)
        except Exception:
            pass
        if retry_number < MANTIZ_TEXT_INSIGHTS_MAX_RETRIES:
            raise RetryRequested(
                max_retries=MANTIZ_TEXT_INSIGHTS_MAX_RETRIES,
                seconds_to_wait=MANTIZ_TEXT_INSIGHTS_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if result_df is not None:
            result_df.unpersist()
        if detail_df is not None:
            detail_df.unpersist()
        if changed_keys_df is not None:
            changed_keys_df.unpersist()
        if context_df is not None:
            context_df.unpersist()
        if sources_df is not None:
            sources_df.unpersist()
        if review_queue_df is not None:
            review_queue_df.unpersist()
        try:
            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)
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
        "changed_ticket_count": changed_ticket_count,
        "review_queue_rows": review_queue_rows,
        "review_queue_merged_rows": review_queue_merged_rows,
        "detail_row_count": detail_row_count,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=MANTIZ_TEXT_INSIGHTS_RETRY_POLICY,
)
def finalize_mantiz_text_insights_run_op(context, result: Dict[str, Any]):
    materialized = _finalize_mantiz_text_insights_run(context, result)
    metadata = dict(materialized.metadata or {})
    metadata.update(
        {
            "changed_ticket_count": result.get("changed_ticket_count", 0),
            "review_queue_rows": result.get("review_queue_rows", 0),
            "review_queue_merged_rows": result.get("review_queue_merged_rows", 0),
            "detail_row_count": result.get("detail_row_count", 0),
        }
    )
    return MaterializeResult(metadata=metadata)


@graph_asset(
    name="dwh_factmantiztickettextinsights",
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_factmantiztickettextinsights():
    window = start_mantiz_text_insights_run_and_get_range_op()
    result = transform_and_load_mantiz_text_insights_op(window)
    return finalize_mantiz_text_insights_run_op(result)
