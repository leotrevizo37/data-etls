from datetime import datetime
import os
from typing import Any, Dict, Optional

from dagster import In, MaterializeResult, Out, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import MantizMySqlResource, SparkResource, SqlServerResource
from ..transformations.mantiz_tickets import (
    build_mantiz_changed_ticket_keys_df,
    build_mantiz_source_texts_df,
    build_mantiz_text_insight_review_queue_df,
    build_mantiz_ticket_text_insight_detail_from_sources_df,
    build_mantiz_ticket_text_insights_fact_df,
    empty_mantiz_classifier_predictions_df,
)
from ..transformations.mantiz_text_classifier import (
    load_mantiz_model_bytes,
    predict_mantiz_text,
)
from .mantiz_common import (
    MANTIZ_MAX_RETRIES,
    MANTIZ_RETRY_DELAY_SECONDS,
    MANTIZ_RETRY_POLICY,
    TEXT_INSIGHTS_INGESTION_NAME,
    changed_text_tickets_query,
    comments_for_changed_tickets_query,
    finalize_run,
    read_mysql_query,
    read_mysql_table,
    start_run,
    validate_mantiz_config,
)


def _load_active_mantiz_text_models(
    context,
    sql: SqlServerResource,
    spark_res: SparkResource,
) -> Dict[str, tuple[Any, str]]:
    active_models = sql.get_active_mantiz_text_classifier_models()
    loaded = {}
    for insight_type, model_info in active_models.items():
        try:
            content = spark_res.read_mantiz_text_model_artifact(model_info["artifact_path"])
            loaded[insight_type] = (
                load_mantiz_model_bytes(content),
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
    models = _load_active_mantiz_text_models(context, sql, spark_res)
    if not models:
        return empty_mantiz_classifier_predictions_df(spark)

    threshold = float(os.getenv("MANTIZ_TEXT_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.75"))
    predictions = []
    rows = (
        sources_df
        .select(
            "OrganizationId",
            "TicketId",
            "SourceType",
            "SourceId",
            "NormalizedText",
        )
        .collect()
    )
    for row in rows:
        for insight_type, model_with_version in models.items():
            model, model_version = model_with_version
            prediction = predict_mantiz_text(
                model,
                row.NormalizedText,
                model_version,
                threshold,
            )
            if prediction is None:
                continue
            predictions.append(
                (
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
            )

    if not predictions:
        return empty_mantiz_classifier_predictions_df(spark)

    return spark.createDataFrame(
        predictions,
        "OrganizationId string, TicketId string, SourceType string, SourceId string, "
        "InsightType string, CanonicalCode string, CanonicalName string, "
        "ClassifierModelVersion string, ClassifierConfidence double",
    )


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_RETRY_POLICY,
)
def start_mantiz_text_insights_run_and_get_range_op(context) -> Dict[str, Any]:
    return start_run(context, TEXT_INSIGHTS_INGESTION_NAME)


@op(
    required_resource_keys={"sqlserver", "spark", "mantiz_mysql"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_RETRY_POLICY,
)
def transform_and_load_mantiz_text_insights_op(
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
    changed_ticket_count = 0
    merged_rows = 0
    review_queue_rows = 0
    review_queue_merged_rows = 0
    detail_row_count = 0
    result_df: Optional[DataFrame] = None
    detail_df: Optional[DataFrame] = None
    changed_keys_df: Optional[DataFrame] = None
    sources_df: Optional[DataFrame] = None
    review_queue_df: Optional[DataFrame] = None

    try:
        if retry_number > 0:
            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)

        tickets_df = read_mysql_query(
            spark_res,
            mysql,
            changed_text_tickets_query(from_utc, to_utc, mysql.organization_id),
            "changed_mantiz_text_tickets",
        )

        if len(tickets_df.head(1)) == 0:
            context.log.info("No Mantiz text insight tickets found in window.")
            succeeded = True
        else:
            branches_df = read_mysql_table(spark_res, mysql, "Branch")
            clients_df = read_mysql_table(spark_res, mysql, "Client")
            equipment_instances_df = read_mysql_table(spark_res, mysql, "EquipmentInstance")
            equipment_catalog_df = read_mysql_table(spark_res, mysql, "EquipmentCatalog")
            failure_types_df = read_mysql_table(spark_res, mysql, "FailureType")
            suppliers_df = read_mysql_table(spark_res, mysql, "Supplier")
            ticket_spare_parts_df = read_mysql_table(spark_res, mysql, "TicketSparePart")
            spare_parts_df = read_mysql_table(spark_res, mysql, "SparePart")
            comments_df = read_mysql_query(
                spark_res,
                mysql,
                comments_for_changed_tickets_query(from_utc, to_utc, mysql.organization_id),
                "changed_mantiz_ticket_comments",
            )

            changed_keys_df = build_mantiz_changed_ticket_keys_df(
                tickets_df=tickets_df,
                branches_df=branches_df,
                clients_df=clients_df,
                equipment_instances_df=equipment_instances_df,
                equipment_catalog_df=equipment_catalog_df,
                failure_types_df=failure_types_df,
                suppliers_df=suppliers_df,
                organization_id=mysql.organization_id,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            changed_ticket_count = changed_keys_df.count()
            sources_df = build_mantiz_source_texts_df(
                tickets_df=tickets_df,
                ticket_comments_df=comments_df,
                branches_df=branches_df,
                clients_df=clients_df,
                equipment_instances_df=equipment_instances_df,
                equipment_catalog_df=equipment_catalog_df,
                failure_types_df=failure_types_df,
                suppliers_df=suppliers_df,
                organization_id=mysql.organization_id,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            classifier_predictions_df = _build_mantiz_classifier_predictions_df(
                context,
                spark_res,
                sql,
                sources_df,
            )

            detail_df = build_mantiz_ticket_text_insight_detail_from_sources_df(
                sources_df=sources_df,
                classifier_predictions_df=classifier_predictions_df,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            result_df = build_mantiz_ticket_text_insights_fact_df(
                tickets_df=tickets_df,
                ticket_comments_df=comments_df,
                branches_df=branches_df,
                clients_df=clients_df,
                equipment_instances_df=equipment_instances_df,
                equipment_catalog_df=equipment_catalog_df,
                failure_types_df=failure_types_df,
                suppliers_df=suppliers_df,
                organization_id=mysql.organization_id,
                insight_detail_df=detail_df,
                ticket_spare_parts_df=ticket_spare_parts_df,
                spare_parts_df=spare_parts_df,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            row_count = result_df.count()
            detail_row_count = detail_df.count()
            sql.clear_fact_mantiz_ticket_text_insights_stage(run_id)
            if changed_ticket_count > 0:
                spark_res.write_mantiz_ticket_text_insight_changed_tickets_stage(
                    changed_keys_df,
                    run_id,
                )
            if row_count > 0:
                max_source_changed_at = spark_res.get_max_time_span(
                    result_df,
                    "SourceLastChangedAt",
                )
                spark_res.write_fact_mantiz_ticket_text_insights_stage(result_df, run_id)
            if detail_row_count > 0:
                spark_res.write_mantiz_ticket_text_insight_detail_stage(detail_df, run_id)
                review_queue_df = build_mantiz_text_insight_review_queue_df(detail_df).persist(
                    StorageLevel.MEMORY_AND_DISK,
                )
                review_queue_rows = review_queue_df.count()
                if review_queue_rows > 0:
                    spark_res.write_mantiz_text_insight_review_queue_stage(
                        review_queue_df,
                        run_id,
                    )
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
        if retry_number < MANTIZ_MAX_RETRIES:
            raise RetryRequested(
                max_retries=MANTIZ_MAX_RETRIES,
                seconds_to_wait=MANTIZ_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if result_df is not None:
            result_df.unpersist()
        if detail_df is not None:
            detail_df.unpersist()
        if changed_keys_df is not None:
            changed_keys_df.unpersist()
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
    retry_policy=MANTIZ_RETRY_POLICY,
)
def finalize_mantiz_text_insights_run_op(context, result: Dict[str, Any]):
    materialized = finalize_run(context, TEXT_INSIGHTS_INGESTION_NAME, result)
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
