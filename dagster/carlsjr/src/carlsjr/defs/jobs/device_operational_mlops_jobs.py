from datetime import datetime, timedelta
import json
import os

from dagster import Out, RetryPolicy, job, op

from ..resources import SparkResource, SqlServerResource
from ..transformations.device_operational_intelligence import (
    FEATURE_SET_VERSION,
    MODEL_FAMILY,
    MODEL_SCOPE,
    SCORING_VERSION,
    feature_schema_json,
)
from ..transformations.device_operational_model import (
    dump_device_operational_model_bytes,
    train_device_operational_model,
)


USER_CODE = "carlsjr"
MLOPS_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)


def _model_promotion_score(metrics: dict) -> float:
    return round(
        (float(metrics.get("macro_f1") or 0.0) * 0.40)
        + (float(metrics.get("weighted_f1") or 0.0) * 0.20)
        + (float(metrics.get("critical_recall") or 0.0) * 0.30)
        + (float(metrics.get("critical_precision") or 0.0) * 0.10)
        - (float(metrics.get("calibration_error") or 0.0) * 0.10),
        6,
    )


def _decide_model_promotion(
    candidate_metrics: dict,
    active_model: dict | None,
    min_macro_f1: float,
    min_critical_recall: float,
    max_calibration_error: float,
    min_score_delta: float,
) -> dict:
    candidate_score = _model_promotion_score(candidate_metrics)
    if float(candidate_metrics["macro_f1"]) < min_macro_f1:
        return {
            "promoted": False,
            "promotion_reason": "candidate_failed_macro_f1_gate",
            "candidate_score": candidate_score,
            "active_score": None,
        }
    if float(candidate_metrics["critical_recall"]) < min_critical_recall:
        return {
            "promoted": False,
            "promotion_reason": "candidate_failed_critical_recall_gate",
            "candidate_score": candidate_score,
            "active_score": None,
        }
    if float(candidate_metrics["calibration_error"]) > max_calibration_error:
        return {
            "promoted": False,
            "promotion_reason": "candidate_failed_calibration_gate",
            "candidate_score": candidate_score,
            "active_score": None,
        }
    if active_model is None:
        return {
            "promoted": True,
            "promotion_reason": "no_active_model",
            "candidate_score": candidate_score,
            "active_score": None,
        }

    active_feature_set = active_model.get("feature_set_version")
    if active_feature_set and active_feature_set != FEATURE_SET_VERSION:
        return {
            "promoted": True,
            "promotion_reason": "active_model_feature_set_incompatible",
            "candidate_score": candidate_score,
            "active_score": None,
            "active_model_version": active_model.get("model_version"),
        }

    active_score = _model_promotion_score(active_model)
    promoted = candidate_score >= active_score + min_score_delta
    return {
        "promoted": promoted,
        "promotion_reason": (
            "candidate_score_met_or_exceeded_active"
            if promoted
            else "active_model_score_not_exceeded"
        ),
        "candidate_score": candidate_score,
        "active_score": active_score,
        "active_model_version": active_model.get("model_version"),
    }


@op(
    required_resource_keys={"spark", "sqlserver"},
    out=Out(dict),
    retry_policy=MLOPS_RETRY_POLICY,
)
def train_device_operational_model_op(context) -> dict:
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    min_training_rows = int(os.getenv("SENSE_DOI_MIN_TRAINING_ROWS", "1000"))
    min_rows_per_class = int(os.getenv("SENSE_DOI_MIN_ROWS_PER_CLASS", "50"))
    training_window_days = int(os.getenv("SENSE_DOI_TRAINING_WINDOW_DAYS", "180"))
    min_macro_f1 = float(os.getenv("SENSE_DOI_MIN_MACRO_F1", "0.70"))
    min_critical_recall = float(os.getenv("SENSE_DOI_MIN_CRITICAL_RECALL", "0.80"))
    max_calibration_error = float(os.getenv("SENSE_DOI_MAX_CALIBRATION_ERROR", "0.10"))
    min_score_delta = float(os.getenv("SENSE_DOI_MIN_PROMOTION_SCORE_DELTA", "0.0"))
    model_base_path = os.getenv(
        "SENSE_DOI_MODEL_BASE_PATH",
        "s3a://data-etls/models/carlsjr/sense/device_operational_intelligence",
    ).rstrip("/")
    active_model = sql.get_active_device_operational_model(USER_CODE, MODEL_SCOPE)
    samples = sql.get_device_operational_training_samples(
        USER_CODE,
        training_window_days,
    )
    training_result = train_device_operational_model(
        samples,
        min_training_rows=min_training_rows,
        min_rows_per_class=min_rows_per_class,
    )
    if training_result is None:
        metadata = {
            "training_rows_available": len(samples),
            "candidate_created": False,
            "reason": "insufficient_training_data_or_class_balance",
        }
        context.add_output_metadata(metadata)
        return metadata

    model_version = datetime.utcnow().strftime("carlsjr-sense-doi-%Y%m%d%H%M%S")
    artifact_path = model_base_path + "/" + model_version
    model_bytes = dump_device_operational_model_bytes(training_result.model_artifact)
    spark_res.write_device_operational_model_artifact(
        artifact_path,
        model_version,
        model_bytes,
    )
    feature_schema = json.dumps(feature_schema_json(), sort_keys=True)
    metrics_json = json.dumps(training_result.metrics, sort_keys=True)
    distribution_json = json.dumps(
        training_result.model_artifact.get("class_counts", {}),
        sort_keys=True,
    )
    sql.register_device_operational_feature_set(
        user_code=USER_CODE,
        feature_set_version=FEATURE_SET_VERSION,
        feature_schema_json=feature_schema,
    )
    candidate_metrics = {
        "macro_f1": training_result.macro_f1,
        "weighted_f1": training_result.weighted_f1,
        "critical_recall": training_result.critical_recall,
        "critical_precision": training_result.critical_precision,
        "calibration_error": training_result.calibration_error,
    }
    promotion = _decide_model_promotion(
        candidate_metrics=candidate_metrics,
        active_model=active_model,
        min_macro_f1=min_macro_f1,
        min_critical_recall=min_critical_recall,
        max_calibration_error=max_calibration_error,
        min_score_delta=min_score_delta,
    )
    sql.register_device_operational_model(
        user_code=USER_CODE,
        model_scope=MODEL_SCOPE,
        model_version=model_version,
        model_family=MODEL_FAMILY,
        artifact_path=artifact_path,
        feature_set_version=FEATURE_SET_VERSION,
        scoring_version=SCORING_VERSION,
        training_window_start=datetime.utcnow() - timedelta(days=training_window_days),
        training_window_end=datetime.utcnow(),
        training_rows=training_result.training_rows,
        validation_rows=training_result.validation_rows,
        test_rows=training_result.test_rows,
        accuracy=training_result.accuracy,
        macro_f1=training_result.macro_f1,
        weighted_f1=training_result.weighted_f1,
        critical_recall=training_result.critical_recall,
        critical_precision=training_result.critical_precision,
        calibration_error=training_result.calibration_error,
        metrics_json=metrics_json,
        feature_schema_json=feature_schema,
        training_distribution_json=distribution_json,
        promote=promotion["promoted"],
    )
    metadata = {
        "model_version": model_version,
        "artifact_path": artifact_path,
        "candidate_created": True,
        "accuracy": training_result.accuracy,
        "macro_f1": training_result.macro_f1,
        "weighted_f1": training_result.weighted_f1,
        "critical_recall": training_result.critical_recall,
        "critical_precision": training_result.critical_precision,
        "calibration_error": training_result.calibration_error,
        "training_rows": training_result.training_rows,
        "validation_rows": training_result.validation_rows,
        "test_rows": training_result.test_rows,
        "min_macro_f1": min_macro_f1,
        "min_critical_recall": min_critical_recall,
        "max_calibration_error": max_calibration_error,
        "min_score_delta": min_score_delta,
        **promotion,
    }
    context.add_output_metadata(metadata)
    return metadata


@op(
    required_resource_keys={"sqlserver"},
    out=Out(dict),
    retry_policy=MLOPS_RETRY_POLICY,
)
def promote_device_operational_reviews_op(context) -> dict:
    sql: SqlServerResource = context.resources.sqlserver
    promoted_rows = sql.promote_device_operational_reviews(USER_CODE)
    metadata = {"promoted_review_rows": promoted_rows}
    context.add_output_metadata(metadata)
    return metadata


@job(name="120_sense_device_operational_training", tags={"job_type": "sense_doi_training", "user_code": "carlsjr"})
def device_operational_training_job():
    train_device_operational_model_op()


@job(name="122_sense_device_insight_review_promotion", tags={"job_type": "sense_doi_training", "user_code": "carlsjr"})
def device_operational_review_promotion_job():
    promote_device_operational_reviews_op()
