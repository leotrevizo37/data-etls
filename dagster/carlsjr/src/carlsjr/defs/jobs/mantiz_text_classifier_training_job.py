from datetime import datetime
import os

from dagster import Out, RetryPolicy, job, op

from ..resources import SparkResource, SqlServerResource
from ..transformations.mantiz_text_classifier import (
    dump_mantiz_model_bytes,
    train_mantiz_text_model,
)


TRAINING_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)
MANTIZ_INSIGHT_TYPES = (
    "FALLA",
    "SOLUCION",
    "REFACCION_SOLUCION",
)


@op(
    required_resource_keys={"spark", "sqlserver"},
    out=Out(dict),
    retry_policy=TRAINING_RETRY_POLICY,
)
def train_mantiz_text_classifier_op(context) -> dict:
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver

    min_training_rows = int(os.getenv("MANTIZ_TEXT_CLASSIFIER_MIN_TRAINING_ROWS", "25"))
    min_accuracy = float(os.getenv("MANTIZ_TEXT_CLASSIFIER_MIN_ACCURACY", "0.75"))
    min_macro_f1 = float(os.getenv("MANTIZ_TEXT_CLASSIFIER_MIN_MACRO_F1", "0.70"))
    model_base_path = os.getenv(
        "MANTIZ_TEXT_MODEL_BASE_PATH",
        "s3a://data-etls/models/carlsjr/mantiz/text_insights",
    ).rstrip("/")

    results = {}
    for insight_type in MANTIZ_INSIGHT_TYPES:
        samples = sql.get_mantiz_text_classifier_training_samples(insight_type)
        training_result = train_mantiz_text_model(
            samples,
            min_training_rows=min_training_rows,
        )
        if training_result is None:
            results[insight_type] = {
                "training_rows": len(samples),
                "promoted": False,
                "reason": "not_enough_labeled_data",
            }
            continue

        model_version = (
            "carlsjr-mantiz-"
            + insight_type.lower()
            + "-"
            + datetime.utcnow().strftime("%Y%m%d%H%M%S")
        )
        artifact_path = model_base_path + "/" + insight_type.lower() + "/" + model_version
        model_bytes = dump_mantiz_model_bytes(training_result.model)
        spark_res.write_mantiz_text_model_artifact(
            artifact_path,
            model_version,
            model_bytes,
        )

        promote = (
            training_result.accuracy >= min_accuracy
            and training_result.macro_f1 >= min_macro_f1
        )
        sql.register_mantiz_text_classifier_model(
            insight_type=insight_type,
            model_version=model_version,
            artifact_path=artifact_path,
            accuracy=training_result.accuracy,
            macro_f1=training_result.macro_f1,
            training_rows=training_result.training_rows,
            promote=promote,
        )
        results[insight_type] = {
            "model_version": model_version,
            "artifact_path": artifact_path,
            "accuracy": training_result.accuracy,
            "macro_f1": training_result.macro_f1,
            "training_rows": training_result.training_rows,
            "promoted": promote,
        }

    context.add_output_metadata(results)
    return results


@job(name="101_mantiz_text_classifier_training", tags={"job_type": "mantiz", "user_code": "carlsjr"})
def mantiz_text_classifier_training_job():
    train_mantiz_text_classifier_op()
