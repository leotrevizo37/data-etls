from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional

import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from .device_operational_intelligence import (
    FEATURE_COLUMNS,
    FEATURE_SET_VERSION,
    STATE_CRITICAL,
    STATE_DEGRADED,
    STATE_EXPECTED_VALUE_DRIFT,
    STATE_OUT_OF_RANGE,
    STATE_SENSOR_FAULT,
)


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


@dataclass(frozen=True)
class DeviceOperationalTrainingResult:
    model_artifact: Dict[str, Any]
    accuracy: float
    macro_f1: float
    weighted_f1: float
    critical_recall: float
    critical_precision: float
    calibration_error: float
    training_rows: int
    validation_rows: int
    test_rows: int
    metrics: Dict[str, Any]


def _feature_vector(sample: Dict[str, Any]) -> List[float]:
    return [float(sample.get(column) or 0.0) for column in FEATURE_COLUMNS]


def _risk_label(label: str) -> int:
    return 1 if label in {
        STATE_CRITICAL,
        STATE_DEGRADED,
        STATE_SENSOR_FAULT,
        STATE_EXPECTED_VALUE_DRIFT,
        STATE_OUT_OF_RANGE,
    } else 0


def _calibration_error(y_true: List[int], y_score: List[float]) -> float:
    if not y_true:
        return 1.0
    return abs((sum(y_true) / len(y_true)) - (sum(y_score) / len(y_score)))


def dump_device_operational_model_bytes(model_artifact: Dict[str, Any]) -> bytes:
    buffer = BytesIO()
    joblib.dump(model_artifact, buffer)
    return buffer.getvalue()


def load_device_operational_model_bytes(content: bytes) -> Dict[str, Any]:
    return joblib.load(BytesIO(content))


def train_device_operational_model(
    samples: Iterable[Dict[str, Any]],
    min_training_rows: int,
    min_rows_per_class: int,
) -> Optional[DeviceOperationalTrainingResult]:
    rows = [sample for sample in samples if sample.get("OperationalStateLabel")]
    if len(rows) < min_training_rows:
        return None

    class_counts: Dict[str, int] = {}
    for row in rows:
        label = str(row["OperationalStateLabel"])
        class_counts[label] = class_counts.get(label, 0) + 1

    if len(class_counts) < 2:
        return None

    x = [_feature_vector(row) for row in rows]
    y = [str(row["OperationalStateLabel"]) for row in rows]
    risk_y = [_risk_label(label) for label in y]
    risk_class_counts = {
        risk_class: risk_y.count(risk_class)
        for risk_class in set(risk_y)
    }
    if len(risk_class_counts) < 2:
        return None

    if any(count < min_rows_per_class for count in risk_class_counts.values()):
        return None

    stratify = risk_y if min(risk_class_counts.values()) >= 2 else None
    x_train, x_tmp, y_train, y_tmp, risk_train, risk_tmp = train_test_split(
        x,
        y,
        risk_y,
        test_size=0.30,
        random_state=42,
        stratify=stratify,
    )
    tmp_risk_counts = {
        risk_class: risk_tmp.count(risk_class)
        for risk_class in set(risk_tmp)
    }
    tmp_stratify = risk_tmp if tmp_risk_counts and min(tmp_risk_counts.values()) >= 2 else None
    x_validation, x_test, y_validation, y_test, risk_validation, risk_test = train_test_split(
        x_tmp,
        y_tmp,
        risk_tmp,
        test_size=0.50,
        random_state=42,
        stratify=tmp_stratify,
    )

    state_model = HistGradientBoostingClassifier(random_state=42)
    state_model.fit(x_train, y_train)

    risk_model = HistGradientBoostingClassifier(random_state=42)
    risk_model.fit(x_train, risk_train)

    y_pred = state_model.predict(x_test)
    risk_score = [float(row[-1]) for row in risk_model.predict_proba(x_test)]
    accuracy = float(accuracy_score(y_test, y_pred))
    macro_f1 = float(f1_score(y_test, y_pred, average="macro", zero_division=0))
    weighted_f1 = float(f1_score(y_test, y_pred, average="weighted", zero_division=0))
    critical_recall = float(
        recall_score(
            [1 if label == STATE_CRITICAL else 0 for label in y_test],
            [1 if label == STATE_CRITICAL else 0 for label in y_pred],
            zero_division=0,
        )
    )
    critical_precision = float(
        precision_score(
            [1 if label == STATE_CRITICAL else 0 for label in y_test],
            [1 if label == STATE_CRITICAL else 0 for label in y_pred],
            zero_division=0,
        )
    )
    calibration_error = float(_calibration_error(risk_test, risk_score))
    model_artifact = {
        "state_model": state_model,
        "risk_model": risk_model,
        "feature_columns": FEATURE_COLUMNS,
        "class_counts": class_counts,
        "risk_class_counts": risk_class_counts,
    }
    metrics = {
        "accuracy": accuracy,
        "macro_f1": macro_f1,
        "weighted_f1": weighted_f1,
        "critical_recall": critical_recall,
        "critical_precision": critical_precision,
        "calibration_error": calibration_error,
        "class_counts": class_counts,
        "risk_class_counts": risk_class_counts,
    }
    return DeviceOperationalTrainingResult(
        model_artifact=model_artifact,
        accuracy=accuracy,
        macro_f1=macro_f1,
        weighted_f1=weighted_f1,
        critical_recall=critical_recall,
        critical_precision=critical_precision,
        calibration_error=calibration_error,
        training_rows=len(x_train),
        validation_rows=len(x_validation),
        test_rows=len(x_test),
        metrics=metrics,
    )
