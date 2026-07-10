import sys
import types
import unittest
from importlib import import_module
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

try:
    __import__("pyodbc")
except ImportError:
    pyodbc_stub = types.ModuleType("pyodbc")
    pyodbc_stub.Connection = object
    pyodbc_stub.Error = Exception
    sys.modules["pyodbc"] = pyodbc_stub

mlops_module = import_module("carlsjr.defs.jobs.device_operational_mlops_jobs")
decide_model_promotion = mlops_module._decide_model_promotion
model_module = import_module("carlsjr.defs.transformations.device_operational_model")
doi_module = import_module("carlsjr.defs.transformations.device_operational_intelligence")
train_device_operational_model = model_module.train_device_operational_model
FEATURE_COLUMNS = doi_module.FEATURE_COLUMNS


class DeviceOperationalMLOpsJobTests(unittest.TestCase):
    def test_training_promotes_when_no_active_model_and_gates_pass(self) -> None:
        decision = decide_model_promotion(
            candidate_metrics={
                "macro_f1": 0.80,
                "weighted_f1": 0.82,
                "critical_recall": 0.90,
                "critical_precision": 0.75,
                "calibration_error": 0.05,
            },
            active_model=None,
            min_macro_f1=0.70,
            min_critical_recall=0.80,
            max_calibration_error=0.10,
            min_score_delta=0.0,
        )

        self.assertTrue(decision["promoted"])
        self.assertEqual(decision["promotion_reason"], "no_active_model")

    def test_training_blocks_candidate_below_required_recall(self) -> None:
        decision = decide_model_promotion(
            candidate_metrics={
                "macro_f1": 0.90,
                "weighted_f1": 0.91,
                "critical_recall": 0.70,
                "critical_precision": 0.90,
                "calibration_error": 0.01,
            },
            active_model=None,
            min_macro_f1=0.70,
            min_critical_recall=0.80,
            max_calibration_error=0.10,
            min_score_delta=0.0,
        )

        self.assertFalse(decision["promoted"])
        self.assertEqual(
            decision["promotion_reason"],
            "candidate_failed_critical_recall_gate",
        )

    def test_training_keeps_active_model_when_candidate_score_is_lower(self) -> None:
        decision = decide_model_promotion(
            candidate_metrics={
                "macro_f1": 0.75,
                "weighted_f1": 0.76,
                "critical_recall": 0.82,
                "critical_precision": 0.70,
                "calibration_error": 0.08,
            },
            active_model={
                "model_version": "active-v1",
                "macro_f1": 0.86,
                "weighted_f1": 0.87,
                "critical_recall": 0.90,
                "critical_precision": 0.83,
                "calibration_error": 0.04,
            },
            min_macro_f1=0.70,
            min_critical_recall=0.80,
            max_calibration_error=0.10,
            min_score_delta=0.0,
        )

        self.assertFalse(decision["promoted"])
        self.assertEqual(
            decision["promotion_reason"],
            "active_model_score_not_exceeded",
        )
        self.assertEqual(decision["active_model_version"], "active-v1")

    def test_training_promotes_candidate_when_score_exceeds_active_model(self) -> None:
        decision = decide_model_promotion(
            candidate_metrics={
                "macro_f1": 0.88,
                "weighted_f1": 0.89,
                "critical_recall": 0.93,
                "critical_precision": 0.85,
                "calibration_error": 0.03,
            },
            active_model={
                "model_version": "active-v1",
                "macro_f1": 0.80,
                "weighted_f1": 0.81,
                "critical_recall": 0.85,
                "critical_precision": 0.78,
                "calibration_error": 0.05,
            },
            min_macro_f1=0.70,
            min_critical_recall=0.80,
            max_calibration_error=0.10,
            min_score_delta=0.0,
        )

        self.assertTrue(decision["promoted"])
        self.assertEqual(
            decision["promotion_reason"],
            "candidate_score_met_or_exceeded_active",
        )
        self.assertEqual(decision["active_model_version"], "active-v1")

    def test_training_promotes_candidate_when_active_feature_set_is_incompatible(self) -> None:
        decision = decide_model_promotion(
            candidate_metrics={
                "macro_f1": 0.80,
                "weighted_f1": 0.82,
                "critical_recall": 0.90,
                "critical_precision": 0.75,
                "calibration_error": 0.05,
            },
            active_model={
                "model_version": "active-v1",
                "feature_set_version": "sense-doi-features-202607",
                "macro_f1": 0.99,
                "weighted_f1": 0.99,
                "critical_recall": 0.99,
                "critical_precision": 0.99,
                "calibration_error": 0.01,
            },
            min_macro_f1=0.70,
            min_critical_recall=0.80,
            max_calibration_error=0.10,
            min_score_delta=0.0,
        )

        self.assertTrue(decision["promoted"])
        self.assertEqual(
            decision["promotion_reason"],
            "active_model_feature_set_incompatible",
        )
        self.assertEqual(decision["active_model_version"], "active-v1")

    def test_training_uses_risk_balance_not_every_state_count(self) -> None:
        samples = []
        for index in range(60):
            sample = {column: float(index % 7) for column in FEATURE_COLUMNS}
            sample["OperationalStateLabel"] = "NORMAL"
            samples.append(sample)
        for index in range(60):
            sample = {column: float(20 + (index % 7)) for column in FEATURE_COLUMNS}
            sample["OperationalStateLabel"] = "DEGRADED"
            samples.append(sample)
        rare_state = {column: 40.0 for column in FEATURE_COLUMNS}
        rare_state["OperationalStateLabel"] = "OUT_OF_RANGE"
        samples.append(rare_state)

        result = train_device_operational_model(
            samples,
            min_training_rows=100,
            min_rows_per_class=50,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.model_artifact["class_counts"]["OUT_OF_RANGE"], 1)
        self.assertGreaterEqual(result.model_artifact["risk_class_counts"][0], 50)
        self.assertGreaterEqual(result.model_artifact["risk_class_counts"][1], 50)


if __name__ == "__main__":
    unittest.main()
