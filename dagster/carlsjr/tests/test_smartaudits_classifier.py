import sys
import unittest
from importlib import import_module
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

classifier_module = import_module("carlsjr.defs.transformations.smartaudits_classifier")


class SmartAuditsClassifierTests(unittest.TestCase):
    def test_deterministic_rules_cover_base_categories(self) -> None:
        cases = [
            ("La imagen esta borrosa y no se distingue.", 0, "IMAGEN_NO_LEGIBLE"),
            ("No se puede determinar la temperatura visible.", 0, "IMAGEN_NO_PROCESABLE"),
            ("La lectura esta fuera del rango esperado.", 0, "FUERA_DE_RANGO"),
            ("Hay suciedad y manchas visibles.", 0, "INCUMPLIMIENTO_LIMPIEZA"),
            ("La evidencia no corresponde con la tarea.", 0, "INCUMPLIMIENTO_GENERAL"),
            ("La evidencia cumple correctamente.", 1, "CUMPLIMIENTO"),
        ]

        for comment, result_ai, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    classifier_module.deterministic_result_category(comment, result_ai),
                    expected,
                )

    def test_unknown_failed_comment_stays_unclassified_before_ml(self) -> None:
        self.assertIsNone(
            classifier_module.deterministic_result_category("Comentario ambiguo", 0)
        )

    def test_mode_category_uses_count_then_severity_then_evidence_id(self) -> None:
        result = classifier_module.choose_mode_category(
            [
                ("IMAGEN_NO_LEGIBLE", 3),
                ("IMAGEN_NO_PROCESABLE", 2),
                ("IMAGEN_NO_PROCESABLE", 4),
                ("IMAGEN_NO_LEGIBLE", 1),
            ]
        )

        self.assertEqual(result, "IMAGEN_NO_PROCESABLE")

    def test_prediction_with_synthetic_training_data(self) -> None:
        try:
            __import__("sklearn")
        except Exception:
            self.skipTest("scikit-learn is not installed in this environment")

        samples = []
        for index in range(30):
            samples.append((f"foto borrosa no se distingue {index}", "IMAGEN_NO_LEGIBLE"))
            samples.append((f"no se puede determinar la lectura {index}", "IMAGEN_NO_PROCESABLE"))

        training = classifier_module.train_result_category_model(
            samples,
            min_training_rows=10,
        )

        self.assertIsNotNone(training)
        prediction = classifier_module.predict_result_category(
            training.model,
            "la imagen esta muy borrosa",
            "test-model",
            0.2,
        )
        self.assertEqual(prediction.category, "IMAGEN_NO_LEGIBLE")
        self.assertGreaterEqual(prediction.confidence, 0.2)


if __name__ == "__main__":
    unittest.main()
