import importlib.util
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    path = (
        PROJECT_ROOT
        / "src"
        / "carlsjr"
        / "defs"
        / "transformations"
        / "mantiz_text_classifier.py"
    )
    spec = importlib.util.spec_from_file_location("carlsjr_mantiz_text_classifier", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MantizTextClassifierTests(unittest.TestCase):
    def test_normalization_removes_accents_and_punctuation(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.normalize_mantiz_text("Se apretó, LLAVE!!"),
            "se apreto llave",
        )

    def test_training_returns_none_with_one_label(self) -> None:
        module = _load_module()

        result = module.train_mantiz_text_model(
            [
                ("refrigerador no enfria", "NO_ENFRIA"),
                ("congelador no congela", "NO_ENFRIA"),
            ],
            min_training_rows=2,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
