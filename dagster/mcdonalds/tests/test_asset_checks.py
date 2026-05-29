import sys
import unittest
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from mcdonalds.defs.asset_checks import (  # noqa: E402
    _build_factlecturas_original_lte_to_utc_result,
    _build_factlecturas_row_count_result,
    _build_local_window_order_result,
    _build_operations_succeeded_result,
    _build_original_lte_to_local_result,
    _build_real_readings_ratio_result,
    _build_to_local_gte_max_time_span_result,
)


class AssetChecksTests(unittest.TestCase):
    def test_factreadings_real_readings_ratio_requires_more_than_60_percent(self):
        passing = _build_real_readings_ratio_result(
            {"row_count": 10, "rows_with_readings_count_gt_0": 7}
        )
        failing = _build_real_readings_ratio_result(
            {"row_count": 10, "rows_with_readings_count_gt_0": 6}
        )

        self.assertTrue(passing.passed)
        self.assertFalse(failing.passed)

    def test_factreadings_local_window_requires_to_local_after_from_local(self):
        passing = _build_local_window_order_result(
            {
                "from_local": "2026-05-01 10:00:00",
                "to_local": "2026-05-01 11:00:00",
            }
        )
        failing = _build_local_window_order_result(
            {
                "from_local": "2026-05-01 10:00:00",
                "to_local": "2026-05-01 10:00:00",
            }
        )

        self.assertTrue(passing.passed)
        self.assertFalse(failing.passed)

    def test_operations_control_validates_success_and_watermarks(self):
        metadata = {
            "succeeded": True,
            "original_last_loaded_at": "2026-05-01 09:00:00",
            "max_time_span": "2026-05-01 10:00:00",
            "to_local": "2026-05-01 11:00:00",
        }

        self.assertTrue(_build_operations_succeeded_result(metadata).passed)
        self.assertTrue(_build_to_local_gte_max_time_span_result(metadata).passed)
        self.assertTrue(_build_original_lte_to_local_result(metadata).passed)

        self.assertFalse(
            _build_operations_succeeded_result({**metadata, "succeeded": False}).passed
        )
        self.assertFalse(
            _build_to_local_gte_max_time_span_result(
                {**metadata, "max_time_span": "2026-05-01 12:00:00"}
            ).passed
        )
        self.assertFalse(
            _build_original_lte_to_local_result(
                {**metadata, "original_last_loaded_at": "2026-05-01 12:00:00"}
            ).passed
        )

    def test_factlecturas_validates_row_count_and_original_before_to_utc(self):
        metadata = {
            "row_count": 1,
            "original_last_loaded_at": "2026-05-01 09:00:00",
            "to_utc": "2026-05-01 10:00:00",
        }

        self.assertTrue(_build_factlecturas_row_count_result(metadata).passed)
        self.assertTrue(
            _build_factlecturas_original_lte_to_utc_result(metadata).passed
        )

        self.assertFalse(
            _build_factlecturas_row_count_result({**metadata, "row_count": 0}).passed
        )
        self.assertFalse(
            _build_factlecturas_original_lte_to_utc_result(
                {**metadata, "original_last_loaded_at": "2026-05-01 11:00:00"}
            ).passed
        )


if __name__ == "__main__":
    unittest.main()
