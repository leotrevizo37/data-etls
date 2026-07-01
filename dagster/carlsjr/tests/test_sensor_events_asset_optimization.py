import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SensorEventsAssetOptimizationTests(unittest.TestCase):
    def test_carlsjr_sensor_events_uses_one_extended_reading_call(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "carlsjr"
            / "defs"
            / "assets"
            / "dwh_factSensorEvents.py"
        ).read_text()

        self.assertEqual(source.count("datalake_readings_extended("), 1)
        self.assertIn("readings_from = min(from_utc, temperature_from_utc)", source)
        self.assertIn("readings_extended_df.localCheckpoint(", source)
        self.assertIn("readings_extended_df.unpersist(blocking=True)", source)
        self.assertIn('col("LocalTimeSpan") >= from_utc', source)
        self.assertIn('col("LocalTimeSpan") >= temperature_from_utc', source)
        self.assertNotIn("head(1)", source)

    def test_datalake_readings_extended_contract_is_unchanged(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "carlsjr"
            / "defs"
            / "transformations"
            / "datalake_readings_extended.py"
        ).read_text()

        self.assertIn("def datalake_readings_extended(", source)
        self.assertIn("if len(readings_extended_df.head(1)) == 0:", source)
        self.assertNotIn("validate_non_empty", source)


if __name__ == "__main__":
    unittest.main()
