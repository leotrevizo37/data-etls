import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_jdbc_in_chunks_section(source: str) -> str:
    match = re.search(
        r"def _write_jdbc_in_chunks\(.*?^    def _has_readable_files",
        source,
        re.DOTALL | re.MULTILINE,
    )
    assert match is not None
    return match.group(0)


class SensorEventsAssetOptimizationTests(unittest.TestCase):
    def test_sensor_events_reuses_single_extended_readings_load(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "emerson"
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
            / "emerson"
            / "defs"
            / "transformations"
            / "datalake_readings_extended.py"
        ).read_text()

        self.assertIn("def datalake_readings_extended(", source)
        self.assertIn("if len(readings_extended_df.head(1)) == 0:", source)
        self.assertNotIn("validate_non_empty", source)

    def test_writer_uses_present_chunks_only(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "emerson"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text()
        section = _write_jdbc_in_chunks_section(source)

        self.assertIn('df_chunked.select("__chunk_id").distinct().collect()', section)
        self.assertNotIn("range(self.jdbc_write_num_chunks)", section)
        self.assertNotIn("head(1)", section)


if __name__ == "__main__":
    unittest.main()
