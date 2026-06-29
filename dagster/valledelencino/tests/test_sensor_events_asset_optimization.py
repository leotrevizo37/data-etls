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
    def test_unified_sense_graph_uses_one_extended_reading_call(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "valledelencino"
            / "defs"
            / "assets"
            / "sense_operational_assets.py"
        ).read_text()
        job_source = (
            PROJECT_ROOT
            / "src"
            / "valledelencino"
            / "defs"
            / "jobs"
            / "sense_operational_job.py"
        ).read_text()

        self.assertEqual(source.count("datalake_readings_extended("), 1)
        self.assertIn("@graph_multi_asset", source)
        self.assertIn('AssetKey(["valledelencino", "dwh_factReadingsMeasurement"])', source)
        self.assertIn('AssetKey(["valledelencino", "ctl_IngestionControl"])', source)
        self.assertIn('AssetKey(["valledelencino", "dwh_factsensorevents"])', source)
        self.assertIn('"020_sense_operations_sensor_events"', job_source)
        self.assertIn('AssetKey(["valledelencino", "dwh_factReadingsMeasurement"])', job_source)
        self.assertIn('AssetKey(["valledelencino", "ctl_IngestionControl"])', job_source)
        self.assertIn('AssetKey(["valledelencino", "dwh_factsensorevents"])', job_source)
        self.assertIn("in_process_executor", job_source)
        self.assertIn("operations_done=finalize_operations", source)
        self.assertIn('col("LocalTimeSpan") >= from_utc', source)
        self.assertIn('asset_key=AssetKey(["valledelencino", "dwh_factsensorevents"])', (
            PROJECT_ROOT
            / "src"
            / "valledelencino"
            / "defs"
            / "assets"
            / "dwh_factSensorEvents.py"
        ).read_text())
        self.assertIn('"dwh_factsensorevents": finalize_events', source)
        self.assertNotIn("AssetIn", source)
        self.assertNotIn("data_transformations", source)
        self.assertNotIn("dwh_factlecturas", source)
        self.assertNotIn("dwh_factlecturas", job_source)
        self.assertNotIn("extended_df.localCheckpoint(", source)
        self.assertNotIn("head(1)", source)

    def test_datalake_readings_extended_contract_is_unchanged(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "valledelencino"
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
            / "valledelencino"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text()
        section = _write_jdbc_in_chunks_section(source)

        self.assertIn('df_chunked.select("__chunk_id").distinct().collect()', section)
        self.assertNotIn("range(self.jdbc_write_num_chunks)", section)
        self.assertNotIn("head(1)", section)

    def test_legacy_operational_defs_are_not_auto_loadable(self) -> None:
        defs_path = PROJECT_ROOT / "src" / "valledelencino" / "defs"

        self.assertNotIn(
            "@graph_asset(",
            (defs_path / "assets" / "dwh_factSensorEvents.py").read_text(),
        )
        self.assertNotIn(
            "@graph_multi_asset(",
            (defs_path / "assets" / "operations_assets.py").read_text(),
        )
        self.assertNotIn(
            "define_asset_job(",
            (defs_path / "jobs" / "fact_operations_tables_job.py").read_text(),
        )
        self.assertNotIn(
            "define_asset_job(",
            (defs_path / "jobs" / "factsensorevents_job.py").read_text(),
        )
        self.assertNotIn(
            "ScheduleDefinition(",
            (defs_path / "schedules" / "factoperations_scheduler.py").read_text(),
        )
        self.assertNotIn(
            "ScheduleDefinition(",
            (defs_path / "schedules" / "factsensorevents_scheduler.py").read_text(),
        )


if __name__ == "__main__":
    unittest.main()
