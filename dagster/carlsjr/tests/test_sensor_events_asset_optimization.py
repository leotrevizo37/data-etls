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

    def test_merge_upserts_all_sensor_event_columns_without_deletes(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "carlsjr" / "defs" / "resources" / "sql_server.py"
        ).read_text()
        merge_section = source.split(
            "def build_fact_sensor_events_merge_sql", 1
        )[1].split("\n\n\ndef ", 1)[0]
        columns = (
            "SensorId",
            "DeviceId",
            "LocationId",
            "LocationName",
            "DeviceType",
            "SensorType",
            "SensorName",
            "LocalTimeSpan",
            "Value",
            "EventMinutes",
            "IsEventCompleted",
        )
        insert_section = merge_section.split("WHEN NOT MATCHED BY TARGET THEN", 1)[1]

        self.assertIn("MERGE dwh.factSensorEvents", merge_section)
        self.assertIn("FROM stg.factSensorEvents", merge_section)
        self.assertIn("WHERE RunId = ?", merge_section)
        self.assertIn("target.ReadId = source.ReadId", merge_section)
        for column in columns:
            self.assertIn("target." + column + " = source." + column, merge_section)
        for column in ("ReadId", *columns):
            self.assertIn("source." + column, insert_section)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", merge_section)
        self.assertNotIn("WHEN NOT MATCHED BY SOURCE", merge_section)
        self.assertNotIn("DELETE", merge_section.upper())

    def test_asset_uses_stage_merge_without_dwh_cleanup(self) -> None:
        asset_source = (
            PROJECT_ROOT
            / "src"
            / "carlsjr"
            / "defs"
            / "assets"
            / "dwh_factSensorEvents.py"
        ).read_text()
        write_section = asset_source.split(
            "def transform_and_load_sensor_events_op", 1
        )[1].split("@op(", 1)[0]
        spark_source = (
            PROJECT_ROOT / "src" / "carlsjr" / "defs" / "resources" / "spark.py"
        ).read_text()
        writer_section = spark_source.split("def write_fact_sensor_events", 1)[1].split(
            "    def ", 1
        )[0]

        self.assertIn("write_fact_sensor_events(events_df, run_id)", write_section)
        self.assertIn("merge_fact_sensor_events(run_id)", write_section)
        self.assertIn("clear_fact_sensor_events_stage(run_id)", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("delete_sensor_events_range(", write_section)
        self.assertIn('.withColumn("RunId", lit(run_id))', writer_section)
        self.assertIn('table="stg.factSensorEvents"', writer_section)


if __name__ == "__main__":
    unittest.main()
