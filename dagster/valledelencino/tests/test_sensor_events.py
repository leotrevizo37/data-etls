import sys
import unittest
import warnings
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from valledelencino.defs.transformations.sensor_events import (  # noqa: E402
    sensor_events,
)


warnings.simplefilter("ignore", ResourceWarning)


class SensorEventsTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("sensor-events-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _readings_df(self, rows: list[dict]) -> DataFrame:
        return self.spark.createDataFrame(rows)

    def test_returns_none_when_no_puerta_or_cava_readings(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Gabinete",
                    "sensor_type": "Temperatura",
                    "sensor_name": "Sensor1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNone(result)

    def test_detects_state_changes_for_puerta(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                },
                {
                    "ReadId": "r2",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 0.0,
                },
                {
                    "ReadId": "r3",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 1.0,
                },
                {
                    "ReadId": "r4",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 15),
                    "Value": 1.0,
                },
                {
                    "ReadId": "r5",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        # State changes: r1(0, first), r3(0->1), r5(1->0)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["ReadId"], "r1")
        self.assertEqual(rows[1]["ReadId"], "r3")
        self.assertEqual(rows[2]["ReadId"], "r5")

    def test_calculates_event_minutes_for_open_then_close(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
                {
                    "ReadId": "r2",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 30),
                    "Value": 0.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual(len(rows), 2)
        # Open event: 30 minutes until close
        self.assertAlmostEqual(rows[0]["EventMinutes"], 30.0)
        self.assertTrue(rows[0]["IsEventCompleted"])
        # Close event: 0 minutes
        self.assertAlmostEqual(rows[1]["EventMinutes"], 0.0)
        self.assertTrue(rows[1]["IsEventCompleted"])

    def test_unclosed_event_uses_to_utc_for_partial_minutes(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Cava",
                    "sensor_type": "Estado",
                    "sensor_name": "Cava 1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        rows = result.collect()
        self.assertEqual(len(rows), 1)
        # Open at 10:00, no close, to_utc = 11:00 → 60 minutes
        self.assertAlmostEqual(rows[0]["EventMinutes"], 60.0)
        self.assertFalse(rows[0]["IsEventCompleted"])

    def test_multiple_sensors_compute_independently(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta 1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
                {
                    "ReadId": "r2",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta 1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 0.0,
                },
                {
                    "ReadId": "r3",
                    "SensorId": "s2",
                    "DeviceId": "d2",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Cava",
                    "sensor_type": "Estado",
                    "sensor_name": "Cava 1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 1.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("SensorId", "LocalTimeSpan").collect()
        self.assertEqual(len(rows), 3)

        # s1: open at 10:00, close at 10:10 → 10 min, completed
        s1_rows = [r for r in rows if r["SensorId"] == "s1"]
        self.assertEqual(len(s1_rows), 2)
        self.assertAlmostEqual(s1_rows[0]["EventMinutes"], 10.0)
        self.assertTrue(s1_rows[0]["IsEventCompleted"])

        # s2: open at 10:05, no close → partial (115 min to 12:00)
        s2_rows = [r for r in rows if r["SensorId"] == "s2"]
        self.assertEqual(len(s2_rows), 1)
        self.assertAlmostEqual(s2_rows[0]["EventMinutes"], 115.0)
        self.assertFalse(s2_rows[0]["IsEventCompleted"])

    def test_output_columns_match_sql_table_schema(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta Principal",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        expected_columns = {
            "ReadId",
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
        }
        self.assertEqual(set(result.columns), expected_columns)

    def test_close_event_has_zero_event_minutes(self) -> None:
        df = self._readings_df(
            [
                {
                    "ReadId": "r1",
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "location_name": "Loc1",
                    "device_type": "Puerta",
                    "sensor_type": "Estado",
                    "sensor_name": "Puerta 1",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                },
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 0.0)
        self.assertTrue(rows[0]["IsEventCompleted"])


if __name__ == "__main__":
    unittest.main()
