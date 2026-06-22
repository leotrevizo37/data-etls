import sys
import unittest
import warnings
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

sensor_events_module = import_module("carlsjr.defs.transformations.sensor_events")
temperature_events_module = import_module("carlsjr.defs.transformations.temperature_events")
sensor_events = sensor_events_module.sensor_events
resolve_sensor_events_from = sensor_events_module.resolve_sensor_events_from
temperature_events = temperature_events_module.temperature_events


warnings.simplefilter("ignore", ResourceWarning)


class SensorEventsTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("carlsjr-sensor-events-tests")
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

    def _expected_values_df(self, rows: list[dict]) -> DataFrame:
        return self.spark.createDataFrame(rows)

    def _row(
        self,
        read_id: str,
        sensor_id: str,
        local_time: datetime,
        value: float,
        device_type: str = "Monitoreo camara",
        sensor_type: str = "Camara de presencia",
        time_span: Optional[datetime] = None,
    ) -> dict:
        return {
            "ReadId": read_id,
            "SensorId": sensor_id,
            "DeviceId": "d-" + sensor_id,
            "LocationId": "l1",
            "sublocation_name": "Local 1",
            "device_type": device_type,
            "sensor_type": sensor_type,
            "sensor_name": "sensor-" + sensor_id,
            "TimeSpan": time_span if time_span is not None else local_time,
            "LocalTimeSpan": local_time,
            "Value": value,
        }

    def test_detects_state_changes_for_presence_camera(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 5), 0.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 10, 10), 1.0),
                self._row("r4", "s1", datetime(2026, 3, 29, 10, 20), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual([row["ReadId"] for row in rows], ["r1", "r3", "r4"])
        self.assertEqual([row["Value"] for row in rows], [0.0, 1.0, 0.0])
        self.assertAlmostEqual(rows[0]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[1]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[2]["EventMinutes"], 40.0)
        self.assertFalse(rows[2]["IsEventCompleted"])

    def test_reprocess_start_uses_open_event_when_older_than_control(self) -> None:
        control_from = datetime(2026, 3, 29, 11, 0)
        open_event_from = datetime(2026, 3, 29, 10, 0)

        result = resolve_sensor_events_from(control_from, open_event_from)

        self.assertEqual(result, open_event_from)

    def test_reprocess_start_uses_control_when_no_open_event_exists(self) -> None:
        control_from = datetime(2026, 3, 29, 11, 0)

        result = resolve_sensor_events_from(control_from, None)

        self.assertEqual(result, control_from)

    def test_ignores_non_presence_camera_readings(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row(
                    "noise-sensor",
                    "s1",
                    datetime(2026, 3, 29, 10, 5),
                    0.0,
                    sensor_type="Encendido",
                ),
                self._row(
                    "noise-device",
                    "s2",
                    datetime(2026, 3, 29, 10, 10),
                    1.0,
                    device_type="Camara fria",
                ),
                self._row("noise-value", "s1", datetime(2026, 3, 29, 10, 15), 2.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 30), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual([row["ReadId"] for row in rows], ["r1", "r2"])
        self.assertAlmostEqual(rows[0]["EventMinutes"], 30.0)
        self.assertAlmostEqual(rows[1]["EventMinutes"], 30.0)

    def test_unclosed_event_uses_utc_time_span_for_run_end(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    time_span=datetime(2026, 3, 29, 16, 0),
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 17:00:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 60.0)
        self.assertFalse(rows[0]["IsEventCompleted"])

    def test_ignores_temperature_out_of_range_when_under_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 11, 30),
                    40.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:00:00")

        self.assertEqual(result.collect(), [])

    def test_detects_completed_temperature_event_after_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 12, 0),
                    40.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:30:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadId"], "t1")
        self.assertEqual(rows[0]["SensorType"], "Temperatura F")
        self.assertAlmostEqual(rows[0]["Value"], 50.0)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 120.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_detects_open_temperature_event_after_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 10, 30),
                    51.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:30:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadId"], "t1")
        self.assertAlmostEqual(rows[0]["EventMinutes"], 150.0)
        self.assertFalse(rows[0]["IsEventCompleted"])

    def test_ignores_temperature_readings_outside_schedule(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "out-time",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
                self._row(
                    "out-day",
                    "temp1",
                    datetime(2026, 3, 29, 12, 30),
                    51.0,
                    device_type="Camara fria",
                    sensor_type="Temperatura F",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "12:00:00",
                    "EndTime": "13:00:00",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 14:00:00")

        self.assertEqual(result.collect(), [])

    def test_output_columns_match_sql_table_schema(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertEqual(
            result.columns,
            [
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
            ],
        )


if __name__ == "__main__":
    unittest.main()
