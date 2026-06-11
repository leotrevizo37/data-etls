import sys
import unittest
import warnings
from datetime import datetime
from importlib import import_module
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

sensor_events = import_module(
    "valledelencino.defs.transformations.sensor_events"
).sensor_events


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

    def _row(
        self,
        read_id: str,
        sensor_id: str,
        local_time: datetime,
        value: float,
        device_type: str = "Puerta",
        sensor_type: str = "Estado",
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
            "LocalTimeSpan": local_time,
            "Value": value,
        }

    def _event_tuples(self, result: DataFrame) -> list[tuple[str, float, float, bool]]:
        return [
            (
                row["ReadId"],
                float(row["Value"]),
                round(float(row["EventMinutes"]), 6),
                bool(row["IsEventCompleted"]),
            )
            for row in result.orderBy("SensorId", "LocalTimeSpan").collect()
        ]

    def test_returns_empty_dataframe_when_no_door_state_readings(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    device_type="Gabinete",
                    sensor_type="Temperatura",
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertEqual(result.count(), 0)

    def test_detects_state_changes_for_door(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 5), 0.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 10, 10), 1.0),
                self._row("r4", "s1", datetime(2026, 3, 29, 10, 15), 1.0),
                self._row("r5", "s1", datetime(2026, 3, 29, 10, 20), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual([row["ReadId"] for row in rows], ["r1", "r3", "r5"])
        self.assertEqual([row["Value"] for row in rows], [0.0, 1.0, 0.0])
        self.assertAlmostEqual(rows[0]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[1]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[2]["EventMinutes"], 100.0)
        self.assertFalse(rows[2]["IsEventCompleted"])

    def test_calculates_event_minutes_for_open_then_close(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 30), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 30.0)
        self.assertTrue(rows[0]["IsEventCompleted"])
        self.assertAlmostEqual(rows[1]["EventMinutes"], 90.0)
        self.assertFalse(rows[1]["IsEventCompleted"])

    def test_unclosed_open_event_uses_run_end(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    device_type="Cava",
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 60.0)
        self.assertFalse(rows[0]["IsEventCompleted"])

    def test_multiple_sensors_compute_independently(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 10), 0.0),
                self._row("r3", "s2", datetime(2026, 3, 29, 10, 5), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("SensorId", "LocalTimeSpan").collect()
        s1_rows = [row for row in rows if row["SensorId"] == "s1"]
        s2_rows = [row for row in rows if row["SensorId"] == "s2"]
        self.assertEqual(len(s1_rows), 2)
        self.assertEqual(len(s2_rows), 1)
        self.assertAlmostEqual(s1_rows[0]["EventMinutes"], 10.0)
        self.assertAlmostEqual(s1_rows[1]["EventMinutes"], 110.0)
        self.assertFalse(s1_rows[1]["IsEventCompleted"])
        self.assertAlmostEqual(s2_rows[0]["EventMinutes"], 115.0)
        self.assertFalse(s2_rows[0]["IsEventCompleted"])

    def test_long_sequence_fills_minutes_for_every_state_change(self) -> None:
        df = self._readings_df(
            [
                self._row("r00", "s1", datetime(2026, 3, 29, 8, 0), 0.0),
                self._row("r01", "s1", datetime(2026, 3, 29, 8, 5), 0.0),
                self._row("r02", "s1", datetime(2026, 3, 29, 8, 15), 1.0),
                self._row("r03", "s1", datetime(2026, 3, 29, 8, 20), 1.0),
                self._row("r04", "s1", datetime(2026, 3, 29, 8, 45), 0.0),
                self._row("r05", "s1", datetime(2026, 3, 29, 9, 10), 1.0),
                self._row("r06", "s1", datetime(2026, 3, 29, 9, 25), 0.0),
                self._row("r07", "s1", datetime(2026, 3, 29, 9, 50), 0.0),
                self._row("r08", "s1", datetime(2026, 3, 29, 10, 10), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("r00", 0.0, 15.0, True),
                ("r02", 1.0, 30.0, True),
                ("r04", 0.0, 25.0, True),
                ("r05", 1.0, 15.0, True),
                ("r06", 0.0, 45.0, True),
                ("r08", 1.0, 50.0, False),
            ],
        )

    def test_interleaved_sensors_keep_independent_timelines(self) -> None:
        df = self._readings_df(
            [
                self._row("s1-r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row("s2-r1", "s2", datetime(2026, 3, 29, 10, 5), 1.0),
                self._row("s1-r2", "s1", datetime(2026, 3, 29, 10, 20), 1.0),
                self._row("s2-r2", "s2", datetime(2026, 3, 29, 10, 25), 0.0),
                self._row("s1-r3", "s1", datetime(2026, 3, 29, 10, 30), 0.0),
                self._row("s2-r3", "s2", datetime(2026, 3, 29, 10, 50), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("s1-r1", 0.0, 20.0, True),
                ("s1-r2", 1.0, 10.0, True),
                ("s1-r3", 0.0, 30.0, False),
                ("s2-r1", 1.0, 20.0, True),
                ("s2-r2", 0.0, 25.0, True),
                ("s2-r3", 1.0, 10.0, False),
            ],
        )

    def test_ignores_non_estado_non_door_and_invalid_values(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row(
                    "noise-temp",
                    "s1",
                    datetime(2026, 3, 29, 10, 5),
                    1.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "noise-device",
                    "s2",
                    datetime(2026, 3, 29, 10, 10),
                    1.0,
                    device_type="Gabinete",
                ),
                self._row("noise-value", "s1", datetime(2026, 3, 29, 10, 15), 2.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 20), 1.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 10, 35), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("r1", 0.0, 20.0, True),
                ("r2", 1.0, 15.0, True),
                ("r3", 0.0, 25.0, False),
            ],
        )

    def test_output_columns_match_sql_table_schema(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
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
