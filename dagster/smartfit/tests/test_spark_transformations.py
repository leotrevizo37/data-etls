import sys
import unittest
import warnings
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from smartfit.defs.transformations.lecturas_time_hours import (  # noqa: E402
    lecturas_time_hours,
)
from smartfit.defs.transformations.sensor_expected_values import (  # noqa: E402
    build_sensor_expected_values_from_dataframes,
)
from smartfit.defs.transformations.stats_calculator import (  # noqa: E402
    stats_calculator,
)
from smartfit.defs.transformations.stats_calculator_anomalies import (  # noqa: E402
    stats_calculator_anomalies,
)
from smartfit.defs.transformations.termostatos import termostatos  # noqa: E402
from smartfit.operations_filters import (  # noqa: E402
    limit_operations_rows_to_closed_hours,
)

warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("smartfit-spark-transformations-tests")
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

    def test_lecturas_time_hours_builds_an_inclusive_hourly_scaffold(self) -> None:
        base = self.spark.createDataFrame(
            [
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100},
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100},
                {"SensorId": 11, "DeviceId": 1, "SensorTyId": 101},
            ]
        )

        result = lecturas_time_hours(
            base,
            "2026-03-29 10:00:00",
            "2026-03-29 12:00:00",
        )

        rows = result.orderBy("SensorId", "LocalTimeSpan").collect()

        self.assertIsNotNone(result)
        self.assertEqual(len(rows), 6)
        self.assertEqual(
            [row["LocalTimeSpan"] for row in rows[:3]],
            [
                datetime(2026, 3, 29, 10, 0),
                datetime(2026, 3, 29, 11, 0),
                datetime(2026, 3, 29, 12, 0),
            ],
        )

    def test_lecturas_time_hours_returns_none_for_empty_base(self) -> None:
        empty_schema = StructType(
            [
                StructField("SensorId", IntegerType(), True),
                StructField("DeviceId", IntegerType(), True),
                StructField("SensorTyId", IntegerType(), True),
            ]
        )
        base = self.spark.createDataFrame([], schema=empty_schema)

        result = lecturas_time_hours(
            base,
            "2026-03-29 10:00:00",
            "2026-03-29 12:00:00",
        )

        self.assertIsNone(result)

    def test_stats_calculator_aggregates_hourly_readings_and_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 3),
                    "Value": 21.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 18),
                    "Value": 24.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 43),
                    "Value": 10.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 55),
                    "Value": 999.0,
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
            ok_ratio=0.6,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 1)
        self.assertEqual(row["StatusCode"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 55 / 3)
        self.assertAlmostEqual(row["Promedio"], 2 / 3)

    def test_stats_calculator_uses_upper_bound_for_status_and_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 25.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 110.0,
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
            max_expected_value=100,
            ok_ratio=0.75,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertEqual(row["StatusCode"], 3)
        self.assertAlmostEqual(row["MeasurementValue"], 67.5)
        self.assertAlmostEqual(row["Promedio"], 0.5)

    def test_stats_calculator_returns_none_when_sensor_type_is_absent(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 5.0,
                }
            ]
        )

        result = stats_calculator(base, sensor_type="Presion", min_expected_value=20)

        self.assertIsNone(result)

    def test_stats_calculator_anomalies_counts_bad_windows_within_the_hour(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                },
            ]
        )

        result = stats_calculator_anomalies(
            base,
            sensor_type="Corriente",
            min_expected_value=0.5,
            time_window=20,
            ok_ratio=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 2)
        self.assertEqual(row["StatusCode"], 3)
        self.assertAlmostEqual(row["MeasurementValue"], 1 / 3)

    def test_stats_calculator_anomalies_treats_missing_windows_as_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                },
            ]
        )

        result = stats_calculator_anomalies(
            base,
            sensor_type="Corriente",
            min_expected_value=0.5,
            time_window=20,
            ok_ratio=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertEqual(row["StatusCode"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_termostatos_accepts_mixed_case_hvac_prefixes(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "a/C rooftop",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 1.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "a/C rooftop",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 1.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C rooftop",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 24.0,
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C rooftop",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 22.0,
                },
                {
                    "DeviceId": 2,
                    "SensorId": 20,
                    "LocationId": 101,
                    "SensorTyId": 300,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "AMBIENTE lounge",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 1.0,
                },
                {
                    "DeviceId": 2,
                    "SensorId": 20,
                    "LocationId": 101,
                    "SensorTyId": 300,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "AMBIENTE lounge",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 1.0,
                },
                {
                    "DeviceId": 2,
                    "SensorId": 21,
                    "LocationId": 101,
                    "SensorTyId": 301,
                    "sensor_type": "Temperatura C",
                    "device_type": "AMBIENTE lounge",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 21.0,
                },
                {
                    "DeviceId": 2,
                    "SensorId": 21,
                    "LocationId": 101,
                    "SensorTyId": 301,
                    "sensor_type": "Temperatura C",
                    "device_type": "AMBIENTE lounge",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 23.0,
                },
            ]
        )
        dimensions_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "a/C rooftop",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C rooftop",
                },
                {
                    "DeviceId": 2,
                    "SensorId": 20,
                    "LocationId": 101,
                    "SensorTyId": 300,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "AMBIENTE lounge",
                },
                {
                    "DeviceId": 2,
                    "SensorId": 21,
                    "LocationId": 101,
                    "SensorTyId": 301,
                    "sensor_type": "Temperatura C",
                    "device_type": "AMBIENTE lounge",
                },
            ]
        )

        result = termostatos(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.orderBy("DeviceId", "SensorId").collect()

        self.assertEqual(len(rows), 4)
        self.assertEqual({row["DeviceId"] for row in rows}, {1, 2})
        self.assertEqual({row["SensorId"] for row in rows}, {10, 11, 20, 21})
        self.assertEqual(
            {row["LocalTimeSpan"] for row in rows},
            {datetime(2026, 3, 29, 10, 0)},
        )
        rows_by_sensor = {row["SensorId"]: row for row in rows}
        self.assertEqual(rows_by_sensor[10]["StatusCode"], 1)
        self.assertEqual(rows_by_sensor[11]["StatusCode"], 1)
        self.assertEqual(rows_by_sensor[20]["StatusCode"], 1)
        self.assertEqual(rows_by_sensor[21]["StatusCode"], 3)
        self.assertEqual(len({row["OperationId"] for row in rows}), 2)

    def test_termostatos_scaffolds_missing_sensor_type_stats(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C rooftop",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 23.0,
                },
            ]
        )
        dimensions_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "On/Off Termostato",
                    "device_type": "a/C rooftop",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C rooftop",
                },
            ]
        )

        result = termostatos(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        result_rows = result.orderBy("SensorId").collect()
        rows = {row["SensorId"]: row for row in result_rows}

        self.assertEqual(len(result_rows), 2)
        self.assertEqual(set(rows), {10, 11})
        self.assertEqual(rows[10]["StatusCode"], 3)
        self.assertEqual(rows[10]["ReadingsCount"], 0)
        self.assertIsNone(rows[10]["MeasurementValue"])
        self.assertEqual(rows[11]["StatusCode"], 3)
        self.assertEqual(rows[11]["ReadingsCount"], 1)
        self.assertAlmostEqual(rows[11]["MeasurementValue"], 23.0)
        self.assertEqual(len({row["OperationId"] for row in rows.values()}), 1)

    def test_build_sensor_expected_values_generates_hvac_and_light_rows(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "sensor-temp",
                    "DeviceId": "device-hvac",
                    "sensor_type": "Temperatura C",
                    "device_type": "a/C de Cocina principal",
                    "Active": True,
                },
                {
                    "SensorId": "sensor-switch",
                    "DeviceId": "device-hvac",
                    "sensor_type": "On/Off Termostato",
                    "device_type": "a/C de Cocina principal",
                    "Active": True,
                },
                {
                    "SensorId": "sensor-light",
                    "DeviceId": "device-light",
                    "sensor_type": "Encendido",
                    "device_type": "Luminaria",
                    "Active": True,
                },
                {
                    "SensorId": "sensor-ignored",
                    "DeviceId": "device-other",
                    "sensor_type": "Temperatura",
                    "device_type": "Congelador",
                    "Active": True,
                },
            ]
        )
        program_profiles_df = self.spark.createDataFrame(
            [
                {
                    "ProgramProfileId": "profile-on",
                    "ProgramProfileTyId": "pty-on",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 20.0,
                    "TemperatureGrace": 1.5,
                    "Active": True,
                },
                {
                    "ProgramProfileId": "profile-off",
                    "ProgramProfileTyId": "pty-off",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 24.0,
                    "TemperatureGrace": 1.0,
                    "Active": True,
                },
            ]
        )
        program_profile_tys_df = self.spark.createDataFrame(
            [
                {
                    "ProgramProfileTyId": "pty-on",
                    "Name": "Wake",
                    "Active": True,
                },
                {
                    "ProgramProfileTyId": "pty-off",
                    "Name": "off",
                    "Active": True,
                },
            ]
        )
        program_schedules_df = self.spark.createDataFrame(
            [
                {
                    "ProgramProfileId": "profile-on",
                    "ScheduleCode": "A",
                    "Days": "Mon",
                    "Time": "08:00",
                    "Active": True,
                },
                {
                    "ProgramProfileId": "profile-off",
                    "ScheduleCode": "B",
                    "Days": "Mon",
                    "Time": "22:00",
                    "Active": True,
                },
                {
                    "ProgramProfileId": "profile-on",
                    "ScheduleCode": "C",
                    "Days": "Mon",
                    "Time": "12:00",
                    "Active": False,
                },
            ]
        )
        group_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "Active": True},
            ]
        )
        group_has_schedules_df = self.spark.createDataFrame(
            [
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 6 * * MON",
                    "Value": True,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 18 * * MON",
                    "Value": False,
                    "Active": True,
                },
            ]
        )
        group_has_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "SensorsSensorId": "sensor-light"},
            ]
        )

        result = build_sensor_expected_values_from_dataframes(
            dimensions_df=dimensions_df,
            program_profile_tys_df=program_profile_tys_df,
            program_profiles_df=program_profiles_df,
            program_schedules_df=program_schedules_df,
            group_sensors_df=group_sensors_df,
            group_has_schedules_df=group_has_schedules_df,
            group_has_sensors_df=group_has_sensors_df,
        )

        rows = {
            (
                row["SensorId"],
                row["StartTime"],
                row["EndTime"],
                row["scheduledDays"],
            ): row
            for row in result.collect()
        }

        self.assertEqual(result.count(), 9)
        self.assertEqual(
            rows[("sensor-temp", "08:00:00", "22:00:00", "1")]["min_expected_value"],
            Decimal("65.300"),
        )
        self.assertEqual(
            rows[("sensor-temp", "08:00:00", "22:00:00", "1")]["max_expected_value"],
            Decimal("70.700"),
        )
        self.assertIsNone(
            rows[("sensor-temp", "22:00:00", "23:59:00", "1")]["min_expected_value"]
        )
        self.assertIsNone(
            rows[("sensor-temp", "22:00:00", "23:59:00", "1")]["max_expected_value"]
        )
        self.assertIn(
            ("sensor-temp", "00:00:00", "07:59:00", "1"),
            rows,
        )
        self.assertNotIn(
            ("sensor-temp", "12:00:00", "22:00:00", "1"),
            rows,
        )
        self.assertEqual(
            rows[("sensor-switch", "22:00:00", "23:59:00", "1")]["min_expected_value"],
            Decimal("0.000"),
        )
        self.assertEqual(
            rows[("sensor-switch", "08:00:00", "22:00:00", "1")]["min_expected_value"],
            Decimal("1.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "00:00:00", "05:59:00", "1")]["max_expected_value"],
            Decimal("0.000"),
        )
        self.assertNotIn(
            ("sensor-ignored", "08:00:00", "22:00:00", "1"),
            rows,
        )

    def test_build_sensor_expected_values_merges_overlapping_hvac_day_sets(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "sensor-temp",
                    "DeviceId": "device-hvac",
                    "sensor_type": "Temperatura C",
                    "device_type": "A/C principal",
                    "Active": True,
                },
                {
                    "SensorId": "sensor-switch",
                    "DeviceId": "device-hvac",
                    "sensor_type": "On/Off Termostato",
                    "device_type": "A/C principal",
                    "Active": True,
                },
            ]
        )
        program_profiles_df = self.spark.createDataFrame(
            [
                {
                    "ProgramProfileId": "all-week-on",
                    "ProgramProfileTyId": "pty-on",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 20.0,
                    "TemperatureGrace": 1.0,
                    "Active": True,
                },
                {
                    "ProgramProfileId": "all-week-off",
                    "ProgramProfileTyId": "pty-off",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 24.0,
                    "TemperatureGrace": 1.0,
                    "Active": True,
                },
                {
                    "ProgramProfileId": "weekdays-on",
                    "ProgramProfileTyId": "pty-on",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 22.0,
                    "TemperatureGrace": 1.0,
                    "Active": True,
                },
                {
                    "ProgramProfileId": "weekdays-off",
                    "ProgramProfileTyId": "pty-off",
                    "DeviceId": "device-hvac",
                    "Mode": "00",
                    "Temperature": 24.0,
                    "TemperatureGrace": 1.0,
                    "Active": True,
                },
            ]
        )
        program_profile_tys_df = self.spark.createDataFrame(
            [
                {"ProgramProfileTyId": "pty-on", "Name": "Wake", "Active": True},
                {"ProgramProfileTyId": "pty-off", "Name": "off", "Active": True},
            ]
        )
        program_schedules_df = self.spark.createDataFrame(
            [
                {
                    "ProgramProfileId": "all-week-on",
                    "ScheduleCode": "A",
                    "Days": "*",
                    "Time": "05:00",
                    "Active": True,
                },
                {
                    "ProgramProfileId": "all-week-off",
                    "ScheduleCode": "B",
                    "Days": "*",
                    "Time": "11:00",
                    "Active": True,
                },
                {
                    "ProgramProfileId": "weekdays-on",
                    "ScheduleCode": "C",
                    "Days": "MON,TUE,WED,THU,FRI",
                    "Time": "07:00",
                    "Active": True,
                },
                {
                    "ProgramProfileId": "weekdays-off",
                    "ScheduleCode": "D",
                    "Days": "MON,TUE,WED,THU,FRI",
                    "Time": "20:00",
                    "Active": True,
                },
            ]
        )
        empty_group_sensors_df = self.spark.createDataFrame(
            [],
            "GroupSensorsId string, Active boolean",
        )
        empty_group_has_schedules_df = self.spark.createDataFrame(
            [],
            "GroupSensorsId string, Crone string, Value boolean, Active boolean",
        )
        empty_group_has_sensors_df = self.spark.createDataFrame(
            [],
            "GroupSensorsId string, SensorsSensorId string",
        )

        result = build_sensor_expected_values_from_dataframes(
            dimensions_df=dimensions_df,
            program_profile_tys_df=program_profile_tys_df,
            program_profiles_df=program_profiles_df,
            program_schedules_df=program_schedules_df,
            group_sensors_df=empty_group_sensors_df,
            group_has_schedules_df=empty_group_has_schedules_df,
            group_has_sensors_df=empty_group_has_sensors_df,
        )

        rows = {
            (
                row["SensorId"],
                row["StartTime"],
                row["EndTime"],
                row["scheduledDays"],
            ): row
            for row in result.collect()
        }

        self.assertIsNone(
            rows[("sensor-temp", "00:00:00", "04:59:00", "1-7")]["min_expected_value"]
        )
        self.assertEqual(
            rows[("sensor-temp", "05:00:00", "07:00:00", "1,2,3,4,5")]["min_expected_value"],
            Decimal("19.000"),
        )
        self.assertEqual(
            rows[("sensor-temp", "05:00:00", "07:00:00", "1,2,3,4,5")]["max_expected_value"],
            Decimal("21.000"),
        )
        self.assertEqual(
            rows[("sensor-temp", "07:00:00", "11:00:00", "1,2,3,4,5")]["min_expected_value"],
            Decimal("21.000"),
        )
        self.assertEqual(
            rows[("sensor-temp", "07:00:00", "11:00:00", "1,2,3,4,5")]["max_expected_value"],
            Decimal("23.000"),
        )
        self.assertEqual(
            rows[("sensor-temp", "05:00:00", "11:00:00", "6,7")]["min_expected_value"],
            Decimal("19.000"),
        )
        self.assertEqual(
            rows[("sensor-switch", "05:00:00", "07:00:00", "1,2,3,4,5")]["min_expected_value"],
            Decimal("1.000"),
        )
        self.assertEqual(
            rows[("sensor-switch", "11:00:00", "23:59:00", "6,7")]["min_expected_value"],
            Decimal("0.000"),
        )
        self.assertNotIn(
            ("sensor-temp", "05:00:00", "11:00:00", "1-7"),
            rows,
        )

    def test_build_sensor_expected_values_merges_overlapping_light_day_sets(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "sensor-light",
                    "DeviceId": "device-light",
                    "sensor_type": "Encendido",
                    "device_type": "Luminaria",
                    "Active": True,
                },
            ]
        )
        empty_profiles_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ProgramProfileTyId string, DeviceId string, Mode string, Temperature double, TemperatureGrace double, Active boolean",
        )
        empty_profile_tys_df = self.spark.createDataFrame(
            [],
            "ProgramProfileTyId string, Name string, Active boolean",
        )
        empty_program_schedules_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ScheduleCode string, Days string, Time string, Active boolean",
        )
        group_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "Active": True},
            ]
        )
        group_has_schedules_df = self.spark.createDataFrame(
            [
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 6 * * ?",
                    "Value": True,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 22 * * ?",
                    "Value": False,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 8 * * MON-FRI",
                    "Value": False,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 20 * * MON-FRI",
                    "Value": True,
                    "Active": True,
                },
            ]
        )
        group_has_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "SensorsSensorId": "sensor-light"},
            ]
        )

        result = build_sensor_expected_values_from_dataframes(
            dimensions_df=dimensions_df,
            program_profile_tys_df=empty_profile_tys_df,
            program_profiles_df=empty_profiles_df,
            program_schedules_df=empty_program_schedules_df,
            group_sensors_df=group_sensors_df,
            group_has_schedules_df=group_has_schedules_df,
            group_has_sensors_df=group_has_sensors_df,
        )

        rows = {
            (
                row["SensorId"],
                row["StartTime"],
                row["EndTime"],
                row["scheduledDays"],
            ): row
            for row in result.collect()
        }

        self.assertEqual(
            rows[("sensor-light", "00:00:00", "05:59:00", "1-7")]["max_expected_value"],
            Decimal("0.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "06:00:00", "08:00:00", "1,2,3,4,5")]["max_expected_value"],
            Decimal("1.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "08:00:00", "20:00:00", "1,2,3,4,5")]["max_expected_value"],
            Decimal("0.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "20:00:00", "22:00:00", "1,2,3,4,5")]["max_expected_value"],
            Decimal("1.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "06:00:00", "22:00:00", "6,7")]["max_expected_value"],
            Decimal("1.000"),
        )
        self.assertEqual(
            rows[("sensor-light", "22:00:00", "23:59:00", "1-7")]["max_expected_value"],
            Decimal("0.000"),
        )
        self.assertNotIn(
            ("sensor-light", "06:00:00", "22:00:00", "1-7"),
            rows,
        )

    def test_build_sensor_expected_values_maps_sunday_to_seven(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "sensor-light",
                    "DeviceId": "device-light",
                    "sensor_type": "Encendido",
                    "device_type": "Luminaria",
                    "Active": True,
                },
            ]
        )
        empty_profiles_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ProgramProfileTyId string, DeviceId string, Mode string, Temperature double, TemperatureGrace double, Active boolean",
        )
        empty_profile_tys_df = self.spark.createDataFrame(
            [],
            "ProgramProfileTyId string, Name string, Active boolean",
        )
        empty_program_schedules_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ScheduleCode string, Days string, Time string, Active boolean",
        )
        group_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "Active": True},
            ]
        )
        group_has_schedules_df = self.spark.createDataFrame(
            [
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 9 * * SUN",
                    "Value": True,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 21 * * SUN",
                    "Value": False,
                    "Active": True,
                },
            ]
        )
        group_has_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "SensorsSensorId": "sensor-light"},
            ]
        )

        result = build_sensor_expected_values_from_dataframes(
            dimensions_df=dimensions_df,
            program_profile_tys_df=empty_profile_tys_df,
            program_profiles_df=empty_profiles_df,
            program_schedules_df=empty_program_schedules_df,
            group_sensors_df=group_sensors_df,
            group_has_schedules_df=group_has_schedules_df,
            group_has_sensors_df=group_has_sensors_df,
        )

        self.assertEqual({row["scheduledDays"] for row in result.collect()}, {"7"})

    def test_build_sensor_expected_values_normalizes_wildcard_days(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "sensor-light",
                    "DeviceId": "device-light",
                    "sensor_type": "Encendido",
                    "device_type": "Luminaria",
                    "Active": True,
                },
            ]
        )
        empty_profiles_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ProgramProfileTyId string, DeviceId string, Mode string, Temperature double, TemperatureGrace double, Active boolean",
        )
        empty_profile_tys_df = self.spark.createDataFrame(
            [],
            "ProgramProfileTyId string, Name string, Active boolean",
        )
        empty_program_schedules_df = self.spark.createDataFrame(
            [],
            "ProgramProfileId string, ScheduleCode string, Days string, Time string, Active boolean",
        )
        group_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "Active": True},
            ]
        )
        group_has_schedules_df = self.spark.createDataFrame(
            [
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "30 9 * * ?",
                    "Value": True,
                    "Active": True,
                },
                {
                    "GroupSensorsId": "group-1",
                    "Crone": "0 21 * * ?",
                    "Value": False,
                    "Active": True,
                },
            ]
        )
        group_has_sensors_df = self.spark.createDataFrame(
            [
                {"GroupSensorsId": "group-1", "SensorsSensorId": "sensor-light"},
            ]
        )

        result = build_sensor_expected_values_from_dataframes(
            dimensions_df=dimensions_df,
            program_profile_tys_df=empty_profile_tys_df,
            program_profiles_df=empty_profiles_df,
            program_schedules_df=empty_program_schedules_df,
            group_sensors_df=group_sensors_df,
            group_has_schedules_df=group_has_schedules_df,
            group_has_sensors_df=group_has_sensors_df,
        )

        rows = result.orderBy("StartTime").collect()

        self.assertEqual(len(rows), 3)
        self.assertEqual({row["scheduledDays"] for row in rows}, {"1-7"})
        self.assertEqual(rows[0]["StartTime"], "00:00:00")
        self.assertEqual(rows[0]["EndTime"], "09:29:00")
        self.assertEqual(rows[-1]["StartTime"], "21:00:00")

    def test_limit_operations_rows_to_closed_hours_removes_future_rows(self) -> None:
        schema = StructType(
            [
                StructField("DeviceId", IntegerType(), True),
                StructField("SensorId", IntegerType(), True),
                StructField("LocalTimeSpan", TimestampType(), True),
                StructField("sensor_type", StringType(), True),
                StructField("MeasurementValue", DoubleType(), True),
            ]
        )
        base = self.spark.createDataFrame(
            [
                (1, 10, datetime(2026, 3, 29, 10, 0), "Corriente", 1.0),
                (1, 10, datetime(2026, 3, 29, 11, 0), "Corriente", 2.0),
            ],
            schema=schema,
        )

        result = limit_operations_rows_to_closed_hours(
            base,
            datetime(2026, 3, 29, 10, 0),
        )

        rows = result.orderBy("LocalTimeSpan").collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))


if __name__ == "__main__":
    unittest.main()
