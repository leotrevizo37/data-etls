import sys
import unittest
import warnings
from datetime import datetime
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

from carlsjr.defs.transformations.lecturas_time_hours import (  # noqa: E402
    lecturas_time_hours,
)
from carlsjr.defs.transformations.cisterna import cisterna  # noqa: E402
from carlsjr.defs.transformations.cuartos_refrigeracion import (  # noqa: E402
    cuartos_refrigeracion,
)
from carlsjr.defs.transformations.presencia import presencia  # noqa: E402
from carlsjr.defs.transformations.stats_calculator import (  # noqa: E402
    stats_calculator,
    with_in_range_read_flag,
)
from carlsjr.operations_filters import (  # noqa: E402
    limit_operations_rows_to_closed_hours,
)


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("carlsjr-spark-transformations-tests")
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
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_type": 1, "device_type": 1, "LocationId": 11},
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_type": 1, "device_type": 1, "LocationId": 11},
                {"SensorId": 11, "DeviceId": 1, "SensorTyId": 101, "sensor_type": 2, "device_type": 1, "LocationId": 11},
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
                StructField("device_type", IntegerType(), True),
                StructField("sensor_type", IntegerType(), True),
                StructField("LocationId", IntegerType(), True),
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
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 18),
                    "Value": 24.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 43),
                    "Value": 10.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 55),
                    "Value": 999.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["InRangeReads"], 3)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 55 / 3)

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
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 110.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
            max_expected_value=100,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 67.5)

    def test_in_range_read_flag_respects_schedules_and_overlapping_ranges(self) -> None:
        base = self._readings_df(
            [
                {
                    "ReadId": "in-low",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 6, 10, 0),
                    "Value": 5.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "ReadId": "in-overlap",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 6, 10, 10),
                    "Value": 25.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "ReadId": "out-overlap",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 6, 10, 20),
                    "Value": 15.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "ReadId": "no-hour-schedule",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 6, 12, 0),
                    "Value": 100.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "ReadId": "no-day-schedule",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                    "Value": 15.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "ReadId": "null-value",
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 7, 6, 10, 30),
                    "Value": None,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )
        expected_values = self.spark.createDataFrame(
            [
                {
                    "SensorId": 10,
                    "DeviceId": 1,
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "09:00:00",
                    "EndTime": "11:00:00",
                    "scheduledDays": "1-1",
                },
                {
                    "SensorId": 10,
                    "DeviceId": 1,
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "09:00:00",
                    "EndTime": "11:00:00",
                    "scheduledDays": "1-1",
                },
            ],
            schema=StructType(
                [
                    StructField("SensorId", IntegerType(), True),
                    StructField("DeviceId", IntegerType(), True),
                    StructField("min_expected_value", DoubleType(), True),
                    StructField("max_expected_value", DoubleType(), True),
                    StructField("StartTime", StringType(), True),
                    StructField("EndTime", StringType(), True),
                    StructField("scheduledDays", StringType(), True),
                ]
            ),
        )

        flagged = with_in_range_read_flag(base, expected_values)
        flags = {
            row["ReadId"]: row["InRangeReadFlag"]
            for row in flagged.select("ReadId", "InRangeReadFlag").collect()
        }
        stats_rows = {
            row["LocalTimeSpan"]: row
            for row in stats_calculator(flagged, sensor_type="Presion").collect()
        }

        self.assertEqual(
            flags,
            {
                "in-low": 1,
                "in-overlap": 1,
                "out-overlap": 0,
                "no-hour-schedule": 1,
                "no-day-schedule": 1,
                "null-value": 0,
            },
        )
        self.assertEqual(stats_rows[datetime(2026, 7, 6, 10, 0)]["ReadingsCount"], 4)
        self.assertEqual(stats_rows[datetime(2026, 7, 6, 10, 0)]["InRangeReads"], 2)

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
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
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
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Corriente",
            min_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 1)
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
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Corriente",
            max_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_cisterna_uses_carlsjr_sensor_name_from_dimensions(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 55.0,
                    "InRangeReadFlag": 1,
                    "device_name": "Cisterna 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 65.0,
                    "InRangeReadFlag": 0,
                    "device_name": "Cisterna 1",
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
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "device_name": "Cisterna 1",
                },
            ]
        )

        result = cisterna(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadingsCount"], 2)
        self.assertEqual(rows[0]["InRangeReads"], 1)
        self.assertAlmostEqual(rows[0]["MeasurementValue"], 60.0)

    def test_cuartos_refrigeracion_uses_carlsjr_temperature_f_sensor(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 35.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 50.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
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
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "device_name": "C\u00e1mara fr\u00eda 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "device_name": "C\u00e1mara fr\u00eda 1",
                },
            ]
        )

        result = cuartos_refrigeracion(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows_by_sensor = {
            row["sensor_type"]: row
            for row in result.collect()
        }

        self.assertEqual(set(rows_by_sensor), {"Temperatura F", "Humedad"})
        self.assertEqual(rows_by_sensor["Temperatura F"]["ReadingsCount"], 1)
        self.assertEqual(rows_by_sensor["Humedad"]["ReadingsCount"], 1)
        self.assertAlmostEqual(
            rows_by_sensor["Temperatura F"]["MeasurementValue"],
            35.0,
        )
        self.assertAlmostEqual(rows_by_sensor["Humedad"]["MeasurementValue"], 50.0)

    def test_presencia_uses_carlsjr_sensor_name_from_dimensions(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Camara de presencia",
                    "device_type": "Monitoreo camara",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 1.0,
                    "device_name": "Monitoreo 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Camara de presencia",
                    "device_type": "Monitoreo camara",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 0.0,
                    "device_name": "Monitoreo 1",
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
                    "sensor_type": "Camara de presencia",
                    "device_type": "Monitoreo camara",
                    "device_name": "Monitoreo 1",
                },
            ]
        )

        result = presencia(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadingsCount"], 2)
        self.assertAlmostEqual(rows[0]["MeasurementValue"], 0.5)

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

    def test_factreadingsmeasurement_uses_stage_merge_without_window_cleanup(self) -> None:
        operations_path = PROJECT_SRC / "carlsjr" / "defs" / "assets" / "operations_assets.py"
        source = operations_path.read_text(encoding="utf-8")
        write_section = source.split("def write_factreadingsmeasurement_op", 1)[1].split("@op(", 1)[0]
        graph_section = source.split("def operations_assets", 1)[1]

        self.assertIn("write_fact_operations_stage(", write_section)
        self.assertIn('"InRangeReads"', write_section)
        self.assertIn("merge_fact_readings_measurement(", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("window_cleanup(fetch_from_ts)", graph_section)


if __name__ == "__main__":
    unittest.main()
