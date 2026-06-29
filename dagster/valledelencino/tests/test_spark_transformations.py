import ast
import sys
import unittest
import warnings
from datetime import datetime, timedelta
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

from valledelencino.defs.transformations.cava import cava  # noqa: E402
from valledelencino.defs.transformations.chiller import chiller  # noqa: E402
from valledelencino.defs.transformations.cisterna import cisterna  # noqa: E402
from valledelencino.defs.transformations.corriente import corriente  # noqa: E402
from valledelencino.defs.transformations.destilados import destilados  # noqa: E402
from valledelencino.defs.transformations.lecturas_time_hours import (  # noqa: E402
    lecturas_time_hours,
)
from valledelencino.defs.transformations.gabinete import gabinete  # noqa: E402
from valledelencino.defs.transformations.puerta import puerta  # noqa: E402
from valledelencino.defs.transformations.stats_calculator import (  # noqa: E402
    stats_calculator,
)
from valledelencino.defs.transformations.tanque import tanque  # noqa: E402
from valledelencino.operations_filters import (  # noqa: E402
    limit_operations_rows_to_closed_hours,
)


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    DEVICE_TRANSFORMATION_CASES = [
        (
            "cava",
            cava,
            "Cava",
            {
                "Temperatura": [16.0, 18.0],
                "Humedad": [55.0, 57.0],
                "Estado": [1.0, 0.0],
            },
            "Nivel",
        ),
        (
            "chiller",
            chiller,
            "Chiller (Compresor)",
            {"Corriente": [6.0, 8.0]},
            "Temperatura",
        ),
        (
            "cisterna",
            cisterna,
            "Cisterna",
            {"Nivel": [62.0, 64.0]},
            "Temperatura",
        ),
        (
            "corriente",
            corriente,
            "Corriente General",
            {"Corriente": [10.0, 12.0]},
            "Humedad",
        ),
        (
            "destilados",
            destilados,
            "Cuarto destilados",
            {
                "Temperatura": [20.0, 22.0],
                "Humedad": [48.0, 50.0],
            },
            "Estado",
        ),
        (
            "puerta",
            puerta,
            "Puerta",
            {
                "Temperatura": [21.0, 23.0],
                "Humedad": [45.0, 47.0],
                "Estado": [1.0, 0.0],
            },
            "Nivel",
        ),
        (
            "tanque",
            tanque,
            "Tanque vino",
            {"Temperatura": [19.0, 21.0]},
            "Humedad",
        ),
    ]

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("gabinete-spark-transformations-tests")
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

    def _device_transform_inputs(
        self,
        method_name: str,
        device_type: str,
        sensor_values: dict[str, list[float]],
        unsupported_sensor_type: str,
    ) -> tuple[DataFrame, DataFrame]:
        reading_rows = []
        dimension_rows = []
        device_id = method_name + "-device"
        location_id = method_name + "-location"
        device_name = method_name + "-device-name"

        for idx, (sensor_type, values) in enumerate(sensor_values.items(), start=1):
            sensor_id = method_name + "-sensor-" + str(idx)
            sensor_ty_id = method_name + "-sensor-ty-" + str(idx)
            dimension_rows.append(
                {
                    "DeviceId": device_id,
                    "SensorId": sensor_id,
                    "LocationId": location_id,
                    "SensorTyId": sensor_ty_id,
                    "sensor_type": sensor_type,
                    "device_type": device_type,
                    "device_name": device_name,
                }
            )
            for read_idx, value in enumerate(values):
                reading_rows.append(
                    {
                        "ReadId": method_name + "-read-" + str(idx) + "-" + str(read_idx),
                        "DeviceId": device_id,
                        "SensorId": sensor_id,
                        "LocationId": location_id,
                        "SensorTyId": sensor_ty_id,
                        "sensor_type": sensor_type,
                        "device_type": device_type,
                        "device_name": device_name,
                        "LocalTimeSpan": datetime(2026, 3, 29, 10, 5 + read_idx * 20),
                        "Value": float(value),
                    }
                )

        unsupported_sensor_id = method_name + "-unsupported-sensor"
        unsupported_sensor_ty_id = method_name + "-unsupported-sensor-ty"
        dimension_rows.append(
            {
                "DeviceId": device_id,
                "SensorId": unsupported_sensor_id,
                "LocationId": location_id,
                "SensorTyId": unsupported_sensor_ty_id,
                "sensor_type": unsupported_sensor_type,
                "device_type": device_type,
                "device_name": device_name,
            }
        )
        reading_rows.append(
            {
                "ReadId": method_name + "-unsupported-read",
                "DeviceId": device_id,
                "SensorId": unsupported_sensor_id,
                "LocationId": location_id,
                "SensorTyId": unsupported_sensor_ty_id,
                "sensor_type": unsupported_sensor_type,
                "device_type": device_type,
                "device_name": device_name,
                "LocalTimeSpan": datetime(2026, 3, 29, 10, 15),
                "Value": 999.0,
            }
        )

        other_device_id = method_name + "-other-device"
        dimension_rows.append(
            {
                "DeviceId": other_device_id,
                "SensorId": method_name + "-other-sensor",
                "LocationId": location_id,
                "SensorTyId": method_name + "-other-sensor-ty",
                "sensor_type": next(iter(sensor_values)),
                "device_type": "Unrelated device",
                "device_name": "unrelated",
            }
        )
        reading_rows.append(
            {
                "ReadId": method_name + "-other-read",
                "DeviceId": other_device_id,
                "SensorId": method_name + "-other-sensor",
                "LocationId": location_id,
                "SensorTyId": method_name + "-other-sensor-ty",
                "sensor_type": next(iter(sensor_values)),
                "device_type": "Unrelated device",
                "device_name": "unrelated",
                "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                "Value": 123.0,
            }
        )

        return self._readings_df(reading_rows), self._readings_df(dimension_rows)

    def _all_device_transform_inputs(
        self,
    ) -> tuple[
        DataFrame,
        DataFrame,
        list[datetime],
        dict[str, dict[tuple[str, datetime], list[float]]],
    ]:
        hour_starts = [
            datetime(2026, 3, 29, 10, 0),
            datetime(2026, 3, 29, 11, 0),
        ]
        reading_rows = []
        dimension_rows = []
        expected_by_method = {}

        for (
            method_name,
            _transform_fn,
            device_type,
            sensor_values,
            _unsupported_sensor_type,
        ) in self.DEVICE_TRANSFORMATION_CASES:
            device_id = method_name + "-full-device"
            location_id = method_name + "-full-location"
            device_name = method_name + "-full-device-name"
            expected_by_method[method_name] = {}

            for sensor_idx, (sensor_type, base_values) in enumerate(
                sensor_values.items(),
                start=1,
            ):
                sensor_id = method_name + "-full-sensor-" + str(sensor_idx)
                sensor_ty_id = method_name + "-full-sensor-ty-" + str(sensor_idx)
                dimension_rows.append(
                    {
                        "DeviceId": device_id,
                        "SensorId": sensor_id,
                        "LocationId": location_id,
                        "SensorTyId": sensor_ty_id,
                        "sensor_type": sensor_type,
                        "device_type": device_type,
                        "device_name": device_name,
                    }
                )

                for hour_idx, hour_start in enumerate(hour_starts):
                    hourly_values = [
                        float(value + hour_idx)
                        for value in base_values
                    ]
                    expected_by_method[method_name][(sensor_id, hour_start)] = (
                        hourly_values
                    )

                    for read_idx, value in enumerate(hourly_values):
                        reading_rows.append(
                            {
                                "ReadId": (
                                    method_name
                                    + "-full-read-"
                                    + str(sensor_idx)
                                    + "-"
                                    + str(hour_idx)
                                    + "-"
                                    + str(read_idx)
                                ),
                                "DeviceId": device_id,
                                "SensorId": sensor_id,
                                "LocationId": location_id,
                                "SensorTyId": sensor_ty_id,
                                "sensor_type": sensor_type,
                                "device_type": device_type,
                                "device_name": device_name,
                                "LocalTimeSpan": hour_start
                                + timedelta(minutes=5 + read_idx * 20),
                                "Value": value,
                            }
                        )

        return (
            self._readings_df(reading_rows),
            self._readings_df(dimension_rows),
            hour_starts,
            expected_by_method,
        )

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
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 3),
                    "Value": 21.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 18),
                    "Value": 24.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 43),
                    "Value": 10.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 55),
                    "Value": 999.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Temperatura",
            min_expected_value=20,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
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
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 25.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 110.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Temperatura",
            min_expected_value=20,
            max_expected_value=100,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 67.5)

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
                    "device_type": "Maquina nieve",
                    "device_name": "Gabinete 1",
                }
            ]
        )

        result = stats_calculator(base, sensor_type="Humedad", min_expected_value=20)

        self.assertIsNone(result)

    def test_stats_calculator_anomalies_counts_bad_windows_within_the_hour(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Humedad",
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
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Humedad",
            max_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_device_transformations_materialize_supported_sensor_readings(self) -> None:
        for (
            method_name,
            transform_fn,
            device_type,
            sensor_values,
            unsupported_sensor_type,
        ) in self.DEVICE_TRANSFORMATION_CASES:
            with self.subTest(method=method_name):
                readings_df, dimensions_df = self._device_transform_inputs(
                    method_name,
                    device_type,
                    sensor_values,
                    unsupported_sensor_type,
                )

                result = transform_fn(
                    readings_df,
                    dimensions_df,
                    "2026-03-29 10:00:00",
                    "2026-03-29 10:00:00",
                )

                self.assertIsNotNone(result)
                rows = result.orderBy("sensor_type").collect()
                rows_by_sensor_type = {row["sensor_type"]: row for row in rows}

                self.assertEqual(set(rows_by_sensor_type), set(sensor_values))
                self.assertNotIn(unsupported_sensor_type, rows_by_sensor_type)
                self.assertEqual({row["device_type"] for row in rows}, {device_type})
                self.assertEqual(
                    {row["LocalTimeSpan"] for row in rows},
                    {datetime(2026, 3, 29, 10, 0)},
                )
                self.assertEqual(len({row["OperationId"] for row in rows}), len(rows))

                for sensor_type, values in sensor_values.items():
                    row = rows_by_sensor_type[sensor_type]
                    self.assertIsNotNone(row["MeasurementValue"])
                    self.assertIsNotNone(row["MeasurementStdDev"])
                    self.assertEqual(row["ReadingsCount"], len(values))
                    self.assertEqual(row["Anomalies"], 0)
                    self.assertAlmostEqual(
                        row["MeasurementValue"],
                        sum(values) / len(values),
                    )

    def test_device_transformations_emit_one_row_per_sensor_per_hour(self) -> None:
        (
            readings_df,
            dimensions_df,
            hour_starts,
            expected_by_method,
        ) = self._all_device_transform_inputs()

        for (
            method_name,
            transform_fn,
            _device_type,
            _sensor_values,
            _unsupported_sensor_type,
        ) in self.DEVICE_TRANSFORMATION_CASES:
            with self.subTest(method=method_name):
                result = transform_fn(
                    readings_df,
                    dimensions_df,
                    "2026-03-29 10:00:00",
                    "2026-03-29 11:00:00",
                )

                self.assertIsNotNone(result)
                rows = result.collect()
                rows_by_sensor_hour = {
                    (row["SensorId"], row["LocalTimeSpan"]): row
                    for row in rows
                }
                expected_rows = expected_by_method[method_name]

                self.assertEqual(len(rows_by_sensor_hour), len(rows))
                self.assertEqual(set(rows_by_sensor_hour), set(expected_rows))
                self.assertEqual(len(rows), len(expected_rows))

                sensor_ids = {
                    sensor_id
                    for sensor_id, _hour_start in expected_rows
                }
                for sensor_id in sensor_ids:
                    observed_hours = {
                        hour_start
                        for candidate_sensor_id, hour_start in rows_by_sensor_hour
                        if candidate_sensor_id == sensor_id
                    }
                    self.assertEqual(observed_hours, set(hour_starts))

                for key, expected_values in expected_rows.items():
                    row = rows_by_sensor_hour[key]
                    self.assertIsNotNone(row["MeasurementValue"])
                    self.assertIsNotNone(row["MeasurementStdDev"])
                    self.assertEqual(row["ReadingsCount"], len(expected_values))
                    self.assertEqual(row["Anomalies"], 0)
                    self.assertAlmostEqual(
                        row["MeasurementValue"],
                        sum(expected_values) / len(expected_values),
                    )

    def test_operations_device_transformations_are_wired_and_covered(self) -> None:
        operations_path = (
            PROJECT_SRC
            / "valledelencino"
            / "defs"
            / "assets"
            / "operations_assets.py"
        )
        transformation_ops_path = (
            PROJECT_SRC
            / "valledelencino"
            / "defs"
            / "resources"
            / "transformation_ops.py"
        )

        operations_tree = ast.parse(operations_path.read_text())
        methods = None
        for node in ast.walk(operations_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_devices_df_op":
                for child in ast.walk(node):
                    if not isinstance(child, ast.Assign):
                        continue
                    if not any(
                        isinstance(target, ast.Name) and target.id == "methods"
                        for target in child.targets
                    ):
                        continue
                    methods = [
                        item.value
                        for item in child.value.elts
                        if isinstance(item, ast.Constant)
                    ]
                    break

        self.assertIsNotNone(methods)
        covered_methods = [
            method_name
            for method_name, *_ in self.DEVICE_TRANSFORMATION_CASES
        ]
        self.assertEqual(methods, covered_methods)

        transformation_ops_tree = ast.parse(transformation_ops_path.read_text())
        wrapper_methods = set()
        for node in ast.walk(transformation_ops_tree):
            if isinstance(node, ast.ClassDef) and node.name == "TransformationOps":
                wrapper_methods = {
                    child.name
                    for child in node.body
                    if isinstance(child, ast.FunctionDef)
                }
                break

        self.assertTrue(wrapper_methods)
        self.assertTrue(set(methods).issubset(wrapper_methods))

    def test_gabinete_builds_one_operation_group_per_device_hour(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 0.5,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 0.6,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 21.0,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 22.0,
                    "device_name": "Gabinete 1",
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
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Máquina de nieve",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = gabinete(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.orderBy("SensorId").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["SensorId"] for row in rows}, {10, 11})
        self.assertEqual(
            {row["LocalTimeSpan"] for row in rows},
            {datetime(2026, 3, 29, 10, 0)},
        )
        self.assertEqual(len({row["OperationId"] for row in rows}), 2)
        self.assertEqual({row["LocationId"] for row in rows}, {100})
        self.assertEqual(
            {row["sensor_type"] for row in rows},
            {"Humedad"},
        )


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
                (1, 10, datetime(2026, 3, 29, 10, 0), "Humedad", 1.0),
                (1, 10, datetime(2026, 3, 29, 11, 0), "Humedad", 2.0),
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
        operations_path = PROJECT_SRC / "valledelencino" / "defs" / "assets" / "operations_assets.py"
        source = operations_path.read_text(encoding="utf-8")
        write_section = source.split("def write_factreadingsmeasurement_op", 1)[1].split("@op(", 1)[0]
        graph_section = source.split("def operations_assets", 1)[1]

        self.assertIn("write_fact_operations_stage(", write_section)
        self.assertIn("merge_fact_readings_measurement(", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("window_cleanup(fetch_from_ts)", graph_section)


if __name__ == "__main__":
    unittest.main()
