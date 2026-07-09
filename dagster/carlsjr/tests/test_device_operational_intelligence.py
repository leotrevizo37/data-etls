import json
import sys
import unittest
import warnings
from datetime import datetime
from importlib import import_module
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

doi_module = import_module("carlsjr.defs.transformations.device_operational_intelligence")
build_device_operational_daily = doi_module.build_device_operational_daily
build_device_operational_hourly = doi_module.build_device_operational_hourly
build_device_operational_raw_hourly_features = doi_module.build_device_operational_raw_hourly_features


warnings.simplefilter("ignore", ResourceWarning)


class _StaticProbabilityModel:
    def __init__(self, classes, probabilities):
        self.classes_ = classes
        self._probabilities = probabilities

    def predict_proba(self, rows):
        return [self._probabilities for _ in rows]


class DeviceOperationalIntelligenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("carlsjr-device-operational-intelligence-tests")
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

    def _dimensions_df(self):
        return self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                    "sensor_type": "Temperatura F",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                    "sensor_type": "Temperatura F",
                },
            ]
        )

    def _raw_readings_df(self, rows):
        return self.spark.createDataFrame(
            [
                {
                    "ReadId": row.get("ReadId", f"r{idx}"),
                    "SensorId": row["SensorId"],
                    "LocalTimeSpan": row["LocalTimeSpan"],
                    "Value": row["Value"],
                }
                for idx, row in enumerate(rows, start=1)
            ]
        )

    def _hourly_from_raw(
        self,
        raw_rows,
        expected_df,
        from_local,
        to_local,
        events_df=None,
        model_version=None,
        model_artifact=None,
        model_confidence_threshold=0.65,
        feature_from_local=None,
    ):
        raw_features = build_device_operational_raw_hourly_features(
            raw_readings_df=self._raw_readings_df(raw_rows),
            dimensions_df=self._dimensions_df(),
            expected_values_df=expected_df,
            feature_from_local=feature_from_local or from_local,
            to_local=to_local,
        )
        return build_device_operational_hourly(
            raw_hourly_features_df=raw_features,
            sensor_events_df=events_df,
            user_code="carlsjr",
            from_local=from_local,
            to_local=to_local,
            model_version=model_version,
            model_artifact=model_artifact,
            model_confidence_threshold=model_confidence_threshold,
        )

    def test_sensor_reliability_penalizes_missing_hourly_readings_per_sensor(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, minute),
                "Value": float(index),
            }
            for index, minute in enumerate([0, 5, 10, 15, 20, 25], start=1)
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
            ]
        )

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        ).collect()[0]
        evidence = json.loads(row.EvidenceJson)

        self.assertEqual(float(row.SensorReliabilityScore), 70.0)
        self.assertEqual(evidence["ExpectedReadingsPerSensorHour"], 12.0)
        self.assertEqual(evidence["ExpectedReadingsPerDeviceHour"], 24.0)
        self.assertEqual(evidence["MissingReadingsPenaltyPoints"], 30.0)

    def test_raw_hourly_features_deduplicate_readids_and_count_raw_ranges(self) -> None:
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
            ]
        )
        raw_features = build_device_operational_raw_hourly_features(
            raw_readings_df=self._raw_readings_df(
                [
                    {
                        "ReadId": "same-read",
                        "SensorId": "s1",
                        "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                        "Value": 5.0,
                    },
                    {
                        "ReadId": "same-read",
                        "SensorId": "s1",
                        "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                        "Value": 5.0,
                    },
                    {
                        "ReadId": "second-read",
                        "SensorId": "s2",
                        "LocalTimeSpan": datetime(2026, 7, 7, 10, 5),
                        "Value": 20.0,
                    },
                ]
            ),
            dimensions_df=self._dimensions_df(),
            expected_values_df=expected_df,
            feature_from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        )

        row = raw_features.collect()[0]

        self.assertEqual(row.RawReadingsCount, 2)
        self.assertEqual(row.ExpectedValueReadings, 2)
        self.assertEqual(row.InRangeReadings, 1)
        self.assertEqual(row.OutsideExpectedReadings, 1)

    def test_raw_hourly_features_empty_adls_frame_returns_empty_features(self) -> None:
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                }
            ]
        )
        raw_features = build_device_operational_raw_hourly_features(
            raw_readings_df=self.spark.createDataFrame([(True,)], ["is_empty"]),
            dimensions_df=self._dimensions_df(),
            expected_values_df=expected_df,
            feature_from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        )

        self.assertEqual(raw_features.count(), 0)
        self.assertIn("RawReadingsCount", raw_features.columns)

    def test_raw_hourly_features_ignore_overnight_expected_windows(self) -> None:
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "22:00:00",
                    "EndTime": "05:59:59",
                    "scheduledDays": "2-2",
                }
            ]
        )
        raw_features = build_device_operational_raw_hourly_features(
            raw_readings_df=self._raw_readings_df(
                [
                    {
                        "SensorId": "s1",
                        "LocalTimeSpan": datetime(2026, 7, 7, 23, 0),
                        "Value": 50.0,
                    }
                ]
            ),
            dimensions_df=self._dimensions_df(),
            expected_values_df=expected_df,
            feature_from_local=datetime(2026, 7, 7, 23, 0),
            to_local=datetime(2026, 7, 8, 0, 0),
        )

        row = raw_features.collect()[0]

        self.assertEqual(row.ExpectedValueReadings, 0)
        self.assertEqual(row.OutsideExpectedReadings, 0)

    def test_hourly_marks_data_unreliable_when_expected_sensors_do_not_report(self) -> None:
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                },
                {
                    "SensorId": "s3",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                },
                {
                    "SensorId": "s4",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "Active": True,
                    "device_type": "Temperatura F",
                    "device_name": "Freezer 1",
                },
            ]
        )
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                }
            ]
        )
        raw_features = build_device_operational_raw_hourly_features(
            raw_readings_df=self._raw_readings_df(
                [
                    {
                        "SensorId": "s1",
                        "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                        "Value": 5.0,
                    }
                ]
            ),
            dimensions_df=dimensions_df,
            expected_values_df=expected_df,
            feature_from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        )

        row = build_device_operational_hourly(
            raw_hourly_features_df=raw_features,
            sensor_events_df=None,
            user_code="carlsjr",
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        ).collect()[0]

        self.assertEqual(float(row.CoverageScore), 25.0)
        self.assertEqual(row.OperationalState, "DATA_UNRELIABLE")

    def test_hourly_scores_expected_value_drift_with_evidence(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 9, 0),
                "Value": -10.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 9, 0),
                "Value": -11.0,
            },
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": -10.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": -11.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": -20.0,
                    "max_expected_value": -15.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": -20.0,
                    "max_expected_value": -15.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
            ]
        )
        events_df = self.spark.createDataFrame(
            [
                {
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                    "EventMinutes": 5.0,
                    "IsEventCompleted": True,
                }
            ]
        )

        result = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            events_df=events_df,
            feature_from_local=datetime(2026, 7, 7, 9, 0),
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        )

        row = result.collect()[0]
        self.assertEqual(row.OperationalState, "EXPECTED_VALUE_DRIFT")
        self.assertEqual(row.DominantReasonCode, "EXPECTED_VALUE_DRIFT")
        self.assertEqual(float(row.CoverageScore), 100.0)
        self.assertEqual(float(row.ExpectedValueComplianceScore), 0.0)
        evidence = json.loads(row.EvidenceJson)
        self.assertEqual(evidence["ReadingsInsideExpectedRangeCount"], 0)
        self.assertEqual(evidence["ReadingsOutsideExpectedRangeCount"], 2)
        self.assertTrue(evidence["ExpectedValueDriftConfirmedByHistory"])
        self.assertIn("HourlyStdDevValue", evidence)
        self.assertNotIn("ExpectedValueSource", evidence)

    def test_hourly_uses_matching_expected_value_schedule(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 25.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 26.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "08:00:00",
                    "EndTime": "11:59:59",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "12:00:00",
                    "EndTime": "18:00:00",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "08:00:00",
                    "EndTime": "11:59:59",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "12:00:00",
                    "EndTime": "18:00:00",
                    "scheduledDays": "2-2",
                },
            ]
        )

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        ).collect()[0]

        self.assertEqual(row.OperationalState, "OUT_OF_RANGE")
        self.assertEqual(float(row.ExpectedValueComplianceScore), 0.0)

    def test_hourly_ignores_expected_values_outside_scheduled_hour(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 25.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 26.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "12:00:00",
                    "EndTime": "13:00:00",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "12:00:00",
                    "EndTime": "13:00:00",
                    "scheduledDays": "2-2",
                },
            ]
        )

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        ).collect()[0]
        evidence = json.loads(row.EvidenceJson)

        self.assertEqual(row.OperationalState, "NORMAL")
        self.assertIsNone(row.ExpectedValueComplianceScore)
        self.assertEqual(evidence["ReadingsWithExpectedRangeCount"], 0)
        self.assertEqual(evidence["ReadingsInsideExpectedRangeCount"], 0)
        self.assertEqual(evidence["ReadingsOutsideExpectedRangeCount"], 0)

    def test_hourly_scheduled_days_use_monday_as_one(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 6, 10, 0),
                "Value": 25.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 6, 10, 0),
                "Value": 26.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-1",
                },
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-1",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "2-2",
                },
            ]
        )

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            from_local=datetime(2026, 7, 6, 10, 0),
            to_local=datetime(2026, 7, 6, 11, 0),
        ).collect()[0]

        self.assertEqual(row.OperationalState, "OUT_OF_RANGE")
        self.assertEqual(float(row.ExpectedValueComplianceScore), 0.0)

    def test_hourly_drift_uses_only_sensors_with_applicable_schedule(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 25.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 25.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "2-2",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 20.0,
                    "max_expected_value": 30.0,
                    "StartTime": "12:00:00",
                    "EndTime": "13:00:00",
                    "scheduledDays": "2-2",
                },
            ]
        )

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
        ).collect()[0]
        evidence = json.loads(row.EvidenceJson)

        self.assertEqual(row.OperationalState, "OUT_OF_RANGE")
        self.assertEqual(float(row.ExpectedValueComplianceScore), 0.0)
        self.assertEqual(evidence["ReadingsWithExpectedRangeCount"], 1)
        self.assertEqual(evidence["ReadingsOutsideExpectedRangeCount"], 1)

    def test_hourly_applies_active_model_predictions_without_sklearn_worker(self) -> None:
        raw_rows = [
            {
                "SensorId": "s1",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 5.0,
            },
            {
                "SensorId": "s2",
                "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                "Value": 6.0,
            },
        ]
        expected_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
                {
                    "SensorId": "s2",
                    "DeviceId": "d1",
                    "min_expected_value": 0.0,
                    "max_expected_value": 10.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1-7",
                },
            ]
        )
        model_artifact = {
            "state_model": _StaticProbabilityModel(["NORMAL", "WATCH"], [0.1, 0.9]),
            "risk_model": _StaticProbabilityModel([0, 1], [0.2, 0.8]),
            "feature_columns": doi_module.FEATURE_COLUMNS,
        }

        row = self._hourly_from_raw(
            raw_rows=raw_rows,
            expected_df=expected_df,
            events_df=None,
            from_local=datetime(2026, 7, 7, 10, 0),
            to_local=datetime(2026, 7, 7, 11, 0),
            model_version="model-v1",
            model_artifact=model_artifact,
            model_confidence_threshold=0.65,
        ).collect()[0]
        evidence = json.loads(row.EvidenceJson)

        self.assertEqual(row.OperationalState, "WATCH")
        self.assertEqual(row.ModelVersion, "model-v1")
        self.assertEqual(float(row.FailureRiskScore), 0.8)
        self.assertEqual(evidence["ModelOperationalState"], "WATCH")
        self.assertEqual(evidence["ModelConfidence"], 0.9)
        self.assertEqual(evidence["ModelFailureRiskScore"], 0.8)

    def test_daily_rollup_keeps_worst_hourly_state(self) -> None:
        hourly_df = self.spark.createDataFrame(
            [
                {
                    "UserCode": "carlsjr",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "DeviceType": "Temperatura F",
                    "DeviceName": "Freezer 1",
                    "LocalTimeSpan": datetime(2026, 7, 7, 10, 0),
                    "HealthScore": 95.0,
                    "OperationalState": "NORMAL",
                    "TrendDirection": "stable",
                    "ConfidenceScore": 95.0,
                    "CoverageScore": 100.0,
                    "ExpectedValueComplianceScore": 100.0,
                    "SensorReliabilityScore": 100.0,
                    "EventStabilityScore": 100.0,
                    "BehaviorStabilityScore": 100.0,
                    "PeerAlignmentScore": 100.0,
                    "FailureRiskScore": 0.05,
                    "DominantReasonCode": "NORMAL_PATTERN",
                    "RecommendedAction": "No immediate action required; continue monitoring.",
                    "EvidenceJson": "{}",
                    "FeatureSetVersion": "sense-doi-features-202607",
                    "ScoringVersion": "sense-doi-scoring-202607",
                    "ModelVersion": "",
                    "EventMinutes": 0.0,
                    "OpenEvents": 0,
                },
                {
                    "UserCode": "carlsjr",
                    "DeviceId": "d1",
                    "LocationId": "l1",
                    "SubLocationId": "sl1",
                    "DeviceType": "Temperatura F",
                    "DeviceName": "Freezer 1",
                    "LocalTimeSpan": datetime(2026, 7, 7, 11, 0),
                    "HealthScore": 20.0,
                    "OperationalState": "CRITICAL",
                    "TrendDirection": "degrading",
                    "ConfidenceScore": 90.0,
                    "CoverageScore": 100.0,
                    "ExpectedValueComplianceScore": 0.0,
                    "SensorReliabilityScore": 100.0,
                    "EventStabilityScore": 0.0,
                    "BehaviorStabilityScore": 100.0,
                    "PeerAlignmentScore": 100.0,
                    "FailureRiskScore": 0.95,
                    "DominantReasonCode": "OPEN_EVENT",
                    "RecommendedAction": "Inspect the active event and prioritize operational follow-up.",
                    "EvidenceJson": "{}",
                    "FeatureSetVersion": "sense-doi-features-202607",
                    "ScoringVersion": "sense-doi-scoring-202607",
                    "ModelVersion": "",
                    "EventMinutes": 30.0,
                    "OpenEvents": 1,
                },
            ]
        )

        row = build_device_operational_daily(hourly_df).collect()[0]

        self.assertEqual(row.WorstHourlyState, "CRITICAL")
        self.assertEqual(row.OperationalState, "CRITICAL")
        self.assertEqual(row.CriticalHours, 1)
        self.assertEqual(row.OpenEvents, 1)


if __name__ == "__main__":
    unittest.main()
