from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


FEATURE_SET_VERSION = "sense-doi-features-202607-self-explaining-evidence"
SCORING_VERSION = "sense-doi-scoring-202607-positive-scores"
MODEL_SCOPE = "carlsjr"
MODEL_FAMILY = "device_operational_state"
EXPECTED_READINGS_PER_SENSOR_HOUR = 12.0
READING_LOSS_RELIABILITY_MAX_PENALTY = 40.0
OUT_OF_RANGE_SCORE_THRESHOLD = 70.0
EXPECTED_VALUE_DRIFT_SCORE_THRESHOLD = 70.0
EXPECTED_VALUE_DRIFT_MAX_BEHAVIOR_CHANGE_SCORE = 25.0
HOURLY_TREND_HEALTH_DELTA_THRESHOLD = 5.0
DAILY_TREND_HEALTH_DELTA_THRESHOLD = 2.0

STATE_NORMAL = "NORMAL"
STATE_WATCH = "WATCH"
STATE_OUT_OF_RANGE = "OUT_OF_RANGE"
STATE_DEGRADED = "DEGRADED"
STATE_SENSOR_FAULT = "SENSOR_FAULT_SUSPECTED"
STATE_EXPECTED_VALUE_DRIFT = "EXPECTED_VALUE_DRIFT"
STATE_DATA_UNRELIABLE = "DATA_UNRELIABLE"
STATE_CRITICAL = "CRITICAL"

REASON_NORMAL = "NORMAL_PATTERN"
REASON_LOW_COVERAGE = "LOW_COVERAGE"
REASON_OUT_OF_RANGE = "OUT_OF_RANGE"
REASON_EXPECTED_VALUE_DRIFT = "EXPECTED_VALUE_DRIFT"
REASON_SENSOR_FLATLINE = "SENSOR_FLATLINE"
REASON_EVENT_RECURRENCE = "EVENT_RECURRENCE"
REASON_OPEN_EVENT = "OPEN_EVENT"
REASON_PEER_OUTLIER = "PEER_OUTLIER"
REASON_BEHAVIOR_CHANGE = "BEHAVIOR_CHANGE"
REASON_FAILURE_RISK = "FAILURE_RISK"

FEATURE_COLUMNS = [
    "CoverageScore",
    "ExpectedValueDriftScoreFilled",
    "SensorReliabilityScore",
    "EventPressureScore",
    "BehaviorChangeScore",
    "PeerDeviationScore",
    "ReadingsCount",
    "ReportingSensors",
    "ExpectedSensors",
    "EventMinutes",
    "OpenEvents",
    "HourlyMeanValueFilled",
    "HourlyStdDevValueFilled",
    "HistoryMeanValueFilled",
    "HistoryStdDevValueFilled",
]

RAW_HOURLY_FEATURE_SCHEMA = StructType(
    [
        StructField("DeviceId", StringType(), True),
        StructField("LocationId", StringType(), True),
        StructField("SubLocationId", StringType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("DeviceName", StringType(), True),
        StructField("LocalTimeSpan", TimestampType(), True),
        StructField("RawReadingsCount", LongType(), True),
        StructField("ReadingsCount", LongType(), True),
        StructField("ObservedSensors", LongType(), True),
        StructField("ReportingSensors", LongType(), True),
        StructField("FlatlineSensors", LongType(), True),
        StructField("ExpectedSensors", LongType(), True),
        StructField("ExpectedValueReadings", LongType(), True),
        StructField("InRangeReadings", LongType(), True),
        StructField("OutsideExpectedReadings", LongType(), True),
        StructField("AnomalyReadings", LongType(), True),
        StructField("ReadingRows", LongType(), True),
        StructField("HourlyMeanValue", DoubleType(), True),
        StructField("HourlyStdDevValue", DoubleType(), True),
    ]
)


def feature_schema_json() -> Dict[str, Any]:
    return {
        "feature_set_version": FEATURE_SET_VERSION,
        "features": FEATURE_COLUMNS,
        "grain": "device_hour",
        "source_tables": [
            "adls.raw.lecturas",
            "dwh.factSensorEvents",
            "dwh.dimSensorExpectedVal",
            "dwh.dimSidonProdDimensions",
        ],
    }


def _score_col(value) -> F.Column:
    return F.round(F.least(F.lit(100.0), F.greatest(F.lit(0.0), value)), 4)


def _state_severity(column_name: str = "OperationalState") -> F.Column:
    return (
        F.when(F.col(column_name) == STATE_CRITICAL, F.lit(7))
        .when(F.col(column_name) == STATE_EXPECTED_VALUE_DRIFT, F.lit(6))
        .when(F.col(column_name) == STATE_DEGRADED, F.lit(5))
        .when(F.col(column_name) == STATE_SENSOR_FAULT, F.lit(4))
        .when(F.col(column_name) == STATE_OUT_OF_RANGE, F.lit(3))
        .when(F.col(column_name) == STATE_WATCH, F.lit(2))
        .when(F.col(column_name) == STATE_DATA_UNRELIABLE, F.lit(1))
        .otherwise(F.lit(0))
    )


def _empty_raw_hourly_features(spark) -> DataFrame:
    return spark.createDataFrame([], RAW_HOURLY_FEATURE_SCHEMA)


def _normalize_expected_schedules(expected_values_df: Optional[DataFrame]) -> Optional[DataFrame]:
    if expected_values_df is None or len(expected_values_df.columns) <= 1:
        return None

    expected_source = expected_values_df
    if "StartTime" not in expected_source.columns:
        expected_source = expected_source.withColumn("StartTime", F.lit("00:00:00"))
    if "EndTime" not in expected_source.columns:
        expected_source = expected_source.withColumn("EndTime", F.lit("23:59:59"))
    if "scheduledDays" not in expected_source.columns:
        expected_source = expected_source.withColumn("scheduledDays", F.lit("1-7"))

    def time_seconds(column_name: str) -> F.Column:
        value = F.regexp_extract(F.col(column_name).cast("string"), r"(\d{1,2}:\d{2}(?::\d{2})?)", 1)
        value = F.when(F.length(value) == 5, F.concat(value, F.lit(":00"))).otherwise(value)
        return (
            F.regexp_extract(value, r"(\d{1,2}):\d{2}:\d{2}", 1).cast("int") * 3600
            + F.regexp_extract(value, r"\d{1,2}:(\d{2}):\d{2}", 1).cast("int") * 60
            + F.regexp_extract(value, r"\d{1,2}:\d{2}:(\d{2})", 1).cast("int")
        )

    scheduled_days = F.regexp_replace(
        F.coalesce(F.col("scheduledDays").cast("string"), F.lit("1-7")),
        r"\s+",
        "",
    )
    scheduled_days = F.when((scheduled_days == "") | (scheduled_days == "*"), F.lit("1-7")).otherwise(scheduled_days)

    return (
        expected_source
        .select(
            F.col("SensorId").cast("string").alias("ExpectedSensorId"),
            F.col("DeviceId").cast("string").alias("ExpectedDeviceId"),
            F.col("min_expected_value").cast("double").alias("ExpectedMinCandidate"),
            F.col("max_expected_value").cast("double").alias("ExpectedMaxCandidate"),
            F.col("StartTime").alias("ExpectedStartTime"),
            F.col("EndTime").alias("ExpectedEndTime"),
            F.explode(F.split(scheduled_days, ",")).alias("ScheduledDayToken"),
        )
        .withColumn("ExpectedStartSecond", time_seconds("ExpectedStartTime"))
        .withColumn("ExpectedEndSecond", time_seconds("ExpectedEndTime"))
        .withColumn("ScheduledDayParts", F.split(F.col("ScheduledDayToken"), "-"))
        .withColumn("ScheduledDayStart", F.col("ScheduledDayParts").getItem(0).cast("int"))
        .withColumn(
            "ScheduledDayEnd",
            F.coalesce(
                F.col("ScheduledDayParts").getItem(1).cast("int"),
                F.col("ScheduledDayStart"),
            ),
        )
        .filter(
            F.col("ExpectedStartSecond").isNotNull()
            & F.col("ExpectedEndSecond").isNotNull()
            & (F.col("ExpectedStartSecond") <= F.col("ExpectedEndSecond"))
            & F.col("ScheduledDayStart").between(1, 7)
            & F.col("ScheduledDayEnd").between(1, 7)
            & (F.col("ScheduledDayStart") <= F.col("ScheduledDayEnd"))
        )
        .withColumn(
            "ExpectedScheduledDay",
            F.explode(F.sequence(F.col("ScheduledDayStart"), F.col("ScheduledDayEnd"))),
        )
        .select(
            "ExpectedSensorId",
            "ExpectedDeviceId",
            "ExpectedMinCandidate",
            "ExpectedMaxCandidate",
            "ExpectedStartSecond",
            "ExpectedEndSecond",
            "ExpectedScheduledDay",
        )
    )


def _state_from_severity(column: F.Column) -> F.Column:
    return (
        F.when(column >= 7, F.lit(STATE_CRITICAL))
        .when(column == 6, F.lit(STATE_EXPECTED_VALUE_DRIFT))
        .when(column == 5, F.lit(STATE_DEGRADED))
        .when(column == 4, F.lit(STATE_SENSOR_FAULT))
        .when(column == 3, F.lit(STATE_OUT_OF_RANGE))
        .when(column == 2, F.lit(STATE_WATCH))
        .when(column == 1, F.lit(STATE_DATA_UNRELIABLE))
        .otherwise(F.lit(STATE_NORMAL))
    )


def _probabilities_to_list(probabilities) -> list[float]:
    if hasattr(probabilities, "tolist"):
        probabilities = probabilities.tolist()
    return [float(value) for value in probabilities]


def _predict_model_artifact(model_artifact: Dict[str, Any], feature_values: list[float]):
    state_model = model_artifact.get("state_model")
    risk_model = model_artifact.get("risk_model")
    state = None
    confidence = None
    risk = None
    if state_model is not None:
        state_proba = _probabilities_to_list(state_model.predict_proba([feature_values])[0])
        state_idx = max(range(len(state_proba)), key=state_proba.__getitem__)
        state = str(list(state_model.classes_)[state_idx])
        confidence = float(state_proba[state_idx])
    if risk_model is not None:
        risk_proba = _probabilities_to_list(risk_model.predict_proba([feature_values])[0])
        risk_classes = list(getattr(risk_model, "classes_", []))
        risk_idx = risk_classes.index(1) if 1 in risk_classes else len(risk_proba) - 1
        risk = float(risk_proba[risk_idx])
    return state, confidence, risk


def _apply_model_predictions(
    features_df: DataFrame,
    model_artifact: Optional[Dict[str, Any]],
    model_confidence_threshold: float,
) -> DataFrame:
    if not model_artifact:
        return (
            features_df
            .withColumn("ModelOperationalState", F.lit(None).cast("string"))
            .withColumn("ModelConfidence", F.lit(None).cast("double"))
            .withColumn("ModelFailureRiskScore", F.lit(None).cast("double"))
            .withColumn("ModelConfidenceThreshold", F.lit(model_confidence_threshold))
        )

    feature_columns = model_artifact.get("feature_columns") or FEATURE_COLUMNS
    missing_features = [
        column
        for column in feature_columns
        if column not in features_df.columns
    ]
    if missing_features:
        raise ValueError(
            "Device operational model is incompatible with current feature set; "
            "missing features: " + ", ".join(missing_features)
        )

    spark = features_df.sparkSession
    key_columns = ["DeviceId", "LocationId", "SubLocationId", "LocalTimeSpan"]
    prediction_rows = []
    for row in features_df.select(*key_columns, *feature_columns).collect():
        values = [float(row[column] or 0.0) for column in feature_columns]
        state, confidence, risk = _predict_model_artifact(model_artifact, values)
        prediction_rows.append(
            (
                row.DeviceId,
                row.LocationId,
                row.SubLocationId,
                row.LocalTimeSpan,
                state,
                confidence,
                risk,
            )
        )

    predictions_schema = StructType(
        [
            StructField("DeviceId", StringType(), True),
            StructField("LocationId", StringType(), True),
            StructField("SubLocationId", StringType(), True),
            StructField("LocalTimeSpan", TimestampType(), True),
            StructField("ModelOperationalState", StringType(), True),
            StructField("ModelConfidence", DoubleType(), True),
            StructField("ModelFailureRiskScore", DoubleType(), True),
        ]
    )
    predictions_df = spark.createDataFrame(prediction_rows, predictions_schema)
    return (
        features_df
        .join(predictions_df, key_columns, "left")
        .withColumn("ModelConfidenceThreshold", F.lit(model_confidence_threshold))
    )


def build_device_operational_raw_hourly_features(
    raw_readings_df: DataFrame,
    dimensions_df: DataFrame,
    expected_values_df: Optional[DataFrame],
    feature_from_local,
    to_local,
) -> DataFrame:
    spark = raw_readings_df.sparkSession
    if len(raw_readings_df.columns) <= 1:
        return _empty_raw_hourly_features(spark)

    dim = (
        dimensions_df
        .filter(F.col("Active") == F.lit(True))
        .select(
            F.col("SensorId").cast("string").alias("DimSensorId"),
            F.col("DeviceId").cast("string").alias("DimDeviceId"),
            F.col("LocationId").cast("string").alias("DimLocationId"),
            F.col("SubLocationId").cast("string").alias("DimSubLocationId"),
            F.col("device_type").cast("string").alias("DimDeviceType"),
            F.col("device_name").cast("string").alias("DimDeviceName"),
        )
    )

    device_dim = (
        dim
        .groupBy("DimDeviceId", "DimLocationId", "DimSubLocationId")
        .agg(
            F.first("DimDeviceType", ignorenulls=True).alias("DeviceType"),
            F.first("DimDeviceName", ignorenulls=True).alias("DeviceName"),
            F.countDistinct("DimSensorId").alias("ExpectedSensors"),
        )
    )

    readings = (
        raw_readings_df
        .select(
            F.col("ReadId").cast("string").alias("ReadId"),
            F.col("SensorId").cast("string").alias("SensorId"),
            F.col("LocalTimeSpan").cast("timestamp").alias("RawLocalTimeSpan"),
            F.col("Value").cast("double").alias("MeasurementValue"),
        )
        .filter(
            F.col("ReadId").isNotNull()
            & F.col("SensorId").isNotNull()
            & F.col("RawLocalTimeSpan").isNotNull()
        )
        .dropDuplicates(["ReadId"])
        .filter(
            (F.col("RawLocalTimeSpan") >= F.lit(feature_from_local))
            & (F.col("RawLocalTimeSpan") < F.lit(to_local))
        )
        .join(dim, F.col("SensorId") == F.col("DimSensorId"), "inner")
        .filter(F.col("DimSubLocationId").isNotNull())
        .select(
            F.col("ReadId"),
            F.col("SensorId"),
            F.col("DimDeviceId").alias("DeviceId"),
            F.col("DimLocationId").alias("LocationId"),
            F.col("DimSubLocationId").alias("SubLocationId"),
            F.date_trunc("hour", F.col("RawLocalTimeSpan")).alias("LocalTimeSpan"),
            F.col("RawLocalTimeSpan"),
            F.col("MeasurementValue"),
        )
    )

    expected_schedules = _normalize_expected_schedules(expected_values_df)
    if expected_schedules is not None:
        reading_columns = readings.columns
        readings = (
            readings
            .withColumn("ReadingLocalDay", F.pmod(F.dayofweek(F.col("RawLocalTimeSpan")) + F.lit(5), F.lit(7)) + F.lit(1))
            .withColumn(
                "ReadingLocalSecond",
                (F.hour(F.col("RawLocalTimeSpan")) * 3600)
                + (F.minute(F.col("RawLocalTimeSpan")) * 60)
                + F.second(F.col("RawLocalTimeSpan")),
            )
            .join(
                expected_schedules,
                (F.col("SensorId") == F.col("ExpectedSensorId"))
                & (F.col("DeviceId") == F.col("ExpectedDeviceId"))
                & (F.col("ReadingLocalDay") == F.col("ExpectedScheduledDay"))
                & (F.col("ReadingLocalSecond") >= F.col("ExpectedStartSecond"))
                & (F.col("ReadingLocalSecond") <= F.col("ExpectedEndSecond")),
                "left",
            )
            .groupBy("ReadId")
            .agg(
                *[F.first(F.col(column), False).alias(column) for column in reading_columns if column != "ReadId"],
                F.min(F.col("ExpectedMinCandidate")).alias("ExpectedMin"),
                F.max(F.col("ExpectedMaxCandidate")).alias("ExpectedMax"),
            )
        )
    else:
        readings = (
            readings
            .withColumn("ExpectedMin", F.lit(None).cast("double"))
            .withColumn("ExpectedMax", F.lit(None).cast("double"))
        )

    readings = (
        readings
        .withColumn(
            "HasExpectedValue",
            F.col("ExpectedMin").isNotNull()
            & F.col("ExpectedMax").isNotNull()
            & F.col("MeasurementValue").isNotNull(),
        )
        .withColumn(
            "OutsideExpected",
            F.when(
                F.col("HasExpectedValue")
                & (
                    (F.col("MeasurementValue") < F.col("ExpectedMin"))
                    | (F.col("MeasurementValue") > F.col("ExpectedMax"))
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "InRangeExpected",
            F.when(F.col("HasExpectedValue") & (F.col("OutsideExpected") == F.lit(0)), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    sensor_hour = (
        readings
        .groupBy("DeviceId", "LocationId", "SubLocationId", "LocalTimeSpan", "SensorId")
        .agg(
            F.countDistinct("ReadId").alias("SensorRawReadings"),
            F.stddev_pop("MeasurementValue").alias("SensorStdDevValue"),
        )
        .withColumn(
            "FlatlineSensor",
            F.when(
                (F.col("SensorRawReadings") > 1)
                & (F.coalesce(F.col("SensorStdDevValue"), F.lit(0.0)) == 0.0),
                F.col("SensorId"),
            ),
        )
    )

    flatlines = (
        sensor_hour
        .groupBy("DeviceId", "LocationId", "SubLocationId", "LocalTimeSpan")
        .agg(F.countDistinct("FlatlineSensor").alias("FlatlineSensors"))
    )

    hourly = (
        readings
        .groupBy("DeviceId", "LocationId", "SubLocationId", "LocalTimeSpan")
        .agg(
            F.countDistinct("ReadId").alias("RawReadingsCount"),
            F.countDistinct("SensorId").alias("ObservedSensors"),
            F.countDistinct(F.when(F.col("MeasurementValue").isNotNull(), F.col("SensorId"))).alias("ReportingSensors"),
            F.sum(F.when(F.col("HasExpectedValue"), F.lit(1)).otherwise(F.lit(0))).alias("ExpectedValueReadings"),
            F.sum("InRangeExpected").alias("InRangeReadings"),
            F.sum("OutsideExpected").alias("OutsideExpectedReadings"),
            F.lit(0).cast("long").alias("AnomalyReadings"),
            F.count(F.lit(1)).alias("ReadingRows"),
            F.avg("MeasurementValue").alias("HourlyMeanValue"),
            F.stddev_pop("MeasurementValue").alias("HourlyStdDevValue"),
        )
        .join(flatlines, ["DeviceId", "LocationId", "SubLocationId", "LocalTimeSpan"], "left")
        .join(
            device_dim,
            (F.col("DeviceId") == F.col("DimDeviceId"))
            & (F.col("LocationId") == F.col("DimLocationId"))
            & (F.col("SubLocationId") == F.col("DimSubLocationId")),
            "left",
        )
        .drop("DimDeviceId", "DimLocationId", "DimSubLocationId")
        .fillna({"FlatlineSensors": 0})
        .withColumn("ReadingsCount", F.col("RawReadingsCount"))
    )

    return hourly.select(
        F.col("DeviceId"),
        F.col("LocationId"),
        F.col("SubLocationId"),
        F.col("DeviceType"),
        F.col("DeviceName"),
        F.col("LocalTimeSpan"),
        F.col("RawReadingsCount").cast("long").alias("RawReadingsCount"),
        F.col("ReadingsCount").cast("long").alias("ReadingsCount"),
        F.col("ObservedSensors").cast("long").alias("ObservedSensors"),
        F.col("ReportingSensors").cast("long").alias("ReportingSensors"),
        F.col("FlatlineSensors").cast("long").alias("FlatlineSensors"),
        F.col("ExpectedSensors").cast("long").alias("ExpectedSensors"),
        F.col("ExpectedValueReadings").cast("long").alias("ExpectedValueReadings"),
        F.col("InRangeReadings").cast("long").alias("InRangeReadings"),
        F.col("OutsideExpectedReadings").cast("long").alias("OutsideExpectedReadings"),
        F.col("AnomalyReadings").cast("long").alias("AnomalyReadings"),
        F.col("ReadingRows").cast("long").alias("ReadingRows"),
        F.col("HourlyMeanValue"),
        F.col("HourlyStdDevValue"),
    )


def build_device_operational_hourly(
    raw_hourly_features_df: DataFrame,
    sensor_events_df: Optional[DataFrame],
    user_code: str,
    from_local,
    to_local,
    model_version: Optional[str] = None,
    model_artifact: Optional[Dict[str, Any]] = None,
    model_confidence_threshold: float = 0.65,
) -> DataFrame:
    history = (
        raw_hourly_features_df
        .filter(F.col("LocalTimeSpan") < F.lit(from_local))
        .groupBy("DeviceId", "LocationId", "SubLocationId")
        .agg(
            F.avg("HourlyMeanValue").alias("HistoryMeanValue"),
            F.stddev_pop("HourlyMeanValue").alias("HistoryStdDevValue"),
        )
    )

    hourly = (
        raw_hourly_features_df
        .filter(
            (F.col("LocalTimeSpan") >= F.lit(from_local))
            & (F.col("LocalTimeSpan") < F.lit(to_local))
        )
        .join(history, ["DeviceId", "LocationId", "SubLocationId"], "left")
    )

    if sensor_events_df is not None:
        events = (
            sensor_events_df
            .filter(
                (F.col("LocalTimeSpan") >= F.lit(from_local))
                & (F.col("LocalTimeSpan") < F.lit(to_local))
            )
            .groupBy(
                F.col("DeviceId").cast("string").alias("EventDeviceId"),
                F.col("LocationId").cast("string").alias("EventLocationId"),
                F.date_trunc("hour", F.col("LocalTimeSpan")).alias("EventLocalTimeSpan"),
            )
            .agg(
                F.sum(F.col("EventMinutes").cast("double")).alias("EventMinutes"),
                F.sum(F.when(F.col("IsEventCompleted") == F.lit(False), F.lit(1)).otherwise(F.lit(0))).alias("OpenEvents"),
                F.count(F.lit(1)).alias("EventCount"),
            )
        )
        hourly = hourly.join(
            events,
            (F.col("DeviceId") == F.col("EventDeviceId"))
            & (F.col("LocationId") == F.col("EventLocationId"))
            & (F.col("LocalTimeSpan") == F.col("EventLocalTimeSpan")),
            "left",
        ).drop("EventDeviceId", "EventLocationId", "EventLocalTimeSpan")
    else:
        hourly = (
            hourly
            .withColumn("EventMinutes", F.lit(0.0))
            .withColumn("OpenEvents", F.lit(0))
            .withColumn("EventCount", F.lit(0))
        )

    hourly = (
        hourly
        .fillna({
            "RawReadingsCount": 0,
            "ReadingsCount": 0,
            "ObservedSensors": 0,
            "ReportingSensors": 0,
            "FlatlineSensors": 0,
            "ExpectedSensors": 0,
            "ExpectedValueReadings": 0,
            "InRangeReadings": 0,
            "OutsideExpectedReadings": 0,
            "AnomalyReadings": 0,
            "ReadingRows": 0,
            "EventMinutes": 0.0,
            "OpenEvents": 0,
            "EventCount": 0,
        })
        .withColumn(
            "CoverageScore",
            _score_col(
                F.when(
                    F.col("ExpectedSensors") > 0,
                    (F.col("ReportingSensors").cast("double") / F.col("ExpectedSensors").cast("double")) * 100.0,
                ).otherwise(F.lit(0.0))
            ),
        )
        .withColumn(
            "ExpectedValueDriftScore",
            F.when(
                F.col("ExpectedValueReadings") > 0,
                _score_col(
                    (F.col("OutsideExpectedReadings").cast("double") / F.col("ExpectedValueReadings").cast("double")) * 100.0
                ),
            ),
        )
        .withColumn(
            "FlatlineRatio",
            F.when(
                F.col("ReportingSensors") > 0,
                (F.col("FlatlineSensors").cast("double") / F.col("ReportingSensors").cast("double")) * 100.0,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "AnomalyRatio",
            F.when(
                F.col("ReadingRows") > 0,
                (F.col("AnomalyReadings").cast("double") / F.col("ReadingRows").cast("double")) * 100.0,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "ExpectedHourlyReadings",
            F.when(
                F.col("ExpectedSensors") > 0,
                F.col("ExpectedSensors").cast("double") * F.lit(EXPECTED_READINGS_PER_SENSOR_HOUR),
            )
            .when(
                F.col("ReportingSensors") > 0,
                F.col("ReportingSensors").cast("double") * F.lit(EXPECTED_READINGS_PER_SENSOR_HOUR),
            )
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "ReadingLossPenalty",
            _score_col(
                F.when(
                    (F.col("ExpectedHourlyReadings") > 0)
                    & (F.col("ReadingsCount") < F.col("ExpectedHourlyReadings")),
                    (
                        (F.col("ExpectedHourlyReadings") - F.col("ReadingsCount").cast("double"))
                        / F.col("ExpectedHourlyReadings")
                    )
                    * F.lit(READING_LOSS_RELIABILITY_MAX_PENALTY),
                ).otherwise(F.lit(0.0))
            ),
        )
        .withColumn(
            "SensorReliabilityScore",
            _score_col(
                F.lit(100.0)
                - (F.col("FlatlineRatio") * 0.7)
                - (F.col("AnomalyRatio") * 0.3)
                - F.col("ReadingLossPenalty")
            ),
        )
        .withColumn(
            "EventPressureScore",
            _score_col(
                (F.coalesce(F.col("EventMinutes"), F.lit(0.0)) * 2.0)
                + (F.coalesce(F.col("OpenEvents"), F.lit(0)) * 25.0)
                + (F.coalesce(F.col("EventCount"), F.lit(0)) * 5.0)
            ),
        )
        .withColumn(
            "BehaviorChangeScore",
            _score_col(
                F.when(
                    F.coalesce(F.col("HistoryStdDevValue"), F.lit(0.0)) > 0,
                    (
                        F.abs(F.col("HourlyMeanValue") - F.col("HistoryMeanValue"))
                        / F.col("HistoryStdDevValue")
                    ) * 20.0,
                ).otherwise(F.lit(0.0))
            ),
        )
    )

    peer_window = Window.partitionBy("LocationId", "DeviceType", "LocalTimeSpan")
    hourly = (
        hourly
        .withColumn("PeerMeanValue", F.avg("HourlyMeanValue").over(peer_window))
        .withColumn("PeerStdDevValue", F.stddev_pop("HourlyMeanValue").over(peer_window))
        .withColumn("PeerDeviceCount", F.count(F.lit(1)).over(peer_window))
        .withColumn(
            "PeerDeviationScore",
            _score_col(
                F.when(
                    (F.col("PeerDeviceCount") >= 3)
                    & (F.coalesce(F.col("PeerStdDevValue"), F.lit(0.0)) > 0),
                    (F.abs(F.col("HourlyMeanValue") - F.col("PeerMeanValue")) / F.col("PeerStdDevValue")) * 20.0,
                ).otherwise(F.lit(0.0))
            ),
        )
        .withColumn("ExpectedValueDriftScoreFilled", F.coalesce(F.col("ExpectedValueDriftScore"), F.lit(0.0)))
        .withColumn(
            "ExpectedValueComplianceScore",
            F.when(
                F.col("ExpectedValueDriftScore").isNotNull(),
                _score_col(F.lit(100.0) - F.col("ExpectedValueDriftScore")),
            ),
        )
        .withColumn("EventStabilityScore", _score_col(F.lit(100.0) - F.col("EventPressureScore")))
        .withColumn("BehaviorStabilityScore", _score_col(F.lit(100.0) - F.col("BehaviorChangeScore")))
        .withColumn("PeerAlignmentScore", _score_col(F.lit(100.0) - F.col("PeerDeviationScore")))
        .withColumn("HourlyMeanValueFilled", F.coalesce(F.col("HourlyMeanValue"), F.lit(0.0)))
        .withColumn("HourlyStdDevValueFilled", F.coalesce(F.col("HourlyStdDevValue"), F.lit(0.0)))
        .withColumn("HistoryMeanValueFilled", F.coalesce(F.col("HistoryMeanValue"), F.lit(0.0)))
        .withColumn("HistoryStdDevValueFilled", F.coalesce(F.col("HistoryStdDevValue"), F.lit(0.0)))
        .withColumn(
            "ExpectedValueOutOfRange",
            F.col("ExpectedValueDriftScoreFilled") >= F.lit(OUT_OF_RANGE_SCORE_THRESHOLD),
        )
        .withColumn(
            "ExpectedValueDriftConfirmed",
            (F.col("ExpectedValueDriftScoreFilled") >= F.lit(EXPECTED_VALUE_DRIFT_SCORE_THRESHOLD))
            & F.col("HistoryMeanValue").isNotNull()
            & (
                F.col("BehaviorChangeScore")
                <= F.lit(EXPECTED_VALUE_DRIFT_MAX_BEHAVIOR_CHANGE_SCORE)
            ),
        )
    )

    hourly = _apply_model_predictions(hourly, model_artifact, model_confidence_threshold)
    health_trend_window = Window.partitionBy("DeviceId", "LocationId", "SubLocationId").orderBy("LocalTimeSpan")
    deterministic_health = (
        F.lit(100.0)
        - ((F.lit(100.0) - F.col("CoverageScore")) * 0.25)
        - (F.col("ExpectedValueDriftScoreFilled") * 0.20)
        - ((F.lit(100.0) - F.col("SensorReliabilityScore")) * 0.20)
        - (F.col("EventPressureScore") * 0.20)
        - (F.col("BehaviorChangeScore") * 0.10)
        - (F.col("PeerDeviationScore") * 0.05)
    )

    hourly = (
        hourly
        .withColumn("HealthScore", _score_col(deterministic_health))
        .withColumn("DeterministicFailureRiskScore", _score_col(F.lit(100.0) - F.col("HealthScore")) / F.lit(100.0))
        .withColumn(
            "FailureRiskScore",
            F.round(
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.col("DeterministicFailureRiskScore"),
                        F.coalesce(F.col("ModelFailureRiskScore"), F.lit(0.0)),
                    ),
                ),
                6,
            ),
        )
        .withColumn(
            "RuleOperationalState",
            F.when(F.col("CoverageScore") < 40.0, F.lit(STATE_DATA_UNRELIABLE))
            .when(F.col("EventPressureScore") >= 85.0, F.lit(STATE_CRITICAL))
            .when(F.col("FailureRiskScore") >= 0.85, F.lit(STATE_CRITICAL))
            .when(F.col("SensorReliabilityScore") < 55.0, F.lit(STATE_SENSOR_FAULT))
            .when(F.col("ExpectedValueDriftConfirmed"), F.lit(STATE_EXPECTED_VALUE_DRIFT))
            .when(F.col("ExpectedValueOutOfRange"), F.lit(STATE_OUT_OF_RANGE))
            .when(F.col("HealthScore") < 70.0, F.lit(STATE_DEGRADED))
            .when(
                (F.col("HealthScore") < 85.0)
                | (F.col("BehaviorChangeScore") >= 50.0)
                | (F.col("PeerDeviationScore") >= 60.0),
                F.lit(STATE_WATCH),
            )
            .otherwise(F.lit(STATE_NORMAL)),
        )
        .withColumn(
            "OperationalState",
            F.when(F.col("RuleOperationalState").isin(STATE_DATA_UNRELIABLE, STATE_CRITICAL, STATE_SENSOR_FAULT), F.col("RuleOperationalState"))
            .when(
                F.col("ModelOperationalState").isNotNull()
                & (F.col("ModelConfidence") >= F.col("ModelConfidenceThreshold"))
                & (F.col("ModelOperationalState") != STATE_NORMAL),
                F.col("ModelOperationalState"),
            )
            .otherwise(F.col("RuleOperationalState")),
        )
        .withColumn(
            "DominantReasonCode",
            F.when(F.col("CoverageScore") < 40.0, F.lit(REASON_LOW_COVERAGE))
            .when(F.col("EventPressureScore") >= 85.0, F.lit(REASON_OPEN_EVENT))
            .when(F.col("FailureRiskScore") >= 0.85, F.lit(REASON_FAILURE_RISK))
            .when(F.col("SensorReliabilityScore") < 55.0, F.lit(REASON_SENSOR_FLATLINE))
            .when(F.col("ExpectedValueDriftConfirmed"), F.lit(REASON_EXPECTED_VALUE_DRIFT))
            .when(F.col("ExpectedValueOutOfRange"), F.lit(REASON_OUT_OF_RANGE))
            .when(F.col("PeerDeviationScore") >= 60.0, F.lit(REASON_PEER_OUTLIER))
            .when(F.col("BehaviorChangeScore") >= 50.0, F.lit(REASON_BEHAVIOR_CHANGE))
            .when(F.col("EventPressureScore") >= 50.0, F.lit(REASON_EVENT_RECURRENCE))
            .otherwise(F.lit(REASON_NORMAL)),
        )
        .withColumn(
            "RecommendedAction",
            F.when(F.col("DominantReasonCode") == REASON_LOW_COVERAGE, F.lit("Validate telemetry coverage before diagnosing the device."))
            .when(F.col("DominantReasonCode") == REASON_OPEN_EVENT, F.lit("Inspect the active event and prioritize operational follow-up."))
            .when(F.col("DominantReasonCode") == REASON_FAILURE_RISK, F.lit("Review the device with maintenance priority based on accumulated risk."))
            .when(F.col("DominantReasonCode") == REASON_SENSOR_FLATLINE, F.lit("Inspect sensor calibration, wiring, or reporting health before replacing the device."))
            .when(F.col("DominantReasonCode") == REASON_EXPECTED_VALUE_DRIFT, F.lit("Expected value drift is supported by historical behavior; review expected value configuration before treating this as a physical failure."))
            .when(F.col("DominantReasonCode") == REASON_OUT_OF_RANGE, F.lit("Readings are outside the configured expected range. Expected value drift is suspected if this repeats while matching the device history."))
            .when(F.col("DominantReasonCode") == REASON_PEER_OUTLIER, F.lit("Compare the device with similar devices in the same location and inspect abnormal operation."))
            .when(F.col("DominantReasonCode") == REASON_BEHAVIOR_CHANGE, F.lit("Review recent operational changes affecting this device."))
            .when(F.col("DominantReasonCode") == REASON_EVENT_RECURRENCE, F.lit("Review recurrent events and confirm whether corrective action is required."))
            .otherwise(F.lit("No immediate action required; continue monitoring."))
        )
        .withColumn(
            "ConfidenceScore",
            _score_col(
                (F.col("CoverageScore") * 0.45)
                + (F.col("SensorReliabilityScore") * 0.25)
                + F.col("PeerAlignmentScore") * 0.10
                + F.col("BehaviorStabilityScore") * 0.10
                + (F.coalesce(F.col("ModelConfidence"), F.lit(0.0)) * 100.0 * 0.10)
            ),
        )
        .withColumn("PreviousHealthScore", F.lag("HealthScore").over(health_trend_window))
        .withColumn("HealthTrendDelta", F.col("HealthScore") - F.col("PreviousHealthScore"))
        .withColumn(
            "TrendDirection",
            F.when(F.col("PreviousHealthScore").isNull(), F.lit("stable"))
            .when(F.col("HealthTrendDelta") <= F.lit(-HOURLY_TREND_HEALTH_DELTA_THRESHOLD), F.lit("degrading"))
            .when(F.col("HealthTrendDelta") >= F.lit(HOURLY_TREND_HEALTH_DELTA_THRESHOLD), F.lit("improving"))
            .otherwise(F.lit("stable")),
        )
    )

    evidence = F.to_json(
        F.struct(
            F.col("RawReadingsCount").alias("RawReadingsCount"),
            F.col("ReadingsCount").alias("TotalReadingsCount"),
            F.col("ExpectedSensors").alias("ExpectedSensorCount"),
            F.col("ObservedSensors").alias("DistinctSensorsObservedCount"),
            F.col("ReportingSensors").alias("SensorsWithValidValueCount"),
            F.col("FlatlineSensors").alias("FlatlineSensorCount"),
            F.lit(EXPECTED_READINGS_PER_SENSOR_HOUR).alias("ExpectedReadingsPerSensorHour"),
            F.col("ExpectedHourlyReadings").alias("ExpectedReadingsPerDeviceHour"),
            F.col("ReadingLossPenalty").alias("MissingReadingsPenaltyPoints"),
            F.col("ExpectedValueReadings").alias("ReadingsWithExpectedRangeCount"),
            F.col("InRangeReadings").alias("ReadingsInsideExpectedRangeCount"),
            F.col("OutsideExpectedReadings").alias("ReadingsOutsideExpectedRangeCount"),
            F.col("ExpectedValueDriftScoreFilled").alias("ExpectedValueOutOfRangePercent"),
            F.col("ExpectedValueOutOfRange").alias("ExpectedValueOutOfRangeDetected"),
            F.col("ExpectedValueDriftConfirmed").alias("ExpectedValueDriftConfirmedByHistory"),
            F.col("EventMinutes").alias("EventMinutes"),
            F.col("OpenEvents").alias("OpenEventCount"),
            F.col("EventCount").alias("EventCount"),
            F.col("HourlyMeanValue").alias("HourlyMeanValue"),
            F.col("HourlyStdDevValue").alias("HourlyStdDevValue"),
            F.col("HistoryMeanValue").alias("HistoricalMeanValue"),
            F.col("HistoryStdDevValue").alias("HistoricalStdDevValue"),
            F.col("BehaviorChangeScore").alias("HistoricalBehaviorChangeScore"),
            F.col("PeerMeanValue").alias("PeerMeanValue"),
            F.col("PeerStdDevValue").alias("PeerStdDevValue"),
            F.col("PeerDeviceCount").alias("PeerDeviceCount"),
            F.col("ModelOperationalState").alias("ModelOperationalState"),
            F.col("ModelConfidence").alias("ModelConfidence"),
            F.col("ModelFailureRiskScore").alias("ModelFailureRiskScore"),
        )
    )

    return hourly.select(
        F.lit(user_code).alias("UserCode"),
        F.col("DeviceId"),
        F.col("LocationId"),
        F.col("SubLocationId"),
        F.col("DeviceType"),
        F.col("DeviceName"),
        F.col("LocalTimeSpan"),
        F.col("HealthScore").cast(DecimalType(9, 4)).alias("HealthScore"),
        F.col("OperationalState"),
        F.col("TrendDirection"),
        F.col("ConfidenceScore").cast(DecimalType(9, 4)).alias("ConfidenceScore"),
        F.col("CoverageScore").cast(DecimalType(9, 4)).alias("CoverageScore"),
        F.col("ExpectedValueComplianceScore").cast(DecimalType(9, 4)).alias("ExpectedValueComplianceScore"),
        F.col("SensorReliabilityScore").cast(DecimalType(9, 4)).alias("SensorReliabilityScore"),
        F.col("EventStabilityScore").cast(DecimalType(9, 4)).alias("EventStabilityScore"),
        F.col("BehaviorStabilityScore").cast(DecimalType(9, 4)).alias("BehaviorStabilityScore"),
        F.col("PeerAlignmentScore").cast(DecimalType(9, 4)).alias("PeerAlignmentScore"),
        F.col("FailureRiskScore").cast(DecimalType(9, 6)).alias("FailureRiskScore"),
        F.col("DominantReasonCode"),
        F.col("RecommendedAction"),
        evidence.alias("EvidenceJson"),
        F.lit(FEATURE_SET_VERSION).alias("FeatureSetVersion"),
        F.lit(SCORING_VERSION).alias("ScoringVersion"),
        F.lit(model_version).cast("string").alias("ModelVersion"),
        F.col("EventMinutes"),
        F.col("OpenEvents"),
    )


def build_device_operational_daily(hourly_df: DataFrame) -> DataFrame:
    base = (
        hourly_df
        .withColumn("LocalDate", F.to_date("LocalTimeSpan"))
        .withColumn("SeverityScore", _state_severity())
    )
    key_cols = [
        "UserCode",
        "DeviceId",
        "LocationId",
        "SubLocationId",
        "DeviceType",
        "DeviceName",
        "LocalDate",
    ]
    agg = (
        base
        .groupBy(*key_cols)
        .agg(
            F.round(F.avg(F.col("HealthScore").cast("double")), 4).alias("HealthScore"),
            F.max("SeverityScore").alias("WorstSeverityScore"),
            F.sum(F.when(F.col("OperationalState") == STATE_CRITICAL, F.lit(1)).otherwise(F.lit(0))).alias("CriticalHours"),
            F.sum(F.when(F.col("OperationalState") == STATE_WATCH, F.lit(1)).otherwise(F.lit(0))).alias("WatchHours"),
            F.sum(F.when(F.col("OperationalState").isin(STATE_DEGRADED, STATE_OUT_OF_RANGE, STATE_SENSOR_FAULT, STATE_EXPECTED_VALUE_DRIFT), F.lit(1)).otherwise(F.lit(0))).alias("DegradedHours"),
            F.round(F.sum(F.col("EventMinutes").cast("double")), 4).alias("EventMinutes"),
            F.sum(F.col("OpenEvents").cast("int")).alias("OpenEvents"),
            F.round(F.avg(F.col("ConfidenceScore").cast("double")), 4).alias("ConfidenceScore"),
            F.round(F.max(F.col("FailureRiskScore").cast("double")), 6).alias("FailureRiskScore"),
            F.first("FeatureSetVersion", ignorenulls=True).alias("FeatureSetVersion"),
            F.first("ScoringVersion", ignorenulls=True).alias("ScoringVersion"),
            F.first("ModelVersion", ignorenulls=True).alias("ModelVersion"),
        )
    )

    rank_window = Window.partitionBy(*key_cols).orderBy(
        F.col("SeverityScore").desc(),
        F.col("FailureRiskScore").desc(),
        F.col("ConfidenceScore").desc(),
    )
    worst = (
        base
        .withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .select(
            *key_cols,
            F.col("OperationalState").alias("WorstHourlyState"),
            F.col("DominantReasonCode"),
            F.col("RecommendedAction"),
            F.col("EvidenceJson").alias("WorstEvidenceJson"),
        )
    )

    daily = agg.join(worst, key_cols, "inner")
    daily = (
        daily
        .withColumn("OperationalState", _state_from_severity(F.col("WorstSeverityScore")))
    )

    rolling_window_7 = (
        Window
        .partitionBy("UserCode", "DeviceId", "LocationId", "SubLocationId")
        .orderBy(F.col("LocalDate"))
        .rowsBetween(-6, 0)
    )
    rolling_window_30 = (
        Window
        .partitionBy("UserCode", "DeviceId", "LocationId", "SubLocationId")
        .orderBy(F.col("LocalDate"))
        .rowsBetween(-29, 0)
    )
    percentile_window = Window.partitionBy("UserCode", "LocalDate").orderBy(F.col("FailureRiskScore"))
    daily = (
        daily
        .withColumn("SevenDayHealthScore", F.round(F.avg("HealthScore").over(rolling_window_7), 4))
        .withColumn("ThirtyDayHealthScore", F.round(F.avg("HealthScore").over(rolling_window_30), 4))
        .withColumn("PeerPercentileRisk", F.round(F.percent_rank().over(percentile_window) * 100.0, 4))
        .withColumn("HealthTrendDelta", F.col("SevenDayHealthScore") - F.col("ThirtyDayHealthScore"))
        .withColumn(
            "TrendDirection",
            F.when(F.col("HealthTrendDelta") <= F.lit(-DAILY_TREND_HEALTH_DELTA_THRESHOLD), F.lit("degrading"))
            .when(F.col("HealthTrendDelta") >= F.lit(DAILY_TREND_HEALTH_DELTA_THRESHOLD), F.lit("improving"))
            .otherwise(F.lit("stable")),
        )
    )

    evidence = F.to_json(
        F.struct(
            F.col("WorstEvidenceJson").alias("worst_hourly_evidence"),
            F.col("CriticalHours"),
            F.col("DegradedHours"),
            F.col("WatchHours"),
            F.col("EventMinutes"),
            F.col("OpenEvents"),
            F.col("SevenDayHealthScore"),
            F.col("ThirtyDayHealthScore"),
            F.col("PeerPercentileRisk"),
        )
    )

    return daily.select(
        "UserCode",
        "DeviceId",
        "LocationId",
        "SubLocationId",
        "DeviceType",
        "DeviceName",
        "LocalDate",
        F.col("HealthScore").cast(DecimalType(9, 4)).alias("HealthScore"),
        "WorstHourlyState",
        "OperationalState",
        F.col("CriticalHours").cast("int").alias("CriticalHours"),
        F.col("WatchHours").cast("int").alias("WatchHours"),
        F.col("DegradedHours").cast("int").alias("DegradedHours"),
        F.col("EventMinutes").cast(DecimalType(19, 4)).alias("EventMinutes"),
        F.col("OpenEvents").cast("int").alias("OpenEvents"),
        F.col("SevenDayHealthScore").cast(DecimalType(9, 4)).alias("SevenDayHealthScore"),
        F.col("ThirtyDayHealthScore").cast(DecimalType(9, 4)).alias("ThirtyDayHealthScore"),
        "TrendDirection",
        F.col("ConfidenceScore").cast(DecimalType(9, 4)).alias("ConfidenceScore"),
        F.col("PeerPercentileRisk").cast(DecimalType(9, 4)).alias("PeerPercentileRisk"),
        F.col("FailureRiskScore").cast(DecimalType(9, 6)).alias("FailureRiskScore"),
        "DominantReasonCode",
        "RecommendedAction",
        evidence.alias("EvidenceJson"),
        "FeatureSetVersion",
        "ScoringVersion",
        "ModelVersion",
    )
