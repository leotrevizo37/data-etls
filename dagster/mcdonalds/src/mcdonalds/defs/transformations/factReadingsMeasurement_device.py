from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    when,
    lit,
)

from ...device_status_rules import DEVICE_STATUS_RULES, DEFAULT_DEVICE_STATUS_CODE


def _rule_condition(expected_values):
    condition = None
    for field_name, expected_value in expected_values.items():
        field_expr = (
            col(field_name).isNull()
            if expected_value is None
            else col(field_name) == lit(expected_value)
        )
        condition = field_expr if condition is None else (condition & field_expr)
    return condition


def factReadingsMeasurement_device(
        factoperations_df: DataFrame) -> DataFrame:

    sensor_typed_df = (
        factoperations_df
        .groupBy("OperationId", "DeviceId", "LocationId", "LocalTimeSpan", "SensorTyId", "_sensor_type")
        .agg(
            when(
                spark_min(col("StatusCode")) == 1, 1
            ).otherwise(3).alias("StatusCode"),
            spark_sum("Anomalies").alias("Anomalies"),
        )
    )

    grouped = (
        sensor_typed_df
        .groupBy("OperationId", "DeviceId", "LocationId", "LocalTimeSpan")
        .agg(
            spark_sum("Anomalies").alias("Anomalies"),

            spark_max(
                when(
                    (col("_sensor_type") == "Corriente") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Corriente") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("amperage_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Presión") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Presión") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("pressure_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Temperatura") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Temperatura") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("temperature_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Encendido") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Encendido") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("current_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "On/Off Termostato") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "On/Off Termostato") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("termostatus"),
        )
    )

    result = (
        grouped
        .withColumn(
            "StatusCode",
            _build_status_expression()
        )
        .select(
            col("DeviceId"),
            col("OperationId"),
            col("LocalTimeSpan"),
            col("StatusCode"),
            col("Anomalies"),
        )
    )

    return result


def _build_status_expression():
    expression = None

    for expected_values, resolved_code in DEVICE_STATUS_RULES:
        rule_expression = _rule_condition(expected_values)
        if expression is None:
            expression = when(rule_expression, lit(resolved_code))
        else:
            expression = expression.when(rule_expression, lit(resolved_code))

    return expression.otherwise(lit(DEFAULT_DEVICE_STATUS_CODE))
