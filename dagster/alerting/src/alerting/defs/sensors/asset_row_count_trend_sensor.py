from dagster import DefaultSensorStatus, SkipReason, sensor


@sensor(
    name="asset_row_count_trend_sensor",
    minimum_interval_seconds=21600,
    default_status=DefaultSensorStatus.STOPPED,
)
def asset_row_count_trend_sensor() -> SkipReason:
    return SkipReason(
        "La detección de tendencias se ejecuta en cada code location tenant."
    )
