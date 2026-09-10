from dagster import DefaultSensorStatus, SkipReason, sensor


@sensor(
    name="operational_alerts_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
)
def operational_alerts_sensor() -> SkipReason:
    return SkipReason(
        "La detección operativa se ejecuta en cada code location tenant."
    )
