from dagster import DefaultSensorStatus, build_sensor_context

from alerting.defs import defs
from alerting.defs.sensors import (
    asset_row_count_trend_sensor,
    operational_alerts_sensor,
)


def test_central_alerting_registers_only_inert_sensors():
    assert {
        sensor.name for sensor in defs.get_repository_def().sensor_defs
    } == {
        "asset_row_count_trend_sensor",
        "operational_alerts_sensor",
    }
    assert defs.resources is None
    for sensor in (
        asset_row_count_trend_sensor,
        operational_alerts_sensor,
    ):
        assert sensor.default_status == DefaultSensorStatus.STOPPED
        assert sensor.required_resource_keys == set()
        result = sensor.evaluate_tick(build_sensor_context())
        assert result.skip_message
