from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from dagster import AssetKey, DagsterInstance, MetadataValue, build_sensor_context

from alerting.defs.resources.alert_state_store import AssetRowCountObservation
from alerting.defs.sensors.asset_row_count_trend_sensor import (
    _coerce_row_count,
    asset_row_count_trend_sensor,
)


def test_coerce_row_count_accepts_non_negative_integers():
    assert _coerce_row_count(MetadataValue.int(12)) == 12
    assert _coerce_row_count("14") == 14
    assert _coerce_row_count(-1) is None
    assert _coerce_row_count(1.5) is None
    assert _coerce_row_count(True) is None


def test_trend_sensor_records_row_count_materialization():
    instance = DagsterInstance.ephemeral()
    state = MagicMock()
    state.record_asset_row_counts.return_value = 1
    record = MagicMock()
    record.storage_id = 321
    record.run_id = "run-1"
    record.timestamp = datetime(2026, 8, 26, 12, 30, tzinfo=timezone.utc).timestamp()
    record.asset_key = AssetKey(["carlsjr", "dwh_factlecturas"])
    record.asset_materialization.metadata = {
        "row_count": MetadataValue.int(240),
    }
    run = MagicMock()
    run.remote_job_origin.repository_origin.code_location_origin.location_name = (
        "carlsjr"
    )
    config = {
        "defaults": {
            "asset_row_count_trends": {
                "enabled": True,
                "metadata_key": "row_count",
                "backfill_days": 28,
                "batch_size": 500,
            }
        }
    }

    with (
        patch.object(
            instance.event_log_storage,
            "get_event_records",
            return_value=[record],
        ) as get_event_records,
        patch.object(instance, "get_run_by_id", return_value=run),
        patch(
            "alerting.defs.sensors.asset_row_count_trend_sensor._load_config",
            return_value=config,
        ),
    ):
        context = build_sensor_context(
            instance=instance,
            resources={"alert_state": state},
        )
        result = asset_row_count_trend_sensor.evaluate_tick(context)

    event_filter = get_event_records.call_args.args[0]
    assert event_filter.after_cursor is None
    assert event_filter.after_timestamp is not None
    state.record_asset_row_counts.assert_called_once()
    observation = state.record_asset_row_counts.call_args.args[0][0]
    assert observation == AssetRowCountObservation(
        observation_date=date(2026, 8, 26),
        code_location="carlsjr",
        asset_key="carlsjr/dwh_factlecturas",
        metadata_key="row_count",
        row_count=240,
        event_storage_id=321,
        run_id="run-1",
        materialized_at=datetime(2026, 8, 26, 12, 30, tzinfo=timezone.utc),
    )
    assert context.cursor == "321"
    assert "recorded 1 row count observations" in result.skip_message


def test_trend_sensor_ignores_materializations_without_row_count():
    instance = DagsterInstance.ephemeral()
    state = MagicMock()
    record = MagicMock()
    record.storage_id = 322
    record.run_id = "run-2"
    record.timestamp = datetime(2026, 8, 26, 13, 0, tzinfo=timezone.utc).timestamp()
    record.asset_key = AssetKey(["carlsjr", "asset_without_count"])
    record.asset_materialization.metadata = {"merged_rows": MetadataValue.int(10)}

    with (
        patch.object(
            instance.event_log_storage,
            "get_event_records",
            return_value=[record],
        ),
        patch(
            "alerting.defs.sensors.asset_row_count_trend_sensor._load_config",
            return_value={"defaults": {"asset_row_count_trends": {}}},
        ),
    ):
        context = build_sensor_context(
            instance=instance,
            resources={"alert_state": state},
        )
        result = asset_row_count_trend_sensor.evaluate_tick(context)

    state.record_asset_row_counts.assert_not_called()
    assert context.cursor == "322"
    assert "recorded 0 row count observations" in result.skip_message
