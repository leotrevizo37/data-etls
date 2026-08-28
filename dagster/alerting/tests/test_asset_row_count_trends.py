from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from dagster import AssetKey, DagsterInstance, MetadataValue, build_sensor_context

from alerting.defs.resources.alert_state_store import (
    AlertRecord,
    AlertStateStoreResource,
    AssetRowCountObservation,
    AssetRowCountTrend,
)
from alerting.defs.resources.email_notifier import (
    AlertPayload,
    ConsolidatedPayload,
    EmailNotifierResource,
    _build_consolidated_html,
    _build_consolidated_subject,
)
from alerting.defs.sensors.asset_row_count_trend_sensor import (
    _coerce_row_count,
    asset_row_count_trend_sensor,
)


def test_coerce_row_count_accepts_non_negative_integers():
    assert asset_row_count_trend_sensor.minimum_interval_seconds == 21600
    assert asset_row_count_trend_sensor.required_resource_keys == {
        "alert_state"
    }
    assert _coerce_row_count(MetadataValue.int(12)) == 12
    assert _coerce_row_count("14") == 14
    assert _coerce_row_count(-1) is None
    assert _coerce_row_count(1.5) is None
    assert _coerce_row_count(True) is None


def test_latest_trends_query_returns_only_closed_days():
    store = AlertStateStoreResource.__new__(AlertStateStoreResource)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = [
        (
            date(2026, 8, 26),
            "carlsjr",
            "carlsjr/dwh_factlecturas",
            "row_count",
            2,
            120,
            100,
            110,
            115,
            7,
            28,
            20,
            9.0909,
            4.3478,
            "run-1",
            datetime(2026, 8, 26, 20, 0, tzinfo=timezone.utc),
        )
    ]

    with patch.object(store, "_get_connection", return_value=connection):
        trends = store.get_latest_asset_row_count_trends()

    query = cursor.execute.call_args.args[0]
    assert "observation_date < (NOW() AT TIME ZONE 'UTC')::DATE" in query
    assert trends == [
        AssetRowCountTrend(
            observation_date=date(2026, 8, 26),
            code_location="carlsjr",
            asset_key="carlsjr/dwh_factlecturas",
            metadata_key="row_count",
            daily_materialization_count=2,
            daily_row_count=120,
            average_1d=100,
            average_7d=110,
            average_28d=115,
            observed_days_7d=7,
            observed_days_28d=28,
            change_vs_1d_pct=20,
            change_vs_7d_pct=9.0909,
            change_vs_28d_pct=4.3478,
            latest_run_id="run-1",
            latest_materialized_at=datetime(
                2026, 8, 26, 20, 0, tzinfo=timezone.utc
            ),
        )
    ]


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
            resources={
                "alert_state": state,
                "email_notifier": MagicMock(),
            },
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
            resources={
                "alert_state": state,
                "email_notifier": MagicMock(),
            },
        )
        result = asset_row_count_trend_sensor.evaluate_tick(context)

    state.record_asset_row_counts.assert_not_called()
    assert context.cursor == "322"
    assert "recorded 0 row count observations" in result.skip_message


def test_trend_sensor_records_open_alert_without_sending_email():
    instance = DagsterInstance.ephemeral()
    state = MagicMock()
    state.get_latest_asset_row_count_trends.return_value = [
        AssetRowCountTrend(
            observation_date=date(2026, 8, 26),
            code_location="carlsjr",
            asset_key="carlsjr/dwh_factlecturas",
            metadata_key="row_count",
            daily_materialization_count=1,
            daily_row_count=50,
            average_1d=100,
            average_7d=110,
            average_28d=120,
            observed_days_7d=7,
            observed_days_28d=28,
            change_vs_1d_pct=-50,
            change_vs_7d_pct=-54.5455,
            change_vs_28d_pct=-58.3333,
            latest_run_id="run-3",
            latest_materialized_at=datetime(
                2026, 8, 26, 22, 0, tzinfo=timezone.utc
            ),
        )
    ]
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    email.send_consolidated.return_value = True
    config = {
        "defaults": {
            "asset_row_count_trends": {
                "enabled": True,
                "notifications_enabled": True,
                "warning_drop_pct": 20,
                "critical_drop_pct": 40,
                "minimum_observed_days_7d": 3,
                "minimum_observed_days_28d": 7,
            },
        },
        "user_codes": {"carlsjr": {}},
    }

    with (
        patch.object(
            instance.event_log_storage,
            "get_event_records",
            return_value=[],
        ),
        patch(
            "alerting.defs.sensors.asset_row_count_trend_sensor._load_config",
            return_value=config,
        ),
    ):
        context = build_sensor_context(
            instance=instance,
            cursor="10",
            resources={
                "alert_state": state,
                "email_notifier": email,
            },
        )
        result = asset_row_count_trend_sensor.evaluate_tick(context)

    email.send_consolidated.assert_not_called()
    state.upsert_alert.assert_called_once()
    call = state.upsert_alert.call_args.kwargs
    assert call["alert_key"].startswith(
        "carlsjr::asset_row_count_trend::carlsjr/dwh_factlecturas::"
    )
    assert call["notification_count"] == 0
    assert call["state"] == "OPEN"
    assert "Severity: CRITICAL" in call["details"]
    assert "Average28d: 120.00" in call["details"]
    assert "1 opened" in result.skip_message


def test_trend_sensor_requires_complete_7d_and_28d_history():
    instance = DagsterInstance.ephemeral()
    state = MagicMock()
    state.get_latest_asset_row_count_trends.return_value = [
        AssetRowCountTrend(
            observation_date=date(2026, 8, 26),
            code_location="carlsjr",
            asset_key="carlsjr/dwh_factlecturas",
            metadata_key="row_count",
            daily_materialization_count=1,
            daily_row_count=90,
            average_1d=100,
            average_7d=100,
            average_28d=140,
            observed_days_7d=6,
            observed_days_28d=27,
            change_vs_1d_pct=-10,
            change_vs_7d_pct=-50,
            change_vs_28d_pct=-50,
            latest_run_id="run-4",
            latest_materialized_at=datetime(
                2026, 8, 26, 22, 0, tzinfo=timezone.utc
            ),
        )
    ]
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    config = {
        "defaults": {
            "asset_row_count_trends": {
                "notifications_enabled": True,
                "warning_drop_pct": 20,
                "minimum_observed_days_7d": 7,
                "minimum_observed_days_28d": 28,
            },
        },
        "user_codes": {"carlsjr": {}},
    }

    with (
        patch.object(
            instance.event_log_storage,
            "get_event_records",
            return_value=[],
        ),
        patch(
            "alerting.defs.sensors.asset_row_count_trend_sensor._load_config",
            return_value=config,
        ),
    ):
        context = build_sensor_context(
            instance=instance,
            cursor="10",
            resources={
                "alert_state": state,
                "email_notifier": email,
            },
        )
        asset_row_count_trend_sensor.evaluate_tick(context)

    email.send_consolidated.assert_not_called()
    state.upsert_alert.assert_not_called()


def test_trend_sensor_queues_resolution_without_sending_email():
    instance = DagsterInstance.ephemeral()
    key = "carlsjr::asset_row_count_trend::carlsjr/dwh_factlecturas"
    state = MagicMock()
    state.get_latest_asset_row_count_trends.return_value = []
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=key,
            user_code="carlsjr",
            alert_type="asset_row_count_trend",
            entity_id="carlsjr/dwh_factlecturas",
            state="OPEN",
            details=None,
            first_detected_at=datetime.now(timezone.utc),
            last_notified_at=datetime.now(timezone.utc),
            resolved_at=None,
            notification_count=1,
        )
    }
    email = MagicMock()
    config = {
        "defaults": {
            "asset_row_count_trends": {
                "notifications_enabled": True,
                "warning_drop_pct": 20,
                "critical_drop_pct": 40,
                "minimum_observed_days_7d": 3,
                "minimum_observed_days_28d": 7,
            },
        },
        "user_codes": {"carlsjr": {}},
    }

    with (
        patch.object(
            instance.event_log_storage,
            "get_event_records",
            return_value=[],
        ),
        patch(
            "alerting.defs.sensors.asset_row_count_trend_sensor._load_config",
            return_value=config,
        ),
    ):
        context = build_sensor_context(
            instance=instance,
            cursor="10",
            resources={
                "alert_state": state,
                "email_notifier": email,
            },
        )
        result = asset_row_count_trend_sensor.evaluate_tick(context)

    email.send_consolidated.assert_not_called()
    state.upsert_alert.assert_not_called()
    state.mark_resolved.assert_called_once_with(key, None)
    assert "0 opened, 1 resolved" in result.skip_message


def test_daily_monitoring_uses_tenant_specific_recipients(monkeypatch):
    monkeypatch.setenv("ALERT_SMTP_HOST", "localhost")
    monkeypatch.setenv("ALERT_SMTP_FROM", "alerts@example.com")
    monkeypatch.setenv("ALERT_SMTP_TO", "default@example.com")
    monkeypatch.setenv("ALERT_SMTP_TO_CARLSJR", "monitoring@example.com")
    monkeypatch.setenv(
        "ALERT_SMTP_TO_CARLSJR_ASSET_ROW_COUNT_TREND",
        "quality@example.com",
    )
    smtp = MagicMock()
    smtp.__enter__.return_value = smtp
    payload = ConsolidatedPayload(
        user_code="carlsjr",
        active_alerts=[
            AlertPayload(
                alert_key="carlsjr::connection_lost::sensor",
                user_code="carlsjr",
                alert_type="connection_lost",
                entity_id="sensor",
                state="REMINDER",
            ),
            AlertPayload(
                alert_key="carlsjr::asset_row_count_trend::asset",
                user_code="carlsjr",
                alert_type="asset_row_count_trend",
                    entity_id="asset",
                    state="REMINDER",
                ),
        ],
    )

    with patch(
        "alerting.defs.resources.email_notifier.smtplib.SMTP",
        return_value=smtp,
    ):
        sent = EmailNotifierResource().send_consolidated(payload)

    assert sent is True
    assert smtp.sendmail.call_args.args[1] == ["monitoring@example.com"]
    html = _build_consolidated_html(payload)
    assert _build_consolidated_subject(payload).startswith(
        "[MONITOREO OPERATIVO] CARLSJR"
    )
    assert "Recordatorio de sensores y pipelines" in html
    assert "Caída de Volumen de Lecturas" in html
