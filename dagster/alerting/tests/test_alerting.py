import importlib
import json
import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import yaml
from dagster import build_sensor_context

from alerting.definitions import defs
from alerting.defs.resources.alert_state_store import AlertRecord
from alerting.defs.resources.email_notifier import (
    AlertPayload,
    ConsolidatedPayload,
    EmailNotifierResource,
    _build_consolidated_html,
    _build_consolidated_subject,
    _format_time_ago,
)
from alerting.defs.resources.mssql_reader import (
    MssqlReaderResource,
    SensorAlert,
    SensorDimension,
    SensorRecovery,
)


sensor_module = importlib.import_module(
    "alerting.defs.sensors.operational_alerts_sensor"
)


def test_definitions_bind_sensor_resources():
    assert set(defs.resources) == {
        "mssql_reader",
        "alert_state",
        "email_notifier",
    }


def test_email_uses_user_code_and_alert_type_recipient(monkeypatch):
    monkeypatch.setenv("ALERT_SMTP_HOST", "localhost")
    monkeypatch.setenv("ALERT_SMTP_FROM", "alerts@example.com")
    monkeypatch.setenv("ALERT_SMTP_TO", "default@example.com")
    monkeypatch.setenv("ALERT_SMTP_TO_CARLSJR", "carlsjr@example.com")
    monkeypatch.setenv(
        "ALERT_SMTP_TO_CARLSJR_CONNECTION_LOST",
        "sensors@example.com, support@example.com",
    )
    smtp = MagicMock()
    smtp.__enter__.return_value = smtp
    with patch(
        "alerting.defs.resources.email_notifier.smtplib.SMTP",
        return_value=smtp,
    ):
        sent = EmailNotifierResource().send(
            AlertPayload(
                alert_key="carlsjr::connection_lost::sensor-1",
                user_code="carlsjr",
                alert_type="connection_lost",
                entity_id="sensor-1",
                state="OPEN",
            )
        )

    assert sent is True
    assert smtp.sendmail.call_args.args[1] == [
        "sensors@example.com",
        "support@example.com",
    ]


def test_mssql_uses_user_code_connection_override(monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "shared-host")
    monkeypatch.setenv("MSSQL_USER", "shared-user")
    monkeypatch.setenv("MSSQL_PASSWORD", "shared-password")
    monkeypatch.setenv("CARLSJR_MSSQL_HOST", "carlsjr-host")
    monkeypatch.setenv("CARLSJR_MSSQL_USER", "carlsjr-user")
    monkeypatch.setenv("CARLSJR_MSSQL_PASSWORD", "carlsjr-password")
    with patch("alerting.defs.resources.mssql_reader.pyodbc.connect") as connect:
        MssqlReaderResource()._get_connection("carlsjr", "carlsjr")

    connection_string = connect.call_args.args[0]
    assert "SERVER=carlsjr-host,1433;" in connection_string
    assert "UID=carlsjr-user;" in connection_string
    assert "PWD=carlsjr-password;" in connection_string


def test_sensor_recovery_requires_latest_healthy_audit():
    resumed_at = datetime(2026, 8, 21, 20, 3, 4)
    last_reading_at = datetime(2026, 8, 21, 22, 1, 30)
    row = MagicMock()
    row.SensorId = "sensor-1"
    row.CommunicationResumedAt = resumed_at
    row.LastReadingAt = last_reading_at
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = [row]
    reader = object.__new__(MssqlReaderResource)
    reader._get_connection = MagicMock(return_value=connection)

    recoveries = reader.get_sensor_recoveries(
        "carlsjr", ["sensor-1"], "carlsjr"
    )

    query, parameters = cursor.execute.call_args.args
    assert "latest.IsConnectionLost = 0" in query
    assert "MIN(reading.TimeSpan) AS CommunicationResumedAt" in query
    assert "MAX(reading.TimeSpan) AS LastReadingAt" in query
    assert parameters == ["sensor-1", "sensor-1"]
    assert recoveries["sensor-1"].communication_resumed_at == resumed_at
    assert recoveries["sensor-1"].last_reading_at == last_reading_at


def test_dagster_failure_query_uses_parameterized_interval(monkeypatch):
    monkeypatch.setenv("DAGSTER_PG_USER", "dagster")
    monkeypatch.setenv("DAGSTER_PG_PASSWORD", "dagster")
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = [
        (
            "run-1",
            "job-1",
            json.dumps(
                {
                    "repository_origin": {
                        "code_location_origin": {"location_name": "carlsjr"}
                    }
                }
            ),
            "FAILURE",
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        ),
        (
            "run-2",
            "job-2",
            json.dumps(
                {
                    "external_repository_origin": {
                        "repository_location_origin": {
                            "location_name": "valledelencino"
                        }
                    }
                }
            ),
            "FAILURE",
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        ),
    ]
    with patch.object(sensor_module.psycopg2, "connect", return_value=connection):
        failures = sensor_module._get_recent_dagster_failures(2)

    query, parameters = cursor.execute.call_args.args
    assert "%s * INTERVAL '1 hour'" in query
    assert "remote_job_origin" in query
    assert "external_pipeline_origin" in query
    assert parameters == (2,)
    assert failures[0]["code_location"] == "carlsjr"
    assert failures[1]["code_location"] == "valledelencino"


def _make_base_config(**overrides):
    """Helper to build a minimal sensor config for tests."""
    config = {
        "defaults": {
            "sensor_alerts": {
                "connection_lost": True,
                "late_readings": False,
            },
            "pipeline_alerts": {
                "ingestion_control_failed": False,
                "dagster_run_failure": False,
                "stale_detection": False,
            },
        },
        "user_codes": {"carlsjr": {"database": "carlsjr"}},
    }
    config["defaults"].update(overrides)
    return config


def _make_resources(mssql=None, state=None, email=None):
    """Helper to build a resources dict for tests."""
    if mssql is None:
        mssql = MagicMock()
        mssql.get_connection_lost_sensors.return_value = []
        mssql.get_sensor_recoveries.return_value = {}
        mssql.get_late_reading_sensors.return_value = []
        mssql.get_failed_ingestions.return_value = []
        mssql.get_stale_ingestions.return_value = []
        mssql.get_sensor_dimensions.return_value = {}
    if state is None:
        state = MagicMock()
        state.get_active_alerts.return_value = {}
    if email is None:
        email = MagicMock()
        email.send_consolidated.return_value = True
    return {
        "mssql_reader": mssql,
        "alert_state": state,
        "email_notifier": email,
    }


def test_failed_email_is_not_recorded(monkeypatch):
    config = _make_base_config()
    state = MagicMock()
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    email.send_consolidated.return_value = False
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = [
        SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)
    ]
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    email.send_consolidated.assert_called_once()
    state.upsert_alert.assert_not_called()


def test_alert_lifecycle_opens_new_occurrence_after_resolution(monkeypatch):
    config = _make_base_config(
        daily_reminders={"enabled": False},
    )
    key = "carlsjr::connection_lost::sensor-1"
    alert = AlertRecord(
        alert_key=f"{key}::incident-1",
        user_code="carlsjr",
        alert_type="connection_lost",
        entity_id="sensor-1",
        state="OPEN",
        details=None,
        first_detected_at=datetime.now(timezone.utc),
        last_notified_at=datetime.now(timezone.utc),
        resolved_at=None,
        notification_count=1,
    )
    resolved_alert = AlertRecord(
        alert_key=f"{key}::incident-1",
        user_code="carlsjr",
        alert_type="connection_lost",
        entity_id="sensor-1",
        state="RESOLVED_PENDING",
        details=(
            "CommunicationResumedAt: 2026-08-21 20:03:04 UTC, "
            "LastReadingAt: 2026-08-21 22:01:30 UTC"
        ),
        first_detected_at=alert.first_detected_at,
        last_notified_at=alert.last_notified_at,
        resolved_at=datetime.now(timezone.utc),
        notification_count=1,
    )
    state = MagicMock()
    state.get_active_alerts.side_effect = [
        {}, {key: alert}, {key: alert}, {},
    ]
    state.get_pending_resolved_alerts.side_effect = [
        {}, {}, {}, {resolved_alert.alert_key: resolved_alert},
    ]
    email = MagicMock()
    email.send_consolidated.return_value = True
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.side_effect = [
        [SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)],
        [SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)],
        [],
        [SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)],
    ]
    mssql.get_sensor_recoveries.return_value = {
        "sensor-1": SensorRecovery(
            sensor_id="sensor-1",
            communication_resumed_at=datetime(2026, 8, 21, 20, 3, 4),
            last_reading_at=datetime(2026, 8, 21, 22, 1, 30),
        )
    }
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    uuid_module = MagicMock()
    uuid_module.uuid4.side_effect = ["incident-1", "incident-2"]

    with (
        patch.object(sensor_module, "_load_config", return_value=config),
        patch.object(sensor_module, "uuid", uuid_module),
    ):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)
        sensor_module.operational_alerts_sensor.evaluate_tick(context)
        sensor_module.operational_alerts_sensor.evaluate_tick(context)
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    assert email.send_consolidated.call_count == 2
    calls = email.send_consolidated.call_args_list
    first_open = calls[0].args[0].active_alerts[0]
    resolved = calls[1].args[0].resolved_alerts[0]
    second_open = calls[1].args[0].active_alerts[0]

    assert first_open.state == "OPEN"
    assert first_open.alert_key == f"{key}::incident-1"
    assert resolved.state == "RESOLVED"
    assert resolved.alert_key == f"{key}::incident-1"
    assert resolved.details == (
        "CommunicationResumedAt: 2026-08-21 20:03:04 UTC, "
        "LastReadingAt: 2026-08-21 22:01:30 UTC"
    )
    assert second_open.state == "OPEN"
    assert second_open.alert_key == f"{key}::incident-2"

    assert state.upsert_alert.call_count == 2
    state.mark_resolved.assert_called_once_with(
        f"{key}::incident-1",
        resolved.details,
    )
    state.mark_resolution_notified.assert_called_once_with(
        f"{key}::incident-1"
    )


def test_daily_reminder_groups_alerts_from_both_sensors_by_tenant():
    config = _make_base_config(
        daily_reminders={
            "enabled": True,
            "time": "00:00",
            "timezone": "UTC",
        },
        quiet_hours={"enabled": False},
    )
    sensor_key = "carlsjr::connection_lost::sensor-1"
    trend_key = "carlsjr::asset_row_count_trend::carlsjr/dwh_factlecturas"
    resolved_key = "carlsjr::pipeline_failure::FactLecturas::incident-3"
    last_notified_at = datetime.now(timezone.utc) - timedelta(days=1)
    state = MagicMock()
    state.get_active_alerts.return_value = {
        sensor_key: AlertRecord(
            alert_key=f"{sensor_key}::incident-1",
            user_code="carlsjr",
            alert_type="connection_lost",
            entity_id="sensor-1",
            state="OPEN",
            details="Sensor details",
            first_detected_at=last_notified_at,
            last_notified_at=last_notified_at,
            resolved_at=None,
            notification_count=1,
        ),
        trend_key: AlertRecord(
            alert_key=f"{trend_key}::incident-2",
            user_code="carlsjr",
            alert_type="asset_row_count_trend",
            entity_id="carlsjr/dwh_factlecturas",
            state="OPEN",
            details="Trend details",
            first_detected_at=last_notified_at,
            last_notified_at=datetime.now(timezone.utc),
            resolved_at=None,
            notification_count=0,
        ),
    }
    state.get_pending_resolved_alerts.return_value = {
        resolved_key: AlertRecord(
            alert_key=resolved_key,
            user_code="carlsjr",
            alert_type="pipeline_failure",
            entity_id="FactLecturas",
            state="RESOLVED_PENDING",
            details="Resolved details",
            first_detected_at=last_notified_at,
            last_notified_at=last_notified_at,
            resolved_at=datetime.now(timezone.utc),
            notification_count=1,
        )
    }
    email = MagicMock()
    email.send_consolidated.return_value = True
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = [
        SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)
    ]
    mssql.get_sensor_recoveries.return_value = {}
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}

    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    email.send_consolidated.assert_called_once()
    payload = email.send_consolidated.call_args.args[0]
    assert payload.user_code == "carlsjr"
    assert payload.recipient_alert_type is None
    assert {alert.alert_type for alert in payload.active_alerts} == {
        "connection_lost",
        "asset_row_count_trend",
    }
    assert {alert.state for alert in payload.active_alerts} == {"REMINDER"}
    assert [alert.alert_key for alert in payload.resolved_alerts] == [
        resolved_key
    ]
    assert state.upsert_alert.call_count == 2
    state.mark_resolution_notified.assert_called_once_with(resolved_key)


def test_connection_lost_is_not_resolved_without_recovery_evidence():
    config = _make_base_config()
    key = "carlsjr::connection_lost::sensor-1"
    state = MagicMock()
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=key,
            user_code="carlsjr",
            alert_type="connection_lost",
            entity_id="sensor-1",
            state="OPEN",
            details="ConnectionLostAt: 2026-08-16 20:10:49",
            first_detected_at=datetime.now(timezone.utc),
            last_notified_at=datetime.now(timezone.utc),
            resolved_at=None,
            notification_count=1,
        )
    }
    resources = _make_resources(state=state)

    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(resources=resources)
        result = sensor_module.operational_alerts_sensor.evaluate_tick(context)

    resources["email_notifier"].send_consolidated.assert_not_called()
    state.mark_resolved.assert_not_called()
    assert "1 active conditions" in result.skip_message
    assert "0 resolved" in result.skip_message


def test_alert_config_covers_active_user_codes():
    with open(
        "src/alerting/alert_config.yaml",
        "r",
        encoding="utf-8",
    ) as config_file:
        config = yaml.safe_load(config_file)

    assert set(config["user_codes"]) == {
        "bafar_poc_gabinete",
        "valledelencino",
        "carlsjr",
        "emerson",
        "bafar",
    }
    assert config["defaults"]["asset_row_count_trends"][
        "minimum_observed_days_7d"
    ] == 7
    assert config["defaults"]["asset_row_count_trends"][
        "minimum_observed_days_28d"
    ] == 28


def test_quiet_hours_suppress_only_reminder_email():
    config = {
        "defaults": {
            "sensor_alerts": {
                "connection_lost": True,
                "late_readings": False,
            },
            "pipeline_alerts": {
                "ingestion_control_failed": False,
                "dagster_run_failure": False,
                "stale_detection": False,
            },
            "daily_reminders": {
                "enabled": True,
                "time": "00:00",
                "timezone": "UTC",
            },
            "quiet_hours": {
                "enabled": True,
                "start": "00:00",
                "end": "23:59:59.999999",
                "timezone": "UTC",
            },
        },
        "user_codes": {"carlsjr": {"database": "carlsjr"}},
    }
    key = "carlsjr::connection_lost::sensor-1"
    state = MagicMock()
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=f"{key}::incident-1",
            user_code="carlsjr",
            alert_type="connection_lost",
            entity_id="sensor-1",
            state="OPEN",
            details=None,
            first_detected_at=datetime.now(timezone.utc),
            last_notified_at=datetime.now(timezone.utc) - timedelta(days=1),
            resolved_at=None,
            notification_count=1,
        )
    }
    email = MagicMock()
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = [
        SensorAlert(sensor_id="sensor-1", minutes_without_readings=90)
    ]
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    email.send_consolidated.assert_not_called()
    state.upsert_alert.assert_not_called()


def test_trend_alert_does_not_create_daily_monitoring_email():
    key = "carlsjr::asset_row_count_trend::carlsjr/dwh_factlecturas"
    state = MagicMock()
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=f"{key}::incident-1",
            user_code="carlsjr",
            alert_type="asset_row_count_trend",
            entity_id="carlsjr/dwh_factlecturas",
            state="OPEN",
            details=None,
            first_detected_at=datetime.now(timezone.utc),
            last_notified_at=datetime.now(timezone.utc) - timedelta(days=1),
            resolved_at=None,
            notification_count=1,
        )
    }
    email = MagicMock()

    config = _make_base_config(
        daily_reminders={
            "enabled": True,
            "time": "00:00",
            "timezone": "UTC",
        },
        quiet_hours={"enabled": False},
    )

    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    email.send_consolidated.assert_not_called()
    state.mark_resolved.assert_not_called()


# ── New tests ─────────────────────────────────────────────────────

def test_consolidated_email_groups_by_user_code():
    """Multiple alerts for the same user_code produce ONE send_consolidated call."""
    config = {
        "defaults": {
            "sensor_alerts": {
                "connection_lost": True,
                "late_readings": False,
            },
            "pipeline_alerts": {
                "ingestion_control_failed": False,
                "dagster_run_failure": False,
                "stale_detection": False,
            },
        },
        "user_codes": {"carlsjr": {"database": "carlsjr"}},
    }
    state = MagicMock()
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    email.send_consolidated.return_value = True
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = [
        SensorAlert(sensor_id="sensor-1", minutes_without_readings=10),
        SensorAlert(sensor_id="sensor-2", minutes_without_readings=20),
        SensorAlert(sensor_id="sensor-3", minutes_without_readings=30),
    ]
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    # Only ONE consolidated email for carlsjr, containing 3 alerts
    email.send_consolidated.assert_called_once()
    payload = email.send_consolidated.call_args.args[0]
    assert isinstance(payload, ConsolidatedPayload)
    assert payload.user_code == "carlsjr"
    assert len(payload.active_alerts) == 3


def test_sensor_dimensions_enrich_alerts():
    """Sensor alerts are enriched with location/device/sensor names."""
    config = _make_base_config()
    state = MagicMock()
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    email.send_consolidated.return_value = True
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = [
        SensorAlert(sensor_id="sensor-abc", minutes_without_readings=60),
    ]
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {
        "sensor-abc": SensorDimension(
            sensor_id="sensor-abc",
            location_name="Sucursal Centro",
            sublocation_name="Cuarto Frío 1",
            device_name="Refrigerador A",
            sensor_name="Temp Interior",
        ),
    }
    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    payload = email.send_consolidated.call_args.args[0]
    alert = payload.active_alerts[0]
    assert alert.location_name == "Sucursal Centro"
    assert alert.sublocation_name == "Cuarto Frío 1"
    assert alert.device_name == "Refrigerador A"
    assert alert.sensor_name == "Temp Interior"


def test_pipeline_alerts_in_consolidated_without_dimensions():
    """Pipeline alerts appear in consolidated email without spatial hierarchy."""
    config = {
        "defaults": {
            "sensor_alerts": {
                "connection_lost": False,
                "late_readings": False,
            },
            "pipeline_alerts": {
                "ingestion_control_failed": True,
                "dagster_run_failure": False,
                "stale_detection": False,
            },
        },
        "user_codes": {"carlsjr": {"database": "carlsjr"}},
    }
    from alerting.defs.resources.mssql_reader import IngestionAlert

    state = MagicMock()
    state.get_active_alerts.return_value = {}
    email = MagicMock()
    email.send_consolidated.return_value = True
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = []
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = [
        IngestionAlert(
            ingestion_name="FactLecturas",
            last_run_status="FAILED",
            last_run_error="Timeout",
        ),
    ]
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    payload = email.send_consolidated.call_args.args[0]
    alert = payload.active_alerts[0]
    assert alert.alert_type == "pipeline_failure"
    assert alert.entity_id == "FactLecturas"
    # No dimension fields set for pipeline alerts
    assert alert.location_name is None
    assert alert.sensor_name is None
    # get_sensor_dimensions should NOT be called (no sensor alerts)
    mssql.get_sensor_dimensions.assert_not_called()


def test_consolidated_subject_format():
    payload = ConsolidatedPayload(
        user_code="carlsjr",
        active_alerts=[
            AlertPayload(
                alert_key="k1", user_code="carlsjr",
                alert_type="connection_lost", entity_id="s1", state="OPEN",
            ),
            AlertPayload(
                alert_key="k2",
                user_code="carlsjr",
                alert_type="pipeline_failure",
                entity_id="ingestion",
                state="OPEN",
            ),
        ],
        resolved_alerts=[
            AlertPayload(
                alert_key="k3",
                user_code="carlsjr",
                alert_type="dagster_run_failure",
                entity_id="run-1",
                state="RESOLVED",
            ),
        ],
    )
    subject = _build_consolidated_subject(payload)
    assert subject.startswith("[MONITOREO DE DATOS] CARLSJR")
    assert "2 activas" in subject
    assert "1 resuelta" in subject


def test_consolidated_subject_uses_alert_type_title():
    titles = {
        "connection_lost": "ALERTAS DE SENSORES",
        "late_readings": "ALERTAS DE SENSORES",
        "pipeline_failure": "MONITOREO INGESTA DE DATOS",
        "pipeline_stale": "ALERTAS PIPELINES DE DATOS",
        "dagster_run_failure": "ALERTAS DAGSTER",
        "asset_row_count_trend": "TENDENCIAS DE LECTURAS",
    }

    for alert_type, title in titles.items():
        payload = ConsolidatedPayload(
            user_code="carlsjr",
            active_alerts=[
                AlertPayload(
                    alert_key="k1",
                    user_code="carlsjr",
                    alert_type=alert_type,
                    entity_id="entity",
                    state="OPEN",
                )
            ],
        )

        assert _build_consolidated_subject(payload).startswith(
            f"[{title}] CARLSJR"
        )


def test_daily_reminder_subject_and_heading():
    payload = ConsolidatedPayload(
        user_code="carlsjr",
        active_alerts=[
            AlertPayload(
                alert_key="k1",
                user_code="carlsjr",
                alert_type="connection_lost",
                entity_id="s1",
                state="REMINDER",
            ),
            AlertPayload(
                alert_key="k2",
                user_code="carlsjr",
                alert_type="asset_row_count_trend",
                entity_id="asset",
                state="REMINDER",
            ),
        ],
        resolved_alerts=[
            AlertPayload(
                alert_key="k3",
                user_code="carlsjr",
                alert_type="pipeline_failure",
                entity_id="ingestion",
                state="RESOLVED",
            )
        ],
    )

    subject = _build_consolidated_subject(payload)
    html = _build_consolidated_html(payload)

    assert subject.startswith("[MONITOREO OPERATIVO] CARLSJR")
    assert "2 alertas pendientes" in subject
    assert "Recordatorio de sensores y pipelines" in html


def test_consolidated_html_contains_all_sections():
    payload = ConsolidatedPayload(
        user_code="carlsjr",
        active_alerts=[
            AlertPayload(
                alert_key="k1", user_code="carlsjr",
                alert_type="connection_lost", entity_id="sensor-1",
                state="OPEN", details="MinutesWithoutReadings: 90",
                location_name="Sucursal Norte",
                sensor_name="Temp Cuarto 1",
            ),
            AlertPayload(
                alert_key="k2", user_code="carlsjr",
                alert_type="pipeline_failure", entity_id="FactLecturas",
                state="OPEN", details="LastRunStatus: FAILED",
            ),
        ],
        resolved_alerts=[
            AlertPayload(
                alert_key="k3", user_code="carlsjr",
                alert_type="connection_lost", entity_id="sensor-2",
                state="RESOLVED",
            ),
        ],
    )
    html = _build_consolidated_html(payload)
    # Header
    assert "CARLSJR" in html
    assert "Alertas de sensores y pipelines" in html
    # Sensor section
    assert "Sensor Desconectado" in html
    assert "Sucursal Norte" in html
    assert "Temp Cuarto 1" in html
    # Pipeline section
    assert "Pipeline Fallido" in html
    assert "FactLecturas" in html
    # Resolved section
    assert "Alertas Resueltas" in html
    # Footer
    assert "sistema de alertas de sensores y pipelines" in html
    assert "background-color:transparent" in html
    assert 'bgcolor="#0f766e"' in html
    assert "Evidencia medida" in html
    assert "#0f0f23" not in html
    assert not any(entity in html for entity in (
        "&#x1F50C;", "&#x23F1;", "&#x274C;", "&#x1F40C;",
        "&#x26A0;", "&#x1F4C9;", "&#x2705;",
    ))


def test_format_time_ago():
    now = datetime.now(timezone.utc)
    assert _format_time_ago(None) == ""
    assert _format_time_ago(now) == "0 min"


def test_dimension_query_not_called_without_sensor_alerts():
    """When there are no sensor-type alerts, get_sensor_dimensions is not called."""
    config = {
        "defaults": {
            "sensor_alerts": {
                "connection_lost": False,
                "late_readings": False,
            },
            "pipeline_alerts": {
                "ingestion_control_failed": False,
                "dagster_run_failure": False,
                "stale_detection": False,
            },
        },
        "user_codes": {"carlsjr": {"database": "carlsjr"}},
    }
    mssql = MagicMock()
    mssql.get_connection_lost_sensors.return_value = []
    mssql.get_late_reading_sensors.return_value = []
    mssql.get_failed_ingestions.return_value = []
    mssql.get_stale_ingestions.return_value = []
    mssql.get_sensor_dimensions.return_value = {}
    state = MagicMock()
    state.get_active_alerts.return_value = {}
    email = MagicMock()

    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources=_make_resources(mssql=mssql, state=state, email=email)
        )
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    mssql.get_sensor_dimensions.assert_not_called()


class LiveSmtpDeliveryTest(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("RUN_LIVE_SMTP_TEST", "").lower() == "true",
        "Set RUN_LIVE_SMTP_TEST=true to enable the live SMTP test",
    )
    def test_real_smtp_delivery_to_test_recipient(self):
        recipient = os.environ.get("ALERT_SMTP_TEST_TO", "")
        self.assertTrue(recipient, "ALERT_SMTP_TEST_TO is required")
        test_id = str(uuid.uuid4())
        with patch.dict(
            os.environ,
            {"ALERT_SMTP_TO_SMTP_TEST_DELIVERY_TEST": recipient},
        ):
            sent = EmailNotifierResource().send(
                AlertPayload(
                    alert_key=f"smtp_test::delivery_test::{test_id}",
                    user_code="smtp_test",
                    alert_type="delivery_test",
                    entity_id=test_id,
                    state="OPEN",
                    details="Controlled live SMTP delivery test",
                )
            )

        self.assertTrue(sent, "SMTP server did not accept the test email")
