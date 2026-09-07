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
from alerting.defs.resources.alert_state_store import (
    AlertRecord,
    AlertStateStoreResource,
)
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
delivery_module = importlib.import_module(
    "alerting.defs.sensors.email_delivery_sensor"
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
        reader = MssqlReaderResource()
        reader._get_connection("carlsjr", "carlsjr")
        read_connection_string = connect.call_args.args[0]
        reader._get_connection("carlsjr", "carlsjr", read_only=False)

    write_connection_string = connect.call_args.args[0]
    assert "SERVER=carlsjr-host,1433;" in read_connection_string
    assert "UID=carlsjr-user;" in read_connection_string
    assert "PWD=carlsjr-password;" in read_connection_string
    assert "ApplicationIntent=ReadOnly;" in read_connection_string
    assert "ApplicationIntent" not in write_connection_string


def test_alert_state_upsert_uses_sql_server_syntax():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    reader = MagicMock()
    reader._get_connection.return_value = connection
    store = AlertStateStoreResource.__new__(AlertStateStoreResource)
    store._mssql_reader = reader

    store.upsert_alert(
        database="carlsjr_db",
        alert_key="alert-1",
        user_code="carlsjr",
        alert_type="connection_lost",
        entity_id="sensor-1",
        details="details",
        trigger_source="sensor_monitoring",
        severity="CRITICAL",
        metadata={"reason": "razón"},
    )

    reader._get_connection.assert_called_once_with(
        "carlsjr_db", "carlsjr", read_only=False
    )
    query = cursor.execute.call_args.args[0]
    assert "IF @@ROWCOUNT = 0" in query
    assert "ON CONFLICT" not in query
    assert "user_code" not in query
    assert "trigger_source" in query
    assert cursor.execute.call_args.args[1:] == (
        "sensor_monitoring",
        "CRITICAL",
        "details",
        '{"reason":"razón"}',
        None,
        3,
        "alert-1",
        "alert-1",
        "sensor_monitoring",
        "connection_lost",
        "sensor-1",
        "CRITICAL",
        "OPEN",
        "OPEN",
        "PENDING",
        "details",
        '{"reason":"razón"}',
        None,
        0,
        3,
    )
    connection.commit.assert_called_once_with()


def test_alert_state_updates_all_resolution_columns():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    reader = MagicMock()
    reader._get_connection.return_value = connection
    store = AlertStateStoreResource.__new__(AlertStateStoreResource)
    store._mssql_reader = reader

    store.mark_resolved(
        "carlsjr_db",
        "carlsjr",
        "alert-1",
        "resolución con acentos",
    )

    resolved_query = cursor.execute.call_args.args[0]
    assert "details = COALESCE(?, details)" in resolved_query
    assert "metadata_json = COALESCE(?, metadata_json)" in resolved_query
    assert "notification_attempt_count = 0" in resolved_query
    assert "resolved_at = SYSUTCDATETIME()" in resolved_query
    assert "updated_at = SYSUTCDATETIME()" in resolved_query
    assert cursor.execute.call_args.args[1:] == (
        "RESOLVED_PENDING",
        "RESOLUTION",
        "PENDING",
        "resolución con acentos",
        None,
        "alert-1",
        "OPEN",
    )

    store.mark_notifications_sent(
        "carlsjr_db", "carlsjr", ["alert-1"]
    )

    notified_query = cursor.execute.call_args.args[0]
    assert "last_notified_at = SYSUTCDATETIME()" in notified_query
    assert "notification_count = notification_count + 1" in notified_query
    assert "updated_at = SYSUTCDATETIME()" in notified_query
    assert cursor.execute.call_args.args[1:] == (
        "RESOLUTION",
        "RESOLVED",
        "SENT",
        "alert-1",
        "PROCESSING",
    )


def test_alert_state_maps_all_sql_server_types():
    now = datetime.now(timezone.utc)
    row = (
        "alert-1",
        "sensor_monitoring",
        "connection_lost",
        "sensor-1",
        "CRITICAL",
        "OPEN",
        "REMINDER",
        "RETRY_PENDING",
        "detalle",
        '{"measurements":{"minutes_without_readings":90}}',
        None,
        now,
        now,
        now,
        now,
        now,
        now,
        None,
        2,
        1,
        3,
        "SMTP_TIMEOUT",
        "timeout",
        now,
        now,
        now,
    )

    alert = AlertStateStoreResource._alert_record_from_row(row, "carlsjr")

    assert alert.user_code == "carlsjr"
    assert alert.trigger_source == "sensor_monitoring"
    assert alert.severity == "CRITICAL"
    assert alert.metadata == {
        "measurements": {"minutes_without_readings": 90}
    }
    assert alert.notification_count == 2
    assert alert.notification_attempt_count == 1
    assert alert.max_notification_attempts == 3
    assert alert.next_retry_at == now
    assert alert.last_error_code == "SMTP_TIMEOUT"
    assert alert.ticket_id is None


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


def test_alert_state_reads_each_tenant_database(monkeypatch):
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
        "user_codes": {
            "carlsjr": {"database": "carlsjr_db"},
            "emerson": {"database_env": "EMERSON_TEST_DB"},
        },
    }
    monkeypatch.setenv("EMERSON_TEST_DB", "emerson_db")
    state = MagicMock()
    state.get_active_alerts.return_value = {}

    with patch.object(sensor_module, "_load_config", return_value=config):
        context = build_sensor_context(resources=_make_resources(state=state))
        sensor_module.operational_alerts_sensor.evaluate_tick(context)

    assert [
        state_call.args for state_call in state.get_active_alerts.call_args_list
    ] == [("carlsjr_db", "carlsjr"), ("emerson_db", "emerson")]
    state.get_pending_resolved_alerts.assert_not_called()


def test_detection_is_recorded_without_sending_email(monkeypatch):
    config = _make_base_config()
    state = MagicMock()
    state.get_active_alerts.return_value = {}
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
    state.upsert_alert.assert_called_once()
    call = state.upsert_alert.call_args.kwargs
    assert call["database"] == "carlsjr"
    assert call["user_code"] == "carlsjr"
    assert call["trigger_source"] == "sensor_monitoring"
    assert call["metadata"]["reason"]
    assert call["metadata"]["measurements"] == {
        "minutes_without_readings": 90,
        "connection_lost_at": None,
    }


def test_email_delivery_claims_and_sends_tenant_notifications():
    detected_at = datetime.now(timezone.utc) - timedelta(hours=1)
    operational = AlertRecord(
        alert_key="operational-1",
        user_code="carlsjr",
        trigger_source="sensor_monitoring",
        alert_type="connection_lost",
        entity_id="sensor-1",
        state="OPEN",
        details="details",
        first_detected_at=detected_at,
        last_notified_at=None,
        resolved_at=None,
        notification_count=0,
        notification_status="PENDING",
        metadata={
            "reason": "Sin comunicación.",
            "context": {"location_name": "Sucursal Centro"},
        },
    )
    trend = AlertRecord(
        alert_key="trend-1",
        user_code="carlsjr",
        trigger_source="dagster",
        alert_type="asset_row_count_trend",
        entity_id="asset-1",
        state="OPEN",
        details="trend",
        first_detected_at=detected_at,
        last_notified_at=None,
        resolved_at=None,
        notification_count=0,
        notification_status="PENDING",
        metadata={"reason": "Caída de volumen."},
    )
    resolution = AlertRecord(
        alert_key="resolution-1",
        user_code="carlsjr",
        trigger_source="sensor_monitoring",
        alert_type="pipeline_failure",
        entity_id="FactLecturas",
        state="RESOLVED_PENDING",
        details="resolved",
        first_detected_at=detected_at,
        last_notified_at=detected_at,
        resolved_at=datetime.now(timezone.utc),
        notification_count=1,
        notification_type="RESOLUTION",
        notification_status="PENDING",
        metadata={"reason": "La condición dejó de detectarse."},
    )
    state = MagicMock()
    state.get_deliverable_notifications.return_value = [
        operational,
        trend,
        resolution,
    ]
    state.claim_notifications.side_effect = (
        lambda _database, _user_code, alert_keys, _timeout: alert_keys
    )
    email = MagicMock()
    email.send_consolidated.return_value = True
    config = {
        "defaults": {"email_delivery": {}},
        "user_codes": {"carlsjr": {"database": "carlsjr_db"}},
    }

    with patch.object(delivery_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources={"alert_state": state, "email_notifier": email}
        )
        delivery_module.email_delivery_sensor.evaluate_tick(context)

    payload = email.send_consolidated.call_args.args[0]
    assert payload.user_code == "carlsjr"
    assert {alert.alert_key for alert in payload.active_alerts} == {
        "operational-1",
        "trend-1",
    }
    assert [alert.alert_key for alert in payload.resolved_alerts] == [
        "resolution-1"
    ]
    assert payload.active_alerts[0].location_name == "Sucursal Centro"
    state.mark_notifications_sent.assert_called_once_with(
        "carlsjr_db",
        "carlsjr",
        ["operational-1", "trend-1", "resolution-1"],
    )


def test_email_delivery_does_not_send_trend_alert_standalone():
    trend = AlertRecord(
        alert_key="trend-1",
        user_code="carlsjr",
        trigger_source="dagster",
        alert_type="asset_row_count_trend",
        entity_id="asset-1",
        state="OPEN",
        details=None,
        first_detected_at=datetime.now(timezone.utc),
        last_notified_at=None,
        resolved_at=None,
        notification_count=0,
        notification_status="PENDING",
    )
    state = MagicMock()
    state.get_deliverable_notifications.return_value = [trend]
    email = MagicMock()
    config = {
        "defaults": {"email_delivery": {}},
        "user_codes": {"carlsjr": {"database": "carlsjr_db"}},
    }

    with patch.object(delivery_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources={"alert_state": state, "email_notifier": email}
        )
        delivery_module.email_delivery_sensor.evaluate_tick(context)

    state.claim_notifications.assert_not_called()
    email.send_consolidated.assert_not_called()


def test_email_delivery_records_retry_error_and_delay():
    alert = AlertRecord(
        alert_key="operational-1",
        user_code="carlsjr",
        trigger_source="sensor_monitoring",
        alert_type="connection_lost",
        entity_id="sensor-1",
        state="OPEN",
        details=None,
        first_detected_at=datetime.now(timezone.utc),
        last_notified_at=None,
        resolved_at=None,
        notification_count=0,
        notification_status="RETRY_PENDING",
        notification_attempt_count=2,
    )
    state = MagicMock()
    state.get_deliverable_notifications.return_value = [alert]
    state.claim_notifications.return_value = [alert.alert_key]
    email = MagicMock()
    email.send_consolidated.return_value = False
    email.last_error_code = "SMTPException"
    email.last_error_message = "timeout"
    config = {
        "defaults": {
            "email_delivery": {"retry_delays_minutes": [5, 15, 60]}
        },
        "user_codes": {"carlsjr": {"database": "carlsjr_db"}},
    }

    with patch.object(delivery_module, "_load_config", return_value=config):
        context = build_sensor_context(
            resources={"alert_state": state, "email_notifier": email}
        )
        delivery_module.email_delivery_sensor.evaluate_tick(context)

    state.mark_notifications_failed.assert_called_once_with(
        "carlsjr_db",
        "carlsjr",
        ["operational-1"],
        "SMTPException",
        "timeout",
        60,
    )


def test_alert_lifecycle_opens_new_occurrence_after_resolution(monkeypatch):
    config = _make_base_config(
        daily_reminders={"enabled": False},
    )
    key = "carlsjr::connection_lost::sensor-1"
    alert = AlertRecord(
        alert_key=f"{key}::incident-1",
        user_code="carlsjr",
        trigger_source="sensor_monitoring",
        alert_type="connection_lost",
        entity_id="sensor-1",
        state="OPEN",
        details=None,
        first_detected_at=datetime.now(timezone.utc),
        last_notified_at=datetime.now(timezone.utc),
        resolved_at=None,
        notification_count=1,
    )
    state = MagicMock()
    state.get_active_alerts.side_effect = [
        {}, {key: alert}, {key: alert}, {},
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

    email.send_consolidated.assert_not_called()
    assert state.upsert_alert.call_count == 3
    assert [
        state_call.kwargs["alert_key"]
        for state_call in state.upsert_alert.call_args_list
    ] == [
        f"{key}::incident-1",
        f"{key}::incident-1",
        f"{key}::incident-2",
    ]
    resolved_call = state.mark_resolved.call_args
    assert resolved_call.args[:3] == (
        "carlsjr",
        "carlsjr",
        f"{key}::incident-1",
    )
    assert resolved_call.args[3] == (
        "CommunicationResumedAt: 2026-08-21 20:03:04 UTC, "
        "LastReadingAt: 2026-08-21 22:01:30 UTC"
    )
    assert resolved_call.args[4]["evidence"] == {
        "communication_resumed_at": "2026-08-21T20:03:04",
        "last_reading_at": "2026-08-21T22:01:30",
    }
    state.mark_resolved.assert_called_once()
    state.mark_resolution_notified.assert_not_called()


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
    last_notified_at = datetime.now(timezone.utc) - timedelta(days=1)
    state = MagicMock()
    state.get_active_alerts.return_value = {
        sensor_key: AlertRecord(
            alert_key=f"{sensor_key}::incident-1",
            user_code="carlsjr",
            trigger_source="sensor_monitoring",
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
            trigger_source="dagster",
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

    email.send_consolidated.assert_not_called()
    state.upsert_alert.assert_called_once()
    assert {
        state_call.args[2]
        for state_call in state.queue_reminder.call_args_list
    } == {
        f"{sensor_key}::incident-1",
        f"{trend_key}::incident-2",
    }


def test_connection_lost_is_not_resolved_without_recovery_evidence():
    config = _make_base_config()
    key = "carlsjr::connection_lost::sensor-1"
    state = MagicMock()
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=key,
            user_code="carlsjr",
            trigger_source="sensor_monitoring",
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
    assert "0 resolutions queued" in result.skip_message


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
            trigger_source="sensor_monitoring",
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
    state.upsert_alert.assert_called_once()
    state.queue_reminder.assert_not_called()


def test_trend_alert_does_not_create_daily_monitoring_email():
    key = "carlsjr::asset_row_count_trend::carlsjr/dwh_factlecturas"
    state = MagicMock()
    state.get_active_alerts.return_value = {
        key: AlertRecord(
            alert_key=f"{key}::incident-1",
            user_code="carlsjr",
            trigger_source="dagster",
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

def test_detector_persists_multiple_alerts_in_tenant_database():
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

    email.send_consolidated.assert_not_called()
    assert state.upsert_alert.call_count == 3
    assert {
        state_call.kwargs["database"]
        for state_call in state.upsert_alert.call_args_list
    } == {"carlsjr"}
    assert {
        state_call.kwargs["trigger_source"]
        for state_call in state.upsert_alert.call_args_list
    } == {"sensor_monitoring"}


def test_sensor_dimensions_are_persisted_in_metadata():
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

    email.send_consolidated.assert_not_called()
    metadata = state.upsert_alert.call_args.kwargs["metadata"]
    assert metadata["context"]["location_name"] == "Sucursal Centro"
    assert metadata["context"]["sublocation_name"] == "Cuarto Frío 1"
    assert metadata["context"]["device_name"] == "Refrigerador A"
    assert metadata["context"]["sensor_name"] == "Temp Interior"


def test_pipeline_alerts_are_persisted_without_dimensions():
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

    email.send_consolidated.assert_not_called()
    call = state.upsert_alert.call_args.kwargs
    assert call["alert_type"] == "pipeline_failure"
    assert call["entity_id"] == "FactLecturas"
    assert "context" not in call["metadata"]
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
                metadata={
                    "reason": "El sensor perdió comunicación.",
                    "measurements": {"minutes_without_readings": 90},
                },
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
    assert "Justificaci&oacute;n" in html
    assert "El sensor perdió comunicación." in html
    assert "Trazabilidad" in html
    assert "minutes_without_readings" in html
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
