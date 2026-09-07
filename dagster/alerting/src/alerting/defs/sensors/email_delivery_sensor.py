"""Deliver pending tenant alerts by email and persist each attempt."""

from typing import Dict

from dagster import (
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..resources.alert_state_store import (
    ALERT_STATE_RESOLVED_PENDING,
    NOTIFICATION_TYPE_REMINDER,
    NOTIFICATION_TYPE_RESOLUTION,
    AlertRecord,
    AlertStateStoreResource,
)
from ..resources.email_notifier import (
    AlertPayload,
    ConsolidatedPayload,
    EmailNotifierResource,
)
from .operational_alerts_sensor import (
    _OPERATIONAL_ALERT_TYPES,
    _load_config,
    _resolve_user_code_databases,
)


def _to_payload(alert: AlertRecord) -> AlertPayload:
    context = alert.metadata.get("context", {})
    if not isinstance(context, dict):
        context = {}
    if alert.notification_type == NOTIFICATION_TYPE_RESOLUTION:
        state = "RESOLVED"
    elif alert.notification_type == NOTIFICATION_TYPE_REMINDER:
        state = "REMINDER"
    else:
        state = "OPEN"
    return AlertPayload(
        alert_key=alert.alert_key,
        user_code=alert.user_code,
        alert_type=alert.alert_type,
        entity_id=alert.entity_id,
        state=state,
        details=alert.details,
        notification_count=alert.notification_count + 1,
        location_name=context.get("location_name"),
        sublocation_name=context.get("sublocation_name"),
        device_name=context.get("device_name"),
        sensor_name=context.get("sensor_name"),
        first_detected_at=alert.first_detected_at,
        metadata=alert.metadata,
    )


@sensor(
    name="email_delivery_sensor",
    minimum_interval_seconds=300,
    required_resource_keys={"alert_state", "email_notifier"},
    default_status=DefaultSensorStatus.RUNNING,
)
def email_delivery_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    state_store: AlertStateStoreResource = context.resources.alert_state
    email: EmailNotifierResource = context.resources.email_notifier
    config = _load_config()
    defaults = config.get("defaults", {})
    delivery_cfg = defaults.get("email_delivery", {})
    processing_timeout_minutes = max(
        1, int(delivery_cfg.get("processing_timeout_minutes", 30))
    )
    retry_delays = [
        max(1, int(delay))
        for delay in delivery_cfg.get("retry_delays_minutes", [5, 15, 60])
    ] or [5]
    user_codes_cfg = config.get("user_codes", {})
    databases = _resolve_user_code_databases(user_codes_cfg)

    sent_count = 0
    failed_count = 0
    for user_code, database in databases.items():
        deliverable = list(state_store.get_deliverable_notifications(
            database,
            user_code,
            processing_timeout_minutes,
        ))
        trigger_alerts = [
            alert
            for alert in deliverable
            if alert.alert_type in _OPERATIONAL_ALERT_TYPES
            and alert.state != ALERT_STATE_RESOLVED_PENDING
            and alert.notification_type != NOTIFICATION_TYPE_RESOLUTION
        ]
        if not trigger_alerts:
            continue

        claimed_trigger_keys = set(state_store.claim_notifications(
            database,
            user_code,
            [alert.alert_key for alert in trigger_alerts],
            processing_timeout_minutes,
        ))
        if not claimed_trigger_keys:
            continue
        other_alerts = [
            alert
            for alert in deliverable
            if alert.alert_key not in {
                trigger.alert_key for trigger in trigger_alerts
            }
        ]
        claimed_other_keys = set(state_store.claim_notifications(
            database,
            user_code,
            [alert.alert_key for alert in other_alerts],
            processing_timeout_minutes,
        ))
        claimed_keys = claimed_trigger_keys | claimed_other_keys
        claimed: Dict[str, AlertRecord] = {
            alert.alert_key: alert
            for alert in deliverable
            if alert.alert_key in claimed_keys
        }
        active_payloads = []
        resolved_payloads = []
        for alert in claimed.values():
            payload = _to_payload(alert)
            if alert.notification_type == NOTIFICATION_TYPE_RESOLUTION:
                resolved_payloads.append(payload)
            else:
                active_payloads.append(payload)

        try:
            sent = email.send_consolidated(ConsolidatedPayload(
                user_code=user_code,
                active_alerts=active_payloads,
                resolved_alerts=resolved_payloads,
            ))
        except Exception as error:
            email.last_error_code = type(error).__name__
            email.last_error_message = str(error)
            sent = False

        if sent:
            state_store.mark_notifications_sent(
                database, user_code, list(claimed)
            )
            sent_count += len(claimed)
            continue

        attempt = max(
            alert.notification_attempt_count + 1
            for alert in claimed.values()
        )
        retry_delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        state_store.mark_notifications_failed(
            database,
            user_code,
            list(claimed),
            email.last_error_code or "EMAIL_DELIVERY_FAILED",
            email.last_error_message or "El correo no pudo enviarse.",
            retry_delay,
        )
        failed_count += len(claimed)

    summary = (
        f"Checked {len(databases)} user_codes: "
        f"{sent_count} notifications sent, {failed_count} attempts failed"
    )
    context.log.info(summary)
    return SkipReason(summary)
