import os
import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional

from dagster import (
    DagsterEventType,
    DefaultSensorStatus,
    EventRecordsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..resources.alert_state_store import (
    ALERT_STATE_OPEN,
    AlertStateStoreResource,
    AssetRowCountObservation,
)
from .operational_alerts_sensor import (
    _build_alert_key,
    _load_config,
)


_ALERT_TYPE = "asset_row_count_trend"


def _coerce_row_count(value: Any) -> Optional[int]:
    raw_value = getattr(value, "value", value)
    if raw_value is None or isinstance(raw_value, bool):
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return None
    if parsed < 0 or isinstance(raw_value, float) and not raw_value.is_integer():
        return None
    return parsed


def _get_code_location(run: Any, asset_key: Any) -> str:
    if run is not None:
        origin = getattr(run, "remote_job_origin", None)
        if origin is not None:
            return origin.repository_origin.code_location_origin.location_name
        for tag_name in ("dagster/code-location", "user_code"):
            value = (getattr(run, "tags", None) or {}).get(tag_name)
            if value:
                return value
    if asset_key.path:
        return asset_key.path[0]
    return "unknown"


@sensor(
    name="asset_row_count_trend_sensor",
    minimum_interval_seconds=21600,
    required_resource_keys={"alert_state"},
    default_status=DefaultSensorStatus.RUNNING,
)
def asset_row_count_trend_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    config = _load_config()
    defaults = config.get("defaults", {})
    trend_defaults = defaults.get("asset_row_count_trends", {})
    if not trend_defaults.get("enabled", True):
        return SkipReason("Asset row count trends are disabled")

    state_store: AlertStateStoreResource = context.resources.alert_state
    metadata_key = trend_defaults.get("metadata_key", "row_count")
    batch_size = max(
        1, min(int(trend_defaults.get("batch_size", 500)), 1000)
    )
    backfill_days = max(1, int(trend_defaults.get("backfill_days", 28)))
    cursor = int(context.cursor) if context.cursor else None
    event_filter = EventRecordsFilter(
        event_type=DagsterEventType.ASSET_MATERIALIZATION,
        after_cursor=cursor,
        after_timestamp=(
            None
            if cursor is not None
            else datetime.combine(
                datetime.now(timezone.utc).date() - timedelta(days=backfill_days),
                time.min,
                tzinfo=timezone.utc,
            ).timestamp()
        ),
    )
    records_page = context.instance.event_log_storage.get_event_records(
        event_filter,
        limit=batch_size + 1,
        ascending=True,
    )
    has_more = len(records_page) > batch_size
    records = records_page[:batch_size]

    if not records:
        if cursor is None:
            latest = context.instance.event_log_storage.get_event_records(
                EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                ),
                limit=1,
                ascending=False,
            )
            if latest:
                context.update_cursor(str(latest[0].storage_id))

    observations = []
    runs = {}
    for record in records:
        materialization = record.asset_materialization
        asset_key = record.asset_key
        if materialization is None or asset_key is None:
            continue
        row_count = _coerce_row_count(materialization.metadata.get(metadata_key))
        if row_count is None:
            continue
        if record.run_id not in runs:
            runs[record.run_id] = context.instance.get_run_by_id(record.run_id)
        materialized_at = datetime.fromtimestamp(record.timestamp, timezone.utc)
        observations.append(
            AssetRowCountObservation(
                observation_date=materialized_at.date(),
                code_location=_get_code_location(runs[record.run_id], asset_key),
                asset_key=asset_key.to_user_string(),
                metadata_key=metadata_key,
                row_count=row_count,
                event_storage_id=record.storage_id,
                run_id=record.run_id,
                materialized_at=materialized_at,
            )
        )

    recorded = (
        state_store.record_asset_row_counts(observations)
        if observations
        else 0
    )
    if records:
        context.update_cursor(str(records[-1].storage_id))

    if has_more:
        return SkipReason(
            f"Scanned {len(records)} materializations, "
            f"recorded {recorded} row count observations, "
            "notifications deferred until backlog is complete"
        )

    if not trend_defaults.get("notifications_enabled", True):
        return SkipReason(
            f"Scanned {len(records)} materializations, "
            f"recorded {recorded} row count observations, "
            "notifications are disabled"
        )

    user_codes_cfg = config.get("user_codes", {})
    uc_databases = {
        user_code: os.environ.get(
            user_code_cfg.get("database_env", ""),
            user_code_cfg.get("database", user_code),
        )
        for user_code, user_code_cfg in user_codes_cfg.items()
    }
    active_conditions: Dict[str, Dict[str, Any]] = {}
    for trend in state_store.get_latest_asset_row_count_trends():
        if trend.code_location not in user_codes_cfg:
            continue
        trend_cfg = {
            **trend_defaults,
            **user_codes_cfg[trend.code_location].get(
                "asset_row_count_trends", {}
            ),
        }
        if not trend_cfg.get("enabled", True) or not trend_cfg.get(
            "notifications_enabled", True
        ):
            continue
        warning_drop = abs(float(trend_cfg.get("warning_drop_pct", 20)))
        critical_drop = max(
            warning_drop,
            abs(float(trend_cfg.get("critical_drop_pct", 40))),
        )
        if (
            trend.average_7d is None
            or trend.average_28d is None
            or trend.change_vs_7d_pct is None
            or trend.change_vs_28d_pct is None
            or trend.observed_days_7d
            < int(trend_cfg.get("minimum_observed_days_7d", 7))
            or trend.observed_days_28d
            < int(trend_cfg.get("minimum_observed_days_28d", 28))
            or trend.change_vs_7d_pct > -warning_drop
            or trend.change_vs_28d_pct > -warning_drop
        ):
            continue
        severity = (
            "CRITICAL"
            if trend.change_vs_7d_pct <= -critical_drop
            and trend.change_vs_28d_pct <= -critical_drop
            else "WARNING"
        )
        average_1d = (
            f"{trend.average_1d:.2f}"
            if trend.average_1d is not None
            else "N/A"
        )
        change_vs_1d = (
            f"{trend.change_vs_1d_pct:.2f}%"
            if trend.change_vs_1d_pct is not None
            else "N/A"
        )
        key = _build_alert_key(
            trend.code_location, _ALERT_TYPE, trend.asset_key
        )
        active_conditions[key] = {
            "user_code": trend.code_location,
            "entity_id": trend.asset_key,
            "details": (
                f"Severity: {severity}, "
                f"ObservationDate: {trend.observation_date}, "
                f"DailyRowCount: {trend.daily_row_count}, "
                f"Average1d: {average_1d}, "
                f"Average7d: {trend.average_7d:.2f}, "
                f"Average28d: {trend.average_28d:.2f}, "
                f"ChangeVs1d: {change_vs_1d}, "
                f"ChangeVs7d: {trend.change_vs_7d_pct:.2f}%, "
                f"ChangeVs28d: {trend.change_vs_28d_pct:.2f}%, "
                f"Materializations: {trend.daily_materialization_count}, "
                f"LatestRunId: {trend.latest_run_id or 'N/A'}"
            ),
        }

    existing_alerts = {}
    for user_code, database in uc_databases.items():
        for key, alert in state_store.get_active_alerts(
            database, user_code
        ).items():
            if alert.alert_type != _ALERT_TYPE:
                continue
            alert_cfg = {
                **trend_defaults,
                **user_codes_cfg[alert.user_code].get(
                    "asset_row_count_trends", {}
                ),
            }
            if alert_cfg.get("enabled", True) and alert_cfg.get(
                "notifications_enabled", True
            ):
                existing_alerts[key] = alert
    opened = 0
    for key, condition in active_conditions.items():
        if key not in existing_alerts:
            occurrence_key = f"{key}::{uuid.uuid4()}"
            state_store.upsert_alert(
                database=uc_databases[condition["user_code"]],
                alert_key=occurrence_key,
                user_code=condition["user_code"],
                alert_type=_ALERT_TYPE,
                entity_id=condition["entity_id"],
                details=condition["details"],
                state=ALERT_STATE_OPEN,
                notification_count=0,
            )
            opened += 1

    resolved = 0
    for key, alert in existing_alerts.items():
        if key in active_conditions:
            continue
        state_store.mark_resolved(
            uc_databases[alert.user_code],
            alert.user_code,
            alert.alert_key,
            alert.details,
        )
        resolved += 1

    summary = (
        f"Scanned {len(records)} materializations, "
        f"recorded {recorded} row count observations, "
        f"{len(active_conditions)} active conditions, "
        f"{opened} opened, {resolved} resolved"
    )
    context.log.info(summary)
    return SkipReason(summary)
