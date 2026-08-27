from datetime import datetime, time, timedelta, timezone
from typing import Any, Optional

from dagster import (
    DagsterEventType,
    DefaultSensorStatus,
    EventRecordsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..resources.alert_state_store import (
    AlertStateStoreResource,
    AssetRowCountObservation,
)
from .operational_alerts_sensor import _load_config


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
    minimum_interval_seconds=300,
    required_resource_keys={"alert_state"},
    default_status=DefaultSensorStatus.RUNNING,
)
def asset_row_count_trend_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    config = _load_config().get("defaults", {}).get("asset_row_count_trends", {})
    if not config.get("enabled", True):
        return SkipReason("Asset row count trends are disabled")

    metadata_key = config.get("metadata_key", "row_count")
    batch_size = max(1, min(int(config.get("batch_size", 500)), 1000))
    backfill_days = max(1, int(config.get("backfill_days", 28)))
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
    records = context.instance.event_log_storage.get_event_records(
        event_filter,
        limit=batch_size,
        ascending=True,
    )

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
        return SkipReason("No new asset materializations")

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

    state_store: AlertStateStoreResource = context.resources.alert_state
    recorded = (
        state_store.record_asset_row_counts(observations)
        if observations
        else 0
    )
    context.update_cursor(str(records[-1].storage_id))
    return SkipReason(
        f"Scanned {len(records)} materializations, "
        f"recorded {recorded} row count observations"
    )
