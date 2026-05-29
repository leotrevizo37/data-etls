from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from dagster import AssetCheckResult, AssetKey, asset_check


PROJECT_NAME = "bafar_poc_gabinete"
MIN_REAL_READINGS_RATIO = 0.6

FACTREADINGS_ASSET_KEY = AssetKey([PROJECT_NAME, "dwh_factReadingsMeasurement"])
OPERATIONS_CONTROL_ASSET_KEY = AssetKey([PROJECT_NAME, "ctl_IngestionControl"])
FACTLECTURAS_ASSET_KEY = AssetKey([PROJECT_NAME, "dwh_factlecturas"])


def _metadata_value(metadata: Mapping[str, Any], key: str) -> Any:
    value = metadata.get(key)
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "text"):
        return value.text
    if hasattr(value, "data"):
        return value.data
    return value


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    return None


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == "none":
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _latest_materialization_metadata(context: Any, asset_key: AssetKey) -> Mapping[str, Any]:
    materialization_event = context.instance.get_latest_materialization_event(asset_key)
    if materialization_event is None:
        return {}

    materialization = getattr(materialization_event, "asset_materialization", None)
    if materialization is not None:
        return materialization.metadata or {}

    dagster_event = getattr(materialization_event, "dagster_event", None)
    event_data = getattr(dagster_event, "event_specific_data", None)
    materialization = getattr(event_data, "materialization", None)
    if materialization is not None:
        return materialization.metadata or {}

    return getattr(materialization_event, "metadata", {}) or {}


def _build_real_readings_ratio_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    row_count = _coerce_int(_metadata_value(metadata, "row_count")) or 0
    rows_with_readings = (
        _coerce_int(_metadata_value(metadata, "rows_with_readings_count_gt_0")) or 0
    )
    ratio = rows_with_readings / row_count if row_count else 0.0

    return AssetCheckResult(
        passed=bool(row_count > 0 and ratio > MIN_REAL_READINGS_RATIO),
        metadata={
            "row_count": row_count,
            "rows_with_readings_count_gt_0": rows_with_readings,
            "ratio": round(ratio, 4),
            "minimum_ratio": MIN_REAL_READINGS_RATIO,
        },
    )


def _build_local_window_order_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    from_local = _coerce_datetime(_metadata_value(metadata, "from_local"))
    to_local = _coerce_datetime(_metadata_value(metadata, "to_local"))

    return AssetCheckResult(
        passed=bool(from_local is not None and to_local is not None
                    and to_local > from_local),
        metadata={
            "from_local": str(from_local),
            "to_local": str(to_local),
        },
    )


def _build_operations_succeeded_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    succeeded = _coerce_bool(_metadata_value(metadata, "succeeded"))

    return AssetCheckResult(
        passed=bool(succeeded is True),
        metadata={"succeeded": bool(succeeded is True)},
    )


def _build_to_local_gte_max_time_span_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    to_local = _coerce_datetime(_metadata_value(metadata, "to_local"))
    max_time_span = _coerce_datetime(_metadata_value(metadata, "max_time_span"))
    passed = to_local is not None and (
        max_time_span is None or to_local >= max_time_span
    )

    return AssetCheckResult(
        passed=bool(passed),
        metadata={
            "to_local": str(to_local),
            "max_time_span": str(max_time_span),
        },
    )


def _build_original_lte_to_local_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    original_last_loaded_at = _coerce_datetime(
        _metadata_value(metadata, "original_last_loaded_at")
    )
    to_local = _coerce_datetime(_metadata_value(metadata, "to_local"))

    return AssetCheckResult(
        passed=bool(original_last_loaded_at is not None and to_local is not None
                    and original_last_loaded_at <= to_local),
        metadata={
            "original_last_loaded_at": str(original_last_loaded_at),
            "to_local": str(to_local),
        },
    )


def _build_factlecturas_row_count_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    row_count = _coerce_int(_metadata_value(metadata, "row_count")) or 0

    return AssetCheckResult(
        passed=bool(row_count > 0),
        metadata={"row_count": row_count},
    )


def _build_factlecturas_original_lte_to_utc_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    original_last_loaded_at = _coerce_datetime(
        _metadata_value(metadata, "original_last_loaded_at")
    )
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))

    return AssetCheckResult(
        passed=bool(original_last_loaded_at is not None and to_utc is not None
                    and original_last_loaded_at <= to_utc),
        metadata={
            "original_last_loaded_at": str(original_last_loaded_at),
            "to_utc": str(to_utc),
        },
    )


@asset_check(
    asset=FACTREADINGS_ASSET_KEY,
    name="real_readings_ratio_gt_0_6",
)
def factreadings_real_readings_ratio_gt_0_6(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTREADINGS_ASSET_KEY)
    return _build_real_readings_ratio_result(metadata)


@asset_check(
    asset=FACTREADINGS_ASSET_KEY,
    name="to_local_gt_from_local",
)
def factreadings_to_local_gt_from_local(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTREADINGS_ASSET_KEY)
    return _build_local_window_order_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="succeeded_is_true",
)
def operations_control_succeeded_is_true(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_operations_succeeded_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="to_local_gte_max_time_span",
)
def operations_control_to_local_gte_max_time_span(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_to_local_gte_max_time_span_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="original_last_loaded_at_lte_to_local",
)
def operations_control_original_lte_to_local(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_original_lte_to_local_result(metadata)


@asset_check(
    asset=FACTLECTURAS_ASSET_KEY,
    name="row_count_gt_0",
)
def factlecturas_row_count_gt_0(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTLECTURAS_ASSET_KEY)
    return _build_factlecturas_row_count_result(metadata)


@asset_check(
    asset=FACTLECTURAS_ASSET_KEY,
    name="original_last_loaded_at_lte_to_utc",
)
def factlecturas_original_lte_to_utc(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTLECTURAS_ASSET_KEY)
    return _build_factlecturas_original_lte_to_utc_result(metadata)


quality_asset_checks = [
    factreadings_real_readings_ratio_gt_0_6,
    factreadings_to_local_gt_from_local,
    operations_control_succeeded_is_true,
    operations_control_to_local_gte_max_time_span,
    operations_control_original_lte_to_local,
    factlecturas_row_count_gt_0,
    factlecturas_original_lte_to_utc,
]
