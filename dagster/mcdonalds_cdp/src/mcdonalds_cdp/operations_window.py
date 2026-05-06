from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


OPERATIONS_TIMEZONE_NAME = "America/Ciudad_Juarez"
DEFAULT_OPERATIONS_BOOTSTRAP_START = datetime(2025, 10, 1)


def get_operations_timezone():
    try:
        return ZoneInfo(OPERATIONS_TIMEZONE_NAME)
    except Exception:
        # Windows dev environments may not have tzdata installed.
        return timezone(timedelta(hours=-6), name=OPERATIONS_TIMEZONE_NAME)


OPERATIONS_TIMEZONE = get_operations_timezone()


def truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def coerce_local_naive(
    dt: datetime,
    timezone: ZoneInfo = OPERATIONS_TIMEZONE,
) -> datetime:
    if dt.tzinfo is None:
        return truncate_to_seconds(dt)
    return truncate_to_seconds(
        dt.astimezone(timezone).replace(tzinfo=None)
    )


def current_operational_hour_start(
    now: Optional[datetime] = None,
    timezone: ZoneInfo = OPERATIONS_TIMEZONE,
) -> datetime:
    current = now if now is not None else datetime.now(timezone)
    if current.tzinfo is not None:
        current = current.astimezone(timezone).replace(tzinfo=None)
    current = truncate_to_seconds(current)
    return current.replace(minute=0, second=0, microsecond=0)


def build_operations_window(
    last_loaded_at: Optional[datetime],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    if last_loaded_at is None:
        last_loaded_at = DEFAULT_OPERATIONS_BOOTSTRAP_START

    from_local = coerce_local_naive(last_loaded_at)
    max_local_hour = current_operational_hour_start(now=now)
    watermark_in_future = from_local > max_local_hour

    processing_anchor = min(from_local, max_local_hour)
    fetch_from = (
        processing_anchor.replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=11)
    )

    return {
        "delete_from": fetch_from,
        "delete_to_exclusive": max(from_local, max_local_hour) + timedelta(hours=1),
        "fetch_from": fetch_from,
        "from_local": from_local,
        "max_local_hour": max_local_hour,
        "original_last_loaded_at": last_loaded_at,
        "watermark_in_future": watermark_in_future,
    }


def resolve_operations_watermark(
    *,
    succeeded: bool,
    original_last_loaded_at: datetime,
    max_time_span: Optional[datetime],
    max_local_hour: datetime,
) -> datetime:
    if not succeeded:
        return original_last_loaded_at
    if max_time_span is None:
        return max_local_hour
    return max_time_span
