import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from uuid import UUID

DEV_EUI_PATTERN = re.compile(r"^[0-9a-f]{16}$")
GATEWAY_STATES = frozenset({"NEVER_SEEN", "ONLINE", "OFFLINE"})
DECIMAL_QUANTUM = Decimal("0.000001")
BATTERY_QUANTUM = Decimal("0.0001")
SQL_INT_MAX = 2147483647
SQL_BIGINT_MAX = 9223372036854775807
SQL_DECIMAL_18_6_MAX = Decimal("999999999999.999999")


def normalize_dev_eui(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("PhysicalIdentifier must be a string DevEUI")
    dev_eui = value.strip().lower()
    if not DEV_EUI_PATTERN.fullmatch(dev_eui):
        raise ValueError("PhysicalIdentifier must be a 16-character hexadecimal DevEUI")
    return dev_eui


def device_references(payload: dict[str, Any], expected_dev_eui: str) -> tuple[str, str]:
    device = _mapping_value(payload, "device", "device response")
    dev_eui = normalize_dev_eui(_required_value(device, "devEui", "device"))
    if dev_eui != expected_dev_eui:
        raise ValueError("ChirpStack returned a different DevEUI")
    application_id = _uuid_value(device, "applicationId", "device")
    device_profile_id = _uuid_value(device, "deviceProfileId", "device")
    return application_id, device_profile_id


def device_profile_supports_battery(payload: dict[str, Any]) -> bool:
    device_profile = _mapping_value(payload, "deviceProfile", "device-profile response")
    measurements = _mapping_value(device_profile, "measurements", "device profile")
    return "battery" in measurements


def build_device_metric_row(
    dev_eui: str,
    device_payload: dict[str, Any],
    application_payload: dict[str, Any],
    device_profile_payload: dict[str, Any],
    link_metrics_payload: dict[str, Any],
    device_metrics_payload: dict[str, Any] | None,
    time_span: datetime,
    extracted_at: datetime,
) -> dict[str, Any]:
    application_id, device_profile_id = device_references(device_payload, dev_eui)
    application = _mapping_value(application_payload, "application", "application response")
    returned_application_id = _uuid_value(application, "id", "application")
    if returned_application_id != application_id:
        raise ValueError("ChirpStack returned a different applicationId")
    tenant_id = _uuid_value(application, "tenantId", "application")

    device_profile = _mapping_value(
        device_profile_payload,
        "deviceProfile",
        "device-profile response",
    )
    returned_device_profile_id = _uuid_value(device_profile, "id", "device profile")
    if returned_device_profile_id != device_profile_id:
        raise ValueError("ChirpStack returned a different deviceProfileId")
    profile_tenant_id = _uuid_value(device_profile, "tenantId", "device profile")
    if profile_tenant_id != tenant_id:
        raise ValueError("Application and device profile belong to different tenants")
    uplink_interval = _nonnegative_integer_value(
        device_profile,
        "uplinkInterval",
        "device profile",
    )
    expected_uplink_count = None
    if uplink_interval > 0:
        expected_uplink_count = (
            Decimal(3600) / Decimal(uplink_interval)
        ).quantize(DECIMAL_QUANTUM, rounding=ROUND_HALF_UP)

    time_span = _utc_naive(time_span, "TimeSpan")
    extracted_at = _utc_naive(extracted_at, "ExtractedAt")
    rx_packets = _metric_chart(link_metrics_payload, "rxPackets", "link metrics")
    gw_rssi = _metric_chart(link_metrics_payload, "gwRssi", "link metrics")
    gw_snr = _metric_chart(link_metrics_payload, "gwSnr", "link metrics")
    errors = _metric_chart(link_metrics_payload, "errors", "link metrics")
    received_uplink_count = _labeled_count(
        rx_packets,
        "rx_count",
        time_span,
        "rxPackets",
    )
    average_rssi = _labeled_decimal(gw_rssi, "rssi", time_span, "gwRssi")
    average_snr = _labeled_decimal(gw_snr, "snr", time_span, "gwSnr")
    link_error_count = _summed_count(errors, time_span, "errors")
    link_error_counts_json = _counts_json(errors, time_span, "errors")
    metrics_available = received_uplink_count is not None
    if metrics_available and link_error_count is None:
        raise ValueError("ChirpStack link metrics omitted errors for the requested hour")
    if (
        received_uplink_count is not None
        and received_uplink_count > 0
        and (average_rssi is None or average_snr is None)
    ):
        raise ValueError("ChirpStack link metrics omitted RSSI or SNR for received uplinks")
    if not metrics_available and any(
        value is not None
        for value in (average_rssi, average_snr, link_error_count)
    ):
        raise ValueError("ChirpStack link metrics contain an inconsistent hourly bucket")

    battery_level = None
    battery_level_source = None
    battery_observed_at = None
    if device_metrics_payload is not None:
        metrics = _mapping_value(device_metrics_payload, "metrics", "device metrics response")
        battery_chart_payload = metrics.get("battery")
        if battery_chart_payload is not None:
            battery_chart = _chart(battery_chart_payload, "battery metric")
            battery_metric_value = _labeled_decimal(
                battery_chart,
                "battery",
                time_span,
                "battery",
                BATTERY_QUANTUM,
            )
            battery_level = _battery_level(battery_metric_value, "battery metric")
            if battery_level is not None:
                battery_level_source = "DEVICE_METRICS"
                battery_observed_at = time_span

    device_status = device_payload.get("deviceStatus")
    external_power_source = None
    if device_status is not None:
        if not isinstance(device_status, dict):
            raise ValueError("deviceStatus must be an object when present")
        if "externalPowerSource" in device_status:
            external_power_source = device_status["externalPowerSource"]
            if not isinstance(external_power_source, bool):
                raise ValueError("deviceStatus.externalPowerSource must be boolean")
        if battery_level is None and "batteryLevel" in device_status:
            status_battery = _decimal_number(
                device_status["batteryLevel"],
                "deviceStatus.batteryLevel",
                BATTERY_QUANTUM,
            )
            battery_level = _battery_level(status_battery, "deviceStatus.batteryLevel")
            if battery_level is not None:
                battery_level_source = "DEVICE_STATUS"
                battery_observed_at = extracted_at

    return {
        "DevEui": dev_eui,
        "TimeSpan": time_span,
        "TenantId": tenant_id,
        "ApplicationId": application_id,
        "DeviceProfileId": device_profile_id,
        "UplinkIntervalSeconds": uplink_interval,
        "ExpectedUplinkCount": expected_uplink_count,
        "ReceivedUplinkCount": received_uplink_count,
        "AverageRssi": average_rssi,
        "AverageSnr": average_snr,
        "LinkErrorCount": link_error_count,
        "LinkErrorCountsJson": link_error_counts_json,
        "BatteryLevel": battery_level,
        "ExternalPowerSource": external_power_source,
        "BatteryLevelSource": battery_level_source,
        "BatteryObservedAt": battery_observed_at,
        "MetricsAvailable": metrics_available,
        "ExtractedAt": extracted_at,
    }


def build_gateway_metric_row(
    gateway_list_item: dict[str, Any],
    gateway_payload: dict[str, Any],
    gateway_metrics_payload: dict[str, Any],
    expected_tenant_id: str,
    time_span: datetime,
    extracted_at: datetime,
) -> dict[str, Any]:
    gateway_id = normalize_dev_eui(
        _required_value(gateway_list_item, "gatewayId", "gateway list item")
    )
    tenant_id = _uuid_value(gateway_list_item, "tenantId", "gateway list item")
    if tenant_id != expected_tenant_id:
        raise ValueError("ChirpStack returned a gateway from a different tenant")
    gateway_state = _required_value(gateway_list_item, "state", "gateway list item")
    if gateway_state not in GATEWAY_STATES:
        raise ValueError("ChirpStack returned an unknown gateway state")
    gateway_last_seen_at = _optional_utc_datetime(
        gateway_list_item.get("lastSeenAt"),
        "gateway.lastSeenAt",
    )
    if gateway_state != "NEVER_SEEN" and gateway_last_seen_at is None:
        raise ValueError("A seen gateway must include lastSeenAt")
    if gateway_state == "NEVER_SEEN" and gateway_last_seen_at is not None:
        raise ValueError("A never-seen gateway cannot include lastSeenAt")

    gateway = _mapping_value(gateway_payload, "gateway", "gateway response")
    returned_gateway_id = normalize_dev_eui(
        _required_value(gateway, "gatewayId", "gateway")
    )
    if returned_gateway_id != gateway_id:
        raise ValueError("ChirpStack returned a different gatewayId")
    detail_tenant_id = _uuid_value(gateway, "tenantId", "gateway")
    if detail_tenant_id != tenant_id:
        raise ValueError("Gateway list and detail belong to different tenants")
    gateway_name = _required_value(gateway, "name", "gateway")
    if not isinstance(gateway_name, str) or not gateway_name.strip():
        raise ValueError("gateway.name must be a non-empty string")
    if len(gateway_name) > 255:
        raise ValueError("gateway.name exceeds the SQL contract")
    stats_interval = _nonnegative_integer_value(gateway, "statsInterval", "gateway")

    time_span = _utc_naive(time_span, "TimeSpan")
    extracted_at = _utc_naive(extracted_at, "ExtractedAt")
    rx_packets = _metric_chart(gateway_metrics_payload, "rxPackets", "gateway metrics")
    tx_packets = _metric_chart(gateway_metrics_payload, "txPackets", "gateway metrics")
    tx_statuses = _metric_chart(
        gateway_metrics_payload,
        "txPacketsPerStatus",
        "gateway metrics",
    )
    received_packet_count = _labeled_count(
        rx_packets,
        "rx_count",
        time_span,
        "gateway rxPackets",
    )
    transmitted_packet_count = _labeled_count(
        tx_packets,
        "tx_count",
        time_span,
        "gateway txPackets",
    )
    status_values = tx_statuses.get(time_span)
    metrics_available = received_packet_count is not None
    if (transmitted_packet_count is not None) != metrics_available:
        raise ValueError("Gateway RX and TX metrics have inconsistent hourly buckets")
    if (status_values is not None) != metrics_available:
        raise ValueError("Gateway packet-status metrics have an inconsistent hourly bucket")
    tx_ok_count = None
    tx_error_count = None
    if status_values is not None:
        tx_ok_count = _count_number(status_values.get("OK", 0), "gateway TX OK")
        tx_error_count = sum(
            _count_number(value, "gateway TX error")
            for label, value in status_values.items()
            if label != "OK"
        )
        if tx_error_count > SQL_BIGINT_MAX:
            raise ValueError("gateway TX error exceeds the SQL BIGINT contract")
    tx_packet_status_counts_json = _counts_json(
        tx_statuses,
        time_span,
        "gateway txPacketsPerStatus",
    )

    return {
        "GatewayId": gateway_id,
        "TimeSpan": time_span,
        "TenantId": tenant_id,
        "GatewayName": gateway_name.strip(),
        "ReceivedPacketCount": received_packet_count,
        "TransmittedPacketCount": transmitted_packet_count,
        "TxOkCount": tx_ok_count,
        "TxErrorCount": tx_error_count,
        "TxPacketStatusCountsJson": tx_packet_status_counts_json,
        "GatewayState": gateway_state,
        "GatewayLastSeenAt": gateway_last_seen_at,
        "StatsIntervalSeconds": stats_interval,
        "MetricsAvailable": metrics_available,
        "ExtractedAt": extracted_at,
    }


def _required_value(payload: dict[str, Any], key: str, context: str) -> Any:
    if key not in payload or payload[key] is None:
        raise ValueError(f"{context} omitted {key}")
    return payload[key]


def _mapping_value(payload: dict[str, Any], key: str, context: str) -> dict[str, Any]:
    value = _required_value(payload, key, context)
    if not isinstance(value, dict):
        raise TypeError(f"{context}.{key} must be an object")
    return value


def _uuid_value(payload: dict[str, Any], key: str, context: str) -> str:
    value = _required_value(payload, key, context)
    if not isinstance(value, str):
        raise TypeError(f"{context}.{key} must be a UUID string")
    try:
        return str(UUID(value))
    except ValueError as ex:
        raise ValueError(f"{context}.{key} must be a valid UUID") from ex


def _nonnegative_integer_value(payload: dict[str, Any], key: str, context: str) -> int:
    value = _required_value(payload, key, context)
    result = _count_number(value, f"{context}.{key}")
    if result > SQL_INT_MAX:
        raise ValueError(f"{context}.{key} exceeds the SQL INT contract")
    return result


def _optional_utc_datetime(value: Any, context: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{context} must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as ex:
        raise ValueError(f"{context} must be a valid RFC3339 timestamp") from ex
    if parsed.tzinfo is None:
        raise ValueError(f"{context} must include a timezone")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def _utc_naive(value: datetime, context: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{context} must be a datetime")
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def _metric_chart(payload: dict[str, Any], key: str, context: str) -> dict[datetime, dict[str, float | int]]:
    return _chart(_required_value(payload, key, context), f"{context}.{key}")


def _chart(value: Any, context: str) -> dict[datetime, dict[str, float | int]]:
    if not isinstance(value, dict):
        raise TypeError(f"{context} must be an object")
    timestamps = _required_value(value, "timestamps", context)
    datasets = _required_value(value, "datasets", context)
    if not isinstance(timestamps, list) or not isinstance(datasets, list):
        raise TypeError(f"{context} timestamps and datasets must be arrays")
    parsed_timestamps = []
    for timestamp in timestamps:
        parsed_timestamp = _optional_utc_datetime(timestamp, f"{context}.timestamps")
        if parsed_timestamp is None:
            raise ValueError(f"{context}.timestamps cannot contain null")
        if parsed_timestamp in parsed_timestamps:
            raise ValueError(f"{context} contains duplicate timestamps")
        parsed_timestamps.append(parsed_timestamp)
    values_by_timestamp: dict[datetime, dict[str, float | int]] = {
        timestamp: {} for timestamp in parsed_timestamps
    }
    labels = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise TypeError(f"{context}.datasets must contain objects")
        label = _required_value(dataset, "label", f"{context}.dataset")
        data = _required_value(dataset, "data", f"{context}.dataset")
        if not isinstance(label, str) or not label:
            raise ValueError(f"{context}.dataset.label must be a non-empty string")
        if label in labels:
            raise ValueError(f"{context} contains duplicate dataset labels")
        if not isinstance(data, list) or len(data) != len(parsed_timestamps):
            raise ValueError(f"{context}.dataset.data must align with timestamps")
        labels.add(label)
        for timestamp, metric_value in zip(parsed_timestamps, data):
            values_by_timestamp[timestamp][label] = _finite_number(
                metric_value,
                f"{context}.{label}",
            )
    return values_by_timestamp


def _labeled_count(
    values_by_timestamp: dict[datetime, dict[str, float | int]],
    label: str,
    time_span: datetime,
    context: str,
) -> int | None:
    values = values_by_timestamp.get(time_span)
    if values is None:
        return None
    if label not in values:
        raise ValueError(f"{context} omitted dataset {label}")
    return _count_number(values[label], f"{context}.{label}")


def _labeled_decimal(
    values_by_timestamp: dict[datetime, dict[str, float | int]],
    label: str,
    time_span: datetime,
    context: str,
    quantum: Decimal = DECIMAL_QUANTUM,
) -> Decimal | None:
    values = values_by_timestamp.get(time_span)
    if values is None:
        return None
    if not values:
        return None
    if label not in values:
        raise ValueError(f"{context} omitted dataset {label}")
    return _decimal_number(values[label], f"{context}.{label}", quantum)


def _summed_count(
    values_by_timestamp: dict[datetime, dict[str, float | int]],
    time_span: datetime,
    context: str,
) -> int | None:
    values = values_by_timestamp.get(time_span)
    if values is None:
        return None
    result = sum(
        _count_number(value, context)
        for value in values.values()
    )
    if result > SQL_BIGINT_MAX:
        raise ValueError(f"{context} exceeds the SQL BIGINT contract")
    return result


def _counts_json(
    values_by_timestamp: dict[datetime, dict[str, float | int]],
    time_span: datetime,
    context: str,
) -> str | None:
    values = values_by_timestamp.get(time_span)
    if values is None:
        return None
    counts = {
        label: count
        for label, value in values.items()
        if (count := _count_number(value, f"{context}.{label}")) > 0
    }
    return json.dumps(
        dict(sorted(counts.items())),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _finite_number(value: Any, context: str) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{context} must be numeric")
    try:
        is_finite = math.isfinite(value)
    except OverflowError as ex:
        raise ValueError(f"{context} exceeds the numeric contract") from ex
    if not is_finite:
        raise ValueError(f"{context} must be finite")
    return value


def _count_number(value: Any, context: str) -> int:
    number = _finite_number(value, context)
    if float(number) < 0 or not float(number).is_integer():
        raise ValueError(f"{context} must be a non-negative integer")
    result = int(number)
    if result > SQL_BIGINT_MAX:
        raise ValueError(f"{context} exceeds the SQL BIGINT contract")
    return result


def _decimal_number(value: Any, context: str, quantum: Decimal) -> Decimal:
    number = _finite_number(value, context)
    try:
        result = Decimal(str(number)).quantize(quantum, rounding=ROUND_HALF_UP)
    except InvalidOperation as ex:
        raise ValueError(f"{context} exceeds the decimal contract") from ex
    if abs(result) > SQL_DECIMAL_18_6_MAX:
        raise ValueError(f"{context} exceeds the decimal contract")
    return result


def _battery_level(value: Decimal | None, context: str) -> Decimal | None:
    if value is None or value == Decimal("-1.0000"):
        return None
    if value < 0 or value > 100:
        raise ValueError(f"{context} must be -1 or a percentage from 0 to 100")
    return value
