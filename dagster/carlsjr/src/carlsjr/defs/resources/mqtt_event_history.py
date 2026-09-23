import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from time import monotonic, sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from uuid import UUID

from dagster import resource


EVENT_API_TENANT = "carls"
EVENT_API_MAX_LIMIT = 5000
EVENT_API_MAX_RETRIES = 3
EVENT_API_MAX_REQUESTS_PER_MINUTE = 95
DEV_EUI_PATTERN = re.compile(r"^[0-9A-Fa-f]{16}$")
ISO_DATETIME_PATTERN = re.compile(
    r"(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})?"
)


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("MQTT event history API datetime must be text")
    match = ISO_DATETIME_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("MQTT event history API datetime has an invalid format")
    fraction = match.group("fraction") or ""
    normalized = match.group("base") + "." + fraction.ljust(6, "0")[:6]
    offset = match.group("offset")
    if offset is not None:
        normalized += "+00:00" if offset == "Z" else offset
    parsed = datetime.strptime(
        normalized,
        "%Y-%m-%dT%H:%M:%S.%f%z"
        if offset is not None
        else "%Y-%m-%dT%H:%M:%S.%f",
    )
    if len(fraction) > 6 and fraction[6] >= "5":
        parsed += timedelta(microseconds=1)
    return parsed


class MqttEventHistoryWindowTooLarge(RuntimeError):
    pass


class MqttEventHistoryApiResource:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        timeout_seconds: int,
        limit: int = EVENT_API_MAX_LIMIT,
        requests_per_minute: int = EVENT_API_MAX_REQUESTS_PER_MINUTE,
        lookback_hours: int = 6,
    ) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.limit = limit
        self.requests_per_minute = requests_per_minute
        self.lookback_hours = lookback_hours
        self.requests_made = 0
        self._last_request_at = None

    def _wait_for_request_slot(self) -> None:
        minimum_interval = 60 / self.requests_per_minute
        if self._last_request_at is not None:
            remaining = minimum_interval - (monotonic() - self._last_request_at)
            if remaining > 0:
                sleep(remaining)
        self._last_request_at = monotonic()

    def fetch_range(
        self,
        from_local: datetime,
        to_local: datetime,
    ) -> list[dict[str, Any]]:
        parsed_url = urlparse(self.url)
        if (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.netloc
            or not self.user
            or not self.password
            or self.timeout_seconds <= 0
            or not 1 <= self.limit <= EVENT_API_MAX_LIMIT
            or not 1 <= self.requests_per_minute < 100
            or not 1 <= self.lookback_hours <= 168
        ):
            raise RuntimeError("MQTT event history API configuration is incomplete")
        if from_local.tzinfo is not None or to_local.tzinfo is not None:
            raise ValueError("MQTT event history API range must use local naive datetimes")
        if from_local >= to_local:
            raise ValueError("MQTT event history API range must use from_local < to_local")
        try:
            rows = self._request_window(from_local, to_local)
        except MqttEventHistoryWindowTooLarge:
            rows = None
        if rows is not None:
            return rows
        if to_local - from_local <= timedelta(microseconds=1):
            raise RuntimeError(
                "MQTT event history API window remains saturated at minimum precision"
            )
        midpoint = from_local + (to_local - from_local) / 2
        combined_rows = self.fetch_range(from_local, midpoint) + self.fetch_range(
            midpoint,
            to_local,
        )
        mqtt_ids = {row["MqttId"] for row in combined_rows}
        if len(mqtt_ids) != len(combined_rows):
            raise RuntimeError("MQTT event history API split windows returned duplicate ids")
        return combined_rows

    def _request_window(
        self,
        from_local: datetime,
        to_local: datetime,
    ) -> list[dict[str, Any]]:
        query = urlencode(
            {
                "tenant": EVENT_API_TENANT,
                "fechaInicio": from_local.isoformat(timespec="microseconds") + "Z",
                "fechaFin": to_local.isoformat(timespec="microseconds") + "Z",
                "limit": self.limit,
            }
        )
        separator = "&" if "?" in self.url else "?"
        authorization = base64.b64encode(
            f"{self.user}:{self.password}".encode("utf-8")
        ).decode("ascii")
        request = Request(
            self.url + separator + query,
            headers={
                "Accept": "application/json",
                "Authorization": "Basic " + authorization,
            },
        )
        for retry_number in range(EVENT_API_MAX_RETRIES + 1):
            self._wait_for_request_slot()
            self.requests_made += 1
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"MQTT event history API returned HTTP {response.status}"
                        )
                    if response.headers.get_content_type() != "application/json":
                        raise RuntimeError("MQTT event history API did not return JSON")
                    payload = json.loads(response.read().decode("utf-8"))
                return self._validate_payload(payload, from_local, to_local)
            except HTTPError as ex:
                if ex.code == 504:
                    raise MqttEventHistoryWindowTooLarge(
                        "MQTT event history API returned HTTP 504"
                    ) from ex
                if ex.code == 429 and retry_number < EVENT_API_MAX_RETRIES:
                    retry_after = ex.headers.get("Retry-After") if ex.headers else None
                    try:
                        delay = min(max(float(retry_after), 1), 60)
                    except (TypeError, ValueError):
                        delay = 60
                    sleep(delay)
                    continue
                if 500 <= ex.code < 600 and retry_number < EVENT_API_MAX_RETRIES:
                    sleep(2 ** retry_number)
                    continue
                raise RuntimeError(
                    f"MQTT event history API returned HTTP {ex.code}"
                ) from ex
            except (TimeoutError, URLError) as ex:
                if retry_number < EVENT_API_MAX_RETRIES:
                    sleep(2 ** retry_number)
                    continue
                raise RuntimeError("MQTT event history API request failed") from ex
            except (UnicodeDecodeError, json.JSONDecodeError) as ex:
                raise RuntimeError("MQTT event history API returned invalid JSON") from ex
        raise RuntimeError("MQTT event history API request failed after retries")

    def _validate_payload(
        self,
        payload: Any,
        from_local: datetime,
        to_local: datetime,
    ) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            raise ValueError("MQTT event history API response must be an object")
        required_fields = {
            "ok",
            "tenant",
            "fecha_inicio",
            "fecha_fin",
            "total",
            "data",
        }
        if not required_fields.issubset(payload):
            raise ValueError("MQTT event history API response does not match the contract")
        if payload["ok"] is not True:
            raise ValueError("MQTT event history API returned ok=false")
        if (
            not isinstance(payload["tenant"], str)
            or payload["tenant"].casefold() != EVENT_API_TENANT
        ):
            raise ValueError("MQTT event history API returned a different tenant")
        total = payload["total"]
        data = payload["data"]
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError("MQTT event history API total must be a non-negative integer")
        if not isinstance(data, list) or len(data) > self.limit:
            raise ValueError("MQTT event history API data does not match the requested limit")
        response_from = _parse_datetime(payload["fecha_inicio"])
        response_to = _parse_datetime(payload["fecha_fin"])
        if (
            response_from.tzinfo is None
            or response_from.utcoffset() != timedelta(0)
            or response_to.tzinfo is None
            or response_to.utcoffset() != timedelta(0)
        ):
            raise ValueError("MQTT event history API response range must use UTC offsets")
        if (
            response_from.replace(tzinfo=None) != from_local
            or response_to.replace(tzinfo=None) != to_local
        ):
            raise ValueError("MQTT event history API response range does not match the request")
        if len(data) == self.limit or total > len(data):
            raise MqttEventHistoryWindowTooLarge(
                "MQTT event history API response reached the requested limit"
            )

        rows = []
        mqtt_ids = set()
        for index, row in enumerate(data):
            if not isinstance(row, dict):
                raise ValueError(
                    f"MQTT event history API row {index} does not match the contract"
                )
            dev_eui = row.get("deveui")
            if not isinstance(dev_eui, str) or DEV_EUI_PATTERN.fullmatch(dev_eui) is None:
                continue
            if not isinstance(row.get("id"), str):
                raise ValueError(f"MQTT event history API row {index} has an invalid id")
            try:
                mqtt_id = str(UUID(row["id"]))
            except ValueError as ex:
                raise ValueError(
                    f"MQTT event history API row {index} has an invalid id"
                ) from ex
            if mqtt_id in mqtt_ids:
                raise ValueError("MQTT event history API response contains duplicate ids")
            mqtt_ids.add(mqtt_id)
            if (
                not isinstance(row.get("tenant"), str)
                or row["tenant"].casefold() != EVENT_API_TENANT
            ):
                raise ValueError(
                    f"MQTT event history API row {index} has a different tenant"
                )
            try:
                received_at = _parse_datetime(row["fecha"])
            except (KeyError, ValueError) as ex:
                raise ValueError(
                    f"MQTT event history API row {index} fecha is invalid"
                ) from ex
            if (
                received_at.tzinfo is not None
                or received_at < from_local
                or received_at >= to_local
            ):
                raise ValueError(
                    f"MQTT event history API row {index} fecha is outside the requested window"
                )
            raw_text = row.get("mensaje_raw")
            if not isinstance(raw_text, str):
                raise ValueError(
                    f"MQTT event history API row {index} mensaje_raw must be JSON text"
                )
            try:
                raw_message = json.loads(raw_text)
                if not isinstance(raw_message, dict):
                    raise ValueError
                json.dumps(raw_message, allow_nan=False)
                event_time = _parse_datetime(raw_message["time"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as ex:
                raise ValueError(
                    f"MQTT event history API row {index} mensaje_raw is invalid"
                ) from ex
            if event_time.tzinfo is None or event_time.utcoffset() != timedelta(0):
                raise ValueError(
                    f"MQTT event history API row {index} mensaje_raw.time must use UTC"
                )
            rows.append(
                {
                    "MqttId": mqtt_id,
                    "DevEui": dev_eui.lower(),
                    "EventTime": event_time.astimezone(timezone.utc).replace(tzinfo=None),
                    "RawMessage": raw_message,
                }
            )
        return rows


@resource
def mqtt_event_history_api_resource(_):
    timeout_text = os.getenv("MQTT_EVENT_HISTORY_API_TIMEOUT_SECONDS", "60").strip()
    limit_text = os.getenv("MQTT_EVENT_HISTORY_API_LIMIT", "5000").strip()
    requests_per_minute_text = os.getenv(
        "MQTT_EVENT_HISTORY_API_REQUESTS_PER_MINUTE",
        "95",
    ).strip()
    lookback_hours_text = os.getenv(
        "MQTT_EVENT_HISTORY_LOOKBACK_HOURS",
        "6",
    ).strip()
    try:
        timeout_seconds = int(timeout_text)
    except ValueError as ex:
        raise ValueError("MQTT_EVENT_HISTORY_API_TIMEOUT_SECONDS must be an integer") from ex
    try:
        limit = int(limit_text)
    except ValueError as ex:
        raise ValueError("MQTT_EVENT_HISTORY_API_LIMIT must be an integer") from ex
    try:
        requests_per_minute = int(requests_per_minute_text)
    except ValueError as ex:
        raise ValueError(
            "MQTT_EVENT_HISTORY_API_REQUESTS_PER_MINUTE must be an integer"
        ) from ex
    try:
        lookback_hours = int(lookback_hours_text)
    except ValueError as ex:
        raise ValueError("MQTT_EVENT_HISTORY_LOOKBACK_HOURS must be an integer") from ex
    return MqttEventHistoryApiResource(
        url=os.getenv("MQTT_EVENT_HISTORY_API_URL", "").strip(),
        user=os.getenv("MQTT_EVENT_HISTORY_API_USER", "").strip(),
        password=os.getenv("MQTT_EVENT_HISTORY_API_PASSWORD", ""),
        timeout_seconds=timeout_seconds,
        limit=limit,
        requests_per_minute=requests_per_minute,
        lookback_hours=lookback_hours,
    )
