import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from time import monotonic, sleep
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID

from dagster import resource


LECTURAS_TENANT = "carls"
LECTURAS_TENANT_API_LIMIT = 5000
LECTURAS_TENANT_API_REQUESTS_PER_MINUTE = 95
LECTURAS_TENANT_API_MAX_RETRIES = 3
LECTURAS_TENANT_API_MIN_WINDOW = timedelta(microseconds=1)
ISO_DATETIME_PATTERN = re.compile(
    r"(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})?"
)
RESPONSE_FIELDS = {
    "ok",
    "tenant",
    "fecha_inicio",
    "fecha_fin",
    "total",
    "data",
}
ROW_FIELDS = {
    "id",
    "fecha",
    "deveui",
    "tenant",
    "dispositivo",
    "aplicacion",
    "sucursal",
    "topic",
    "variable",
    "valor",
    "valor_texto",
    "mediciones",
    "fecha_recepcion",
    "mensaje_raw",
}


def _parse_api_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Lecturas tenant API datetime must be text.")
    match = ISO_DATETIME_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("Lecturas tenant API datetime has an invalid format.")
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


class LecturasTenantWindowTooLarge(RuntimeError):
    pass


class LecturasTenantApiResource:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        timeout_seconds: int,
        limit: int = LECTURAS_TENANT_API_LIMIT,
        requests_per_minute: int = LECTURAS_TENANT_API_REQUESTS_PER_MINUTE,
    ) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.limit = limit
        self.requests_per_minute = requests_per_minute
        self.requests_made = 0
        self._last_request_at = None

    def _wait_for_request_slot(self) -> None:
        minimum_interval = 60 / self.requests_per_minute
        if self._last_request_at is not None:
            remaining = minimum_interval - (monotonic() - self._last_request_at)
            if remaining > 0:
                sleep(remaining)
        self._last_request_at = monotonic()

    def validate_payload(
        self,
        payload: Any,
        from_local: datetime,
        to_local: datetime,
    ) -> list[Dict[str, Any]]:
        if type(payload) is not dict or set(payload) != RESPONSE_FIELDS:
            raise ValueError("Lecturas tenant API response does not match the contract.")
        if payload["ok"] is not True:
            raise ValueError("Lecturas tenant API returned ok=false.")
        if not isinstance(payload["tenant"], str) or payload["tenant"].casefold() != LECTURAS_TENANT:
            raise ValueError("Lecturas tenant API returned a different tenant.")
        if type(payload["total"]) is not int or payload["total"] < 0:
            raise ValueError("Lecturas tenant API total must be a non-negative integer.")
        if type(payload["data"]) is not list or len(payload["data"]) > self.limit:
            raise ValueError("Lecturas tenant API data does not match the requested limit.")
        try:
            response_from = _parse_api_datetime(payload["fecha_inicio"])
            response_to = _parse_api_datetime(payload["fecha_fin"])
        except ValueError as exc:
            raise ValueError(
                "Lecturas tenant API response range must use UTC ISO datetimes."
            ) from exc
        if (
            response_from.tzinfo is None
            or response_from.utcoffset() != timedelta(0)
            or response_to.tzinfo is None
            or response_to.utcoffset() != timedelta(0)
        ):
            raise ValueError(
                "Lecturas tenant API response range must use UTC ISO datetimes."
            )
        response_from = response_from.replace(tzinfo=None)
        response_to = response_to.replace(tzinfo=None)
        if response_from != from_local or response_to != to_local:
            raise ValueError("Lecturas tenant API response range does not match the request.")

        normalized_rows = []
        ids = set()
        for index, row in enumerate(payload["data"]):
            if type(row) is not dict or set(row) != ROW_FIELDS:
                raise ValueError(
                    f"Lecturas tenant API row {index} does not match the contract."
                )
            if not isinstance(row["id"], str):
                raise ValueError(
                    f"Lecturas tenant API row {index} has an invalid id."
                )
            try:
                mqtt_id = str(UUID(row["id"]))
            except (ValueError, AttributeError) as exc:
                raise ValueError(
                    f"Lecturas tenant API row {index} has an invalid id."
                ) from exc
            if mqtt_id in ids:
                raise ValueError("Lecturas tenant API response contains duplicate ids.")
            ids.add(mqtt_id)
            if not isinstance(row["tenant"], str) or row["tenant"].casefold() != LECTURAS_TENANT:
                raise ValueError(
                    f"Lecturas tenant API row {index} has a different tenant."
                )
            if not isinstance(row["deveui"], str) or not row["deveui"].strip():
                raise ValueError(
                    f"Lecturas tenant API row {index} has an invalid deveui."
                )
            if len(row["deveui"]) > 64 or len(row["tenant"]) > 100:
                raise ValueError(
                    f"Lecturas tenant API row {index} exceeds a persisted string limit."
                )
            for field in (
                "dispositivo",
                "aplicacion",
                "sucursal",
                "topic",
                "variable",
                "valor_texto",
            ):
                if row[field] is not None and not isinstance(row[field], str):
                    raise ValueError(
                        f"Lecturas tenant API row {index} field {field} must be text or null."
                    )
            if any(
                row[field] is not None and len(row[field]) > 255
                for field in ("dispositivo", "aplicacion")
            ):
                raise ValueError(
                    f"Lecturas tenant API row {index} exceeds an indexed string limit."
                )
            if row["fecha_recepcion"] is not None and (
                not isinstance(row["fecha_recepcion"], str)
                or len(row["fecha_recepcion"]) > 100
            ):
                raise ValueError(
                    f"Lecturas tenant API row {index} fecha_recepcion is invalid."
                )
            if row["mediciones"] is not None and type(row["mediciones"]) is not dict:
                raise ValueError(
                    f"Lecturas tenant API row {index} mediciones must be an object or null."
                )
            try:
                json.dumps(row["valor"], ensure_ascii=False, allow_nan=False)
                json.dumps(row["mediciones"], ensure_ascii=False, allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Lecturas tenant API row {index} contains invalid JSON values."
                ) from exc
            try:
                fecha_local = _parse_api_datetime(row["fecha"])
            except ValueError as exc:
                raise ValueError(
                    f"Lecturas tenant API row {index} fecha must be a local ISO datetime."
                ) from exc
            if fecha_local.tzinfo is not None or not from_local <= fecha_local < to_local:
                raise ValueError(
                    f"Lecturas tenant API row {index} fecha is outside the requested local window."
                )
            if not isinstance(row["mensaje_raw"], str):
                raise ValueError(
                    f"Lecturas tenant API row {index} mensaje_raw must be JSON text."
                )
            try:
                raw_message = json.loads(row["mensaje_raw"])
                if type(raw_message) is not dict:
                    raise ValueError
                json.dumps(raw_message, allow_nan=False)
                time_span = _parse_api_datetime(raw_message["time"])
            except (
                KeyError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                raise ValueError(
                    f"Lecturas tenant API row {index} mensaje_raw.time must be a UTC ISO datetime."
                ) from exc
            if time_span.tzinfo is None or time_span.utcoffset() != timedelta(0):
                raise ValueError(
                    f"Lecturas tenant API row {index} mensaje_raw.time must use UTC."
                )

            normalized_row = dict(row)
            normalized_row["mqtt_id"] = mqtt_id
            normalized_row["fecha_local"] = fecha_local
            normalized_row["time_span_utc"] = time_span.astimezone(timezone.utc).replace(tzinfo=None)
            normalized_rows.append(normalized_row)

        return normalized_rows

    def _request_window(
        self,
        from_local: datetime,
        to_local: datetime,
    ) -> list[Dict[str, Any]]:
        if (
            not self.url
            or not self.user
            or not self.password
            or self.timeout_seconds <= 0
            or not 1 <= self.limit <= LECTURAS_TENANT_API_LIMIT
            or not 1 <= self.requests_per_minute < 100
        ):
            raise RuntimeError("Lecturas tenant API configuration is incomplete.")
        query = urlencode(
            {
                "tenant": LECTURAS_TENANT,
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

        for retry_number in range(LECTURAS_TENANT_API_MAX_RETRIES + 1):
            self._wait_for_request_slot()
            self.requests_made += 1
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"Lecturas tenant API returned HTTP {response.status}."
                        )
                    if response.headers.get_content_type() != "application/json":
                        raise RuntimeError("Lecturas tenant API did not return JSON.")
                    payload = json.loads(response.read().decode("utf-8"))
                return self.validate_payload(payload, from_local, to_local)
            except HTTPError as exc:
                if exc.code == 504:
                    raise LecturasTenantWindowTooLarge(
                        "Lecturas tenant API returned HTTP 504."
                    ) from exc
                if exc.code == 429 and retry_number < LECTURAS_TENANT_API_MAX_RETRIES:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        delay = max(float(retry_after), 60 / self.requests_per_minute)
                    except (TypeError, ValueError):
                        delay = 60
                    sleep(delay)
                    continue
                raise RuntimeError(
                    f"Lecturas tenant API returned HTTP {exc.code}."
                ) from exc
            except URLError as exc:
                raise RuntimeError("Lecturas tenant API request failed.") from exc
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("Lecturas tenant API returned invalid JSON.") from exc

        raise RuntimeError("Lecturas tenant API request failed after retries.")

    def fetch_range(
        self,
        from_local: datetime,
        to_local: datetime,
    ) -> list[Dict[str, Any]]:
        if from_local.tzinfo is not None or to_local.tzinfo is not None:
            raise ValueError("Lecturas tenant API range must use local naive datetimes.")
        if from_local >= to_local:
            raise ValueError("Lecturas tenant API range must use from_local < to_local.")
        try:
            rows = self._request_window(from_local, to_local)
        except LecturasTenantWindowTooLarge:
            rows = None
        if rows is not None and len(rows) < self.limit:
            return rows
        if to_local - from_local <= LECTURAS_TENANT_API_MIN_WINDOW:
            raise RuntimeError(
                "Lecturas tenant API window remains saturated at minimum precision."
            )

        midpoint = from_local + (to_local - from_local) / 2
        left_rows = self.fetch_range(from_local, midpoint)
        right_rows = self.fetch_range(midpoint, to_local)
        combined_rows = left_rows + right_rows
        combined_ids = {row["mqtt_id"] for row in combined_rows}
        if len(combined_ids) != len(combined_rows):
            raise RuntimeError("Lecturas tenant API split windows returned duplicate ids.")
        return combined_rows


@resource
def lecturas_tenant_api_resource(_):
    return LecturasTenantApiResource(
        url=os.getenv("CARLSJR_LECTURAS_TENANT_API_URL", ""),
        user=os.getenv("CARLSJR_LECTURAS_TENANT_API_USER", ""),
        password=os.getenv("CARLSJR_LECTURAS_TENANT_API_PASSWORD", ""),
        timeout_seconds=int(
            os.getenv("CARLSJR_LECTURAS_TENANT_API_TIMEOUT_SECONDS", "60")
        ),
    )
