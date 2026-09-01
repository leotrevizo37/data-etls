import base64
import json
import math
import os
from datetime import date, datetime
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dagster import resource


RESPONSE_FIELDS = {
    "ok",
    "fechaCorte",
    "corteHora",
    "totalGerentes",
    "totalIngresaronAntesDeCorte",
    "totalSinIngresoEnCorte",
    "porcentajeCumplimiento",
    "data",
}
ROW_FIELDS = {
    "idUsuario",
    "noEmpleado",
    "nombreGerente",
    "email",
    "puesto",
    "rol",
    "activo",
    "idTienda",
    "claveTienda",
    "nombreTienda",
    "zona",
    "ultimoInicioSesion",
    "ingresoAntesDelCorte",
    "diasSinIngresar",
    "estatusCorte",
}
ROW_TYPES = {
    "idUsuario": int,
    "noEmpleado": str,
    "nombreGerente": str,
    "email": str,
    "puesto": str,
    "rol": str,
    "activo": bool,
    "idTienda": int,
    "claveTienda": str,
    "nombreTienda": str,
    "zona": str,
    "ultimoInicioSesion": str,
    "ingresoAntesDelCorte": bool,
    "diasSinIngresar": int,
    "estatusCorte": str,
}
NULLABLE_ROW_FIELDS = {"idTienda", "ultimoInicioSesion", "diasSinIngresar"}
STRING_LIMITS = {
    "noEmpleado": 50,
    "nombreGerente": 250,
    "email": 320,
    "puesto": 250,
    "rol": 250,
    "claveTienda": 50,
    "nombreTienda": 250,
    "zona": 150,
    "estatusCorte": 50,
}


class ManagerAccessApiResource:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        timeout_seconds: int,
    ) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds

    def validate_payload(
        self,
        payload: Any,
        requested_date: date,
        cutoff_hour: int,
    ) -> Dict[str, Any]:
        if type(payload) is not dict:
            raise ValueError("Manager access API response must be a JSON object.")
        if set(payload) != RESPONSE_FIELDS:
            raise ValueError("Manager access API response fields do not match the contract.")
        if payload["ok"] is not True:
            raise ValueError("Manager access API returned ok=false.")
        for field in (
            "corteHora",
            "totalGerentes",
            "totalIngresaronAntesDeCorte",
            "totalSinIngresoEnCorte",
        ):
            if type(payload[field]) is not int:
                raise ValueError(f"Manager access API field {field} must be an integer.")
        if type(payload["porcentajeCumplimiento"]) not in (int, float):
            raise ValueError(
                "Manager access API field porcentajeCumplimiento must be numeric."
            )
        if type(payload["data"]) is not list:
            raise ValueError("Manager access API field data must be an array.")
        if payload["corteHora"] != cutoff_hour:
            raise ValueError("Manager access API cutoff hour does not match the request.")
        if not 0 <= cutoff_hour <= 23:
            raise ValueError("Manager access API cutoff hour must be between 0 and 23.")

        try:
            cutoff_at = datetime.fromisoformat(payload["fechaCorte"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Manager access API field fechaCorte must be an ISO datetime."
            ) from exc
        if cutoff_at.tzinfo is not None:
            raise ValueError("Manager access API field fechaCorte must use local time.")
        if (
            cutoff_at.date() != requested_date
            or cutoff_at.hour != cutoff_hour
            or cutoff_at.minute != 0
            or cutoff_at.second != 0
            or cutoff_at.microsecond != 0
        ):
            raise ValueError("Manager access API fechaCorte does not match the request.")

        rows = payload["data"]
        keys = set()
        for index, row in enumerate(rows):
            if type(row) is not dict:
                raise ValueError(f"Manager access API row {index} must be an object.")
            if set(row) != ROW_FIELDS:
                raise ValueError(
                    f"Manager access API row {index} fields do not match the contract."
                )
            for field, expected_type in ROW_TYPES.items():
                value = row[field]
                if value is None and field in NULLABLE_ROW_FIELDS:
                    continue
                if type(value) is not expected_type:
                    raise ValueError(
                        f"Manager access API row {index} field {field} has an invalid type."
                    )
                if field in STRING_LIMITS and len(value) > STRING_LIMITS[field]:
                    raise ValueError(
                        f"Manager access API row {index} field {field} exceeds its limit."
                    )
            if row["idUsuario"] <= 0:
                raise ValueError(
                    f"Manager access API row {index} field idUsuario must be positive."
                )
            if row["idTienda"] is not None and row["idTienda"] <= 0:
                raise ValueError(
                    f"Manager access API row {index} field idTienda must be positive."
                )
            if row["diasSinIngresar"] is not None and row["diasSinIngresar"] < 0:
                raise ValueError(
                    f"Manager access API row {index} field diasSinIngresar cannot be negative."
                )
            last_access = None
            if row["ultimoInicioSesion"] is not None:
                try:
                    last_access = datetime.fromisoformat(row["ultimoInicioSesion"])
                except ValueError as exc:
                    raise ValueError(
                        "Manager access API row "
                        f"{index} field ultimoInicioSesion must be an ISO datetime."
                    ) from exc
                if last_access.tzinfo is not None:
                    raise ValueError(
                        "Manager access API row "
                        f"{index} field ultimoInicioSesion must use local time."
                    )
            expected_status = (
                "Ingresó a tiempo"
                if row["ingresoAntesDelCorte"]
                else "Sin ingreso hoy"
            )
            if row["estatusCorte"] != expected_status:
                raise ValueError(
                    f"Manager access API row {index} cutoff status is inconsistent."
                )
            if row["ingresoAntesDelCorte"] and (
                last_access is None
                or last_access.date() != requested_date
                or last_access > cutoff_at
            ):
                raise ValueError(
                    f"Manager access API row {index} cutoff evaluation is inconsistent."
                )
            key = (row["idUsuario"], row["claveTienda"].strip().casefold())
            if key in keys:
                raise ValueError("Manager access API contains duplicate snapshot keys.")
            keys.add(key)

        total = payload["totalGerentes"]
        entered = payload["totalIngresaronAntesDeCorte"]
        missing = payload["totalSinIngresoEnCorte"]
        if min(total, entered, missing) < 0:
            raise ValueError("Manager access API totals cannot be negative.")
        if total != len(rows) or total != entered + missing:
            raise ValueError("Manager access API totals are inconsistent.")
        if total == 0:
            raise ValueError("Manager access API returned an empty snapshot.")
        if entered != sum(row["ingresoAntesDelCorte"] for row in rows):
            raise ValueError("Manager access API detail does not match its totals.")
        expected_percentage = round(entered * 100 / total, 2) if total else 0.0
        percentage = float(payload["porcentajeCumplimiento"])
        if not math.isfinite(percentage) or abs(percentage - expected_percentage) > 0.01:
            raise ValueError("Manager access API percentage is inconsistent.")

        return payload

    def fetch(self, requested_date: date, cutoff_hour: int) -> Dict[str, Any]:
        if (
            not self.url
            or not self.user
            or not self.password
            or self.timeout_seconds <= 0
        ):
            raise RuntimeError("Manager access API configuration is incomplete.")
        query = urlencode(
            {
                "fecha": requested_date.isoformat(),
                "corteHora": cutoff_hour,
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
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"Manager access API returned HTTP {response.status}."
                    )
                if response.headers.get_content_type() != "application/json":
                    raise RuntimeError("Manager access API did not return JSON.")
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(
                f"Manager access API returned HTTP {exc.code}."
            ) from exc
        except URLError as exc:
            raise RuntimeError("Manager access API request failed.") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Manager access API returned invalid JSON.") from exc

        return self.validate_payload(payload, requested_date, cutoff_hour)


@resource
def manager_access_api_resource(_):
    return ManagerAccessApiResource(
        url=os.getenv("BAFAR_MANAGER_ACCESS_API_URL", ""),
        user=os.getenv("BAFAR_MANAGER_ACCESS_API_USER", ""),
        password=os.getenv("BAFAR_MANAGER_ACCESS_API_PASSWORD", ""),
        timeout_seconds=int(
            os.getenv("BAFAR_MANAGER_ACCESS_API_TIMEOUT_SECONDS", "30")
        ),
    )
