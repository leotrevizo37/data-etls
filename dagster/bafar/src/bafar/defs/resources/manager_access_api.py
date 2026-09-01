import base64
import json
import math
import os
import re
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
    "diasHistorial",
    "totalGerentes",
    "totalIngresaronAntesDeCorte",
    "totalSinIngresoEnCorte",
    "porcentajeCumplimiento",
    "totalRegistros",
    "data",
}
ROW_FIELDS = {
    "idUsuario",
    "noEmpleado",
    "nombreGerente",
    "nombreUsuario",
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
    "nombreUsuario": str,
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
    "nombreUsuario": 250,
    "email": 320,
    "puesto": 250,
    "rol": 250,
    "claveTienda": 50,
    "nombreTienda": 250,
    "zona": 150,
    "estatusCorte": 50,
}
LOCAL_ISO_DATETIME_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?"
)


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
        history_days: int,
    ) -> Dict[str, Any]:
        if type(payload) is not dict:
            raise ValueError("Manager access API response must be a JSON object.")
        if set(payload) != RESPONSE_FIELDS:
            raise ValueError("Manager access API response fields do not match the contract.")
        if payload["ok"] is not True:
            raise ValueError("Manager access API returned ok=false.")
        for field in (
            "corteHora",
            "diasHistorial",
            "totalGerentes",
            "totalIngresaronAntesDeCorte",
            "totalSinIngresoEnCorte",
            "totalRegistros",
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
        if payload["diasHistorial"] != history_days:
            raise ValueError("Manager access API history days do not match the request.")
        if not 0 <= cutoff_hour <= 23:
            raise ValueError("Manager access API cutoff hour must be between 0 and 23.")
        if not 1 <= history_days <= 730:
            raise ValueError("Manager access API history days must be between 1 and 730.")

        if not isinstance(payload["fechaCorte"], str) or not (
            LOCAL_ISO_DATETIME_PATTERN.fullmatch(payload["fechaCorte"])
        ):
            raise ValueError(
                "Manager access API field fechaCorte must be an ISO datetime."
            )
        try:
            cutoff_at = datetime.strptime(
                payload["fechaCorte"][:26],
                "%Y-%m-%dT%H:%M:%S.%f"
                if "." in payload["fechaCorte"]
                else "%Y-%m-%dT%H:%M:%S",
            )
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
                if not LOCAL_ISO_DATETIME_PATTERN.fullmatch(
                    row["ultimoInicioSesion"]
                ):
                    raise ValueError(
                        "Manager access API row "
                        f"{index} field ultimoInicioSesion must be an ISO datetime."
                    )
                try:
                    last_access = datetime.strptime(
                        row["ultimoInicioSesion"][:26],
                        "%Y-%m-%dT%H:%M:%S.%f"
                        if "." in row["ultimoInicioSesion"]
                        else "%Y-%m-%dT%H:%M:%S",
                    )
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
            if row["ingresoAntesDelCorte"]:
                expected_status = "Ingresó a tiempo"
                valid_evaluation = (
                    last_access is not None
                    and last_access.date() == requested_date
                    and last_access <= cutoff_at
                    and row["diasSinIngresar"] == 0
                )
            elif last_access is None:
                expected_status = f"Sin ingreso en los últimos {history_days} días"
                valid_evaluation = row["diasSinIngresar"] is None
            else:
                expected_status = "Sin ingreso al corte"
                expected_days_without_access = (
                    requested_date - last_access.date()
                ).days
                valid_evaluation = (
                    last_access < cutoff_at
                    and expected_days_without_access >= 1
                    and row["diasSinIngresar"] == expected_days_without_access
                )
            if row["estatusCorte"] != expected_status:
                raise ValueError(
                    f"Manager access API row {index} cutoff status is inconsistent."
                )
            if not valid_evaluation:
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
        total_records = payload["totalRegistros"]
        if min(total, entered, missing, total_records) < 0:
            raise ValueError("Manager access API totals cannot be negative.")
        manager_ids = {row["idUsuario"] for row in rows}
        entered_manager_ids = {
            row["idUsuario"] for row in rows if row["ingresoAntesDelCorte"]
        }
        missing_manager_ids = {
            row["idUsuario"] for row in rows if not row["ingresoAntesDelCorte"]
        }
        if (
            total_records != len(rows)
            or total != len(manager_ids)
            or total != entered + missing
        ):
            raise ValueError("Manager access API totals are inconsistent.")
        if entered != len(entered_manager_ids) or missing != len(missing_manager_ids):
            raise ValueError("Manager access API detail does not match its totals.")
        expected_percentage = round(entered * 100 / total, 2) if total else 0.0
        percentage = float(payload["porcentajeCumplimiento"])
        if not math.isfinite(percentage) or abs(percentage - expected_percentage) > 0.01:
            raise ValueError("Manager access API percentage is inconsistent.")

        return payload

    def fetch(
        self,
        requested_date: date,
        cutoff_hour: int,
        history_days: int,
    ) -> Dict[str, Any]:
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
                "diasHistorial": history_days,
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

        return self.validate_payload(
            payload,
            requested_date,
            cutoff_hour,
            history_days,
        )


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
