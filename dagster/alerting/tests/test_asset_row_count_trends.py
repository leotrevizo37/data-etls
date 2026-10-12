import re
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[3]
_SIGNAL_MODEL = "\n".join(
    (_ROOT / "sql" / "init" / name).read_text(encoding="utf-8")
    for name in (
        "026_alerting_catalog_tables.sql",
        "027_alerting_catalog_values.sql",
        "028_alerting_config_model.sql",
        "029_alerting_event_model.sql",
    )
)
_VIEWS = "\n".join(
    (_ROOT / "sql" / "init" / name).read_text(encoding="utf-8")
    for name in (
        "030_serving_alert_config_validation.sql",
        "031_serving_alert_state.sql",
        "032_alerting_state_guards.sql",
        "033_alerting_catalog_guards.sql",
    )
)


def test_signal_model_uses_stable_guid_catalogs_and_append_only_events():
    for code in (
        "connection_lost",
        "late_readings",
        "pipeline_failure",
        "pipeline_stale",
        "dagster_run_failure",
        "asset_row_count_trend",
        "sidon_device_configuration",
        "sidon_alert_configuration",
        "DETECTED",
        "CLEARED",
        "INFO",
        "WARNING",
        "CRITICAL",
        "sensor_monitoring",
        "dagster_etl",
        "dagster",
        "DRAFT",
        "ACTIVE",
        "RETIRED",
        "NEW",
        "OPEN",
        "ACKNOWLEDGED",
        "IN_PROGRESS",
        "RESOLVED",
        "CLOSED",
        "FALSE_POSITIVE",
        "DUPLICATE",
        "CONFIGURATION_CHANGE",
        "NO_ACTION_REQUIRED",
        "CANCELLED",
        "EMAIL",
        "TICKET",
        "PUSH_NOTIFICATION",
        "WHATSAPP",
        "TEAMS",
        "SEND",
        "CREATE",
        "UPDATE",
        "CLOSE",
        "REOPEN",
        "PENDING",
        "PROCESSING",
        "SUCCEEDED",
        "DELIVERED",
        "RETRY_PENDING",
        "FAILED",
    ):
        assert f"N'{code}'" in _SIGNAL_MODEL
    assert _SIGNAL_MODEL.count("UNIQUEIDENTIFIER NOT NULL") >= 10
    assert "ux_alert_incident_active_condition" in _SIGNAL_MODEL
    assert "ux_alert_incident_point_occurrence" in _SIGNAL_MODEL
    assert "UQ_alert_signal_event_transition" in _SIGNAL_MODEL
    assert _VIEWS.count("append_only") == 3
    assert _VIEWS.count("_stable") == 10


def test_shared_catalog_preserves_sidon_alert_type_ids():
    catalog = (
        _ROOT / "sql" / "init" / "027_alerting_catalog_values.sql"
    ).read_text(encoding="utf-8")
    for alert_type_id, code, sort_order in (
        ("10000000-0000-0000-0000-000000000007", "sidon_device_configuration", 70),
        ("10000000-0000-0000-0000-000000000008", "sidon_alert_configuration", 80),
    ):
        rows = [line for line in catalog.splitlines() if f"N'{code}'" in line]
        assert len(rows) == 1
        assert f"('{alert_type_id}', N'{code}'," in rows[0]
        assert rows[0].rstrip(",").endswith(f", 1, 1, {sort_order})")


def test_serving_view_exposes_only_spanish_pascal_case_contract():
    expected = [
        "AlertaId",
        "CodigoAlerta",
        "CodigoUsuario",
        "TipoAlertaId",
        "TipoAlertaCodigo",
        "TipoAlertaNombre",
        "SeveridadId",
        "SeveridadCodigo",
        "SeveridadNombre",
        "FuenteDisparoId",
        "FuenteDisparoCodigo",
        "FuenteDisparoNombre",
        "EntidadId",
        "ClaveEntidad",
        "ConfiguracionAlertaId",
        "ReglaAlertaId",
        "EventoDeteccionId",
        "FechaOcurrenciaDeteccion",
        "FechaRegistroDeteccion",
        "EventoFinCondicionId",
        "FechaOcurrenciaFinCondicion",
        "FechaRegistroFinCondicion",
        "EsCondicionContinua",
        "CondicionActiva",
        "DetalleDeteccion",
        "MetadatosDeteccionJson",
        "EventoEstadoAlertaId",
        "EstadoAlertaId",
        "EstadoAlertaCodigo",
        "EstadoAlertaNombre",
        "FechaOcurrenciaEstadoAlerta",
        "FechaRegistroEstadoAlerta",
        "TipoCierreId",
        "TipoCierreCodigo",
        "TipoCierreNombre",
        "CerradaPor",
        "NotaCierre",
        "AccionTicketId",
        "TicketIdExterno",
        "FechaCreacionTicket",
        "FechaUltimoIntentoCreacionTicket",
        "CantidadIntentosCreacionTicket",
        "EstadoUltimaAccionTicketId",
        "EstadoUltimaAccionTicketCodigo",
        "EstadoUltimaAccionTicketNombre",
        "UltimoCodigoErrorTicket",
        "UltimoMensajeErrorTicket",
        "AccionCorreoId",
        "FechaUltimoIntentoCorreo",
        "CantidadIntentosCorreo",
        "FechaUltimoCorreoExitoso",
        "EstadoUltimaAccionCorreoId",
        "EstadoUltimaAccionCorreoCodigo",
        "EstadoUltimaAccionCorreoNombre",
        "UltimoCodigoErrorCorreo",
        "UltimoMensajeErrorCorreo",
        "EsLegada",
        "ClaveLegada",
    ]
    view = _VIEWS.split(
        "CREATE OR ALTER VIEW serving.vwEstadoAlertas",
        1,
    )[1].split("\nGO", 1)[0]
    aliases = [
        alias
        for alias in re.findall(
            r"\bAS ([A-Z][A-Za-z0-9]*)[,\r\n]",
            view,
        )
        if alias != "BIT"
    ]
    assert aliases == expected
    assert "COUNT(*) AS attempt_count" in view
    assert "ticket_attempts.attempt_count AS CantidadIntentosCreacionTicket" in view
    assert "email_attempts.attempt_count AS CantidadIntentosCorreo" in view
    assert "WHEN incident.is_point_in_time = 1 THEN NULL" in view


def test_configuration_validation_contract():
    assert "serving.vwValidacionConfiguracionesAlerta" in _VIEWS
    assert "usp_AlertConfig_Activate" in _VIEWS
    for validation_code in (
        "REGLA_DEFAULT_FALTANTE",
        "CODIGO_USUARIO_INVALIDO",
        "CODIGO_USUARIO_INCONSISTENTE",
        "CLAVE_METADATA_TENDENCIA_INVALIDA",
        "CLAVE_SENSOR_INVALIDA",
        "TIPO_ALERTA_INACTIVO",
        "SEVERIDAD_INACTIVA",
        "SEVERIDAD_CRITICA_TENDENCIA_INVALIDA",
        "VERSION_CONFIGURACION_NO_VIGENTE",
    ):
        assert validation_code in _VIEWS
    for error_number in range(51024, 51028):
        assert f"THROW {error_number}" in _VIEWS


def test_tenant_alert_sensor_copies_match_carlsjr():
    tenants = (
        "bafar",
        "ecosat",
        "emerson",
        "valledelencino",
    )
    for filename in (
        "operational_alerts_sensor.py",
        "asset_row_count_trend_sensor.py",
    ):
        canonical = (
            _ROOT
            / "dagster"
            / "carlsjr"
            / "src"
            / "carlsjr"
            / "defs"
            / "sensors"
            / filename
        ).read_text(encoding="utf-8")
        canonical = re.sub(
            r"minimum_interval_seconds=\d+",
            "minimum_interval_seconds=<tenant-specific>",
            canonical,
        ).rstrip()
        for tenant in tenants:
            candidate = (
                _ROOT
                / "dagster"
                / tenant
                / "src"
                / tenant
                / "defs"
                / "sensors"
                / filename
            ).read_text(encoding="utf-8").replace(
                f'_USER_CODE = "{tenant}"',
                '_USER_CODE = "carlsjr"',
            )
            candidate = re.sub(
                r"minimum_interval_seconds=\d+",
                "minimum_interval_seconds=<tenant-specific>",
                candidate,
            ).rstrip()
            assert candidate == canonical
