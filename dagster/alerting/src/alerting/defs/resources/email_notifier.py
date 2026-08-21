"""SMTP email notifier resource for sending alert notifications.

Supports both individual alert emails and consolidated per-tenant
digest emails with dark-themed HTML."""

import os
import smtplib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from dagster import InitResourceContext, get_dagster_logger, resource


logger = get_dagster_logger()


@dataclass
class AlertPayload:
    """Data needed to compose an alert email."""
    alert_key: str
    user_code: str
    alert_type: str
    entity_id: str
    state: str
    details: Optional[str] = None
    notification_count: int = 1
    # Dimensional context (populated from dwh.dimSidonProdDimensions)
    location_name: Optional[str] = None
    sublocation_name: Optional[str] = None
    device_name: Optional[str] = None
    sensor_name: Optional[str] = None
    first_detected_at: Optional[datetime] = None


@dataclass
class ConsolidatedPayload:
    """All alerts for a single user_code to be sent in one email."""
    user_code: str
    active_alerts: List[AlertPayload] = field(default_factory=list)
    resolved_alerts: List[AlertPayload] = field(default_factory=list)


_TYPE_LABELS = {
    "connection_lost": "Sensor Desconectado",
    "late_readings": "Lecturas Retrasadas",
    "pipeline_failure": "Pipeline Fallido",
    "pipeline_stale": "Pipeline Atrasado",
    "dagster_run_failure": "Run de Dagster Fallido",
}

_TYPE_ICONS = {
    "connection_lost": "&#x1F50C;",   # 🔌
    "late_readings": "&#x23F1;",      # ⏱
    "pipeline_failure": "&#x274C;",   # ❌
    "pipeline_stale": "&#x1F40C;",    # 🐌
    "dagster_run_failure": "&#x26A0;", # ⚠
}

_STATE_LABELS = {
    "OPEN": "ALERTA",
    "REMINDER": "RECORDATORIO",
    "RESOLVED": "RESUELTO",
}

_STATE_BADGE_COLORS = {
    "OPEN": "#e74c3c",
    "REMINDER": "#f39c12",
    "RESOLVED": "#27ae60",
}

# ── Visual constants ──────────────────────────────────────────────
_CLR_BG = "transparent"
_CLR_HEADER = "#0f766e"
_CLR_SUMMARY_BAR = "transparent"
_CLR_CARD = "transparent"
_CLR_DETAIL_BG = "#f8fafc"
_CLR_TEXT = "#0f172a"
_CLR_TEXT_MUTED = "#64748b"
_CLR_BORDER = "#e2e8f0"
_CLR_FOOTER_BG = "transparent"
_CLR_FOOTER_TEXT = "#64748b"
_CLR_TIME_BADGE = "#f1f5f9"


def _format_time_ago(dt: Optional[datetime]) -> str:
    """Return a human-readable time-since string."""
    if dt is None:
        return ""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    total_minutes = max(0, int((now - dt).total_seconds() / 60))
    if total_minutes < 60:
        return f"{total_minutes} min"
    hours = total_minutes // 60
    if hours < 24:
        mins = total_minutes % 60
        return f"{hours}h {mins}m"
    days = hours // 24
    remaining = hours % 24
    return f"{days}d {remaining}h"


def _esc(text: Optional[str]) -> str:
    """Minimal HTML escaping."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("\n", "<br/>")
    )


# ── Individual email (legacy, kept for backward-compat) ──────────

def _build_subject(payload: AlertPayload) -> str:
    state_label = _STATE_LABELS.get(payload.state, payload.state)
    type_label = _TYPE_LABELS.get(payload.alert_type, payload.alert_type)
    subject = f"[{state_label}] {type_label} — {payload.user_code}: {payload.entity_id}"
    if payload.state == "REMINDER" and payload.notification_count > 1:
        subject = (
            f"[{state_label} #{payload.notification_count}] "
            f"{type_label} — {payload.user_code}: {payload.entity_id}"
        )
    return subject


def _build_html_body(payload: AlertPayload) -> str:
    state_label = _STATE_LABELS.get(payload.state, payload.state)
    type_label = _TYPE_LABELS.get(payload.alert_type, payload.alert_type)

    if payload.state == "RESOLVED":
        color = "#27ae60"
    elif payload.state == "REMINDER":
        color = "#f39c12"
    else:
        color = "#e74c3c"

    details_row = ""
    if payload.details:
        escaped = _esc(payload.details)
        details_row = f"""
        <tr>
            <td style="padding:8px;font-weight:bold;">Detalles</td>
            <td style="padding:8px;">{escaped}</td>
        </tr>"""

    return f"""
    <html>
    <body style="font-family:Arial,sans-serif;">
        <div style="background-color:{color};color:white;padding:12px 16px;
                    border-radius:4px;margin-bottom:16px;">
            <strong>{state_label}</strong> — {type_label}
        </div>
        <table style="border-collapse:collapse;width:100%;" border="1"
               cellpadding="0" cellspacing="0"
               bordercolor="#ddd">
            <tr>
                <td style="padding:8px;font-weight:bold;">User Code</td>
                <td style="padding:8px;">{payload.user_code}</td>
            </tr>
            <tr>
                <td style="padding:8px;font-weight:bold;">Tipo</td>
                <td style="padding:8px;">{type_label}</td>
            </tr>
            <tr>
                <td style="padding:8px;font-weight:bold;">Entidad</td>
                <td style="padding:8px;">{payload.entity_id}</td>
            </tr>
            <tr>
                <td style="padding:8px;font-weight:bold;">Estado</td>
                <td style="padding:8px;">{state_label}</td>
            </tr>
            <tr>
                <td style="padding:8px;font-weight:bold;">Notificación #</td>
                <td style="padding:8px;">{payload.notification_count}</td>
            </tr>{details_row}
        </table>
        <p style="color:#999;font-size:12px;margin-top:16px;">
            Alert key: {payload.alert_key}
        </p>
    </body>
    </html>
    """


# ── Consolidated email (new) ─────────────────────────────────────

def _build_consolidated_subject(payload: ConsolidatedPayload) -> str:
    n_active = len(payload.active_alerts)
    n_resolved = len(payload.resolved_alerts)
    now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    uc = payload.user_code.upper()
    parts = []
    if n_active:
        parts.append(f"{n_active} activa{'s' if n_active != 1 else ''}")
    if n_resolved:
        parts.append(f"{n_resolved} resuelta{'s' if n_resolved != 1 else ''}")
    counts = ", ".join(parts)
    return f"[ALERTAS OPERATIVAS] {uc} — {counts} ({now_str})"


def _build_alert_card(
    payload: AlertPayload,
    border_color: str,
) -> str:
    """Build a single alert card HTML (table-based for email compat)."""
    state_label = _STATE_LABELS.get(payload.state, payload.state)
    badge_color = _STATE_BADGE_COLORS.get(payload.state, "#e74c3c")

    # Time badge
    time_badge = ""
    if payload.first_detected_at:
        time_ago = _format_time_ago(payload.first_detected_at)
        if time_ago:
            time_badge = (
                f'&nbsp;&nbsp;<span style="background:{_CLR_TIME_BADGE};'
                f"color:#475569;padding:3px 10px;border-radius:20px;"
                f'font-size:11px;font-weight:bold;letter-spacing:0.5px;">'
                f"ABIERTA HACE {_esc(time_ago).upper()}</span>"
            )

    # Sensor hierarchy (only for sensor-type alerts)
    hierarchy_html = ""
    is_sensor_alert = payload.alert_type in ("connection_lost", "late_readings")
    if is_sensor_alert:
        rows: List[str] = []
        if payload.location_name:
            rows.append(
                f'<span style="color:{_CLR_TEXT_MUTED};font-size:13px;">'
                f"Sucursal: "
                f'<strong style="color:{_CLR_TEXT};">'
                f"{_esc(payload.location_name)}</strong></span>"
            )
        if payload.sublocation_name:
            rows.append(
                f'<span style="color:{_CLR_TEXT_MUTED};font-size:13px;">'
                f"&Aacute;rea: "
                f'<strong style="color:{_CLR_TEXT};">'
                f"{_esc(payload.sublocation_name)}</strong></span>"
            )
        if payload.device_name:
            rows.append(
                f'<span style="color:{_CLR_TEXT_MUTED};font-size:13px;">'
                f"Dispositivo: "
                f'<strong style="color:{_CLR_TEXT};">'
                f"{_esc(payload.device_name)}</strong></span>"
            )
        if payload.sensor_name:
            rows.append(
                f'<span style="color:{_CLR_TEXT_MUTED};font-size:13px;">'
                f"Sensor: "
                f'<strong style="color:{_CLR_TEXT};">'
                f"{_esc(payload.sensor_name)}</strong></span>"
            )
        if rows:
            hierarchy_html = "<br/>".join(rows) + "<br/><br/>"

    # Entity ID label
    type_label = _TYPE_LABELS.get(payload.alert_type, payload.alert_type)
    entity_line = (
        f'<span style="color:{_CLR_TEXT};font-weight:bold;font-size:18px;line-height:1.55;">'
        f"{_esc(type_label)}: {_esc(payload.entity_id)}</span>"
    )
    if is_sensor_alert and payload.sensor_name:
        # Use sensor_name as the primary label when available
        entity_line = (
            f'<span style="color:{_CLR_TEXT};font-weight:bold;font-size:18px;line-height:1.55;">'
            f"{_esc(payload.sensor_name)}</span>"
        )

    # Details box
    details_html = ""
    if payload.details:
        detail_items = [
            item.strip()
            for item in payload.details.replace("\n", ", ").split(", ")
            if item.strip()
        ]
        items_html = "".join(
            f'<li style="color:#334155;font-size:13.5px;line-height:1.55;'
            f'padding:0 0 4px 0;">{_esc(item)}</li>'
            for item in detail_items
        )
        details_html = (
            f'<table width="100%" cellpadding="0" cellspacing="0" '
            f'style="border-left:3px solid {border_color};'
            f"background-color:{_CLR_DETAIL_BG};"
            f'margin-top:12px;border-radius:0 6px 6px 0;">'
            f"<tr><td style=\"padding:12px 16px;\">"
            f'<span style="color:{_CLR_TEXT_MUTED};font-size:11px;'
            f'text-transform:uppercase;letter-spacing:1px;font-weight:bold;">'
            f"Evidencia medida</span><br/>"
            f'<ul style="margin:8px 0 0 16px;padding:0;">'
            f"{items_html}</ul>"
            f"</td></tr></table>"
        )

    # Notification count for reminders
    notif_html = ""
    if payload.state == "REMINDER" and payload.notification_count > 1:
        notif_html = (
            f'<br/><span style="color:{_CLR_TEXT_MUTED};font-size:11px;">'
            f"Notificaci&oacute;n #{payload.notification_count}</span>"
        )

    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" '
        f'style="background-color:{_CLR_CARD};margin-bottom:18px;">'
        f'<tr><td style="padding:8px 0 18px 0;">'
        # Badges
        f'<span style="background:{badge_color};color:white;'
        f"padding:3px 10px;border-radius:20px;font-size:11px;"
        f'font-weight:bold;text-transform:uppercase;letter-spacing:1px;">'
        f"{state_label}</span>"
        f"{time_badge}"
        f"{notif_html}"
        f"<br/><span style=\"line-height:12px;\">&nbsp;</span><br/>"
        # Entity
        f"{entity_line}<br/><span style=\"line-height:8px;\">&nbsp;</span><br/>"
        # Hierarchy
        f"{hierarchy_html}"
        # Details
        f"{details_html}"
        f"</td></tr></table>"
    )


def _build_section_header(title: str) -> str:
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" '
        f'style="margin-bottom:10px;margin-top:20px;">'
        f"<tr><td style=\"color:{_CLR_TEXT_MUTED};font-size:11px;"
        f"font-weight:bold;padding-bottom:6px;text-transform:uppercase;"
        f'letter-spacing:1px;">'
        f"{title}</td></tr></table>"
    )


def _build_location_header(location_name: str) -> str:
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" '
        f'style="margin-bottom:8px;margin-top:4px;">'
        f'<tr><td style="color:{_CLR_TEXT_MUTED};font-size:12px;'
        f'padding:6px 0;">'
        f"Sucursal: "
        f'<strong style="color:{_CLR_TEXT};">'
        f"{_esc(location_name)}</strong>"
        f"</td></tr></table>"
    )


def _build_consolidated_html(payload: ConsolidatedPayload) -> str:
    """Build the full consolidated HTML email for one user_code."""
    now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    n_active = len(payload.active_alerts)
    n_resolved = len(payload.resolved_alerts)
    uc_display = payload.user_code.upper().replace("_", " ")

    # Summary badges
    active_badge = ""
    if n_active:
        active_badge = (
            f'<span style="background:#dc2626;color:white;'
            f"padding:3px 10px;border-radius:20px;font-size:11px;"
            f'font-weight:bold;letter-spacing:0.5px;">'
            f"{n_active} ACTIVA{'S' if n_active != 1 else ''}</span>"
        )
    resolved_badge = ""
    if n_resolved:
        resolved_badge = (
            f'<span style="background:#16a34a;color:white;'
            f"padding:3px 10px;border-radius:20px;font-size:11px;"
            f'font-weight:bold;letter-spacing:0.5px;margin-left:8px;">'
            f"{n_resolved} RESUELTA{'S' if n_resolved != 1 else ''}</span>"
        )

    # ── Group active alerts by type, then by location ─────────────
    by_type: Dict[str, List[AlertPayload]] = defaultdict(list)
    for a in payload.active_alerts:
        by_type[a.alert_type].append(a)

    sensor_types = ("connection_lost", "late_readings")
    type_order = [
        "connection_lost",
        "late_readings",
        "pipeline_failure",
        "pipeline_stale",
        "dagster_run_failure",
    ]

    active_sections = ""
    for atype in type_order:
        alerts = by_type.get(atype)
        if not alerts:
            continue
        icon = _TYPE_ICONS.get(atype, "")
        label = _TYPE_LABELS.get(atype, atype)
        active_sections += _build_section_header(f"{icon} {label}")

        if atype in sensor_types:
            # Group by location
            by_location: Dict[str, List[AlertPayload]] = defaultdict(list)
            for a in alerts:
                loc = a.location_name or "Sin sucursal"
                by_location[loc].append(a)
            for loc_name in sorted(by_location):
                if len(by_location) > 1 or loc_name != "Sin sucursal":
                    active_sections += _build_location_header(loc_name)
                for a in by_location[loc_name]:
                    border = _STATE_BADGE_COLORS.get(a.state, "#e74c3c")
                    active_sections += _build_alert_card(a, border)
        else:
            for a in alerts:
                border = _STATE_BADGE_COLORS.get(a.state, "#e74c3c")
                active_sections += _build_alert_card(a, border)

    # ── Resolved alerts section ───────────────────────────────────
    resolved_section = ""
    if payload.resolved_alerts:
        resolved_section += _build_section_header("&#x2705; Alertas Resueltas")
        for a in payload.resolved_alerts:
            resolved_section += _build_alert_card(a, "#27ae60")

    # ── Collect all alert keys for footer ─────────────────────────
    all_keys = [a.alert_key for a in payload.active_alerts + payload.resolved_alerts]
    keys_text = ", ".join(_esc(k) for k in all_keys[:20])
    if len(all_keys) > 20:
        keys_text += f" ... y {len(all_keys) - 20} m&aacute;s"

    # ── Assemble full email ───────────────────────────────────────
    return f"""\
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>
<body style="margin:0;padding:0;background-color:{_CLR_BG};font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" \
style="background-color:{_CLR_BG};">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" \
style="max-width:600px;width:100%;">

<!-- ═══ HEADER ═══ -->
<tr><td bgcolor="{_CLR_HEADER}" \
style="background-color:{_CLR_HEADER};padding:20px 24px;\
border-radius:10px 10px 0 0;">
<span style="color:#ccfbf1;font-size:12px;font-weight:bold;\
text-transform:uppercase;letter-spacing:1.5px;">{_esc(uc_display)}</span><br/>
<span style="color:#ffffff;font-size:20px;font-weight:bold;\
line-height:31px;">Alertas Operativas</span><br/>
<span style="color:#99f6e4;font-size:13px;">{now_str}</span>
</td></tr>

<!-- ═══ CONTENT ═══ -->
<tr><td style="background-color:{_CLR_SUMMARY_BAR};padding:22px 24px 0 24px;\
border-left:1px solid {_CLR_BORDER};border-right:1px solid {_CLR_BORDER};">
{active_badge} {resolved_badge}
</td></tr>

<tr><td style="background-color:{_CLR_BG};padding:0 24px 4px 24px;\
border-left:1px solid {_CLR_BORDER};border-right:1px solid {_CLR_BORDER};">
{active_sections}
{resolved_section}
</td></tr>

<!-- ═══ FOOTER ═══ -->
<tr><td style="background-color:{_CLR_FOOTER_BG};padding:10px 24px 18px 24px;\
border-left:1px solid {_CLR_BORDER};border-right:1px solid {_CLR_BORDER};\
border-bottom:1px solid {_CLR_BORDER};border-radius:0 0 10px 10px;">
<div style="border-top:1px solid {_CLR_BORDER};padding-top:14px;">
<span style="color:{_CLR_FOOTER_TEXT};font-size:11.5px;">\
Generado autom&aacute;ticamente por el sistema de monitoreo operativo. \
Los datos provienen de las bases de datos de cada tenant en tiempo real.\
</span><br/>
<span style="color:{_CLR_FOOTER_TEXT};font-size:10px;">\
Alert keys: {keys_text}</span>
</div>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


class EmailNotifierResource:
    """Sends alert emails via SMTP."""

    def __init__(self) -> None:
        self._host = os.environ.get("ALERT_SMTP_HOST", "")
        self._port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
        self._user = os.environ.get("ALERT_SMTP_USER", "")
        self._password = os.environ.get("ALERT_SMTP_PASSWORD", "")
        self._from_addr = os.environ.get("ALERT_SMTP_FROM", "")
        self._default_to_addrs = [
            addr.strip()
            for addr in os.environ.get("ALERT_SMTP_TO", "").split(",")
            if addr.strip()
        ]
        self._use_tls = os.environ.get(
            "ALERT_SMTP_USE_TLS", "true"
        ).lower() in ("true", "1", "yes")
        self._enabled = bool(self._host and self._from_addr)

    def _resolve_recipients(
        self,
        user_code: str,
        alert_type: Optional[str] = None,
    ) -> List[str]:
        """Resolve email recipients using env-var cascade."""
        uc = "".join(
            c if c.isalnum() else "_" for c in user_code.upper()
        )
        candidates = [f"ALERT_SMTP_TO_{uc}"]
        if alert_type:
            at = "".join(
                c if c.isalnum() else "_" for c in alert_type.upper()
            )
            # More specific first
            candidates = [
                f"ALERT_SMTP_TO_{uc}_{at}",
                f"ALERT_SMTP_TO_{uc}",
                f"ALERT_SMTP_TO_{at}",
            ]
        for var in candidates:
            addrs = [
                a.strip()
                for a in os.environ.get(var, "").split(",")
                if a.strip()
            ]
            if addrs:
                return addrs
        return self._default_to_addrs

    def send(self, payload: AlertPayload) -> bool:
        """Send an individual alert email.  Returns True on success."""
        if not self._enabled:
            logger.warning(
                "Email notifier is disabled (missing SMTP config). "
                "Skipping alert: %s",
                payload.alert_key,
            )
            return False

        to_addrs = self._resolve_recipients(
            payload.user_code, payload.alert_type
        )
        if not to_addrs:
            logger.warning(
                "No recipients configured for alert: %s",
                payload.alert_key,
            )
            return False

        subject = _build_subject(payload)
        html_body = _build_html_body(payload)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._from_addr
        msg["To"] = ", ".join(to_addrs)
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self._host, self._port, timeout=30) as smtp:
                if self._use_tls:
                    smtp.starttls()
                if self._user and self._password:
                    smtp.login(self._user, self._password)
                smtp.sendmail(self._from_addr, to_addrs, msg.as_string())
            logger.info("Sent alert email: %s — %s", subject, payload.alert_key)
            return True
        except Exception:
            logger.exception(
                "Failed to send alert email for %s", payload.alert_key
            )
            return False

    def send_consolidated(
        self,
        payload: ConsolidatedPayload,
    ) -> bool:
        """Send a single consolidated email for one user_code.

        Returns True on success, False on skip/error.
        """
        if not self._enabled:
            logger.warning(
                "Email notifier is disabled (missing SMTP config). "
                "Skipping consolidated alert for %s",
                payload.user_code,
            )
            return False

        if not payload.active_alerts and not payload.resolved_alerts:
            return False

        to_addrs = self._resolve_recipients(payload.user_code)
        if not to_addrs:
            logger.warning(
                "No recipients configured for user_code: %s",
                payload.user_code,
            )
            return False

        subject = _build_consolidated_subject(payload)
        html_body = _build_consolidated_html(payload)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._from_addr
        msg["To"] = ", ".join(to_addrs)
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self._host, self._port, timeout=30) as smtp:
                if self._use_tls:
                    smtp.starttls()
                if self._user and self._password:
                    smtp.login(self._user, self._password)
                smtp.sendmail(self._from_addr, to_addrs, msg.as_string())
            logger.info(
                "Sent consolidated email: %s (%d active, %d resolved)",
                subject,
                len(payload.active_alerts),
                len(payload.resolved_alerts),
            )
            return True
        except Exception:
            logger.exception(
                "Failed to send consolidated email for %s",
                payload.user_code,
            )
            return False


@resource
def email_notifier_resource(
    _context: InitResourceContext,
) -> EmailNotifierResource:
    return EmailNotifierResource()
