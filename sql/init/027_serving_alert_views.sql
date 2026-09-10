SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'serving')
    BEGIN
        EXEC ('CREATE SCHEMA serving');
    END;
GO

CREATE OR ALTER VIEW serving.vwValidacionConfiguracionesAlerta
AS
WITH Configuraciones AS (
    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        status.code AS status_code
    FROM alerting.alert_config AS config
    INNER JOIN alerting.dim_config_status AS status
        ON status.config_status_id = config.config_status_id
),
Problemas AS (
    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        CAST(NULL AS UNIQUEIDENTIFIER) AS alert_rule_id,
        alert_type.code AS alert_type_code,
        CAST(NULL AS NVARCHAR(450)) AS entity_key,
        CAST(N'REGLA_DEFAULT_FALTANTE' AS NVARCHAR(100)) AS validation_code,
        CAST(
            N'Falta la regla default completa para ' + alert_type.code + N'.'
            AS NVARCHAR(500)
        ) AS validation_description
    FROM Configuraciones AS config
    CROSS JOIN alerting.dim_alert_type AS alert_type
    WHERE
        alert_type.is_active = 1
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule AS alert_rule
            WHERE
                alert_rule.alert_config_id = config.alert_config_id
                AND alert_rule.alert_type_id = alert_type.alert_type_id
                AND alert_rule.entity_key IS NULL
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'CONFIG_DETECTOR_OPERATIVO_FALTANTE',
        N'Falta la configuración técnica del detector operativo.'
    FROM Configuraciones AS config
    WHERE NOT EXISTS (
        SELECT 1
        FROM alerting.alert_detector_operational_config AS detector
        WHERE detector.alert_config_id = config.alert_config_id
    )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'CONFIG_DETECTOR_TENDENCIA_FALTANTE',
        N'Falta la configuración técnica del detector de tendencias.'
    FROM Configuraciones AS config
    WHERE NOT EXISTS (
        SELECT 1
        FROM alerting.alert_detector_row_count_config AS detector
        WHERE detector.alert_config_id = config.alert_config_id
    )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_LECTURAS_FALTANTE',
        N'La regla requiere umbrales completos de ausencia de lecturas.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    WHERE
        alert_type.code IN (N'connection_lost', N'late_readings')
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_no_readings AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_PIPELINE_ATRASADO_FALTANTE',
        N'La regla requiere un intervalo esperado de ejecución.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    WHERE
        alert_type.code = N'pipeline_stale'
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_pipeline_stale AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_TENDENCIA_FALTANTE',
        N'La regla requiere umbrales completos de tendencia de volumen.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    WHERE
        alert_type.code = N'asset_row_count_trend'
        AND NOT EXISTS (
            SELECT 1
            FROM alerting.alert_rule_row_count_trend AS detail
            WHERE detail.alert_rule_id = alert_rule.alert_rule_id
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_LECTURAS_NO_PERMITIDO',
        N'La regla contiene umbrales de lecturas que no corresponden a su tipo.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    INNER JOIN alerting.alert_rule_no_readings AS detail
        ON detail.alert_rule_id = alert_rule.alert_rule_id
    WHERE alert_type.code NOT IN (N'connection_lost', N'late_readings')

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_PIPELINE_ATRASADO_NO_PERMITIDO',
        N'La regla contiene un intervalo de pipeline que no corresponde a su tipo.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    INNER JOIN alerting.alert_rule_pipeline_stale AS detail
        ON detail.alert_rule_id = alert_rule.alert_rule_id
    WHERE alert_type.code <> N'pipeline_stale'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'DETALLE_TENDENCIA_NO_PERMITIDO',
        N'La regla contiene umbrales de tendencia que no corresponden a su tipo.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    INNER JOIN alerting.alert_rule_row_count_trend AS detail
        ON detail.alert_rule_id = alert_rule.alert_rule_id
    WHERE alert_type.code <> N'asset_row_count_trend'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'CODIGO_USUARIO_INVALIDO',
        N'El código de usuario no puede estar vacío.'
    FROM Configuraciones AS config
    WHERE LEN(LTRIM(RTRIM(config.user_code))) = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'CODIGO_USUARIO_INCONSISTENTE',
        N'El código de usuario debe coincidir con el historial del tenant.'
    FROM Configuraciones AS config
    WHERE EXISTS (
        SELECT 1
        FROM alerting.alert_config AS previous_config
        INNER JOIN alerting.dim_config_status AS previous_status
            ON previous_status.config_status_id
                = previous_config.config_status_id
        WHERE
            previous_config.alert_config_id <> config.alert_config_id
            AND previous_config.user_code <> config.user_code
            AND previous_status.code IN (N'ACTIVE', N'RETIRED')
    )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'CLAVE_METADATA_TENDENCIA_INVALIDA',
        N'El detector de tendencias debe leer la metadata row_count.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_detector_row_count_config AS detector
        ON detector.alert_config_id = config.alert_config_id
    WHERE detector.metadata_key COLLATE Latin1_General_100_BIN2 <> N'row_count'

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'CLAVE_SENSOR_INVALIDA',
        N'La clave del sensor debe ser un GUID canónico en minúsculas.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    WHERE
        alert_type.code IN (N'connection_lost', N'late_readings')
        AND alert_rule.entity_key IS NOT NULL
        AND (
            TRY_CONVERT(UNIQUEIDENTIFIER, alert_rule.entity_key) IS NULL
            OR alert_rule.entity_key <> LOWER(CONVERT(
                NVARCHAR(36),
                TRY_CONVERT(UNIQUEIDENTIFIER, alert_rule.entity_key)
            ))
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'TIPO_ALERTA_INACTIVO',
        N'La regla referencia un tipo de alerta inactivo.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    WHERE alert_type.is_active = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'SEVERIDAD_INACTIVA',
        N'La regla referencia una severidad inactiva.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    INNER JOIN alerting.dim_severity AS severity
        ON severity.severity_id = alert_rule.severity_id
    WHERE severity.is_active = 0

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        alert_rule.alert_rule_id,
        alert_type.code,
        alert_rule.entity_key,
        N'SEVERIDAD_CRITICA_TENDENCIA_INVALIDA',
        N'La severidad crítica de tendencia debe usar el catálogo CRITICAL activo.'
    FROM Configuraciones AS config
    INNER JOIN alerting.alert_rule AS alert_rule
        ON alert_rule.alert_config_id = config.alert_config_id
    INNER JOIN alerting.dim_alert_type AS alert_type
        ON alert_type.alert_type_id = alert_rule.alert_type_id
    INNER JOIN alerting.alert_rule_row_count_trend AS detail
        ON detail.alert_rule_id = alert_rule.alert_rule_id
    INNER JOIN alerting.dim_severity AS critical_severity
        ON critical_severity.severity_id = detail.critical_severity_id
    WHERE
        alert_type.code = N'asset_row_count_trend'
        AND (
            critical_severity.code <> N'CRITICAL'
            OR critical_severity.is_active = 0
        )

    UNION ALL

    SELECT
        config.alert_config_id,
        config.user_code,
        config.version_number,
        config.config_status_id,
        config.status_code,
        NULL,
        NULL,
        NULL,
        N'VERSION_CONFIGURACION_NO_VIGENTE',
        N'La versión debe ser posterior a todas las versiones activas o retiradas.'
    FROM Configuraciones AS config
    WHERE
        config.status_code IN (N'DRAFT', N'ACTIVE')
        AND EXISTS (
            SELECT 1
            FROM alerting.alert_config AS previous_config
            INNER JOIN alerting.dim_config_status AS previous_status
                ON previous_status.config_status_id
                    = previous_config.config_status_id
            WHERE
                previous_config.alert_config_id <> config.alert_config_id
                AND previous_config.user_code = config.user_code
                AND previous_status.code IN (N'ACTIVE', N'RETIRED')
                AND previous_config.version_number >= config.version_number
        )
)
SELECT
    alert_config_id AS ConfiguracionAlertaId,
    user_code AS CodigoUsuario,
    version_number AS NumeroVersion,
    config_status_id AS EstadoConfiguracionId,
    status_code AS EstadoConfiguracionCodigo,
    alert_rule_id AS ReglaAlertaId,
    alert_type_code AS TipoAlertaCodigo,
    entity_key AS ClaveEntidad,
    validation_code AS CodigoValidacion,
    validation_description AS DescripcionValidacion,
    CAST(1 AS BIT) AS EsBloqueante
FROM Problemas;
GO

CREATE OR ALTER VIEW serving.vwEstadoAlertas
AS
SELECT
    incident.alert_id AS AlertaId,
    incident.alert_code AS CodigoAlerta,
    incident.user_code AS CodigoUsuario,
    alert_type.alert_type_id AS TipoAlertaId,
    alert_type.code AS TipoAlertaCodigo,
    alert_type.name_es AS TipoAlertaNombre,
    severity.severity_id AS SeveridadId,
    severity.code AS SeveridadCodigo,
    severity.name_es AS SeveridadNombre,
    trigger_source.trigger_source_id AS FuenteDisparoId,
    trigger_source.code AS FuenteDisparoCodigo,
    trigger_source.name_es AS FuenteDisparoNombre,
    incident.entity_id AS EntidadId,
    incident.entity_key AS ClaveEntidad,
    incident.alert_config_id AS ConfiguracionAlertaId,
    incident.alert_rule_id AS ReglaAlertaId,
    detection.event_id AS EventoDeteccionId,
    detection.occurred_at AS FechaOcurrenciaDeteccion,
    detection.recorded_at AS FechaRegistroDeteccion,
    clearance.event_id AS EventoFinCondicionId,
    clearance.occurred_at AS FechaOcurrenciaFinCondicion,
    clearance.recorded_at AS FechaRegistroFinCondicion,
    CAST(CASE WHEN incident.is_point_in_time = 0 THEN 1 ELSE 0 END AS BIT)
        AS EsCondicionContinua,
    CAST(
        CASE
            WHEN incident.is_point_in_time = 1 THEN NULL
            WHEN incident.cleared_at IS NULL THEN 1
            ELSE 0
        END
        AS BIT
    ) AS CondicionActiva,
    detection.details AS DetalleDeteccion,
    detection.metadata_json AS MetadatosDeteccionJson,
    workflow.workflow_event_id AS EventoEstadoAlertaId,
    workflow.workflow_status_id AS EstadoAlertaId,
    workflow.status_code AS EstadoAlertaCodigo,
    workflow.status_name AS EstadoAlertaNombre,
    workflow.occurred_at AS FechaOcurrenciaEstadoAlerta,
    workflow.recorded_at AS FechaRegistroEstadoAlerta,
    workflow.closure_type_id AS TipoCierreId,
    workflow.closure_code AS TipoCierreCodigo,
    workflow.closure_name AS TipoCierreNombre,
    workflow.closed_by AS CerradaPor,
    workflow.closing_note AS NotaCierre,
    ticket_action.action_id AS AccionTicketId,
    ticket_reference.external_reference AS TicketIdExterno,
    ticket_metrics.created_at AS FechaCreacionTicket,
    ticket_attempts.last_attempt_at AS FechaUltimoIntentoCreacionTicket,
    COALESCE(ticket_attempts.attempt_count, 0)
        AS CantidadIntentosCreacionTicket,
    ticket_latest.action_status_id AS EstadoUltimaAccionTicketId,
    ticket_latest.status_code AS EstadoUltimaAccionTicketCodigo,
    ticket_latest.status_name AS EstadoUltimaAccionTicketNombre,
    ticket_latest.error_code AS UltimoCodigoErrorTicket,
    ticket_latest.error_message AS UltimoMensajeErrorTicket,
    email_action.action_id AS AccionCorreoId,
    email_attempts.last_attempt_at AS FechaUltimoIntentoCorreo,
    COALESCE(email_attempts.attempt_count, 0) AS CantidadIntentosCorreo,
    email_metrics.last_succeeded_at AS FechaUltimoCorreoExitoso,
    email_latest.action_status_id AS EstadoUltimaAccionCorreoId,
    email_latest.status_code AS EstadoUltimaAccionCorreoCodigo,
    email_latest.status_name AS EstadoUltimaAccionCorreoNombre,
    email_latest.error_code AS UltimoCodigoErrorCorreo,
    email_latest.error_message AS UltimoMensajeErrorCorreo,
    incident.is_legacy AS EsLegada,
    incident.legacy_alert_key AS ClaveLegada
FROM alerting.alert_incident AS incident
INNER JOIN alerting.dim_alert_type AS alert_type
    ON alert_type.alert_type_id = incident.alert_type_id
LEFT JOIN alerting.dim_severity AS severity
    ON severity.severity_id = incident.severity_id
INNER JOIN alerting.dim_trigger_source AS trigger_source
    ON trigger_source.trigger_source_id = incident.trigger_source_id
OUTER APPLY (
    SELECT TOP 1
        signal.event_id,
        signal.occurred_at,
        signal.recorded_at,
        signal.details,
        signal.metadata_json
    FROM alerting.alert_signal_event AS signal
    INNER JOIN alerting.dim_signal_type AS signal_type
        ON signal_type.signal_type_id = signal.signal_type_id
    WHERE
        signal.alert_id = incident.alert_id
        AND signal_type.code = N'DETECTED'
    ORDER BY signal.event_sequence DESC
) AS detection
OUTER APPLY (
    SELECT TOP 1
        signal.event_id,
        signal.occurred_at,
        signal.recorded_at
    FROM alerting.alert_signal_event AS signal
    INNER JOIN alerting.dim_signal_type AS signal_type
        ON signal_type.signal_type_id = signal.signal_type_id
    WHERE
        signal.alert_id = incident.alert_id
        AND signal_type.code = N'CLEARED'
    ORDER BY signal.event_sequence DESC
) AS clearance
OUTER APPLY (
    SELECT TOP 1
        event.workflow_event_id,
        event.workflow_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.occurred_at,
        event.recorded_at,
        event.closure_type_id,
        closure.code AS closure_code,
        closure.name_es AS closure_name,
        event.closed_by,
        event.closing_note
    FROM alerting.alert_workflow_event AS event
    INNER JOIN alerting.dim_workflow_status AS status
        ON status.workflow_status_id = event.workflow_status_id
    LEFT JOIN alerting.dim_closure_type AS closure
        ON closure.closure_type_id = event.closure_type_id
    WHERE event.alert_id = incident.alert_id
    ORDER BY event.event_sequence DESC
) AS workflow
OUTER APPLY (
    SELECT TOP 1 event.action_id
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.alert_id = incident.alert_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
    ORDER BY event.event_sequence DESC
) AS ticket_action
OUTER APPLY (
    SELECT TOP 1
        event.action_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.error_code,
        event.error_message
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON status.action_status_id = event.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
    ORDER BY event.event_sequence DESC
) AS ticket_latest
OUTER APPLY (
    SELECT TOP 1 event.external_reference
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
        AND event.external_reference IS NOT NULL
    ORDER BY event.event_sequence DESC
) AS ticket_reference
OUTER APPLY (
    SELECT MIN(event.occurred_at) AS created_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON status.action_status_id = event.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.action_id = ticket_action.action_id
        AND action_type.code = N'TICKET'
        AND operation.code = N'CREATE'
        AND status.code IN (N'SUCCEEDED', N'DELIVERED')
) AS ticket_metrics
OUTER APPLY (
    SELECT
        COUNT(*) AS attempt_count,
        MAX(attempt.first_attempt_at) AS last_attempt_at
    FROM (
        SELECT
            event.attempt_number,
            MIN(event.occurred_at) AS first_attempt_at
        FROM alerting.alert_action_event AS event
        INNER JOIN alerting.dim_action_type AS action_type
            ON action_type.action_type_id = event.action_type_id
        INNER JOIN alerting.dim_action_operation AS operation
            ON operation.action_operation_id = event.action_operation_id
        WHERE
            event.action_id = ticket_action.action_id
            AND action_type.code = N'TICKET'
            AND operation.code = N'CREATE'
            AND event.attempt_number > 0
        GROUP BY event.attempt_number
    ) AS attempt
) AS ticket_attempts
OUTER APPLY (
    SELECT TOP 1 event.action_id
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.alert_id = incident.alert_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
    ORDER BY event.event_sequence DESC
) AS email_action
OUTER APPLY (
    SELECT TOP 1
        event.action_status_id,
        status.code AS status_code,
        status.name_es AS status_name,
        event.error_code,
        event.error_message
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON status.action_status_id = event.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.action_id = email_action.action_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
    ORDER BY event.event_sequence DESC
) AS email_latest
OUTER APPLY (
    SELECT MAX(event.occurred_at) AS last_succeeded_at
    FROM alerting.alert_action_event AS event
    INNER JOIN alerting.dim_action_status AS status
        ON status.action_status_id = event.action_status_id
    INNER JOIN alerting.dim_action_type AS action_type
        ON action_type.action_type_id = event.action_type_id
    INNER JOIN alerting.dim_action_operation AS operation
        ON operation.action_operation_id = event.action_operation_id
    WHERE
        event.action_id = email_action.action_id
        AND action_type.code = N'EMAIL'
        AND operation.code = N'SEND'
        AND status.code IN (N'SUCCEEDED', N'DELIVERED')
) AS email_metrics
OUTER APPLY (
    SELECT
        COUNT(*) AS attempt_count,
        MAX(attempt.first_attempt_at) AS last_attempt_at
    FROM (
        SELECT
            event.attempt_number,
            MIN(event.occurred_at) AS first_attempt_at
        FROM alerting.alert_action_event AS event
        INNER JOIN alerting.dim_action_type AS action_type
            ON action_type.action_type_id = event.action_type_id
        INNER JOIN alerting.dim_action_operation AS operation
            ON operation.action_operation_id = event.action_operation_id
        WHERE
            event.action_id = email_action.action_id
            AND action_type.code = N'EMAIL'
            AND operation.code = N'SEND'
            AND event.attempt_number > 0
        GROUP BY event.attempt_number
    ) AS attempt
) AS email_attempts;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_config_guard
ON alerting.alert_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM inserted AS current_row
        LEFT JOIN deleted AS previous_row
            ON previous_row.alert_config_id = current_row.alert_config_id
        INNER JOIN alerting.dim_config_status AS current_status
            ON current_status.config_status_id = current_row.config_status_id
        WHERE
            previous_row.alert_config_id IS NULL
            AND current_status.code <> N'DRAFT'
    )
        THROW 51024, 'New alert configurations must start as drafts.', 1;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        INNER JOIN inserted AS current_row
            ON current_row.alert_config_id = previous_row.alert_config_id
        WHERE
            current_row.user_code <> previous_row.user_code
            OR current_row.version_number <> previous_row.version_number
            OR current_row.created_at <> previous_row.created_at
    )
        THROW 51025, 'Alert configuration identity is immutable.', 1;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        INNER JOIN inserted AS current_row
            ON current_row.alert_config_id = previous_row.alert_config_id
        INNER JOIN alerting.dim_config_status AS previous_status
            ON previous_status.config_status_id = previous_row.config_status_id
        INNER JOIN alerting.dim_config_status AS current_status
            ON current_status.config_status_id = current_row.config_status_id
        WHERE
            previous_status.code = N'DRAFT'
            AND current_status.code NOT IN (N'DRAFT', N'ACTIVE')
    )
        THROW 51026, 'Draft alert configurations can only become active.', 1;

    IF EXISTS (
        SELECT 1
        FROM inserted AS current_row
        INNER JOIN alerting.dim_config_status AS current_status
            ON current_status.config_status_id = current_row.config_status_id
        WHERE
            current_row.updated_at < current_row.created_at
            OR current_row.activated_at < current_row.created_at
            OR current_row.retired_at < current_row.activated_at
            OR (
                current_status.code = N'DRAFT'
                AND (
                    current_row.activated_at IS NOT NULL
                    OR current_row.retired_at IS NOT NULL
                )
            )
            OR (
                current_status.code = N'ACTIVE'
                AND (
                    current_row.activated_at IS NULL
                    OR current_row.retired_at IS NOT NULL
                )
            )
            OR (
                current_status.code = N'RETIRED'
                AND (
                    current_row.activated_at IS NULL
                    OR current_row.retired_at IS NULL
                )
            )
    )
        THROW 51027, 'Alert configuration status timestamps are invalid.', 1;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.alert_config_id = previous_row.alert_config_id
        INNER JOIN alerting.dim_config_status AS previous_status
            ON previous_status.config_status_id = previous_row.config_status_id
        WHERE
            current_row.alert_config_id IS NULL
            AND previous_status.code <> N'DRAFT'
    )
        THROW 51009, 'Active and retired alert configurations cannot be deleted.', 1;

    IF EXISTS (
        SELECT 1
        FROM inserted AS current_row
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = current_row.config_status_id
        INNER JOIN serving.vwValidacionConfiguracionesAlerta AS validation
            ON validation.ConfiguracionAlertaId = current_row.alert_config_id
        WHERE
            status.code = N'ACTIVE'
            AND validation.EsBloqueante = 1
    )
        THROW 51010, 'An invalid alert configuration cannot be activated.', 1;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        INNER JOIN inserted AS current_row
            ON current_row.alert_config_id = previous_row.alert_config_id
        INNER JOIN alerting.dim_config_status AS previous_status
            ON previous_status.config_status_id = previous_row.config_status_id
        WHERE previous_status.code = N'RETIRED'
    )
        THROW 51011, 'Retired alert configurations are immutable.', 1;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        INNER JOIN inserted AS current_row
            ON current_row.alert_config_id = previous_row.alert_config_id
        INNER JOIN alerting.dim_config_status AS previous_status
            ON previous_status.config_status_id = previous_row.config_status_id
        INNER JOIN alerting.dim_config_status AS current_status
            ON current_status.config_status_id = current_row.config_status_id
        WHERE
            previous_status.code = N'ACTIVE'
            AND (
                current_status.code <> N'RETIRED'
                OR current_row.user_code <> previous_row.user_code
                OR current_row.version_number <> previous_row.version_number
            )
    )
        THROW 51012, 'Active alert configurations can only be retired.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_signal_event_append_only
ON alerting.alert_signal_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51021, 'Alert signal events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_workflow_event_append_only
ON alerting.alert_workflow_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51022, 'Alert workflow events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_action_event_append_only
ON alerting.alert_action_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51023, 'Alert action events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_guard
ON alerting.alert_rule
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_config_id FROM inserted
            UNION
            SELECT alert_config_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = changed.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51013, 'Rules outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_no_readings_guard
ON alerting.alert_rule_no_readings
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_rule_id FROM inserted
            UNION
            SELECT alert_rule_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_rule AS alert_rule
            ON alert_rule.alert_rule_id = changed.alert_rule_id
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = alert_rule.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51014, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_pipeline_stale_guard
ON alerting.alert_rule_pipeline_stale
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_rule_id FROM inserted
            UNION
            SELECT alert_rule_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_rule AS alert_rule
            ON alert_rule.alert_rule_id = changed.alert_rule_id
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = alert_rule.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51015, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_row_count_trend_guard
ON alerting.alert_rule_row_count_trend
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_rule_id FROM inserted
            UNION
            SELECT alert_rule_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_rule AS alert_rule
            ON alert_rule.alert_rule_id = changed.alert_rule_id
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = alert_rule.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51016, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_detector_operational_config_guard
ON alerting.alert_detector_operational_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_config_id FROM inserted
            UNION
            SELECT alert_config_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = changed.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51017, 'Detector settings outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_detector_row_count_config_guard
ON alerting.alert_detector_row_count_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT alert_config_id FROM inserted
            UNION
            SELECT alert_config_id FROM deleted
        ) AS changed
        INNER JOIN alerting.alert_config AS config
            ON config.alert_config_id = changed.alert_config_id
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE status.code <> N'DRAFT'
    )
        THROW 51018, 'Detector settings outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER PROCEDURE alerting.usp_AlertConfig_Activate
    @alert_config_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @user_code NVARCHAR(100);
    DECLARE @active_status_id UNIQUEIDENTIFIER;
    DECLARE @retired_status_id UNIQUEIDENTIFIER;

    SELECT @active_status_id = config_status_id
    FROM alerting.dim_config_status
    WHERE code = N'ACTIVE';

    SELECT @retired_status_id = config_status_id
    FROM alerting.dim_config_status
    WHERE code = N'RETIRED';

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT @user_code = config.user_code
        FROM alerting.alert_config AS config WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN alerting.dim_config_status AS status
            ON status.config_status_id = config.config_status_id
        WHERE
            config.alert_config_id = @alert_config_id
            AND status.code = N'DRAFT';

        IF @user_code IS NULL
            THROW 51019, 'The alert configuration does not exist or is not a draft.', 1;

        IF EXISTS (
            SELECT 1
            FROM serving.vwValidacionConfiguracionesAlerta
            WHERE
                ConfiguracionAlertaId = @alert_config_id
                AND EsBloqueante = 1
        )
            THROW 51020, 'The alert configuration has blocking validation errors.', 1;

        UPDATE alerting.alert_config
        SET
            config_status_id = @retired_status_id,
            retired_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
        WHERE
            user_code = @user_code
            AND config_status_id = @active_status_id;

        UPDATE alerting.alert_config
        SET
            config_status_id = @active_status_id,
            activated_at = SYSUTCDATETIME(),
            retired_at = NULL,
            updated_at = SYSUTCDATETIME()
        WHERE alert_config_id = @alert_config_id;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_alert_type_stable
ON alerting.dim_alert_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.alert_type_id = previous_row.alert_type_id
        WHERE
            current_row.alert_type_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
            OR current_row.is_continuous <> previous_row.is_continuous
    )
        THROW 51030, 'Alert type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_signal_type_stable
ON alerting.dim_signal_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.signal_type_id = previous_row.signal_type_id
        WHERE
            current_row.signal_type_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51031, 'Signal type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_severity_stable
ON alerting.dim_severity
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.severity_id = previous_row.severity_id
        WHERE
            current_row.severity_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51032, 'Severity identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_trigger_source_stable
ON alerting.dim_trigger_source
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.trigger_source_id = previous_row.trigger_source_id
        WHERE
            current_row.trigger_source_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51033, 'Trigger source identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_config_status_stable
ON alerting.dim_config_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.config_status_id = previous_row.config_status_id
        WHERE
            current_row.config_status_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51034, 'Configuration status identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_workflow_status_stable
ON alerting.dim_workflow_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.workflow_status_id = previous_row.workflow_status_id
        WHERE
            current_row.workflow_status_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51035, 'Workflow status identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_closure_type_stable
ON alerting.dim_closure_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.closure_type_id = previous_row.closure_type_id
        WHERE
            current_row.closure_type_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51036, 'Closure type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_type_stable
ON alerting.dim_action_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.action_type_id = previous_row.action_type_id
        WHERE
            current_row.action_type_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51037, 'Action type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_operation_stable
ON alerting.dim_action_operation
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.action_operation_id = previous_row.action_operation_id
        WHERE
            current_row.action_operation_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51038, 'Action operation identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_status_stable
ON alerting.dim_action_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM deleted AS previous_row
        LEFT JOIN inserted AS current_row
            ON current_row.action_status_id = previous_row.action_status_id
        WHERE
            current_row.action_status_id IS NULL
            OR current_row.code <> previous_row.code
            OR current_row.sort_order <> previous_row.sort_order
    )
        THROW 51039, 'Action status identifiers and codes are stable.', 1;
END;
GO
