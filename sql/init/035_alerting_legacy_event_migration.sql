SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'alerting.alert_workflow_state_legacy', N'U') IS NOT NULL
    BEGIN
        WITH legacy_config AS (
            SELECT TOP 1 user_code
            FROM alerting.alert_config_legacy
            WHERE config_id = 1
        )

        INSERT INTO alerting.alert_incident (
            alert_id,
            alert_code,
            user_code,
            alert_type_id,
            severity_id,
            trigger_source_id,
            entity_id,
            entity_key,
            alert_config_id,
            alert_rule_id,
            detected_at,
            cleared_at,
            details,
            metadata_json,
            is_point_in_time,
            is_legacy,
            legacy_alert_key,
            created_at
        )
        SELECT
            NEWID() AS alert_id,
            NULL AS alert_code,
            COALESCE(
                legacy_config.user_code,
                CASE
                    WHEN CHARINDEX(N'::', legacy.alert_key) > 1
                        THEN LEFT(
                            legacy.alert_key,
                            CHARINDEX(N'::', legacy.alert_key) - 1
                        )
                    ELSE N'legacy'
                END
            ) AS user_code,
            alert_type.alert_type_id,
            severity.severity_id,
            trigger_source.trigger_source_id,
            legacy.entity_id,
            CASE
                WHEN legacy.alert_type = N'dagster_run_failure' THEN NULL
                ELSE legacy.entity_id
            END AS entity_key,
            NULL AS alert_config_id,
            NULL AS alert_rule_id,
            legacy.first_detected_at,
            CASE
                WHEN
                    legacy.alert_type <> N'dagster_run_failure'
                    AND legacy.state IN (N'RESOLVED_PENDING', N'RESOLVED')
                    THEN COALESCE(legacy.resolved_at, legacy.updated_at)
            END AS cleared_at,
            legacy.details,
            legacy.metadata_json,
            CASE
                WHEN legacy.alert_type = N'dagster_run_failure' THEN 1
                ELSE 0
            END AS is_point_in_time,
            1 AS is_legacy,
            legacy.alert_key,
            legacy.created_at
        FROM alerting.alert_workflow_state_legacy AS legacy
        INNER JOIN alerting.dim_alert_type AS alert_type
            ON alert_type.code COLLATE DATABASE_DEFAULT = legacy.alert_type
        LEFT JOIN alerting.dim_severity AS severity
            ON severity.code COLLATE DATABASE_DEFAULT = legacy.severity
        INNER JOIN alerting.dim_trigger_source AS trigger_source
            ON
                trigger_source.code COLLATE DATABASE_DEFAULT
                = legacy.trigger_source
        OUTER APPLY legacy_config
        WHERE NOT EXISTS (
            SELECT 1
            FROM alerting.alert_incident AS incident
            WHERE incident.legacy_alert_key = legacy.alert_key
        );

        INSERT INTO alerting.alert_signal_event (
            event_id,
            alert_id,
            signal_type_id,
            occurred_at,
            recorded_at,
            details,
            metadata_json
        )
        SELECT
            NEWID() AS event_id,
            incident.alert_id,
            signal_type.signal_type_id,
            legacy.first_detected_at,
            legacy.created_at,
            legacy.details,
            legacy.metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON incident.legacy_alert_key = legacy.alert_key
        CROSS JOIN alerting.dim_signal_type AS signal_type
        WHERE
            incident.is_legacy = 1
            AND signal_type.code = N'DETECTED'
            AND NOT EXISTS (
                SELECT 1
                FROM alerting.alert_signal_event AS event
                WHERE
                    event.alert_id = incident.alert_id
                    AND event.signal_type_id = signal_type.signal_type_id
            );

        INSERT INTO alerting.alert_signal_event (
            event_id,
            alert_id,
            signal_type_id,
            occurred_at,
            recorded_at,
            details,
            metadata_json
        )
        SELECT
            NEWID() AS event_id,
            incident.alert_id,
            signal_type.signal_type_id,
            COALESCE(legacy.resolved_at, legacy.updated_at) AS occurred_at,
            legacy.updated_at,
            legacy.details,
            legacy.metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON incident.legacy_alert_key = legacy.alert_key
        CROSS JOIN alerting.dim_signal_type AS signal_type
        WHERE
            incident.is_legacy = 1
            AND incident.is_point_in_time = 0
            AND legacy.state IN (N'RESOLVED_PENDING', N'RESOLVED')
            AND signal_type.code = N'CLEARED'
            AND NOT EXISTS (
                SELECT 1
                FROM alerting.alert_signal_event AS event
                WHERE
                    event.alert_id = incident.alert_id
                    AND event.signal_type_id = signal_type.signal_type_id
            );

        INSERT INTO alerting.alert_workflow_event (
            workflow_event_id,
            alert_id,
            workflow_status_id,
            occurred_at,
            recorded_at,
            metadata_json
        )
        SELECT
            NEWID() AS workflow_event_id,
            incident.alert_id,
            workflow_status.workflow_status_id,
            COALESCE(legacy.resolved_at, legacy.updated_at) AS occurred_at,
            legacy.updated_at,
            CONCAT(N'{"legacy_state":"', legacy.state, N'"}') AS metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON incident.legacy_alert_key = legacy.alert_key
        INNER JOIN alerting.dim_workflow_status AS workflow_status
            ON workflow_status.code COLLATE DATABASE_DEFAULT = CASE
                WHEN legacy.state = N'OPEN' THEN N'OPEN'
                ELSE N'RESOLVED'
            END
        WHERE
            incident.is_legacy = 1
            AND NOT EXISTS (
                SELECT 1
                FROM alerting.alert_workflow_event AS event
                WHERE event.alert_id = incident.alert_id
            );

        INSERT INTO alerting.alert_action_event (
            action_event_id,
            action_id,
            alert_id,
            source_signal_event_id,
            action_type_id,
            action_operation_id,
            action_status_id,
            attempt_number,
            occurred_at,
            recorded_at,
            next_retry_at,
            error_code,
            error_message,
            metadata_json
        )
        SELECT
            NEWID() AS action_event_id,
            NEWID() AS action_id,
            incident.alert_id,
            source_event.event_id,
            action_type.action_type_id,
            operation.action_operation_id,
            action_status.action_status_id,
            legacy.notification_attempt_count,
            COALESCE(
                legacy.last_notification_attempt_at,
                legacy.notification_queued_at,
                legacy.updated_at
            ) AS occurred_at,
            legacy.updated_at,
            legacy.next_retry_at,
            legacy.last_error_code,
            legacy.last_error_message,
            CONCAT(
                N'{"legacy_notification_type":"',
                legacy.notification_type,
                N'"}'
            ) AS metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON incident.legacy_alert_key = legacy.alert_key
        CROSS JOIN alerting.dim_action_type AS action_type
        CROSS JOIN alerting.dim_action_operation AS operation
        INNER JOIN alerting.dim_action_status AS action_status
            ON
                action_status.code COLLATE DATABASE_DEFAULT
                = CASE legacy.notification_status
                    WHEN N'SENT' THEN N'DELIVERED'
                    ELSE legacy.notification_status
                END
        OUTER APPLY (
            SELECT TOP 1 signal.event_id
            FROM alerting.alert_signal_event AS signal
            INNER JOIN alerting.dim_signal_type AS signal_type
                ON signal.signal_type_id = signal_type.signal_type_id
            WHERE
                signal.alert_id = incident.alert_id
                AND signal_type.code = CASE
                    WHEN legacy.notification_type = N'RESOLUTION'
                        THEN N'CLEARED'
                    ELSE N'DETECTED'
                END
            ORDER BY signal.event_sequence DESC
        ) AS source_event
        WHERE
            incident.is_legacy = 1
            AND action_type.code = N'EMAIL'
            AND operation.code = N'SEND'
            AND NOT EXISTS (
                SELECT 1
                FROM alerting.alert_action_event AS existing
                INNER JOIN alerting.dim_action_type AS existing_type
                    ON existing.action_type_id = existing_type.action_type_id
                WHERE
                    existing.alert_id = incident.alert_id
                    AND existing_type.code = N'EMAIL'
            );

        INSERT INTO alerting.alert_action_event (
            action_event_id,
            action_id,
            alert_id,
            source_signal_event_id,
            action_type_id,
            action_operation_id,
            action_status_id,
            attempt_number,
            external_reference,
            occurred_at,
            recorded_at,
            metadata_json
        )
        SELECT
            NEWID() AS action_event_id,
            NEWID() AS action_id,
            incident.alert_id,
            source_event.event_id,
            action_type.action_type_id,
            operation.action_operation_id,
            action_status.action_status_id,
            0 AS attempt_number,
            legacy.ticket_id,
            COALESCE(legacy.ticket_created_at, legacy.updated_at) AS occurred_at,
            legacy.updated_at,
            N'{}' AS metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON incident.legacy_alert_key = legacy.alert_key
        CROSS JOIN alerting.dim_action_type AS action_type
        CROSS JOIN alerting.dim_action_operation AS operation
        CROSS JOIN alerting.dim_action_status AS action_status
        OUTER APPLY (
            SELECT TOP 1 signal.event_id
            FROM alerting.alert_signal_event AS signal
            INNER JOIN alerting.dim_signal_type AS signal_type
                ON signal.signal_type_id = signal_type.signal_type_id
            WHERE
                signal.alert_id = incident.alert_id
                AND signal_type.code = N'DETECTED'
            ORDER BY signal.event_sequence DESC
        ) AS source_event
        WHERE
            incident.is_legacy = 1
            AND (legacy.ticket_id IS NOT NULL OR legacy.ticket_created_at IS NOT NULL)
            AND action_type.code = N'TICKET'
            AND operation.code = N'CREATE'
            AND action_status.code = N'SUCCEEDED'
            AND NOT EXISTS (
                SELECT 1
                FROM alerting.alert_action_event AS existing
                INNER JOIN alerting.dim_action_type AS existing_type
                    ON existing.action_type_id = existing_type.action_type_id
                WHERE
                    existing.alert_id = incident.alert_id
                    AND existing_type.code = N'TICKET'
            );
    END;
GO
