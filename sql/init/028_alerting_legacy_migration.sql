SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'alerting.alert_config_legacy', N'U') IS NOT NULL
    AND OBJECT_ID(N'alerting.alert_rule_legacy', N'U') IS NOT NULL
    AND EXISTS (SELECT 1 FROM alerting.alert_config_legacy)
    AND NOT EXISTS (SELECT 1 FROM alerting.alert_config)
    BEGIN
        BEGIN TRANSACTION;

        DECLARE @alert_config_id UNIQUEIDENTIFIER = NEWID();
        DECLARE @user_code NVARCHAR(100);

        SELECT @user_code = user_code
        FROM alerting.alert_config_legacy
        WHERE config_id = 1;

        INSERT INTO alerting.alert_config (
            alert_config_id,
            user_code,
            version_number,
            config_status_id,
            created_at,
            updated_at
        )
        SELECT
            @alert_config_id,
            legacy.user_code,
            1,
            status.config_status_id,
            legacy.created_at,
            legacy.updated_at
        FROM alerting.alert_config_legacy AS legacy
        CROSS JOIN alerting.dim_config_status AS status
        WHERE
            legacy.config_id = 1
            AND status.code = N'DRAFT';

        INSERT INTO alerting.alert_detector_operational_config (
            alert_config_id,
            sensor_lookback_hours,
            dagster_failure_lookback_minutes,
            dagster_failure_batch_size
        )
        SELECT
            @alert_config_id,
            sensor_lookback_hours,
            120,
            200
        FROM alerting.alert_config_legacy
        WHERE config_id = 1;

        INSERT INTO alerting.alert_detector_row_count_config (
            alert_config_id,
            capture_interval_seconds,
            metadata_key,
            backfill_days,
            batch_size
        )
        SELECT
            @alert_config_id,
            trend_capture_interval_seconds,
            trend_metadata_key,
            trend_backfill_days,
            trend_batch_size
        FROM alerting.alert_config_legacy
        WHERE config_id = 1;

        DECLARE @rules TABLE (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL,
            alert_type NVARCHAR(50) NOT NULL,
            entity_key NVARCHAR(450) NULL,
            is_enabled BIT NOT NULL,
            severity NVARCHAR(20) NOT NULL,
            evaluation_interval_seconds INT NOT NULL,
            no_readings_minutes INT NOT NULL,
            source_max_age_minutes INT NOT NULL,
            expected_interval_minutes INT NOT NULL,
            warning_drop_pct DECIMAL(6, 2) NOT NULL,
            critical_drop_pct DECIMAL(6, 2) NOT NULL,
            minimum_observed_days_7d TINYINT NOT NULL,
            minimum_observed_days_28d TINYINT NOT NULL
        );

        INSERT INTO @rules (
            alert_rule_id,
            alert_type,
            entity_key,
            is_enabled,
            severity,
            evaluation_interval_seconds,
            no_readings_minutes,
            source_max_age_minutes,
            expected_interval_minutes,
            warning_drop_pct,
            critical_drop_pct,
            minimum_observed_days_7d,
            minimum_observed_days_28d
        )
        SELECT
            NEWID(),
            alert_type.code,
            NULL,
            COALESCE(default_rule.enabled, config.enabled),
            CASE
                WHEN alert_type.code = N'asset_row_count_trend'
                    THEN N'WARNING'
                ELSE COALESCE(default_rule.severity, config.severity)
            END,
            COALESCE(
                default_rule.evaluation_interval_seconds,
                config.evaluation_interval_seconds
            ),
            COALESCE(
                default_rule.no_readings_minutes,
                config.no_readings_minutes
            ),
            COALESCE(
                default_rule.source_max_age_minutes,
                config.source_max_age_minutes
            ),
            COALESCE(
                default_rule.expected_interval_minutes,
                config.expected_interval_minutes
            ),
            COALESCE(default_rule.warning_drop_pct, config.warning_drop_pct),
            COALESCE(default_rule.critical_drop_pct, config.critical_drop_pct),
            COALESCE(
                default_rule.minimum_observed_days_7d,
                config.minimum_observed_days_7d
            ),
            COALESCE(
                default_rule.minimum_observed_days_28d,
                config.minimum_observed_days_28d
            )
        FROM alerting.dim_alert_type AS alert_type
        CROSS JOIN alerting.alert_config_legacy AS config
        LEFT JOIN alerting.alert_rule_legacy AS default_rule
            ON default_rule.alert_type =
                alert_type.code COLLATE DATABASE_DEFAULT
            AND default_rule.entity_id = N''
        WHERE
            alert_type.is_active = 1
            AND config.config_id = 1;

        INSERT INTO @rules (
            alert_rule_id,
            alert_type,
            entity_key,
            is_enabled,
            severity,
            evaluation_interval_seconds,
            no_readings_minutes,
            source_max_age_minutes,
            expected_interval_minutes,
            warning_drop_pct,
            critical_drop_pct,
            minimum_observed_days_7d,
            minimum_observed_days_28d
        )
        SELECT
            NEWID(),
            entity_rule.alert_type,
            CASE
                WHEN entity_rule.alert_type IN (
                    N'connection_lost',
                    N'late_readings'
                ) THEN LOWER(entity_rule.entity_id)
                ELSE entity_rule.entity_id
            END,
            COALESCE(entity_rule.enabled, default_rule.enabled, config.enabled),
            CASE
                WHEN entity_rule.alert_type = N'asset_row_count_trend'
                    THEN N'WARNING'
                ELSE COALESCE(
                    entity_rule.severity,
                    default_rule.severity,
                    config.severity
                )
            END,
            COALESCE(
                entity_rule.evaluation_interval_seconds,
                default_rule.evaluation_interval_seconds,
                config.evaluation_interval_seconds
            ),
            COALESCE(
                entity_rule.no_readings_minutes,
                default_rule.no_readings_minutes,
                config.no_readings_minutes
            ),
            COALESCE(
                entity_rule.source_max_age_minutes,
                default_rule.source_max_age_minutes,
                config.source_max_age_minutes
            ),
            COALESCE(
                entity_rule.expected_interval_minutes,
                default_rule.expected_interval_minutes,
                config.expected_interval_minutes
            ),
            COALESCE(
                entity_rule.warning_drop_pct,
                default_rule.warning_drop_pct,
                config.warning_drop_pct
            ),
            COALESCE(
                entity_rule.critical_drop_pct,
                default_rule.critical_drop_pct,
                config.critical_drop_pct
            ),
            COALESCE(
                entity_rule.minimum_observed_days_7d,
                default_rule.minimum_observed_days_7d,
                config.minimum_observed_days_7d
            ),
            COALESCE(
                entity_rule.minimum_observed_days_28d,
                default_rule.minimum_observed_days_28d,
                config.minimum_observed_days_28d
            )
        FROM alerting.alert_rule_legacy AS entity_rule
        CROSS JOIN alerting.alert_config_legacy AS config
        LEFT JOIN alerting.alert_rule_legacy AS default_rule
            ON default_rule.alert_type = entity_rule.alert_type
            AND default_rule.entity_id = N''
        WHERE
            entity_rule.entity_id <> N''
            AND config.config_id = 1;

        INSERT INTO alerting.alert_rule (
            alert_rule_id,
            alert_config_id,
            alert_type_id,
            entity_key,
            is_enabled,
            severity_id,
            evaluation_interval_seconds
        )
        SELECT
            alert_rule.alert_rule_id,
            @alert_config_id,
            alert_type.alert_type_id,
            alert_rule.entity_key,
            alert_rule.is_enabled,
            severity.severity_id,
            alert_rule.evaluation_interval_seconds
        FROM @rules AS alert_rule
        INNER JOIN alerting.dim_alert_type AS alert_type
            ON alert_type.code COLLATE DATABASE_DEFAULT = alert_rule.alert_type
        INNER JOIN alerting.dim_severity AS severity
            ON severity.code COLLATE DATABASE_DEFAULT = alert_rule.severity;

        INSERT INTO alerting.alert_rule_no_readings (
            alert_rule_id,
            no_readings_minutes,
            source_max_age_minutes
        )
        SELECT
            alert_rule_id,
            no_readings_minutes,
            source_max_age_minutes
        FROM @rules
        WHERE alert_type IN (N'connection_lost', N'late_readings');

        INSERT INTO alerting.alert_rule_pipeline_stale (
            alert_rule_id,
            expected_interval_minutes
        )
        SELECT alert_rule_id, expected_interval_minutes
        FROM @rules
        WHERE alert_type = N'pipeline_stale';

        INSERT INTO alerting.alert_rule_row_count_trend (
            alert_rule_id,
            warning_drop_pct,
            critical_drop_pct,
            minimum_observed_days_7d,
            minimum_observed_days_28d,
            critical_severity_id
        )
        SELECT
            alert_rule.alert_rule_id,
            alert_rule.warning_drop_pct,
            alert_rule.critical_drop_pct,
            alert_rule.minimum_observed_days_7d,
            alert_rule.minimum_observed_days_28d,
            severity.severity_id
        FROM @rules AS alert_rule
        CROSS JOIN alerting.dim_severity AS severity
        WHERE
            alert_rule.alert_type = N'asset_row_count_trend'
            AND severity.code = N'CRITICAL';

        EXEC alerting.usp_AlertConfig_Activate @alert_config_id;

        COMMIT TRANSACTION;
    END;
GO

IF OBJECT_ID(N'alerting.alert_workflow_state_legacy', N'U') IS NOT NULL
    BEGIN
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
            NEWID(),
            NULL,
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
            ),
            alert_type.alert_type_id,
            severity.severity_id,
            trigger_source.trigger_source_id,
            legacy.entity_id,
            CASE
                WHEN legacy.alert_type = N'dagster_run_failure' THEN NULL
                ELSE legacy.entity_id
            END,
            NULL,
            NULL,
            legacy.first_detected_at,
            CASE
                WHEN
                    legacy.alert_type <> N'dagster_run_failure'
                    AND legacy.state IN (N'RESOLVED_PENDING', N'RESOLVED')
                    THEN COALESCE(legacy.resolved_at, legacy.updated_at)
                ELSE NULL
            END,
            legacy.details,
            legacy.metadata_json,
            CASE
                WHEN legacy.alert_type = N'dagster_run_failure' THEN 1
                ELSE 0
            END,
            1,
            legacy.alert_key,
            legacy.created_at
        FROM alerting.alert_workflow_state_legacy AS legacy
        INNER JOIN alerting.dim_alert_type AS alert_type
            ON alert_type.code COLLATE DATABASE_DEFAULT = legacy.alert_type
        LEFT JOIN alerting.dim_severity AS severity
            ON severity.code COLLATE DATABASE_DEFAULT = legacy.severity
        INNER JOIN alerting.dim_trigger_source AS trigger_source
            ON trigger_source.code COLLATE DATABASE_DEFAULT =
                legacy.trigger_source
        OUTER APPLY (
            SELECT TOP 1 user_code
            FROM alerting.alert_config_legacy
            WHERE config_id = 1
        ) AS legacy_config
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
            NEWID(),
            incident.alert_id,
            signal_type.signal_type_id,
            legacy.first_detected_at,
            legacy.created_at,
            legacy.details,
            legacy.metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON legacy.alert_key = incident.legacy_alert_key
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
            NEWID(),
            incident.alert_id,
            signal_type.signal_type_id,
            COALESCE(legacy.resolved_at, legacy.updated_at),
            legacy.updated_at,
            legacy.details,
            legacy.metadata_json
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON legacy.alert_key = incident.legacy_alert_key
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
            NEWID(),
            incident.alert_id,
            workflow_status.workflow_status_id,
            COALESCE(legacy.resolved_at, legacy.updated_at),
            legacy.updated_at,
            CONCAT(N'{"legacy_state":"', legacy.state, N'"}')
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON legacy.alert_key = incident.legacy_alert_key
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
            NEWID(),
            NEWID(),
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
            ),
            legacy.updated_at,
            legacy.next_retry_at,
            legacy.last_error_code,
            legacy.last_error_message,
            CONCAT(
                N'{"legacy_notification_type":"',
                legacy.notification_type,
                N'"}'
            )
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON legacy.alert_key = incident.legacy_alert_key
        CROSS JOIN alerting.dim_action_type AS action_type
        CROSS JOIN alerting.dim_action_operation AS operation
        INNER JOIN alerting.dim_action_status AS action_status
            ON action_status.code COLLATE DATABASE_DEFAULT =
                CASE legacy.notification_status
                WHEN N'SENT' THEN N'DELIVERED'
                ELSE legacy.notification_status
            END
        OUTER APPLY (
            SELECT TOP 1 signal.event_id
            FROM alerting.alert_signal_event AS signal
            INNER JOIN alerting.dim_signal_type AS signal_type
                ON signal_type.signal_type_id = signal.signal_type_id
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
                    ON existing_type.action_type_id = existing.action_type_id
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
            NEWID(),
            NEWID(),
            incident.alert_id,
            source_event.event_id,
            action_type.action_type_id,
            operation.action_operation_id,
            action_status.action_status_id,
            0,
            legacy.ticket_id,
            COALESCE(legacy.ticket_created_at, legacy.updated_at),
            legacy.updated_at,
            N'{}'
        FROM alerting.alert_incident AS incident
        INNER JOIN alerting.alert_workflow_state_legacy AS legacy
            ON legacy.alert_key = incident.legacy_alert_key
        CROSS JOIN alerting.dim_action_type AS action_type
        CROSS JOIN alerting.dim_action_operation AS operation
        CROSS JOIN alerting.dim_action_status AS action_status
        OUTER APPLY (
            SELECT TOP 1 signal.event_id
            FROM alerting.alert_signal_event AS signal
            INNER JOIN alerting.dim_signal_type AS signal_type
                ON signal_type.signal_type_id = signal.signal_type_id
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
                    ON existing_type.action_type_id = existing.action_type_id
                WHERE
                    existing.alert_id = incident.alert_id
                    AND existing_type.code = N'TICKET'
            );
    END;
GO
