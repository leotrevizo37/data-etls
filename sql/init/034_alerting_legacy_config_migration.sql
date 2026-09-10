SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF
    OBJECT_ID(N'alerting.alert_config_legacy', N'U') IS NOT NULL
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
            @alert_config_id AS alert_config_id,
            legacy.user_code,
            1 AS version_number,
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
            @alert_config_id AS alert_config_id,
            sensor_lookback_hours,
            120 AS dagster_failure_lookback_minutes,
            200 AS dagster_failure_batch_size
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
            @alert_config_id AS alert_config_id,
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
            NEWID() AS alert_rule_id,
            alert_type.code,
            NULL AS entity_key,
            COALESCE(default_rule.enabled, config.enabled) AS is_enabled,
            CASE
                WHEN alert_type.code = N'asset_row_count_trend'
                    THEN N'WARNING'
                ELSE COALESCE(default_rule.severity, config.severity)
            END AS severity,
            COALESCE(
                default_rule.evaluation_interval_seconds,
                config.evaluation_interval_seconds
            ) AS evaluation_interval_seconds,
            COALESCE(
                default_rule.no_readings_minutes,
                config.no_readings_minutes
            ) AS no_readings_minutes,
            COALESCE(
                default_rule.source_max_age_minutes,
                config.source_max_age_minutes
            ) AS source_max_age_minutes,
            COALESCE(
                default_rule.expected_interval_minutes,
                config.expected_interval_minutes
            ) AS expected_interval_minutes,
            COALESCE(default_rule.warning_drop_pct, config.warning_drop_pct)
                AS warning_drop_pct,
            COALESCE(default_rule.critical_drop_pct, config.critical_drop_pct)
                AS critical_drop_pct,
            COALESCE(
                default_rule.minimum_observed_days_7d,
                config.minimum_observed_days_7d
            ) AS minimum_observed_days_7d,
            COALESCE(
                default_rule.minimum_observed_days_28d,
                config.minimum_observed_days_28d
            ) AS minimum_observed_days_28d
        FROM alerting.dim_alert_type AS alert_type
        CROSS JOIN alerting.alert_config_legacy AS config
        LEFT JOIN alerting.alert_rule_legacy AS default_rule
            ON
                default_rule.alert_type
                = alert_type.code COLLATE DATABASE_DEFAULT
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
            NEWID() AS alert_rule_id,
            entity_rule.alert_type,
            CASE
                WHEN entity_rule.alert_type IN (
                    N'connection_lost',
                    N'late_readings'
                ) THEN LOWER(entity_rule.entity_id)
                ELSE entity_rule.entity_id
            END AS entity_key,
            COALESCE(entity_rule.enabled, default_rule.enabled, config.enabled)
                AS is_enabled,
            CASE
                WHEN entity_rule.alert_type = N'asset_row_count_trend'
                    THEN N'WARNING'
                ELSE COALESCE(
                    entity_rule.severity,
                    default_rule.severity,
                    config.severity
                )
            END AS severity,
            COALESCE(
                entity_rule.evaluation_interval_seconds,
                default_rule.evaluation_interval_seconds,
                config.evaluation_interval_seconds
            ) AS evaluation_interval_seconds,
            COALESCE(
                entity_rule.no_readings_minutes,
                default_rule.no_readings_minutes,
                config.no_readings_minutes
            ) AS no_readings_minutes,
            COALESCE(
                entity_rule.source_max_age_minutes,
                default_rule.source_max_age_minutes,
                config.source_max_age_minutes
            ) AS source_max_age_minutes,
            COALESCE(
                entity_rule.expected_interval_minutes,
                default_rule.expected_interval_minutes,
                config.expected_interval_minutes
            ) AS expected_interval_minutes,
            COALESCE(
                entity_rule.warning_drop_pct,
                default_rule.warning_drop_pct,
                config.warning_drop_pct
            ) AS warning_drop_pct,
            COALESCE(
                entity_rule.critical_drop_pct,
                default_rule.critical_drop_pct,
                config.critical_drop_pct
            ) AS critical_drop_pct,
            COALESCE(
                entity_rule.minimum_observed_days_7d,
                default_rule.minimum_observed_days_7d,
                config.minimum_observed_days_7d
            ) AS minimum_observed_days_7d,
            COALESCE(
                entity_rule.minimum_observed_days_28d,
                default_rule.minimum_observed_days_28d,
                config.minimum_observed_days_28d
            ) AS minimum_observed_days_28d
        FROM alerting.alert_rule_legacy AS entity_rule
        CROSS JOIN alerting.alert_config_legacy AS config
        LEFT JOIN alerting.alert_rule_legacy AS default_rule
            ON
                entity_rule.alert_type = default_rule.alert_type
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
            @alert_config_id AS alert_config_id,
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
        SELECT
            alert_rule_id,
            expected_interval_minutes
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
