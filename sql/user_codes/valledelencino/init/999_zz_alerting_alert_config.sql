SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;

IF NOT EXISTS (SELECT 1 FROM alerting.alert_config WITH (UPDLOCK, HOLDLOCK))
    BEGIN
        INSERT INTO alerting.alert_config (
            alert_config_id, user_code, version_number, config_status_id
        )
        VALUES (
            'a11e0000-0000-0000-0000-000000000001', N'valledelencino', 1,
            '50000000-0000-0000-0000-000000000001'
        );

        INSERT INTO alerting.alert_detector_operational_config (
            alert_config_id, sensor_lookback_hours,
            dagster_failure_lookback_minutes, dagster_failure_batch_size
        )
        VALUES (
            'a11e0000-0000-0000-0000-000000000001', 1, 120, 200
        );

        INSERT INTO alerting.alert_detector_row_count_config (
            alert_config_id, capture_interval_seconds, metadata_key,
            backfill_days, batch_size
        )
        VALUES (
            'a11e0000-0000-0000-0000-000000000001',
            21600, N'row_count', 28, 500
        );

        INSERT INTO alerting.alert_rule (
            alert_rule_id, alert_config_id, alert_type_id, entity_key,
            is_enabled, severity_id, evaluation_interval_seconds
        )
        VALUES
        ('a11e0000-0000-0000-0000-000000000101', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000001', NULL, 1, '30000000-0000-0000-0000-000000000003', 300),
        ('a11e0000-0000-0000-0000-000000000102', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000002', NULL, 0, '30000000-0000-0000-0000-000000000002', 300),
        ('a11e0000-0000-0000-0000-000000000103', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000003', NULL, 1, '30000000-0000-0000-0000-000000000003', 300),
        ('a11e0000-0000-0000-0000-000000000104', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000004', NULL, 1, '30000000-0000-0000-0000-000000000002', 300),
        ('a11e0000-0000-0000-0000-000000000105', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000005', NULL, 1, '30000000-0000-0000-0000-000000000003', 300),
        ('a11e0000-0000-0000-0000-000000000106', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000006', NULL, 1, '30000000-0000-0000-0000-000000000002', 21600),
        ('a11e0000-0000-0000-0000-000000000201', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000004', N'factRedingsAudits', 1, '30000000-0000-0000-0000-000000000002', 300),
        ('a11e0000-0000-0000-0000-000000000202', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000004', N'FactSensorEvents', 1, '30000000-0000-0000-0000-000000000002', 300),
        ('a11e0000-0000-0000-0000-000000000203', 'a11e0000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000004', N'factReadingsMeasurement', 1, '30000000-0000-0000-0000-000000000002', 300);

        INSERT INTO alerting.alert_rule_no_readings (
            alert_rule_id, no_readings_minutes, source_max_age_minutes
        )
        VALUES
        ('a11e0000-0000-0000-0000-000000000101', 60, 60),
        ('a11e0000-0000-0000-0000-000000000102', 15, 60);

        INSERT INTO alerting.alert_rule_pipeline_stale (
            alert_rule_id, expected_interval_minutes
        )
        VALUES
        ('a11e0000-0000-0000-0000-000000000104', 720),
        ('a11e0000-0000-0000-0000-000000000201', 60),
        ('a11e0000-0000-0000-0000-000000000202', 700),
        ('a11e0000-0000-0000-0000-000000000203', 640);

        INSERT INTO alerting.alert_rule_row_count_trend (
            alert_rule_id, warning_drop_pct, critical_drop_pct,
            minimum_observed_days_7d, minimum_observed_days_28d,
            critical_severity_id
        )
        VALUES (
            'a11e0000-0000-0000-0000-000000000106', 20, 40, 7, 28,
            '30000000-0000-0000-0000-000000000003'
        );

        EXEC alerting.usp_AlertConfig_Activate
            @alert_config_id = 'a11e0000-0000-0000-0000-000000000001';
    END;

COMMIT TRANSACTION;
GO
