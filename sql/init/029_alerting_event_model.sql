SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'alerting.alert_incident', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_incident (
            alert_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_incident PRIMARY KEY,
            alert_code NVARCHAR(450) NULL,
            user_code NVARCHAR(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
            alert_type_id UNIQUEIDENTIFIER NOT NULL,
            severity_id UNIQUEIDENTIFIER NULL,
            trigger_source_id UNIQUEIDENTIFIER NOT NULL,
            entity_id NVARCHAR(250) COLLATE Latin1_General_100_BIN2 NOT NULL,
            entity_key NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NULL,
            alert_config_id UNIQUEIDENTIFIER NULL,
            alert_rule_id UNIQUEIDENTIFIER NULL,
            detected_at DATETIME2(3) NOT NULL,
            cleared_at DATETIME2(3) NULL,
            details NVARCHAR(MAX) NULL,
            metadata_json NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_alert_incident_metadata DEFAULT (N'{}'),
            is_point_in_time BIT NOT NULL,
            is_legacy BIT NOT NULL
            CONSTRAINT DF_alert_incident_legacy DEFAULT (0),
            legacy_alert_key NVARCHAR(450) NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_incident_created DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT FK_alert_incident_type FOREIGN KEY (alert_type_id)
            REFERENCES alerting.dim_alert_type (alert_type_id),
            CONSTRAINT FK_alert_incident_severity FOREIGN KEY (severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT FK_alert_incident_source FOREIGN KEY (trigger_source_id)
            REFERENCES alerting.dim_trigger_source (trigger_source_id),
            CONSTRAINT FK_alert_incident_rule_config
            FOREIGN KEY (alert_rule_id, alert_config_id)
            REFERENCES alerting.alert_rule (alert_rule_id, alert_config_id),
            CONSTRAINT CK_alert_incident_metadata
            CHECK (ISJSON(metadata_json) = 1),
            CONSTRAINT CK_alert_incident_clear_order
            CHECK (cleared_at IS NULL OR cleared_at >= detected_at),
            CONSTRAINT CK_alert_incident_point
            CHECK (is_point_in_time = 0 OR cleared_at IS NULL),
            CONSTRAINT CK_alert_incident_new_contract CHECK (
                is_legacy = 1
                OR (
                    alert_code IS NOT NULL
                    AND entity_key IS NOT NULL
                    AND severity_id IS NOT NULL
                    AND alert_config_id IS NOT NULL
                    AND alert_rule_id IS NOT NULL
                )
            )
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_incident_active_condition'
            AND object_id = OBJECT_ID(N'alerting.alert_incident')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_incident_active_condition
            ON alerting.alert_incident (user_code, alert_type_id, entity_key)
            WHERE is_point_in_time = 0 AND cleared_at IS NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_incident_point_occurrence'
            AND object_id = OBJECT_ID(N'alerting.alert_incident')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_incident_point_occurrence
            ON alerting.alert_incident (
                user_code,
                alert_type_id,
                entity_id,
                entity_key
            )
            WHERE is_point_in_time = 1 AND entity_key IS NOT NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_incident_legacy_key'
            AND object_id = OBJECT_ID(N'alerting.alert_incident')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_incident_legacy_key
            ON alerting.alert_incident (legacy_alert_key)
            WHERE legacy_alert_key IS NOT NULL;
    END;
GO

IF OBJECT_ID(N'alerting.alert_signal_event', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_signal_event (
            event_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_signal_event PRIMARY KEY NONCLUSTERED,
            event_sequence BIGINT IDENTITY (1, 1) NOT NULL,
            alert_id UNIQUEIDENTIFIER NOT NULL,
            signal_type_id UNIQUEIDENTIFIER NOT NULL,
            occurred_at DATETIME2(3) NOT NULL,
            recorded_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_signal_event_recorded DEFAULT (SYSUTCDATETIME()),
            details NVARCHAR(MAX) NULL,
            metadata_json NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_alert_signal_event_metadata DEFAULT (N'{}'),
            CONSTRAINT UQ_alert_signal_event_sequence
            UNIQUE CLUSTERED (event_sequence),
            CONSTRAINT UQ_alert_signal_event_transition
            UNIQUE (alert_id, signal_type_id),
            CONSTRAINT FK_alert_signal_event_alert FOREIGN KEY (alert_id)
            REFERENCES alerting.alert_incident (alert_id),
            CONSTRAINT FK_alert_signal_event_type FOREIGN KEY (signal_type_id)
            REFERENCES alerting.dim_signal_type (signal_type_id),
            CONSTRAINT CK_alert_signal_event_metadata
            CHECK (ISJSON(metadata_json) = 1)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_signal_event_recorded'
            AND object_id = OBJECT_ID(N'alerting.alert_signal_event')
    )
    BEGIN
        CREATE INDEX ix_alert_signal_event_recorded
            ON alerting.alert_signal_event (recorded_at, event_sequence);
    END;
GO

IF OBJECT_ID(N'alerting.alert_workflow_event', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_workflow_event (
            workflow_event_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_workflow_event PRIMARY KEY NONCLUSTERED,
            event_sequence BIGINT IDENTITY (1, 1) NOT NULL,
            alert_id UNIQUEIDENTIFIER NOT NULL,
            workflow_status_id UNIQUEIDENTIFIER NOT NULL,
            closure_type_id UNIQUEIDENTIFIER NULL,
            closed_by NVARCHAR(250) NULL,
            closing_note NVARCHAR(MAX) NULL,
            occurred_at DATETIME2(3) NOT NULL,
            recorded_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_event_recorded
            DEFAULT (SYSUTCDATETIME()),
            metadata_json NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_alert_workflow_event_metadata DEFAULT (N'{}'),
            CONSTRAINT UQ_alert_workflow_event_sequence
            UNIQUE CLUSTERED (event_sequence),
            CONSTRAINT FK_alert_workflow_event_alert FOREIGN KEY (alert_id)
            REFERENCES alerting.alert_incident (alert_id),
            CONSTRAINT FK_alert_workflow_event_status
            FOREIGN KEY (workflow_status_id)
            REFERENCES alerting.dim_workflow_status (workflow_status_id),
            CONSTRAINT FK_alert_workflow_event_closure
            FOREIGN KEY (closure_type_id)
            REFERENCES alerting.dim_closure_type (closure_type_id),
            CONSTRAINT CK_alert_workflow_event_metadata
            CHECK (ISJSON(metadata_json) = 1)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_workflow_event_alert'
            AND object_id = OBJECT_ID(N'alerting.alert_workflow_event')
    )
    BEGIN
        CREATE INDEX ix_alert_workflow_event_alert
            ON alerting.alert_workflow_event (alert_id, event_sequence DESC);
    END;
GO

IF OBJECT_ID(N'alerting.alert_action_event', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_action_event (
            action_event_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_action_event PRIMARY KEY NONCLUSTERED,
            event_sequence BIGINT IDENTITY (1, 1) NOT NULL,
            action_id UNIQUEIDENTIFIER NOT NULL,
            alert_id UNIQUEIDENTIFIER NOT NULL,
            source_signal_event_id UNIQUEIDENTIFIER NULL,
            action_type_id UNIQUEIDENTIFIER NOT NULL,
            action_operation_id UNIQUEIDENTIFIER NOT NULL,
            action_status_id UNIQUEIDENTIFIER NOT NULL,
            attempt_number SMALLINT NOT NULL,
            external_reference NVARCHAR(500) NULL,
            occurred_at DATETIME2(3) NOT NULL,
            recorded_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_action_event_recorded DEFAULT (SYSUTCDATETIME()),
            next_retry_at DATETIME2(3) NULL,
            error_code NVARCHAR(100) NULL,
            error_message NVARCHAR(MAX) NULL,
            metadata_json NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_alert_action_event_metadata DEFAULT (N'{}'),
            CONSTRAINT UQ_alert_action_event_sequence
            UNIQUE CLUSTERED (event_sequence),
            CONSTRAINT FK_alert_action_event_alert FOREIGN KEY (alert_id)
            REFERENCES alerting.alert_incident (alert_id),
            CONSTRAINT FK_alert_action_event_signal
            FOREIGN KEY (source_signal_event_id)
            REFERENCES alerting.alert_signal_event (event_id),
            CONSTRAINT FK_alert_action_event_type FOREIGN KEY (action_type_id)
            REFERENCES alerting.dim_action_type (action_type_id),
            CONSTRAINT FK_alert_action_event_operation
            FOREIGN KEY (action_operation_id)
            REFERENCES alerting.dim_action_operation (action_operation_id),
            CONSTRAINT FK_alert_action_event_status
            FOREIGN KEY (action_status_id)
            REFERENCES alerting.dim_action_status (action_status_id),
            CONSTRAINT CK_alert_action_event_attempt CHECK (attempt_number >= 0),
            CONSTRAINT CK_alert_action_event_metadata
            CHECK (ISJSON(metadata_json) = 1)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_action_event_alert'
            AND object_id = OBJECT_ID(N'alerting.alert_action_event')
    )
    BEGIN
        CREATE INDEX ix_alert_action_event_alert
            ON alerting.alert_action_event (
                alert_id,
                action_type_id,
                action_operation_id,
                event_sequence DESC
            );
    END;
GO
