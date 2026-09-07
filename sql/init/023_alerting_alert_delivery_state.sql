SET QUOTED_IDENTIFIER ON;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'alerting'
    )
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF OBJECT_ID(N'alerting.alert_delivery_state', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_delivery_state (
            alert_key NVARCHAR(500) NOT NULL
            CONSTRAINT PK_alert_delivery_state PRIMARY KEY,
            user_code NVARCHAR(100) NOT NULL,
            alert_type NVARCHAR(50) NOT NULL,
            entity_id NVARCHAR(250) NOT NULL,
            state NVARCHAR(20) NOT NULL,
            details NVARCHAR(MAX) NULL,
            first_detected_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_delivery_state_first_detected_at
            DEFAULT (SYSUTCDATETIME()),
            last_notified_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_delivery_state_last_notified_at
            DEFAULT (SYSUTCDATETIME()),
            resolved_at DATETIME2(3) NULL,
            notification_count INT NOT NULL
            CONSTRAINT DF_alert_delivery_state_notification_count
            DEFAULT (1),
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_delivery_state_created_at
            DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_delivery_state_updated_at
            DEFAULT (SYSUTCDATETIME())
        );
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'ix_alert_state_active'
      AND object_id = OBJECT_ID(N'alerting.alert_delivery_state')
)
    BEGIN
        CREATE INDEX ix_alert_state_active
        ON alerting.alert_delivery_state (state)
        WHERE state IN ('OPEN', 'REMINDER');
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'ux_alert_state_active_condition'
      AND object_id = OBJECT_ID(N'alerting.alert_delivery_state')
)
    BEGIN
        CREATE UNIQUE INDEX ux_alert_state_active_condition
        ON alerting.alert_delivery_state (user_code, alert_type, entity_id)
        WHERE state IN ('OPEN', 'REMINDER');
    END;
GO
