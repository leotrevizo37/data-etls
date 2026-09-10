SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'alerting')
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NOT NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_workflow_state_legacy', N'U') IS NOT NULL
            THROW 51000, 'alert_workflow_state_legacy already exists.', 1;

        EXEC sys.sp_rename
            N'alerting.alert_workflow_state',
            N'alert_workflow_state_legacy';
    END;
GO

IF OBJECT_ID(N'alerting.alert_config', N'U') IS NOT NULL
    AND COL_LENGTH(N'alerting.alert_config', N'alert_config_id') IS NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_config_legacy', N'U') IS NOT NULL
            THROW 51001, 'alert_config_legacy already exists.', 1;

        EXEC sys.sp_rename N'alerting.alert_config', N'alert_config_legacy';
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule', N'U') IS NOT NULL
    AND COL_LENGTH(N'alerting.alert_rule', N'alert_rule_id') IS NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_rule_legacy', N'U') IS NOT NULL
            THROW 51002, 'alert_rule_legacy already exists.', 1;

        EXEC sys.sp_rename N'alerting.alert_rule', N'alert_rule_legacy';
    END;
GO

IF OBJECT_ID(N'alerting.dim_alert_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_alert_type (
            alert_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_alert_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_continuous BIT NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_alert_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_signal_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_signal_type (
            signal_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_signal_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_signal_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_severity', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_severity (
            severity_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_severity PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_severity_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_trigger_source', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_trigger_source (
            trigger_source_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_trigger_source PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_trigger_source_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_config_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_config_status (
            config_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_config_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_config_status_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_workflow_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_workflow_status (
            workflow_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_workflow_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_workflow_status_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_closure_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_closure_type (
            closure_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_closure_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_closure_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_type (
            action_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_operation', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_operation (
            action_operation_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_operation PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_operation_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_status (
            action_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_status_code UNIQUE (code)
        );
    END;
GO

MERGE alerting.dim_alert_type AS target
USING (
    VALUES
        ('10000000-0000-0000-0000-000000000001', N'connection_lost', N'Comunicación perdida', N'El sensor superó el tiempo permitido sin comunicar lecturas.', 1, 1, 10),
        ('10000000-0000-0000-0000-000000000002', N'late_readings', N'Lecturas retrasadas', N'El sensor superó el umbral configurado para lecturas retrasadas.', 1, 1, 20),
        ('10000000-0000-0000-0000-000000000003', N'pipeline_failure', N'Fallo de ingesta', N'La última ejecución terminal de una ingesta terminó con error.', 1, 1, 30),
        ('10000000-0000-0000-0000-000000000004', N'pipeline_stale', N'Ingesta atrasada', N'La última ejecución exitosa superó su intervalo esperado.', 1, 1, 40),
        ('10000000-0000-0000-0000-000000000005', N'dagster_run_failure', N'Fallo de ejecución Dagster', N'Una corrida Dagster falló mientras materializaba un asset comprobable.', 0, 1, 50),
        ('10000000-0000-0000-0000-000000000006', N'asset_row_count_trend', N'Caída de volumen del asset', N'El volumen materializado cayó frente a los promedios configurados.', 1, 1, 60)
) AS source (
    alert_type_id, code, name_es, description_es, is_continuous,
    is_active, sort_order
)
ON target.alert_type_id = source.alert_type_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_continuous = source.is_continuous,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        alert_type_id, code, name_es, description_es, is_continuous,
        is_active, sort_order
    )
    VALUES (
        source.alert_type_id, source.code, source.name_es,
        source.description_es, source.is_continuous, source.is_active,
        source.sort_order
    );
GO

MERGE alerting.dim_signal_type AS target
USING (
    VALUES
        ('20000000-0000-0000-0000-000000000001', N'DETECTED', N'Detectada', N'Comenzó una condición comprobada por el detector.', 1, 10),
        ('20000000-0000-0000-0000-000000000002', N'CLEARED', N'Condición terminada', N'Existe evidencia posterior que demuestra que la condición terminó.', 1, 20)
) AS source (signal_type_id, code, name_es, description_es, is_active, sort_order)
ON target.signal_type_id = source.signal_type_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (signal_type_id, code, name_es, description_es, is_active, sort_order)
    VALUES (
        source.signal_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_severity AS target
USING (
    VALUES
        ('30000000-0000-0000-0000-000000000001', N'INFO', N'Informativa', N'Condición informativa que no requiere atención inmediata.', 1, 10),
        ('30000000-0000-0000-0000-000000000002', N'WARNING', N'Advertencia', N'Condición que requiere seguimiento operativo.', 1, 20),
        ('30000000-0000-0000-0000-000000000003', N'CRITICAL', N'Crítica', N'Condición que requiere atención prioritaria.', 1, 30)
) AS source (severity_id, code, name_es, description_es, is_active, sort_order)
ON target.severity_id = source.severity_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (severity_id, code, name_es, description_es, is_active, sort_order)
    VALUES (
        source.severity_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_trigger_source AS target
USING (
    VALUES
        ('40000000-0000-0000-0000-000000000001', N'sensor_monitoring', N'Monitoreo de sensores', N'Evidencia operativa producida por el monitoreo de lecturas IoT.', 1, 10),
        ('40000000-0000-0000-0000-000000000002', N'dagster_etl', N'Control de ingestiones', N'Evidencia de ejecución registrada en el control de ingestiones.', 1, 20),
        ('40000000-0000-0000-0000-000000000003', N'dagster', N'Dagster', N'Evidencia obtenida de corridas o materializaciones de Dagster.', 1, 30)
) AS source (
    trigger_source_id, code, name_es, description_es, is_active, sort_order
)
ON target.trigger_source_id = source.trigger_source_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        trigger_source_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.trigger_source_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_config_status AS target
USING (
    VALUES
        ('50000000-0000-0000-0000-000000000001', N'DRAFT', N'Borrador', N'Configuración editable que todavía no gobierna la detección.', 1, 10),
        ('50000000-0000-0000-0000-000000000002', N'ACTIVE', N'Activa', N'Única versión vigente que gobierna nuevas detecciones.', 1, 20),
        ('50000000-0000-0000-0000-000000000003', N'RETIRED', N'Retirada', N'Versión histórica inmutable que dejó de estar vigente.', 1, 30)
) AS source (
    config_status_id, code, name_es, description_es, is_active, sort_order
)
ON target.config_status_id = source.config_status_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        config_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.config_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_workflow_status AS target
USING (
    VALUES
        ('60000000-0000-0000-0000-000000000001', N'NEW', N'Nueva', N'La alerta fue recibida y aún no ha sido gestionada.', 1, 10),
        ('60000000-0000-0000-0000-000000000002', N'OPEN', N'Abierta', N'La alerta está abierta y requiere seguimiento.', 1, 20),
        ('60000000-0000-0000-0000-000000000003', N'ACKNOWLEDGED', N'Reconocida', N'Una entidad responsable confirmó que conoce la alerta.', 1, 30),
        ('60000000-0000-0000-0000-000000000004', N'IN_PROGRESS', N'En atención', N'La alerta se encuentra bajo atención operativa.', 1, 40),
        ('60000000-0000-0000-0000-000000000005', N'RESOLVED', N'Resuelta', N'La causa de la alerta fue resuelta.', 1, 50),
        ('60000000-0000-0000-0000-000000000006', N'CLOSED', N'Cerrada', N'El ciclo de vida de la alerta terminó.', 1, 60)
) AS source (
    workflow_status_id, code, name_es, description_es, is_active, sort_order
)
ON target.workflow_status_id = source.workflow_status_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        workflow_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.workflow_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_closure_type AS target
USING (
    VALUES
        ('70000000-0000-0000-0000-000000000001', N'RESOLVED', N'Resuelta', N'La causa fue atendida y dejó de requerir seguimiento.', 1, 10),
        ('70000000-0000-0000-0000-000000000002', N'FALSE_POSITIVE', N'Falso positivo', N'La evidencia no representaba una condición real.', 1, 20),
        ('70000000-0000-0000-0000-000000000003', N'DUPLICATE', N'Duplicada', N'La alerta corresponde a otra incidencia ya registrada.', 1, 30),
        ('70000000-0000-0000-0000-000000000004', N'CONFIGURATION_CHANGE', N'Cambio de configuración', N'La alerta se cerró como consecuencia de una decisión de configuración.', 1, 40),
        ('70000000-0000-0000-0000-000000000005', N'NO_ACTION_REQUIRED', N'Sin acción requerida', N'La revisión determinó que no era necesaria una acción.', 1, 50),
        ('70000000-0000-0000-0000-000000000006', N'CANCELLED', N'Cancelada', N'El seguimiento fue cancelado de forma explícita.', 1, 60)
) AS source (
    closure_type_id, code, name_es, description_es, is_active, sort_order
)
ON target.closure_type_id = source.closure_type_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        closure_type_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.closure_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_type AS target
USING (
    VALUES
        ('80000000-0000-0000-0000-000000000001', N'EMAIL', N'Correo electrónico', N'Entrega de una notificación por correo electrónico.', 1, 10),
        ('80000000-0000-0000-0000-000000000002', N'TICKET', N'Ticket', N'Gestión de un ticket en un sistema externo.', 1, 20),
        ('80000000-0000-0000-0000-000000000003', N'PUSH_NOTIFICATION', N'Notificación push', N'Entrega de una notificación push.', 1, 30),
        ('80000000-0000-0000-0000-000000000004', N'WHATSAPP', N'WhatsApp', N'Entrega de una notificación mediante WhatsApp.', 1, 40),
        ('80000000-0000-0000-0000-000000000005', N'TEAMS', N'Microsoft Teams', N'Entrega de una notificación mediante Microsoft Teams.', 1, 50)
) AS source (
    action_type_id, code, name_es, description_es, is_active, sort_order
)
ON target.action_type_id = source.action_type_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        action_type_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.action_type_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_operation AS target
USING (
    VALUES
        ('90000000-0000-0000-0000-000000000001', N'SEND', N'Enviar', N'Enviar una notificación mediante el canal seleccionado.', 1, 10),
        ('90000000-0000-0000-0000-000000000002', N'CREATE', N'Crear', N'Crear una entidad en el sistema externo.', 1, 20),
        ('90000000-0000-0000-0000-000000000003', N'UPDATE', N'Actualizar', N'Actualizar una entidad existente en el sistema externo.', 1, 30),
        ('90000000-0000-0000-0000-000000000004', N'CLOSE', N'Cerrar', N'Cerrar una entidad existente en el sistema externo.', 1, 40),
        ('90000000-0000-0000-0000-000000000005', N'REOPEN', N'Reabrir', N'Reabrir una entidad existente en el sistema externo.', 1, 50)
) AS source (
    action_operation_id, code, name_es, description_es, is_active, sort_order
)
ON target.action_operation_id = source.action_operation_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        action_operation_id, code, name_es, description_es, is_active,
        sort_order
    )
    VALUES (
        source.action_operation_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

MERGE alerting.dim_action_status AS target
USING (
    VALUES
        ('a0000000-0000-0000-0000-000000000001', N'PENDING', N'Pendiente', N'La acción está registrada y todavía no inicia.', 1, 10),
        ('a0000000-0000-0000-0000-000000000002', N'PROCESSING', N'En proceso', N'La acción está siendo ejecutada.', 1, 20),
        ('a0000000-0000-0000-0000-000000000003', N'SUCCEEDED', N'Exitosa', N'La operación terminó correctamente.', 1, 30),
        ('a0000000-0000-0000-0000-000000000004', N'DELIVERED', N'Entregada', N'El proveedor confirmó la entrega.', 1, 40),
        ('a0000000-0000-0000-0000-000000000005', N'RETRY_PENDING', N'Reintento pendiente', N'La acción espera un nuevo intento.', 1, 50),
        ('a0000000-0000-0000-0000-000000000006', N'FAILED', N'Fallida', N'La acción agotó o detuvo sus intentos sin éxito.', 1, 60),
        ('a0000000-0000-0000-0000-000000000007', N'CANCELLED', N'Cancelada', N'La acción fue cancelada y no continuará.', 1, 70)
) AS source (
    action_status_id, code, name_es, description_es, is_active, sort_order
)
ON target.action_status_id = source.action_status_id
WHEN MATCHED AND target.code = source.code THEN
    UPDATE SET
        name_es = source.name_es,
        description_es = source.description_es,
        is_active = source.is_active,
        sort_order = source.sort_order
WHEN NOT MATCHED THEN
    INSERT (
        action_status_id, code, name_es, description_es, is_active, sort_order
    )
    VALUES (
        source.action_status_id, source.code, source.name_es,
        source.description_es, source.is_active, source.sort_order
    );
GO

IF OBJECT_ID(N'alerting.alert_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_config_v2 PRIMARY KEY,
            user_code NVARCHAR(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
            version_number INT NOT NULL,
            config_status_id UNIQUEIDENTIFIER NOT NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_v2_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_config_v2_updated DEFAULT (SYSUTCDATETIME()),
            activated_at DATETIME2(3) NULL,
            retired_at DATETIME2(3) NULL,
            version ROWVERSION,
            CONSTRAINT UQ_alert_config_v2_user_version
            UNIQUE (user_code, version_number),
            CONSTRAINT FK_alert_config_v2_status FOREIGN KEY (config_status_id)
            REFERENCES alerting.dim_config_status (config_status_id),
            CONSTRAINT CK_alert_config_v2_version CHECK (version_number > 0)
        );
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        name = N'ux_alert_config_v2_active'
        AND object_id = OBJECT_ID(N'alerting.alert_config')
)
    BEGIN
        CREATE UNIQUE INDEX ux_alert_config_v2_active
            ON alerting.alert_config (user_code)
            WHERE
                config_status_id
                = '50000000-0000-0000-0000-000000000002';
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        name = N'ux_alert_config_v2_single_active'
        AND object_id = OBJECT_ID(N'alerting.alert_config')
)
    BEGIN
        CREATE UNIQUE INDEX ux_alert_config_v2_single_active
            ON alerting.alert_config (config_status_id)
            WHERE
                config_status_id
                = '50000000-0000-0000-0000-000000000002';
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_v2 PRIMARY KEY,
            alert_config_id UNIQUEIDENTIFIER NOT NULL,
            alert_type_id UNIQUEIDENTIFIER NOT NULL,
            entity_key NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NULL,
            is_enabled BIT NOT NULL,
            severity_id UNIQUEIDENTIFIER NOT NULL,
            evaluation_interval_seconds INT NOT NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_v2_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_rule_v2_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT UQ_alert_rule_v2_config
            UNIQUE (alert_rule_id, alert_config_id),
            CONSTRAINT FK_alert_rule_v2_config FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT FK_alert_rule_v2_type FOREIGN KEY (alert_type_id)
            REFERENCES alerting.dim_alert_type (alert_type_id),
            CONSTRAINT FK_alert_rule_v2_severity FOREIGN KEY (severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT CK_alert_rule_v2_interval
            CHECK (evaluation_interval_seconds >= 30),
            CONSTRAINT CK_alert_rule_v2_entity
            CHECK (entity_key IS NULL OR LEN(entity_key) > 0)
        );
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        name = N'ux_alert_rule_v2_default'
        AND object_id = OBJECT_ID(N'alerting.alert_rule')
)
    BEGIN
        CREATE UNIQUE INDEX ux_alert_rule_v2_default
            ON alerting.alert_rule (alert_config_id, alert_type_id)
            WHERE entity_key IS NULL;
    END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        name = N'ux_alert_rule_v2_entity'
        AND object_id = OBJECT_ID(N'alerting.alert_rule')
)
    BEGIN
        CREATE UNIQUE INDEX ux_alert_rule_v2_entity
            ON alerting.alert_rule (
                alert_config_id,
                alert_type_id,
                entity_key
            )
            WHERE entity_key IS NOT NULL;
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_no_readings', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_no_readings (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_no_readings PRIMARY KEY,
            no_readings_minutes INT NOT NULL,
            source_max_age_minutes INT NOT NULL,
            CONSTRAINT FK_alert_rule_no_readings_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT CK_alert_rule_no_readings_value
            CHECK (no_readings_minutes > 0),
            CONSTRAINT CK_alert_rule_no_readings_source_age
            CHECK (source_max_age_minutes > 0)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_pipeline_stale', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_pipeline_stale (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_pipeline_stale PRIMARY KEY,
            expected_interval_minutes INT NOT NULL,
            CONSTRAINT FK_alert_rule_pipeline_stale_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT CK_alert_rule_pipeline_stale_interval
            CHECK (expected_interval_minutes > 0)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_rule_row_count_trend', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_rule_row_count_trend (
            alert_rule_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_rule_row_count_trend PRIMARY KEY,
            warning_drop_pct DECIMAL(6, 2) NOT NULL,
            critical_drop_pct DECIMAL(6, 2) NOT NULL,
            minimum_observed_days_7d TINYINT NOT NULL,
            minimum_observed_days_28d TINYINT NOT NULL,
            critical_severity_id UNIQUEIDENTIFIER NOT NULL,
            CONSTRAINT FK_alert_rule_row_count_trend_rule
            FOREIGN KEY (alert_rule_id)
            REFERENCES alerting.alert_rule (alert_rule_id),
            CONSTRAINT FK_alert_rule_row_count_trend_severity
            FOREIGN KEY (critical_severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT CK_alert_rule_row_count_trend_warning
            CHECK (warning_drop_pct BETWEEN 0 AND 100),
            CONSTRAINT CK_alert_rule_row_count_trend_critical
            CHECK (
                critical_drop_pct BETWEEN 0 AND 100
                AND critical_drop_pct >= warning_drop_pct
            ),
            CONSTRAINT CK_alert_rule_row_count_trend_days_7d
            CHECK (minimum_observed_days_7d BETWEEN 1 AND 7),
            CONSTRAINT CK_alert_rule_row_count_trend_days_28d
            CHECK (minimum_observed_days_28d BETWEEN 1 AND 28)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_detector_operational_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_detector_operational_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_detector_operational_config PRIMARY KEY,
            sensor_lookback_hours INT NOT NULL,
            dagster_failure_lookback_minutes INT NOT NULL,
            dagster_failure_batch_size INT NOT NULL,
            CONSTRAINT FK_alert_detector_operational_config
            FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT CK_alert_detector_operational_lookback
            CHECK (
                sensor_lookback_hours > 0
                AND dagster_failure_lookback_minutes > 0
            ),
            CONSTRAINT CK_alert_detector_operational_batch
            CHECK (dagster_failure_batch_size BETWEEN 1 AND 1000)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_detector_row_count_config', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_detector_row_count_config (
            alert_config_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_detector_row_count_config PRIMARY KEY,
            capture_interval_seconds INT NOT NULL,
            metadata_key NVARCHAR(100) NOT NULL,
            backfill_days INT NOT NULL,
            batch_size INT NOT NULL,
            CONSTRAINT FK_alert_detector_row_count_config
            FOREIGN KEY (alert_config_id)
            REFERENCES alerting.alert_config (alert_config_id),
            CONSTRAINT CK_alert_detector_row_count_interval
            CHECK (capture_interval_seconds >= 30),
            CONSTRAINT CK_alert_detector_row_count_metadata
            CHECK (LEN(metadata_key) > 0),
            CONSTRAINT CK_alert_detector_row_count_backfill
            CHECK (backfill_days > 0),
            CONSTRAINT CK_alert_detector_row_count_batch
            CHECK (batch_size BETWEEN 1 AND 1000)
        );
    END;
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

IF NOT EXISTS (
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

IF NOT EXISTS (
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

IF NOT EXISTS (
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
            event_sequence BIGINT IDENTITY(1, 1) NOT NULL,
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

IF NOT EXISTS (
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
            event_sequence BIGINT IDENTITY(1, 1) NOT NULL,
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

IF NOT EXISTS (
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
            event_sequence BIGINT IDENTITY(1, 1) NOT NULL,
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

IF NOT EXISTS (
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

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        name = N'ix_alert_action_event_action'
        AND object_id = OBJECT_ID(N'alerting.alert_action_event')
)
    BEGIN
        CREATE INDEX ix_alert_action_event_action
            ON alerting.alert_action_event (action_id, event_sequence DESC);
    END;
GO
