IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'dwh'
    )
    BEGIN
        EXEC ('CREATE SCHEMA dwh');
    END;
GO

IF OBJECT_ID(N'dwh.dimOutOfHoursTypes', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimOutOfHoursTypes
        (
            OutOfHoursType TINYINT NOT NULL,
            OutOfHoursTypeName NVARCHAR(100) NOT NULL,
            OutOfHoursTypeDescription NVARCHAR(500) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimOutOfHoursTypes_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimOutOfHoursTypes
            PRIMARY KEY CLUSTERED (OutOfHoursType)
        );
    END;
GO

MERGE dwh.dimOutOfHoursTypes AS target
USING (
    VALUES
    (1, N'Antes de apertura', N'Venta realizada antes de la hora de apertura.'),
    (2, N'Horario de comida', N'Venta realizada durante el intervalo de comida; solo aplica cuando ambas horas de comida existen.'),
    (3, N'Después del cierre', N'Venta realizada después de la hora de cierre.'),
    (4, N'Sin horario aplicable', N'Venta cuya tienda, fecha o día de la semana no tiene un horario aplicable.')
)
    AS source (
        OutOfHoursType,
        OutOfHoursTypeName,
        OutOfHoursTypeDescription
    )
    ON target.OutOfHoursType = source.OutOfHoursType
WHEN MATCHED AND (
    target.OutOfHoursTypeName <> source.OutOfHoursTypeName
    OR target.OutOfHoursTypeDescription <> source.OutOfHoursTypeDescription
)
    THEN
    UPDATE
        SET
            OutOfHoursTypeName = source.OutOfHoursTypeName,
            OutOfHoursTypeDescription = source.OutOfHoursTypeDescription,
            ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED
    THEN
    INSERT (
        OutOfHoursType,
        OutOfHoursTypeName,
        OutOfHoursTypeDescription
    )
    VALUES (
        source.OutOfHoursType,
        source.OutOfHoursTypeName,
        source.OutOfHoursTypeDescription
    );
GO
