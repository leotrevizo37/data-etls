IF
    SCHEMA_ID(N'stg') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s
            ON t.schema_id = s.schema_id
        WHERE s.name = N'stg'
    )
    BEGIN
        DROP SCHEMA stg;
    END;
GO
