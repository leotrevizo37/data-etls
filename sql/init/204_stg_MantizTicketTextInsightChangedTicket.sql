IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'stg'
    )
    BEGIN
        EXEC ('CREATE SCHEMA stg');
    END;
GO

IF OBJECT_ID(N'stg.MantizTicketTextInsightChangedTicket', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.MantizTicketTextInsightChangedTicket
        (
            RunId NVARCHAR(100) NOT NULL,
            OrganizationId NVARCHAR(100) NOT NULL,
            TicketId NVARCHAR(100) NOT NULL
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'stg'
            AND o.name = N'MantizTicketTextInsightChangedTicket'
            AND i.name = N'ix_stg_MantizTicketTextInsightChangedTicket_run'
    )
    BEGIN
        CREATE INDEX ix_stg_MantizTicketTextInsightChangedTicket_run
            ON stg.MantizTicketTextInsightChangedTicket (RunId, OrganizationId, TicketId);
    END;
GO
