IF OBJECT_ID(N'dwh.usp_FactLecturas_DeleteRange', N'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_FactLecturas_DeleteRange;
GO

CREATE PROCEDURE dwh.usp_FactLecturas_DeleteRange
    (
        @FromUtc DATETIME2(0),
        @ToUtc DATETIME2(0)
    )
AS
BEGIN
    SET NOCOUNT ON;

    IF @FromUtc IS NULL OR @ToUtc IS NULL
        BEGIN
            RAISERROR (
                'usp_FactLecturas_DeleteRange: @FromUtc and @ToUtc must not be NULL.',
                16,
                1
            );
            RETURN;
        END;

    IF @FromUtc > @ToUtc
        BEGIN
            RAISERROR (
                'usp_FactLecturas_DeleteRange: @FromUtc and @ToUtc must not be NULL.',
                16,
                1
            );
            RETURN;
        END;

    DELETE FROM dwh.FactLecturas WITH (ROWLOCK)
    WHERE
        TimeSpan >= @FromUtc
        AND TimeSpan <= @ToUtc;

    DECLARE @DeletedRows INT = @@ROWCOUNT;

    SELECT @DeletedRows AS DeletedRowCount;
END;
GO
