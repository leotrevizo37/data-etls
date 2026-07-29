IF OBJECT_ID(N'observability.dimAspNetUsers', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.dimAspNetUsers
        (
            Id UNIQUEIDENTIFIER NOT NULL,
            UserName NVARCHAR(256) NULL,
            Name NVARCHAR(50) NULL,
            LastName NVARCHAR(50) NULL,
            Position NVARCHAR(100) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimAspNetUsers_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimAspNetUsers_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimAspNetUsers
            PRIMARY KEY CLUSTERED (Id)
        );
    END;
GO
