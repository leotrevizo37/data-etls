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

IF OBJECT_ID(N'stg.factMantizTicketLifecycle', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factMantizTicketLifecycle
        (
            RunId NVARCHAR(100) NOT NULL,
            OrganizationId NVARCHAR(100) NOT NULL,
            ClientId NVARCHAR(100) NULL,
            BranchId NVARCHAR(100) NULL,
            BranchName NVARCHAR(255) NULL,
            TicketId NVARCHAR(100) NOT NULL,
            Folio NVARCHAR(191) NULL,
            EquipmentId NVARCHAR(100) NULL,
            EquipmentName NVARCHAR(255) NULL,
            EquipmentCatalogName NVARCHAR(255) NULL,
            FailureTypeId NVARCHAR(100) NULL,
            FailureTypeName NVARCHAR(255) NULL,
            SupplierId NVARCHAR(100) NULL,
            SupplierName NVARCHAR(255) NULL,
            TicketStatus NVARCHAR(191) NULL,
            TicketPriority NVARCHAR(191) NULL,
            TicketType NVARCHAR(191) NULL,
            TicketSource NVARCHAR(191) NULL,
            CreatedAt DATETIME2(0) NULL,
            ScheduledAt DATETIME2(0) NULL,
            ResolvedAt DATETIME2(0) NULL,
            ApprovedAt DATETIME2(0) NULL,
            CancelledAt DATETIME2(0) NULL,
            UpdatedAt DATETIME2(0) NULL,
            OpenToScheduledHours DECIMAL(18, 4) NULL,
            ScheduledToResolvedHours DECIMAL(18, 4) NULL,
            ResolvedToApprovedHours DECIMAL(18, 4) NULL,
            OpenToResolvedHours DECIMAL(18, 4) NULL,
            OpenToApprovedHours DECIMAL(18, 4) NULL,
            OpenToCancelledHours DECIMAL(18, 4) NULL,
            CurrentAgeHours DECIMAL(18, 4) NULL,
            IsOpen BIT NOT NULL,
            IsScheduled BIT NOT NULL,
            IsResolved BIT NOT NULL,
            IsApproved BIT NOT NULL,
            IsCancelled BIT NOT NULL,
            IsTerminal BIT NOT NULL,
            HasEquipment BIT NOT NULL,
            HasFailureType BIT NOT NULL,
            HasPhoto BIT NOT NULL,
            HasResolutionPhoto BIT NOT NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL
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
            AND o.name = N'factMantizTicketLifecycle'
            AND i.name = N'ix_stg_factMantizTicketLifecycle_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factMantizTicketLifecycle_run
            ON stg.factMantizTicketLifecycle (RunId, OrganizationId, TicketId);
    END;
GO
