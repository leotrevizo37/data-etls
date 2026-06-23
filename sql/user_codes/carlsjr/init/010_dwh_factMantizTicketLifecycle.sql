IF OBJECT_ID(N'dwh.factMantizTicketLifecycle', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factMantizTicketLifecycle
        (
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
            IsOpen BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsOpen
            DEFAULT (0),
            IsScheduled BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsScheduled
            DEFAULT (0),
            IsResolved BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsResolved
            DEFAULT (0),
            IsApproved BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsApproved
            DEFAULT (0),
            IsCancelled BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsCancelled
            DEFAULT (0),
            IsTerminal BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_IsTerminal
            DEFAULT (0),
            HasEquipment BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_HasEquipment
            DEFAULT (0),
            HasFailureType BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_HasFailureType
            DEFAULT (0),
            HasPhoto BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_HasPhoto
            DEFAULT (0),
            HasResolutionPhoto BIT NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_HasResolutionPhoto
            DEFAULT (0),
            SourceLastChangedAt DATETIME2(0) NOT NULL,
            LoadedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_LoadedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketLifecycle_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factMantizTicketLifecycle
            PRIMARY KEY CLUSTERED (OrganizationId, TicketId)
            WITH (IGNORE_DUP_KEY = ON)
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
            s.name = N'dwh'
            AND o.name = N'factMantizTicketLifecycle'
            AND i.name = N'ix_factMantizTicketLifecycle_source_changed'
    )
    BEGIN
        CREATE INDEX ix_factMantizTicketLifecycle_source_changed
            ON dwh.factMantizTicketLifecycle (SourceLastChangedAt);
    END;
GO
