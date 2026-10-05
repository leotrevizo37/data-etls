# factMantizTicketLifecycle

Estado: Mantiz desactivado. El contrato existente se conserva como referencia.

## Proposito

`dwh.factMantizTicketLifecycle` modela el ciclo de vida de tickets Mantiz para medir tiempos, estado operativo y calidad de captura.

## Fuente de datos

- MySQL Mantiz: `Ticket`, `Branch`, `Client`, `EquipmentInstance`, `EquipmentCatalog`, `FailureType`, `Supplier`.
- Staging: `stg.factMantizTicketLifecycle`.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'factMantizTicketLifecycle'`.

## Granularidad

Un registro representa un ticket dentro de una organizacion: `(OrganizationId, TicketId)`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `OrganizationId` | `NVARCHAR(100)` | Organizacion Mantiz |
| `ClientId`, `BranchId`, `BranchName` | `NVARCHAR` | Contexto de cliente/sucursal |
| `TicketId`, `Folio` | `NVARCHAR` | Identificadores del ticket |
| `EquipmentId`, `EquipmentName`, `EquipmentCatalogName` | `NVARCHAR` | Contexto del equipo |
| `FailureTypeId`, `FailureTypeName` | `NVARCHAR` | Tipo de falla fuente |
| `SupplierId`, `SupplierName` | `NVARCHAR` | Proveedor asignado |
| `TicketStatus`, `TicketPriority`, `TicketType`, `TicketSource` | `NVARCHAR` | Estado y clasificacion fuente |
| `CreatedAt`, `ScheduledAt`, `ResolvedAt`, `ApprovedAt`, `CancelledAt`, `UpdatedAt` | `DATETIME2(0)` | Fechas del ciclo |
| `OpenToScheduledHours`, `ScheduledToResolvedHours`, `ResolvedToApprovedHours` | `DECIMAL(18,4)` | Duraciones parciales |
| `OpenToResolvedHours`, `OpenToApprovedHours`, `OpenToCancelledHours`, `CurrentAgeHours` | `DECIMAL(18,4)` | Duraciones acumuladas |
| `IsOpen`, `IsScheduled`, `IsResolved`, `IsApproved`, `IsCancelled`, `IsTerminal` | `BIT` | Banderas de estado |
| `HasEquipment`, `HasFailureType`, `HasPhoto`, `HasResolutionPhoto` | `BIT` | Banderas de calidad de captura |
| `SourceLastChangedAt` | `DATETIME2(0)` | Ultimo cambio fuente |

## Reglas de negocio

- La llave primaria es `(OrganizationId, TicketId)`.
- La carga incremental filtra por `COALESCE(updatedAt, createdAt)`.
- El pipeline escribe a `stg` y consolida con `MERGE`.
- `IsTerminal` se deriva de cancelacion o aprobacion.

## Uso esperado

- Aging de tickets abiertos.
- SLA operativo por sucursal/proveedor/equipo.
- Calidad de captura de tickets.

