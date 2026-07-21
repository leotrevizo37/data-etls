# factMantizTicketTextInsights

## Proposito

`dwh.factMantizTicketTextInsights` convierte textos libres de tickets Mantiz en categorias medibles: falla principal, solucion, refaccion y evidencia textual.

## Fuente de datos

- MySQL Mantiz: tickets, contexto de sucursal/equipo/proveedor, comentarios, refacciones.
- Staging: `stg.factMantizTicketTextInsights`.
- Llaves cambiadas: `stg.MantizTicketTextInsightChangedTicket`.
- Detalle de extraccion: `stg.MantizTicketTextInsightDetail`.
- Cola de revision: `ctl.MantizTextInsightReviewQueue`.

## Granularidad

Un registro representa un ticket dentro de una organizacion: `(OrganizationId, TicketId)`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `OrganizationId`, `TicketId`, `Folio` | `NVARCHAR` | Identificadores |
| `BranchId` | `NVARCHAR(100)` | Sucursal |
| `EquipmentId`, `EquipmentName` | `NVARCHAR` | Equipo asociado |
| `FailureCode`, `FailureSubject`, `FailureProblem` | `NVARCHAR` | Falla principal extraida |
| `SolutionCode`, `SolutionSubject`, `SolutionPartName` | `NVARCHAR` | Solucion y refaccion extraida |
| `SparePartName`, `SparePartUnitPrice`, `SparePartCurrency` | `NVARCHAR` / `DECIMAL` | Refaccion de fuente Mantiz |
| `InsightStatus` | `NVARCHAR(30)` | `CLASSIFIED` o `SIN_CLASIFICAR` |
| `ClassificationMethod` | `NVARCHAR(50)` | `RULE`, `ML` o `SIN_CLASIFICAR` |
| `ClassifierConfidence` | `DECIMAL(9,6)` | Confianza maxima |
| `NeedsReview` | `BIT` | Requiere revision |
| `InsightsJson` | `NVARCHAR(MAX)` | Detalle estructurado de insights |
| `TextEvidenceJson` | `NVARCHAR(MAX)` | Evidencia textual usada |
| `SourceLastChangedAt` | `DATETIME2(0)` | Ultimo cambio fuente |

## Reglas de negocio

- El pipeline reemplaza los tickets cambiados: borra por llaves en staging e inserta la nueva version.
- Se extraen textos desde titulo, descripcion, notas, resolucion, aprobacion, cancelacion y comentarios.
- Los conceptos nuevos o ambiguos se envian a cola de revision.
- `NeedsReview` se activa cuando no hay detalle clasificado o existe candidato nuevo.

## Uso esperado

- Analisis de fallas reales descritas por usuarios.
- Deteccion de soluciones frecuentes.
- Relacion entre fallas, refacciones y costo operativo.

