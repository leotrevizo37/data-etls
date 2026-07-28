# MantizTextInsightReviewQueue

## Proposito

`ctl.MantizTextInsightReviewQueue` concentra conceptos extraidos de textos Mantiz que requieren revision o aprobacion.

## Granularidad

Un registro representa un concepto por tipo de insight: `(InsightType, ConceptHash)`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `InsightType` | `NVARCHAR(30)` | Tipo de concepto |
| `ConceptHash` | `CHAR(64)` | Hash del concepto |
| `CanonicalCode`, `CanonicalName` | `NVARCHAR` | Canonico sugerido |
| `RepresentativeText`, `RepresentativeRawText` | `NVARCHAR` | Texto representativo |
| `ConceptStatus` | `NVARCHAR(20)` | Estado del concepto |
| `CandidateCount`, `TicketCount` | `INT` | Frecuencia acumulada |
| `FirstSeenAt`, `LastSeenAt` | `DATETIME2(0)` | Ventana de aparicion |
| `LastTicketId` | `NVARCHAR(100)` | Ultimo ticket observado |
| `SuggestedByMethod` | `NVARCHAR(50)` | Metodo que sugirio el concepto |
| `ReviewStatus` | `NVARCHAR(20)` | Estado de revision |
| `ReviewedCanonicalCode`, `ReviewedCanonicalName` | `NVARCHAR` | Canonico aprobado |

## Reglas de negocio

- Nuevos conceptos entran como candidatos pendientes.
- La promocion puede activar canonicos revisados.
- El entrenamiento de modelos Mantiz usa conceptos aprobados, promovidos o activos.

