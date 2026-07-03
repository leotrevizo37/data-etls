# SmartAuditsAiCommentReviewQueue

## Proposito

`ctl.SmartauditsAiCommentReviewQueue` concentra comentarios SmartAudits que requieren revision humana porque no fueron resueltos por lookup, reglas o modelo con suficiente confianza.

## Granularidad

Un registro representa la combinacion `(NormalizedCommentHash, AiResult)` pendiente o revisada.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `NormalizedCommentHash` | `CHAR(64)` | Hash del comentario |
| `AiResult` | `TINYINT` | Resultado IA |
| `NormalizedComment` | `NVARCHAR(2000)` | Comentario normalizado |
| `SampleComment` | `NVARCHAR(2000)` | Ejemplo original |
| `CandidateCount` | `INT` | Numero de ocurrencias acumuladas |
| `FirstSeenAt`, `LastSeenAt` | `DATETIME2(0)` | Ventana de observacion |
| `SuggestedCategory` | `NVARCHAR(100)` | Categoria sugerida |
| `SuggestedMethod` | `NVARCHAR(50)` | Metodo sugerido |
| `SuggestedConfidence` | `DECIMAL(9,6)` | Confianza sugerida |
| `ReviewStatus` | `NVARCHAR(20)` | Estado de revision |
| `ReviewedResultCategory` | `NVARCHAR(100)` | Categoria aprobada |

## Reglas de negocio

- Nuevos candidatos entran con `ReviewStatus = 'PENDING'`.
- Al aprobar y promover, pasan a `ctl.SmartauditsAiCommentLookup`.
- La cola acumula conteos y conserva fechas de primera/ultima aparicion.

