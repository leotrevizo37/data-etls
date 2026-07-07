# SmartAuditsAiCommentLookup

## Proposito

`ctl.SmartauditsAiCommentLookup` guarda clasificaciones aprobadas o conocidas por comentario normalizado y resultado IA.

## Granularidad

Un registro representa la combinacion `(NormalizedCommentHash, AiResult)`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `NormalizedCommentHash` | `CHAR(64)` | Hash SHA-256 del comentario normalizado |
| `NormalizedComment` | `NVARCHAR(2000)` | Comentario normalizado |
| `AiResult` | `TINYINT` | Resultado IA asociado |
| `ResultCategory` | `NVARCHAR(100)` | Categoria aprobada |
| `ClassificationMethod` | `NVARCHAR(50)` | Origen de clasificacion |
| `ClassifierModelVersion` | `NVARCHAR(100)` | Version de modelo si aplica |
| `ClassifierConfidence` | `DECIMAL(9,6)` | Confianza registrada |
| `ClassifiedAt`, `ModifiedAt` | `DATETIME2(0)` | Fechas de control |

## Reglas de negocio

- Las promociones desde review queue aprobada escriben con `ClassificationMethod = 'HUMAN'`.
- La llave primaria evita duplicar el mismo comentario normalizado para el mismo resultado IA.
- El lookup se consulta antes de clasificar por ML.

