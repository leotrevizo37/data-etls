# factSmartauditsCategories

## Proposito

`dwh.factSmartauditsCategories` resume tareas SmartAudits revisadas automaticamente por IA y clasifica el motivo del resultado para analitica operativa.

## Fuente de datos

- MySQL SmartAudits: `plan_results`, `evidence_photos`, `work_plan`, `tasks`, `checkpoints`, `sublocations`, `locations`, `asp_net_users`, `priority_types`, `task_categories`, `categories`.
- Lookup humano/modelo: `ctl.SmartauditsAiCommentLookup`.
- Staging: `stg.factSmartauditsCategories`.

## Granularidad

Un registro representa un `PlanResultId`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `PlanResultId` | `BIGINT` | Llave del resultado de plan |
| `WorkPlanId` | `BIGINT` | Plan de trabajo |
| `EvidencePhotoId` | `BIGINT` | Evidencia representativa de falla |
| `LocationId`, `SublocationId` | `NVARCHAR(100)` | Contexto operativo |
| `Checkpoint`, `Task` | `NVARCHAR(100)` | Identificadores de checkpoint y tarea |
| `LocationName`, `SublocationName`, `CheckpointName`, `TaskName` | `NVARCHAR(500)` | Contexto legible |
| `ExecutorName`, `AuditorName` | `NVARCHAR(500)` | Usuarios normalizados |
| `TaskCategory`, `Priority` | `NVARCHAR(255)` | Clasificacion operativa de la tarea |
| `ResultCategory` | `NVARCHAR(100)` | Categoria final |
| `AiResult` | `TINYINT` | Resultado IA normalizado |
| `ClassificationMethod` | `NVARCHAR(50)` | `RULE`, `ML`, `HUMAN` o `SIN_CLASIFICAR` |
| `ClassifierModelVersion` | `NVARCHAR(100)` | Version del modelo si aplica |
| `ClassifierConfidence` | `DECIMAL(9,6)` | Confianza de clasificacion |
| `FailedEvidenceCount`, `EvidenceCount` | `INT` | Conteos de evidencias |
| `SourceLastChangedAt` | `DATETIME2(0)` | Ultimo cambio fuente considerado |

## Reglas de negocio

- Solo considera tareas con revision IA automatica.
- La categoria final prioriza cumplimiento, lookup humano, reglas deterministas, prediccion ML y `SIN_CLASIFICAR`.
- El pipeline escribe a `stg` y consolida con `MERGE` por `PlanResultId`.
- El watermark avanza con `SourceLastChangedAt` cuando la corrida es exitosa.

## Uso esperado

- Fallas recurrentes por sucursal, sububicación, checkpoint o tarea.
- Calidad de evidencia y errores de captura.
- Entrenamiento y mejora del clasificador de comentarios.

