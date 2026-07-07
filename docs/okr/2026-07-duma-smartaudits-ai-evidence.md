# SmartAudits AI - Release productivo DUMA - 2026-07-03

Estado: PRODUCCION

## Objetivo cubierto

Implementar los insights de SmartAudits AI para DUMA.

## Entregable

Flujo productivo de SmartAudits AI con tabla de insights y clasificacion de comentarios disponible para consumo.

## Valor

Convierte comentarios de revisiones operativas en datos estructurados para analisis, reportes y automatizaciones. El flujo deja trazabilidad entre resultado operativo, evidencia, categoria, metodo de clasificacion, lookup humano, cola de revision y modelo versionado.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Objetivo cubierto | Implementar los insights de SmartAudits AI para DUMA |
| Entregable | Flujo productivo de SmartAudits AI con tabla de insights y clasificacion de comentarios |
| Tabla principal | `dwh.factSmartauditsCategories` |
| Staging | `stg.factSmartauditsCategories`, `stg.SmartauditsAiCommentReviewQueue` |
| Lookup | `ctl.SmartauditsAiCommentLookup` |
| Cola de revision | `ctl.SmartauditsAiCommentReviewQueue` |
| Modelos | `ctl.SmartauditsClassifierModels` |
| Asset | `carlsjr/dwh_factsmartauditscategories` |
| Jobs | `110_smartaudits_categories`, `111_smartaudits_classifier_training`, `112_smartaudits_comment_lookup_promotion` |
| Fuente transaccional | MySQL SmartAudits: `plan_results`, `evidence_photos` y tablas de contexto |
| Validaciones | Clasificacion de comentarios, lookup normalizado, cola de revision IA, modelo versionado y merge SQL |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | `dwh.factSmartauditsCategories`, `stg.factSmartauditsCategories` | `sql/init/101_dwh_stg_factSmartauditsCategories.sql` |
| DDL | `ctl.SmartauditsAiCommentLookup` | `sql/init/100_ctl_SmartauditsAiCommentLookup.sql` |
| DDL | `ctl.SmartauditsAiCommentReviewQueue`, `stg.SmartauditsAiCommentReviewQueue` | `sql/init/102_ctl_stg_SmartauditsAiCommentReviewQueue.sql` |
| DDL | `ctl.SmartauditsClassifierModels` | `sql/init/103_ctl_SmartauditsClassifierModels.sql` |
| Asset | `dwh_factsmartauditscategories` | `dagster/carlsjr/src/carlsjr/defs/assets/dwh_factSmartauditsCategories.py` |
| Job | `110_smartaudits_categories` | `dagster/carlsjr/src/carlsjr/defs/jobs/factsmartauditscategories_job.py` |
| Job | `111_smartaudits_classifier_training` | `dagster/carlsjr/src/carlsjr/defs/jobs/smartaudits_classifier_training_job.py` |
| Job | `112_smartaudits_comment_lookup_promotion` | `dagster/carlsjr/src/carlsjr/defs/jobs/smartaudits_comment_lookup_promotion_job.py` |
| Transformacion | Categoria, normalizacion, review queue | `dagster/carlsjr/src/carlsjr/defs/transformations/smartaudits_categories.py` |
| Transformacion | Entrenamiento y prediccion | `dagster/carlsjr/src/carlsjr/defs/transformations/smartaudits_classifier.py` |
| Tests | Contrato y comportamiento SmartAudits | `dagster/carlsjr/tests/test_smartaudits_categories.py`, `dagster/carlsjr/tests/test_smartaudits_classifier.py`, `dagster/carlsjr/tests/test_smartaudits_merge_sql.py`, `dagster/carlsjr/tests/test_smartaudits_mysql_resource.py` |

## Flujo productivo

1. `110_smartaudits_categories` lee cambios SmartAudits desde MySQL.
2. El asset cruza resultados, evidencias, tareas, ubicaciones, usuarios, prioridades y categorias.
3. Los comentarios se normalizan y se clasifican por lookup humano, reglas o modelo.
4. Las filas sin clasificacion confiable se envian a `ctl.SmartauditsAiCommentReviewQueue`.
5. El resultado final se escribe en `stg.factSmartauditsCategories`.
6. SQL Server consolida el resultado en `dwh.factSmartauditsCategories` mediante `MERGE`.
7. `112_smartaudits_comment_lookup_promotion` promueve comentarios aprobados al lookup.
8. `111_smartaudits_classifier_training` entrena, versiona y registra modelos cuando hay suficientes datos.

## Validaciones funcionales

| Validacion | Evidencia esperada |
|---|---|
| Clasificacion de comentarios disponible | `ResultCategory`, `ClassificationMethod`, `ClassifierModelVersion`, `ClassifierConfidence` en `dwh.factSmartauditsCategories` |
| Lookup de comentarios normalizados | Llave `(NormalizedCommentHash, AiResult)` en `ctl.SmartauditsAiCommentLookup` |
| Cola de revision IA | Comentarios no resueltos en `ctl.SmartauditsAiCommentReviewQueue` con `ReviewStatus` |
| Modelo clasificador versionado | Registros en `ctl.SmartauditsClassifierModels` con `ModelVersion`, `ArtifactPath`, metricas e `IsActive` |
| Merge idempotente | `MERGE` hacia `dwh.factSmartauditsCategories` por `PlanResultId` |

## Relacion con OKRs de julio

| Fecha | Objetivo | Relacion con este release                          |
|---|---|----------------------------------------------------|
| 2026-07-03 | Implementar insights de SmartAudits AI para DUMA | Objetivo principal cubierto, feature en producción |
