# Pipeline `110_smartaudits_categories`

## Proposito

Clasificar resultados SmartAudits revisados por IA y consolidarlos en `dwh.factSmartauditsCategories`.

## Entradas

- MySQL SmartAudits: `plan_results`, `evidence_photos` y tablas de contexto.
- `ctl.SmartauditsAiCommentLookup`.
- `ctl.SmartauditsClassifierModels`.
- Watermark `factSmartauditsCategories`.

## Salidas

- `dwh.factSmartauditsCategories`.
- `ctl.SmartauditsAiCommentReviewQueue`.

## Flujo

1. Selecciona `plan_results` cambiados por `reviewAIDate` o evidencias nuevas.
2. Lee evidencias relacionadas.
3. Carga lookup humano y modelo activo si existe.
4. Construye cola de revision para comentarios no resueltos.
5. Construye fact final con contexto de tarea, ubicacion, usuarios y categoria.
6. Escribe staging y ejecuta `MERGE` por `PlanResultId`.
7. Avanza watermark por `SourceLastChangedAt`.

## Clasificacion

La categoria final se resuelve por prioridad:

1. Cumplimiento cuando IA aprueba.
2. Lookup humano.
3. Reglas deterministas.
4. Modelo ML si supera umbral.
5. `SIN_CLASIFICAR`.

## Jobs relacionados

- [`111_smartaudits_classifier_training`](smartaudits_classifier_training.md)
- [`112_smartaudits_comment_lookup_promotion`](smartaudits_comment_lookup_promotion.md)
