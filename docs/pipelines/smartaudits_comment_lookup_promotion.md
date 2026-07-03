# Pipeline `112_smartaudits_comment_lookup_promotion`

## Proposito

Promover comentarios SmartAudits revisados y aprobados desde la cola manual hacia el lookup de clasificacion.

## Entradas

- `ctl.SmartauditsAiCommentReviewQueue`.

## Salida

- `ctl.SmartauditsAiCommentLookup`.

## Flujo

1. Busca filas aprobadas en `ctl.SmartauditsAiCommentReviewQueue`.
2. Inserta o actualiza `ctl.SmartauditsAiCommentLookup` por `(NormalizedCommentHash, AiResult)`.
3. Registra la categoria aprobada como `ClassificationMethod = 'HUMAN'`.
4. Marca las filas promovidas de la cola como `PROMOTED`.
5. Publica `promoted_rows` como metadata del job.

## Reglas

- Solo se promueven filas con `ReviewStatus = 'APPROVED'`.
- `ReviewedResultCategory` debe tener valor.
- La confianza registrada para la clasificacion humana es `1.0`.

## Alcance

Este job esta implementado en `carlsjr` y usa tags `job_type = 'smartaudits'`, `user_code = 'carlsjr'`.

