# Pipeline `111_smartaudits_classifier_training`

## Proposito

Entrenar y registrar un modelo de clasificacion de comentarios SmartAudits para `carlsjr`.

## Entradas

- `ctl.SmartauditsAiCommentLookup` para etiquetas humanas con `ClassificationMethod = 'HUMAN'`.
- MySQL SmartAudits `evidence_photos` para muestras clasificables por reglas.
- `dwh.factSmartauditsCategories` para detectar pendientes `SIN_CLASIFICAR`.
- Variables de umbral de entrenamiento, evaluacion y ruta base del modelo.

## Salidas

- Artefacto de modelo en almacenamiento remoto.
- Registro en `ctl.SmartauditsClassifierModels`.
- Re-clasificacion de filas `SIN_CLASIFICAR` cuando el modelo se promueve.

## Flujo

1. Lee el modelo activo y el conteo de filas `SIN_CLASIFICAR`.
2. Si ya existe modelo activo y el umbral de pendientes no se alcanza, termina sin entrenar.
3. Construye muestras humanas desde el lookup.
4. Construye muestras por reglas desde evidencias SmartAudits.
5. Entrena el clasificador con el numero de filas configurado.
6. Guarda el artefacto del modelo.
7. Registra metricas en `ctl.SmartauditsClassifierModels`.
8. Promueve el modelo si supera `accuracy` y `macro_f1` minimos.
9. Si se promueve, re-clasifica filas pendientes en `dwh.factSmartauditsCategories`.

## Metadatos

El job publica:

| Campo | Descripcion |
|---|---|
| `model_version` | Version generada para el modelo |
| `artifact_path` | Ruta del artefacto |
| `accuracy` | Accuracy de evaluacion |
| `macro_f1` | Macro F1 de evaluacion |
| `human_training_rows` | Muestras humanas usadas |
| `rule_training_rows` | Muestras por reglas usadas |
| `training_rows` | Total de filas de entrenamiento |
| `promoted` | Indica si el modelo quedo activo |
| `reclassified_rows` | Filas pendientes re-clasificadas |

## Alcance

Este job esta implementado en `carlsjr` y usa tags `job_type = 'smartaudits'`, `user_code = 'carlsjr'`.

