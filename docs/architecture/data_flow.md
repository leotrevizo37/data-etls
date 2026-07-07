# Flujo de datos

## Vista general

```mermaid
flowchart LR
  adls["ADLS / parquets de lecturas"] --> factLecturas["dwh.factLecturas"]
  adls --> readingsExtended["lecturas extendidas + dimensiones"]
  sqlDims["SQL Server origen de dimensiones"] --> dimSidon["dwh.dimSidonProdDimensions"]
  sqlSchedule["Programacion y valores esperados"] --> expected["dwh.dimSensorExpectedVal"]

  dimSidon --> readingsExtended
  readingsExtended --> measurements["dwh.factReadingsMeasurement"]
  measurements --> deviceView["dwh.vwFactReadingsMeasurementDevice"]

  readingsExtended --> sensorEvents["dwh.factSensorEvents"]
  expected --> sensorEvents

  smartauditsMySql["MySQL SmartAudits"] --> smartauditsFact["dwh.factSmartauditsCategories"]
  smartauditsFact --> smartauditsReview["ctl.SmartauditsAiCommentReviewQueue"]
  smartauditsReview --> smartauditsLookup["ctl.SmartauditsAiCommentLookup"]

  mantizMySql["MySQL Mantiz"] --> mantizLifecycle["dwh.factMantizTicketLifecycle"]
  mantizMySql --> mantizInsights["dwh.factMantizTicketTextInsights"]
  mantizInsights --> mantizReview["ctl.MantizTextInsightReviewQueue"]

  control["ctl.IngestionControl"] --> factLecturas
  control --> measurements
  control --> sensorEvents
  control --> smartauditsFact
  control --> mantizLifecycle
  control --> mantizInsights
```

## Sense

El flujo Sense parte de lecturas crudas en parquet. `dwh.factLecturas` conserva una copia para corroboraciones especificas y consultas ad hoc; ningun asset, job o materializacion depende de esa tabla como entrada. Las mediciones operativas se construyen desde lecturas del data lake enriquecidas con `dwh.dimSidonProdDimensions`.

## Eventos de sensores

`dwh.factSensorEvents` se alimenta desde lecturas extendidas. Los eventos de presencia detectan cambios de estado por sensor. Los eventos de temperatura comparan lecturas contra `dwh.dimSensorExpectedVal` y solo conservan periodos fuera de rango con duracion suficiente.

## SmartAudits

SmartAudits lee cambios en `plan_results` y `evidence_photos`, cruza contexto de tareas, checkpoints, ubicaciones, usuarios, prioridades y categorias, y consolida el resultado en `dwh.factSmartauditsCategories`. Los comentarios no resueltos o de baja confianza se envian a `ctl.SmartauditsAiCommentReviewQueue`.

## Mantiz

Mantiz tiene dos salidas principales. `dwh.factMantizTicketLifecycle` modela el estado y tiempos del ciclo de vida del ticket. `dwh.factMantizTicketTextInsights` extrae fallas, soluciones y refacciones desde textos de tickets y comentarios; los conceptos nuevos o ambiguos se envian a `ctl.MantizTextInsightReviewQueue`.
