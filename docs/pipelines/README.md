# Pipelines

Los pipelines se documentan por salida funcional, no por implementacion duplicada. Cuando un flujo existe en varios `user_codes`, el contrato debe conservarse compatible entre ellos.

| Pipeline | Job principal | Salida |
|---|---|---|
| Lecturas | `010_sense_lecturas_ingestion` | `dwh.factLecturas` |
| Cleanup lecturas | `011_sense_lecturas_cleanup` | Retencion de `dwh.factLecturas` |
| Backfill lecturas | `012_sense_lecturas_backfill_daily`, `013_sense_lecturas_backfill_sensor` | Rango historico de `dwh.factLecturas` |
| Audiencia | `014_sense_audiencia_ingestion` | `dwh.factAudiencia` |
| Backfill audiencia | `015_sense_audiencia_backfill_daily` | Rango diario de `dwh.factAudiencia` |
| Operaciones Sense | `020_sense_operations` | `dwh.factReadingsMeasurement`, `ctl.IngestionControl` |
| Eventos de sensores | `021_sense_sensor_events` | `dwh.factSensorEvents` |
| Mantiz tickets | `100_mantiz_tickets` | `dwh.factMantizTicketLifecycle`, `dwh.factMantizTicketTextInsights` |
| SmartAudits categorias | `110_smartaudits_categories` | `dwh.factSmartauditsCategories` |
| SmartAudits training | `111_smartaudits_classifier_training` | `ctl.SmartauditsClassifierModels`, artefacto de modelo |
| SmartAudits lookup promotion | `112_smartaudits_comment_lookup_promotion` | `ctl.SmartauditsAiCommentLookup` |
| Ventas fuera de horario Bafar | `020_bafar_out_of_hours_sales` | `dwh.factOutOfHoursSales` |
| Backfill ventas fuera de horario Bafar | `022_bafar_out_of_hours_sales_backfill_daily` | Rango diario de `dwh.factOutOfHoursSales` |
| Acceso diario de gerentes Bafar | `024_bafar_manager_access_daily` | `dwh.factManagerAccessDaily` |
| Backfill acceso de gerentes Bafar | `025_bafar_manager_access_backfill_daily` | Snapshots diarias de `dwh.factManagerAccessDaily` |
| Mantenimiento | `080_maintenance_cleanup`, `081_maintenance_index`, `089_maintenance_spark_healthcheck` | Salud operativa |
