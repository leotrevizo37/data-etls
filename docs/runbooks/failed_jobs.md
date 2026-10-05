# Runbook: failed_jobs

## Objetivo

Diagnosticar una corrida fallida sin modificar codigo ni contratos.

## Checklist

1. Identificar `user_code`, job y run id.
2. Revisar el error en Dagster.
3. Consultar `ctl.IngestionControl` por `IngestionName`.
4. Revisar si hubo staging pendiente por `RunId`.
5. Verificar disponibilidad de Spark y SQL Server.
6. Reintentar desde Dagster si el error fue transitorio.

Para `sidon_configuration_job`, utilizar el [runbook SIDON/MQTT](sidon_mqtt_recovery.md): el cursor y los reintentos pertenecen al sensor, no a `ctl.IngestionControl`. Una evaluación inconclusa puede tener run exitoso sin señal. Para URLs, un fallo posterior a un upsert puede dejar filas confirmadas; revisar el rango antes de interpretar el run como ausencia de datos escritos.

## Watermarks

Los pipelines principales conservan el watermark anterior cuando fallan. Si una corrida fallo despues de staging, los recursos intentan limpiar `stg` al finalizar o al reintentar.

## Fallas comunes

| Sintoma | Posible causa | Accion |
|---|---|---|
| No hay filas nuevas | Ventana sin cambios o fuente vacia | Revisar watermark y rango |
| Error JDBC | SQL Server no disponible o timeout | Revisar conectividad y `MSSQL_COMMAND_TIMEOUT` |
| Error Spark capacity | No hay cores/memoria libres | Ver runbook de Spark |
| Merge falla por columna | Contrato desalineado | Revisar DDL, writer Spark y merge SQL |

