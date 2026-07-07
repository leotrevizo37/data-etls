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

## Watermarks

Los pipelines principales conservan el watermark anterior cuando fallan. Si una corrida fallo despues de staging, los recursos intentan limpiar `stg` al finalizar o al reintentar.

## Fallas comunes

| Sintoma | Posible causa | Accion |
|---|---|---|
| No hay filas nuevas | Ventana sin cambios o fuente vacia | Revisar watermark y rango |
| Error JDBC | SQL Server no disponible o timeout | Revisar conectividad y `MSSQL_COMMAND_TIMEOUT` |
| Error Spark capacity | No hay cores/memoria libres | Ver runbook de Spark |
| Merge falla por columna | Contrato desalineado | Revisar DDL, writer Spark y merge SQL |

