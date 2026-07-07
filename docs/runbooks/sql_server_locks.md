# Runbook: SQL Server locks

## Objetivo

Responder a bloqueos o lentitud en SQL Server durante cargas o mantenimiento.

## Contexto

Los pipelines usan tres patrones:

| Patron | Tablas |
|---|---|
| Borrado por ventana e insercion | `dwh.factLecturas`, `dwh.factSensorEvents` |
| Staging + `MERGE` | `dwh.factReadingsMeasurement`, `dwh.factSmartauditsCategories`, `dwh.factMantizTicketLifecycle` |
| Reemplazo por llaves cambiadas | `dwh.factMantizTicketTextInsights` |

## Diagnostico

1. Identificar tabla bloqueada.
2. Revisar si corre un job de mantenimiento de indices.
3. Revisar si hay staging grande para el mismo `RunId`.
4. Revisar si el job esta escribiendo en chunks JDBC.
5. Pausar schedules concurrentes si compiten por la misma tabla.

## Acciones seguras

- Esperar si el bloqueo corresponde a un `MERGE` activo y avanza.
- Reintentar desde Dagster si la corrida fallo por timeout.
- Ejecutar mantenimiento de indices fuera de horas de carga pesada.

## Acciones que requieren cuidado

- Modificar watermarks.
- Borrar staging manualmente.
- Cancelar sesiones SQL Server activas.

