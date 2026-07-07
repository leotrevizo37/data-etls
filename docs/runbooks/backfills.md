# Runbook: backfills

## Objetivo

Reprocesar rangos historicos de `dwh.factLecturas` sin cambiar manualmente los contratos DWH.

## Jobs disponibles

| Job | Uso |
|---|---|
| `012_sense_lecturas_backfill_daily` | Backfill por dias |
| `013_sense_lecturas_backfill_sensor` | Backfill acotado por sensor |

## Pasos

1. Identificar `user_code` y rango requerido.
2. Validar que Spark tenga capacidad para perfil `backfill`.
3. Ejecutar el job desde Dagster en la location correspondiente.
4. Revisar metadatos de filas escritas y errores.
5. Validar el resultado con consultas ad hoc sobre `dwh.factLecturas`.

## Precauciones

- No editar archivos `.env` para correr un backfill.
- No cambiar watermarks manualmente salvo que sea parte de una recuperacion controlada.
- Evitar rangos grandes si el cluster Spark no tiene memoria suficiente.
