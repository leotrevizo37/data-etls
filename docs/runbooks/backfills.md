# Runbook: backfills

## Objetivo

Reprocesar rangos historicos de `dwh.factLecturas` sin cambiar manualmente los contratos DWH.

## Jobs disponibles

| Job | Uso |
|---|---|
| `012_sense_lecturas_backfill_daily` | Backfill por dias |
| `013_sense_lecturas_backfill_sensor` | Backfill acotado por sensor |

## Otros contratos de backfill

| Job | Alcance | Ventana y control |
|---|---|---|
| `015_sense_audiencia_backfill_daily` | Carlsjr | Día UTC semiabierto; borra y recarga; sin filtro por sensor ni cambio del watermark incremental |
| `022_bafar_out_of_hours_sales_backfill_daily` | Bafar | Día local desde `2026-05-01`; reutiliza staging y merge; no modifica watermark |
| `023_bafar_store_sales_daily_backfill_daily` | Bafar | Día local desde `2026-05-01`; agrega ventas y reutiliza staging y merge; no modifica watermark |
| `025_bafar_manager_access_backfill_daily` | Bafar | Fecha local desde `2026-01-01`; solicita snapshot API con corte 23 e historial 180; no modifica watermark |

Para estos jobs, identificar tenant y partición, revisar la configuración generada y capacidad disponible, ejecutar únicamente el rango autorizado y comprobar run, metadata y salida de la misma ventana. Bafar utiliza `America/Ciudad_Juarez`. El estado histórico de gerentes depende de la respuesta real de la API; su upsert no elimina filas ausentes en una recarga.

Los flujos de ventas limpian staging por `RunId`, incluso ante fallos, y permiten hasta tres reintentos con demora de 60 segundos. El detalle contractual está en [audiencia](../pipelines/fact_audiencia.md), [ventas fuera de horario](../pipelines/fact_out_of_hours_sales.md), [ventas diarias](../pipelines/fact_store_sales_daily.md) y [gerentes](../pipelines/fact_manager_access_daily.md). ChirpStack no dispone de un job de backfill; su lookback MQTT se describe en [recuperación MQTT](sidon_mqtt_recovery.md).

## Pasos para lecturas

1. Identificar `user_code` y rango requerido.
2. Validar que Spark tenga capacidad para perfil `backfill`.
3. Ejecutar el job desde Dagster en la location correspondiente.
4. Revisar metadatos de filas escritas y errores.
5. Validar el resultado con consultas ad hoc sobre `dwh.factLecturas`.

## Precauciones

- No editar archivos `.env` para correr un backfill.
- No cambiar watermarks manualmente salvo que sea parte de una recuperacion controlada.
- Evitar rangos grandes si el cluster Spark no tiene memoria suficiente.
