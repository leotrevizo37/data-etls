# ADR 0003: Separar `dwh`, `stg` y `ctl`

## Estado

Aceptado.

## Contexto

Los pipelines necesitan contratos estables de consumo, cargas idempotentes y control de watermarks/modelos/revision.

## Decision

Separar las responsabilidades de warehouse, staging y control:

| Esquema | Responsabilidad |
|---|---|
| `dwh` | Tablas y vistas de consumo |
| `stg` | Tablas temporales por corrida |
| `ctl` | Control de ingestion, colas, modelos y logs |

## Consecuencias

- Los consumidores consultan los contratos documentados en `dwh`, `serving` y `observability`; no consumen staging como salida final.
- Los pipelines escriben staging por `RunId` cuando necesitan `MERGE` o reemplazo controlado.
- Los estados operativos y de revision se mantienen fuera de las tablas finales.
- Los cambios de contrato deben cuidar DDL, staging y merge de forma sincronizada.

## Evolución del contrato

La implementación actual incorpora `raw` para páginas de API y rechazos, `observability` para hechos operativos, `serving` para vistas de consumo y `alerting` para configuración e incidencias. Estas capas complementan la separación original. Las responsabilidades de alertamiento se precisan en [ADR 0007](0007-tenant-alert-detection.md); las políticas de evidencia MQTT, en [ADR 0008](0008-chirpstack-partial-mqtt-evidence.md).

