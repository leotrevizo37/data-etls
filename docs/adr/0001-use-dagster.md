# ADR 0001: Usar Dagster para orquestacion

## Estado

Aceptado.

## Contexto

El proyecto requiere ejecutar pipelines incrementales, backfills, schedules, assets y jobs de mantenimiento por `user_code`.

## Decision

Usar Dagster como orquestador principal con seis code locations gRPC, una por `user_code`.

## Consecuencias

- Cada cliente puede tener assets y schedules propios sin mezclar runtime.
- Cada cliente puede tener dependencias o drivers unicos sin mezclar runtime ni datos.
- Los jobs pueden etiquetar `job_type` para seleccionar perfiles Spark.
- Los assets publican metadatos utiles para validacion.
- Los cambios compartidos deben revisarse en cada implementacion que tenga el mismo flujo.

