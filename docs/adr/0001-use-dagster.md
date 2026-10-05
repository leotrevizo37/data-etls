# ADR 0001: Usar Dagster para orquestacion

## Estado

Aceptado.

## Contexto

El proyecto requiere ejecutar pipelines incrementales, backfills, schedules, assets y jobs de mantenimiento por `user_code`.

## Decision

Usar Dagster como orquestador principal con code locations independientes por `user_code`. Tras la retirada descrita en [ADR 0005](0005-remove-code-locations.md), `workspace.yaml` registra cinco locations: `bafar_poc_gabinete`, `valledelencino`, `carlsjr`, `emerson` y `bafar`. `alerting` conserva sensores inertes conforme al [ADR 0007](0007-tenant-alert-detection.md).

## Consecuencias

- Cada cliente puede tener assets y schedules propios sin mezclar runtime.
- Cada cliente puede tener dependencias o drivers unicos sin mezclar runtime ni datos.
- Los jobs pueden etiquetar `job_type` para seleccionar perfiles Spark.
- Los assets publican metadatos utiles para validacion.
- Los cambios compartidos deben revisarse en cada implementacion que tenga el mismo flujo.

