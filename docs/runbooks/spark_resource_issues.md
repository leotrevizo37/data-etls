# Runbook: Spark resource issues

## Objetivo

Diagnosticar fallas por falta de recursos Spark, workers no disponibles o problemas de IO manager.

## Senales

| Sintoma | Posible causa |
|---|---|
| Timeout esperando recursos | No hay cores o memoria libres en Spark master |
| Worker unreachable | Problema de red hacia worker o block manager |
| Error escribiendo parquet intermedio | `SPARK_DF_IO_BASE_PATH` no disponible |
| Job pesado se queda pendiente | Perfil Spark insuficiente |

## Perfiles relevantes

| Perfil | Variables |
|---|---|
| `lecturas` | `SPARK_LECTURAS_MIN_CORES`, `SPARK_LECTURAS_MIN_MEMORY_MB`, `SPARK_LECTURAS_CORES_MAX` |
| `operaciones` | `SPARK_OPERACIONES_MIN_CORES`, `SPARK_OPERACIONES_MIN_MEMORY_MB`, `SPARK_OPERACIONES_CORES_MAX` |
| `smartaudits` | `SPARK_SMARTAUDITS_MIN_CORES`, `SPARK_SMARTAUDITS_MIN_MEMORY_MB`, `SPARK_SMARTAUDITS_CORES_MAX` |
| `mantiz` | `SPARK_MANTIZ_MIN_CORES`, `SPARK_MANTIZ_MIN_MEMORY_MB`, `SPARK_MANTIZ_CORES_MAX` |
| `maintenance` | `SPARK_MAINTENANCE_MIN_CORES`, `SPARK_MAINTENANCE_MIN_MEMORY_MB`, `SPARK_MAINTENANCE_CORES_MAX` |

## Pasos

1. Revisar Spark master UI o `SPARK_MASTER_STATUS_URL`.
2. Confirmar que el job tenga el tag `job_type` esperado.
3. Ejecutar `089_maintenance_spark_healthcheck` en el `user_code` afectado.
4. Validar que `SPARK_DF_IO_BASE_PATH` sea accesible para driver y workers.
5. Revisar logs de driver y workers.
5. Reintentar cuando haya recursos libres.
6. Ó cancelar aplicación en caso de que se haya quedado colgando

