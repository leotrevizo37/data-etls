# Pipelines de mantenimiento

## Jobs

| Job | Proposito |
|---|---|
| `080_maintenance_cleanup` | Limpieza de assets/runs segun seleccion configurada |
| `081_maintenance_index` | Reorganiza o reconstruye indices SQL Server |
| `082_maintenance_refresh_dimensions` | Refresca `dwh.dimSidonProdDimensions` |
| `083_maintenance_refresh_sensor_expected_values` | Refresca `dwh.dimSensorExpectedVal` en `user_codes` que lo implementan |
| `089_maintenance_spark_healthcheck` | Verifica escritura/lectura del IO manager Spark |

## SQL Server

`ctl.usp_IndexMaintenance_Run` revisa fragmentacion con `sys.dm_db_index_physical_stats` y decide:

| Condicion | Accion |
|---|---|
| Fragmentacion menor a umbral de reorganizacion | `SKIP` |
| Fragmentacion entre reorganizacion y rebuild | `REORGANIZE` + `UPDATE STATISTICS` |
| Fragmentacion mayor a umbral de rebuild | `REBUILD` |

El resultado queda en `ctl.IndexMaintenanceLog`.

## Spark

El healthcheck usa el perfil de mantenimiento cuando el nombre del job contiene mantenimiento o cuando se fuerza por tags/env. Sirve para validar disponibilidad de Spark y del path de `spark_df_io_manager`.

