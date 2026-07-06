# DUMA disponibilidad y frescura del dato - Release de optimizacion - 2026-07-17

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Mejorar la disponibilidad y frescura del dato en DUMA.

## Entregable

Optimizacion de escrituras a base de datos, lecturas al datalake, tiempos de ejecucion y cargas incrementales en procesos criticos.

## Valor

Reduce latencia, bloqueos e inconsistencias, mejorando la confiabilidad del dato consumido por dashboards, backend y agentes.

## Resumen directo

| Area | Cambio | Evidencia |
|---|---|---|
| Escrituras DWH | Se cambio el patron de carga directa con ventana sobre `dwh` por carga a `stg` y `MERGE` contra `dwh` | `stg.factReadingsMeasurement`, `dwh.factReadingsMeasurement`, `MERGE dwh.factReadingsMeasurement` |
| Idempotencia | El merge actualiza o inserta por `OperationId` y filtra la corrida por `RunId` | `target.OperationId = source.OperationId`, `WHERE RunId = ?` |
| Solape incremental | La ventana relee horas recientes para cubrir datos tardios o en carrera antes de avanzar watermark | `FACTREADINGS_OVERLAP_HOURS`, `build_operations_window` |
| Lectura datalake | Spark valida rutas parquet existentes antes de leer y devuelve dataframes vacios cuando no hay archivos legibles | `_existing_readable_paths`, `_has_readable_files`, `read_lecturas_range` |
| Sensor events | `dwh_factsensorevents` reutiliza una sola lectura extendida, materializa de forma controlada y evita validaciones extra sobre el dataframe transformado | `datalake_readings_extended(` aparece una vez, `localCheckpoint`, `unpersist` |
| Vistas DWH | Se agregaron vistas de consumo sobre mediciones por dispositivo y agrupacion CARLS | `dwh.vwFactReadingsMeasurementDevice`, `dwh.vwFactReadingsMeasurementDeviceGroups` |
| Dependencias Ozone/S3A | Las dependencias AWS/S3A quedaron en las imagenes base de Spark/Dagster y no duplicadas por driver | `hadoop-aws-3.4.1.jar`, `software.amazon.awssdk_bundle-2.24.6.jar` |
| Spark runtime | Se ajusto el master, los endpoints ruteables y la limpieza de workers para reducir ejecuciones colgadas y acumulacion de data temporal | `spark-master`, `SPARK_MASTER_BIND_HOST`, `SPARK_WORKER_CLEANUP_ENABLED` |

## Cambio arquitectonico principal

Antes, el flujo critico de `factReadingsMeasurement` dependia de limpiar una ventana directa en `dwh` y volver a cargar el rango procesado. Ese patron era simple, pero aumentaba el riesgo operativo: durante la ventana de escritura podia dejar huecos temporales, provocar bloqueos mas largos y ser menos tolerante a datos que llegan tarde.

El release cambia el contrato a:

1. Calcular una ventana incremental con solape.
2. Leer el rango necesario desde el datalake.
3. Transformar el dataframe operacional.
4. Escribir la corrida en `stg.factReadingsMeasurement` con `RunId`.
5. Ejecutar `MERGE` contra `dwh.factReadingsMeasurement` por `OperationId`.
6. Limpiar staging por `RunId`.
7. Avanzar watermark solo con la corrida terminada.

Este flujo permite reintentos mas seguros, cargas solapadas e idempotencia por llave operacional.

## Contratos de tablas

| Tabla | Rol en el release |
|---|---|
| `stg.factReadingsMeasurement` | Buffer de corrida para cargas operacionales antes del merge |
| `dwh.factReadingsMeasurement` | Tabla final de mediciones operacionales por hora, actualizada por upsert |
| `ctl.IngestionControl` | Watermark y estado de ejecucion del flujo incremental |

## Vistas agregadas

| Vista | Rol |
|---|---|
| `dwh.vwFactReadingsMeasurementDevice` | Expone mediciones por dispositivo y hora con sensores en JSON |
| `dwh.vwFactReadingsMeasurementDeviceGroups` | Vista CARLS para agrupar dispositivos por familia operativa y exponer dispositivos/sensores en JSON |

## Assets y runtime cubiertos

| Componente | Evidencia |
|---|---|
| `dwh_factReadingsMeasurement` | Escribe a stage, ejecuta merge y publica metadata de `staged_row_count`, `merged_rows` y `row_count` |
| `dwh_factsensorevents` | Reutiliza lectura extendida y evita re-materializaciones innecesarias |
| `SparkResource` | Configura S3A/Ozone, verifica recursos del master y controla escritura JDBC por chunks |
| `spark-master` | Publica puerto y host configurables para scheduler y status |
| `spark-worker` | Activa limpieza de app data con TTL configurable |

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/carlsjr/tests/test_factreadings_merge_sql.py` | Verifica merge de `stg.factReadingsMeasurement` a `dwh.factReadingsMeasurement` por `OperationId` y `RunId` |
| `dagster/carlsjr/tests/test_spark_transformations.py` | Cubre escritura staging y comportamiento de transformaciones Spark relacionadas |
| `dagster/carlsjr/tests/test_sensor_events_asset_optimization.py` | Verifica que sensor events use una sola lectura extendida y conserve el contrato de transformacion |
| `dagster/carlsjr/tests/test_spark_ozone_config.py` | Cubre configuracion Spark/Ozone/S3A |
| `dagster/carlsjr/tests/test_operations_window.py` | Cubre ventana operacional y solape incremental |

## Justificacion del OKR

Este release cubre el OKR del 17 de julio porque cambia el flujo critico de escrituras hacia un contrato mas estable: staging por corrida, merge idempotente y ventana solapada. Ese patron reduce el tiempo con datos finales expuestos a cambios parciales y permite que ejecuciones tardias o reintentadas converjan sobre el mismo estado final.

También mejora la frescura porque la ventana incremental no depende de recargar todo el historico y porque 
las lecturas del datalake evitan fallar ante particiones sin parquet legible. En runtime, las dependencias
S3A en imagen base, el master configurable y la limpieza de workers reducen friccion operativa para cargas
recurrentes, además la nueva imagen del spark, reduce el numero de dependencias resueltas desde el driver, 
lo que agiliza las aplicaciones y la duración de los pipelines. 
Además de la disponibilidad, se mejora la visibilidad de los datos gracias a vistas implementadas en esquema de dwh
que mejoran la redaccion, la abstraccion y el uso o consumo de los datos.
