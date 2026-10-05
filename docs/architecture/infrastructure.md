# Infraestructura

## Runtime

El repositorio corre con Docker Compose, Dagster y Spark standalone. `workspace.yaml` registra por gRPC `bafar_poc_gabinete`, `valledelencino`, `carlsjr`, `emerson` y `bafar`. La location inerte `alerting` existe, pero no está registrada en ese workspace; su existencia no acredita un servicio desplegado.

| Servicio logico | Referencia |
|---|---|
| Dagster webserver | `DAGSTER_WEB_PORT` |
| User code gRPC | Puertos internos 4004, 4005, 4006 y 4007 para Gabinete, Valle, Carlsjr y Emerson |
| Spark master | `SPARK_MASTER_URL`, `SPARK_MASTER_STATUS_URL` |
| Spark workers | `SPARK_WORKER_MEM`, `SPARK_WORKER_CORES` |
| SQL Server DWH | `MSSQL_HOST`, `MSSQL_PORT` |
| SQL Server dimensiones | `DIMSQL_HOST`, `DIMSQL_PORT` |
| ADLS | `ADLS_ACCOUNT_NAME`, `ADLS_CONTAINER_NAME`, `ADLS_BASE_PATH` |
| Ozone/S3A | `OZONE_S3_ENDPOINT`, `OZONE_S3_REGION` |

## Perfiles Spark

El recurso Spark selecciona perfil desde tags Dagster o variables de entorno. Los perfiles actuales son:

| Perfil | Uso |
|---|---|
| `default` | Jobs sin perfil especifico |
| `maintenance` | Healthcheck y mantenimiento |
| `dimensiones` | Refresh de dimensiones |
| `lecturas` | Ingestion de `FactLecturas` |
| `operaciones` | `factReadingsMeasurement` y tablas derivadas |
| `smartaudits` | Pipelines SmartAudits |
| `mantiz` | Pipelines Mantiz |
| `ventas` | Pipelines de ventas Bafar |
| `backfill` | Backfills |
| `observability` | Auditoría de lecturas y métricas de observabilidad |

## Almacenamiento intermedio

Los DataFrames entre ops se materializan con `spark_df_io_manager`. El path base se controla con `SPARK_DF_IO_BASE_PATH` y debe quedar separado por `user_code`. McDonalds, CDP y Smartfit son tenants retirados, no implementaciones actuales del flujo.

## Concurrencia

`dagster/dagster_home/dagster.yaml` configura como máximo tres runs concurrentes, uno por valor de `dagster/code_location`, uno por `user_code_run_lane` y dos por `job_type`. Los límites se aplican conjuntamente cuando los tags correspondientes están presentes; no son reservas de cores ni memoria Spark. Los jobs distinguen lanes `operational` y `observability` mediante sus tags, pero el límite por location sigue restringiendo cada tenant a un run concurrente. Un run en cola no demuestra una falla de Spark. Estos valores describen el archivo del repositorio, no el estado efectivo de una instancia desplegada.

## Regla de configuracion

La documentacion puede nombrar variables, pero no debe incluir valores reales. Para entender nombres de variables se usa `compose-data_etls/.env.example`; los archivos `.env` reales no se deben abrir ni copiar.

