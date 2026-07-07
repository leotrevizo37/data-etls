# Infraestructura

## Runtime

El repositorio corre con Docker Compose, Dagster y Spark standalone. `workspace.yaml` registra seis code locations por gRPC, una por cada `user_code`.

| Servicio logico | Referencia |
|---|---|
| Dagster webserver | `DAGSTER_WEB_PORT` |
| User code gRPC | `MCDONALDS_USER_CODE_PORT`, `SMARTFIT_USER_CODE_PORT`, `CDP_USER_CODE_PORT`, `GABINETE_USER_CODE_PORT`, `VALLEDELENCINO_USER_CODE_PORT`, `CARLSJR_USER_CODE_PORT` |
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
| `backfill` | Backfills |

## Almacenamiento intermedio

Los DataFrames entre ops se materializan con `spark_df_io_manager`. El path base se controla con `SPARK_DF_IO_BASE_PATH` y en Compose se define por `user_code`, por ejemplo un prefijo separado para `mcdonalds`, `smartfit`, `mcdonalds_cdp`, `bafar_poc_gabinete`, `valledelencino` y `carlsjr`.

## Regla de configuracion

La documentacion puede nombrar variables, pero no debe incluir valores reales. Para entender nombres de variables se usa `compose-data_etls/.env.example`; los archivos `.env` reales no se deben abrir ni copiar.

