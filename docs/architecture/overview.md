# Arquitectura

## Proposito

El proyecto implementa una capa de servicio para analitica y consumo por DUMA, Power BI y herramientas agenticas. La idea central es transformar datos de origen lentos o fragmentados en contratos SQL Server mas estables, indexados y consultables.

## Componentes

| Componente | Responsabilidad |
|---|---|
| Dagster | Orquestacion de assets, jobs, schedules, reintentos y metadatos de ejecucion |
| Spark standalone | Transformacion distribuida de lecturas, dimensiones y fuentes MySQL |
| SQL Server | DWH, staging, tablas de control y procedimientos operativos |
| ADLS / Ozone S3A | Lectura de parquets y almacenamiento intermedio/modelos |
| MySQL SmartAudits | Fuente transaccional para resultados y evidencias revisadas por IA |
| MySQL Mantiz | Fuente transaccional para tickets, comentarios, equipos y refacciones |

## Capas de datos

| Capa | Esquema / ubicacion | Uso |
|---|---|---|
| Fuente | ADLS, MySQL, SQL Server origen | Datos operativos sin contrato de serving |
| Staging | `stg` | Carga temporal por `RunId` antes de `MERGE` o reemplazo controlado |
| Warehouse | `dwh` | Tablas y vistas para consumo analitico |
| Control | `ctl` | Watermarks, colas de revision, modelos activos y logs operativos |

## Patron de ejecucion

1. Dagster abre una ventana incremental desde `ctl.IngestionControl`.
2. Spark lee fuentes externas o tablas DWH existentes.
3. La transformacion produce un DataFrame con el contrato esperado.
4. El DataFrame se escribe directo a `dwh` o primero a `stg`.
5. SQL Server consolida los datos mediante `MERGE`, reemplazo por llaves o borrado por ventana.
6. Dagster actualiza el watermark solo si la corrida termina correctamente.

## Limites importantes

- Los seis `user_codes` comparten patrones, pero no todos tienen las mismas features.
- SmartAudits y Mantiz estan implementados en `carlsjr`.
- `FactSensorEvents` existe en los `user_codes` que implementan eventos de sensores.
- Los contratos de tabla se deben cambiar en DDL, writer Spark, merge SQL y tests cuando aplique.

