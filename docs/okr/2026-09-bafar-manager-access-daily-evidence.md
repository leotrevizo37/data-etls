# Bafar acceso diario de gerentes - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Automatizar en Bafar la ingesta de una snapshot diaria del ultimo acceso de los gerentes y conservar el historico para consultas por fecha, gerente y sucursal.

## Entregable

Pipeline diario desde API hacia SQL Server, con validacion estricta del contrato JSON, staging aislado por corrida, upsert historico, control de ingesta, indices de consulta y backfill diario.

## Valor

Conserva un corte diario consistente del estado de acceso reportado por la API y habilita comparaciones historicas por gerente, sucursal, zona y periodo sin consultar la fuente en tiempo real.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Alcance | Exclusivamente el `user_code` Bafar |
| Fuente | Endpoint versionado de ultimo acceso de gerentes |
| Tabla principal | `dwh.factManagerAccessDaily` |
| Staging | `stg.factManagerAccessDaily` aislado por `RunId` |
| Control de ingesta | `ctl.IngestionControl` con `IngestionName = 'FactManagerAccessDaily'` |
| Asset | `bafar/dwh_factmanageraccessdaily` |
| Job diario | `024_bafar_manager_access_daily` |
| Job de backfill | `025_bafar_manager_access_backfill_daily` |
| Schedule | `024_bafar_manager_access_daily_every_1d`, activo a las 21:40 en `America/Ciudad_Juarez` |
| Corte solicitado | `corteHora=23` |
| Ventana historica | `diasHistorial=180` |
| Particiones de backfill | Dias desde `2026-01-01` en `America/Ciudad_Juarez` |
| Llave historica | `Fecha`, `IdUsuario`, `ClaveTienda` |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | `dwh.factManagerAccessDaily`, `stg.factManagerAccessDaily`, indices y control | `sql/user_codes/bafar/init/012_dwh_stg_factManagerAccessDaily.sql` |
| Recurso API | Autenticacion, solicitud y validacion del contrato JSON | `dagster/bafar/src/bafar/defs/resources/manager_access_api.py` |
| Recurso SQL Server | Escritura de staging y `MERGE` historico | `dagster/bafar/src/bafar/defs/resources/sql_server.py` |
| Asset | `dwh_factmanageraccessdaily` | `dagster/bafar/src/bafar/defs/assets/dwh_factManagerAccessDaily.py` |
| Job diario | `024_bafar_manager_access_daily` | `dagster/bafar/src/bafar/defs/jobs/manager_access_daily_job.py` |
| Job de backfill | `025_bafar_manager_access_backfill_daily` | `dagster/bafar/src/bafar/defs/jobs/manager_access_backfill_job.py` |
| Schedule | `024_bafar_manager_access_daily_every_1d` | `dagster/bafar/src/bafar/defs/schedules/manager_access_daily_schedule.py` |
| Configuracion | Variables de conexion a la API | `compose-data_etls/.env.example` |

## Flujo tecnico

1. El asset calcula la fecha local en `America/Ciudad_Juarez` y obtiene el watermark `FactManagerAccessDaily`.
2. Solicita a la API la snapshot completa con `fecha`, `corteHora=23` y `diasHistorial=180` explicitos, sin filtrar `soloSinIngreso`.
3. Verifica la respuesta HTTP, el tipo `application/json` y el contrato completo antes de escribir datos.
4. Limpia e inserta la snapshot en `stg.factManagerAccessDaily` con el `RunId` de la corrida.
5. SQL Server consolida la informacion en `dwh.factManagerAccessDaily` mediante `MERGE` por `Fecha`, `IdUsuario` y `ClaveTienda`.
6. El staging se limpia y el watermark avanza a `FechaCorte` unicamente cuando la corrida termina correctamente.
7. Ante una falla, el pipeline conserva el watermark previo, registra el error y permite hasta tres reintentos con espera de 60 segundos.

## Backfill diario

1. `025_bafar_manager_access_backfill_daily` genera una corrida por fecha desde `2026-01-01`.
2. Cada particion solicita una sola snapshot con el mismo corte y aplica la misma validacion contractual.
3. Reutiliza el staging y el `MERGE`, por lo que una repeticion actualiza la fecha solicitada sin duplicar la llave historica.
4. El backfill no consulta ni modifica el watermark operativo de `FactManagerAccessDaily`.
5. El staging del `RunId` se limpia antes y despues de cada carga, incluso ante errores.

## Validaciones del contrato API

| Control | Cobertura |
|---|---|
| Estructura | Campos obligatorios y rechazo de campos desconocidos en encabezado y detalle |
| Tipos y nulos | Tipos exactos, campos nulables permitidos y longitudes compatibles con SQL Server |
| Fechas | Formato local ISO con entre uno y siete digitos fraccionarios, compatible con Python 3.10 y escritura tipada a SQL Server |
| Corte | Correspondencia entre `fecha`, `corteHora`, `diasHistorial` y `fechaCorte` en hora local |
| Consistencia | Gerentes distintos, `totalRegistros`, porcentaje, tres estados de corte y ultimo inicio de sesion |
| Identidad | Rechazo de llaves duplicadas por gerente y sucursal dentro de la snapshot |
| Volumen | Aceptacion de snapshots vacias cuando los agregados son consistentes |

## Indices para consulta historica

| Indice | Uso cubierto |
|---|---|
| `PK_factManagerAccessDaily` | Identidad por fecha, gerente y sucursal |
| `ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte` | Consultas por periodo y estado de acceso |
| `ix_factManagerAccessDaily_claveTienda_fecha` | Comparaciones historicas por sucursal y fecha |
| `ix_stg_factManagerAccessDaily_run` | Aislamiento y limpieza eficiente de staging por corrida |

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/bafar/tests/test_manager_access_daily.py` | Contrato API, asset, watermark, reintentos, backfill y jobs |
| `dagster/bafar/tests/test_manager_access_daily_schedule.py` | Nombre, job, cron, zona horaria y estado del schedule |
| `dagster/bafar/tests/test_fact_manager_access_daily_merge_sql.py` | Staging, nulabilidad, `MERGE`, llave historica, indices y control de ingesta |

La suite focalizada registra 15 pruebas exitosas. La suite completa del `user_code` Bafar registra 46 pruebas exitosas, incluida la validacion del DDL con el arbol SQL montado. La validacion de definiciones de `bafar.definitions` finalizo correctamente.

## Evidencia pendiente

La evidencia tecnica confirma que el contrato, la carga diaria, el backfill, el upsert y los indices estan definidos y validados en el repositorio. Para acreditar operacion productiva falta corroborar la aplicacion del DDL y una materializacion exitosa contra la API y la base de datos del entorno autorizado.

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release |
|---|---|---|
| Septiembre 2026 | Automatizar la ingesta historica del ultimo acceso de gerentes Bafar | El release incorpora el contrato API, la snapshot diaria, el control de ingesta, el upsert, los indices y el backfill necesarios para disponer del historico en SQL Server. |
