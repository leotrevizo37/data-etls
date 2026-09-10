# Bafar horarios de sucursales desde API - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Reemplazar la carga estatica de horarios de sucursales Bafar por una ingesta diaria desde API, con capas de persistencia separadas y proteccion del ultimo horario valido ante respuestas incompletas o fallas de procesamiento.

## Entregable

Pipeline diario desde API hacia SQL Server, con paginacion por cursor, persistencia de respuestas crudas, validacion estricta del contrato, staging aislado por corrida, consolidacion idempotente, control de ingesta y migracion del esquema ancho anterior.

## Valor

Mantiene actualizado el horario operativo usado para clasificar ventas fuera de horario y evita que una pagina fallida, un cambio incompatible del contrato o un error de escritura reemplace informacion valida en el DWH.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Alcance | Exclusivamente el `user_code` Bafar |
| Fuente | API de horarios de sucursales con prefijo `9999` |
| Raw | `raw.StoreSchedulesApiPage` por `RunId`, intento y pagina |
| Staging | `stg.dimStoreSchedules` normalizada y aislada por `RunId` |
| Tabla principal | `dwh.dimStoreSchedules` |
| Control de ingesta | `ctl.IngestionControl` con `IngestionName = 'DimStoreSchedules'` |
| Asset | `bafar/dwh_dimstoreschedules` |
| Job diario | `010_bafar_store_schedules` |
| Schedule | `010_bafar_store_schedules_every_1d`, detenido por defecto a las 02:30 en `America/Ciudad_Juarez` |
| Granularidad | Una fila por `StoreId` y `DayNo` |
| Borrado logico | `IsDeleted = 1` cuando `ClosingDate` es anterior a la fecha UTC actual |
| Consumidor | Pipeline `020_bafar_out_of_hours_sales` |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | `dwh.dimStoreSchedules`, `stg.dimStoreSchedules` e indice de staging | `sql/user_codes/bafar/init/001_dwh_stg_dimStoreSchedules.sql` |
| DDL | `raw.StoreSchedulesApiPage` y control de ingesta | `sql/user_codes/bafar/init/017_raw_ctl_dimStoreSchedulesApi.sql` |
| Migracion | Conversion transaccional del esquema ancho anterior | `sql/user_codes/bafar/init/018_migrate_dimStoreSchedulesApi.sql` |
| Recurso API | Solicitud paginada y validacion del contrato JSON | `dagster/bafar/src/bafar/defs/resources/store_schedules_api.py` |
| Recurso SQL Server | Escritura de `raw`, staging y `MERGE` | `dagster/bafar/src/bafar/defs/resources/sql_server.py` |
| Asset | `dwh_dimstoreschedules` | `dagster/bafar/src/bafar/defs/assets/dwh_dimStoreSchedules.py` |
| Job | `010_bafar_store_schedules` | `dagster/bafar/src/bafar/defs/jobs/store_schedules_job.py` |
| Schedule | `010_bafar_store_schedules_every_1d` | `dagster/bafar/src/bafar/defs/schedules/store_schedules_schedule.py` |
| Configuracion | URL, autenticacion, timeout y tamano de pagina | `compose-data_etls/.env.example`, `compose-data_etls/docker-compose.yml` |
| Documentacion | Pipeline y contrato de datos | `docs/pipelines/dim_store_schedules.md`, `docs/data_model/out_of_hours_sales/dim_store_schedules.md` |

## Flujo tecnico

1. El asset obtiene el watermark `DimStoreSchedules`, marca el inicio y limpia el staging de su `RunId`.
2. Solicita todas las paginas de la API mediante `HAS_MORE` y `NEXT_CURSOR`.
3. Persiste cada respuesta en `raw.StoreSchedulesApiPage` antes de validar su contenido.
4. Valida el contrato de cada pagina y la identidad de las sucursales en el conjunto completo.
5. Normaliza cada sucursal a siete filas, una por dia de la semana.
6. Escribe las filas en `stg.dimStoreSchedules` y ejecuta el `MERGE` por `StoreId` y `DayNo`.
7. Limpia el staging y avanza el watermark unicamente cuando la corrida termina correctamente.
8. Ante una falla, limpia el staging del intento, conserva el watermark anterior y permite hasta tres reintentos con espera de 60 segundos.

## Capas de persistencia

| Capa | Comportamiento |
|---|---|
| `raw` | Conserva el JSON exacto recibido por corrida, intento y pagina; no se vacia al completar la carga. |
| `stg` | Contiene temporalmente las filas normalizadas; se limpia al iniciar, completar o fallar la corrida. |
| `dwh` | Conserva el ultimo horario validado y solo cambia despues de recibir y validar todas las paginas. |

La transformacion de `raw` a staging ocurre durante la misma corrida a partir de la respuesta ya persistida. El pipeline no vuelve a leer `raw` para construir el staging.

## Validaciones del contrato API

| Control | Cobertura |
|---|---|
| Resultado | Rechazo de `SUCCESS = false` aunque la respuesta HTTP sea exitosa |
| Conteo | Correspondencia exacta entre `COUNT` y el numero de sucursales de la pagina |
| Esquema | Rechazo de campos nuevos, faltantes o con tipos incompatibles |
| Fechas y horas | Formatos, valores, vigencias y rangos validos |
| Semana completa | Siete dias unicos por sucursal y correspondencia entre `DAY` y `DAY_NO` |
| Estado del dia | Compatibilidad entre `CLOSED` y la presencia de slots |
| Franjas | Uno o dos slots para dias abiertos, rangos ordenados y sin superposicion |
| Identidad | Rechazo de sucursales duplicadas dentro de una pagina o entre paginas |
| Paginacion | Cursor obligatorio cuando existen mas paginas y rechazo de cursores repetidos |
| Transporte | Rechazo de errores de red, HTTP no exitoso, contenido no JSON o JSON invalido |
| Persistencia | El `MERGE` no se ejecuta ante fallas de normalizacion o escritura de staging |

## Consolidacion y preservacion del DWH

- El `MERGE` compara todos los atributos de negocio mediante `EXCEPT`.
- `ModifiedAt` solo cambia cuando cambia un atributo de la fila; una respuesta identica no ejecuta el `UPDATE`.
- `CreatedAt` se asigna al insertar y no se modifica posteriormente.
- No existe `WHEN NOT MATCHED BY SOURCE`, por lo que una sucursal ausente no se elimina ni se modifica automaticamente.
- Si falla una pagina posterior, el flujo no escribe staging ni ejecuta el `MERGE` y conserva el ultimo horario valido.
- `IsDeleted` se deriva de `ClosingDate` para las filas recibidas y no depende de una eliminacion fisica.

## Migracion del esquema anterior

1. El DDL principal define directamente el contrato normalizado para instalaciones nuevas.
2. La migracion solo se ejecuta cuando la tabla existente no contiene `DayNo`.
3. Cada fila del esquema ancho se convierte en siete filas dentro de una transaccion.
4. El staging anterior se reemplaza porque es una capa efimera.
5. Las referencias a columnas legadas se compilan dinamicamente unicamente cuando la migracion corresponde, permitiendo que el script sea idempotente sobre una tabla ya normalizada.

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/bafar/tests/test_store_schedules_api.py` | Contrato API, normalizacion, paginacion, fallas parciales, preservacion del DWH, reintentos, job y schedule |
| `dagster/bafar/tests/test_dim_store_schedules_merge_sql.py` | Llave del `MERGE`, cambios idempotentes, timestamps, soft delete, limpieza y escritura de staging |
| `dagster/bafar/tests/test_spark.py` | Lectura de horarios normalizados y conversion del numero de dia para el consumidor Spark |

Las pruebas focalizadas de API, asset y `MERGE` registran 20 casos exitosos. La suite completa del `user_code` Bafar registra 64 pruebas exitosas.

## Evidencia pendiente

La evidencia tecnica confirma el contrato API, la paginacion, las validaciones, la separacion de capas, la consolidacion y las protecciones ante fallas mediante pruebas locales. Para acreditar operacion en el entorno autorizado falta volver a ejecutar el SQL init con la correccion de compilacion de la migracion, comprobar el esquema resultante y completar una materializacion contra la API y SQL Server reales.

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release |
|---|---|---|
| Septiembre 2026 | Automatizar y robustecer la actualizacion de horarios de sucursales Bafar | El release sustituye la fuente estatica por una API paginada, conserva evidencia raw, valida el contrato completo y protege el ultimo horario valido usado por ventas fuera de horario. |
