# Pipeline `010_bafar_store_schedules`

## Proposito

Obtener los horarios de las sucursales Bafar desde la API de horarios y consolidarlos en `dwh.dimStoreSchedules` sin perder el ultimo horario valido cuando una pagina o una corrida falla.

## Entradas

- API Bafar de horarios de sucursales con prefijo `9999` y paginacion por cursor.
- Watermark `DimStoreSchedules` en `ctl.IngestionControl`.

## Salidas

- `raw.StoreSchedulesApiPage` conserva cada pagina recibida por corrida, intento y numero de pagina.
- `stg.dimStoreSchedules` contiene temporalmente los horarios normalizados de la corrida.
- `dwh.dimStoreSchedules` conserva el ultimo horario valido por sucursal y dia.

## Programacion

- Job: `010_bafar_store_schedules`.
- Schedule: `010_bafar_store_schedules_every_1d`.
- Frecuencia: diariamente a las 02:30.
- Zona horaria: `America/Ciudad_Juarez`.
- Estado inicial: detenido.

## Flujo

1. Lee el watermark anterior y marca el inicio de la corrida.
2. Limpia las filas de `stg.dimStoreSchedules` correspondientes al `RunId`.
3. Consulta la API pagina por pagina.
4. Persiste cada respuesta en `raw.StoreSchedulesApiPage` antes de validarla.
5. Valida el contrato y la paginacion completa.
6. Normaliza cada sucursal a siete filas, una por dia de la semana.
7. Escribe las filas normalizadas en `stg.dimStoreSchedules`.
8. Ejecuta el `MERGE` hacia `dwh.dimStoreSchedules`.
9. Limpia el staging y actualiza el watermark solamente si la corrida termina correctamente.

La tabla `raw.StoreSchedulesApiPage` no se vacia al terminar. `stg.dimStoreSchedules` es efimera y se limpia al iniciar, al completar y al fallar la corrida.

## Validaciones

El flujo rechaza la corrida antes del `MERGE` cuando encuentra:

- `SUCCESS = false`, `HTTP_STATUS` inesperado o `COUNT` distinto al numero de tiendas.
- Campos nuevos, faltantes o con tipos incompatibles.
- Fechas, horas o rangos invalidos.
- Sucursales sin los siete dias o con dias duplicados.
- Estados `CLOSED` incompatibles con los slots.
- Dias abiertos sin slots, con mas de dos slots o con franjas superpuestas.
- Sucursales duplicadas dentro de una pagina o entre paginas.
- Cursor ausente, repetido o inconsistente con `HAS_MORE`.
- Fallos de red, normalizacion, escritura de staging o ejecucion del `MERGE`.

## Consolidacion

El `MERGE` usa `(StoreId, DayNo)` como llave. Una fila existente se actualiza unicamente cuando cambia alguno de sus atributos de negocio, incluyendo las horas de apertura, comida o cierre. En consecuencia, `ModifiedAt` permanece intacto cuando la API entrega los mismos valores.

`CreatedAt` se asigna solamente al insertar una fila nueva. `IsDeleted` se calcula como verdadero cuando `ClosingDate` es anterior a la fecha UTC de la corrida.

El `MERGE` no elimina ni modifica filas ausentes en la fuente. Si falla cualquier pagina antes de completar la validacion, no se ejecuta el `MERGE` y `dwh.dimStoreSchedules` conserva el ultimo horario valido.

## Reintentos

El pipeline permite hasta tres reintentos con una espera de 60 segundos. Las paginas recibidas permanecen en `raw` separadas por `AttemptNumber`. Si se agotan los reintentos, se conserva el watermark anterior y se registra el error en el control de ingestiones.

## DDL y migracion

- El contrato actual de `dwh.dimStoreSchedules` y `stg.dimStoreSchedules` vive en `sql/user_codes/bafar/init/001_dwh_stg_dimStoreSchedules.sql`.
- La tabla `raw` y el registro de control viven en `sql/user_codes/bafar/init/017_raw_ctl_dimStoreSchedulesApi.sql`.
- La conversion del esquema ancho anterior vive en `sql/user_codes/bafar/init/018_migrate_dimStoreSchedulesApi.sql`.
