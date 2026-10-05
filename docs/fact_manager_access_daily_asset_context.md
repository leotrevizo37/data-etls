# Contexto y contrato de `dwh_factmanageraccessdaily`

Estado verificado contra el código del repositorio: 2026-09-01.

## 1. Propósito y alcance

`dwh_factmanageraccessdaily` es un asset exclusivo del `user_code` Bafar. Su propósito es solicitar a una API la snapshot diaria del último acceso de los gerentes y persistirla en SQL Server para consulta histórica.

El asset evita que los consumidores analíticos tengan que consultar repetidamente la API. La interfaz estable para esos consumidores es `dwh.factManagerAccessDaily`.

El asset no representa eventos individuales de inicio de sesión. Cada fila representa el estado de un gerente en una sucursal para una fecha de snapshot.

## 2. Identidad en Dagster

| Propiedad | Valor actual |
|---|---|
| Asset key | `AssetKey(["bafar", "dwh_factmanageraccessdaily"])` |
| Nombre | `dwh_factmanageraccessdaily` |
| Prefijo | `bafar` |
| Grupo | `bafar` |
| Implementación | `graph_asset` respaldado por `ingest_fact_manager_access_daily_op` |
| Job | `024_bafar_manager_access_daily` |
| Schedule | `024_bafar_manager_access_daily_every_1d` |
| Recursos requeridos | `manager_access_api`, `sqlserver` |
| Upstream assets declarados | Ninguno |
| Particiones del asset | Ninguna |
| Asset checks dedicados | Ninguno |

Los recursos son dependencias de ejecución, pero no aparecen como upstream assets en el grafo. El asset se carga automáticamente mediante `load_from_defs_folder` dentro de `bafar.definitions`.

## 3. Frecuencia y tiempo

| Propiedad | Valor actual |
|---|---|
| Frecuencia programada | Una vez al día |
| Cron | `40 21 * * *` |
| Hora programada | 21:40 |
| Zona horaria | `America/Ciudad_Juarez` |
| Estado inicial | `RUNNING` |
| Hora de corte solicitada | `23` |

La fecha solicitada por el asset se calcula con la fecha actual de `America/Ciudad_Juarez`. El asset siempre envía `corteHora=23`.

El estado actual tiene una diferencia importante: el schedule ejecuta a las 21:40, mientras la API recibe un corte de las 23:00 del mismo día. El código no documenta ni demuestra cómo resuelve la API ese desfase. Por lo tanto, un consumidor no debe asumir que la ejecución ocurrió después de las 23:00; debe utilizar `Fecha`, `FechaCorte` y el estado real de la materialización.

El asset no utiliza el watermark para decidir qué fecha solicitar ni para omitir una fecha ya cargada. Una nueva ejecución del mismo día vuelve a solicitar y hacer upsert sobre esa fecha.

## 4. Interfaz de entrada: API

### Solicitud HTTP

| Elemento | Contrato actual |
|---|---|
| Método | `GET` |
| Parámetro `fecha` | Fecha ISO `YYYY-MM-DD` |
| Parámetro `corteHora` | Entero entre `0` y `23`; el asset y el backfill actuales envían `23` |
| Parámetro `diasHistorial` | Entero entre `1` y `730`; el asset y el backfill actuales envían `180` |
| Header `Accept` | `application/json` |
| Autenticación | HTTP Basic |
| Respuesta aceptada | HTTP `200` con content type `application/json` |
| Codificación | UTF-8 |

Variables de configuración utilizadas, sin valores:

- `BAFAR_MANAGER_ACCESS_API_URL`
- `BAFAR_MANAGER_ACCESS_API_USER`
- `BAFAR_MANAGER_ACCESS_API_PASSWORD`
- `BAFAR_MANAGER_ACCESS_API_TIMEOUT_SECONDS`

La URL, usuario y contraseña deben ser no vacíos, y el timeout debe ser mayor que cero. El timeout predeterminado en el recurso es de 30 segundos.

No existe paginación en la implementación actual. La API debe devolver la snapshot completa en una sola respuesta.

### Contrato exacto del encabezado JSON

La respuesta debe ser un objeto y contener exactamente estos campos. Un campo faltante o adicional provoca el rechazo de toda la carga.

| Campo | Tipo requerido | Regla |
|---|---|---|
| `ok` | Booleano | Debe ser exactamente `true` |
| `fechaCorte` | String ISO datetime | Hora local sin offset; debe coincidir con `fecha` y `corteHora`, con minutos, segundos y microsegundos en cero; acepta de 1 a 7 dígitos fraccionarios |
| `corteHora` | Entero | Debe coincidir con el valor solicitado |
| `diasHistorial` | Entero | Debe coincidir con el valor solicitado y estar entre `1` y `730` |
| `totalGerentes` | Entero | No negativo e igual al número de `idUsuario` distintos |
| `totalIngresaronAntesDeCorte` | Entero | Gerentes distintos con ingreso antes del corte |
| `totalSinIngresoEnCorte` | Entero | Gerentes distintos sin ingreso al corte; junto con el total anterior debe sumar `totalGerentes` |
| `porcentajeCumplimiento` | Entero o decimal | Finito y consistente con el porcentaje calculado, con tolerancia de `0.01` |
| `totalRegistros` | Entero | Número de filas devueltas en `data` |
| `data` | Array | Puede estar vacío y conserva siempre el mismo contrato |

### Contrato exacto de cada elemento de `data`

Cada fila debe contener exactamente los siguientes campos:

| Campo JSON | Tipo JSON | Nulable | Límite o validación |
|---|---|---:|---|
| `idUsuario` | Entero | No | Mayor que cero |
| `noEmpleado` | String | No | Máximo 50 caracteres |
| `nombreGerente` | String | No | Máximo 250 caracteres |
| `nombreUsuario` | String | No | Máximo 250 caracteres |
| `email` | String | No | Máximo 320 caracteres |
| `puesto` | String | No | Máximo 250 caracteres |
| `rol` | String | No | Máximo 250 caracteres |
| `activo` | Booleano | No | Se conserva tal como lo entrega la API |
| `idTienda` | Entero | Sí | Si existe, debe ser mayor que cero |
| `claveTienda` | String | No | Máximo 50 caracteres |
| `nombreTienda` | String | No | Máximo 250 caracteres |
| `zona` | String | No | Máximo 150 caracteres |
| `ultimoInicioSesion` | String ISO datetime | Sí | Debe ser una fecha y hora local sin offset |
| `ingresoAntesDelCorte` | Booleano | No | Determina el estado esperado |
| `diasSinIngresar` | Entero | Sí | Si existe, no puede ser negativo |
| `estatusCorte` | String | No | Máximo 50 caracteres y valor consistente con el booleano |

Valores válidos de `estatusCorte` según la implementación:

| `ingresoAntesDelCorte` | `estatusCorte` requerido |
|---:|---|
| `true` | `Ingresó a tiempo` |
| `false`, con acceso previo dentro de la ventana | `Sin ingreso al corte` |
| `false`, sin acceso dentro de la ventana | `Sin ingreso en los últimos N días` |

Si `ingresoAntesDelCorte=true`, `ultimoInicioSesion` es obligatorio, debe pertenecer a la fecha solicitada, no puede ser posterior a `fechaCorte` y `diasSinIngresar` debe ser `0`.

Si `ingresoAntesDelCorte=false` y existe `ultimoInicioSesion`, el acceso debe ser anterior al día evaluado, `diasSinIngresar` debe coincidir con la diferencia de días y el estado debe ser `Sin ingreso al corte`. Si no existe acceso dentro de `diasHistorial`, `ultimoInicioSesion` y `diasSinIngresar` deben ser nulos y el estado incorpora el valor de la ventana.

La unicidad dentro de una respuesta se valida por `idUsuario` y `claveTienda`. Para detectar duplicados, `claveTienda` se compara sin espacios laterales y sin distinguir mayúsculas de minúsculas.

### Límites del contrato validado

La implementación actual no valida el formato semántico del correo, no exige que los strings sean distintos de vacío y no comprueba una llave foránea contra un catálogo de tiendas.

## 5. Interfaz de salida: tabla histórica

La tabla de consumo es `dwh.factManagerAccessDaily`.

### Granularidad y llave

Una fila representa:

> Un gerente, en una sucursal, para una fecha de snapshot.

La llave primaria clustered es:

```text
(Fecha, IdUsuario, ClaveTienda)
```

Un mismo gerente puede producir más de una fila en una fecha si aparece asociado con claves de tienda distintas.

### Esquema SQL

| Columna SQL | Tipo SQL | Nulable | Origen o semántica |
|---|---|---:|---|
| `Fecha` | `DATE` | No | Fecha local solicitada por el asset o la partición de backfill |
| `FechaCorte` | `DATETIME2(0)` | No | `fechaCorte` de la API; fecha y hora local sin offset almacenado |
| `CorteHora` | `TINYINT` | No | `corteHora` de la API |
| `DiasHistorial` | `SMALLINT` | Sí para datos históricos previos | `diasHistorial` aplicado a la snapshot; las cargas nuevas siempre lo informan |
| `IdUsuario` | `INT` | No | `idUsuario` |
| `NoEmpleado` | `NVARCHAR(50)` | No | `noEmpleado` |
| `NombreGerente` | `NVARCHAR(250)` | No | `nombreGerente` |
| `NombreUsuario` | `NVARCHAR(250)` | Sí para datos históricos previos | `nombreUsuario`; las cargas nuevas siempre lo informan |
| `Email` | `NVARCHAR(320)` | No | `email` |
| `Puesto` | `NVARCHAR(250)` | No | `puesto` |
| `Rol` | `NVARCHAR(250)` | No | `rol` |
| `Activo` | `BIT` | No | `activo` |
| `IdTienda` | `INT` | Sí | `idTienda` |
| `ClaveTienda` | `NVARCHAR(50)` | No | `claveTienda` |
| `NombreTienda` | `NVARCHAR(250)` | No | `nombreTienda` |
| `Zona` | `NVARCHAR(150)` | No | `zona` |
| `UltimoInicioSesion` | `DATETIME2(0)` | Sí | `ultimoInicioSesion`; hora local sin offset almacenado |
| `IngresoAntesDelCorte` | `BIT` | No | `ingresoAntesDelCorte` |
| `DiasSinIngresar` | `INT` | Sí | `diasSinIngresar` |
| `EstatusCorte` | `NVARCHAR(50)` | No | `estatusCorte` |
| `ModifiedAt` | `DATETIME2(0)` | No | Momento UTC de inserción o último upsert, generado con `SYSUTCDATETIME()` |

`FechaCorte` y `UltimoInicioSesion` no contienen zona u offset porque se almacenan como `DATETIME2(0)`. Su interpretación contractual es hora local. Las fracciones recibidas se validan antes de la escritura y SQL Server conserva la precisión a segundos del esquema existente. `ModifiedAt`, en cambio, se genera en UTC.

## 6. Staging y upsert

La carga utiliza `stg.factManagerAccessDaily`. El staging contiene las columnas de negocio de la tabla final más `RunId`, y no contiene `ModifiedAt`.

Flujo actual:

1. La respuesta completa se valida antes de cualquier escritura.
2. Se eliminan del staging las filas del `RunId` actual.
3. Se inserta el detalle validado mediante `executemany` y `fast_executemany`.
4. Se ejecuta un `MERGE` filtrado por ese `RunId`.
5. Se vuelve a limpiar el staging del `RunId`.

El `MERGE` compara `Fecha`, `IdUsuario` y `ClaveTienda`:

- Si la llave existe, actualiza todos los atributos no pertenecientes a la llave y renueva `ModifiedAt`.
- Si la llave no existe, inserta una fila nueva.
- No elimina filas de la tabla final que estén ausentes en una recarga de la misma fecha.

Por lo tanto, la operación es un upsert, no un reemplazo completo de la partición diaria. Una respuesta posterior con menos gerentes no elimina automáticamente las filas previamente almacenadas para ese día.

Las operaciones de escritura en staging, `MERGE`, limpieza y actualización del control usan conexiones y commits separados; no constituyen una única transacción de extremo a extremo. Si el `MERGE` ya fue confirmado y posteriormente falla la limpieza o el control, la tabla final puede contener el upsert aunque la corrida termine marcada como fallida. Una repetición vuelve a aplicar el mismo upsert.

## 7. Control de ingesta y fallos

El flujo diario utiliza `ctl.IngestionControl` con:

```text
IngestionName = FactManagerAccessDaily
```

Comportamiento:

1. Lee el watermark existente.
2. Marca la corrida como iniciada.
3. Ejecuta API, staging y `MERGE`.
4. En éxito, guarda como nuevo watermark el mayor valor entre el watermark previo y `FechaCorte`.
5. En el fallo final, conserva el watermark previo y registra el error, truncado a 4,000 caracteres.

El watermark nunca retrocede. Tampoco determina el rango de lectura: la ejecución normal siempre procesa la fecha local actual.

El asset admite hasta tres reintentos, con 60 segundos entre intentos. Antes de reintentar intenta limpiar el staging correspondiente al `RunId`.

## 8. Backfill

| Propiedad | Valor actual |
|---|---|
| Job | `025_bafar_manager_access_backfill_daily` |
| Tipo de partición | Diaria |
| Fecha inicial | `2026-01-01` |
| Zona horaria | `America/Ciudad_Juarez` |
| Configuración por corrida | `{"fecha": "YYYY-MM-DD"}` |
| Corte | `23` |
| Ventana histórica | `180` días |
| Reintentos | 3, con espera de 60 segundos |

Cada partición solicita exactamente una fecha, valida el mismo contrato y reutiliza el mismo staging y `MERGE`.

El backfill no lee, inicia ni actualiza `ctl.IngestionControl`. Su ejecución no cambia el watermark operativo del asset diario.

La capacidad histórica real depende de que la API devuelva una snapshot válida para la fecha solicitada. El código no sintetiza datos históricos cuando la API no los proporciona.

## 9. Índices disponibles

| Índice | Columnas llave | Columnas incluidas | Uso soportado |
|---|---|---|---|
| `PK_factManagerAccessDaily` | `Fecha`, `IdUsuario`, `ClaveTienda` | Ninguna | Unicidad y acceso por fecha/gerente/sucursal |
| `ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte` | `Fecha`, `IngresoAntesDelCorte` | `IdUsuario`, `IdTienda`, `ClaveTienda`, `UltimoInicioSesion`, `DiasSinIngresar` | Filtros por periodo y estado diario |
| `ix_factManagerAccessDaily_claveTienda_fecha` | `ClaveTienda`, `Fecha` | `IdUsuario`, `IdTienda`, `IngresoAntesDelCorte`, `UltimoInicioSesion`, `DiasSinIngresar` | Series históricas por sucursal |
| `ix_stg_factManagerAccessDaily_run` | `RunId`, `Fecha`, `IdUsuario`, `ClaveTienda` | Ninguna | Escritura, consolidación y limpieza aisladas por corrida |

## 10. Cómo depender del asset desde Dagster

La identidad exacta es:

```python
AssetKey(["bafar", "dwh_factmanageraccessdaily"])
```

Para declarar una dependencia de orden sin recibir un objeto en memoria, el patrón compatible es:

```python
from dagster import AssetKey, asset


@asset(deps=[AssetKey(["bafar", "dwh_factmanageraccessdaily"])])
def consumidor():
    ...
```

Esa dependencia expresa que el asset consumidor debe ejecutarse después de `dwh_factmanageraccessdaily`. No entrega las filas como DataFrame ni como argumento Python. El consumidor debe leer `dwh.factManagerAccessDaily` mediante su recurso de SQL Server.

Si únicamente se necesita incluir este asset en un job, la selección actual utilizada por el proyecto es:

```python
AssetSelection.keys(
    AssetKey(["bafar", "dwh_factmanageraccessdaily"]),
)
```

## 11. Cómo depender de la tabla desde SQL o BI

Los consumidores deben consultar `dwh.factManagerAccessDaily`, nunca `stg.factManagerAccessDaily`.

### Snapshot de una fecha conocida

```sql
SELECT
    Fecha,
    FechaCorte,
    DiasHistorial,
    IdUsuario,
    NombreUsuario,
    ClaveTienda,
    IdTienda,
    IngresoAntesDelCorte,
    UltimoInicioSesion,
    DiasSinIngresar
FROM dwh.factManagerAccessDaily
WHERE Fecha = @Fecha;
```

### Histórico de una sucursal

```sql
SELECT
    Fecha,
    FechaCorte,
    DiasHistorial,
    IdUsuario,
    NombreUsuario,
    IngresoAntesDelCorte,
    UltimoInicioSesion,
    DiasSinIngresar
FROM dwh.factManagerAccessDaily
WHERE ClaveTienda = @ClaveTienda
  AND Fecha >= @FechaDesde
  AND Fecha <= @FechaHasta;
```

Reglas para consumo correcto:

- Utilizar `Fecha` como fecha lógica de la snapshot.
- Utilizar `FechaCorte` para conocer el corte exacto devuelto por la API.
- Utilizar `DiasHistorial` para interpretar un `UltimoInicioSesion` nulo y el estado de falta de acceso.
- No utilizar `ModifiedAt` como fecha del evento ni de la snapshot; es auditoría técnica del upsert.
- Tratar `IdTienda`, `UltimoInicioSesion` y `DiasSinIngresar` como nulables.
- Utilizar `ClaveTienda` cuando se requiera la identidad que participa en la llave histórica; `IdTienda` puede ser nulo.
- Considerar que contar filas a través de varias fechas produce gerente-sucursal-días, no número de inicios de sesión.
- No interpretar `UltimoInicioSesion` como una tabla de eventos: solo conserva el último valor informado en cada snapshot.
- Verificar que la fecha esperada exista antes de publicar un corte. El schedule indica intención de ejecución, no garantía de materialización exitosa.

La tabla no persiste `totalGerentes`, `totalIngresaronAntesDeCorte`, `totalSinIngresoEnCorte`, `totalRegistros` ni `porcentajeCumplimiento`. Esos valores se publican como metadata de la materialización Dagster. En SQL pueden recalcularse desde el detalle, pero no recuperarse como el encabezado original de la API.

## 12. Metadata de materialización

Una materialización exitosa expone:

| Clave | Contenido |
|---|---|
| `fecha` | Fecha local solicitada |
| `fecha_corte` | `fechaCorte` devuelta por la API |
| `history_days` | `diasHistorial` devuelto por la API |
| `evaluated_rows` | Filas insertadas en staging |
| `affected_rows` | Filas reportadas por el `MERGE` como insertadas o actualizadas |
| `total_records` | `totalRegistros` devuelto por la API |
| `total_managers` | `totalGerentes` de la API |
| `entered_before_cutoff` | `totalIngresaronAntesDeCorte` |
| `without_entry_at_cutoff` | `totalSinIngresoEnCorte` |
| `compliance_percentage` | `porcentajeCumplimiento` |
| `rows_without_store_id` | Filas cuyo `idTienda` es nulo |

Esta metadata vive en Dagster y no forma parte del esquema de `dwh.factManagerAccessDaily`.

## 13. Garantías y límites para consumidores

### Garantías aplicadas antes de cargar

- Contrato JSON cerrado: no se aceptan campos faltantes ni adicionales.
- Tipos exactos y nulabilidad controlada.
- Fecha y hora de corte consistentes con la solicitud.
- Snapshot vacía aceptada únicamente cuando los totales y `totalRegistros` también son consistentes.
- Totales y porcentaje consistentes con el detalle.
- Estado de corte consistente con el booleano.
- Duplicados rechazados dentro de la respuesta.
- Longitudes compatibles con las columnas SQL.

### Aspectos que el asset no garantiza actualmente

- No existe un `AssetCheck` dedicado de frescura o volumen histórico.
- No existe validación de integridad referencial contra una dimensión de tiendas.
- No se compara el universo de gerentes contra días anteriores.
- El upsert no elimina filas ausentes en una recarga.
- El schedule de las 21:40 no demuestra por sí mismo que el corte de las 23:00 esté completo.
- El éxito del backfill histórico depende de la respuesta real de la API para cada fecha.
- No se conserva el JSON original ni el encabezado agregado dentro de la tabla final.

## 14. Seguridad de los datos

La tabla contiene datos identificables de empleados, incluyendo nombre, número de empleado y correo. Debe consumirse únicamente desde accesos autorizados del proyecto. Los secretos de la API y de SQL Server no forman parte de este contrato documental y no deben incorporarse en consultas, logs o artefactos derivados.

## 15. Archivos fuente verificados

- `dagster/bafar/src/bafar/defs/assets/dwh_factManagerAccessDaily.py`
- `dagster/bafar/src/bafar/defs/resources/manager_access_api.py`
- `dagster/bafar/src/bafar/defs/resources/sql_server.py`
- `dagster/bafar/src/bafar/defs/jobs/manager_access_daily_job.py`
- `dagster/bafar/src/bafar/defs/jobs/manager_access_backfill_job.py`
- `dagster/bafar/src/bafar/defs/schedules/manager_access_daily_schedule.py`
- `dagster/bafar/src/bafar/definitions.py`
- `sql/user_codes/bafar/init/012_dwh_stg_factManagerAccessDaily.sql`
- `dagster/bafar/tests/test_manager_access_daily.py`
- `dagster/bafar/tests/test_manager_access_daily_schedule.py`
- `dagster/bafar/tests/test_fact_manager_access_daily_merge_sql.py`
