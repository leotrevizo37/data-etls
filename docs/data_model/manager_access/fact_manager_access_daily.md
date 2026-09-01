# factManagerAccessDaily

## Proposito

`dwh.factManagerAccessDaily` conserva una snapshot diaria del estado de acceso de cada gerente y sucursal reportados por la API de Bafar.

## Fuentes de datos

- API de ultimo acceso de gerentes de sucursal.
- Control de ingesta: `ctl.IngestionControl` con `IngestionName = 'FactManagerAccessDaily'`.

## Granularidad

Un registro representa el estado de un gerente en una sucursal para una fecha de evaluacion. La clave primaria es `Fecha`, `IdUsuario` y `ClaveTienda`.

## Columnas principales

| Columna | Tipo SQL | Nulable | Descripcion |
|---|---|---:|---|
| `Fecha` | `DATE` | No | Fecha local solicitada a la API y particion logica de la snapshot. |
| `FechaCorte` | `DATETIME2(0)` | No | Fecha y hora local exacta aplicada como corte. |
| `CorteHora` | `TINYINT` | No | Hora del corte devuelta por la API. |
| `IdUsuario` | `INT` | No | Identificador del gerente. |
| `NoEmpleado` | `NVARCHAR(50)` | No | Numero de empleado. |
| `NombreGerente` | `NVARCHAR(250)` | No | Nombre reportado para el gerente. |
| `Email` | `NVARCHAR(320)` | No | Correo reportado para el gerente. |
| `Puesto` | `NVARCHAR(250)` | No | Puesto reportado. |
| `Rol` | `NVARCHAR(250)` | No | Rol del usuario en la plataforma. |
| `Activo` | `BIT` | No | Estado activo del usuario. |
| `IdTienda` | `INT` | Si | Identificador numerico de la tienda cuando esta disponible. |
| `ClaveTienda` | `NVARCHAR(50)` | No | Clave de tienda usada en la identidad de la snapshot. |
| `NombreTienda` | `NVARCHAR(250)` | No | Nombre de la tienda. |
| `Zona` | `NVARCHAR(150)` | No | Zona asociada con la tienda. |
| `UltimoInicioSesion` | `DATETIME2(0)` | Si | Ultima fecha y hora local de acceso disponible. |
| `IngresoAntesDelCorte` | `BIT` | No | Indica si el acceso del dia ocurrio antes o a la hora de corte. |
| `DiasSinIngresar` | `INT` | Si | Dias sin ingreso reportados por la API. |
| `EstatusCorte` | `NVARCHAR(50)` | No | Estado textual consistente con `IngresoAntesDelCorte`. |
| `ModifiedAt` | `DATETIME2(0)` | No | Fecha UTC de insercion o ultima actualizacion por upsert. |

## Reglas de negocio

- La snapshot se escribe unicamente despues de validar el contrato completo de la API.
- `IdTienda`, `UltimoInicioSesion` y `DiasSinIngresar` pueden ser nulos y no provocan que el registro sea excluido.
- Los agregados del encabezado se validan contra el detalle y se publican como metadatos de la materializacion; no se duplican en cada fila.
- El `MERGE` compara `Fecha`, `IdUsuario` y `ClaveTienda`.
- Una repeticion de la misma snapshot actualiza los atributos y `ModifiedAt`; una fecha nueva conserva el historico anterior.
- El indice `ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte` soporta comparaciones por periodo y estado de acceso.
- El indice `ix_factManagerAccessDaily_claveTienda_fecha` soporta comparaciones historicas por tienda.
- Los indices del esquema `dwh` quedan incluidos en el job `081_maintenance_index`.
- La tabla contiene datos personales y debe permanecer dentro de los accesos autorizados del proyecto.

## Backfill

- El job `025_bafar_manager_access_backfill_daily` reprocesa particiones desde `2026-08-27` en `America/Ciudad_Juarez`.
- Cada particion solicita una fecha, reutiliza `stg.factManagerAccessDaily` y consolida con el mismo `MERGE`.
- El backfill no lee ni actualiza el watermark `FactManagerAccessDaily`.
- Los datos historicos reflejan la respuesta que la API entregue para la fecha solicitada.

## Uso esperado

- Comparaciones historicas de actividad por gerente, sucursal, zona y periodo.
- Consulta del estado diario aplicado en cada fecha de corte.
