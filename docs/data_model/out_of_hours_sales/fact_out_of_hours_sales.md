# factOutOfHoursSales

## Proposito

`dwh.factOutOfHoursSales` conserva las ventas Bafar que ocurren antes de la apertura, durante el horario de comida, despues del cierre o cuando no existe un horario aplicable para la sucursal.

## Fuentes de datos

- Ventas Bafar desde SQL Server.
- `dwh.dimStoreSchedules` para el horario vigente por sucursal y dia de la semana.
- `dwh.dimOutOfHoursTypes` para la descripcion del tipo de desviacion.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'FactOutOfHoursSales'`.

## Granularidad

Un registro representa una venta fuera de horario identificada por `SaleId`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `SaleId` | `BIGINT` | Identificador unico de la venta. |
| `SaleNumber` | `VARCHAR(50)` | Folio de venta. |
| `Amount` | `DECIMAL(19,4)` | Importe de la venta. |
| `LocalTimeSpan` | `DATETIME2(0)` | Fecha y hora local de la venta. |
| `UserName` | `NVARCHAR(150)` | Usuario asociado a la venta. |
| `StoreId` | `VARCHAR(50)` | Sucursal donde se realizo la venta. |
| `OutOfHoursType` | `TINYINT` | Tipo de venta fuera de horario. |
| `DeviationMinutes` | `INT` | Minutos de desviacion respecto al limite aplicable. |
| `ModifiedAt` | `DATETIME2(0)` | Ultima actualizacion del registro. |

## Tipos de venta fuera de horario

| Valor | Tipo |
|---:|---|
| `1` | Antes de apertura |
| `2` | Horario de comida |
| `3` | Despues del cierre |
| `4` | Sin horario aplicable |

## Reglas de negocio

- El horario se selecciona por sucursal, dia de la semana y vigencia entre `OpeningDate` y `ClosingDate`.
- Una venta anterior a la apertura se clasifica con tipo `1`.
- Una venta dentro del intervalo de comida se clasifica con tipo `2` solo si existen ambas horas de comida.
- Una venta igual o posterior al cierre se clasifica con tipo `3`.
- Cuando no existe un horario aplicable se clasifica con tipo `4`.
- Las ventas dentro del horario no se conservan en esta tabla; si una venta previamente materializada deja de estar fuera de horario, el `MERGE` la elimina.

## Uso esperado

- Seguimiento de ventas realizadas fuera de los horarios configurados.
- Analisis por sucursal, usuario, tipo de desviacion e importe.
