# dimStoreSchedules

## Proposito

`dwh.dimStoreSchedules` conserva el ultimo horario valido recibido desde la API Bafar para clasificar ventas fuera de horario.

## Granularidad

Un registro representa el horario de una sucursal para un dia de la semana. La llave primaria es `(StoreId, DayNo)`.

## Columnas

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `StoreId` | `VARCHAR(4)` | Identificador de la sucursal. |
| `CustomerNo` | `VARCHAR(20)` | Numero de cliente informado por la API. |
| `StoreName` | `NVARCHAR(250)` | Nombre de la sucursal. |
| `OpeningDate` | `DATE` | Inicio de vigencia de la sucursal. |
| `ClosingDate` | `DATE` | Fin de vigencia de la sucursal. |
| `IsBlocked` | `BIT` | Indica si la sucursal esta bloqueada. |
| `BlockedFrom` | `DATE` | Inicio del bloqueo. |
| `BlockedTo` | `DATE` | Fin del bloqueo. |
| `ReceivingId` | `VARCHAR(20)` | Identificador de recepcion informado por la API. |
| `DayCode` | `CHAR(3)` | Codigo ISO del dia entre `MON` y `SUN`. |
| `DayNo` | `TINYINT` | Numero ISO del dia entre `1` y `7`. |
| `IsClosed` | `BIT` | Indica que la sucursal no opera ese dia. |
| `OpeningTime` | `TIME(0)` | Hora de apertura. |
| `LunchStartTime` | `TIME(0)` | Inicio del intervalo de comida cuando existen dos slots. |
| `LunchEndTime` | `TIME(0)` | Fin del intervalo de comida cuando existen dos slots. |
| `ClosingTime` | `TIME(0)` | Hora de cierre. |
| `IsDeleted` | `BIT` | Borrado logico derivado de `ClosingDate`. |
| `CreatedAt` | `DATETIME2(0)` | Momento UTC en que se creo la fila. |
| `ModifiedAt` | `DATETIME2(0)` | Momento UTC de la ultima modificacion real. |

## Reglas de persistencia

- Una fila nueva establece `CreatedAt` y `ModifiedAt` con la fecha UTC actual.
- Una fila existente solo se actualiza si cambia alguno de sus atributos de negocio.
- `ModifiedAt` no cambia cuando todos los valores recibidos coinciden con los almacenados.
- Las filas que no aparecen en una corrida no se eliminan ni se marcan automáticamente.
- `IsDeleted = 1` cuando `ClosingDate` es anterior a la fecha UTC actual; en caso contrario permanece en `0`.

## Capas relacionadas

| Tabla | Persistencia | Uso |
|---|---|---|
| `raw.StoreSchedulesApiPage` | Persistente | Respuesta JSON original por corrida, intento y pagina. |
| `stg.dimStoreSchedules` | Efimera | Filas normalizadas de la corrida antes del `MERGE`. |
| `dwh.dimStoreSchedules` | Persistente | Ultimo estado valido consumido por ventas fuera de horario. |

Los consumidores de ventas convierten `DayNo` ISO al numero de dia usado por Spark y excluyen los dias con `IsClosed = 1`.
