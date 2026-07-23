# Ventas fuera de horario

## Proposito

Identificar ventas Bafar que no corresponden al horario configurado de su sucursal para facilitar su seguimiento operativo.

## Fuente

La feature consume `dwh.factOutOfHoursSales`, enriquecida con el catalogo `dwh.dimOutOfHoursTypes` y los horarios de `dwh.dimStoreSchedules`.

## Indicadores disponibles

| Indicador | Descripcion |
|---|---|
| Cantidad de ventas | Numero de ventas fuera de horario. |
| Importe | Suma del importe de las ventas identificadas. |
| Minutos de desviacion | Distancia en minutos respecto a apertura, comida o cierre. |
| Tipo de desviacion | Antes de apertura, horario de comida, despues del cierre o sin horario aplicable. |

## Dimensiones de analisis

- Sucursal.
- Fecha y hora local de la venta.
- Usuario.
- Tipo de venta fuera de horario.

## Reglas de consumo

- Usar `SaleId` como identificador unico de la venta.
- Considerar que el tipo `4` indica ausencia de horario aplicable, no una desviacion calculada.
- Para intervalos de comida, el limite inicial se usa para calcular `DeviationMinutes`.
