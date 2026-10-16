# Pipeline `020_sense_operations`

## Proposito

Construir la tabla operacional horaria `dwh.factReadingsMeasurement` desde lecturas extendidas y dimensiones.

## Entradas

- Lecturas del data lake para la ventana UTC de operaciones basada en `TimeSpan`.
- `dwh.dimSidonProdDimensions`, incluyendo `TimeZone` por sensor.
- Watermark `factReadingsMeasurement` en `ctl.IngestionControl`.

## Salida

- `dwh.factReadingsMeasurement`.
- Metadatos de corrida en `ctl.IngestionControl`.

## Flujo

1. Resuelve la ventana y el watermark en UTC.
2. Lee dimensiones activas.
3. Lee lecturas por `TimeSpan` y las cruza con dimensiones.
4. Genera el scaffold, agrega y cruza las horas usando `TimeSpan`.
5. Limita filas a horas UTC cerradas y al ultimo `TimeSpan` con lecturas reales.
6. Convierte `TimeSpan` a `LocalTimeSpan` con el `TimeZone` del sensor.
7. Escribe a `stg.factReadingsMeasurement` con `RunId`.
8. Ejecuta `MERGE` hacia `dwh.factReadingsMeasurement` por `OperationId`, calculado con `SensorId` y `TimeSpan` UTC.
9. Limpia staging y actualiza watermark.

## Reglas de avance

- Si no hay lecturas reales, conserva el watermark.
- Si la corrida falla, conserva el watermark previo.
- Si hay datos reales, avanza al maximo `TimeSpan` procesado.

## Compatibilidad entre `user_codes`

El flujo existe en `ecosat`, `carlsjr`, `emerson` y `valledelencino`, con transformaciones específicas por cliente. McDonalds, CDP y Smartfit fueron retirados según [ADR 0005](../adr/0005-remove-code-locations.md). El DDL agrega `TimeSpan` nullable al histórico existente y lo exige en tablas nuevas; no reconstruye la identidad ni el tiempo del histórico. Véase [ADR 0009](../adr/0009-utc-fact-identities.md).

Los sensores binarios de presencia, estado de puerta y control de luces quedan fuera de las agregaciones y del scaffold de este asset. En `valledelencino`, `cava` y `puerta` conservan sus canales de temperatura y humedad. La exclusion se realiza por tipo de sensor, no por observar valores 0 y 1 en una medicion continua.

## Tipos soportados en `ecosat`

Cada `device_type` tiene su archivo en `dagster/ecosat/src/ecosat/defs/transformations`. Dentro de cada transformación se seleccionan sus `sensor_types`; `TransformationOps` los expone y `build_devices_df_op` une los resultados. La selección por tipo conserva las etiquetas originales y admite espacios al inicio o al final de los tipos nuevos.

| `device_type` | Archivo | `sensor_types` | Canales del catálogo proporcionado |
| --- | --- | --- | ---: |
| `Gabinete` | `gabinete.py` | `Temperatura`, `Humedad` | 2 |
| `CISTERNAS` | `cisternas.py` | `EM500-SWL-Submersible Water Level Sensor` | 2 |
| `Sensor fase` | `sensor_fase.py` | `CORRIENTE ELECTRICA` | 3 |
| `TEMPERATURA Y HUMEDAD` | `temperatura_humedad.py` | `Temperatura` | 1 |
| `ACOMETIDA` | `acometida.py` | `CORRIENTE ELECTRICA` | 3 |
| `Weather Station` | `weather_station.py` | `Velocidad del viento`, `Lluvia acumulada`, `Dirección del viento`, `Temperatura`, `WTS506 - IoT Weather Station`, `Humedad`, `Porcentaje de Bateria` | 7 |

El catálogo soportado incluye 18 sensores continuos. El procesamiento se selecciona por tipo, usando las identidades de las dimensiones activas; los GUID de ese catálogo no están codificados en las transformaciones. Cada scaffold se limita a los tipos de dispositivo y sensor de su archivo, para que cada sensor y hora UTC produzca una sola fila al unir resultados.

Las cinco transformaciones nuevas consideran válidas todas las lecturas: `Anomalies = 0`, `HasAnomalies = false`, `AnomalousReadingsJson = []` y las lecturas se conservan en `CorrectReadingsJson`. Se mantienen la media aritmética horaria en `MeasurementValue`, desviación estándar muestral, mínimos, máximos y conteo del contrato existente. Los valores conservan la escala y unidad de origen; la lluvia acumulada y la dirección del viento también usan esta media aritmética.

Gabinete conserva sus límites existentes de temperatura y humedad. Las horas sin lecturas tienen estadísticas nulas, conteo cero y arrays vacíos. Se conservan la identidad UTC, conversión por `TimeZone`, staging, MERGE y reglas de avance del watermark.
