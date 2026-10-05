# Vistas de consumo Sense

## Catálogo y granularidad

| Vista | Fuente | Granularidad y columnas |
|---|---|---|
| `serving.vwSenseSensors` | `dwh.dimSidonProdDimensions` | Sensor activo; identidad, identificador físico y nombres de sensor, modelo, hub, dispositivo y ubicación |
| `serving.vwSenseReadingsHourly` | `dwh.factReadingsMeasurement` | Fila horaria de medición; `SensorId`, `TimeSpan`, `LocalTimeSpan`, `AnomaliesSum`, `AverageValue` |
| `serving.vwSenseReadingsDaily` | Vista indexada diaria de mediciones | Sensor-día local; `LocalDate`, anomalías y promedio |
| `serving.vwSenseReadingsWeekly` | Vista indexada semanal de mediciones | Sensor-semana local; inicio lunes, fin domingo, anomalías y promedio |
| `serving.vwSenseSensorCoverageHourly` | `observability.factRedingsAudits` | Sensor-hora UTC y local; conteo, gap promedio y cobertura |
| `serving.vwSenseSensorCoverageDaily` | Vista indexada diaria de auditorías | Sensor-día local; conteo, gap promedio y cobertura |
| `serving.vwSenseSensorCoverageWeekly` | Vista indexada semanal de auditorías | Sensor-semana local; inicio, fin, conteo, gap promedio y cobertura |
| `observability.vwSenseDevEuiUplinkReadingsDaily` | Vista horaria de comunicación y dimensiones | DevEUI-día local; `SensorIdCount`, `ReceivedUplinkCount`, `ReadingsCount` |

## Cálculo y límites

Las vistas diaria y semanal de mediciones dividen la suma de `MeasurementValue` entre el conteo de valores no nulos. No ponderan por `ReadingsCount`; el resultado es un promedio de mediciones, no de lecturas individuales. `NULLIF` conserva `NULL` si no hay valores. La semana se ancla en lunes y utiliza tiempo local.

La cobertura agregada promedia porcentajes sobre las filas disponibles; el gap promedio utiliza el conteo de gaps no nulos. Las vistas no fabrican horas ausentes ni certifican cobertura completa del periodo. Las vistas indexadas de agregación utilizan `NOEXPAND` en sus wrappers de consumo.

La comparación diaria DevEUI agrupa primero por dispositivo y hora, toma el máximo uplink entre los sensores de esa hora y suma lecturas. Evita sumar el mismo uplink por cada sensor. `SensorIdCount` proviene de dimensiones y no equivale a sensores que comunicaron ese día. Lecturas y uplinks no tienen una relación obligatoria uno a uno.

Los contratos anteriores `serving.vwSenseSensorHealthDaily` y `Weekly` se eliminan en el DDL de cobertura; los nombres vigentes son `vwSenseSensorCoverageDaily` y `Weekly`. El nombre del archivo SQL conserva la referencia histórica.

Las vistas JSON por dispositivo y grupo se describen en [dispositivo](vw_fact_readings_measurement_device.md) y [grupos Carlsjr](vw_fact_readings_measurement_device_groups.md). Su agrupación por hora local no distingue por sí sola horas repetidas de DST. Salud de gateways y comunicación DATA-ChirpStack se documentan en el [modelo ChirpStack](chirpstack_observability.md), separado de la cobertura de lecturas.

## Fuentes del contrato

- [Sensores](../../../sql/init/017_serving_vwSenseSensors.sql).
- [Lecturas horarias](../../../sql/init/018_serving_vwSenseReadingsHourly.sql).
- [Lecturas diarias y semanales](../../../sql/init/019_serving_vwSenseReadingsDaily.sql).
- [Cobertura](../../../sql/init/023_observability_serving_vwSenseSensorHealth.sql).
- [Comparación diaria DevEUI](../../../sql/init/034_observability_vwSenseDevEuiUplinkReadingsDaily.sql).

La definición de una vista en el repositorio no demuestra que esté instalada ni que sus fuentes contengan datos recientes en cada tenant.
