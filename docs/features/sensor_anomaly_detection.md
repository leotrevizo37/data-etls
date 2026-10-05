# Sensor Anomaly Detection

## Proposito

Detectar comportamientos anormales o eventos relevantes a partir de lecturas Sense.

## Contratos

| Objeto | Uso |
|---|---|
| `dwh.factReadingsMeasurement` | Metricas horarias por sensor/dispositivo |
| `dwh.factSensorEvents` | Eventos discretos con duracion |
| `dwh.dimSensorExpectedVal` | Rangos esperados por horario |

## Reglas existentes

- `ReadingsCount` mide si la hora tuvo lecturas reales.
- `MeasurementStdDev` y `Anomalies` permiten incorporar dispersion o conteos si la transformacion los produce.
- Eventos de presencia se detectan por cambios de estado.
- Los eventos se segmentan ante interrupciones de comunicación de al menos 60 minutos; su duración no presupone continuidad ilimitada sin lecturas. Presencia utiliza estados 0/1 y descarta timestamps con valores contradictorios.
- Eventos de temperatura se detectan por lecturas fuera de rango durante al menos 120 minutos.

## Salidas esperadas

- Minutos fuera de rango.
- Eventos abiertos.
- Eventos cerrados.
- Cobertura horaria por sensor.

