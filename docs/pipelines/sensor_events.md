# Pipeline `021_sense_sensor_events`

## Proposito

Materializar eventos de sensores en `dwh.factSensorEvents`.

## Entradas

- Lecturas extendidas por sensor.
- `dwh.dimSidonProdDimensions`.
- `dwh.dimSensorExpectedVal` para reglas de temperatura.
- Watermark `FactSensorEvents`.

## Salida

- `dwh.factSensorEvents`.

## Flujo

1. Lee watermark y eventos abiertos para definir la ventana.
2. Para temperatura, aplica un lookback adicional.
3. Limpia eventos del rango a reprocesar.
4. Lee lecturas extendidas.
5. Ejecuta transformaciones de presencia y temperatura.
6. Escribe eventos a `dwh.factSensorEvents`.
7. Actualiza el watermark.

## Reglas

- Presencia detecta cambios de estado `0/1`.
- Temperatura compara contra valores esperados por horario y dia.
- Temperatura solo materializa eventos fuera de rango de al menos 120 minutos.
- Los eventos abiertos se reabren en la siguiente corrida para calcular continuidad.

