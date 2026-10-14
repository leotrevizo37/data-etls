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
3. Limpia staging del `RunId` y obtiene eventos completados para preservar continuidad.
4. Lee lecturas extendidas.
5. Ejecuta transformaciones de presencia y temperatura; Ecosat agrega un DataFrame de eventos de luces mediante `light_events`.
6. Escribe eventos a `stg.factSensorEvents` y consolida con `MERGE` por `EventId` en Carlsjr, Emerson, Valle y Ecosat.
7. Actualiza el watermark.

## Reglas

- Presencia detecta cambios de estado `0/1`.
- Temperatura compara contra valores esperados por horario y dia.
- Temperatura solo materializa eventos fuera de rango de al menos 120 minutos.
- Ecosat conserva sus tipos de sensor: presencia por coincidencia con `presencia` y temperatura `Temperatura`.
- En Ecosat, `light_events.py` selecciona los tipos que contienen `control de luces` con `rlike` y la constante `r"(?iu)control de luces"`, conservando el tipo original del sensor. Aplica la misma logica de presencia a ambos estados `0/1`, con ventanas e historial de continuidad propios; el DataFrame se combina mediante `unionByName` antes de staging.
- Los eventos abiertos se reabren en la siguiente corrida para calcular continuidad.
- La identidad utiliza sensor e inicio UTC; `ReadId` conserva la lectura fuente y `LocalTimeSpan` se deriva con `TimeZone`.
- Los intervalos sin comunicación de al menos 60 minutos segmentan y cierran eventos. Para presencia se aceptan solo estados 0/1 y se excluyen timestamps con valores contradictorios.
- La consolidación elimina filas legacy sin `TimeSpan` que coincidan con sensor e inicio local del evento recibido. Un fallo conserva el watermark anterior; un éxito sin eventos avanza al cierre de ventana. El alcance se detalla en el [modelo](../data_model/sense/fact_sensor_events.md).
