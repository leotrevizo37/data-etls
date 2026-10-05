# ADR 0008: Conservar evidencia MQTT parcialmente utilizable en agregados horarios

## Estado

Aceptado. Documenta las decisiones de la extension MQTT de observabilidad en
`carlsjr`, `emerson` y `valledelencino`; no modifica el comportamiento ejecutable.

## Contexto

Las fuentes actuales pueden devolver mensajes con campos opcionales ausentes,
malformados o invalidos junto con evidencia aprovechable. Descartar un evento
completo por una señal invalida pierde tambien sus recepciones, tiempos y otros
valores validos. Conservar todos los mensajes indefinidamente tiene un costo
incompatible con el proposito de esta SSOT horaria.

## Decision

Se conserva cada parte utilizable de los eventos que superan las validaciones
necesarias de identidad, estructura y alcance. La aceptacion parcial es
intencional: un campo invalido no convierte automaticamente en inutil todo el
mensaje. No se garantiza recuperar recepciones de un evento cuya estructura o
identidad invalida provoca su rechazo completo.

Los campos opcionales distinguen `valid`, `missing` e `invalid`. Ausencia y `null`
no se sustituyen por ceros ni por valores inferidos del protocolo. Un cero o
`false` explicito se acepta cuando cumple el contrato. RSSI y SNR se validan por
separado; una recepcion puede conservarse aunque ninguno tenga muestras validas.
Las reglas de formato y rango no certifican una configuracion radio regional
correcta ni establecen umbrales de buena o mala señal.

Se mantienen dos granularidades: dispositivo-hora y dispositivo-gateway-hora.
Las seis columnas JSON conservan perfiles y contextos agrupados, estadisticos de
recepcion, calidad por campo y extremos observados de contadores. Los contratos
exactos estan en el [modelo](../data_model/sense/chirpstack_observability.md).
No se agrega una fila de hecho por mensaje ni un arreglo con todos los mensajes.

Se almacenan sumas y conteos validos de RSSI/SNR, junto con minimos y maximos,
en lugar de promedios redundantes por grupo. El promedio se deriva dividiendo
la suma entre el conteo valido; entre horas se suman ambos componentes antes de
dividir. Esto evita el promedio no ponderado y el redondeo intermedio de promedios
persistidos. Con conteo cero el resultado es `NULL`. El promedio de RSSI en dBm
es aritmetico en esa escala, no un promedio de potencia lineal.

Los dos hechos MQTT comparten la aceptacion de snapshot. Los JSON se reemplazan
con el snapshot aceptado, sin concatenar ni acumular ejecuciones. Se protegen los
conteos previos y los conteos validos por campo, ademas de la señal previa frente
a su desaparicion. Bajo el nuevo contrato de calidad, un cero explicito puede
reemplazar la señal anterior; sin ese contrato se mantiene la rama legacy.
Estas comprobaciones no comparan cada grupo de recepcion ni prueban identidad o
completitud del conjunto de eventos.

El historico `raw.MqttEventHistoryRejectedRow` conserva los rechazos individuales
que produce el flujo actual. Los eventos parcialmente aceptados con campos
invalidos se contabilizan en calidad y no se incorporan automaticamente a ese
historico. Esta decision documental conserva ese comportamiento; no implementa
el registro adicional de incidencias discutido como alternativa. El historico
tiene retencion y limites de longitud, por lo que no sirve como archivo completo
para reconstruir todos los mensajes.

## Fundamento y decisiones de la revision

Se priorizó conservar la evidencia de esta SSOT frente a reemplazarla 
con respuestas posteriores potencialmente incompletas. Una respuesta 
mas reciente no demuestra mayor completitud. Validar su estructura,
aprovechar sus campos y autorizar su persistencia son decisiones distintas.
Las protecciones siguientes son deliberadas; no deben eliminarse unicamente
para aceptar mas filas o hacer pasar una prueba basada en otra semantica.

### Ceros, ausencia y recuperacion historica

Una respuesta vacia de MQTT significa que la consulta no devolvio eventos, no
que se haya demostrado que no ocurrieron. Del mismo modo, un bucket ChirpStack
con ceros puede representar ausencia real de trafico o falta de evidencia
recuperable. El valor aislado no permite distinguir ambas situaciones. Un
evento individual valido tampoco certifica la completitud de toda la ventana.

Se conserva el filtro de recuperacion ChirpStack que busca evidencia positiva
y omite las horas anteriores al primer indicio encontrado. Si no encuentra
evidencia positiva, puede no escribir filas y aun avanzar el watermark al
completar la ejecucion. Es una politica conservadora, no una certificacion de
completitud historica; puede omitir periodos de inactividad real. Tampoco prueba
que todos los buckets posteriores al primer indicio sean completos.

La observacion inicial que exigia guardar todos los buckets en cero fue
reconsiderada: la prueba demostraba su descarte, pero suponia sin demostrar que
esos ceros eran confiables. Se decidio mantener la proteccion. Esto no contradice
aceptar un cero explicito de un campo opcional MQTT bajo el contrato de calidad
descrito arriba: disponibilidad historica y validez de un campo son propiedades
distintas. No se flexibilizan los contratos de entrada solo porque rechacen
`null`; estos fueron revisados por el responsable contra documentacion y ejemplos
practicos. La aceptacion parcial no autoriza identidades o estructuras invalidas.

### Defectos corregidos sin retirar las protecciones

- **Identidad UUID:** se normaliza `deduplicationId` tanto en la clave de
  deduplicacion como en el contenido utilizado para comparar duplicados, sin
  modificar el mensaje original. Mayusculas y minusculas del mismo UUID no
  generan otro uplink ni un falso conflicto. Un contenido realmente diferente
  para la misma identidad sigue siendo un conflicto.
- **Coherencia entre hechos:** antes cada MERGE podia aceptar una captura
  diferente. Con tres uplinks persistidos y una recepcion de gateway, un replay
  de dos uplinks podia conservar el total de tres pero reemplazar 33.3333% por
  50%. El defecto era mezclar denominadores, no rechazar un contador decreciente.
  Ahora se calcula un conjunto comun de snapshots aceptados por corrida,
  dispositivo y hora antes de modificar ambos hechos, dentro de la misma
  transaccion. Rechazar una combinacion no rechaza automaticamente otras horas
  o dispositivos. Se conservan las protecciones y el enriquecimiento permitido
  por el contrato; una transaccion por si sola no sustituye esta decision comun.

La aceptacion conjunta evita nuevas incoherencias por decisiones independientes;
no constituye una reparacion automatica de inconsistencias historicas ni una
prueba de que el conjunto recibido contiene todos los eventos anteriores.


## Artefactos involucrados

Las rutas Python siguientes son relativas a
`dagster/<user_code>/src/<user_code>/defs` en los tres tenants indicados.

| Artefacto | Responsabilidad |
|---|---|
| `resources/mqtt_event_history.py` | Extraccion, normalizacion y rechazos del contrato de entrada |
| `transformations/chirpstack_metrics.py` | Deduplicacion, validacion por campo, agregacion y rechazos de transformacion |
| `assets/observability_chirpstack_metrics.py` | Esquemas, persistencia de rechazos y coordinacion de materializacion |
| `resources/spark.py` | Escritura de columnas a staging por corrida |
| `resources/sql_server.py` | Historico de rechazos, aceptacion conjunta de snapshots y MERGE |
| `sql/init/035_observability_factChirpStackDeviceEventMetrics.sql` | Contrato SQL de dispositivo-hora y staging |
| `sql/init/036_observability_factChirpStackDeviceGatewayMetrics.sql` | Contrato SQL de dispositivo-gateway-hora y staging |
| `sql/init/037_raw_MqttEventHistoryRejectedRow.sql` | Historico acotado de rechazos |

Los DDL de los dos hechos incluyen el contrato completo al crear las tablas y
las adiciones condicionales para tablas existentes. Se conservan las
implementaciones por tenant sin introducir una nueva abstraccion compartida.

## Consecuencias

- Se preserva evidencia parcial sin introducir valores invalidos en sus
  estadisticos. Esto puede cambiar columnas originales y crear grupos antes
  descartados; se conserva la granularidad, no la igualdad de todos los valores.
- Los JSON de calidad tienen estructura acotada; las distribuciones crecen con
  las combinaciones observadas y no tienen tamaño constante garantizado.
- No se conservan `data`, `location`, el mensaje completo, la secuencia individual
  ni las asociaciones entre arreglos distintos en los hechos horarios.
- Los extremos de `fCnt` no prueban sesiones, reinicios, retransmisiones o perdida
  de paquetes; los empates temporales conservan valores distintos sin inventar orden.
- La calidad de los campos y la completitud de captura son propiedades distintas.
  Los contadores de calidad no sustituyen la evidencia individual de rechazos.
- Se descartan como alternativas para este contrato el rechazo total por cualquier
  campo opcional invalido y el almacenamiento indefinido de todos los mensajes.
- La suscripcion directa a topicos y una infraestructura como Kafka quedan fuera
  de esta decision y no son requisitos de la implementacion actual.

## Validacion y limites de evidencia

La revision comparo HEAD y worktree con 30 escenarios sinteticos de transformacion
repartidos entre los tres tenants. Los casos sin cambios de aceptacion conservaron
las columnas originales; los casos con RSSI/SNR invalidos mostraron cambios en
conteos, señal o filas, coherentes con el procesamiento parcial. Esta evidencia
no equivale a validar materializacion completa ni concurrencia en SQL Server.
Este ADR no acredita un despliegue ni agrega pruebas ejecutables.
