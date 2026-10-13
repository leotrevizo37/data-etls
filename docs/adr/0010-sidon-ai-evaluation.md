# ADR 0010: Evaluación de configuración SIDON mediante jobs por entidad

## Estado

Implementado en Carlsjr, Emerson y Valle. Documenta la arquitectura existente; no acredita precisión del modelo ni habilita llamadas externas.

## Contexto

Las referencias y rangos pueden validarse estructuralmente, pero la congruencia entre nombres, tipos, sensores y función física requiere una evaluación semántica. Consultar IA dentro del tick acoplaría su duración al sensor.

## Decisión implementada

`sidon_configuration_sensor` consulta marcas de cambios, reglas y estado de ejecución, y solicita como máximo un job por entidad en cada tick. El job lee la evidencia completa, aplica validaciones estructurales, consulta IA cuando corresponde y persiste señales en el SQL Server del tenant. El cursor conserva entidades pendientes hasta confirmar un run exitoso.

El proveedor puede ser Azure OpenAI o un servidor local compatible. Local no utiliza Azure como fallback. Antes de escribir se comprueban nuevamente evidencia, regla, configuración y política; no se mantiene una transacción SQL abierta durante la llamada a IA.

## Datos y límites

El payload incluye nombres, tipos, unidades, operadores, umbrales, rangos con sus horarios y los identificadores de entidad descritos en el [pipeline](../pipelines/sidon_configuration_signals.md). No incluye ubicaciones, identificadores físicos, fórmulas, tópicos ni datos de notificación. Los nombres se tratan como datos no confiables, no instrucciones.

Una respuesta JSON válida no demuestra certeza semántica. La confianza declarada no es precisión medida. Se conservan los límites de emisión y recuperación del pipeline; una eliminación, cambio de modelo o ausencia de rangos no confirma por sí sola recuperación.

La elección de proveedor no autoriza transmitir contenido académico confidencial. Una ejecución con proveedor externo requiere autorización explícita del responsable; la actualización documental no realiza ninguna llamada.

## Consecuencias

- Los errores técnicos fallan el job y mantienen la entidad pendiente para reintento administrado por el sensor.
- Las evaluaciones inconclusas quedan registradas sin inventar señales ni modificar automáticamente la configuración fuente.
- Dagster conserva detección y evidencia; workflow y entrega siguen siendo responsabilidad del sistema externo.
- La validación semántica requiere casos etiquetados del dominio, además de las pruebas sintéticas de contrato.

Fuentes: [sensor](../../dagster/carlsjr/src/carlsjr/defs/sensors/sidon_configuration_sensor.py), [job](../../dagster/carlsjr/src/carlsjr/defs/jobs/sidon_configuration_job.py) y [recurso IA](../../dagster/carlsjr/src/carlsjr/defs/resources/sidon_configuration_ai.py), con copias tenant en Emerson, Valle y Ecosat.
