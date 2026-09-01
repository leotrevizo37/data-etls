# ADR 0006: Crear una code location dedicada para `alerting`

## Estado

Aceptado.

## Contexto

El alertamiento consulta señales de varios tenants, corridas de Dagster y materializaciones de assets. También 
necesita conservar el ciclo de vida de cada incidencia y enviar notificaciones sin ejecutar pipelines de datos.

Incluir esta responsabilidad dentro de una code location de tenant asignaria el monitoreo global a un cliente 
especifico, acoplaria su disponibilidad y despliegue con pipelines que no le pertenecen y dificultaria 
mantener una sola politica de alertas para todos los `user_codes` monitoreados.

## Decision

Crear `dagster/alerting` como una code location gRPC independiente, registrada como `alerting`, 
con puerto interno `4009` y puerto publicado mediante `ALERTING_USER_CODE_PORT`.

Su objetivo es centralizar la deteccion y entrega de alertas de sensores, ingestiones, corridas 
fallidas de Dagster y tendencias de volumen, manteniendo esta responsabilidad separada de la ejecucion
de pipelines de datos.

La code location usa el mismo `dagster/Dockerfile` que los demas servicios, inicia con 
`dagster code-server start`, consulta SQL Server en modo de solo lectura y almacena el 
estado de incidencias en el esquema `alerting` del PostgreSQL de Dagster.

## Consecuencias

- El alertamiento multi-tenant tiene una sola implementacion y una sola configuracion de reglas.
- Los pipelines de cada tenant no incorporan sensores, recursos SMTP ni estado de entrega que no forman parte de su responsabilidad.
- El ciclo de despliegue, healthcheck y recarga de definiciones de alerting queda aislado del codigo de los tenants.
- Las incidencias y tendencias se conservan en PostgreSQL de Dagster sin crear tablas de alertamiento en SQL Server.
- Se agrega un servicio Compose, un puerto gRPC, un registro en `workspace.yaml` y una dependencia de salud para `dagster-daemon`.
- El servicio requiere acceso de solo lectura a las bases monitoreadas, acceso a PostgreSQL de Dagster y configuracion SMTP.
- Si la code location no esta disponible, los pipelines de datos permanecen separados, pero sus sensores de alertamiento no pueden evaluarse hasta que el servicio se recupere.
- Incorporar un tenant nuevo al monitoreo requiere agregar su configuracion y cadencias en `alert_config.yaml`.
