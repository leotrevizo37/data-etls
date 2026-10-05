# Contratos de auditoría y observabilidad

## Alcance

Los pipelines están implementados en Gabinete, Carlsjr, Emerson y Valle. Cada base tenant mantiene sus datos. Los DDL globales no demuestran que una tabla esté materializada. La extracción y programación están en los [pipelines](../pipelines/observability.md).

## Auditoría de lecturas

`observability.factRedingsAudits` tiene llave `(SensorId, TimeSpan)`, con inicio de hora UTC y su conversión local. `stg.factRedingsAudits` agrega `RunId`. El [DDL](../../sql/init/014_observability_factRedingsAudits.sql) define:

| Campos | Semántica |
|---|---|
| `ReadingsCount` | Lecturas recibidas por sensor y hora |
| `AverageGapMinutes`, `MaximumGapMinutes`, `MinimumGapMinutes` | Estadísticos de separación entre lecturas; nullable sin muestras |
| `ReadingsCoveragePercentage`, `CoveredReadingBlocks` | Cobertura temporal en bloques de diez minutos |
| `HasLateReadings`, `IsConnectionLost` | Indicadores de auditoría con umbrales propios |
| `LastReadingAt`, `ConnectionLostAt`, `MinutesWithoutReadings` | Evidencia de comunicación, nullable cuando no puede determinarse |
| `ModifiedAt` | Auditoría UTC de escritura |

Conteo cero no demuestra falla física. La auditoría sirve como entrada de alertamiento y de diagnóstico ChirpStack, pero sus flags no sustituyen las reglas activas del detector. Las [vistas de cobertura](sense/serving_views.md) agregan esta evidencia por periodos.

## Usuarios SIDON

`observability.dimAspNetUsers` conserva una fila por `Id UNIQUEIDENTIFIER`, con `UserName NVARCHAR(256)`, `Name` y `LastName NVARCHAR(50)`, `Position NVARCHAR(100)` y fechas UTC `CreatedAt`/`ModifiedAt DATETIME2(0)`. Los campos descriptivos son nullable. Staging contiene `RunId` y los atributos recibidos, sin fechas de warehouse. El [DDL](../../sql/init/015_observability_dimAspNetUsers.sql) define la llave y el índice por corrida.

Los nombres y cuentas son datos personales y requieren accesos autorizados. El contrato no representa un evento de login ni prueba autorización de un usuario.

## Uso SIDON

`observability.factSidonUserUsage` tiene llave `([Date], UserId)`, por día UTC y usuario. Su [DDL](../../sql/init/016_observability_factSidonUserUsage.sql) y [transformación](../../dagster/carlsjr/src/carlsjr/defs/transformations/sidon_user_usage.py) definen:

| Columna | Interpretación |
|---|---|
| `HasConnected` | Existe al menos una solicitud con `StatusCode` entre 200 y 399 |
| `MadeCompleteInteraction` | Al menos dos módulos funcionales distintos y más de diez segundos inferidos |
| `TimeConnected` | Segundos inferidos sumando gaps entre eventos humanos de 0 a 1800 segundos |
| `AvgLatency` | Promedio de `ExecutionTimeMs` válidos de eventos humanos, convertido a entero; cero sin muestras |
| `Latency95thPercentile` | Percentil 95 en milisegundos, `DECIMAL(18,2) NULL` |
| `ModifiedAt` | Momento UTC de escritura |

Los eventos humanos excluyen los módulos técnicos y los nombres con forma GUID definidos en la transformación. `login` no cuenta como módulo funcional. `TimeConnected` no mide una sesión real ni tiempo total de presencia; el percentil nulo distingue falta de muestras del promedio con fallback cero. No se suman promedios ni percentiles para obtener una latencia global.

## Disponibilidad de URLs

`observability.factUrlAvailabilityDaily` tiene llave por fecha UTC y URL, con `UptimePercentage DECIMAL(7,4)`, latencias en segundos `DECIMAL(18,6)`, `IsUp`, `TimeoutsPresent` y `ModifiedAt`. El [DDL](../../sql/init/022_observability_factUrlAvailabilityDaily.sql) define los tipos exactos.

`UptimePercentage` es la proporción de muestras exitosas, no una medida continua de tiempo disponible. El percentil 95 usa la posición superior de las duraciones ordenadas. `IsUp` corresponde a la última muestra de éxito del día; no garantiza disponibilidad en el instante de consulta. `TimeoutsPresent` requiere una muestra fallida y duración del mismo timestamp que alcance el umbral configurado.

El día actual es parcial y las estadísticas dependen del step y de la retención de Prometheus. La ausencia de muestras requeridas falla el pipeline; no se rellena como cero disponibilidad. El contrato no acredita SLA contractual.
