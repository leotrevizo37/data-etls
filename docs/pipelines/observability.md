# Pipelines de auditoría y observabilidad

## Alcance

Gabinete, Carlsjr, Emerson y Valle contienen los cuatro flujos siguientes con conexiones y prefijos independientes. Los defaults de sensores y schedules pueden variar por tenant; su existencia en el código no acredita ejecución. Los contratos de consumo están en el [modelo](../data_model/observability.md).

## Auditoria de lecturas

`022_observability_redings_audits` materializa `<user_code>/observability_factredingsaudits` desde parquets, dimensiones activas y estado previo de auditoría. No lee `dwh.factLecturas` como upstream.

La ventana UTC relee un solape mínimo de una hora (`FACTREDINGSAUDITS_OVERLAP_HOURS`), calcula conteos, cobertura en bloques de diez minutos, gaps y estado de comunicación, y escribe staging por `RunId`. El merge usa sensor y hora UTC. En éxito el control `factRedingsAudits` avanza al instante de corte; el fallo intenta limpiar staging y conservar el watermark previo. Admite tres reintentos con demora de 60 segundos.

Los umbrales de transformación son 15 minutos para lecturas tardías y 60 para pérdida de comunicación. No son los umbrales de reglas SQL de detección de alertas, que se configuran independientemente. Las dimensiones activas requieren `TimeZone` no vacío.

## Usuarios SIDON

`120_observability_dim_aspnetusers` materializa `observability_dimaspnetusers` desde `dbo.AspNetUsers` de la conexión de dimensiones. Selecciona identidad, cuenta, nombre, apellido y puesto, normaliza el DataFrame, escribe staging por `RunId` y consolida por `Id`. Limpia staging al finalizar; no utiliza un watermark de ingesta. El sensor dimensional solicita el job ante la lógica de cambios de su implementación.

## Uso SIDON

`121_observability_sidon_user_usage` materializa `observability_factsidonuserusage` desde usuarios y `dbo.SystemLogs`. Procesa días UTC cerrados desde el día del watermark `factSidonUserUsage`, con inicio alternativo `2026-06-05`.

Construye usuario-día para usuarios creados antes del cierre de ese día, obtiene interacción, tiempo inferido y latencias desde solicitudes exitosas, escribe staging y consolida por usuario y fecha. El éxito avanza hasta el cierre UTC; el fallo conserva el watermark previo. El schedule de Carlsjr declara ejecución diaria a las 01:30 en `America/Ciudad_Juarez`, inicialmente `STOPPED`.

## Disponibilidad de URLs

`122_observability_url_availability` materializa `observability_facturlavailabilitydaily` sin Spark. Consulta targets activos de Prometheus para `blackbox_probe` y el tenant, y las series `probe_success` y `probe_duration_seconds`, desde medianoche UTC del día anterior hasta el instante actual.

Valida las respuestas, agrega por URL y fecha UTC y ejecuta un upsert por cada fila. Un target configurado sin muestras de éxito, o un día sin muestras de duración, falla la corrida. El watermark `factUrlAvailabilityDaily` registra el éxito, pero no determina el rango consultado. Puede haber filas ya confirmadas si falla un upsert posterior; el reintento vuelve a aplicar las llaves.

El cron se toma de `PROMETHEUS_ENDPOINT_PROBE_CRON`, por defecto cada dos horas, con zona de ejecución `America/Ciudad_Juarez`. El día actual es parcial. Se utilizan `PROMETHEUS_API_URL`, timeout, step y umbral de timeout sin documentar valores privados.

## Fuentes

Las implementaciones de referencia están en los assets Carlsjr de [auditoría](../../dagster/carlsjr/src/carlsjr/defs/assets/observability_factRedingsAudits.py), [usuarios](../../dagster/carlsjr/src/carlsjr/defs/assets/observability_dimAspNetUsers.py), [uso](../../dagster/carlsjr/src/carlsjr/defs/assets/observability_factSidonUserUsage.py) y [URLs](../../dagster/carlsjr/src/carlsjr/defs/assets/observability_factUrlAvailabilityDaily.py), con equivalentes en Gabinete, Emerson y Valle. Esta documentación no ejecuta esos flujos ni demuestra conectividad o disponibilidad productiva.
