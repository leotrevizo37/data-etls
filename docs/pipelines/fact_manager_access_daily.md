# Pipeline `024_bafar_manager_access_daily`

## Proposito

Capturar diariamente una snapshot completa del ultimo acceso de los gerentes Bafar y conservarla en `dwh.factManagerAccessDaily` para consultas historicas.

## Entradas

- Endpoint versionado de ultimo acceso de gerentes.
- Parametro `fecha` con el dia local solicitado.
- Parametro `corteHora=23`.
- Parametro `diasHistorial=180`.
- Variables `BAFAR_MANAGER_ACCESS_API_URL`, `BAFAR_MANAGER_ACCESS_API_USER`, `BAFAR_MANAGER_ACCESS_API_PASSWORD` y `BAFAR_MANAGER_ACCESS_API_TIMEOUT_SECONDS`.
- Watermark `FactManagerAccessDaily` en `ctl.IngestionControl`.

## Salida

- Staging: `stg.factManagerAccessDaily`, aislado por `RunId`.
- Historico: `dwh.factManagerAccessDaily`.

## Programacion

- Asset: `bafar/dwh_factmanageraccessdaily`.
- Job: `024_bafar_manager_access_daily`.
- Schedule: `024_bafar_manager_access_daily_every_1d`.
- Frecuencia: diaria a las 21:40.
- Zona horaria: `America/Ciudad_Juarez`.
- Estado inicial: activo.

## Flujo

1. Obtiene el watermark `FactManagerAccessDaily` y calcula la fecha actual en `America/Ciudad_Juarez`.
2. Marca la corrida como iniciada en `ctl.IngestionControl`.
3. Solicita la respuesta completa de la API con `fecha`, `corteHora=23` y `diasHistorial=180` explicitos, sin enviar `soloSinIngreso` para recibir ambos grupos.
4. Valida campos, tipos, nulos, longitudes, fechas con hasta siete digitos fraccionarios, tres estados, duplicados, gerentes distintos, `totalRegistros`, totales y porcentaje. Una respuesta vacia es valida cuando sus agregados son consistentes.
5. Limpia las filas de staging asociadas al `RunId` actual.
6. Inserta la snapshot validada en `stg.factManagerAccessDaily`.
7. Ejecuta el `MERGE` hacia `dwh.factManagerAccessDaily` por `Fecha`, `IdUsuario` y `ClaveTienda`.
8. Limpia el staging y actualiza el watermark con `FechaCorte` solo si la corrida termina correctamente.

## Reintentos

El pipeline permite hasta tres reintentos con una espera de 60 segundos. Si no se completa, limpia el staging del `RunId`, conserva el watermark previo y registra el error en el control de ingestiones.

## Backfill diario

- Job: `025_bafar_manager_access_backfill_daily`.
- Particiones: dias desde `2026-01-01` en `America/Ciudad_Juarez`.
- Cada particion solicita una sola `fecha` y aplica la misma validacion estricta y el mismo `MERGE`.
- El backfill no lee ni actualiza el watermark operativo.
- El staging se limpia antes y despues de cada corrida, incluso si la corrida falla.
- El resultado historico depende de la respuesta que la API entregue para la fecha solicitada.
