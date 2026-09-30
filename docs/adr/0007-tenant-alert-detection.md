# ADR 0007: Detectar señales de alerta en cada tenant

## Estado

Aceptado. Sustituye la responsabilidad de alertamiento central definida por el
ADR 0006.

## Contexto

El diseño anterior concentraba detección, ciclo de vida y entrega en una code
location central. Esas responsabilidades tienen ritmos, datos y propietarios
diferentes. Además, cada tenant necesita reglas y overrides por entidad que se
evalúen contra su propia base SQL Server.

Dagster puede comprobar el inicio o fin de una condición, pero no debe ser la
fuente de verdad del reconocimiento, resolución administrativa, correo,
tickets ni reintentos.

## Decisión

Cada code location de `bafar`, `bafar_poc_gabinete`, `carlsjr`, `emerson` y
`valledelencino` tendrá copias independientes de
`operational_alerts_sensor` y `asset_row_count_trend_sensor`. Ambos usan el recurso
SQL Server local. El sensor operativo descubre pendientes y dispara
`operational_alerts_job`; el asset `operational_alerts` evalúa una entidad por run
y escribe únicamente incidencias y señales append-only `DETECTED` o `CLEARED`.
El cursor confirma la entidad al terminar el run correctamente y conserva los
fallos para reintentar respetando su intervalo. Los estados iniciales de los
sensores y el procesamiento del sensor de tendencias se conservan.

Cada base tenant almacena catálogos con GUID estables, versiones inmutables de
configuración, reglas default y overrides exactos, incidencias y bitácoras de
señales, workflow y acciones. El sistema externo de alertamiento será el único
escritor de las bitácoras de workflow y acciones.

`serving.vwEstadoAlertas` consolida una fila por incidencia para reporteo, con
nombre y columnas en español PascalCase. La configuración y regla asociadas a
una incidencia son las vigentes al detectarla y no cambian retroactivamente.

La code location `alerting` conserva ambos sensores registrados únicamente por
compatibilidad operativa. Permanecen `STOPPED`, devuelven siempre `SkipReason`,
no solicitan recursos y no conservan rutas de escritura o entrega.

## Consecuencias

- La detección queda aislada por tenant y no requiere lectura multitenant.
- El ciclo de vida y las acciones pueden evolucionar fuera de Dagster sin
  alterar el contrato de señales.
- Las implementaciones de los cinco tenants permanecen duplicadas de forma
  deliberada para conservar su independencia.
- La activación de sensores es manual después de validar DDL y configuración.
- La vista de serving puede reportar workflow y acciones cuando el sistema
  externo los escriba, sin inventar valores mientras no existan.
- El servicio central conserva compatibilidad de registro, pero deja de
  detectar, persistir estado o enviar notificaciones.
