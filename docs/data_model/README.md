# Modelo de datos

Este apartado documenta los contratos de consumo del DWH y las tablas de control que sostienen los pipelines.

## Familias

| Familia | Carpeta | Estado                                                                                |
|---|---|---------------------------------------------------------------------------------------|
| Sense | `sense/` | Implementada en Gabinete, Carlsjr, Emerson y Valle, con diferencias por cliente |
| SmartAudits | `smartaudits/` | Implementada y desplegada en `carlsjr`                                                |
| Ventas fuera de horario | `out_of_hours_sales/` | Implementada en `bafar` para ventas fuera de los horarios de sucursales               |
| Acceso de gerentes | `manager_access/` | Implementada en `bafar` como snapshots historicas diarias                             |
| Dimensiones compartidas | `shared_dimensions.md` | Compartida por los pipelines Sense                                                    |
| Mantiz | `mantiz/` | Desactivado; documentación existente conservada como referencia |
| Alertamiento | [alerting.md](alerting.md), [alerting_email.md](alerting_email.md) | Detección por tenant y contratos del consumidor externo |
| Observabilidad | [observability.md](observability.md) | Auditoría de lecturas, usuarios, uso SIDON y URLs |
| Ventas diarias | [fact_store_sales_daily.md](out_of_hours_sales/fact_store_sales_daily.md) | Agregados de Bafar por sucursal y fecha |
| Vistas Sense | [serving_views.md](sense/serving_views.md) | Contratos de sensores, lecturas y salud |
