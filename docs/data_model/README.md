# Modelo de datos

Este apartado documenta los contratos de consumo del DWH y las tablas de control que sostienen los pipelines.

## Familias

| Familia | Carpeta | Estado                                                                                |
|---|---|---------------------------------------------------------------------------------------|
| Sense | `sense/` | Compartida por los seis `user_codes`, con diferencias de transformaciones por cliente |
| SmartAudits | `smartaudits/` | Implementada y desplegada en `carlsjr`                                                |
| Ventas fuera de horario | `out_of_hours_sales/` | Implementada en `bafar` para ventas fuera de los horarios de sucursales               |
| Dimensiones compartidas | `shared_dimensions.md` | Compartida por los pipelines Sense                                                    |
