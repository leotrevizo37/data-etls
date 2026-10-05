# Propuesta de infraestructura - Data&AISolutions (Proyecto DUMA)

La propuesta separa el procesamiento ETL, la ejecución de agentes y el servicio de datos. Se mantiene Dagster con Spark para las transformaciones y SQL como capa de consulta para aplicaciones, dashboards y agentes.

El dimensionamiento contempla un crecimiento de **15 veces el volumen actual**, con la VM del ETL definida en **8 vCPU y 32 GiB de RAM**. Los otros proyectos que actualmente comparten infraestructura quedan fuera de esta propuesta.

## Componentes

| Proyecto o finalidad | SKU o servicio de referencia | Dimensiones y capacidades |
|---|---|---|
| ETL — Dagster y Spark (data) | Azure D8ads_v5 | 8 vCPU, 32 GiB de RAM y 300 GiB de SSD temporal para procesamiento |
| Aplicación agéntica (agentes) | Azure F4s_v2 | VM independiente con 4 vCPU y 8 GiB de RAM |
| Aplicación web y sus servicios (web) | Azure D4as_v5 | VM con 4 vCPU y 16 GiB de RAM.  |
| Registro de imágenes de contenedor (web, agentes, data) | Azure Container Registry — Standard | Imágenes de producción. La compilación ocurre en CI; la VM solo ejecuta imágenes ya construidas |
| Secretos y material criptográfico (web) | Azure Key Vault — Standard | Clave de firma JWT, keyring de sesión del BFF, clave privada RS256 interna, clave de cifrado de fuentes por tenant y credenciales de las bases de los clientes |
| Warehouse — servicio de consultas (data) | Azure SQL Database Elastic Pool Standard | Cuatro bases independientes. 50 eDTU compartidas entre todos los tenants |
| Históricos e intermedios (data, web, agentes) | Blob Storage / ADLS Gen2 | Capacidad ampliable. Retención indefinida de históricos y limpieza de intermedios según su uso |
| Base de datos de plataforma (web) | Azure SQL Database — Standard S1 | Base `platform`: identidad, tenants y catálogo de módulos. 20 DTU, 250 GB incluidos |
| Recuperación y respaldo VMs (data, web, agentes) | Azure Managed Disks | Persistencia de metadatos de Dagster, estado de aplicaciones y copias de PostgresSQL; separados del SSD temporal |
| Respaldo de largo plazo de la warehouse (data) | Azure SQL Database — LTR, RA-GZRS | Una copia mensual por base, conservada durante 12 meses. Referencia de costo: 100 GB acumulados entre todas las copias |

Para un poco de contexto, las cinco bases que actualmente se manteinen son de 3GiB, pero la de mcdonalds con data de octubre 2025 - junio 2026 terminó pesando 25GiB. El escenario de crecimiento corresponde a unos **60 GB**, antes de considerar espacio adicional para índices, staging y operación.

En general la infraestructura para discos en las VMs es sencilla y no tiende a ser lo más caro:

```
VM ETL — 8 vCPU / 32 GiB
│
├── P6 — 64 GiB — Disco del sistema operativo
│   └── Linux, Docker, imágenes y aplicación
│
├── P10 — 128 GiB — Montado en /data/etl
│   └── PostgreSQL de Dagster y estado persistente
│
└── SSD temporal — 300 GiB
    └── Archivos temporales de Spark y procesamiento


VM AGENTES — 4 vCPU / 8 GiB
│
├── P6 — 64 GiB — Disco del sistema operativo
│   └── Linux, Docker, imágenes y aplicación
│
└── P15 — 256 GiB — Montado en /data/agentes
    └── MSSQL de histórico y estado persistente de agentes y conversaciones
    
    
VM WEB — 4 vCPU / 16 GiB
│
└── P10 — 128 GiB — Disco del sistema operativo
    ├── Linux, Docker e imágenes de aplicación, Front y BFF
    ├── Data API
    └── Backends de observabilidad
```


## Alcance de capacidad

La capacidad prevista es trabajar con **15-20 veces los datos actuales**, conservando los tiempos de respuesta de las consultas y una frescura de **15–30 minutos** para los datos prioritarios.

Esto contempla el procesamiento incremental, las consultas de los agentes y el trabajo ordinario de respaldo y mantenimiento. La capacidad de cada componente podrá ampliarse por separado conforme aumente el uso.


