# MCDONALDS - Serving Layer ETLs
Serving layer de procesamiento y preparación de datos destinados al consumo en Power BI para Sidon,
desarrollado para MCDONALDS, integrando extracción, transformación y carga automatizada.

[![docker-image.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml)
[![ci-python.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml)
[![validate-compose.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml)
[![lint-sql.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml)

# BI serving layer (Dagster + SQL Server)

Se soluciona el problema del small file size mediante una capa relacional en el servidor (10.80), en la base de datos 
correspondiente a mcdonalds.
- SQL Server (esquema T-SQL + stored procedures)
- Dagster (assets/jobs, scheduling/ejecución)

El proyecto arranca con un docker compose (más detalles de esto a continuación), y se consume en Power BI 
como cualquier SQL endpoint, en este caso MySQL, pues la base de datos correspondiente al proyecto está en MySQL.

## Alcance

Este repositorio provee:
- Un webserver + daemon de Dagster corriendo en contenedores.
- Un master de Spark corriendo en contenedores.
- Automatización/Orquestación de un clúster Spark multinodo.
- Integración con el data lake ADLS.
- Transformación de parquets del data lake a una tabla relacion (capa de servicio) para consumo de BI.

No objetivos:
- ETLs complejos, mas allá de las transformaciones correpondientes para cargar en tablas.

## Estructura del repositorio

- `compose-mcdonalds/`
  - `docker-compose.yml`: orquestación local
  - `.env` / `.env.example`: variables de entorno
- `dagster/`
  - `Dockerfile`: imagen de Dagster
  - `requirements.txt`: dependencias de Python
  - `mcdonalds/`: proyecto de Dagster (assets/jobs/resources)
  - `dagster_home/`: configuración/estado de Dagster
- `sql/`
  - `init/`: scripts TSQL ejecutados una vez directamente en la base de datos antes de arrancar 
los servicios (definiciones de tablas, etc)
  - `procedures/`: scripts TSQL ejecutados una vez directamente en la base de datos antes de arrancar los servicios
- `workspace.yaml`: configuración del workspace de Dagster

## Prerrequisitos

- Docker Desktop con Docker compose v2 (mínimo)
- Configurar archivo `.env`

## Configuración

Crea `compose-mcdonalds/.env` a partir de `compose-mcdonalds/.env.example`.

### Dónde corre (entorno esperado)
Este servicio está pensado para ejecutarse en:
- **Servidor/host**: `80`
- **Entorno**: ` production`
- **Runtime**: Docker Engine (server/daemon en Linux):** `29.1.2` / Python 3.11+ <3.14+ / etc.


## Ejecutar el proyecto (Docker Compose)

Desde la raíz del repo:

### En caso de tener make:
```bash
make up
```

### De lo contrario:
```bash
docker compose --env-file compose-mcdonalds/.env -f compose-mcdonalds/docker-compose.yml up -d --build
```
### Para mejor renidmiento si el host lo permite, se agregan workers
```bash
docker compose --env-file compose-mcdonalds/.env -f compose-mcdonalds/docker-compose.yml up -d --scale spark-worker=4
```
## Para desarrollo local y pruebas

### Localizarse en la raiz del proyecto dagster
```bash
cd dagster/mcdonalds
```

### Activar la venv default de dagster

* Windows: 
```bash
py -m venv .venv
```
```bash
uv sync
```
```bash
.venv\Scripts\activate.bat
```
* Linux:
```bash
source venv/bin/activate
```

### Correr el proyecto
```bash
uv run dagster dev
```
