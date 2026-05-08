# DUMA - Serving Layer ETLs
Serving layer de procesamiento y preparación de datos destinados al consumo en Power BI y DUMA para Sidon,
desarrollado para MCDONALDS, MCDONALDS - CENTROS DE POSTRES Y SMARTFIT, integrando extracción, transformación
y carga automatizada de información y manejo de errores.

[![docker-image.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml)
[![ci-python.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml)
[![validate-compose.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml)
[![lint-sql.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml)

# BI serving layer (DUMA)

Se busca agilizar el comportamiento y respuestas de herramientas agénticas mediante la preparacion de 
una caché rápida y constantemente disponible con respecto a consultas lentas a un datalake que encima
presentarían el problema del small file size. Se soluciona con:
- SQL Server (esquema T-SQL + stored procedures)
- Dagster (assets/jobs, scheduling/ejecución, orquestación)
- Spark (master/workers)

## Alcance

Este repositorio provee:
- Un webserver + daemon de Dagster corriendo en contenedores.
- Un master de Spark corriendo en contenedores.
- Automatización/Orquestación de un clúster Spark multinodo.
- Integración con el data lake ADLS.
- Transformación de parquets del data lake a una tabla relacion (capa de servicio).
- Preparación/transformaciones de datos que alimentan las herramientas agénticas.
- 
No objetivos:
- ETLs complejos, no necesarias para las herramientas agénticas.

## Estructura del repositorio

- `compose-mcdonalds/`
  - `docker-compose.yml`: orquestación local
  - `.env` / `.env.example`: variables de entorno
- `dagster/`
  - `Dockerfile`: imagen de Dagster
  - `requirements.txt`: dependencias de Python
  - `mcdonalds/`: proyecto de Dagster (assets/jobs/resources)
     - `defs/`: definición del proyecto de Dagster
     - `tests`: tests unitarios
  - `mcdonalds_cdp/`: proyecto de Dagster (assets/jobs/resources)
     - `defs/`: definición del proyecto de Dagster
     - `tests`: tests unitarios
  - `smartfit/`: proyecto de Dagster (assets/jobs/resources)
     - `defs/`: definición del proyecto de Dagster
     - `tests`: tests unitarios
  - `dagster_home/`: configuración/estado de Dagster
- `sql/`
  - `init/`: scripts TSQL ejecutados una vez directamente en la base de datos antes de arrancar 
los servicios (definiciones de tablas, etc)
  - `procedures/`: scripts TSQL ejecutados una vez directamente en la base de datos antes de arrancar los servicios
- `workspace.yaml`: configuración del workspace de Dagster

## Prerrequisitos

- Docker Desktop con Docker compose v2 (mínimo)
- Configurar archivo `.env`
- Tener sinstaladas funcionalidades make (opcional)

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
### Para testing:
```bash
make test-python
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
.venv\Scripts\activate
```
* Linux:
```bash
source venv/bin/activate
```

### Correr el proyecto
```bash
uv run dagster dev
```
