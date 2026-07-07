# DUMA - Serving Layer ETLs

Serving layer de procesamiento y preparacion de datos para consumo analitico 
en DUMA, BIs y herramientas agénticas. El repositorio integra extraccion, 
transformacion, carga, orquestacion y contratos SQL Server para seis `user_codes` 
Dagster casi gemelos, manteniendo separada la configuracion sensible.

[![docker-image.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-docker-image.yml)
[![ci-python.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-python.yml)
[![validate-compose.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/validate-compose.yml)
[![lint-sql.yml](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml/badge.svg)](https://github.com/leotrevizo37/mcdonalds-etl/actions/workflows/lint-sql.yml)
[![Deploy Production](https://github.com/leotrevizo37/data-etls/actions/workflows/deploy-production.yml/badge.svg?branch=main&event=release)](https://github.com/leotrevizo37/data-etls/actions/workflows/deploy-production.yml)

# BI serving layer (DUMA)

Se busca agilizar el comportamiento y las respuestas de herramientas agénticas mediante una 
capa de servicio mantenida y disponible para consultas que originalmente dependerian 
de data lake, fuentes transaccionales o procesos pesados de lectura y lentos de procesar. 
Se soluciona con:

- SQL Server (`dwh`, `stg`, `ctl`, vistas y stored procedures)
- Dagster (assets/jobs, schedules, daemon y webserver)
- Spark (master/workers y perfiles por tipo de job)
- ADLS y Ozone/S3A como fuentes o almacenamiento intermedio

## Alcance

Este repositorio provee:

- Un webserver de Dagster en modo lectura y un webserver administrativo local.
- Un daemon de Dagster y seis code locations gRPC, una por cada `user_code`.
- Un master de Spark y workers escalables en contenedores.
- Inicializacion SQL compartida y extensiones especificas por `user_code`.
- Ingestion incremental, transformacion y materializacion de contratos `dwh`.
- Tablas `stg` para cargas controladas y tablas `ctl` para watermarks, logs y colas operativas.
- Pipelines Sense, mantenimiento, eventos de sensores y flujos especializados donde aplica.
- Documentacion tecnica en `docs/` para arquitectura, modelos, pipelines, features y runbooks.

No objetivos:

- Sustituir los sistemas transaccionales de origen.
- Documentar valores reales de entorno, credenciales o secretos.

## Estructura del repositorio

- `compose-data_etls/`
  - `docker-compose.yml`: orquestacion principal de servicios.
  - `docker-compose.staging.yml`: variante para staging.
  - `.env.example`: plantilla de variables de entorno.
- `dagster/`
  - `Dockerfile`: imagen de Dagster y code locations.
  - `requirements.txt`: dependencias Python compartidas.
  - `<user_code>/`: proyecto Dagster con assets, jobs, schedules, resources y tests.
  - `dagster_home/`: configuracion y estado local de Dagster.
- `spark/`
  - `Dockerfile`: imagen Spark con conectores ADLS, SQL Server y S3A.
- `sql/`
  - `init/`: DDL compartido para esquemas `dwh`, `stg` y `ctl`.
  - `procedures/`: stored procedures operativos.
  - `user_codes/<user_code>/`: SQL especifico de cada `user_code`.
- `docs/`
  - Documentacion de arquitectura, modelo de datos, pipelines, features, runbooks y ADRs.
- `workspace.yaml`: workspace Dagster con las seis code locations.
- `Makefile`: comandos de build, ejecucion, lint y tests.

## Prerrequisitos

- Docker Desktop o Docker Engine con Docker Compose v2.
- Acceso a SQL Server y a las fuentes externas requeridas por cada `user_code`.
- Credenciales de ADLS/Ozone/S3A configuradas solo en archivos de entorno privados.
- `make` disponible si se quieren usar los targets del Makefile.
- `uv` y Python local solo para desarrollo fuera de Docker.

## Configuracion

Crea el archivo de entorno privado requerido por el Makefile a partir de `compose-data_etls/.env.example` y completa los valores reales en tu entorno local o servidor.

No se deben versionar, documentar ni copiar valores reales de archivos `.env`. Para revisar nombres de variables usa exclusivamente `compose-data_etls/.env.example`.

### Donde corre (entorno esperado)

Este servicio esta pensado para ejecutarse en:

- **Runtime**: Docker Compose sobre Docker Engine o Docker Desktop.
- **Dagster**: Python 3.10 en la imagen del proyecto.
- **Spark**: Spark standalone con master y workers.
- **Base operativa**: SQL Server para contratos `dwh`, staging `stg` y control `ctl`.

## Ejecutar el proyecto (Docker Compose)

Desde la raiz del repo:

### En caso de tener make:

```bash
make up
```

### De lo contrario:

```bash
docker compose --env-file compose-data_etls/.env.local.prod -f compose-data_etls/docker-compose.yml up -d --build
```

### Para mejor rendimiento si el host lo permite, se agregan workers

```bash
docker compose --env-file compose-data_etls/.env.local.prod -f compose-data_etls/docker-compose.yml up -d --scale spark-worker=4
```

### Para testing:

```bash
make lint-python
make test-python
```

## Para desarrollo local y pruebas

### Localizarse en la raiz del proyecto Dagster de un user_code

```bash
cd dagster/<user_code>
```

### Activar la venv default de Dagster

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
python -m venv .venv
```

```bash
uv sync
```

```bash
source .venv/bin/activate
```

### Correr el proyecto

```bash
uv run dagster dev
```
