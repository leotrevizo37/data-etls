FROM python:3.10-slim-bookworm

ENV ACCEPT_EULA=Y \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/dagster/app/src

RUN mkdir -p "${DAGSTER_HOME}" /opt/dagster/app/mcdonalds /opt/dagster/app/smartfit /opt/dagster/app/mcdonalds_cdp /opt/dagster/app/bafar_poc_gabinete /opt/dagster/app/valledelencino

WORKDIR /opt/dagster/app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       curl ca-certificates gnupg apt-transport-https \
       unixodbc unixodbc-dev gcc g++ \
       sudo \
       unzip \
       rsync \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 \
  && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}"

COPY dagster/requirements.txt /tmp/requirements.txt
COPY dagster/mcdonalds /opt/dagster/app/mcdonalds
COPY dagster/smartfit /opt/dagster/app/smartfit
COPY dagster/mcdonalds_cdp /opt/dagster/app/mcdonalds_cdp
COPY dagster/mcdonalds_cdp /opt/dagster/app/bafar_poc_gabinete
COPY dagster/valledelencino /opt/dagster/app/valledelencino

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r /tmp/requirements.txt
