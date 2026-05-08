FROM python:3.10-slim-bookworm

ENV ACCEPT_EULA=Y \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/dagster/app/src

RUN mkdir -p "${DAGSTER_HOME}" /opt/dagster/app/mcdonalds /opt/dagster/app/smartfit /opt/dagster/app/mcdonalds_cdp

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

RUN mkdir -p /opt/spark/jars-extra \
  && curl -fL -o /opt/spark/jars-extra/hadoop-azure-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
  && curl -fL -o /opt/spark/jars-extra/hadoop-azure-datalake-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.4/hadoop-azure-datalake-3.3.4.jar \
  && curl -fL -o /opt/spark/jars-extra/azure-storage-8.6.6.jar \
       https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \
  && curl -fL -o /opt/spark/jars-extra/mssql-jdbc-12.6.1.jre11.jar \
       https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars-extra/*

COPY dagster/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r /tmp/requirements.txt

COPY dagster/mcdonalds /opt/dagster/app/mcdonalds
COPY dagster/smartfit /opt/dagster/app/smartfit
COPY dagster/mcdonalds_cdp /opt/dagster/app/mcdonalds_cdp
