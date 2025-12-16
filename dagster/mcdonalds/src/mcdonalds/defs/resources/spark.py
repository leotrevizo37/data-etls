from datetime import datetime, timedelta
from typing import Optional, List

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark import SparkConf
from dagster import resource

def _daterange_days(start: datetime, end: datetime) -> List[datetime]:
    current = start.date()
    last = end.date()
    days = []
    while current <= last:
        days.append(datetime(current.year, current.month, current.day))
        current = current + timedelta(days=1)
    return days

class SparkResource:
    def __init__(
        self,
        master_url: str,
        app_name: str,
        adls_account_name: str,
        adls_container_name: str,
        adls_base_path: str,
        adls_account_key: Optional[str],
        jdbc_host: str,
        jdbc_port: int,
        jdbc_database: str,
        jdbc_user: str,
        jdbc_password: str,
    ) -> None:
        self.master_url = master_url
        self.app_name = app_name
        self.adls_account_name = adls_account_name
        self.adls_container_name = adls_container_name
        self.adls_base_path = adls_base_path
        self.adls_account_key = adls_account_key
        self.jdbc_host = jdbc_host
        self.jdbc_port = jdbc_port
        self.jdbc_database = jdbc_database
        self.jdbc_user = jdbc_user
        self.jdbc_password = jdbc_password
        self._spark: Optional[SparkSession] = None

    def get_spark(self) -> SparkSession:
        if self._spark is not None:
            return self._spark

        conf = (
            SparkConf()
            .setAppName(self.app_name)
            .setMaster(self.master_url)
        )
        if self.adls_account_name and self.adls_account_key:
            account = self.adls_account_name
            conf.set(
                f"fs.azure.account.key.{account}.dfs.core.windows.net",
                self.adls_account_key,
            )
            conf.set(
                "fs.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            )
            conf.set(
                "spark.jars.packages",
                    ",".join([
                        "org.apache.hadoop:hadoop-azure:3.3.4",
                        "com.microsoft.azure:azure-storage:8.6.6",
                        "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11",
                    ])
            )
        self._spark = SparkSession.builder.config(conf=conf).getOrCreate()

        self._spark = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.master_url)
            .config(conf=conf)
            .getOrCreate()
        )
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None

    def _build_adls_path(self) -> str:
        return (
            "abfss://"
            + self.adls_container_name
            + "@"
            + self.adls_account_name
            + ".dfs.core.windows.net/"
            + self.adls_base_path
        )

    def read_lecturas_range(self, from_utc: datetime, to_utc: datetime) -> DataFrame:
        spark = self.get_spark()
        base = self._build_adls_path()

        days = _daterange_days(from_utc, to_utc)
        paths = []
        for d in days:
            year_str = str(d.year).zfill(4)
            month_str = str(d.month)
            day_str = str(d.day)
            path = (
                    base
                    + "/"
                    + year_str
                    + "/"
                    + month_str
                    + "/"
                    + day_str
                    + "//"
            )
            paths.append(path)

        if not paths:
            return spark.createDataFrame([], schema="ReadId string")

        df = spark.read.parquet(*paths)
        return df.filter(
            (col("TimeSpan") >= from_utc) & (col("TimeSpan") <= to_utc)
        )

    def write_fact_lecturas_to_sql(self, df: DataFrame) -> None:
        if df.rdd.isEmpty():
            return
        url = (
            "jdbc:sqlserver://"
            + self.jdbc_host
            + ":"
            + str(self.jdbc_port)
            + ";databaseName="
            + self.jdbc_database
            + ";encrypt=true;trustServerCertificate=true"
        )
        properties = {
            "user": self.jdbc_user,
            "password": self.jdbc_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        df.write.mode("append").jdbc(url=url, table="dwh.factLecturas", properties=properties)

    def get_max_timespan(self, df: DataFrame) -> Optional[datetime]:
        if df.rdd.isEmpty():
            return None
        row = df.agg(spark_max("TimeSpan").alias("max_ts")).collect()[0]
        max_ts = row.max_ts
        if max_ts is None:
            return None
        return max_ts


@resource
def spark_resource(_):
    master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    app_name = os.getenv("SPARK_APP_NAME", "FactLecturasIngestion")
    adls_account_name = os.getenv("ADLS_ACCOUNT_NAME", "")
    adls_container_name = os.getenv("ADLS_CONTAINER_NAME", "")
    adls_base_path = os.getenv("ADLS_BASE_PATH", "")
    adls_account_key = os.getenv("ADLS_ACCOUNT_KEY")
    jdbc_host = os.getenv("MSSQL_HOST", "localhost")
    jdbc_port = int(os.getenv("MSSQL_PORT", "1433"))
    jdbc_database = os.getenv("APP_DB", "")
    jdbc_user = os.getenv("MSSQL_USER", "")
    jdbc_password = os.getenv("MSSQL_PASSWORD", "")
    spark_res = SparkResource(
        master_url=master_url,
        app_name=app_name,
        adls_account_name=adls_account_name,
        adls_container_name=adls_container_name,
        adls_base_path=adls_base_path,
        adls_account_key=adls_account_key,
        jdbc_host=jdbc_host,
        jdbc_port=jdbc_port,
        jdbc_database=jdbc_database,
        jdbc_user=jdbc_user,
        jdbc_password=jdbc_password,
    )
    try:
        yield spark_res
    finally:
        spark_res.stop()
