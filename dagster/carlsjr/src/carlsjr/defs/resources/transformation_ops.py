from typing import Optional

from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               factReadingsMeasurement_device,
                               cisterna,
                               presencia,
                               cuartos_refrigeracion)
from ..resources import (SqlServerResource,
                         SparkResource,
                         SqlDimServerResource)

from pyspark.sql import DataFrame
from dagster import resource

class TransformationOps:
    def cisterna(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return cisterna(df, dims, min_hour, max_hour)
    def cuartos_refrigeracion(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return cuartos_refrigeracion(df, dims, min_hour, max_hour)
    def factlecturas_extended(
            self,
            spark_res: SparkResource,
            sqlserver: SqlServerResource,
            raw_dimensions: DataFrame,
            fetch_factlecturas_from: str) -> Optional[DataFrame]:
        return factlecturas_extended(
            spark_res,
            sqlserver,
            raw_dimensions,
            fetch_factlecturas_from
        )
    def factReadingsMeasurement_device(self, df:DataFrame) -> Optional[DataFrame]:
        return factReadingsMeasurement_device(df)
    def presencia(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return presencia(df, dims, min_hour, max_hour)
    def raw_dimensions(
            self,
            spark_res: SparkResource,
            dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
        return raw_dimensions(spark_res, dim_sqlserver)

@resource
def transformation_ops():
    return TransformationOps()
