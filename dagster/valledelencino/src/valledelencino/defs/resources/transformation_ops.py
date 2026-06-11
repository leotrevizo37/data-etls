from typing import Optional

from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               cava,
                               chiller,
                               cisterna,
                               corriente,
                               destilados,
                               puerta,
                               tanque)
from ..resources import (SqlServerResource,
                         SparkResource,
                         SqlDimServerResource)
from pyspark.sql import DataFrame
from dagster import resource


class TransformationOps:
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
    def raw_dimensions(
            self,
            spark_res: SparkResource,
            dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
        return raw_dimensions(spark_res, dim_sqlserver)
    def cava(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return cava(df, dims, min_hour, max_hour)
    def chiller(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return chiller(df, dims, min_hour, max_hour)

    def cisterna(self, df: DataFrame, dims: DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return cisterna(df, dims, min_hour, max_hour)
    def corriente(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return corriente(df, dims, min_hour, max_hour)
    def destilados(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return destilados(df, dims, min_hour, max_hour)
    def puerta(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return puerta(df, dims, min_hour, max_hour)
    def tanque(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return tanque(df, dims, min_hour, max_hour)


@resource
def transformation_ops():
    return TransformationOps()
