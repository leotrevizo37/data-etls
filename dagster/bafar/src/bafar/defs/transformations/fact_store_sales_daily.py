from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, to_date


def transform_fact_store_sales_daily(sales_df: DataFrame) -> DataFrame:
    return (
        sales_df
        .select(
            col("Id").cast("long").alias("SaleId"),
            col("Clave_Tienda").cast("string").alias("StoreId"),
            to_date(col("Fecha")).alias("SalesDate"),
            col("Importe").cast("decimal(19,4)").alias("Amount"),
        )
        .dropDuplicates(["SaleId"])
        .groupBy("StoreId", "SalesDate")
        .agg(
            count("*").cast("int").alias("SalesCount"),
            spark_sum("Amount").cast("decimal(19,4)").alias("SalesAmount"),
        )
        .select("StoreId", "SalesDate", "SalesCount", "SalesAmount")
    )
