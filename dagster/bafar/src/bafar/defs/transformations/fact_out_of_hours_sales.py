from pyspark.sql import DataFrame
from pyspark.sql.functions import ceil, col, date_format, dayofweek, lit, to_date, unix_timestamp, when


def transform_fact_out_of_hours_sales(
    sales_df: DataFrame,
    schedules_df: DataFrame,
) -> DataFrame:
    return (
        sales_df
        .select(
            col("Id").cast("long").alias("SaleId"),
            col("NumeroVenta").cast("string").alias("SaleNumber"),
            col("Importe").cast("decimal(19,4)").alias("Amount"),
            col("Fecha").cast("timestamp").alias("LocalTimeSpan"),
            col("NombreUsuario").cast("string").alias("UserName"),
            col("Clave_Tienda").cast("string").alias("StoreId"),
        )
        .withColumn("DayOfWeek", dayofweek(col("LocalTimeSpan")))
        .join(
            schedules_df.select(
                col("StoreId").cast("string").alias("ScheduleStoreId"),
                col("DayOfWeek").alias("ScheduleDayOfWeek"),
                "OpeningDate",
                "ClosingDate",
                "OpeningTime",
                "LunchStartTime",
                "LunchEndTime",
                "ClosingTime",
            ),
            (col("StoreId") == col("ScheduleStoreId"))
            & (col("DayOfWeek") == col("ScheduleDayOfWeek"))
            & (col("OpeningDate").isNull() | (to_date(col("LocalTimeSpan")) >= to_date(col("OpeningDate"))))
            & (col("ClosingDate").isNull() | (to_date(col("LocalTimeSpan")) <= to_date(col("ClosingDate")))),
            "left",
        )
        .withColumn("SaleTime", date_format(col("LocalTimeSpan"), "HH:mm:ss"))
        .withColumn("SaleSeconds", unix_timestamp(col("SaleTime"), "HH:mm:ss"))
        .withColumn("OpeningSeconds", unix_timestamp(col("OpeningTime"), "HH:mm:ss"))
        .withColumn("LunchStartSeconds", unix_timestamp(col("LunchStartTime"), "HH:mm:ss"))
        .withColumn("LunchEndSeconds", unix_timestamp(col("LunchEndTime"), "HH:mm:ss"))
        .withColumn("ClosingSeconds", unix_timestamp(col("ClosingTime"), "HH:mm:ss"))
        .withColumn(
            "OutOfHoursType",
            when(col("ScheduleStoreId").isNull(), lit(4))
            .when(col("SaleSeconds") < col("OpeningSeconds"), lit(1))
            .when(
                col("LunchStartSeconds").isNotNull()
                & col("LunchEndSeconds").isNotNull()
                & (col("SaleSeconds") >= col("LunchStartSeconds"))
                & (col("SaleSeconds") < col("LunchEndSeconds")),
                lit(2),
            )
            .when(col("SaleSeconds") >= col("ClosingSeconds"), lit(3))
            .cast("byte"),
        )
        .withColumn(
            "DeviationMinutes",
            when(
                col("OutOfHoursType") == 1,
                ceil((col("OpeningSeconds") - col("SaleSeconds")) / lit(60.0)),
            )
            .when(
                col("OutOfHoursType") == 2,
                ceil((col("SaleSeconds") - col("LunchStartSeconds")) / lit(60.0)),
            )
            .when(
                col("OutOfHoursType") == 3,
                ceil((col("SaleSeconds") - col("ClosingSeconds")) / lit(60.0)),
            )
            .cast("int"),
        )
        .select(
            "SaleId",
            "SaleNumber",
            "Amount",
            "LocalTimeSpan",
            "UserName",
            "StoreId",
            "OutOfHoursType",
            "DeviationMinutes",
        )
    )
