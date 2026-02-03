import dlt
from pyspark.sql.functions import *

config = (
    spark.read.json(
        "/Volumes/dlt_retail_catalog/config/config_param/00_config.json",
        multiLine=True,
    )
    .first()
    .asDict()
)

SOURCE_PATH = config.get(
    "source_path",
    "/Volumes/dlt_retail_catalog/raw_schema/raw_vol/",
)

#-------------------------------------------------------------------------
# Retail bronze B
#-------------------------------------------------------------------------

@dlt.table(
    name="retail_bronze",
    comment="Bronze table - raw incremental ingestion from UC Volume using Autoloader"
)
def retail_bronze():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(SOURCE_PATH)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("input_file_name", col("_metadata.file_path"))  # <-- FIXED
    )


#----------------------------------------------------------------------------------------------------------
# Retail Silver
#----------------------------------------------------------------------------------------------------------

@dlt.table(
    name="retail_silver",
    comment="Silver table - cleaned and transformed retail sales data with data quality rules"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect("valid_date", "tran_date IS NOT NULL")
def retail_silver():

    df = dlt.read_stream("retail_bronze")

    return (
        df.withColumn("tran_date", to_date("tran_date", "yyyy-MM-dd"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("price", col("price").cast("double"))
          .withColumn("amount", col("amount").cast("double"))
          .withColumn("year", year("tran_date"))
          .withColumn("month", month("tran_date"))
          .withColumn("day", dayofmonth("tran_date"))
          .dropna(subset=["tran_id", "tran_date", "amount"])
    )

#----------------------------------------------------------------------------------------------------------
# Gold Layer
#----------------------------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------------------------
# 1. DAILY REVENUE
#----------------------------------------------------------------------------------------------------------

@dlt.table(
    name="gold_daily_revenue",
    comment="Gold table - total sales revenue per day"
)
def gold_daily_revenue():
    
    df = dlt.read("retail_silver")

    return (
        df.groupBy("tran_date")
          .agg(
              sum("amount").alias("daily_revenue"),
              count("*").alias("transactions")
          )
          .orderBy("tran_date")
    )

#----------------------------------------------------------------------------------------------------------
# 2. MONTHLY REVENUE
#----------------------------------------------------------------------------------------------------------

@dlt.table(
    name="gold_monthly_revenue",
    comment="Gold table - total revenue grouped by year and month"
)
def gold_monthly_revenue():

    df = dlt.read("retail_silver")

    return (
        df.groupBy("year", "month")
          .agg(
              sum("amount").alias("monthly_revenue"),
              count("*").alias("transactions")
          )
          .orderBy("year", "month")
    )

#----------------------------------------------------------------------------------------------------------
# 3. TOP PRODUCTS
#----------------------------------------------------------------------------------------------------------

@dlt.table(
    name="gold_top_products",
    comment="Gold table - top selling products by total revenue"
)
def gold_top_products():

    df = dlt.read("retail_silver")

    return (
        df.groupBy("product_id", "product_name")
          .agg(
              sum("amount").alias("total_revenue"),
              sum("quantity").alias("total_quantity"),
              count("*").alias("orders")
          )
          .orderBy(col("total_revenue").desc())
    )
