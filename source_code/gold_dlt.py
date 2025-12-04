import dlt
from pyspark.sql.functions import *

# ---------------------------------------------------------------
# 1. DAILY REVENUE
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# 2. MONTHLY REVENUE
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# 3. TOP PRODUCTS
# ---------------------------------------------------------------

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
