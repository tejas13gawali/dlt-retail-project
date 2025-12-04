import dlt
from pyspark.sql.functions import *

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
