import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="retail_bronze",
    comment="Bronze table - raw incremental ingestion from UC Volume using Autoloader"
)
def retail_bronze():

    source_path = "/Volumes/dlt_retail_catalog/raw_schema/raw_vol/"  # update if needed

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(source_path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("input_file_name", col("_metadata.file_path"))  # <-- FIXED
    )
