import dlt
from pyspark.sql.functions import *

# Bronze Layer - Ingest raw files from UC Volume using Auto Loader

@dlt.table(
    name="retail_bronze",
    comment="Bronze table - raw incremental ingestion from UC Volume using Autoloader"
)
def retail_bronze():

    source_path = "/Volumes/dlt_retail_catalog/raw_schema/raw_vol/"  
    # Update catalog, schema, and volume names

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(source_path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("input_file_name", input_file_name())
    )
