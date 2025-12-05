
# 🚀 Retail Medallion Data Pipeline using Delta Live Tables (DLT)

A complete **end-to-end Medallion architecture** built using **Delta Live Tables (DLT)** on Databricks.  
This project demonstrates **incremental ingestion**, **data cleaning**, **data quality checks**, and **business-level aggregations** using **Bronze → Silver → Gold** layers.

---

## 🏗 Architecture Diagram

```
                    Raw CSV Files (UC Volume)
                                │
                      Auto Loader (cloudFiles)
                                ▼
                      🟫 Bronze Layer
                 - Incremental ingestion
                 - Metadata columns
                                │
                                ▼
                      ⚪ Silver Layer
                 - Data cleaning
                 - Type casting
                 - DLT expectations
                                │
                                ▼
                      🟡 Gold Layer
     ┌───────────────────────┬────────────────────────┬────────────────────────┐
     ▼                       ▼                        ▼
Daily Revenue     Monthly Revenue        Top Selling Products
```

---

## 📂 Folder Structure

```
dlt-retail-project/
│── bronze/bronze_dlt.py
│── silver/silver_dlt.py
│── gold/gold_dlt.py
│── config/00_config.json
└── retail_dlt_pipeline
```

---

## 🟫 Bronze Layer – Incremental Ingestion

```python
source_path = "/Volumes/dlt_retail_catalog/raw_schema/raw_vol/"

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("input_file_name", col("_metadata.file_path"))
)
```

---

## ⚪ Silver Layer – Cleaning & Standardization

```python
df_clean = (
    dlt.read_stream("retail_bronze")
       .withColumn("tran_date", to_date("tran_date", "yyyy-MM-dd"))
       .drop("_rescued_data")
)
```

---

## 🟡 Gold Layer – Business Aggregations

### Daily Revenue
```python
df_daily = (
    dlt.read("retail_silver")
        .groupBy("tran_date")
        .agg(sum("amount").alias("daily_revenue"))
)
```

### Top Products
```python
df_top = (
    dlt.read("retail_silver")
       .groupBy("product_id", "product_name")
       .agg(sum("quantity").alias("total_qty"))
)
```

---

## 📊 Output Tables

| Layer | Table Name |
|-------|------------|
| Bronze | retail_bronze |
| Silver | retail_silver |
| Gold   | gold_daily_revenue |
| Gold   | gold_monthly_revenue |
| Gold   | gold_top_products |

---

## 🚀 How to Run

1. Upload raw CSV files into a UC Volume  
2. Create a new **Delta Live Tables pipeline**  
3. Configure:
   - Catalog: `dlt_retail_catalog`
   - Schema: `analytics`
4. Set pipeline source folder  
5. Run pipeline  

---

## 👨‍💼 **Author**  
 - [@Tejas Gawali]([https://www.github.com/octokatherine](https://github.com/tejas13gawali))
Azure Data Engineer | Databricks | PySpark | Delta Lake | SQL  
