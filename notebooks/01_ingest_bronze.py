# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC **Objective:** Read Olist raw CSVs and save them as Delta Tables in the Bronze layer.
# MAGIC
# MAGIC The Bronze layer preserves data exactly as it came from the source.
# MAGIC The only transformations applied are the addition of traceability metadata:
# MAGIC - `ingestion_timestamp`: when the data was ingested
# MAGIC - `ingestion_date`: ingestion date (used for future partitioning)
# MAGIC - `source_file`: path of the source file
# MAGIC
# MAGIC **Technical decision — explicit schemas vs inferSchema:**
# MAGIC `inferSchema=True` performs two scans of the file (one to infer, one to read),
# MAGIC is slower and may infer incorrect types (e.g.: order_id as Integer).
# MAGIC Explicit schemas defined in `src/schemas.py` make the pipeline predictable.
# MAGIC Dates remain as StringType in Bronze — conversion is Silver's responsibility.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date, col
from src.config import RAW_DATA_PATH, SOURCE_FILES
from src.config import (
    BRONZE_ORDERS, BRONZE_ITEMS, BRONZE_PAYMENTS, BRONZE_CUSTOMERS,
    BRONZE_PRODUCTS, BRONZE_SELLERS, BRONZE_REVIEWS, BRONZE_CATEGORY
)
from src.schemas import (
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA, PAYMENTS_SCHEMA, CUSTOMERS_SCHEMA,
    PRODUCTS_SCHEMA, SELLERS_SCHEMA, REVIEWS_SCHEMA, CATEGORY_TRANSLATION_SCHEMA
)

# COMMAND ----------
# MAGIC %md ## Ingestion function

# COMMAND ----------

def ingest_csv_to_bronze(file_name: str, schema, table_name: str, multiline: bool = False):
    """
    Reads a CSV from the Volume with an explicit schema, adds ingestion metadata,
    and saves it as a Delta Table in the Bronze layer.

    multiline=True: required for files with free text that may contain line breaks
    inside quoted fields (e.g.: review_comment_message). Without this option, Spark
    interprets each \\n as a new row and misaligns subsequent columns.
    """
    path = f"{RAW_DATA_PATH}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", multiline)
        # escape='"' follows the RFC 4180 standard: "" inside a quoted field = literal quote.
        # Spark's default escape='\\' does not understand "", causing
        # column misalignment in fields with escaped double quotes (e.g.: review_comment_message).
        .option("escape", '"')
        .schema(schema)
        .csv(path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_file", col("_metadata.file_path"))
    )

    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"  ✓ {table_name} — {df.count()} rows ingested")
    return df

# COMMAND ----------
# MAGIC %md ## Ingestion of all tables

# COMMAND ----------

print("Starting Bronze ingestion...\n")

ingest_csv_to_bronze(SOURCE_FILES["orders"],             ORDERS_SCHEMA,              BRONZE_ORDERS)
ingest_csv_to_bronze(SOURCE_FILES["order_items"],        ORDER_ITEMS_SCHEMA,         BRONZE_ITEMS)
ingest_csv_to_bronze(SOURCE_FILES["payments"],           PAYMENTS_SCHEMA,            BRONZE_PAYMENTS)
ingest_csv_to_bronze(SOURCE_FILES["customers"],          CUSTOMERS_SCHEMA,           BRONZE_CUSTOMERS)
ingest_csv_to_bronze(SOURCE_FILES["products"],           PRODUCTS_SCHEMA,            BRONZE_PRODUCTS)
ingest_csv_to_bronze(SOURCE_FILES["sellers"],            SELLERS_SCHEMA,             BRONZE_SELLERS)
ingest_csv_to_bronze(SOURCE_FILES["reviews"],            REVIEWS_SCHEMA,             BRONZE_REVIEWS,    multiline=True)
ingest_csv_to_bronze(SOURCE_FILES["category_translation"], CATEGORY_TRANSLATION_SCHEMA, BRONZE_CATEGORY)

print("\nBronze ingestion complete.")

# COMMAND ----------
# MAGIC %md ## Quick validation

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS total FROM {BRONZE_ORDERS}").show()
spark.sql(f"DESCRIBE DETAIL {BRONZE_ORDERS}").select("format", "numFiles", "sizeInBytes").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discussion points
# MAGIC
# MAGIC - **Bronze preserves raw data:** no business rules are applied.
# MAGIC   This enables auditing and reprocessing when rules change.
# MAGIC - **Explicit schemas:** avoid double inference and incorrect types.
# MAGIC   Dates as StringType are intentional — the original data is preserved.
# MAGIC - **Delta Table:** unlike plain Parquet, Delta maintains a transaction log,
# MAGIC   supports ACID, schema enforcement, and time travel.
# MAGIC - **Lazy evaluation:** no data is read until `.write` is called —
# MAGIC   Spark builds the execution plan before executing.
