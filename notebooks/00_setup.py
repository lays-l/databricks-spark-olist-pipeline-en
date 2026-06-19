# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Environment Setup
# MAGIC
# MAGIC **Objective:** Prepare the environment for running the pipeline.
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Validates the environment (catalog, Spark version)
# MAGIC 2. Creates Bronze, Silver, and Gold schemas in Unity Catalog
# MAGIC 3. Creates the Volume to store raw CSVs
# MAGIC 4. Downloads the Olist CSVs via Kaggle API
# MAGIC 5. Verifies that all required files are available

# COMMAND ----------
# MAGIC %md ## Cell 1 — Validate environment

# COMMAND ----------

print(f"Spark version: {spark.version}")
spark.sql("SELECT current_catalog(), current_database()").show()

# COMMAND ----------
# MAGIC %md ## Cell 2 — Create schemas and Volume in Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 'workspace' is the default catalog confirmed via SELECT current_catalog() with Spark 4.1.0
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC
# MAGIC -- Volume to store raw CSVs
# MAGIC -- Path: /Volumes/workspace/default/olist_raw/
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.olist_raw;

# COMMAND ----------
# MAGIC %md ## Cell 3 — Download Olist CSVs via Kaggle API
# MAGIC
# MAGIC **Important:** enter your token before running. Do not commit this value to Git.
# MAGIC Token obtained at: kaggle.com/settings > API > Create New API Token

# COMMAND ----------

import os

# Databricks Free Edition (Serverless) does not support configuring environment variables
# through the interface. The token must be entered directly in this cell before running.
# WARNING: replace the value below with your token and DO NOT commit with the real token.
# After the download, CSVs are persisted in the Volume and this cell does not need to be
# run again.
os.environ["KAGGLE_API_TOKEN"] = "KGAT_your_token_here"  # replace before running

print("Token configured.")

# COMMAND ----------

# MAGIC %pip install kaggle --quiet

# COMMAND ----------

import subprocess

result = subprocess.run(
    [
        "kaggle", "datasets", "download",
        "--dataset", "olistbr/brazilian-ecommerce",
        "--unzip",
        "--path", "/Volumes/workspace/default/olist_raw/"
    ],
    capture_output=True, text=True
)

print(result.stdout)
if result.returncode != 0:
    print("Error:", result.stderr)

# COMMAND ----------
# MAGIC %md ## Cell 4 — Verify files in the Volume

# COMMAND ----------

from src.config import RAW_DATA_PATH, SOURCE_FILES

files = dbutils.fs.ls(RAW_DATA_PATH)
found_files = {f.name for f in files}

print(f"Files in {RAW_DATA_PATH}:\n")
for name in sorted(found_files):
    print(f"  ✓ {name}")

missing = [n for n in SOURCE_FILES.values() if n not in found_files]
if missing:
    print("\nMissing files:")
    for name in missing:
        print(f"  ✗ {name}")
else:
    print("\nAll files available. Environment ready for 01_ingest_bronze.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discussion points
# MAGIC
# MAGIC - The Medallion Architecture (Bronze/Silver/Gold) separates responsibilities by layer,
# MAGIC   enabling governance, traceability, and controlled reprocessing.
# MAGIC - In Databricks Free Edition, the default catalog is `workspace` (Unity Catalog).
# MAGIC - Volumes replace FileStore with integrated governance: permissions, audit, and standardized path.
# MAGIC - Serverless compute eliminates the need to create and manage clusters —
# MAGIC   the environment starts automatically when the first cell is executed.
