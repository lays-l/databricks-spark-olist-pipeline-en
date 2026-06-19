# Databricks + PySpark + Delta Lake — Olist E-Commerce Pipeline

Data engineering pipeline built as a personal project using
**Databricks Free Edition**, **PySpark**, **Spark SQL**, and **Delta Lake** with the public dataset
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

## Stack

| Component | Details |
|---|---|
| Platform | Databricks Free Edition |
| Compute | Serverless (Spark 4.1.0, Python 3.11) |
| Catalog | Unity Catalog — default catalog `workspace` |
| Storage | Unity Catalog Volumes (`/Volumes/workspace/default/olist_raw/`) |
| Format | Delta Lake (ACID, time travel, incremental MERGE) |
| Language | Python / PySpark / Spark SQL |

---

## Architecture

```
Olist CSVs (Kaggle → Unity Catalog Volume)
        ↓  [01_ingest_bronze.py]
workspace.bronze.*   — raw data with explicit schema + ingestion metadata
        ↓  [02_transform_silver.py]
workspace.silver.*   — typed, cleaned, validated, invalid records isolated
        ↓  [03_build_gold.py]
workspace.gold.*     — analytical tables ready for consumption
        ↓
Databricks SQL Editor / sql/sample_queries.sql
```

Layer details: [`docs/architecture.md`](docs/architecture.md)

---

## Business questions answered

- What was the daily revenue?
- Which states generate the most revenue?
- Which product categories sell the most?
- What is the average delivery time?
- Which orders were delivered late?
- Which payment methods are most commonly used?
- Do installment orders have a higher average ticket?
- Which sellers have the highest sales volume?

---

## Repository structure

```
databricks-spark-olist-pipeline-en/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── notebooks/
│   ├── 00_setup.py               # environment setup, CSV download
│   ├── 01_ingest_bronze.py       # CSV → Delta Bronze ingestion
│   ├── 02_transform_silver.py    # cleaning, typing, validation
│   ├── 03_build_gold.py          # analytical tables
│   ├── 04_data_quality_checks.py # validations with audit logging
│   └── 05_spark_optimization_examples.py  # broadcast, ZORDER, time travel
│
├── src/
│   ├── config.py       # constants: paths, table names, catalog
│   ├── schemas.py      # explicit StructType for each Bronze table
│   └── data_quality.py # reusable validation functions
│
├── sql/
│   └── sample_queries.sql  # 14 analytical queries ready for the SQL Editor
│
└── docs/
    ├── architecture.md            # diagram and layer descriptions
    ├── spark_concepts.md          # lazy eval, broadcast, ZORDER, time travel
    └── project_talking_points.md  # technical decisions and discussion points
```

---

## Prerequisites

- [Databricks Free Edition](https://www.databricks.com/try-databricks) account
- [Kaggle](https://www.kaggle.com) account with API Token generated
- Repository connected to Databricks via **Workspace → Repos**

---

## How to run

### 1. Connect the repository to Databricks

1. In Databricks, go to **Workspace → Repos → Add Repo**
2. Paste this repository's URL
3. Click **Create Repo**

### 2. Configure the Kaggle API Token

> **Databricks Free Edition restriction:** Serverless does not support environment variables
> through the configuration interface. The token must be entered directly in the notebook cell
> before running the download, and **must not be committed to the repository**.

1. Go to [kaggle.com/settings](https://www.kaggle.com/settings) → **API** → **Create New API Token**
2. In `00_setup.py`, replace the value in the configuration cell before running:

```python
os.environ["KAGGLE_API_TOKEN"] = "KGAT_your_token_here"
```

The token is only needed for the initial download — CSVs are persisted in the Volume afterward.

### 3. Run the notebooks in order

Open each notebook inside the repo in Databricks and run with **Serverless compute**:

| Notebook | What it does |
|---|---|
| `00_setup.py` | Creates schemas, Volume and downloads CSVs via Kaggle API |
| `01_ingest_bronze.py` | Reads CSVs with explicit schema → `workspace.bronze.*` |
| `02_transform_silver.py` | Typing, cleaning, calculated fields → `workspace.silver.*` |
| `03_build_gold.py` | Analytical tables → `workspace.gold.*` |
| `04_data_quality_checks.py` | Validations with audit → `workspace.gold.data_quality_summary` |
| `05_spark_optimization_examples.py` | Examples of OPTIMIZE, ZORDER and time travel |

---

## Tables created

### Bronze

| Table | Source |
|---|---|
| `workspace.bronze.orders` | olist_orders_dataset.csv |
| `workspace.bronze.order_items` | olist_order_items_dataset.csv |
| `workspace.bronze.payments` | olist_order_payments_dataset.csv |
| `workspace.bronze.customers` | olist_customers_dataset.csv |
| `workspace.bronze.products` | olist_products_dataset.csv |
| `workspace.bronze.sellers` | olist_sellers_dataset.csv |
| `workspace.bronze.reviews` | olist_order_reviews_dataset.csv |
| `workspace.bronze.category_translation` | product_category_name_translation.csv |

### Silver

| Table | Description |
|---|---|
| `workspace.silver.orders` | Orders with converted dates, `is_late` and `is_delivered` flags |
| `workspace.silver.order_items` | Items with calculated `item_total_value` |
| `workspace.silver.payments` | Validated payments |
| `workspace.silver.customers` | Standardized customers |
| `workspace.silver.products` | Products with English category (broadcast join) |
| `workspace.silver.sellers` | Standardized sellers |
| `workspace.silver.reviews` | Reviews with proper typing |
| `workspace.silver.invalid_orders` | Invalid orders isolated for audit |
| `workspace.silver.invalid_payments` | Invalid payments isolated for audit |

### Gold

| Table | Description |
|---|---|
| `workspace.gold.fact_order_revenue` | Fact table, 1 row per order, partitioned by date |
| `workspace.gold.daily_revenue` | Revenue by day and state |
| `workspace.gold.customer_state_revenue` | Revenue and late rate by state |
| `workspace.gold.product_category_revenue` | Revenue by category |
| `workspace.gold.seller_performance` | Performance by seller |
| `workspace.gold.payment_method_summary` | Summary by payment method |
| `workspace.gold.data_quality_summary` | Data quality audit log |

---

## Future improvements (production implementation)

In a real scenario with continuously arriving data, the following evolutions would apply:

**Pipeline and ingestion**
- Incremental load with watermark and `MERGE INTO` in Silver — pattern documented in `docs/project_talking_points.md`
- `pipeline_control.last_run` control table to manage watermark robustly and decoupled from Bronze
- Automated `VACUUM` on Gold tables to release old files after the time travel retention period

**Scheduling and orchestration**
- Migrate notebook logic to **pure Python modules** (`src/`) — notebooks are suitable for exploration and development, but in production they hinder testing, versioning, and reuse
- Package the pipeline as a **Python wheel** and run via `spark-submit` or Databricks Jobs, eliminating the dependency on the interactive notebook environment
- Orchestrate stages with **Apache Airflow** (Astronomer) or **Databricks Workflows** calling jobs directly — with explicit dependencies Bronze → Silver → Gold → Quality
- Automatic failure alerts — email or Slack integration via webhook in the orchestrator

**Quality and testing**
- Unit tests with `pytest` for `src/data_quality.py` functions
- Row count validation between layers as a gate before advancing to the next stage

**Consumption and visualization**
- Databricks SQL Dashboard connected to Gold tables — business question visualization without manual SQL

**Architecture**
- Separate catalogs per environment (`dev.bronze.*`, `prod.bronze.*`) using Unity Catalog multi-catalog
- Job clusters instead of Serverless for larger workloads — greater configuration and cost control

---

## Documentation

| File | Content |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Diagram, layer descriptions, Unity Catalog, Delta Lake |
| [`docs/spark_concepts.md`](docs/spark_concepts.md) | Lazy eval, broadcast join, window functions, ZORDER, time travel, multiLine CSV |
| [`docs/project_talking_points.md`](docs/project_talking_points.md) | Technical decisions and project discussion points |
