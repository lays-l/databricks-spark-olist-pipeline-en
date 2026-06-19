# Pipeline Architecture

## Overview

```
Olist CSVs (Kaggle)
        ↓
/Volumes/workspace/default/olist_raw/   ← Unity Catalog Volume
        ↓  [01_ingest_bronze.py]
workspace.bronze.*    Delta Tables — raw data, explicit schema, ingestion metadata
        ↓  [02_transform_silver.py]
workspace.silver.*    Delta Tables — typed, cleaned, calculated fields, isolated invalids
        ↓  [03_build_gold.py]
workspace.gold.*      Delta Tables — analytical tables ready for consumption
        ↓
Databricks SQL Editor / sample_queries.sql
```

---

## Layers

### Bronze — raw ingestion

Responsibility: persist the original data with minimal transformation.

- Explicit schema via `StructType` in `src/schemas.py` — no `inferSchema=True`
- Dates kept as `StringType` — conversion is Silver's responsibility
- Metadata columns added: `ingestion_timestamp`, `ingestion_date`, `source_file`
- CSV read options: `multiLine=True` and `escape='"'` for reviews (field with free text)
- Write mode: `overwrite`

### Silver — cleaning and typing

Responsibility: reliable, typed, and standardized data for internal consumption.

- Conversion from `StringType` to `TimestampType` with `to_timestamp`
- Calculated fields: `order_purchase_date`, `delivery_days`, `estimated_delivery_days`, `is_delivered`, `is_late`
- `is_late = null` for undelivered orders — avoids distortion in the late rate
- Invalid records isolated in `silver.invalid_orders` and `silver.invalid_payments`
- Broadcast join for category table (71 rows) with `silver.products`
- Explicit select on all tables — defines the layer's schema contract
- Write mode: `overwrite`

### Gold — analytical consumption

Responsibility: tables modeled to answer business questions without repeated joins.

- `fact_order_revenue` — fact table, 1 row per order, partitioned by `order_purchase_date`
- `daily_revenue` — revenue by day × state
- `customer_state_revenue` — revenue and late rate by state
- `product_category_revenue` — revenue by category
- `seller_performance` — performance by seller
- `payment_method_summary` — summary by payment method
- Write mode: `overwrite` (static dataset — see `docs/project_talking_points.md` for incremental pattern with watermark and MERGE)

---

## Unity Catalog

All tables follow the three-part naming convention: `catalog.schema.table`.

| Catalog | Schema | Purpose |
|---|---|---|
| `workspace` | `bronze` | Ingested raw data |
| `workspace` | `silver` | Cleaned and typed data |
| `workspace` | `gold` | Analytical tables |
| `workspace` | `default` | Volume `olist_raw` with CSVs |

The `workspace` catalog is the default for Databricks Free Edition (confirmed via `SELECT current_catalog()`).

---

## Delta Lake

All tables are Delta — not plain Parquet. This enables:

- **ACID transactions** — atomic writes without corrupted data on partial failures.
  Without ACID, a failure mid-`overwrite` can leave the table with partial files.
  Delta writes to staging and only commits to the transaction log after completion.

- **Schema enforcement** — rejects writes that violate the defined schema.
  If a new field arrives with the wrong type (e.g.: `price` as `StringType`), Delta refuses
  the write instead of silently corrupting the data.

- **Time travel** — query previous versions via version number or timestamp.

  ```python
  # Table state before the last overwrite
  spark.read.format("delta").option("versionAsOf", 0).table("workspace.gold.fact_order_revenue")

  # History of all operations
  spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").show(truncate=False)
  ```

- **Transaction log** — each operation (`write`, `MERGE`, `OPTIMIZE`) generates an entry in
  `_delta_log/`. Spark reads this log to reconstruct the current state of the table without
  needing to scan all physical files.

- **OPTIMIZE + ZORDER** — small file compaction and physical data reorganization for
  data skipping. See details in `docs/spark_concepts.md`.
