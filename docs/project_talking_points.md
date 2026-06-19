# Project talking points

Reference for presenting or defending the post-graduation project.

---

## Why Databricks and not local Spark?

Databricks offers managed Serverless compute — no need to configure clusters,
install dependencies, or manage resources. Unity Catalog centralizes governance, lineage,
and access control. In production, data pipelines run on platforms like Databricks,
AWS EMR, or Google Dataproc — not on local machines.

---

## Why Medallion Architecture (Bronze/Silver/Gold)?

- **Bronze** preserves original data — any reprocessing can start from scratch
- **Silver** centralizes cleaning rules — business changes are applied in one place
- **Gold** eliminates repeated joins — BI tools consume ready-to-use data

Alternative without Medallion: transform directly from CSV to analytical table. The problem is that
any logic error forces re-ingesting the original data. Layer separation creates
independent reprocessing points.

---

## Why Delta Lake and not plain Parquet?

| Feature | Plain Parquet | Delta Lake |
|---|---|---|
| ACID transactions | No | Yes |
| Schema enforcement | No | Yes |
| Time travel | No | Yes |
| MERGE (upsert) | No | Yes |
| Transaction log | No | Yes |
| OPTIMIZE/ZORDER | No | Yes |

In production, without ACID, a failure mid-write leaves the table in an inconsistent state.

---

## Why `is_late = null` for undelivered orders?

Using `False` for orders still in transit would mask the real late delivery rate.
If 10% of orders are in transit and counted as "not late", the calculated rate
would be artificially low. With `null`, the filter `WHERE is_delivered = true` ensures that
only completed orders enter the denominator of the rate.

---

## Why overwrite and not incremental load?

The Olist dataset is a static historical snapshot — no new data is arriving. Doing a MERGE
on data that never changes adds no practical value. In production with dynamic data:
- Bronze: `mode("append")` with control by ingestion date
- Silver: `MERGE INTO` to apply status updates (e.g.: `shipped` → `delivered`)

  Without a time window control, MERGE would join the entire Silver against the entire source on each
  execution — expensive at scale. The correct approach uses a **watermark**:
  filters only the records modified since the last execution before reaching the MERGE.

  ```python
  from delta.tables import DeltaTable
  from pyspark.sql.functions import (
      col, to_timestamp, to_date, datediff, when, lower, trim, max as spark_max, expr
  )

  # Watermark: fetches the timestamp of the last ingestion in Bronze.
  last_ingestion = spark.sql("""
      SELECT MAX(ingestion_timestamp) AS last_run
      FROM workspace.bronze.orders
      WHERE ingestion_date = current_date() - INTERVAL 1 DAY
  """).collect()[0]["last_run"]

  # Reads only the new or modified records since the last run
  # and applies the same transformations from 02_transform_silver.py
  new_orders = (
      spark.table("workspace.bronze.orders")
      .filter(col("ingestion_timestamp") > last_ingestion)
      .withColumn("order_purchase_timestamp",      to_timestamp("order_purchase_timestamp"))
      .withColumn("order_approved_at",             to_timestamp("order_approved_at"))
      .withColumn("order_delivered_carrier_date",  to_timestamp("order_delivered_carrier_date"))
      .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
      .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
      .withColumn("order_status",                  lower(trim(col("order_status"))))
      .withColumn("order_purchase_date",           to_date("order_purchase_timestamp"))
      .withColumn("delivery_days",
          datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
      .withColumn("estimated_delivery_days",
          datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")))
      .withColumn("is_delivered",
          when(col("order_delivered_customer_date").isNotNull(), True).otherwise(False))
      .withColumn("is_late",
          when(col("is_delivered") == True,
              when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                  True).otherwise(False)))
  )

  # MERGE processes only the delta — not the entire historical table
  silver_table = DeltaTable.forName(spark, "workspace.silver.orders")

  (
      silver_table.alias("target").merge(
          new_orders.alias("source"),
          "target.order_id = source.order_id"
      ).whenMatchedUpdate(
          # updates only if the status changed — avoids unnecessary rewrites
          condition="target.order_status != source.order_status",
          set={
              "order_status":                  "source.order_status",
              "order_delivered_customer_date": "source.order_delivered_customer_date",
              "delivery_days":                 "source.delivery_days",
              "is_delivered":                  "source.is_delivered",
              "is_late":                       "source.is_late",
          }
      ).whenNotMatchedInsertAll()  # inserts if the order_id does not yet exist
      .execute()
  )
  ```
- Gold: incremental recalculation by affected partition

---

## Modeling decisions in Gold

**Why pre-aggregate payments and items before the fact join?**

An order can have multiple payments and multiple items. Without pre-aggregation, the join
would multiply rows: an order with 3 items and 2 payments would generate 6 rows in the fact.
Pre-aggregation guarantees 1 row per order before the final join.

**Why partition the fact by `order_purchase_date`?**

Analytical queries frequently filter by period. With partitioning, Spark reads
only the files for the filtered date (partition pruning), ignoring everything else.

**Why `countDistinct` for `total_orders` in `seller_performance`?**

A seller can have multiple items in the same order. `count("order_id")` would count each item
as an order. `countDistinct("order_id")` counts unique orders — the correct metric.

---

## What demonstrates data engineering knowledge

- Explicit schema instead of `inferSchema` — predictability and performance
- Separation of responsibilities by layer — maintainability
- Isolation of invalid records instead of discarding — traceability
- Window function for primary payment — avoids nested subquery
- Broadcast join for small lookup table — avoids unnecessary shuffle
- Fact table partitioning — read optimization by period
- ZORDER on most filtered columns — efficient data skipping
- `to_timestamp` in Silver for date conversion — kept as `StringType` in Bronze intentionally
- `multiLine=True` + `escape='"'` for CSV with free text — ingestion robustness
- Data quality with audit logging — pipeline observability
