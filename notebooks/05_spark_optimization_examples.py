# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Spark Optimization Examples
# MAGIC
# MAGIC Demonstrative notebook: runs and explains optimization techniques applied over the
# MAGIC already-created tables. The goal is to demonstrate practical knowledge of Spark's
# MAGIC and Delta Lake's characteristics — no new data is processed.
# MAGIC
# MAGIC **Techniques demonstrated:**
# MAGIC 1. Column selection before joins
# MAGIC 2. Filter before aggregation (predicate pushdown)
# MAGIC 3. Broadcast join for small tables
# MAGIC 4. Repartition with care
# MAGIC 5. Cache with criteria
# MAGIC 6. OPTIMIZE and ZORDER (Delta Lake)
# MAGIC 7. Delta time travel

# COMMAND ----------

from pyspark.sql.functions import col, broadcast, count, sum, avg, round

from src.config import (
    SILVER_ORDERS, SILVER_CUSTOMERS, SILVER_PRODUCTS,
    BRONZE_CATEGORY, GOLD_FACT
)

orders    = spark.table(SILVER_ORDERS)
customers = spark.table(SILVER_CUSTOMERS)
products  = spark.table(SILVER_PRODUCTS)
fact      = spark.table(GOLD_FACT)

# COMMAND ----------
# MAGIC %md ## 1. Column selection before joins
# MAGIC
# MAGIC **Why it matters:** Spark serializes and shuffles all DataFrame columns during
# MAGIC a join. Loading the entire table and only filtering afterwards wastes memory, I/O,
# MAGIC and network time between executors.
# MAGIC
# MAGIC **Rule:** select only the necessary columns *before* the join — never after.

# COMMAND ----------

# ❌ Inefficient version: join with all columns, select afterwards
inefficient = (
    orders.join(customers, "customer_id", "left")
    .select("order_id", "customer_state", "order_purchase_date")
)

# ✅ Optimized version: select before join reduces data in memory and shuffle
orders_slim    = orders.select("order_id", "customer_id", "order_purchase_date")
customers_slim = customers.select("customer_id", "customer_state")

efficient = orders_slim.join(customers_slim, "customer_id", "left")

# Both produce the same result, but the second is more efficient
print("Records (inefficient):", inefficient.count())
print("Records (optimized): ", efficient.count())

# COMMAND ----------
# MAGIC %md ## 2. Filter before aggregation (predicate pushdown)
# MAGIC
# MAGIC **Why it matters:** Spark reads data from storage and processes it in memory.
# MAGIC Applying filters as early as possible reduces the volume of data that flows through all
# MAGIC stages of the execution plan. This pattern is called **predicate pushdown** — the filter is
# MAGIC "pushed down" to the read, not applied at the end.
# MAGIC
# MAGIC **Delta Lake** optimizes further: with `ZORDER` and min/max statistics per block,
# MAGIC Spark can skip entire data blocks without even opening them (**data skipping**).

# COMMAND ----------

# ❌ Inefficient version: aggregates everything, filters afterwards
late_rate_bad = (
    fact
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total"),
        count(col("is_late")).alias("late")
    )
    .filter(col("total") > 100)
)

# ✅ Optimized version: filters before aggregating, reducing the groupBy volume
late_rate_good = (
    fact
    .filter(col("is_delivered") == True)   # eliminates in-transit orders before groupBy
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("delivered_orders"),
        count(col("is_late")).alias("late_orders")
    )
)

print("States with delivered orders:", late_rate_good.count())

# COMMAND ----------
# MAGIC %md ## 3. Broadcast join for small tables
# MAGIC
# MAGIC **Why it matters:** in a standard join (sort-merge join), Spark shuffles (*shuffles*)
# MAGIC both tables across the network to place records with the same key on the same executor.
# MAGIC This is expensive for large tables.
# MAGIC
# MAGIC **Broadcast join:** when one of the tables fits entirely in memory (typically
# MAGIC < 10 MB), Spark sends a copy of it to all executors. The join happens
# MAGIC locally on each executor, without shuffling the large table.
# MAGIC
# MAGIC **When to use:** lookup tables, small dimensions, reference lists.
# MAGIC In the project, the category translation table has only 71 rows — an ideal candidate.

# COMMAND ----------

category_translation = spark.table(BRONZE_CATEGORY)
print(f"Rows in category table: {category_translation.count()}")  # ~71 rows

# ❌ Without broadcast: Spark may choose to shuffle both sides
products_translated_bad = products.join(
    category_translation.select("product_category_name", "product_category_name_english"),
    "product_category_name",
    "left"
)

# ✅ With broadcast: sends the small table to all executors, zero shuffle on the large one
products_translated = products.join(
    broadcast(category_translation.select("product_category_name", "product_category_name_english")),
    "product_category_name",
    "left"
)

print("Products with translated category:", products_translated.count())

# To confirm Spark used broadcast, inspect the plan:
# products_translated.explain()  # look for "BroadcastHashJoin" in the plan

# COMMAND ----------
# MAGIC %md ## 4. Repartition with care
# MAGIC
# MAGIC **Why it matters:** Spark divides data into partitions processed in parallel.
# MAGIC Very small partitions create scheduling overhead; very large partitions cause
# MAGIC `OutOfMemoryError`. `repartition` forces a full shuffle to redistribute data —
# MAGIC use only when the parallelism gain compensates the shuffle cost.
# MAGIC
# MAGIC **`repartition` vs `coalesce`:**
# MAGIC - `repartition(n)` → full shuffle, useful to increase or redistribute partitions
# MAGIC - `coalesce(n)` → no shuffle, just merges partitions — useful to reduce before writes

# COMMAND ----------

# Note: .rdd.getNumPartitions() is not supported on Databricks Serverless (RDD API blocked).
# On environments with a classic cluster, it would be possible to inspect the partition count:
#   print(f"Current partitions: {fact.rdd.getNumPartitions()}")
# On Serverless, the number of partitions is automatically managed by the runtime.

# ✅ Repartition by grouping column before multiple aggregations
# Useful when the same column will be used in several subsequent groupBy operations
fact_repartitioned = fact.repartition("customer_state")

# ❌ Do not use repartition unnecessarily — generates unnecessary shuffle
# fact_repartitioned = fact.repartition(200)  # avoid arbitrary numbers without analysis

print(f"Repartition by customer_state applied: {fact_repartitioned.count()} rows")

# COMMAND ----------
# MAGIC %md ## 5. Cache with criteria
# MAGIC
# MAGIC **Why it matters:** Spark re-evaluates the full execution plan on every *action*
# MAGIC (`count`, `show`, `write`). If the same DataFrame is used in multiple actions,
# MAGIC it will be read from disk and recalculated every time.
# MAGIC
# MAGIC **`cache()`** stores the DataFrame in memory after the first materialization, avoiding
# MAGIC re-reads and recalculations on subsequent actions.
# MAGIC
# MAGIC **When to use:** only when the same DataFrame will be reused 2+ times in the same
# MAGIC session. Unnecessary cache consumes memory that could be used for shuffle.

# COMMAND ----------

# Note: .cache() uses PERSIST TABLE internally, which is not supported on Databricks Serverless.
# On environments with a classic cluster, the pattern would be:
#
#   fact_cached = fact.cache()
#
#   # First action: materializes and stores in memory
#   agg1 = fact_cached.filter(col("customer_state") == "SP").count()
#
#   # Second action: reads from cache, does not re-read Delta
#   agg2 = fact_cached.filter(col("is_late") == True).count()
#
#   print(f"Orders in SP: {agg1}")
#   print(f"Late orders: {agg2}")
#
#   # Release memory when no longer needed
#   fact_cached.unpersist()
#
# On Serverless, the runtime automatically manages DataFrame caching — explicit .cache()
# is neither necessary nor supported.

print("Cache example commented out — not supported on Serverless (PERSIST TABLE blocked).")

# COMMAND ----------
# MAGIC %md ## 6. OPTIMIZE and ZORDER (Delta Lake)
# MAGIC
# MAGIC **Problem without OPTIMIZE:** each execution of `write` or `MERGE` generates new small
# MAGIC Parquet files in Delta. Over time, a table can have hundreds of tiny files
# MAGIC that Spark needs to open and close individually — significant overhead.
# MAGIC
# MAGIC **`OPTIMIZE`** compacts these files into larger ones (target ~1 GB each),
# MAGIC reducing the number of I/O operations.
# MAGIC
# MAGIC **`ZORDER BY`** goes further: physically reorganizes the data within files so that
# MAGIC records with similar values in the chosen column are stored together.
# MAGIC Delta stores `min/max` statistics per block — when a query filters by
# MAGIC `customer_state = 'SP'`, Spark checks the statistics and **skips** blocks where
# MAGIC `min > 'SP'` or `max < 'SP'`. This is **data skipping**.
# MAGIC
# MAGIC **ZORDER column choice:** should be the column most used in analytical filters.
# MAGIC Avoid high-cardinality columns like `order_id` — they rarely appear in analytical
# MAGIC filters and the benefit would be minimal.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files in Gold tables
# MAGIC OPTIMIZE workspace.gold.fact_order_revenue;
# MAGIC OPTIMIZE workspace.gold.daily_revenue;
# MAGIC OPTIMIZE workspace.gold.product_category_revenue;
# MAGIC OPTIMIZE workspace.gold.seller_performance;
# MAGIC OPTIMIZE workspace.gold.customer_state_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Physically reorganize the fact table by the columns most used in analytical filters:
# MAGIC -- customer_state: regional filters (which states generate the most revenue?)
# MAGIC -- order_status:   status filters (delivered orders only)
# MAGIC OPTIMIZE workspace.gold.fact_order_revenue
# MAGIC ZORDER BY (customer_state, order_status);

# COMMAND ----------
# MAGIC %md ## 7. Delta time travel
# MAGIC
# MAGIC **What it is:** Delta Lake maintains a transaction log with all versions of the table
# MAGIC since its creation. Each `write`, `MERGE`, or `OPTIMIZE` creates a new numbered version.
# MAGIC
# MAGIC **Why it is exclusive to Delta:** plain Parquet tables have no transaction log —
# MAGIC each write overwrites the files. Delta preserves old files until `VACUUM` is run,
# MAGIC enabling historical queries.
# MAGIC
# MAGIC **Use cases:**
# MAGIC - "What did the revenue look like before the last load?"
# MAGIC - "Which records existed before yesterday's MERGE?"
# MAGIC - Analysis reproducibility: ensuring the same result is obtained even
# MAGIC   after data updates

# COMMAND ----------

# Query the initial version of the table (state after the first write)
df_v0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("workspace.gold.fact_order_revenue")
)
print(f"Records in version 0: {df_v0.count()}")

# View the complete history of operations on the table
print("\nVersion history:")
spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").select(
    "version", "timestamp", "operation", "operationParameters"
).show(truncate=False)

# COMMAND ----------
# MAGIC %md ## Summary: when to use each technique
# MAGIC
# MAGIC | Technique | When to use | When to avoid |
# MAGIC |---|---|---|
# MAGIC | **Select before join** | Always | — |
# MAGIC | **Filter early** | Whenever possible | — |
# MAGIC | **Broadcast join** | Table < 10 MB | Large tables (OutOfMemory) |
# MAGIC | **Repartition** | Before multiple groupBy on the same column | Without volume analysis |
# MAGIC | **Cache** | DataFrame reused 2+ times in the session | Single use or very large DataFrames |
# MAGIC | **OPTIMIZE** | After incremental loads with many writes | — (no cost on static dataset) |
# MAGIC | **ZORDER** | Medium-cardinality columns used in filters | Columns like `order_id` (high cardinality) |
# MAGIC | **Time travel** | Auditing, debugging, reproducibility | — |
