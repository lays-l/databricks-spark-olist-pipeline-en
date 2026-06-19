# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Data Quality Checks
# MAGIC
# MAGIC Validates the integrity of the processed data and records results in an audit table.
# MAGIC The goal is not only to remove invalid data — it is to **log** the problems found
# MAGIC to enable traceability, investigation, and improvement of the data source.
# MAGIC
# MAGIC **Output table:** `workspace.gold.data_quality_summary`
# MAGIC
# MAGIC Each row represents the result of a rule applied to a table:
# MAGIC `table_name | rule_name | total_records | invalid_records | invalid_pct | status | checked_at`

# COMMAND ----------

from pyspark.sql.functions import col, count, when, current_timestamp, lit
# 'round' is not imported from Spark to avoid shadowing Python's built-in round,
# which is used for the percentage calculation of invalid records (float operation).
from datetime import datetime

from src.config import (
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS,
    GOLD_FACT, GOLD_DQ_SUMMARY
)
from src.data_quality import (
    check_not_null, check_non_negative, check_valid_status,
    check_no_duplicates, VALID_ORDER_STATUSES
)

# COMMAND ----------
# MAGIC %md ## Check function
# MAGIC
# MAGIC Centralizes the logic for each check: counts total records, counts invalid ones
# MAGIC (records that violate the rule) and calculates the percentage. The result is an audit row.
# MAGIC
# MAGIC `invalid_df` is a filtered DataFrame that returns records that **violate** the rule.
# MAGIC Example: `col("order_id").isNull()` marks orders without an ID as invalid.

# COMMAND ----------

results = []

def check(table_name: str, rule_name: str, df, invalid_df):
    """
    Runs a quality rule and stores the result in the audit list.

    table_name:  full name of the table being checked
    rule_name:   rule description (e.g.: "order_id not null")
    df:          full DataFrame (to count the total)
    invalid_df:  filtered DataFrame returned by src.data_quality functions
                 (check_not_null, check_non_negative, check_valid_status, check_no_duplicates)
    """
    total   = df.count()
    invalid = invalid_df.count()
    pct     = round(invalid / total * 100, 4) if total > 0 else 0.0
    status  = "PASS" if invalid == 0 else "FAIL"

    results.append({
        "table_name":      table_name,
        "rule_name":       rule_name,
        "total_records":   total,
        "invalid_records": invalid,
        "invalid_pct":     pct,
        "status":          status,
        "checked_at":      datetime.now().isoformat()
    })
    print(f"  [{status}] {table_name} — {rule_name}: {invalid}/{total} invalid ({pct}%)")

# COMMAND ----------
# MAGIC %md ## Checks: `silver.orders`
# MAGIC
# MAGIC Minimum integrity rules for the orders table:
# MAGIC - `order_id` cannot be null — it is the primary key
# MAGIC - `customer_id` cannot be null — an order without a customer is not traceable
# MAGIC - `order_status` must belong to the set of valid Olist values
# MAGIC - Grain: there must be no duplicate `order_id` (1 order = 1 row)

# COMMAND ----------

orders = spark.table(SILVER_ORDERS)

check(SILVER_ORDERS, "order_id not null",
      orders, check_not_null(orders, "order_id"))

check(SILVER_ORDERS, "customer_id not null",
      orders, check_not_null(orders, "customer_id"))

check(SILVER_ORDERS, "order_status valid",
      orders, check_valid_status(orders, "order_status", VALID_ORDER_STATUSES))

# check_no_duplicates returns groups with more than 1 occurrence — each row = 1 duplicate order_id
check(SILVER_ORDERS, "order_id no duplicates",
      orders, check_no_duplicates(orders, "order_id"))

# COMMAND ----------
# MAGIC %md ## Checks: `silver.order_payments`
# MAGIC
# MAGIC - `order_id` cannot be null
# MAGIC - `payment_value` cannot be negative (free shipping or full discount = 0, not negative)
# MAGIC - `payment_installments` cannot be negative
# MAGIC - `payment_type` cannot be null

# COMMAND ----------

payments = spark.table(SILVER_PAYMENTS)

check(SILVER_PAYMENTS, "order_id not null",
      payments, check_not_null(payments, "order_id"))

check(SILVER_PAYMENTS, "payment_value >= 0",
      payments, check_non_negative(payments, "payment_value"))

check(SILVER_PAYMENTS, "payment_installments >= 0",
      payments, check_non_negative(payments, "payment_installments"))

check(SILVER_PAYMENTS, "payment_type not null",
      payments, check_not_null(payments, "payment_type"))

# COMMAND ----------
# MAGIC %md ## Checks: `silver.order_items`
# MAGIC
# MAGIC - `order_id`, `product_id`, and `seller_id` cannot be null
# MAGIC - `price` cannot be negative
# MAGIC - `freight_value` cannot be negative

# COMMAND ----------

items = spark.table(SILVER_ITEMS)

check(SILVER_ITEMS, "order_id not null",
      items, check_not_null(items, "order_id"))

check(SILVER_ITEMS, "product_id not null",
      items, check_not_null(items, "product_id"))

check(SILVER_ITEMS, "seller_id not null",
      items, check_not_null(items, "seller_id"))

check(SILVER_ITEMS, "price >= 0",
      items, check_non_negative(items, "price"))

check(SILVER_ITEMS, "freight_value >= 0",
      items, check_non_negative(items, "freight_value"))

# COMMAND ----------
# MAGIC %md ## Checks: `gold.fact_order_revenue`
# MAGIC
# MAGIC Validates consistency of the fact table after joins and aggregations:
# MAGIC - Total orders in the fact must match `silver.orders`
# MAGIC - No `order_id` should be null
# MAGIC - No delivered order should have null `delivery_days` (calculated field from Silver)
# MAGIC - `payment_total_value` should not be negative

# COMMAND ----------

fact = spark.table(GOLD_FACT)
orders_count = orders.count()
fact_count   = fact.count()

# Grain consistency: fact must have the same number of orders as Silver
grain_status = "PASS" if fact_count == orders_count else "FAIL"
results.append({
    "table_name":      GOLD_FACT,
    "rule_name":       "grain matches silver.orders",
    "total_records":   fact_count,
    "invalid_records": abs(fact_count - orders_count),
    "invalid_pct":     0.0 if fact_count == orders_count else round(abs(fact_count - orders_count) / orders_count * 100, 4),
    "status":          grain_status,
    "checked_at":      datetime.now().isoformat()
})
print(f"  [{grain_status}] {GOLD_FACT} — grain matches silver.orders: fact={fact_count}, silver={orders_count}")

check(GOLD_FACT, "order_id not null",
      fact, check_not_null(fact, "order_id"))

check(GOLD_FACT, "delivered orders have delivery_days",
      fact, fact.filter((col("is_delivered") == True) & col("delivery_days").isNull()))

check(GOLD_FACT, "payment_total_value >= 0",
      fact, check_non_negative(fact, "payment_total_value"))

# COMMAND ----------
# MAGIC %md ## Save results to the audit table
# MAGIC
# MAGIC Converts the results list into a DataFrame and saves it to `gold.data_quality_summary`.
# MAGIC The `overwrite` mode rewrites the summary on each run — in production, `append` would preserve
# MAGIC the history of previous runs for quality trend analysis over time.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

dq_schema = StructType([
    StructField("table_name",      StringType(), True),
    StructField("rule_name",       StringType(), True),
    StructField("total_records",   LongType(),   True),
    StructField("invalid_records", LongType(),   True),
    StructField("invalid_pct",     DoubleType(), True),
    StructField("status",          StringType(), True),
    StructField("checked_at",      StringType(), True),
])

dq_df = spark.createDataFrame(results, schema=dq_schema)
dq_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DQ_SUMMARY)

print(f"\n  ✓ {GOLD_DQ_SUMMARY} — {dq_df.count()} rules checked")

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

print("\n--- Quality check results ---\n")
passes = sum(1 for r in results if r["status"] == "PASS")
fails  = sum(1 for r in results if r["status"] == "FAIL")
print(f"  PASS: {passes}")
print(f"  FAIL: {fails}")

if fails > 0:
    print("\n  Failed rules:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"    - [{r['table_name']}] {r['rule_name']}: {r['invalid_records']} invalid records")
