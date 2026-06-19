# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer Build
# MAGIC
# MAGIC Creates analytical tables ready for consumption, eliminating the need for repeated joins
# MAGIC in BI tools or ad-hoc analyses.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `workspace.gold.fact_order_revenue` — fact table, 1 row per order, partitioned by date
# MAGIC - `workspace.gold.daily_revenue` — aggregated revenue by day and state
# MAGIC - `workspace.gold.customer_state_revenue` — revenue, volume, and late rate by state
# MAGIC - `workspace.gold.product_category_revenue` — revenue and volume by category
# MAGIC - `workspace.gold.seller_performance` — sales and delivery performance by seller
# MAGIC - `workspace.gold.payment_method_summary` — summary by payment method

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, round, when,
    row_number, collect_set, max as spark_max
)
from pyspark.sql.window import Window

from src.config import (
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS,
    SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_SELLERS,
    GOLD_FACT, GOLD_DAILY, GOLD_STATE,
    GOLD_CATEGORY, GOLD_SELLER, GOLD_PAYMENT
)

# COMMAND ----------
# MAGIC %md ## Reading Silver tables

# COMMAND ----------

orders   = spark.table(SILVER_ORDERS)
items    = spark.table(SILVER_ITEMS)
payments = spark.table(SILVER_PAYMENTS)
customers = spark.table(SILVER_CUSTOMERS)
products = spark.table(SILVER_PRODUCTS)
sellers  = spark.table(SILVER_SELLERS)

# COMMAND ----------
# MAGIC %md ## Payment aggregation per order
# MAGIC
# MAGIC The Olist dataset allows a single order to be paid with more than one method —
# MAGIC for example, part on credit card and part with a voucher. In `silver.payments`,
# MAGIC each payment method is a separate row. Without aggregation, a direct join with
# MAGIC `orders` would multiply rows, making the order appear more than once in the fact table.
# MAGIC
# MAGIC **`main_payment` (window function):** identifies the primary method — defined as
# MAGIC the one with the highest `payment_value`. The window orders the order's payments by value desc
# MAGIC and selects `row_number() == 1`, without needing a nested subquery.
# MAGIC
# MAGIC **`payments_agg` (groupBy):** collapses all rows for the order into one, calculating:
# MAGIC - `payment_total_value` — sum of all payments
# MAGIC - `payment_installments_max` — maximum installments among the methods used
# MAGIC - `payment_types` — array with all methods (`["credit_card", "voucher"]`)
# MAGIC - `payment_count` — number of payment methods used

# COMMAND ----------

# Window to identify the highest-value payment per order
w_payment = Window.partitionBy("order_id").orderBy(col("payment_value").desc())

main_payment = (
    payments
    .withColumn("rn", row_number().over(w_payment))
    .filter(col("rn") == 1)
    .select("order_id", col("payment_type").alias("main_payment_type"))
)

payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        round(sum("payment_value"), 2).alias("payment_total_value"),
        spark_max("payment_installments").alias("payment_installments_max"),
        collect_set("payment_type").alias("payment_types"),
        count("*").alias("payment_count")
    )
    .join(main_payment, "order_id", "left")
)

# COMMAND ----------
# MAGIC %md ## Item aggregation per order
# MAGIC
# MAGIC In `silver.order_items`, each item of an order is a separate row — an order with
# MAGIC 3 products has 3 rows. Without aggregation, the join with `orders` would triple that order in the fact.
# MAGIC
# MAGIC The groupBy consolidates everything into one row per order, calculating:
# MAGIC - `item_count` — total items (rows) in the order
# MAGIC - `product_count` — distinct products (using `countDistinct`)
# MAGIC - `seller_count` — distinct sellers involved in the order
# MAGIC - `items_total_value` — sum of product prices
# MAGIC - `freight_total_value` — sum of freight costs
# MAGIC - `order_total_value` — order total (product + freight), field already calculated in Silver

# COMMAND ----------

items_agg = (
    items
    .groupBy("order_id")
    .agg(
        count("*").alias("item_count"),
        countDistinct("product_id").alias("product_count"),
        countDistinct("seller_id").alias("seller_count"),
        round(sum("price"), 2).alias("items_total_value"),
        round(sum("freight_value"), 2).alias("freight_total_value"),
        round(sum("item_total_value"), 2).alias("order_total_value")
    )
)

# COMMAND ----------
# MAGIC %md ## Table: `gold.fact_order_revenue`
# MAGIC
# MAGIC Central table of the Gold layer: consolidates into **one row per order** all the attributes
# MAGIC relevant for analysis, eliminating the need for repeated joins in analytical queries.
# MAGIC
# MAGIC **Sources:** `silver.orders` + `silver.customers` + `payments_agg` + `items_agg`
# MAGIC (the last two already pre-aggregated in the previous steps to guarantee 1:1 granularity).
# MAGIC
# MAGIC **Partitioned by `order_purchase_date`:** when a query filters by period
# MAGIC (e.g.: `WHERE order_purchase_date = '2018-01-01'`), Spark reads only the Delta files
# MAGIC for that partition, ignoring all others — this is **partition pruning**.
# MAGIC
# MAGIC **Calculated fields inherited from Silver:** `delivery_days`, `estimated_delivery_days`,
# MAGIC `is_delivered`, and `is_late` arrive ready — Gold just exposes them in the fact.

# COMMAND ----------

fact = (
    orders
    .join(customers.select("customer_id", "customer_city", "customer_state"), "customer_id", "left")
    .join(payments_agg, "order_id", "left")
    .join(items_agg, "order_id", "left")
    .select(
        "order_id",
        "customer_id",
        "customer_city",
        "customer_state",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "estimated_delivery_days",
        "is_delivered",
        "is_late",
        "payment_total_value",
        "payment_installments_max",
        "main_payment_type",
        "payment_count",
        "item_count",
        "product_count",
        "seller_count",
        "items_total_value",
        "freight_total_value",
        "order_total_value"
    )
)

(
    fact.write.format("delta")
    .mode("overwrite")
    .partitionBy("order_purchase_date")
    .saveAsTable(GOLD_FACT)
)
print(f"  ✓ {GOLD_FACT} — {fact.count()} rows")

# COMMAND ----------
# MAGIC %md ## Table: `gold.daily_revenue`
# MAGIC
# MAGIC Aggregates revenue and delivery metrics by **day × state**, from the already-built fact table.
# MAGIC Answers questions like "what was SP's revenue in December 2017?".
# MAGIC
# MAGIC - `total_orders` — total orders on the day/state
# MAGIC - `delivered_orders` — actually delivered orders (`is_delivered = true`)
# MAGIC - `late_orders` — orders delivered late (`is_late = true`)
# MAGIC - `avg_delivery_days` — average days to delivery (only orders with a delivery date)
# MAGIC
# MAGIC `count(when(...))` is the Spark pattern for conditional counting without needing a
# MAGIC subquery — equivalent to `COUNT(CASE WHEN ... END)` in SQL.

# COMMAND ----------

daily = (
    fact
    .groupBy("order_purchase_date", "customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_delivered") == True, 1)).alias("delivered_orders"),
        count(when(col("is_late") == True, 1)).alias("late_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .orderBy("order_purchase_date", "customer_state")
)

daily.write.format("delta").mode("overwrite").saveAsTable(GOLD_DAILY)
print(f"  ✓ {GOLD_DAILY} — {daily.count()} rows")

# COMMAND ----------
# MAGIC %md ## Table: `gold.customer_state_revenue`
# MAGIC
# MAGIC Aggregates revenue by **customer state**, directly from the fact table.
# MAGIC Answers: "Which states generate the most revenue?" and "Where are the highest late rates?".
# MAGIC
# MAGIC - `late_rate` — proportion of late orders over **delivered** orders (not over all orders).
# MAGIC   Using `delivered_orders` in the denominator avoids distortion: orders still in transit
# MAGIC   have `is_late = null` and should not enter the late rate calculation.
# MAGIC - `withColumn` after agg calculates `late_rate` as a derived column, using aliases already
# MAGIC   defined in `agg` — only possible after `groupBy` resolves the names.

# COMMAND ----------

state_revenue = (
    fact
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_delivered") == True, 1)).alias("delivered_orders"),
        count(when(col("is_late") == True, 1)).alias("late_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .withColumn(
        "late_rate",
        round(col("late_orders") / col("delivered_orders"), 4)
    )
    .orderBy(col("total_revenue").desc())
)

state_revenue.write.format("delta").mode("overwrite").saveAsTable(GOLD_STATE)
print(f"  ✓ {GOLD_STATE} — {state_revenue.count()} rows")

# COMMAND ----------
# MAGIC %md ## Table: `gold.product_category_revenue`
# MAGIC
# MAGIC Aggregates revenue by **product category** (English name, normalized in Silver).
# MAGIC Answers: "Which categories sell the most and generate the most revenue?".
# MAGIC
# MAGIC **Source:** `silver.order_items` (granularity: 1 row per item), enriched with the
# MAGIC category via join with `silver.products`. The join with `orders` brings `order_status`,
# MAGIC allowing future filters by status if needed.
# MAGIC
# MAGIC - `total_orders` uses `countDistinct("order_id")` — an order with 3 items from the same
# MAGIC   category counts as 1 order, not 3.
# MAGIC - `total_items` uses `count("*")` — counts each item row individually.
# MAGIC - `avg_item_price` is the average per item (not per order), reflecting the average ticket
# MAGIC   of a product within the category.

# COMMAND ----------

# Join between items, orders (to filter delivered) and products (for category)
category_revenue = (
    items
    .join(orders.select("order_id", "order_status"), "order_id", "left")
    .join(products.select("product_id", "product_category_name_english"), "product_id", "left")
    .groupBy("product_category_name_english")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items"),
        round(sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_item_price")
    )
    .orderBy(col("total_revenue").desc())
)

category_revenue.write.format("delta").mode("overwrite").saveAsTable(GOLD_CATEGORY)
print(f"  ✓ {GOLD_CATEGORY} — {category_revenue.count()} rows")

# COMMAND ----------
# MAGIC %md ## Table: `gold.seller_performance`
# MAGIC
# MAGIC Aggregates sales and delivery metrics by **seller**.
# MAGIC Answers: "Which sellers have the highest volume and lowest late rate?".
# MAGIC
# MAGIC **Two-step join strategy:**
# MAGIC 1. `items` + `orders` → aggregates performance metrics by `seller_id`
# MAGIC 2. Result + `sellers` → enriches with seller city and state
# MAGIC
# MAGIC Separating into two steps prevents the join with `orders` (large) from occurring before
# MAGIC aggregation, reducing the volume of data processed in Spark.
# MAGIC
# MAGIC - `total_orders` uses `countDistinct("order_id")` — an order with 3 items from the same
# MAGIC   seller counts as 1 order.
# MAGIC - `late_rate` = `late_order_count / total_orders` — proportion of late orders over
# MAGIC   the seller's total orders (includes undelivered in denominator, unlike
# MAGIC   `customer_state_revenue` where we use only delivered orders).

# COMMAND ----------

seller_orders = (
    items
    .join(
        orders.select("order_id", "delivery_days", "is_late", "is_delivered"),
        "order_id", "left"
    )
    .groupBy("seller_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items"),
        round(sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_item_price"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days"),
        count(when(col("is_late") == True, 1)).alias("late_order_count")
    )
    .withColumn(
        "late_rate",
        round(col("late_order_count") / col("total_orders"), 4)
    )
)

seller_perf = (
    seller_orders
    .join(sellers.select("seller_id", "seller_city", "seller_state"), "seller_id", "left")
    .select(
        "seller_id", "seller_city", "seller_state",
        "total_orders", "total_items", "total_revenue",
        "avg_item_price", "avg_delivery_days",
        "late_order_count", "late_rate"
    )
    .orderBy(col("total_revenue").desc())
)

seller_perf.write.format("delta").mode("overwrite").saveAsTable(GOLD_SELLER)
print(f"  ✓ {GOLD_SELLER} — {seller_perf.count()} rows")

# COMMAND ----------
# MAGIC %md ## Table: `gold.payment_method_summary`
# MAGIC
# MAGIC Aggregates totals by **primary payment method** (the `main_payment_type` field from the fact,
# MAGIC defined in the payment aggregation step as the highest-value method for the order).
# MAGIC Answers: "Which method is most used?" and "Do installment orders have a higher average ticket?".
# MAGIC
# MAGIC - `avg_installments` — average of maximum installments per order. Values close to 1
# MAGIC   indicate cash payment; high values indicate recurring installment payments.
# MAGIC - The comparison between `avg_order_value` for `credit_card` vs `boleto` reveals whether
# MAGIC   customers who pay in installments tend to buy higher-value items — a common pattern in Brazilian e-commerce.

# COMMAND ----------

payment_summary = (
    fact
    .groupBy("main_payment_type")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("payment_installments_max"), 1).alias("avg_installments")
    )
    .orderBy(col("total_orders").desc())
)

payment_summary.write.format("delta").mode("overwrite").saveAsTable(GOLD_PAYMENT)
print(f"  ✓ {GOLD_PAYMENT} — {payment_summary.count()} rows")

# COMMAND ----------
# MAGIC %md ## Gold layer summary

# COMMAND ----------

gold_tables = [GOLD_FACT, GOLD_DAILY, GOLD_STATE, GOLD_CATEGORY, GOLD_SELLER, GOLD_PAYMENT]

print("\nGold layer summary:\n")
for t in gold_tables:
    count_val = spark.table(t).count()
    print(f"  {t}: {count_val} records")
