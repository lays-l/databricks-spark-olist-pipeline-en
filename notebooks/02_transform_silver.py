# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC
# MAGIC **Objective:** Create clean, typed, and standardized tables from Bronze.
# MAGIC
# MAGIC The Silver layer is where we apply business rules and data quality.
# MAGIC Invalid records are isolated in dedicated tables for auditing — not discarded.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `workspace.silver.orders` + `workspace.silver.invalid_orders`
# MAGIC - `workspace.silver.order_items`
# MAGIC - `workspace.silver.payments` + `workspace.silver.invalid_payments`
# MAGIC - `workspace.silver.customers`
# MAGIC - `workspace.silver.products` (with broadcast join for category translation)
# MAGIC - `workspace.silver.sellers`
# MAGIC - `workspace.silver.reviews`

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, to_date, datediff,
    when, lower, trim, coalesce, lit, broadcast
)
from src.config import (
    BRONZE_ORDERS, BRONZE_ITEMS, BRONZE_PAYMENTS, BRONZE_CUSTOMERS,
    BRONZE_PRODUCTS, BRONZE_SELLERS, BRONZE_REVIEWS, BRONZE_CATEGORY,
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS, SILVER_CUSTOMERS,
    SILVER_PRODUCTS, SILVER_SELLERS, SILVER_REVIEWS,
    SILVER_INVALID_ORDERS, SILVER_INVALID_PAYMENTS
)
from src.data_quality import VALID_ORDER_STATUSES

# COMMAND ----------
# MAGIC %md ## silver.orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations applied:**
# MAGIC - Date conversion from StringType to TimestampType
# MAGIC - Standardization of order_status to lowercase
# MAGIC - Creation of calculated fields: order_purchase_date, delivery_days, estimated_delivery_days
# MAGIC - is_delivered: True if the delivery date is filled
# MAGIC - is_late: True/False only for delivered orders — null for undelivered orders
# MAGIC   (using False for in-transit orders would mask the real late delivery rate)

# COMMAND ----------

orders_raw = spark.table(BRONZE_ORDERS)

orders_clean = (
    orders_raw
    .withColumn("order_purchase_timestamp",      to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",             to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",  to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    .withColumn("order_status",                  lower(trim(col("order_status"))))
    .withColumn("order_purchase_date",           to_date("order_purchase_timestamp"))
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
    .withColumn(
        "estimated_delivery_days",
        datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
    )
    .withColumn(
        "is_delivered",
        when(col("order_delivered_customer_date").isNotNull(), True).otherwise(False)
    )
    .withColumn(
        "is_late",
        when(
            col("is_delivered") == True,
            when(
                col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                True
            ).otherwise(False)
        )
        # null for undelivered orders — is_late is not applicable to in-transit orders
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "estimated_delivery_days",
        "is_delivered",
        "is_late",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

# Separate invalid records before saving Silver
invalid_orders = orders_clean.filter(
    col("order_id").isNull() |
    col("customer_id").isNull() |
    col("order_purchase_timestamp").isNull() |
    ~col("order_status").isin(VALID_ORDER_STATUSES)
)

valid_orders = orders_clean.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("order_purchase_timestamp").isNotNull() &
    col("order_status").isin(VALID_ORDER_STATUSES)
)

valid_orders.write.format("delta").mode("overwrite").saveAsTable(SILVER_ORDERS)
invalid_orders.write.format("delta").mode("overwrite").saveAsTable(SILVER_INVALID_ORDERS)

print(f"  ✓ {SILVER_ORDERS}: {valid_orders.count()} valid records")
print(f"  ⚠ {SILVER_INVALID_ORDERS}: {invalid_orders.count()} invalid records isolated")

# COMMAND ----------
# MAGIC %md ## silver.payments

# COMMAND ----------

payments_raw = spark.table(BRONZE_PAYMENTS)

payments_clean = (
    payments_raw
    .withColumn("payment_value",        col("payment_value").cast("double"))
    .withColumn("payment_installments", col("payment_installments").cast("integer"))
    .withColumn("payment_type",         lower(trim(col("payment_type"))))
    .withColumn("is_credit_card",       when(col("payment_type") == "credit_card", True).otherwise(False))
    .withColumn("is_installment",       when(col("payment_installments") > 1, True).otherwise(False))
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "is_credit_card",
        "is_installment",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

invalid_payments = payments_clean.filter(
    col("order_id").isNull() |
    (col("payment_value") < 0) |
    col("payment_type").isNull()
)

valid_payments = payments_clean.filter(
    col("order_id").isNotNull() &
    (col("payment_value") >= 0) &
    col("payment_type").isNotNull()
)

valid_payments.write.format("delta").mode("overwrite").saveAsTable(SILVER_PAYMENTS)
invalid_payments.write.format("delta").mode("overwrite").saveAsTable(SILVER_INVALID_PAYMENTS)

print(f"  ✓ {SILVER_PAYMENTS}: {valid_payments.count()} valid records")
print(f"  ⚠ {SILVER_INVALID_PAYMENTS}: {invalid_payments.count()} invalid records isolated")

# COMMAND ----------
# MAGIC %md ## silver.order_items

# COMMAND ----------

items_raw = spark.table(BRONZE_ITEMS)

items_clean = (
    items_raw
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
    .withColumn("price",         col("price").cast("double"))
    .withColumn("freight_value", col("freight_value").cast("double"))
    # In the current dataset, freight_value is never null or zero (verified during exploration).
    # The coalesce ensures robustness in case future sources send null for free shipping.
    # Bronze preserves the raw data — the decision to treat null as 0.0 belongs to Silver.
    .withColumn("freight_value",    coalesce(col("freight_value"), lit(0.0)))
    .withColumn("item_total_value", col("price") + col("freight_value"))
    .filter(
        col("order_id").isNotNull() &
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        (col("price") >= 0) &
        (col("freight_value") >= 0)
    )
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "item_total_value",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

items_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_ITEMS)
print(f"  ✓ {SILVER_ITEMS}: {items_clean.count()} records")

# COMMAND ----------
# MAGIC %md ## silver.customers

# COMMAND ----------

customers_raw = spark.table(BRONZE_CUSTOMERS)

customers_clean = (
    customers_raw
    .withColumn("customer_city",  lower(trim(col("customer_city"))))
    .withColumn("customer_state", trim(col("customer_state")))
    .filter(col("customer_id").isNotNull())
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

customers_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_CUSTOMERS)
print(f"  ✓ {SILVER_CUSTOMERS}: {customers_clean.count()} records")

# COMMAND ----------
# MAGIC %md ## silver.products
# MAGIC
# MAGIC **Technical decision — Broadcast join:**
# MAGIC The category translation table has only 71 records and fits entirely in memory.
# MAGIC By using `broadcast()`, Spark sends a copy of this table to each executor,
# MAGIC avoiding the shuffle of the large products table. Without broadcast, both tables
# MAGIC would need to be redistributed across the network — much more expensive.

# COMMAND ----------

products_raw = spark.table(BRONZE_PRODUCTS)

# Select only the business columns from the translation table before the join.
# The translation table also has ingestion metadata columns
# (ingestion_date, ingestion_timestamp, source_file) inherited from Bronze.
# Without this select, the join would result in duplicate columns and Delta would reject the write.
category_filtered_raw = spark.table(BRONZE_CATEGORY).select(
    "product_category_name",
    "product_category_name_english"
)

products_clean = (
    products_raw
    .join(broadcast(category_filtered_raw), "product_category_name", "left")
    .withColumn(
        "product_category_name_english",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    .filter(col("product_id").isNotNull())
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

products_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_PRODUCTS)
print(f"  ✓ {SILVER_PRODUCTS}: {products_clean.count()} records")

# COMMAND ----------
# MAGIC %md ## silver.sellers

# COMMAND ----------

sellers_raw = spark.table(BRONZE_SELLERS)

sellers_clean = (
    sellers_raw
    .withColumn("seller_city",  lower(trim(col("seller_city"))))
    .withColumn("seller_state", trim(col("seller_state")))
    .filter(col("seller_id").isNotNull())
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

sellers_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_SELLERS)
print(f"  ✓ {SILVER_SELLERS}: {sellers_clean.count()} records")

# COMMAND ----------
# MAGIC %md ## silver.reviews

# COMMAND ----------

reviews_raw = spark.table(BRONZE_REVIEWS)

reviews_clean = (
    reviews_raw
    .withColumn("review_creation_date",    to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
    .filter(
        col("review_id").isNotNull() &
        col("order_id").isNotNull()
    )
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

reviews_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_REVIEWS)
print(f"  ✓ {SILVER_REVIEWS}: {reviews_clean.count()} records")

# COMMAND ----------
# MAGIC %md ## Final validation

# COMMAND ----------

print("\nSilver layer summary:\n")
silver_tables = [
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS, SILVER_CUSTOMERS,
    SILVER_PRODUCTS, SILVER_SELLERS, SILVER_REVIEWS,
    SILVER_INVALID_ORDERS, SILVER_INVALID_PAYMENTS
]
for t in silver_tables:
    count = spark.table(t).count()
    print(f"  {t}: {count} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discussion points
# MAGIC
# MAGIC - **Explicit select on all tables:** defines the column order and contract for each table.
# MAGIC   Makes the schema predictable, facilitates code review, and prevents unwanted or duplicate
# MAGIC   columns from reaching Silver unintentionally.
# MAGIC - **Invalid record isolation:** records with null order_id or invalid status are not discarded —
# MAGIC   they go to `silver.invalid_orders` for auditing and investigation of the data source issue.
# MAGIC - **is_late as null:** undelivered orders receive `null` in `is_late`, not `False`.
# MAGIC   This prevents in-transit orders from distorting late rate analyses.
# MAGIC - **Broadcast join:** category translation table (71 records) is sent to all
# MAGIC   executors, avoiding a shuffle of the large products table.
# MAGIC - **Dates as StringType in Bronze:** conversion to TimestampType happens here in Silver,
# MAGIC   ensuring raw data is preserved exactly as it came from the source.
