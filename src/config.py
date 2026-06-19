# Default catalog on Databricks Free Edition
# Confirmed via SELECT current_catalog() with Spark 4.1.0
CATALOG = "workspace"

# Volume where CSVs are loaded
# Path: Catalog > workspace > default > Volumes > olist_raw
RAW_DATA_PATH = "/Volumes/workspace/default/olist_raw"

# Schemas with explicit catalog prefix
BRONZE_SCHEMA = "workspace.bronze"
SILVER_SCHEMA = "workspace.silver"
GOLD_SCHEMA = "workspace.gold"

# Full table names (catalog.schema.table)
BRONZE_ORDERS   = "workspace.bronze.orders"
BRONZE_ITEMS    = "workspace.bronze.order_items"
BRONZE_PAYMENTS = "workspace.bronze.payments"
BRONZE_CUSTOMERS = "workspace.bronze.customers"
BRONZE_PRODUCTS = "workspace.bronze.products"
BRONZE_SELLERS  = "workspace.bronze.sellers"
BRONZE_REVIEWS  = "workspace.bronze.reviews"
BRONZE_CATEGORY = "workspace.bronze.category_translation"

SILVER_ORDERS           = "workspace.silver.orders"
SILVER_ITEMS            = "workspace.silver.order_items"
SILVER_PAYMENTS         = "workspace.silver.payments"
SILVER_CUSTOMERS        = "workspace.silver.customers"
SILVER_PRODUCTS         = "workspace.silver.products"
SILVER_SELLERS          = "workspace.silver.sellers"
SILVER_REVIEWS          = "workspace.silver.reviews"
SILVER_INVALID_ORDERS   = "workspace.silver.invalid_orders"
SILVER_INVALID_PAYMENTS = "workspace.silver.invalid_payments"

GOLD_FACT       = "workspace.gold.fact_order_revenue"
GOLD_DAILY      = "workspace.gold.daily_revenue"
GOLD_STATE      = "workspace.gold.customer_state_revenue"
GOLD_CATEGORY   = "workspace.gold.product_category_revenue"
GOLD_SELLER     = "workspace.gold.seller_performance"
GOLD_PAYMENT    = "workspace.gold.payment_method_summary"
GOLD_DQ_SUMMARY = "workspace.gold.data_quality_summary"

SOURCE_FILES = {
    "orders":             "olist_orders_dataset.csv",
    "order_items":        "olist_order_items_dataset.csv",
    "payments":           "olist_order_payments_dataset.csv",
    "customers":          "olist_customers_dataset.csv",
    "products":           "olist_products_dataset.csv",
    "sellers":            "olist_sellers_dataset.csv",
    "reviews":            "olist_order_reviews_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
