# Spark concepts demonstrated in the project

## Lazy evaluation

Spark does not execute transformations immediately. Each `.withColumn`, `.filter`, `.join` only
adds a step to the **logical execution plan**. The actual execution only happens when an *action*
is called: `.count()`, `.write`, `.show()`, `.collect()`.

This allows Spark to optimize the plan before executing — for example, applying filters before
joins (predicate pushdown) even if the code defines them afterwards.

```python
df = spark.table("workspace.silver.orders")    # no execution
df = df.filter(col("order_status") == "delivered")  # no execution
df = df.select("order_id", "customer_id")           # no execution
count = df.count()                                   # execution happens here
```

---

## Explicit schemas (StructType)

`inferSchema=True` performs two file scans and may infer incorrect types (e.g.: `review_score`
inferred as `Long` when it is `Integer`). The project defines all schemas explicitly in
`src/schemas.py`:

```python
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
    ...
])
```

Dates are intentionally kept as `StringType` in Bronze — conversion is Silver's responsibility,
which also validates the format.

---

## Broadcast join

In a standard join (sort-merge join), Spark shuffles both tables across the network to place
records with the same key on the same executor. When one of the tables is small (< ~10 MB), the
**broadcast join** sends a copy of it to all executors — the join happens locally,
without shuffling the large table.

Used in the project for the category translation table (71 rows) with `silver.products`:

```python
products.join(
    broadcast(category_translation.select("product_category_name", "product_category_name_english")),
    "product_category_name", "left"
)
```

---

## Window functions

Allow calculating values that depend on other rows in the same group without needing a subquery.
Used in the project to identify the primary payment method (highest value) per order:

```python
w = Window.partitionBy("order_id").orderBy(col("payment_value").desc())

main_payment = (
    payments
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .select("order_id", col("payment_type").alias("main_payment_type"))
)
```

`row_number()` numbers the rows within each `order_id`, ordered by value desc. Taking
`rn == 1` selects the highest-value payment per order.

---

## Partitioning

The `gold.fact_order_revenue` table is partitioned by `order_purchase_date`:

```python
fact.write.format("delta")
    .partitionBy("order_purchase_date")
    .saveAsTable("workspace.gold.fact_order_revenue")
```

Queries filtering by date (`WHERE order_purchase_date = '2018-01-01'`) read only the
files for the corresponding partition — **partition pruning**. Spark does not open files from
other dates.

---

## OPTIMIZE and ZORDER

Delta Lake accumulates small Parquet files with each write. `OPTIMIZE` compacts them.
`ZORDER BY` physically reorganizes the data so that similar values are in the same block.

Delta stores `min/max` statistics per column in each block **automatically** on all writes.
ZORDER does not create the statistics — it reorganizes the data so that the statistics become
useful: when blocks have non-overlapping min/max ranges, Spark can skip most of them when
filtering (**data skipping**).

```sql
OPTIMIZE workspace.gold.fact_order_revenue
ZORDER BY (customer_state, order_status);
```

---

## Delta time travel

Delta maintains a transaction log with all versions of the table. Each `write` or `MERGE`
creates a numbered version. It is possible to query previous versions:

```python
# By version number
spark.read.format("delta").option("versionAsOf", 0).table("workspace.gold.fact_order_revenue")

# By timestamp
spark.read.format("delta").option("timestampAsOf", "2024-01-01").table(...)

# Operation history
spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").show()
```

Old files are preserved until `VACUUM` is run (minimum recommended: 7 days).

---

## Multiline CSV and RFC 4180

The `olist_order_reviews_dataset.csv` file has two parsing issues:

1. **Line breaks within fields** — `review_comment_message` may contain `\n`.
   Fixed with `.option("multiLine", True)`.

2. **Quotes escaped with `""`** — RFC 4180 standard (`""hello""` = `"hello"`).
   Spark uses `\` as the default escape — to understand `""`, it requires
   `.option("escape", '"')`.

Without these options, fields subsequent to the line break or quotes get misaligned,
causing comment text to appear in the `review_creation_date` column.
