from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, round, coalesce, lit


spark = (
    SparkSession.builder
    .appName("Gold Customer Sales")
    .master("local[*]")
    .getOrCreate()
)

# Read Silver data
customers = spark.read.parquet("data/silver/customers")
orders = spark.read.parquet("data/silver/orders")
items = spark.read.parquet("data/silver/order_items")


# Aliases
o = orders.alias("o")
i = items.alias("i")


# Join orders with order items
order_details = o.join(
    i,
    o.order_id == i.order_id,
    "inner"
)


# Calculate customer sales metrics
customer_sales = (
    order_details
    .groupBy("o.customer_id")
    .agg(
        countDistinct("o.order_id").alias("total_orders"),
        sum("i.quantity").alias("total_items"),
        round(sum("i.line_total"), 2).alias("total_spend")
    )
)


# Add customer name
c = customers.alias("c")

gold_customer_sales = (
    c.join(
        customer_sales,
        c.customer_id == customer_sales.customer_id,
        "left"
    )
    .select(
        c.customer_id,
        c.name.alias("customer_name"),
        coalesce(customer_sales.total_orders, lit(0)).alias("total_orders"),
        coalesce(customer_sales.total_items, lit(0)).alias("total_items"),
        coalesce(customer_sales.total_spend, lit(0.0)).alias("total_spend")
    )
)


# Show result
gold_customer_sales.orderBy(
    "total_spend",
    ascending=False
).show(10)


# Write Gold dataset
gold_customer_sales.write.mode("overwrite").parquet(
    "data/gold/customer_sales"
)


spark.stop()