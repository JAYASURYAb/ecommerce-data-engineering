from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, round, year, month


spark = (
    SparkSession.builder
    .appName("Gold Monthly Sales")
    .master("local[*]")
    .getOrCreate()
)

# Read Silver data
orders = spark.read.parquet("data/silver/orders")
items = spark.read.parquet("data/silver/order_items")


# Create aliases
o = orders.alias("o")
i = items.alias("i")


# Join orders and order items
order_details = o.join(
    i,
    o.order_id == i.order_id,
    "inner"
)


# Create monthly sales metrics
monthly_sales = (
    order_details
    .groupBy(
        year("o.order_date").alias("year"),
        month("o.order_date").alias("month")
    )
    .agg(
        countDistinct("o.order_id").alias("total_orders"),
        sum("i.quantity").alias("total_items"),
        round(sum("i.line_total"), 2).alias("revenue")
    )
    .orderBy("year", "month")
)


# Display result
monthly_sales.show(50)


# Write Gold dataset
monthly_sales.write.mode("overwrite").parquet(
    "data/gold/monthly_sales"
)


spark.stop()