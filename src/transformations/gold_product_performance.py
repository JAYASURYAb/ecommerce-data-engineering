from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, coalesce, lit


spark = (
    SparkSession.builder
    .appName("Gold Product Performance")
    .master("local[*]")
    .getOrCreate()
)

# Read Silver data
products = spark.read.parquet("data/silver/products")
items = spark.read.parquet("data/silver/order_items")


# Calculate product sales metrics
product_sales = (
    items
    .groupBy("product_id")
    .agg(
        sum("quantity").alias("units_sold"),
        round(sum("line_total"), 2).alias("revenue")
    )
)


# Alias products
p = products.alias("p")


# Add product information
gold_product_performance = (
    p.join(
        product_sales,
        p.product_id == product_sales.product_id,
        "left"
    )
    .select(
        p.product_id,
        p.product_name,
        p.category,
        coalesce(product_sales.units_sold, lit(0)).alias("units_sold"),
        coalesce(product_sales.revenue, lit(0.0)).alias("revenue")
    )
)


# Display top products
gold_product_performance.orderBy(
    "revenue",
    ascending=False
).show(10)


# Write Gold dataset
gold_product_performance.write.mode("overwrite").parquet(
    "data/gold/product_performance"
)


spark.stop()