from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    round,
    avg,
    count,
    coalesce,
    lit
)

spark = (
    SparkSession.builder
    .appName("Gold Product Ratings")
    .master("local[*]")
    .getOrCreate()
)

# Read Silver data
products = spark.read.parquet("data/silver/products")
items = spark.read.parquet("data/silver/order_items")
reviews = spark.read.parquet("data/silver/reviews")


# --------------------------------------------------
# 1. Aggregate sales by product
# --------------------------------------------------

product_sales = (
    items
    .groupBy("product_id")
    .agg(
        sum("quantity").alias("units_sold"),
        round(sum("line_total"), 2).alias("revenue")
    )
)


# --------------------------------------------------
# 2. Aggregate reviews by product
# --------------------------------------------------

product_reviews = (
    reviews
    .groupBy("product_id")
    .agg(
        round(avg("rating"), 2).alias("average_rating"),
        count("review_id").alias("review_count")
    )
)


# --------------------------------------------------
# 3. Join aggregated data with products
# --------------------------------------------------

p = products.alias("p")
s = product_sales.alias("s")
r = product_reviews.alias("r")

gold_product_ratings = (
    p
    .join(
        s,
        p.product_id == s.product_id,
        "left"
    )
    .join(
        r,
        p.product_id == r.product_id,
        "left"
    )
    .select(
        p.product_id,
        p.product_name,
        p.category,
        coalesce(s.units_sold, lit(0)).alias("units_sold"),
        coalesce(s.revenue, lit(0.0)).alias("revenue"),
        r.average_rating,
        coalesce(r.review_count, lit(0)).alias("review_count")
    )
)


# --------------------------------------------------
# 4. Write Gold data
# --------------------------------------------------

output_path = "data/gold/product_ratings"

gold_product_ratings.write.mode("overwrite").parquet(output_path)


# --------------------------------------------------
# 5. Verification
# --------------------------------------------------

print("Product Ratings Gold table created successfully")

print("Count:", gold_product_ratings.count())

gold_product_ratings.orderBy(
    "average_rating",
    ascending=False
).show(10, truncate=False)

gold_product_ratings.printSchema()


spark.stop()