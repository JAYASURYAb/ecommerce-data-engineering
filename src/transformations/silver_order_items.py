from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverOrderItems")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")

# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

bronze_path = "data/bronze/order_items"
silver_path = "data/silver/order_items"

# --------------------------------------------------
# 3. Read Bronze Order Items
# --------------------------------------------------

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE ORDER ITEMS ==========")
df.show(5, truncate=False)
print("Bronze record count:", df.count())

# --------------------------------------------------
# 4. Silver Transformations
# --------------------------------------------------

silver_df = (
    df
    # Data-quality rules
    .filter(col("order_id") > 0)
    .filter(col("product_id") > 0)
    .filter(col("quantity") > 0)
    .filter(col("price") >= 0)

    # Standardize price
    .withColumn("price", round(col("price"), 2))

    # Calculate total value for each order item
    .withColumn(
        "line_total",
        round(col("quantity") * col("price"), 2)
    )

    # Remove exact duplicate records
    .dropDuplicates()
)

# --------------------------------------------------
# 5. Inspect Silver Data
# --------------------------------------------------

print("\n========== SILVER ORDER ITEMS ==========")

silver_df.show(10, truncate=False)

print("\nSilver Schema:")
silver_df.printSchema()

print("\nSilver record count:", silver_df.count())

# --------------------------------------------------
# 6. Write Silver Parquet
# --------------------------------------------------

silver_df.write \
    .mode("overwrite") \
    .parquet(silver_path)

print(f"\nSilver data written to: {silver_path}")

# --------------------------------------------------
# 7. Verify Silver Data
# --------------------------------------------------

print("\n========== VERIFY SILVER ==========")

verified_df = spark.read.parquet(silver_path)

verified_df.show(5, truncate=False)

print("Verified Silver record count:", verified_df.count())

# --------------------------------------------------
# 8. Stop Spark
# --------------------------------------------------

spark.stop()

print("\nSpark session stopped.")
print("SILVER ORDER ITEMS TRANSFORMATION SUCCESSFUL!")