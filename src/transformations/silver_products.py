from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, round

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverProducts")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")

# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

bronze_path = "data/bronze/products"
silver_path = "data/silver/products"

# --------------------------------------------------
# 3. Read Bronze Products
# --------------------------------------------------

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE PRODUCTS ==========")
df.show(5, truncate=False)
print("Bronze record count:", df.count())

# --------------------------------------------------
# 4. Silver Transformations
# --------------------------------------------------

silver_df = (
    df
    # Standardize text fields
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", lower(trim(col("category"))))
    .withColumn("supplier", trim(col("supplier")))

    # Validate IDs and price
    .filter(col("product_id") > 0)
    .filter(col("price") >= 0)

    # Standardize price
    .withColumn("price", round(col("price"), 2))

    # Remove exact duplicates
    .dropDuplicates()
)

# --------------------------------------------------
# 5. Inspect Silver Data
# --------------------------------------------------

print("\n========== SILVER PRODUCTS ==========")

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
print("SILVER PRODUCTS TRANSFORMATION SUCCESSFUL!")