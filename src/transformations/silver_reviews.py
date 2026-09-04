from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_date

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverReviews")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")

# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

bronze_path = "data/bronze/reviews"
silver_path = "data/silver/reviews"

# --------------------------------------------------
# 3. Read Bronze Reviews
# --------------------------------------------------

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE REVIEWS ==========")
df.show(5, truncate=False)
print("Bronze record count:", df.count())

# --------------------------------------------------
# 4. Silver Transformations
# --------------------------------------------------

silver_df = (
    df
    # Clean review text
    .withColumn("review_text", trim(col("review_text")))

    # Validate IDs
    .filter(col("review_id") > 0)
    .filter(col("customer_id") > 0)
    .filter(col("product_id") > 0)

    # Rating must be between 1 and 5
    .filter(col("rating").between(1, 5))

    # Review date cannot be in the future
    .filter(col("review_date") <= current_date())

    # Remove exact duplicate records
    .dropDuplicates()
)

# --------------------------------------------------
# 5. Inspect Silver Data
# --------------------------------------------------

print("\n========== SILVER REVIEWS ==========")

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
print("SILVER REVIEWS TRANSFORMATION SUCCESSFUL!")