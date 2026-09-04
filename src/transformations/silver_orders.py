from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, current_date

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverOrders")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")

# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

bronze_path = "data/bronze/orders"
silver_path = "data/silver/orders"

# --------------------------------------------------
# 3. Read Bronze Orders
# --------------------------------------------------

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE ORDERS ==========")
df.show(5, truncate=False)
print("Bronze record count:", df.count())

# --------------------------------------------------
# 4. Silver Transformations
# --------------------------------------------------

silver_df = (
    df
    # Standardize text columns
    .withColumn("status", lower(trim(col("status"))))
    .withColumn("payment_method", lower(trim(col("payment_method"))))

    # Data-quality rules
    .filter(col("order_id") > 0)
    .filter(col("customer_id") > 0)
    .filter(col("order_date") <= current_date())

    # Valid order statuses
    .filter(
        col("status").isin(
            "pending",
            "shipped",
            "completed",
            "cancelled"
        )
    )

    # Valid payment methods
    .filter(
        col("payment_method").isin(
            "credit card",
            "net banking",
            "debit card",
            "cash on delivery",
            "upi"
        )
    )

    # Remove exact duplicate records
    .dropDuplicates()
)

# --------------------------------------------------
# 5. Inspect Silver Data
# --------------------------------------------------

print("\n========== SILVER ORDERS ==========")

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
print("SILVER ORDERS TRANSFORMATION SUCCESSFUL!")