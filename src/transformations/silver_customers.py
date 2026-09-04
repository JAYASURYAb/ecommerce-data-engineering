from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, current_date

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverCustomers")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")


# --------------------------------------------------
# 2. Read Bronze Customers
# --------------------------------------------------

bronze_path = "data/bronze/customers"
silver_path = "data/silver/customers"

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE DATA ==========")
df.show(5, truncate=False)

print("Bronze record count:", df.count())


# --------------------------------------------------
# 3. Transform Customer Data
# --------------------------------------------------

silver_df = (
    df

    # Remove unnecessary whitespace
    .withColumn("name", trim(col("name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))

    # Keep only valid customer IDs
    .filter(col("customer_id") > 0)

    # Registration date cannot be in the future
    .filter(col("registration_date") <= current_date())

    # Remove exact duplicate records
    .dropDuplicates()
)


# --------------------------------------------------
# 4. Display Silver Data
# --------------------------------------------------

print("\n========== SILVER DATA ==========")

silver_df.show(10, truncate=False)

print("\nSilver Schema:")
silver_df.printSchema()

print("\nSilver record count:", silver_df.count())


# --------------------------------------------------
# 5. Write Silver Data
# --------------------------------------------------

silver_df.write \
    .mode("overwrite") \
    .parquet(silver_path)

print(f"\nSilver data written to: {silver_path}")


# --------------------------------------------------
# 6. Verify Silver Data
# --------------------------------------------------

print("\n========== VERIFY SILVER ==========")

verified_df = spark.read.parquet(silver_path)

verified_df.show(5, truncate=False)

print("Verified Silver record count:", verified_df.count())


# --------------------------------------------------
# 7. Stop Spark
# --------------------------------------------------

spark.stop()

print("\nSpark session stopped.")
print("SILVER CUSTOMERS TRANSFORMATION SUCCESSFUL!")