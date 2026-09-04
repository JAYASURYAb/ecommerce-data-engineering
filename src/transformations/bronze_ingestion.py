from pyspark.sql import SparkSession
from pathlib import Path

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BronzeIngestion")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Session created successfully")
print("Spark version:", spark.version)


# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

raw_path = Path("data/raw")
bronze_path = Path("data/bronze")

bronze_path.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 3. List source CSV files
# --------------------------------------------------

csv_files = [
    "customers.csv",
    "orders.csv",
    "order_items.csv",
    "products.csv",
    "reviews.csv"
]


# --------------------------------------------------
# 4. Ingest each CSV independently
# --------------------------------------------------

for file_name in csv_files:

    print("\n" + "=" * 60)
    print(f"Processing: {file_name}")
    print("=" * 60)

    input_file = str(raw_path / file_name)

    # Remove .csv extension for Bronze folder name
    dataset_name = Path(file_name).stem

    output_path = str(bronze_path / dataset_name)

    # Read CSV
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_file)
    )

    print("\nSchema:")
    df.printSchema()

    print(f"Record count: {df.count()}")

    print("\nSample records:")
    df.show(5, truncate=False)

    # Write to Bronze as Parquet
    df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Bronze written to: {output_path}")


# --------------------------------------------------
# 5. Verify Bronze layer
# --------------------------------------------------

print("\n" + "=" * 60)
print("BRONZE INGESTION COMPLETED")
print("=" * 60)

for file_name in csv_files:

    dataset_name = Path(file_name).stem
    output_path = str(bronze_path / dataset_name)

    bronze_df = spark.read.parquet(output_path)

    print(
        f"{dataset_name}: "
        f"{bronze_df.count()} records"
    )


# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()

print("\nSpark session stopped.")
print("BRONZE INGESTION SUCCESSFUL!")