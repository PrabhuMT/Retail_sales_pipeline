
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

# Project paths
BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = BASE_DIR / "data" / "processed" / "real_retail"
OUTPUT_DIR = BASE_DIR / "data" / "warehouse"

# Spark session
spark = (
    SparkSession.builder
    .appName("RetailDataIngestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Explicit schemas
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True)
])

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True)
])

sales_schema = StructType([
    StructField("sale_id", StringType(), True),
    StructField("invoice_no", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_date", TimestampType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])

# Read CSV files
customers_df = (
    spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(str(INPUT_DIR / "customers.csv"))
)

products_df = (
    spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(str(INPUT_DIR / "products.csv"))
)

sales_df = (
    spark.read
    .option("header", True)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .schema(sales_schema)
    .csv(str(INPUT_DIR / "sales.csv"))
)

# Inspect schemas
print("\nCUSTOMER SCHEMA")
customers_df.printSchema()

print("\nPRODUCT SCHEMA")
products_df.printSchema()

print("\nSALES SCHEMA")
sales_df.printSchema()

# Record counts
print("\n========== RECORD COUNTS ==========")

print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Sales:", sales_df.count())

# Preview
print("\n========== SALES SAMPLE ==========")
sales_df.show(5, truncate=False)

# Save as Parquet
customers_df.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "customers")
)

products_df.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "products")
)

sales_df.write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "sales")
)

print("\nParquet files generated successfully!")

spark.stop()