import os
import sys

os.environ["HADOOP_HOME"] = "E:/hadoop"
os.environ["PATH"] = (os.environ["HADOOP_HOME"] + "/bin;" + os.environ["PATH"])
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("RetailDataQuality")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.extraLibraryPath", "E:/hadoop/bin")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read raw datasets
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/processed/real_retail/customers.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/processed/real_retail/products.csv")
)

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/processed/real_retail/sales.csv")
)

# Validation function
validation_results = []

def validate_count(df, condition, validation_name):
    failed_count = df.filter(condition).count()

    status = "PASS" if failed_count == 0 else "FAIL"

    validation_results.append((
        validation_name,
        failed_count,
        status
    ))

    print(
        f"{validation_name}: {status} "
        f"| Failed records: {failed_count}"
    )

# CUSTOMER VALIDATIONS
print("\nCUSTOMER VALIDATIONS")

validate_count(
    customers_df,
    col("customer_id").isNull(),
    "Missing customer ID"
)

validate_count(
    customers_df,
    col("customer_id").isNotNull() &
    col("customer_id").isin(
        customers_df.groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .select("customer_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    ),
    "Duplicate customer ID"
)

# PRODUCT VALIDATIONS
print("\nPRODUCT VALIDATIONS")

validate_count(
    products_df,
    col("product_id").isNull(),
    "Missing product ID"
)

validate_count(
    products_df,
    col("product_id").isNotNull() &
    col("product_id").isin(
        products_df.groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .select("product_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    ),
    "Duplicate product ID"
)

# SALES VALIDATIONS
print("\nSALES VALIDATIONS")

validate_count(
    sales_df,
    col("sale_id").isNull(),
    "Missing sale ID"
)

validate_count(
    sales_df,
    col("quantity") <= 0,
    "Invalid sale quantity"
)

# Duplicate sales
duplicate_sales = (
    sales_df.groupBy("sale_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_sales_records = sales_df.join(
    duplicate_sales.select("sale_id"),
    on="sale_id",
    how="inner"
)

validate_count(
    duplicate_sales_records,
    col("sale_id").isNotNull(),
    "Duplicate sales"
)

# Invalid customer references
invalid_customers = sales_df.join(
    customers_df.select("customer_id").distinct(),
    on="customer_id",
    how="left_anti"
)

print("Invalid customer references:", invalid_customers.count())

validate_count(
    invalid_customers,
    col("customer_id").isNotNull(),
    "Invalid customer references"
)

# Products with valid IDs and positive prices
valid_products = products_df.filter(
    col("product_id").isNotNull()
).select("product_id").distinct()

# Find sales referencing missing or invalid products
invalid_products = sales_df.join(
    valid_products,
    on="product_id",
    how="left_anti"
)

validate_count(
    invalid_products,
    col("product_id").isNotNull(),
    "Invalid product references"
)

# Create validation report DataFrame
report_df = spark.createDataFrame(
    validation_results,
    ["validation_name", "failed_records", "status"]
)

# Display report
print("\nDATA QUALITY REPORT")
report_df.show(truncate=False)

# Save report
report_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("data/processed/real_validation_report")

print("Validation report generated successfully!")

spark.stop()