
from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]
WAREHOUSE_DIR = BASE_DIR / "data" / "warehouse"

# Start Spark with Hive support
spark = (
    SparkSession.builder
    .appName("RetailHiveIntegration")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Create database
spark.sql("""
    CREATE DATABASE IF NOT EXISTS retail_db
""")

spark.sql("USE retail_db")

# Parquet locations
customers_path = (
    WAREHOUSE_DIR / "customers"
).resolve().as_uri()

products_path = (
    WAREHOUSE_DIR / "products"
).resolve().as_uri()

sales_path = (
    WAREHOUSE_DIR / "sales"
).resolve().as_uri()

# Register Parquet datasets as Hive tables
tables = {
    "customers": customers_path,
    "products": products_path,
    "sales_real": sales_path
}

for table_name, location in tables.items():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING PARQUET
        LOCATION '{location}'
    """)

    print(f"Registered table: {table_name}")

# Verify registered tables
print("\n========== HIVE TABLES ==========")

spark.sql("SHOW TABLES IN retail_db").show(
    truncate=False
)

spark.stop()