
from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

PARTITION_PATH = (
    BASE_DIR / "data" / "warehouse" / "sales_partitioned"
).resolve()

spark = (
    SparkSession.builder
    .appName("RegisterPartitionedSales")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS retail_db")

table_name = "retail_db.sales_partitioned"

# Verify source data
df = spark.read.parquet(str(PARTITION_PATH))

print("Parquet record count:", df.count())

# Register partitioned table
if not spark.catalog.tableExists(table_name):

    spark.sql(f"""
        CREATE TABLE {table_name} (
            sale_id STRING,
            invoice_no STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            sale_date TIMESTAMP,
            unit_price DOUBLE,
            line_total DOUBLE,
            sales_year STRING,
            sales_month STRING
        )
        USING PARQUET
        PARTITIONED BY (sales_year, sales_month)
        LOCATION '{PARTITION_PATH.as_uri()}'
    """)

# Discover existing partition directories
spark.sql(f"MSCK REPAIR TABLE {table_name}")

# Verify records
hive_count = spark.sql(
    f"SELECT COUNT(*) FROM {table_name}"
).first()[0]

print("\nHive partitioned table count:", hive_count)

# Show registered partitions
print("\nRegistered partitions:")

spark.sql(f"""
    SHOW PARTITIONS {table_name}
""").show(30, truncate=False)

# Test partition pruning
print("\nNovember 2011 records:")

spark.sql(f"""
    SELECT COUNT(*)
    FROM {table_name}
    WHERE sales_year = '2011'
      AND sales_month = '11'
""").show()

# Inspect execution plan
print("\nExecution plan:")

spark.sql(f"""
    EXPLAIN FORMATTED
    SELECT SUM(line_total)
    FROM {table_name}
    WHERE sales_year = '2011'
      AND sales_month = '11'
""").show(100, truncate=False)

spark.stop()