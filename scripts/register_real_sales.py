
from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]
SALES_PATH = (BASE_DIR / "data" / "warehouse" / "sales").resolve()

spark = (
    SparkSession.builder
    .appName("RegisterRealSales")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Verify the new Parquet directory
sales_df = spark.read.parquet(str(SALES_PATH))

print("Direct Parquet record count:", sales_df.count())
print("Number of Parquet files:", len(sales_df.inputFiles()))

# 2. Create database
spark.sql("CREATE DATABASE IF NOT EXISTS retail_db")

# 3. Register using a new table name
table_name = "retail_db.sales_real"

if spark.catalog.tableExists(table_name):
    raise Exception(
        f"{table_name} already exists. "
        "Inspect its location before replacing it."
    )

spark.sql(f"""
    CREATE TABLE {table_name} (
        sale_id STRING,
        invoice_no STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        sale_date TIMESTAMP,
        unit_price DOUBLE,
        line_total DOUBLE
    )
    USING PARQUET
    LOCATION '{SALES_PATH.as_uri()}'
""")

# 4. Verify Hive table
print("\n========== NEW HIVE SALES TABLE ==========")

spark.sql(f"DESCRIBE {table_name}").show(
    truncate=False
)

hive_count = spark.sql(
    f"SELECT COUNT(*) FROM {table_name}"
).first()[0]

print("New Hive sales count:", hive_count)

# 5. Verify sample data
spark.sql(f"""
    SELECT *
    FROM {table_name}
    LIMIT 5
""").show(truncate=False)

spark.stop()
