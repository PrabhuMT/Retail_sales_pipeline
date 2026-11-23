
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, lpad

BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_PATH = BASE_DIR / "data" / "warehouse" / "sales_partitioned"

spark = (
    SparkSession.builder
    .appName("RetailSalesPartitioning")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read existing Hive sales table
sales_df = spark.table("retail_db.sales_real")

print("Original records:", sales_df.count())

# Create partition columns
partitioned_df = (
    sales_df
    .withColumn("sales_year", year(col("sale_date")).cast("string"))
    .withColumn(
        "sales_month",
        lpad(month(col("sale_date")).cast("string"), 2, "0")
    )
)

# Write partitioned Parquet
(
    partitioned_df.write
    .mode("overwrite")
    .partitionBy("sales_year", "sales_month")
    .parquet(str(OUTPUT_PATH))
)

print("\nPartitioned Parquet created!")

# Read partitioned data
result_df = spark.read.parquet(str(OUTPUT_PATH))

print("Partitioned records:", result_df.count())

print("\nPartition folders:")

for path in sorted(OUTPUT_PATH.iterdir()):
    if path.is_dir():
        print(path.name)

print("\nSample data:")
result_df.show(5, truncate=False)

spark.stop()