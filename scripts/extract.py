
from pyspark.sql import SparkSession

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("RetailSalesETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define source paths
CUSTOMERS_PATH = "data/raw/customers.csv"
PRODUCTS_PATH = "data/raw/products.csv"
SALES_PATH = "data/raw/sales.csv"

# Read CSV files
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(CUSTOMERS_PATH)
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTS_PATH)
)

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SALES_PATH)
)

# Display customer data
print("\nCUSTOMERS DATA")
customers_df.show()

# Display product data
print("\nPRODUCTS DATA")
products_df.show()

# Display sales data
print("\nSALES DATA")
sales_df.show()

# Display schemas
print("\nCUSTOMERS SCHEMA")
customers_df.printSchema()

print("\nPRODUCTS SCHEMA")
products_df.printSchema()

print("\nSALES SCHEMA")
sales_df.printSchema()

# Display record counts
print("\nRECORD COUNTS")
print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Sales:", sales_df.count())

spark.stop()