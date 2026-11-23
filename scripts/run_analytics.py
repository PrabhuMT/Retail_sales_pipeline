
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailSalesAnalytics")
    .master("local[*]")
    .config("spark.driver.extraLibraryPath", "E:/hadoop/bin")
    .config("spark.sql.warehouse.dir", "file:///E:/Personal/retail-data-engineering/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Select Hive database
spark.sql("USE retail_db")


# Read SQL file
with open("sql/analytics.sql", "r") as file:
    sql_content = file.read()

# Split multiple SQL queries using semicolon
queries = [
    query.strip()
    for query in sql_content.split(";")
    if query.strip()
]

# Execute each query
for index, query in enumerate(queries, start=1):

    print(f"\nExecuting Query {index}")
    print("-" * 50)

    result_df = spark.sql(query)

    result_df.show(truncate=False)

spark.stop()
