
from pathlib import Path
from time import perf_counter
from statistics import median

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

spark = (
    SparkSession.builder
    .appName("PartitionPerformanceComparison")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Query 1: Original non-partitioned table
original_query = """
    SELECT
        COUNT(*) AS total_records,
        ROUND(SUM(line_total), 2) AS total_revenue
    FROM retail_db.sales_real
    WHERE sale_date >= TIMESTAMP '2011-11-01 00:00:00'
      AND sale_date < TIMESTAMP '2011-12-01 00:00:00'
"""

# Query 2: Partitioned table
partitioned_query = """
    SELECT
        COUNT(*) AS total_records,
        ROUND(SUM(line_total), 2) AS total_revenue
    FROM retail_db.sales_partitioned
    WHERE sales_year = '2011'
      AND sales_month = '11'
"""

def benchmark(name, query, runs=3):

    print(f"\nRunning: {name}")

    timings = []
    result = None

    for i in range(runs):

        start = perf_counter()

        result = spark.sql(query).collect()

        elapsed = perf_counter() - start
        timings.append(elapsed)

        print(f"Run {i + 1}: {elapsed:.3f} seconds")

    print("Median:", round(median(timings), 3), "seconds")
    print("Result:", result)

    return median(timings), result

# Warm-up both queries
spark.sql(original_query).collect()
spark.sql(partitioned_query).collect()

# Benchmark
original_time, original_result = benchmark(
    "Original Parquet",
    original_query
)

partitioned_time, partitioned_result = benchmark(
    "Partitioned Parquet",
    partitioned_query
)

# Verify identical results
assert original_result == partitioned_result, (
    "Results do not match!"
)

print("\n========== PERFORMANCE COMPARISON ==========")

print("Original:", round(original_time, 3), "seconds")
print("Partitioned:", round(partitioned_time, 3), "seconds")

if partitioned_time < original_time:
    improvement = (
        (original_time - partitioned_time)
        / original_time
    ) * 100

    print(f"Performance improvement: {improvement:.2f}%")
else:
    print("Partitioned query was not faster in this test.")

print("\nBoth queries returned identical results.")

spark.stop()