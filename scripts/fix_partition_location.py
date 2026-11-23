
from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

PARTITION_ROOT = (
    BASE_DIR / "data" / "warehouse" / "sales_partitioned"
)

spark = (
    SparkSession.builder
    .appName("FixHivePartitionLocations")
    .enableHiveSupport()
    .getOrCreate()
)

try:
    table = "retail_db.sales_partitioned"

    if not PARTITION_ROOT.exists():
        raise FileNotFoundError(
            f"Partition directory not found: {PARTITION_ROOT}"
        )

    print("========== REPAIRING PARTITIONS ==========")

    # Discover any partitions not yet registered
    spark.sql(f"MSCK REPAIR TABLE {table}")

    # Retrieve registered partitions
    partitions = spark.sql(
        f"SHOW PARTITIONS {table}"
    ).collect()

    updated = 0

    for row in partitions:
        values = {}
    
        for item in row["partition"].split("/"):
            key, value = item.split("=", 1)
            values[key] = value
    
        year = values["sales_year"]
        month = values["sales_month"]
    
        partition_path = (
            PARTITION_ROOT
            / f"sales_year={year}"
            / f"sales_month={month}"
        )
    
        if not partition_path.exists():
            raise FileNotFoundError(
                f"Missing partition: {partition_path}"
            )
    
        # Preserve '=' in partition folder names
        location = "file:/" + partition_path.resolve().as_posix()
    
        spark.sql(f"""
            ALTER TABLE retail_db.sales_partitioned
            PARTITION (
                sales_year = '{year}',
                sales_month = '{month}'
            )
            SET LOCATION '{location}'
        """)
    
        print(f"Updated: {year}/{month} -> {location}")

    spark.catalog.refreshTable(table)

    print(f"\nTotal partitions updated: {updated}")

    print("\n========== VERIFYING COUNTS ==========")

    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {table}
    """).show()

    spark.sql(f"""
        SELECT COUNT(*) AS november_records,
               ROUND(SUM(line_total), 2) AS november_revenue
        FROM {table}
        WHERE sales_year = '2011'
          AND sales_month = '11'
    """).show()

finally:
    spark.stop()