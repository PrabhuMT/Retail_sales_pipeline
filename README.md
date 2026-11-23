# Retail Sales Data Engineering Pipeline

## Project Overview

An end-to-end data engineering project built using **Python, PySpark, Apache Spark, Hive, and Spark SQL**, using real-world retail transaction data.

The pipeline performs data extraction, cleaning, quality validation, distributed processing, Parquet storage, Hive integration, SQL analytics, and automated PDF report generation with charts.

The project also implements date-based partitioning to improve query performance through Spark partition pruning.

## Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python |
| Big Data Processing | Apache Spark, PySpark |
| Data Warehouse | Apache Hive |
| Query Language | Spark SQL |
| Storage Format | Parquet |
| Data Preparation | Pandas, OpenPyXL |
| Data Visualization | Matplotlib |
| Report Generation | ReportLab |
| Dataset | UCI Online Retail |
| Development Environment | VS Code, Git Bash |

## Project Architecture

```text
              UCI Online Retail Dataset
                         |
                         v
                  Raw Excel Dataset
                         |
                         v
               Data Preparation (Pandas)
                         |
                         v
                    Data Cleaning
                         |
                         v
                Quality Validation
                         |
                         v
                 PySpark Ingestion
                         |
                         v
                  Parquet Storage
                         |
                         v
                  Hive Integration
                         |
                         v
                   Spark SQL
                         |
                         v
                 Business Analytics
                         |
              +----------+----------+
              |          |          |
           Monthly    Country     Product &
           Revenue    Revenue     Customer
              |          |          |
              +----------+----------+
                         |
                         v
                  Matplotlib Charts
                         |
                         v
                  PDF Report (ReportLab)
                         |
                         v
              Retail Sales Analytics Report
```

## Dataset Information

The project uses the UCI Online Retail dataset containing historical transactions from a UK-based online retailer.

### Prepared datasets

| Dataset | Records | Description |
|---|---:|---|
| Customers | 4,338 | Customer identifiers and country |
| Products | 3,665 | Product identifiers and descriptions |
| Sales | 397,884 | Retail transaction line items |

### Data Cleaning

The following records were excluded during preparation:

- Cancelled invoices.
- Transactions with invalid or non-positive quantities.
- Transactions with invalid or non-positive unit prices.
- Records with missing customer IDs.
- Records with missing product IDs or descriptions.
- Records with missing transaction dates.

The original dataset does not provide customer names, email addresses, or product categories. These attributes were not fabricated.

## Data Quality Validation

Implemented 11 data quality checks using Python and Pandas.

Validation checks include:

- Duplicate and missing customer IDs.
- Duplicate and missing product IDs.
- Duplicate and missing sale IDs.
- Invalid quantities.
- Invalid unit prices.
- Invalid line totals.
- Invalid customer references.
- Invalid product references.

**Result: 11/11 validation checks passed** on the prepared datasets.

## PySpark Data Processing

Used PySpark to:

- Read cleaned CSV datasets using explicit schemas.
- Process customer, product, and sales data.
- Store processed datasets in Parquet format.
- Read Parquet datasets for analytical processing.

### Parquet Storage Locations

```text
data/warehouse/
    customers/
    products/
    sales/
    sales_partitioned/
```

## Hive Integration

Registered processed Parquet datasets as Hive tables.

| Hive Table | Description |
|---|---|
| retail_db.customers | Customer information |
| retail_db.products | Product information |
| retail_db.sales_real | Original sales Parquet dataset |
| retail_db.sales_partitioned | Date-partitioned sales dataset |

The partitioned sales table uses:

- `sales_year`
- `sales_month`

Hive partitions were discovered and registered using `MSCK REPAIR TABLE`.

The partitioned table contains **397,884 records across 13 partitions**.

## Spark SQL Analytics

Implemented five analytical queries using Spark SQL.

### 1. Top 10 Products by Revenue

Identifies the top-performing products based on total revenue and quantity sold.

### 2. Monthly Revenue Analysis

Calculates monthly revenue to understand sales trends over time.

### 3. Top 10 Customers by Spending

Identifies the highest-spending customers and their number of distinct orders.

### 4. Average Order Value

Calculates:

- Total sales transactions.
- Distinct invoice count.
- Total revenue.
- Average order value.

### 5. Country-wise Revenue

Joins sales and customer datasets to calculate revenue by country.

### Key Business Metrics

| Metric | Result |
|---|---:|
| Total Sales Records | 397,884 |
| Distinct Invoices | 18,532 |
| Total Revenue | £8,911,407.90 |
| Average Order Value | £480.87 |

Average Order Value is calculated as total revenue divided by distinct invoice count.

## Automated PDF Analytics Report

Integrated **Matplotlib and ReportLab** into the Spark SQL analytics script to automatically generate a PDF report using actual query results.

Whenever the analytics script runs, it:

1. Executes the five Spark SQL queries.
2. Collects the analytical results.
3. Generates charts using Matplotlib.
4. Creates a formatted PDF report using ReportLab.
5. Saves the report and chart images in the project directory.

### Visualizations included

| Visualization | Description |
|---|---|
| Monthly Revenue | Monthly sales revenue trends |
| Country-wise Revenue | Revenue distribution across the top 15 countries |
| Top 10 Products | Products ranked by revenue |
| Top 10 Customers | Customers ranked by spending |

### PDF Report Contents

- Executive summary.
- Total revenue and sales KPIs.
- Average order value.
- Monthly revenue chart and table.
- Country-wise revenue chart and table.
- Top 10 products chart and table.
- Top 10 customers chart and table.

### Generated Report Structure

```text
reports/
│
├── Retail_Sales_Analytics_Report.pdf
│
└── charts/
    ├── monthly_revenue.png
    ├── country_revenue.png
    ├── top_products.png
    └── top_customers.png
```

The PDF is automatically refreshed whenever `run_real_analytics.py` executes.

## Partitioning and Performance Optimization

Implemented year- and month-based partitioning on the sales dataset.

```python
sales_df = sales_df.withColumn(
    "sales_year",
    year(col("sale_date")).cast("string")
).withColumn(
    "sales_month",
    lpad(month(col("sale_date")).cast("string"), 2, "0")
)

sales_df.write \
    .mode("overwrite") \
    .partitionBy("sales_year", "sales_month") \
    .parquet(output_path)
```

### Partition Pruning

Used `EXPLAIN` to inspect Spark execution plans and verify partition pruning.

For a November 2011 query, Spark scans the relevant partition:

```text
sales_year=2011/sales_month=11
```

This allows Spark to skip unrelated month partitions.

### Performance Benchmark

Compared the original Parquet dataset against the partitioned Parquet dataset using the same November 2011 analytical query.

Historical benchmark results:

| Metric | Original | Partitioned |
|---|---:|---:|
| Records | 64,531 | 64,531 |
| Revenue | £1,161,817.38 | £1,161,817.38 |
| Median Execution Time | 0.191 sec | 0.158 sec |

The earlier benchmark showed a **17.15% reduction in median execution time**.

Note: This is a historical local benchmark. After correcting Hive partition locations following the project folder rename, the benchmark should be rerun before treating this improvement as the final result.

## Project Structure

```text
Retail_sales_pipeline/
│
├── data/
│   ├── raw/
│   ├── processed/
│   │   └── real_retail/
│   └── warehouse/
│       ├── customers/
│       ├── products/
│       ├── sales/
│       └── sales_partitioned/
│
├── scripts/
│   ├── prepare_real_data.py
│   ├── quality_check.py
│   ├── ingest_to_parquet.py
│   ├── create_hive_tables.py
│   ├── update_hive_locations.py
│   ├── run_real_analytics.py
│   ├── create_partitioned_sales.py
│   ├── register_partitioned_sales.py
│   ├── fix_partition_locations.py
│   ├── diagnose_partitioned_sales.py
│   └── compare_partition_performance.py
│
├── reports/
│   ├── Retail_Sales_Analytics_Report.pdf
│   └── charts/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## How to Run the Project

### Prerequisites

- Python 3.12
- Java 17
- Apache Spark 3.5.9
- Hadoop Windows binaries and `winutils.exe`
- Apache Hive metastore configuration
- Git Bash

### 1. Activate Python Environment

```bash
source /e/Personal/.venv/Scripts/activate
```

### 2. Configure Hadoop

```bash
export HADOOP_HOME="E:/hadoop"
export JAVA_TOOL_OPTIONS="-Djava.library.path=E:/hadoop/bin"
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Execute the Pipeline

Run scripts from the project root in the following order:

```bash
python scripts/prepare_real_data.py

python scripts/quality_check.py

python scripts/ingest_to_parquet.py

python scripts/create_hive_tables.py

python scripts/update_hive_locations.py

python scripts/run_real_analytics.py

python scripts/create_partitioned_sales.py

python scripts/register_partitioned_sales.py

python scripts/compare_partition_performance.py
```

Ensure the source Excel dataset is available in the expected raw-data location before running the preparation script.

The Hive location correction scripts are useful when existing metadata references outdated project paths. They are not required on every normal run once the metadata is correct.

### 5. Generate the Analytics PDF

To regenerate the report independently after the data and Hive tables are ready:

```bash
python scripts/run_real_analytics.py
```

The report will be saved automatically:

```text
reports/Retail_Sales_Analytics_Report.pdf
```

## Key Learning Outcomes

- Built an end-to-end ETL pipeline using PySpark.
- Performed data cleaning and quality validation.
- Processed large transaction datasets using Spark DataFrames.
- Stored structured datasets in Parquet format.
- Integrated Parquet datasets with Hive.
- Implemented Hive external and partitioned tables.
- Used Spark SQL for business analytics.
- Verified partition pruning using execution plans.
- Compared query performance between partitioned and nonpartitioned datasets.
- Generated automated visual analytics reports using Matplotlib and ReportLab.

## Conclusion

This project demonstrates practical experience in building an end-to-end data engineering pipeline, processing real-world retail data, integrating Spark with Hive, performing SQL-based analytics, implementing partitioning techniques, and generating automated PDF reports with business visualizations.