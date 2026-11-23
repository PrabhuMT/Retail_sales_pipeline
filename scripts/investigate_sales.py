
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailSalesInvestigation")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("USE retail_db")

# 1. Investigate unusual product
print("\nQUERY 1: PRODUCT 23843")

spark.sql("""
    SELECT
        s.product_id,
        p.product_name,
        SUM(s.quantity) AS total_quantity,
        SUM(s.line_total) AS revenue,
        COUNT(DISTINCT s.invoice_no) AS invoices
    FROM sales_real s
    JOIN products p
        ON s.product_id = p.product_id
    WHERE s.product_id = '23843'
    GROUP BY s.product_id, p.product_name
""").show(truncate=False)


# 2. Investigate unusual customer
print("\nQUERY 2: CUSTOMER 16446")

spark.sql("""
    SELECT
        invoice_no,
        product_id,
        quantity,
        unit_price,
        line_total,
        sale_date
    FROM sales_real
    WHERE customer_id = '16446'
    ORDER BY line_total DESC
""").show(100, truncate=False)


# 3. Identify special product codes
print("\nQUERY 3: SPECIAL PRODUCT CODES")

spark.sql("""
    SELECT
        s.product_id,
        p.product_name,
        SUM(s.line_total) AS revenue
    FROM sales_real s
    JOIN products p
        ON s.product_id = p.product_id
    WHERE s.product_id IN ('POST', 'M')
    GROUP BY s.product_id, p.product_name
""").show(truncate=False)


# 4. Check date range
print("\nQUERY 4: SALES DATE RANGE")

spark.sql("""
    SELECT
        MIN(sale_date) AS first_sale,
        MAX(sale_date) AS last_sale
    FROM sales_real
""").show(truncate=False)


# 5. Inspect query execution plan
print("\nQUERY 5: EXECUTION PLAN")

spark.sql("""
    EXPLAIN FORMATTED
    SELECT
        product_id,
        SUM(line_total) AS revenue
    FROM sales_real
    WHERE product_id NOT IN ('POST', 'M')
    GROUP BY product_id
    ORDER BY revenue DESC
    LIMIT 10
""").show(100, truncate=False)

spark.stop()