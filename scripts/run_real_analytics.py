
from pathlib import Path
from datetime import datetime
from xml.sax.saxutils import escape

import matplotlib.pyplot as plt

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
    PageBreak
)

from pyspark.sql import SparkSession


# ==========================================================
# 1. PROJECT PATHS
# ==========================================================

BASE_DIR = Path(__file__).resolve().parents[1]

REPORT_DIR = BASE_DIR / "reports"
CHART_DIR = REPORT_DIR / "charts"

REPORT_DIR.mkdir(parents=True, exist_ok=True)
CHART_DIR.mkdir(parents=True, exist_ok=True)

PDF_PATH = REPORT_DIR / "Retail_Sales_Analytics_Report.pdf"


# ==========================================================
# 2. SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("RetailSalesAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================================
# 3. HELPER FUNCTIONS
# ==========================================================

def format_currency(value):
    return f"£{float(value or 0):,.2f}"


def create_chart(filename, title, labels, values,
                 chart_type="bar", horizontal=False):

    chart_path = CHART_DIR / filename

    plt.figure(figsize=(10, 5))

    if horizontal:
        plt.barh(labels[::-1], values[::-1])
        plt.xlabel("Revenue (£)")
    else:
        plt.bar(labels, values)
        plt.ylabel("Revenue (£)")

        if len(labels) > 8:
            plt.xticks(rotation=45, ha="right")

    plt.title(title)
    plt.grid(axis="x" if horizontal else "y", alpha=0.25)
    plt.tight_layout()

    plt.savefig(chart_path, dpi=180, bbox_inches="tight")
    plt.close()

    return chart_path


def create_table(data, widths=None):

    table = Table(data, colWidths=widths, repeatRows=1)

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0),
         colors.HexColor("#17324D")),

        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),

        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),

        ("FONTSIZE", (0, 0), (-1, -1), 8),

        ("GRID", (0, 0), (-1, -1),
         0.4, colors.HexColor("#D8E1E8")),

        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#F2F5F8")]),

        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),

        ("TOPPADDING", (0, 0), (-1, -1), 6),

        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))

    return table


# ==========================================================
# 4. RUN ANALYTICS AND COLLECT RESULTS
# ==========================================================

try:

    spark.sql("USE retail_db")

    print("\n========== AVAILABLE HIVE TABLES ==========")

    spark.sql("SHOW TABLES").show(truncate=False)

    # Verify dataset
    sales_count = spark.sql("""
        SELECT COUNT(*) AS total_records
        FROM sales_real
    """).first()["total_records"]

    print("\nSales records:", sales_count)


    # ------------------------------------------------------
    # QUERY 1: TOP 10 PRODUCTS BY REVENUE
    # ------------------------------------------------------

    print("\nQUERY 1: TOP 10 PRODUCTS BY REVENUE")

    product_df = spark.sql("""
        SELECT
            product_id,
            SUM(quantity) AS total_quantity,
            ROUND(SUM(line_total), 2) AS total_revenue
        FROM sales_real
        GROUP BY product_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """)

    product_df.show(truncate=False)

    product_results = product_df.collect()


    # ------------------------------------------------------
    # QUERY 2: MONTHLY REVENUE
    # ------------------------------------------------------

    print("\nQUERY 2: MONTHLY REVENUE")

    monthly_df = spark.sql("""
        SELECT
            DATE_FORMAT(sale_date, 'yyyy-MM') AS sales_month,
            ROUND(SUM(line_total), 2) AS monthly_revenue
        FROM sales_real
        GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
        ORDER BY sales_month
    """)

    monthly_df.show(100, truncate=False)

    monthly_results = monthly_df.collect()


    # ------------------------------------------------------
    # QUERY 3: TOP 10 CUSTOMERS
    # ------------------------------------------------------

    print("\nQUERY 3: TOP CUSTOMERS BY SPENDING")

    customer_df = spark.sql("""
        SELECT
            customer_id,
            COUNT(DISTINCT invoice_no) AS total_orders,
            ROUND(SUM(line_total), 2) AS total_spending
        FROM sales_real
        GROUP BY customer_id
        ORDER BY total_spending DESC
        LIMIT 10
    """)

    customer_df.show(truncate=False)

    customer_results = customer_df.collect()


    # ------------------------------------------------------
    # QUERY 4: AVERAGE ORDER VALUE
    # ------------------------------------------------------

    print("\nQUERY 4: AVERAGE ORDER VALUE")

    kpi_df = spark.sql("""
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(DISTINCT invoice_no) AS total_orders,
            ROUND(SUM(line_total), 2) AS total_revenue,
            ROUND(
                SUM(line_total) / COUNT(DISTINCT invoice_no),
                2
            ) AS average_order_value
        FROM sales_real
    """)

    kpi_df.show(truncate=False)

    kpi = kpi_df.first()


    # ------------------------------------------------------
    # QUERY 5: COUNTRY-WISE REVENUE
    # ------------------------------------------------------

    print("\nQUERY 5: COUNTRY-WISE REVENUE")

    country_df = spark.sql("""
        SELECT
            c.country,
            ROUND(SUM(s.line_total), 2) AS total_revenue
        FROM sales_real s
        INNER JOIN customers c
            ON s.customer_id = c.customer_id
        GROUP BY c.country
        ORDER BY total_revenue DESC
        LIMIT 15
    """)

    country_df.show(truncate=False)

    country_results = country_df.collect()


    # ======================================================
    # 5. GENERATE CHARTS FROM SPARK RESULTS
    # ======================================================

    print("\n========== GENERATING CHARTS ==========")

    # Monthly revenue
    monthly_chart = create_chart(
        "monthly_revenue.png",
        "Monthly Revenue",
        [r["sales_month"] for r in monthly_results],
        [float(r["monthly_revenue"]) for r in monthly_results]
    )

    # Country revenue
    country_chart = create_chart(
        "country_revenue.png",
        "Revenue by Country - Top 15",
        [r["country"] for r in country_results],
        [float(r["total_revenue"]) for r in country_results],
        horizontal=True
    )

    # Top products
    product_chart = create_chart(
        "top_products.png",
        "Top 10 Products by Revenue",
        [r["product_id"] for r in product_results],
        [float(r["total_revenue"]) for r in product_results]
    )

    # Top customers
    customer_chart = create_chart(
        "top_customers.png",
        "Top 10 Customers by Spending",
        [str(r["customer_id"]) for r in customer_results],
        [float(r["total_spending"]) for r in customer_results]
    )

    print("Charts generated successfully.")


    # ======================================================
    # 6. CREATE PDF REPORT
    # ======================================================

    print("\n========== GENERATING PDF REPORT ==========")

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="ReportTitle",
        parent=styles["Title"],
        fontSize=24,
        leading=29,
        textColor=colors.HexColor("#17324D"),
        alignment=0
    ))

    styles.add(ParagraphStyle(
        name="SectionTitle",
        parent=styles["Heading2"],
        fontSize=15,
        textColor=colors.HexColor("#17324D"),
        spaceBefore=8,
        spaceAfter=10
    ))

    styles.add(ParagraphStyle(
        name="ReportBody",
        parent=styles["BodyText"],
        fontSize=9,
        leading=14,
        spaceAfter=8
    ))

    styles.add(ParagraphStyle(
        name="KPIValue",
        parent=styles["BodyText"],
        fontSize=15,
        leading=18,
        alignment=1,
        textColor=colors.HexColor("#17324D")
    ))

    styles.add(ParagraphStyle(
        name="KPILabel",
        parent=styles["BodyText"],
        fontSize=8,
        alignment=1,
        textColor=colors.HexColor("#526579")
    ))

    story = []

    # ------------------------------------------------------
    # REPORT HEADER
    # ------------------------------------------------------

    story.append(Spacer(1, 10 * mm))

    story.append(Paragraph(
        "Retail Sales Analytics Report",
        styles["ReportTitle"]
    ))

    story.append(Paragraph(
        "PySpark | Apache Hive | Spark SQL | Parquet",
        styles["ReportBody"]
    ))

    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%d %B %Y %H:%M')}",
        styles["ReportBody"]
    ))

    story.append(Spacer(1, 8 * mm))


    # ------------------------------------------------------
    # KPI SUMMARY
    # ------------------------------------------------------

    kpi_data = [
        [
            Paragraph(
                format_currency(kpi["total_revenue"]),
                styles["KPIValue"]
            ),
            Paragraph(
                f"{kpi['total_transactions']:,}",
                styles["KPIValue"]
            ),
            Paragraph(
                f"{kpi['total_orders']:,}",
                styles["KPIValue"]
            ),
            Paragraph(
                format_currency(kpi["average_order_value"]),
                styles["KPIValue"]
            )
        ],
        [
            Paragraph("Total Revenue", styles["KPILabel"]),
            Paragraph("Sales Records", styles["KPILabel"]),
            Paragraph("Total Orders", styles["KPILabel"]),
            Paragraph("Average Order Value", styles["KPILabel"])
        ]
    ]

    kpi_table = Table(
        kpi_data,
        colWidths=[42 * mm] * 4,
        rowHeights=[13 * mm, 10 * mm]
    )

    kpi_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1),
         colors.HexColor("#F1F5F8")),

        ("BOX", (0, 0), (-1, -1),
         0.6, colors.HexColor("#D8E1E8")),

        ("INNERGRID", (0, 0), (-1, -1),
         0.4, colors.HexColor("#D8E1E8")),

        ("VALIGN", (0, 0), (-1, -1), "MIDDLE")
    ]))

    story.append(kpi_table)

    story.append(Spacer(1, 8 * mm))


    # ------------------------------------------------------
    # EXECUTIVE SUMMARY
    # ------------------------------------------------------

    story.append(Paragraph(
        "Executive Summary",
        styles["SectionTitle"]
    ))

    summary = (
        f"The retail dataset contains {sales_count:,} sales records "
        f"across {kpi['total_orders']:,} distinct invoices. "
        f"Total reported revenue is "
        f"{format_currency(kpi['total_revenue'])}, with an "
        f"average order value of "
        f"{format_currency(kpi['average_order_value'])}."
    )

    story.append(Paragraph(summary, styles["ReportBody"]))

    story.append(PageBreak())


    # ------------------------------------------------------
    # MONTHLY REVENUE
    # ------------------------------------------------------

    story.append(Paragraph(
        "1. Monthly Revenue Analysis",
        styles["SectionTitle"]
    ))

    story.append(Image(
        str(monthly_chart),
        width=170 * mm,
        height=82 * mm
    ))

    monthly_table = [["Month", "Revenue"]]

    monthly_table += [
        [
            r["sales_month"],
            format_currency(r["monthly_revenue"])
        ]
        for r in monthly_results
    ]

    story.append(create_table(
        monthly_table,
        [65 * mm, 65 * mm]
    ))

    story.append(PageBreak())


    # ------------------------------------------------------
    # COUNTRY REVENUE
    # ------------------------------------------------------

    story.append(Paragraph(
        "2. Country-wise Revenue",
        styles["SectionTitle"]
    ))

    story.append(Image(
        str(country_chart),
        width=165 * mm,
        height=90 * mm
    ))

    country_table = [["Country", "Total Revenue"]]

    country_table += [
        [
            r["country"],
            format_currency(r["total_revenue"])
        ]
        for r in country_results
    ]

    story.append(create_table(
        country_table,
        [75 * mm, 65 * mm]
    ))

    story.append(PageBreak())


    # ------------------------------------------------------
    # PRODUCT REVENUE
    # ------------------------------------------------------

    story.append(Paragraph(
        "3. Top 10 Products by Revenue",
        styles["SectionTitle"]
    ))

    story.append(Image(
        str(product_chart),
        width=165 * mm,
        height=85 * mm
    ))

    product_table = [
        ["Product ID", "Quantity", "Revenue"]
    ]

    product_table += [
        [
            str(r["product_id"]),
            f"{r['total_quantity']:,}",
            format_currency(r["total_revenue"])
        ]
        for r in product_results
    ]

    story.append(create_table(
        product_table,
        [45 * mm, 45 * mm, 55 * mm]
    ))

    story.append(PageBreak())


    # ------------------------------------------------------
    # CUSTOMER ANALYSIS
    # ------------------------------------------------------

    story.append(Paragraph(
        "4. Top 10 Customers by Spending",
        styles["SectionTitle"]
    ))

    story.append(Image(
        str(customer_chart),
        width=165 * mm,
        height=85 * mm
    ))

    customer_table = [
        ["Customer ID", "Orders", "Total Spending"]
    ]

    customer_table += [
        [
            str(r["customer_id"]),
            f"{r['total_orders']:,}",
            format_currency(r["total_spending"])
        ]
        for r in customer_results
    ]

    story.append(create_table(
        customer_table,
        [45 * mm, 45 * mm, 60 * mm]
    ))


    # ======================================================
    # 7. SAVE PDF
    # ======================================================

    def add_footer(canvas, doc):
        canvas.saveState()

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.grey)

        canvas.drawString(
            18 * mm,
            10 * mm,
            "Retail Sales Pipeline - Analytics Report"
        )

        canvas.drawRightString(
            190 * mm,
            10 * mm,
            f"Page {doc.page}"
        )

        canvas.restoreState()


    pdf = SimpleDocTemplate(
        str(PDF_PATH),
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=20 * mm
    )

    pdf.build(
        story,
        onFirstPage=add_footer,
        onLaterPages=add_footer
    )

    print("\n========================================")
    print("ANALYTICS REPORT GENERATED SUCCESSFULLY")
    print("========================================")
    print(f"PDF: {PDF_PATH}")
    print(f"Charts: {CHART_DIR}")
    print("========================================")


finally:
    spark.stop()