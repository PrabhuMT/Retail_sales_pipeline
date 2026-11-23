
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "processed" / "real_retail"

customers = pd.read_csv(DATA_DIR / "customers.csv")
products = pd.read_csv(DATA_DIR / "products.csv")
sales = pd.read_csv(DATA_DIR / "sales.csv")

checks = []

def add_check(name, failed_records):
    status = "PASS" if failed_records == 0 else "FAIL"
    checks.append({
        "validation_name": name,
        "failed_records": failed_records,
        "status": status
    })

# CUSTOMER VALIDATIONS
add_check(
    "Duplicate customer ID",
    customers["customer_id"].duplicated().sum()
)

add_check(
    "Missing customer ID",
    customers["customer_id"].isna().sum()
)

# PRODUCT VALIDATIONS
add_check(
    "Duplicate product ID",
    products["product_id"].duplicated().sum()
)

add_check(
    "Missing product ID",
    products["product_id"].isna().sum()
)

# SALES VALIDATIONS
add_check(
    "Duplicate sale ID",
    sales["sale_id"].duplicated().sum()
)

add_check(
    "Missing sale ID",
    sales["sale_id"].isna().sum()
)

add_check(
    "Invalid sale quantity",
    (sales["quantity"] <= 0).sum()
)

add_check(
    "Invalid unit price",
    (sales["unit_price"] <= 0).sum()
)

add_check(
    "Invalid line total",
    (
        (sales["line_total"] -
         sales["quantity"] * sales["unit_price"]).abs() > 0.01
    ).sum()
)

# REFERENTIAL INTEGRITY
add_check(
    "Invalid customer references",
    (~sales["customer_id"].isin(customers["customer_id"])).sum()
)

add_check(
    "Invalid product references",
    (~sales["product_id"].isin(products["product_id"])).sum()
)

# REPORT
report = pd.DataFrame(checks)

print("\n========== DATA QUALITY REPORT ==========")
print(report.to_string(index=False))

print("\n========== DATA SUMMARY ==========")
print("Customers:", len(customers))
print("Products:", len(products))
print("Sales:", len(sales))

print(
    "Total Revenue:",
    round(sales["line_total"].sum(), 2),
    "GBP"
)

print("\nPASS:", (report["status"] == "PASS").sum())
print("FAIL:", (report["status"] == "FAIL").sum())

# Save report
OUTPUT_DIR = BASE_DIR / "data" / "processed"
report.to_csv(
    OUTPUT_DIR / "quality_check_report.csv",
    index=False
)

print("\nQuality report saved successfully.")