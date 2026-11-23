
import pandas as pd
from pathlib import Path

# Project directories
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_FILE = BASE_DIR / "data" / "raw" / "online_retail.xlsx"
OUTPUT_DIR = BASE_DIR / "data" / "processed" / "real_retail"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("Reading UCI Online Retail dataset...")

# Load source data
df = pd.read_excel(RAW_FILE, engine="openpyxl")

print("Original records:", len(df))
print("Original columns:", df.columns.tolist())

# Standardize column names
df.columns = df.columns.str.strip()

# Normalize identifiers
df["InvoiceNo"] = df["InvoiceNo"].astype("string").str.strip()
df["StockCode"] = df["StockCode"].astype("string").str.strip()
df["CustomerID"] = (
    pd.to_numeric(df["CustomerID"], errors="coerce")
    .astype("Int64")
    .astype("string")
)

df["Description"] = df["Description"].astype("string").str.strip()
df["Country"] = df["Country"].astype("string").str.strip()

df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

# Remove cancelled invoices
df = df[
    ~df["InvoiceNo"].str.upper().str.startswith("C", na=False)
].copy()

# Keep valid sales records
df = df[
    (df["Quantity"] > 0) &
    (df["UnitPrice"] > 0) &
    (df["CustomerID"].notna()) &
    (df["StockCode"].notna()) &
    (df["Description"].notna()) &
    (df["InvoiceDate"].notna())
].copy()

print("Valid sales records:", len(df))

# CUSTOMER DIMENSION
customers = (
    df.groupby("CustomerID")
    .agg(
        country=("Country", lambda x: x.mode().iloc[0]
                 if not x.mode().empty else x.iloc[0])
    )
    .reset_index()
)

customers = customers.rename(
    columns={"CustomerID": "customer_id"}
)

# PRODUCT DIMENSION
products = (
    df.groupby("StockCode")
    .agg(
        product_name=("Description", lambda x: x.mode().iloc[0]
                      if not x.mode().empty else x.iloc[0])
    )
    .reset_index()
)

products = products.rename(
    columns={"StockCode": "product_id"}
)

# SALES FACT TABLE
sales = df[
    [
        "InvoiceNo",
        "CustomerID",
        "StockCode",
        "Quantity",
        "InvoiceDate",
        "UnitPrice"
    ]
].copy()

sales = sales.reset_index(drop=True)

sales.insert(
    0,
    "sale_id",
    ["SALE" + str(i).zfill(8)
     for i in range(1, len(sales) + 1)]
)

sales = sales.rename(columns={
    "InvoiceNo": "invoice_no",
    "CustomerID": "customer_id",
    "StockCode": "product_id",
    "Quantity": "quantity",
    "InvoiceDate": "sale_date",
    "UnitPrice": "unit_price"
})

sales["sale_date"] = (
    sales["sale_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
)

sales["line_total"] = (
    sales["quantity"] * sales["unit_price"]
).round(2)

# Save CSV files
customers.to_csv(
    OUTPUT_DIR / "customers.csv",
    index=False
)

products.to_csv(
    OUTPUT_DIR / "products.csv",
    index=False
)

sales.to_csv(
    OUTPUT_DIR / "sales.csv",
    index=False
)

# Summary
print("\n========== DATA PREPARATION SUMMARY ==========")

print("Customers:", len(customers))
print("Products:", len(products))
print("Sales:", len(sales))

print("\nMissing values:")
print(sales.isnull().sum())

print("\nDuplicate sale IDs:", sales["sale_id"].duplicated().sum())
print("Invalid quantities:", (sales["quantity"] <= 0).sum())
print("Invalid prices:", (sales["unit_price"] <= 0).sum())

print("\nTotal revenue (GBP):", round(sales["line_total"].sum(), 2))

print("\nFiles generated successfully:")
print(OUTPUT_DIR)