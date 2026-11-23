
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "processed" / "real_retail"

files = ["customers.csv", "products.csv", "sales.csv"]

for file in files:
    df = pd.read_csv(DATA_DIR / file)

    print(f"\n{'=' * 50}")
    print(f"FILE: {file}")
    print(f"Records: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print("\nSample records:")
    print(df.head(5).to_string(index=False))

    print("\nMissing values:")
    print(df.isnull().sum().to_string())
