"""
01_load_to_postgres.py
======================
Loads all 9 Olist E-Commerce CSV files into PostgreSQL.

SETUP:
  1. Create a .env file in the /python directory with your DB credentials:
       DB_HOST=localhost
       DB_PORT=5432
       DB_NAME=supply_chain
       DB_USER=your_username
       DB_PASSWORD=your_password

  2. Install dependencies:
       pip install -r requirements.txt

  3. Make sure your PostgreSQL database exists:
       createdb supply_chain

  4. Place the 9 Olist CSVs in the ../data/ directory.

  5. Run this script:
       python 01_load_to_postgres.py

The script handles:
  - Column name normalization
  - Date/timestamp parsing
  - Numeric type casting
  - Schema creation
  - Duplicate/conflict handling
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Maps CSV filename → (table_name, parse_dates columns, dtype overrides)
OLIST_FILES = {
    "olist_customers_dataset.csv": {
        "table": "customers",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {
            "customer_zip_code_prefix": str,
        },
        "rename": {
            "customer_zip_code_prefix": "customer_zip_code"
        }
    },
    "olist_sellers_dataset.csv": {
        "table": "sellers",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {
            "seller_zip_code_prefix": str,
        },
        "rename": {
            "seller_zip_code_prefix": "seller_zip_code"
        }
    },
    "olist_product_category_name_translation.csv": {
        "table": "product_categories",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {},
        "rename": {}
    },
    "olist_products_dataset.csv": {
        "table": "products",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {
            "product_name_lenght":        "Int64",
            "product_description_lenght": "Int64",
            "product_photos_qty":         "Int64",
        },
        "rename": {
            "product_name_lenght":        "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    },
    "olist_orders_dataset.csv": {
        "table": "orders",
        "schema": "olist",
        "parse_dates": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "dtype": {},
        "rename": {
            "order_delivered_carrier_date": "order_carrier_delivery_date"
        }
    },
    "olist_order_items_dataset.csv": {
        "table": "order_items",
        "schema": "olist",
        "parse_dates": ["shipping_limit_date"],
        "dtype": {
            "price":         float,
            "freight_value": float,
        },
        "rename": {}
    },
    "olist_order_payments_dataset.csv": {
        "table": "order_payments",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {
            "payment_value": float,
        },
        "rename": {}
    },
    "olist_order_reviews_dataset.csv": {
        "table": "order_reviews",
        "schema": "olist",
        "parse_dates": ["review_creation_date", "review_answer_timestamp"],
        "dtype": {},
        "rename": {}
    },
    "olist_geolocation_dataset.csv": {
        "table": "geolocation",
        "schema": "olist",
        "parse_dates": [],
        "dtype": {
            "geolocation_zip_code_prefix": str,
        },
        "rename": {
            "geolocation_zip_code_prefix": "geolocation_zip_code"
        }
    },
}

# Load order matters (foreign key dependencies)
LOAD_ORDER = [
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_product_category_name_translation.csv",
    "olist_products_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_geolocation_dataset.csv",
]


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def get_engine():
    """Create and return a SQLAlchemy engine."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url, echo=False)
    return engine


def validate_csv_files():
    """Check that all required CSVs are present before loading."""
    missing = []
    for filename in LOAD_ORDER:
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            missing.append(filename)
    if missing:
        print("\n[ERROR] The following CSV files are missing from the data/ directory:")
        for f in missing:
            print(f"  - {f}")
        print("\nDownload the Olist dataset from:")
        print("  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        return False
    return True


def load_csv(filename, config, engine):
    """Load a single CSV file into PostgreSQL."""
    filepath = os.path.join(DATA_DIR, filename)
    table    = config["table"]
    schema   = config["schema"]

    print(f"\n[LOADING] {filename} → {schema}.{table}")
    start = time.time()

    # Read CSV
    df = pd.read_csv(
        filepath,
        parse_dates=config["parse_dates"] if config["parse_dates"] else False,
        dtype=config["dtype"] if config["dtype"] else None,
        low_memory=False,
    )

    # Apply column renames
    if config["rename"]:
        df.rename(columns=config["rename"], inplace=True)

    print(f"  Rows read: {len(df):,}  |  Columns: {list(df.columns)}")

    # Load into PostgreSQL (replace to handle re-runs cleanly)
    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    elapsed = time.time() - start
    print(f"  [OK] Loaded {len(df):,} rows in {elapsed:.1f}s")
    return len(df)


def verify_row_counts(engine):
    """Print final row counts for all tables."""
    print("\n" + "="*55)
    print("FINAL ROW COUNT VERIFICATION")
    print("="*55)
    tables = [cfg["table"] for cfg in OLIST_FILES.values()]
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM olist.{table}")
            )
            count = result.scalar()
            print(f"  olist.{table:<35} {count:>8,} rows")
    print("="*55)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("="*55)
    print("OLIST SUPPLY CHAIN — PostgreSQL Data Loader")
    print("="*55)

    # Step 1: Validate CSV files exist
    if not validate_csv_files():
        return

    # Step 2: Connect to PostgreSQL
    print(f"\nConnecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  [OK] Connected successfully.")
    except Exception as e:
        print(f"  [ERROR] Could not connect: {e}")
        print("  Check your .env file or PostgreSQL server status.")
        return

    # Step 3: Create schema
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS olist"))
        conn.commit()
    print("  [OK] Schema 'olist' ready.")

    # Step 4: Load each CSV in dependency order
    total_rows = 0
    for filename in LOAD_ORDER:
        config = OLIST_FILES[filename]
        try:
            rows = load_csv(filename, config, engine)
            total_rows += rows
        except Exception as e:
            print(f"  [ERROR] Failed loading {filename}: {e}")
            raise

    # Step 5: Verify
    verify_row_counts(engine)
    print(f"\nTotal rows loaded: {total_rows:,}")
    print("\nNext step: python 02_demand_forecast.py")


if __name__ == "__main__":
    main()
