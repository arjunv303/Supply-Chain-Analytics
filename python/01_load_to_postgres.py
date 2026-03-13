import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

OLIST_FILES = {
    "olist_customers_dataset.csv": {
        "table": "customers", "schema": "olist", "parse_dates": [],
        "dtype": {"customer_zip_code_prefix": str},
        "rename": {"customer_zip_code_prefix": "customer_zip_code"}
    },
    "olist_sellers_dataset.csv": {
        "table": "sellers", "schema": "olist", "parse_dates": [],
        "dtype": {"seller_zip_code_prefix": str},
        "rename": {"seller_zip_code_prefix": "seller_zip_code"}
    },
    "olist_product_category_name_translation.csv": {
        "table": "product_categories", "schema": "olist",
        "parse_dates": [], "dtype": {}, "rename": {}
    },
    "olist_products_dataset.csv": {
        "table": "products", "schema": "olist", "parse_dates": [],
        "dtype": {
            "product_name_lenght": "Int64",
            "product_description_lenght": "Int64",
            "product_photos_qty": "Int64",
        },
        "rename": {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    },
    "olist_orders_dataset.csv": {
        "table": "orders", "schema": "olist",
        "parse_dates": [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "dtype": {},
        "rename": {"order_delivered_carrier_date": "order_carrier_delivery_date"}
    },
    "olist_order_items_dataset.csv": {
        "table": "order_items", "schema": "olist",
        "parse_dates": ["shipping_limit_date"],
        "dtype": {"price": float, "freight_value": float},
        "rename": {}
    },
    "olist_order_payments_dataset.csv": {
        "table": "order_payments", "schema": "olist",
        "parse_dates": [], "dtype": {"payment_value": float}, "rename": {}
    },
    "olist_order_reviews_dataset.csv": {
        "table": "order_reviews", "schema": "olist",
        "parse_dates": ["review_creation_date", "review_answer_timestamp"],
        "dtype": {}, "rename": {}
    },
    "olist_geolocation_dataset.csv": {
        "table": "geolocation", "schema": "olist", "parse_dates": [],
        "dtype": {"geolocation_zip_code_prefix": str},
        "rename": {"geolocation_zip_code_prefix": "geolocation_zip_code"}
    },
}

# Load order respects foreign key dependencies
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


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def validate_csv_files():
    missing = []
    for filename in LOAD_ORDER:
        if not os.path.exists(os.path.join(DATA_DIR, filename)):
            missing.append(filename)
    if missing:
        print("\n[ERROR] Missing CSV files:")
        for f in missing:
            print(f"  - {f}")
        print("\nDownload from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        return False
    return True


def load_csv(filename, config, engine):
    filepath = os.path.join(DATA_DIR, filename)
    table, schema = config["table"], config["schema"]
    print(f"\n[LOADING] {filename} -> {schema}.{table}")
    start = time.time()

    df = pd.read_csv(
        filepath,
        parse_dates=config["parse_dates"] if config["parse_dates"] else False,
        dtype=config["dtype"] if config["dtype"] else None,
        low_memory=False,
    )
    if config["rename"]:
        df.rename(columns=config["rename"], inplace=True)

    print(f"  Rows: {len(df):,}")
    df.to_sql(
        name=table, schema=schema, con=engine,
        if_exists="replace", index=False, method="multi", chunksize=5000,
    )
    print(f"  Done in {time.time() - start:.1f}s")
    return len(df)


def verify_row_counts(engine):
    print("\n=== Row Counts ===")
    with engine.connect() as conn:
        for cfg in OLIST_FILES.values():
            result = conn.execute(text(f"SELECT COUNT(*) FROM olist.{cfg['table']}"))
            print(f"  {cfg['table']:<35} {result.scalar():>8,}")


def main():
    print("OLIST Supply Chain - PostgreSQL Data Loader")

    if not validate_csv_files():
        return

    print(f"\nConnecting to {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Connected.")
    except Exception as e:
        print(f"  [ERROR] {e}")
        return

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS olist"))
        conn.commit()

    total_rows = 0
    for filename in LOAD_ORDER:
        rows = load_csv(filename, OLIST_FILES[filename], engine)
        total_rows += rows

    verify_row_counts(engine)
    print(f"\nTotal rows loaded: {total_rows:,}")


if __name__ == "__main__":
    main()
