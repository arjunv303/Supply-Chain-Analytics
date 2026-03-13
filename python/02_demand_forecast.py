"""
02_demand_forecast.py
=====================
AI-Powered Demand Forecasting using Facebook Prophet.

What this script does:
  1. Pulls daily order counts per product category from PostgreSQL
  2. Trains a separate Prophet model for each of the top 5 categories
  3. Generates a 90-day forward forecast with confidence intervals
  4. Combines historical + forecast data into one clean CSV for Tableau

WHY PROPHET?
  - Developed at Meta, widely used in industry for business forecasting
  - Handles seasonality (weekly, yearly), holidays, and trend changes automatically
  - Much more robust than naive smoothing or ARIMA for irregular e-commerce data
  - Adding this to your portfolio immediately separates you from BA candidates
    who only use Excel trend lines

OUTPUT:
  data/tableau_exports/forecast_output.csv
  Columns: order_date, category, daily_orders, forecast, lower_bound, upper_bound,
           is_forecast (True/False)

USAGE:
  python 02_demand_forecast.py
"""

import os
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

FORECAST_DAYS   = 90     # How many days ahead to forecast
TOP_N_CATEGORIES = 5     # Forecast only the top N categories by order volume
MIN_HISTORY_DAYS = 180   # Minimum days of data required to train Prophet

OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "tableau_exports")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "forecast_output.csv")

# Brazilian national holidays (for Prophet's holiday regressor)
BRAZIL_HOLIDAYS = pd.DataFrame({
    "holiday": [
        "Carnaval", "Carnaval", "Carnaval",
        "Tiradentes", "Tiradentes",
        "Dia do Trabalhador", "Dia do Trabalhador",
        "Independência", "Independência",
        "Nossa Senhora Aparecida", "Nossa Senhora Aparecida",
        "Finados", "Finados",
        "Proclamação da República", "Proclamação da República",
        "Natal", "Natal",
        "Ano Novo", "Ano Novo",
        "Black Friday", "Black Friday",
    ],
    "ds": pd.to_datetime([
        "2016-02-08", "2017-02-27", "2018-02-12",
        "2016-04-21", "2017-04-21",
        "2016-05-01", "2017-05-01",
        "2016-09-07", "2017-09-07",
        "2016-10-12", "2017-10-12",
        "2016-11-02", "2017-11-02",
        "2016-11-15", "2017-11-15",
        "2016-12-25", "2017-12-25",
        "2017-01-01", "2018-01-01",
        "2016-11-25", "2017-11-24",
    ]),
    "lower_window": 0,
    "upper_window": 1,
})


# ──────────────────────────────────────────────
# DATA LOADING
# ──────────────────────────────────────────────

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def load_daily_orders(engine):
    """Pull daily order counts per category from the Tableau view."""
    query = """
        SELECT
            order_date,
            category,
            daily_orders,
            daily_revenue
        FROM olist.vw_category_daily_orders
        ORDER BY order_date, category
    """
    print("[INFO] Loading daily order data from PostgreSQL...")
    df = pd.read_sql(text(query), con=engine.connect())
    df["order_date"] = pd.to_datetime(df["order_date"])
    print(f"  Loaded {len(df):,} rows | "
          f"{df['order_date'].min().date()} to {df['order_date'].max().date()}")
    return df


def get_top_categories(df, n=TOP_N_CATEGORIES):
    """Return the top N categories by total order volume."""
    top = (
        df.groupby("category")["daily_orders"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .index.tolist()
    )
    print(f"\n[INFO] Top {n} categories by order volume:")
    for i, cat in enumerate(top, 1):
        vol = df[df["category"] == cat]["daily_orders"].sum()
        print(f"  {i}. {cat} ({vol:,} orders)")
    return top


# ──────────────────────────────────────────────
# PROPHET FORECASTING
# ──────────────────────────────────────────────

def train_and_forecast(category_df, category_name, forecast_days):
    """
    Train Prophet model on one category and return combined
    historical + forecast DataFrame.
    """
    try:
        from prophet import Prophet
    except ImportError:
        raise ImportError(
            "Prophet not installed. Run: pip install prophet\n"
            "Note: Prophet requires pystan. On M1/M2 Mac, use conda if pip fails."
        )

    # Prophet requires columns named 'ds' (date) and 'y' (value)
    train = category_df[["order_date", "daily_orders"]].rename(
        columns={"order_date": "ds", "daily_orders": "y"}
    )

    # Fill missing dates with 0 (no orders on that day)
    date_range = pd.date_range(train["ds"].min(), train["ds"].max(), freq="D")
    train = train.set_index("ds").reindex(date_range, fill_value=0).reset_index()
    train.columns = ["ds", "y"]

    if len(train) < MIN_HISTORY_DAYS:
        print(f"  [SKIP] {category_name}: only {len(train)} days of history "
              f"(need {MIN_HISTORY_DAYS})")
        return None

    # Configure Prophet
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        holidays=BRAZIL_HOLIDAYS,
        seasonality_mode="multiplicative",   # e-commerce demand scales multiplicatively
        changepoint_prior_scale=0.1,          # conservative trend changes
        interval_width=0.80,                  # 80% confidence interval
    )

    model.fit(train)

    # Create future dataframe
    future = model.make_future_dataframe(periods=forecast_days, freq="D")
    forecast = model.predict(future)

    # Combine historical + forecast
    historical = train.copy()
    historical["is_forecast"]   = False
    historical["forecast"]      = historical["y"]
    historical["lower_bound"]   = historical["y"]
    historical["upper_bound"]   = historical["y"]

    future_only = forecast[forecast["ds"] > train["ds"].max()].copy()
    future_only["y"]            = np.nan
    future_only["is_forecast"]  = True
    future_only = future_only.rename(columns={
        "yhat":       "forecast",
        "yhat_lower": "lower_bound",
        "yhat_upper": "upper_bound",
    })

    # Clip negative forecast values (can't have negative orders)
    future_only["forecast"]    = future_only["forecast"].clip(lower=0)
    future_only["lower_bound"] = future_only["lower_bound"].clip(lower=0)
    future_only["upper_bound"] = future_only["upper_bound"].clip(lower=0)

    combined = pd.concat([
        historical[["ds", "y", "forecast", "lower_bound", "upper_bound", "is_forecast"]],
        future_only[["ds", "y", "forecast", "lower_bound", "upper_bound", "is_forecast"]],
    ], ignore_index=True)

    combined.rename(columns={"ds": "order_date", "y": "actual_orders"}, inplace=True)
    combined["category"] = category_name

    return combined


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("="*55)
    print("DEMAND FORECASTING — Facebook Prophet")
    print(f"Forecast horizon: {FORECAST_DAYS} days")
    print("="*55)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load data
    engine = get_engine()
    daily_df = load_daily_orders(engine)
    top_cats = get_top_categories(daily_df)

    # Train Prophet on each top category
    all_forecasts = []
    for cat in top_cats:
        print(f"\n[TRAINING] Category: {cat}")
        cat_df = daily_df[daily_df["category"] == cat].copy()
        result = train_and_forecast(cat_df, cat, FORECAST_DAYS)
        if result is not None:
            all_forecasts.append(result)
            actual_rows   = result[~result["is_forecast"]].shape[0]
            forecast_rows = result[result["is_forecast"]].shape[0]
            print(f"  [OK] {actual_rows} historical days + {forecast_rows} forecast days")

    if not all_forecasts:
        print("\n[ERROR] No categories had enough data to forecast.")
        return

    # Combine and export
    output_df = pd.concat(all_forecasts, ignore_index=True)
    output_df["order_date"] = output_df["order_date"].dt.strftime("%Y-%m-%d")

    # Round numeric columns for clean CSV
    for col in ["actual_orders", "forecast", "lower_bound", "upper_bound"]:
        output_df[col] = output_df[col].round(2)

    output_df.to_csv(OUTPUT_FILE, index=False)

    print("\n" + "="*55)
    print(f"[DONE] Forecast saved to:")
    print(f"  {OUTPUT_FILE}")
    print(f"  Total rows: {len(output_df):,}")
    print(f"  Categories: {output_df['category'].nunique()}")
    print(f"  Forecast rows: {output_df['is_forecast'].sum():,}")
    print("="*55)
    print("\nNext step: python 03_anomaly_detection.py")


if __name__ == "__main__":
    main()
