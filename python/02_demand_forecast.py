import os
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings("ignore")

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

FORECAST_DAYS    = 90
TOP_N_CATEGORIES = 5
MIN_HISTORY_DAYS = 180

OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "tableau_exports")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "forecast_output.csv")

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


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def load_daily_orders(engine):
    query = """
        SELECT order_date, category, daily_orders, daily_revenue
        FROM olist.vw_category_daily_orders
        ORDER BY order_date, category
    """
    print("[INFO] Loading daily order data...")
    df = pd.read_sql(text(query), con=engine.connect())
    df["order_date"] = pd.to_datetime(df["order_date"])
    print(f"  {len(df):,} rows | {df['order_date'].min().date()} to {df['order_date'].max().date()}")
    return df


def get_top_categories(df, n=TOP_N_CATEGORIES):
    top = (
        df.groupby("category")["daily_orders"]
        .sum().sort_values(ascending=False).head(n).index.tolist()
    )
    print(f"\n[INFO] Top {n} categories:")
    for i, cat in enumerate(top, 1):
        print(f"  {i}. {cat} ({df[df['category'] == cat]['daily_orders'].sum():,} orders)")
    return top


def train_and_forecast(category_df, category_name, forecast_days):
    try:
        from prophet import Prophet
    except ImportError:
        raise ImportError("Run: pip install prophet")

    train = category_df[["order_date", "daily_orders"]].rename(
        columns={"order_date": "ds", "daily_orders": "y"}
    )
    date_range = pd.date_range(train["ds"].min(), train["ds"].max(), freq="D")
    train = train.set_index("ds").reindex(date_range, fill_value=0).reset_index()
    train.columns = ["ds", "y"]

    if len(train) < MIN_HISTORY_DAYS:
        print(f"  [SKIP] {category_name}: only {len(train)} days of history")
        return None

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        holidays=BRAZIL_HOLIDAYS,
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.1,
        interval_width=0.80,
    )
    model.fit(train)

    future = model.make_future_dataframe(periods=forecast_days, freq="D")
    forecast = model.predict(future)

    historical = train.copy()
    historical["is_forecast"]  = False
    historical["forecast"]     = historical["y"]
    historical["lower_bound"]  = historical["y"]
    historical["upper_bound"]  = historical["y"]

    future_only = forecast[forecast["ds"] > train["ds"].max()].copy()
    future_only["y"]           = np.nan
    future_only["is_forecast"] = True
    future_only = future_only.rename(columns={
        "yhat": "forecast", "yhat_lower": "lower_bound", "yhat_upper": "upper_bound"
    })

    for col in ["forecast", "lower_bound", "upper_bound"]:
        future_only[col] = future_only[col].clip(lower=0)

    combined = pd.concat([
        historical[["ds", "y", "forecast", "lower_bound", "upper_bound", "is_forecast"]],
        future_only[["ds", "y", "forecast", "lower_bound", "upper_bound", "is_forecast"]],
    ], ignore_index=True)

    combined.rename(columns={"ds": "order_date", "y": "actual_orders"}, inplace=True)
    combined["category"] = category_name
    return combined


def main():
    print(f"DEMAND FORECASTING — Facebook Prophet ({FORECAST_DAYS}-day horizon)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = get_engine()
    daily_df = load_daily_orders(engine)
    top_cats = get_top_categories(daily_df)

    all_forecasts = []
    for cat in top_cats:
        print(f"\n[TRAINING] {cat}")
        result = train_and_forecast(daily_df[daily_df["category"] == cat].copy(), cat, FORECAST_DAYS)
        if result is not None:
            all_forecasts.append(result)
            print(f"  {result[~result['is_forecast']].shape[0]} historical + {result[result['is_forecast']].shape[0]} forecast days")

    if not all_forecasts:
        print("\n[ERROR] No categories had enough data.")
        return

    output_df = pd.concat(all_forecasts, ignore_index=True)
    output_df["order_date"] = output_df["order_date"].dt.strftime("%Y-%m-%d")
    for col in ["actual_orders", "forecast", "lower_bound", "upper_bound"]:
        output_df[col] = output_df[col].round(2)

    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n[DONE] {len(output_df):,} rows saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
