import os
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

warnings.filterwarnings("ignore")

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

CONTAMINATION = 0.05  # ~5% of days flagged as anomalous

OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "tableau_exports")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "anomaly_flags.csv")

KNOWN_EVENTS = {
    "2016-11-25": "Black Friday",   "2017-11-24": "Black Friday",
    "2016-12-25": "Christmas",      "2017-12-25": "Christmas",
    "2016-12-26": "Post-Christmas", "2017-12-26": "Post-Christmas",
    "2017-02-27": "Carnaval",       "2017-02-28": "Carnaval",
    "2017-03-01": "Carnaval",       "2017-09-07": "Independence Day",
    "2017-10-12": "Nossa Senhora Aparecida",
    "2017-11-02": "Finados",        "2017-11-15": "Republic Day",
    "2018-01-01": "New Year",
}


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def load_daily_totals(engine):
    query = """
        SELECT order_date, SUM(daily_orders) AS daily_orders, SUM(daily_revenue) AS daily_revenue
        FROM olist.vw_category_daily_orders
        GROUP BY order_date ORDER BY order_date
    """
    print("[INFO] Loading daily order totals...")
    df = pd.read_sql(text(query), con=engine.connect())
    df["order_date"] = pd.to_datetime(df["order_date"])

    full_range = pd.date_range(df["order_date"].min(), df["order_date"].max(), freq="D")
    df = df.set_index("order_date").reindex(full_range, fill_value=0).reset_index()
    df.rename(columns={"index": "order_date"}, inplace=True)
    print(f"  {len(df):,} days | {df['order_date'].min().date()} to {df['order_date'].max().date()}")
    return df


def engineer_features(df):
    df = df.copy().sort_values("order_date").reset_index(drop=True)

    df["day_of_week"]    = df["order_date"].dt.dayofweek
    df["is_weekend"]     = (df["day_of_week"] >= 5).astype(int)
    df["month"]          = df["order_date"].dt.month
    df["day_of_month"]   = df["order_date"].dt.day
    df["week_of_year"]   = df["order_date"].dt.isocalendar().week.astype(int)

    df["rolling_7d_avg"] = df["daily_orders"].rolling(7, min_periods=1).mean()
    df["rolling_30d_avg"]= df["daily_orders"].rolling(30, min_periods=1).mean()
    df["rolling_7d_std"] = df["daily_orders"].rolling(7, min_periods=1).std().fillna(0)

    df["lag_1d"]         = df["daily_orders"].shift(1).fillna(0)
    df["lag_7d"]         = df["daily_orders"].shift(7).fillna(0)

    df["deviation_from_7d"] = np.where(
        df["rolling_7d_avg"] > 0,
        (df["daily_orders"] - df["rolling_7d_avg"]) / df["rolling_7d_avg"],
        0
    )
    df["avg_order_value"] = np.where(
        df["daily_orders"] > 0, df["daily_revenue"] / df["daily_orders"], 0
    )
    return df


def run_isolation_forest(df):
    feature_cols = [
        "daily_orders", "rolling_7d_avg", "rolling_30d_avg", "rolling_7d_std",
        "deviation_from_7d", "lag_1d", "lag_7d", "is_weekend", "month", "week_of_year",
    ]

    X = df[feature_cols].fillna(0).values
    X_scaled = StandardScaler().fit_transform(X)

    print(f"\n[INFO] Training Isolation Forest on {len(df):,} days...")

    model = IsolationForest(n_estimators=200, contamination=CONTAMINATION, random_state=42, n_jobs=-1)
    model.fit(X_scaled)

    df["anomaly_label"] = model.predict(X_scaled)
    df["is_anomaly"]    = (df["anomaly_label"] == -1)

    raw_scores = model.decision_function(X_scaled)
    df["anomaly_score"] = (
        (raw_scores - raw_scores.max()) /
        (raw_scores.min() - raw_scores.max()) * 100
    ).round(1)

    df["deviation_pct"] = (df["deviation_from_7d"] * 100).round(1)

    print(f"  Detected {df['is_anomaly'].sum()} anomalous days ({df['is_anomaly'].mean()*100:.1f}%)")
    return df


def label_anomalies(df):
    def get_anomaly_type(row):
        if not row["is_anomaly"]:
            return "Normal"
        date_str = row["order_date"].strftime("%Y-%m-%d")
        if date_str in KNOWN_EVENTS:
            return KNOWN_EVENTS[date_str]
        if row["deviation_pct"] > 50:
            if row["is_weekend"]:
                return "Weekend Demand Spike"
            if row["month"] in [11, 12]:
                return "Holiday Season Spike"
            return "Unusual Demand Spike"
        elif row["deviation_pct"] < -40:
            return "Weekend Demand Drop" if row["is_weekend"] else "Unusual Demand Drop"
        return "Minor Anomaly"

    df["anomaly_type"] = df.apply(get_anomaly_type, axis=1)
    return df


def main():
    print("ANOMALY DETECTION — Isolation Forest")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = get_engine()
    daily_df = load_daily_totals(engine)
    daily_df = engineer_features(daily_df)
    daily_df = run_isolation_forest(daily_df)
    daily_df = label_anomalies(daily_df)

    output_cols = [
        "order_date", "daily_orders", "daily_revenue", "rolling_7d_avg",
        "rolling_30d_avg", "deviation_pct", "is_anomaly", "anomaly_score",
        "anomaly_type", "is_weekend", "month",
    ]

    output_df = daily_df[output_cols].copy()
    output_df["order_date"]       = output_df["order_date"].dt.strftime("%Y-%m-%d")
    output_df["rolling_7d_avg"]   = output_df["rolling_7d_avg"].round(1)
    output_df["rolling_30d_avg"]  = output_df["rolling_30d_avg"].round(1)
    output_df.to_csv(OUTPUT_FILE, index=False)

    print("\n[ANOMALY SUMMARY]")
    for anomaly_type, count in output_df[output_df["is_anomaly"]]["anomaly_type"].value_counts().items():
        print(f"  {anomaly_type:<35} {count:>3} days")
    print(f"\n[DONE] {len(output_df):,} rows saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
