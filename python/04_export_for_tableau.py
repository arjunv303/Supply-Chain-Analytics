"""
04_export_for_tableau.py
========================
Final export pipeline: runs all PostgreSQL views and exports them as
clean CSVs for Tableau Public.

Since Tableau Public cannot connect to a live PostgreSQL database,
this script acts as the bridge between your database and your dashboards.

What it exports:
  1. exec_kpis.csv           → Executive KPI Dashboard
  2. seller_scorecard.csv    → Seller Performance Scorecard Dashboard
  3. regional_demand.csv     → Regional Demand Map Dashboard
  4. category_daily.csv      → AI Demand Forecast Dashboard (historical side)

The forecast and anomaly CSVs are created by the previous scripts:
  - forecast_output.csv  (created by 02_demand_forecast.py)
  - anomaly_flags.csv    (created by 03_anomaly_detection.py)

USAGE:
  python 04_export_for_tableau.py
"""

import os
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

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "tableau_exports")

# Maps view name → output CSV filename
EXPORTS = {
    "vw_exec_kpis":             "exec_kpis.csv",
    "vw_seller_scorecard":      "seller_scorecard.csv",
    "vw_regional_demand":       "regional_demand.csv",
    "vw_category_daily_orders": "category_daily.csv",
}


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def export_view(view_name, filename, engine):
    """Query a PostgreSQL view and export to CSV."""
    print(f"\n[EXPORTING] olist.{view_name} → {filename}")
    query = f"SELECT * FROM olist.{view_name}"
    df = pd.read_sql(text(query), con=engine.connect())
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}  |  Saved: {output_path}")
    return df


def validate_ai_outputs():
    """Check that forecast and anomaly CSVs exist from previous scripts."""
    required = ["forecast_output.csv", "anomaly_flags.csv"]
    missing  = []
    for f in required:
        path = os.path.join(OUTPUT_DIR, f)
        if not os.path.exists(path):
            missing.append(f)

    if missing:
        print("\n[WARNING] The following AI output files are missing:")
        for f in missing:
            print(f"  - {f}")
        print("\n  Run these first:")
        if "forecast_output.csv" in missing:
            print("    python 02_demand_forecast.py")
        if "anomaly_flags.csv" in missing:
            print("    python 03_anomaly_detection.py")
    else:
        for f in required:
            path = os.path.join(OUTPUT_DIR, f)
            df   = pd.read_csv(path)
            print(f"  [OK] {f} — {len(df):,} rows")


def print_tableau_connection_guide():
    """Print instructions for connecting CSVs to Tableau Public."""
    print("\n" + "="*60)
    print("TABLEAU PUBLIC CONNECTION GUIDE")
    print("="*60)
    guide = {
        "Dashboard 1 — Executive KPI":  "exec_kpis.csv",
        "Dashboard 2 — Seller Scorecard": "seller_scorecard.csv",
        "Dashboard 3 — Regional Demand":  "regional_demand.csv",
        "Dashboard 4 — AI Forecast":      "forecast_output.csv + anomaly_flags.csv",
    }
    for dashboard, source in guide.items():
        print(f"\n  {dashboard}")
        print(f"    Data source: data/tableau_exports/{source}")

    print("\n  In Tableau Public:")
    print("  1. Open Tableau Public Desktop")
    print("  2. Click 'Connect to Data' → 'Text File'")
    print("  3. Select the CSV file for each dashboard")
    print("  4. See tableau/DASHBOARD_BUILD_GUIDE.md for full setup")
    print("="*60)


# ──────────────────────────────────────────────
# POST-PROCESSING
# ──────────────────────────────────────────────

def enrich_regional_demand(df):
    """
    Add full state names for Tableau map recognition.
    Tableau's Brazil map uses full state names or ISO codes.
    """
    state_names = {
        "AC": "Acre",           "AL": "Alagoas",        "AP": "Amapá",
        "AM": "Amazonas",       "BA": "Bahia",           "CE": "Ceará",
        "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
        "MA": "Maranhão",       "MT": "Mato Grosso",     "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",   "PA": "Pará",            "PB": "Paraíba",
        "PR": "Paraná",         "PE": "Pernambuco",      "PI": "Piauí",
        "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul",
        "RO": "Rondônia",       "RR": "Roraima",         "SC": "Santa Catarina",
        "SP": "São Paulo",      "SE": "Sergipe",         "TO": "Tocantins",
    }
    df["state_name"] = df["state"].map(state_names).fillna(df["state"])
    return df


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("="*55)
    print("TABLEAU EXPORT PIPELINE")
    print("="*55)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = get_engine()

    # Export all PostgreSQL views
    exported = {}
    for view_name, filename in EXPORTS.items():
        try:
            df = export_view(view_name, filename, engine)
            exported[filename] = df

            # Post-process regional demand to add full state names
            if view_name == "vw_regional_demand":
                df = enrich_regional_demand(df)
                output_path = os.path.join(OUTPUT_DIR, filename)
                df.to_csv(output_path, index=False)
                print(f"  Enriched with full state names.")

        except Exception as e:
            print(f"  [ERROR] Failed to export {view_name}: {e}")
            print(f"  Make sure you ran: psql -f sql/07_tableau_views.sql")

    # Validate AI output files
    print("\n[CHECKING AI OUTPUT FILES]")
    validate_ai_outputs()

    # Final summary
    print("\n" + "="*55)
    print("EXPORT SUMMARY")
    print("="*55)
    total_rows = 0
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith(".csv"):
            path     = os.path.join(OUTPUT_DIR, filename)
            df       = pd.read_csv(path)
            size_kb  = os.path.getsize(path) / 1024
            print(f"  {filename:<45} {len(df):>6,} rows  ({size_kb:.0f} KB)")
            total_rows += len(df)
    print(f"\n  Total: {total_rows:,} rows across all files")

    print_tableau_connection_guide()
    print("\n[ALL DONE] Your project is ready. See the README for next steps.")


if __name__ == "__main__":
    main()
