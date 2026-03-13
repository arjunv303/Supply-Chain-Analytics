import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "supply_chain")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "tableau_exports")

EXPORTS = {
    "vw_exec_kpis":             "exec_kpis.csv",
    "vw_seller_scorecard":      "seller_scorecard.csv",
    "vw_regional_demand":       "regional_demand.csv",
    "vw_category_daily_orders": "category_daily.csv",
}

STATE_NAMES = {
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


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, echo=False)


def export_view(view_name, filename, engine):
    print(f"\n[EXPORTING] olist.{view_name} -> {filename}")
    df = pd.read_sql(text(f"SELECT * FROM olist.{view_name}"), con=engine.connect())
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")
    return df


def validate_ai_outputs():
    for f in ["forecast_output.csv", "anomaly_flags.csv"]:
        path = os.path.join(OUTPUT_DIR, f)
        if not os.path.exists(path):
            print(f"  [MISSING] {f} — run the corresponding script first")
        else:
            df = pd.read_csv(path)
            print(f"  [OK] {f} — {len(df):,} rows")


def main():
    print("TABLEAU EXPORT PIPELINE")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = get_engine()

    for view_name, filename in EXPORTS.items():
        try:
            df = export_view(view_name, filename, engine)
            if view_name == "vw_regional_demand":
                df["state_name"] = df["state"].map(STATE_NAMES).fillna(df["state"])
                df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
        except Exception as e:
            print(f"  [ERROR] {view_name}: {e}")

    print("\n[CHECKING AI OUTPUT FILES]")
    validate_ai_outputs()

    print("\n=== Export Summary ===")
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith(".csv"):
            path = os.path.join(OUTPUT_DIR, filename)
            df = pd.read_csv(path)
            size_kb = os.path.getsize(path) / 1024
            print(f"  {filename:<45} {len(df):>6,} rows  ({size_kb:.0f} KB)")

    print("\n[DONE] All files ready in data/tableau_exports/")


if __name__ == "__main__":
    main()
