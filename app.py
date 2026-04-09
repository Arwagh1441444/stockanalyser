from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from typing import Dict, List, Sequence

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from market_db import MarketDatabase

APP_DIR = Path(__file__).resolve().parent
MASTER_DB = os.environ.get("MASTER_MARKET_DB", str(APP_DIR / "master_market_data.db"))
APP_TITLE = "AR Tiger Tech Analysis"
USERNAME = os.environ.get("APP_USERNAME", "Anand1234")
PASSWORD = os.environ.get("APP_PASSWORD", "618523")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "f8d8079a60f84e02bb987ed0ad62b79d")
PORTFOLIO_TABLE = "portfolio_positions"
PORTFOLIO_BACKUP_PATH = APP_DIR / "portfolio_positions.json"
STATIC_DASHBOARD_TABLE = "static_dashboard_nifty"

DEFAULT_STATIC_DASHBOARD_ROWS = [
    {"row_id": "RELIANCE", "Symbol": "RELIANCE", "Current Price": 1350.9, "52W High": 1611.8, "52W Low": 1114.85},
    {"row_id": "TCS", "Symbol": "TCS", "Current Price": 2445.7, "52W High": 3630.5, "52W Low": 2346.2},
    {"row_id": "INFY", "Symbol": "INFY", "Current Price": 1297.5, "52W High": 1728.0, "52W Low": 1215.1},
    {"row_id": "HDFCBANK", "Symbol": "HDFCBANK", "Current Price": 751.0, "52W High": 1020.5, "52W Low": 726.65},
    {"row_id": "ICICIBANK", "Symbol": "ICICIBANK", "Current Price": 1216.5, "52W High": 1500.0, "52W Low": 1187.6},
    {"row_id": "SBIN", "Symbol": "SBIN", "Current Price": 1018.0, "52W High": 1234.7, "52W Low": 730.0},
    {"row_id": "KOTAKBANK", "Symbol": "KOTAKBANK", "Current Price": 357.8, "52W High": 460.38, "52W Low": 345.5},
    {"row_id": "LT", "Symbol": "LT", "Current Price": 3610.0, "52W High": 4440.0, "52W Low": 2965.3},
    {"row_id": "BHARTIARTL", "Symbol": "BHARTIARTL", "Current Price": 1792.0, "52W High": 2174.5, "52W Low": 1669.4},
    {"row_id": "HINDUNILVR", "Symbol": "HINDUNILVR", "Current Price": 2065.0, "52W High": 2750.0, "52W Low": 2022.5},
    {"row_id": "ITC", "Symbol": "ITC", "Current Price": 292.3, "52W High": 444.2, "52W Low": 287.0},
    {"row_id": "ASIANPAINT", "Symbol": "ASIANPAINT", "Current Price": 2170.4, "52W High": 2985.7, "52W Low": 2115.0},
    {"row_id": "AXISBANK", "Symbol": "AXISBANK", "Current Price": 1198.1, "52W High": 1418.3, "52W Low": 1032.35},
    {"row_id": "BAJFINANCE", "Symbol": "BAJFINANCE", "Current Price": 828.9, "52W High": 1102.5, "52W Low": 787.9},
    {"row_id": "MARUTI", "Symbol": "MARUTI", "Current Price": 12590.0, "52W High": 17370.0, "52W Low": 11059.45},
    {"row_id": "SUNPHARMA", "Symbol": "SUNPHARMA", "Current Price": 1692.0, "52W High": 1851.2, "52W Low": 1548.0},
    {"row_id": "ONGC", "Symbol": "ONGC", "Current Price": 286.1, "52W High": 293.0, "52W Low": 205.0},
    {"row_id": "NTPC", "Symbol": "NTPC", "Current Price": 358.9, "52W High": 394.5, "52W Low": 315.55},
    {"row_id": "POWERGRID", "Symbol": "POWERGRID", "Current Price": 290.95, "52W High": 322.0, "52W Low": 250.0},
    {"row_id": "TITAN", "Symbol": "TITAN", "Current Price": 4078.0, "52W High": 4378.4, "52W Low": 2925.0},
]

st.set_page_config(page_title=APP_TITLE, page_icon="📈", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    :root {
        --app-bg-1: #07111f;
        --app-bg-2: #0c1b2e;
        --app-panel: rgba(12, 23, 38, 0.92);
        --app-panel-2: rgba(17, 31, 50, 0.96);
        --app-border: rgba(120, 151, 187, 0.18);
        --app-text: #e6eef8;
        --app-muted: #8fa7c2;
        --app-accent: #1ec8a5;
        --app-accent-2: #5fa8ff;
        --app-danger: #ff6b81;
        --app-warning: #ffb454;
    }
    .stApp {
        background:
            radial-gradient(circle at top right, rgba(95,168,255,0.14), transparent 28%),
            radial-gradient(circle at top left, rgba(30,200,165,0.12), transparent 24%),
            linear-gradient(180deg, var(--app-bg-1) 0%, var(--app-bg-2) 100%);
        color: var(--app-text);
    }
    .block-container {
        padding-top: 0.85rem;
        padding-bottom: 1.2rem;
        max-width: 1450px;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(7,17,31,0.98) 0%, rgba(11,24,42,0.98) 100%);
        border-right: 1px solid var(--app-border);
    }
    [data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(18,31,51,0.95) 0%, rgba(12,22,37,0.95) 100%);
        border: 1px solid var(--app-border);
        border-radius: 18px;
        padding: 0.7rem 0.8rem;
        box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
    }
    [data-testid="stMetricLabel"], [data-testid="stMetricValue"], [data-testid="stMetricDelta"] {
        color: var(--app-text);
    }
    .app-hero {
        background: linear-gradient(135deg, rgba(18,31,51,0.96) 0%, rgba(13,24,41,0.96) 65%, rgba(12,37,48,0.92) 100%);
        border: 1px solid var(--app-border);
        border-radius: 24px;
        padding: 1rem 1.15rem;
        margin: 0.1rem 0 1rem 0;
        box-shadow: 0 14px 34px rgba(0,0,0,0.20);
    }
    .app-hero-title {
        color: var(--app-text);
        font-size: 1.7rem;
        font-weight: 800;
        letter-spacing: 0.02em;
        margin-bottom: 0.25rem;
    }
    .app-hero-subtitle {
        color: var(--app-muted);
        font-size: 0.95rem;
        margin-bottom: 0.8rem;
    }
    .app-badges {display:flex; gap:0.55rem; flex-wrap:wrap;}
    .app-badge {
        display:inline-flex;
        align-items:center;
        gap:0.35rem;
        padding:0.32rem 0.7rem;
        border-radius:999px;
        background: rgba(95,168,255,0.12);
        border:1px solid rgba(95,168,255,0.20);
        color: var(--app-text);
        font-size: 0.82rem;
        font-weight: 600;
    }
    .app-badge.positive {
        background: rgba(30,200,165,0.14);
        border-color: rgba(30,200,165,0.22);
    }
    .app-badge.warning {
        background: rgba(255,180,84,0.14);
        border-color: rgba(255,180,84,0.22);
    }
    .app-badge.danger {
        background: rgba(255,107,129,0.14);
        border-color: rgba(255,107,129,0.24);
    }
    div[data-baseweb="tab-list"] {
        gap: 0.45rem;
        padding: 0.15rem 0 0.85rem 0;
    }
    button[data-baseweb="tab"] {
        background: rgba(16, 30, 49, 0.75);
        border: 1px solid var(--app-border);
        border-radius: 14px;
        padding: 0.45rem 0.95rem;
    }
    button[data-baseweb="tab"] p {font-weight: 600; color: var(--app-text);}
    button[data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, rgba(30,200,165,0.18) 0%, rgba(95,168,255,0.16) 100%);
        border-color: rgba(95,168,255,0.35);
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid var(--app-border);
        border-radius: 18px;
        overflow: hidden;
        box-shadow: 0 10px 24px rgba(0, 0, 0, 0.14);
    }
    div[data-testid="stForm"], div[data-testid="stExpander"] {
        border: 1px solid var(--app-border);
        border-radius: 18px;
        background: rgba(12, 22, 37, 0.62);
    }
    .stButton > button, .stDownloadButton > button, [data-testid="baseButton-secondary"] {
        border-radius: 14px;
        border: 1px solid rgba(95,168,255,0.22);
        background: linear-gradient(135deg, rgba(20,55,95,0.98) 0%, rgba(17,40,71,0.98) 100%);
        color: var(--app-text);
        font-weight: 700;
        min-height: 2.8rem;
        box-shadow: 0 10px 20px rgba(0,0,0,0.16);
    }
    .stButton > button:hover {
        border-color: rgba(30,200,165,0.36);
        color: white;
    }
    .delete-callout {
        margin-top: 0.5rem;
        padding: 0.75rem 0.9rem;
        border-radius: 16px;
        border: 1px solid rgba(255,107,129,0.22);
        background: rgba(90, 22, 34, 0.18);
        color: var(--app-text);
    }
    .section-caption {
        color: var(--app-muted);
        font-size: 0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@dataclass
class DatabaseBundle:
    db_path: str
    mtime: float
    tables: Dict[str, pd.DataFrame]

    def get(self, name: str) -> pd.DataFrame:
        return self.tables.get(name, pd.DataFrame()).copy()


@st.cache_data(show_spinner=False)
def load_bundle(db_path: str, mtime: float) -> DatabaseBundle:
    needed = [
        "Universe_Master",
        "Latest_Quotes",
        "Latest_Signals",
        "News_Scores",
        "App_Input_View",
        "Provider_Log",
        "Refresh_Control",
        "Config",
        PORTFOLIO_TABLE,
    ]
    db = MarketDatabase(db_path)
    tables = db.read_tables(needed)
    return DatabaseBundle(db_path=db_path, mtime=mtime, tables=tables)


@st.cache_data(show_spinner=False)
def load_symbol_history(db_path: str, mtime: float, symbol: str, limit: int = 320) -> pd.DataFrame:
    db = MarketDatabase(db_path)
    try:
        return db.read_symbol_history(symbol, limit=limit)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_symbol_news(db_path: str, mtime: float, symbol: str, limit: int = 30) -> pd.DataFrame:
    db = MarketDatabase(db_path)
    try:
        return db.read_symbol_news(symbol, limit=limit)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_news_for_symbols(db_path: str, mtime: float, symbols: Sequence[str], limit: int = 80) -> pd.DataFrame:
    db = MarketDatabase(db_path)
    try:
        return db.read_news_for_symbols(list(symbols), limit=limit)
    except Exception:
        return pd.DataFrame()



def default_static_dashboard_df() -> pd.DataFrame:
    df = pd.DataFrame(DEFAULT_STATIC_DASHBOARD_ROWS).copy()
    return normalize_static_dashboard(df)


def normalize_static_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["row_id", "Symbol", "Current Price", "52W High", "52W Low", "% from High", "% from Low"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    out["row_id"] = out["row_id"].astype(str).replace({"": pd.NA})
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.strip()
    for col in ["Current Price", "52W High", "52W Low"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["row_id"] = out["row_id"].where(out["row_id"].notna(), out["Symbol"])
    missing_ids = out["row_id"].isna() | (out["row_id"].astype(str).str.strip() == "")
    if missing_ids.any():
        out.loc[missing_ids, "row_id"] = [str(uuid4()) for _ in range(int(missing_ids.sum()))]
    high = out["52W High"]
    low = out["52W Low"]
    current = out["Current Price"]
    out["% from High"] = ((high - current) / high * 100.0).where(high > 0)
    out["% from Low"] = ((current - low) / low * 100.0).where(low > 0)
    out = out.dropna(subset=["Symbol"]).copy()
    out = out[out["Symbol"].astype(str).str.strip() != ""]
    out = out.drop_duplicates(subset=["row_id"], keep="last")
    return out[columns].sort_values("Symbol").reset_index(drop=True)


def ensure_static_dashboard_table(db_path: Path) -> pd.DataFrame:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {STATIC_DASHBOARD_TABLE} (
                row_id TEXT PRIMARY KEY,
                Symbol TEXT NOT NULL,
                "Current Price" REAL,
                "52W High" REAL,
                "52W Low" REAL,
                "% from High" REAL,
                "% from Low" REAL
            )
        ''')
        existing = pd.read_sql_query(f'SELECT * FROM {STATIC_DASHBOARD_TABLE}', conn)
        existing = normalize_static_dashboard(existing)
        if existing.empty:
            seed = default_static_dashboard_df()
            seed.to_sql(STATIC_DASHBOARD_TABLE, conn, if_exists="replace", index=False)
            existing = seed
    return existing


def load_static_dashboard(db_path: Path) -> pd.DataFrame:
    ensure_static_dashboard_table(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        df = pd.read_sql_query(f'SELECT * FROM {STATIC_DASHBOARD_TABLE}', conn)
    return normalize_static_dashboard(df)


def save_static_dashboard(db_path: Path, df: pd.DataFrame) -> None:
    cleaned = normalize_static_dashboard(df)
    with sqlite3.connect(str(db_path)) as conn:
        cleaned.to_sql(STATIC_DASHBOARD_TABLE, conn, if_exists="replace", index=False)


def render_html_dashboard(df: pd.DataFrame) -> str:
    safe = normalize_static_dashboard(df)
    data_json = json.dumps([
        {
            "Symbol": row["Symbol"],
            "Current": None if pd.isna(row["Current Price"]) else round(float(row["Current Price"]), 2),
            "High": None if pd.isna(row["52W High"]) else round(float(row["52W High"]), 2),
            "Low": None if pd.isna(row["52W Low"]) else round(float(row["52W Low"]), 2),
            "PHigh": None if pd.isna(row["% from High"]) else round(float(row["% from High"]), 2),
            "PLow": None if pd.isna(row["% from Low"]) else round(float(row["% from Low"]), 2),
        }
        for _, row in safe.iterrows()
    ])
    return f"""
    <!DOCTYPE html>
    <html lang=\"en\">
    <head>
      <meta charset=\"UTF-8\">
      <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
        th {{ background-color: #333; color: white; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .near-low {{ background-color: lightgreen; }}
        .near-high {{ background-color: lightcoral; }}
        #searchBox {{ margin-bottom: 15px; padding: 8px; width: 300px; border: 1px solid #ccc; border-radius: 6px; }}
      </style>
    </head>
    <body>
      <h1>📈 Live Nifty 50 Dashboard</h1>
      <input type=\"text\" id=\"searchBox\" placeholder=\"Search company...\">
      <table id=\"niftyTable\">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Current Price</th>
            <th>52W High</th>
            <th>52W Low</th>
            <th>% from High</th>
            <th>% from Low</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
      <script>
        const data = {data_json};
        const tbody = document.querySelector('#niftyTable tbody');
        const searchBox = document.getElementById('searchBox');
        function fmt(v) {{
          if (v === null || v === undefined || Number.isNaN(v)) return '—';
          return Number(v).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
        }}
        function render(rows) {{
          tbody.innerHTML = '';
          rows.forEach(item => {{
            const tr = document.createElement('tr');
            if (item.PLow !== null && item.PLow <= 5) tr.classList.add('near-low');
            if (item.PHigh !== null && item.PHigh <= 5) tr.classList.add('near-high');
            tr.innerHTML = `
              <td>${{item.Symbol ?? '—'}}</td>
              <td>${{fmt(item.Current)}}</td>
              <td>${{fmt(item.High)}}</td>
              <td>${{fmt(item.Low)}}</td>
              <td>${{fmt(item.PHigh)}}${{item.PHigh === null ? '' : '%'}}<\/td>
              <td>${{fmt(item.PLow)}}${{item.PLow === null ? '' : '%'}}<\/td>
            `;
            tbody.appendChild(tr);
          }});
        }}
        render(data);
        searchBox.addEventListener('input', () => {{
          const q = searchBox.value.toLowerCase().trim();
          const filtered = data.filter(item => (item.Symbol || '').toLowerCase().includes(q));
          render(filtered);
        }});
      </script>
    </body>
    </html>
    """


def require_login() -> bool:
    if st.session_state.get("logged_in"):
        return True
    st.title("🔐 Login Required")
    user = st.text_input("Username")
    pwd = st.text_input("Password", type="password")
    if st.button("Login", use_container_width=True):
        if user == USERNAME and pwd == PASSWORD:
            st.session_state.logged_in = True
            st.rerun()
        st.error("Invalid credentials")
    return False


def refresh_market_database(
    db_path: str,
    sample_limit: int,
    news_limit: int,
    refresh_universe: str,
    priority_symbols: List[str] | None = None,
    refresh_symbols: List[str] | None = None,
) -> None:
    from refresh_engine import RefreshEngine

    RefreshEngine(db_path=db_path).refresh(
        sample_limit=sample_limit,
        news_limit=news_limit,
        refresh_universe=refresh_universe,
        priority_symbols=priority_symbols,
        refresh_symbols=refresh_symbols,
    )


def fmt_price(x):
    return "—" if pd.isna(x) else f"₹{float(x):,.2f}"


def fmt_pct(x):
    return "—" if pd.isna(x) else f"{float(x):.2f}%"


def fmt_num(x, digits: int = 1):
    return "—" if pd.isna(x) else f"{float(x):,.{digits}f}"


@st.cache_data(show_spinner=False)
def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = df.copy()
    for col in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = pd.to_datetime(export_df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return export_df.to_csv(index=False).encode("utf-8")


def get_db_mtime(db_path: Path) -> float:
    try:
        return db_path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def company_lookup_map(app_view: pd.DataFrame) -> Dict[str, str]:
    if app_view.empty:
        return {}
    return {str(row.symbol): str(row.company_name) for row in app_view[["symbol", "company_name"]].drop_duplicates().itertuples(index=False)}


def normalize_app_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "company_name",
                "tags",
                "ltp",
                "technical_score",
                "news_score",
                "overall_score",
                "recommendation",
                "actionable_flag",
                "status_note",
            ]
        )
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    numeric_cols = [
        "ltp",
        "change_1d_pct",
        "change_1w_pct",
        "change_1m_pct",
        "change_3m_pct",
        "change_6m_pct",
        "change_9m_pct",
        "change_12m_pct",
        "technical_score",
        "rsi14",
        "sma20",
        "sma50",
        "sma200",
        "volume_ratio",
        "overall_score",
        "dqi_final",
        "ema20",
        "atr14",
        "range_position_52w_pct",
        "trend_quality",
        "news_score",
        "news_confidence",
        "news_article_count_7d",
        "news_article_count_30d",
        "liquidity_score",
        "priority",
        "news_bullish_count",
        "news_bearish_count",
        "history_days",
        "history_recency_days",
        "recommendation_confidence",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in [
        "company_name",
        "tags",
        "recommendation",
        "actionable_flag",
        "status_note",
        "quote_source",
        "exchange_primary",
        "headline_risk_flag",
        "news_summary",
        "top_positive_headline",
        "top_negative_headline",
        "technical_regime",
        "recommendation_reason",
        "history_source_hint",
    ]:
        if col not in out.columns:
            out[col] = ""
    if "latest_news_ts" in out.columns:
        out["latest_news_ts"] = pd.to_datetime(out["latest_news_ts"], errors="coerce", utc=True)
    return out


def normalize_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["position_id", "symbol", "company_name", "buy_price", "buy_quantity", "added_at"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = ""
    out["position_id"] = out["position_id"].astype(str)
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    out["company_name"] = out["company_name"].astype(str)
    out["buy_price"] = pd.to_numeric(out["buy_price"], errors="coerce")
    out["buy_quantity"] = pd.to_numeric(out["buy_quantity"], errors="coerce")
    out["added_at"] = pd.to_datetime(out["added_at"], errors="coerce")
    out = out.dropna(subset=["symbol", "buy_price", "buy_quantity"])
    out = out[out["buy_price"] > 0]
    out = out[out["buy_quantity"] > 0]
    out["added_at"] = out["added_at"].fillna(pd.Timestamp.now(tz="Asia/Kolkata"))
    return out[columns].reset_index(drop=True)


def portfolio_symbols(portfolio_df: pd.DataFrame) -> List[str]:
    if portfolio_df.empty:
        return []
    return sorted({str(x).upper().strip() for x in portfolio_df["symbol"].tolist() if str(x).strip()})


def _portfolio_backup_frame() -> pd.DataFrame:
    if not PORTFOLIO_BACKUP_PATH.exists():
        return normalize_portfolio(pd.DataFrame())
    try:
        payload = json.loads(PORTFOLIO_BACKUP_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return normalize_portfolio(pd.DataFrame())
        return normalize_portfolio(pd.DataFrame(payload))
    except Exception:
        return normalize_portfolio(pd.DataFrame())


def _write_portfolio_backup(portfolio_df: pd.DataFrame) -> None:
    safe = normalize_portfolio(portfolio_df).copy()
    if "added_at" in safe.columns:
        safe["added_at"] = pd.to_datetime(safe["added_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    PORTFOLIO_BACKUP_PATH.write_text(
        json.dumps(safe.to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_persisted_portfolio(db_path: Path) -> pd.DataFrame:
    db = MarketDatabase(str(db_path))
    db_df = normalize_portfolio(db.read_table(PORTFOLIO_TABLE)) if db_path.exists() else normalize_portfolio(pd.DataFrame())
    if not db_df.empty:
        _write_portfolio_backup(db_df)
        return db_df

    backup_df = _portfolio_backup_frame()
    if not backup_df.empty:
        db.initialize()
        db.write_table(PORTFOLIO_TABLE, backup_df)
    return backup_df


def save_portfolio(db_path: Path, portfolio_df: pd.DataFrame) -> None:
    normalized = normalize_portfolio(portfolio_df)
    db = MarketDatabase(str(db_path))
    db.initialize()
    db.write_table(PORTFOLIO_TABLE, normalized)
    _write_portfolio_backup(normalized)


def add_portfolio_position(db_path: Path, portfolio_df: pd.DataFrame, symbol: str, company_name: str, buy_price: float, buy_quantity: int) -> None:
    new_row = pd.DataFrame(
        [
            {
                "position_id": f"{symbol}-{pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y%m%d%H%M%S%f')}",
                "symbol": str(symbol).upper().strip(),
                "company_name": str(company_name or symbol),
                "buy_price": float(buy_price),
                "buy_quantity": int(buy_quantity),
                "added_at": pd.Timestamp.now(tz="Asia/Kolkata"),
            }
        ]
    )
    out = pd.concat([portfolio_df, new_row], ignore_index=True)
    save_portfolio(db_path, out)


def delete_portfolio_positions(db_path: Path, portfolio_df: pd.DataFrame, position_ids: List[str]) -> None:
    keep = portfolio_df[~portfolio_df["position_id"].astype(str).isin([str(x) for x in position_ids])].copy()
    save_portfolio(db_path, keep)


def history_for_symbol(history: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if history.empty:
        return history
    x = history[history["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if x.empty:
        return x
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    return x.sort_values("date")


def news_for_symbol(news_articles: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if news_articles.empty:
        return news_articles
    x = news_articles[news_articles["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if x.empty:
        return x
    x["published_at"] = pd.to_datetime(x["published_at"], errors="coerce", utc=True)
    return x.sort_values(["published_at", "article_sentiment_raw"], ascending=[False, False])


def provider_summary(logs: pd.DataFrame) -> pd.DataFrame:
    if logs.empty:
        return pd.DataFrame(columns=["provider", "dataset", "status", "rows_loaded", "avg_latency_ms", "latest_error"])
    cols = [c for c in ["provider", "dataset", "status", "rows_loaded", "avg_latency_ms", "latest_error"] if c in logs.columns]
    return logs[cols].copy().sort_values(["provider", "dataset"])


def config_map(df: pd.DataFrame) -> Dict[str, str]:
    if df.empty or not {"key", "value"}.issubset(df.columns):
        return {}
    return {str(k): str(v) for k, v in zip(df["key"], df["value"])}


def build_exit_plan(row: pd.Series, buy_price: float) -> Dict[str, object]:
    ltp = pd.to_numeric(pd.Series([row.get("ltp")]), errors="coerce").iloc[0]
    overall_score = pd.to_numeric(pd.Series([row.get("overall_score")]), errors="coerce").iloc[0]
    technical_score = pd.to_numeric(pd.Series([row.get("technical_score")]), errors="coerce").iloc[0]
    news_score = pd.to_numeric(pd.Series([row.get("news_score")]), errors="coerce").iloc[0]
    atr14 = pd.to_numeric(pd.Series([row.get("atr14")]), errors="coerce").iloc[0]
    recommendation = str(row.get("recommendation") or "NO DATA")
    risk_flag = str(row.get("headline_risk_flag") or "LOW")

    atr_pct = float(atr14 / ltp * 100.0) if pd.notna(atr14) and pd.notna(ltp) and float(ltp) != 0 else 0.0
    stop_pct = min(max(max(atr_pct * 1.8, 4.0), 4.0), 10.0)

    if pd.isna(ltp):
        return {
            "target_exit_price": pd.NA,
            "stop_loss_price": pd.NA,
            "target_return_pct": pd.NA,
            "action": "NO DATA",
            "exit_note": "Current price not available.",
        }

    if risk_flag == "HIGH":
        target_pct = 2.5
        action = "EXIT / REDUCE"
        exit_note = "High headline risk detected. Book profit on strength or cut risk early."
    elif recommendation == "BUY":
        if pd.notna(overall_score) and overall_score >= 85 and pd.notna(news_score) and news_score >= 60:
            target_pct = 14.0
        elif pd.notna(overall_score) and overall_score >= 78:
            target_pct = 10.0
        else:
            target_pct = 8.0
        action = "HOLD / TRAIL"
        exit_note = "Trend and score remain supportive. Trail stop and review on score weakness."
    elif recommendation == "HOLD":
        if pd.notna(overall_score) and overall_score >= 68 and pd.notna(technical_score) and technical_score >= 55:
            target_pct = 7.0
            action = "HOLD / REVIEW"
            exit_note = "Moderate setup. Hold with tighter monitoring of news and score trend."
        else:
            target_pct = 4.0
            action = "PARTIAL EXIT / TIGHT STOP"
            exit_note = "Momentum is weaker. Keep tighter stop and reduce if score slips further."
    elif recommendation == "SELL":
        target_pct = 1.5
        action = "EXIT ON BOUNCE"
        exit_note = "Current ranking is weak. Prefer exit or use only a very small bounce target."
    else:
        target_pct = 0.0
        action = "NO DATA"
        exit_note = "Insufficient signal quality for an exit target."

    target_exit_price = float(buy_price) * (1 + float(target_pct) / 100.0)
    stop_loss_price = float(buy_price) * (1 - float(stop_pct) / 100.0)

    if pd.notna(ltp) and float(ltp) <= stop_loss_price and action not in {"NO DATA", "EXIT / REDUCE"}:
        action = "STOP TRIGGER ZONE"
        exit_note = "Current price is near or below the suggested stop zone. Protect capital first."

    return {
        "target_exit_price": round(target_exit_price, 2),
        "stop_loss_price": round(stop_loss_price, 2),
        "target_return_pct": round(float(target_pct), 2),
        "action": action,
        "exit_note": exit_note,
    }


def build_portfolio_view(portfolio_df: pd.DataFrame, app_view: pd.DataFrame) -> pd.DataFrame:
    if portfolio_df.empty:
        return pd.DataFrame()

    price_cols = [
        "symbol",
        "company_name",
        "ltp",
        "technical_score",
        "news_score",
        "overall_score",
        "recommendation",
        "headline_risk_flag",
        "news_summary",
        "top_positive_headline",
        "top_negative_headline",
        "news_article_count_7d",
        "change_1d_pct",
        "change_1w_pct",
        "change_1m_pct",
        "change_3m_pct",
        "atr14",
    ]
    available_cols = [c for c in price_cols if c in app_view.columns]
    merged = portfolio_df.merge(app_view[available_cols], on="symbol", how="left", suffixes=("", "_live"))

    if "company_name_live" in merged.columns:
        merged["company_name"] = merged["company_name_live"].fillna(merged["company_name"])
    merged["company_name"] = merged["company_name"].fillna(merged["symbol"])

    merged["invested_value"] = merged["buy_price"] * merged["buy_quantity"]
    merged["current_value"] = merged["ltp"] * merged["buy_quantity"]
    merged["pnl"] = merged["current_value"] - merged["invested_value"]
    merged["pnl_pct"] = ((merged["ltp"] - merged["buy_price"]) / merged["buy_price"] * 100.0).where(merged["buy_price"] > 0)

    exit_bits = merged.apply(lambda r: pd.Series(build_exit_plan(r, float(r["buy_price"]))), axis=1)
    merged = pd.concat([merged, exit_bits], axis=1)

    score_rank = (
        merged[["symbol", "overall_score"]]
        .drop_duplicates()
        .sort_values(["overall_score", "symbol"], ascending=[False, True])
        .reset_index(drop=True)
    )
    score_rank["portfolio_rank"] = range(1, len(score_rank) + 1)
    merged = merged.merge(score_rank, on=["symbol", "overall_score"], how="left")

    merged = merged.sort_values(["portfolio_rank", "overall_score", "symbol", "added_at"], ascending=[True, False, True, True]).reset_index(drop=True)
    return merged


def latest_portfolio_news(news_articles: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
    if news_articles.empty or not symbols:
        return pd.DataFrame()
    wanted = {str(x).upper().strip() for x in symbols if str(x).strip()}
    out = news_articles[news_articles["symbol"].astype(str).str.upper().isin(wanted)].copy()
    if out.empty:
        return out
    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce", utc=True)
    return out.sort_values(["published_at", "article_sentiment_raw"], ascending=[False, False])


def main() -> None:
    if not require_login():
        return

    db_path = Path(MASTER_DB)
    if not db_path.is_absolute():
        db_path = APP_DIR / db_path
    ensure_static_dashboard_table(db_path)
    existing_portfolio = load_persisted_portfolio(db_path)
    priority_symbols = portfolio_symbols(existing_portfolio)

    with st.sidebar:
        st.header("SQL + News Engine")
        st.caption("Prices flow into SQLite from NSE/Yahoo/cache. News scoring is cached in the same database.")
        st.text_input("Database path", value=str(db_path), disabled=True)
        refresh_mode = st.selectbox("Refresh mode", ["Quick", "Standard", "Deep"], index=1)
        refresh_universe = st.selectbox(
            "Refresh basket",
            ["ALL", "NIFTY 50", "BANK NIFTY", "NIFTY 500", "REMAINING OTHER"],
            index=0,
        )
        mode_map = {"Quick": 500, "Standard": 1200, "Deep": 3000}
        basket_hint_map = {
            "ALL": mode_map[refresh_mode],
            "NIFTY 50": 50,
            "BANK NIFTY": 12,
            "NIFTY 500": 500,
            "REMAINING OTHER": max(100, mode_map[refresh_mode]),
        }
        default_sample = basket_hint_map.get(refresh_universe, mode_map[refresh_mode])
        sample_limit = st.number_input(
            "Active market refresh symbols",
            min_value=10,
            max_value=6000,
            value=int(default_sample),
            step=10,
            help="This caps how many symbols are refreshed from the selected basket.",
        )
        news_limit = st.slider("News API request budget", min_value=0, max_value=90, value=35, step=5)
        st.caption("With the current NewsAPI key, higher budgets may hit plan limits. Cached news is reused for the rest.")
        if priority_symbols:
            st.caption(f"Portfolio priority in news refresh: {len(priority_symbols)} symbols")
        if st.button("API Update / Refresh Database", use_container_width=True):
            with st.spinner("Refreshing market prices, history, and news scores..."):
                refresh_market_database(
                    str(db_path),
                    int(sample_limit),
                    int(news_limit),
                    refresh_universe,
                    priority_symbols=priority_symbols,
                )
                load_bundle.clear()
                load_symbol_history.clear()
                load_symbol_news.clear()
                load_news_for_symbols.clear()
            st.success(f"Database refreshed successfully for {refresh_universe}.")
            st.rerun()
        st.divider()
        st.caption(f"News API key detected: {'YES' if NEWS_API_KEY else 'NO'}")

    if not db_path.exists():
        st.warning(f"Database not found: {db_path}. A new database file was created for the editable dashboard table.")

    db_mtime = get_db_mtime(db_path)
    bundle = load_bundle(str(db_path), db_mtime)
    app_view = normalize_app_view(bundle.get("App_Input_View"))
    logs = bundle.get("Provider_Log")
    refresh_control = bundle.get("Refresh_Control")
    cfg = config_map(bundle.get("Config"))
    portfolio_df = normalize_portfolio(bundle.get(PORTFOLIO_TABLE))
    if portfolio_df.empty:
        portfolio_df = load_persisted_portfolio(db_path)
    symbol_name_map = company_lookup_map(app_view)

    last_refresh = "—"
    refresh_status = "UNKNOWN"
    active_market_refresh = "—"
    if not refresh_control.empty and {"Control", "Value"}.issubset(refresh_control.columns):
        rc = dict(zip(refresh_control["Control"].astype(str), refresh_control["Value"]))
        last_refresh = str(rc.get("last_refresh_ts", "—"))
        refresh_status = str(rc.get("refresh_status", "UNKNOWN"))
        active_market_refresh = str(rc.get("active_market_refresh_count", "—"))

    refresh_badge_class = "positive" if str(refresh_status).upper() == "SUCCESS" else ("warning" if str(refresh_status).upper() in {"RUNNING", "IN_PROGRESS"} else "danger")
    st.markdown(
        f"""
        <div class="app-hero">
            <div class="app-hero-title">{APP_TITLE}</div>
            <div class="app-hero-subtitle">Professional trading workspace for screening, position tracking, technical ranking, and news-led decision support.</div>
            <div class="app-badges">
                <span class="app-badge {refresh_badge_class}">Refresh status: {refresh_status}</span>
                <span class="app-badge">Universe tracked: {len(app_view):,}</span>
                <span class="app-badge positive">Portfolio positions: {len(portfolio_df):,}</span>
                <span class="app-badge warning">News priority symbols: {len(portfolio_symbols(portfolio_df)):,}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top1, top2, top3, top4, top5, top6 = st.columns(6)
    top1.metric("Database Last Refresh", last_refresh)
    top2.metric("Refresh Status", refresh_status)
    top3.metric("Universe in App View", f"{len(app_view):,}")
    top4.metric("Actionable Stocks", f"{int((app_view['actionable_flag'] == 'YES').sum()) if not app_view.empty else 0:,}")
    top5.metric(
        "News-covered Stocks",
        f"{int((app_view['news_article_count_7d'].fillna(0) > 0).sum()) if not app_view.empty and 'news_article_count_7d' in app_view.columns else 0:,}",
    )
    top6.metric("Portfolio Positions", f"{len(portfolio_df):,}")

    has_market_data = not app_view.empty

    tabs = st.tabs(["HTML Dashboard", "Portfolio", "Screener", "Selected Stock", "News Monitor", "Provider Health"])

    with tabs[0]:
        st.subheader("Editable HTML Dashboard")
        st.caption("Same HTML-style dashboard view with SQLite-backed add, edit, delete, search, and CSV export. This tab is not connected to live data.")
        html_df = load_static_dashboard(db_path)
        search_html = st.text_input("Search company", placeholder="RELIANCE / SBIN / HDFCBANK", key="html_dashboard_search")
        display_df = html_df[html_df["Symbol"].astype(str).str.contains(search_html.strip(), case=False, na=False)].copy() if search_html.strip() else html_df.copy()
        components.html(render_html_dashboard(display_df), height=760, scrolling=True)

        a1, a2, a3, a4 = st.columns(4)
        new_symbol = a1.text_input("Add symbol", key="html_add_symbol")
        new_current = a2.number_input("Current Price", min_value=0.0, value=0.0, step=0.05, key="html_add_current")
        new_high = a3.number_input("52W High", min_value=0.0, value=0.0, step=0.05, key="html_add_high")
        new_low = a4.number_input("52W Low", min_value=0.0, value=0.0, step=0.05, key="html_add_low")

        b1, b2 = st.columns([1.2, 2.0])
        if b1.button("Add row", use_container_width=True, key="html_add_row"):
            symbol = new_symbol.strip().upper()
            if not symbol:
                st.error("Enter a symbol before adding a row.")
            else:
                new_row = pd.DataFrame([{
                    "row_id": f"{symbol}-{uuid4().hex[:8]}",
                    "Symbol": symbol,
                    "Current Price": float(new_current),
                    "52W High": float(new_high),
                    "52W Low": float(new_low),
                }])
                save_static_dashboard(db_path, pd.concat([html_df, new_row], ignore_index=True))
                st.success(f"Added {symbol} to the dashboard.")
                st.rerun()
        b2.download_button(
            "Download HTML Dashboard CSV",
            data=dataframe_to_csv_bytes(html_df.drop(columns=["row_id"], errors="ignore")),
            file_name="html_dashboard_rows.csv",
            mime="text/csv",
            use_container_width=True,
            key="download_html_dashboard_csv",
        )

        edited = st.data_editor(
            html_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="html_dashboard_editor",
            column_config={
                "row_id": st.column_config.TextColumn("row_id", disabled=True),
                "Symbol": st.column_config.TextColumn("Symbol", required=True),
                "Current Price": st.column_config.NumberColumn("Current Price", min_value=0.0, step=0.05, format="%.2f"),
                "52W High": st.column_config.NumberColumn("52W High", min_value=0.0, step=0.05, format="%.2f"),
                "52W Low": st.column_config.NumberColumn("52W Low", min_value=0.0, step=0.05, format="%.2f"),
                "% from High": st.column_config.NumberColumn("% from High", disabled=True, format="%.2f"),
                "% from Low": st.column_config.NumberColumn("% from Low", disabled=True, format="%.2f"),
            },
        )
        if not edited.empty:
            missing_ids = edited["row_id"].isna() | (edited["row_id"].astype(str).str.strip() == "")
            if missing_ids.any():
                edited.loc[missing_ids, "row_id"] = [f"row-{uuid4().hex[:10]}" for _ in range(int(missing_ids.sum()))]

        c1, c2 = st.columns([1.2, 2.0])
        if c1.button("Save changes", use_container_width=True, key="html_save_changes"):
            save_static_dashboard(db_path, edited)
            st.success("Dashboard changes saved successfully.")
            st.rerun()

        delete_options = {f"{row.Symbol} | ₹{float(row._2):,.2f}": row.row_id for row in html_df[["row_id", "Symbol", "Current Price"]].itertuples(index=False)}
        selected_delete = c2.multiselect("Delete rows", options=list(delete_options.keys()), key="html_delete_rows")
        if selected_delete and st.button("Delete selected rows", use_container_width=True, key="html_delete_button"):
            remaining = html_df[~html_df["row_id"].isin([delete_options[x] for x in selected_delete])].copy()
            save_static_dashboard(db_path, remaining)
            st.success("Selected rows deleted.")
            st.rerun()

    if not has_market_data:
        st.info("Market tables are empty before refresh. The HTML Dashboard tab above is fully usable now; the other tabs will populate after market refresh.")
        return

    with tabs[1]:
        st.subheader("Portfolio Tracker")

        add_col, refresh_col = st.columns([1.8, 1.2])
        with add_col:
            with st.form("portfolio_add_form", clear_on_submit=True):
                symbol_options = app_view.sort_values(["symbol"])["symbol"].tolist()
                selected_add_symbol = st.selectbox(
                    "Stock name / symbol",
                    options=symbol_options,
                    format_func=lambda s: f"{s} · {symbol_name_map.get(s, s)}",
                )
                p1, p2 = st.columns(2)
                buy_price = p1.number_input("Buy price", min_value=0.01, value=100.0, step=0.05)
                buy_quantity = p2.number_input("Buy quantity", min_value=1, value=1, step=1)
                submitted = st.form_submit_button("Add stock to portfolio", use_container_width=True)
                if submitted:
                    company_name = symbol_name_map.get(selected_add_symbol, selected_add_symbol)
                    add_portfolio_position(db_path, portfolio_df, selected_add_symbol, company_name, float(buy_price), int(buy_quantity))
                    load_bundle.clear()
                    load_symbol_history.clear()
                    load_symbol_news.clear()
                    load_news_for_symbols.clear()
                    st.success(f"Added {selected_add_symbol} to portfolio.")
                    st.rerun()

        with refresh_col:
            st.markdown("### Portfolio refresh")
            st.caption("This updates only your portfolio symbols and gives them first priority in news checks.")
            unique_portfolio_symbols = portfolio_symbols(portfolio_df)
            if st.button("Refresh Portfolio Stocks Only", use_container_width=True, disabled=not bool(unique_portfolio_symbols)):
                with st.spinner("Refreshing only portfolio positions..."):
                    refresh_market_database(
                        str(db_path),
                        sample_limit=max(len(unique_portfolio_symbols), 1),
                        news_limit=int(news_limit),
                        refresh_universe="ALL",
                        priority_symbols=unique_portfolio_symbols,
                        refresh_symbols=unique_portfolio_symbols,
                    )
                    load_bundle.clear()
                load_symbol_history.clear()
                load_symbol_news.clear()
                load_news_for_symbols.clear()
                st.success("Portfolio stocks refreshed successfully.")
                st.rerun()
            if not unique_portfolio_symbols:
                st.info("Add at least one stock to enable portfolio-only refresh.")

        portfolio_view = build_portfolio_view(portfolio_df, app_view)
        if portfolio_view.empty:
            st.info("No portfolio positions added yet.")
        else:
            total_invested = portfolio_view["invested_value"].sum()
            total_current = portfolio_view["current_value"].fillna(0).sum()
            total_pnl = portfolio_view["pnl"].fillna(0).sum()
            total_pnl_pct = (total_pnl / total_invested * 100.0) if total_invested else 0.0
            pf1, pf2, pf3, pf4, pf5 = st.columns(5)
            pf1.metric("Invested Value", fmt_price(total_invested))
            pf2.metric("Current Value", fmt_price(total_current))
            pf3.metric("Total P/L", fmt_price(total_pnl), delta=fmt_pct(total_pnl_pct))
            pf4.metric("Best Ranked Holding", str(portfolio_view.sort_values(["portfolio_rank", "symbol"]).iloc[0]["symbol"]))
            pf5.metric("Live Portfolio Symbols", f"{len(portfolio_symbols(portfolio_df))}")

            display_pf = portfolio_view[
                [
                    "portfolio_rank",
                    "symbol",
                    "company_name",
                    "buy_price",
                    "buy_quantity",
                    "ltp",
                    "invested_value",
                    "current_value",
                    "pnl",
                    "pnl_pct",
                    "technical_score",
                    "news_score",
                    "overall_score",
                    "recommendation",
                    "headline_risk_flag",
                    "target_exit_price",
                    "stop_loss_price",
                    "action",
                    "news_article_count_7d",
                ]
            ].copy()
            st.dataframe(display_pf, use_container_width=True, hide_index=True)
            st.download_button(
                "Download Portfolio CSV",
                data=dataframe_to_csv_bytes(display_pf),
                file_name="portfolio_stocks.csv",
                mime="text/csv",
                use_container_width=True,
                key="download_portfolio_csv",
            )

            with st.expander("Portfolio exit notes and reasons", expanded=False):
                exit_notes = portfolio_view[
                    [
                        "symbol",
                        "company_name",
                        "recommendation",
                        "overall_score",
                        "news_score",
                        "headline_risk_flag",
                        "action",
                        "exit_note",
                        "news_summary",
                        "top_positive_headline",
                        "top_negative_headline",
                    ]
                ].copy()
                st.dataframe(exit_notes, use_container_width=True, hide_index=True)

            with st.expander("Remove stock from portfolio", expanded=False):
                removable = {
                    f"{r.symbol} | Buy {r.buy_quantity:g} @ ₹{r.buy_price:,.2f} | Added {pd.to_datetime(r.added_at).strftime('%Y-%m-%d %H:%M') if pd.notna(r.added_at) else 'NA'}": r.position_id
                    for r in portfolio_view.itertuples()
                }
                remove_labels = st.multiselect(
                    "Select positions to remove",
                    options=list(removable.keys()),
                    key="portfolio_remove_labels",
                    placeholder="Choose one or more holdings",
                )
                if remove_labels:
                    st.markdown(
                        f"<div class='delete-callout'><strong>Delete ready:</strong> {len(remove_labels)} selected holding(s) will be removed from the portfolio when you press the delete button.</div>",
                        unsafe_allow_html=True,
                    )
                    delete_col, note_col = st.columns([1.2, 1.8])
                    with delete_col:
                        if st.button("Delete selected positions", key="portfolio_delete_button", use_container_width=True):
                            delete_portfolio_positions(db_path, portfolio_df, [removable[x] for x in remove_labels])
                            st.session_state["portfolio_remove_labels"] = []
                            load_bundle.clear()
                            load_symbol_history.clear()
                            load_symbol_news.clear()
                            load_news_for_symbols.clear()
                            st.success("Selected portfolio positions deleted.")
                            st.rerun()
                    with note_col:
                        st.caption("Delete action appears only after you select at least one portfolio stock.")
                else:
                    st.caption("Select one or more portfolio holdings to make the delete button appear.")

            portfolio_news = latest_portfolio_news(load_news_for_symbols(str(db_path), db_mtime, tuple(portfolio_symbols(portfolio_df)), limit=80), portfolio_symbols(portfolio_df))
            if portfolio_news.empty:
                st.info("No recent cached news for portfolio holdings.")
            else:
                st.markdown("### Latest portfolio headlines")
                news_display = portfolio_news[
                    ["published_at", "symbol", "source_name", "title", "article_sentiment_label", "article_sentiment_raw", "url"]
                ].copy()
                news_display["published_at"] = news_display["published_at"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M")
                st.dataframe(news_display.head(20), use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("Fast Screener")
        f1, f2, f3, f4, f5 = st.columns([1.2, 1.0, 1.0, 1.0, 1.7])
        raw_tags = ", ".join(app_view["tags"].fillna("").astype(str).tolist())
        tag_options = sorted({tag.strip() for tag in raw_tags.split(",") if tag.strip()})
        selected_tag = f1.selectbox("Universe tag", ["All"] + tag_options, index=0)
        actionable = f2.selectbox("Actionable", ["All", "YES", "NO"], index=0)
        min_score = f3.slider("Min technical score", min_value=0, max_value=100, value=55)
        min_news = f4.slider("Min news score", min_value=0, max_value=100, value=0)
        search = f5.text_input("Search", placeholder="SBIN / RELIANCE / HDFCBANK")

        filtered = app_view.copy()
        if selected_tag != "All":
            filtered = filtered[filtered["tags"].fillna("").str.contains(selected_tag, case=False, na=False)]
        if actionable != "All":
            filtered = filtered[filtered["actionable_flag"] == actionable]
        filtered = filtered[filtered["technical_score"].fillna(0) >= min_score]
        filtered = filtered[filtered["news_score"].fillna(50) >= min_news]
        if search.strip():
            q = search.strip()
            filtered = filtered[
                filtered["symbol"].str.contains(q, case=False, na=False)
                | filtered["company_name"].astype(str).str.contains(q, case=False, na=False)
            ]

        filtered = filtered.sort_values(["overall_score", "technical_score", "news_score"], ascending=[False, False, False])
        screener_display = filtered[
            [
                "symbol",
                "company_name",
                "exchange_primary",
                "tags",
                "ltp",
                "change_1d_pct",
                "change_1m_pct",
                "change_3m_pct",
                "change_12m_pct",
                "technical_score",
                "news_score",
                "news_article_count_7d",
                "headline_risk_flag",
                "overall_score",
                "recommendation",
                "actionable_flag",
                "status_note",
            ]
        ].copy()
        st.dataframe(
            screener_display,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download Screener CSV",
            data=dataframe_to_csv_bytes(screener_display),
            file_name="screener_stocks.csv",
            mime="text/csv",
            use_container_width=True,
            key="download_screener_csv",
        )

        st.caption("Recommendation uses technical trend, data quality, liquidity, and news score. High headline risk blocks BUY.")

        options = filtered["symbol"].tolist() if not filtered.empty else app_view["symbol"].tolist()
        if not options:
            st.info("No stocks available after applying filters.")
            return
        default_symbol = st.session_state.get("selected_symbol", options[0])
        default_index = options.index(default_symbol) if default_symbol in options else 0
        selected_symbol = st.selectbox("Selected stock", options, index=default_index)
        st.session_state["selected_symbol"] = selected_symbol

    selected_symbol = st.session_state.get("selected_symbol", app_view.iloc[0]["symbol"])
    if selected_symbol not in app_view["symbol"].tolist():
        selected_symbol = app_view.iloc[0]["symbol"]
    row = app_view[app_view["symbol"] == selected_symbol].iloc[0]
    hist = history_for_symbol(load_symbol_history(str(db_path), db_mtime, selected_symbol, limit=320), selected_symbol)
    stock_news = news_for_symbol(load_symbol_news(str(db_path), db_mtime, selected_symbol, limit=30), selected_symbol)

    with tabs[3]:
        st.subheader(f"{selected_symbol} · {row.get('company_name', '')}")
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("LTP", fmt_price(row.get("ltp")))
        c2.metric("1D", fmt_pct(row.get("change_1d_pct")))
        c3.metric("1W", fmt_pct(row.get("change_1w_pct")))
        c4.metric("1M", fmt_pct(row.get("change_1m_pct")))
        c5.metric("3M", fmt_pct(row.get("change_3m_pct")))
        c6.metric("12M", fmt_pct(row.get("change_12m_pct")))

        d1, d2, d3, d4, d5, d6 = st.columns(6)
        d1.metric("Tech Score", fmt_num(row.get("technical_score"), 0))
        d2.metric("News Score", fmt_num(row.get("news_score"), 0))
        d3.metric("Overall Score", fmt_num(row.get("overall_score"), 0))
        d4.metric("Trend Quality", fmt_num(row.get("trend_quality"), 0))
        d5.metric("Confidence", fmt_num(row.get("recommendation_confidence"), 0))
        d6.metric("History Days", fmt_num(row.get("history_days"), 0))

        st.markdown(
            f"**Exchange:** {row.get('exchange_primary', '')}  |  **Universe:** {row.get('tags', '')}  |  **Recommendation:** {row.get('recommendation', '')}  |  **Actionable:** {row.get('actionable_flag', '')}"
        )
        st.markdown(
            f"**Technical regime:** {row.get('technical_regime', 'NA')}  |  **Headline risk:** {row.get('headline_risk_flag', 'LOW')}  |  **History source:** {row.get('history_source_hint', 'Unknown')}"
        )
        st.markdown(f"**Recommendation reason:** {row.get('recommendation_reason', '')}")
        st.markdown(f"**Status:** {row.get('status_note', '')}")
        st.markdown(f"**News summary:** {row.get('news_summary', 'No recent cached news')}")

        left, right = st.columns([1.6, 1.1])
        with left:
            st.markdown("### Price history")
            if not hist.empty and {"date", "close"}.issubset(hist.columns):
                chart_df = hist[["date", "close"]].dropna().set_index("date")
                st.line_chart(chart_df)
            else:
                st.info("No price history found for this stock.")

        with right:
            st.markdown("### Technical + news snapshot")
            tech = pd.DataFrame(
                {
                    "Metric": [
                        "SMA20",
                        "SMA50",
                        "SMA200",
                        "EMA20",
                        "RSI14",
                        "ATR14",
                        "Volume Ratio",
                        "52W Range %",
                        "Liquidity Score",
                        "Data Quality",
                        "News Confidence",
                        "Articles (7D)",
                        "Bullish Articles",
                        "Bearish Articles",
                        "History Recency (days)",
                        "Recommendation Confidence",
                        "Reason",
                    ],
                    "Value": [
                        row.get("sma20"),
                        row.get("sma50"),
                        row.get("sma200"),
                        row.get("ema20"),
                        row.get("rsi14"),
                        row.get("atr14"),
                        row.get("volume_ratio"),
                        row.get("range_position_52w_pct"),
                        row.get("liquidity_score"),
                        row.get("dqi_final"),
                        row.get("news_confidence"),
                        row.get("news_article_count_7d"),
                        row.get("news_bullish_count"),
                        row.get("news_bearish_count"),
                        row.get("history_recency_days"),
                        row.get("recommendation_confidence"),
                        row.get("recommendation_reason"),
                    ],
                }
            )
            st.dataframe(tech, use_container_width=True, hide_index=True)

        if stock_news.empty:
            st.info("No recent cached news for this stock.")
        else:
            st.markdown("### Latest headlines")
            display_news = stock_news[["published_at", "source_name", "title", "article_sentiment_label", "article_sentiment_raw", "url"]].copy()
            display_news["published_at"] = display_news["published_at"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(display_news.head(12), use_container_width=True, hide_index=True)

    with tabs[4]:
        st.subheader("News Monitor")
        n1, n2 = st.columns(2)
        bullish = app_view.sort_values(["news_score", "news_article_count_7d"], ascending=[False, False]).head(15)
        bearish = app_view.sort_values(["news_score", "headline_risk_flag"], ascending=[True, True]).head(15)
        with n1:
            st.markdown("#### Stronger news flow")
            st.dataframe(
                bullish[["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_positive_headline"]],
                use_container_width=True,
                hide_index=True,
            )
        with n2:
            st.markdown("#### Weaker / riskier news flow")
            st.dataframe(
                bearish[["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_negative_headline"]],
                use_container_width=True,
                hide_index=True,
            )

        st.caption("The current NewsAPI key gives delayed developer-plan data. Cached scores still help rank recent sentiment, but this is not true exchange-grade real-time news.")

    with tabs[5]:
        st.subheader("Provider Health")
        st.dataframe(provider_summary(logs), use_container_width=True, hide_index=True)
        if cfg:
            st.markdown("### Engine config")
            st.dataframe(pd.DataFrame([{"key": k, "value": v} for k, v in cfg.items()]), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
