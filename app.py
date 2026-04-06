from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st

from market_db import MarketDatabase

MASTER_DB = os.environ.get("MASTER_MARKET_DB", "master_market_data.db")
APP_TITLE = "AR Tiger Tech Analysis"
USERNAME = os.environ.get("APP_USERNAME", "Anand1234")
PASSWORD = os.environ.get("APP_PASSWORD", "618523")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "f8d8079a60f84e02bb987ed0ad62b79d")
PORTFOLIO_TABLE = "portfolio_positions"

st.set_page_config(page_title=APP_TITLE, page_icon="📈", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    .block-container {padding-top: 0.8rem; padding-bottom: 1rem;}
    .stMetric {background: rgba(250,250,250,0.02); border-radius: 12px; padding: 0.6rem;}
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
        "Daily_History",
        "Latest_Signals",
        "News_Articles",
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


def _ensure_columns(df: pd.DataFrame, columns: List[str], fill_value="") -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = fill_value
    return out


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
    if "symbol" not in out.columns:
        out["symbol"] = ""
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
    out = out[out["symbol"].str.len() > 0]
    out = out[out["buy_price"] > 0]
    out = out[out["buy_quantity"] > 0]
    out["added_at"] = out["added_at"].fillna(pd.Timestamp.now(tz="Asia/Kolkata"))
    return out[columns].reset_index(drop=True)


def history_for_symbol(history: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if history.empty or "symbol" not in history.columns:
        return pd.DataFrame()
    x = history[history["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if x.empty:
        return x
    if "date" in x.columns:
        x["date"] = pd.to_datetime(x["date"], errors="coerce")
    return x.sort_values("date") if "date" in x.columns else x


def news_for_symbol(news_articles: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if news_articles.empty or "symbol" not in news_articles.columns:
        return pd.DataFrame()
    x = news_articles[news_articles["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if x.empty:
        return x
    if "published_at" in x.columns:
        x["published_at"] = pd.to_datetime(x["published_at"], errors="coerce", utc=True)
    if "article_sentiment_raw" not in x.columns:
        x["article_sentiment_raw"] = 0
    sort_cols = [c for c in ["published_at", "article_sentiment_raw"] if c in x.columns]
    if sort_cols:
        return x.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    return x


def provider_summary(logs: pd.DataFrame) -> pd.DataFrame:
    if logs.empty:
        return pd.DataFrame(columns=["provider", "dataset", "status", "rows_loaded", "avg_latency_ms", "latest_error"])
    cols = [c for c in ["provider", "dataset", "status", "rows_loaded", "avg_latency_ms", "latest_error"] if c in logs.columns]
    return logs[cols].copy().sort_values([c for c in ["provider", "dataset"] if c in cols])


def config_map(df: pd.DataFrame) -> Dict[str, str]:
    if df.empty or not {"key", "value"}.issubset(df.columns):
        return {}
    return {str(k): str(v) for k, v in zip(df["key"], df["value"])}


def portfolio_symbols(portfolio_df: pd.DataFrame) -> List[str]:
    if portfolio_df.empty or "symbol" not in portfolio_df.columns:
        return []
    return sorted({str(x).upper().strip() for x in portfolio_df["symbol"].tolist() if str(x).strip()})


def load_portfolio_now(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(columns=["position_id", "symbol", "company_name", "buy_price", "buy_quantity", "added_at"])
    db = MarketDatabase(str(db_path))
    return normalize_portfolio(db.read_table(PORTFOLIO_TABLE))


def save_portfolio(db_path: Path, portfolio_df: pd.DataFrame) -> None:
    db = MarketDatabase(str(db_path))
    db.initialize()
    db.write_table(PORTFOLIO_TABLE, normalize_portfolio(portfolio_df))


def add_portfolio_position(db_path: Path, portfolio_df: pd.DataFrame, symbol: str, company_name: str, buy_price: float, buy_quantity: int) -> None:
    symbol = str(symbol).upper().strip()
    new_row = pd.DataFrame(
        [
            {
                "position_id": f"{symbol}-{pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y%m%d%H%M%S%f')}",
                "symbol": symbol,
                "company_name": str(company_name or symbol),
                "buy_price": float(buy_price),
                "buy_quantity": int(buy_quantity),
                "added_at": pd.Timestamp.now(tz="Asia/Kolkata"),
            }
        ]
    )
    updated = pd.concat([normalize_portfolio(portfolio_df), new_row], ignore_index=True)
    save_portfolio(db_path, updated)


def delete_portfolio_positions(db_path: Path, portfolio_df: pd.DataFrame, position_ids: List[str]) -> None:
    work = normalize_portfolio(portfolio_df)
    if not position_ids:
        save_portfolio(db_path, work)
        return
    remaining = work[~work["position_id"].astype(str).isin([str(x) for x in position_ids])].copy()
    save_portfolio(db_path, remaining)


def compute_portfolio_view(portfolio_df: pd.DataFrame, app_view: pd.DataFrame) -> pd.DataFrame:
    portfolio_df = normalize_portfolio(portfolio_df)
    if portfolio_df.empty:
        return pd.DataFrame()

    app_view = normalize_app_view(app_view)
    lookup = app_view.set_index("symbol", drop=False) if not app_view.empty else pd.DataFrame()
    rows = []
    for _, pos in portfolio_df.iterrows():
        symbol = str(pos.get("symbol") or "").upper().strip()
        app_row = lookup.loc[symbol] if (not lookup.empty and symbol in lookup.index) else None
        company_name = str(pos.get("company_name") or symbol)
        if app_row is not None:
            company_name = str(app_row.get("company_name") or company_name)

        buy_price = float(pos.get("buy_price") or 0)
        qty = float(pos.get("buy_quantity") or 0)
        invested = buy_price * qty
        ltp = float(app_row.get("ltp")) if (app_row is not None and pd.notna(app_row.get("ltp"))) else float("nan")
        market_value = ltp * qty if pd.notna(ltp) else float("nan")
        pnl_value = (ltp - buy_price) * qty if pd.notna(ltp) else float("nan")
        pnl_pct = ((ltp / buy_price) - 1.0) * 100.0 if (pd.notna(ltp) and buy_price > 0) else float("nan")

        overall = float(app_row.get("overall_score")) if (app_row is not None and pd.notna(app_row.get("overall_score"))) else float("nan")
        technical = float(app_row.get("technical_score")) if (app_row is not None and pd.notna(app_row.get("technical_score"))) else float("nan")
        news_score = float(app_row.get("news_score")) if (app_row is not None and pd.notna(app_row.get("news_score"))) else 50.0
        atr14 = float(app_row.get("atr14")) if (app_row is not None and pd.notna(app_row.get("atr14"))) else 0.0
        headline_risk = str(app_row.get("headline_risk_flag") or "LOW") if app_row is not None else "LOW"
        recommendation = str(app_row.get("recommendation") or "NO DATA") if app_row is not None else "NO DATA"
        news_summary = str(app_row.get("news_summary") or "No recent cached news") if app_row is not None else "No recent cached news"

        base_target_pct = 3.0
        target_days = 25
        action = "Data Pending"
        if pd.notna(overall):
            if overall >= 85:
                base_target_pct, target_days, action = 12.0, 8, "Hold / Trail"
            elif overall >= 75:
                base_target_pct, target_days, action = 9.0, 12, "Hold"
            elif overall >= 65:
                base_target_pct, target_days, action = 7.0, 16, "Hold / Review"
            elif overall >= 55:
                base_target_pct, target_days, action = 5.0, 20, "Review Closely"
            elif overall >= 45:
                base_target_pct, target_days, action = 3.0, 10, "Reduce / Exit on Strength"
            else:
                base_target_pct, target_days, action = 1.5, 5, "Exit / Protect Capital"

        if news_score >= 70 and headline_risk == "LOW":
            base_target_pct += 1.5
        elif news_score < 40:
            base_target_pct -= 1.5
        if headline_risk == "MEDIUM":
            base_target_pct -= 1.0
        elif headline_risk == "HIGH":
            base_target_pct -= 3.0
            action = "Exit / Tight Stop"
            target_days = min(target_days, 7)
        base_target_pct = max(1.0, min(base_target_pct, 15.0))

        target_from_buy = buy_price * (1.0 + base_target_pct / 100.0)
        target_from_ltp = ltp + max(atr14 * 1.25, ltp * 0.01) if pd.notna(ltp) else float("nan")
        if pd.notna(target_from_ltp):
            target_exit = max(target_from_buy, target_from_ltp)
        else:
            target_exit = target_from_buy

        stop_pct = 5.0
        if pd.notna(overall):
            if overall >= 75:
                stop_pct = 6.0
            elif overall >= 60:
                stop_pct = 5.0
            elif overall >= 45:
                stop_pct = 4.0
            else:
                stop_pct = 2.5
        stop_from_buy = buy_price * (1.0 - stop_pct / 100.0)
        stop_from_ltp = ltp - max(atr14 * 1.1, ltp * 0.008) if pd.notna(ltp) else float("nan")
        if pd.notna(stop_from_ltp):
            stop_loss = max(stop_from_buy, stop_from_ltp)
        else:
            stop_loss = stop_from_buy
        if headline_risk == "HIGH":
            stop_loss = max(stop_loss, buy_price * 0.985)

        rows.append(
            {
                "position_id": pos.get("position_id"),
                "symbol": symbol,
                "company_name": company_name,
                "buy_price": buy_price,
                "buy_quantity": qty,
                "invested_value": invested,
                "ltp": ltp,
                "market_value": market_value,
                "pnl_value": pnl_value,
                "pnl_pct": pnl_pct,
                "technical_score": technical,
                "news_score": news_score,
                "overall_score": overall,
                "headline_risk_flag": headline_risk,
                "recommendation": recommendation,
                "suggested_action": action,
                "target_exit_price": target_exit,
                "stop_loss_price": stop_loss,
                "expected_days": target_days,
                "news_summary": news_summary,
                "added_at": pos.get("added_at"),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["ranking"] = out["overall_score"].fillna(-1).rank(method="dense", ascending=False).astype(int)
    out = out.sort_values(["ranking", "overall_score", "symbol"], ascending=[True, False, True]).reset_index(drop=True)
    return out


def main() -> None:
    if not require_login():
        return

    db_path = Path(MASTER_DB)
    current_portfolio = load_portfolio_now(db_path)
    current_portfolio_symbols = portfolio_symbols(current_portfolio)

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
        st.caption("Portfolio stocks get news priority first during full refresh. Use the separate portfolio button for portfolio-only refresh.")
        if st.button("API Update / Refresh Database", use_container_width=True):
            with st.spinner("Refreshing market prices, history, and news scores..."):
                refresh_market_database(
                    str(db_path),
                    int(sample_limit),
                    int(news_limit),
                    refresh_universe,
                    priority_symbols=current_portfolio_symbols,
                )
                load_bundle.clear()
            st.success(f"Database refreshed successfully for {refresh_universe}.")
            st.rerun()

        portfolio_only_disabled = len(current_portfolio_symbols) == 0
        if st.button("Refresh Portfolio Stocks Only", use_container_width=True, disabled=portfolio_only_disabled):
            with st.spinner("Refreshing only portfolio symbols..."):
                refresh_market_database(
                    str(db_path),
                    max(len(current_portfolio_symbols), 10),
                    max(len(current_portfolio_symbols), 5),
                    "ALL",
                    priority_symbols=current_portfolio_symbols,
                    refresh_symbols=current_portfolio_symbols,
                )
                load_bundle.clear()
            st.success("Portfolio symbols refreshed successfully.")
            st.rerun()
        if portfolio_only_disabled:
            st.caption("Add at least one portfolio stock to enable portfolio-only refresh.")
        st.divider()
        st.caption(f"News API key detected: {'YES' if NEWS_API_KEY else 'NO'}")

    st.title(APP_TITLE)

    if db_path.exists():
        bundle = load_bundle(str(db_path), db_path.stat().st_mtime)
    else:
        bundle = DatabaseBundle(str(db_path), 0.0, {})

    app_view = normalize_app_view(bundle.get("App_Input_View"))
    history = bundle.get("Daily_History")
    news_articles = bundle.get("News_Articles")
    logs = bundle.get("Provider_Log")
    refresh_control = bundle.get("Refresh_Control")
    cfg = config_map(bundle.get("Config"))
    portfolio_df = normalize_portfolio(bundle.get(PORTFOLIO_TABLE)) if db_path.exists() else current_portfolio
    if portfolio_df.empty and not current_portfolio.empty:
        portfolio_df = current_portfolio

    last_refresh = "—"
    refresh_status = "UNKNOWN"
    active_market_refresh = "—"
    if not refresh_control.empty and {"Control", "Value"}.issubset(refresh_control.columns):
        rc = dict(zip(refresh_control["Control"].astype(str), refresh_control["Value"]))
        last_refresh = str(rc.get("last_refresh_ts", "—"))
        refresh_status = str(rc.get("refresh_status", "UNKNOWN"))
        active_market_refresh = str(rc.get("active_market_refresh_count", "—"))

    top1, top2, top3, top4, top5 = st.columns(5)
    top1.metric("Database Last Refresh", last_refresh)
    top2.metric("Universe in App View", f"{len(app_view):,}")
    top3.metric("Actionable Stocks", f"{int((app_view['actionable_flag'] == 'YES').sum()) if not app_view.empty else 0:,}")
    top4.metric("News-covered Stocks", f"{int((app_view['news_article_count_7d'].fillna(0) > 0).sum()) if (not app_view.empty and 'news_article_count_7d' in app_view.columns) else 0:,}")
    top5.metric("Active Market Refresh", active_market_refresh)

    tabs = st.tabs(["Screener", "Selected Stock", "Portfolio", "News Monitor", "Provider Health"])

    selected_symbol = None
    with tabs[0]:
        st.subheader("Fast Screener")
        if app_view.empty:
            st.warning("app_input_view is empty. Refresh database first.")
        else:
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
            screener_cols = [
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
            st.dataframe(_ensure_columns(filtered, screener_cols)[screener_cols], use_container_width=True, hide_index=True)

            st.caption("Recommendation uses technical trend, data quality, liquidity, and news score. High headline risk blocks BUY.")

            options = filtered["symbol"].tolist() if not filtered.empty else app_view["symbol"].tolist()
            if options:
                selected_symbol = st.selectbox("Selected stock", options, index=0)
                st.session_state["selected_symbol"] = selected_symbol
            else:
                st.info("No stocks available after applying filters.")

    if app_view.empty:
        selected_symbol = None
    elif selected_symbol is None:
        selected_symbol = st.session_state.get("selected_symbol", app_view.iloc[0]["symbol"])

    with tabs[1]:
        st.subheader("Selected Stock")
        if not selected_symbol or app_view.empty:
            st.info("Refresh the database and select a stock from the screener.")
        else:
            row = app_view[app_view["symbol"] == selected_symbol].iloc[0]
            hist = history_for_symbol(history, selected_symbol)
            stock_news = news_for_symbol(news_articles, selected_symbol)

            st.markdown(f"### {selected_symbol} · {row.get('company_name', '')}")
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("LTP", fmt_price(row.get("ltp")))
            c2.metric("1D", fmt_pct(row.get("change_1d_pct")))
            c3.metric("1W", fmt_pct(row.get("change_1w_pct")))
            c4.metric("1M", fmt_pct(row.get("change_1m_pct")))
            c5.metric("3M", fmt_pct(row.get("change_3m_pct")))
            c6.metric("12M", fmt_pct(row.get("change_12m_pct")))

            d1, d2, d3, d4, d5 = st.columns(5)
            d1.metric("Tech Score", fmt_num(row.get("technical_score"), 0))
            d2.metric("News Score", fmt_num(row.get("news_score"), 0))
            d3.metric("Overall Score", fmt_num(row.get("overall_score"), 0))
            d4.metric("Trend Quality", fmt_num(row.get("trend_quality"), 0))
            d5.metric("Headline Risk", str(row.get("headline_risk_flag", "LOW")))

            st.markdown(
                f"**Exchange:** {row.get('exchange_primary', '')}  |  **Universe:** {row.get('tags', '')}  |  **Recommendation:** {row.get('recommendation', '')}  |  **Actionable:** {row.get('actionable_flag', '')}"
            )
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
                        ],
                    }
                )
                st.dataframe(tech, use_container_width=True, hide_index=True)

            if stock_news.empty:
                st.info("No recent cached news for this stock.")
            else:
                st.markdown("### Latest headlines")
                news_cols = ["published_at", "source_name", "title", "article_sentiment_label", "article_sentiment_raw", "url"]
                display_news = _ensure_columns(stock_news, news_cols)[news_cols].copy()
                display_news["published_at"] = pd.to_datetime(display_news["published_at"], errors="coerce", utc=True)
                display_news["published_at"] = display_news["published_at"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M")
                st.dataframe(display_news.head(12), use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("Portfolio")
        add_left, add_right = st.columns([1.4, 1.6])
        with add_left:
            entry_mode = st.radio("Add stock by", ["Select from app", "Type symbol"], horizontal=True)
            with st.form("portfolio_add_form", clear_on_submit=True):
                if entry_mode == "Select from app" and not app_view.empty:
                    selectable = app_view[["symbol", "company_name"]].drop_duplicates().sort_values("symbol")
                    selectable["label"] = selectable["symbol"] + " · " + selectable["company_name"].astype(str)
                    selected_label = st.selectbox("Stock name", selectable["label"].tolist(), index=0)
                    selected_match = selectable[selectable["label"] == selected_label].iloc[0]
                    new_symbol = str(selected_match["symbol"])
                    new_company_name = str(selected_match["company_name"])
                else:
                    new_symbol = st.text_input("Stock name / symbol", placeholder="SBIN")
                    new_company_name = st.text_input("Company name (optional)", placeholder="State Bank of India")
                buy_price = st.number_input("Buy price", min_value=0.01, value=100.00, step=0.05)
                buy_quantity = st.number_input("Buy quantity", min_value=1, value=1, step=1)
                add_clicked = st.form_submit_button("Add to Portfolio", use_container_width=True)

            if add_clicked:
                symbol_clean = str(new_symbol).upper().strip()
                if not symbol_clean:
                    st.error("Enter a valid stock symbol.")
                else:
                    fallback_name = new_company_name or symbol_clean
                    if not app_view.empty:
                        match = app_view[app_view["symbol"] == symbol_clean]
                        if not match.empty:
                            fallback_name = str(match.iloc[0].get("company_name") or fallback_name)
                    add_portfolio_position(db_path, portfolio_df, symbol_clean, fallback_name, float(buy_price), int(buy_quantity))
                    load_bundle.clear()
                    st.success(f"Added {symbol_clean} to portfolio.")
                    st.rerun()

        with add_right:
            portfolio_view = compute_portfolio_view(portfolio_df, app_view)
            total_invested = portfolio_view["invested_value"].sum() if not portfolio_view.empty else 0.0
            total_market = portfolio_view["market_value"].sum(min_count=1) if not portfolio_view.empty else float("nan")
            total_pnl = portfolio_view["pnl_value"].sum(min_count=1) if not portfolio_view.empty else float("nan")
            total_pnl_pct = ((total_market / total_invested) - 1.0) * 100.0 if (pd.notna(total_market) and total_invested > 0) else float("nan")
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Portfolio Stocks", f"{len(portfolio_df):,}")
            p2.metric("Total Invested", fmt_price(total_invested))
            p3.metric("Current Value", fmt_price(total_market))
            p4.metric("Total P/L", fmt_price(total_pnl), delta=fmt_pct(total_pnl_pct) if pd.notna(total_pnl_pct) else None)
            st.caption("Target exit, stop loss, and suggested action are derived from current overall score, technical score, ATR, and news sentiment.")

        if portfolio_df.empty:
            st.info("No portfolio positions added yet.")
        else:
            portfolio_cols = [
                "ranking",
                "symbol",
                "company_name",
                "buy_price",
                "buy_quantity",
                "invested_value",
                "ltp",
                "market_value",
                "pnl_value",
                "pnl_pct",
                "technical_score",
                "news_score",
                "overall_score",
                "headline_risk_flag",
                "recommendation",
                "suggested_action",
                "target_exit_price",
                "stop_loss_price",
                "expected_days",
                "news_summary",
            ]
            st.dataframe(_ensure_columns(portfolio_view, portfolio_cols)[portfolio_cols], use_container_width=True, hide_index=True)

            remove_options = portfolio_view[["position_id", "symbol", "buy_quantity"]].copy()
            remove_options["label"] = remove_options["symbol"] + " | Qty " + remove_options["buy_quantity"].astype(int).astype(str) + " | " + remove_options["position_id"].astype(str)
            selected_remove_labels = st.multiselect("Remove positions", remove_options["label"].tolist())
            if st.button("Delete Selected Portfolio Positions", use_container_width=True, disabled=len(selected_remove_labels) == 0):
                delete_ids = remove_options[remove_options["label"].isin(selected_remove_labels)]["position_id"].astype(str).tolist()
                delete_portfolio_positions(db_path, portfolio_df, delete_ids)
                load_bundle.clear()
                st.success("Selected portfolio positions deleted.")
                st.rerun()

        if not portfolio_df.empty and not portfolio_view.empty:
            st.markdown("### Portfolio news priority summary")
            summary_cols = ["symbol", "overall_score", "news_score", "headline_risk_flag", "suggested_action", "target_exit_price", "stop_loss_price", "expected_days"]
            st.dataframe(_ensure_columns(portfolio_view, summary_cols)[summary_cols], use_container_width=True, hide_index=True)

    with tabs[3]:
        st.subheader("News Monitor")
        if app_view.empty:
            st.info("No stock data available yet.")
        else:
            n1, n2 = st.columns(2)
            bullish = app_view.sort_values(["news_score", "news_article_count_7d"], ascending=[False, False]).head(15)
            bearish = app_view.sort_values(["news_score", "headline_risk_flag"], ascending=[True, True]).head(15)
            with n1:
                st.markdown("#### Stronger news flow")
                good_cols = ["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_positive_headline"]
                st.dataframe(_ensure_columns(bullish, good_cols)[good_cols], use_container_width=True, hide_index=True)
            with n2:
                st.markdown("#### Weaker / riskier news flow")
                bad_cols = ["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_negative_headline"]
                st.dataframe(_ensure_columns(bearish, bad_cols)[bad_cols], use_container_width=True, hide_index=True)

            st.caption("The current NewsAPI key gives delayed developer-plan data. Cached scores still help rank recent sentiment, but this is not true exchange-grade real-time news.")

    with tabs[4]:
        st.subheader("Provider Health")
        st.dataframe(provider_summary(logs), use_container_width=True, hide_index=True)
        if cfg:
            st.markdown("### Engine config")
            st.dataframe(pd.DataFrame([{"key": k, "value": v} for k, v in cfg.items()]), use_container_width=True, hide_index=True)
        st.caption(f"Refresh status: {refresh_status}")


if __name__ == "__main__":
    main()
