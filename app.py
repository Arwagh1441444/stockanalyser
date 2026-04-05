from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd
import streamlit as st

from market_db import MarketDatabase

MASTER_DB = os.environ.get("MASTER_MARKET_DB", "master_market_data.db")
APP_TITLE = "AR Tiger Tech Analysis"
USERNAME = os.environ.get("APP_USERNAME", "Anand1234")
PASSWORD = os.environ.get("APP_PASSWORD", "618523")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "f8d8079a60f84e02bb987ed0ad62b79d")

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


def refresh_market_database(db_path: str, sample_limit: int, news_limit: int) -> None:
    from refresh_engine import RefreshEngine

    RefreshEngine(db_path=db_path).refresh(sample_limit=sample_limit, news_limit=news_limit)


def fmt_price(x):
    return "—" if pd.isna(x) else f"₹{float(x):,.2f}"


def fmt_pct(x):
    return "—" if pd.isna(x) else f"{float(x):.2f}%"


def fmt_num(x, digits: int = 1):
    return "—" if pd.isna(x) else f"{float(x):,.{digits}f}"


def normalize_app_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["symbol", "company_name", "tags", "ltp", "technical_score", "news_score", "overall_score", "recommendation", "actionable_flag", "status_note"])
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    numeric_cols = [
        "ltp", "change_1d_pct", "change_1w_pct", "change_1m_pct", "change_3m_pct", "change_6m_pct", "change_9m_pct", "change_12m_pct",
        "technical_score", "rsi14", "sma20", "sma50", "sma200", "volume_ratio", "overall_score", "dqi_final", "ema20", "atr14",
        "range_position_52w_pct", "trend_quality", "news_score", "news_confidence", "news_article_count_7d", "news_article_count_30d", "liquidity_score", "priority",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["company_name", "tags", "recommendation", "actionable_flag", "status_note", "quote_source", "exchange_primary", "headline_risk_flag", "news_summary"]:
        if col not in out.columns:
            out[col] = ""
    if "latest_news_ts" in out.columns:
        out["latest_news_ts"] = pd.to_datetime(out["latest_news_ts"], errors="coerce", utc=True)
    return out


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


def main() -> None:
    if not require_login():
        return

    db_path = Path(MASTER_DB)
    with st.sidebar:
        st.header("SQL + News Engine")
        st.caption("Prices flow into SQLite from NSE/Yahoo/cache. News scoring is cached in the same database.")
        st.text_input("Database path", value=str(db_path), disabled=True)
        refresh_mode = st.selectbox("Refresh mode", ["Quick", "Standard", "Deep"], index=1)
        mode_map = {"Quick": 500, "Standard": 1200, "Deep": 3000}
        sample_limit = st.number_input("Active market refresh symbols", min_value=100, max_value=6000, value=mode_map[refresh_mode], step=100)
        news_limit = st.slider("News API request budget", min_value=0, max_value=90, value=35, step=5)
        st.caption("With the current NewsAPI key, higher budgets may hit plan limits. Cached news is reused for the rest.")
        if st.button("API Update / Refresh Database", use_container_width=True):
            with st.spinner("Refreshing market prices, history, and news scores..."):
                refresh_market_database(str(db_path), int(sample_limit), int(news_limit))
                load_bundle.clear()
            st.success("Database refreshed successfully.")
            st.rerun()
        st.divider()
        st.caption(f"News API key detected: {'YES' if NEWS_API_KEY else 'NO'}")

    st.title(APP_TITLE)
    if not db_path.exists():
        st.warning(f"Database not found: {db_path}. Click refresh to create it.")
        return

    bundle = load_bundle(str(db_path), db_path.stat().st_mtime)
    app_view = normalize_app_view(bundle.get("App_Input_View"))
    history = bundle.get("Daily_History")
    news_articles = bundle.get("News_Articles")
    logs = bundle.get("Provider_Log")
    refresh_control = bundle.get("Refresh_Control")
    cfg = config_map(bundle.get("Config"))

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
    top4.metric("News-covered Stocks", f"{int((app_view['news_article_count_7d'].fillna(0) > 0).sum()) if not app_view.empty and 'news_article_count_7d' in app_view.columns else 0:,}")
    top5.metric("Active Market Refresh", active_market_refresh)

    if app_view.empty:
        st.warning("app_input_view is empty. Refresh database first.")
        return

    tabs = st.tabs(["Screener", "Selected Stock", "News Monitor", "Provider Health"])

    with tabs[0]:
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
        st.dataframe(
            filtered[
                [
                    "symbol", "company_name", "exchange_primary", "tags", "ltp", "change_1d_pct", "change_1m_pct", "change_3m_pct", "change_12m_pct",
                    "technical_score", "news_score", "news_article_count_7d", "headline_risk_flag", "overall_score", "recommendation", "actionable_flag", "status_note",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.caption("Recommendation uses technical trend, data quality, liquidity, and news score. High headline risk blocks BUY.")

        options = filtered["symbol"].tolist() if not filtered.empty else app_view["symbol"].tolist()
        if not options:
            st.info("No stocks available after applying filters.")
            return
        selected_symbol = st.selectbox("Selected stock", options, index=0)
        st.session_state["selected_symbol"] = selected_symbol

    selected_symbol = st.session_state.get("selected_symbol", app_view.iloc[0]["symbol"])
    row = app_view[app_view["symbol"] == selected_symbol].iloc[0]
    hist = history_for_symbol(history, selected_symbol)
    stock_news = news_for_symbol(news_articles, selected_symbol)

    with tabs[1]:
        st.subheader(f"{selected_symbol} · {row.get('company_name', '')}")
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
                        "SMA20", "SMA50", "SMA200", "EMA20", "RSI14", "ATR14", "Volume Ratio", "52W Range %",
                        "Liquidity Score", "Data Quality", "News Confidence", "Articles (7D)", "Bullish Articles", "Bearish Articles",
                    ],
                    "Value": [
                        row.get("sma20"), row.get("sma50"), row.get("sma200"), row.get("ema20"), row.get("rsi14"), row.get("atr14"), row.get("volume_ratio"), row.get("range_position_52w_pct"),
                        row.get("liquidity_score"), row.get("dqi_final"), row.get("news_confidence"), row.get("news_article_count_7d"), row.get("news_bullish_count"), row.get("news_bearish_count"),
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

    with tabs[2]:
        st.subheader("News Monitor")
        n1, n2 = st.columns(2)
        bullish = app_view.sort_values(["news_score", "news_article_count_7d"], ascending=[False, False]).head(15)
        bearish = app_view.sort_values(["news_score", "headline_risk_flag"], ascending=[True, True]).head(15)
        with n1:
            st.markdown("#### Stronger news flow")
            st.dataframe(bullish[["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_positive_headline"]], use_container_width=True, hide_index=True)
        with n2:
            st.markdown("#### Weaker / riskier news flow")
            st.dataframe(bearish[["symbol", "company_name", "news_score", "news_article_count_7d", "headline_risk_flag", "top_negative_headline"]], use_container_width=True, hide_index=True)

        st.caption("The current NewsAPI key gives delayed developer-plan data. Cached scores still help rank recent sentiment, but this is not true exchange-grade real-time news.")

    with tabs[3]:
        st.subheader("Provider Health")
        st.dataframe(provider_summary(logs), use_container_width=True, hide_index=True)
        if cfg:
            st.markdown("### Engine config")
            st.dataframe(pd.DataFrame([{"key": k, "value": v} for k, v in cfg.items()]), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
