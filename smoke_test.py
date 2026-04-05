from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from market_db import MarketDatabase
from refresh_engine import RefreshEngine


def main() -> None:
    db_path = Path("smoke_test_market.db")
    if db_path.exists():
        db_path.unlink()

    engine = RefreshEngine(db_path=str(db_path))
    universe = pd.DataFrame(
        [
            {
                "symbol": "SBIN",
                "company_name": "State Bank of India",
                "exchange_primary": "NSE",
                "nse_symbol": "SBIN",
                "bse_symbol": "",
                "bse_code": "500112",
                "tags": "NIFTY50, PSU, ALL_NSE",
                "priority": 1,
            }
        ]
    )
    quotes = pd.DataFrame(
        [
            {
                "symbol": "SBIN",
                "company_name": "State Bank of India",
                "sector": "Bank",
                "series": "EQ",
                "ltp": 800,
                "prev_close": 790,
                "open": 795,
                "day_high": 805,
                "day_low": 792,
                "volume": 1000000,
                "quote_ts": "2026-04-05 20:00:00",
                "quote_source": "NSE",
                "market_symbol": "SBIN",
                "api_latency_ms": 100,
            }
        ]
    )
    signals = pd.DataFrame(
        [
            {
                "symbol": "SBIN",
                "signal_ts": "2026-04-05 20:00:00",
                "ltp": 800,
                "prev_close": 790,
                "change_1d_pct": 1.26,
                "change_1w_pct": 2,
                "change_1m_pct": 4,
                "change_3m_pct": 8,
                "change_6m_pct": 12,
                "change_9m_pct": 18,
                "change_12m_pct": 22,
                "sma20": 780,
                "sma50": 760,
                "sma200": 700,
                "ema20": 782,
                "rsi14": 58,
                "atr14": 18,
                "high_52w": 840,
                "low_52w": 560,
                "range_position_52w_pct": 85,
                "volume_ratio": 1.4,
                "trend_quality": 78,
                "technical_score": 82,
                "status_note": "OK",
            }
        ]
    )
    news = pd.DataFrame(
        [
            {
                "symbol": "SBIN",
                "company_name": "State Bank of India",
                "news_score": 71,
                "news_confidence": 55,
                "news_article_count_7d": 4,
                "news_article_count_30d": 7,
                "news_bullish_count": 3,
                "news_bearish_count": 1,
                "headline_risk_flag": "LOW",
                "latest_news_ts": "2026-04-05T10:00:00Z",
                "top_positive_headline": "SBI sees strong loan growth",
                "top_negative_headline": "SBI faces margin pressure risk",
                "news_summary": "news flow tilted positive; 4 recent articles",
                "news_refresh_ts": "2026-04-05T15:00:00Z",
            }
        ]
    )

    app_view = engine._build_app_input_view(universe, quotes, signals, news)
    assert not app_view.empty
    assert "overall_score" in app_view.columns

    db = MarketDatabase(str(db_path))
    db.initialize()
    db.write_tables({"App_Input_View": app_view, "Refresh_Control": pd.DataFrame({"Control": ["ok"], "Value": ["yes"]})})
    roundtrip = db.read_table("App_Input_View")
    assert len(roundtrip) == 1
    print("smoke test passed")


if __name__ == "__main__":
    main()
