from __future__ import annotations

import io
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import yfinance as yf
    YF_AVAILABLE = True
except Exception:
    YF_AVAILABLE = False

try:
    from universe_lists import (
        BANKNIFTY_BACKUP,
        COMPANY_NAME_BACKUP,
        NIFTY50_BACKUP,
        NIFTY500_BACKUP,
        PSU_BACKUP,
    )
except ModuleNotFoundError:
    from universe_lists_v2 import (
        BANKNIFTY_BACKUP,
        COMPANY_NAME_BACKUP,
        NIFTY50_BACKUP,
        NIFTY500_BACKUP,
        PSU_BACKUP,
    )

from market_db import MarketDatabase
from news_engine import refresh_news_for_universe

APP_DIR = Path(__file__).resolve().parent
DB_DEFAULT = str(APP_DIR / "master_market_data.db")
NSE_BASE = "https://www.nseindia.com"
NSE_EQUITY_LIST_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
]
BSE_COMPANY_LIST_URLS = [
    "https://www.bseindia.com/downloads1/List_of_companies.csv",
]
UNIVERSE_SNAPSHOT_PATH = APP_DIR / "universe_master_snapshot.csv"
PROVIDER_STATE_PATH = APP_DIR / "provider_state.json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def build_retry_session(base_headers: Optional[dict] = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.7,
        status_forcelist=(401, 403, 408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if base_headers:
        session.headers.update(base_headers)
    return session
REQUEST_TIMEOUT = 20
HISTORY_TRADING_DAYS = 320
MAX_WORKERS = 12
DEFAULT_SAMPLE_LIMIT = 1200
DEFAULT_NEWS_LIMIT = 35

INDEX_NAMES = ["NIFTY 50", "NIFTY BANK", "NIFTY 500"]
RETURN_WINDOWS = {
    "change_1d_pct": 1,
    "change_1w_pct": 5,
    "change_1m_pct": 21,
    "change_3m_pct": 63,
    "change_6m_pct": 126,
    "change_9m_pct": 189,
    "change_12m_pct": 252,
}

SENSEX_STATIC = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "BHARTIARTL", "SBIN", "KOTAKBANK", "ITC",
    "LT", "ASIANPAINT", "HINDUNILVR", "BAJFINANCE", "SUNPHARMA", "MARUTI", "M&M", "AXISBANK",
    "ULTRACEMCO", "NTPC", "POWERGRID", "TITAN", "NESTLEIND", "BAJAJFINSV", "TATAMOTORS", "TATASTEEL",
    "HCLTECH", "TECHM", "ZOMATO", "ADANIPORTS", "INDUSINDBK",
]


def _safe_series(df: pd.DataFrame, col_name: str, default="") -> pd.Series:
    """Return a pandas Series even when a column is missing."""
    if col_name in df.columns:
        s = df[col_name]
        if not isinstance(s, pd.Series):
            s = pd.Series([default] * len(df), index=df.index)
    else:
        s = pd.Series([default] * len(df), index=df.index)
    return s.fillna(default)


@dataclass
class ProviderLogRow:
    provider: str
    dataset: str
    status: str
    rows_loaded: int
    avg_latency_ms: float
    latest_error: str
    refresh_ts: str


class NSEClient:
    def __init__(self) -> None:
        self.session = build_retry_session(
            {
                "user-agent": USER_AGENT,
                "accept": "application/json,text/plain,*/*",
                "accept-language": "en-US,en;q=0.9",
                "referer": NSE_BASE,
                "cache-control": "no-cache",
                "pragma": "no-cache",
            }
        )
        self._primed = False

    def reset(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.__init__()

    def _prime(self) -> None:
        if self._primed:
            return
        self.session.get(NSE_BASE, timeout=REQUEST_TIMEOUT)
        self.session.get(f"{NSE_BASE}/market-data/live-equity-market", timeout=REQUEST_TIMEOUT)
        self._primed = True

    def get_json(self, path: str) -> dict:
        self._prime()
        url = f"{NSE_BASE}{path}"
        r = self.session.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code in (401, 403):
            self.reset()
            self._prime()
            r = self.session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def get_index_constituents(self, index_name: str) -> pd.DataFrame:
        payload = self.get_json(f"/api/equity-stockIndices?index={quote(index_name)}")
        rows = payload.get("data", [])
        out = []
        for row in rows:
            symbol = str(row.get("symbol") or row.get("identifier") or "").strip().upper()
            if not symbol or symbol in {"NIFTY 50", "NIFTY BANK", "NIFTY 500"}:
                continue
            out.append(
                {
                    "symbol": symbol,
                    "company_name": (
                        row.get("meta", {}).get("companyName")
                        or row.get("companyName")
                        or COMPANY_NAME_BACKUP.get(symbol)
                        or symbol
                    ),
                    "index_name": index_name,
                }
            )
        return pd.DataFrame(out)

    def get_quote(self, symbol: str) -> dict:
        return self.get_json(f"/api/quote-equity?symbol={quote(symbol)}")

    def get_history(self, symbol: str, from_dt: datetime, to_dt: datetime) -> List[dict]:
        from_s = from_dt.strftime("%d-%m-%Y")
        to_s = to_dt.strftime("%d-%m-%Y")
        path = f"/api/historical/cm/equity?symbol={quote(symbol)}&series=%5B%22EQ%22%5D&from={from_s}&to={to_s}"
        payload = self.get_json(path)
        return payload.get("data", [])


class RefreshEngine:
    def __init__(self, db_path: str = DB_DEFAULT, max_workers: int = MAX_WORKERS) -> None:
        raw_db_path = Path(db_path)
        self.db_path = raw_db_path if raw_db_path.is_absolute() else APP_DIR / raw_db_path
        self.db = MarketDatabase(str(self.db_path))
        self.max_workers = max_workers
        self.client = NSEClient()
        self.http = build_retry_session({"User-Agent": USER_AGENT, "Accept": "text/csv,application/json,text/plain,*/*"})
        self.provider_logs: List[ProviderLogRow] = []
        self.run_ts = pd.Timestamp.now(tz="Asia/Kolkata")
        self._cached_quotes = pd.DataFrame()
        self._cached_history = pd.DataFrame()
        self._cached_signals = pd.DataFrame()
        self._cached_news_articles = pd.DataFrame()
        self._cached_news_scores = pd.DataFrame()
        self._cached_universe = pd.DataFrame()
        self._provider_state = self._load_provider_state()

    def log(
        self,
        dataset: str,
        status: str,
        rows_loaded: int,
        latency_ms: float,
        latest_error: str = "",
        provider: str = "ENGINE",
    ) -> None:
        self.provider_logs.append(
            ProviderLogRow(
                provider=provider,
                dataset=dataset,
                status=status,
                rows_loaded=int(rows_loaded),
                avg_latency_ms=float(latency_ms),
                latest_error=str(latest_error or ""),
                refresh_ts=self.run_ts.isoformat(),
            )
        )

    def _load_provider_state(self) -> Dict[str, str]:
        if not PROVIDER_STATE_PATH.exists():
            return {}
        try:
            payload = json.loads(PROVIDER_STATE_PATH.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _save_provider_state(self) -> None:
        try:
            PROVIDER_STATE_PATH.write_text(json.dumps(self._provider_state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _remember_provider_endpoint(self, key: str, url: str) -> None:
        if not key or not url:
            return
        self._provider_state[str(key)] = str(url)
        self._save_provider_state()

    def _ordered_candidate_urls(self, state_key: str, urls: Sequence[str]) -> List[str]:
        preferred = str(self._provider_state.get(state_key) or "").strip()
        ordered: List[str] = []
        if preferred and preferred in urls:
            ordered.append(preferred)
        ordered.extend([str(u) for u in urls if str(u) not in ordered])
        return ordered

    def _fetch_csv_from_candidates(
        self,
        *,
        urls: Sequence[str],
        state_key: str,
        session: Optional[requests.Session] = None,
        parse_kwargs: Optional[dict] = None,
        prime_nse: bool = False,
    ) -> Tuple[pd.DataFrame, str, List[str]]:
        errors: List[str] = []
        active_session = session or self.http
        kwargs = parse_kwargs or {}

        if prime_nse:
            try:
                self.client._prime()
                active_session = self.client.session
            except Exception as exc:
                errors.append(f"prime:{exc}")

        for url in self._ordered_candidate_urls(state_key, urls):
            try:
                response = active_session.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code in (401, 403) and prime_nse:
                    self.client.reset()
                    self.client._prime()
                    active_session = self.client.session
                    response = active_session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                df = pd.read_csv(io.StringIO(response.text), **kwargs)
                if df is None or df.empty:
                    raise ValueError("empty csv response")
                self._remember_provider_endpoint(state_key, url)
                return df, url, errors
            except Exception as exc:
                errors.append(f"{url}: {exc}")

        return pd.DataFrame(), "", errors

    def _load_universe_snapshot(self) -> pd.DataFrame:
        if not UNIVERSE_SNAPSHOT_PATH.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(UNIVERSE_SNAPSHOT_PATH)
            if "symbol" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
            return df
        except Exception:
            return pd.DataFrame()

    def _save_universe_snapshot(self, universe: pd.DataFrame) -> None:
        if universe is None or universe.empty:
            return
        try:
            universe.sort_values(["priority", "symbol"], ascending=[True, True]).to_csv(UNIVERSE_SNAPSHOT_PATH, index=False)
        except Exception:
            pass

    def _fallback_universe_from_cache(self) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for df in [self._cached_universe, self._load_universe_snapshot()]:
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df.copy())

        if not self._cached_quotes.empty:
            frames.append(
                self._cached_quotes[[c for c in ["symbol", "company_name"] if c in self._cached_quotes.columns]].assign(
                    exchange_primary="NSE",
                    nse_symbol=self._cached_quotes.get("symbol"),
                    bse_symbol="",
                    bse_code="",
                    series="EQ",
                    tags="CACHE_DERIVED",
                )
            )

        if not self._cached_signals.empty:
            frames.append(
                self._cached_signals[[c for c in ["symbol"] if c in self._cached_signals.columns]].assign(
                    company_name=lambda x: x["symbol"],
                    exchange_primary="NSE",
                    nse_symbol=lambda x: x["symbol"],
                    bse_symbol="",
                    bse_code="",
                    series="EQ",
                    tags="CACHE_DERIVED",
                )
            )

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True, sort=False)
        if "symbol" not in combined.columns:
            return pd.DataFrame()
        combined["symbol"] = combined["symbol"].astype(str).str.upper().str.strip()
        combined = combined[combined["symbol"].str.len() > 0]
        if "company_name" not in combined.columns:
            combined["company_name"] = combined["symbol"]
        combined["company_name"] = combined.apply(lambda r: COMPANY_NAME_BACKUP.get(r["symbol"], str(r.get("company_name") or r["symbol"])), axis=1)
        for col, default in {"exchange_primary": "NSE", "nse_symbol": "", "bse_symbol": "", "bse_code": "", "series": "EQ", "isin": "", "tags": "CACHE_DERIVED"}.items():
            if col not in combined.columns:
                combined[col] = default
            combined[col] = combined[col].fillna(default)
        combined.loc[combined["nse_symbol"].astype(str).str.len() == 0, "nse_symbol"] = combined["symbol"]
        return combined.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)

    def refresh(
        self,
        sample_limit: Optional[int] = DEFAULT_SAMPLE_LIMIT,
        news_limit: int = DEFAULT_NEWS_LIMIT,
        refresh_universe: str = "ALL",
        priority_symbols: Optional[Sequence[str]] = None,
        refresh_symbols: Optional[Sequence[str]] = None,
    ) -> None:
        started = time.perf_counter()
        self.db.initialize()
        self._write_refresh_only("RUNNING")
        self._load_existing_cache()

        try:
            universe = self._build_universe()
            if universe.empty:
                self._write_refresh_only("FAILED - universe empty")
                raise RuntimeError("Universe build returned zero symbols")

            universe = self._apply_priority_symbols(universe, priority_symbols)
            total_universe = len(universe)

            active_universe = self._filter_refresh_universe(universe, refresh_universe)
            if active_universe.empty:
                active_universe = universe.copy()
            active_universe = self._filter_to_symbols(active_universe, refresh_symbols)
            if active_universe.empty and refresh_symbols:
                active_universe = self._filter_to_symbols(universe, refresh_symbols)
            active_universe = self._apply_priority_symbols(active_universe, priority_symbols)
            if active_universe.empty:
                active_universe = universe.copy()
            if sample_limit is not None and int(sample_limit) > 0 and int(sample_limit) < len(active_universe):
                active_universe = active_universe.head(int(sample_limit)).copy()

            quotes_df, history_df, signals_df = self._fetch_market_data(active_universe)
            quotes_df = self._merge_with_cached_quotes(universe, quotes_df)
            history_df = self._merge_with_cached_history(universe, history_df)
            signals_df = self._merge_with_cached_signals(universe, signals_df)

            news_universe = self._filter_to_symbols(universe, refresh_symbols)
            if news_universe.empty:
                news_universe = universe.copy()
            news_universe = self._apply_priority_symbols(news_universe, priority_symbols)

            news_result = refresh_news_for_universe(
                universe=news_universe,
                cached_scores=self._cached_news_scores,
                cached_articles=self._cached_news_articles,
                max_requests=max(0, int(news_limit or 0)),
            )
            for row in news_result.provider_rows:
                self.provider_logs.append(ProviderLogRow(**row))

            app_view = self._build_app_input_view(universe, quotes_df, signals_df, news_result.scores)
            provider_log_df = pd.DataFrame([x.__dict__ for x in self.provider_logs])
            config_df = self._default_config(
                total_universe=total_universe,
                active_symbols=len(active_universe),
                news_limit=int(news_limit or 0),
                refresh_universe=refresh_universe,
            )

            refresh_control = pd.DataFrame(
                {
                    "Control": [
                        "last_refresh_ts",
                        "refresh_status",
                        "universe_count",
                        "active_market_refresh_count",
                        "quote_count",
                        "signal_count",
                        "history_rows",
                        "news_score_count",
                        "news_article_rows",
                        "engine_seconds",
                        "refresh_universe",
                        "priority_symbols_count",
                        "specific_refresh_symbols_count",
                    ],
                    "Value": [
                        self.run_ts.strftime("%Y-%m-%d %H:%M:%S %Z"),
                        "SUCCESS" if not app_view.empty else "FAILED - no rows generated",
                        len(universe),
                        len(active_universe),
                        len(quotes_df),
                        len(signals_df),
                        len(history_df),
                        len(news_result.scores),
                        len(news_result.articles),
                        round(time.perf_counter() - started, 2),
                        refresh_universe,
                        len({str(x).upper().strip() for x in (priority_symbols or []) if str(x).strip()}),
                        len({str(x).upper().strip() for x in (refresh_symbols or []) if str(x).strip()}),
                    ],
                }
            )

            self.db.write_tables(
                {
                    "Universe_Master": universe,
                    "Latest_Quotes": quotes_df,
                    "Daily_History": history_df,
                    "Latest_Signals": signals_df,
                    "News_Articles": news_result.articles,
                    "News_Scores": news_result.scores,
                    "App_Input_View": app_view,
                    "Provider_Log": provider_log_df,
                    "Refresh_Control": refresh_control,
                    "Config": config_df,
                }
            )
        except Exception as exc:
            self._write_refresh_only(f"FAILED - {exc}")
            raise

    def _default_config(self, total_universe: int, active_symbols: int, news_limit: int, refresh_universe: str = "ALL") -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "key": "source",
                    "value": "NSE + BSE master -> NSE/Yahoo Finance -> NewsAPI -> SQLite",
                    "notes": "Prices and history prefer NSE, then Yahoo Finance (.NS/.BO), then SQLite cache.",
                },
                {
                    "key": "history_trading_days",
                    "value": HISTORY_TRADING_DAYS,
                    "notes": "Used for returns and indicators.",
                },
                {
                    "key": "max_workers",
                    "value": self.max_workers,
                    "notes": "Parallel symbol processing.",
                },
                {
                    "key": "return_windows",
                    "value": json.dumps(RETURN_WINDOWS),
                    "notes": "Trading day approximations used in technical scoring.",
                },
                {
                    "key": "universe_mode",
                    "value": f"all NSE securities + BSE reference master, total={total_universe}, active_refresh={active_symbols}, basket={refresh_universe}",
                    "notes": "Universe includes all NSE equities when the exchange master is available. BSE-only rows use Yahoo .BO fallback when possible.",
                },
                {
                    "key": "news_refresh_limit",
                    "value": news_limit,
                    "notes": "Max NewsAPI external requests attempted per refresh. Cached news scores persist for the rest.",
                },
                {
                    "key": "newsapi_plan_warning",
                    "value": "Developer plan has delayed data and is not for production",
                    "notes": "Upgrade NewsAPI plan before using this on a public production deployment.",
                },
                {
                    "key": "yfinance_available",
                    "value": str(YF_AVAILABLE),
                    "notes": "Whether Yahoo fallback is available in environment.",
                },
            ]
        )

    def _load_existing_cache(self) -> None:
        self._provider_state = self._load_provider_state()
        if not self.db_path.exists():
            self._cached_quotes = pd.DataFrame()
            self._cached_history = pd.DataFrame()
            self._cached_signals = pd.DataFrame()
            self._cached_news_articles = pd.DataFrame()
            self._cached_news_scores = pd.DataFrame()
            self._cached_universe = self._load_universe_snapshot()
            return

        try:
            self._cached_quotes = self.db.read_table("Latest_Quotes")
            self._cached_history = self.db.read_table("Daily_History")
            self._cached_signals = self.db.read_table("Latest_Signals")
            self._cached_news_articles = self.db.read_table("News_Articles")
            self._cached_news_scores = self.db.read_table("News_Scores")
            self._cached_universe = self.db.read_table("Universe_Master")
        except Exception:
            self._cached_quotes = pd.DataFrame()
            self._cached_history = pd.DataFrame()
            self._cached_signals = pd.DataFrame()
            self._cached_news_articles = pd.DataFrame()
            self._cached_news_scores = pd.DataFrame()
            self._cached_universe = self._load_universe_snapshot()

        for df_name in ["_cached_quotes", "_cached_history", "_cached_signals", "_cached_news_articles", "_cached_news_scores", "_cached_universe"]:
            df = getattr(self, df_name)
            if not df.empty and "symbol" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
            if not df.empty and "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if not df.empty and "published_at" in df.columns:
                df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
            setattr(self, df_name, df)

    def _build_universe(self) -> pd.DataFrame:
        started = time.perf_counter()
        errors: List[str] = []
        nse_master = self._fetch_nse_master()
        bse_master = self._fetch_bse_master()
        cached_universe = self._fallback_universe_from_cache()

        live_index_frames: Dict[str, pd.DataFrame] = {}
        live_to_category = {"NIFTY 50": "NIFTY50", "NIFTY BANK": "NIFTYBANK", "NIFTY 500": "NIFTY500"}
        for idx in INDEX_NAMES:
            try:
                live_df = self.client.get_index_constituents(idx)
                if not live_df.empty:
                    live_df["category"] = live_to_category[idx]
                    live_index_frames[idx] = live_df
                else:
                    raise ValueError("empty constituent response")
            except Exception as exc:
                errors.append(f"{idx}: {exc}")
                live_index_frames[idx] = self._backup_index_frame(idx)

        records: Dict[str, dict] = {}

        def upsert_record(row: dict) -> None:
            key = str(row.get("isin") or "").strip().upper() or f"{row.get('exchange_primary')}::{row.get('symbol')}"
            current = records.get(key, {})
            incoming = {k: v for k, v in row.items() if pd.notna(v) and v != ""}
            merged = {**current, **incoming}
            # Prefer NSE symbol as the primary app symbol when both exchanges map to the same ISIN.
            if current.get("nse_symbol"):
                merged["symbol"] = current.get("symbol") or current.get("nse_symbol")
                merged["exchange_primary"] = "NSE"
                merged["nse_symbol"] = current.get("nse_symbol")
            elif incoming.get("nse_symbol"):
                merged["symbol"] = incoming.get("symbol") or incoming.get("nse_symbol")
                merged["exchange_primary"] = "NSE"
            tags = set(filter(None, [x.strip() for x in str(current.get("tags") or "").split(",") if x.strip()]))
            tags.update([x.strip() for x in str(row.get("tags") or "").split(",") if x.strip()])
            merged["tags"] = ", ".join(sorted(tags))
            records[key] = merged

        if not cached_universe.empty:
            for _, r in cached_universe.iterrows():
                symbol = str(r.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                upsert_record(
                    {
                        "symbol": symbol,
                        "company_name": str(r.get("company_name") or COMPANY_NAME_BACKUP.get(symbol, symbol)),
                        "isin": str(r.get("isin") or "").upper().strip(),
                        "series": str(r.get("series") or "EQ").upper().strip(),
                        "exchange_primary": str(r.get("exchange_primary") or "NSE").upper().strip() or "NSE",
                        "nse_symbol": str(r.get("nse_symbol") or symbol if str(r.get("exchange_primary") or "NSE").upper().strip() == "NSE" else r.get("nse_symbol") or "").upper().strip(),
                        "bse_symbol": str(r.get("bse_symbol") or "").upper().strip(),
                        "bse_code": str(r.get("bse_code") or "").strip(),
                        "tags": str(r.get("tags") or "CACHE_DERIVED"),
                    }
                )

        if not nse_master.empty:
            for _, r in nse_master.iterrows():
                symbol = str(r.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                upsert_record(
                    {
                        "symbol": symbol,
                        "company_name": str(r.get("company_name") or COMPANY_NAME_BACKUP.get(symbol, symbol)),
                        "isin": str(r.get("isin") or "").upper().strip(),
                        "series": str(r.get("series") or "EQ").upper().strip(),
                        "exchange_primary": "NSE",
                        "nse_symbol": symbol,
                        "bse_symbol": "",
                        "bse_code": "",
                        "tags": "ALL_NSE",
                    }
                )
        else:
            if not cached_universe.empty:
                errors.append(f"NSE master unavailable; restored {len(cached_universe):,} cached/snapshot symbols")
            else:
                errors.append("NSE master unavailable; using backup universe only")

        if not bse_master.empty:
            for _, r in bse_master.iterrows():
                bse_symbol = str(r.get("bse_symbol") or "").upper().strip()
                bse_code = str(r.get("bse_code") or "").strip()
                company_name = str(r.get("company_name") or bse_symbol or bse_code)
                isin = str(r.get("isin") or "").upper().strip()
                symbol = bse_symbol or bse_code
                if not symbol:
                    continue
                upsert_record(
                    {
                        "symbol": symbol,
                        "company_name": company_name,
                        "isin": isin,
                        "series": str(r.get("series") or "EQ").upper().strip(),
                        "exchange_primary": "BSE",
                        "nse_symbol": "",
                        "bse_symbol": bse_symbol,
                        "bse_code": bse_code,
                        "tags": "ALL_BSE",
                    }
                )
        else:
            errors.append("BSE master unavailable; BSE-only coverage reduced")

        # Seed from backups so high priority baskets always exist.
        seed_symbols = set(NIFTY50_BACKUP) | set(BANKNIFTY_BACKUP) | set(NIFTY500_BACKUP) | set(PSU_BACKUP) | set(SENSEX_STATIC)
        for raw_symbol in seed_symbols:
            symbol = str(raw_symbol).upper().strip()
            upsert_record(
                {
                    "symbol": symbol,
                    "company_name": COMPANY_NAME_BACKUP.get(symbol, symbol),
                    "isin": "",
                    "series": "EQ",
                    "exchange_primary": "NSE",
                    "nse_symbol": symbol,
                    "bse_symbol": "",
                    "bse_code": "",
                    "tags": "",
                }
            )

        universe = pd.DataFrame(records.values())
        if universe.empty:
            self.log("Universe_Master", "FAILED", 0, (time.perf_counter() - started) * 1000, "; ".join(errors), provider="UNIVERSE")
            return universe

        universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
        universe["nse_symbol"] = _safe_series(universe, "nse_symbol", "").astype(str).str.upper().str.strip()
        universe["bse_symbol"] = _safe_series(universe, "bse_symbol", "").astype(str).str.upper().str.strip()
        universe["bse_code"] = _safe_series(universe, "bse_code", "").astype(str).str.strip()
        universe["company_name"] = universe["company_name"].fillna(universe["symbol"]).astype(str)
        universe["company_name"] = universe.apply(lambda r: COMPANY_NAME_BACKUP.get(r["symbol"], r["company_name"]), axis=1)

        # Merge BSE info into NSE primary symbols when ISIN matches.
        if "isin" in universe.columns:
            isin_to_nse = {
                str(row["isin"]).upper().strip(): row["symbol"]
                for _, row in universe.iterrows()
                if str(row.get("isin") or "").strip() and str(row.get("nse_symbol") or "").strip()
            }
            for idx, row in universe.iterrows():
                isin = str(row.get("isin") or "").upper().strip()
                if isin and isin in isin_to_nse and row["symbol"] != isin_to_nse[isin] and not str(row.get("nse_symbol") or "").strip():
                    # keep BSE-only rows if no paired NSE symbol exists; otherwise mark low priority reference row later.
                    universe.loc[idx, "paired_nse_symbol"] = isin_to_nse[isin]
                else:
                    universe.loc[idx, "paired_nse_symbol"] = ""
        else:
            universe["paired_nse_symbol"] = ""

        # High priority tags.
        tag_map: Dict[str, set] = {sym: set(filter(None, [x.strip() for x in str(tags).split(",") if x.strip()])) for sym, tags in zip(universe["symbol"], universe["tags"])}
        for idx_name, idx_df in live_index_frames.items():
            if idx_df.empty:
                continue
            category = idx_df.get("category", pd.Series(dtype=str)).iloc[0] if "category" in idx_df.columns and not idx_df.empty else idx_name.replace(" ", "")
            for sym in idx_df["symbol"].astype(str).str.upper().str.strip().tolist():
                tag_map.setdefault(sym, set()).add(category)
        for sym in PSU_BACKUP:
            tag_map.setdefault(str(sym).upper().strip(), set()).add("PSU")
        for sym in SENSEX_STATIC:
            tag_map.setdefault(str(sym).upper().strip(), set()).add("SENSEX")

        universe["tags"] = universe["symbol"].map(lambda s: ", ".join(sorted(tag_map.get(s, set()))))
        universe["include_flag"] = "YES"
        universe["yahoo_ticker_ns"] = np.where(universe["nse_symbol"].astype(str).str.len() > 0, universe["nse_symbol"] + ".NS", "")
        universe["yahoo_ticker_bo"] = np.where(universe["bse_code"].astype(str).str.len() > 0, universe["bse_code"] + ".BO", "")
        universe["priority"] = np.select(
            [
                universe["tags"].str.contains("NIFTY50", na=False),
                universe["tags"].str.contains("NIFTYBANK", na=False),
                universe["tags"].str.contains("SENSEX", na=False),
                universe["tags"].str.contains("PSU", na=False),
                universe["tags"].str.contains("NIFTY500", na=False),
                universe["tags"].str.contains("ALL_NSE", na=False),
                universe["tags"].str.contains("ALL_BSE", na=False),
            ],
            [1, 2, 3, 4, 5, 7, 8],
            default=9,
        )

        universe = universe.sort_values(["priority", "symbol"]).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
        self._save_universe_snapshot(universe)
        self.log("Universe_Master", "SUCCESS", len(universe), (time.perf_counter() - started) * 1000, "; ".join(errors), provider="UNIVERSE")
        return universe

    def _filter_refresh_universe(self, universe: pd.DataFrame, refresh_universe: str) -> pd.DataFrame:
        if universe.empty:
            return universe
        choice = str(refresh_universe or "ALL").strip().upper()
        tags = _safe_series(universe, "tags", "").astype(str)

        if choice == "NIFTY 50":
            filtered = universe[tags.str.contains("NIFTY50", na=False)].copy()
        elif choice == "BANK NIFTY":
            filtered = universe[tags.str.contains("NIFTYBANK", na=False)].copy()
        elif choice == "NIFTY 500":
            filtered = universe[tags.str.contains("NIFTY500", na=False)].copy()
        elif choice == "REMAINING OTHER":
            filtered = universe[~(
                tags.str.contains("NIFTY50", na=False)
                | tags.str.contains("NIFTYBANK", na=False)
                | tags.str.contains("NIFTY500", na=False)
            )].copy()
        else:
            filtered = universe.copy()

        return filtered.sort_values(["priority", "symbol"]).reset_index(drop=True)

    def _fetch_nse_master(self) -> pd.DataFrame:
        started = time.perf_counter()
        try:
            df, selected_url, errors = self._fetch_csv_from_candidates(
                urls=NSE_EQUITY_LIST_URLS,
                state_key="nse_master_url",
                session=self.client.session,
                parse_kwargs={"engine": "python"},
                prime_nse=True,
            )
            if df.empty:
                raise ValueError("; ".join(errors) if errors else "empty response")
            df.columns = [str(c).strip() for c in df.columns]
            colmap = {c.lower().strip(): c for c in df.columns}
            symbol_col = colmap.get("symbol")
            name_col = colmap.get("name of company") or colmap.get("company name")
            series_col = colmap.get("series")
            isin_col = colmap.get("isin number") or colmap.get("isin")
            if not symbol_col:
                raise ValueError("symbol column missing in NSE master")
            out = pd.DataFrame(
                {
                    "symbol": df[symbol_col].astype(str).str.upper().str.strip(),
                    "company_name": df[name_col].astype(str).str.strip() if name_col else df[symbol_col].astype(str),
                    "series": df[series_col].astype(str).str.upper().str.strip() if series_col else "EQ",
                    "isin": df[isin_col].astype(str).str.upper().str.strip() if isin_col else "",
                }
            )
            out = out[out["symbol"].str.len() > 0]
            out = out[out["series"].isin(["EQ", "BE", "BZ", "SM", "ST", "T", "M", "A", "B"]) | (out["series"] == "EQ")]
            out = out.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
            self.log("NSE_Master", "SUCCESS", len(out), (time.perf_counter() - started) * 1000, f"url={selected_url}", provider="NSE")
            return out
        except Exception as exc:
            self.log("NSE_Master", "FAILED", 0, (time.perf_counter() - started) * 1000, str(exc), provider="NSE")
            return pd.DataFrame(columns=["symbol", "company_name", "series", "isin"])

    def _fetch_bse_master(self) -> pd.DataFrame:
        started = time.perf_counter()
        try:
            df, selected_url, errors = self._fetch_csv_from_candidates(
                urls=BSE_COMPANY_LIST_URLS,
                state_key="bse_master_url",
                session=self.http,
                parse_kwargs={"engine": "python", "on_bad_lines": "skip"},
            )
            if df.empty:
                raise ValueError("; ".join(errors) if errors else "empty response")
            df.columns = [str(c).strip() for c in df.columns]
            colmap = {c.lower().strip(): c for c in df.columns}
            code_col = (
                colmap.get("security code")
                or colmap.get("scrip code")
                or colmap.get("securitycode")
                or colmap.get("scripcode")
                or colmap.get("code")
            )
            symbol_col = (
                colmap.get("security id")
                or colmap.get("securityid")
                or colmap.get("symbol")
                or colmap.get("ticker")
            )
            name_col = (
                colmap.get("issuer name")
                or colmap.get("security name")
                or colmap.get("company name")
                or colmap.get("name of company")
                or symbol_col
            )
            isin_col = colmap.get("isin no") or colmap.get("isin") or colmap.get("isin number")
            if not code_col and not symbol_col:
                raise ValueError("code/symbol columns missing in BSE master")
            out = pd.DataFrame(
                {
                    "bse_code": df[code_col].astype(str).str.extract(r"(\d+)", expand=False).fillna("") if code_col else "",
                    "bse_symbol": df[symbol_col].astype(str).str.upper().str.strip() if symbol_col else "",
                    "company_name": df[name_col].astype(str).str.strip() if name_col else "",
                    "series": "EQ",
                    "isin": df[isin_col].astype(str).str.upper().str.strip() if isin_col else "",
                }
            )
            out["company_name"] = out["company_name"].replace({"nan": "", "None": ""})
            out = out[(out["bse_code"].astype(str).str.len() > 0) | (out["bse_symbol"].astype(str).str.len() > 0)]
            out = out.drop_duplicates(subset=["bse_code", "bse_symbol"], keep="first").reset_index(drop=True)
            self.log("BSE_Master", "SUCCESS", len(out), (time.perf_counter() - started) * 1000, f"url={selected_url}", provider="BSE")
            return out
        except Exception as exc:
            self.log("BSE_Master", "FAILED", 0, (time.perf_counter() - started) * 1000, str(exc), provider="BSE")
            return pd.DataFrame(columns=["bse_code", "bse_symbol", "company_name", "series", "isin"])

    def _backup_index_frame(self, index_name: str) -> pd.DataFrame:
        if index_name == "NIFTY 50":
            syms = NIFTY50_BACKUP
            category = "NIFTY50"
        elif index_name == "NIFTY BANK":
            syms = BANKNIFTY_BACKUP
            category = "NIFTYBANK"
        elif index_name == "NIFTY 500":
            syms = NIFTY500_BACKUP
            category = "NIFTY500"
        else:
            syms = []
            category = index_name.replace(" ", "")
        syms = sorted(set([str(x).upper().strip() for x in syms if str(x).strip()]))
        return pd.DataFrame(
            {
                "symbol": syms,
                "company_name": [COMPANY_NAME_BACKUP.get(s, s) for s in syms],
                "index_name": f"{index_name}_BACKUP",
                "category": category,
            }
        )

    def _fetch_market_data(self, universe: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if universe is None or universe.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        quote_rows: List[dict] = []
        hist_rows: List[dict] = []
        signal_rows: List[dict] = []
        quote_errors: List[str] = []
        history_errors: List[str] = []
        from_dt = datetime.now() - timedelta(days=520)
        to_dt = datetime.now()

        def worker(row: dict) -> Tuple[dict, pd.DataFrame, dict]:
            quote_row, quote_payload, quote_errs, quote_latency = self._get_best_quote(row)
            hist_df, hist_source, hist_errs, hist_latency = self._get_best_history(row, from_dt, to_dt)
            signal = self._compute_signals(row["symbol"], hist_df, quote_payload)
            signal["quote_latency_ms"] = round(float(quote_latency), 2)
            signal["history_latency_ms"] = round(float(hist_latency), 2)
            status_parts = []
            if quote_row.get("quote_source"):
                status_parts.append(f"Quote={quote_row.get('quote_source')}")
            if hist_source:
                status_parts.append(f"History={hist_source}")
            if quote_errs:
                status_parts.append(f"QuoteErr={quote_errs[-1]}")
            if hist_errs:
                status_parts.append(f"HistErr={hist_errs[-1]}")
            base_status = str(signal.get("status_note") or "")
            signal["status_note"] = f"{base_status} | {' | '.join(status_parts)}" if base_status != "OK" and status_parts else ("OK | " + " | ".join(status_parts) if status_parts else base_status)
            return quote_row, hist_df, signal

        rows = universe.to_dict("records")
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(worker, row): row["symbol"] for row in rows}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    quote_row, hist_df, signal = future.result()
                    if quote_row:
                        quote_rows.append(quote_row)
                    if hist_df is not None and not hist_df.empty:
                        hist_rows.extend(hist_df.to_dict("records"))
                    signal_rows.append(signal)
                except Exception as exc:
                    quote_errors.append(f"{symbol}: {exc}")
                    history_errors.append(f"{symbol}: {exc}")
                    signal_rows.append({"symbol": symbol, "technical_score": np.nan, "status_note": f"Fetch failed: {exc}"})

        quotes_cols = [
            "symbol", "company_name", "sector", "series", "ltp", "prev_close", "open",
            "day_high", "day_low", "volume", "quote_ts", "quote_source", "market_symbol", "api_latency_ms"
        ]
        history_cols = ["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"]
        signals_cols = [
            "symbol", "signal_ts", "ltp", "prev_close", "change_1d_pct", "change_1w_pct", "change_1m_pct", "change_3m_pct",
            "change_6m_pct", "change_9m_pct", "change_12m_pct", "sma20", "sma50", "sma200", "ema20", "rsi14", "atr14",
            "high_52w", "low_52w", "range_position_52w_pct", "volume_ratio", "trend_quality", "technical_score", "status_note",
            "quote_latency_ms", "history_latency_ms", "history_days", "history_recency_days", "technical_regime"
        ]

        quotes_df = pd.DataFrame(quote_rows, columns=quotes_cols)
        history_df = pd.DataFrame(hist_rows, columns=history_cols)
        signals_df = pd.DataFrame(signal_rows, columns=signals_cols)

        if not quotes_df.empty:
            quotes_df["symbol"] = quotes_df["symbol"].astype(str).str.upper().str.strip()
            quotes_df = quotes_df.sort_values("symbol").drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
        if not history_df.empty:
            history_df["symbol"] = history_df["symbol"].astype(str).str.upper().str.strip()
            history_df["date"] = pd.to_datetime(history_df["date"], errors="coerce")
            history_df = history_df.dropna(subset=["date", "close"]).sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)
        if not signals_df.empty:
            signals_df["symbol"] = signals_df["symbol"].astype(str).str.upper().str.strip()
            signals_df = signals_df.sort_values("symbol").drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)

        self.log("Latest_Quotes", "SUCCESS" if not quotes_df.empty else "FAILED", len(quotes_df), float(quotes_df.get("api_latency_ms", pd.Series([0])).mean() if not quotes_df.empty else 0), " | ".join(quote_errors[:10]), provider="MARKET")
        self.log("Daily_History", "SUCCESS" if not history_df.empty else "FAILED", len(history_df), float(signals_df.get("history_latency_ms", pd.Series([0])).mean() if not signals_df.empty else 0), " | ".join(history_errors[:10]), provider="MARKET")
        self.log("Latest_Signals", "SUCCESS" if not signals_df.empty else "FAILED", len(signals_df), 0.0, "", provider="ENGINE")
        return quotes_df, history_df, signals_df

    def _merge_with_cached_quotes(self, universe: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
        if universe.empty:
            return fresh
        current = fresh.copy() if fresh is not None else pd.DataFrame()
        cached = self._cached_quotes.copy()
        if not cached.empty:
            missing = set(universe["symbol"]) - set(current["symbol"]) if not current.empty else set(universe["symbol"])
            cached = cached[cached["symbol"].isin(missing)]
            current = pd.concat([current, cached], ignore_index=True)
        return current.sort_values("symbol").drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True) if not current.empty else current

    def _merge_with_cached_history(self, universe: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
        current = fresh.copy() if fresh is not None else pd.DataFrame()
        cached = self._cached_history.copy()
        frames = [df for df in [current, cached] if df is not None and not df.empty]
        if not frames:
            return pd.DataFrame()
        current = pd.concat(frames, ignore_index=True)
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        current = current.dropna(subset=["date", "close"]).sort_values(["symbol", "date"])
        current = current.drop_duplicates(subset=["symbol", "date"], keep="last")
        current = current.groupby("symbol", group_keys=False).tail(HISTORY_TRADING_DAYS)
        return current.reset_index(drop=True)

    def _merge_with_cached_signals(self, universe: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
        current = fresh.copy() if fresh is not None else pd.DataFrame()
        cached = self._cached_signals.copy()
        if not cached.empty:
            missing = set(universe["symbol"]) - set(current["symbol"]) if not current.empty else set(universe["symbol"])
            cached = cached[cached["symbol"].isin(missing)]
            current = pd.concat([current, cached], ignore_index=True)
        return current.sort_values("symbol").drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True) if not current.empty else current

    def _get_best_quote(self, row: dict) -> Tuple[dict, dict, List[str], float]:
        symbol = str(row.get("symbol") or "").upper().strip()
        company_name = str(row.get("company_name") or symbol)
        errors: List[str] = []
        started = time.perf_counter()

        nse_symbol = str(row.get("nse_symbol") or "").upper().strip()
        if nse_symbol:
            try:
                payload = self.client.get_quote(nse_symbol)
                quote_row = self._quote_to_row_nse(symbol=symbol, market_symbol=nse_symbol, company_name=company_name, payload=payload, latency_ms=(time.perf_counter() - started) * 1000)
                return quote_row, payload, errors, (time.perf_counter() - started) * 1000
            except Exception as exc:
                errors.append(f"NSE:{exc}")

        for ticker, source in [(row.get("yahoo_ticker_ns"), "YAHOO_NS"), (row.get("yahoo_ticker_bo"), "YAHOO_BO")]:
            ticker = str(ticker or "").strip()
            if not ticker:
                continue
            try:
                quote_row, payload = self._quote_from_yfinance(symbol=symbol, market_symbol=ticker, source_label=source, company_name=company_name)
                return quote_row, payload, errors, (time.perf_counter() - started) * 1000
            except Exception as exc:
                errors.append(f"{source}:{exc}")

        cached_row = self._quote_from_cache(symbol)
        if cached_row:
            return cached_row, self._quote_payload_from_row(cached_row), errors, (time.perf_counter() - started) * 1000

        return {
            "symbol": symbol,
            "company_name": company_name,
            "sector": "",
            "series": "EQ",
            "ltp": np.nan,
            "prev_close": np.nan,
            "open": np.nan,
            "day_high": np.nan,
            "day_low": np.nan,
            "volume": np.nan,
            "quote_ts": self.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quote_source": "UNAVAILABLE",
            "market_symbol": nse_symbol or row.get("yahoo_ticker_ns") or row.get("yahoo_ticker_bo") or symbol,
            "api_latency_ms": np.nan,
        }, {}, errors, (time.perf_counter() - started) * 1000

    def _get_best_history(self, row: dict, from_dt: datetime, to_dt: datetime) -> Tuple[pd.DataFrame, str, List[str], float]:
        symbol = str(row.get("symbol") or "").upper().strip()
        errors: List[str] = []
        started = time.perf_counter()
        nse_symbol = str(row.get("nse_symbol") or "").upper().strip()
        cached = self._history_from_cache(symbol)
        effective_from_dt = from_dt
        if not cached.empty:
            cached["date"] = pd.to_datetime(cached["date"], errors="coerce")
            cached = cached.dropna(subset=["date"])
            latest_cached_date = cached["date"].max() if not cached.empty else pd.NaT
            if pd.notna(latest_cached_date) and len(cached) >= min(HISTORY_TRADING_DAYS, 180):
                effective_from_dt = max(from_dt, pd.Timestamp(latest_cached_date).to_pydatetime() - timedelta(days=15))

        if nse_symbol:
            try:
                hist_rows = self.client.get_history(nse_symbol, effective_from_dt, to_dt)
                hist_df = self._history_to_frame_nse(symbol, hist_rows)
                if not hist_df.empty:
                    return hist_df, "NSE", errors, (time.perf_counter() - started) * 1000
                errors.append("NSE:empty")
            except Exception as exc:
                errors.append(f"NSE:{exc}")

        for ticker, source in [(row.get("yahoo_ticker_ns"), "YAHOO_NS"), (row.get("yahoo_ticker_bo"), "YAHOO_BO")]:
            ticker = str(ticker or "").strip()
            if not ticker:
                continue
            try:
                hist_df = self._history_from_yfinance(symbol=symbol, market_symbol=ticker, from_dt=effective_from_dt, to_dt=to_dt, source_label=source)
                if not hist_df.empty:
                    return hist_df, source, errors, (time.perf_counter() - started) * 1000
                errors.append(f"{source}:empty")
            except Exception as exc:
                errors.append(f"{source}:{exc}")

        if not cached.empty:
            return cached, "CACHE", errors, (time.perf_counter() - started) * 1000
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"]), "", errors, (time.perf_counter() - started) * 1000

    def _quote_to_row_nse(self, symbol: str, market_symbol: str, company_name: str, payload: dict, latency_ms: float) -> dict:
        info = payload.get("priceInfo", {}) or {}
        quote_ts = self.run_ts.strftime("%Y-%m-%d %H:%M:%S")
        return {
            "symbol": symbol,
            "company_name": payload.get("info", {}).get("companyName") or company_name,
            "sector": payload.get("info", {}).get("industry") or "",
            "series": payload.get("securityInfo", {}).get("series") or "EQ",
            "ltp": _to_float(info.get("lastPrice")),
            "prev_close": _to_float(info.get("previousClose")),
            "open": _to_float(info.get("open")),
            "day_high": _to_float(info.get("intraDayHighLow", {}).get("max")),
            "day_low": _to_float(info.get("intraDayHighLow", {}).get("min")),
            "volume": _to_float(payload.get("securityWiseDP", {}).get("quantityTraded")) or _to_float(payload.get("preOpenMarket", {}).get("totalTradedVolume")),
            "quote_ts": quote_ts,
            "quote_source": "NSE",
            "market_symbol": market_symbol,
            "api_latency_ms": round(float(latency_ms), 2),
        }

    def _quote_from_yfinance(self, symbol: str, market_symbol: str, source_label: str, company_name: str) -> Tuple[dict, dict]:
        if not YF_AVAILABLE:
            raise RuntimeError("yfinance not installed")
        ticker = yf.Ticker(str(market_symbol))
        fast = getattr(ticker, "fast_info", None) or {}
        hist = ticker.history(period="5d", auto_adjust=False)

        ltp = _to_float(fast.get("lastPrice"))
        prev_close = _to_float(fast.get("previousClose"))
        day_high = _to_float(fast.get("dayHigh"))
        day_low = _to_float(fast.get("dayLow"))
        volume = _to_float(fast.get("lastVolume"))

        if (pd.isna(ltp) or pd.isna(prev_close)) and hist is not None and not hist.empty:
            hist = hist.copy()
            hist.columns = [str(c).lower() for c in hist.columns]
            last_row = hist.iloc[-1]
            ltp = ltp if not pd.isna(ltp) else _to_float(last_row.get("close"))
            day_high = day_high if not pd.isna(day_high) else _to_float(last_row.get("high"))
            day_low = day_low if not pd.isna(day_low) else _to_float(last_row.get("low"))
            volume = volume if not pd.isna(volume) else _to_float(last_row.get("volume"))
            if len(hist) >= 2:
                prev_close = prev_close if not pd.isna(prev_close) else _to_float(hist.iloc[-2].get("close"))

        row = {
            "symbol": symbol,
            "company_name": company_name,
            "sector": "",
            "series": "EQ",
            "ltp": ltp,
            "prev_close": prev_close,
            "open": np.nan,
            "day_high": day_high,
            "day_low": day_low,
            "volume": volume,
            "quote_ts": self.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quote_source": source_label,
            "market_symbol": market_symbol,
            "api_latency_ms": np.nan,
        }
        payload = {
            "priceInfo": {"lastPrice": row["ltp"], "previousClose": row["prev_close"], "open": row["open"], "intraDayHighLow": {"max": row["day_high"], "min": row["day_low"]}},
            "info": {"companyName": row["company_name"], "industry": ""},
            "securityInfo": {"series": "EQ"},
        }
        return row, payload

    def _quote_from_cache(self, symbol: str) -> dict:
        if self._cached_quotes.empty or "symbol" not in self._cached_quotes.columns:
            return {}
        rows = self._cached_quotes[self._cached_quotes["symbol"] == symbol]
        if rows.empty:
            return {}
        row = rows.iloc[0].to_dict()
        return {
            "symbol": symbol,
            "company_name": row.get("company_name") or COMPANY_NAME_BACKUP.get(symbol, symbol),
            "sector": row.get("sector", ""),
            "series": row.get("series", "EQ"),
            "ltp": _to_float(row.get("ltp")),
            "prev_close": _to_float(row.get("prev_close")),
            "open": _to_float(row.get("open")),
            "day_high": _to_float(row.get("day_high")),
            "day_low": _to_float(row.get("day_low")),
            "volume": _to_float(row.get("volume")),
            "quote_ts": row.get("quote_ts") or self.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quote_source": "CACHE",
            "market_symbol": row.get("market_symbol") or symbol,
            "api_latency_ms": np.nan,
        }

    def _quote_payload_from_row(self, row: dict) -> dict:
        return {
            "priceInfo": {
                "lastPrice": row.get("ltp"),
                "previousClose": row.get("prev_close"),
                "open": row.get("open"),
                "intraDayHighLow": {"max": row.get("day_high"), "min": row.get("day_low")},
            },
            "info": {"companyName": row.get("company_name") or COMPANY_NAME_BACKUP.get(row.get("symbol"), row.get("symbol"))},
            "securityInfo": {"series": row.get("series", "EQ")},
        }

    def _history_to_frame_nse(self, symbol: str, rows: Sequence[dict]) -> pd.DataFrame:
        out = []
        for row in rows:
            dt = pd.to_datetime(row.get("CH_TIMESTAMP") or row.get("mTIMESTAMP") or row.get("date"), dayfirst=True, errors="coerce")
            if pd.isna(dt):
                continue
            out.append(
                {
                    "symbol": symbol,
                    "date": dt.normalize(),
                    "open": _to_float(row.get("CH_OPENING_PRICE") or row.get("OPEN")),
                    "high": _to_float(row.get("CH_TRADE_HIGH_PRICE") or row.get("HIGH")),
                    "low": _to_float(row.get("CH_TRADE_LOW_PRICE") or row.get("LOW")),
                    "close": _to_float(row.get("CH_CLOSING_PRICE") or row.get("CLOSE")),
                    "prev_close": _to_float(row.get("CH_PREVIOUS_CLS_PRICE") or row.get("PREVCLOSE")),
                    "volume": _to_float(row.get("CH_TOT_TRADED_QTY") or row.get("TOTTRDQTY")),
                    "value": _to_float(row.get("CH_TOT_TRADED_VAL") or row.get("TOTTRDVAL")),
                    "source": "NSE",
                }
            )
        df = pd.DataFrame(out)
        if df.empty:
            return df
        return df.dropna(subset=["date", "close"]).drop_duplicates(subset=["symbol", "date"]).sort_values("date").tail(HISTORY_TRADING_DAYS).reset_index(drop=True)

    def _history_from_yfinance(self, symbol: str, market_symbol: str, from_dt: datetime, to_dt: datetime, source_label: str) -> pd.DataFrame:
        if not YF_AVAILABLE:
            raise RuntimeError("yfinance not installed")
        ticker = yf.Ticker(str(market_symbol))
        hist = ticker.history(start=(from_dt - timedelta(days=5)).strftime("%Y-%m-%d"), end=(to_dt + timedelta(days=1)).strftime("%Y-%m-%d"), auto_adjust=False, actions=False)
        if hist is None or hist.empty:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"])
        hist = hist.reset_index().copy()
        date_col = "Date" if "Date" in hist.columns else hist.columns[0]
        hist["date"] = pd.to_datetime(hist[date_col], errors="coerce").dt.tz_localize(None).dt.normalize()
        hist["symbol"] = symbol
        hist["open"] = pd.to_numeric(hist.get("Open"), errors="coerce")
        hist["high"] = pd.to_numeric(hist.get("High"), errors="coerce")
        hist["low"] = pd.to_numeric(hist.get("Low"), errors="coerce")
        hist["close"] = pd.to_numeric(hist.get("Close"), errors="coerce")
        hist["prev_close"] = hist["close"].shift(1)
        hist["volume"] = pd.to_numeric(hist.get("Volume"), errors="coerce")
        hist["value"] = hist["close"] * hist["volume"]
        hist["source"] = source_label
        keep = ["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"]
        hist = hist[keep]
        return hist.dropna(subset=["date", "close"]).drop_duplicates(subset=["symbol", "date"]).sort_values("date").tail(HISTORY_TRADING_DAYS).reset_index(drop=True)

    def _history_from_cache(self, symbol: str) -> pd.DataFrame:
        if self._cached_history.empty or "symbol" not in self._cached_history.columns:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"])
        df = self._cached_history[self._cached_history["symbol"] == symbol].copy()
        if df.empty:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"])
        for col in ["open", "high", "low", "close", "prev_close", "volume", "value"]:
            df[col] = pd.to_numeric(df.get(col), errors="coerce")
        df["source"] = "CACHE"
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df.dropna(subset=["date", "close"]).drop_duplicates(subset=["symbol", "date"]).sort_values("date").tail(HISTORY_TRADING_DAYS).reset_index(drop=True)[["symbol", "date", "open", "high", "low", "close", "prev_close", "volume", "value", "source"]]

    def _compute_signals(self, symbol: str, hist: pd.DataFrame, quote_payload: dict) -> dict:
        ltp = _to_float(quote_payload.get("priceInfo", {}).get("lastPrice"))
        prev_close = _to_float(quote_payload.get("priceInfo", {}).get("previousClose"))
        if hist is None or hist.empty or len(hist) < 30:
            return {
                "symbol": symbol,
                "signal_ts": self.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "ltp": ltp,
                "prev_close": prev_close,
                "change_1d_pct": np.nan,
                "change_1w_pct": np.nan,
                "change_1m_pct": np.nan,
                "change_3m_pct": np.nan,
                "change_6m_pct": np.nan,
                "change_9m_pct": np.nan,
                "change_12m_pct": np.nan,
                "sma20": np.nan,
                "sma50": np.nan,
                "sma200": np.nan,
                "ema20": np.nan,
                "rsi14": np.nan,
                "atr14": np.nan,
                "high_52w": np.nan,
                "low_52w": np.nan,
                "range_position_52w_pct": np.nan,
                "volume_ratio": np.nan,
                "trend_quality": np.nan,
                "technical_score": np.nan,
                "history_days": int(len(hist)) if hist is not None else 0,
                "history_recency_days": np.nan,
                "technical_regime": "INSUFFICIENT_HISTORY",
                "status_note": "Insufficient history",
            }

        hist = hist.sort_values("date").copy()
        close = pd.to_numeric(hist["close"], errors="coerce")
        high = pd.to_numeric(hist["high"], errors="coerce")
        low = pd.to_numeric(hist["low"], errors="coerce")
        volume = pd.to_numeric(hist["volume"], errors="coerce").fillna(0)

        hist["sma20"] = close.rolling(20).mean()
        hist["sma50"] = close.rolling(50).mean()
        hist["sma200"] = close.rolling(200).mean()
        hist["ema20"] = close.ewm(span=20, adjust=False).mean()
        hist["avg_volume_20"] = volume.rolling(20).mean()

        delta = close.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        avg_gain = up.rolling(14).mean()
        avg_loss = down.rolling(14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        hist["rsi14"] = 100 - (100 / (1 + rs))

        tr_components = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1)
        hist["tr"] = tr_components.max(axis=1)
        hist["atr14"] = hist["tr"].rolling(14).mean()

        for col, window in RETURN_WINDOWS.items():
            hist[col] = close.pct_change(window) * 100.0

        latest = hist.iloc[-1]
        if pd.isna(ltp):
            ltp = _to_float(latest.get("close"))
        if pd.isna(prev_close):
            prev_close = _to_float(hist.iloc[-2]["close"]) if len(hist) >= 2 else np.nan
        one_day_change = ((ltp / prev_close) - 1.0) * 100.0 if prev_close and not pd.isna(prev_close) else np.nan

        avg_vol_20 = _to_float(latest.get("avg_volume_20"))
        vol_ratio = float(latest["volume"] / avg_vol_20) if avg_vol_20 and not pd.isna(avg_vol_20) else np.nan
        high_52w = close.tail(252).max()
        low_52w = close.tail(252).min()
        range_pos = ((ltp - low_52w) / (high_52w - low_52w) * 100.0) if pd.notna(high_52w) and pd.notna(low_52w) and high_52w != low_52w else np.nan
        history_days = int(len(hist))
        latest_hist_date = pd.to_datetime(latest.get("date"), errors="coerce")
        history_recency_days = int(max((pd.Timestamp(self.run_ts.tz_localize(None) if getattr(self.run_ts, "tzinfo", None) else self.run_ts) - latest_hist_date).days, 0)) if pd.notna(latest_hist_date) else np.nan

        score = 0.0
        trend_quality = 0.0
        if pd.notna(latest["sma20"]) and ltp > latest["sma20"]:
            score += 10
        if pd.notna(latest["sma50"]) and ltp > latest["sma50"]:
            score += 12
        if pd.notna(latest["sma200"]) and ltp > latest["sma200"]:
            score += 16
        if pd.notna(latest["sma20"]) and pd.notna(latest["sma50"]) and pd.notna(latest["sma200"]):
            if latest["sma20"] > latest["sma50"] > latest["sma200"]:
                score += 14
                trend_quality += 25
            elif latest["sma20"] > latest["sma50"]:
                score += 8
                trend_quality += 12

        rsi = _to_float(latest.get("rsi14"))
        if pd.notna(rsi):
            score += float(np.clip(15 - abs(rsi - 58) * 0.75, 0, 15))
            if 48 <= rsi <= 68:
                trend_quality += 15

        c3 = _to_float(latest.get("change_3m_pct"))
        c12 = _to_float(latest.get("change_12m_pct"))
        if pd.notna(c3):
            score += float(np.clip((c3 + 5) * 0.55, 0, 12))
            if c3 > 0:
                trend_quality += 10
        if pd.notna(c12):
            score += float(np.clip((c12 + 10) * 0.18, 0, 10))
            if c12 > 5:
                trend_quality += 10
        if pd.notna(vol_ratio):
            score += float(np.clip(vol_ratio * 6, 0, 10))
        if pd.notna(range_pos):
            score += float(np.clip(10 - abs(range_pos - 78) * 0.18, 0, 10))
        atr14 = _to_float(latest.get("atr14"))
        if pd.notna(atr14) and ltp and not pd.isna(ltp):
            atr_pct = atr14 / ltp * 100.0
            if atr_pct > 6.5:
                score -= min((atr_pct - 6.5) * 1.2, 8)
            else:
                trend_quality += 8

        technical_score = float(np.clip(score, 0, 100))
        trend_quality = float(np.clip(trend_quality, 0, 100))
        if technical_score >= 80 and pd.notna(latest.get("sma20")) and pd.notna(latest.get("sma50")) and ltp > latest.get("sma20") > latest.get("sma50"):
            technical_regime = "STRONG_UPTREND"
        elif technical_score >= 65:
            technical_regime = "UPTREND"
        elif technical_score >= 50:
            technical_regime = "NEUTRAL"
        else:
            technical_regime = "WEAK"
        return {
            "symbol": symbol,
            "signal_ts": self.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "ltp": ltp,
            "prev_close": prev_close,
            "change_1d_pct": one_day_change,
            "change_1w_pct": latest.get("change_1w_pct"),
            "change_1m_pct": latest.get("change_1m_pct"),
            "change_3m_pct": latest.get("change_3m_pct"),
            "change_6m_pct": latest.get("change_6m_pct"),
            "change_9m_pct": latest.get("change_9m_pct"),
            "change_12m_pct": latest.get("change_12m_pct"),
            "sma20": latest.get("sma20"),
            "sma50": latest.get("sma50"),
            "sma200": latest.get("sma200"),
            "ema20": latest.get("ema20"),
            "rsi14": latest.get("rsi14"),
            "atr14": latest.get("atr14"),
            "high_52w": high_52w,
            "low_52w": low_52w,
            "range_position_52w_pct": range_pos,
            "volume_ratio": vol_ratio,
            "trend_quality": trend_quality,
            "technical_score": technical_score,
            "history_days": history_days,
            "history_recency_days": history_recency_days,
            "technical_regime": technical_regime,
            "status_note": "OK",
        }

    def _build_app_input_view(self, universe: pd.DataFrame, quotes: pd.DataFrame, signals: pd.DataFrame, news_scores: pd.DataFrame) -> pd.DataFrame:
        if universe is None or universe.empty:
            return pd.DataFrame()
        universe = universe.copy()
        universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
        quotes = quotes.copy() if quotes is not None else pd.DataFrame()
        signals = signals.copy() if signals is not None else pd.DataFrame()
        news_scores = news_scores.copy() if news_scores is not None else pd.DataFrame()
        for df in [quotes, signals, news_scores]:
            if not df.empty and "symbol" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

        df = universe.merge(quotes, on="symbol", how="left", suffixes=("", "_quote"))
        df = df.merge(signals, on="symbol", how="left", suffixes=("", "_signal"))
        if not news_scores.empty:
            df = df.merge(news_scores, on="symbol", how="left", suffixes=("", "_news"))

        if "company_name_quote" not in df.columns:
            df["company_name_quote"] = np.nan
        df["company_name"] = df["company_name_quote"].fillna(df["company_name"]).fillna(df["symbol"])
        df["company_name"] = df.apply(lambda r: COMPANY_NAME_BACKUP.get(r["symbol"], r["company_name"]), axis=1)

        if "ltp_signal" not in df.columns:
            df["ltp_signal"] = np.nan
        df["ltp"] = df["ltp_signal"].fillna(df.get("ltp"))
        df["quote_source"] = _safe_series(df, "quote_source", "UNAVAILABLE")

        df["history_source_hint"] = np.select(
            [
                df["status_note"].astype(str).str.contains("History=NSE", na=False),
                df["status_note"].astype(str).str.contains("History=YAHOO_NS", na=False),
                df["status_note"].astype(str).str.contains("History=YAHOO_BO", na=False),
                df["status_note"].astype(str).str.contains("History=CACHE", na=False),
            ],
            ["NSE historical equity", "Yahoo Finance NSE", "Yahoo Finance BSE", "SQLite cache"],
            default="Unknown",
        )

        numeric_cols = [
            "ltp", "change_1d_pct", "change_1w_pct", "change_1m_pct", "change_3m_pct", "change_6m_pct", "change_9m_pct", "change_12m_pct",
            "sma20", "sma50", "sma200", "ema20", "rsi14", "atr14", "range_position_52w_pct", "volume_ratio", "trend_quality",
            "technical_score", "news_score", "news_confidence", "news_article_count_7d", "news_article_count_30d", "news_bullish_count", "news_bearish_count",
            "history_days", "history_recency_days",
        ]
        for col in numeric_cols:
            if col not in df.columns:
                df[col] = np.nan
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["headline_risk_flag"] = _safe_series(df, "headline_risk_flag", "LOW")
        df["news_summary"] = _safe_series(df, "news_summary", "No recent cached news")
        df["top_positive_headline"] = _safe_series(df, "top_positive_headline", "")
        df["top_negative_headline"] = _safe_series(df, "top_negative_headline", "")
        df["latest_news_ts"] = pd.to_datetime(df.get("latest_news_ts"), errors="coerce", utc=True)

        df["liquidity_score"] = np.clip(40 + df["volume_ratio"].fillna(0) * 14, 0, 100)
        df["dqi_final"] = 100.0
        df.loc[df["ltp"].isna(), "dqi_final"] -= 45
        df.loc[df["technical_score"].isna(), "dqi_final"] -= 20
        df.loc[df["quote_source"].eq("CACHE"), "dqi_final"] -= 12
        df.loc[df["quote_source"].eq("YAHOO_BO"), "dqi_final"] -= 10
        df.loc[df["quote_source"].eq("YAHOO_NS"), "dqi_final"] -= 6
        df.loc[df["quote_source"].eq("UNAVAILABLE"), "dqi_final"] -= 30
        df.loc[df["history_source_hint"].eq("SQLite cache"), "dqi_final"] -= 10
        df.loc[df["history_days"].fillna(0) < 180, "dqi_final"] -= 12
        df.loc[df["history_recency_days"].fillna(999) > 7, "dqi_final"] -= 10
        df["dqi_final"] = df["dqi_final"].clip(0, 100)

        df["news_available_weight"] = np.select(
            [df["news_confidence"].fillna(0) >= 35, df["news_confidence"].fillna(0) >= 20],
            [0.22, 0.14],
            default=0.0,
        )
        base_component = (df["technical_score"].fillna(0) * 0.58) + (df["dqi_final"].fillna(0) * 0.20) + (df["liquidity_score"].fillna(0) * 0.10) + (df["trend_quality"].fillna(0) * 0.10)
        news_component = df["news_score"].fillna(50) * df["news_available_weight"]
        denominator = 0.58 + 0.20 + 0.10 + 0.10 + df["news_available_weight"]
        df["overall_score"] = np.round((base_component + news_component) / denominator.replace(0, 1), 2)

        high_risk = df["headline_risk_flag"].eq("HIGH")
        df["recommendation_confidence"] = (
            (df["technical_score"].fillna(0) * 0.45)
            + (df["dqi_final"].fillna(0) * 0.20)
            + (df["trend_quality"].fillna(0) * 0.15)
            + (df["liquidity_score"].fillna(0) * 0.10)
            + (df["news_confidence"].fillna(0) * 0.10)
        )
        df.loc[high_risk, "recommendation_confidence"] -= 25
        df.loc[df["history_days"].fillna(0) < 180, "recommendation_confidence"] -= 10
        df.loc[df["history_recency_days"].fillna(999) > 7, "recommendation_confidence"] -= 10
        df["recommendation_confidence"] = df["recommendation_confidence"].clip(0, 100)

        buy_gate = (
            df["ltp"].notna()
            & (~high_risk)
            & (df["technical_score"].fillna(0) >= 68)
            & (df["overall_score"].fillna(0) >= 80)
            & (df["dqi_final"].fillna(0) >= 74)
            & (df["history_days"].fillna(0) >= 180)
            & (df["history_recency_days"].fillna(999) <= 7)
            & (df["recommendation_confidence"].fillna(0) >= 60)
            & (df["change_3m_pct"].fillna(-999) > 0)
            & (df["volume_ratio"].fillna(0) >= 0.8)
        )
        hold_gate = (df["ltp"].notna()) & (df["overall_score"].fillna(0) >= 60) & (df["dqi_final"].fillna(0) >= 50)
        no_data_gate = df["ltp"].isna() | df["technical_score"].isna()
        df["recommendation"] = np.select(
            [buy_gate, hold_gate, no_data_gate],
            ["BUY", "HOLD", "NO DATA"],
            default="SELL",
        )
        df["actionable_flag"] = np.where(buy_gate, "YES", "NO")
        df["recommendation_reason"] = np.select(
            [
                buy_gate,
                df["ltp"].isna(),
                df["technical_score"].isna(),
                high_risk,
                df["dqi_final"].fillna(0) < 74,
                df["history_days"].fillna(0) < 180,
                df["history_recency_days"].fillna(999) > 7,
                df["technical_score"].fillna(0) < 68,
                df["overall_score"].fillna(0) < 80,
                df["change_3m_pct"].fillna(-999) <= 0,
                df["volume_ratio"].fillna(0) < 0.8,
                df["recommendation_confidence"].fillna(0) < 60,
            ],
            [
                "Buy criteria satisfied",
                "Missing live quote",
                "Insufficient price history",
                "High headline risk",
                "Low data quality",
                "History window too short",
                "History not recent",
                "Technical score below buy threshold",
                "Overall score below buy threshold",
                "Medium-term momentum weak",
                "Volume confirmation weak",
                "Confidence below buy threshold",
            ],
            default="Setup is weak versus current buy rules",
        )

        default_status = np.where(df["ltp"].notna(), "OK", "Quote/History missing")
        df["status_note"] = df.get("status_note").where(df.get("status_note").notna(), default_status)
        df.loc[high_risk, "status_note"] = df["status_note"].astype(str) + " | High headline risk"

        keep_cols = [
            "symbol", "company_name", "exchange_primary", "nse_symbol", "bse_symbol", "bse_code", "tags", "priority", "ltp", "quote_ts", "quote_source", "market_symbol",
            "change_1d_pct", "change_1w_pct", "change_1m_pct", "change_3m_pct", "change_6m_pct", "change_9m_pct", "change_12m_pct",
            "sma20", "sma50", "sma200", "ema20", "rsi14", "atr14", "range_position_52w_pct", "volume_ratio", "trend_quality", "technical_score",
            "news_score", "news_confidence", "news_article_count_7d", "news_article_count_30d", "news_bullish_count", "news_bearish_count", "headline_risk_flag", "latest_news_ts",
            "top_positive_headline", "top_negative_headline", "news_summary", "liquidity_score", "dqi_final", "overall_score", "recommendation_confidence", "recommendation_reason", "recommendation", "actionable_flag", "history_days", "history_recency_days", "technical_regime", "history_source_hint", "status_note",
        ]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = np.nan
        return df[keep_cols].sort_values(["overall_score", "technical_score", "priority", "symbol"], ascending=[False, False, True, True]).reset_index(drop=True)

    def _filter_to_symbols(self, df: pd.DataFrame, symbols: Optional[Sequence[str]]) -> pd.DataFrame:
        if df is None or df.empty or not symbols:
            return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        symbol_set = {str(x).upper().strip() for x in symbols if str(x).strip()}
        if not symbol_set or "symbol" not in df.columns:
            return df.copy()
        out = df[df["symbol"].astype(str).str.upper().isin(symbol_set)].copy()
        return out.reset_index(drop=True)

    def _apply_priority_symbols(self, df: pd.DataFrame, priority_symbols: Optional[Sequence[str]]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df.copy()
        out = df.copy()
        if "symbol" not in out.columns:
            return out
        out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
        if "priority" not in out.columns:
            out["priority"] = 999999
        out["priority"] = pd.to_numeric(out["priority"], errors="coerce").fillna(999999)
        priority_set = {str(x).upper().strip() for x in (priority_symbols or []) if str(x).strip()}
        if priority_set:
            out.loc[out["symbol"].isin(priority_set), "priority"] = out.loc[out["symbol"].isin(priority_set), "priority"] - 100000
        return out.sort_values(["priority", "symbol"], ascending=[True, True]).reset_index(drop=True)

    def _write_refresh_only(self, refresh_status: str) -> None:
        refresh_control = pd.DataFrame({"Control": ["last_refresh_ts", "refresh_status"], "Value": [self.run_ts.strftime("%Y-%m-%d %H:%M:%S %Z"), refresh_status]})
        self.db.write_tables({"Refresh_Control": refresh_control})


def _to_float(value) -> float:
    try:
        if value in (None, "", "-", "nan", "None"):
            return np.nan
        return float(str(value).replace(",", ""))
    except Exception:
        return np.nan


if __name__ == "__main__":
    RefreshEngine().refresh()
