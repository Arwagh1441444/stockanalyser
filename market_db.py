from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

import numpy as np
import pandas as pd

TABLE_MAP = {
    "Universe_Master": "universe_master",
    "Latest_Quotes": "latest_quotes",
    "Daily_History": "daily_history",
    "Latest_Signals": "latest_signals",
    "App_Input_View": "app_input_view",
    "News_Articles": "news_articles",
    "News_Scores": "news_scores",
    "Provider_Log": "provider_log",
    "Refresh_Control": "refresh_control",
    "Config": "config",
    "portfolio_positions": "portfolio_positions",
}

REVERSE_TABLE_MAP = {v: k for k, v in TABLE_MAP.items()}

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_universe_symbol ON universe_master(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_universe_priority ON universe_master(priority, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_quotes_symbol ON latest_quotes(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_quotes_ts ON latest_quotes(quote_ts)",
    "CREATE INDEX IF NOT EXISTS idx_history_symbol_date ON daily_history(symbol, date)",
    "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON latest_signals(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_app_view_symbol ON app_input_view(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_app_view_score ON app_input_view(overall_score DESC, technical_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_news_articles_symbol_time ON news_articles(symbol, published_at)",
    "CREATE INDEX IF NOT EXISTS idx_news_articles_url ON news_articles(url)",
    "CREATE INDEX IF NOT EXISTS idx_news_scores_symbol ON news_scores(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_provider_refresh_ts ON provider_log(refresh_ts)",
    "CREATE INDEX IF NOT EXISTS idx_refresh_control_key ON refresh_control(Control)",
    "CREATE INDEX IF NOT EXISTS idx_portfolio_symbol ON portfolio_positions(symbol)",
]


class MarketDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA foreign_keys=OFF;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA cache_size=-20000;")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            for sql in INDEX_SQL:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError:
                    pass
            conn.commit()

    def table_exists(self, name: str) -> bool:
        table = TABLE_MAP.get(name, name.lower())
        if not self.db_path.exists():
            return False
        with self.connect() as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
        return bool(exists)



    def table_columns(self, name: str) -> list[str]:
        table = TABLE_MAP.get(name, name.lower())
        if not self.db_path.exists():
            return []
        with self.connect() as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                return []
            rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        return [str(r[1]) for r in rows if len(r) > 1]

    def table_has_columns(self, name: str, required: Sequence[str]) -> bool:
        cols = {c.lower() for c in self.table_columns(name)}
        return all(str(col).lower() in cols for col in required)

    def write_table(self, name: str, df: pd.DataFrame, if_exists: str = "replace") -> None:
        table = TABLE_MAP.get(name, name.lower())
        with self.connect() as conn:
            self._write_df(conn, table, df, if_exists=if_exists)
            self._ensure_indexes(conn)
            conn.commit()

    def write_tables(self, sheet_map: Dict[str, pd.DataFrame]) -> None:
        with self.connect() as conn:
            for name, df in sheet_map.items():
                table = TABLE_MAP.get(name, name.lower())
                self._write_df(conn, table, df, if_exists="replace")
            self._ensure_indexes(conn)
            conn.commit()

    def append_table(self, name: str, df: pd.DataFrame) -> None:
        table = TABLE_MAP.get(name, name.lower())
        with self.connect() as conn:
            self._write_df(conn, table, df, if_exists="append")
            self._ensure_indexes(conn)
            conn.commit()

    def read_table(self, name: str) -> pd.DataFrame:
        table = TABLE_MAP.get(name, name.lower())
        if not self.db_path.exists():
            return pd.DataFrame()
        with self.connect() as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                return pd.DataFrame()
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
        if "__empty__" in df.columns:
            return pd.DataFrame()
        return df

    def read_table_columns(self, name: str, columns: Sequence[str]) -> pd.DataFrame:
        if not columns:
            return self.read_table(name)
        table = TABLE_MAP.get(name, name.lower())
        if not self.table_exists(table):
            return pd.DataFrame(columns=list(columns))
        quoted = ", ".join([f'"{str(c)}"' for c in columns])
        with self.connect() as conn:
            df = pd.read_sql_query(f'SELECT {quoted} FROM "{table}"', conn)
        if "__empty__" in df.columns:
            return pd.DataFrame(columns=list(columns))
        return df

    def read_tables(self, names: Iterable[str]) -> Dict[str, pd.DataFrame]:
        return {name: self.read_table(name) for name in names}

    def query(self, sql: str, params: Optional[Sequence] = None) -> pd.DataFrame:
        if not self.db_path.exists():
            return pd.DataFrame()
        with self.connect() as conn:
            return pd.read_sql_query(sql, conn, params=params or [])

    def read_symbol_history(self, symbol: str, limit: Optional[int] = None) -> pd.DataFrame:
        symbol = str(symbol or "").upper().strip()
        if not symbol or not self.table_exists("Daily_History"):
            return pd.DataFrame()
        cols = self.table_columns("Daily_History")
        if not cols or "__empty__" in cols or not self.table_has_columns("Daily_History", ["symbol", "date"]):
            return pd.DataFrame()
        sql = 'SELECT * FROM "daily_history" WHERE UPPER(symbol) = ? ORDER BY date ASC'
        if limit is not None and int(limit) > 0:
            sql = (
                'SELECT * FROM ('
                'SELECT * FROM "daily_history" WHERE UPPER(symbol) = ? ORDER BY date DESC LIMIT ?'
                ') ORDER BY date ASC'
            )
            return self.query(sql, [symbol, int(limit)])
        return self.query(sql, [symbol])

    def read_symbol_news(self, symbol: str, limit: Optional[int] = None) -> pd.DataFrame:
        symbol = str(symbol or "").upper().strip()
        if not symbol or not self.table_exists("News_Articles"):
            return pd.DataFrame()
        cols = self.table_columns("News_Articles")
        if not cols or "__empty__" in cols or "symbol" not in {c.lower() for c in cols}:
            return pd.DataFrame()
        order_parts = []
        cols_lower = {c.lower(): c for c in cols}
        if "published_at" in cols_lower:
            order_parts.append(f'"{cols_lower["published_at"]}" DESC')
        if "article_sentiment_raw" in cols_lower:
            order_parts.append(f'"{cols_lower["article_sentiment_raw"]}" DESC')
        order_sql = f' ORDER BY {", ".join(order_parts)}' if order_parts else ""
        sql = f'SELECT * FROM "news_articles" WHERE UPPER(symbol) = ?{order_sql}'
        params: list[object] = [symbol]
        if limit is not None and int(limit) > 0:
            sql += ' LIMIT ?'
            params.append(int(limit))
        return self.query(sql, params)

    def read_news_for_symbols(self, symbols: Sequence[str], limit: Optional[int] = None) -> pd.DataFrame:
        wanted = [str(x).upper().strip() for x in (symbols or []) if str(x).strip()]
        if not wanted or not self.table_exists("News_Articles"):
            return pd.DataFrame()
        cols = self.table_columns("News_Articles")
        if not cols or "__empty__" in cols or "symbol" not in {c.lower() for c in cols}:
            return pd.DataFrame()
        placeholders = ", ".join(["?"] * len(wanted))
        order_parts = []
        cols_lower = {c.lower(): c for c in cols}
        if "published_at" in cols_lower:
            order_parts.append(f'"{cols_lower["published_at"]}" DESC')
        if "article_sentiment_raw" in cols_lower:
            order_parts.append(f'"{cols_lower["article_sentiment_raw"]}" DESC')
        order_sql = f' ORDER BY {", ".join(order_parts)}' if order_parts else ""
        sql = (
            'SELECT * FROM "news_articles" '
            f'WHERE UPPER(symbol) IN ({placeholders}){order_sql}'
        )
        params: list[object] = wanted
        if limit is not None and int(limit) > 0:
            sql += ' LIMIT ?'
            params.append(int(limit))
        return self.query(sql, params)

    @staticmethod
    def _normalize_sql_value(value):
        if value is None:
            return None
        if value is pd.NaT:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        if isinstance(value, (np.floating, np.integer, np.bool_)):
            return value.item()
        if isinstance(value, (pd.Timestamp, datetime)):
            if pd.isna(value):
                return None
            return value.isoformat(sep=" ")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, (list, dict, tuple, set)):
            try:
                return json.dumps(list(value) if isinstance(value, set) else value, ensure_ascii=False)
            except Exception:
                return str(value)
        return value

    def _write_df(self, conn: sqlite3.Connection, table: str, df: pd.DataFrame, if_exists: str) -> None:
        if df is None or df.empty:
            if if_exists == "append":
                return
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (__empty__ TEXT)')
            conn.execute(f'DELETE FROM "{table}"')
            return

        safe_df = df.copy()
        safe_df.columns = [str(c) for c in safe_df.columns]

        for col in safe_df.columns:
            series = safe_df[col]
            if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_timedelta64_dtype(series):
                safe_df[col] = series.astype(str).replace({"NaT": None, "nan": None})
            elif pd.api.types.is_object_dtype(series) or str(series.dtype).startswith("period"):
                safe_df[col] = series.map(self._normalize_sql_value)

        safe_df = safe_df.where(pd.notnull(safe_df), None)
        safe_df.to_sql(table, conn, if_exists=if_exists, index=False, chunksize=1000)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        for sql in INDEX_SQL:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass
