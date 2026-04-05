from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

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
]


class MarketDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA foreign_keys=OFF;")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            for sql in INDEX_SQL:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError:
                    pass
            conn.commit()

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

    def read_tables(self, names: Iterable[str]) -> Dict[str, pd.DataFrame]:
        return {name: self.read_table(name) for name in names}

    def query(self, sql: str, params: Optional[Sequence] = None) -> pd.DataFrame:
        if not self.db_path.exists():
            return pd.DataFrame()
        with self.connect() as conn:
            return pd.read_sql_query(sql, conn, params=params or [])

    def _write_df(self, conn: sqlite3.Connection, table: str, df: pd.DataFrame, if_exists: str) -> None:
        if df is None or df.empty:
            if if_exists == "append":
                return
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (__empty__ TEXT)')
            conn.execute(f'DELETE FROM "{table}"')
            return

        safe_df = df.copy()
        for col in safe_df.columns:
            if pd.api.types.is_datetime64_any_dtype(safe_df[col]):
                safe_df[col] = safe_df[col].astype(str)
        safe_df.to_sql(table, conn, if_exists=if_exists, index=False)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        for sql in INDEX_SQL:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass
