from __future__ import annotations

import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NEWS_API_URL = "https://newsapi.org/v2/everything"
DEFAULT_NEWS_API_KEY = "f8d8079a60f84e02bb987ed0ad62b79d"
NEWS_TIMEOUT = 20
MAX_ARTICLES_PER_SYMBOL = 30
REFRESH_COOLDOWN_HOURS = 18

POSITIVE_PATTERNS = {
    r"\bbeat(s|ing)?\b": 11,
    r"\bstrong\b": 6,
    r"\brecord\b": 5,
    r"\borders?\b": 7,
    r"\border win\b": 10,
    r"\bcontract\b": 6,
    r"\bapproval\b": 7,
    r"\bdividend\b": 6,
    r"\bbuyback\b": 8,
    r"\bupgrade\b": 8,
    r"\bexpansion\b": 5,
    r"\bcapex\b": 4,
    r"\bguidance raised\b": 10,
    r"\bprofit rises?\b": 8,
    r"\bmargin expands?\b": 8,
    r"\bnew project\b": 8,
    r"\bcommissioned\b": 7,
    r"\bacquisition\b": 4,
    r"\bstake increase\b": 6,
    r"\bdebt reduction\b": 7,
    r"\bdeleverag\w*\b": 7,
}

NEGATIVE_PATTERNS = {
    r"\bmiss(es|ed)?\b": -10,
    r"\bweak\b": -6,
    r"\bloss(es| widened)?\b": -9,
    r"\bdowngrade\b": -9,
    r"\bfraud\b": -18,
    r"\bdefault\b": -18,
    r"\binsolvenc\w*\b": -20,
    r"\bpenalty\b": -10,
    r"\bprobe\b": -11,
    r"\binvestigation\b": -12,
    r"\braid\b": -12,
    r"\bpledge\b": -8,
    r"\bfire\b": -7,
    r"\baccident\b": -8,
    r"\bdelay\b": -5,
    r"\bfalls?\b": -4,
    r"\bcut(s|ting)? guidance\b": -12,
    r"\bresigns?\b": -6,
    r"\bblock deal\b": -3,
    r"\bgovernance\b": -10,
    r"\bsebi\b": -4,
    r"\bwarning\b": -7,
    r"\bslump\b": -7,
    r"\bdisappoint\w*\b": -9,
}

SEVERE_NEGATIVE_PATTERNS = [
    r"\bfraud\b",
    r"\bdefault\b",
    r"\binsolvenc\w*\b",
    r"\bprobe\b",
    r"\binvestigation\b",
    r"\braid\b",
    r"\bgovernance\b",
]

SOURCE_WEIGHTS = {
    "reuters": 1.20,
    "bloomberg": 1.20,
    "livemint": 1.10,
    "economictimes": 1.08,
    "business-standard": 1.08,
    "moneycontrol": 1.04,
    "cnbctv18": 1.04,
    "thehindubusinessline": 1.06,
    "ndtvprofit": 1.04,
    "mint": 1.08,
    "nseindia": 1.20,
    "bseindia": 1.20,
}

CORPORATE_SUFFIXES = [
    " limited",
    " ltd",
    " ltd.",
    " limited.",
    " corporation",
    " corp",
    " industries",
    " company",
    " services",
    " holdings",
    " financial services",
    " finance",
    " india",
]


def build_retry_session(api_key: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.7,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "X-Api-Key": api_key,
            "User-Agent": "AR-Tiger-Tech-Analysis/2.0",
            "Accept": "application/json",
        }
    )
    return session


@dataclass
class NewsRefreshResult:
    articles: pd.DataFrame
    scores: pd.DataFrame
    provider_rows: List[dict]


class NewsAPIClient:
    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("NEWS_API_KEY") or DEFAULT_NEWS_API_KEY
        self.session = build_retry_session(self.api_key)

    def enabled(self) -> bool:
        return bool(self.api_key)

    def search_company_news(
        self,
        *,
        symbol: str,
        company_name: str,
        query_aliases: Sequence[str],
        from_dt: datetime,
        to_dt: datetime,
        page_size: int = MAX_ARTICLES_PER_SYMBOL,
    ) -> List[dict]:
        if not self.enabled():
            raise RuntimeError("News API key missing")

        query = build_news_query(company_name=company_name, symbol=symbol, aliases=query_aliases)
        params = {
            "q": query,
            "searchIn": "title,description",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": min(int(page_size), 100),
            "page": 1,
            "from": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        r = self.session.get(NEWS_API_URL, params=params, timeout=NEWS_TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != "ok":
            raise RuntimeError(payload.get("message") or "News API error")
        return payload.get("articles", [])


def build_news_query(company_name: str, symbol: str, aliases: Sequence[str]) -> str:
    cleaned_name = _clean_company_name(company_name)
    phrases: List[str] = []
    if company_name:
        phrases.append(f'"{company_name}"')
    if cleaned_name and cleaned_name.lower() != company_name.lower():
        phrases.append(f'"{cleaned_name}"')

    for alias in aliases:
        alias = str(alias or "").strip()
        if alias and alias.lower() not in {company_name.lower(), cleaned_name.lower()}:
            phrases.append(f'"{alias}"')

    symbol = str(symbol or "").strip().upper()
    if 3 <= len(symbol) <= 10 and re.fullmatch(r"[A-Z0-9&.-]+", symbol):
        phrases.append(f'"{symbol}"')

    phrases = list(dict.fromkeys([p for p in phrases if p]))
    if not phrases:
        return f'"{symbol}"'

    if len(phrases) == 1:
        return phrases[0]
    return " OR ".join(phrases[:4])


class NewsScorer:
    def __init__(self) -> None:
        self.positive_patterns = [(re.compile(p, re.I), score) for p, score in POSITIVE_PATTERNS.items()]
        self.negative_patterns = [(re.compile(p, re.I), score) for p, score in NEGATIVE_PATTERNS.items()]
        self.severe_negative_patterns = [re.compile(p, re.I) for p in SEVERE_NEGATIVE_PATTERNS]

    def article_score(self, title: str, description: str, source_name: str, published_at: pd.Timestamp) -> Dict[str, object]:
        title = str(title or "")
        description = str(description or "")
        source_name = str(source_name or "")
        text = f"{title}. {description}".strip().lower()

        raw = 0.0
        positive_hits = 0
        negative_hits = 0
        severe_flag = False

        for pattern, score in self.positive_patterns:
            matches = len(pattern.findall(text))
            if matches:
                raw += score * min(matches, 2)
                positive_hits += matches

        for pattern, score in self.negative_patterns:
            matches = len(pattern.findall(text))
            if matches:
                raw += score * min(matches, 2)
                negative_hits += matches

        for pattern in self.severe_negative_patterns:
            if pattern.search(text):
                severe_flag = True
                raw -= 8

        if title:
            raw *= 1.15

        age_hours = 0.0
        if pd.notna(published_at):
            now_utc = pd.Timestamp.now(tz="UTC")
            age_hours = max((now_utc - published_at.tz_convert("UTC")).total_seconds() / 3600.0, 0.0)
        recency_weight = float(math.exp(-age_hours / 96.0)) if age_hours else 1.0

        source_mult = 1.0
        source_key = re.sub(r"[^a-z0-9]+", "", source_name.lower())
        for key, mult in SOURCE_WEIGHTS.items():
            if key in source_key:
                source_mult = max(source_mult, mult)

        weighted_raw = float(np.clip(raw * recency_weight * source_mult, -100, 100))
        return {
            "article_sentiment_raw": weighted_raw,
            "article_sentiment_label": "Positive" if weighted_raw >= 8 else "Negative" if weighted_raw <= -8 else "Neutral",
            "positive_hits": positive_hits,
            "negative_hits": negative_hits,
            "severe_negative_flag": int(severe_flag),
            "recency_weight": recency_weight,
            "source_weight": source_mult,
        }

    def aggregate_symbol(self, symbol: str, company_name: str, articles: pd.DataFrame) -> Dict[str, object]:
        if articles is None or articles.empty:
            return {
                "symbol": symbol,
                "company_name": company_name,
                "news_score": 50.0,
                "news_confidence": 0.0,
                "news_article_count_7d": 0,
                "news_article_count_30d": 0,
                "news_bullish_count": 0,
                "news_bearish_count": 0,
                "headline_risk_flag": "LOW",
                "latest_news_ts": pd.NaT,
                "top_positive_headline": "",
                "top_negative_headline": "",
                "news_summary": "No recent cached news",
                "news_refresh_ts": pd.Timestamp.now(tz="Asia/Kolkata"),
            }

        work = articles.copy()
        work["published_at"] = pd.to_datetime(work["published_at"], errors="coerce", utc=True)
        now_utc = pd.Timestamp.now(tz="UTC")
        work["days_old"] = (now_utc - work["published_at"]).dt.total_seconds().div(86400).clip(lower=0)
        work_7 = work[work["days_old"] <= 7].copy()
        if work_7.empty:
            work_7 = work.head(min(len(work), 10)).copy()

        weights = (work_7["recency_weight"].fillna(0.2) * work_7["source_weight"].fillna(1.0)).replace(0, 0.2)
        weighted_mean = np.average(work_7["article_sentiment_raw"].fillna(0.0), weights=weights)
        coverage_bonus = min(math.log1p(len(work_7)) * 4.5, 12)
        severe_penalty = min(work_7["severe_negative_flag"].fillna(0).sum() * 7, 20)
        news_score = float(np.clip(50 + weighted_mean + coverage_bonus - severe_penalty, 0, 100))

        bullish_count = int((work_7["article_sentiment_raw"] >= 8).sum())
        bearish_count = int((work_7["article_sentiment_raw"] <= -8).sum())
        source_diversity = work_7["source_name"].fillna("").astype(str).str.lower().nunique()
        confidence = float(np.clip(15 + len(work_7) * 6 + source_diversity * 7, 0, 100))

        risk_flag = "HIGH" if work_7["severe_negative_flag"].fillna(0).sum() > 0 or news_score < 35 else "MEDIUM" if bearish_count > bullish_count and bearish_count >= 2 else "LOW"
        positive_row = work_7.sort_values("article_sentiment_raw", ascending=False).head(1)
        negative_row = work_7.sort_values("article_sentiment_raw", ascending=True).head(1)

        summary_bits = []
        if bullish_count > bearish_count:
            summary_bits.append("news flow tilted positive")
        elif bearish_count > bullish_count:
            summary_bits.append("news flow tilted negative")
        else:
            summary_bits.append("news flow mixed")
        if len(work_7) >= 5:
            summary_bits.append(f"{len(work_7)} recent articles")
        if risk_flag == "HIGH":
            summary_bits.append("headline risk elevated")

        return {
            "symbol": symbol,
            "company_name": company_name,
            "news_score": round(news_score, 2),
            "news_confidence": round(confidence, 2),
            "news_article_count_7d": int(len(work_7)),
            "news_article_count_30d": int(len(work)),
            "news_bullish_count": bullish_count,
            "news_bearish_count": bearish_count,
            "headline_risk_flag": risk_flag,
            "latest_news_ts": work["published_at"].max(),
            "top_positive_headline": positive_row["title"].iloc[0] if not positive_row.empty else "",
            "top_negative_headline": negative_row["title"].iloc[0] if not negative_row.empty else "",
            "news_summary": "; ".join(summary_bits),
            "news_refresh_ts": pd.Timestamp.now(tz="Asia/Kolkata"),
        }


def refresh_news_for_universe(
    universe: pd.DataFrame,
    cached_scores: pd.DataFrame,
    cached_articles: pd.DataFrame,
    *,
    max_requests: int,
    cooldown_hours: int = REFRESH_COOLDOWN_HOURS,
) -> NewsRefreshResult:
    client = NewsAPIClient()
    scorer = NewsScorer()
    started = time.perf_counter()
    provider_rows: List[dict] = []

    if universe is None or universe.empty:
        return NewsRefreshResult(pd.DataFrame(), pd.DataFrame(), provider_rows)

    if not client.enabled() or max_requests <= 0:
        scores = cached_scores.copy() if cached_scores is not None else pd.DataFrame()
        provider_rows.append(
            _provider_row(
                provider="NEWSAPI",
                dataset="News_Scores",
                status="SKIPPED",
                rows_loaded=0,
                latency_ms=(time.perf_counter() - started) * 1000,
                latest_error="News disabled or request budget is zero",
            )
        )
        return NewsRefreshResult(cached_articles if cached_articles is not None else pd.DataFrame(), scores, provider_rows)

    cached_scores = cached_scores.copy() if cached_scores is not None else pd.DataFrame()
    cached_articles = cached_articles.copy() if cached_articles is not None else pd.DataFrame()
    if not cached_scores.empty and "news_refresh_ts" in cached_scores.columns:
        cached_scores["news_refresh_ts"] = pd.to_datetime(cached_scores["news_refresh_ts"], errors="coerce", utc=True)

    universe = universe.copy().sort_values(["priority", "symbol"]).reset_index(drop=True)
    now_utc = datetime.now(timezone.utc)
    from_dt = now_utc - timedelta(days=7)

    article_rows: List[dict] = []
    score_rows: List[dict] = []
    errors: List[str] = []
    requests_used = 0

    cached_score_map = {}
    if not cached_scores.empty and "symbol" in cached_scores.columns:
        cached_score_map = {
            str(row["symbol"]).upper(): row for _, row in cached_scores.iterrows()
        }

    refresh_cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=cooldown_hours)

    for _, row in universe.iterrows():
        symbol = str(row.get("symbol") or "").upper().strip()
        company_name = str(row.get("company_name") or symbol)
        aliases = _collect_aliases(row)
        cached_row = cached_score_map.get(symbol)
        should_refresh = requests_used < max_requests
        if cached_row is not None:
            last_ts = pd.to_datetime(cached_row.get("news_refresh_ts"), errors="coerce", utc=True)
            if pd.notna(last_ts) and last_ts >= refresh_cutoff:
                should_refresh = False

        symbol_articles = pd.DataFrame()
        if should_refresh:
            try:
                fetched = client.search_company_news(
                    symbol=symbol,
                    company_name=company_name,
                    query_aliases=aliases,
                    from_dt=from_dt,
                    to_dt=now_utc,
                )
                requests_used += 1
                symbol_articles = _articles_to_frame(symbol=symbol, company_name=company_name, raw_articles=fetched, scorer=scorer)
            except Exception as exc:
                errors.append(f"{symbol}: {exc}")

        if symbol_articles.empty and not cached_articles.empty and "symbol" in cached_articles.columns:
            symbol_articles = cached_articles[cached_articles["symbol"].astype(str).str.upper() == symbol].copy()

        if not symbol_articles.empty:
            article_rows.extend(symbol_articles.to_dict("records"))

        aggregated = scorer.aggregate_symbol(symbol=symbol, company_name=company_name, articles=symbol_articles)
        if cached_row is not None and aggregated["news_article_count_30d"] == 0:
            aggregated = dict(cached_row)
            aggregated["symbol"] = symbol
            aggregated["company_name"] = company_name
        score_rows.append(aggregated)

    articles_df = pd.DataFrame(article_rows)
    if not articles_df.empty:
        articles_df["url"] = articles_df["url"].astype(str)
        articles_df = articles_df.drop_duplicates(subset=["symbol", "url"], keep="first").reset_index(drop=True)

    scores_df = pd.DataFrame(score_rows)
    if not scores_df.empty:
        scores_df["symbol"] = scores_df["symbol"].astype(str).str.upper().str.strip()
        scores_df = scores_df.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)

    provider_rows.append(
        _provider_row(
            provider="NEWSAPI",
            dataset="News_Articles",
            status="SUCCESS" if requests_used > 0 or not articles_df.empty else "FAILED",
            rows_loaded=len(articles_df),
            latency_ms=(time.perf_counter() - started) * 1000,
            latest_error=" | ".join(errors[:8]),
        )
    )
    provider_rows.append(
        _provider_row(
            provider="NEWSAPI",
            dataset="News_Scores",
            status="SUCCESS" if not scores_df.empty else "FAILED",
            rows_loaded=len(scores_df),
            latency_ms=(time.perf_counter() - started) * 1000,
            latest_error="",
        )
    )
    return NewsRefreshResult(articles_df, scores_df, provider_rows)


def _provider_row(provider: str, dataset: str, status: str, rows_loaded: int, latency_ms: float, latest_error: str) -> dict:
    return {
        "provider": provider,
        "dataset": dataset,
        "status": status,
        "rows_loaded": int(rows_loaded),
        "avg_latency_ms": round(float(latency_ms), 2),
        "latest_error": str(latest_error or ""),
        "refresh_ts": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
    }


def _articles_to_frame(symbol: str, company_name: str, raw_articles: Iterable[dict], scorer: NewsScorer) -> pd.DataFrame:
    rows: List[dict] = []
    for article in raw_articles or []:
        published_at = pd.to_datetime(article.get("publishedAt"), errors="coerce", utc=True)
        title = str(article.get("title") or "").strip()
        description = str(article.get("description") or "").strip()
        source_name = str((article.get("source") or {}).get("name") or "").strip()
        url = str(article.get("url") or "").strip()
        if not title or not url:
            continue
        score_bits = scorer.article_score(title=title, description=description, source_name=source_name, published_at=published_at)
        rows.append(
            {
                "symbol": symbol,
                "company_name": company_name,
                "published_at": published_at,
                "source_name": source_name,
                "author": article.get("author"),
                "title": title,
                "description": description,
                "url": url,
                "image_url": article.get("urlToImage"),
                **score_bits,
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values(["published_at", "article_sentiment_raw"], ascending=[False, False]).drop_duplicates(subset=["url"], keep="first")
    return df.reset_index(drop=True)


def _collect_aliases(row: pd.Series) -> List[str]:
    aliases = []
    for key in ["company_name", "symbol", "nse_symbol", "bse_symbol"]:
        value = str(row.get(key) or "").strip()
        if value:
            aliases.append(value)
    cleaned = _clean_company_name(str(row.get("company_name") or ""))
    if cleaned:
        aliases.append(cleaned)
    return list(dict.fromkeys([a for a in aliases if a]))


def _clean_company_name(name: str) -> str:
    out = str(name or "").strip()
    lower = out.lower()
    for suffix in CORPORATE_SUFFIXES:
        if lower.endswith(suffix):
            out = out[: -len(suffix)].strip(" ,.-")
            lower = out.lower()
    return out
