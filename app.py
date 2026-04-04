import pandas as pd
import numpy as np
import streamlit as st
import yfinance as yf
from datetime import datetime, time
import pytz

# ---------------- CONFIG ----------------
CAPITAL = 100000
RISK_PER_TRADE = 0.01
MAX_POSITION_PCT = 0.2

# ---------------- MARKET TIME ----------------
def is_market_open():
    try:
        india = pytz.timezone("Asia/Kolkata")
        now = datetime.now(india)
        return now.weekday() < 5 and time(9, 15) <= now.time() <= time(15, 30)
    except Exception:
        return False

# ---------------- DATA FETCH ----------------
@st.cache_data(ttl=300)
def get_data(symbol):
    try:
        df = yf.download(symbol + ".NS", period="1y", interval="1d", progress=False)
        if df is None or df.empty:
            return None
        df = df.dropna()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if len(df) < 200:
            return None
        return df
    except Exception:
        return None

# ---------------- INDICATORS ----------------
def compute_indicators(df):
    try:
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / (loss + 1e-9)
        rsi = (100 - (100 / (1 + rs))).iloc[-1]

        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = (ema12 - ema26).iloc[-1]

        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)

        atr = tr.rolling(14).mean().iloc[-1]
        avg_vol = volume.tail(20).mean()

        values = [ma50, ma200, rsi, macd, atr, avg_vol]
        if any(pd.isna(v) for v in values):
            return None

        return tuple(map(float, values))
    except Exception:
        return None

# ---------------- SCORING ----------------
def score_stock(price, ma50, ma200, rsi, macd, atr):
    try:
        trend = 1 if price > ma200 else 0
        momentum = (0.5 if macd > 0 else 0) + (0.5 if 40 < rsi < 70 else 0)
        deviation = abs(price - ma50) / (ma50 + 1e-9)
        mean_rev = 1 if deviation < 0.07 else 0
        vol_score = 1 / (1 + (atr / (price + 1e-9)))
        score = (0.35 * trend + 0.30 * momentum +
                 0.20 * mean_rev + 0.15 * vol_score)
        return round(score * 100, 2)
    except Exception:
        return 0

# ---------------- TRADE ----------------
def generate_trade(price, score, atr):
    try:
        if score > 70:
            action = "BUY"
            stop = price - 1.5 * atr
            target = price + 3 * atr
        elif score < 30:
            action = "SELL"
            stop = price + 1.5 * atr
            target = price - 3 * atr
        else:
            return None

        risk_amt = CAPITAL * RISK_PER_TRADE
        risk_per_share = abs(price - stop)

        qty = int(risk_amt / risk_per_share) if risk_per_share > 0 else 0
        max_qty = int((CAPITAL * MAX_POSITION_PCT) / price)
        qty = min(qty, max_qty)

        move = abs(target - price)
        duration = min((move / (atr + 1e-9)) * 5, 30)  # practical duration
        target_pct = ((target - price) / (price + 1e-9)) * 100

        return {
            "Action": action,
            "StopLoss": round(stop, 2),
            "Target": round(target, 2),
            "Target %": round(target_pct, 2),
            "Qty": qty,
            "Est Days": round(duration, 1)
        }
    except Exception:
        return None

# ---------------- UI ----------------
st.set_page_config(page_title="NSE Stock Analyzer", layout="wide")
st.title("📊 NSE Stock Analyzer")

st.info("🟢 Market Open" if is_market_open() else "🔴 Market Closed")

# User enters or searches for a stock symbol
symbol = st.text_input("Enter NSE stock symbol (e.g., RELIANCE, TCS, INFY):")

if symbol:
    df = get_data(symbol.upper())
    if df is None:
        st.error("No data found for this symbol")
    else:
        try:
            price = float(df["Close"].iloc[-1])
        except Exception:
            st.error("Could not read price")
            st.stop()

        indicators = compute_indicators(df)
        if indicators is None:
            st.error("Indicators could not be computed")
        else:
            ma50, ma200, rsi, macd, atr, avg_vol = indicators
            score = score_stock(price, ma50, ma200, rsi, macd, atr)
            trade = generate_trade(price, score, atr)

            st.subheader(f"📊 Analysis for {symbol.upper()}")
            st.write(f"**Price:** {round(price,2)}")
            st.write(f"**MA50:** {round(ma50,2)} | **MA200:** {round(ma200,2)}")
            st.write(f"**RSI:** {round(rsi,2)} | **MACD:** {round(macd,2)}")
            st.write(f"**ATR:** {round(atr,2)} | **Avg Vol (20d):** {int(avg_vol)}")
            st.write(f"**Score:** {score}")

            if trade:
                st.write(f"**Action:** {trade['Action']}")
                st.write(f"**StopLoss:** {trade['StopLoss']} | **Target:** {trade['Target']} ({trade['Target %']}%)")
                st.write(f"**Qty (Auto):** {trade['Qty']} | **Est Days:** {trade['Est Days']}")
            else:
                st.warning("No strong trade signal (HOLD)")