"""Automated X poster for @qibbler — random historical BTC day with chart.

Fetches all data from Binance public API directly. No parquet file needed.
Daily klines (~2 API calls) for regime detection + minute klines (~2 API calls) for the chosen day.
"""
import tweepy
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
import os
import json
import random
import requests
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")

SCRIPT_DIR = Path(__file__).parent
POSTED_LOG = SCRIPT_DIR / "posted_days.json"

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"
HISTORY_START_MS = int(datetime(2021, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)

# ── Load posted days log ──
def load_posted():
    if POSTED_LOG.exists():
        return set(json.loads(POSTED_LOG.read_text()))
    return set()

def save_posted(posted):
    POSTED_LOG.write_text(json.dumps(sorted(posted)))


def fetch_daily_klines():
    """Fetch daily BTCUSDT klines for full history from Binance. ~2 API calls."""
    all_bars = []
    cursor_ms = HISTORY_START_MS
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    while cursor_ms < end_ms:
        params = {
            "symbol": "BTCUSDT", "interval": "1d",
            "startTime": cursor_ms, "endTime": end_ms, "limit": 1000,
        }
        resp = requests.get(BINANCE_URL, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            break
        all_bars.extend(raw)
        cursor_ms = raw[-1][0] + 86_400_000  # next day
        if len(raw) < 1000:
            break
        time.sleep(0.6)

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "num_trades",
            "taker_buy_vol", "taker_buy_quote_vol", "ignore"]
    df = pd.DataFrame(all_bars, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "taker_buy_vol"]:
        df[c] = df[c].astype(float)
    df["date_utc"] = df["open_time"].dt.date.astype(str)
    df["buy_vol"] = df["taker_buy_vol"]
    df["sell_vol"] = df["volume"] - df["taker_buy_vol"]
    df["roll_ret"] = df["close"].pct_change(30) * 100
    return df


def fetch_minute_klines(date_str):
    """Fetch 1-minute BTCUSDT klines for a single UTC day. ~2 API calls."""
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = start_ms + 86_400_000 - 1  # end of day

    all_bars = []
    cursor_ms = start_ms
    while cursor_ms < end_ms:
        params = {
            "symbol": "BTCUSDT", "interval": "1m",
            "startTime": cursor_ms, "endTime": end_ms, "limit": 1000,
        }
        resp = requests.get(BINANCE_URL, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            break
        all_bars.extend(raw)
        cursor_ms = raw[-1][0] + 60_000
        if len(raw) < 1000:
            break
        time.sleep(0.6)

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "num_trades",
            "taker_buy_vol", "taker_buy_quote_vol", "ignore"]
    df = pd.DataFrame(all_bars, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "taker_buy_vol"]:
        df[c] = df[c].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)
    df.drop(columns=["ignore", "close_time", "quote_volume", "taker_buy_quote_vol"], inplace=True)
    df["buy_vol"] = df["taker_buy_vol"]
    df["sell_vol"] = df["volume"] - df["taker_buy_vol"]
    df["net_flow"] = df["buy_vol"] - df["sell_vol"]
    df["date_utc"] = df["open_time"].dt.date.astype(str)
    return df


# ── Fetch daily data from Binance ──
print("Fetching daily klines from Binance...")
daily = fetch_daily_klines()
print(f"  {len(daily)} daily bars, {daily['date_utc'].iloc[0]} → {daily['date_utc'].iloc[-1]}")

# Exclude today (incomplete day)
today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
daily = daily[daily["date_utc"] != today_str].reset_index(drop=True)

def assign_regimes(daily, threshold=10, min_hold=14):
    regimes = ["CHOP"] * len(daily)
    last_switch = 0
    for i in range(30, len(daily)):
        r = daily["roll_ret"].iloc[i]
        held = i - last_switch
        if r > threshold and held >= min_hold:
            regimes[i] = "BULL"
            if regimes[i - 1] != "BULL":
                last_switch = i
        elif r < -threshold and held >= min_hold:
            regimes[i] = "BEAR"
            if regimes[i - 1] != "BEAR":
                last_switch = i
        else:
            regimes[i] = regimes[i - 1] if held < min_hold else "CHOP"
    return regimes

daily["regime"] = assign_regimes(daily)

# ── Pick a random unposted day ──
posted = load_posted()
available = daily.iloc[30:].copy()  # skip first 30 (no regime)
available = available[~available["date_utc"].isin(posted)]

if len(available) == 0:
    print("All days posted! Resetting log.")
    posted = set()
    save_posted(posted)
    available = daily.iloc[30:].copy()

row = available.sample(1).iloc[0]
chosen_date = row["date_utc"]
regime = row["regime"]

# ── Fetch minute data for chosen day ──
print(f"Fetching minute bars for {chosen_date}...")
day = fetch_minute_klines(chosen_date)
print(f"  {len(day)} minute bars")

# ── Compute stats ──
open_px = day["open"].iloc[0]
close_px = day["close"].iloc[-1]
high_px = day["high"].max()
low_px = day["low"].min()
day_return = (close_px / open_px - 1) * 100
total_vol = day["volume"].sum()
total_buy = day["buy_vol"].sum()
total_sell = day["sell_vol"].sum()
net_btc = total_buy - total_sell
day["cum_flow"] = (day["buy_vol"] - day["sell_vol"]).cumsum()
day["hour"] = day["open_time"].dt.hour
cum_flow_final = day["cum_flow"].iloc[-1]


def session_return(s):
    if len(s) == 0:
        return 0.0
    return (s["close"].iloc[-1] / s["open"].iloc[0] - 1) * 100


asia = day[day["hour"].between(0, 7)]
europe = day[day["hour"].between(8, 13)]
us = day[day["hour"].between(14, 23)]
asia_ret = session_return(asia)
europe_ret = session_return(europe)
us_ret = session_return(us)

# Flow-price alignment
if (cum_flow_final > 0 and day_return > 0.5) or (cum_flow_final < 0 and day_return < -0.5):
    alignment, align_desc = "ALIGNED", "flow confirmed price"
elif (cum_flow_final > 0 and day_return < -0.5) or (cum_flow_final < 0 and day_return > 0.5):
    alignment, align_desc = "DIVERGENT", "flow diverged from price"
else:
    alignment, align_desc = "NEUTRAL", "mixed signals"

direction = "buyers" if net_btc > 0 else "sellers"

# Price description
if abs(day_return) < 0.3:
    price_desc = "chopped sideways"
elif day_return > 5:
    price_desc = "exploded higher"
elif day_return > 2:
    price_desc = "ripped higher"
elif day_return > 0.3:
    price_desc = "ground higher"
elif day_return < -5:
    price_desc = "crashed"
elif day_return < -2:
    price_desc = "sold off hard"
else:
    price_desc = "drifted lower"

# Best/worst session
sessions = {"Asia": asia_ret, "Europe": europe_ret, "US": us_ret}
best_sess = max(sessions, key=sessions.get)
worst_sess = min(sessions, key=sessions.get)

date_display = pd.to_datetime(chosen_date).strftime("%b %d, %Y")

# ── Tweet templates ──
# Each tweet links to the specific day's blog page
day_url = f"qibble.io/btc-flow/{chosen_date}/"

templates = [
    # Standard recap
    (
        f"BTC on {date_display} [{regime} regime]\n\n"
        f"${close_px:,.0f} ({day_return:+.2f}%) — {price_desc}\n"
        f"Net flow: {net_btc:+,.0f} BTC — {align_desc}\n\n"
        f"Asia {asia_ret:+.2f}% | Europe {europe_ret:+.2f}% | US {us_ret:+.2f}%\n\n"
        f"Full flow analysis: {day_url}"
    ),
    # Question hook
    (
        f"What did BTC flow look like on {date_display}?\n\n"
        f"[{regime}] ${close_px:,.0f} ({day_return:+.2f}%)\n"
        f"{net_btc:+,.0f} BTC net flow — {align_desc}\n"
        f"{best_sess} led at {sessions[best_sess]:+.2f}%\n\n"
        f"Charts + session breakdown: {day_url}"
    ),
    # Session spotlight
    (
        f"{date_display} — {best_sess} session dominated\n\n"
        f"[{regime}] BTC {price_desc} to ${close_px:,.0f}\n"
        f"Asia {asia_ret:+.2f}% | Europe {europe_ret:+.2f}% | US {us_ret:+.2f}%\n"
        f"Net flow: {net_btc:+,.0f} BTC ({direction})\n\n"
        f"Minute-level breakdown: {day_url}"
    ),
    # Flow-first
    (
        f"{net_btc:+,.0f} BTC net flow on {date_display}\n\n"
        f"[{regime}] {direction.title()} pushed ${close_px:,.0f} ({day_return:+.2f}%)\n"
        f"{align_desc.capitalize()}\n"
        f"Volume: {total_vol:,.0f} BTC\n\n"
        f"Full analysis: {day_url}"
    ),
    # Regime-first
    (
        f"BTC in a {regime} regime — {date_display}\n\n"
        f"${close_px:,.0f} ({day_return:+.2f}%) — {price_desc}\n"
        f"Flow: {net_btc:+,.0f} BTC | Vol: {total_vol:,.0f} BTC\n"
        f"{align_desc.capitalize()}\n\n"
        f"Whale activity + flow charts: {day_url}"
    ),
    # Throwback
    (
        f"Throwback: {date_display}\n\n"
        f"BTC at ${close_px:,.0f} [{regime}]\n"
        f"{price_desc.capitalize()} — {day_return:+.2f}%\n"
        f"{direction.title()} dominated with {net_btc:+,.0f} BTC net flow\n\n"
        f"See the full day: {day_url}"
    ),
    # Data nerd
    (
        f"{date_display} [{regime}]\n\n"
        f"O: ${open_px:,.0f} H: ${high_px:,.0f} L: ${low_px:,.0f} C: ${close_px:,.0f}\n"
        f"Net flow: {net_btc:+,.0f} BTC\n"
        f"Vol: {total_vol:,.0f} BTC | Trades: {day['num_trades'].sum():,.0f}\n"
        f"Flow-price: {alignment}\n\n"
        f"Minute-level data: {day_url}"
    ),
    # Divergence/alignment highlight
    (
        f"Flow {alignment.lower()} on {date_display}\n\n"
        f"[{regime}] BTC {price_desc} ({day_return:+.2f}%)\n"
        f"while {direction} pushed {abs(net_btc):,.0f} BTC\n"
        f"{worst_sess} weakest at {sessions[worst_sess]:+.2f}%\n\n"
        f"Flow analysis + charts: {day_url}"
    ),
]

tweet_text = random.choice(templates)

# Truncate if needed
if len(tweet_text) > 280:
    tweet_text = tweet_text[:277] + "..."

# ── Chart ──
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6.75), gridspec_kw={"height_ratios": [2, 1]})
fig.patch.set_facecolor("#0f1117")

for ax in [ax1, ax2]:
    ax.set_facecolor("#0f1117")
    ax.tick_params(colors="#8b8fa3", labelsize=9)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color("#2a2d3e")

# Use actual timestamps for x-axis
times = day["open_time"]

regime_colors = {"BULL": "#34d399", "BEAR": "#f87171", "CHOP": "#fbbf24"}
regime_color = regime_colors[regime]

# Top: Price
ax1.plot(times, day["close"], color="#60a5fa", linewidth=1.2)
ax1.fill_between(times, day["low"], day["high"], alpha=0.08, color="#60a5fa")
ax1.set_ylabel("Price ($)", color="#8b8fa3", fontsize=10)
ax1.set_title(f"BTC — {date_display}  [{regime}]", color="white", fontsize=14, fontweight="bold", pad=12)

# Regime badge
ax1.text(0.02, 0.95, f"30d: {regime}", transform=ax1.transAxes, fontsize=11, fontweight="bold",
         va="top", ha="left", color=regime_color,
         bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1d2e", edgecolor=regime_color, alpha=0.9))

# Session shading using actual times
day_start = times.iloc[0].normalize()
import pytz
utc = pytz.UTC

asia_start = day_start.replace(hour=0, tzinfo=utc)
asia_end = day_start.replace(hour=8, tzinfo=utc)
europe_end = day_start.replace(hour=14, tzinfo=utc)
us_end = day_start.replace(hour=23, minute=59, tzinfo=utc)

ax1.axvspan(asia_start, asia_end, alpha=0.06, color="#fbbf24", label="Asia (00-08)")
ax1.axvspan(asia_end, europe_end, alpha=0.06, color="#34d399", label="Europe (08-14)")
ax1.axvspan(europe_end, us_end, alpha=0.06, color="#f87171", label="US (14-00)")

ax1.legend(loc="upper center", fontsize=8, facecolor="#0f1117", edgecolor="#2a2d3e", labelcolor="white", ncol=3)

# Format x-axis as UTC times
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=utc))
ax1.xaxis.set_major_locator(mdates.HourLocator(interval=4))

# Stats box
stats_text = (
    f"Open ${open_px:,.0f} -> Close ${close_px:,.0f}  ({day_return:+.2f}%)\n"
    f"Range: ${low_px:,.0f} - ${high_px:,.0f}\n"
    f"Net Flow: {net_btc:+,.0f} BTC ({direction})\n"
    f"Volume: {total_vol:,.0f} BTC\n"
    f"Flow-Price: {alignment}"
)
ax1.text(0.98, 0.95, stats_text, transform=ax1.transAxes, fontsize=8.5, va="top", ha="right",
         color="white", fontfamily="monospace",
         bbox=dict(boxstyle="round,pad=0.5", facecolor="#1a1d2e", edgecolor="#2a2d3e", alpha=0.9))

# Bottom: Cumulative flow
flow_color = "#34d399" if cum_flow_final > 0 else "#f87171"
ax2.plot(times, day["cum_flow"], color=flow_color, linewidth=1.2)
ax2.fill_between(times, 0, day["cum_flow"], alpha=0.15, color=flow_color)
ax2.axhline(0, color="#2a2d3e", linewidth=0.5)
ax2.set_ylabel("Cum Net Flow (BTC)", color="#8b8fa3", fontsize=10)
ax2.set_xlabel("Time (UTC)", color="#8b8fa3", fontsize=10)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=utc))
ax2.xaxis.set_major_locator(mdates.HourLocator(interval=4))

# Branding
fig.text(0.99, 0.01, "qibble.io — free BTC flow analytics", color="#4a4d5e", fontsize=8, ha="right", style="italic")

plt.tight_layout()
chart_path = SCRIPT_DIR / "daily_flow_recap.png"
plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor="#0f1117")
plt.close()

# ── Post ──
client = tweepy.Client(
    consumer_key=os.getenv("X_API_KEY"),
    consumer_secret=os.getenv("X_API_SECRET"),
    access_token=os.getenv("X_ACCESS_TOKEN"),
    access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET"),
)

auth = tweepy.OAuth1UserHandler(
    os.getenv("X_API_KEY"),
    os.getenv("X_API_SECRET"),
    os.getenv("X_ACCESS_TOKEN"),
    os.getenv("X_ACCESS_TOKEN_SECRET"),
)
api_v1 = tweepy.API(auth)

media = api_v1.media_upload(str(chart_path))
response = client.create_tweet(text=tweet_text, media_ids=[media.media_id])

# Log posted day
posted.add(chosen_date)
save_posted(posted)

tweet_id = response.data["id"]
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"[{now}] Posted: {chosen_date} [{regime}] — https://x.com/qibbler/status/{tweet_id}")
print(f"Template used, {len(tweet_text)} chars")
print(f"Days posted: {len(posted)} / {len(daily) - 30}")
