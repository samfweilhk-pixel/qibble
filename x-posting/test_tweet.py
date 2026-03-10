"""Test tweet: random historical BTC day with regime label + chart."""
import tweepy
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

load_dotenv()

# ── Load data + regimes ──
df = pd.read_parquet("../data/btc_1m.parquet")
df["open_time"] = pd.to_datetime(df["open_time"], utc=True)

# Regime detection (same as dashboard: 30-day rolling return, ±10%, 14-day hold)
daily = df.groupby("date_utc").agg(
    open=("open", "first"), close=("close", "last"),
    high=("high", "max"), low=("low", "min"),
    volume=("volume", "sum"), buy_vol=("buy_vol", "sum"),
    sell_vol=("sell_vol", "sum"), num_trades=("num_trades", "sum"),
).reset_index()
daily["date"] = pd.to_datetime(daily["date_utc"])
daily = daily.sort_values("date").reset_index(drop=True)
daily["roll_ret"] = daily["close"].pct_change(30) * 100

def assign_regimes(daily, threshold=10, min_hold=14):
    regimes = ["CHOP"] * len(daily)
    last_switch = 0
    for i in range(30, len(daily)):
        r = daily["roll_ret"].iloc[i]
        held = i - last_switch
        if r > threshold and held >= min_hold:
            regimes[i] = "BULL"
            if regimes[i-1] != "BULL":
                last_switch = i
        elif r < -threshold and held >= min_hold:
            regimes[i] = "BEAR"
            if regimes[i-1] != "BEAR":
                last_switch = i
        else:
            regimes[i] = regimes[i-1] if held < min_hold else "CHOP"
    return regimes

daily["regime"] = assign_regimes(daily)

# ── Pick a random day ──
np.random.seed()  # truly random
idx = np.random.randint(30, len(daily))  # skip first 30 (no regime yet)
chosen_date = daily["date_utc"].iloc[idx]
regime = daily["regime"].iloc[idx]

day = df[df["date_utc"] == chosen_date].copy().sort_values("open_time").reset_index(drop=True)

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
cum_flow_final = day["cum_flow"].iloc[-1]
if (cum_flow_final > 0 and day_return > 0.5) or (cum_flow_final < 0 and day_return < -0.5):
    alignment = "ALIGNED"
    align_desc = "flow confirmed price"
elif (cum_flow_final > 0 and day_return < -0.5) or (cum_flow_final < 0 and day_return > 0.5):
    alignment = "DIVERGENT"
    align_desc = "flow diverged from price"
else:
    alignment = "NEUTRAL"
    align_desc = "mixed signals"

direction = "buyers" if net_btc > 0 else "sellers"

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

minutes = np.arange(len(day))

# Regime color
regime_colors = {"BULL": "#34d399", "BEAR": "#f87171", "CHOP": "#fbbf24"}
regime_color = regime_colors[regime]

# Top: Price
ax1.plot(minutes, day["close"], color="#60a5fa", linewidth=1.2)
ax1.fill_between(minutes, day["low"], day["high"], alpha=0.08, color="#60a5fa")
ax1.set_ylabel("Price ($)", color="#8b8fa3", fontsize=10)

date_display = pd.to_datetime(chosen_date).strftime("%b %d, %Y")
ax1.set_title(f"BTC — {date_display}  [{regime}]", color="white", fontsize=14, fontweight="bold", pad=12)

# Regime badge
ax1.text(0.02, 0.95, regime, transform=ax1.transAxes, fontsize=11, fontweight="bold",
         va="top", ha="left", color=regime_color,
         bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1d2e", edgecolor=regime_color, alpha=0.9))

# Session shading
for s_range, color, label in [((0, 480), "#fbbf24", "Asia"), ((480, 840), "#34d399", "Europe"), ((840, len(day)), "#f87171", "US")]:
    ax1.axvspan(s_range[0], min(s_range[1], len(day) - 1), alpha=0.06, color=color, label=label)

ax1.legend(loc="upper center", fontsize=8, facecolor="#0f1117", edgecolor="#2a2d3e", labelcolor="white", ncol=3)

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
ax2.plot(minutes, day["cum_flow"], color=flow_color, linewidth=1.2)
ax2.fill_between(minutes, 0, day["cum_flow"], alpha=0.15, color=flow_color)
ax2.axhline(0, color="#2a2d3e", linewidth=0.5)
ax2.set_ylabel("Cum Net Flow (BTC)", color="#8b8fa3", fontsize=10)
ax2.set_xlabel("Minute of Day (UTC)", color="#8b8fa3", fontsize=10)

# Branding
fig.text(0.99, 0.01, "qibble.io — free BTC flow analytics", color="#4a4d5e", fontsize=8, ha="right", style="italic")

plt.tight_layout()
chart_path = "daily_flow_recap.png"
plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor="#0f1117")
plt.close()
print(f"Chart saved: {chart_path}")

# ── Tweet text ──
# Narrative based on what happened
if abs(day_return) < 0.3:
    price_desc = "chopped sideways"
elif day_return > 2:
    price_desc = "ripped higher"
elif day_return > 0.3:
    price_desc = "ground higher"
elif day_return < -2:
    price_desc = "sold off hard"
else:
    price_desc = "drifted lower"

tweet_text = (
    f"BTC on {date_display} [{regime} regime]\n\n"
    f"${close_px:,.0f} ({day_return:+.2f}%) — {price_desc}\n"
    f"Net flow: {net_btc:+,.0f} BTC — {align_desc}\n\n"
    f"Asia {asia_ret:+.2f}% | Europe {europe_ret:+.2f}% | US {us_ret:+.2f}%\n\n"
    f"Explore 5 years of BTC flow data free at qibble.io"
)

print(f"\n--- TWEET ---\n{tweet_text}\n--- END ---\n")
print(f"Characters: {len(tweet_text)}")

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

media = api_v1.media_upload(chart_path)
response = client.create_tweet(text=tweet_text, media_ids=[media.media_id])
print(f"Tweet posted! ID: {response.data['id']}")
print(f"https://x.com/qibbler/status/{response.data['id']}")
