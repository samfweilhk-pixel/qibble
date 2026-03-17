"""
Blog page generator for qibble.io/btc-flow/YYYY-MM-DD
=====================================================
Fetches BTC data from Binance, detects patterns, generates narratives,
renders charts, and outputs static HTML pages.

Usage:
    python generate_daily.py                              # Generate 5 sample days
    python generate_daily.py 2024-11-05                   # Generate specific date(s)
    python generate_daily.py --all                        # Generate all days
    python generate_daily.py --parquet PATH               # Use local parquet instead of Binance
    python generate_daily.py --parquet PATH --phase1      # Phase 1: only days with 4+ patterns
    python generate_daily.py --parquet PATH --phase2      # Phase 2: days with 3+ patterns
    python generate_daily.py --parquet PATH --min-patterns 4  # Custom minimum pattern count
    python generate_daily.py --parquet PATH --top 100     # Top N most interesting days

Phased rollout (recommended for SEO):
    Phase 1: --phase1           → ~200-400 pages (4+ patterns — crashes, whales, divergences)
    Phase 2: --phase2           → ~600-800 pages (3+ patterns)
    Phase 3: --all              → all ~1,840 pages
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz
import os
import sys
import json
import requests
import time as time_module
from datetime import datetime, timezone, timedelta
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

UTC = pytz.UTC
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR.parent / "blog-static" / "btc-flow"
TEMPLATE_DIR = SCRIPT_DIR / "templates"
BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"
HISTORY_START_MS = int(datetime(2021, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)


# ── Data Loading ─────────────────────────────────────────────────────────

def fetch_daily_klines():
    """Fetch daily BTCUSDT klines from Binance."""
    all_bars = []
    cursor_ms = HISTORY_START_MS
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    while cursor_ms < end_ms:
        params = {"symbol": "BTCUSDT", "interval": "1d",
                  "startTime": cursor_ms, "endTime": end_ms, "limit": 1000}
        resp = requests.get(BINANCE_URL, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            break
        all_bars.extend(raw)
        cursor_ms = raw[-1][0] + 86_400_000
        if len(raw) < 1000:
            break
        time_module.sleep(0.6)

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
    df["net_flow"] = df["buy_vol"] - df["sell_vol"]
    df["roll_ret"] = df["close"].pct_change(30) * 100
    return df


def fetch_minute_klines(date_str):
    """Fetch 1-minute BTCUSDT klines for a single UTC day."""
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = start_ms + 86_400_000 - 1
    all_bars = []
    cursor_ms = start_ms
    while cursor_ms < end_ms:
        params = {"symbol": "BTCUSDT", "interval": "1m",
                  "startTime": cursor_ms, "endTime": end_ms, "limit": 1000}
        resp = requests.get(BINANCE_URL, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            break
        all_bars.extend(raw)
        cursor_ms = raw[-1][0] + 60_000
        if len(raw) < 1000:
            break
        time_module.sleep(0.6)

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "num_trades",
            "taker_buy_vol", "taker_buy_quote_vol", "ignore"]
    df = pd.DataFrame(all_bars, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "taker_buy_vol", "quote_volume"]:
        df[c] = df[c].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)
    df.drop(columns=["ignore", "close_time", "taker_buy_quote_vol"], inplace=True)
    df["buy_vol"] = df["taker_buy_vol"]
    df["sell_vol"] = df["volume"] - df["taker_buy_vol"]
    df["net_flow"] = df["buy_vol"] - df["sell_vol"]
    df["bar_imbalance"] = np.where(df["volume"] > 0, df["net_flow"] / df["volume"], 0.0)
    df["avg_trade_size"] = np.where(df["num_trades"] > 0, df["volume"] / df["num_trades"], 0.0)
    df["pct_return"] = (df["close"] / df["open"] - 1) * 100
    df["hour"] = df["open_time"].dt.hour
    return df


def assign_regimes(daily, threshold=10, min_hold=14):
    """Same regime detection as main backend."""
    regimes = ["CHOP"] * len(daily)
    last_switch = 0
    for i in range(30, len(daily)):
        r = daily["roll_ret"].iloc[i]
        held = i - last_switch
        if pd.isna(r):
            regimes[i] = regimes[i - 1]
            continue
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


def load_parquet(path):
    """Load from local parquet file instead of Binance."""
    df = pd.read_parquet(path)
    # Build daily from minute bars
    daily = df.groupby("date_utc").agg(
        open=("open", "first"), close=("close", "last"),
        high=("high", "max"), low=("low", "min"),
        volume=("volume", "sum"),
        taker_buy_vol=("buy_vol", "sum"),
    ).reset_index()
    daily["open_time"] = pd.to_datetime(daily["date_utc"])
    daily["date_utc"] = daily["date_utc"].astype(str)
    daily["buy_vol"] = daily["taker_buy_vol"]
    daily["sell_vol"] = daily["volume"] - daily["taker_buy_vol"]
    daily["net_flow"] = daily["buy_vol"] - daily["sell_vol"]
    daily.sort_values("date_utc", inplace=True)
    daily.reset_index(drop=True, inplace=True)
    daily["roll_ret"] = daily["close"].pct_change(30) * 100
    return df, daily


# ── Regime Context ───────────────────────────────────────────────────────

def build_regime_context(daily):
    """Pre-compute regime stats for contextual comparisons."""
    daily = daily.copy()
    daily["regime"] = assign_regimes(daily)
    daily["return_pct"] = (daily["close"] / daily["open"] - 1) * 100

    # Find regime periods (contiguous stretches)
    daily["regime_group"] = (daily["regime"] != daily["regime"].shift(1)).cumsum()
    periods = []
    for gid, grp in daily.groupby("regime_group"):
        periods.append({
            "regime": grp["regime"].iloc[0],
            "start": grp["date_utc"].iloc[0],
            "end": grp["date_utc"].iloc[-1],
            "n_days": len(grp),
        })

    # Regime averages
    regime_stats = {}
    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = daily[daily["regime"] == regime]
        if len(rdf) == 0:
            continue
        regime_stats[regime] = {
            "avg_return": float(rdf["return_pct"].mean()),
            "std_return": float(rdf["return_pct"].std()),
            "avg_net_flow": float(rdf["net_flow"].mean()),
            "std_net_flow": float(rdf["net_flow"].std()),
            "avg_volume": float(rdf["volume"].mean()),
            "std_volume": float(rdf["volume"].std()),
            "n_days": len(rdf),
        }

    regime_map = dict(zip(daily["date_utc"], daily["regime"]))
    return regime_map, regime_stats, periods, daily


# ── Pattern Detection ────────────────────────────────────────────────────

def compute_day_features(bars, daily_row, regime, regime_stats, regime_periods, daily_df):
    """Compute all features needed for pattern detection from minute bars."""
    features = {}

    # Basic stats
    open_px = bars["open"].iloc[0]
    close_px = bars["close"].iloc[-1]
    high_px = bars["high"].max()
    low_px = bars["low"].min()
    day_return = (close_px / open_px - 1) * 100
    total_vol = bars["volume"].sum()
    total_buy = bars["buy_vol"].sum()
    total_sell = bars["sell_vol"].sum()
    net_flow = total_buy - total_sell
    num_trades = bars["num_trades"].sum()
    avg_trade_size = total_vol / num_trades if num_trades > 0 else 0
    buy_sell_ratio = total_buy / total_sell if total_sell > 0 else float("inf")

    features["open"] = open_px
    features["close"] = close_px
    features["high"] = high_px
    features["low"] = low_px
    features["return_pct"] = day_return
    features["total_vol"] = total_vol
    features["total_buy"] = total_buy
    features["total_sell"] = total_sell
    features["net_flow"] = net_flow
    features["num_trades"] = num_trades
    features["avg_trade_size"] = avg_trade_size
    features["buy_sell_ratio"] = buy_sell_ratio
    features["range_pct"] = (high_px - low_px) / open_px * 100

    # Session decomposition
    asia = bars[bars["hour"].between(0, 7)]
    europe = bars[bars["hour"].between(8, 13)]
    us = bars[bars["hour"].between(14, 23)]

    def sess_stats(s, name):
        if len(s) == 0:
            return {"return": 0.0, "net_flow": 0.0, "volume": 0.0, "name": name}
        return {
            "return": (s["close"].iloc[-1] / s["open"].iloc[0] - 1) * 100,
            "net_flow": s["net_flow"].sum(),
            "volume": s["volume"].sum(),
            "name": name,
        }

    features["sessions"] = {
        "Asia": sess_stats(asia, "Asia"),
        "Europe": sess_stats(europe, "Europe"),
        "US": sess_stats(us, "US"),
    }

    # Which session dominated flow
    total_abs_flow = sum(abs(s["net_flow"]) for s in features["sessions"].values())
    if total_abs_flow > 0:
        for name, s in features["sessions"].items():
            s["flow_share"] = abs(s["net_flow"]) / total_abs_flow
    else:
        for s in features["sessions"].values():
            s["flow_share"] = 1 / 3

    # CVD trajectory
    bars = bars.copy()
    bars["cum_flow"] = bars["net_flow"].cumsum()
    first_px = bars["close"].iloc[0]
    bars["cum_return"] = ((bars["close"] / first_px) - 1) * 100 if first_px > 0 else 0.0
    features["cum_flow_final"] = float(bars["cum_flow"].iloc[-1])

    # CVD-price correlation (full day)
    if len(bars) > 20:
        cvd_price_corr = bars["cum_flow"].corr(bars["cum_return"])
        features["cvd_price_corr"] = float(cvd_price_corr) if not pd.isna(cvd_price_corr) else 0.0
    else:
        features["cvd_price_corr"] = 0.0

    # Whale detection: bars where avg_trade_size > 3σ above day mean
    # Use trailing computation: each bar's z-score vs all prior bars that day
    bars["ats_expanding_mean"] = bars["avg_trade_size"].expanding(min_periods=10).mean().shift(1)
    bars["ats_expanding_std"] = bars["avg_trade_size"].expanding(min_periods=10).std().shift(1)
    bars["ats_z"] = np.where(
        bars["ats_expanding_std"] > 0,
        (bars["avg_trade_size"] - bars["ats_expanding_mean"]) / bars["ats_expanding_std"],
        0.0,
    )
    whale_bars = bars[bars["ats_z"] > 3].copy()
    features["whale_bars"] = []
    if len(whale_bars) > 0:
        for _, wb in whale_bars.iterrows():
            features["whale_bars"].append({
                "time": wb["open_time"].strftime("%H:%M"),
                "z_score": float(wb["ats_z"]),
                "net_flow": float(wb["net_flow"]),
                "avg_trade_size": float(wb["avg_trade_size"]),
            })

    # Peak rolling imbalance
    bars["rolling_imb_20"] = bars["bar_imbalance"].rolling(20, min_periods=1).mean()
    peak_buy_imb = bars["rolling_imb_20"].max()
    peak_sell_imb = bars["rolling_imb_20"].min()
    peak_buy_time = bars.loc[bars["rolling_imb_20"].idxmax(), "open_time"].strftime("%H:%M")
    peak_sell_time = bars.loc[bars["rolling_imb_20"].idxmin(), "open_time"].strftime("%H:%M")
    features["peak_buy_imb"] = {"value": float(peak_buy_imb), "time": peak_buy_time}
    features["peak_sell_imb"] = {"value": float(peak_sell_imb), "time": peak_sell_time}

    # Intraday flow reversal (first half vs second half)
    mid = len(bars) // 2
    first_half_flow = bars.iloc[:mid]["net_flow"].sum()
    second_half_flow = bars.iloc[mid:]["net_flow"].sum()
    features["first_half_flow"] = float(first_half_flow)
    features["second_half_flow"] = float(second_half_flow)
    features["flow_reversed"] = (first_half_flow > 0) != (second_half_flow > 0)

    # Regime context
    features["regime"] = regime
    rs = regime_stats.get(regime, {})
    if rs:
        flow_std = rs.get("std_net_flow", 1)
        features["flow_z_vs_regime"] = net_flow / flow_std if flow_std > 0 else 0
        ret_std = rs.get("std_return", 1)
        features["return_z_vs_regime"] = (day_return - rs["avg_return"]) / ret_std if ret_std > 0 else 0
        vol_std = rs.get("std_volume", 1)
        features["vol_z_vs_regime"] = (total_vol - rs["avg_volume"]) / vol_std if vol_std > 0 else 0
        features["regime_avg_return"] = rs["avg_return"]
        features["regime_avg_flow"] = rs["avg_net_flow"]
    else:
        features["flow_z_vs_regime"] = 0
        features["return_z_vs_regime"] = 0
        features["vol_z_vs_regime"] = 0
        features["regime_avg_return"] = 0
        features["regime_avg_flow"] = 0

    # Position within regime period
    date_str = daily_row["date_utc"]
    for p in regime_periods:
        if p["start"] <= date_str <= p["end"]:
            # Days into regime
            start_dt = datetime.strptime(p["start"], "%Y-%m-%d")
            current_dt = datetime.strptime(date_str, "%Y-%m-%d")
            end_dt = datetime.strptime(p["end"], "%Y-%m-%d")
            features["days_into_regime"] = (current_dt - start_dt).days + 1
            features["days_until_regime_end"] = (end_dt - current_dt).days
            features["regime_period_length"] = p["n_days"]
            # Previous/next regime
            idx = regime_periods.index(p)
            features["prev_regime"] = regime_periods[idx - 1]["regime"] if idx > 0 else None
            features["next_regime"] = regime_periods[idx + 1]["regime"] if idx < len(regime_periods) - 1 else None
            break
    else:
        features["days_into_regime"] = None
        features["days_until_regime_end"] = None
        features["regime_period_length"] = None
        features["prev_regime"] = None
        features["next_regime"] = None

    # Consecutive flow direction streak
    date_idx = daily_df[daily_df["date_utc"] == date_str].index
    if len(date_idx) > 0:
        idx = date_idx[0]
        streak = 1
        direction = 1 if net_flow > 0 else -1
        for j in range(idx - 1, max(idx - 30, -1), -1):
            prev_flow = daily_df.iloc[j]["net_flow"]
            if (prev_flow > 0 and direction > 0) or (prev_flow < 0 and direction < 0):
                streak += 1
            else:
                break
        features["flow_streak"] = streak * direction
    else:
        features["flow_streak"] = 0

    # Store bars for chart generation
    features["_bars"] = bars
    features["_date_str"] = daily_row["date_utc"]

    return features


# ── Narrative Generation ─────────────────────────────────────────────────

def _pick(variants, date_str):
    """Deterministic variant selection seeded by date string.
    Different patterns use different offsets so two patterns on the same day
    don't always pick variant[0] together."""
    h = hash(date_str)
    return variants[abs(h) % len(variants)]


def _pick_offset(variants, date_str, offset, extra=0):
    """Like _pick but with an offset so co-occurring patterns get different slots.
    extra: additional numeric value (e.g. streak count, whale count) to further
    differentiate pages that trigger the same pattern with different magnitudes."""
    h = hash(date_str) + offset + extra
    return variants[abs(h) % len(variants)]


def detect_patterns(f):
    """Detect notable patterns from features, return list of (priority, narrative, tag)."""
    patterns = []
    date_str = f.get("_date_str", "2024-01-01")

    regime = f["regime"]
    net_flow = f["net_flow"]
    day_return = f["return_pct"]
    direction = "buying" if net_flow > 0 else "selling"
    opp_direction = "selling" if net_flow > 0 else "buying"
    flow_btc = f"{abs(net_flow):,.0f}"
    regime_lower = regime.lower()

    # ── 1. Extreme flow intensity (vs regime) ──
    flow_z = f["flow_z_vs_regime"]
    if abs(flow_z) > 2:
        variants = [
            f"At {flow_z:+.1f}σ from the {regime_lower}-regime average, this was one of the "
            f"most intense {direction} days in the period — {flow_btc} BTC of net pressure "
            f"overwhelmed the typical flow pattern.",

            f"Net {direction} hit {flow_btc} BTC, landing {flow_z:+.1f}σ outside the "
            f"{regime_lower}-regime norm. Days with this level of one-sided flow have "
            f"historically marked inflection points within the regime.",

            f"The {flow_btc} BTC of net {direction} was extreme by any measure "
            f"({flow_z:+.1f}σ vs {regime_lower}-regime average). The market's typical "
            f"flow balance broke down.",

            f"Flow conviction was unusually strong: {flow_btc} BTC net {direction}, "
            f"registering at {flow_z:+.1f}σ relative to other {regime_lower}-regime days. "
            f"This wasn't noise — it was a directional statement.",

            f"Among the heaviest {direction} days in this {regime_lower} regime. "
            f"Net flow of {net_flow:+,.0f} BTC placed this day at {flow_z:+.1f}σ from "
            f"the regime mean.",
        ]
        patterns.append((1, _pick(variants, date_str)))
    elif abs(flow_z) > 1.5:
        variants = [
            f"Net flow of {net_flow:+,.0f} BTC was elevated at {flow_z:+.1f}σ "
            f"vs the {regime_lower}-regime average of {f['regime_avg_flow']:+,.0f} BTC.",

            f"With {net_flow:+,.0f} BTC of net flow ({flow_z:+.1f}σ), {direction} pressure "
            f"ran above the {regime_lower}-regime baseline of {f['regime_avg_flow']:+,.0f} BTC.",

            f"A {flow_z:+.1f}σ day — {direction} pressure exceeded the typical {regime_lower}-regime "
            f"flow ({f['regime_avg_flow']:+,.0f} BTC) by a notable margin.",
        ]
        patterns.append((3, _pick(variants, date_str)))

    # ── 2. Session dominance ──
    sessions = f["sessions"]
    dominant_sess = max(sessions.values(), key=lambda s: s["flow_share"])
    if dominant_sess["flow_share"] > 0.6:
        sn = dominant_sess["name"]
        sf = abs(dominant_sess["net_flow"])
        share = dominant_sess["flow_share"]
        sd = "buying" if dominant_sess["net_flow"] > 0 else "selling"
        variants = [
            f"The {sn} session drove {share:.0%} of the day's directional flow, "
            f"with {sf:,.0f} BTC of net {sd}. The other two sessions were "
            f"comparatively quiet.",

            f"Most of the action happened during {sn} hours, which accounted for "
            f"{share:.0%} of the day's net flow ({sf:,.0f} BTC {sd}). "
            f"The rest of the day was a footnote.",

            f"{sn} traders set the tone. {share:.0%} of directional flow — "
            f"{sf:,.0f} BTC of {sd} — came from that single session.",

            f"Strip out {sn} and the day would look flat. That session alone "
            f"contributed {share:.0%} of the net flow ({sf:,.0f} BTC {sd}).",
        ]
        patterns.append((2, _pick_offset(variants, date_str, 7)))

    # Session return breakdown (always include)
    sess_parts = []
    for name in ["Asia", "Europe", "US"]:
        s = sessions[name]
        sess_parts.append(f"{name} {s['return']:+.2f}%")
    sess_leader = max(sessions.values(), key=lambda s: abs(s["return"]))
    sess_laggard = min(sessions.values(), key=lambda s: s["return"])

    if abs(sess_leader["return"]) > 0.5:
        variants = [
            f"Session returns: {', '.join(sess_parts)}. "
            f"{sess_leader['name']} led the move.",

            f"Across sessions: {', '.join(sess_parts)} — "
            f"with {sess_leader['name']} doing the heavy lifting.",

            f"The {sess_leader['name']} session posted the largest move. "
            f"Full breakdown: {', '.join(sess_parts)}.",

            f"Breaking it down by session: {', '.join(sess_parts)}. "
            f"{sess_leader['name']} stood out.",
        ]
        patterns.append((4, _pick_offset(variants, date_str, 13)))
    else:
        variants = [
            f"Session returns were muted across the board: {', '.join(sess_parts)}.",

            f"No single session dominated price action: {', '.join(sess_parts)}.",

            f"All three sessions posted small moves: {', '.join(sess_parts)} — "
            f"a balanced day with no clear regional driver.",
        ]
        patterns.append((4, _pick_offset(variants, date_str, 13)))

    # ── 3. CVD-price divergence ──
    corr = f["cvd_price_corr"]
    if corr < -0.3:
        if day_return > 0.3:
            variants = [
                f"Aggressor-side sellers pushed flow negative throughout the day "
                f"(flow-price correlation: {corr:.2f}), yet price finished up "
                f"{day_return:+.2f}%. Passive buyers on the bid absorbed the "
                f"selling without showing up in aggressor-side data.",

                f"A textbook absorption day. Sellers hit the bid aggressively "
                f"(correlation between cumulative flow and price: {corr:.2f}), "
                f"but price still climbed {day_return:+.2f}%. The buying was "
                f"passive — limit orders soaking up the supply.",

                f"Flow said sell, price said buy. The {corr:.2f} correlation "
                f"between cumulative flow and return tells the story: someone "
                f"was absorbing the selling quietly while price rose {day_return:+.2f}%.",

                f"Despite persistent sell-side aggression (flow-price correlation {corr:.2f}), "
                f"price rose {day_return:+.2f}%. This divergence typically signals "
                f"strong passive demand — buyers who don't need to cross the spread.",
            ]
            patterns.append((1, _pick_offset(variants, date_str, 3)))
        elif day_return < -0.3:
            variants = [
                f"Buyers were the aggressors but couldn't lift price, which fell "
                f"{day_return:+.2f}% despite net buying pressure (flow-price "
                f"correlation: {corr:.2f}). Overhead supply absorbed every push higher.",

                f"A frustrating day for buyers. Cumulative flow leaned positive but "
                f"price declined {day_return:+.2f}% (correlation: {corr:.2f}). "
                f"Passive selling at higher prices kept rejecting the advances.",

                f"Flow-price divergence: aggressive buying met a wall. Despite "
                f"bid-side pressure, price dropped {day_return:+.2f}% "
                f"(correlation: {corr:.2f}). The sellers were patient, the "
                f"buyers were not.",

                f"The {corr:.2f} flow-price correlation reveals a mismatch — "
                f"buyers crossed the spread aggressively, but price still fell "
                f"{day_return:+.2f}%. Passive supply overwhelmed active demand.",
            ]
            patterns.append((1, _pick_offset(variants, date_str, 3)))
        else:
            variants = [
                f"Flow and price told different stories (correlation: {corr:.2f}). "
                f"Aggressor-side flow leaned one way but price barely moved — "
                f"the opposing side matched it passively.",

                f"A disconnect between flow and price (correlation: {corr:.2f}). "
                f"Neither buyers nor sellers could convert aggressor-side "
                f"pressure into directional movement.",
            ]
            patterns.append((2, _pick_offset(variants, date_str, 3)))
    elif corr > 0.8:
        variants = [
            f"Flow and price moved in lockstep (correlation: {corr:.2f}). "
            f"Aggressor-side {direction} translated directly into price movement "
            f"with minimal resistance.",

            f"A clean, flow-driven day. The {corr:.2f} correlation between "
            f"cumulative flow and price means {direction} pressure was the "
            f"dominant force — no hidden passive flow muddying the signal.",

            f"Price followed flow faithfully (correlation: {corr:.2f}). When "
            f"you see this level of alignment, it means one side is in control "
            f"and the other isn't even fighting.",
        ]
        patterns.append((5, _pick_offset(variants, date_str, 3)))

    # ── 4. Whale activity ──
    whales = f["whale_bars"]
    if len(whales) >= 3:
        whale_flow = sum(w["net_flow"] for w in whales)
        whale_dir = "buying" if whale_flow > 0 else "selling"
        whale_opp = "selling" if whale_flow > 0 else "buying"
        whale_times = [w["time"] for w in whales[:3]]
        max_z = max(w["z_score"] for w in whales)
        n_whales = len(whales)
        whale_btc = f"{abs(whale_flow):,.0f}"
        # Check if whale flow aligned with or opposed the day's direction
        whale_aligned = (whale_flow > 0) == (net_flow > 0)
        first_time = whales[0]["time"]
        last_time = whales[-1]["time"]
        spread = f"{first_time}–{last_time}"

        # Cross-pattern: whales + divergence
        if corr < -0.3 and not whale_aligned:
            variants = [
                f"Large players were on the wrong side — or the patient side. "
                f"{n_whales} bars of outsized trade sizes (peak {max_z:.1f}σ) "
                f"showed net {whale_dir}, but price moved against them. "
                f"Either they were accumulating into weakness or got caught.",

                f"Whale-sized trades ({n_whales} bars above 3σ, peak {max_z:.1f}σ) "
                f"pushed {whale_btc} BTC of net {whale_dir} between {spread} UTC — "
                f"while price went the other way. Possible accumulation, "
                f"possible pain.",
            ]
            patterns.append((1, _pick_offset(variants, date_str, 5, extra=n_whales)))
        elif whale_aligned:
            # Whales aligned with day direction — concentrated or spread out?
            if n_whales > 10:
                variants = [
                    f"Sustained large-player {whale_dir} throughout the day. "
                    f"{n_whales} bars with trade sizes 3σ+ above normal "
                    f"(peak: {max_z:.1f}σ), spread across {spread} UTC. "
                    f"The {whale_btc} BTC of whale flow reinforced the day's "
                    f"{direction} bias.",

                    f"The big players were active all day. {n_whales} bars of "
                    f"outsized trades ({max_z:.1f}σ peak) from {spread} UTC, "
                    f"adding {whale_btc} BTC of {whale_dir} pressure on top "
                    f"of the broader {direction} flow.",
                ]
            else:
                variants = [
                    f"A cluster of large trades between {spread} UTC — "
                    f"{n_whales} bars with trade sizes spiking to {max_z:.1f}σ "
                    f"above normal. Net {whale_dir} of {whale_btc} BTC, "
                    f"aligned with the day's direction.",

                    f"Whale activity concentrated around {first_time} UTC: "
                    f"{n_whales} bars with average trade sizes {max_z:.1f}σ above "
                    f"baseline. The {whale_btc} BTC of net {whale_dir} added "
                    f"conviction to the {direction} move.",

                    f"Someone was moving size. {n_whales} bars between "
                    f"{spread} UTC registered trade sizes 3σ+ above normal "
                    f"(peak: {max_z:.1f}σ), pushing {whale_btc} BTC net {whale_dir}.",
                ]
            patterns.append((1, _pick_offset(variants, date_str, 5, extra=n_whales)))
        else:
            variants = [
                f"Large-player activity detected: {n_whales} bars with trade "
                f"sizes 3σ+ above normal (peak: {max_z:.1f}σ) between "
                f"{spread} UTC. Whale flow netted {whale_btc} BTC of {whale_dir}.",

                f"{n_whales} bars of outsized trades (peak {max_z:.1f}σ) "
                f"appeared between {spread} UTC, with whale flow netting "
                f"{whale_btc} BTC of {whale_dir}.",
            ]
            patterns.append((1, _pick_offset(variants, date_str, 5, extra=n_whales)))

    elif len(whales) == 1 or len(whales) == 2:
        whale_flow = sum(w["net_flow"] for w in whales)
        whale_dir = "buying" if whale_flow > 0 else "selling"
        wt = whales[0]["time"]
        wz = whales[0]["z_score"]
        variants = [
            f"A brief flash of large-player activity at {wt} UTC — "
            f"trade sizes spiked to {wz:.1f}σ above normal, net {whale_dir}.",

            f"One outsized-trade bar stood out at {wt} UTC ({wz:.1f}σ trade size), "
            f"leaning {whale_dir}. An isolated event, but notable.",

            f"At {wt} UTC, average trade size jumped to {wz:.1f}σ above the "
            f"day's baseline — a momentary spike of large-player {whale_dir}.",
        ]
        patterns.append((3, _pick_offset(variants, date_str, 5, extra=len(whales))))

    # ── 5. Intraday flow reversal ──
    if f["flow_reversed"]:
        first_dir = "buyers" if f["first_half_flow"] > 0 else "sellers"
        second_dir = "sellers" if f["first_half_flow"] > 0 else "buyers"
        fh = f["first_half_flow"]
        sh = f["second_half_flow"]
        variants = [
            f"The day split in half. {first_dir.title()} controlled the first "
            f"12 hours ({fh:+,.0f} BTC), then {second_dir} took over "
            f"({sh:+,.0f} BTC). The reversal changed the character of the day.",

            f"A tale of two halves — {first_dir} pushed {abs(fh):,.0f} BTC "
            f"through the morning, then {second_dir} flipped the script with "
            f"{abs(sh):,.0f} BTC in the afternoon.",

            f"Flow reversed midday. The first 12 hours belonged to {first_dir} "
            f"({fh:+,.0f} BTC), but {second_dir} erased that and then some "
            f"({sh:+,.0f} BTC) in the back half.",

            f"Morning and afternoon traded like two different markets. "
            f"{first_dir.title()} led early ({fh:+,.0f} BTC), {second_dir} "
            f"dominated late ({sh:+,.0f} BTC).",
        ]
        patterns.append((2, _pick_offset(variants, date_str, 11)))

    # ── 6. Extreme return (vs regime) ──
    ret_z = f["return_z_vs_regime"]
    if abs(ret_z) > 2:
        variants = [
            f"A statistical outlier: {day_return:+.2f}% return sits at {ret_z:+.1f}σ "
            f"from the {regime_lower}-regime average of {f['regime_avg_return']:+.2f}%. "
            f"Days like this are rare within this regime.",

            f"Even by {regime_lower}-regime standards, {day_return:+.2f}% was extreme — "
            f"{ret_z:+.1f}σ from the {f['regime_avg_return']:+.2f}% average. "
            f"This wasn't a normal {regime_lower} day.",

            f"At {ret_z:+.1f}σ from the regime mean, this {day_return:+.2f}% move was "
            f"an outlier within the {regime_lower} period (avg {f['regime_avg_return']:+.2f}%).",
        ]
        patterns.append((2, _pick_offset(variants, date_str, 17)))

    # ── 7. Regime transition proximity ──
    days_in = f.get("days_into_regime")
    days_left = f.get("days_until_regime_end")
    next_regime = f.get("next_regime")
    prev_regime = f.get("prev_regime")
    period_len = f.get("regime_period_length")

    if days_in is not None and days_in <= 3 and prev_regime:
        variants = [
            f"Day {days_in} of a fresh {regime_lower} regime. The market had just "
            f"flipped from {prev_regime.lower()}, and the new character was "
            f"still establishing itself.",

            f"The market was {days_in} day(s) into a new {regime_lower} regime, "
            f"having transitioned from {prev_regime.lower()}. Early-regime days "
            f"often carry residual momentum from the prior period.",

            f"This was near the start of a {regime_lower} regime (day {days_in}), "
            f"with the {prev_regime.lower()} regime barely in the rearview mirror.",
        ]
        patterns.append((2, _pick_offset(variants, date_str, 19)))
    elif days_left is not None and 0 <= days_left <= 3 and next_regime:
        variants = [
            f"The {regime_lower} regime was running out of road — {days_left} "
            f"day(s) before the market shifted to {next_regime.lower()}. "
            f"The transition was already underway in hindsight.",

            f"Among the final days of this {regime_lower} period. "
            f"Within {days_left} day(s), the market would flip to "
            f"{next_regime.lower()}.",

            f"In retrospect, the {regime_lower} regime was ending. "
            f"{next_regime} conditions were {days_left} day(s) away.",
        ]
        patterns.append((2, _pick_offset(variants, date_str, 19)))

    # ── 8. Volume anomaly ──
    vol_z = f["vol_z_vs_regime"]
    if vol_z > 2:
        variants = [
            f"Volume ran hot: {f['total_vol']:,.0f} BTC traded, "
            f"{vol_z:+.1f}σ above the {regime_lower}-regime average. "
            f"High volume days tend to carry more signal.",

            f"Trading activity surged to {vol_z:+.1f}σ above the regime norm "
            f"({f['total_vol']:,.0f} BTC). When volume spikes like this, "
            f"the flow data carries more weight.",

            f"Unusually heavy volume at {f['total_vol']:,.0f} BTC — "
            f"{vol_z:+.1f}σ above the {regime_lower}-regime baseline. "
            f"The market was paying attention.",
        ]
        patterns.append((3, _pick_offset(variants, date_str, 23)))
    elif vol_z < -2:
        variants = [
            f"Volume dried up: {f['total_vol']:,.0f} BTC traded, "
            f"{vol_z:+.1f}σ below the {regime_lower}-regime average. "
            f"Thin markets amplify noise.",

            f"A quiet day by volume — {f['total_vol']:,.0f} BTC, sitting at "
            f"{vol_z:+.1f}σ below the regime norm. Flow signals in low-volume "
            f"environments are less reliable.",
        ]
        patterns.append((3, _pick_offset(variants, date_str, 23)))

    # ── 9. Flow streak ──
    streak = f["flow_streak"]
    if abs(streak) >= 3:
        streak_dir = "buying" if streak > 0 else "selling"
        n = abs(streak)
        variants = [
            f"This was day {n} of consecutive net {streak_dir}. "
            f"Multi-day streaks reflect sustained conviction, not noise.",

            f"Net {streak_dir} for {n} straight days. "
            f"Whether it's institutional positioning or sentiment-driven, "
            f"the directional bias was persistent.",

            f"The {streak_dir} streak extended to {n} days. "
            f"Streaks this long suggest a structural flow, not just "
            f"intraday traders flipping positions.",

            f"Day {n} of unbroken net {streak_dir}. "
            f"The market had a directional lean and wasn't letting go.",
        ]
        patterns.append((3, _pick_offset(variants, date_str, 29, extra=n)))

    # ── 10. Range ──
    range_pct = f["range_pct"]
    if range_pct < 1.0 and abs(day_return) < 0.3:
        variants = [
            f"An unusually tight range: {range_pct:.2f}% from high to low. "
            f"Compression days like this tend to resolve with a larger move "
            f"in the days that follow.",

            f"Price was pinned — only {range_pct:.2f}% separated the day's "
            f"high and low. Periods of compression often precede expansion.",

            f"The {range_pct:.2f}% intraday range was minimal. Neither side "
            f"could generate momentum, creating a coiled market.",
        ]
        patterns.append((5, _pick_offset(variants, date_str, 31)))
    elif range_pct > 8:
        variants = [
            f"A volatile day: {range_pct:.1f}% range from ${f['low']:,.0f} to "
            f"${f['high']:,.0f}. Wide ranges like this create opportunities "
            f"but also traps for directional traders.",

            f"The {range_pct:.1f}% intraday range (${f['low']:,.0f} – "
            f"${f['high']:,.0f}) tells the story of a market in flux. "
            f"Both sides had their moments.",

            f"From ${f['low']:,.0f} to ${f['high']:,.0f} — a {range_pct:.1f}% "
            f"range that forced both bulls and bears to respect the volatility.",
        ]
        patterns.append((3, _pick_offset(variants, date_str, 31)))

    # Sort by priority (1 = most important)
    patterns.sort(key=lambda x: x[0])

    return patterns


def assign_tags(features, patterns):
    """Assign searchable tags based on detected patterns and features."""
    tags = []
    f = features

    # Regime (always)
    tags.append(f["regime"].lower())

    # Flow intensity
    if abs(f["flow_z_vs_regime"]) > 2:
        tags.append("extreme-flow")
    if f["net_flow"] > 0:
        tags.append("net-buying")
    else:
        tags.append("net-selling")

    # Whale activity
    if len(f["whale_bars"]) >= 3:
        tags.append("whale-activity")

    # CVD-price divergence
    if f["cvd_price_corr"] < -0.3:
        tags.append("flow-divergence")

    # Intraday reversal
    if f["flow_reversed"]:
        tags.append("flow-reversal")

    # Session dominance
    sessions = f["sessions"]
    dominant = max(sessions.values(), key=lambda s: s["flow_share"])
    if dominant["flow_share"] > 0.6:
        tags.append(f"{dominant['name'].lower()}-dominated")

    # Extreme return
    if abs(f["return_z_vs_regime"]) > 2:
        tags.append("outlier-return")

    # Regime transition
    days_in = f.get("days_into_regime")
    days_left = f.get("days_until_regime_end")
    if days_in is not None and days_in <= 3:
        tags.append("regime-start")
    if days_left is not None and days_left <= 3 and days_left >= 0:
        tags.append("regime-end")

    # Volume anomaly
    if abs(f["vol_z_vs_regime"]) > 2:
        tags.append("volume-spike" if f["vol_z_vs_regime"] > 0 else "low-volume")

    # Range
    if f["range_pct"] > 8:
        tags.append("high-volatility")
    elif f["range_pct"] < 1.0 and abs(f["return_pct"]) < 0.3:
        tags.append("compression")

    # Streak
    if abs(f["flow_streak"]) >= 3:
        tags.append("flow-streak")

    return tags


TAG_DESCRIPTIONS = {
    "bull": "Days during bull market regimes (30-day return > +10%)",
    "bear": "Days during bear market regimes (30-day return < -10%)",
    "chop": "Days during sideways/choppy market regimes",
    "extreme-flow": "Days with net flow > 2σ above regime average",
    "net-buying": "Days where buyers dominated aggregate flow",
    "net-selling": "Days where sellers dominated aggregate flow",
    "whale-activity": "Days with 3+ bars of outsized trade sizes (3σ+)",
    "flow-divergence": "Days where flow and price moved in opposite directions",
    "flow-reversal": "Days where buying/selling pressure reversed intraday",
    "asia-dominated": "Days where Asia session drove > 60% of directional flow",
    "europe-dominated": "Days where Europe session drove > 60% of directional flow",
    "us-dominated": "Days where US session drove > 60% of directional flow",
    "outlier-return": "Days with returns > 2σ from regime average",
    "regime-start": "First 3 days of a new market regime",
    "regime-end": "Final 3 days before a regime transition",
    "volume-spike": "Days with volume > 2σ above regime average",
    "low-volume": "Days with volume > 2σ below regime average",
    "high-volatility": "Days with intraday range > 8%",
    "compression": "Days with intraday range < 1% and flat return",
    "flow-streak": "Days within a 3+ day consecutive buying or selling streak",
}


def generate_narrative(date_str, features, patterns):
    """Build the full article text from detected patterns."""
    f = features
    regime = f["regime"]
    regime_lower = regime.lower()
    day_return = f["return_pct"]
    net_flow = f["net_flow"]

    # Headline / hook — multiple price action phrasings
    if abs(day_return) < 0.3:
        price_actions = ["traded flat", "went nowhere", f"finished near unchanged ({day_return:+.2f}%)"]
    elif day_return > 5:
        price_actions = [f"surged {day_return:+.1f}%", f"exploded {day_return:+.1f}% higher",
                         f"ripped {day_return:+.1f}% to the upside"]
    elif day_return > 2:
        price_actions = [f"rallied {day_return:+.1f}%", f"climbed {day_return:+.1f}%",
                         f"pushed {day_return:+.1f}% higher"]
    elif day_return > 0.3:
        price_actions = [f"edged higher ({day_return:+.2f}%)", f"ground out a {day_return:+.2f}% gain",
                         f"ticked up {day_return:+.2f}%"]
    elif day_return < -5:
        price_actions = [f"crashed {day_return:+.1f}%", f"plunged {day_return:+.1f}%",
                         f"dropped {abs(day_return):.1f}% in a sharp selloff"]
    elif day_return < -2:
        price_actions = [f"sold off {day_return:+.1f}%", f"fell {abs(day_return):.1f}%",
                         f"lost {abs(day_return):.1f}%"]
    else:
        price_actions = [f"drifted lower ({day_return:+.2f}%)", f"slipped {abs(day_return):.2f}%",
                         f"gave back {abs(day_return):.2f}%"]

    price_action = _pick(price_actions, date_str)
    direction = "buyers" if net_flow > 0 else "sellers"
    dir_adj = "buying" if net_flow > 0 else "selling"
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%B %d, %Y")
    weekday = dt.strftime("%A")

    # Hook variants
    hook_variants = [
        f"Bitcoin {price_action} on {weekday}, {date_display}, "
        f"closing at ${f['close']:,.0f} as {direction} controlled "
        f"aggregate flow with {abs(net_flow):,.0f} BTC of net pressure.",

        f"On {weekday}, {date_display}, Bitcoin {price_action} to "
        f"${f['close']:,.0f}. Net flow: {net_flow:+,.0f} BTC — "
        f"{direction} had the edge.",

        f"Bitcoin closed at ${f['close']:,.0f} on {date_display} after "
        f"{price_action.replace('+', '').replace('-', '')}. "
        f"Aggressor-side flow netted {net_flow:+,.0f} BTC, favoring {direction}.",

        f"{date_display}: Bitcoin {price_action}. "
        f"The {abs(net_flow):,.0f} BTC of net {dir_adj} flow at "
        f"${f['close']:,.0f} told a clear story — {direction} were in charge.",
    ]

    # Assemble sections
    sections = {
        "hook": _pick_offset(hook_variants, date_str, 37),
        "flow_analysis": [],
        "session_analysis": [],
        "context": [],
    }

    for priority, text in patterns:
        # Route pattern to appropriate section
        if any(kw in text.lower() for kw in ["session", "asia", "europe",
                                               "morning", "afternoon", "strip out"]):
            sections["session_analysis"].append(text)
        elif any(kw in text.lower() for kw in ["regime", "outlier", "transition",
                                                 "statistical", "flip"]):
            sections["context"].append(text)
        else:
            sections["flow_analysis"].append(text)

    # Build final narrative dict
    narrative = {
        "title": f"BTC Flow Analysis — {date_display}",
        "meta_description": (
            f"Bitcoin {price_action} on {date_display} with {abs(net_flow):,.0f} BTC "
            f"net {dir_adj} flow. "
            f"Minute-level order flow analysis from Binance."
        ),
        "hook": sections["hook"],
        "flow_paragraphs": sections["flow_analysis"],
        "session_paragraphs": sections["session_analysis"],
        "context_paragraphs": sections["context"],
        "date_display": date_display,
        "weekday": weekday,
        "n_patterns": len(patterns),
    }

    # Fallback for quiet days — with variants
    if not sections["flow_analysis"]:
        fallbacks = [
            f"Flow was balanced throughout the day. Net {abs(net_flow):,.0f} BTC "
            f"of {dir_adj} pressure fell within normal range for the "
            f"{regime_lower} regime.",

            f"Neither side forced the issue. The {abs(net_flow):,.0f} BTC of net "
            f"{dir_adj} was unremarkable relative to typical {regime_lower}-regime days.",

            f"A quiet day on the flow front. {abs(net_flow):,.0f} BTC of net {dir_adj} "
            f"didn't stand out against the {regime_lower}-regime baseline.",
        ]
        narrative["flow_paragraphs"] = [_pick_offset(fallbacks, date_str, 41)]

    if not sections["context"]:
        ctx_fallbacks = [
            f"This day sat within a {regime_lower} regime. "
            f"The {day_return:+.2f}% return tracked the regime "
            f"average of {f['regime_avg_return']:+.2f}%.",

            f"Market conditions were {regime_lower} — and this day didn't deviate. "
            f"A {day_return:+.2f}% return against a regime average of "
            f"{f['regime_avg_return']:+.2f}%.",

            f"Nothing unusual for a {regime_lower}-regime day. The {day_return:+.2f}% "
            f"return was consistent with the period's average of "
            f"{f['regime_avg_return']:+.2f}%.",
        ]
        narrative["context_paragraphs"] = [_pick_offset(ctx_fallbacks, date_str, 43)]

    return narrative


# ── Chart Generation ─────────────────────────────────────────────────────

def generate_charts(date_str, features, output_dir):
    """Generate 3 chart PNGs for the blog page."""
    bars = features["_bars"]
    times = bars["open_time"]
    regime = features["regime"]
    regime_colors = {"BULL": "#34d399", "BEAR": "#f87171", "CHOP": "#fbbf24"}
    regime_color = regime_colors.get(regime, "#fbbf24")

    def style_ax(ax):
        ax.set_facecolor("#0f1117")
        ax.tick_params(colors="#8b8fa3", labelsize=9)
        for s in ["top", "right"]:
            ax.spines[s].set_visible(False)
        for s in ["left", "bottom"]:
            ax.spines[s].set_color("#2a2d3e")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=UTC))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))

    day_start = times.iloc[0].normalize()
    asia_end = day_start.replace(hour=8, tzinfo=UTC)
    europe_end = day_start.replace(hour=14, tzinfo=UTC)
    us_end = day_start.replace(hour=23, minute=59, tzinfo=UTC)
    asia_start = day_start.replace(hour=0, tzinfo=UTC)

    def add_sessions(ax):
        ax.axvspan(asia_start, asia_end, alpha=0.06, color="#fbbf24")
        ax.axvspan(asia_end, europe_end, alpha=0.06, color="#34d399")
        ax.axvspan(europe_end, us_end, alpha=0.06, color="#f87171")

    # Chart 1: Price + Volume (dual axis)
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 4))
    fig.patch.set_facecolor("#0f1117")
    style_ax(ax1)
    add_sessions(ax1)
    ax1.plot(times, bars["close"], color="#60a5fa", linewidth=1.2)
    ax1.fill_between(times, bars["low"], bars["high"], alpha=0.08, color="#60a5fa")
    ax1.set_ylabel("Price ($)", color="#8b8fa3", fontsize=10)
    ax1.set_xlabel("Time (UTC)", color="#8b8fa3", fontsize=10)

    ax2 = ax1.twinx()
    ax2.bar(times, bars["volume"], width=0.0007, alpha=0.2, color="#00d4ff")
    ax2.set_ylabel("Volume (BTC)", color="#8b8fa3", fontsize=10)
    ax2.tick_params(colors="#8b8fa3", labelsize=9)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color("#2a2d3e")
    ax2.set_ylim(0, bars["volume"].max() * 4)

    # Regime badge
    ax1.text(0.02, 0.95, regime, transform=ax1.transAxes, fontsize=11, fontweight="bold",
             va="top", ha="left", color=regime_color,
             bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1d2e", edgecolor=regime_color, alpha=0.9))

    fig.text(0.99, 0.01, "qibble.io", color="#4a4d5e", fontsize=8, ha="right", style="italic")
    plt.tight_layout()
    fig.savefig(output_dir / "price_volume.png", dpi=120, bbox_inches="tight", facecolor="#0f1117")
    plt.close(fig)

    # Chart 2: CVD vs Cumulative Return
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 4))
    fig.patch.set_facecolor("#0f1117")
    style_ax(ax1)
    add_sessions(ax1)

    ax1.fill_between(times, 0, bars["cum_flow"], alpha=0.3, color="#7c3aed")
    ax1.plot(times, bars["cum_flow"], color="#7c3aed", linewidth=1.2, label="Cumulative Flow (BTC)")
    ax1.set_ylabel("Cumulative Net Flow (BTC)", color="#8b8fa3", fontsize=10)
    ax1.set_xlabel("Time (UTC)", color="#8b8fa3", fontsize=10)
    ax1.axhline(0, color="#2a2d3e", linewidth=0.5)

    ax2 = ax1.twinx()
    ax2.plot(times, bars["cum_return"], color="#fbbf24", linewidth=1.2, label="Cumulative Return (%)")
    ax2.set_ylabel("Cumulative Return (%)", color="#8b8fa3", fontsize=10)
    ax2.tick_params(colors="#8b8fa3", labelsize=9)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color("#2a2d3e")

    # Legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left",
               fontsize=8, facecolor="#0f1117", edgecolor="#2a2d3e", labelcolor="white")

    fig.text(0.99, 0.01, "qibble.io", color="#4a4d5e", fontsize=8, ha="right", style="italic")
    plt.tight_layout()
    fig.savefig(output_dir / "cvd_return.png", dpi=120, bbox_inches="tight", facecolor="#0f1117")
    plt.close(fig)

    # Chart 3: Buy/Sell Volume
    fig, ax = plt.subplots(1, 1, figsize=(10, 3.5))
    fig.patch.set_facecolor("#0f1117")
    style_ax(ax)
    add_sessions(ax)

    ax.bar(times, bars["buy_vol"], width=0.0007, alpha=0.8, color="#00ff88", label="Buy Volume")
    ax.bar(times, -bars["sell_vol"], width=0.0007, alpha=0.8, color="#ff3366", label="Sell Volume")
    ax.axhline(0, color="#2a2d3e", linewidth=0.5)
    ax.set_ylabel("Volume (BTC)", color="#8b8fa3", fontsize=10)
    ax.set_xlabel("Time (UTC)", color="#8b8fa3", fontsize=10)
    ax.legend(loc="upper left", fontsize=8, facecolor="#0f1117", edgecolor="#2a2d3e", labelcolor="white")

    fig.text(0.99, 0.01, "qibble.io", color="#4a4d5e", fontsize=8, ha="right", style="italic")
    plt.tight_layout()
    fig.savefig(output_dir / "buy_sell_volume.png", dpi=120, bbox_inches="tight", facecolor="#0f1117")
    plt.close(fig)

    return ["price_volume.png", "cvd_return.png", "buy_sell_volume.png"]


# ── HTML Generation ──────────────────────────────────────────────────────

def generate_html(date_str, features, narrative, chart_files, prev_date, next_date,
                   tags=None, related_days=None):
    """Render HTML from Jinja2 template."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("daily.html")

    dt = datetime.strptime(date_str, "%Y-%m-%d")

    return template.render(
        date=date_str,
        date_display=narrative["date_display"],
        weekday=narrative["weekday"],
        title=narrative["title"],
        meta_description=narrative["meta_description"],
        regime=features["regime"],
        open=f"${features['open']:,.0f}",
        close=f"${features['close']:,.0f}",
        high=f"${features['high']:,.0f}",
        low=f"${features['low']:,.0f}",
        return_pct=f"{features['return_pct']:+.2f}%",
        return_class="positive" if features["return_pct"] > 0 else "negative" if features["return_pct"] < 0 else "neutral",
        net_flow=f"{features['net_flow']:+,.0f}",
        net_flow_direction="buying" if features["net_flow"] > 0 else "selling",
        total_vol=f"{features['total_vol']:,.0f}",
        num_trades=f"{features['num_trades']:,}",
        buy_sell_ratio=f"{features['buy_sell_ratio']:.2f}",
        avg_trade_size=f"{features['avg_trade_size']:.4f}",
        hook=narrative["hook"],
        flow_paragraphs=narrative["flow_paragraphs"],
        session_paragraphs=narrative["session_paragraphs"],
        context_paragraphs=narrative["context_paragraphs"],
        chart_files=chart_files,
        sessions=features["sessions"],
        prev_date=prev_date,
        next_date=next_date,
        n_patterns=narrative["n_patterns"],
        year=dt.year,
        tags=tags or [],
        related_days=related_days or [],
    )


# ── Main Pipeline ────────────────────────────────────────────────────────

def generate_page(date_str, daily_df, regime_map, regime_stats, regime_periods,
                  minute_bars=None, all_dates=None, page_manifest=None):
    """Generate a single blog page for a given date. Returns metadata dict or None."""
    print(f"  Generating {date_str}...")

    # Get daily row
    row_mask = daily_df["date_utc"] == date_str
    if not row_mask.any():
        print(f"    SKIP: {date_str} not in daily data")
        return None
    daily_row = daily_df[row_mask].iloc[0]

    # Get regime
    regime = regime_map.get(date_str, "CHOP")

    # Get minute bars
    if minute_bars is not None:
        bars = minute_bars[minute_bars["date_utc"] == date_str] if "date_utc" in minute_bars.columns else minute_bars
    else:
        print(f"    Fetching minute bars from Binance...")
        bars = fetch_minute_klines(date_str)

    if len(bars) < 60:
        print(f"    SKIP: only {len(bars)} bars")
        return None

    # Compute features
    features = compute_day_features(bars, daily_row, regime, regime_stats, regime_periods, daily_df)

    # Detect patterns + tags
    patterns = detect_patterns(features)
    tags = assign_tags(features, patterns)
    print(f"    {len(patterns)} patterns, tags: {tags}")

    # Generate narrative
    narrative = generate_narrative(date_str, features, patterns)

    # Create output directory
    page_dir = OUTPUT_DIR / date_str
    page_dir.mkdir(parents=True, exist_ok=True)

    # Generate charts
    chart_files = generate_charts(date_str, features, page_dir)

    # Find prev/next dates
    prev_date = next_date = None
    if all_dates:
        idx = all_dates.index(date_str) if date_str in all_dates else -1
        prev_date = all_dates[idx - 1] if idx > 0 else None
        next_date = all_dates[idx + 1] if idx < len(all_dates) - 1 else None

    # Find related days from manifest (same tags, different date)
    related_days = []
    if page_manifest:
        related_days = find_related_days(date_str, tags, regime, page_manifest)

    # Generate HTML
    html = generate_html(date_str, features, narrative, chart_files, prev_date, next_date,
                          tags=tags, related_days=related_days)
    (page_dir / "index.html").write_text(html)

    # Build metadata for manifest
    meta = {
        "date": date_str,
        "regime": regime,
        "return_pct": round(features["return_pct"], 2),
        "net_flow": round(features["net_flow"], 0),
        "close": round(features["close"], 0),
        "tags": tags,
        "n_patterns": len(patterns),
        "hook": narrative["hook"][:120],
    }

    print(f"    Done: {page_dir}")
    return meta


def find_related_days(date_str, tags, regime, manifest, max_related=5):
    """Find related days by tag overlap, excluding self."""
    scored = []
    my_tags = set(tags)
    for entry in manifest:
        if entry["date"] == date_str:
            continue
        other_tags = set(entry["tags"])
        # Score: shared interesting tags (exclude regime + direction which are too common)
        interesting_shared = my_tags & other_tags - {"net-buying", "net-selling", "bull", "bear", "chop"}
        # Bonus for same regime
        regime_bonus = 1 if entry["regime"] == regime else 0
        score = len(interesting_shared) * 3 + regime_bonus
        if score > 0:
            scored.append((score, entry))

    scored.sort(key=lambda x: -x[0])
    return [
        {
            "date": e["date"],
            "regime": e["regime"],
            "return_pct": e["return_pct"],
            "hook": e["hook"],
            "tags": e["tags"],
        }
        for _, e in scored[:max_related]
    ]


def generate_index_page(manifest):
    """Generate the /btc-flow/ index page with all days listed."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("index.html")

    # Sort by date descending
    sorted_manifest = sorted(manifest, key=lambda x: x["date"], reverse=True)

    # Group by year-month for browsing
    months = {}
    for entry in sorted_manifest:
        ym = entry["date"][:7]  # YYYY-MM
        months.setdefault(ym, []).append(entry)

    # Collect all tags with counts
    tag_counts = {}
    for entry in manifest:
        for tag in entry["tags"]:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    # Sort tags by count descending, exclude regime/direction (too common)
    interesting_tags = {k: v for k, v in tag_counts.items()
                        if k not in ("net-buying", "net-selling", "bull", "bear", "chop")}
    sorted_tags = sorted(interesting_tags.items(), key=lambda x: -x[1])

    html = template.render(
        months=months,
        total_days=len(manifest),
        tag_counts=sorted_tags,
        tag_descriptions=TAG_DESCRIPTIONS,
        regime_counts={r: sum(1 for e in manifest if e["regime"] == r)
                       for r in ["BULL", "BEAR", "CHOP"]},
    )

    index_dir = OUTPUT_DIR
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / "index.html").write_text(html)
    print(f"  Index page: {index_dir / 'index.html'}")


def generate_tag_pages(manifest):
    """Generate /btc-flow/tag/{tag}/ pages for each tag."""
    tag_map = {}
    for entry in manifest:
        for tag in entry["tags"]:
            tag_map.setdefault(tag, []).append(entry)

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("tag.html")

    tag_dir = OUTPUT_DIR / "tag"
    tag_dir.mkdir(parents=True, exist_ok=True)

    for tag, entries in tag_map.items():
        sorted_entries = sorted(entries, key=lambda x: x["date"], reverse=True)
        html = template.render(
            tag=tag,
            tag_description=TAG_DESCRIPTIONS.get(tag, ""),
            entries=sorted_entries,
            total=len(entries),
            all_tags=sorted(tag_map.keys()),
        )
        page_dir = tag_dir / tag
        page_dir.mkdir(parents=True, exist_ok=True)
        (page_dir / "index.html").write_text(html)

    print(f"  Tag pages: {len(tag_map)} tags → {tag_dir}")


def generate_sitemap(manifest):
    """Generate sitemap.xml."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
             '  <url><loc>https://qibble.io/</loc><priority>1.0</priority></url>',
             '  <url><loc>https://qibble.io/btc-flow/</loc><priority>0.9</priority></url>']

    for entry in sorted(manifest, key=lambda x: x["date"], reverse=True):
        lines.append(f'  <url><loc>https://qibble.io/btc-flow/{entry["date"]}/</loc>'
                     f'<lastmod>{entry["date"]}</lastmod><priority>0.6</priority></url>')

    # Tag pages
    tags = set()
    for entry in manifest:
        tags.update(entry["tags"])
    for tag in sorted(tags):
        lines.append(f'  <url><loc>https://qibble.io/btc-flow/tag/{tag}/</loc>'
                     f'<priority>0.7</priority></url>')

    lines.append('</urlset>')

    sitemap_path = SCRIPT_DIR.parent / "blog-static" / "sitemap.xml"
    sitemap_path.write_text('\n'.join(lines))
    print(f"  Sitemap: {sitemap_path} ({len(manifest)} daily + {len(tags)} tag URLs)")


def generate_robots_txt():
    """Generate robots.txt."""
    content = """User-agent: *
Allow: /

Sitemap: https://qibble.io/sitemap.xml
"""
    path = SCRIPT_DIR.parent / "blog-static" / "robots.txt"
    path.write_text(content)
    print(f"  robots.txt: {path}")


def scan_day_patterns(date_str, bars, daily_row, regime, regime_stats, regime_periods, daily_df):
    """Lightweight scan: compute features + pattern count without charts/HTML."""
    if len(bars) < 60:
        return None
    features = compute_day_features(bars, daily_row, regime, regime_stats, regime_periods, daily_df)
    patterns = detect_patterns(features)
    tags = assign_tags(features, patterns)
    return {
        "date": date_str,
        "n_patterns": len(patterns),
        "tags": tags,
        "return_pct": round(features["return_pct"], 2),
        "net_flow": round(features["net_flow"], 0),
        "regime": regime,
    }


def _parse_args():
    """Parse CLI args into a dict."""
    args = sys.argv[1:]
    opts = {"parquet": None, "mode": "sample", "min_patterns": 0, "top": 0, "dates": []}

    # Extract flags
    if "--parquet" in args:
        idx = args.index("--parquet")
        opts["parquet"] = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    if "--phase1" in args:
        opts["min_patterns"] = 4
        opts["mode"] = "all"
        args.remove("--phase1")
    elif "--phase2" in args:
        opts["min_patterns"] = 3
        opts["mode"] = "all"
        args.remove("--phase2")

    if "--min-patterns" in args:
        idx = args.index("--min-patterns")
        opts["min_patterns"] = int(args[idx + 1])
        opts["mode"] = "all"
        args = args[:idx] + args[idx + 2:]

    if "--top" in args:
        idx = args.index("--top")
        opts["top"] = int(args[idx + 1])
        opts["mode"] = "all"
        args = args[:idx] + args[idx + 2:]

    if "--all" in args:
        opts["mode"] = "all"
        args.remove("--all")

    # Remaining args = specific dates
    if args:
        opts["dates"] = args
        opts["mode"] = "dates"

    return opts


def main():
    opts = _parse_args()

    if opts["parquet"]:
        print(f"Loading from parquet: {opts['parquet']}")
        minute_df, daily = load_parquet(opts["parquet"])
    else:
        print("Fetching daily klines from Binance...")
        daily = fetch_daily_klines()
        minute_df = None

    # Exclude today (incomplete)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = daily[daily["date_utc"] != today_str].reset_index(drop=True)

    # Build regime context
    regime_map, regime_stats, regime_periods, daily = build_regime_context(daily)
    all_dates = sorted(daily["date_utc"].unique().tolist())
    valid_dates = [d for d in all_dates if d in regime_map]

    print(f"Total dates: {len(valid_dates)}")
    print(f"Regime stats: { {k: v['n_days'] for k, v in regime_stats.items()} }")

    # ── Determine target dates ──
    if opts["mode"] == "dates":
        target_dates = [d for d in opts["dates"] if d in valid_dates]
        if not target_dates:
            print(f"No valid dates found: {opts['dates']}")
            print(f"Available range: {valid_dates[0]} to {valid_dates[-1]}")
            return
    elif opts["mode"] == "all":
        # If filtering by pattern count or top N, scan first
        if opts["min_patterns"] > 0 or opts["top"] > 0:
            print(f"\n=== Scanning all days for pattern counts ===")
            scans = []
            for date_str in valid_dates:
                row_mask = daily["date_utc"] == date_str
                if not row_mask.any():
                    continue
                daily_row = daily[row_mask].iloc[0]
                regime = regime_map.get(date_str, "CHOP")

                if minute_df is not None:
                    bars = minute_df[minute_df["date_utc"] == date_str].copy()
                    if "hour" not in bars.columns:
                        bars["hour"] = bars["open_time"].dt.hour
                else:
                    continue  # Can't scan without parquet in filter mode

                result = scan_day_patterns(date_str, bars, daily_row, regime,
                                           regime_stats, regime_periods, daily)
                if result:
                    scans.append(result)

            # Distribution
            pattern_dist = {}
            for s in scans:
                n = s["n_patterns"]
                pattern_dist[n] = pattern_dist.get(n, 0) + 1
            print(f"  Pattern distribution: {dict(sorted(pattern_dist.items()))}")

            # Apply filters
            if opts["min_patterns"] > 0:
                filtered = [s for s in scans if s["n_patterns"] >= opts["min_patterns"]]
                print(f"  After min_patterns >= {opts['min_patterns']}: {len(filtered)} days")
            else:
                filtered = scans

            if opts["top"] > 0:
                filtered.sort(key=lambda x: (-x["n_patterns"], -abs(x["return_pct"])))
                filtered = filtered[:opts["top"]]
                print(f"  After top {opts['top']}: {len(filtered)} days")

            target_dates = [s["date"] for s in filtered]
            print(f"  Generating {len(target_dates)} pages")
        else:
            target_dates = valid_dates
    else:
        # Sample 5 diverse days
        print("\nSampling 5 diverse days for prototype...")
        import random
        random.seed(42)
        samples = []
        for regime in ["BULL", "BEAR", "CHOP"]:
            regime_dates = [d for d in valid_dates if regime_map.get(d) == regime]
            if regime_dates:
                samples.append(random.choice(regime_dates))
        recent = valid_dates[-30:]
        old = valid_dates[30:200]
        if recent:
            samples.append(random.choice(recent))
        if old:
            samples.append(random.choice(old))
        target_dates = list(set(samples))[:5]
        print(f"Selected: {target_dates}")

    # ── Pass 1: Generate all pages, collect manifest ──
    print(f"\n=== Pass 1: Generate pages ({len(target_dates)} days) ===")
    manifest = []
    for i, date_str in enumerate(target_dates):
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(target_dates)}")
        if minute_df is not None:
            day_bars = minute_df[minute_df["date_utc"] == date_str].copy()
            if "hour" not in day_bars.columns:
                day_bars["hour"] = day_bars["open_time"].dt.hour
            meta = generate_page(date_str, daily, regime_map, regime_stats, regime_periods,
                                 minute_bars=day_bars, all_dates=valid_dates)
        else:
            meta = generate_page(date_str, daily, regime_map, regime_stats, regime_periods,
                                 all_dates=valid_dates)
        if meta:
            manifest.append(meta)

    # ── Pass 2: Re-render HTML with related-day links (now that manifest exists) ──
    if len(manifest) > 1:
        print(f"\n=== Pass 2: Add related-day links ===")
        env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
        for meta in manifest:
            date_str = meta["date"]
            related = find_related_days(date_str, meta["tags"], meta["regime"], manifest)
            if not related:
                continue
            # Re-read and patch the HTML — inject related days section
            page_dir = OUTPUT_DIR / date_str
            html_path = page_dir / "index.html"
            html = html_path.read_text()
            # Insert related days before the CTA box
            related_html = render_related_html(related)
            html = html.replace('<!-- CTA -->', related_html + '\n        <!-- CTA -->')
            html_path.write_text(html)
            print(f"  {date_str}: {len(related)} related days linked")

    # ── Generate index, tag pages, sitemap ──
    print(f"\n=== Generating index & discovery pages ===")
    generate_index_page(manifest)
    generate_tag_pages(manifest)
    generate_sitemap(manifest)
    generate_robots_txt()

    # Save manifest
    manifest_path = OUTPUT_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {manifest_path}")

    print(f"\nDone: {len(manifest)}/{len(target_dates)} pages → {OUTPUT_DIR}")


def render_related_html(related_days):
    """Render related days section as raw HTML (injected into existing pages)."""
    if not related_days:
        return ""

    rows = []
    for rd in related_days:
        regime_colors = {"BULL": "#34d399", "BEAR": "#f87171", "CHOP": "#fbbf24"}
        rc = regime_colors.get(rd["regime"], "#fbbf24")
        ret_color = "#34d399" if rd["return_pct"] > 0 else "#f87171" if rd["return_pct"] < 0 else "#ccc"
        tag_badges = " ".join(
            f'<span style="background:#1e1e2e;color:#00d4ff;'
            f'padding:2px 6px;border-radius:3px;font-size:9px;'
            f'border:1px solid #2a2d3e;display:inline-block;">{t}</span>'
            for t in rd["tags"]
            if t not in ("net-buying", "net-selling", "bull", "bear", "chop")
        )
        dt = datetime.strptime(rd["date"], "%Y-%m-%d")
        date_display = dt.strftime("%b %d, %Y")

        rows.append(
            f'<a href="/btc-flow/{rd["date"]}/" style="display:block;padding:12px;'
            f'border:1px solid #1e1e2e;border-radius:6px;text-decoration:none;'
            f'margin-bottom:8px;background:rgba(17,17,24,0.5);">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">'
            f'<span style="color:#fff;font-size:13px;font-weight:500;">{date_display}</span>'
            f'<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:3px;'
            f'background:rgba(0,0,0,0.3);color:{rc};border:1px solid {rc};">{rd["regime"]}</span>'
            f'</div>'
            f'<div style="color:{ret_color};font-size:12px;margin-bottom:4px;">{rd["return_pct"]:+.2f}%</div>'
            f'<div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:4px;">{tag_badges}</div>'
            f'</a>'
        )

    return f'''
        <div class="content-section">
            <h2>Related Days</h2>
            <p style="font-size:11px;color:#888;margin-bottom:12px;">
                Days with similar flow patterns and market conditions.
            </p>
            {"".join(rows)}
        </div>
    '''


if __name__ == "__main__":
    main()
