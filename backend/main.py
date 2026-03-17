"""
Qibble BTC Flow Dashboard — Backend
=====================================
FastAPI serving pre-computed analytics from btc_1m.parquet.
All heavy computation runs on startup; endpoints serve cached results.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pandas as pd
import numpy as np
import os
import time as time_module
import threading
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("qibble-sync")

# Lock for swapping global data structures during background sync
data_lock = threading.Lock()

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"
SYNC_INTERVAL_S = 6 * 3600  # 6 hours

app = FastAPI(title="Qibble BTC Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


# ── Load Data ───────────────────────────────────────────────────────────

DATA_PATH = os.environ.get(
    "DATA_PATH",
    os.path.join(os.path.dirname(__file__), "..", "data", "btc_1m.parquet"),
)
ROLLING_WINDOW_DAYS = 365 * 5  # 5-year rolling window

print(f"Loading data from {DATA_PATH}...")
df = pd.read_parquet(DATA_PATH)
df["time"] = df["open_time"].dt.strftime("%H:%M")
df["date_str"] = df["date_utc"]  # already string
print(f"  Loaded {len(df):,} bars, {df['date_str'].nunique()} days")

# Trim to 5-year rolling window to prevent OOM as data grows
cutoff = df["open_time"].max() - pd.Timedelta(days=ROLLING_WINDOW_DAYS)
before = len(df)
df = df[df["open_time"] >= cutoff].reset_index(drop=True)
if len(df) < before:
    print(f"  Trimmed to {ROLLING_WINDOW_DAYS}-day window: {before:,} → {len(df):,} bars, {df['date_str'].nunique()} days")


# ── Regime Detection ────────────────────────────────────────────────────
# Rolling 30-day return, ±10% threshold, 14-day minimum hold

def _detect_regimes() -> dict:
    """Auto-detect bull/bear/chop regimes from 30-day rolling returns."""
    daily = df.groupby("date_str").agg(
        open=("open", "first"),
        close=("close", "last"),
    ).reset_index()
    daily["date"] = pd.to_datetime(daily["date_str"])
    daily.sort_values("date", inplace=True)
    daily.reset_index(drop=True, inplace=True)

    daily["ret_30d"] = (daily["close"] / daily["close"].shift(30) - 1) * 100

    # Raw regime label
    daily["raw_regime"] = "CHOP"
    daily.loc[daily["ret_30d"] > 10, "raw_regime"] = "BULL"
    daily.loc[daily["ret_30d"] < -10, "raw_regime"] = "BEAR"

    # 14-day minimum hold: don't flip unless new regime persists 14+ days
    regime = []
    current = "CHOP"
    pending = None
    pending_count = 0

    for _, row in daily.iterrows():
        if pd.isna(row["ret_30d"]):
            regime.append(current)
            continue

        raw = row["raw_regime"]
        if raw == current:
            pending = None
            pending_count = 0
            regime.append(current)
        elif raw == pending:
            pending_count += 1
            if pending_count >= 14:
                current = pending
                pending = None
                pending_count = 0
            regime.append(current)
        else:
            pending = raw
            pending_count = 1
            regime.append(current)

    daily["regime"] = regime

    # Build regime map: date_str -> regime
    regime_map = dict(zip(daily["date_str"], daily["regime"]))

    # Build regime periods (contiguous stretches)
    daily["regime_group"] = (daily["regime"] != daily["regime"].shift(1)).cumsum()
    periods = daily.groupby("regime_group").agg(
        regime=("regime", "first"),
        start=("date_str", "first"),
        end=("date_str", "last"),
        n_days=("date_str", "count"),
        avg_price=("close", "mean"),
        start_price=("close", "first"),
        end_price=("close", "last"),
    ).reset_index(drop=True)
    periods["return_pct"] = ((periods["end_price"] / periods["start_price"]) - 1) * 100

    return {
        "map": regime_map,
        "periods": periods.to_dict("records"),
        "daily": daily[["date_str", "regime", "ret_30d", "close"]].to_dict("records"),
    }


print("Computing regimes...")
REGIMES = _detect_regimes()
df["regime"] = df["date_str"].map(REGIMES["map"]).fillna("CHOP")
print(f"  Regimes: {df['regime'].value_counts().to_dict()}")


# ── Pre-compute Daily Aggregates ────────────────────────────────────────

def _build_daily_agg() -> pd.DataFrame:
    daily = df.groupby(["date_str", "regime"]).agg(
        total_vol=("volume", "sum"),
        total_buy=("buy_vol", "sum"),
        total_sell=("sell_vol", "sum"),
        net_flow=("net_flow", "sum"),
        open_px=("open", "first"),
        close_px=("close", "last"),
        high_px=("high", "max"),
        low_px=("low", "min"),
        total_trades=("num_trades", "sum"),
        total_quote_vol=("quote_volume", "sum"),
        n_bars=("close", "count"),
        avg_trade_size=("avg_trade_size", "mean"),
    ).reset_index()
    daily["return_pct"] = ((daily["close_px"] / daily["open_px"]) - 1) * 100
    daily["day_imb"] = np.where(
        daily["total_vol"] > 0,
        (daily["total_buy"] - daily["total_sell"]) / daily["total_vol"],
        0.0,
    )
    return daily


print("Computing daily aggregates...")
DAILY_AGG = _build_daily_agg()


# ── Pre-compute Analytics ───────────────────────────────────────────────

# --- Intraday Correlation (by hour of day) ---
def _build_intraday_corr() -> dict:
    """Rolling 20-bar correlation of cum_flow vs cum_return, averaged by hour."""
    results = {}
    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        corr_data = []
        for date, day_df in rdf.groupby("date_str"):
            if len(day_df) < 30:
                continue
            d = day_df.sort_values("open_time").copy()
            first_px = d["close"].iloc[0]
            if first_px == 0:
                continue
            d["cum_flow"] = d["net_flow"].cumsum()
            d["cum_return"] = ((d["close"] / first_px) - 1) * 100
            d["roll_corr"] = d["cum_flow"].rolling(20, min_periods=10).corr(d["cum_return"])
            # Z-score flow within day for cross-day averaging
            flow_std = d["cum_flow"].std()
            if flow_std > 0:
                d["flow_z"] = d["cum_flow"] / flow_std
            else:
                d["flow_z"] = 0.0
            d["hour"] = d["open_time"].dt.hour
            corr_data.append(d[["hour", "roll_corr", "cum_return", "flow_z"]].copy())

        if not corr_data:
            results[regime] = []
            continue

        all_corr = pd.concat(corr_data)
        hourly = all_corr.groupby("hour").agg(
            avg_corr=("roll_corr", "mean"),
            std_corr=("roll_corr", "std"),
            avg_return=("cum_return", "mean"),
            avg_flow_z=("flow_z", "mean"),
            n=("roll_corr", "count"),
        ).reset_index()
        results[regime] = hourly.to_dict("records")

    return results


# --- Lead-Lag Cross-Correlation ---
def _build_lead_lag() -> dict:
    """Cross-correlation of net_flow vs bar return at lags -20 to +20."""
    results = {}
    lags = list(range(-20, 21))

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        all_corrs = {lag: [] for lag in lags}

        for date, day_df in rdf.groupby("date_str"):
            if len(day_df) < 60:
                continue
            d = day_df.sort_values("open_time").copy()
            flow = d["net_flow"].values
            ret = d["pct_return"].values

            for lag in lags:
                if lag >= 0:
                    f = flow[:len(flow) - lag] if lag > 0 else flow
                    r = ret[lag:] if lag > 0 else ret
                else:
                    f = flow[-lag:]
                    r = ret[:len(ret) + lag]
                if len(f) < 20:
                    continue
                c = np.corrcoef(f, r)
                if not np.isnan(c[0, 1]):
                    all_corrs[lag].append(c[0, 1])

        bars = []
        for lag in lags:
            vals = all_corrs[lag]
            if vals:
                bars.append({"lag": lag, "corr": float(np.mean(vals)), "n_days": len(vals)})
            else:
                bars.append({"lag": lag, "corr": 0.0, "n_days": 0})

        peak = max(bars, key=lambda x: abs(x["corr"]))
        results[regime] = {"bars": bars, "peak_lag": peak["lag"], "peak_corr": peak["corr"]}

    return results


# --- Flow Extremes ---
def _build_flow_extremes() -> dict:
    """2σ extreme flow events + forward return curves (1-10 bars)."""
    results = {}

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        buy_events = []
        sell_events = []

        for date, day_df in rdf.groupby("date_str"):
            if len(day_df) < 100:
                continue
            d = day_df.sort_values("open_time").reset_index(drop=True)

            # Trailing 1440-bar (prior day) stats — use expanding with 1440 cap
            # For first day in regime, use full day's own stats (unavoidable)
            flow_mean = d["net_flow"].rolling(1440, min_periods=60).mean().shift(1)
            flow_std = d["net_flow"].rolling(1440, min_periods=60).std().shift(1)

            for i in range(60, len(d)):
                if pd.isna(flow_mean.iloc[i]) or flow_std.iloc[i] == 0:
                    continue
                z = (d["net_flow"].iloc[i] - flow_mean.iloc[i]) / flow_std.iloc[i]
                if abs(z) < 2:
                    continue

                # Forward returns (1-10 bars, same day only)
                fwd = {}
                for h in range(1, 11):
                    if i + h < len(d):
                        fwd[h] = (d["close"].iloc[i + h] / d["close"].iloc[i] - 1) * 10000  # bps

                if not fwd:
                    continue

                event = {
                    "nf": float(d["net_flow"].iloc[i]),
                    "z": float(z),
                    "t": d["time"].iloc[i],
                    "fwd": fwd,
                }
                if z > 0:
                    buy_events.append(event)
                else:
                    sell_events.append(event)

        # Build forward curves
        def _fwd_curve(events):
            curve = []
            for h in range(1, 11):
                vals = [e["fwd"][h] for e in events if h in e["fwd"]]
                if vals:
                    curve.append({"bar": h, "avg_bps": float(np.mean(vals)), "n": len(vals)})
                else:
                    curve.append({"bar": h, "avg_bps": 0.0, "n": 0})
            return curve

        results[regime] = {
            "n_buy": len(buy_events),
            "n_sell": len(sell_events),
            "fwd_curve_buy": _fwd_curve(buy_events),
            "fwd_curve_sell": _fwd_curve(sell_events),
            "scatter_buy": buy_events[:500],  # Cap for response size
            "scatter_sell": sell_events[:500],
        }

    return results


# --- Correlation Divergence ---
def _build_corr_divergence() -> dict:
    """Flow-price divergence detector: when rolling corr breaks down."""
    results = {}

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]

        # Build per-day rolling correlations
        day_corrs = []
        for date, day_df in rdf.groupby("date_str"):
            if len(day_df) < 40:
                continue
            d = day_df.sort_values("open_time").copy()
            first_px = d["close"].iloc[0]
            if first_px == 0:
                continue
            d["cum_flow"] = d["net_flow"].cumsum()
            d["cum_return"] = ((d["close"] / first_px) - 1) * 100
            d["roll_corr"] = d["cum_flow"].rolling(20, min_periods=10).corr(d["cum_return"])

            # Trailing 20-bar flow/return direction for divergence classification
            d["trail_flow_20"] = d["net_flow"].rolling(20).sum()
            d["trail_ret_20"] = d["close"].pct_change(20) * 100

            day_corrs.append(d[["date_str", "time", "roll_corr", "trail_flow_20", "trail_ret_20", "close"]].dropna())

        if not day_corrs:
            results[regime] = {"thresholds": {}}
            continue

        all_corr = pd.concat(day_corrs).reset_index(drop=True)

        # Baseline: prior 5 days (7200 bars) rolling stats
        corr_mean = all_corr["roll_corr"].rolling(7200, min_periods=1000).mean().shift(1)
        corr_std = all_corr["roll_corr"].rolling(7200, min_periods=1000).std().shift(1)

        all_corr["corr_z"] = np.where(
            corr_std > 0,
            (all_corr["roll_corr"] - corr_mean) / corr_std,
            0.0,
        )

        thresholds = {}
        for thresh in [-1.0, -1.5, -2.0]:
            mask = all_corr["corr_z"] < thresh

            events = all_corr[mask].copy()
            if len(events) == 0:
                thresholds[str(thresh)] = {"bullish": {"n": 0, "curve": []}, "bearish": {"n": 0, "curve": []}, "noise": {"n": 0, "curve": []}}
                continue

            # Classify: bullish = flow positive + return negative, bearish = opposite
            bullish_mask = (events["trail_flow_20"] > 0) & (events["trail_ret_20"] < 0)
            bearish_mask = (events["trail_flow_20"] < 0) & (events["trail_ret_20"] > 0)
            noise_mask = ~bullish_mask & ~bearish_mask

            # Forward returns from all_corr (need index alignment)
            def _div_fwd_curve(event_indices):
                curve = []
                for h in range(1, 11):
                    vals = []
                    for idx in event_indices:
                        fwd_idx = idx + h
                        if fwd_idx < len(all_corr):
                            fwd_ret = (all_corr["close"].iloc[fwd_idx] / all_corr["close"].iloc[idx] - 1) * 10000
                            vals.append(fwd_ret)
                    if vals:
                        curve.append({"bar": h, "avg_bps": float(np.mean(vals)), "n": len(vals)})
                    else:
                        curve.append({"bar": h, "avg_bps": 0.0, "n": 0})
                return curve

            thresholds[str(thresh)] = {
                "bullish": {"n": int(bullish_mask.sum()), "curve": _div_fwd_curve(events[bullish_mask].index.tolist())},
                "bearish": {"n": int(bearish_mask.sum()), "curve": _div_fwd_curve(events[bearish_mask].index.tolist())},
                "noise": {"n": int(noise_mask.sum()), "curve": _div_fwd_curve(events[noise_mask].index.tolist())},
            }

        results[regime] = {"thresholds": thresholds, "n_events": int(mask.sum())}

    return results


# --- Time-of-Day Profiles ---
def _build_flow_tod() -> dict:
    """Hourly flow profiles (24 buckets), demeaned for net_flow and bar_imbalance.

    Binance taker_buy_volume averages ~48.7% of total volume (not 50%), creating
    a persistent negative bias in raw net_flow and bar_imbalance. To show meaningful
    regime differences, we subtract the overall hourly average (across ALL regimes)
    from each regime's hourly value. This way:
      - BULL shows positive if buying is stronger than average
      - BEAR shows negative if selling is stronger than average
      - CHOP stays near zero
    Volume and avg_trade_size are NOT demeaned (absolute values are meaningful).
    """
    results = {}
    df["hour"] = df["open_time"].dt.hour

    # Compute overall (all-regime) hourly baseline
    baseline = df.groupby("hour").agg(
        baseline_net_flow=("net_flow", "mean"),
        baseline_bar_imb=("bar_imbalance", "mean"),
    ).reset_index()

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        hourly = rdf.groupby("hour").agg(
            avg_net_flow=("net_flow", "mean"),
            avg_bar_imb=("bar_imbalance", "mean"),
            avg_volume=("volume", "mean"),
            avg_trade_size=("avg_trade_size", "mean"),
            n_bars=("close", "count"),
        ).reset_index()

        # Demean: subtract overall hourly average
        hourly = hourly.merge(baseline, on="hour", how="left")
        hourly["avg_net_flow"] = hourly["avg_net_flow"] - hourly["baseline_net_flow"]
        hourly["avg_bar_imb"] = hourly["avg_bar_imb"] - hourly["baseline_bar_imb"]
        hourly.drop(columns=["baseline_net_flow", "baseline_bar_imb"], inplace=True)

        results[regime] = hourly.to_dict("records")

    return results


# --- Session Performance ---
def _build_session_performance() -> dict:
    """Average return per session (Asia/Europe/US)."""
    results = {}
    df["hour"] = df["open_time"].dt.hour
    df["session"] = pd.cut(
        df["hour"],
        bins=[-1, 7, 13, 23],
        labels=["ASIA", "EUROPE", "US"],
    )

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        sessions = {}

        for session_name in ["ASIA", "EUROPE", "US"]:
            sdf = rdf[rdf["session"] == session_name]
            # Per-day session return
            day_sess = sdf.groupby("date_str").agg(
                open=("open", "first"),
                close=("close", "last"),
                volume=("volume", "sum"),
                net_flow=("net_flow", "sum"),
            ).reset_index()
            day_sess["return_pct"] = ((day_sess["close"] / day_sess["open"]) - 1) * 100

            sessions[session_name] = {
                "avg_return": float(day_sess["return_pct"].mean()),
                "std_return": float(day_sess["return_pct"].std()),
                "avg_volume": float(day_sess["volume"].mean()),
                "avg_net_flow": float(day_sess["net_flow"].mean()),
                "n_days": len(day_sess),
            }

        results[regime] = sessions

    return results


# --- Session Flow → Next Session Return ---
def _build_session_flow_fwd() -> dict:
    """Does one session's flow predict the next session's return?"""
    results = {}
    df["hour"] = df["open_time"].dt.hour

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        pairs = {}

        for date, day_df in rdf.groupby("date_str"):
            d = day_df.sort_values("open_time")
            asia = d[d["hour"].between(0, 7)]
            europe = d[d["hour"].between(8, 13)]
            us = d[d["hour"].between(14, 23)]

            if len(asia) > 0 and len(europe) > 0:
                asia_flow = float(asia["net_flow"].sum())
                eu_open = float(europe["open"].iloc[0])
                eu_close = float(europe["close"].iloc[-1])
                if eu_open > 0:
                    eu_ret = (eu_close / eu_open - 1) * 100
                    pairs.setdefault("asia_to_europe", []).append({"flow": asia_flow, "return": eu_ret, "date": date})

            if len(europe) > 0 and len(us) > 0:
                eu_flow = float(europe["net_flow"].sum())
                us_open = float(us["open"].iloc[0])
                us_close = float(us["close"].iloc[-1])
                if us_open > 0:
                    us_ret = (us_close / us_open - 1) * 100
                    pairs.setdefault("europe_to_us", []).append({"flow": eu_flow, "return": us_ret, "date": date})

        # Compute correlations
        regime_result = {}
        for pair_name, points in pairs.items():
            flows = [p["flow"] for p in points]
            rets = [p["return"] for p in points]
            corr = float(np.corrcoef(flows, rets)[0, 1]) if len(points) > 10 else 0.0
            regime_result[pair_name] = {
                "points": points[:500],
                "correlation": corr,
                "n_days": len(points),
            }

        results[regime] = regime_result

    return results


# --- Whale Activity ---
def _build_whale_activity() -> dict:
    """Daily avg_trade_size z-score vs trailing 1440-bar baseline."""
    daily = df.groupby("date_str").agg(
        avg_trade_size=("avg_trade_size", "mean"),
        close=("close", "last"),
        regime=("regime", "first"),
    ).reset_index()
    daily.sort_values("date_str", inplace=True)

    # Rolling 30-day baseline (not 1440 bars, since this is daily)
    daily["ats_mean"] = daily["avg_trade_size"].rolling(30, min_periods=7).mean().shift(1)
    daily["ats_std"] = daily["avg_trade_size"].rolling(30, min_periods=7).std().shift(1)
    daily["whale_z"] = np.where(
        daily["ats_std"] > 0,
        (daily["avg_trade_size"] - daily["ats_mean"]) / daily["ats_std"],
        0.0,
    )

    return daily[["date_str", "avg_trade_size", "whale_z", "close", "regime"]].dropna().to_dict("records")


# --- Flow Persistence (ACF) ---
def _build_flow_persistence() -> dict:
    """Autocorrelation of bar_imbalance at lags 1-30."""
    results = {}

    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = df[df["regime"] == regime]
        acf_by_lag = {lag: [] for lag in range(1, 31)}

        for date, day_df in rdf.groupby("date_str"):
            if len(day_df) < 60:
                continue
            imb = day_df.sort_values("open_time")["bar_imbalance"].values
            for lag in range(1, 31):
                if lag >= len(imb):
                    break
                c = np.corrcoef(imb[:-lag], imb[lag:])
                if not np.isnan(c[0, 1]):
                    acf_by_lag[lag].append(c[0, 1])

        bars = []
        for lag in range(1, 31):
            vals = acf_by_lag[lag]
            bars.append({"lag": lag, "acf": float(np.mean(vals)) if vals else 0.0, "n_days": len(vals)})

        results[regime] = bars

    return results


# --- Flow Classification ---
def _build_flow_classification() -> dict:
    """Daily classification: flow aligned with price or divergent."""
    daily = DAILY_AGG.copy()

    daily["return_bps"] = daily["return_pct"] * 100
    daily["classification"] = np.where(
        (daily["net_flow"] > 0) & (daily["return_pct"] > 0), "ALIGNED",
        np.where(
            (daily["net_flow"] < 0) & (daily["return_pct"] < 0), "ALIGNED",
            np.where(
                abs(daily["return_pct"]) < 0.1, "NEUTRAL", "DIVERGENT"
            )
        )
    )

    results = {}
    for regime in ["BULL", "BEAR", "CHOP"]:
        rdf = daily[daily["regime"] == regime]
        n = len(rdf)
        if n == 0:
            results[regime] = {"n_total": 0, "aligned_pct": 0, "divergent_pct": 0, "neutral_pct": 0}
            continue

        counts = rdf["classification"].value_counts().to_dict()
        results[regime] = {
            "n_total": n,
            "n_aligned": counts.get("ALIGNED", 0),
            "n_divergent": counts.get("DIVERGENT", 0),
            "n_neutral": counts.get("NEUTRAL", 0),
            "aligned_pct": round(counts.get("ALIGNED", 0) / n * 100, 1),
            "divergent_pct": round(counts.get("DIVERGENT", 0) / n * 100, 1),
            "neutral_pct": round(counts.get("NEUTRAL", 0) / n * 100, 1),
            "avg_return_aligned": float(rdf[rdf["classification"] == "ALIGNED"]["return_pct"].mean()) if counts.get("ALIGNED", 0) > 0 else 0,
            "avg_return_divergent": float(rdf[rdf["classification"] == "DIVERGENT"]["return_pct"].mean()) if counts.get("DIVERGENT", 0) > 0 else 0,
            "days": rdf[["date_str", "net_flow", "return_pct", "classification"]].to_dict("records"),
        }

    return results


# --- Volume Trend ---
def _build_volume_trend() -> list:
    """Rolling 30-day avg daily volume + price overlay."""
    daily = DAILY_AGG.copy().sort_values("date_str")
    daily["roll_vol_30d"] = daily["total_vol"].rolling(30, min_periods=7).mean()
    daily["roll_quote_vol_30d"] = daily["total_quote_vol"].rolling(30, min_periods=7).mean()

    return daily[["date_str", "close_px", "total_vol", "roll_vol_30d", "roll_quote_vol_30d", "regime"]].dropna().to_dict("records")


# --- Regime Daily (for period overlay charts) ---
def _build_regime_daily() -> dict:
    """Daily aggregates within each regime period, with cum_flow and cum_return."""
    results = {}

    for period in REGIMES["periods"]:
        regime = period["regime"]
        start = period["start"]
        end = period["end"]

        mask = (DAILY_AGG["date_str"] >= start) & (DAILY_AGG["date_str"] <= end)
        pdays = DAILY_AGG[mask].sort_values("date_str").copy()

        if len(pdays) == 0:
            continue

        pdays["day_num"] = range(len(pdays))
        pdays["cum_flow"] = pdays["net_flow"].cumsum()
        first_px = pdays["open_px"].iloc[0]
        if first_px > 0:
            pdays["cum_return"] = ((pdays["close_px"] / first_px) - 1) * 100
        else:
            pdays["cum_return"] = 0.0

        # Normalize cum_flow to match cum_return scale
        flow_std = pdays["cum_flow"].std()
        ret_std = pdays["cum_return"].std()
        if flow_std > 0 and ret_std > 0:
            pdays["cum_flow_norm"] = pdays["cum_flow"] / flow_std * ret_std
        else:
            pdays["cum_flow_norm"] = pdays["cum_flow"]

        key = f"{start}_{end}"
        results[key] = {
            "regime": regime,
            "start": start,
            "end": end,
            "n_days": len(pdays),
            "return_pct": float(period["return_pct"]),
            "days": pdays[["day_num", "date_str", "close_px", "net_flow", "cum_flow",
                           "cum_flow_norm", "cum_return", "day_imb", "total_vol", "return_pct"]].to_dict("records"),
        }

    return results


# ── Run All Pre-computations ────────────────────────────────────────────

print("Computing intraday correlation...")
INTRADAY_CORR = _build_intraday_corr()

print("Computing lead-lag...")
LEAD_LAG = _build_lead_lag()

print("Computing flow extremes...")
FLOW_EXTREMES = _build_flow_extremes()

print("Computing correlation divergence...")
CORR_DIVERGENCE = _build_corr_divergence()

print("Computing time-of-day profiles...")
FLOW_TOD = _build_flow_tod()

print("Computing session performance...")
SESSION_PERF = _build_session_performance()

print("Computing session flow forward...")
SESSION_FLOW_FWD = _build_session_flow_fwd()

print("Computing whale activity...")
WHALE_ACTIVITY = _build_whale_activity()

print("Computing flow persistence...")
FLOW_PERSISTENCE = _build_flow_persistence()

print("Computing flow classification...")
FLOW_CLASSIFICATION = _build_flow_classification()

print("Computing volume trend...")
VOLUME_TREND = _build_volume_trend()

print("Computing regime daily overlays...")
REGIME_DAILY = _build_regime_daily()

print("All pre-computations complete!")


# ── Background Data Sync ────────────────────────────────────────────────

def _fetch_klines_batch(start_ms):
    """Fetch up to 1000 1-min klines from Binance starting at start_ms."""
    params = {
        "symbol": "BTCUSDT", "interval": "1m",
        "startTime": start_ms, "limit": 1000,
    }
    resp = requests.get(BINANCE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _raw_to_df(raw_bars):
    """Convert raw Binance kline arrays to a typed DataFrame with derived columns."""
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "num_trades",
            "taker_buy_vol", "taker_buy_quote_vol", "ignore"]
    new = pd.DataFrame(raw_bars, columns=cols)
    new["open_time"] = pd.to_datetime(new["open_time"], unit="ms", utc=True)
    new["close_time"] = pd.to_datetime(new["close_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "quote_volume",
              "taker_buy_vol", "taker_buy_quote_vol"]:
        new[c] = new[c].astype(float)
    new["num_trades"] = new["num_trades"].astype(int)
    new.drop(columns=["ignore"], inplace=True)
    new["buy_vol"] = new["taker_buy_vol"]
    new["sell_vol"] = new["volume"] - new["taker_buy_vol"]
    new["net_flow"] = new["buy_vol"] - new["sell_vol"]
    new["bar_imbalance"] = np.where(new["volume"] > 0, new["net_flow"] / new["volume"], 0.0)
    new["pct_return"] = (new["close"] / new["open"] - 1) * 100
    new["avg_trade_size"] = np.where(new["num_trades"] > 0, new["volume"] / new["num_trades"], 0.0)
    new["date_utc"] = new["open_time"].dt.date.astype(str)
    new["time"] = new["open_time"].dt.strftime("%H:%M")
    new["date_str"] = new["date_utc"]
    return new


def _sync_once():
    """Fetch new bars from Binance, append to df, recompute all analytics."""
    global df, DAILY_AGG, REGIMES
    global INTRADAY_CORR, LEAD_LAG, FLOW_EXTREMES, CORR_DIVERGENCE
    global FLOW_TOD, SESSION_PERF, SESSION_FLOW_FWD, WHALE_ACTIVITY
    global FLOW_PERSISTENCE, FLOW_CLASSIFICATION, VOLUME_TREND, REGIME_DAILY

    last_ts = df["open_time"].max()
    start_ms = int(last_ts.timestamp() * 1000) + 60_000
    log.info(f"Sync: fetching bars since {last_ts}...")

    all_bars = []
    cursor_ms = start_ms
    while True:
        raw = _fetch_klines_batch(cursor_ms)
        if not raw:
            break
        all_bars.extend(raw)
        cursor_ms = raw[-1][0] + 60_000
        if len(raw) < 1000:
            break
        time_module.sleep(0.6)

    if not all_bars:
        log.info("Sync: no new bars")
        return

    new_df = _raw_to_df(all_bars)
    log.info(f"Sync: fetched {len(new_df)} new bars")

    # Merge
    merged = pd.concat([df, new_df], ignore_index=True)
    merged.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
    merged.sort_values("open_time", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    # Save to disk (survives container restarts within same deployment)
    try:
        merged.to_parquet(DATA_PATH, index=False)
        log.info(f"Sync: saved {len(merged)} bars to {DATA_PATH}")
    except Exception as e:
        log.warning(f"Sync: failed to save parquet: {e}")

    # Swap df and recompute
    df = merged

    log.info("Sync: recomputing all analytics...")
    new_regimes = _detect_regimes()
    df["regime"] = df["date_str"].map(new_regimes["map"]).fillna("CHOP")

    new_daily_agg = _build_daily_agg()
    new_intraday_corr = _build_intraday_corr()
    new_lead_lag = _build_lead_lag()
    new_flow_extremes = _build_flow_extremes()
    new_corr_div = _build_corr_divergence()
    new_flow_tod = _build_flow_tod()
    new_session_perf = _build_session_performance()
    new_session_flow = _build_session_flow_fwd()
    new_whale = _build_whale_activity()
    new_persistence = _build_flow_persistence()
    new_classification = _build_flow_classification()
    new_volume = _build_volume_trend()
    new_regime_daily = _build_regime_daily()

    # Atomic swap under lock
    with data_lock:
        REGIMES = new_regimes
        DAILY_AGG = new_daily_agg
        INTRADAY_CORR = new_intraday_corr
        LEAD_LAG = new_lead_lag
        FLOW_EXTREMES = new_flow_extremes
        CORR_DIVERGENCE = new_corr_div
        FLOW_TOD = new_flow_tod
        SESSION_PERF = new_session_perf
        SESSION_FLOW_FWD = new_session_flow
        WHALE_ACTIVITY = new_whale
        FLOW_PERSISTENCE = new_persistence
        FLOW_CLASSIFICATION = new_classification
        VOLUME_TREND = new_volume
        REGIME_DAILY = new_regime_daily

    log.info(f"Sync: complete. {len(df)} total bars, {df['date_str'].nunique()} days")


def _sync_loop():
    """Background loop: sync every SYNC_INTERVAL_S seconds."""
    while True:
        time_module.sleep(SYNC_INTERVAL_S)
        try:
            _sync_once()
        except Exception as e:
            log.error(f"Sync failed: {e}")


# Start background sync thread
_sync_thread = threading.Thread(target=_sync_loop, daemon=True)
_sync_thread.start()
log.info(f"Background sync started (every {SYNC_INTERVAL_S // 3600}h)")


# ── API Endpoints ───────────────────────────────────────────────────────

@app.get("/api/dates")
def get_dates():
    """All dates with daily summary stats."""
    result = []
    for _, row in DAILY_AGG.iterrows():
        result.append({
            "date": row["date_str"],
            "regime": row["regime"],
            "total_vol": float(row["total_vol"]),
            "total_buy": float(row["total_buy"]),
            "total_sell": float(row["total_sell"]),
            "net_flow": float(row["net_flow"]),
            "open": float(row["open_px"]),
            "close": float(row["close_px"]),
            "high": float(row["high_px"]),
            "low": float(row["low_px"]),
            "return_pct": round(float(row["return_pct"]), 4),
            "day_imb": round(float(row["day_imb"]), 6),
            "n_bars": int(row["n_bars"]),
            "avg_trade_size": float(row["avg_trade_size"]),
            "total_trades": int(row["total_trades"]),
        })
    result.sort(key=lambda x: x["date"], reverse=True)
    return result


@app.get("/api/day/{date}")
def get_day(date: str):
    """Minute-level bars + stats for a single UTC day."""
    day_df = df[df["date_str"] == date].copy().sort_values("open_time")
    if len(day_df) == 0:
        return {"bars": [], "stats": {}}

    first_px = day_df["close"].iloc[0]
    day_df["cum_flow"] = day_df["net_flow"].cumsum()
    day_df["cum_return"] = ((day_df["close"] / first_px) - 1) * 100 if first_px > 0 else 0.0
    day_df["rolling_imb_20"] = day_df["bar_imbalance"].rolling(20, min_periods=1).mean()

    bars = []
    for _, row in day_df.iterrows():
        bars.append({
            "time": row["time"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "quote_volume": float(row["quote_volume"]),
            "buy_vol": float(row["buy_vol"]),
            "sell_vol": float(row["sell_vol"]),
            "net_flow": float(row["net_flow"]),
            "bar_imb": float(row["bar_imbalance"]),
            "cum_flow": float(row["cum_flow"]),
            "cum_return": float(row["cum_return"]),
            "rolling_imb_20": float(row["rolling_imb_20"]),
            "num_trades": int(row["num_trades"]),
            "avg_trade_size": float(row["avg_trade_size"]),
        })

    # Day stats
    stats = {
        "total_vol": float(day_df["volume"].sum()),
        "total_buy": float(day_df["buy_vol"].sum()),
        "total_sell": float(day_df["sell_vol"].sum()),
        "net_flow": float(day_df["net_flow"].sum()),
        "open": float(day_df["open"].iloc[0]),
        "close": float(day_df["close"].iloc[-1]),
        "high": float(day_df["high"].max()),
        "low": float(day_df["low"].min()),
        "return_pct": float(((day_df["close"].iloc[-1] / day_df["open"].iloc[0]) - 1) * 100),
        "n_bars": len(day_df),
        "day_imb": float((day_df["buy_vol"].sum() - day_df["sell_vol"].sum()) / day_df["volume"].sum()) if day_df["volume"].sum() > 0 else 0.0,
        "total_trades": int(day_df["num_trades"].sum()),
        "avg_trade_size": float(day_df["avg_trade_size"].mean()),
        "regime": day_df["regime"].iloc[0],
    }

    return {"bars": bars, "stats": stats}


@app.get("/api/regimes")
def get_regimes():
    """Regime metadata — all detected periods."""
    return {
        "periods": REGIMES["periods"],
        "summary": {
            regime: {
                "n_days": int((df["regime"] == regime).sum() / 1440),
                "pct": round((df["regime"] == regime).sum() / len(df) * 100, 1),
            }
            for regime in ["BULL", "BEAR", "CHOP"]
        },
    }


@app.get("/api/regime-daily-all")
def get_regime_daily_all():
    """Daily aggregates within each regime period, for overlay charts."""
    return REGIME_DAILY


@app.get("/api/intraday-correlation-all")
def get_intraday_corr():
    """Rolling flow-price correlation by hour of day, per regime."""
    return INTRADAY_CORR


@app.get("/api/lead-lag-all")
def get_lead_lag():
    """Cross-correlation at lags -20 to +20, per regime."""
    return LEAD_LAG


@app.get("/api/flow-extremes-all")
def get_flow_extremes():
    """2σ extreme flow events + forward return curves, per regime."""
    return FLOW_EXTREMES


@app.get("/api/corr-divergence-all")
def get_corr_divergence():
    """Flow-price divergence detector, per regime."""
    return CORR_DIVERGENCE


@app.get("/api/flow-tod-all")
def get_flow_tod():
    """Time-of-day profiles (hourly buckets), per regime."""
    return FLOW_TOD


@app.get("/api/session-performance-all")
def get_session_performance():
    """Average session return (Asia/Europe/US), per regime."""
    return SESSION_PERF


@app.get("/api/session-flow-fwd-all")
def get_session_flow_fwd():
    """Session flow → next session return, per regime."""
    return SESSION_FLOW_FWD


@app.get("/api/whale-activity")
def get_whale_activity():
    """Daily avg_trade_size z-score (whale detection)."""
    return WHALE_ACTIVITY


@app.get("/api/flow-persistence-all")
def get_flow_persistence():
    """Autocorrelation of bar_imbalance at lags 1-30, per regime."""
    return FLOW_PERSISTENCE


@app.get("/api/flow-classification-all")
def get_flow_classification():
    """Daily aligned vs divergent classification, per regime."""
    return FLOW_CLASSIFICATION


@app.get("/api/volume-trend")
def get_volume_trend():
    """Rolling 30-day avg daily volume + price overlay."""
    return VOLUME_TREND


# ── Blog Static Files ──────────────────────────────────────────────────

BLOG_DIR = os.path.join(os.path.dirname(__file__), "..", "blog-static")


@app.get("/btc-flow/{path:path}")
def serve_blog(path: str):
    """Serve pre-generated blog pages from blog-static/btc-flow/."""
    if not os.path.exists(BLOG_DIR):
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    file_path = os.path.join(BLOG_DIR, "btc-flow", path)
    # /btc-flow/2024-01-01/ → blog-static/btc-flow/2024-01-01/index.html
    if os.path.isdir(file_path):
        index = os.path.join(file_path, "index.html")
        if os.path.isfile(index):
            return FileResponse(index, media_type="text/html")
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/sitemap.xml")
def serve_sitemap():
    """Serve sitemap.xml from blog-static/."""
    sitemap = os.path.join(BLOG_DIR, "sitemap.xml")
    if os.path.isfile(sitemap):
        return FileResponse(sitemap, media_type="application/xml")
    return {"error": "not found"}


@app.get("/robots.txt")
def serve_robots():
    """Serve robots.txt from blog-static/."""
    robots = os.path.join(BLOG_DIR, "robots.txt")
    if os.path.isfile(robots):
        return FileResponse(robots, media_type="text/plain")
    return FileResponse(robots, media_type="text/plain")


# ── Static Files (Frontend) ────────────────────────────────────────────

STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(STATIC_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(STATIC_DIR, "assets")), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        """Serve React SPA for all non-API routes."""
        file_path = os.path.join(STATIC_DIR, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))
