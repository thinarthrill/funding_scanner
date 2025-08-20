#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding Signaler (cron-friendly, single-run):
- Scans perpetual funding on Binance, Bybit, OKX
- Opens/closes delta-neutral hedge signals based on APR thresholds
- Keeps state in CSV files (positions, signals log)
- Optional Telegram notifications
- Designed to be run every 15 minutes by cron/Render

Author: ChatGPT (GPT-5 Thinking)
License: MIT
"""
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# deps
try:
    import requests
    import pandas as pd
except Exception as e:
    print("Install deps first: pip install requests pandas", file=sys.stderr)
    raise

# ------------------------------
# Config & logging
# ------------------------------
DEFAULT_EXCHANGES = ["binance", "bybit", "okx"]
DEFAULT_SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","BNBUSDT",
    "AVAXUSDT","LINKUSDT","ADAUSDT","TONUSDT","OPUSDT","ARBUSDT","PEPEUSDT"
]
DEFAULT_ENTRY_APR = 15.0     # % APR to OPEN
DEFAULT_EXIT_APR  = 8.0      # % APR to CLOSE
DEFAULT_MAX_HOLD_H = 48       # force close after N hours
DEFAULT_NOTIONAL = None       # e.g. 10000 to estimate payouts
DEFAULT_RAW_CSV = None
DEFAULT_LOG_CSV = "signals_log.csv"
DEFAULT_POS_CSV = "positions.csv"
DEFAULT_TIMEOUT = 12
USER_AGENT = "FundingSignaler/1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# ------------------------------
# Helpers
# ------------------------------
def utc_ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def annualize_from_8h(rate_8h: float) -> float:
    # simple APR (fraction), 3 periods per day * 365
    return rate_8h * 3 * 365

def fmt_ts(ms: Optional[int]) -> Optional[str]:
    if not ms: return None
    try:
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return None

def suggestion_from_rate(rate_8h: Optional[float]) -> str:
    if rate_8h is None: return "n/a"
    if rate_8h > 0: return "SHORT_PERP_LONG_SPOT"  # shorts receive funding
    if rate_8h < 0: return "LONG_PERP_SHORT_SPOT"  # longs receive funding
    return "NEUTRAL"

def maybe_send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.debug("Telegram not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
        r = SESSION.post(url, json=payload, timeout=DEFAULT_TIMEOUT)
        if r.status_code != 200:
            logging.warning("Telegram send failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:
        logging.warning("Telegram exception: %s", e)

def hedge_qty(notional_usd: Optional[float], price: Optional[float]) -> Optional[float]:
    if notional_usd is None or price is None or price <= 0:
        return None
    return round(float(notional_usd) / float(price), 8)

def payout_8h_usd(rate_8h: Optional[float], notional: Optional[float]) -> Optional[float]:
    if rate_8h is None or notional is None:
        return None
    return round(rate_8h * float(notional), 4)

def payout_day_usd(rate_8h: Optional[float], notional: Optional[float]) -> Optional[float]:
    if rate_8h is None or notional is None:
        return None
    return round(3 * rate_8h * float(notional), 4)

# ------------------------------
# Exchange clients (public REST)
# ------------------------------
def binance_premium_index(symbol: str) -> Optional[Dict[str, Any]]:
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    try:
        r = SESSION.get(url, params={"symbol": symbol.upper()}, timeout=DEFAULT_TIMEOUT)
        if r.status_code != 200:
            logging.debug("Binance %s -> %s", symbol, r.text[:200])
            return None
        j = r.json()
        return {
            "exchange": "binance",
            "symbol": symbol.upper(),
            "price": to_float(j.get("markPrice")),
            "rate_8h": to_float(j.get("lastFundingRate")),
            "next_funding_time": int(j.get("nextFundingTime")) if j.get("nextFundingTime") else None,
            "ts": int(j.get("time")) if j.get("time") else utc_ms_now(),
        }
    except Exception as e:
        logging.debug("Binance error %s: %s", symbol, e)
        return None

def fetch_bybit_mark_price(symbol: str) -> Optional[float]:
    try:
        url = "https://api.bybit.com/v5/market/tickers"
        params = {"category": "linear", "symbol": symbol.upper()}
        r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            rows = (j.get("result") or {}).get("list") or []
            if rows:
                return to_float(rows[0].get("lastPrice"))
    except Exception as e:
        logging.debug("Bybit price error %s: %s", symbol, e)
    return None

def bybit_latest_funding(symbol: str) -> Optional[Dict[str, Any]]:
    # v5 funding history (last event)
    try:
        url = "https://api.bybit.com/v5/market/funding/history"
        params = {"category": "linear", "symbol": symbol.upper(), "limit": 1}
        r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            rows = (j.get("result") or {}).get("list") or []
            if rows:
                row = rows[0]
                rate = to_float(row.get("fundingRate"))
                ts = int(row.get("fundingTime")) if row.get("fundingTime") else utc_ms_now()
                price = fetch_bybit_mark_price(symbol.upper())
                return {
                    "exchange": "bybit",
                    "symbol": symbol.upper(),
                    "price": price,
                    "rate_8h": rate,
                    "next_funding_time": None,
                    "ts": ts,
                }
    except Exception as e:
        logging.debug("Bybit v5 error %s: %s", symbol, e)
    return None

def okx_inst_id(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}-USDT-SWAP"
    if s.endswith("USD"):
        base = s[:-3]
        return f"{base}-USD-SWAP"
    return f"{s}-USDT-SWAP"

def okx_mark_price(inst_id: str) -> Optional[float]:
    try:
        url = "https://www.okx.com/api/v5/public/mark-price"
        params = {"instType": "SWAP", "instId": inst_id}
        r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            data = j.get("data") or []
            if data:
                return to_float(data[0].get("markPx"))
    except Exception as e:
        logging.debug("OKX mark price error %s: %s", inst_id, e)
    return None

def okx_funding(symbol: str) -> Optional[Dict[str, Any]]:
    inst_id = okx_inst_id(symbol)
    url = "https://www.okx.com/api/v5/public/funding-rate"
    try:
        r = SESSION.get(url, params={"instId": inst_id}, timeout=DEFAULT_TIMEOUT)
        if r.status_code != 200:
            logging.debug("OKX %s -> %s", inst_id, r.text[:200])
            return None
        j = r.json()
        data = j.get("data") or []
        if not data:
            return None
        d0 = data[0]
        rate = to_float(d0.get("fundingRate"))
        next_time = int(d0.get("nextFundingTime")) if d0.get("nextFundingTime") else None
        price = okx_mark_price(inst_id)
        return {
            "exchange": "okx",
            "symbol": symbol.upper(),
            "instId": inst_id,
            "price": price,
            "rate_8h": rate,
            "next_funding_time": next_time,
            "ts": utc_ms_now(),
        }
    except Exception as e:
        logging.debug("OKX error %s: %s", symbol, e)
        return None

# ------------------------------
# State & I/O
# ------------------------------
def read_csv(path: str, columns: List[str]) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path)
        # ensure columns
        for c in columns:
            if c not in df.columns:
                df[c] = None
        return df[columns]
    except Exception:
        return pd.DataFrame(columns=columns)

def write_csv(path: str, df: pd.DataFrame) -> None:
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def append_csv(path: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=header, index=False)

# ------------------------------
# Core scan & signal logic
# ------------------------------
def scan_all(exchanges: List[str], symbols: List[str]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for ex in exchanges:
        for sym in symbols:
            row = None
            if ex == "binance":
                row = binance_premium_index(sym)
            elif ex == "bybit":
                row = bybit_latest_funding(sym)
            elif ex == "okx":
                row = okx_funding(sym)
            if not row: 
                continue
            r8 = row.get("rate_8h")
            apr = annualize_from_8h(r8) if r8 is not None else None
            row["apr"] = apr
            row["apr_pct"] = round(apr*100, 4) if apr is not None else None
            row["rate_8h_pct"] = round(r8*100, 6) if r8 is not None else None
            row["time_utc"] = fmt_ts(row.get("ts"))
            row["next_funding_utc"] = fmt_ts(row.get("next_funding_time"))
            row["direction"] = suggestion_from_rate(r8)
            rows.append(row)
    return pd.DataFrame(rows)

def main():
    p = argparse.ArgumentParser(description="Funding signaler (single-run for cron).")
    p.add_argument("--exchanges", nargs="+", default=DEFAULT_EXCHANGES)
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    p.add_argument("--entry-apr", type=float, default=DEFAULT_ENTRY_APR, help="Open when |APR| >= this percent")
    p.add_argument("--exit-apr",  type=float, default=DEFAULT_EXIT_APR,  help="Close when |APR| < this percent")
    p.add_argument("--max-holding-h", type=float, default=DEFAULT_MAX_HOLD_H, help="Force close after N hours")
    p.add_argument("--notional", type=float, default=DEFAULT_NOTIONAL, help="Notional per leg (USD) for payout estimates")
    p.add_argument("--raw-csv", default=DEFAULT_RAW_CSV, help="Append all scans here (optional)")
    p.add_argument("--log-csv", default=DEFAULT_LOG_CSV, help="Signals log CSV")
    p.add_argument("--positions-csv", default=DEFAULT_POS_CSV, help="Active positions CSV")
    p.add_argument("--notify", action="store_true", help="Send Telegram messages")
    p.add_argument("--dry-run", action="store_true", help="Do not modify positions/log, just print/notify")
    p.add_argument("--debug", action="store_true", help="Verbose logging")
    args = p.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # scan
    df = scan_all([x.lower() for x in args.exchanges], [x.upper() for x in args.symbols])

    # raw logging
    if args.raw_csv:
        append_csv(args.raw_csv, df)

    # load state
    pos_cols = ["exchange","symbol","direction","entry_time_utc","entry_ts","entry_rate_8h","entry_apr_pct","entry_price","notional_usd"]
    positions = read_csv(args.positions_csv, pos_cols)

    # prepare logs
    log_cols = ["time_utc","action","exchange","symbol","direction","rate_8h","apr_pct","price","notional_usd","payout_8h_usd","payout_day_usd","reason"]
    batch_logs = []

    # helpers to find current row
    def current_row(ex, sym) -> Optional[pd.Series]:
        sub = df[(df["exchange"]==ex) & (df["symbol"]==sym)]
        if sub.empty: return None
        return sub.iloc[0]

    now_ms = utc_ms_now()
    now_utc = fmt_ts(now_ms)

    # ---- CLOSE logic for existing positions ----
    to_remove_idx = []
    for i, pos in positions.iterrows():
        ex, sym, dir_ = pos["exchange"], pos["symbol"], pos["direction"]
        entry_ts = pos["entry_ts"]
        cr = current_row(ex, sym)
        if cr is None:
            logging.debug("No fresh row for %s %s", ex, sym)
            continue

        apr_abs = abs(cr["apr_pct"]) if cr["apr_pct"] is not None else None
        sign_flip = cr["direction"] != dir_ and cr["direction"] in ["SHORT_PERP_LONG_SPOT","LONG_PERP_SHORT_SPOT"]
        holding_h = None
        if entry_ts:
            holding_h = round((now_ms - int(entry_ts)) / (1000*3600), 2)

        reason = None
        if apr_abs is not None and apr_abs < float(args.exit_apr):
            reason = f"APR {apr_abs}% < exit {args.exit_apr}%"
        if reason is None and holding_h is not None and holding_h >= float(args.max_holding_h):
            reason = f"Max holding {holding_h}h ≥ {args.max_holding_h}h"
        if reason is None and sign_flip:
            reason = f"Direction flip {dir_} -> {cr['direction']}"

        if reason:
            # log CLOSE
            payout8 = payout_8h_usd(cr["rate_8h"], args.notional)
            payoutd = payout_day_usd(cr["rate_8h"], args.notional)
            batch_logs.append({
                "time_utc": now_utc, "action":"CLOSE",
                "exchange": ex, "symbol": sym, "direction": dir_,
                "rate_8h": cr["rate_8h"], "apr_pct": cr["apr_pct"], "price": cr["price"],
                "notional_usd": args.notional, "payout_8h_usd": payout8, "payout_day_usd": payoutd,
                "reason": reason
            })
            msg = (f"✅ <b>Funding CLOSE</b>\n"
                   f"{ex.upper()} {sym}\n"
                   f"APR: {cr['apr_pct']}% | 8h: {round((cr['rate_8h'] or 0)*100,6)}%\n"
                   f"Dir: {dir_}\nReason: {reason}")
            if args.notify: maybe_send_telegram(msg)
            to_remove_idx.append(i)

    if not args.dry_run and to_remove_idx:
        positions = positions.drop(index=to_remove_idx).reset_index(drop=True)

    # ---- OPEN logic for new opportunities ----
    for _, r in df.iterrows():
        ex, sym, dir_ = r["exchange"], r["symbol"], r["direction"]
        if dir_ not in ["SHORT_PERP_LONG_SPOT","LONG_PERP_SHORT_SPOT"]:
            continue
        apr_abs = abs(r["apr_pct"]) if r["apr_pct"] is not None else None
        if apr_abs is None or apr_abs < float(args.entry_apr):
            continue
        # skip if already open
        already = positions[(positions["exchange"]==ex) & (positions["symbol"]==sym)]
        if not already.empty:
            continue

        # OPEN
        entry_qty = hedge_qty(args.notional, r["price"])
        payout8 = payout_8h_usd(r["rate_8h"], args.notional)
        payoutd = payout_day_usd(r["rate_8h"], args.notional)

        if args.notify:
            direction_human = "Short perp & Long spot" if dir_=="SHORT_PERP_LONG_SPOT" else "Long perp & Short spot"
            nf = r.get("next_funding_utc") or "n/a"
            msg = (f"🚀 <b>Funding OPEN</b>\n"
                   f"{ex.upper()} {sym}\n"
                   f"APR: {r['apr_pct']}% | 8h: {round((r['rate_8h'] or 0)*100,6)}%\n"
                   f"Dir: {direction_human}\n"
                   f"Price: {r['price']} | Qty(est): {entry_qty}\n"
                   f"Next funding: {nf}\n"
                   f"Payout est: 8h ${payout8} / day ${payoutd}")
            maybe_send_telegram(msg)

        # persist position & log
        new_pos = {
            "exchange": ex, "symbol": sym, "direction": dir_,
            "entry_time_utc": r["time_utc"], "entry_ts": r["ts"],
            "entry_rate_8h": r["rate_8h"], "entry_apr_pct": r["apr_pct"],
            "entry_price": r["price"], "notional_usd": args.notional
        }
        if not args.dry_run:
            positions = pd.concat([positions, pd.DataFrame([new_pos])], ignore_index=True)

        batch_logs.append({
            "time_utc": now_utc, "action":"OPEN",
            "exchange": ex, "symbol": sym, "direction": dir_,
            "rate_8h": r["rate_8h"], "apr_pct": r["apr_pct"], "price": r["price"],
            "notional_usd": args.notional, "payout_8h_usd": payout8, "payout_day_usd": payoutd,
            "reason": f"|APR| {apr_abs}% ≥ entry {args.entry_apr}%"
        })

    # write out
    if not args.dry_run:
        write_csv(args.positions_csv, positions)
        if batch_logs:
            append_csv(args.log_csv, pd.DataFrame(batch_logs))

    # friendly stdout for cron logs
    if not df.empty:
        printable = df[["exchange","symbol","price","rate_8h","rate_8h_pct","apr_pct","time_utc","next_funding_utc","direction"]]
        print(printable.to_string(index=False))
    else:
        print("No data rows.")

if __name__ == "__main__":
    main()
