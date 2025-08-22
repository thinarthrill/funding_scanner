#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding Signaler (env-driven, cron-friendly, GCS-aware)

ENV (все необязательны, даны значения по умолчанию):
  # Биржи / символы
  EXCHANGES=binance,bybit,okx
  SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
  SYMBOLS_SOURCE=binance-top           # или empty для ручного списка из SYMBOLS
  TOP_N=200
  MIN_QUOTE_USDT=10000000
  SAVE_SYMBOLS_PATH=gs://bucket/symbols_active.txt  # или локальный путь, либо пусто

  # Пороговые значения и режим удержания
  ENTRY_APR=30           # % для открытия
  EXIT_APR=12            # % для закрытия
  MAX_HOLDING_H=48

  # Капитал / маржа / комиссии / заём / горизонт удержания
  NOTIONAL=              # если задан, переопределяет расчёт из CAPITAL/LEVERAGE
  CAPITAL=1000
  PERP_LEVERAGE=5
  TAKER_FEE=0.0005       # 0.05%
  BORROW_APR=0.10        # 10% годовых
  EXPECTED_HOLDING_H=24

  # Публикация в Телеграм
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...
  TOP_N_TELEGRAM=3

  # Ротация (держим одну позицию)
  ROTATE=true            # true/false
  ROTATE_DELTA_USD=0.5   # минимальное улучшение net/day ($/day) для ротации

  # Пути CSV (локально или GCS: gs://bucket/path.csv)
  RAW_CSV_PATH=
  LOG_CSV_PATH=gs://bucket/funding/signals_log.csv
  POSITIONS_CSV_PATH=gs://bucket/funding/positions.csv

  # Прочее
  DEBUG=false            # true/false
  USER_AGENT=FundingSignaler/1.2
  REQUEST_TIMEOUT=12

GCS доступ:
  GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json  # или используйте ADC/Workload Identity

Author: ChatGPT (GPT-5 Thinking)
License: MIT
"""

import os
import sys
import argparse
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# deps
try:
    import requests
    import pandas as pd
except Exception as e:
    print("Install deps first: pip install requests pandas", file=sys.stderr)
    raise

# google cloud storage (опционально)
GCS_AVAILABLE = False
try:
    from google.cloud import storage  # type: ignore
    GCS_AVAILABLE = True
except Exception:
    GCS_AVAILABLE = False

# ------------------------------
# Utils: ENV parsing
# ------------------------------
def getenv_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None or v.strip() == "" else v.strip()

def getenv_float(key: str, default: float) -> float:
    v = os.getenv(key)
    try:
        return float(v) if v is not None and v.strip() != "" else default
    except Exception:
        return default

def getenv_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    v = v.strip().lower()
    return v in ["1","true","yes","y","on"]

def getenv_list(key: str, default_list: List[str]) -> List[str]:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default_list
    return [x.strip() for x in v.split(",") if x.strip()]

# ------------------------------
# Config & logging
# ------------------------------
DEFAULT_EXCHANGES = ["binance", "bybit", "okx"]
DEFAULT_SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","BNBUSDT","AVAXUSDT","LINKUSDT","ADAUSDT","TONUSDT","OPUSDT","ARBUSDT","PEPEUSDT"]

USER_AGENT = getenv_str("USER_AGENT", "FundingSignaler/1.2")
REQUEST_TIMEOUT = int(getenv_float("REQUEST_TIMEOUT", 12))

logging.basicConfig(
    level=logging.DEBUG if getenv_bool("DEBUG", False) else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# --- tame noisy third-party loggers ---
for noisy in ("urllib3", "requests.packages.urllib3", "aiohttp", "google"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger(noisy).propagate = False

# --- add retries for transient HTTP errors ---
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# ------------------------------
# GCS helpers
# ------------------------------
def is_gs(path: Optional[str]) -> bool:
    return bool(path) and str(path).startswith("gs://")

def gcs_split(gs_path: str):
    # gs://bucket/path/to/file
    raw = gs_path[5:]
    bucket, _, blob = raw.partition("/")
    return bucket, blob

def gcs_client():
    if not GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage is not installed. pip install google-cloud-storage")
    return storage.Client()

def gcs_read_csv(gs_path: str, expected_columns: List[str]) -> pd.DataFrame:
    try:
        client = gcs_client()
        bucket_name, blob_name = gcs_split(gs_path)
        blob = client.bucket(bucket_name).blob(blob_name)
        if not blob.exists():
            return pd.DataFrame(columns=expected_columns)
        data = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(data))
        for c in expected_columns:
            if c not in df.columns:
                df[c] = None
        return df[expected_columns]
    except Exception as e:
        logging.warning("GCS read error %s: %s", gs_path, e)
        return pd.DataFrame(columns=expected_columns)

def gcs_write_csv(gs_path: str, df: pd.DataFrame) -> None:
    try:
        client = gcs_client()
        bucket_name, blob_name = gcs_split(gs_path)
        blob = client.bucket(bucket_name).blob(blob_name)
        # write to bytes buffer
        from io import StringIO
        buf = StringIO()
        df.to_csv(buf, index=False)
        blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    except Exception as e:
        logging.warning("GCS write error %s: %s", gs_path, e)

def gcs_append_csv(gs_path: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    # read old, concat, write new
    cols = list(df.columns)
    existing = gcs_read_csv(gs_path, cols) if is_gs(gs_path) else pd.DataFrame(columns=cols)
    out = pd.concat([existing, df], ignore_index=True)
    gcs_write_csv(gs_path, out)

# ------------------------------
# Local I/O helpers
# ------------------------------
def read_csv(path: Optional[str], columns: List[str]) -> pd.DataFrame:
    if not path or path.strip() == "":
        return pd.DataFrame(columns=columns)
    if is_gs(path):
        return gcs_read_csv(path, columns)
    if not os.path.exists(path):
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path)
        for c in columns:
            if c not in df.columns:
                df[c] = None
        return df[columns]
    except Exception:
        return pd.DataFrame(columns=columns)

def write_csv(path: Optional[str], df: pd.DataFrame) -> None:
    if not path or path.strip() == "":
        return
    if is_gs(path):
        gcs_write_csv(path, df)
        return
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def append_csv(path: Optional[str], df: pd.DataFrame) -> None:
    if df is None or df.empty or not path or path.strip() == "":
        return
    if is_gs(path):
        gcs_append_csv(path, df)
        return
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=header, index=False)

# ------------------------------
# Generic helpers
# ------------------------------
def utc_ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def to_float(x) -> Optional[float]:
    try:
        if x is None: return None
        return float(x)
    except Exception:
        return None

def annualize_from_8h(rate_8h: float) -> float:
    return rate_8h * 3 * 365

def fmt_ts(ms: Optional[int]) -> Optional[str]:
    if not ms: return None
    try:
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return None

def suggestion_from_rate(rate_8h: Optional[float]) -> str:
    if rate_8h is None: return "n/a"
    if rate_8h > 0: return "SHORT_PERP_LONG_SPOT"
    if rate_8h < 0: return "LONG_PERP_SHORT_SPOT"
    return "NEUTRAL"

def maybe_send_telegram(text: str) -> None:
    token = getenv_str("TELEGRAM_BOT_TOKEN", "")
    chat_id = getenv_str("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logging.debug("Telegram not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
        r = SESSION.post(url, json=payload, timeout=REQUEST_TIMEOUT)
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

def leg_notional_from_capital(capital_usd: float, perp_leverage: float) -> float:
    if perp_leverage <= 0:
        return max(0.0, float(capital_usd))
    return float(capital_usd) / (1.0 + 1.0/float(perp_leverage))

def daily_borrow_cost_usd(notional: float, borrow_apr: float) -> float:
    return float(notional) * float(borrow_apr) / 365.0

def round2(x):
    return None if x is None else (round(float(x), 2))

def format_open_card(ex, sym, r, qty, nf, net_day_usd, gross_day_usd, fees_day_usd, borrow_day_usd):
    d_human = "Short perp & Long spot" if r["direction"]=="SHORT_PERP_LONG_SPOT" else "Long perp & Short spot"
    lines = [
        "🚀 <b>Funding OPEN</b>",
        f"<b>{ex.upper()} {sym}</b>",
        f"<code>APR: {r['apr_pct']}% | 8h: {round((r['rate_8h'] or 0)*100,6)}%</code>",
        f"Dir: <b>{d_human}</b>",
        f"Price: {r['price']} | Qty(est): {qty}",
        f"Next funding: {nf}",
        "",
        f"<b>Profit/day (net): ${round2(net_day_usd)}</b>",
        f"  • Funding/day (gross): ${round2(gross_day_usd)}",
        f"  • Fees/day (amort): ${round2(fees_day_usd)}",
        f"  • Borrow/day: ${round2(borrow_day_usd)}",
    ]
    return "\n".join(lines)

# ---------- Binance Top-N symbols (by 24h quote volume) ----------
def binance_top_perp_usdt(top_n: int = 200, min_quote_usdt: float = 0.0) -> List[str]:
    try:
        exinfo = SESSION.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=REQUEST_TIMEOUT)
        exinfo.raise_for_status()
        info = exinfo.json()
        perp_usdt = {
            s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }
        t24 = SESSION.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=REQUEST_TIMEOUT)
        t24.raise_for_status()
        rows = t24.json()
        items = []
        for r in rows:
            sym = r.get("symbol")
            if sym in perp_usdt:
                qv = to_float(r.get("quoteVolume")) or 0.0
                if qv >= float(min_quote_usdt):
                    items.append((sym, qv))
        items.sort(key=lambda x: x[1], reverse=True)
        symbols = [sym for sym, _ in items[: int(top_n)]]
        return symbols
    except Exception as e:
        logging.warning("binance_top_perp_usdt error: %s", e)
        return []

# ------------------------------
# Exchange clients (public REST)
# ------------------------------
def binance_premium_index(symbol: str) -> Optional[Dict[str, Any]]:
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    try:
        r = SESSION.get(url, params={"symbol": symbol.upper()}, timeout=REQUEST_TIMEOUT)
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
        # 1) tickers
        url = "https://api.bybit.com/v5/market/tickers"
        params = {"category": "linear", "symbol": symbol.upper()}
        r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            rows = (j.get("result") or {}).get("list") or []
            if rows:
                px = (to_float(rows[0].get("lastPrice"))
                      or to_float(rows[0].get("markPrice"))
                      or None)
                if px:
                    return px
        # 2) fallback: mark-price kline close
        mp = SESSION.get("https://api.bybit.com/v5/market/mark-price-kline",
                         params={"category":"linear","symbol":symbol.upper(),"interval":"1","limit":1},
                         timeout=REQUEST_TIMEOUT)
        if mp.status_code == 200:
            jm = mp.json()
            rows = (jm.get("result") or {}).get("list") or []
            if rows and len(rows[0]) >= 5:
                return to_float(rows[0][4])  # close
    except Exception as e:
        logging.debug("Bybit price fallback error %s: %s", symbol, e)
    return None

def bybit_latest_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        url = "https://api.bybit.com/v5/market/funding/history"
        params = {"category": "linear", "symbol": symbol.upper(), "limit": 1}
        r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
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
        r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
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
        r = SESSION.get(url, params={"instId": inst_id}, timeout=REQUEST_TIMEOUT)
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
# Scan
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

# ------------------------------
# Main
# ------------------------------
def main():
    # аргументы CLI оставлены для совместимости, но всё берём из ENV
    ap = argparse.ArgumentParser()
    ap.add_argument("--noop", action="store_true", help="No-op, everything via ENV")
    ap.parse_args()

    # ENV -> настройки
    exchanges = [x.lower() for x in getenv_list("EXCHANGES", DEFAULT_EXCHANGES)]
    symbols_source = getenv_str("SYMBOLS_SOURCE", "binance-top").lower()
    symbols = [x.upper() for x in getenv_list("SYMBOLS", DEFAULT_SYMBOLS)]
    top_n = int(getenv_float("TOP_N", 200))
    min_quote_usdt = float(getenv_float("MIN_QUOTE_USDT", 10_000_000))
    save_symbols_path = getenv_str("SAVE_SYMBOLS_PATH", "")

    entry_apr = float(getenv_float("ENTRY_APR", 15.0))
    exit_apr  = float(getenv_float("EXIT_APR", 8.0))
    max_holding_h = float(getenv_float("MAX_HOLDING_H", 48.0))

    notional_env = getenv_str("NOTIONAL","")
    notional = float(notional_env) if notional_env else None
    capital = float(getenv_float("CAPITAL", 1000.0))
    perp_leverage = float(getenv_float("PERP_LEVERAGE", 5.0))
    taker_fee = float(getenv_float("TAKER_FEE", 0.0005))
    borrow_apr = float(getenv_float("BORROW_APR", 0.10))
    expected_holding_h = float(getenv_float("EXPECTED_HOLDING_H", 24.0))

    # additional filters
    MIN_NET_DAY_USD = float(getenv_float("MIN_NET_DAY_USD", 0.0))
    MIN_PRICE = float(getenv_float("MIN_PRICE", 0.0))

    top_n_tg = int(getenv_float("TOP_N_TELEGRAM", 3))
    rotate = getenv_bool("ROTATE", False)
    rotate_delta_usd = float(getenv_float("ROTATE_DELTA_USD", 0.0))

    raw_csv_path = getenv_str("RAW_CSV_PATH", "")
    log_csv_path = getenv_str("LOG_CSV_PATH", "signals_log.csv")
    positions_csv_path = getenv_str("POSITIONS_CSV_PATH", "positions.csv")

    # итоговый нотuонал
    if notional is not None:
        eff_notional = float(notional)
    else:
        eff_notional = leg_notional_from_capital(capital, perp_leverage)
    logging.info("Effective per-leg notional = $%.2f (capital=%.2f, leverage=%.2f)",
                 eff_notional, capital, perp_leverage)

    # авто-список символов
    if symbols_source == "binance-top":
        logging.info("Building symbols from Binance top-%s by 24h quote volume (min %s USDT)...",
                     top_n, min_quote_usdt)
        symbols = binance_top_perp_usdt(top_n=top_n, min_quote_usdt=min_quote_usdt) or symbols
        logging.info("Got %d symbols. First 10: %s", len(symbols), " ".join(symbols[:10]))
        if save_symbols_path:
            try:
                # запишем список в локальный или GCS путь
                if is_gs(save_symbols_path):
                    from io import StringIO
                    buf = StringIO()
                    buf.write("\n".join(symbols) + "\n")
                    client = gcs_client()
                    bucket, blob = gcs_split(save_symbols_path)
                    client.bucket(bucket).blob(blob).upload_from_string(buf.getvalue(), content_type="text/plain")
                else:
                    with open(save_symbols_path, "w") as f:
                        for s in symbols: f.write(s+"\n")
                logging.info("Saved symbols to %s", save_symbols_path)
            except Exception as e:
                logging.warning("save-symbols error: %s", e)

    # скан
    df = scan_all(exchanges, symbols)
    if df.empty:
        print("No data rows.")
        return

    # расчёт чистой прибыли/день
    gross_day, fees_day, borrow_day, net_day = [], [], [], []
    hold_days = max(1.0/24.0, expected_holding_h/24.0)
    fees_total = 4.0 * taker_fee * eff_notional  # entry+exit, spot+perp
    fees_day_amort = fees_total / hold_days

    for _, row in df.iterrows():
        r8 = row.get("rate_8h") or 0.0
        g = 3.0 * float(r8) * eff_notional
        b = daily_borrow_cost_usd(eff_notional, borrow_apr) if row["direction"] == "LONG_PERP_SHORT_SPOT" else 0.0
        gross_day.append(g)
        fees_day.append(fees_day_amort)
        borrow_day.append(b)
        net_day.append(g - fees_day_amort - b)

    df["gross_day_usd"] = gross_day
    df["fees_day_usd"]  = fees_day
    df["borrow_day_usd"] = borrow_day
    df["net_day_usd"]   = net_day

    # filter bad/illiquid prices early
    df = df[(~df["price"].isna()) & (df["price"] >= MIN_PRICE)]

    # сырые логи скана
    if raw_csv_path:
        append_csv(raw_csv_path, df)

    # загрузить состояние позиций
    pos_cols = ["exchange","symbol","direction","entry_time_utc","entry_ts","entry_rate_8h","entry_apr_pct","entry_price","notional_usd","net_day_usd"]
    positions = read_csv(positions_csv_path, pos_cols)

    # подготовка логов
    log_cols = ["time_utc","action","exchange","symbol","direction","rate_8h","apr_pct","price","notional_usd","payout_8h_usd","payout_day_usd","reason"]
    batch_logs = []

    def current_row(ex, sym) -> Optional[pd.Series]:
        sub = df[(df["exchange"]==ex) & (df["symbol"]==sym)]
        if sub.empty: return None
        return sub.iloc[0]

    now_ms = utc_ms_now()
    now_utc = fmt_ts(now_ms)

    # CLOSE‑логика
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
        if apr_abs is not None and apr_abs < exit_apr:
            reason = f"APR {apr_abs}% < exit {exit_apr}%"
        if reason is None and holding_h is not None and holding_h >= max_holding_h:
            reason = f"Max holding {holding_h}h ≥ {max_holding_h}h"
        if reason is None and sign_flip:
            reason = f"Direction flip {dir_} -> {cr['direction']}"

        if reason:
            payout8 = payout_8h_usd(cr["rate_8h"], eff_notional)
            payoutd = payout_day_usd(cr["rate_8h"], eff_notional)
            batch_logs.append({
                "time_utc": now_utc, "action":"CLOSE",
                "exchange": ex, "symbol": sym, "direction": dir_,
                "rate_8h": cr["rate_8h"], "apr_pct": cr["apr_pct"], "price": cr["price"],
                "notional_usd": eff_notional, "payout_8h_usd": payout8, "payout_day_usd": payoutd,
                "reason": reason
            })
            msg = (f"✅ <b>Funding CLOSE</b>\n"
                   f"{ex.upper()} {sym}\n"
                   f"APR: {cr['apr_pct']}% | 8h: {round((cr['rate_8h'] or 0)*100,6)}%\n"
                   f"Dir: {dir_}\nReason: {reason}")
            maybe_send_telegram(msg)
            to_remove_idx.append(i)

    if to_remove_idx:
        positions = positions.drop(index=to_remove_idx).reset_index(drop=True)

    # Кандидаты по net/day
    candidates = df.copy()
    candidates = candidates[
        (candidates["apr_pct"].abs() >= entry_apr) &
        (candidates["net_day_usd"] > MIN_NET_DAY_USD)
    ]
    candidates = candidates.sort_values("net_day_usd", ascending=False)

    # TOP‑N в канал (без дублирования лучшего)
    if top_n_tg > 0 and not candidates.empty:
        publish_rows = candidates.iloc[1:1+top_n_tg] if len(candidates) > 1 else pd.DataFrame(columns=candidates.columns)
        for _, r in publish_rows.iterrows():
            if r["price"] is None or r["price"] < MIN_PRICE:
                continue
            qty = hedge_qty(eff_notional, r["price"])
            nf = r.get("next_funding_utc") or "n/a"
            card = format_open_card(r["exchange"], r["symbol"], r, qty, nf,
                                    r["net_day_usd"], r["gross_day_usd"], r["fees_day_usd"], r["borrow_day_usd"])
            maybe_send_telegram(card)

    # OPEN/ROTATE: держим одну позицию
    current_pos = positions.iloc[0] if len(positions) > 0 else None
    best = candidates.head(1)
    if not best.empty:
        r = best.iloc[0]
        ex, sym, dir_ = r["exchange"], r["symbol"], r["direction"]

        need_open = current_pos is None
        if current_pos is not None:
            same = (str(current_pos["exchange"]).lower() == ex) and (str(current_pos["symbol"]).upper() == sym)
            if same:
                need_open = False
            else:
                current_net = 0.0 if pd.isna(current_pos.get("net_day_usd", None)) else float(current_pos.get("net_day_usd", 0.0))
                delta_usd = float(r["net_day_usd"] or 0.0) - current_net
                if rotate and delta_usd > float(rotate_delta_usd):
                    close_msg = (f"🔁 <b>Funding ROTATE</b>\n"
                                 f"Close: {str(current_pos['exchange']).upper()} {str(current_pos['symbol']).upper()}\n"
                                 f"Open:  {ex.upper()} {sym}\n"
                                 f"Delta net/day: ${round2(delta_usd)}")
                    maybe_send_telegram(close_msg)
                    positions = positions.iloc[0:0]
                    need_open = True
                else:
                    need_open = False

        if need_open and (r["price"] is not None) and (r["price"] >= MIN_PRICE):
            entry_qty = hedge_qty(eff_notional, r["price"])
            payout8 = payout_8h_usd(r["rate_8h"], eff_notional)
            payoutd = r["gross_day_usd"]
            nf = r.get("next_funding_utc") or "n/a"

            card = format_open_card(ex, sym, r, entry_qty, nf,
                                    r["net_day_usd"], r["gross_day_usd"], r["fees_day_usd"], r["borrow_day_usd"])
            maybe_send_telegram(card)

            new_pos = {
                "exchange": ex, "symbol": sym, "direction": dir_,
                "entry_time_utc": r["time_utc"], "entry_ts": r["ts"],
                "entry_rate_8h": r["rate_8h"], "entry_apr_pct": r["apr_pct"],
                "entry_price": r["price"], "notional_usd": eff_notional,
                "net_day_usd": r["net_day_usd"]
            }
            positions = pd.DataFrame([new_pos], columns=list(new_pos.keys()))
            batch_logs.append({
                "time_utc": now_utc, "action":"OPEN",
                "exchange": ex, "symbol": sym, "direction": dir_,
                "rate_8h": r["rate_8h"], "apr_pct": r["apr_pct"], "price": r["price"],
                "notional_usd": eff_notional, "payout_8h_usd": payout8, "payout_day_usd": payoutd,
                "reason": "best net/day"
            })

    # запись состояния
    write_csv(positions_csv_path, positions)
    if batch_logs:
        append_csv(log_csv_path, pd.DataFrame(batch_logs))

    # stdout
    if not df.empty:
        printable = df[["exchange","symbol","price","rate_8h","rate_8h_pct","apr_pct","net_day_usd","time_utc","next_funding_utc","direction"]]
        print(printable.to_string(index=False))
    else:
        print("No data rows.")

if __name__ == "__main__":
    main()
