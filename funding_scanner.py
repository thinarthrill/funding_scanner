#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding Signaler + Availability Matrix (All-in-One)

- Поддержка бирж: binance, bybit, okx, mexc, kucoin, deribit, bitget, gate, phemex, krakenf
- Сигналы funding (APR/net/day и т.д.), логирование позиций/сигналов
- Построение "матрицы доступности" perp-контрактов по выбранным биржам
- Сохранение CSV локально или в GCS
- ENV BACKET: если задано имя bucket, относительные пути автоматически преобразуются в gs://<BACKET>/<filename>

Примеры ENV:
  EXCHANGES=binance,bybit,okx,mexc,kucoin,bitget,gate,phemex,krakenf,deribit
  SYMBOLS_SOURCE=binance-top              # common | binance-top | union | manual
  SYMBOLS=BTCUSDT,ETHUSDT                 # если SYMBOLS_SOURCE=manual
  TOP_N=150
  MIN_QUOTE_USDT=15000000

  # Пороговые параметры
  ENTRY_APR=15
  EXIT_APR=8
  MAX_HOLDING_H=48
  NOTIONAL=                                 # если пусто — считается от CAPITAL и PERP_LEVERAGE
  CAPITAL=1000
  PERP_LEVERAGE=5
  TAKER_FEE=0.0005
  BORROW_APR=0.10
  EXPECTED_HOLDING_H=24
  MIN_NET_DAY_USD=0
  MIN_PRICE=0

  # Ротация
  TOP_N_TELEGRAM=3
  ROTATE=false
  ROTATE_DELTA_USD=0

  # Пути вывода (могут быть относительные или gs://...)
  RAW_CSV_PATH=raw.csv
  LOG_CSV_PATH=signals_log.csv
  POSITIONS_CSV_PATH=positions.csv

  # GCS & bucket
  BACKET=my-bucket-name                     # если задать: raw.csv => gs://my-bucket-name/raw.csv
  GCS_KEY_JSON=...                          # service account JSON (строкой) ИЛИ GOOGLE_APPLICATION_CREDENTIALS

  # Matrix (по желанию)
  MATRIX_EXCHANGES=binance,bybit,okx,mexc,kucoin,bitget,gate,phemex,krakenf,deribit
  MATRIX_UNIVERSE=union                     # common | binance-top | union | manual
  MATRIX_SYMBOLS=BTCUSDT,ETHUSDT            # когда MATRIX_UNIVERSE=manual
  MATRIX_TOP_N=200
  MATRIX_MIN_QUOTE=10000000
  MATRIX_SLEEP_MS=300
  MATRIX_SAVE_PATH=matrix.csv

  # Телеграм (опционально)
  TELEGRAM_BOT_TOKEN=
  TELEGRAM_CHAT_ID=

  # Прочее
  USER_AGENT=FundingSignaler/1.4
  REQUEST_TIMEOUT=15
  DEBUG=false
"""
# Новые ENV для работы с матрицей:
# MATRIX_READ_PATH=gs://thinarthrillbucket/funding_scanner/matrix_union.csv
# MATRIX_MODE=union           # union | intersection | atleast:2
# MATRIX_USE_PER_EXCHANGE=true  # если true — сканер берёт набор тикеров отдельно для каждой биржи из матрицы

import os
import sys
import argparse
import logging
import json
import hmac, hashlib, time
from typing import Dict, Any, List, Optional, Set, Any as AnyT
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()  # подгрузить .env

# Deps
try:
    import requests
    import pandas as pd
except Exception:
    print("Install dependencies: pip install requests pandas", file=sys.stderr)
    raise

# ------------------------------
# ENV helpers
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
    return v in ("1","true","yes","y","on")

def getenv_list(key: str, default_list: List[str]) -> List[str]:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default_list
    return [x.strip() for x in v.split(",") if x.strip()]

# ------------------------------
# Requests session + logging
# ------------------------------
USER_AGENT = getenv_str("USER_AGENT", "FundingSignaler/1.4")
REQUEST_TIMEOUT = int(getenv_float("REQUEST_TIMEOUT", 15))

logging.basicConfig(
    level=logging.DEBUG if getenv_bool("DEBUG", False) else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(
    total=3,
    backoff_factor=0.4,
    status_forcelist=[429,500,502,503,504],
    allowed_methods=["GET","POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

for noisy in ("urllib3", "requests.packages.urllib3", "google"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger(noisy).propagate = False

# ------------------------------
# GCS helpers + BACKET support
# ------------------------------
GCS_AVAILABLE = True
try:
    from google.cloud import storage  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    GCS_AVAILABLE = True
except Exception:
    GCS_AVAILABLE = False

def is_gs(path: Optional[str]) -> bool:
    return bool(path) and str(path).startswith("gs://")

def gcs_split(gs_path: str):
    raw = gs_path[5:]
    bucket, _, blob = raw.partition("/")
    return bucket, blob

def gcs_client():
    if not GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage not installed")
    key_str = os.getenv("GCS_KEY_JSON", "").strip()
    if key_str:
        info = json.loads(key_str)
        creds = service_account.Credentials.from_service_account_info(info)
        return storage.Client(project=info.get("project_id"), credentials=creds)
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if key_path:
        return storage.Client.from_service_account_json(key_path)
    return storage.Client()

def gcs_read_csv(gs_path: str, expected_columns: List[str]) -> pd.DataFrame:
    try:
        client = gcs_client()
        bucket_name, blob_name = gcs_split(gs_path)
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            raise FileNotFoundError(f"Bucket '{bucket_name}' not found or access denied")
        blob = bucket.blob(blob_name)
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
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            raise FileNotFoundError(f"Bucket '{bucket_name}' not found or access denied")
        blob = bucket.blob(blob_name)
        from io import StringIO
        buf = StringIO()
        df.to_csv(buf, index=False)
        blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    except Exception as e:
        logging.warning("GCS write error %s: %s", gs_path, e)

# ---------- BACKET auto-prefix ----------
def bucketize_path(path: Optional[str]) -> Optional[str]:
    """
    Если задан BACKET (строкой), и path относительный (не gs:// и не абсолютный),
    вернём gs://<BACKET>/<path>.
    """
    if not path or path.strip() == "":
        return path
    p = path.strip()
    if is_gs(p):
        return p
    if os.path.isabs(p):
        return p
    backet = getenv_str("BACKET", "")
    if backet:
        # sanitize slashes
        p = p.lstrip("/").replace("\\", "/")
        return f"gs://{backet}/{p}"
    return p

# ------------------------------
# Generic utils
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

def maybe_send_telegram(text: str) -> None:
    token = getenv_str("TELEGRAM_BOT_TOKEN", "")
    chat_id = getenv_str("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
        r = SESSION.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.warning("Telegram send failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:
        logging.warning("Telegram exception: %s", e)

def load_positions_df(path: str) -> pd.DataFrame:
    # полный набор полей, с которыми работает симулятор
    expected_cols = [
        "id","symbol","long_ex","short_ex",
        "opened_ms","last_ms","held_h",
        "size_usd","open_apr_combo",
        "status","accrued_usd",
        "open_note","closed_ms","pnl_usd","close_note"
    ]

    path = bucketize_path(path)

    def _empty_df():
        return pd.DataFrame(columns=expected_cols)

    if not path or str(path).strip() == "":
        return _empty_df()

    try:
        if is_gs(path):
            # у тебя gcs_read_csv требует expected_columns — передаём!
            df = gcs_read_csv(path, expected_columns=expected_cols)
        else:
            if not os.path.exists(path):
                return _empty_df()
            df = pd.read_csv(path)
    except Exception as e:
        logging.warning("Positions load error %s: %s", path, e)
        return _empty_df()

    if df is None or df.empty:
        return _empty_df()

    # добавим недостающие колонки, сохраним порядок
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None
    # оставим только нужные (и в правильном порядке)
    df = df[expected_cols].copy()

    # базовая нормализация типов (без фанатизма)
    for col in ["id","opened_ms","last_ms","closed_ms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["size_usd","open_apr_combo","accrued_usd","pnl_usd","held_h"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["symbol","long_ex","short_ex","status","open_note","close_note"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

def save_positions_df(path: str, df: pd.DataFrame) -> None:
    path = bucketize_path(path)
    if is_gs(path):
        gcs_write_csv(path, df)
    else:
        # гарантируем, что папка существует
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        df.to_csv(path, index=False)


def read_csv(path: Optional[str], columns: List[str]) -> pd.DataFrame:
    if not path or path.strip() == "":
        return pd.DataFrame(columns=columns)
    p = bucketize_path(path)
    if is_gs(p):
        return gcs_read_csv(p, columns)
    if not os.path.exists(p):
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(p)
        for c in columns:
            if c not in df.columns:
                df[c] = None
        return df[columns]
    except Exception:
        return pd.DataFrame(columns=columns)

def write_csv(path: Optional[str], df: pd.DataFrame) -> None:
    if not path or path.strip() == "": return
    p = bucketize_path(path)
    if is_gs(p):
        gcs_write_csv(p, df)
        return
    tmp = f"{p}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, p)

def append_csv(path: Optional[str], df: pd.DataFrame) -> None:
    if df is None or df.empty or not path or path.strip() == "": return
    p = bucketize_path(path)
    if is_gs(p):
        # emulate append: read+concat+write
        cols = list(df.columns)
        existing = gcs_read_csv(p, cols)
        out = pd.concat([existing, df], ignore_index=True)
        gcs_write_csv(p, out)
        return
    header = not os.path.exists(p)
    df.to_csv(p, mode="a", header=header, index=False)

# ===== Matrix I/O helpers =====

def _gcs_read_any(gs_path: str) -> pd.DataFrame:
    """Прямое чтение CSV из GCS без списка ожидаемых колонок."""
    try:
        client = gcs_client()
        bucket_name, blob_name = gcs_split(gs_path)
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            raise FileNotFoundError(f"Bucket '{bucket_name}' not found or access denied")
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return pd.DataFrame()
        data = blob.download_as_bytes()
        return pd.read_csv(pd.io.common.BytesIO(data))
    except Exception as e:
        logging.warning("GCS read (any) error %s: %s", gs_path, e)
        return pd.DataFrame()

def load_matrix_df(matrix_path: str) -> pd.DataFrame:
    """
    Считывает матрицу доступности из локального пути или gs://...
    Приводит TRUE/FALSE к bool.
    """
    p = bucketize_path(matrix_path)
    if not p:
        return pd.DataFrame()
    try:
        if is_gs(p):
            df = _gcs_read_any(p)
        else:
            df = pd.read_csv(p)
    except Exception as e:
        logging.warning("Matrix read error %s: %s", p, e)
        return pd.DataFrame()

    if df.empty:
        return df

    # нормализуем колонки и типы
    df.columns = [c.strip() for c in df.columns]
    if "symbol" not in df.columns:
        logging.warning("Matrix has no 'symbol' column: %s", df.columns.tolist())
        return pd.DataFrame()

    # приведение True/False (иногда приезжает как 'True'/'False'/'1'/'0')
    for c in df.columns:
        if c in ("symbol", "listed_on"):
            continue
        try:
            if df[c].dtype == object:
                df[c] = df[c].astype(str).str.strip().str.lower().isin(("true","1","t","y","yes"))
            else:
                df[c] = df[c].astype(bool)
        except Exception:
            pass
    if "listed_on" in df.columns:
        try:
            df["listed_on"] = pd.to_numeric(df["listed_on"], errors="coerce").fillna(0).astype(int)
        except Exception:
            pass
    return df

def symbols_from_matrix(
    matrix_path: str,
    exchanges: List[str],
    mode: str = "union"  # "union" | "intersection" | "atleast:k"
) -> List[str]:
    """
    Возвращает список символов из матрицы по заданным биржам.
    - union: есть хотя бы на одной из бирж
    - intersection: есть на всех биржах
    - atleast:k  (например, atleast:2)
    """
    df = load_matrix_df(matrix_path)
    if df.empty:
        return []

    ex_cols = [ex for ex in exchanges if ex in df.columns]
    if not ex_cols:
        # если в матрице нет колонок с такими биржами — отдаём пусто
        logging.warning("Matrix has no columns for exchanges: %s (have: %s)", exchanges, df.columns.tolist())
        return []

    sub = df[["symbol"] + ex_cols].copy()

    if mode.startswith("atleast:"):
        try:
            k = int(mode.split(":",1)[1])
        except Exception:
            k = 1
        mask = sub[ex_cols].sum(axis=1) >= k
    elif mode == "intersection":
        mask = sub[ex_cols].all(axis=1)
    else:
        mask = sub[ex_cols].any(axis=1)

    out = sorted(sub.loc[mask, "symbol"].dropna().astype(str).str.upper().unique().tolist())
    return out

def matrix_by_exchange(
    matrix_path: str,
    exchanges: List[str]
) -> Dict[str, List[str]]:
    """
    Возвращает словарь {exchange: [symbols...]} — только те пары,
    где в матрице для этой биржи стоит True.
    Удобно для точечного сканирования без «лишних» запросов к API.
    """
    df = load_matrix_df(matrix_path)
    if df.empty:
        return {ex: [] for ex in exchanges}

    ex_cols = [ex for ex in exchanges if ex in df.columns]
    out: Dict[str, List[str]] = {}
    for ex in exchanges:
        if ex not in ex_cols:
            out[ex] = []
            continue
        syms = df.loc[df[ex].astype(bool), "symbol"].dropna().astype(str).str.upper().unique().tolist()
        out[ex] = sorted(syms)
    return out

# ------------------------------
# Exchange clients (funding)
# ------------------------------
def bybit_get_fee(symbol: str) -> Optional[float]:
    try:
        api_key = os.getenv("BYBIT_API_KEY", "")
        api_secret = os.getenv("BYBIT_API_SECRET", "")
        if not api_key or not api_secret:
            return None
        url = "https://api.bybit.com/v5/account/fee-rate"
        params = {"category":"linear","symbol":symbol.upper()}
        ts = str(int(time.time()*1000))
        param_str = "&".join([f"{k}={v}" for k,v in sorted(params.items())])
        sign_str = ts + api_key + "5000" + param_str
        signature = hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": "5000",
        }
        r = SESSION.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        j = r.json()
        if j.get("retCode") == 0:
            fee_str = j["result"]["list"][0]["takerFeeRate"]
            return float(fee_str)
    except Exception as e:
        logging.warning("Bybit fee API exception: %s", e)
    return None

# Binance
def binance_premium_index(symbol: str) -> Optional[Dict[str, Any]]:
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    try:
        r = SESSION.get(url, params={"symbol": symbol.upper()}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
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
    except Exception:
        return None

# Bybit
def fetch_bybit_mark_price(symbol: str) -> Optional[float]:
    try:
        r = SESSION.get("https://api.bybit.com/v5/market/tickers",
                        params={"category": "linear", "symbol": symbol.upper()},
                        timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            rows = (j.get("result") or {}).get("list") or []
            if rows:
                px = (to_float(rows[0].get("lastPrice")) or to_float(rows[0].get("markPrice")))
                if px: return px
        # fallback
        r2 = SESSION.get("https://api.bybit.com/v5/market/mark-price-kline",
                         params={"category":"linear","symbol":symbol.upper(),"interval":"1","limit":1},
                         timeout=REQUEST_TIMEOUT)
        if r2.status_code == 200:
            jm = r2.json()
            rows = (jm.get("result") or {}).get("list") or []
            if rows and len(rows[0]) >= 5:
                return to_float(rows[0][4])
    except Exception:
        pass
    return None

def bybit_latest_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        r = SESSION.get("https://api.bybit.com/v5/market/funding/history",
                        params={"category":"linear","symbol":symbol.upper(),"limit":1},
                        timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            rows = (j.get("result") or {}).get("list") or []
            if rows:
                row = rows[0]
                rate = to_float(row.get("fundingRate"))
                ts = int(row.get("fundingTime")) if row.get("fundingTime") else utc_ms_now()
                price = fetch_bybit_mark_price(symbol.upper())
                return {"exchange":"bybit","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":None,"ts":ts}
    except Exception:
        pass
    return None

# OKX
def okx_inst_id(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"): return f"{s[:-4]}-USDT-SWAP"
    if s.endswith("USD"):  return f"{s[:-3]}-USD-SWAP"
    return f"{s}-USDT-SWAP"

def okx_mark_price(inst_id: str) -> Optional[float]:
    try:
        r = SESSION.get("https://www.okx.com/api/v5/public/mark-price",
                        params={"instType":"SWAP","instId":inst_id}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            data = j.get("data") or []
            if data:
                return to_float(data[0].get("markPx"))
    except Exception:
        pass
    return None

def okx_funding(symbol: str) -> Optional[Dict[str, Any]]:
    inst_id = okx_inst_id(symbol)
    try:
        r = SESSION.get("https://www.okx.com/api/v5/public/funding-rate",
                        params={"instId": inst_id}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        j = r.json()
        data = j.get("data") or []
        if not data:
            return None
        d0 = data[0]
        rate = to_float(d0.get("fundingRate"))
        next_time = int(d0.get("nextFundingTime")) if d0.get("nextFundingTime") else None
        price = okx_mark_price(inst_id)
        return {"exchange":"okx","symbol":symbol.upper(),"instId":inst_id,"price":price,"rate_8h":rate,"next_funding_time":next_time,"ts":utc_ms_now()}
    except Exception:
        return None

# MEXC
def mexc_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"): return f"{s[:-4]}_USDT"
    if s.endswith("USD"):  return f"{s[:-3]}_USD"
    return s.replace("-", "_")

def mexc_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://contract.mexc.com/api/v1/contract/ticker",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            data = (j.get("data") or {})
            if isinstance(data, list) and data:
                data = data[0]
            return to_float((data or {}).get("fair_price")) or to_float((data or {}).get("last_price"))
    except Exception:
        pass
    return None
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MEXC_SESSION = None

def build_short_session(timeout: float = 6.0, retries: int = 1, backoff: float = 0.0):
    """
    Отдельная сессия для MEXC с коротким таймаутом и без длинных backoff/Retry-After.
    """
    sess = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        respect_retry_after_header=False,  # ВАЖНО: не ждать 60с по Retry-After
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({"User-Agent": "FundingScanner/fast-mexc"})
    # кастомные поля, если используешь их где-то
    sess.request_timeout = timeout
    return sess

def mexc_funding(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Быстрый запрос funding по MEXC c короткими таймаутами и без минутных бэк-оффов.
    Примерный формат ответа остаётся как в старом коде: {exchange, symbol, rate_8h, ts, next_funding_time, ...}
    """
    global MEXC_SESSION
    if MEXC_SESSION is None:
        MEXC_SESSION = build_short_session(timeout=6.0, retries=1, backoff=0.0)

    base = "https://contract.mexc.com"
    path = "/api/v1/contract/funding_rate/record"   # исторический/последний; если у тебя был другой — оставь свой
    params = {"symbol": symbol, "page_size": 1}     # берём последнее значение

    t0 = time.time()
    try:
        r = MEXC_SESSION.get(base + path, params=params, timeout=getattr(MEXC_SESSION, "request_timeout", 6.0))
        dt = (time.time() - t0) * 1000
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug("MEXC HTTP %s %s -> %s in %.0fms", path, symbol, r.status_code, dt)

        # Быстрая обработка 429: маленькая пауза и один ре-трай вручную (без минутных спячек)
        if r.status_code == 429:
            time.sleep(1.5)
            r = MEXC_SESSION.get(base + path, params=params, timeout=getattr(MEXC_SESSION, "request_timeout", 6.0))

        if r.status_code != 200:
            return None

        j = r.json()
        # Под разные форматы данных: берём последнюю запись, где есть rate/time
        data = None
        if isinstance(j, dict):
            data = j.get("data") or j.get("result") or j.get("records") or j.get("list")
        elif isinstance(j, list):
            data = j

        if not data:
            return None

        # Берём первую/последнюю запись
        rec = data[0] if isinstance(data, list) else data
        # Часто поля называются по-разному, пробуем несколько ключей:
        rate = rec.get("fundingRate") or rec.get("rate") or rec.get("funding_rate")
        ts   = rec.get("time") or rec.get("timestamp") or rec.get("createdTime") or rec.get("created_at")

        # Нормализуем типы
        try:
            rate = float(rate)
        except Exception:
            rate = None

        # Переводим секунды → мс (если вдруг sec)
        if ts is not None:
            try:
                ts = int(ts)
                if ts < 10**12:  # похоже на секунды
                    ts *= 1000
            except Exception:
                ts = None

        row = {
            "exchange": "mexc",
            "symbol": symbol,
            "rate_8h": rate,              # если MEXC отдаёт не 8h — адаптируй ниже
            "ts": ts,
            "next_funding_time": None,    # при желании сделай отдельный вызов "next"
        }

        return row

    except Exception as e:
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug("MEXC funding error %s: %s", symbol, e)
        return None

# KuCoin Futures
def kucoin_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"): return s + "M"
    return s

def kucoin_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://api-futures.kucoin.com/api/v1/mark-price",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            d = j.get("data") or {}
            return to_float(d.get("value"))
    except Exception:
        pass
    return None

def kucoin_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        sym_native = kucoin_symbol(symbol)
        r = SESSION.get("https://api-futures.kucoin.com/api/v1/funding-rate",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        j = r.json()
        d = j.get("data") or {}
        rate = to_float(d.get("fundingRate"))
        next_time = int(d.get("nextFundingTime")) if d.get("nextFundingTime") else None
        price = kucoin_mark_price(sym_native)
        return {"exchange":"kucoin","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":next_time,"ts":utc_ms_now()}
    except Exception:
        return None

# Deribit
def deribit_symbol(symbol: str) -> str:
    base = symbol.upper().replace("USDT","").replace("USD","")
    return f"{base}-PERPETUAL"

def deribit_mark_price(inst: str) -> Optional[float]:
    try:
        r = SESSION.get("https://www.deribit.com/api/v2/public/ticker",
                        params={"instrument_name": inst}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            d = (j.get("result") or {})
            return to_float(d.get("mark_price"))
    except Exception:
        pass
    return None

def deribit_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        inst = deribit_symbol(symbol)
        r = SESSION.get("https://www.deribit.com/api/v2/public/get_funding_rate_value",
                        params={"instrument_name": inst}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        j = r.json()
        d = j.get("result") or {}
        rate = to_float(d.get("data"))  # трактуем как 8h (некоторые инсталляции отдают hourly — учти при анализе)
        price = deribit_mark_price(inst)
        return {"exchange":"deribit","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":None,"ts":utc_ms_now()}
    except Exception:
        return None

# Bitget
def bitget_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"): return f"{s}_UMCBL"
    if s.endswith("USD"):  return f"{s}_DMCBL"
    return s + "_UMCBL"

def bitget_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://api.bitget.com/api/mix/v1/market/mark-price",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            d = (j.get("data") or {})
            return to_float(d.get("markPrice"))
    except Exception:
        pass
    return None

def bitget_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        sym_native = bitget_symbol(symbol)
        r = SESSION.get("https://api.bitget.com/api/mix/v1/market/current-fundRate",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        j = r.json()
        d = j.get("data") or {}
        rate = to_float(d.get("fundingRate"))
        next_time = int(d.get("nextSettleTime")) if d.get("nextSettleTime") else None
        price = bitget_mark_price(sym_native)
        return {"exchange":"bitget","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":next_time,"ts":utc_ms_now()}
    except Exception:
        return None

# Gate
def gate_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"): return f"{s[:-4]}_USDT"
    if s.endswith("USD"):  return f"{s[:-3]}_USD"
    return s.replace("-", "_")

def gate_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://api.gateio.ws/api/v4/futures/usdt/tickers",
                        params={"contract": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and arr:
                d = arr[0]
                return to_float(d.get("mark_price")) or to_float(d.get("last"))
    except Exception:
        pass
    return None

def gate_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        sym_native = gate_symbol(symbol)
        r = SESSION.get("https://api.gateio.ws/api/v4/futures/usdt/funding_rate",
                        params={"contract": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        arr = r.json()
        d = arr[0] if isinstance(arr, list) and arr else {}
        rate = to_float(d.get("funding_rate"))
        next_time = int(d.get("next_funding_time"))*1000 if d.get("next_funding_time") else None
        price = gate_mark_price(sym_native)
        return {"exchange":"gate","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":next_time,"ts":utc_ms_now()}
    except Exception:
        return None

# Phemex
def phemex_symbol(symbol: str) -> str:
    return symbol.upper()

def phemex_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://api.phemex.com/md/ticker/24hr",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            d = (j.get("result") or {})
            if isinstance(d, dict):
                d = d.get("tickers") or []
            if isinstance(d, list) and d:
                item = d[0]
                ep = to_float(item.get("markPriceEp"))
                if ep is not None:
                    return ep/1e4
    except Exception:
        pass
    return None

def phemex_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        sym_native = phemex_symbol(symbol)
        r = SESSION.get("https://api.phemex.com/md/funding",
                        params={"symbol": sym_native}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        j = r.json()
        d = (j.get("result") or {})
        rate = to_float(d.get("fundingRate"))
        next_time = int(d.get("timestamp")) if d.get("timestamp") else None
        price = phemex_mark_price(sym_native)
        return {"exchange":"phemex","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":next_time,"ts":utc_ms_now()}
    except Exception:
        return None

# Kraken Futures
def krakenf_symbol(symbol: str) -> str:
    base = symbol.upper().replace("BTC","XBT")
    if base.endswith("USDT"): return f"PI_{base[:-4]}USDT"
    if base.endswith("USD"):  return f"PI_{base[:-3]}USD"
    return f"PI_{base}USD"

def krakenf_mark_price(sym_native: str) -> Optional[float]:
    try:
        r = SESSION.get("https://futures.kraken.com/derivatives/api/v3/tickers", timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            arr = (r.json().get("tickers") or [])
            for it in arr:
                if it.get("symbol") == sym_native:
                    return to_float(it.get("markPrice"))
    except Exception:
        pass
    return None

def krakenf_funding(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        sym_native = krakenf_symbol(symbol)
        r = SESSION.get("https://futures.kraken.com/derivatives/api/v3/funding-rates", timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        arr = (r.json().get("instruments") or [])
        rate = None
        next_ts = None
        for it in arr:
            if it.get("symbol") == sym_native:
                rate = to_float(it.get("fundingRate8h"))
                next_ts = int(it.get("nextFundingRateTime")) if it.get("nextFundingRateTime") else None
                break
        if rate is None:
            return None
        price = krakenf_mark_price(sym_native)
        return {"exchange":"krakenf","symbol":symbol.upper(),"price":price,"rate_8h":rate,"next_funding_time":next_ts,"ts":utc_ms_now()}
    except Exception:
        return None

# ------------------------------
# Binance Top symbols util
# ------------------------------
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
# Scan
# ------------------------------
DEFAULT_EXCHANGES = ["binance","bybit","okx"]
DEFAULT_SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","BNBUSDT","LINKUSDT","ADAUSDT","TONUSDT","OPUSDT","ARBUSDT","PEPEUSDT"]
COMMON_SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","LINKUSDT","BNBUSDT","ADAUSDT"]

# ===== Symbols resolution =====
exchanges = getenv_list("EXCHANGES", DEFAULT_EXCHANGES)
src = getenv_str("SYMBOLS_SOURCE", "common").lower()  # common | binance-top | union | manual | matrix
symbols_env = getenv_list("SYMBOLS", [])              # если manual
top_n = int(getenv_float("TOP_N", 200))
min_quote = float(getenv_float("MIN_QUOTE_USDT", 10_000_000))

# Путь к матрице и режим выбора из неё
matrix_path = getenv_str("MATRIX_READ_PATH", "gs://thinarthrillbucket/funding_scanner/matrix_union.csv")
matrix_mode = getenv_str("MATRIX_MODE", "union")  # union | intersection | atleast:k
use_per_ex = getenv_bool("MATRIX_USE_PER_EXCHANGE", True)

if src == "manual" and symbols_env:
    symbols = [s.upper() for s in symbols_env]
elif src == "binance-top":
    symbols = binance_top_perp_usdt(top_n=top_n, min_quote_usdt=min_quote)
elif src == "union":
    # «на лету» собирать union по API как раньше — можно, но лучше взять из матрицы, если задана
    if matrix_path:
        symbols = symbols_from_matrix(matrix_path, exchanges, mode="union")
    else:
        symbols = COMMON_SYMBOLS  # фолбэк
elif src == "common":
    symbols = COMMON_SYMBOLS
elif src == "matrix":
    if not matrix_path:
        logging.error("SYMBOLS_SOURCE=matrix, но MATRIX_READ_PATH не задан — беру COMMON")
        symbols = COMMON_SYMBOLS
    else:
        symbols = symbols_from_matrix(matrix_path, exchanges, mode=matrix_mode)
else:
    symbols = COMMON_SYMBOLS

logging.info("Symbols selected (%d): %s", len(symbols), symbols[:20])

# Если хотим ещё точнее — использовать матрицу ПО-БИРЖАМ:
symbols_by_ex: Optional[Dict[str, List[str]]] = None
if use_per_ex and matrix_path:
    symbols_by_ex = matrix_by_exchange(matrix_path, exchanges)
    # На всякий случай — ограничим словари только теми символами, что попали в итоговый набор 'symbols'
    symbols_set = set(symbols)
    for ex in list(symbols_by_ex.keys()):
        symbols_by_ex[ex] = sorted(list(symbols_set.intersection(symbols_by_ex.get(ex, []))))

def scan_all(exchanges: List[str], symbols: List[str], symbols_by_ex: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for ex in exchanges:
        # подберём подходящий список тикеров
        ex_symbols = symbols_by_ex.get(ex) if symbols_by_ex else None
        if ex_symbols is None:
            ex_symbols = symbols
        if not ex_symbols:
            continue

        for sym in ex_symbols:
            row = None
            if ex == "binance":
                row = binance_premium_index(sym)
            elif ex == "bybit":
                row = bybit_latest_funding(sym)
            elif ex == "okx":
                row = okx_funding(sym)
            elif ex == "mexc":
                row = mexc_funding(sym)
            elif ex == "kucoin":
                row = kucoin_funding(sym)
            elif ex == "deribit":
                row = deribit_funding(sym)
            elif ex == "bitget":
                row = bitget_funding(sym)
            elif ex == "gate":
                row = gate_funding(sym)
            elif ex == "phemex":
                row = phemex_funding(sym)
            elif ex == "krakenf":
                row = krakenf_funding(sym)
            
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug("Scanned %s %s", ex, sym)

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
# Availability Matrix (встроено)
# ------------------------------
def _std_from_binance(sym: str) -> str: return sym.upper()
def _std_from_underscore(s: str) -> str:
    s = s.upper().replace("-", "_")
    if "_" in s:
        a,b=s.split("_",1)
        return a+b
    return s
def _std_from_suffix(s: str, suf: str) -> str:
    s = s.upper()
    if s.endswith(suf.upper()): return s[:-len(suf)]
    return s
def _std_from_kucoin(s: str) -> str: return s[:-1] if s.upper().endswith("M") else s.upper()
def _std_from_okx_inst(inst: str) -> Optional[str]:
    inst = inst.upper(); parts = inst.split("-")
    if len(parts)>=3 and parts[-1]=="SWAP": return parts[0]+parts[1]
    return None
def _std_from_krakenf(sym: str) -> Optional[str]:
    s = sym.upper()
    if not s.startswith("PI_"): return None
    s = s[3:].replace("XBT","BTC")
    return s
def _std_from_deribit(inst: str) -> Optional[str]:
    inst = inst.upper()
    if inst.endswith("-PERPETUAL"): return inst.replace("-PERPETUAL","")+"USD"
    return None

def list_binance_perp_usdt() -> Set[str]:
    r = SESSION.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for s in r.json().get("symbols", []):
        if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING":
            out.add(_std_from_binance(s.get("symbol","")))
    return out

def lister_bybit() -> Set[str]:
    r = SESSION.get("https://api.bybit.com/v5/market/instruments-info", params={"category":"linear"}, timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("result") or {}).get("list") or []:
        sym=(it.get("symbol") or "").upper()
        if sym.endswith("USDT"): out.add(sym)
    return out

def lister_okx() -> Set[str]:
    r = SESSION.get("https://www.okx.com/api/v5/public/instruments", params={"instType":"SWAP"}, timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("data") or []):
        std=_std_from_okx_inst(it.get("instId",""))
        if std and (std.endswith("USDT") or std.endswith("USD")): out.add(std)
    return out

def lister_mexc() -> Set[str]:
    r = SESSION.get("https://contract.mexc.com/api/v1/contract/detail", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("data") or []):
        std=_std_from_underscore(it.get("symbol",""))
        if std.endswith("USDT") or std.endswith("USD"): out.add(std)
    return out

def lister_kucoin() -> Set[str]:
    r = SESSION.get("https://api-futures.kucoin.com/api/v1/contracts/active", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("data") or []):
        std=_std_from_kucoin(it.get("symbol",""))
        if std.endswith("USDT") or std.endswith("USD"): out.add(std)
    return out

def lister_bitget() -> Set[str]:
    r = SESSION.get("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType":"umcbl"}, timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("data") or []):
        std=_std_from_suffix(it.get("symbol",""), "_UMCBL")
        if std.endswith("USDT"): out.add(std)
    return out

def lister_gate() -> Set[str]:
    r = SESSION.get("https://api.gateio.ws/api/v4/futures/usdt/contracts", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in r.json():
        std=_std_from_underscore(it.get("name",""))
        if std.endswith("USDT"): out.add(std)
    return out

def lister_phemex() -> Set[str]:
    r = SESSION.get("https://api.phemex.com/exchange/public/products", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    j=r.json()
    rows: Optional[List[AnyT]] = None
    if isinstance(j, dict):
        data=j.get("data")
        if isinstance(data, dict):
            rows=data.get("products")
        elif isinstance(data, list):
            rows=data
        else:
            rows=j.get("products") or j.get("result")
    elif isinstance(j, list):
        rows=j
    out=set()
    if not rows: return out
    for it in rows:
        try:
            sym=str(it.get("symbol","")).upper()
            typ=it.get("type") or it.get("contractType") or ""
            if isinstance(typ,str) and "perpet" in typ.lower():
                out.add(sym)
        except Exception:
            continue
    return out

def lister_krakenf() -> Set[str]:
    r = SESSION.get("https://futures.kraken.com/derivatives/api/v3/instruments", timeout=REQUEST_TIMEOUT); r.raise_for_status()
    out=set()
    for it in (r.json().get("instruments") or []):
        std=_std_from_krakenf(it.get("symbol",""))
        if std and (std.endswith("USDT") or std.endswith("USD")): out.add(std)
    return out

def lister_deribit() -> Set[str]:
    out=set()
    for cur in ["BTC","ETH"]:
        r = SESSION.get("https://www.deribit.com/api/v2/public/get_instruments",
                        params={"currency":cur,"kind":"future","expired":"false"},
                        timeout=REQUEST_TIMEOUT); r.raise_for_status()
        for it in (r.json().get("result") or []):
            if it.get("settlement_period")=="perpetual":
                std=_std_from_deribit(it.get("instrument_name",""))
                if std: out.add(std)
    return out

LISTERS = {
    "binance": list_binance_perp_usdt,
    "bybit":   lister_bybit,
    "okx":     lister_okx,
    "mexc":    lister_mexc,
    "kucoin":  lister_kucoin,
    "bitget":  lister_bitget,
    "gate":    lister_gate,
    "phemex":  lister_phemex,
    "krakenf": lister_krakenf,
    "deribit": lister_deribit,
}

def build_availability_matrix(
    exchanges: List[str],
    universe_mode: str = "common",         # common | binance-top | union | manual
    manual_symbols: Optional[List[str]] = None,
    top_n: int = 200,
    min_quote: float = 10_000_000,
    sleep_ms: int = 250
) -> pd.DataFrame:
    exchanges = [e.lower().strip() for e in exchanges if e.strip()]
    for e in exchanges:
        if e not in LISTERS:
            raise ValueError(f"Unsupported exchange: {e}")

    avail: Dict[str, Set[str]] = {}
    for i, ex in enumerate(exchanges):
        try:
            avail[ex] = LISTERS[ex]()
        except Exception as ex_err:
            logging.warning("Failed to list %s: %s", ex, ex_err)
            avail[ex] = set()
        if sleep_ms and i < len(exchanges)-1:
            time.sleep(sleep_ms/1000.0)

    if universe_mode == "common":
        universe = set(COMMON_SYMBOLS)
    elif universe_mode == "binance-top":
        try:
            top = binance_top_perp_usdt(top_n=top_n, min_quote_usdt=min_quote)
            universe = set(top)
        except Exception as ex_err:
            logging.warning("binance-top failed; fallback to COMMON: %s", ex_err)
            universe = set(COMMON_SYMBOLS)
    elif universe_mode == "union":
        universe = set()
        for s in avail.values():
            universe |= s
    elif universe_mode == "manual":
        if not manual_symbols:
            raise ValueError("manual universe requires manual_symbols")
        universe = set([x.strip().upper() for x in manual_symbols if x.strip()])
    else:
        raise ValueError("universe must be one of: common | binance-top | union | manual")

    rows = []
    for sym in sorted(universe):
        row = {"symbol": sym}
        cnt = 0
        for ex in exchanges:
            has = sym in avail.get(ex, set())
            row[ex] = bool(has)
            if has: cnt += 1
        row["listed_on"] = cnt
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df[["symbol"] + exchanges + ["listed_on"]]
    df = df.sort_values(["listed_on","symbol"], ascending=[False, True]).reset_index(drop=True)
    return df

def run_matrix_from_env() -> Optional[pd.DataFrame]:
    ex_env = getenv_str("MATRIX_EXCHANGES", "")
    save_path = getenv_str("MATRIX_SAVE_PATH", "")
    if not ex_env:
        return None
    exchanges = [x.strip() for x in ex_env.split(",") if x.strip()]
    universe = getenv_str("MATRIX_UNIVERSE", "common").lower()
    symbols_env = getenv_str("MATRIX_SYMBOLS", "")
    manual_symbols = [x.strip() for x in symbols_env.split(",")] if (universe=="manual" and symbols_env) else None
    top_n = int(getenv_float("MATRIX_TOP_N", 200))
    min_quote = float(getenv_float("MATRIX_MIN_QUOTE", 10_000_000))
    sleep_ms = int(getenv_float("MATRIX_SLEEP_MS", 250))

    logging.info("Building availability matrix: exchanges=%s | universe=%s", ",".join(exchanges), universe)
    df = build_availability_matrix(exchanges, universe, manual_symbols, top_n, min_quote, sleep_ms)

    try:
        print(df.to_string(index=False))
    except Exception:
        print(df.head().to_string(index=False))

    if save_path:
        p = bucketize_path(save_path)
        if is_gs(p):
            try:
                gcs_write_csv(p, df)
                logging.info("Matrix saved to %s", p)
            except Exception as e:
                logging.warning("Failed to save matrix to GCS: %s", e)
        else:
            try:
                df.to_csv(p, index=False)
                logging.info("Matrix saved to %s", p)
            except Exception as e:
                logging.warning("Failed to save matrix to file: %s", e)
    return df
# ===== Funding → кросс-биржевые кандидаты =====
# ===== Пэйпер-симуляция позиций (perp vs perp cross-ex) =====

def _now_ms() -> int:
    return int(time.time() * 1000)

def _hours_between(ts_ms_a: int, ts_ms_b: int) -> float:
    return abs(ts_ms_a - ts_ms_b) / 3_600_000.0

def positions_open_close_loop(
    df_raw: pd.DataFrame,
    best_row: Optional[pd.Series],
    per_leg_notional_usd: float,
    entry_apr_threshold: float,
    exit_apr_threshold: float,
    max_holding_h: float,
    default_fee: float,
    pos_path: str,
    paper: bool = True,
) -> list[str]:
    """
    Обновляет позы (открывает/закрывает) и возвращает список Telegram-сообщений.
    Логика:
      - если best_row есть и apr_combo >= ENTRY_APR — открыть, если нет активной по этой паре.
      - для открытых поз — накапливаем PnL по времени (приближение), закрываем при apr_combo < EXIT_APR или по сроку.
    """
    messages: list[str] = []
    now_ms = _now_ms()

    # загрузить текущие позы
    df_pos = load_positions_df(pos_path)
    if df_pos.empty:
        df_pos = pd.DataFrame(columns=[
            "id","symbol","long_ex","short_ex","opened_ms","last_ms",
            "size_usd","open_apr_combo","status","accrued_usd","open_note"
        ])

    # построим быструю карту текущих APR (чтобы апдейтить PnL)
    apr_map = {}  # (ex,sym)->apr
    for _, r in df_raw[["exchange","symbol","apr"]].dropna().iterrows():
        apr_map[(str(r["exchange"]).lower(), str(r["symbol"]).upper())] = float(r["apr"])

    # 1) апдейт открытых
    for i in range(len(df_pos)):
        if str(df_pos.at[i,"status"]) != "open":
            continue
        long_ex  = str(df_pos.at[i,"long_ex"])
        short_ex = str(df_pos.at[i,"short_ex"])
        sym      = str(df_pos.at[i,"symbol"]).upper()
        last_ms  = int(df_pos.at[i,"last_ms"] or df_pos.at[i,"opened_ms"])
        held_h   = float(df_pos.at[i].get("held_h", 0.0))

        # текущие APR
        apr_long  = apr_map.get((long_ex, sym), 0.0)
        apr_short = apr_map.get((short_ex, sym), 0.0)
        apr_combo = abs(min(0.0, apr_long)) + max(0.0, apr_short)  # сколько «получаем» сейчас

        dt_h = _hours_between(now_ms, last_ms)
        df_pos.at[i,"held_h"] = held_h + dt_h

        # начислить за dt_h
        combo_frac = (dt_h / (24.0 * 365.0))
        delta_usd = per_leg_notional_usd * (apr_combo * combo_frac)
        df_pos.at[i,"accrued_usd"] = float(df_pos.at[i].get("accrued_usd", 0.0)) + delta_usd
        df_pos.at[i,"last_ms"] = now_ms

        # критерии выхода
        do_close = False
        reason = ""
        if apr_combo*100.0 < float(exit_apr_threshold):
            do_close = True
            reason = f"APR fell below EXIT ({apr_combo*100:.2f}% < {exit_apr_threshold:.2f}%)"
        if df_pos.at[i,"held_h"] >= float(max_holding_h):
            do_close = True
            reason = f"MAX_HOLDING_H reached ({df_pos.at[i]['held_h']:.1f}h)"

        if do_close:
            # комиссии на выход
            fee_long  = _taker_fee_for(long_ex, default_fee)
            fee_short = _taker_fee_for(short_ex, default_fee)
            exit_fees = per_leg_notional_usd * (fee_long + fee_short) * 2  # 2 ордера (закрытие 2 ног)
            pnl = float(df_pos.at[i,"accrued_usd"]) - exit_fees
            df_pos.at[i,"status"] = "closed"
            df_pos.at[i,"closed_ms"] = now_ms
            df_pos.at[i,"pnl_usd"] = round(pnl, 4)
            df_pos.at[i,"close_note"] = reason

            msg = (
                f"✅ <b>Closed</b> {sym}\n"
                f"LONG {long_ex.upper()} / SHORT {short_ex.upper()}\n"
                f"Held: {df_pos.at[i,'held_h']:.1f}h | APR_now: {apr_combo*100:.2f}%\n"
                f"Accrued: ${float(df_pos.at[i,'accrued_usd']):.2f}\n"
                f"Exit fees: ${exit_fees:.2f}\n"
                f"<b>PNL:</b> ${pnl:.2f}\n"
                f"Reason: {reason}"
            )
            messages.append(msg)

    # 2) открыть новую, если есть сильный кандидат и нет открытой на ту же пару
    if best_row is not None and not df_raw.empty:
        apr_combo_pct = float(best_row["apr_combo"]) * 100.0
        if apr_combo_pct >= float(entry_apr_threshold):
            long_ex  = str(best_row["long_ex"])
            short_ex = str(best_row["short_ex"])
            sym      = str(best_row["symbol"]).upper()
            # нет ли уже такой открытой?
            exists = False
            for _, p in df_pos.iterrows():
                if p.get("status") == "open" and p.get("symbol")==sym and p.get("long_ex")==long_ex and p.get("short_ex")==short_ex:
                    exists = True
                    break
            if not exists:
                # комиссии на вход
                fee_long  = _taker_fee_for(long_ex, default_fee)
                fee_short = _taker_fee_for(short_ex, default_fee)
                entry_fees = per_leg_notional_usd * (fee_long + fee_short) * 2  # 2 ордера (вход 2 ног)

                cur_max = None
                if "id" in df_pos.columns:
                    cur_max = pd.to_numeric(df_pos["id"], errors="coerce").max()
                next_id = int(cur_max) + 1 if (cur_max is not None and pd.notna(cur_max)) else 1

                new = {
                    "id": next_id,
                    "symbol": sym,
                    "long_ex": long_ex,
                    "short_ex": short_ex,
                    "opened_ms": now_ms,
                    "last_ms": now_ms,
                    "size_usd": per_leg_notional_usd,
                    "open_apr_combo": float(best_row["apr_combo"]),
                    "status": "open",
                    "accrued_usd": -entry_fees,
                    "open_note": f"entry fees ${entry_fees:.2f}",
                    "held_h": 0.0,
                }
                df_pos = pd.concat([df_pos, pd.DataFrame([new])], ignore_index=True)

                msg = (
                    f"🚀 <b>Opened</b> {sym}\n"
                    f"LONG {long_ex.upper()} / SHORT {short_ex.upper()}\n"
                    f"Combo APR: {apr_combo_pct:.2f}% | Size: ${per_leg_notional_usd:,.2f} per leg\n"
                    f"Expected (next {int(best_row['exp_hours'])}h): "
                    f"<b>${float(best_row['net_usd']):.2f}</b> after fees"
                )
                messages.append(msg)

    # сохранить позы
    save_positions_df(pos_path, df_pos)
    return messages

def _taker_fee_for(ex: str, default_fee: float) -> float:
    # при желании заполни реальными комиссиями по биржам
    per_ex = {
        "bybit": 0.00055,
        # "binance": 0.0004,
        # "okx": 0.0005,
        # ...
    }
    return float(per_ex.get(ex, default_fee))

def build_cross_exchange_candidates(
    df_raw: pd.DataFrame,
    expected_h: float,
    per_leg_notional_usd: float,
    default_fee: float,
) -> pd.DataFrame:
    """
    Находит пары бирж для одного и того же символа, где
    на одной funding APR > 0 (SHORT получает), а на другой APR < 0 (LONG получает).
    Возвращает DataFrame, отсортированный по ожидаемой прибыли (USD) за expected_h.
    """
    if df_raw.empty:
        return pd.DataFrame()

    # нормализуем
    use = df_raw[["exchange","symbol","apr","rate_8h","next_funding_utc","time_utc"]].copy()
    use["exchange"] = use["exchange"].str.lower()
    use["symbol"] = use["symbol"].str.upper()
    use = use.dropna(subset=["apr"])

    # по символам — строим список строк
    symbols = use["symbol"].unique().tolist()
    rows = []
    hours_frac = max(0.0, float(expected_h)) / (24.0 * 365.0)  # доля года

    for sym in symbols:
        sub = use[use["symbol"] == sym]
        if len(sub) < 2:
            continue
        # положительные (SHORT получает), отрицательные (LONG получает)
        pos = sub[sub["apr"] > 0.0]
        neg = sub[sub["apr"] < 0.0]
        if pos.empty or neg.empty:
            continue

        for _, r_pos in pos.iterrows():
            for _, r_neg in neg.iterrows():
                ex_short = r_pos["exchange"]  # там шортим перп (получаем +APR)
                ex_long  = r_neg["exchange"]  # там лонг перп (получаем |APR|)
                apr_short = float(r_pos["apr"])
                apr_long_abs = abs(float(r_neg["apr"]))
                apr_combo = apr_short + apr_long_abs  # комбинированный APR «получаем»

                # комиссии (в USD) — 2 ордера на вход + 2 на выход
                fee_short = _taker_fee_for(ex_short, default_fee)
                fee_long  = _taker_fee_for(ex_long,  default_fee)
                fees_usd  = per_leg_notional_usd * (2*fee_short + 2*fee_long)

                # ожидаемый доход по фандингу за expected_h
                funding_usd = per_leg_notional_usd * apr_combo * hours_frac

                net_usd = funding_usd - fees_usd

                rows.append({
                    "symbol": sym,
                    "long_ex": ex_long,
                    "short_ex": ex_short,
                    "apr_long_abs": apr_long_abs,
                    "apr_short": apr_short,
                    "apr_combo": apr_combo,        # %
                    "exp_hours": expected_h,
                    "funding_usd": round(funding_usd, 4),
                    "fees_usd": round(fees_usd, 4),
                    "net_usd": round(net_usd, 4),
                })

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["net_usd","apr_combo"], ascending=[False, False]).reset_index(drop=True)
    return out

# ------------------------------
# Main
# ------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--noop", action="store_true")
    args = ap.parse_args()

    exchanges = [x.lower() for x in getenv_list("EXCHANGES", DEFAULT_EXCHANGES)]
    top_n = int(getenv_float("TOP_N", 200))
    min_quote_usdt = float(getenv_float("MIN_QUOTE_USDT", 1_000_000))

    # Paths (with BACKET auto-prefix for relative paths)
    raw_csv_path = bucketize_path(getenv_str("RAW_CSV_PATH", ""))
    log_csv_path = bucketize_path(getenv_str("LOG_CSV_PATH", "signals_log.csv"))
    positions_csv_path = bucketize_path(getenv_str("POSITIONS_CSV_PATH", "positions.csv"))

    entry_apr = float(getenv_float("ENTRY_APR", 15.0))
    exit_apr  = float(getenv_float("EXIT_APR", 8.0))
    max_holding_h = float(getenv_float("MAX_HOLDING_H", 48.0))

    notional_env = getenv_str("NOTIONAL","")
    notional = float(notional_env) if notional_env else None
    capital = float(getenv_float("CAPITAL", 1000.0))
    perp_leverage = float(getenv_float("PERP_LEVERAGE", 5.0))
    taker_fee = float(getenv_float("TAKER_FEE", 0.0005))

    if "bybit" in exchanges:
        try:
            f = bybit_get_fee("BTCUSDT")
            if f:
                taker_fee = f
                logging.info("Bybit taker fee auto-set to %s", taker_fee)
        except Exception as e:
            logging.warning("Bybit auto-fee failed: %s", e)

    borrow_apr = float(getenv_float("BORROW_APR", 0.10))
    expected_holding_h = float(getenv_float("EXPECTED_HOLDING_H", 24.0))

    MIN_NET_DAY_USD = float(getenv_float("MIN_NET_DAY_USD", 0.0))
    MIN_PRICE = float(getenv_float("MIN_PRICE", 0.0))

    top_n_tg = int(getenv_float("TOP_N_TELEGRAM", 3))
    rotate = getenv_bool("ROTATE", False)
    rotate_delta_usd = float(getenv_float("ROTATE_DELTA_USD", 0.0))

    # Effective notional per leg
    if notional is not None:
        eff_notional = float(notional)
    else:
        eff_notional = leg_notional_from_capital(capital, perp_leverage)
    logging.info("Effective per-leg notional = $%.2f (capital=%.2f, leverage=%.2f)", eff_notional, capital, perp_leverage)

    # Scan
    if symbols_by_ex:
        for ex in exchanges:
            logging.info("Will scan %s: %d symbols", ex, len(symbols_by_ex.get(ex, [])))
        else:
            logging.info("Will scan all exchanges with the same symbol set: %d", len(symbols))
    df = scan_all(exchanges, symbols, symbols_by_ex=symbols_by_ex)

    # ===== после df_raw = scan_all(...) =====

    # 1) параметры
    paper = getenv_bool("PAPER_TRADING", True)
    expected_h = float(getenv_float("EXPECTED_HOLDING_H", 72))
    entry_apr = float(getenv_float("ENTRY_APR", 25))       # %
    exit_apr  = float(getenv_float("EXIT_APR", 12))        # %
    max_hold  = float(getenv_float("MAX_HOLDING_H", 72))   # h
    default_fee = float(getenv_float("TAKER_FEE", 0.0005))

    # размер позиции: если у тебя уже вычисляется «Effective per-leg notional», используй его.
    # иначе — безопасный дефолт:
    capital = float(getenv_float("CAPITAL", 1000))
    lev     = float(getenv_float("PERP_LEVERAGE", 5))
    per_leg_notional = max(10.0, round((capital * lev) / 2.0, 2))  # на каждую ногу

    # 2) построить кандидатов cross-ex
    cands = build_cross_exchange_candidates(
        df_raw=df,
        expected_h=expected_h,
        per_leg_notional_usd=per_leg_notional,
        default_fee=default_fee,
    )

    best = cands.iloc[0] if not cands.empty else None

    # 3) выслать лучший сигнал в Telegram
    if best is not None:
        msg = (
            f"📈 <b>Best cross-ex funding</b>\n"
            f"{best['symbol']}: LONG {str(best['long_ex']).upper()} / SHORT {str(best['short_ex']).upper()}\n"
            f"Combo APR: {float(best['apr_combo'])*100:.2f}%\n"
            f"Expected {int(expected_h)}h: <b>${float(best['net_usd']):.2f}</b> "
            f"(funding ${float(best['funding_usd']):.2f} − fees ${float(best['fees_usd']):.2f})"
        )
        maybe_send_telegram(msg)

    # 4) симуляция открытия/закрытия + Telegram апдейты
    pos_path = getenv_str("POSITIONS_CSV_PATH", "positions.csv")
    events = positions_open_close_loop(
        df_raw=df,
        best_row=best,
        per_leg_notional_usd=per_leg_notional,
        entry_apr_threshold=entry_apr,
        exit_apr_threshold=exit_apr,
        max_holding_h=max_hold,
        default_fee=default_fee,
        pos_path=pos_path,
        paper=paper,
    )
    for e in events:
        maybe_send_telegram(e)

    if not df.empty:
        # Costs/PNL metrics
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

        df = df[(~df["price"].isna()) & (df["price"] >= MIN_PRICE)]

        if raw_csv_path:
            append_csv(raw_csv_path, df)

        # Positions & signals
        pos_cols = ["exchange","symbol","direction","entry_time_utc","entry_ts","entry_rate_8h","entry_apr_pct","entry_price","notional_usd","net_day_usd"]
        positions = read_csv(positions_csv_path, pos_cols)

        log_cols = ["time_utc","action","exchange","symbol","direction","rate_8h","apr_pct","price","notional_usd","payout_8h_usd","payout_day_usd","reason"]
        batch_logs = []

        def current_row(ex, sym) -> Optional[pd.Series]:
            sub = df[(df["exchange"]==ex) & (df["symbol"]==sym)]
            if sub.empty: return None
            return sub.iloc[0]

        now_ms = utc_ms_now()
        now_utc = fmt_ts(now_ms)

        to_remove_idx = []
        for i, pos in positions.iterrows():
            ex, sym, dir_ = pos["exchange"], pos["symbol"], pos["direction"]
            entry_ts = pos["entry_ts"]
            cr = current_row(ex, sym)
            if cr is None:
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

        candidates = df.copy()
        candidates = candidates[
            (candidates["apr_pct"].abs() >= entry_apr) &
            (candidates["net_day_usd"] > MIN_NET_DAY_USD)
        ]
        candidates = candidates.sort_values("net_day_usd", ascending=False)

        top_n_tg = max(0, int(top_n_tg))
        if top_n_tg > 0 and not candidates.empty:
            publish_rows = candidates.iloc[:top_n_tg] if len(candidates) > 1 else pd.DataFrame(columns=candidates.columns)
            for _, r in publish_rows.iterrows():
                if r["price"] is None or r["price"] < MIN_PRICE:
                    continue
                qty = hedge_qty(eff_notional, r["price"])
                nf = r.get("next_funding_utc") or "n/a"
                card = (
                    "🚀 <b>Funding OPEN</b>\n"
                    f"<b>{r['exchange'].upper()} {r['symbol']}</b>\n"
                    f"<code>APR: {r['apr_pct']}% | 8h: {round((r['rate_8h'] or 0)*100,6)}%</code>\n"
                    f"Dir: <b>{'Short perp & Long spot' if r['direction']=='SHORT_PERP_LONG_SPOT' else 'Long perp & Short spot'}</b>\n"
                    f"Price: {r['price']} | Qty(est): {qty}\n"
                    f"Next funding: {nf}\n\n"
                    f"<b>Profit/day (net): ${round2(r['net_day_usd'])}</b>\n"
                    f"  • Funding/day (gross): ${round2(r['gross_day_usd'])}\n"
                    f"  • Fees/day (amort): ${round2(r['fees_day_usd'])}\n"
                    f"  • Borrow/day: ${round2(r['borrow_day_usd'])}\n"
                )
                maybe_send_telegram(card)

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

                card = (
                    "🚀 <b>Funding OPEN</b>\n"
                    f"<b>{ex.upper()} {sym}</b>\n"
                    f"<code>APR: {r['apr_pct']}% | 8h: {round((r['rate_8h'] or 0)*100,6)}%</code>\n"
                    f"Dir: <b>{'Short perp & Long spot' if dir_=='SHORT_PERP_LONG_SPOT' else 'Long perp & Short spot'}</b>\n"
                    f"Price: {r['price']} | Qty(est): {entry_qty}\n"
                    f"Next funding: {nf}\n\n"
                    f"<b>Profit/day (net): ${round2(r['net_day_usd'])}</b>\n"
                    f"  • Funding/day (gross): ${round2(r['gross_day_usd'])}\n"
                    f"  • Fees/day (amort): ${round2(r['fees_day_usd'])}\n"
                    f"  • Borrow/day: ${round2(r['borrow_day_usd'])}\n"
                )
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

        write_csv(positions_csv_path, positions)
        if batch_logs:
            append_csv(log_csv_path, pd.DataFrame(batch_logs))

        try:
            printable = df[["exchange","symbol","price","rate_8h","rate_8h_pct","apr_pct","net_day_usd","time_utc","next_funding_utc","direction"]]
            print(printable.to_string(index=False))
        except Exception:
            print(df.head().to_string(index=False))
    else:
        print("No data rows.")

    # ---- ДОПОЛНИТЕЛЬНО: построить матрицу, если задан MATRIX_EXCHANGES ----
    try:
        run_matrix_from_env()
    except Exception as e:
        logging.warning("Matrix build failed: %s", e)

if __name__ == "__main__":
    main()
