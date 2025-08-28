#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perp Availability Matrix (cross-exchange) — robust + GCS + BACKET/GCS_BUCKET

Пример:
  python funding_matrix.py ^
    --exchanges binance,bybit,okx,mexc,kucoin,bitget,gate,phemex,krakenf,deribit ^
    --universe union ^
    --save matrix_union.csv

Аргументы:
  --exchanges    CSV список бирж (см. SUPPORTED_EXCHANGES)
  --universe     common | binance-top | union | manual
  --symbols      при universe=manual: CSV символов
  --top-n        для binance-top
  --min-quote    для binance-top (24h quoteVolume, USDT)
  --save         путь (локальный или gs://...); если задан BACKET или GCS_BUCKET,
                 относительные пути сохраняются в gs://<bucket>/<path>
  --timeout      таймаут HTTP (сек)
  --retries      ретраи HTTP
  --sleep-ms     задержка между опросом бирж (мс)
  --debug        включить DEBUG-логи

ENV:
  BACKET=<bucket>                 # имя бакета (вариант 1)
  GCS_BUCKET=<bucket>             # имя бакета (вариант 2, синоним)
  GCS_KEY_JSON=<json-string>      # JSON ключ сервис-аккаунта (строкой)
  GOOGLE_APPLICATION_CREDENTIALS=<path-to-key.json>  # стандартный путь к ключу
  PROXY=http://127.0.0.1:7890     # опционально (если есть блокировки)
"""

import argparse
import logging
import os
import time
from typing import List, Dict, Set, Optional, Any, Tuple

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Конфиг ----------
SUPPORTED_EXCHANGES = [
    "binance","bybit","okx","mexc","kucoin","bitget","gate","phemex","krakenf","deribit"
]
DEFAULT_TIMEOUT = 25
DEFAULT_RETRIES = 4
DEFAULT_BACKOFF = 0.5
DEFAULT_SLEEP_MS = 300

# ---------- HTTP session ----------
def build_session(timeout: int, retries: int, backoff: float, proxy: Optional[str]) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    if proxy:
        sess.proxies.update({"http": proxy, "https": proxy})
    sess.headers.update({"User-Agent": "PerpMatrix/2.2"})
    sess.request_timeout = timeout  # кастомный атрибут
    return sess

SESSION: requests.Session  # инициализируется в main()

# ---------- утилиты ----------
def to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def fetch_json(url: str, params: Optional[dict] = None, timeout: Optional[int] = None,
               alt_urls: Optional[List[str]] = None) -> Tuple[int, Any]:
    """GET с ретраями и фолбэком домена (alt_urls)."""
    timeout = timeout or getattr(SESSION, "request_timeout", DEFAULT_TIMEOUT)
    alt_urls = alt_urls or []
    urls = [url] + alt_urls
    last_exc = None
    for i, u in enumerate(urls):
        try:
            r = SESSION.get(u, params=params, timeout=timeout)
            if r.status_code == 200:
                try:
                    return 200, r.json()
                except Exception as je:
                    last_exc = je
            else:
                last_exc = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_exc = e
        if i < len(urls) - 1:
            time.sleep(0.25)
    raise RuntimeError(f"Failed GET {url} ({params}): {last_exc}")

# ---------- нормализация символов ----------
def std_from_binance(sym: str) -> str: return sym.upper()
def std_from_underscore(s: str) -> str:
    s = s.upper().replace("-", "_")
    if "_" in s:
        a,b = s.split("_",1)
        return a + b
    return s
def std_from_suffix(s: str, suffix: str) -> str:
    s = s.upper()
    if s.endswith(suffix.upper()):
        return s[:-len(suffix)]
    return s
def std_from_kucoin(s: str) -> str:
    s = s.upper()
    if s.endswith("M"): return s[:-1]
    return s
def std_from_okx_inst(inst: str) -> Optional[str]:
    inst = inst.upper()
    parts = inst.split("-")
    if len(parts) >= 3 and parts[-1] == "SWAP":
        return parts[0] + parts[1]   # BTCUSDT / BTCUSD
    return None
def std_from_krakenf(sym: str) -> Optional[str]:
    s = sym.upper()
    if not s.startswith("PI_"): return None
    return s[3:].replace("XBT", "BTC")
def std_from_deribit(inst: str) -> Optional[str]:
    inst = inst.upper()
    if inst.endswith("-PERPETUAL"):
        return inst.replace("-PERPETUAL","") + "USD"
    return None

# ---------- списки инструментов ----------
def list_binance_perp_usdt() -> Set[str]:
    _, j = fetch_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    out = set()
    for s in j.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            out.add(std_from_binance(s.get("symbol","")))
    return out

def binance_top_perp_usdt(top_n=200, min_quote=10_000_000) -> List[str]:
    ex = list_binance_perp_usdt()
    _, rows = fetch_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
    items = []
    for row in rows:
        sym = row.get("symbol","")
        if sym in ex:
            qv = to_float(row.get("quoteVolume")) or 0.0
            if qv >= float(min_quote):
                items.append((sym, qv))
    items.sort(key=lambda x: x[1], reverse=True)
    return [s for s,_ in items[:int(top_n)]]

def list_bybit_linear() -> Set[str]:
    _, j = fetch_json("https://api.bybit.com/v5/market/instruments-info", params={"category":"linear"})
    out = set()
    for it in (j.get("result") or {}).get("list") or []:
        sym = (it.get("symbol") or "").upper()
        if sym.endswith("USDT"):
            out.add(sym)
    return out

def list_okx_swap() -> Set[str]:
    _, j = fetch_json(
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType":"SWAP"},
        alt_urls=["https://okx.com/api/v5/public/instruments"]
    )
    out = set()
    for it in (j.get("data") or []):
        std = std_from_okx_inst(it.get("instId",""))
        if std and (std.endswith("USDT") or std.endswith("USD")):
            out.add(std)
    return out

def list_mexc_contracts() -> Set[str]:
    _, j = fetch_json("https://contract.mexc.com/api/v1/contract/detail")
    out = set()
    for it in (j.get("data") or []):
        std = std_from_underscore(it.get("symbol",""))
        if std.endswith(("USDT","USD")):
            out.add(std)
    return out

def list_kucoin_futures() -> Set[str]:
    _, j = fetch_json("https://api-futures.kucoin.com/api/v1/contracts/active")
    out = set()
    for it in (j.get("data") or []):
        std = std_from_kucoin(it.get("symbol",""))
        if std.endswith(("USDT","USD")):
            out.add(std)
    return out

def list_bitget_umcbl() -> Set[str]:
    _, j = fetch_json("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType":"umcbl"})
    out = set()
    for it in (j.get("data") or []):
        std = std_from_suffix(it.get("symbol",""), "_UMCBL")
        if std.endswith("USDT"):
            out.add(std)
    return out

def list_gate_usdt() -> Set[str]:
    _, rows = fetch_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    out = set()
    for it in rows:
        std = std_from_underscore(it.get("name",""))
        if std.endswith("USDT"):
            out.add(std)
    return out

def list_phemex() -> Set[str]:
    _, j = fetch_json("https://api.phemex.com/exchange/public/products")
    rows: Optional[List[Any]] = None
    if isinstance(j, dict):
        data = j.get("data")
        if isinstance(data, dict):
            rows = data.get("products")
        elif isinstance(data, list):
            rows = data
        else:
            rows = j.get("products") or j.get("result")
    elif isinstance(j, list):
        rows = j
    out = set()
    if not rows:
        return out
    for it in rows:
        try:
            sym = str(it.get("symbol","")).upper()
            typ = it.get("type") or it.get("contractType") or ""
            if isinstance(typ, str) and "perpet" in typ.lower():
                out.add(sym)
        except Exception:
            continue
    return out

def list_krakenf() -> Set[str]:
    _, j = fetch_json("https://futures.kraken.com/derivatives/api/v3/instruments",
                      timeout=max(getattr(SESSION,'request_timeout', DEFAULT_TIMEOUT), 30))
    out = set()
    for it in (j.get("instruments") or []):
        std = std_from_krakenf(it.get("symbol",""))
        if std and std.endswith(("USDT","USD")):
            out.add(std)
    return out

def list_deribit() -> Set[str]:
    out = set()
    for cur in ["BTC","ETH"]:
        _, j = fetch_json("https://www.deribit.com/api/v2/public/get_instruments",
                          params={"currency":cur,"kind":"future","expired":"false"})
        for it in (j.get("result") or []):
            if it.get("settlement_period") == "perpetual":
                std = std_from_deribit(it.get("instrument_name",""))
                if std:
                    out.add(std)
    return out

LISTERS = {
    "binance":  list_binance_perp_usdt,
    "bybit":    list_bybit_linear,
    "okx":      list_okx_swap,
    "mexc":     list_mexc_contracts,
    "kucoin":   list_kucoin_futures,
    "bitget":   list_bitget_umcbl,
    "gate":     list_gate_usdt,
    "phemex":   list_phemex,
    "krakenf":  list_krakenf,
    "deribit":  list_deribit,
}

COMMON_SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","LINKUSDT","BNBUSDT","ADAUSDT"]

# ---------- GCS helpers + BACKET/GCS_BUCKET ----------
GCS_AVAILABLE = False
try:
    from google.cloud import storage  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    GCS_AVAILABLE = True
except Exception:
    GCS_AVAILABLE = False

def is_gs(path: Optional[str]) -> bool:
    return bool(path) and str(path).startswith("gs://")

def gcs_split(gs_path: str) -> Tuple[str,str]:
    raw = gs_path[5:]
    bucket, _, blob = raw.partition("/")
    return bucket, blob

def gcs_client():
    """
    Инициализация клиента GCS:
    1) GCS_KEY_JSON — JSON-строка с ключом;
    2) GOOGLE_APPLICATION_CREDENTIALS — ПУТЬ к JSON-файлу (стандарт);
    3) ADC по умолчанию (если доступно).
    """
    if not GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage not installed")
    key_json = os.getenv("GCS_KEY_JSON", "").strip()
    if key_json:
        import json
        info = json.loads(key_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return storage.Client(project=info.get("project_id"), credentials=creds)
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if key_path:
        # если это путь к файлу — storage.Client сам его подхватит
        return storage.Client()
    # последняя надежда — ADC
    return storage.Client()

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
        logging.info("GCS: uploaded %s (%d rows)", gs_path, len(df))
    except Exception as e:
        logging.error("GCS write error for %s: %s", gs_path, e)
        raise

def resolve_bucket_env() -> Optional[str]:
    """
    Возьмём бакет из BACKET (твоя переменная) или из GCS_BUCKET (синоним).
    """

    b2 = os.getenv("GCS_BUCKET", "").strip()

    if b2:
        return b2
    return None

def bucketize_path(path: Optional[str]) -> Optional[str]:
    """
    Если задан BACKET или GCS_BUCKET и путь относительный — вернём gs://<bucket>/<path>.
    Если путь уже gs://... или абсолютный — оставим как есть.
    """
    if not path or path.strip() == "":
        return path
    p = path.strip()
    if is_gs(p) or os.path.isabs(p):
        return p
    bucket = resolve_bucket_env()
    if bucket:
        p = p.lstrip("/").replace("\\","/")
        return f"gs://{bucket}/{p}"
    return p

# ---------- матрица ----------
def build_matrix(exchanges: List[str], universe_mode: str, manual_symbols: Optional[List[str]],
                 top_n: int, min_quote: float, sleep_ms: int) -> pd.DataFrame:
    exchanges = [e.lower().strip() for e in exchanges if e.strip()]
    for e in exchanges:
        if e not in LISTERS:
            raise ValueError(f"Unsupported exchange: {e}")

    avail: Dict[str, Set[str]] = {}
    for idx, e in enumerate(exchanges):
        try:
            avail[e] = LISTERS[e]()
        except Exception as ex:
            logging.warning("Failed to list %s: %s", e, ex)
            avail[e] = set()
        if sleep_ms and idx < len(exchanges)-1:
            time.sleep(sleep_ms/1000.0)

    if universe_mode == "common":
        universe = set(COMMON_SYMBOLS)
    elif universe_mode == "binance-top":
        try:
            top = binance_top_perp_usdt(top_n=top_n, min_quote=min_quote)
            universe = set(top)
        except Exception as ex:
            logging.warning("binance-top failed, fallback to COMMON: %s", ex)
            universe = set(COMMON_SYMBOLS)
    elif universe_mode == "union":
        universe = set()
        for s in avail.values():
            universe |= s
    elif universe_mode == "manual":
        if not manual_symbols:
            raise ValueError("manual universe requires --symbols")
        universe = set([x.strip().upper() for x in manual_symbols if x.strip()])
    else:
        raise ValueError("universe must be one of: common | binance-top | union | manual")

    rows = []
    for sym in sorted(universe):
        row = {"symbol": sym}
        listed_on = 0
        for ex in exchanges:
            has = sym in avail.get(ex, set())
            row[ex] = bool(has)
            if has:
                listed_on += 1
        row["listed_on"] = listed_on
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df[["symbol"] + exchanges + ["listed_on"]]
    df = df.sort_values(["listed_on","symbol"], ascending=[False, True]).reset_index(drop=True)
    return df

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchanges", type=str, required=True, help="comma-separated list")
    ap.add_argument("--universe", type=str, default="common", help="common | binance-top | union | manual")
    ap.add_argument("--symbols", type=str, default="", help="manual symbols CSV when --universe manual")
    ap.add_argument("--top-n", type=int, default=200, help="for binance-top")
    ap.add_argument("--min-quote", type=float, default=10_000_000, help="for binance-top")
    ap.add_argument("--save", type=str, default="", help="CSV path (local or gs://...)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="HTTP retries")
    ap.add_argument("--sleep-ms", type=int, default=DEFAULT_SLEEP_MS, help="sleep between exchange requests (ms)")
    ap.add_argument("--debug", action="store_true", help="debug logs")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    proxy = os.getenv("PROXY","").strip() or os.getenv("HTTPS_PROXY","").strip() or os.getenv("HTTP_PROXY","").strip()
    global SESSION
    SESSION = build_session(timeout=args.timeout, retries=args.retries, backoff=DEFAULT_BACKOFF, proxy=proxy)

    exchanges = [x.strip() for x in args.exchanges.split(",") if x.strip()]
    manual_symbols = [x.strip() for x in args.symbols.split(",")] if args.symbols else None

    df = build_matrix(exchanges, args.universe, manual_symbols, args.top_n, args.min_quote, args.sleep_ms)

    # вывод
    try:
        print(df.to_string(index=False))
    except Exception:
        print(df.head().to_string(index=False))

    # сохранение
    if args.save:
        save_path = bucketize_path(args.save)
        if is_gs(save_path):
            try:
                gcs_write_csv(save_path, df)
                logging.info("Saved to %s", save_path)
            except Exception as e:
                logging.error("Save failed to %s: %s", save_path, e)
                raise
        else:
            df.to_csv(save_path, index=False)
            logging.info("Saved to %s", save_path)

if __name__ == "__main__":
    main()
