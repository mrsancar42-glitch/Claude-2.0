#!/usr/bin/env python3
"""
Signal G7 — трендовый свинг-сканер (OKX + Binance + Bybit -> сигналы в Telegram)

Режим ОДИН — «тренд» (ICT удалён). Дневной таймфрейм (1D):
  * направление: тренд монеты (цена > EMA50 > EMA100, EMA50 растёт) + режим BTC
  * относительная сила монеты за 21 день (лонг — только из сильной половины)
  * сетап: пробой 20-дневного канала с объёмом ИЛИ откат к EMA20 после недавнего пробоя
  * стоп = ATR_STOP_MULT * ATR(14); TP1 = 1.5R (закрыть 50%, стоп в безубыток),
    остаток — chandelier-трейлинг 3 ATR.

Как работает сканирование:
  1. Загружается список USDT-перпетуалов с ТРЁХ бирж: OKX, Binance, Bybit.
     Если одна биржа недоступна — бот продолжает работу на остальных.
  2. Монеты сводятся к общему тикеру (1000PEPE = PEPE и т.п., масштаб учитывается).
  3. Сверка: сигнал отправляется только по монетам, которые торгуются на Bybit
     (REQUIRE_BYBIT=1). Цены в сообщении пересчитываются под контракт Bybit.
  4. Свечи берутся с первой доступной биржи (DATA_SOURCES, по умолчанию okx,binance,bybit),
     при ошибке автоматически берётся следующая.
  5. Новая дневная свеча обрабатывается ОДИН раз (не каждый час). Каждый час бот
     только сопровождает открытые позиции (стоп / TP1 / трейлинг).

Скрипт ничего не торгует и не размещает ордера. Это идея для ручной проверки.

Запуск (cron раз в час, через 5 минут после закрытия 1H-свечи, UTC):
  5 * * * *  /usr/bin/python3 /path/signal_g7.py

Аргументы:
  --dry        пробный прогон: весь вывод и найденные сигналы уходят в Telegram
               с пометкой DRY-RUN; состояние (позиции, пауза) НЕ сохраняется
  --backtest   бэктест трендового режима на истории OKX (BT_DAYS, BT_TOP, BT_INSTS)
  --stats      статистика закрытых сигналов из state-файла
  --force      искать сигналы, даже если эта дневная свеча уже обработана

Переменные окружения:
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
  EQUITY_USDT          размер счёта                                   (1000)
  RISK_PCT             риск на сделку, % счёта                        (0.5)
  MAX_LEVERAGE         потолок плеча                                  (3)
  MAX_OPEN             макс. одновременных позиций                    (6)
  MAX_NEW_PER_RUN      макс. новых сигналов за запуск                 (2)
  MAX_PER_DAY          макс. сигналов за 24 часа                      (3)
  TOP_N                сколько самых ликвидных монет, 0 = все         (0)
  MIN_TURNOVER_USDT    мин. суточный оборот (максимум по биржам)      (5000000)
  RS_MIN_PCT           лонг только из верхних (100-X)% по силе 21д    (50)
  EXCLUDE              тикеры-исключения через запятую                (пусто)
  ALLOW_SHORT          1 = шорты при медвежьем BTC                    (0)
  ATR_STOP_MULT        стоп в дневных ATR                             (2.5)
  MAX_STOP_PCT         макс. ширина стопа, %                          (18)
  MAX_FUNDING_PCT      макс. фандинг против позиции, %/период        (0.05)
  MAX_CHASE_ATR        не слать, если цена уже ушла от входа > N ATR  (0.5)
  SYMBOL_COOLDOWN_DAYS пауза по монете после сигнала                  (2)
  MAX_HOLD_DAYS        обзор позиции без TP1 через N дней             (30)
  REQUIRE_BYBIT        1 = слать только монеты, которые есть на Bybit (1)
  BYBIT_ON_ERROR       send | skip — если список Bybit не загрузился  (send)
  DATA_SOURCES         порядок бирж для свечей                        (okx,binance,bybit)
  BYBIT_HOSTS / BINANCE_HOSTS / OKX_HOST   адреса API через запятую
  DAILY_SUMMARY        1 = раз в день короткий отчёт о скане          (1)
  STATE_FILE           (signal_g7_state.json)
"""

import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import requests

# ----------------------------------------------------------------- настройки
TIMEOUT = 20
H1 = 3_600_000
DAY = 24 * H1
UA = {"User-Agent": "SignalG7-Trend-Scanner/2.0"}


def _f(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return float(default)


def _hosts(env, default):
    return [h.strip().rstrip("/") for h in os.getenv(env, default).split(",") if h.strip()]


EQUITY = _f("EQUITY_USDT", 1000)
RISK_PCT = _f("RISK_PCT", 0.5)
MAX_LEV = _f("MAX_LEVERAGE", 3)
MAX_OPEN = int(_f("MAX_OPEN", 6))
MAX_NEW = int(_f("MAX_NEW_PER_RUN", 2))
MAX_PER_DAY = int(_f("MAX_PER_DAY", 3))
TOP_N = int(_f("TOP_N", 0))
MIN_TURNOVER = _f("MIN_TURNOVER_USDT", 5_000_000)
RS_MIN_PCT = _f("RS_MIN_PCT", 50)
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "0") == "1"
ATR_STOP = _f("ATR_STOP_MULT", 2.5)
MAX_STOP_PCT = _f("MAX_STOP_PCT", 18)
MAX_FUNDING = _f("MAX_FUNDING_PCT", 0.05)
MAX_CHASE_ATR = _f("MAX_CHASE_ATR", 0.5)
COOLDOWN_DAYS = _f("SYMBOL_COOLDOWN_DAYS", 2)
MAX_HOLD_DAYS = _f("MAX_HOLD_DAYS", 30)
STATE_FILE = os.getenv("STATE_FILE", "signal_g7_state.json")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EXCLUDE = {s.strip().upper() for s in os.getenv("EXCLUDE", "").split(",") if s.strip()}
REQUIRE_BYBIT = os.getenv("REQUIRE_BYBIT", "1") == "1"
BYBIT_ON_ERROR = os.getenv("BYBIT_ON_ERROR", "send").strip().lower()
DAILY_SUMMARY = os.getenv("DAILY_SUMMARY", "1") == "1"
SIGNAL_MAX_AGE_H = 12          # дневная свеча старше N часов — сигналы уже неактуальны
FETCH_WORKERS = 5              # параллельная загрузка свечей
MIN_NOTIONAL = 5.0             # минимальная стоимость ордера на Bybit, USDT

EXCHANGES = ("okx", "binance", "bybit")
HOSTS = {
    "okx": _hosts("OKX_HOST", "https://www.okx.com"),
    "binance": _hosts("BINANCE_HOSTS", "https://fapi.binance.com,https://fapi1.binance.com,"
                                       "https://fapi2.binance.com,https://fapi3.binance.com"),
    "bybit": _hosts("BYBIT_HOSTS", "https://api.bybit.com,https://api.bytick.com"),
}
_src = [s.strip().lower() for s in os.getenv("DATA_SOURCES", "okx,binance,bybit").split(",")]
DATA_SOURCES = [s for s in _src if s in EXCHANGES] or list(EXCHANGES)

TP1_R = 1.5          # первая цель в R (50% позиции, стоп в безубыток)
TP2_R = 3.0          # вторая цель в R (ориентир для остатка)
TRAIL_ATR = 3.0      # chandelier-трейлинг остатка (дневной ATR)
DON_N = 20           # канал пробоя (1D)
RS_N = 21            # окно относительной силы (3 недели)
FEE_ROUNDTRIP_PCT = 0.12   # комиссии + проскальзывание туда-обратно (оценка, бэктест)
STABLES = {"USDC", "DAI", "TUSD", "FDUSD", "USDE", "USDG", "USDP", "BUSD", "PYUSD", "USD1", "RLUSD"}
MIN_BARS = 110       # прогрев дневных индикаторов

DRY = "--dry" in sys.argv
LOG = []


def log(*parts):
    line = " ".join(str(p) for p in parts)
    print(line, flush=True)
    LOG.append(line)


# ----------------------------------------------------------------- HTTP (общий)
def http_json(hosts, path, params=None, retries=3, label="API"):
    """GET с повторами. 403/451 (блок региона) — сразу следующий хост."""
    last = None
    for host in hosts:
        for attempt in range(retries):
            try:
                r = requests.get(host + path, params=params, timeout=TIMEOUT, headers=UA)
                if r.status_code in (429, 418, 500, 502, 503, 504):
                    last = f"{host}: HTTP {r.status_code}"
                    time.sleep(1.5 * (attempt + 1))
                    continue
                if r.status_code in (403, 451):
                    last = f"{host}: HTTP {r.status_code} (доступ закрыт для этого региона/IP)"
                    break
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as exc:
                last = f"{host}: {exc}"
                time.sleep(0.8 * (attempt + 1))
    raise RuntimeError(f"{label} {path}: {last}")


def okx_json(path, params=None):
    for attempt in range(4):
        payload = http_json(HOSTS["okx"], path, params, label="OKX")
        code = payload.get("code")
        if code == "50011":                       # rate limit
            time.sleep(1.5 * (attempt + 1))
            continue
        if code != "0":
            raise RuntimeError(f"OKX {path}: {payload.get('msg')} ({code})")
        return payload.get("data", [])
    raise RuntimeError(f"OKX {path}: превышен лимит запросов")


# ----------------------------------------------------------------- тикеры / масштаб
def split_base(raw):
    """'1000PEPE' -> ('PEPE', 1000); 'SHIB1000' -> ('SHIB', 1000); '1MBABYDOGE' -> ('BABYDOGE', 1e6)."""
    s, scale = raw.upper(), 1
    m = re.match(r"^(1(?:0{3,}))([A-Z].*)$", s)
    if m:
        return m.group(2), int(m.group(1))
    if s.startswith("1M") and len(s) > 2 and s[2].isalpha():
        return s[2:], 1_000_000
    m = re.match(r"^([A-Z].*[A-Z])1000$", s)
    if m:
        return m.group(1), 1000
    return s, scale


def _put(out, raw_base, row):
    base, scale = split_base(raw_base)
    if base in STABLES or base in EXCLUDE:
        return
    row["scale"] = scale
    if base not in out or row["turnover"] > out[base]["turnover"]:
        out[base] = row


# ----------------------------------------------------------------- загрузчики бирж
def load_okx():
    ins = {}
    for r in okx_json("/api/v5/public/instruments", {"instType": "SWAP"}):
        iid = r.get("instId", "")
        if r.get("settleCcy") == "USDT" and r.get("state") == "live" and iid.endswith("-USDT-SWAP"):
            ins[iid] = float(r["ctVal"]) * float(r.get("ctMult") or 1)
    out = {}
    for t in okx_json("/api/v5/market/tickers", {"instType": "SWAP"}):
        iid = t.get("instId")
        if iid not in ins:
            continue
        try:
            last = float(t["last"])
            turnover = float(t["vol24h"]) * ins[iid] * last
        except (KeyError, ValueError):
            continue
        _put(out, iid.split("-")[0], {"sym": iid, "turnover": turnover, "last": last, "funding": None})
    return out


def load_binance():
    info = http_json(HOSTS["binance"], "/fapi/v1/exchangeInfo", label="Binance")
    ok = {s["symbol"]: s for s in info.get("symbols", [])
          if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
          and s.get("quoteAsset") == "USDT"}
    tick = http_json(HOSTS["binance"], "/fapi/v1/ticker/24hr", label="Binance")
    fund = {}
    try:
        for p in http_json(HOSTS["binance"], "/fapi/v1/premiumIndex", label="Binance"):
            fund[p["symbol"]] = float(p["lastFundingRate"]) * 100.0
    except Exception:                                    # noqa: BLE001  — фандинг не критичен
        pass
    out = {}
    for t in tick:
        sym = t.get("symbol")
        if sym not in ok:
            continue
        try:
            last, turnover = float(t["lastPrice"]), float(t["quoteVolume"])
        except (KeyError, ValueError):
            continue
        _put(out, ok[sym]["baseAsset"], {"sym": sym, "turnover": turnover, "last": last,
                                         "funding": fund.get(sym)})
    return out


def load_bybit():
    syms, cursor = {}, ""
    for _ in range(20):
        params = {"category": "linear", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        payload = http_json(HOSTS["bybit"], "/v5/market/instruments-info", params, label="Bybit")
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit: retCode={payload.get('retCode')} {payload.get('retMsg')}")
        res = payload.get("result") or {}
        for row in res.get("list", []):
            if (row.get("status") == "Trading" and row.get("quoteCoin") == "USDT"
                    and row.get("contractType") == "LinearPerpetual"):
                syms[row["symbol"]] = row.get("baseCoin") or row["symbol"][:-4]
        cursor = res.get("nextPageCursor") or ""
        if not cursor:
            break
    if not syms:
        raise RuntimeError("Bybit: пустой список инструментов")
    out = {}
    tick = http_json(HOSTS["bybit"], "/v5/market/tickers", {"category": "linear"}, label="Bybit")
    for t in (tick.get("result") or {}).get("list", []):
        sym = t.get("symbol")
        if sym not in syms:
            continue
        try:
            last, turnover = float(t["lastPrice"]), float(t["turnover24h"])
        except (KeyError, ValueError):
            continue
        try:
            fr = float(t["fundingRate"]) * 100.0 if t.get("fundingRate") not in (None, "") else None
        except ValueError:
            fr = None
        _put(out, syms[sym], {"sym": sym, "turnover": turnover, "last": last, "funding": fr})
    # монеты без тикера тоже считаем «есть на Bybit» (оборот 0 — отсеются фильтром ликвидности)
    for sym, base in syms.items():
        b, _ = split_base(base)
        if b not in out and b not in STABLES and b not in EXCLUDE:
            _put(out, base, {"sym": sym, "turnover": 0.0, "last": 0.0, "funding": None})
    return out


LOADERS = {"okx": load_okx, "binance": load_binance, "bybit": load_bybit}


def build_coins():
    """Сводит три биржи в один список. Возвращает (coins, loaded, bybit_ok)."""
    loaded, per = [], {}
    for ex in EXCHANGES:
        try:
            per[ex] = LOADERS[ex]()
            loaded.append(ex)
            log(f"{ex.upper()}: загружено {len(per[ex])} USDT-перпетуалов")
        except Exception as exc:                          # noqa: BLE001
            log(f"⚠ {ex.upper()} недоступен: {exc}")
    if not loaded:
        raise RuntimeError("Ни одна биржа не отвечает (OKX, Binance, Bybit)")
    coins = {}
    for ex, data in per.items():
        for base, row in data.items():
            c = coins.setdefault(base, {"base": base, "turnover": 0.0, "ex": {}})
            c["ex"][ex] = {"sym": row["sym"], "scale": row["scale"], "last": row["last"],
                           "funding": row["funding"]}
            c["turnover"] = max(c["turnover"], row["turnover"])
    bybit_ok = "bybit" in per
    total = len(coins)
    coins = {b: c for b, c in coins.items() if c["turnover"] >= MIN_TURNOVER}
    log(f"Всего уникальных монет: {total} · с оборотом ≥ {MIN_TURNOVER:,.0f} USDT: {len(coins)}")
    if REQUIRE_BYBIT and bybit_ok:
        before = len(coins)
        coins = {b: c for b, c in coins.items() if "bybit" in c["ex"]}
        log(f"Сверка с Bybit: оставлено {len(coins)} из {before} (остальных на Bybit нет)")
    ranked = sorted(coins.values(), key=lambda c: c["turnover"], reverse=True)
    if TOP_N > 0:
        ranked = ranked[:TOP_N]
    return ranked, loaded, bybit_ok


# ----------------------------------------------------------------- свечи (любая биржа)
_BAR = {
    "okx": {"1D": "1Dutc", "1H": "1H"},
    "binance": {"1D": "1d", "1H": "1h"},
    "bybit": {"1D": "D", "1H": "60"},
}
_BAR_MS = {"1D": DAY, "1H": H1}
HEALTH = {ex: {"ok": 0, "fail": 0} for ex in EXCHANGES}


def ex_dead(ex):
    """Биржа «мертва» в этом запуске: много ошибок подряд и ни одного успеха."""
    h = HEALTH[ex]
    return h["fail"] >= 10 and h["ok"] == 0


def fetch_klines(ex, sym, tf, limit=300):
    """Закрытые свечи [(ts,o,h,l,c,vol)], по возрастанию времени."""
    now_ms = time.time() * 1000
    bar_ms = _BAR_MS[tf]
    out = []
    if ex == "okx":
        rows = okx_json("/api/v5/market/candles",
                        {"instId": sym, "bar": _BAR[ex][tf], "limit": str(min(limit, 300))})
        for r in rows:
            if len(r) > 8 and r[8] == "1":
                out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] or 0)))
    elif ex == "binance":
        rows = http_json(HOSTS["binance"], "/fapi/v1/klines",
                         {"symbol": sym, "interval": _BAR[ex][tf], "limit": str(limit)}, label="Binance")
        for r in rows:
            if int(r[6]) < now_ms:                       # closeTime в прошлом = свеча закрыта
                out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] or 0)))
    else:
        payload = http_json(HOSTS["bybit"], "/v5/market/kline",
                            {"category": "linear", "symbol": sym, "interval": _BAR[ex][tf],
                             "limit": str(min(limit, 1000))}, label="Bybit")
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit kline: {payload.get('retMsg')}")
        for r in (payload.get("result") or {}).get("list", []):
            if int(r[0]) + bar_ms <= now_ms:
                out.append((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] or 0)))
    out.sort()
    return out


def fetch_any(exinfo, tf, limit=300, order=None, scale=None):
    """Пробует биржи по очереди. exinfo = {ex: {"sym","scale"}}.
    scale — если задан, берём только биржи с таким же масштабом цены.
    Возвращает (candles, ex)."""
    errs = []
    for ex in (order or DATA_SOURCES):
        info = exinfo.get(ex)
        if not info or ex_dead(ex):
            continue
        if scale is not None and info["scale"] != scale:
            continue
        try:
            cs = fetch_klines(ex, info["sym"], tf, limit)
            HEALTH[ex]["ok"] += 1
            if cs:
                return cs, ex
            errs.append(f"{ex}: пусто")
        except Exception as exc:                          # noqa: BLE001
            HEALTH[ex]["fail"] += 1
            errs.append(f"{ex}: {exc}")
    raise RuntimeError("; ".join(errs) or "нет доступной биржи")


def get_history_okx(inst, bar, total):
    """Постраничная история OKX для бэктеста (history-candles, по 100)."""
    got, after = {}, None
    while len(got) < total:
        params = {"instId": inst, "bar": bar, "limit": "100"}
        if after:
            params["after"] = str(after)
        rows = okx_json("/api/v5/market/history-candles", params)
        if not rows:
            break
        for r in rows:
            if len(r) > 8 and r[8] == "1":
                got[int(r[0])] = (int(r[0]), float(r[1]), float(r[2]), float(r[3]),
                                  float(r[4]), float(r[5] or 0))
        oldest = min(int(r[0]) for r in rows)
        if after is not None and oldest >= after:
            break
        after = oldest
        time.sleep(0.15)
    return [got[k] for k in sorted(got)]


# ----------------------------------------------------------------- индикаторы
def ema(values, n):
    k = 2.0 / (n + 1)
    out, prev = [], None
    for v in values:
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(prev)
    return out


def atr_series(candles, n=14):
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c[2] - c[3])
        else:
            pc = candles[i - 1][4]
            trs.append(max(c[2] - c[3], abs(c[2] - pc), abs(c[3] - pc)))
    out, prev = [], None
    for i, tr in enumerate(trs):
        if i < n:
            prev = sum(trs[:i + 1]) / (i + 1)
        else:
            prev = (prev * (n - 1) + tr) / n
        out.append(prev)
    return out


def build_indicators(candles):
    o = [c[1] for c in candles]
    h = [c[2] for c in candles]
    l = [c[3] for c in candles]
    cl = [c[4] for c in candles]
    v = [c[5] for c in candles]
    n = len(candles)
    don_hi, don_lo, vol_ma = [None] * n, [None] * n, [None] * n
    for i in range(n):
        if i >= DON_N:
            don_hi[i] = max(h[i - DON_N:i])
            don_lo[i] = min(l[i - DON_N:i])
        if i >= 20:
            vol_ma[i] = sum(v[i - 20:i]) / 20
    return {
        "ts": [c[0] for c in candles], "o": o, "h": h, "l": l, "c": cl, "v": v,
        "ema20": ema(cl, 20), "ema50": ema(cl, 50), "ema100": ema(cl, 100),
        "atr": atr_series(candles, 14),
        "don_hi": don_hi, "don_lo": don_lo, "vol_ma": vol_ma,
    }


def btc_regime(ind, i=None):
    """bull / bear / neutral по дневному BTC."""
    if i is None:
        i = len(ind["c"]) - 1
    if i < MIN_BARS:
        return "neutral"
    c, e50, e100 = ind["c"][i], ind["ema50"][i], ind["ema100"][i]
    if c > e50 and e50 > e100:
        return "bull"
    if c < e50 and e50 < e100:
        return "bear"
    return "neutral"


def trend_gate(ind, i, side, regime):
    """Дневной фильтр направления: тренд монеты + режим BTC."""
    if i < MIN_BARS or i >= len(ind["c"]):
        return False
    c, e50, e100 = ind["c"][i], ind["ema50"][i], ind["ema100"][i]
    if side == "long":
        return regime != "bear" and c > e50 and e50 > e100 and e50 > ind["ema50"][i - 5]
    return ALLOW_SHORT and regime == "bear" and c < e50 and e50 < e100 and e50 < ind["ema50"][i - 5]


# ----------------------------------------------------------------- сетап: дневной тренд
def signal_at(ind, i, regime, side):
    """Трендовый сетап на закрытой дневной свече i (пробой / откат к EMA20)."""
    if i < MIN_BARS or ind["don_hi"][i] is None or ind["vol_ma"][i] is None:
        return None
    if not trend_gate(ind, i, side, regime):
        return None
    c, o, h, l = ind["c"][i], ind["o"][i], ind["h"][i], ind["l"][i]
    e20 = ind["ema20"][i]
    a = ind["atr"][i]
    if a <= 0 or c <= 0:
        return None
    ext = (c - e20) / a if side == "long" else (e20 - c) / a   # насколько цена «убежала»
    if ext > 3.0:
        return None

    if side == "long":
        breakout = c > ind["don_hi"][i] and ind["v"][i] >= ind["vol_ma"][i]
        had_break = any(ind["don_hi"][j] is not None and ind["c"][j] > ind["don_hi"][j]
                        for j in range(i - 15, i))
        pullback = had_break and l <= e20 + 0.25 * a and c > e20 and c > o and not breakout
    else:
        breakout = c < ind["don_lo"][i] and ind["v"][i] >= ind["vol_ma"][i]
        had_break = any(ind["don_lo"][j] is not None and ind["c"][j] < ind["don_lo"][j]
                        for j in range(i - 15, i))
        pullback = had_break and h >= e20 - 0.25 * a and c < e20 and c < o and not breakout

    if not (breakout or pullback):
        return None
    setup = "Пробой 20д-канала" if breakout else "Откат к EMA20 в тренде"
    entry = c
    risk = ATR_STOP * a
    if side == "long":
        stop = entry - risk
        tp1, tp2 = entry + TP1_R * risk, entry + TP2_R * risk
    else:
        stop = entry + risk
        tp1, tp2 = entry - TP1_R * risk, entry - TP2_R * risk
    stop_pct = risk / entry * 100
    if stop <= 0 or stop_pct > MAX_STOP_PCT:
        return None
    return {"side": side, "setup": setup, "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "risk": risk, "stop_pct": stop_pct, "atr": a, "ts": ind["ts"][i]}


# ----------------------------------------------------------------- размер позиции
def size_position(price, stop_pct):
    """price — цена в масштабе Bybit-контракта. Размер считается в USDT и в монетах."""
    risk_usdt = EQUITY * RISK_PCT / 100.0
    notional = risk_usdt / (stop_pct / 100.0)
    capped = False
    if notional / EQUITY > MAX_LEV:
        notional, capped = EQUITY * MAX_LEV, True
    if notional < MIN_NOTIONAL:
        return None
    real_risk = notional * stop_pct / 100.0
    return {"notional": notional, "qty": notional / price, "lev": notional / EQUITY,
            "risk_usdt": real_risk, "risk_pct": real_risk / EQUITY * 100, "capped": capped}


# ----------------------------------------------------------------- состояние / Telegram
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
    except (OSError, json.JSONDecodeError):
        st = {}
    st.setdefault("open", {})
    st.setdefault("closed", [])
    st.setdefault("cooldown", {})
    st.setdefault("sent", [])
    st.setdefault("last_scan_day", 0)
    # миграция со старого формата (ключи вида "BTC-USDT-SWAP")
    for key in list(st["open"].keys()):
        if key.endswith("-USDT-SWAP"):
            pos = st["open"].pop(key)
            base = key.split("-")[0]
            pos.setdefault("ex", "okx")
            pos.setdefault("dscale", 1)
            pos.setdefault("mult", 1.0)
            pos.setdefault("syms", {"okx": {"sym": key, "scale": 1}})
            st["open"][base] = pos
    for key in list(st["cooldown"].keys()):
        if key.endswith("-USDT-SWAP"):
            st["cooldown"][key.split("-")[0]] = st["cooldown"].pop(key)
    return st


def save_state(st):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def _plain(text):
    for t in ("<b>", "</b>", "<code>", "</code>", "<pre>", "</pre>"):
        text = text.replace(t, "")
    return html.unescape(text)


def _chunks(text, limit=3800):
    out, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit and cur:
            out.append(cur)
            cur = ""
        while len(line) > limit:
            out.append(line[:limit])
            line = line[limit:]
        cur += line + "\n"
    if cur.strip():
        out.append(cur)
    return out


def tg_send(text):
    """Отправка в Telegram (HTML). В dry-режиме добавляет пометку DRY-RUN."""
    if DRY:
        text = "🧪 <b>DRY-RUN</b> · позиции не записываются\n" + text
    if not TG_TOKEN or not TG_CHAT:
        print(_plain(text))
        print("-" * 40)
        print("(Telegram не настроен: задайте TELEGRAM_TOKEN и TELEGRAM_CHAT_ID)")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    for chunk in _chunks(text):
        for attempt in range(3):
            r = requests.post(url, json={"chat_id": TG_CHAT, "text": chunk, "parse_mode": "HTML",
                                         "disable_web_page_preview": True}, timeout=TIMEOUT)
            if r.status_code == 429:
                try:
                    wait = int(r.json().get("parameters", {}).get("retry_after", 3))
                except Exception:                         # noqa: BLE001
                    wait = 3
                time.sleep(min(wait, 30) + 1)
                continue
            if not r.ok:
                raise RuntimeError(f"Telegram {r.status_code}: {r.text[:200]}")
            break
        else:
            raise RuntimeError("Telegram: слишком много повторов (429)")
        time.sleep(1.1)


def fmt(p):
    if p >= 1000:
        return f"{p:,.1f}"
    if p >= 1:
        return f"{p:.4f}".rstrip("0").rstrip(".")
    return f"{p:.8f}".rstrip("0").rstrip(".")


def name(base):
    return f"{base}/USDT"


def fmt_signal(base, sig, size, rs_pct, regime, funding, ex, bybit, mult):
    """Цены сигнала даются в масштабе Bybit-контракта (mult уже применён к уровням)."""
    long_ = sig["side"] == "long"
    arrow = "🟢 LONG" if long_ else "🔴 SHORT"
    m = mult
    lines = [
        f"{arrow} <b>{name(base)}</b> — свинг (дни–недели)",
        f"Сетап: {sig['setup']}",
        f"Режим BTC: {regime} · RS 21д: {'топ' if long_ else 'низ'} {rs_pct:.0f}%",
    ]
    if bybit == "unverified":
        lines.append("⚠ Bybit: наличие не удалось проверить — проверьте вручную")
    elif bybit:
        t = f"Bybit: ✅ {bybit['sym']} (USDT perp) · свечи: {ex.upper()}"
        if bybit["scale"] != 1:
            t += f" · ⚠ контракт ×{bybit['scale']:g}, цены ниже уже пересчитаны"
        lines.append(t)
    lines += [
        "",
        f"Вход: <code>{fmt(sig['entry'] * m)}</code> (не гнаться, если цена ушла дальше 0.5 ATR)",
        f"Стоп: <code>{fmt(sig['stop'] * m)}</code> ({sig['stop_pct']:.1f}%)",
        f"TP1: <code>{fmt(sig['tp1'] * m)}</code> ({TP1_R:g}R) — закрыть 50%, стоп в безубыток",
        f"TP2: <code>{fmt(sig['tp2'] * m)}</code> — остаток по трейлингу {TRAIL_ATR:g} ATR",
        "",
        f"Размер: ≈ {size['notional']:,.0f} USDT (~{size['qty']:,.4g} монет), плечо ≈ {size['lev']:.1f}x",
        f"Риск: {size['risk_usdt']:.1f} USDT ({size['risk_pct']:.2f}% счёта)"
        + (" — урезан лимитом плеча" if size["capped"] else ""),
        "Маржа: isolated. Ликвидация должна быть заметно дальше стопа.",
    ]
    if funding is not None:
        lines.append(f"Фандинг: {funding:+.4f}% за период")
    lines += ["", "Идея для ручной проверки, не гарантия результата. Прибыль системы "
                  "даёт хвост крупных выигрышей, серии стопов — норма."]
    return "\n".join(lines)


# ----------------------------------------------------------------- сканирование
BTC_EX = {"okx": {"sym": "BTC-USDT-SWAP", "scale": 1},
          "binance": {"sym": "BTCUSDT", "scale": 1},
          "bybit": {"sym": "BTCUSDT", "scale": 1}}


def rs_return(ind):
    c = ind["c"]
    if len(c) <= RS_N:
        return None
    return c[-1] / c[-1 - RS_N] - 1.0


def _fetch_daily(coin):
    try:
        cs, ex = fetch_any(coin["ex"], "1D", 300)
        time.sleep(0.08)
        return coin, cs, ex, None
    except Exception as exc:                              # noqa: BLE001
        return coin, None, None, exc


def scan_new(state, force=False):
    btc, _ = fetch_any(BTC_EX, "1D", 300)
    if len(btc) < MIN_BARS:
        raise RuntimeError("Недостаточно дневных данных BTC")
    day_ts = btc[-1][0]
    if not force and state.get("last_scan_day") == day_ts:
        log("Эта дневная свеча уже обработана; новых сигналов не ищу.")
        return []
    age_h = (time.time() * 1000 - (day_ts + DAY)) / H1
    if not force and age_h > SIGNAL_MAX_AGE_H:
        log(f"Дневная свеча закрылась {age_h:.0f} ч назад — сигналы устарели, пропускаю день.")
        state["last_scan_day"] = day_ts
        return []

    coins, loaded, bybit_ok = build_coins()
    if REQUIRE_BYBIT and not bybit_ok and BYBIT_ON_ERROR == "skip":
        log("Bybit недоступен и BYBIT_ON_ERROR=skip — день не обрабатываю (повторю в следующий запуск).")
        return []

    regime = btc_regime(build_indicators(btc))
    log(f"Режим BTC: {regime} · монет в скане: {len(coins)} · источники свечей: {','.join(DATA_SOURCES)}")

    # 1) дневные свечи по всем монетам (параллельно, с автопереключением бирж)
    daily, failed = {}, 0
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex_pool:
        for coin, cs, ex, err in ex_pool.map(_fetch_daily, coins):
            if err is not None:
                failed += 1
                if failed <= 15:
                    log(f"пропуск {coin['base']}: {err}")
            elif cs and len(cs) >= MIN_BARS:
                daily[coin["base"]] = (coin, build_indicators(cs), ex)
    if failed > 15:
        log(f"… и ещё {failed - 15} монет пропущено из-за ошибок загрузки")
    complete = failed <= max(3, 0.2 * len(coins))

    rets = {b: rs_return(v[1]) for b, v in daily.items()}
    rets = {b: r for b, r in rets.items() if r is not None}
    ordered = sorted(rets, key=lambda k: rets[k])           # от слабых к сильным
    pct = {k: (idx + 1) / len(ordered) * 100 for idx, k in enumerate(ordered)}

    # 2) фильтр направления + относительная сила + сетап
    now = time.time()
    gated, cands = 0, []
    for base, (coin, ind, dex) in daily.items():
        if base in state["open"]:
            continue
        if now - state["cooldown"].get(base, 0) < COOLDOWN_DAYS * 86400:
            continue
        di = len(ind["c"]) - 1
        p = pct.get(base, 50)
        for side in ("long", "short"):
            if not trend_gate(ind, di, side, regime):
                continue
            if side == "long" and p < RS_MIN_PCT:
                continue
            if side == "short" and p > 100 - RS_MIN_PCT:
                continue
            gated += 1
            sig = signal_at(ind, di, regime, side)
            if sig:
                strength = p if side == "long" else 100 - p
                cands.append((strength, base, coin, dex, sig, p))
    cands.sort(key=lambda z: z[0], reverse=True)
    log(f"Загружено дневок: {len(daily)} · прошли фильтр направления: {gated} · сетапов: {len(cands)}")

    # 3) лимиты, проверки и отправка
    state["sent"] = [t for t in state.get("sent", []) if now - t < 86400]
    room = min(MAX_NEW, MAX_OPEN - len(state["open"]), MAX_PER_DAY - len(state["sent"]))
    log(f"Сигналов за последние 24ч: {len(state['sent'])} из {MAX_PER_DAY} · "
        f"открытых позиций: {len(state['open'])} из {MAX_OPEN}")

    out, sent_n = [], 0
    for _, base, coin, dex, sig, p in cands:
        if len(out) >= room:
            break
        exinfo = coin["ex"]
        data_info = exinfo[dex]
        # не гнаться: цена уже ушла от входа или пробила стоп
        last = data_info.get("last") or 0
        if last > 0:
            moved = (last - sig["entry"]) if sig["side"] == "long" else (sig["entry"] - last)
            broke = last <= sig["stop"] if sig["side"] == "long" else last >= sig["stop"]
            if broke or moved > MAX_CHASE_ATR * sig["atr"]:
                log(f"пропуск {base}: цена ушла от входа (last {fmt(last)}, вход {fmt(sig['entry'])})")
                continue
        # сверка с Bybit и пересчёт масштаба цен
        if "bybit" in exinfo:
            bb = exinfo["bybit"]
            mult = bb["scale"] / data_info["scale"]
        elif REQUIRE_BYBIT and bybit_ok:
            log(f"пропуск {base}: нет на Bybit")
            continue
        elif REQUIRE_BYBIT:                                # список Bybit не загрузился
            bb, mult = "unverified", 1.0
        else:
            bb, mult = None, 1.0
        # фандинг: Bybit -> Binance -> OKX
        funding = None
        for fx in ("bybit", "binance", "okx"):
            if fx in exinfo and exinfo[fx].get("funding") is not None:
                funding = exinfo[fx]["funding"]
                break
        if funding is None and "okx" in exinfo:
            try:
                funding = float(okx_json("/api/v5/public/funding-rate",
                                         {"instId": exinfo["okx"]["sym"]})[0]["fundingRate"]) * 100.0
            except Exception:                              # noqa: BLE001
                funding = None
        if funding is not None:
            against = funding if sig["side"] == "long" else -funding
            if against > MAX_FUNDING:
                log(f"пропуск {base}: фандинг против позиции {funding:+.4f}%")
                continue
        size = size_position(sig["entry"] * mult, sig["stop_pct"])
        if not size:
            log(f"пропуск {base}: размер позиции меньше минимального ордера")
            continue
        out.append((base, coin, dex, sig, size, p, funding, bb, mult))

    for base, coin, dex, sig, size, p, funding, bb, mult in out:
        rs_pct = (100 - p) if sig["side"] == "long" else p
        try:
            tg_send(fmt_signal(base, sig, size, rs_pct, regime, funding, dex, bb, mult))
        except Exception as exc:                           # noqa: BLE001
            log(f"⚠ не удалось отправить сигнал {base} в Telegram: {exc}")
            continue
        now_ms_i = int(time.time() * 1000)
        state["open"][base] = {
            "side": sig["side"], "entry": sig["entry"], "stop": sig["stop"], "stop0": sig["stop"],
            "tp1": sig["tp1"], "tp2": sig["tp2"], "risk": sig["risk"], "atr": sig["atr"],
            "opened_ts": now_ms_i, "last_ts": now_ms_i, "status": "open", "setup": sig["setup"],
            "risk_usdt": size["risk_usdt"], "extreme": sig["entry"],
            "ex": dex, "dscale": coin["ex"][dex]["scale"], "mult": mult,
            "syms": {e: {"sym": v["sym"], "scale": v["scale"]} for e, v in coin["ex"].items()},
        }
        state["cooldown"][base] = time.time()
        state["sent"].append(time.time())
        sent_n += 1
        log(f"СИГНАЛ: {sig['side'].upper()} {name(base)} · {sig['setup']}")

    if complete:
        state["last_scan_day"] = day_ts
    else:
        log(f"⚠ Много ошибок загрузки ({failed} из {len(coins)}) — день не помечаю обработанным, "
            f"повторю в следующий запуск.")
    if not out:
        why = ""
        if room <= 0:
            why = " (достигнут лимит позиций или сигналов за сутки)"
        log("Новых сигналов нет." + why)
    if DAILY_SUMMARY and not DRY and complete:
        try:
            tg_send(f"📊 <b>SIGNAL G7</b> · скан дня завершён\n"
                    f"Биржи: {', '.join(e.upper() for e in loaded)} · монет в скане: {len(daily)}\n"
                    f"Режим BTC: {regime} · прошли фильтр: {gated} · сетапов: {len(cands)} · "
                    f"отправлено сигналов: {sent_n}")
        except Exception as exc:                           # noqa: BLE001
            log(f"⚠ сводка не отправлена: {exc}")
    return out


# ----------------------------------------------------------------- сопровождение
def r_of(pos, price):
    d = (price - pos["entry"]) if pos["side"] == "long" else (pos["entry"] - price)
    return d / pos["risk"]


def close_pos(state, base, pos, exit_price, reason, msgs):
    rem_r = r_of(pos, exit_price)
    total = pos.get("realized_r", 0.0) + rem_r * pos.get("remaining", 1.0)
    state["closed"].append({
        "inst": base, "side": pos["side"], "setup": pos["setup"], "src": "trend",
        "r": round(total, 2), "reason": reason, "opened_ts": pos["opened_ts"],
        "closed_ts": int(time.time() * 1000),
    })
    del state["open"][base]
    m = pos.get("mult", 1.0)
    icon = "✅" if total > 0 else "🛑"
    msgs.append(f"{icon} <b>{name(base)}</b> закрыт: {reason}\n"
                f"Выход ≈ <code>{fmt(exit_price * m)}</code> · результат {total:+.2f}R")


def _pos_candles(pos, tf, limit):
    order = [pos.get("ex", "okx")] + [e for e in DATA_SOURCES if e != pos.get("ex", "okx")]
    return fetch_any(pos["syms"], tf, limit, order=order, scale=pos.get("dscale", 1))[0]


def manage_open(state):
    msgs = []
    for base in list(state["open"].keys()):
        pos = state["open"][base]
        try:
            bars = _pos_candles(pos, "1H", 300)
            d1 = _pos_candles(pos, "1D", 150)
        except Exception as exc:                           # noqa: BLE001
            log(f"сопровождение {base}: {exc}")
            continue
        time.sleep(0.1)
        long_ = pos["side"] == "long"
        m = pos.get("mult", 1.0)
        pos.setdefault("extreme", pos["entry"])

        for ts, o, h, l, c, v in bars:
            if ts + H1 <= pos["last_ts"]:
                continue
            pos["extreme"] = max(pos["extreme"], h) if long_ else min(pos["extreme"], l)
            hit_stop = l <= pos["stop"] if long_ else h >= pos["stop"]
            hit_tp1 = h >= pos["tp1"] if long_ else l <= pos["tp1"]
            if hit_stop:                                   # при неоднозначности — стоп первым
                reason = "стоп" if pos["status"] == "open" else "трейлинг/безубыток"
                gap = min(o, pos["stop"]) if long_ else max(o, pos["stop"])   # гэп через стоп
                close_pos(state, base, pos, gap, reason, msgs)
                break
            if pos["status"] == "open" and hit_tp1:
                pos["status"] = "tp1"
                pos["realized_r"] = 0.5 * TP1_R
                pos["remaining"] = 0.5
                pos["stop"] = pos["entry"]                 # безубыток
                msgs.append(f"🎯 <b>{name(base)}</b> достиг TP1 ({fmt(pos['tp1'] * m)}, {TP1_R:g}R)\n"
                            f"Закрыть 50%, стоп остатка → вход <code>{fmt(pos['entry'] * m)}</code>. "
                            f"Дальше трейлинг {TRAIL_ATR:g} ATR.")
        else:
            if bars:                                       # продолжаем с конца последней закрытой свечи
                pos["last_ts"] = bars[-1][0] + H1

        if base not in state["open"]:
            continue

        if pos["status"] == "tp1" and len(d1) > 20:        # трейлинг от экстремума после входа
            a = build_indicators(d1)["atr"][-1]
            if long_:
                new_stop = pos["extreme"] - TRAIL_ATR * a
                better = new_stop > pos["stop"]
            else:
                new_stop = pos["extreme"] + TRAIL_ATR * a
                better = new_stop < pos["stop"]
            if better:
                pos["stop"] = new_stop
                msgs.append(f"🔼 <b>{name(base)}</b> трейлинг: новый стоп "
                            f"<code>{fmt(new_stop * m)}</code> ({r_of(pos, new_stop):+.2f}R от входа)")

        age_days = (time.time() * 1000 - pos["opened_ts"]) / DAY
        if pos["status"] == "open" and age_days > MAX_HOLD_DAYS and not pos.get("timeout_sent"):
            pos["timeout_sent"] = True
            msgs.append(f"⏳ <b>{name(base)}</b> в позиции {age_days:.0f} дн. без TP1 — "
                        f"пересмотрите идею (тайм-аут).")
    return msgs


# ----------------------------------------------------------------- статистика
def summarize(rs):
    if not rs:
        return "сделок нет"
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    gp, gl = sum(wins), -sum(losses)
    eq, peak, dd = 0.0, 0.0, 0.0
    for r in rs:
        eq += r
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    pf = (gp / gl) if gl > 0 else float("inf")
    return (f"сделок {len(rs)} · винрейт {len(wins) / len(rs) * 100:.0f}% · "
            f"средний R {sum(rs) / len(rs):+.2f} · PF {pf:.2f} · "
            f"сумма {sum(rs):+.1f}R · макс.просадка {dd:.1f}R")


def cmd_stats():
    st = load_state()
    log("Все закрытые: " + summarize([c["r"] for c in st["closed"]]))
    log(f"Открыто сейчас: {len(st['open'])}")


# ----------------------------------------------------------------- бэктест (тренд, дневки OKX)
def simulate(ind, regime_map, side):
    """Решение — по закрытой дневной свече i, вход — по открытию i+1. Стоп первым при
    неоднозначности (гэп через стоп — по цене открытия); 50% на TP1 + стоп в БУ; остаток —
    chandelier 3 ATR. Комиссии учтены. Возвращает [(R, ts_выхода), ...]."""
    n = len(ind["c"])
    ts = ind["ts"]
    long_ = side == "long"
    trades = []
    i = MIN_BARS
    while i < n - 1:
        sig = signal_at(ind, i, regime_map.get(ts[i], "neutral"), side)
        if not sig:
            i += 1
            continue
        entry = ind["o"][i + 1]
        risk = sig["risk"]
        stop = entry - risk if long_ else entry + risk
        tp1 = entry + TP1_R * risk if long_ else entry - TP1_R * risk
        cost_r = (FEE_ROUNDTRIP_PCT / 100.0 * entry) / risk
        status, realized, remaining = "open", 0.0, 1.0
        extreme = entry
        res, exit_j = None, n - 1
        for j in range(i + 1, n):
            o, h, l = ind["o"][j], ind["h"][j], ind["l"][j]
            if long_:
                hit_stop, hit_tp = l <= stop, h >= tp1
            else:
                hit_stop, hit_tp = h >= stop, l <= tp1
            if hit_stop:
                px = min(o, stop) if long_ else max(o, stop)
                rr = ((px - entry) if long_ else (entry - px)) / risk
                res, exit_j = realized + rr * remaining, j
                break
            extreme = max(extreme, h) if long_ else min(extreme, l)
            if status == "open" and hit_tp:
                status, realized, remaining = "tp1", 0.5 * TP1_R, 0.5
                stop = entry
            if status == "tp1":
                a = ind["atr"][j]
                tr = extreme - TRAIL_ATR * a if long_ else extreme + TRAIL_ATR * a
                stop = max(stop, tr) if long_ else min(stop, tr)
        if res is None:                                    # конец данных — по последней цене
            cl = ind["c"][-1]
            rr = ((cl - entry) if long_ else (entry - cl)) / risk
            res = realized + rr * remaining
        trades.append((res - cost_r, ts[exit_j]))
        i = exit_j + 1                                     # одна позиция на монету
    return trades


def _halves(trades):
    if len(trades) < 8:
        return None
    ts_sorted = sorted(t[1] for t in trades)
    mid = ts_sorted[len(ts_sorted) // 2]
    return [t[0] for t in trades if t[1] < mid], [t[0] for t in trades if t[1] >= mid]


def cmd_backtest():
    global ALLOW_SHORT
    days = int(_f("BT_DAYS", 400))
    insts = os.getenv("BT_INSTS", "")
    if insts:
        names = [f"{x.strip().upper()}-USDT-SWAP" for x in insts.split(",") if x.strip()]
    else:
        okx = load_okx()
        top = sorted(okx.values(), key=lambda r: r["turnover"], reverse=True)
        names = [r["sym"] for r in top if r["turnover"] >= MIN_TURNOVER][:int(_f("BT_TOP", 40))]
    log(f"БЭКТЕСТ (тренд, 1D, OKX): {len(names)} монет · ~{days} дней · "
        f"комиссии+проскальзывание {FEE_ROUNDTRIP_PCT}% за круг")
    btc = get_history_okx("BTC-USDT-SWAP", "1Dutc", days + MIN_BARS + 40)
    bi = build_indicators(btc)
    regime = {bi["ts"][k]: btc_regime(bi, k) for k in range(len(bi["ts"]))}
    ALLOW_SHORT = True                                     # шорты проверяем отдельно
    agg = {"long": [], "short": []}
    for inst in names:
        try:
            dcs = get_history_okx(inst, "1Dutc", days + MIN_BARS + 40)
        except Exception as exc:                           # noqa: BLE001
            log(f"пропуск {inst}: {exc}")
            continue
        if len(dcs) < MIN_BARS + 30:
            continue
        ind = build_indicators(dcs)
        for side in ("long", "short"):
            agg[side] += simulate(ind, regime, side)
    log("")
    for side in ("long", "short"):
        tr = agg[side]
        log(f"=== {'ЛОНГ' if side == 'long' else 'ШОРТ (только при медвежьем BTC)'} ===")
        log(summarize([t[0] for t in tr]) + f" · ≈{len(tr) / days:.2f} сделок/день")
        hv = _halves(tr)
        if hv:
            log(f"  1-я половина: {summarize(hv[0])}")
            log(f"  2-я половина: {summarize(hv[1])}")
    log("\nКак читать: смотрите профит-фактор и просадку на ОБЕИХ половинах выборки. "
        "Мало сделок (<30) — вывода нет. «Сделок/день» — без лимитов MAX_OPEN/MAX_PER_DAY, "
        "фильтра силы и фандинга; в бою сигналов будет меньше. Параметры не оптимизировались; "
        "прошлое не гарантирует будущее.")


# ----------------------------------------------------------------- main
def run():
    state = load_state()
    if DRY:
        state = deepcopy(state)                            # dry ничего не сохраняет
    snapshot = json.dumps(state, sort_keys=True)
    msgs = manage_open(state)
    if msgs:
        tg_send("📌 SIGNAL G7 — обновления\n\n" + "\n\n".join(msgs))
    if len(state["open"]) < MAX_OPEN:
        scan_new(state, force=("--force" in sys.argv) or DRY)
    else:
        log("Достигнут MAX_OPEN; новые сигналы не ищу.")
    if not DRY and json.dumps(state, sort_keys=True) != snapshot:
        save_state(state)


def send_dry_report():
    body = "\n".join(LOG).strip() or "(пустой вывод)"
    parts = _chunks(body, 3300)
    for k, part in enumerate(parts, 1):
        title = "📋 <b>SIGNAL G7 — отчёт запуска</b>" + (f" ({k}/{len(parts)})" if len(parts) > 1 else "")
        tg_send(title + "\n<pre>" + html.escape(part.strip()) + "</pre>")


if __name__ == "__main__":
    code = 0
    try:
        if "--backtest" in sys.argv:
            cmd_backtest()
        elif "--stats" in sys.argv:
            cmd_stats()
        else:
            run()
    except Exception as exc:                               # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        LOG.append(f"ERROR: {exc}")
        code = 1
    if DRY:
        try:
            send_dry_report()
        except Exception as exc:                           # noqa: BLE001
            print(f"Не удалось отправить отчёт в Telegram: {exc}", file=sys.stderr)
            code = code or 1
    sys.exit(code)
