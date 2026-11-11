#!/usr/bin/env python3
"""
Trend Bot — трендовый свинг-сканер (OKX + BingX -> сигналы в Telegram)

Четыре сетапа, все в лонг и в шорт, с рейтингом идеи 0–100:
  A) ТРЕНД (1D)      — пробой / откат к EMA20 по дневному тренду (описание ниже);
  B) СЛОМ СТРУКТУРЫ  (4H) — MSS в зоне Discount (лонг) / Premium (шорт): диапазон = max/min последних
     STRUCT_RANGE свечей 4H, equilibrium = 50%; TP1 — equilibrium, TP2 — противоположный край диапазона;
  C) ОТКАТ К EMA20   (4H) — вход по тренду после отката, стоп за минимумом отката;
  D) СВИП ЛИКВИДНОСТИ (4H) — прокол экстремума 20 свечей и возврат за уровень в Discount/Premium.
График выдачи: цель DAILY_TARGET сигналов за день (с QUIET_END до полуночи), не больше MAX_PER_DAY.
Сильные идеи (оценка >= SCORE_HIGH) отправляются сразу, остальные — только чтобы догнать график и не
ниже SCORE_FLOOR: мусор бот не отправляет даже ради нормы. Стоп всегда не уже MIN_STOP_PCT и MIN_STOP_ATR,
TP1 не ближе 1.5R, TP2 не ближе 2.5R. Order block / FVG не используются.

Трендовый сетап — дневной таймфрейм (1D):
  * направление: тренд монеты (цена > EMA50 > EMA100, EMA50 растёт) + режим BTC
  * относительная сила монеты за 21 день (лонг — только из сильной половины)
  * сетап: пробой 20-дневного канала с объёмом ИЛИ откат к EMA20 после недавнего пробоя
  * стоп = ATR_STOP_MULT * ATR(14); TP1 = 1.5R (закрыть 50%, стоп в безубыток),
    остаток — chandelier-трейлинг 3 ATR.

Как работает сканирование:
  1. Загружается список USDT-перпетуалов с двух бирж: OKX и BingX.
     Если одна биржа недоступна — бот продолжает работу на другой.
  2. Монеты сводятся к общему тикеру (1000PEPE = PEPE и т.п., масштаб учитывается).
  3. Сверка: сигнал отправляется только по монетам, которые торгуются на BingX
     (REQUIRE_BINGX=1). Цены в сообщении пересчитываются под контракт BingX.
  4. Свечи берутся с первой доступной биржи (DATA_SOURCES, по умолчанию okx,bingx),
     при ошибке автоматически берётся следующая.
  5. Новая дневная свеча обрабатывается ОДИН раз (не каждый час). Каждый час бот
     только сопровождает открытые позиции (стоп / TP1 / трейлинг).

Скрипт ничего не торгует и не размещает ордера. Это идея для ручной проверки.

Запуск (cron раз в час, через 5 минут после закрытия 1H-свечи, UTC):
  5 * * * *  /usr/bin/python3 /path/trend_bot.py

Аргументы:
  --dry        пробный прогон: весь вывод и найденные сигналы уходят в Telegram
               с пометкой DRY-RUN; состояние (позиции, пауза) НЕ сохраняется
  --backtest   бэктест трендового режима на истории OKX (BT_DAYS, BT_TOP, BT_INSTS)
  --stats      статистика закрытых сигналов из state-файла
  --no-quiet   игнорировать тихий режим (для ручного запуска)
  --force      искать сигналы, даже если эта дневная свеча уже обработана

Переменные окружения:
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
  EQUITY_USDT          размер счёта                                   (1000)
  RISK_PCT             риск на сделку, % счёта                        (0.5)
  MAX_LEVERAGE         потолок плеча                                  (3)
  MAX_OPEN             макс. одновременных позиций в учёте            (20)
  MAX_NEW_PER_RUN      макс. новых сигналов за запуск                 (3)
  MAX_PER_DAY          макс. сигналов за день                         (6)
  DAILY_TARGET         цель сигналов в день                           (5)
  SCORE_HIGH/SCORE_FLOOR  порог «сильной» идеи / минимальное качество (68 / 52)
  MIN_STOP_PCT/MIN_STOP_ATR  минимальный стоп: % от входа / ATR(4H)  (2.0 / 1.5)
  TP2_MIN_R/TP2_MAX_R  границы TP2 в R для откатов и свипов           (2.5 / 4)
  PULLBACK_ENABLED / SWEEP_ENABLED  откат EMA20 / свип ликвидности    (1 / 1)
  MAX_PER_SIDE         макс. одновременных идей в одну сторону        (6)
  MIN_ATR_PCT          пропуск вялых монет: ATR(4H) < N % цены        (0.7)
  REVERSAL_BTC_GATE    1 = слом структуры/свип не против режима BTC   (1)
  STRUCT_TRAIL_ATR     трейлинг остатка 4H-идей после TP1, ATR(4H)    (2.5)
  TOP_N                сколько самых ликвидных монет, 0 = все         (0)
  MIN_TURNOVER_USDT    мин. суточный оборот (максимум по биржам)      (5000000)
  RS_MIN_PCT           лонг только из верхних (100-X)% по силе 21д    (50)
  EXCLUDE              тикеры-исключения через запятую                (пусто)
  ALLOW_SHORT          1 = разрешить шорты (тренд — при медвежьем BTC; структура — всегда) (1)
  STRUCT_ENABLED       1 = структурный сетап (слом структуры 4H)       (1)
  STRUCT_RANGE         диапазон Discount/Premium, свечей 4H            (72)
  STRUCT_MIN_RR        мин. R до противоположного края диапазона      (2.0)
  STRUCT_MAX_STOP_PCT  макс. стоп 4H-сетапов, %                         (10)
  ATR_STOP_MULT        стоп в дневных ATR                             (2.5)
  MAX_STOP_PCT         макс. ширина стопа, %                          (18)
  MAX_FUNDING_PCT      макс. фандинг против позиции, %/период        (0.05)
  MAX_CHASE_ATR        не слать, если цена уже ушла от входа > N ATR  (0.5)
  SYMBOL_COOLDOWN_DAYS пауза по монете после сигнала                  (2)
  MAX_HOLD_DAYS        обзор позиции без TP1 через N дней             (30)
  REQUIRE_BINGX        1 = слать только монеты, которые есть на BingX (1)
  BINGX_ON_ERROR       send | skip — если список BingX не загрузился  (send)
  DATA_SOURCES         порядок бирж для свечей                        (okx,bingx)
  BINGX_HOSTS / OKX_HOST   адреса API через запятую
  DAILY_SUMMARY        1 = раз в день короткий отчёт о скане          (1)
  QUIET_START/QUIET_END  тихий режим: часы без сообщений              (0 / 7)
  QUIET_TZ             часовой пояс тихого режима                     (Europe/Moscow)
  STATE_FILE           (trend_bot_state.json)
"""

import html
import json
import math
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
UA = {"User-Agent": "TrendBot-Scanner/2.0"}


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
MAX_OPEN = int(_f("MAX_OPEN", 20))
MAX_NEW = int(_f("MAX_NEW_PER_RUN", 3))
MAX_PER_DAY = int(_f("MAX_PER_DAY", 6))
TOP_N = int(_f("TOP_N", 0))
MIN_TURNOVER = _f("MIN_TURNOVER_USDT", 5_000_000)
RS_MIN_PCT = _f("RS_MIN_PCT", 50)
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "1") == "1"
ATR_STOP = _f("ATR_STOP_MULT", 2.5)
MAX_STOP_PCT = _f("MAX_STOP_PCT", 18)
MAX_FUNDING = _f("MAX_FUNDING_PCT", 0.05)
MAX_CHASE_ATR = _f("MAX_CHASE_ATR", 0.5)
COOLDOWN_DAYS = _f("SYMBOL_COOLDOWN_DAYS", 2)
MAX_HOLD_DAYS = _f("MAX_HOLD_DAYS", 30)
STATE_FILE = os.getenv("STATE_FILE", "trend_bot_state.json")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EXCLUDE = {s.strip().upper() for s in os.getenv("EXCLUDE", "").split(",") if s.strip()}
# не-крипто контракты BingX (индексы NCSI*, акции NCSK*, сырьё NCCO*, форекс NCFX*) — пропускаем
EXCLUDE_PREFIXES = tuple(p.strip().upper() for p in
                         os.getenv("EXCLUDE_PREFIXES", "NCSI,NCSK,NCCO,NCFX").split(",") if p.strip())
REQUIRE_BINGX = os.getenv("REQUIRE_BINGX", "1") == "1"
BINGX_ON_ERROR = os.getenv("BINGX_ON_ERROR", "send").strip().lower()
DAILY_SUMMARY = os.getenv("DAILY_SUMMARY", "1") == "1"
QUIET_START = int(_f("QUIET_START", 0))       # тихий режим: с этого часа ...
QUIET_END = int(_f("QUIET_END", 7))           # ... до этого часа (по времени QUIET_TZ)
QUIET_TZ = os.getenv("QUIET_TZ", "Europe/Moscow")
STRUCT_ENABLED = os.getenv("STRUCT_ENABLED", "1") == "1"   # структурный сетап (слом структуры, 4H)
STRUCT_RANGE = int(_f("STRUCT_RANGE", 72))                 # диапазон для зон Discount/Premium, свечей 4H (72 = 12 дней)
STRUCT_PIV = int(_f("STRUCT_PIV", 3))                      # свинг-точка: N свечей слева и справа
STRUCT_ENTRY_POS = _f("STRUCT_ENTRY_POS", 0.6)             # вход не выше 60% диапазона (лонг) / не ниже 40% (шорт)
STRUCT_BODY_ATR = _f("STRUCT_BODY_ATR", 0.5)               # импульс слома: тело свечи >= N ATR
STRUCT_BUFFER_ATR = _f("STRUCT_BUFFER_ATR", 0.25)          # запас стопа за экстремум зоны, ATR
STRUCT_MAX_STOP_PCT = _f("STRUCT_MAX_STOP_PCT", 10)         # макс. ширина стопа, %
STRUCT_MIN_RR = _f("STRUCT_MIN_RR", 2.0)                   # мин. R до противоположного края диапазона
STRUCT_MAX_AGE_H = _f("STRUCT_MAX_AGE_H", 4)               # свеча 4H старше N часов — сигнал неактуален
STRUCT_HOLD_DAYS = _f("STRUCT_HOLD_DAYS", 3)               # срок жизни 4H-идеи, дней (потом закрывается в учёте)
STRUCT_COOLDOWN_H = _f("STRUCT_COOLDOWN_H", 12)            # пауза по монете после структурного сигнала
STRUCT_MIN_BARS = 100                                      # прогрев на 4H
STRUCT_PIV_MAXAGE = 40                                     # свинг-хай/лоу не старше N свечей 4H
H4 = 4 * H1
PULLBACK_ENABLED = os.getenv("PULLBACK_ENABLED", "1") == "1"   # откат к EMA20 в тренде (4H)
SWEEP_ENABLED = os.getenv("SWEEP_ENABLED", "1") == "1"         # свип ликвидности + возврат (4H)
DAILY_TARGET = int(_f("DAILY_TARGET", 5))                      # цель: сигналов в день
SCORE_HIGH = _f("SCORE_HIGH", 68)                              # сильные сигналы идут сразу
SCORE_FLOOR = _f("SCORE_FLOOR", 52)                            # ниже этого балла сигналы не отправляются никогда
MIN_STOP_PCT = _f("MIN_STOP_PCT", 2.0)                         # стоп не уже N % от входа
MIN_STOP_ATR = _f("MIN_STOP_ATR", 1.5)                         # и не уже N ATR(4H)
TP2_MIN_R = _f("TP2_MIN_R", 2.5)                               # TP2 не ближе N R
TP2_MAX_R = _f("TP2_MAX_R", 4.0)
MAX_PER_SIDE = int(_f("MAX_PER_SIDE", 6))                      # не больше N одновременных идей в одну сторону (корреляция альтов)
MIN_ATR_PCT = _f("MIN_ATR_PCT", 0.7)                           # пропуск «вялых» монет: ATR(4H) < N % цены
REVERSAL_BTC_GATE = os.getenv("REVERSAL_BTC_GATE", "1") == "1" # слом структуры/свип не против режима BTC
STRUCT_TRAIL_ATR = _f("STRUCT_TRAIL_ATR", 2.5)                 # трейлинг остатка 4H-идей после TP1, ATR(4H)
STRUCT_HOLD_TP1_DAYS = _f("STRUCT_HOLD_TP1_DAYS", 6)           # срок жизни после TP1, дней
SIGNAL_MAX_AGE_H = 12          # дневная свеча старше N часов — сигналы уже неактуальны
FETCH_WORKERS = 5              # параллельная загрузка свечей
MIN_NOTIONAL = 5.0             # минимальная стоимость ордера, USDT

EXCHANGES = ("okx", "bingx")
HOSTS = {
    "okx": _hosts("OKX_HOST", "https://www.okx.com"),
    "bingx": _hosts("BINGX_HOSTS", "https://open-api.bingx.com"),
}
_src = [s.strip().lower() for s in os.getenv("DATA_SOURCES", "okx,bingx").split(",")]
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


def in_quiet_hours():
    """True, если сейчас тихие часы (по умолчанию 23:00–06:00 по Москве). start==end — режим выключен."""
    if QUIET_START == QUIET_END:
        return False
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        hour = datetime.now(ZoneInfo(QUIET_TZ)).hour
    except Exception:                                      # noqa: BLE001  — нет tzdata: считаем МСК = UTC+3
        hour = int((time.time() // 3600 + 3) % 24)
    if QUIET_START < QUIET_END:
        return QUIET_START <= hour < QUIET_END
    return hour >= QUIET_START or hour < QUIET_END


def day_window():
    """(начало дня в ts, доля прошедшего «бодрствующего» дня 0..1). День начинается в QUIET_END по QUIET_TZ."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timedelta
        now = datetime.now(ZoneInfo(QUIET_TZ))
        start = now.replace(hour=QUIET_END, minute=0, second=0, microsecond=0)
        if now < start:
            start -= timedelta(days=1)
        start_ts, hours = start.timestamp(), (now - start).total_seconds() / 3600.0
    except Exception:                                      # noqa: BLE001
        now_s = time.time() + 3 * 3600
        start_ts = (now_s // 86400) * 86400 + QUIET_END * 3600 - 3 * 3600
        if start_ts > time.time():
            start_ts -= 86400
        hours = (time.time() - start_ts) / 3600.0
    awake_end = QUIET_START if QUIET_START > QUIET_END else 24
    awake = max(1.0, awake_end - QUIET_END)
    return start_ts, min(1.0, max(0.0, hours / awake))


def build_levels(side, entry, stop, atr, tp1_hint=None, tp2_hint=None, tp2_min_r=None, tp2_max_r=None):
    """Единые правила для всех сетапов: стоп не короче MIN_STOP_PCT и MIN_STOP_ATR,
    TP1 не ближе TP1_R, TP2 не ближе tp2_min_r. Слишком короткий стоп расширяется (размер позиции
    уменьшится автоматически). Возвращает None, если стоп получается шире допустимого."""
    long_ = side == "long"
    sgn = 1.0 if long_ else -1.0
    risk = max(abs(entry - stop), MIN_STOP_PCT / 100.0 * entry, MIN_STOP_ATR * atr)
    stop = entry - sgn * risk
    stop_pct = risk / entry * 100
    if stop <= 0 or stop_pct > STRUCT_MAX_STOP_PCT:
        return None
    tp1 = entry + sgn * TP1_R * risk
    tp1_hint_used = False
    if tp1_hint is not None and sgn * (tp1_hint - entry) >= TP1_R * risk:
        tp1, tp1_hint_used = tp1_hint, True
    lo = (tp2_min_r if tp2_min_r is not None else TP2_MIN_R) * risk
    hi = (tp2_max_r if tp2_max_r is not None else TP2_MAX_R) * risk
    d2 = lo if tp2_hint is None else min(max(sgn * (tp2_hint - entry), lo), hi)
    d2 = max(d2, sgn * (tp1 - entry) + 0.75 * risk)       # TP2 всегда заметно дальше TP1
    return {"stop": stop, "risk": risk, "stop_pct": stop_pct, "tp1": tp1, "tp2": entry + sgn * d2,
            "tp1_hint_used": tp1_hint_used}


_KIND_BASE = {"trend": 55.0, "pullback": 58.0, "struct": 50.0, "sweep": 42.0}


def score_sig(sig, ind, i, regime, strength=None):
    """Рейтинг идеи 0..100: качество сетапа + R до цели + согласие с BTC + объём (+ сила монеты для тренда)."""
    rr = abs(sig["tp2"] - sig["entry"]) / sig["risk"]
    sc = _KIND_BASE.get(sig.get("kind", "trend"), 50.0) + min(rr, 5.0) / 5.0 * 20.0
    if (sig["side"] == "long" and regime == "bull") or (sig["side"] == "short" and regime == "bear"):
        sc += 10
    elif regime == "neutral":
        sc += 4
    else:
        sc -= 8
    vm = ind["vol_ma"][i]
    if vm and vm > 0:
        vr = ind["v"][i] / vm
        sc += 10 if vr >= 1.8 else 6 if vr >= 1.2 else 2 if vr >= 0.8 else 0
    if strength is not None:
        sc += (strength - 50.0) / 5.0                      # -10..+10
    return round(max(0.0, min(100.0, sc)), 1)


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
    if base in STABLES or base in EXCLUDE or base.startswith(EXCLUDE_PREFIXES):
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


def bingx_json(path, params=None):
    payload = http_json(HOSTS["bingx"], path, params, label="BingX")
    code = payload.get("code")
    if code not in (0, "0", None):
        raise RuntimeError(f"BingX {path}: {payload.get('msg')} ({code})")
    return payload.get("data")


def load_bingx():
    contracts = bingx_json("/openApi/swap/v2/quote/contracts") or []
    ok = {}
    for c in contracts:
        sym = c.get("symbol", "")
        if not sym.endswith("-USDT"):
            continue
        if str(c.get("status", "1")) not in ("1", "TRADING", "online"):
            continue
        ok[sym] = c.get("asset") or sym.split("-")[0]
    if not ok:
        raise RuntimeError("BingX: пустой список контрактов")
    fund = {}
    try:
        prem = bingx_json("/openApi/swap/v2/quote/premiumIndex") or []
        if isinstance(prem, dict):
            prem = [prem]
        for p in prem:
            fund[p["symbol"]] = float(p["lastFundingRate"]) * 100.0
    except Exception:                                    # noqa: BLE001  — фандинг не критичен
        pass
    out = {}
    try:
        tick = bingx_json("/openApi/swap/v2/quote/ticker") or []
        if isinstance(tick, dict):
            tick = [tick]
    except Exception:                                    # noqa: BLE001  — без оборотов
        tick = []
    for t in tick:
        sym = t.get("symbol")
        if sym not in ok:
            continue
        try:
            last, turnover = float(t["lastPrice"]), float(t.get("quoteVolume") or 0)
        except (KeyError, ValueError):
            continue
        _put(out, ok[sym], {"sym": sym, "turnover": turnover, "last": last, "funding": fund.get(sym)})
    # монеты без тикера тоже считаем «есть на BingX» (оборот 0 — отсеются фильтром ликвидности)
    for sym, asset in ok.items():
        base, _ = split_base(asset)
        if base not in out:
            _put(out, asset, {"sym": sym, "turnover": 0.0, "last": 0.0, "funding": fund.get(sym)})
    return out


LOADERS = {"okx": load_okx, "bingx": load_bingx}


def build_coins():
    """Сводит две биржи в один список. Возвращает (coins, loaded, bingx_ok)."""
    loaded, per = [], {}
    for ex in EXCHANGES:
        try:
            per[ex] = LOADERS[ex]()
            loaded.append(ex)
            log(f"{ex.upper()}: загружено {len(per[ex])} USDT-перпетуалов")
        except Exception as exc:                          # noqa: BLE001
            log(f"⚠ {ex.upper()} недоступен: {exc}")
    if not loaded:
        raise RuntimeError("Ни одна биржа не отвечает (OKX, BingX)")
    coins = {}
    for ex, data in per.items():
        for base, row in data.items():
            c = coins.setdefault(base, {"base": base, "turnover": 0.0, "ex": {}})
            c["ex"][ex] = {"sym": row["sym"], "scale": row["scale"], "last": row["last"],
                           "funding": row["funding"]}
            c["turnover"] = max(c["turnover"], row["turnover"])
    bingx_ok = "bingx" in per
    total = len(coins)
    coins = {b: c for b, c in coins.items() if c["turnover"] >= MIN_TURNOVER}
    log(f"Всего уникальных монет: {total} · с оборотом ≥ {MIN_TURNOVER:,.0f} USDT: {len(coins)}")
    if REQUIRE_BINGX and bingx_ok:
        before = len(coins)
        coins = {b: c for b, c in coins.items() if "bingx" in c["ex"]}
        log(f"Сверка с BingX: оставлено {len(coins)} из {before} (остальных на BingX нет)")
    ranked = sorted(coins.values(), key=lambda c: c["turnover"], reverse=True)
    if TOP_N > 0:
        ranked = ranked[:TOP_N]
    return ranked, loaded, bingx_ok


# ----------------------------------------------------------------- свечи (любая биржа)
_BAR = {
    "okx": {"1D": "1Dutc", "4H": "4H", "1H": "1H"},
    "bingx": {"1D": "1d", "4H": "4h", "1H": "1h"},
}
_BAR_MS = {"1D": DAY, "4H": H4, "1H": H1}
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
    else:                                                # bingx
        rows = bingx_json("/openApi/swap/v3/quote/klines",
                          {"symbol": sym, "interval": _BAR[ex][tf], "limit": str(min(limit, 1000))}) or []
        for r in rows:
            if isinstance(r, dict):
                ts = int(r["time"])
                row = (ts, float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
                       float(r.get("volume") or 0))
            else:
                row = (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] or 0))
            if row[0] + bar_ms <= now_ms:                # только закрытые свечи
                out.append(row)
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


# ----------------------------------------------------------------- сетап: слом структуры (4H, Discount/Premium)
def pivots(ind, n):
    """Свинг-хаи и свинг-лоу (фракталы). Точка подтверждается через n свечей после себя."""
    h, l = ind["h"], ind["l"]
    ph, pl = [], []
    for j in range(n, len(h) - n):
        if h[j] > max(h[j - n:j]) and h[j] >= max(h[j + 1:j + n + 1]):
            ph.append((j, h[j]))
        if l[j] < min(l[j - n:j]) and l[j] <= min(l[j + 1:j + n + 1]):
            pl.append((j, l[j]))
    return ph, pl


def struct_signal(ind, i, side, piv=None):
    """Слом структуры (MSS) на закрытой свече 4H i.
    Лонг : прежняя структура нисходящая (lower high), свеча закрывается ВЫШЕ последнего
           свинг-хая, минимум импульса лежит в зоне DISCOUNT (нижняя половина диапазона).
           Стоп — под этим минимумом, цели: equilibrium (50%) и верх диапазона (PREMIUM).
    Шорт : зеркально — слом вниз в зоне PREMIUM, стоп над максимумом, цели к DISCOUNT."""
    n = STRUCT_PIV
    if i < STRUCT_MIN_BARS - 1:
        return None
    if piv is None:
        piv = pivots(ind, n)
    ph = [p for p in piv[0] if p[0] <= i - n]
    pl = [p for p in piv[1] if p[0] <= i - n]
    c, o, h, l = ind["c"][i], ind["o"][i], ind["h"], ind["l"]
    cl = ind["c"]
    atr = ind["atr"][i]
    if atr <= 0 or c <= 0:
        return None
    lo_i = max(0, i - STRUCT_RANGE + 1)
    rhi, rlo = max(h[lo_i:i + 1]), min(l[lo_i:i + 1])
    rng = rhi - rlo
    if rng < 2 * atr:                                       # диапазон слишком узкий — зон нет
        return None
    eq = rlo + rng / 2.0
    if abs(c - o) < STRUCT_BODY_ATR * atr:                  # нужен импульс, а не «ползущий» пробой
        return None

    if side == "long":
        if len(ph) < 2:
            return None
        (j1, p1), (_, p0) = ph[-1], ph[-2]
        if p1 >= p0 or i - j1 > STRUCT_PIV_MAXAGE:          # прежняя структура должна быть нисходящей
            return None
        if not (c > p1 and c > o):
            return None
        if i - 1 > j1 and max(cl[j1 + 1:i]) > p1:           # пробой должен быть свежим
            return None
        leg = min(l[j1:i + 1])                              # минимум ноги перед сломом
        if leg > eq or c > rlo + STRUCT_ENTRY_POS * rng:    # слом происходит в DISCOUNT
            return None
        raw_stop, edge, zone = leg - STRUCT_BUFFER_ATR * atr, rhi, "Discount"
        if c - raw_stop <= 0 or (edge - c) < STRUCT_MIN_RR * max(c - raw_stop, MIN_STOP_PCT / 100 * c):
            return None                                     # до верха диапазона слишком близко
    else:
        if len(pl) < 2:
            return None
        (j1, p1), (_, p0) = pl[-1], pl[-2]
        if p1 <= p0 or i - j1 > STRUCT_PIV_MAXAGE:          # прежняя структура — восходящая (higher low)
            return None
        if not (c < p1 and c < o):
            return None
        if i - 1 > j1 and min(cl[j1 + 1:i]) < p1:
            return None
        leg = max(h[j1:i + 1])
        if leg < eq or c < rlo + (1 - STRUCT_ENTRY_POS) * rng:   # слом в PREMIUM
            return None
        raw_stop, edge, zone = leg + STRUCT_BUFFER_ATR * atr, rlo, "Premium"
        if raw_stop - c <= 0 or (c - edge) < STRUCT_MIN_RR * max(raw_stop - c, MIN_STOP_PCT / 100 * c):
            return None

    lv = build_levels(side, c, raw_stop, atr, tp1_hint=eq, tp2_hint=edge, tp2_min_r=STRUCT_MIN_RR, tp2_max_r=8.0)
    if not lv:
        return None
    setup = ("Слом структуры вверх (MSS) в зоне Discount" if side == "long"
             else "Слом структуры вниз (MSS) в зоне Premium")
    goal = "верх диапазона (Premium)" if side == "long" else "низ диапазона (Discount)"
    return {"side": side, "kind": "struct", "setup": setup, "zone": zone, "entry": c, "stop": lv["stop"],
            "tp1": lv["tp1"], "tp2": lv["tp2"], "risk": lv["risk"], "stop_pct": lv["stop_pct"],
            "atr": atr, "ts": ind["ts"][i], "rr": abs(lv["tp2"] - c) / lv["risk"], "eq": eq,
            "rng_hi": rhi, "rng_lo": rlo, "tp1_note": "equilibrium" if lv["tp1_hint_used"] else "",
            "tp2_note": goal if abs(lv["tp2"] - edge) / edge < 0.003 else "",
            "note": f"Диапазон: {fmt(rlo)} – {fmt(rhi)}, equilibrium {fmt(eq)}"}


def pullback_signal(ind, i, side):
    """Откат к EMA20 в тренде 4H: тренд (цена > EMA50 > EMA100, EMA50 растёт), перед этим был импульс,
    свеча касается EMA20 и закрывается обратно в сторону тренда. Стоп — за минимумом отката."""
    if i < STRUCT_MIN_BARS - 1:
        return None
    c, o, h, l = ind["c"][i], ind["o"][i], ind["h"], ind["l"]
    a, e20, e50, e100 = ind["atr"][i], ind["ema20"][i], ind["ema50"][i], ind["ema100"][i]
    if a <= 0 or c <= 0 or h[i] - l[i] <= 0:
        return None
    pos_in_bar = (c - l[i]) / (h[i] - l[i])
    if side == "long":
        if not (c > e50 and e50 > e100 and e50 > ind["ema50"][i - 6]):
            return None
        if not (l[i] <= e20 + 0.2 * a and c > e20 and c > o and l[i] >= e50 - 0.5 * a):
            return None
        if pos_in_bar < 0.55 or (c - e20) / a > 1.5:
            return None
        if max(h[i - 12:i]) < e20 + 1.5 * a:                # перед откатом был импульс вверх
            return None
        raw_stop = min(l[i - 3:i + 1]) - 0.3 * a
        hint = max(h[i - 40:i + 1])
        note = f"Тренд 4H вверх, откат к EMA20 ({fmt(e20)})"
    else:
        if not (c < e50 and e50 < e100 and e50 < ind["ema50"][i - 6]):
            return None
        if not (h[i] >= e20 - 0.2 * a and c < e20 and c < o and h[i] <= e50 + 0.5 * a):
            return None
        if pos_in_bar > 0.45 or (e20 - c) / a > 1.5:
            return None
        if min(l[i - 12:i]) > e20 - 1.5 * a:
            return None
        raw_stop = max(h[i - 3:i + 1]) + 0.3 * a
        hint = min(l[i - 40:i + 1])
        note = f"Тренд 4H вниз, откат к EMA20 ({fmt(e20)})"
    lv = build_levels(side, c, raw_stop, a, tp2_hint=hint)
    if not lv:
        return None
    return {"side": side, "kind": "pullback", "setup": "Откат к EMA20 в тренде (4H)", "entry": c,
            "stop": lv["stop"], "tp1": lv["tp1"], "tp2": lv["tp2"], "risk": lv["risk"],
            "stop_pct": lv["stop_pct"], "atr": a, "ts": ind["ts"][i],
            "rr": abs(lv["tp2"] - c) / lv["risk"], "tp1_note": "", "tp2_note": "",
            "note": note}


def sweep_signal(ind, i, side):
    """Свип ликвидности: прокол экстремума последних 20 свечей и возврат за уровень в зоне
    Discount (лонг) / Premium (шорт). Стоп — за кончиком прокола. Цели: equilibrium и край диапазона."""
    if i < STRUCT_MIN_BARS - 1:
        return None
    c, o, h, l = ind["c"][i], ind["o"][i], ind["h"], ind["l"]
    a = ind["atr"][i]
    rng_bar = h[i] - l[i]
    if a <= 0 or c <= 0 or rng_bar < 0.8 * a:
        return None
    lo_i = max(0, i - STRUCT_RANGE + 1)
    rhi, rlo = max(h[lo_i:i + 1]), min(l[lo_i:i + 1])
    rng = rhi - rlo
    if rng < 2 * a:
        return None
    eq = rlo + rng / 2.0
    e50, e100 = ind["ema50"][i], ind["ema100"][i]
    if side == "long":
        level = min(l[i - 22:i - 2])
        if e50 <= e100:                                     # свип только по тренду 4H (без ловли ножей)
            return None
        if not (l[i] < level and c > level and (c - l[i]) / rng_bar >= 0.6):
            return None
        if c > rlo + STRUCT_ENTRY_POS * rng:
            return None
        raw_stop, edge = l[i] - 0.5 * a, rhi
        note = f"Свип минимума {fmt(level)} и возврат выше"
        goal = "верх диапазона (Premium)"
    else:
        level = max(h[i - 22:i - 2])
        if e50 >= e100:
            return None
        if not (h[i] > level and c < level and (h[i] - c) / rng_bar >= 0.6):
            return None
        if c < rlo + (1 - STRUCT_ENTRY_POS) * rng:
            return None
        raw_stop, edge = h[i] + 0.5 * a, rlo
        note = f"Свип максимума {fmt(level)} и возврат ниже"
        goal = "низ диапазона (Discount)"
    lv = build_levels(side, c, raw_stop, a, tp1_hint=eq, tp2_hint=edge)
    if not lv:
        return None
    return {"side": side, "kind": "sweep", "setup": "Свип ликвидности + возврат (4H)", "entry": c,
            "stop": lv["stop"], "tp1": lv["tp1"], "tp2": lv["tp2"], "risk": lv["risk"],
            "stop_pct": lv["stop_pct"], "atr": a, "ts": ind["ts"][i],
            "rr": abs(lv["tp2"] - c) / lv["risk"], "eq": eq,
            "tp1_note": "equilibrium" if lv["tp1_hint_used"] else "",
            "tp2_note": goal if abs(lv["tp2"] - edge) / edge < 0.003 else "", "note": note}


# ----------------------------------------------------------------- размер позиции
def size_position(price, stop_pct):
    """price — цена в масштабе BingX-контракта. Размер считается в USDT и в монетах."""
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
    st.setdefault("last_scan_struct", 0)
    st.setdefault("cd_until", {})
    for key, ts in list(st["cooldown"].items()):             # перенос старых пауз в новый формат
        st["cd_until"].setdefault(key, ts + COOLDOWN_DAYS * 86400)
    st["cooldown"] = {}
    # убираем ранее записанные не-крипто инструменты (индексы, акции и т.п.)
    for key in list(st["open"].keys()):
        if key.upper().startswith(EXCLUDE_PREFIXES):
            del st["open"][key]
    for key in list(st["cd_until"].keys()):
        if key.upper().startswith(EXCLUDE_PREFIXES):
            del st["cd_until"][key]
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
    for key in list(st["cd_until"].keys()):
        if key.endswith("-USDT-SWAP"):
            st["cd_until"][key.split("-")[0]] = st["cd_until"].pop(key)
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
    if in_quiet_hours() and not DRY and "--no-quiet" not in sys.argv:
        # страховка: сообщение не отправляется, исключение откатывает запуск (состояние не сохраняется)
        raise RuntimeError("тихий режим: отправка отложена до окончания тихих часов")
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


def fmt_signal(base, sig, size, rs_pct, regime, funding, ex, bingx, mult):
    """Цены сигнала даются в масштабе BingX-контракта (mult уже применён к уровням)."""
    long_ = sig["side"] == "long"
    arrow = "🟢 LONG" if long_ else "🔴 SHORT"
    m = mult
    kind = sig.get("kind", "trend")
    risk = sig["risk"]
    r1 = abs(sig["tp1"] - sig["entry"]) / risk
    r2 = abs(sig["tp2"] - sig["entry"]) / risk
    score = sig.get("score")
    head = "свинг (дни–недели)" if kind == "trend" else "4H (часы–дни)"
    lines = [f"{arrow} <b>{name(base)}</b> — {head}", f"Сетап: {sig['setup']}"]
    if sig.get("note"):
        lines.append(sig["note"])
    if kind == "trend" and rs_pct is not None:
        lines.append(f"Режим BTC: {regime} · RS 21д: {'топ' if long_ else 'низ'} {rs_pct:.0f}%")
    else:
        lines.append(f"Режим BTC: {regime}")
    if score is not None:
        lines.append(f"Оценка идеи: {score:.0f}/100")
    if bingx == "unverified":
        lines.append("⚠ BingX: наличие не удалось проверить — проверьте вручную")
    elif bingx:
        t = f"BingX: ✅ {bingx['sym']} (USDT perp) · свечи: {ex.upper()}"
        if bingx["scale"] != 1:
            t += f" · ⚠ контракт ×{bingx['scale']:g}, цены ниже уже пересчитаны"
        lines.append(t)
    lines += ["", f"Вход: <code>{fmt(sig['entry'] * m)}</code> (не гнаться, если цена ушла дальше 0.5 ATR)",
              f"Стоп: <code>{fmt(sig['stop'] * m)}</code> ({sig['stop_pct']:.1f}%)"]
    if kind == "trend":
        lines += [f"TP1: <code>{fmt(sig['tp1'] * m)}</code> ({r1:.1f}R) — закрыть 50%, стоп в безубыток",
                  f"TP2: <code>{fmt(sig['tp2'] * m)}</code> — остаток по трейлингу {TRAIL_ATR:g} ATR"]
    else:
        t1 = f", {sig['tp1_note']}" if sig.get("tp1_note") else ""
        t2 = f", {sig['tp2_note']}" if sig.get("tp2_note") else ""
        lines += [f"TP1: <code>{fmt(sig['tp1'] * m)}</code> ({r1:.1f}R{t1}) — закрыть 50%, стоп в безубыток",
                  f"TP2: <code>{fmt(sig['tp2'] * m)}</code> ({r2:.1f}R{t2}) — закрыть остаток"]
    lines += [
        "",
        f"Размер: ≈ {size['notional']:,.0f} USDT (~{size['qty']:,.4g} монет), плечо ≈ {size['lev']:.2f}x",
        f"Риск: {size['risk_usdt']:.1f} USDT ({size['risk_pct']:.2f}% счёта)"
        + (" — урезан лимитом плеча" if size["capped"] else ""),
        "Маржа: isolated. Ликвидация должна быть заметно дальше стопа.",
    ]
    if funding is not None:
        lines.append(f"Фандинг: {funding:+.4f}% за период")
    lines += ["", "Идея для ручной проверки, не гарантия результата. Серии стопов — норма, "
                  "прибыль даёт хвост крупных выигрышей."]
    return "\n".join(lines)


# ----------------------------------------------------------------- сканирование
BTC_EX = {"okx": {"sym": "BTC-USDT-SWAP", "scale": 1},
          "bingx": {"sym": "BTC-USDT", "scale": 1}}


def rs_return(ind):
    c = ind["c"]
    if len(c) <= RS_N:
        return None
    return c[-1] / c[-1 - RS_N] - 1.0


def _fetch_tf(args):
    coin, tf = args
    try:
        cs, ex = fetch_any(coin["ex"], tf, 300)
        time.sleep(0.08)
        return coin, cs, ex, None
    except Exception as exc:                              # noqa: BLE001
        return coin, None, None, exc


def _load_series(coins, tf, min_bars):
    """Свечи по всем монетам параллельно. Возвращает ({base: (coin, ind, ex)}, ошибок, полнота)."""
    out, failed = {}, 0
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        for coin, cs, ex, err in pool.map(_fetch_tf, [(c, tf) for c in coins]):
            if err is not None:
                failed += 1
                if failed <= 10:
                    log(f"пропуск {coin['base']} ({tf}): {err}")
            elif cs and len(cs) >= min_bars:
                out[coin["base"]] = (coin, build_indicators(cs), ex)
    if failed > 10:
        log(f"… и ещё {failed - 10} монет пропущено ({tf}) из-за ошибок загрузки")
    return out, failed, failed <= max(3, 0.2 * len(coins))


def scan_new(state, force=False):
    now_ms = time.time() * 1000
    btc_d, _ = fetch_any(BTC_EX, "1D", 300)
    if len(btc_d) < MIN_BARS:
        raise RuntimeError("Недостаточно дневных данных BTC")
    day_ts = btc_d[-1][0]

    # какие проходки нужны: тренд (новая дневная свеча) и/или структура (новая свеча 4H)
    do_trend = force or state.get("last_scan_day") != day_ts
    if do_trend and not force and (now_ms - (day_ts + DAY)) / H1 > SIGNAL_MAX_AGE_H:
        log("Дневная свеча закрылась давно — тренд-сигналы устарели, пропускаю день.")
        state["last_scan_day"] = day_ts
        do_trend = False
    do_struct, bar_ts = False, 0
    if STRUCT_ENABLED:
        try:
            btc_4h, _ = fetch_any(BTC_EX, "4H", 300)
            if len(btc_4h) >= STRUCT_MIN_BARS:
                bar_ts = btc_4h[-1][0]
                do_struct = force or state.get("last_scan_struct") != bar_ts
                if do_struct and not force and (now_ms - (bar_ts + H4)) / H1 > STRUCT_MAX_AGE_H:
                    log("Свеча 4H закрылась давно — структурные сигналы устарели, пропускаю.")
                    state["last_scan_struct"] = bar_ts
                    do_struct = False
        except Exception as exc:                           # noqa: BLE001
            log(f"⚠ 4H-данные BTC недоступны, структурный поиск пропущен: {exc}")
    if not do_trend and not do_struct:
        log("Новых закрытых свечей (1D/4H) нет — поиск сигналов не нужен.")
        return []

    coins, loaded, bingx_ok = build_coins()
    if REQUIRE_BINGX and not bingx_ok and BINGX_ON_ERROR == "skip":
        log("BingX недоступен и BINGX_ON_ERROR=skip — поиск не выполняю (повторю в следующий запуск).")
        return []

    regime = btc_regime(build_indicators(btc_d))
    log(f"Режим BTC: {regime} · монет в скане: {len(coins)} · источники свечей: {','.join(DATA_SOURCES)} · "
        f"проходки: {'тренд 1D ' if do_trend else ''}{'структура 4H' if do_struct else ''}")
    now = time.time()
    cands, gated, n_trend, n_struct = [], 0, 0, 0
    trend_done = struct_done = False
    daily_n = h4_n = 0

    def free(base):
        return base not in state["open"] and now >= state["cd_until"].get(base, 0)

    # --- проходка 1: тренд на дневках
    if do_trend:
        daily, failed, complete = _load_series(coins, "1D", MIN_BARS)
        trend_done = complete
        daily_n = len(daily)
        rets = {b: rs_return(v[1]) for b, v in daily.items()}
        rets = {b: r for b, r in rets.items() if r is not None}
        ordered = sorted(rets, key=lambda k: rets[k])       # от слабых к сильным
        pct = {k: (idx + 1) / len(ordered) * 100 for idx, k in enumerate(ordered)}
        for base, (coin, ind, dex) in daily.items():
            if not free(base):
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
                    sig["kind"] = "trend"
                    strength = p if side == "long" else 100 - p
                    sig["score"] = score_sig(sig, ind, di, regime, strength)
                    cands.append({"score": sig["score"], "base": base, "coin": coin, "dex": dex,
                                  "sig": sig, "p": p})
                    n_trend += 1
        log(f"Тренд 1D: загружено {daily_n} · прошли фильтр направления: {gated} · сетапов: {n_trend}")
        if not complete:
            log(f"⚠ Много ошибок загрузки 1D ({failed}) — день не помечаю обработанным, повторю.")

    # --- проходка 2: слом структуры на 4H (лонг и шорт, без привязки к режиму BTC)
    if do_struct:
        h4, failed4, complete4 = _load_series(coins, "4H", STRUCT_MIN_BARS)
        struct_done = complete4
        h4_n = len(h4)
        for base, (coin, ind, dex) in h4.items():
            if not free(base):
                continue
            i = len(ind["c"]) - 1
            if ind["atr"][i] / ind["c"][i] * 100 < MIN_ATR_PCT:
                continue                                    # вялая монета: стоп 2% = много ATR, цели недостижимы
            piv = pivots(ind, STRUCT_PIV)
            for side in ("long", "short"):
                if side == "short" and not ALLOW_SHORT:
                    continue
                against_btc = (side == "long" and regime == "bear") or (side == "short" and regime == "bull")
                found = [struct_signal(ind, i, side, piv) if not (REVERSAL_BTC_GATE and against_btc) else None]
                if PULLBACK_ENABLED:
                    found.append(pullback_signal(ind, i, side))
                if SWEEP_ENABLED and not (REVERSAL_BTC_GATE and against_btc):
                    found.append(sweep_signal(ind, i, side))
                for sig in found:
                    if sig:
                        sig["score"] = score_sig(sig, ind, i, regime)
                        cands.append({"score": sig["score"], "base": base, "coin": coin,
                                      "dex": dex, "sig": sig, "p": 50})
                        n_struct += 1
        log(f"Сетапы 4H (слом структуры / откат EMA / свип ликвидности): загружено {h4_n} · кандидатов: {n_struct}")
        if not complete4:
            log(f"⚠ Много ошибок загрузки 4H ({failed4}) — свечу не помечаю обработанной, повторю.")

    cands.sort(key=lambda z: z["score"], reverse=True)

    # --- график выдачи: цель DAILY_TARGET сигналов за «бодрствующий» день (с QUIET_END до полуночи)
    day_start, progress = day_window()
    state["sent"] = [t for t in state.get("sent", []) if now - t < 2 * 86400]
    sent_today = sum(1 for t in state["sent"] if t >= day_start)
    expected = max(1, math.ceil(DAILY_TARGET * progress - 1e-9))     # сколько должно быть отправлено к этому часу
    need = max(0, expected - sent_today)
    room = min(MAX_NEW, MAX_OPEN - len(state["open"]), MAX_PER_DAY - sent_today)
    top = ", ".join(f"{c['base']} {c['score']:.0f}" for c in cands[:6]) or "—"
    log(f"Кандидатов всего: {len(cands)} (топ по оценке: {top})")
    log(f"Сигналов сегодня: {sent_today} из {MAX_PER_DAY} (цель {DAILY_TARGET}, к этому часу по графику {expected}) · "
        f"открытых позиций: {len(state['open'])} из {MAX_OPEN}")

    side_open = {"long": 0, "short": 0}
    for p_ in state["open"].values():
        if p_.get("status") == "open":                      # позиции после TP1 (стоп в БУ) риск не несут
            side_open[p_["side"]] = side_open.get(p_["side"], 0) + 1
    out, taken = [], set()
    for cd in cands:
        if len(out) >= room:
            break
        if cd["score"] < SCORE_FLOOR:
            break                                           # ниже минимального качества не берём никогда
        if cd["score"] < SCORE_HIGH and len(out) >= need:
            continue                                        # слабее «сильных» — только чтобы догнать график
        base, coin, dex, sig, p = cd["base"], cd["coin"], cd["dex"], cd["sig"], cd["p"]
        if base in taken:
            continue
        if side_open.get(sig["side"], 0) >= MAX_PER_SIDE:
            log(f"пропуск {base}: уже {MAX_PER_SIDE} идей в сторону {sig['side']} (лимит корреляции)")
            continue
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
        # сверка с BingX и пересчёт масштаба цен
        if "bingx" in exinfo:
            bb = exinfo["bingx"]
            mult = bb["scale"] / data_info["scale"]
        elif REQUIRE_BINGX and bingx_ok:
            log(f"пропуск {base}: нет на BingX")
            continue
        elif REQUIRE_BINGX:                                # список BingX не загрузился
            bb, mult = "unverified", 1.0
        else:
            bb, mult = None, 1.0
        # фандинг: BingX -> OKX
        funding = None
        for fx in ("bingx", "okx"):
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
        taken.add(base)
        side_open[sig["side"]] = side_open.get(sig["side"], 0) + 1
        out.append((base, coin, dex, sig, size, p, funding, bb, mult))

    sent_n = 0
    for base, coin, dex, sig, size, p, funding, bb, mult in out:
        rs_pct = (100 - p) if sig["side"] == "long" else p
        try:
            tg_send(fmt_signal(base, sig, size, rs_pct, regime, funding, dex, bb, mult))
        except Exception as exc:                           # noqa: BLE001
            log(f"⚠ не удалось отправить сигнал {base} в Telegram: {exc}")
            continue
        now_ms_i = int(time.time() * 1000)
        state["open"][base] = {
            "kind": sig["kind"], "side": sig["side"], "entry": sig["entry"], "stop": sig["stop"],
            "stop0": sig["stop"], "tp1": sig["tp1"], "tp2": sig["tp2"], "risk": sig["risk"],
            "atr": sig["atr"], "opened_ts": now_ms_i, "last_ts": now_ms_i, "status": "open",
            "setup": sig["setup"], "risk_usdt": size["risk_usdt"], "extreme": sig["entry"],
            "score": sig.get("score"),
            "ex": dex, "dscale": coin["ex"][dex]["scale"], "mult": mult,
            "syms": {e: {"sym": v["sym"], "scale": v["scale"]} for e, v in coin["ex"].items()},
        }
        cd_s = COOLDOWN_DAYS * 86400 if sig["kind"] == "trend" else STRUCT_COOLDOWN_H * 3600
        state["cd_until"][base] = time.time() + cd_s
        state["sent"].append(time.time())
        sent_n += 1
        log(f"СИГНАЛ [{sig['kind']}] оценка {sig.get('score', 0):.0f}: {sig['side'].upper()} {name(base)} · {sig['setup']}")

    if do_trend and trend_done:
        state["last_scan_day"] = day_ts
    if do_struct and struct_done:
        state["last_scan_struct"] = bar_ts
    if not out:
        log("Новых сигналов нет." + (" (достигнут лимит позиций или сигналов за сутки)" if room <= 0 else ""))
    if DAILY_SUMMARY and not DRY and do_trend and trend_done:
        try:
            extra = f" · сетапов 4H: {n_struct}" if do_struct else ""
            tg_send(f"📊 <b>TREND BOT</b> · скан дня завершён\n"
                    f"Биржи: {', '.join(e.upper() for e in loaded)} · монет в скане: {daily_n}\n"
                    f"Режим BTC: {regime} · тренд-сетапов: {n_trend}{extra} · "
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
        "inst": base, "side": pos["side"], "setup": pos["setup"], "src": pos.get("kind", "trend"),
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
        kind = pos.get("kind", "trend")
        try:
            bars = _pos_candles(pos, "1H", 300)
        except Exception as exc:                           # noqa: BLE001
            log(f"сопровождение {base}: {exc}")
            continue
        time.sleep(0.1)
        long_ = pos["side"] == "long"
        m = pos.get("mult", 1.0)
        pos.setdefault("extreme", pos["entry"])
        r1 = abs(pos["tp1"] - pos["entry"]) / pos["risk"]

        for ts, o, h, l, c, v in bars:
            if ts + H1 <= pos["last_ts"]:
                continue
            pos["extreme"] = max(pos["extreme"], h) if long_ else min(pos["extreme"], l)
            hit_stop = l <= pos["stop"] if long_ else h >= pos["stop"]
            hit_tp1 = h >= pos["tp1"] if long_ else l <= pos["tp1"]
            if hit_stop:                                   # при неоднозначности — стоп первым
                reason = "стоп" if pos["status"] == "open" else "безубыток/трейлинг"
                gap = min(o, pos["stop"]) if long_ else max(o, pos["stop"])   # гэп через стоп
                close_pos(state, base, pos, gap, reason, msgs)
                break
            if pos["status"] == "open" and hit_tp1:
                pos["status"] = "tp1"
                pos["realized_r"] = 0.5 * r1
                pos["remaining"] = 0.5
                pos["stop"] = pos["entry"]                 # безубыток
                tail = (f"Остаток — к TP2 <code>{fmt(pos['tp2'] * m)}</code>, стоп подтягивается (трейлинг {STRUCT_TRAIL_ATR:g} ATR)." if kind != "trend"
                        else f"Дальше трейлинг {TRAIL_ATR:g} ATR.")
                msgs.append(f"🎯 <b>{name(base)}</b> достиг TP1 ({fmt(pos['tp1'] * m)}, {r1:.1f}R)\n"
                            f"Закрыть 50%, стоп остатка → вход <code>{fmt(pos['entry'] * m)}</code>. {tail}")
            if kind != "trend" and pos["status"] == "tp1":
                hit_tp2 = h >= pos["tp2"] if long_ else l <= pos["tp2"]
                if hit_tp2:
                    close_pos(state, base, pos, pos["tp2"], "цель TP2 (противоположная зона)", msgs)
                    break
        else:
            if bars:                                       # продолжаем с конца последней закрытой свечи
                pos["last_ts"] = bars[-1][0] + H1

        if base not in state["open"]:
            continue

        if kind != "trend" and pos["status"] == "tp1":      # трейлинг остатка 4H-идей от экстремума
            a4 = pos.get("atr") or 0
            if a4 > 0:
                new_stop = pos["extreme"] - STRUCT_TRAIL_ATR * a4 if long_ else pos["extreme"] + STRUCT_TRAIL_ATR * a4
                if (new_stop > pos["stop"]) if long_ else (new_stop < pos["stop"]):
                    pos["stop"] = new_stop
                    last = pos.get("notified_stop", pos["entry"])
                    if abs(new_stop - last) >= 0.3 * pos["risk"]:        # сообщаем о заметном сдвиге
                        pos["notified_stop"] = new_stop
                        msgs.append(f"🔼 <b>{name(base)}</b> трейлинг: новый стоп "
                                    f"<code>{fmt(new_stop * m)}</code> ({r_of(pos, new_stop):+.2f}R от входа)")

        if kind == "trend" and pos["status"] == "tp1":      # трейлинг только у трендовых сделок
            try:
                d1 = _pos_candles(pos, "1D", 150)
            except Exception as exc:                       # noqa: BLE001
                log(f"трейлинг {base}: {exc}")
                d1 = []
            if len(d1) > 20:
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

        hold = STRUCT_HOLD_DAYS if kind != "trend" else MAX_HOLD_DAYS
        if kind != "trend" and pos["status"] == "tp1":
            hold = STRUCT_HOLD_TP1_DAYS                      # после TP1 даём прибыли дорасти
        age_days = (time.time() * 1000 - pos["opened_ts"]) / DAY
        if kind != "trend" and age_days > hold:             # 4H-идея устарела — закрываем в учёте по рынку
            px = bars[-1][4] if bars else pos["entry"]
            close_pos(state, base, pos, px, f"срок идеи истёк ({hold:g} дн.)", msgs)
            continue
        if kind == "trend" and pos["status"] == "open" and age_days > hold and not pos.get("timeout_sent"):
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
    """Разбор закрытых идей из state-файла и отправка отчёта в Telegram (режим stats)."""
    st = load_state()
    closed = st.get("closed", [])
    rs = [c["r"] for c in closed]
    lines = ["📈 <b>TREND BOT — статистика</b>", f"Закрыто идей: {len(closed)} · открыто сейчас: {len(st['open'])}"]
    if closed:
        lines += ["", "Все: " + summarize(rs)]
        names = {"trend": "Тренд 1D", "struct": "Слом структуры", "pullback": "Откат к EMA20", "sweep": "Свип ликвидности"}
        for kind in sorted({c.get("src", "trend") for c in closed}):
            sub = [c["r"] for c in closed if c.get("src", "trend") == kind]
            lines.append(f"• {names.get(kind, kind)}: " + summarize(sub))
        for side, label in (("long", "Лонг"), ("short", "Шорт")):
            sub = [c["r"] for c in closed if c.get("side") == side]
            if sub:
                lines.append(f"• {label}: " + summarize(sub))
        reasons = {}
        for c in closed:
            key = c.get("reason", "?")
            reasons[key] = reasons.get(key, 0) + 1
        lines += ["", "Причины закрытия: " + ", ".join(f"{k} — {v}" for k, v in sorted(reasons.items(), key=lambda z: -z[1]))]
        lines += ["", "Последние 10:"]
        for c in closed[-10:]:
            lines.append(f"{c['inst']} {c.get('side', '')} [{c.get('src', 'trend')}] {c['r']:+.2f}R · {c.get('reason', '')}")
    else:
        lines.append("Закрытых идей пока нет.")
    if st["open"]:
        lines += ["", "Открыты сейчас:"]
        for base, p in st["open"].items():
            lines.append(f"{base} {p['side']} [{p.get('kind', 'trend')}] · статус {p['status']} · "
                         f"оценка {p.get('score') or '—'}")
    text = "\n".join(lines)
    log(_plain(text))
    try:
        tg_send(text)
    except Exception as exc:                               # noqa: BLE001
        log(f"⚠ отчёт не отправлен в Telegram: {exc}")


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


def simulate_struct(ind, side):
    """Слом структуры 4H. Решение — по закрытой свече i, вход — по открытию i+1.
    Стоп первым при неоднозначности (гэп — по цене открытия); 50% на TP1 (equilibrium) + стоп в БУ;
    остаток — до TP2 (противоположный край диапазона); тайм-аут STRUCT_HOLD_DAYS. Комиссии учтены."""
    n = len(ind["c"])
    ts = ind["ts"]
    long_ = side == "long"
    piv = pivots(ind, STRUCT_PIV)
    trades = []
    max_bars = int(STRUCT_HOLD_DAYS * 6)
    i = STRUCT_MIN_BARS
    while i < n - 1:
        sig = struct_signal(ind, i, side, piv)
        if not sig:
            i += 1
            continue
        entry = ind["o"][i + 1]
        stop, tp1, tp2 = sig["stop"], sig["tp1"], sig["tp2"]
        risk = (entry - stop) if long_ else (stop - entry)
        bad = risk <= 0 or ((entry >= tp1) if long_ else (entry <= tp1))
        if bad:                                            # гэп испортил сетап — пропускаем
            i += 1
            continue
        r1 = abs(tp1 - entry) / risk
        r2 = abs(tp2 - entry) / risk
        cost_r = (FEE_ROUNDTRIP_PCT / 100.0 * entry) / risk
        status, realized, remaining = "open", 0.0, 1.0
        res, exit_j = None, min(n - 1, i + max_bars)
        for j in range(i + 1, min(n, i + 1 + max_bars)):
            o, h, l = ind["o"][j], ind["h"][j], ind["l"][j]
            if long_:
                hit_stop, hit_tp1, hit_tp2 = l <= stop, h >= tp1, h >= tp2
            else:
                hit_stop, hit_tp1, hit_tp2 = h >= stop, l <= tp1, l <= tp2
            if hit_stop:
                px = min(o, stop) if long_ else max(o, stop)
                rr = ((px - entry) if long_ else (entry - px)) / risk
                res, exit_j = realized + rr * remaining, j
                break
            if status == "open" and hit_tp1:
                status, realized, remaining = "tp1", 0.5 * r1, 0.5
                stop = entry
            if status == "tp1" and hit_tp2:
                res, exit_j = realized + 0.5 * r2, j
                break
        if res is None:                                    # тайм-аут / конец данных — по закрытию
            cl = ind["c"][exit_j]
            rr = ((cl - entry) if long_ else (entry - cl)) / risk
            res = realized + rr * remaining
        trades.append((res - cost_r, ts[exit_j]))
        i = exit_j + 1
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
    if STRUCT_ENABLED:
        sdays = int(_f("BT_STRUCT_DAYS", 120))
        log(f"\n=== СТРУКТУРА 4H (слом в Discount/Premium), ~{sdays} дней ===")
        sagg = {"long": [], "short": []}
        for inst in names:
            try:
                h4 = get_history_okx(inst, "4H", sdays * 6 + STRUCT_MIN_BARS + 20)
            except Exception as exc:                       # noqa: BLE001
                log(f"пропуск {inst} (4H): {exc}")
                continue
            if len(h4) < STRUCT_MIN_BARS + 30:
                continue
            ind4 = build_indicators(h4)
            for side in ("long", "short"):
                sagg[side] += simulate_struct(ind4, side)
        for side in ("long", "short"):
            tr = sagg[side]
            log(f"--- {'ЛОНГ' if side == 'long' else 'ШОРТ'} ---")
            log(summarize([t[0] for t in tr]) + f" · ≈{len(tr) / sdays:.2f} сделок/день")
            hv = _halves(tr)
            if hv:
                log(f"  1-я половина: {summarize(hv[0])}")
                log(f"  2-я половина: {summarize(hv[1])}")
        allt = sagg["long"] + sagg["short"]
        log("--- ЛОНГ+ШОРТ вместе ---")
        log(summarize([t[0] for t in allt]))
    log("\nКак читать: смотрите профит-фактор и просадку на ОБЕИХ половинах выборки. "
        "Мало сделок (<30) — вывода нет. «Сделок/день» — без лимитов MAX_OPEN/MAX_PER_DAY, "
        "фильтра силы и фандинга; в бою сигналов будет меньше. Параметры не оптимизировались; "
        "прошлое не гарантирует будущее.")


# ----------------------------------------------------------------- main
def run():
    if in_quiet_hours() and not DRY and "--no-quiet" not in sys.argv:
        log(f"Тихий режим ({QUIET_START:02d}:00–{QUIET_END:02d}:00, {QUIET_TZ}) — сообщений нет. "
            f"Дневная свеча и позиции будут обработаны после окончания тихих часов.")
        return
    state = load_state()
    if DRY:
        state = deepcopy(state)                            # dry ничего не сохраняет
    snapshot = json.dumps(state, sort_keys=True)
    msgs = manage_open(state)
    if msgs:
        tg_send("📌 TREND BOT — обновления\n\n" + "\n\n".join(msgs))
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
        title = "📋 <b>TREND BOT — отчёт запуска</b>" + (f" ({k}/{len(parts)})" if len(parts) > 1 else "")
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
