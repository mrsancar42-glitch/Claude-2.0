#!/usr/bin/env python3
"""Сигналы от Claude: SMC-сканер (Smart Money Concepts) для крипто-перпетуалов.

Что делает:
  - каждые 10 минут смотрит ликвидные USDT-перпетуалы (Bybit -> OKX -> Binance);
  - ищет сетап: снятие ликвидности -> слом структуры (MSS) -> откат в FVG / Order Block / Fib 62-79 %;
  - фильтрует по тренду 1H/4H и тренду BTC, даёт оценку из 12;
  - шлёт в Telegram: монету, направление, вход, TP1, TP2, SL, причину и время;
  - ведёт журнал: по каждому сигналу записывает исход (TP1, SL, лимит не исполнен, истёк),
    раз в сутки присылает сводку винрейта и среднего результата в R.

Бот ничего не торгует и не знает ваш баланс. Только стандартная библиотека Python.

Запуск:
  python3 signals_claude.py           обычный проход (по расписанию)
  python3 signals_claude.py --test    проверка связи и источников
  python3 signals_claude.py --dry     без отправки, вывод в консоль
"""
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

_COINS_ENV = os.environ.get("COINS", "ALL").replace(" ", "").upper()
FIXED_COINS = None if _COINS_ENV in ("", "ALL") else [c for c in _COINS_ENV.split(",") if c]
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
M15 = 900_000


def _num(name, default):
    v = os.environ.get(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except ValueError:
        return float(default)


def load_params():
    return {
        "swing_k": int(_num("SWING_K", 3)),             # фрактал для уровней ликвидности
        "sweep_window": int(_num("SWEEP_WINDOW", 10)),  # свечей между снятием и MSS
        "fresh": int(_num("FRESH", 2)),                 # MSS не старше fresh+1 свечей
        "disp_atr": _num("DISP_ATR", 1.0),              # тело импульса, в ATR
        "min_leg_atr": _num("MIN_LEG_ATR", 1.5),        # минимальная нога, в ATR
        "sl_buf_atr": _num("SL_BUF_ATR", 0.2),          # запас SL за экстремумом снятия
        "min_sl_pct": _num("MIN_SL_PCT", 0.7),        # SL не ближе, %
        "max_sl_pct": _num("MAX_SL_PCT", 3.5),          # SL не дальше, %
        "sweep_pos_max": _num("SWEEP_POS_MAX", 0.5),    # снятие должно быть у края диапазона 24ч
        "min_vol_ratio": _num("MIN_VOL_RATIO", 1.2),    # объём к норме на снятии/импульсе
        "max_entry_atr": _num("MAX_ENTRY_ATR", 1.3),    # лимит не дальше N ATR от цены
        "allow_counter": int(_num("ALLOW_COUNTER", 0)), # 0 = не торговать против тренда 1H/4H
        "entry_edge": int(_num("ENTRY_EDGE", 1)),       # 1 = вход у края зоны FVG/OB
        "htf_gate": int(_num("HTF_GATE", 0)),           # 1 = нужен слом 1H и вход в дисконте 1H
        "no_score": int(_num("NO_SCORE", 0)),           # 1 = оценка отключена
        "min_rr": _num("MIN_RR", 1.5),
        "max_tp_pct": _num("MAX_TP_PCT", 6.0),
        "min_score": int(_num("MIN_SCORE", 8)),         # строгий порог (из 12)
        "floor_score": int(_num("FLOOR_SCORE", 6)),     # порог, если за сутки мало сигналов
        "target_per_day": int(_num("TARGET_PER_DAY", 5)),
        "max_per_day": int(_num("MAX_PER_DAY", 10)),
        "max_alerts": int(_num("MAX_ALERTS", 3)),       # за один проход
        "cooldown_min": int(_num("COOLDOWN_MIN", 180)),
        "limit_ttl_min": int(_num("LIMIT_TTL_MIN", 90)),  # лимит ждём не дольше, мин
        "min_turnover": _num("MIN_TURNOVER_USD", 20_000_000),
        "max_coins": int(_num("MAX_COINS", 80)),
        "workers": int(_num("WORKERS", 8)),
        "tz_offset": _num("TZ_OFFSET", 3),              # часовой пояс для времени в сообщении
        "lev_hint": int(_num("LEV_HINT", 20)),          # плечо для подсказки в % маржи
    }


# ======================= данные =======================
IV_MS = {"15m": M15, "1h": HOUR_MS}
IV_NAME = {"bybit": {"15m": "15", "1h": "60"},
           "okx": {"15m": "15m", "1h": "1H"},
           "binance": {"15m": "15m", "1h": "1h"}}
SOURCES = [("bybit", "api.bybit.com"), ("bybit2", "api.bytick.com"),
           ("okx", "www.okx.com"), ("binance", "data-api.binance.vision")]
STABLE = {"USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDP", "BUSD", "EUR", "USD1", "PYUSD", "USDD"}
LIMITS = {"15m": 240, "1h": 300}


def http_json(url, data=None, timeout=15):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": "Mozilla/5.0 signals-claude"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def _kind(source):
    return "bybit" if source.startswith("bybit") else source


def _row(r):
    return (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))


def klines(source, host, coin, iv, limit, now_ms):
    """Закрытые свечи (ts, o, h, l, c, vol) от старых к новым."""
    kind, name = _kind(source), IV_NAME[_kind(source)][iv]
    if kind == "bybit":
        j = http_json("https://%s/v5/market/kline?category=linear&symbol=%sUSDT&interval=%s&limit=%d"
                      % (host, coin, name, limit))
        if j.get("retCode") != 0:
            raise RuntimeError("bybit: %s" % j.get("retMsg"))
        rows = [_row(r) for r in j["result"]["list"]]
    elif kind == "okx":
        j = http_json("https://%s/api/v5/market/candles?instId=%s-USDT-SWAP&bar=%s&limit=%d"
                      % (host, coin, name, limit))
        if j.get("code") != "0":
            raise RuntimeError("okx: %s" % j.get("msg"))
        rows = [_row(r) for r in j["data"] if r[8] == "1"]
    else:
        j = http_json("https://%s/api/v3/klines?symbol=%sUSDT&interval=%s&limit=%d"
                      % (host, coin, name, limit))
        rows = [_row(r) for r in j]
    rows.sort()
    ms = IV_MS[iv]
    return [x for x in rows if x[0] + ms <= now_ms]


_gate = threading.Lock()
_last_call = {}
MIN_GAP = {"www.okx.com": 0.07}


def throttle(host):
    gap = MIN_GAP.get(host, 0)
    if not gap:
        return
    with _gate:
        wait = _last_call.get(host, 0) + gap - time.time()
        if wait > 0:
            time.sleep(wait)
        _last_call[host] = time.time()


def _good_base(b, kind):
    if not b or b in STABLE:
        return False
    if kind == "binance" and b.endswith(("UP", "DOWN", "BULL", "BEAR")):
        return False
    return True


def get_universe(min_turnover, max_coins):
    """Ликвидные USDT-рынки. -> (source, [монеты]) или (None, [])."""
    for source, host in SOURCES:
        kind = _kind(source)
        try:
            throttle(host)
            if kind == "bybit":
                j = http_json("https://%s/v5/market/tickers?category=linear" % host)
                if j.get("retCode") != 0:
                    raise RuntimeError(j.get("retMsg"))
                rows = [(float(t.get("turnover24h") or 0), t["symbol"][:-4])
                        for t in j["result"]["list"]
                        if t["symbol"].endswith("USDT") and "-" not in t["symbol"]]
            elif kind == "okx":
                j = http_json("https://%s/api/v5/market/tickers?instType=SWAP" % host)
                if j.get("code") != "0":
                    raise RuntimeError(j.get("msg"))
                rows = [(float(t.get("volCcy24h") or 0) * float(t.get("last") or 0),
                         t["instId"].split("-")[0])
                        for t in j["data"] if t["instId"].endswith("-USDT-SWAP")]
            else:
                j = http_json("https://%s/api/v3/ticker/24hr" % host)
                rows = [(float(t.get("quoteVolume") or 0), t["symbol"][:-4])
                        for t in j if t["symbol"].endswith("USDT")]
            rows = sorted((r for r in rows if r[0] >= min_turnover and _good_base(r[1], kind)),
                          reverse=True)
            coins = [c for _, c in rows[:max_coins]]
            if coins:
                return source, coins
        except Exception:
            continue
    return None, []


def get_candles(coin, now_ms, sources=None):
    """-> (source, c15, c1h, errors); source None, если ни один источник не ответил."""
    errors = []
    for source, host in (sources or SOURCES):
        for _ in (1, 2):
            try:
                throttle(host)
                c15 = klines(source, host, coin, "15m", LIMITS["15m"], now_ms)
                throttle(host)
                c1h = klines(source, host, coin, "1h", LIMITS["1h"], now_ms)
                return source, c15, c1h, errors
            except Exception as e:
                errors.append("%s: %s" % (source, str(e)[:80]))
                time.sleep(0.4)
    return None, [], [], errors


# ======================= индикаторы =======================
def ema(values, n):
    k = 2.0 / (n + 1)
    e = sum(values[:n]) / n
    for v in values[n:]:
        e = v * k + e * (1 - k)
    return e


def atr(c, n=14):
    if len(c) <= n:
        return 0.0
    trs = []
    for i in range(len(c) - n, len(c)):
        h, l, pc = c[i][2], c[i][3], c[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / n


def swings(c, k):
    """Подтверждённые фрактальные максимумы и минимумы: ([(i, цена)], [(i, цена)])."""
    highs, lows = [], []
    for i in range(k, len(c) - k):
        h, l = c[i][2], c[i][3]
        if all(h > c[j][2] for j in range(i - k, i)) and all(h >= c[j][2] for j in range(i + 1, i + k + 1)):
            highs.append((i, h))
        if all(l < c[j][3] for j in range(i - k, i)) and all(l <= c[j][3] for j in range(i + 1, i + k + 1)):
            lows.append((i, l))
    return highs, lows


def mirror(c):
    """Зеркало цены (p -> -p): шорт считается как лонг, одна логика на оба направления."""
    return [(t, -o, -l, -h, -cl, v) for (t, o, h, l, cl, v) in c]


def aggregate_4h(c1h):
    """Собирает 4H из закрытых 1H; неполные группы отбрасываются."""
    out, grp = [], []

    def flush():
        if len(grp) == 4:
            out.append((grp[0][0], grp[0][1], max(x[2] for x in grp),
                        min(x[3] for x in grp), grp[-1][4], sum(x[5] for x in grp)))
    for x in c1h:
        if grp and grp[0][0] // (4 * HOUR_MS) != x[0] // (4 * HOUR_MS):
            flush()
            grp = []
        grp.append(x)
    flush()
    return out


# ======================= тренд (bias) =======================
def htf_bias(c1h, c4h):
    """Тренд по 1H/4H. -> {"dir": bull|bear|neutral, "notes": [(dir, текст)]}."""
    notes, bull, bear = [], 0, 0
    hi, lo = swings(c1h, 3)
    if len(hi) >= 2 and len(lo) >= 2:
        if hi[-1][1] > hi[-2][1] and lo[-1][1] > lo[-2][1]:
            bull += 1
            notes.append(("bull", "структура 1H: HH + HL"))
        elif hi[-1][1] < hi[-2][1] and lo[-1][1] < lo[-2][1]:
            bear += 1
            notes.append(("bear", "структура 1H: LH + LL"))
    closes = [x[4] for x in c1h]
    if len(closes) >= 60:
        if closes[-1] > ema(closes, 50):
            bull += 1
            notes.append(("bull", "цена выше EMA50 (1H)"))
        else:
            bear += 1
            notes.append(("bear", "цена ниже EMA50 (1H)"))
    closes4 = [x[4] for x in c4h]
    if len(closes4) >= 55:
        if ema(closes4, 21) > ema(closes4, 50):
            bull += 1
            notes.append(("bull", "EMA21 > EMA50 (4H)"))
        else:
            bear += 1
            notes.append(("bear", "EMA21 < EMA50 (4H)"))
    d = "bull" if bull >= 2 else "bear" if bear >= 2 else "neutral"
    return {"dir": d, "notes": notes}


def htf_break(c1h, lookback=6):
    """Слом 1H вверх: за последние lookback закрытых 1H свечей закрытие выше последнего
    подтверждённого максимума 1H. Для шорта вызывается на зеркальных данных."""
    if len(c1h) < 60:
        return False
    hi, _ = swings(c1h, 3)
    confirmed = [(i, v) for i, v in hi if i < len(c1h) - 3]
    if not confirmed:
        return False
    level = confirmed[-1][1]
    return any(x[4] > level for x in c1h[-lookback:])


def htf_discount(c1h, entry, bars=40):
    """Вход в нижней половине диапазона 1H за последние bars свечей (для шорта — на зеркале)."""
    seg = c1h[-bars:]
    if len(seg) < 10:
        return False
    hi = max(x[2] for x in seg)
    lo = min(x[3] for x in seg)
    return entry <= (hi + lo) / 2.0


# ======================= сетап (всегда «в лонг»; шорт — через зеркало) =======================
def find_long_setup(c, c1h, P):
    """Снятие ликвидности -> MSS -> откат. Возвращает словарь сетапа или None."""
    n, k = len(c), P["swing_k"]
    if n < 100:
        return None
    a = atr(c, 14)
    if a <= 0:
        return None
    sw_hi, sw_lo = swings(c, k)
    mn_hi, _ = swings(c, 2)
    p0 = c[-1][4]
    lo96 = min(x[3] for x in c[-96:])
    hi96 = max(x[2] for x in c[-96:])
    eq = (lo96 + hi96) / 2.0
    hi1h = swings(c1h, 3)[0] if c1h else []

    for m in range(n - 1, n - 2 - P["fresh"], -1):
        body = c[m][4] - c[m][1]
        if body < P["disp_atr"] * a:
            continue
        for s in range(m - 1, max(m - P["sweep_window"], 25) - 1, -1):
            swept = [(i, v) for i, v in sw_lo if i + k < s and i >= s - 100 and c[s][3] < v < c[s][4]]
            if not swept:
                continue
            L = swept[-1][1]
            if hi96 > lo96 and (L - lo96) / (hi96 - lo96) > P["sweep_pos_max"]:
                continue                      # снятие не у края диапазона
            prior_hi = [(i, v) for i, v in mn_hi if i + 2 < s and i >= s - 40]
            if not prior_hi:
                continue
            H = prior_hi[-1][1]
            if c[s][4] >= H or c[m][4] <= H:
                continue
            if any(c[j][4] > H for j in range(s + 1, m)):
                continue                      # слом структуры уже был раньше
            S = min(x[3] for x in c[s:])
            I = max(x[2] for x in c[s:])
            R = I - S
            if R < P["min_leg_atr"] * a or p0 <= S:
                continue
            f0 = (I - p0) / R
            if f0 > 0.9 or f0 < -0.3:
                continue                      # цена почти вернулась к низу или убежала вверх
            base = [x[5] for x in c[max(0, s - 20):s]]
            avg_v = sum(base) / len(base) if base else 0
            vr = max(c[s][5], c[m][5]) / avg_v if avg_v > 0 else 0.0
            if vr < P["min_vol_ratio"]:
                continue

            fvg = None
            for i in range(s + 1, n - 1):
                lo_g, hi_g = c[i - 1][2], c[i + 1][3]
                if hi_g > lo_g and S <= lo_g and hi_g <= I:
                    fvg = (lo_g, hi_g)
            ob = None
            for j in range(m - 1, s - 1, -1):
                if c[j][4] < c[j][1]:
                    ob = (c[j][3], c[j][2])
                    break

            band_hi, band_lo = I - 0.6 * R, I - 0.79 * R
            entry, src = None, "Fib"
            for name, z in (("OB", ob), ("FVG", fvg)):
                if z and band_lo <= (z[0] + z[1]) / 2.0 <= band_hi:
                    entry, src = (z[1] if P["entry_edge"] else (z[0] + z[1]) / 2.0), name
                    break
            if entry is None:
                entry = I - 0.618 * R
            mode = "limit"
            if p0 <= entry:
                entry, mode = p0, "market"
            elif p0 - entry > P["max_entry_atr"] * a:
                continue                      # лимит слишком далеко, вряд ли исполнится

            if P.get("htf_gate"):
                if not htf_break(c1h) or not htf_discount(c1h, entry):
                    continue

            sl = S - P["sl_buf_atr"] * a
            risk = entry - sl
            risk_pct = risk / abs(entry) * 100.0    # abs: в зеркале цена отрицательная
            if not (P["min_sl_pct"] <= risk_pct <= P["max_sl_pct"]):
                continue

            cap = entry + abs(entry) * P["max_tp_pct"] / 100.0
            levels = sorted({I} | {v for _, v in sw_hi if v > entry} | {v for _, v in hi1h if v > entry})
            tp1 = None
            for lv in levels:
                if lv <= cap and (lv - entry) / risk >= P["min_rr"]:
                    tp1 = lv
                    break
            if tp1 is None:
                ext = S + 1.272 * R
                if ext <= cap and (ext - entry) / risk >= P["min_rr"]:
                    tp1 = ext
                else:
                    continue
            if p0 >= tp1:
                continue
            tp2 = max(S + 1.618 * R, tp1 + risk)

            minutes = max(30.0, (tp1 - entry) / (0.5 * a) * 15.0)
            return {"s": s, "m": m, "L": L, "H": H, "S": S, "I": I, "R": R,
                    "entry": entry, "mode": mode, "src": src, "sl": sl, "tp1": tp1, "tp2": tp2,
                    "risk_pct": risk_pct, "rr": (tp1 - entry) / risk, "fvg": fvg, "ob": ob,
                    "f_e": (I - entry) / R, "disp": body / a, "vol_ratio": vr,
                    "disc": entry < eq, "atr": a, "minutes": minutes, "price": p0,
                    "candle": c[m][0]}
    return None


def unmirror(st):
    out = dict(st)
    for key in ("L", "H", "S", "I", "entry", "sl", "tp1", "tp2", "price"):
        out[key] = -st[key]
    for key in ("fvg", "ob"):
        if st[key]:
            out[key] = (-st[key][1], -st[key][0])
    return out


# ======================= оценка =======================
WORDS = {
    "long": {"name": "LONG", "emoji": "🟢", "trend": "вверх", "swept": "минимум", "side": "выше",
             "zone": "дисконт", "dir": "bull"},
    "short": {"name": "SHORT", "emoji": "🔴", "trend": "вниз", "swept": "максимум", "side": "ниже",
              "zone": "премиум", "dir": "bear"},
}


def score_setup(st, side, bias, btc, is_btc):
    """Оценка из 12 и список причин."""
    w = WORDS[side]
    mine = w["dir"]
    opp = "bear" if mine == "bull" else "bull"
    pts, why = 4, []   # 2 за снятие и 2 за MSS: обязательные условия сетапа
    why.append("Снятие ликвидности: свеча сняла %s %s и закрылась %s уровня (ложный пробой)"
               % (w["swept"], fmt(st["L"]), w["side"]))
    why.append("Слом структуры (MSS): закрытие %s %s импульсной свечой ×%.1f ATR"
               % (w["side"], fmt(st["H"]), st["disp"]))
    if bias["dir"] == mine:
        pts += 2
        txt = "; ".join(t for d, t in bias["notes"] if d == mine)
        why.append("Тренд 1H/4H %s: %s" % (w["trend"], txt))
    elif bias["dir"] == "neutral":
        pts += 1
        why.append("Тренд 1H/4H нейтральный: вход от ликвидности")
    else:
        why.append("⚠ Против тренда 1H/4H: повышенный риск")
    if st["fvg"]:
        pts += 1
        why.append("FVG (имбаланс) %s–%s" % (fmt(st["fvg"][0]), fmt(st["fvg"][1])))
    if st["ob"]:
        pts += 1
        why.append("Order Block %s–%s" % (fmt(st["ob"][0]), fmt(st["ob"][1])))
    if 0.55 <= st["f_e"] <= 0.80:
        pts += 1
        why.append("Вход на %.0f %% отката ноги %s→%s (зона Фибоначчи 62–79 %%)"
                   % (st["f_e"] * 100, fmt(st["S"]), fmt(st["I"])))
    else:
        why.append("Вход на %.0f %% отката ноги %s→%s" % (st["f_e"] * 100, fmt(st["S"]), fmt(st["I"])))
    if st["vol_ratio"] >= 2.0:
        pts += 1
        why.append("Сильный объём ×%.1f от нормы на снятии/импульсе" % st["vol_ratio"])
    if st["disc"]:
        pts += 1
        why.append("Вход в зоне «%s» диапазона 24ч" % w["zone"])
    if not is_btc and btc in ("bull", "bear"):
        if btc == mine:
            pts += 1
            why.append("Тренд BTC 1H совпадает")
        elif btc == opp:
            pts -= 1
            why.append("⚠ BTC идёт против сигнала")
    return pts, why


# ======================= сообщение =======================
def fmt(p):
    a = abs(p)
    if a >= 1000:
        return "{:,.1f}".format(p).replace(",", " ")
    if a >= 100:
        return "%.2f" % p
    if a >= 1:
        return "%.3f" % p
    if a >= 0.1:
        return "%.4f" % p
    return "%.6f" % p


def build_message(coin, cand, now_ms, P, source):
    st, side, score, why = cand["st"], cand["side"], cand["score"], cand["why"]
    w = WORDS[side]
    e, sl, tp1, tp2 = st["entry"], st["sl"], st[
