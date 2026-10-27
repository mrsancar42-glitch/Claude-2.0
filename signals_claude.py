#!/usr/bin/env python3
"""SMC-сканер (Smart Money Concepts) для крипто-перпетуалов.

Ищет на 15-минутках классический сетап института:
  снятие ликвидности (sweep) -> слом структуры (MSS) импульсной свечой
  -> откат в FVG / Order Block / зону Фибоначчи (OTE 0.62-0.79).
Направление фильтруется трендом 1H/4H и трендом BTC. Каждый сетап получает
оценку (скоринг); сильнейшие уходят в Telegram: монета, направление, вход,
TP, SL, причина, ориентир по времени. Бот ничего не торгует и не знает ваш баланс.

Данные: публичные свечи Bybit -> OKX -> Binance (что доступно с сервера).
Только стандартная библиотека Python.

  python3 signals_claude.py           обычный проход (по расписанию)
  python3 signals_claude.py --test    проверка связи + тестовое сообщение
  python3 signals_claude.py --dry     без отправки, сигналы в консоль
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


def _num(name, default):
    v = os.environ.get(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except ValueError:
        return float(default)


def load_params():
    return {
        "swing_k": int(_num("SWING_K", 3)),          # фрактал для уровней ликвидности
        "sweep_window": int(_num("SWEEP_WINDOW", 10)),  # свечей между свипом и MSS
        "fresh": int(_num("FRESH", 2)),              # MSS не старше fresh+1 свечей
        "disp_atr": _num("DISP_ATR", 1.0),           # тело импульса, в ATR
        "min_leg_atr": _num("MIN_LEG_ATR", 1.5),     # минимальная нога, в ATR
        "sl_buf_atr": _num("SL_BUF_ATR", 0.2),       # запас SL за экстремумом свипа
        "min_sl_pct": _num("MIN_SL_PCT", 0.3),       # SL не ближе, % (иначе шум)
        "max_sl_pct": _num("MAX_SL_PCT", 3.5),       # SL не дальше, % (плечо 20x)
        "sweep_pos_max": _num("SWEEP_POS_MAX", 0.5),  # снятый уровень: доля диапазона 24ч от края (1 = без фильтра)
        "min_vol_ratio": _num("MIN_VOL_RATIO", 1.2),  # объём свипа/импульса к норме (0 = без фильтра)
        "max_entry_atr": _num("MAX_ENTRY_ATR", 1.3),  # лимит не дальше N ATR от цены
        "allow_counter": int(_num("ALLOW_COUNTER", 0)),  # 0 = не торговать против тренда 1H/4H
        "entry_edge": int(_num("ENTRY_EDGE", 1)),      # 1 = вход у края зоны FVG/OB, а не в середине
        "min_rr": _num("MIN_RR", 1.5),
        "max_tp_pct": _num("MAX_TP_PCT", 6.0),
        "min_score": int(_num("MIN_SCORE", 8)),      # обычный порог (из 12)
        "floor_score": int(_num("FLOOR_SCORE", 6)),  # порог в «догоняющем» режиме
        "target_per_day": int(_num("TARGET_PER_DAY", 5)),
        "max_per_day": int(_num("MAX_PER_DAY", 10)),
        "max_alerts": int(_num("MAX_ALERTS", 3)),    # за один проход
        "cooldown_min": int(_num("COOLDOWN_MIN", 180)),
        "limit_ttl_min": int(_num("LIMIT_TTL_MIN", 90)),
        "min_turnover": _num("MIN_TURNOVER_USD", 20_000_000),
        "max_coins": int(_num("MAX_COINS", 80)),
        "workers": int(_num("WORKERS", 8)),
        "tz_offset": _num("TZ_OFFSET", 3),           # для времени в сообщении
        "lev_hint": int(_num("LEV_HINT", 20)),       # плечо для подсказки «SL/TP в % маржи»
    }


# ======================= данные =======================
IV_MS = {"15m": 900_000, "1h": HOUR_MS}
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
        rows = [_row(r) for r in j["data"]]
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
    """-> (source, c15, c1h, errors); source None, если нигде не вышло."""
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
    """Зеркало цены (p -> -p): шорт превращается в лонг, одна логика на оба направления."""
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


# ======================= сетап (всегда «в лонг»; шорт — через зеркало) =======================
def find_long_setup(c, c1h, P):
    """Sweep -> MSS -> откат. Возвращает словарь сетапа или None."""
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
                continue                      # ликвидность не у края диапазона
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
                continue                      # цена почти вернулась к низу / убежала вверх
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

            band_hi, band_lo = I - 0.382 * R, I - 0.79 * R
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
                continue                      # лимит слишком далеко — вряд ли исполнится

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
    pts, why = 4, []   # 2 за свип + 2 за MSS — обязательные условия сетапа
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
        why.append("Тренд 1H/4H нейтральный — вход от ликвидности")
    else:
        why.append("⚠ Против тренда 1H/4H — повышенный риск")
    if st["fvg"]:
        pts += 1
        why.append("FVG (имбаланс) %s–%s" % (fmt(st["fvg"][0]), fmt(st["fvg"][1])))
    if st["ob"]:
        pts += 1
        why.append("Order Block %s–%s" % (fmt(st["ob"][0]), fmt(st["ob"][1])))
    if 0.55 <= st["f_e"] <= 0.80:
        pts += 1
        why.append("Вход на %.0f %% отката ноги %s→%s (зона Фибоначчи OTE 62–79 %%)"
                   % (st["f_e"] * 100, fmt(st["S"]), fmt(st["I"])))
    else:
        why.append("Вход на %.0f %% отката ноги %s→%s" % (st["f_e"] * 100, fmt(st["S"]), fmt(st["I"])))
    if st["vol_ratio"] >= 2.0:
        pts += 1
        why.append("Сильный объём ×%.1f от нормы на свипе/импульсе" % st["vol_ratio"])
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
    e, sl, tp1, tp2 = st["entry"], st["sl"], st["tp1"], st["tp2"]
    pct = lambda x: abs(x - e) / e * 100.0
    hours = max(0.5, round(st["minutes"] / 60.0 * 2) / 2)
    lev = max(1, P["lev_hint"])
    age = max(0, int((now_ms - (st["candle"] + IV_MS["15m"])) // 60_000))   # возраст MSS-свечи
    tp1_pct = pct(tp1)
    local = time.gmtime(now_ms / 1000.0 + P["tz_offset"] * 3600)
    hhmm = "%02d:%02d" % (local.tm_hour, local.tm_min)
    if st["mode"] == "market":
        entry_line = "Вход: по рынку ≈ %s" % fmt(e)
        valid = "вход сейчас"
    else:
        entry_line = "Вход: лимит %s (ждать отката%s)" % (fmt(e), " в " + st["src"] if st["src"] != "Fib" else "")
        valid = "лимит актуален ~%d мин, не дошла цена — отмена" % P["limit_ttl_min"]
    lines = [
        "📡 Сигналы от Claude",
        "%s %s  %sUSDT" % (w["emoji"], w["name"], coin),
        "Монета: %sUSDT" % coin,
        "Направление: %s" % w["name"],
        entry_line,
        "TP1: %s (+%.2f %%)  RR %.1f" % (fmt(tp1), pct(tp1), st["rr"]),
        "TP2: %s (+%.2f %%)" % (fmt(tp2), pct(tp2)),
        "SL: %s (−%.2f %%)" % (fmt(sl), st["risk_pct"]),
        "Время: ориентир до TP1 ~%.1f ч · %s" % (hours, valid),
        "Отправлен: %s (UTC%+d)%s" % (hhmm, int(P["tz_offset"]),
                                      " · сетап сложился %d мин назад" % age if age >= 10 else ""),
        "Причина:",
    ]
    lines += ["• " + x for x in why]
    tag = "  ⚡ смягчённые условия (мало сигналов за сутки)" if cand.get("relaxed") else ""
    lines.append("Оценка: %d/12%s" % (score, tag))
    lines.append("На %dx: SL ≈ −%.0f %% маржи, TP1 ≈ +%.0f %% (без комиссий)"
                 % (lev, lev * st["risk_pct"], lev * tp1_pct))
    if st["risk_pct"] >= 100.0 / lev - 0.5:
        lines.append("⚠ На %dx ликвидация раньше SL — снизьте плечо" % lev)
    lines.append("Данные: %s, 15m/1H. Не финансовый совет." % source)
    return "\n".join(lines)


# ======================= сканирование =======================
def detect(c15, c1h, P, btc_bias=None, is_btc=False):
    """Все сетапы монеты (лонг и шорт) с оценкой."""
    if len(c15) < 100 or len(c1h) < 60:
        return []
    bias = htf_bias(c1h, aggregate_4h(c1h))
    out = []
    for side in ("long", "short"):
        cc, hh = (c15, c1h) if side == "long" else (mirror(c15), mirror(c1h))
        st = find_long_setup(cc, hh, P)
        if not st:
            continue
        if side == "short":
            st = unmirror(st)
        if not P["allow_counter"] and bias["dir"] == ("bear" if side == "long" else "bull"):
            continue
        score, why = score_setup(st, side, bias, btc_bias, is_btc)
        out.append({"side": side, "st": st, "score": score, "why": why})
    return out


def load_state(path):
    try:
        with open(path) as f:
            s = json.load(f)
    except (OSError, ValueError):
        s = {}
    s.setdefault("cool", {})
    s.setdefault("log", [])
    s.setdefault("last_error_ms", 0)
    return s


def save_state(path, state, original):
    if state != original:
        with open(path, "w") as f:
            json.dump(state, f, indent=1, sort_keys=True)


def relaxed_params(P):
    """Условия для «догоняющего» режима: больше сетапов, ниже качество."""
    q = dict(P)
    q.update(allow_counter=1, min_vol_ratio=0.0, sweep_pos_max=max(P["sweep_pos_max"], 0.8),
             max_entry_atr=max(P["max_entry_atr"], 2.0), disp_atr=min(P["disp_atr"], 0.8))
    return q


def run_once(coins, fetch, send, state, now_ms, P):
    """Один проход. fetch(coin, now_ms) -> (source, c15, c1h, errors)."""
    state["cool"] = {k: t for k, t in state["cool"].items() if now_ms - t < 2 * DAY_MS}
    state["log"] = [r for r in state["log"] if now_ms - r[0] < 2 * DAY_MS]
    with ThreadPoolExecutor(max_workers=max(1, P["workers"])) as pool:
        results = list(pool.map(lambda c: fetch(c, now_ms), coins))
    data = {coin: r for coin, r in zip(coins, results) if r[0] is not None}

    btc_bias = None
    if "BTC" in data and len(data["BTC"][2]) >= 60:
        b = data["BTC"][2]
        btc_bias = htf_bias(b, aggregate_4h(b))["dir"]

    n24 = sum(1 for r in state["log"] if now_ms - r[0] < DAY_MS)
    n6 = sum(1 for r in state["log"] if now_ms - r[0] < 6 * HOUR_MS)
    slots = max(1, -(-P["target_per_day"] // 4))        # слотов «догонялки» на 6 часов
    behind = n24 < P["target_per_day"] and n6 < slots
    PE = relaxed_params(P) if behind else P
    threshold = P["floor_score"] if behind else P["min_score"]
    allowed = min(P["max_alerts"], max(0, P["max_per_day"] - n24))
    if behind:
        allowed = min(allowed, slots - n6)

    cands = []
    for coin, (source, c15, c1h, _e) in data.items():
        for cand in detect(c15, c1h, PE, btc_bias, coin == "BTC"):
            key = "%s|%s" % (coin, cand["side"])
            if now_ms - state["cool"].get(key, 0) < P["cooldown_min"] * 60_000:
                continue
            cand.update(coin=coin, key=key, source=source)
            cands.append(cand)

    cands = [x for x in cands if x["score"] >= threshold]
    cands.sort(key=lambda x: -x["score"])
    sent = []
    for cand in cands[:allowed]:
        cand["relaxed"] = behind
        msg = build_message(cand["coin"], cand, now_ms, P, cand["source"])
        send(msg)
        sent.append(msg)
        state["cool"][cand["key"]] = now_ms
        state["log"].append([now_ms, cand["key"], cand["score"]])
    if not data and coins and now_ms - state["last_error_ms"] > 6 * HOUR_MS:
        send("⚠️ Бот не получил данные ни по одной монете (биржи недоступны). "
             "Проверю снова на следующем запуске.")
        state["last_error_ms"] = now_ms
    return sent


# ======================= Telegram =======================
def tg_sender(token, chat_id):
    def send(text):
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        r = http_json("https://api.telegram.org/bot%s/sendMessage" % token, data=data)
        if not r.get("ok"):
            raise RuntimeError("telegram: %s" % r)
    return send


def discover_chat_id(token):
    r = http_json("https://api.telegram.org/bot%s/getUpdates?limit=50" % token)
    chat = None
    for u in r.get("result", []):
        m = u.get("message") or u.get("edited_message")
        if m and m.get("chat", {}).get("id"):
            chat = m["chat"]["id"]
    return chat


def main(argv):
    P = load_params()
    now_ms = int(time.time() * 1000)
    if "--dry" in argv:
        def send(t):
            print(t, "\n---")
    else:
        token = os.environ.get("TELEGRAM_TOKEN", "").strip()
        if not token:
            print("Нет секрета TELEGRAM_TOKEN.")
            return 1
        chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip() or discover_chat_id(token)
        if not chat:
            print("Не нашёл chat_id: отправьте своему боту /start и запустите снова.")
            return 1
        send = tg_sender(token, chat)

    if FIXED_COINS:
        src, coins, sources = "список", FIXED_COINS, None
    else:
        src, coins = get_universe(P["min_turnover"], P["max_coins"])
        sources = [s for s in SOURCES if s[0] == src] if src else None

    def fetch(coin, now):
        return get_candles(coin, now, sources)

    if "--test" in argv:
        if not coins:
            txt = "⚠️ Бот запущен, но биржи не отдали список монет (недоступны с этого сервера)."
        else:
            with ThreadPoolExecutor(max_workers=P["workers"]) as pool:
                res = list(pool.map(lambda c: fetch(c, now_ms), coins))
            got = sum(1 for r in res if r[0])
            txt = ("✅ Сигналы от Claude работают. Источник: %s. Монет в проверке: %d, свечи получены по %d.\n"
                   "Строгий режим: оценка от %d/12. Если за сутки меньше %d сигналов, условия "
                   "временно смягчаются (метка ⚡), не чаще двух раз за 6 часов."
                   % (src, len(coins), got, P["min_score"], P["target_per_day"]))
            miss = [c for c, r in zip(coins, res) if not r[0]]
            if miss:
                txt += "\nНет данных: " + ", ".join(miss[:15])
        send(txt)
        print(txt)
        return 0

    original = load_state(STATE_FILE)
    state = json.loads(json.dumps(original))
    if not coins:
        if now_ms - state["last_error_ms"] > 6 * HOUR_MS:
            send("⚠️ Бот не смог получить список монет (биржи недоступны). Проверю снова позже.")
            state["last_error_ms"] = now_ms
        sent = []
    else:
        sent = run_once(coins, fetch, send, state, now_ms, P)
    save_state(STATE_FILE, state, original)
    print("источник: %s, монет: %d, сигналов отправлено: %d" % (src, len(coins), len(sent)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
