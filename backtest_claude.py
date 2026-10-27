#!/usr/bin/env python3
"""Бэктест сигналов Сигналы от Claude на истории OKX (запускается в GitHub Actions).

Берёт свечи 15m и 1H за N месяцев по списку монет, проходит по времени без заглядывания
вперёд (сигнал на баре t использует только закрытые свечи до t), считает исход:
TP1 или SL, с той же логикой, что и в боевом боте. Пишет отчёт и отправляет в Telegram.
Только стандартная библиотека и модуль signals_claude из этого же репозитория.
"""
import bisect
import os
import sys
import time
import urllib.request
import json
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import signals_claude as sc  # noqa: E402

M15 = 900_000
H1 = 3_600_000
OKX = "https://www.okx.com/api/v5/market/history-candles"
DEFAULT_COINS = "BTC,ETH,SOL,XRP,BNB,DOGE,NEAR,ADA,AVAX,LINK,LTC,TRX,DOT,SUI,APT"


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 signals-backtest"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def okx_history(coin, bar, months):
    """Закрытые свечи (ts, o, h, l, c, vol) за ~months месяцев, от старых к новым."""
    days = months * 30
    since = int(time.time() * 1000) - days * 24 * H1
    rows, after = {}, ""
    for _ in range(2000):
        url = "%s?instId=%s-USDT-SWAP&bar=%s&limit=100" % (OKX, coin, bar)
        if after:
            url += "&after=%s" % after
        j = get_json(url)
        if j.get("code") != "0":
            raise RuntimeError("okx: %s" % j.get("msg"))
        batch = j.get("data") or []
        if not batch:
            break
        for r in batch:
            if r[8] == "1":                       # только подтверждённые свечи
                ts = int(r[0])
                rows[ts] = (ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        oldest = min(int(r[0]) for r in batch)
        if oldest <= since or len(batch) < 100:
            break
        after = str(oldest)
        time.sleep(0.12)
    return [rows[k] for k in sorted(rows) if k >= since]


def outcome(c, t, cand):
    """Победа/поражение/не исполнен — та же логика, что в калибровке (пессимистично при касании обоих)."""
    st, side = cand["st"], cand["side"]
    e, sl, tp = st["entry"], st["sl"], st["tp1"]
    start = t + 1
    if st["mode"] == "limit":
        filled = None
        for j in range(start, min(start + 6, len(c))):
            lo, hi = c[j][3], c[j][2]
            if (side == "long" and lo <= e) or (side == "short" and hi >= e):
                filled = j
                break
        if filled is None:
            return "nofill"
        start = filled
    for j in range(start, min(start + 96, len(c))):
        lo, hi = c[j][3], c[j][2]
        hit_sl = lo <= sl if side == "long" else hi >= sl
        hit_tp = hi >= tp if side == "long" else lo <= tp
        if hit_sl:
            return "loss"
        if hit_tp:
            return "win"
    return "open"


def btc_bias_at(btc1h_ts, btc1h, now_ms, P):
    k = bisect.bisect_right(btc1h_ts, now_ms - H1 + 1)   # закрытые к моменту now
    win = btc1h[max(0, k - 300):k]
    if len(win) < 60:
        return None
    return sc.htf_bias(win, sc.aggregate_4h(win))["dir"]


def backtest_coin(coin, c15, c1h, P, btc=None):
    """Возвращает список (score, side, mode, rr, исход)."""
    ts1 = [x[0] for x in c1h]
    ts15 = [x[0] for x in c15]
    out, cool = [], {}
    cooldown_bars = max(1, P["cooldown_min"] // 15)
    for t in range(240, len(c15) - 6):
        now = c15[t][0] + M15            # момент закрытия бара t
        win15 = c15[t - 239:t + 1]
        k = bisect.bisect_right(ts1, now - H1)  # 1H, закрытые до now
        win1 = c1h[max(0, k - 300):k]
        if len(win15) < 240 or len(win1) < 60:
            continue
        btc_dir = btc_bias_at(*btc, now, P) if btc else None
        for cand in sc.detect(win15, win1, P, btc_dir, coin == "BTC"):
            key = cand["side"]
            if t - cool.get(key, -10**9) < cooldown_bars:
                continue
            if cand["score"] < P["min_score"]:
                continue
            cool[key] = t
            res = outcome(c15, t, cand)
            out.append((cand["score"], cand["side"], cand["st"]["mode"], cand["st"]["rr"], res))
    return out


def summarize(rows):
    """Винрейт, средний R и разбивка по оценке. Средний R: победа +RR, поражение -1."""
    closed = [r for r in rows if r[4] in ("win", "loss")]
    wins = sum(1 for r in closed if r[4] == "win")
    losses = len(closed) - wins
    avg_r = (sum(r[3] for r in closed if r[4] == "win") - losses) / len(closed) if closed else 0.0
    lines = ["Сделок всего: %d (не исполнено лимитом: %d, открыто: %d)"
             % (len(rows), sum(1 for r in rows if r[4] == "nofill"), sum(1 for r in rows if r[4] == "open"))]
    if closed:
        lines.append("Закрыто: %d · побед %d · поражений %d · винрейт %.1f %%"
                     % (len(closed), wins, losses, 100.0 * wins / len(closed)))
        lines.append("Средний результат на сделку: %+.2f R" % avg_r)
        by = defaultdict(lambda: [0, 0])
        for r in closed:
            by[r[0]][0] += 1
            by[r[0]][1] += r[4] == "win"
        for sc_v in sorted(by):
            n, w = by[sc_v]
            lines.append("  оценка %d: %d сделок, винрейт %.0f %%" % (sc_v, n, 100.0 * w / n))
    else:
        lines.append("Закрытых сделок нет — нечего считать.")
    return "\n".join(lines)


def main(argv):
    months = int(os.environ.get("MONTHS", "3"))
    coins = [c.strip().upper() for c in os.environ.get("COINS", DEFAULT_COINS).split(",") if c.strip()]
    P = sc.load_params()
    P = dict(P, allow_counter=0, min_score=int(os.environ.get("MIN_SCORE", P["min_score"])))
    all_rows, failed = [], []
    btc = None
    try:
        b15 = okx_history("BTC", "15m", months)
        b1 = okx_history("BTC", "1H", months)
        btc = ([x[0] for x in b1], b1)
    except Exception as e:
        failed.append("BTC (фильтр тренда): %s" % str(e)[:80])
    for coin in coins:
        try:
            c15 = okx_history(coin, "15m", months)
            c1h = okx_history(coin, "1H", months)
            if len(c15) < 300 or len(c1h) < 100:
                failed.append("%s: мало истории (%d/%d свечей)" % (coin, len(c15), len(c1h)))
                continue
            all_rows += backtest_coin(coin, c15, c1h, P, btc if coin != "BTC" else None)
            print("%s: %d свечей 15m, %d сетапов накоплено" % (coin, len(c15), len(all_rows)), flush=True)
        except Exception as e:
            failed.append("%s: %s" % (coin, str(e)[:80]))
        time.sleep(0.2)
    report = ("📊 Бэктест «Сигналы от Claude» на OKX\n"
              "Период: ~%d мес. · монет: %d · строгий порог %d/12\n"
              % (months, len(coins), P["min_score"])) + summarize(all_rows)
    if failed:
        report += "\nНе загружены: " + "; ".join(failed[:10])
    report += "\nВажно: история в прошлом не гарантирует результат. Не финансовый совет."
    print(report)
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if token and chat:
        sc.tg_sender(token, chat)(report)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
