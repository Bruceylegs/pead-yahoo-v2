#!/usr/bin/env python3
# Minimal Yahoo-first PEAD runner (stdlib only).
# - Captures Yahoo options chains
# - Computes 30-day ATM IV by variance interpolation
# - Pulls D-1 & D0 OHLCV
# - Emits a compact JSON in reports/

import os, json, math, csv, pathlib, datetime as dt, urllib.request, urllib.parse

TZ_UTC = dt.timezone.utc
ROOT = pathlib.Path(".")
DATA_DIR = ROOT / "data"
IV_DIR = DATA_DIR / "iv"
REPORTS_DIR = ROOT / "reports"
for p in (IV_DIR, REPORTS_DIR): p.mkdir(parents=True, exist_ok=True)

def now_utc(): return dt.datetime.now(tz=TZ_UTC)

def http_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))

def yahoo_chain(ticker: str):
    u = f"https://query2.finance.yahoo.com/v7/finance/options/{urllib.parse.quote(ticker)}"
    return http_json(u)

def yahoo_chain_for_date(ticker: str, exp_unix: int):
    u = f"https://query2.finance.yahoo.com/v7/finance/options/{urllib.parse.quote(ticker)}?date={exp_unix}"
    return http_json(u)

def yahoo_hist_day(ticker: str, day: dt.date):
    p1 = int(dt.datetime.combine(day, dt.time(0,0), tzinfo=TZ_UTC).timestamp())
    p2 = int(dt.datetime.combine(day+dt.timedelta(days=1), dt.time(0,0), tzinfo=TZ_UTC).timestamp())
    u = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?period1={p1}&period2={p2}&interval=1d"
    j = http_json(u)
    r = j["chart"]["result"][0]; q = r["indicators"]["quote"][0]
    return {"open":q["open"][0],"high":q["high"][0],"low":q["low"][0],"close":q["close"][0],"volume":q["volume"][0]}

def pick_expiries(exp_unix, t_now_unix, target_days=30.0):
    days = [(e - t_now_unix)/86400.0 for e in exp_unix if e > t_now_unix + 86400]
    if not days: return None, None
    below = [d for d in days if d <= target_days]
    above = [d for d in days if d >= target_days]
    e1 = t_now_unix + int((below[-1] if below else above[0])*86400)
    e2 = t_now_unix + int((above[0] if above else below[-1])*86400)
    return e1, e2

def nearest_atm_iv(options, spot):
    best_iv, best_diff = None, 1e18
    for opt in options:
        k = opt.get("strike"); iv = opt.get("impliedVolatility")
        if k is None or iv in (None, 0): continue
        d = abs(k - spot)
        if d < best_diff:
            best_diff, best_iv = d, iv
    return best_iv

def iv30_from_yahoo(chain_json, when):
    try:
        res = chain_json["optionChain"]["result"][0]
        quote = res["quote"]; spot = quote.get("regularMarketPrice")
        if spot is None: return None
        exp_unix = res.get("expirationDates", [])
        t_now = int(when.timestamp())
        e1, e2 = pick_expiries(exp_unix, t_now)
        if not e1 or not e2: return None

        j1 = yahoo_chain_for_date(quote["symbol"], e1)["optionChain"]["result"][0]["options"][0]
        j2 = yahoo_chain_for_date(quote["symbol"], e2)["optionChain"]["result"][0]["options"][0]
        iv1c, iv1p = nearest_atm_iv(j1.get("calls",[]), spot), nearest_atm_iv(j1.get("puts",[]), spot)
        iv2c, iv2p = nearest_atm_iv(j2.get("calls",[]), spot), nearest_atm_iv(j2.get("puts",[]), spot)
        iv1 = iv1c if iv1p is None else (iv1c+iv1p)/2 if iv1c else iv1p
        iv2 = iv2c if iv2p is None else (iv2c+iv2p)/2 if iv2c else iv2p
        if iv1 is None and iv2 is None: return None
        if iv1 is None: return iv2
        if iv2 is None: return iv1
        T1 = max(1e-6,(e1 - t_now)/31557600.0); T2 = max(1e-6,(e2 - t_now)/31557600.0); Tt = 30.0/365.25
        var1, var2 = iv1*iv1*T1, iv2*iv2*T2
        alpha = (Tt - T1)/(T2 - T1) if abs(T2-T1)>1e-9 else 0.5
        vart = var1 + (var2 - var1)*alpha
        return math.sqrt(max(1e-10, vart / Tt))
    except Exception:
        return None

def gapfill_and_clv(prev_close, d0):
    o,h,l,c = d0["open"], d0["high"], d0["low"], d0["close"]
    gap = (o - prev_close) if o>=prev_close else (prev_close - o)
    retr = (o - l) if o>=prev_close else (h - o)
    gapFillPct = min(150.0, 100.0 * (retr / gap)) if gap>1e-9 else 0.0
    clv = (2*c - h - l) / (h - l) if h>l else 0.0
    return round(gapFillPct,2), round(clv,3)

def simple_pead(clv, gapFillPct, ivCrush):
    score=0.0
    if clv is not None: score += 1.0 if clv>=0 else -0.5
    if gapFillPct is not None: score += 1.0 if gapFillPct<=33.0 else -0.5
    if ivCrush is not None:
        if -40<=ivCrush<=-5: score += 1.0
        elif ivCrush<-50: score -= 0.5
    pead = max(-8.0, min(12.0, 2.0*score))
    rng = [int(pead-3), int(pead+3)]
    conf = max(0.1, min(0.8, round(0.35+0.1*score,2)))
    return round(pead,2), rng, conf

def load_tickers(csv_path="tickers.csv"):
    rows=[]
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append({"ticker":r["ticker"].strip().upper(),"report_date":r["report_date"].strip(),"class":r["class"].strip().upper()})
    return rows

def main():
    now = now_utc(); today = now.date()
    # Phase detection: if run near 19:55 → PRE; else POST (as per workflow schedules)
    hhmm = now.strftime("%H:%M")
    phase = "pre" if hhmm.startswith("19:55") else "post"
    out = {"executionTimestamp": now.isoformat(), "basis":"vs D-1 close", "mode":"Yahoo-first (free)", "phase":phase, "companies":[]}

    for row in load_tickers():
        tkr = row["ticker"]; rep_date = dt.date.fromisoformat(row["report_date"]); klass=row["class"]
        capture_pre = (phase=="pre" and klass=="AMC" and rep_date==today) \
                   or (phase=="post" and klass=="BMO" and rep_date==today+dt.timedelta(days=1))
        capture_post = (phase=="post" and rep_date==today)

        iv_pre=iv_post=iv_crush=None; attempts=[]
        if capture_pre:
            try:
                iv_pre = iv30_from_yahoo(yahoo_chain(tkr), now); attempts.append({"when":"pre","source":"yahoo","ok":bool(iv_pre)})
                if iv_pre: (IV_DIR / f"{tkr}_{rep_date}_pre.json").write_text(json.dumps({"iv30":iv_pre,"ts":now.isoformat()}))
            except Exception as e: attempts.append({"when":"pre","source":"yahoo","ok":False,"reason":str(e)})
        if capture_post:
            try:
                iv_post = iv30_from_yahoo(yahoo_chain(tkr), now); attempts.append({"when":"post","source":"yahoo","ok":bool(iv_post)})
                if iv_post: (IV_DIR / f"{tkr}_{today}_post.json").write_text(json.dumps({"iv30":iv_post,"ts":now.isoformat()}))
            except Exception as e: attempts.append({"when":"post","source":"yahoo","ok":False,"reason":str(e)})

        # reuse saved snapshots if present
        pre_file = IV_DIR / f"{tkr}_{rep_date}_pre.json"
        post_file = IV_DIR / f"{tkr}_{today}_post.json"
        if iv_pre is None and pre_file.exists(): iv_pre = json.loads(pre_file.read_text()).get("iv30")
        if iv_post is None and post_file.exists(): iv_post = json.loads(post_file.read_text()).get("iv30")
        if iv_pre and iv_post: iv_crush = round((iv_post-iv_pre)/iv_pre*100.0,2)

        gapFillPct=clv=None
        if capture_post:
            try:
                d0 = yahoo_hist_day(tkr, today); dm1 = yahoo_hist_day(tkr, today-dt.timedelta(days=1))
                gapFillPct, clv = gapfill_and_clv(dm1["close"], d0)
            except Exception: pass

        pead, rng, conf = simple_pead(clv, gapFillPct, iv_crush)
        cont = None
        if clv is not None and gapFillPct is not None:
            cont = (clv>=0 and gapFillPct<=33.0)

        out["companies"].append({
            "ticker": tkr,
            "reportDate": row["report_date"],
            "class": klass,
            "timestampUtc": now.isoformat(),
            "iv30": {"pre":iv_pre,"post":iv_post,"iv30CrushPct":iv_crush,"source":"yahoo","attempts":attempts},
            "realized": {"d0":{"gapFillPct":gapFillPct,"clv":clv}},
            "guidancePattern": {"label":"Maintain (Data-Lite)","rationale":"Free mode (no IR/8-K parse)"},
            "peadPct": pead,
            "rangePct": rng,
            "confidence": conf,
            "continuationFriendly": cont
        })

    path = REPORTS_DIR / f"pead_{today}_{phase}.json"
    path.write_text(json.dumps(out, indent=2))
    print(f"Wrote {path}")

if __name__ == "__main__":
    main()
