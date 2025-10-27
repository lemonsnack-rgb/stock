import os, json
import pandas as pd, numpy as np
from datetime import datetime, timedelta
from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

# ---- 환경값 ----
SHEET_ID = os.getenv("SHEET_ID")                   # 스프레드시트 ID
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MAX_PRICE = int(os.getenv("MAX_PRICE", "150000"))  # 150000 이하
TOP_N = int(os.getenv("TOP_N", "200"))             # 시총 Top N
MIN_TRADING_VALUE = int(os.getenv("MIN_TRADING_VALUE", "5000000000"))  # 20일 평균 거래대금 하한(50억)
ATR_N = int(os.getenv("ATR_N", "20"))
EMA_N = int(os.getenv("EMA_N", "20"))
PRICE_BONUS = int(os.getenv("PRICE_BONUS", "100000"))  # 10만원 이하 가점 기준

def yesterday_trading_date():
    today = datetime.now()
    # 최근 7일 내 영업일 찾기
    for i in range(1, 8):
        d = today - timedelta(days=i)
        try:
            df = stock.get_market_ohlcv_by_date(d.strftime("%Y%m%d"), d.strftime("%Y%m%d"), "005930")
            if df is not None and len(df) > 0:
                return d.date()
        except:
            pass
    return (today - timedelta(days=1)).date()

def build_universe(ref):
    ymd = ref.strftime("%Y%m%d")
    cap = stock.get_market_cap_by_ticker(ymd, market="KOSPI")
    cap = cap.sort_values("시가총액", ascending=False).head(max(TOP_N*2, 300))
    cap = cap[cap["종가"] <= MAX_PRICE]
    # 유동성 필터(최근 20일 평균 거래대금)
    start = ref - timedelta(days=90)
    ok = []
    for t in cap.index.tolist():
        try:
            df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, t)
            if df is None or len(df) < 25: 
                continue
            avg_val = (df["거래대금"].tail(20)).mean() if "거래대금" in df.columns else (df["거래량"].tail(20) * df["종가"].tail(20)).mean()
            if avg_val >= MIN_TRADING_VALUE:
                ok.append(t)
        except:
            pass
    return cap.loc[cap.index.intersection(ok)].sort_values("시가총액", ascending=False).head(TOP_N)

def calc_levels(tkr, ref):
    ymd = ref.strftime("%Y%m%d")
    start = ref - timedelta(days=150)
    df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, tkr)
    if df is None or len(df) < EMA_N+1:
        return None
    h, l, c = df["고가"].iloc[-1], df["저가"].iloc[-1], df["종가"].iloc[-1]
    # Pivot (전일 H/L/C)
    pp = (h + l + c)/3
    s1, r1 = 2*pp - h, 2*pp - l
    s2, r2 = pp - (h-l), pp + (h-l)
    # ATR
    high, low, close = df["고가"], df["저가"], df["종가"]
    prev_close = close.shift(1)
    tr = np.maximum(high-low, np.maximum(abs(high-prev_close), abs(low-prev_close)))
    atr = tr.rolling(ATR_N).mean().iloc[-1]
    # EMA
    ema = close.ewm(span=EMA_N).mean().iloc[-1]
    # ATR 밴드
    atr_buy_lo, atr_buy_hi = ema - 1.0*atr, ema - 0.5*atr
    atr_sell_lo, atr_sell_hi = ema + 0.5*atr, ema + 1.0*atr
    stop = min(s2, ema - 1.5*atr)
    name = stock.get_market_ticker_name(tkr)
    # 간단 점수: (밴드겹침 + EMA아래 위치 + 10만원 이하 보너스)
    in_atr_buy = (c >= atr_buy_lo) and (c <= atr_buy_hi)
    in_pivot_buy = (c >= s2) and (c <= s1)
    overlap = in_atr_buy and in_pivot_buy
    bonus = 0.3 if c <= PRICE_BONUS else 0.0
    ema_dist = (ema - c)/atr if atr else 0
    score = (1.0 if overlap else 0.5 if (in_atr_buy or in_pivot_buy) else 0.0) + 0.2*max(0, ema_dist) + bonus
    return {
        "ticker": tkr, "name": name, "close": int(c),
        "buy_pivot": f"{int(s2)}~{int(s1)}", "sell_pivot": f"{int(r1)}~{int(r2)}",
        "buy_atr": f"{int(atr_buy_lo)}~{int(atr_buy_hi)}", "sell_atr": f"{int(atr_sell_lo)}~{int(atr_sell_hi)}",
        "stop": int(stop), "atr": float(atr), "ema": float(ema), "score": round(float(score), 4),
        "in_atr_buy": in_atr_buy, "in_pivot_buy": in_pivot_buy
    }

def sheet_client():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = json.loads(os.getenv("GCP_SA_JSON"))  # GitHub Secret
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    return gspread.authorize(creds)

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)

def write_universe_and_top10(rows, ref):
    gc = sheet_client()
    sh = gc.open_by_key(SHEET_ID)
    # universe
    df = pd.DataFrame(rows)
    df.insert(0, "date", ref.strftime("%Y-%m-%d"))
    uni_ws = sh.worksheet("universe")
    uni_ws.clear()
    uni_ws.update([df.columns.tolist()] + df.values.tolist())
    # top10
    top = df.sort_values(["score", "close"], ascending=[False, False]).head(10).reset_index(drop=True)
    out = top[["ticker","name","close","buy_atr","sell_atr","buy_pivot","sell_pivot","stop","score"]].copy()
    out.insert(0, "rank", range(1, len(out)+1))
    top_ws = sh.worksheet("top10_today")
    top_ws.clear()
    top_ws.update([out.columns.tolist()] + out.values.tolist())
    # 텔레그램
    lines = [f"[KOSPI 상위10 매수 후보 | {ref.strftime('%Y-%m-%d')}]"]
    for _, r in out.iterrows():
        lines.append(f"{int(r['rank']):02d}. {r['ticker']} {r['name']}  종가 {int(r['close']):,}원")
        lines.append(f"   매수(ATR): {r['buy_atr']} | 매도(ATR): {r['sell_atr']} | 손절: {int(r['stop']):,}")
    send_telegram("\n".join(lines))

def check_positions_and_alert(ref):
    gc = sheet_client()
    sh = gc.open_by_key(SHEET_ID)
    pos = pd.DataFrame(sh.worksheet("positions").get_all_records())
    if pos.empty: 
        return
    uni = pd.DataFrame(sh.worksheet("universe").get_all_records())
    latest = uni[uni["date"] == ref.strftime("%Y-%m-%d")]
    if latest.empty:
        latest = uni
    merge = pos.merge(latest, on="ticker", how="left", suffixes=("_pos",""))
    alerts = []
    for _, r in merge.iterrows():
        if pd.isna(r.get("sell_atr")) or not r.get("avg_cost"):
            continue
        try:
            sell_hi = int(str(r["sell_atr"]).split("~")[1])
        except:
            continue
        if int(r["avg_cost"]) < sell_hi:
            name = r.get("name_pos") or r.get("name") or ""
            alerts.append(f"{r['ticker']} {name} 매도 구간 도달 후보: 목표(ATR 상단) {sell_hi:,}원 | 보유평단 {int(r['avg_cost']):,}원")
    if alerts:
        send_telegram("[보유종목 매도 시그널]\n" + "\n".join(alerts))

def main():
    ref = yesterday_trading_date()
    uni = build_universe(ref)
    rows = []
    for t in uni.index:
        lv = calc_levels(t, ref)
        if lv: rows.append(lv)
    write_universe_and_top10(rows, ref)
    check_positions_and_alert(ref)

if __name__ == "__main__":
    main()
