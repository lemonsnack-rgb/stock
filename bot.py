# bot.py
# -*- coding: utf-8 -*-
"""
KOSPI 시총 Top200(가격 ≤ 15만원) 우량주 대상
- 추천 매수가/매도가(피벗 + ATR 밴드) 계산
- 구글 스프레드시트 업데이트(universe, top10_today)
- 보유표(positions) 조회해 매도 시점 도래 후보 텔레그램 알림
"""

import os, json, sys, traceback
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests

from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

# ===== 환경값 =====
SHEET_ID_OR_URL = os.getenv("SHEET_ID", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MAX_PRICE = int(os.getenv("MAX_PRICE", "150000"))
TOP_N = int(os.getenv("TOP_N", "200"))
MIN_TRADING_VALUE = int(os.getenv("MIN_TRADING_VALUE", "5000000000"))
ATR_N = int(os.getenv("ATR_N", "20"))
EMA_N = int(os.getenv("EMA_N", "20"))
PRICE_BONUS = int(os.getenv("PRICE_BONUS", "100000"))

# ===== 기본 시트명 =====
SHEET_UNIVERSE = "universe"
SHEET_TOP10 = "top10_today"
SHEET_POSITIONS = "positions"

UNIVERSE_HEADERS = [
    "date","ticker","name","close",
    "buy_pivot","sell_pivot","buy_atr","sell_atr","stop",
    "atr","ema","score","in_atr_buy","in_pivot_buy"
]
TOP10_HEADERS = [
    "rank","ticker","name","close",
    "buy_atr","sell_atr","buy_pivot","sell_pivot","stop","score"
]
POSITIONS_HEADERS = ["ticker","name","qty","avg_cost","note"]

def log(msg):
    print(msg, flush=True)

def is_url(s: str) -> bool:
    return s.lower().startswith("http")

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[WARN] Telegram 설정이 없어 전송 생략")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)

def sheet_client():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json_raw = os.getenv("GCP_SA_JSON", "").strip()
    sa_json = json.loads(sa_json_raw)
    log(f"[DEBUG] Service Account: {sa_json.get('client_email')}")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    if is_url(SHEET_ID_OR_URL):
        log("[INFO] open_by_url 사용")
        return gc.open_by_url(SHEET_ID_OR_URL)
    else:
        log("[INFO] open_by_key 사용")
        return gc.open_by_key(SHEET_ID_OR_URL)

def ensure_worksheet(sh, title, headers):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        log(f"[INFO] 워크시트 '{title}' 없음 → 새로 생성")
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(10,len(headers)))
        if headers: ws.update([headers])
        return ws

def yesterday_trading_date():
    today = datetime.now()
    for i in range(1,8):
        d = today - timedelta(days=i)
        try:
            df = stock.get_market_ohlcv_by_date(d.strftime("%Y%m%d"), d.strftime("%Y%m%d"), "005930")
            if df is not None and len(df)>0:
                return d.date()
        except: pass
    return (today - timedelta(days=1)).date()

def build_universe(ref):
    ymd = ref.strftime("%Y%m%d")
    cap = stock.get_market_cap_by_ticker(ymd, market="KOSPI")
    cap = cap.sort_values("시가총액", ascending=False).head(max(TOP_N*2,300))
    cap = cap[cap["종가"]<=MAX_PRICE]
    ok=[]
    start = ref - timedelta(days=90)
    for t in cap.index.tolist():
        try:
            df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, t)
            if df is None or len(df)<25: continue
            avg_val = df["거래대금"].tail(20).mean()
            if avg_val>=MIN_TRADING_VALUE:
                ok.append(t)
        except: continue
    return cap.loc[cap.index.intersection(ok)].sort_values("시가총액",ascending=False).head(TOP_N)

def calc_levels(tkr, ref):
    ymd = ref.strftime("%Y%m%d")
    start = ref - timedelta(days=150)
    df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, tkr)
    if df is None or len(df)<EMA_N+1: return None
    h,l,c = df["고가"].iloc[-1], df["저가"].iloc[-1], df["종가"].iloc[-1]
    pp=(h+l+c)/3; s1=2*pp-h; r1=2*pp-l; s2=pp-(h-l); r2=pp+(h-l)
    high,low,close = df["고가"], df["저가"], df["종가"]
    prev=close.shift(1)
    tr=np.maximum(high-low, np.maximum(abs(high-prev), abs(low-prev)))
    atr=tr.rolling(ATR_N).mean().iloc[-1]
    ema=close.ewm(span=EMA_N).mean().iloc[-1]
    atr_buy_lo, atr_buy_hi = ema-1*atr, ema-0.5*atr
    atr_sell_lo, atr_sell_hi = ema+0.5*atr, ema+1*atr
    stop=min(s2, ema-1.5*atr)
    name=stock.get_market_ticker_name(tkr)
    in_atr=(c>=atr_buy_lo and c<=atr_buy_hi)
    in_pivot=(c>=s2 and c<=s1)
    score=(1.0 if (in_atr and in_pivot) else 0.5 if (in_atr or in_pivot) else 0.0)
    if c<=PRICE_BONUS: score+=0.3
    return {
        "ticker":tkr,"name":name,"close":int(c),
        "buy_pivot":f"{int(s2)}~{int(s1)}","sell_pivot":f"{int(r1)}~{int(r2)}",
        "buy_atr":f"{int(atr_buy_lo)}~{int(atr_buy_hi)}","sell_atr":f"{int(atr_sell_lo)}~{int(atr_sell_hi)}",
        "stop":int(stop),"atr":float(atr),"ema":float(ema),
        "score":round(score,4),"in_atr_buy":in_atr,"in_pivot_buy":in_pivot
    }

def write_universe_and_top10(rows, ref):
    gc=sheet_client(); sh=open_spreadsheet(gc)
    uni_ws=ensure_worksheet(sh,SHEET_UNIVERSE,UNIVERSE_HEADERS)
    top_ws=ensure_worksheet(sh,SHEET_TOP10,TOP10_HEADERS)
    ensure_worksheet(sh,SHEET_POSITIONS,POSITIONS_HEADERS)
    df=pd.DataFrame(rows); df.insert(0,"date",ref.strftime("%Y-%m-%d"))
    uni_ws.clear(); uni_ws.update([df.columns.tolist()]+df.values.tolist())
    top=df.sort_values(["score","close"],ascending=[False,False]).head(10).reset_index(drop=True)
    out=top[["ticker","name","close","buy_atr","sell_atr","buy_pivot","sell_pivot","stop","score"]].copy()
    out.insert(0,"rank",range(1,len(out)+1))
    top_ws.clear(); top_ws.update([out.columns.tolist()]+out.values.tolist())
    lines=[f"[KOSPI 상위10 매수 후보 | {ref.strftime('%Y-%m-%d')}]"]
    for _,r in out.iterrows():
        lines.append(f"{r['rank']:02d}. {r['ticker']} {r['name']} 종가 {r['close']:,}원")
        lines.append(f"   매수(ATR): {r['buy_atr']} | 매도(ATR): {r['sell_atr']} | 손절: {r['stop']:,}")
    send_telegram("\n".join(lines))

def check_positions_and_alert(ref):
    gc=sheet_client(); sh=open_spreadsheet(gc)
    try: pos_ws=sh.worksheet(SHEET_POSITIONS)
    except WorksheetNotFound: return
    pos=pd.DataFrame(pos_ws.get_all_records())
    if pos.empty: return
    uni=pd.DataFrame(sh.worksheet(SHEET_UNIVERSE).get_all_records())
    latest=uni[uni["date"]==ref.strftime("%Y-%m-%d")]
    if latest.empty: latest=uni
    merge=pos.merge(latest,on="ticker",how="left",suffixes=("_pos",""))
    alerts=[]
    for _,r in merge.iterrows():
        try: sell_hi=int(str(r["sell_atr"]).split("~")[1])
        except: continue
        try: avg=int(float(r["avg_cost"]))
        except: continue
        if avg<sell_hi:
            name=r.get("name_pos") or r.get("name") or ""
            alerts.append(f"{r['ticker']} {name} 매도 후보: 목표 {sell_hi:,}원 | 평단 {avg:,}원")
    if alerts: send_telegram("[보유종목 매도 시그널]\n"+"\n".join(alerts))

def main():
    try:
        ref=yesterday_trading_date()
        log(f"[INFO] 기준일 {ref}")
        uni=build_universe(ref); rows=[]
        for t in uni.index:
            lv=calc_levels(t,ref)
            if lv: rows.append(lv)
        write_universe_and_top10(rows,ref)
        check_positions_and_alert(ref)
        log("[SUCCESS] 완료")
    except Exception as e:
        err="".join(traceback.format_exception_only(type(e),e)).strip()
        send_telegram(f"[bot 오류]\n{err}")
        sys.exit(1)

if __name__=="__main__":
    main()
