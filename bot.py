# bot.py
# -*- coding: utf-8 -*-
"""
KOSPI 시총 Top200(가격 ≤ 15만원) 우량주 대상
- 추천 매수가/매도가(피벗 + ATR 밴드) 계산
- 구글 스프레드시트 업데이트(universe, top10_today)
- 보유표(positions) 조회해 매도 시점 도래 후보 텔레그램 알림
- GitHub Actions(크론+workflow_dispatch)에서 매일 실행하기 적합

필수 Secrets:
- SHEET_ID         : 스프레드시트 ID (또는 URL 전체 넣어도 자동 판별)
- GCP_SA_JSON      : Google 서비스계정 JSON "내용 전체" (문자열)
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

선택 환경변수:
- MAX_PRICE=150000, TOP_N=200, MIN_TRADING_VALUE=5000000000, ATR_N=20, EMA_N=20, PRICE_BONUS=100000
"""

import os, json, sys, traceback
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests

# 외부 라이브러리
from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound, APIError

# ===== 환경값 =====
SHEET_ID_OR_URL = os.getenv("SHEET_ID", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MAX_PRICE = int(os.getenv("MAX_PRICE", "150000"))             # 15만원 이하
TOP_N = int(os.getenv("TOP_N", "200"))                        # 시총 상위 N
MIN_TRADING_VALUE = int(os.getenv("MIN_TRADING_VALUE", "5000000000"))  # 20일 평균 거래대금 하한(50억)
ATR_N = int(os.getenv("ATR_N", "20"))
EMA_N = int(os.getenv("EMA_N", "20"))
PRICE_BONUS = int(os.getenv("PRICE_BONUS", "100000"))         # 10만원 이하 가점 기준

# ===== 기본 시트명/헤더 =====
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


# ===== 유틸 =====
def log(msg):
    print(msg, flush=True)

def is_url(s: str) -> bool:
    s = s.lower()
    return s.startswith("http://") or s.startswith("https://")

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[WARN] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 미설정. 텔레그램 전송 생략.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
        if r.status_code != 200:
            log(f"[ERROR] Telegram 전송 실패: {r.status_code} {r.text}")
    except Exception as e:
        log(f"[ERROR] Telegram 예외: {e}")


# ===== Google Sheets =====
def sheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_json_raw = os.getenv("GCP_SA_JSON", "").strip()
    if not sa_json_raw:
        raise RuntimeError("GCP_SA_JSON이 비어 있습니다. 서비스계정 JSON 내용을 Secrets에 전체 붙여넣으세요.")
    try:
        sa_json = json.loads(sa_json_raw)
    except Exception as e:
        raise RuntimeError("GCP_SA_JSON 파싱 실패. JSON 전체를 그대로 붙여넣었는지 확인하세요.") from e

    log(f"[DEBUG] Service Account: {sa_json.get('client_email')}")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    if not SHEET_ID_OR_URL:
        raise RuntimeError("SHEET_ID가 비어 있습니다. 스프레드시트 ID 또는 URL 전체를 입력하세요.")

    try:
        if is_url(SHEET_ID_OR_URL):
            log("[INFO] SHEET_ID가 URL로 감지되어 open_by_url 사용")
            sh = gc.open_by_url(SHEET_ID_OR_URL)
        else:
            log("[INFO] SHEET_ID가 ID로 감지되어 open_by_key 사용")
            sh = g
