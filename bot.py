# bot.py (모바일 보기 최적화 + 안정 로그)
# -*- coding: utf-8 -*-

import os, json, sys, traceback
from datetime import datetime, timedelta, date
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

MAX_PRICE = int(os.getenv("MAX_PRICE", "150000"))                 # 종가 ≤ 15만원
TOP_N = int(os.getenv("TOP_N", "200"))                            # 시총 상위 N
MIN_TRADING_VALUE = int(os.getenv("MIN_TRADING_VALUE", "5000000000"))  # 20일 평균 거래대금 하한(50억)
ATR_N = int(os.getenv("ATR_N", "20"))
EMA_N = int(os.getenv("EMA_N", "20"))
PRICE_BONUS = int(os.getenv("PRICE_BONUS", "100000"))             # 10만원 이하 가점

# ===== 시트명/헤더 =====
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


# ===== 공용 유틸 =====
def log(msg: str):
    print(msg, flush=True)

def is_url(s: str) -> bool:
    s = s.lower()
    return s.startswith("http://") or s.startswith("https://")

def _rank_emoji(i: int) -> str:
    table = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    return table[i-1] if 1 <= i <= 10 else f"{i}."

def _fmt_won(x) -> str:
    try:
        return f"{int(float(x)):,}원"
    except Exception:
        return str(x)

def make_top10_mobile_message(out: pd.DataFrame, ref_date: date) -> str:
    """모바일에서 보기 좋은 카드형 리스트 메시지"""
    header = f"📊 KOSPI Top10 ( {ref_date.strftime('%Y-%m-%d')} )"
    lines = [header, ""]
    for _, r in out.iterrows():
        rank = int(r["rank"])
        name = str(r["name"])
        close = _fmt_won(r["close"])
        buy_atr = str(r["buy_atr"])
        sell_atr = str(r["sell_atr"])
        stop = _fmt_won(r["stop"])
        # 종목당 4줄 + 공백
        lines.append(f"{_rank_emoji(rank)} {name} ({close})")
        lines.append(f"매수 {buy_atr}")
        lines.append(f"매도 {sell_atr}")
        lines.append(f"손절 {stop}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[WARN] Telegram 설정이 없어 전송 생략")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        if len(text) > 3800:
            text = text[:3800] + "\n...(truncated)"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        log(f"[DEBUG] Telegram POST -> payload={payload}")
        r = requests.post(url, json=payload, timeout=30)
        log(f"[DEBUG] Telegram resp {r.status_code}: {r.text}")
        if r.status_code != 200:
            raise RuntimeError(f"Telegram 전송 실패: {r.status_code} {r.text}")
    except Exception as e:
        log(f"[ERROR] Telegram 예외: {e}")
        # 실패 이유를 워크플로 로그에서 바로 볼 수 있도록 예외 전파
        raise


# ===== Google Sheets =====
def sheet_client():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json_raw = os.getenv("GCP_SA_JSON", "").strip()
    if not sa_json_raw:
        raise RuntimeError("GCP_SA_JSON이 비어 있습니다. 서비스계정 JSON 전체를 Secrets에 붙여넣으세요.")
    try:
        sa_json = json.loads(sa_json_raw)
    except Exception as e:
        raise RuntimeError("GCP_SA_JSON 파싱 실패(유효한 JSON 아님).") from e
    log(f"[DEBUG] Service Account: {sa_json.get('client_email')}")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    return gspread.authorize(creds)

def open_spreadsheet(gc):
    try:
        if is_url(SHEET_ID_OR_URL):
            log("[INFO] open_by_url 사용")
            sh = gc.open_by_url(SHEET_ID_OR_URL)
        else:
            log("[INFO] open_by_key 사용")
            sh = gc.open_by_key(SHEET_ID_OR_URL)
        log("[STEP] 스프레드시트 제목: " + sh.title)
        return sh
    except SpreadsheetNotFound as e:
        hint = (
            "SpreadsheetNotFound(404)\n"
            "- SHEET_ID가 문서 ID인지 확인(전체 URL이 아니라면 ID만)\n"
            "- 또는 SHEET_ID에 URL 전체를 넣으면 open_by_url로 열립니다\n"
            "- 스프레드시트에서 서비스계정 이메일을 '편집자'로 공유했는지 확인\n"
            "- 조직(워크스페이스) 공유제한이 있으면 개인 GDrive로 테스트\n"
        )
        raise RuntimeError(hint) from e
    except APIError as e:
        raise RuntimeError(f"Google API Error: {e}") from e

def ensure_worksheet(sh, title, headers):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        log(f"[INFO] 워크시트 '{title}' 없음 → 새로 생성")
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(headers)))
        if headers:
            ws.update([headers])
        return ws


# ===== 데이터/전략 =====
def yesterday_trading_date():
    """최근 7일 내 가장 가까운 영업일"""
    today = datetime.now()
    for i in range(1, 8):
        d = today - timedelta(days=i)
        try:
            df = stock.get_market_ohlcv_by_date(d.strftime("%Y%m%d"), d.strftime("%Y%m%d"), "005930")
            if df is not None and len(df) > 0:
                return d.date()
        except Exception:
            pass
    return (today - timedelta(days=1)).date()

def build_universe(ref_date: date):
    """KOSPI 시총 상위 + 가격 ≤ MAX_PRICE + 유동성 필터"""
    ymd = ref_date.strftime("%Y%m%d")
    cap = stock.get_market_cap_by_ticker(ymd, market="KOSPI")
    cap = cap.sort_values("시가총액", ascending=False).head(max(TOP_N*2, 300))
    cap = cap[cap["종가"] <= MAX_PRICE]

    # 유동성(20일 평균 거래대금) 필터
    start = ref_date - timedelta(days=90)
    ok = []
    for t in cap.index.tolist():
        try:
            df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, t)
            if df is None or len(df) < 25:
                continue
            avg_val = df["거래대금"].tail(20).mean() if "거래대금" in df.columns else (df["거래량"].tail(20) * df["종가"].tail(20)).mean()
            if avg_val and avg_val >= MIN_TRADING_VALUE:
                ok.append(t)
        except Exception:
            continue

    uni = cap.loc[cap.index.intersection(ok)].sort_values("시가총액", ascending=False).head(TOP_N)
    return uni

def calc_levels(tkr: str, ref_date: date):
    """전일 H/L/C 기반 Pivot, 20일 EMA/ATR 기반 밴드 + 간단 스코어"""
    ymd = ref_date.strftime("%Y%m%d")
    start = ref_date - timedelta(days=150)
    df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), ymd, tkr)
    if df is None or len(df) < EMA_N + 1:
        return None

    h, l, c = df["고가"].iloc[-1], df["저가"].iloc[-1], df["종가"].iloc[-1]

    # Pivot
    pp = (h + l + c) / 3
    s1, r1 = 2*pp - h, 2*pp - l
    s2, r2 = pp - (h - l), pp + (h - l)

    # ATR
    high, low, close = df["고가"], df["저가"], df["종가"]
    prev = close.shift(1)
    tr = np.maximum(high - low, np.maximum(abs(high - prev), abs(low - prev)))
    atr = tr.rolling(ATR_N).mean().iloc[-1]

    # EMA
    ema = close.ewm(span=EMA_N).mean().iloc[-1]

    # ATR 밴드
    atr_buy_lo, atr_buy_hi = ema - 1.0*atr, ema - 0.5*atr
    atr_sell_lo, atr_sell_hi = ema + 0.5*atr, ema + 1.0*atr

    # 손절(보수)
    stop = min(s2, ema - 1.5*atr)

    name = stock.get_market_ticker_name(tkr)

    in_atr = (c >= atr_buy_lo and c <= atr_buy_hi)
    in_pivot = (c >= s2 and c <= s1)
    score = (1.0 if (in_atr and in_pivot) else 0.5 if (in_atr or in_pivot) else 0.0)
    if c <= PRICE_BONUS:
        score += 0.3

    return {
        "ticker": tkr, "name": name, "close": int(c),
        "buy_pivot": f"{int(s2)}~{int(s1)}", "sell_pivot": f"{int(r1)}~{int(r2)}",
        "buy_atr": f"{int(atr_buy_lo)}~{int(atr_buy_hi)}", "sell_atr": f"{int(atr_sell_lo)}~{int(atr_sell_hi)}",
        "stop": int(stop), "atr": float(atr), "ema": float(ema),
        "score": round(float(score), 4), "in_atr_buy": bool(in_atr), "in_pivot_buy": bool(in_pivot)
    }


# ===== 시트 업데이트 & 알림 =====
def write_universe_and_top10(rows: list, ref: date):
    log("[STEP] Google Sheets 연결 시작")
    gc = sheet_client()
    sh = open_spreadsheet(gc)

    log("[STEP] 워크시트 확인/생성")
    uni_ws = ensure_worksheet(sh, SHEET_UNIVERSE, UNIVERSE_HEADERS)
    top_ws = ensure_worksheet(sh, SHEET_TOP10, TOP10_HEADERS)
    ensure_worksheet(sh, SHEET_POSITIONS, POSITIONS_HEADERS)

    log("[STEP] universe 시트 업데이트")
    df = pd.DataFrame(rows)
    df.insert(0, "date", ref.strftime("%Y-%m-%d"))
    uni_ws.clear()
    uni_ws.update([df.columns.tolist()] + df.values.tolist())

    log("[STEP] top10 시트 업데이트 및 텔레그램 발송")
    top = df.sort_values(["score", "close"], ascending=[False, False]).head(10).reset_index(drop=True)
    out = top[["ticker","name","close","buy_atr","sell_atr","buy_pivot","sell_pivot","stop","score"]].copy()
    out.insert(0, "rank", range(1, len(out)+1))
    top_ws.clear()
    top_ws.update([out.columns.tolist()] + out.values.tolist())

    # 모바일 친화 메시지로 발송
    msg = make_top10_mobile_message(out, ref)
    send_telegram(msg)

    log("[STEP] write_universe_and_top10 완료")

def check_positions_and_alert(ref: date):
    log("[STEP] positions 체크 시작")
    gc = sheet_client()
    sh = open_spreadsheet(gc)

    try:
        pos_ws = sh.worksheet(SHEET_POSITIONS)
    except WorksheetNotFound:
        log(f"[INFO] '{SHEET_POSITIONS}' 시트 없음 → 생성 후 스킵")
        ensure_worksheet(sh, SHEET_POSITIONS, POSITIONS_HEADERS)
        return

    pos = pd.DataFrame(pos_ws.get_all_records())
    if pos.empty:
        log("[INFO] positions 비어있음 → 스킵")
        return

    uni = pd.DataFrame(sh.worksheet(SHEET_UNIVERSE).get_all_records())
    latest = uni[uni["date"] == ref.strftime("%Y-%m-%d")]
    if latest.empty:
        latest = uni

    merge = pos.merge(latest, on="ticker", how="left", suffixes=("_pos",""))

    alerts = []
    for _, r in merge.iterrows():
        sell_atr = r.get("sell_atr")
        avg_cost = r.get("avg_cost")
        try:
            sell_hi = int(str(sell_atr).split("~")[1])
        except Exception:
            continue
        try:
            avg_cost_int = int(float(avg_cost)) if str(avg_cost).strip() != "" else None
        except Exception:
            avg_cost_int = None
        if avg_cost_int is None:
            continue
        if avg_cost_int < sell_hi:
            name = r.get("name_pos") or r.get("name") or ""
            alerts.append(
                f"{r['ticker']} {name} 매도 후보: 목표(ATR 상단) {sell_hi:,}원 | 평단 {avg_cost_int:,}원"
            )

    if alerts:
        send_telegram("[보유종목 매도 시그널]\n" + "\n".join(alerts))

    log("[STEP] positions 체크 완료")


# ===== 메인 =====
def main():
    try:
        ref = yesterday_trading_date()
        log(f"[INFO] 기준일 {ref}")

        log("[STEP] 유니버스 구성")
        uni = build_universe(ref)
        if uni is None or uni.empty:
            raise RuntimeError("유니버스가 비었습니다. (필터 과严/데이터 실패/휴장일)")
        log(f"[INFO] 유니버스 종목수: {len(uni)}")

        log("[STEP] 레벨 계산")
        rows = []
        for t in uni.index:
            lv = calc_levels(t, ref)
            if lv:
                rows.append(lv)
        if not rows:
            raise RuntimeError("레벨 계산 결과가 비었습니다.")

        write_universe_and_top10(rows, ref)
        check_positions_and_alert(ref)

        log("[SUCCESS] 작업 완료")

    except Exception:
        tb = traceback.format_exc()
        log("[FATAL]\n" + tb)
        # 에러도 텔레그램으로 보냄(토큰/채팅 설정이 맞다면 수신 가능)
        try:
            send_telegram("[bot 오류]\n" + tb)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
