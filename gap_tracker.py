# -*- coding: utf-8 -*-
"""
gap_tracker.py — 목표주가 괴리율 트래커 (1주차 뼈대)

사용법 (터미널에서):
  1) 준비물 설치 (딱 한 번만):
     pip install requests beautifulsoup4 pykrx

  2) 1단계 — 페이지 구조 확인 (코드를 짜기 전에 반드시!):
     python gap_tracker.py --inspect
     → inspect_page.html 파일이 생김. 브라우저로 열어서
       종목명/목표가가 어떤 태그에 있는지 눈으로 확인.

  3) 2단계 — 실제 수집:
     python gap_tracker.py --crawl
     → data.json 이 생성됨 (최근 90일 컨센서스, 괴리율 내림차순 전체 종목)

설계 원칙:
  - koica_ingest.py 와 같은 패턴: inspect 모드로 먼저 구조 확인 → 수집
  - 서버 없음. 결과물은 data.json 하나 (나중에 HTML이 fetch로 읽음)
  - 요청 간격 준수(2초) + 실패해도 죽지 않는 방어적 파싱
  - 목록은 최근 90일치까지만 훑고(작성일 기준 조기 종료), 이미 처리한
    리포트는 reports_history.json에 캐시해서 다음 실행 때 다시 긁지 않음
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

KST = ZoneInfo("Asia/Seoul")

# ──────────────────────────────────────────────────────────────
# 0. 설정값 — 나중에 바꿀 일이 있으면 여기만 건드리면 됨
# ──────────────────────────────────────────────────────────────

# 네이버 금융 > 리서치 > 종목분석 리포트 목록 페이지
LIST_URL = "https://finance.naver.com/research/company_list.naver"

# 브라우저인 척하는 헤더 (없으면 차단당하는 경우가 많음)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY_SEC = 2      # 요청 사이 대기 시간 (예의 + 차단 방지)
MAX_PAGES = 60             # 목록 몇 페이지까지 훑을지 (1페이지 ≈ 하루치, 60페이지 ≈ 2~3개월)
CONSENSUS_WINDOW_DAYS = 90  # 컨센서스에 포함할 리포트 작성일 범위(일). 이보다 오래되면 수집 중단
# TOP_N: 더 이상 사용하지 않음 — 목표가+현재가 확보된 종목 전체를 저장한다.
OUTPUT_JSON = "data.json"
HISTORY_JSON = "reports_history.json"  # nid → 상세페이지 처리 결과 캐시 (재수집 방지)


# ──────────────────────────────────────────────────────────────
# 1. --inspect 모드: 페이지 원본을 파일로 저장해서 구조 확인
# ──────────────────────────────────────────────────────────────

def inspect_page():
    """목록 페이지 HTML을 통째로 저장한다.
    → 브라우저/에디터로 열어서 실제 태그 구조를 확인하는 용도.
    셀렉터(어떤 태그를 집을지)는 '추측'이 아니라 '확인' 후에 확정한다."""
    print(f"[inspect] {LIST_URL} 요청 중...")
    resp = requests.get(LIST_URL, headers=HEADERS, timeout=10)
    resp.encoding = "euc-kr"  # 네이버 금융 구페이지는 euc-kr 인코딩인 경우가 많음
    print(f"[inspect] 응답 코드: {resp.status_code}")

    with open("inspect_page.html", "w", encoding="utf-8") as f:
        f.write(resp.text)
    print("[inspect] inspect_page.html 저장 완료.")
    print("          → 파일을 열어 <table> 구조와 컬럼(종목명/제목/증권사/날짜)을 확인할 것.")
    print("          → 확인한 구조가 아래 parse_list_page()의 가정과 다르면 그 부분만 수정.")


# ──────────────────────────────────────────────────────────────
# 2. 목록 페이지 파싱: 리포트 한 건 = dict 하나
# ──────────────────────────────────────────────────────────────

def parse_report_date(date_str):
    """네이버 리서치 목록의 작성일 문자열("26.07.09")을 datetime으로 변환.
    2000년대(20YY)로 해석. 형식이 다르면 None 반환 — 호출부에서 방어적으로 처리."""
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", (date_str or "").strip())
    if not m:
        return None
    yy, mm, dd = (int(x) for x in m.groups())
    try:
        return datetime(2000 + yy, mm, dd)
    except ValueError:
        return None


def parse_list_page(html):
    """목록 페이지 HTML에서 리포트 행들을 추출한다.

    ★ 주의: 아래 셀렉터는 '일반적인 구조 가정'이다.
      --inspect 로 저장한 실제 HTML을 보고 태그/클래스명이 다르면 여기를 고친다.
      (사이트 개편 시 제일 먼저 깨지는 부분도 여기다)

    가정하는 구조:
      <table> 안에 행(tr)들이 있고, 각 행의 셀(td)이
      [종목명(링크에 code= 포함), 리포트제목, 증권사, 첨부, 작성일, 조회수] 순서
    """
    soup = BeautifulSoup(html, "html.parser")
    reports = []

    for tr in soup.select("table tr"):
        tds = tr.select("td")
        if len(tds) < 5:          # 헤더 행이나 구분선 행은 건너뜀
            continue

        # (1) 종목명 + 종목코드: 첫 번째 셀의 링크에서 추출
        link = tds[0].select_one("a")
        if link is None:
            continue
        name = link.get_text(strip=True)
        href = link.get("href", "")
        m = re.search(r"code=(\d{6})", href)   # URL 안의 6자리 종목코드
        if not m:
            continue
        code = m.group(1)

        # (2) 리포트 제목 / 증권사 / 작성일
        title = tds[1].get_text(strip=True)
        broker = tds[2].get_text(strip=True)
        date = tds[4].get_text(strip=True)

        # 목표주가는 목록에 없고 상세 페이지에 있음 → 링크만 우선 저장
        detail_link = tds[1].select_one("a")
        detail_href = detail_link.get("href", "") if detail_link else ""
        # nid(리포트 고유번호): reports_history.json 캐시 키로 사용
        nid_m = re.search(r"nid=(\d+)", detail_href)
        nid = nid_m.group(1) if nid_m else None

        reports.append({
            "code": code,
            "name": name,
            "title": title,
            "broker": broker,
            "date": date,
            "detail_href": detail_href,
            "nid": nid,
        })

    return reports


# ──────────────────────────────────────────────────────────────
# 3. 상세 페이지에서 목표주가 추출
# ──────────────────────────────────────────────────────────────

def fetch_target_price(detail_href):
    """리포트 상세 페이지에서 '목표주가 XXX,XXX원' 패턴을 찾는다.

    전략: 특정 태그에 의존하지 않고, 페이지 전체 텍스트에서
    정규식으로 숫자를 뽑는다. (태그 구조가 바뀌어도 살아남을 확률↑)
    실패하면 None 반환 — 죽지 않고 그 리포트만 건너뛴다."""
    if not detail_href:
        return None
    url = urljoin("https://finance.naver.com/research/", detail_href)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.encoding = "euc-kr"
        text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)

        # "목표주가 85,000" / "목표가: 85,000원" 등 다양한 표기를 커버
        m = re.search(r"목표\s*주?가[^\d]{0,10}([\d,]{4,})", text)
        if m:
            return int(m.group(1).replace(",", ""))
    except requests.RequestException as e:
        print(f"  [warn] 상세 페이지 실패: {e}")
    return None


# ──────────────────────────────────────────────────────────────
# 4. 현재가 조회 (PyKrx) + 괴리율 계산
# ──────────────────────────────────────────────────────────────

def fetch_current_prices(codes):
    """종목코드 리스트 → {코드: 현재가} dict.
    PyKrx는 '가장 최근 거래일' 종가를 준다. (장중 실시간 아님 — MVP에는 충분)"""
    from pykrx import stock  # 여기서 import: pykrx 없이도 --inspect는 돌게 하기 위함

    today = datetime.now(KST).strftime("%Y%m%d")
    week_ago = (datetime.now(KST) - timedelta(days=7)).strftime("%Y%m%d")
    prices = {}
    for code in codes:
        try:
            df = stock.get_market_ohlcv_by_date(today, today, code)
            if df.empty:
                # 오늘이 휴장일이면 최근 7일 범위로 다시 시도
                df = stock.get_market_ohlcv_by_date(week_ago, today, code)
            if not df.empty:
                prices[code] = int(df["종가"].iloc[-1])
        except Exception as e:
            print(f"  [warn] {code} 현재가 조회 실패: {e}")
        time.sleep(0.3)  # KRX 서버에도 예의를 지키자
    return prices


def build_dataset(reports, prices):
    """리포트 + 현재가 → 종목별 집계 + 괴리율 계산.

    같은 종목에 여러 리포트가 있어도, 같은 증권사가 낸 리포트는
    작성일이 가장 최신인 1건만 목표가 계산에 반영한다.
    (컨센서스는 "증권사별 최신 의견"의 평균이지, 리포트 발행 횟수가 아니다)
      - 목표주가는 증권사별 최신 리포트들의 평균/최고/최저로 집계
      - report_count는 중복 제거 후의 증권사 수
    """
    by_stock = {}
    for r in reports:
        if r.get("target_price") is None:
            continue
        s = by_stock.setdefault(r["code"], {
            "code": r["code"], "name": r["name"],
            "by_broker": {},  # 증권사명 → 최신 리포트 dict
        })

        broker = r["broker"]
        r_date = parse_report_date(r["date"])
        existing = s["by_broker"].get(broker)
        existing_date = existing["_date"] if existing else None

        # 기존 것이 없거나, 새 리포트의 작성일을 알 수 있고 더 최신이면 교체.
        # 날짜를 못 읽으면(파싱 실패) 먼저 들어온 것을 그대로 유지한다.
        if existing is None or (r_date is not None and (existing_date is None or r_date > existing_date)):
            s["by_broker"][broker] = {
                "title": r["title"], "broker": broker,
                "date": r["date"], "target_price": r["target_price"],
                "_date": r_date,
            }

    rows = []
    for code, s in by_stock.items():
        cur = prices.get(code)
        if cur is None or cur == 0:
            continue
        latest_reports = [
            {k: v for k, v in rep.items() if k != "_date"}
            for rep in s["by_broker"].values()
        ]
        targets = [rep["target_price"] for rep in latest_reports]
        avg_target = sum(targets) / len(targets)
        report_count = len(latest_reports)  # 중복 제거 후 증권사 수
        gap_pct = round((avg_target - cur) / cur * 100, 1)  # ★ 괴리율

        # 이상치 의심 플래그: 종목은 제외하지 않고 표시만 한다 (판단은 프론트에서).
        # - 괴리율이 비정상적으로 크거나(150% 초과) 작음(-50% 미만)
        # - 리포트가 1건뿐인데 괴리율이 100%를 넘음 (표본 부족 + 극단값 조합)
        suspect = (
            gap_pct > 150 or gap_pct < -50
            or (report_count == 1 and gap_pct > 100)
        )

        rows.append({
            "code": code,
            "name": s["name"],
            "current_price": cur,
            "target_avg": round(avg_target),
            "target_max": max(targets),
            "target_min": min(targets),
            "gap_pct": gap_pct,
            "report_count": report_count,
            "reports": latest_reports,
            "suspect": suspect,
        })

    rows.sort(key=lambda x: x["gap_pct"], reverse=True)  # 괴리율 큰 순
    return rows


# ──────────────────────────────────────────────────────────────
# 5. 리포트 캐시(reports_history.json): nid 기준으로 중복 상세요청 방지
# ──────────────────────────────────────────────────────────────

def load_history():
    """{nid: {code, name, title, broker, date, target_price}} 형태의 캐시를 읽는다.
    파일이 없거나 깨졌으면 빈 캐시로 시작 — 첫 실행/오류 상황도 죽지 않게."""
    try:
        with open(HISTORY_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_history(history):
    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────────────────
# 6. --crawl 모드: 전체 파이프라인 실행
# ──────────────────────────────────────────────────────────────

def crawl():
    # (1) 목록 페이지 수집 — 작성일이 CONSENSUS_WINDOW_DAYS보다 오래되면 중단
    # parse_report_date()는 시간대 정보 없는(naive) datetime을 반환하므로
    # (작성일 문자열에 시간이 없음) cutoff_date도 naive로 맞춘다 — KST 벽시계 기준.
    cutoff_date = datetime.now(KST).replace(tzinfo=None) - timedelta(days=CONSENSUS_WINDOW_DAYS)
    all_reports = []
    for page in range(1, MAX_PAGES + 1):
        print(f"[crawl] 목록 {page}페이지 요청...")
        resp = requests.get(LIST_URL, params={"page": page},
                            headers=HEADERS, timeout=10)
        resp.encoding = "euc-kr"
        found = parse_list_page(resp.text)
        print(f"[crawl]   → 리포트 {len(found)}건 발견")

        page_reports = []
        hit_cutoff = False
        for r in found:
            r_date = parse_report_date(r["date"])
            if r_date is not None and r_date < cutoff_date:
                hit_cutoff = True
                break
            page_reports.append(r)
        all_reports.extend(page_reports)

        if hit_cutoff:
            print(f"[crawl]   → 작성일이 최근 {CONSENSUS_WINDOW_DAYS}일보다 오래됨 → 목록 수집 중단")
            break

        time.sleep(REQUEST_DELAY_SEC)

    if not all_reports:
        print("[error] 리포트를 하나도 못 찾음.")
        print("        → --inspect 로 HTML을 저장해 parse_list_page()의 셀렉터를 실제 구조에 맞게 수정할 것.")
        sys.exit(1)

    # (2) 각 리포트 상세에서 목표주가 추출 — nid가 캐시에 있으면 재사용, 없으면만 네트워크 요청
    history = load_history()
    cached_count = 0
    fetched_count = 0
    print(f"[crawl] 상세 페이지 처리: 전체 {len(all_reports)}건 "
          f"(캐시 파일: {HISTORY_JSON})")
    for i, r in enumerate(all_reports, 1):
        nid = r.get("nid")
        if nid and nid in history:
            r["target_price"] = history[nid]["target_price"]
            cached_count += 1
            continue

        r["target_price"] = fetch_target_price(r["detail_href"])
        fetched_count += 1
        status = f"{r['target_price']:,}원" if r["target_price"] else "추출 실패(건너뜀)"
        print(f"  ({i}/{len(all_reports)}) {r['name']}: {status}")
        if nid:
            history[nid] = {
                "code": r["code"], "name": r["name"], "title": r["title"],
                "broker": r["broker"], "date": r["date"],
                "target_price": r["target_price"],
            }
        time.sleep(REQUEST_DELAY_SEC)

    save_history(history)
    print(f"[crawl]   → 신규 요청 {fetched_count}건, 캐시 재사용 {cached_count}건")

    # (3) 현재가 조회
    codes = sorted({r["code"] for r in all_reports if r.get("target_price")})
    print(f"[crawl] 현재가 조회: {len(codes)}종목...")
    prices = fetch_current_prices(codes)

    # (4) 괴리율 계산 + 저장
    rows = build_dataset(all_reports, prices)
    output = {
        "updated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        "count": len(rows),
        "rows": rows,
    }
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n[done] {OUTPUT_JSON} 저장 완료 ({len(rows)}종목)")
    if rows:
        print("      괴리율 TOP 5 미리보기:")
        for r in rows[:5]:
            print(f"        {r['name']:<12} 현재가 {r['current_price']:>10,}  "
                  f"목표평균 {r['target_avg']:>10,}  괴리율 {r['gap_pct']:>6}%  "
                  f"(리포트 {r['report_count']}건)")

    suspect_count = sum(1 for r in rows if r["suspect"])
    print(f"[summary] 리포트 {len(all_reports)}건 → 목표가 확보 {len(codes)}종목 → "
          f"가격 확보 {len(prices)}종목 → 저장 {len(rows)}종목 (검증필요 {suspect_count}종목)")


# ──────────────────────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="목표주가 괴리율 트래커")
    parser.add_argument("--inspect", action="store_true",
                        help="목록 페이지 HTML을 저장해 구조 확인")
    parser.add_argument("--crawl", action="store_true",
                        help="수집 → 괴리율 계산 → data.json 생성")
    args = parser.parse_args()

    if args.inspect:
        inspect_page()
    elif args.crawl:
        crawl()
    else:
        parser.print_help()