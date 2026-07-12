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

  ※ 한경컨센서스(2번째 데이터 소스) 구조 확인:
     python gap_tracker.py --inspect-hk
     → inspect_hankyung.html 파일이 생김 (실수집과 동일한 기업(CO) 뷰).
       parse_hankyung_list()의 가정과 실제 구조가 다르면 여기부터 볼 것.

설계 원칙:
  - koica_ingest.py 와 같은 패턴: inspect 모드로 먼저 구조 확인 → 수집
  - 서버 없음. 결과물은 data.json 하나 (나중에 HTML이 fetch로 읽음)
  - 요청 간격 준수(2초) + 실패해도 죽지 않는 방어적 파싱
  - 목록은 최근 90일치까지만 훑고(작성일 기준 조기 종료), 이미 처리한
    리포트는 reports_history.json에 캐시해서 다음 실행 때 다시 긁지 않음
  - 2번째 소스(한경컨센서스)는 Stage 1: 목록 메타데이터만 수집해서
    네이버가 못 보는 (종목, 증권사) 커버리지를 측정한다. 그 수치를 보고
    Stage 2(한경 목표가를 컨센서스 계산에 반영) 진행 여부를 결정.
    CO 목록에 '적정가격' 컬럼이 있어서 Stage 2에 PDF 파싱은 필요 없다 —
    listed_target_price로 이미 캐시까지 되고 있으므로 반영만 하면 됨.
    한경이 죽어도 네이버 파이프라인은 계속 돈다 — 백업 소스 원칙.
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

# 한경컨센서스(2번째 데이터 소스) > 리포트 목록 페이지
# 파라미터 구조 (--inspect-hk 저장 HTML + 실요청으로 확인 완료):
#   sdate/edate=YYYY-MM-DD, report_type=CO(기업), pagenum=행 수(20/50/80),
#   now_page=페이지 번호.
# ※ report_type=CO 응답은 분류 없는 기본 목록과 컬럼이 다르다:
#   [작성일, 제목, 적정가격, 투자의견, 작성자, 제공출처, 기업정보, 차트, 첨부]
#   → 적정가격(목표가)이 목록에 바로 노출됨. Stage 1에서는 이 값을
#   listed_target_price로 보존만 하고 컨센서스 계산에는 쓰지 않는다.
HK_LIST_URL = "http://consensus.hankyung.com/analysis/list"
HK_PAGENUM = 80    # 페이지당 행 수 — 사이트가 지원하는 최대값(20/50/80)
HK_MAX_PAGES = 20  # 안전상한 (80행 × 20페이지 = 1,600건 — 90일치엔 충분할 것)

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
# 1-b. --inspect-hk 모드: 한경컨센서스 목록 페이지 구조 확인
# ──────────────────────────────────────────────────────────────

def inspect_hankyung():
    """한경컨센서스 기업(CO) 목록 페이지 HTML을 통째로 저장한다.
    네이버와 동일한 원칙: 파서를 짜기 전에 실제 구조부터 눈으로 확인.
    ※ 실제 수집(crawl_hankyung)과 같은 CO 파라미터로 요청한다 —
      CO 뷰는 기본 목록과 컬럼 구성이 다르므로, 저장 파일이
      parse_hankyung_list()가 보는 구조와 일치해야 의미가 있다."""
    params = {
        "sdate": (datetime.now(KST) - timedelta(days=7)).strftime("%Y-%m-%d"),
        "edate": datetime.now(KST).strftime("%Y-%m-%d"),
        "report_type": "CO", "pagenum": 20, "now_page": 1,
    }
    print(f"[inspect-hk] {HK_LIST_URL} 요청 중... (기업 리포트, 최근 7일)")
    try:
        resp = requests.get(HK_LIST_URL, params=params, headers=HEADERS, timeout=10)
    except requests.RequestException as e:
        print(f"[inspect-hk] 요청 자체가 실패함: {e}")
        print("             → 네트워크 상태, 또는 사이트가 https로 바뀌었는지 확인할 것.")
        sys.exit(1)

    print(f"[inspect-hk] 응답 코드: {resp.status_code} (최종 URL: {resp.url})")

    if resp.status_code != 200:
        print(f"[inspect-hk] 접근 실패 (HTTP {resp.status_code}) — 차단(403 등) 또는 경로 변경 가능성.")
        print("             → 브라우저로 직접 열어 보고, User-Agent/Referer 요구 여부를 확인할 것.")
        sys.exit(1)

    # 인코딩 판단: 응답 헤더에 charset이 있으면 그대로 쓰고,
    # 없으면 requests가 ISO-8859-1로 잘못 가정하므로 본문 기반 추정
    # (apparent_encoding)으로 교체한다. → utf-8 / euc-kr 어느 쪽이든 대응.
    header_enc = resp.encoding
    if header_enc is None or header_enc.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding
    print(f"[inspect-hk] 인코딩: 헤더 선언={header_enc}, 본문 추정={resp.apparent_encoding} → {resp.encoding} 사용")

    with open("inspect_hankyung.html", "w", encoding="utf-8") as f:
        f.write(resp.text)
    print("[inspect-hk] inspect_hankyung.html 저장 완료.")
    print("             → CO 뷰 컬럼: [작성일, 제목, 적정가격, 투자의견, 작성자,")
    print("               제공출처, 기업정보, 차트, 첨부파일] — 이 구조가 다르면")
    print("               parse_hankyung_list()의 셀렉터/인덱스를 실제에 맞게 수정할 것.")


# ──────────────────────────────────────────────────────────────
# 2. 목록 페이지 파싱: 리포트 한 건 = dict 하나
# ──────────────────────────────────────────────────────────────

def parse_report_date(date_str):
    """리포트 작성일 문자열을 datetime으로 변환.
    네이버("26.07.09")와 한경컨센서스("2026-07-10") 두 형식을 지원.
    네이버 쪽 2자리 연도는 2000년대(20YY)로 해석.
    형식이 다르면 None 반환 — 호출부에서 방어적으로 처리."""
    s = (date_str or "").strip()
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", s)
    if m:
        yy, mm, dd = (int(x) for x in m.groups())
        yy += 2000
    else:
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
        if not m:
            return None
        yy, mm, dd = (int(x) for x in m.groups())
    try:
        return datetime(yy, mm, dd)
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
            "source": "naver",
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
# 3-b. 한경컨센서스 수집 — 2번째 소스, Stage 1: 목록 메타데이터만
# ──────────────────────────────────────────────────────────────

def parse_hankyung_list(html):
    """한경컨센서스 기업(CO) 목록 HTML에서 리포트 행들을 추출한다.

    ★ 셀렉터 근거: report_type=CO 응답을 실제로 저장해서 확인한 구조.
      주의 — CO 뷰는 분류 없는 기본 목록과 컬럼 구성이 완전히 다르다!
      (--inspect-hk 도 같은 CO 파라미터로 저장하므로 파일과 파서가 1:1 대응)

      <div class="table_style01"> 안 <table> 행(tr)의 셀(td) 9개 순서 =
      [작성일(YYYY-MM-DD), 제목, 적정가격, 투자의견, 작성자, 제공출처,
       기업정보, 차트, 첨부파일]
      - 제목 형식: "팬오션(028670) Deep Value, ..." → 이름+6자리 코드
      - 제목 링크 href: /analysis/downpdf?report_idx=NNNNNN
        (PDF 직링크. 간혹 역슬래시 경로로 나와서 / 로 정규화한다)
      - 적정가격: "8,000" 형식, 의견 없는 리포트는 "0" → None 처리.
        Stage 1에서는 listed_target_price에 담아두기만 하고 컨센서스
        계산(target_price)에는 반영하지 않는다 — 커버리지 측정이 먼저.

    반환: (리포트 dict 리스트, 데이터 행 수)
      데이터 행 수는 페이징 종료 판단용 — 코드 필터로 걸러진 행도 센다.
      (0이면 마지막 페이지를 넘어간 것)
    """
    soup = BeautifulSoup(html, "html.parser")
    reports = []
    row_count = 0

    for tr in soup.select("div.table_style01 table tr"):
        tds = tr.select("td")
        if len(tds) < 9:            # 헤더 행이나 "데이터 없음" 행은 건너뜀
            continue
        row_count += 1

        link = tds[1].select_one("a")
        if link is None:
            continue
        title = link.get_text(strip=True)

        # 제목 맨 앞의 "종목명(6자리코드)" 추출. 코드가 없으면 개별 상장종목
        # 리포트가 아닌 것으로 보고 스킵 (비상장/테마성 리포트 등).
        m = re.match(r"^(.+?)\((\d{6})\)", title)
        if not m:
            continue

        href = link.get("href", "").replace("\\", "/")  # 역슬래시 경로 정규화
        idx_m = re.search(r"report_idx=(\d+)", href)
        if not idx_m:
            continue

        # 적정가격(목표가) 컬럼 — 의견 없는 리포트는 "0"으로 나온다
        raw_price = tds[2].get_text(strip=True).replace(",", "")
        listed_target = int(raw_price) if raw_price.isdigit() and int(raw_price) > 0 else None

        reports.append({
            "code": m.group(2),
            "name": m.group(1).strip(),
            "title": title,
            "broker": tds[5].get_text(strip=True),  # 제공출처(증권사)
            "date": tds[0].get_text(strip=True),    # "2026-07-10"
            "detail_href": href,
            # 캐시 키: 네이버 nid와 절대 겹치지 않도록 "hk:" 접두사를 붙인다
            "nid": f"hk:{idx_m.group(1)}",
            "target_price": None,                   # Stage 1: 컨센서스 계산에는 미사용
            "listed_target_price": listed_target,   # 목록의 '적정가격' — Stage 2 근거
            "source": "hankyung",
        })

    return reports, row_count


def crawl_hankyung(cutoff_date):
    """한경컨센서스 기업(CO) 리포트 목록을 수집한다 — Stage 1: 메타데이터만.
    (목록의 적정가격은 listed_target_price로 보존만 — 계산 반영은 Stage 2에서)

    백업 소스 원칙: 어떤 실패가 나도 경고만 내고 지금까지 모은 것
    (최악에는 빈 리스트)을 반환한다. 한경이 죽어도 네이버 파이프라인은
    계속 돌아야 한다."""
    sdate = cutoff_date.strftime("%Y-%m-%d")        # 90일 전 (KST)
    edate = datetime.now(KST).strftime("%Y-%m-%d")  # 오늘 (KST)
    reports = []
    seen = set()  # report_idx 중복 방지 (페이징 도중 새 글이 밀려 들어오는 경우)

    try:
        for page in range(1, HK_MAX_PAGES + 1):
            print(f"[crawl-hk] 한경 목록 {page}페이지 요청...")
            try:
                resp = requests.get(
                    HK_LIST_URL,
                    params={"sdate": sdate, "edate": edate, "report_type": "CO",
                            "pagenum": HK_PAGENUM, "now_page": page},
                    headers=HEADERS, timeout=10,
                )
            except requests.RequestException as e:
                print(f"  [warn] 한경 요청 실패: {e} — 여기까지 모은 {len(reports)}건만 사용")
                break

            if resp.status_code != 200:
                print(f"  [warn] 한경 응답 코드 {resp.status_code} — 수집 중단")
                break

            # 인코딩: charset 헤더가 없으면 본문 추정으로 교체 (inspect와 동일 원칙)
            if resp.encoding is None or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding

            found, row_count = parse_hankyung_list(resp.text)

            hit_cutoff = False
            new_count = 0
            for r in found:
                r_date = parse_report_date(r["date"])
                if r_date is not None and r_date < cutoff_date:
                    hit_cutoff = True   # 목록은 작성일 내림차순 → 이후 행은 더 오래됨
                    break
                if r["nid"] in seen:
                    continue
                seen.add(r["nid"])
                reports.append(r)
                new_count += 1
            print(f"[crawl-hk]   → 행 {row_count}개 중 기업 리포트 신규 {new_count}건")

            if hit_cutoff:
                print(f"[crawl-hk]   → 작성일이 최근 {CONSENSUS_WINDOW_DAYS}일보다 오래됨 → 수집 종료")
                break
            if row_count == 0:          # 마지막 페이지를 넘어감
                print("[crawl-hk]   → 더 이상 행이 없음 → 수집 종료")
                break
            if page > 1 and found and new_count == 0:
                # 페이지를 넘겼는데 전부 이미 본 리포트 → 페이징이 안 먹는 상황.
                # 같은 페이지를 상한까지 반복해서 긁는 낭비를 막는다.
                print("  [warn] 새 리포트가 없음(페이징 미동작 의심) → 수집 중단")
                break

            time.sleep(REQUEST_DELAY_SEC)
    except Exception as e:
        # 파싱 오류 등 예상 못한 문제 — 백업 소스가 메인을 죽이면 안 된다
        print(f"  [warn] 한경 수집 중 오류: {e} — 한경 수집분 없이 진행")
        return []

    return reports


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
    """리포트(네이버 + 한경) + 현재가 → 종목별 집계 + 괴리율 계산.

    증권사(broker) 단위 병합 규칙:
      - 같은 종목·같은 증권사는 소스가 달라도 1곳으로 센다.
        report_count = 양 소스 통합 기준 고유 증권사 수.
      - 같은 (종목, 증권사)가 양 소스에 있으면 네이버를 우선한다
        (목표가는 네이버에만 있음) — 한경 쪽은 카운트에 중복 반영하지 않는다.
      - 같은 소스 안에서는: 목표가 있는 리포트 > 없는 리포트, 그다음
        작성일 최신 순. (증권사의 최신 리포트가 목표가 추출에 실패했어도
        이전 리포트의 목표가를 버리지 않는다 — 기존 동작 유지)
      - 목표가 계산(평균/최고/최저/괴리율)은 target_price 있는 것만 사용.
        target_count = 목표가 있는 증권사 수.
      - 목표가가 하나도 없는 종목(한경 단독 커버)은 괴리율을 낼 수 없으므로
        rows에서 제외한다. Stage 2에서 한경 목표가가 채워지면 자연히 포함됨.
    """
    SOURCE_RANK = {"naver": 1, "hankyung": 0}  # 네이버 > 한경

    by_stock = {}
    for r in reports:
        s = by_stock.setdefault(r["code"], {
            "code": r["code"], "name": r["name"],
            "by_broker": {},  # 증권사명 → 채택된 리포트 dict
        })

        broker = r["broker"]
        r_date = parse_report_date(r["date"])
        # 채택 우선순위: (소스 순위, 목표가 유무). 같으면 작성일로 비교.
        r_key = (SOURCE_RANK.get(r.get("source"), 0),
                 0 if r.get("target_price") is None else 1)

        existing = s["by_broker"].get(broker)
        if existing is None:
            replace = True
        elif r_key != existing["_key"]:
            replace = r_key > existing["_key"]
        else:
            # 우선순위가 같으면 작성일이 더 최신일 때만 교체.
            # 날짜를 못 읽으면(파싱 실패) 먼저 들어온 것을 그대로 유지한다.
            replace = r_date is not None and (
                existing["_date"] is None or r_date > existing["_date"])

        if replace:
            s["by_broker"][broker] = {
                "title": r["title"], "broker": broker,
                "date": r["date"], "target_price": r.get("target_price"),
                "source": r.get("source", "naver"),
                "_date": r_date, "_key": r_key,
            }

    rows = []
    for code, s in by_stock.items():
        cur = prices.get(code)
        if cur is None or cur == 0:
            continue
        latest_reports = [
            {k: v for k, v in rep.items() if not k.startswith("_")}
            for rep in s["by_broker"].values()
        ]
        targets = [rep["target_price"] for rep in latest_reports
                   if rep["target_price"] is not None]
        if not targets:  # 목표가가 하나도 없으면 괴리율 계산 불가 → 제외
            continue

        avg_target = sum(targets) / len(targets)
        report_count = len(latest_reports)  # 고유 증권사 수 (양 소스 통합)
        target_count = len(targets)         # 그중 목표가 있는 증권사 수
        gap_pct = round((avg_target - cur) / cur * 100, 1)  # ★ 괴리율

        # 이상치 의심 플래그: 종목은 제외하지 않고 표시만 한다 (판단은 프론트에서).
        # - 괴리율이 비정상적으로 크거나(150% 초과) 작음(-50% 미만)
        # - 목표가가 1건뿐인데 괴리율이 100%를 넘음 (표본 부족 + 극단값 조합)
        #   ※ 기준은 report_count가 아니라 target_count — 괴리율의 표본은 목표가 수다.
        suspect = (
            gap_pct > 150 or gap_pct < -50
            or (target_count == 1 and gap_pct > 100)
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
            "target_count": target_count,
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
                "source": "naver",
            }
        time.sleep(REQUEST_DELAY_SEC)

    save_history(history)
    print(f"[crawl]   → 신규 요청 {fetched_count}건, 캐시 재사용 {cached_count}건")

    # (2-b) 한경컨센서스 수집 — Stage 1: 목록 메타데이터만 (목표가는 None).
    # 네이버가 못 보는 (종목, 증권사) 커버리지를 측정하는 게 목적이다.
    naver_count = len(all_reports)
    hk_reports = crawl_hankyung(cutoff_date)
    hk_new = 0
    for r in hk_reports:
        key = r["nid"]  # "hk:{report_idx}"
        if key in history:
            # 이미 처리한 리포트 → 캐시 값 재사용. Stage 2에서 목표가가
            # 채워지기 시작하면 재실행 때 여기로 그대로 살아난다.
            r["target_price"] = history[key].get("target_price")
        else:
            history[key] = {
                "code": r["code"], "name": r["name"], "title": r["title"],
                "broker": r["broker"], "date": r["date"],
                "target_price": r["target_price"],
                "listed_target_price": r.get("listed_target_price"),
                "source": "hankyung",
            }
            hk_new += 1
    all_reports.extend(hk_reports)
    if hk_new:
        save_history(history)
    print(f"[crawl] 한경컨센서스 {len(hk_reports)}건 추가 "
          f"(신규 {hk_new}건, 캐시 재사용 {len(hk_reports) - hk_new}건)")

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
                  f"(리포트 {r['report_count']}건·목표가 {r['target_count']}건)")

    suspect_count = sum(1 for r in rows if r["suspect"])
    print(f"[summary] 리포트 {len(all_reports)}건 → 목표가 확보 {len(codes)}종목 → "
          f"가격 확보 {len(prices)}종목 → 저장 {len(rows)}종목 (검증필요 {suspect_count}종목)")

    # 한경 고유 기여: 네이버에는 없는 (종목, 증권사) 쌍의 수.
    # 이 수치가 Stage 2(한경 PDF에서 목표가 추출) 진행 여부의 판단 근거다.
    naver_pairs = {(r["code"], r["broker"]) for r in all_reports if r.get("source") == "naver"}
    hk_pairs = {(r["code"], r["broker"]) for r in all_reports if r.get("source") == "hankyung"}
    hk_only_pairs = len(hk_pairs - naver_pairs)
    print(f"[summary] 네이버 {naver_count}건 + 한경 {len(all_reports) - naver_count}건 → "
          f"한경 고유 기여 (종목,증권사) {hk_only_pairs}쌍 → 저장 {len(rows)}종목")

    # 그 고유 쌍 중 목록에 적정가격까지 노출된 쌍 — Stage 2에서 PDF 파싱 없이
    # listed_target_price만 반영하면 바로 컨센서스에 기여할 수 있는 몫이다.
    hk_priced_pairs = {(r["code"], r["broker"]) for r in all_reports
                       if r.get("source") == "hankyung" and r.get("listed_target_price")}
    print(f"[summary] 한경 고유 {hk_only_pairs}쌍 중 적정가격 확보 "
          f"{len(hk_priced_pairs - naver_pairs)}쌍 (Stage 2는 PDF 없이 이 값만 반영하면 됨)")


# ──────────────────────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="목표주가 괴리율 트래커")
    parser.add_argument("--inspect", action="store_true",
                        help="목록 페이지 HTML을 저장해 구조 확인")
    parser.add_argument("--inspect-hk", action="store_true",
                        help="한경컨센서스 목록 페이지 HTML을 저장해 구조 확인")
    parser.add_argument("--crawl", action="store_true",
                        help="수집 → 괴리율 계산 → data.json 생성")
    args = parser.parse_args()

    if args.inspect:
        inspect_page()
    elif args.inspect_hk:
        inspect_hankyung()
    elif args.crawl:
        crawl()
    else:
        parser.print_help()