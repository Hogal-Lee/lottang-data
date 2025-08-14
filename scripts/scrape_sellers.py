#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, time, re, requests
from lxml import html

BASE = "https://www.dhlottery.co.kr/store.do?method=sellerInfo645"

def clean(s): 
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_page(page:int):
    # 페이지 파라미터: nowPage=숫자 (기본 리스트)
    url = f"{BASE}&nowPage={page}"
    r = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    doc = html.fromstring(r.text)
    rows = []
    # 판매점 표 선택자(사이트 변경 시 아래를 조정)
    for tr in doc.cssselect("table.tbl_data tbody tr"):
        tds = ["".join(td.xpath(".//text()")) for td in tr.xpath("./td")]
        tds = [clean(x) for x in tds if clean(x)]
        if len(tds) < 3: 
            continue
        # 보통: [번호, 상호, 전화, 주소] 혹은 [번호, 상호, 주소, 전화]
        # 전화번호 패턴으로 위치 판정
        phone_idx = next((i for i,x in enumerate(tds) if re.search(r"\d{2,4}-\d{3,4}-\d{3,4}", x)), -1)
        if phone_idx == -1:
            # 전화 미기재일 수도 있음 → 빈 값
            phone = ""
            # 번호 칸 제거
            cells = tds[1:] if re.fullmatch(r"\d+", tds[0]) else tds
            if len(cells)>=2:
                name, addr = cells[0], cells[-1]
            else:
                continue
        else:
            phone = tds[phone_idx]
            cells = [x for i,x in enumerate(tds) if i != phone_idx]
            # 번호 칸 제거
            if cells and re.fullmatch(r"\d+", cells[0]):
                cells = cells[1:]
            if len(cells)>=2:
                name, addr = cells[0], cells[-1]
            else:
                continue

        rows.append({
            "name": name, "phone": phone, "address": addr
        })
    return rows

def main():
    out = "data/sellers_master.csv"
    all_rows = []
    page = 1
    while True:
        print(f"[SELLERS] page {page}")
        rows = parse_page(page)
        if not rows:
            break
        all_rows.extend(rows)
        page += 1
        time.sleep(0.4)  # 과한 요청 방지
        if page > 2000:  # 안전브레이크
            break
    # 중복 제거
    seen = set(); dedup=[]
    for r in all_rows:
        key = (r["name"], r["address"])
        if key in seen: 
            continue
        seen.add(key); dedup.append(r)
    # 저장
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name","phone","address"])
        w.writeheader(); w.writerows(dedup)
    print(f"[WRITE] {out} rows={len(dedup)}")

if __name__ == "__main__":
    main()
