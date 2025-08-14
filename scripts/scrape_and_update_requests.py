#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
동행복권 1/2등 판매점 스크레이핑 (Requests+lxml)
- 대상: https://www.dhlottery.co.kr/store.do?method=topStore&pageGubun=L645&drwNo=회차
- 산출: data/dhlottery_stores.csv (누적), data/wins.csv, data/wins_unmatched.csv,
       data/stores_clean.a3.geojson, data/scores_a3_summary.csv
"""
import os, re, csv, json, time, subprocess
from datetime import date, datetime, timedelta
from typing import List, Tuple, Dict
import requests
import pandas as pd
from lxml import html

BASE_URL = "https://www.dhlottery.co.kr/store.do?method=topStore&pageGubun=L645&drwNo={drwNo}"
LOTTO_JSON = "https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={drwNo}"

def get_draw_date(drw_no: int) -> str:
    try:
        r = requests.get(LOTTO_JSON.format(drwNo=drw_no), timeout=10)
        if r.ok:
            j = r.json()
            d = j.get("drwNoDate")
            if d:
                return d  # YYYY-MM-DD
    except Exception:
        pass
    return ""

def fetch_table(drw_no: int):
    """특정 회차 페이지에서 테이블 파싱 → [(name, choice, addr, rank)], draw_date"""
    url = BASE_URL.format(drwNo=drw_no)
    r = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    doc = html.fromstring(r.text)

    draw_date = get_draw_date(drw_no)
    rows = []

    # 1) 데스크톱 표(.tbl_data) 모두 스캔
    tables = doc.cssselect("table.tbl_data")
    if tables:
        for t in tables:
            # 테이블 제목/헤더에서 1등/2등 힌트
            rank_hint = 0
            head_text = " ".join(t.xpath(".//caption//text()") + t.xpath(".//th//text()")).strip()
            if "1등" in head_text: rank_hint = 1
            if "2등" in head_text and rank_hint == 0: rank_hint = 2

            for tr in t.xpath(".//tbody/tr"):
                tds = [("".join(td.xpath(".//text()"))).strip() for td in tr.xpath("./td")]
                if len(tds) >= 3:
                    name, choice, addr = tds[0], tds[1], tds[2]
                    rank = rank_hint
                    # 셀에 '1등/2등' 표기가 별도 있을 경우 덮어쓰기
                    for cell in tds:
                        if "1등" in cell: rank = 1
                        if "2등" in cell: rank = 2
                    if rank in (1,2):
                        rows.append((name, choice, addr, rank))
    return rows, draw_date

def normalize(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+","",s)  # 공백 제거
    s = s.replace("편의점","")  # 과한 수식어 예시 제거
    return s

def build_store_index(geojson_path: str):
    with open(geojson_path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    idx={}
    for ft in gj.get("features",[]):
        p=ft.get("properties",{})
        key = normalize(p.get("name","")) + "|" + normalize(p.get("address",""))
        idx[key]=p.get("store_id")
    return idx

def main(repo_root: str, base_draw: int, base_date: str):
    data_dir = os.path.join(repo_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    # 최신 회차(주 1회) 추정
    base_d = datetime.strptime(base_date, "%Y-%m-%d").date()
    today = date.today()
    est = base_draw + ((today - base_d).days // 7)

    # 기존 CSV 로드
    stores_csv = os.path.join(data_dir, "dhlottery_stores.csv")
    if os.path.exists(stores_csv):
        df = pd.read_csv(stores_csv)
    else:
        df = pd.DataFrame(columns=["draw","draw_date","rank","name","choice_type","address"])

    have_draws = set(df["draw"].unique().tolist()) if not df.empty else set()
    to_fetch = [d for d in range(min(have_draws|{est}), est+1) if d not in have_draws] if have_draws else [est]

    all_rows = []
    for drw in to_fetch:
        print(f"[SCRAPE] {drw}")
        rows, ddate = fetch_table(drw)
        if not rows:
            print(f"[WARN] draw {drw}: parsed 0 rows (HTML 구조 변화 가능)")
        for name, choice, addr, rank in rows:
            all_rows.append({"draw": drw, "draw_date": ddate, "rank": rank,
                             "name": name, "choice_type": choice, "address": addr})

    if all_rows:
        df_new = pd.DataFrame(all_rows)
        df = pd.concat([df, df_new], ignore_index=True)

    # 저장(빈 경우라도 파일 생성)
    df.sort_values(["draw","rank","name"], inplace=True)
    df.to_csv(stores_csv, index=False, encoding="utf-8")
    print(f"[SAVE] {stores_csv} ({len(df)} rows)")

    # 매칭 → wins.csv
    geojson_path = os.path.join(data_dir, "stores_clean.geojson")
    wins_csv = os.path.join(data_dir, "wins.csv")
    unmatched_csv = os.path.join(data_dir, "wins_unmatched.csv")

    if os.path.exists(geojson_path):
        idx = build_store_index(geojson_path)
        wins_rows = []
        unmatched = []
        for _, r in df.iterrows():
            key = normalize(str(r["name"])) + "|" + normalize(str(r["address"]))
            sid = idx.get(key, "")
            row = {
                "store_id": sid,
                "date": r["draw_date"],
                "rank": int(r["rank"]),
                "draw_no": int(r["draw"]),
                "name": r["name"],
                "address": r["address"]
            }
            if sid: wins_rows.append(row)
            else: unmatched.append(row)
        pd.DataFrame(wins_rows).to_csv(wins_csv, index=False, encoding="utf-8")
        pd.DataFrame(unmatched).to_csv(unmatched_csv, index=False, encoding="utf-8")
        print(f"[SAVE] {wins_csv}, {unmatched_csv}")
    else:
        print(f"[WARN] {geojson_path} not found. A3 will be skipped.")

    # A3 계산(geojson & wins 둘 다 있을 때만)
    if os.path.exists(geojson_path) and os.path.exists(wins_csv):
        out_geo = os.path.join(data_dir, "stores_clean.a3.geojson")
        out_sum = os.path.join(data_dir, "scores_a3_summary.csv")
        cmd = [
            "python", os.path.join(repo_root, "scripts", "compute_a3_scores.py"),
            "--geojson", geojson_path,
            "--events", wins_csv,
            "--out-geojson", out_geo,
            "--out-summary", out_sum,
        ]
        print("[A3] running:", " ".join(cmd))
        subprocess.check_call(cmd)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", required=True)
    p.add_argument("--base-draw", type=int, required=True)
    p.add_argument("--base-date", type=str, required=True)
    a = p.parse_args()
    main(a.repo_root, a.base_draw, a.base_date)
