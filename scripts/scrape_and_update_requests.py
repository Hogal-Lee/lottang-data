#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LoTTang 주간 스크레이퍼 (Requests + lxml)

동작:
1) 회차별 1/2등 판매점 표를 스크랩해 raw CSV(= dhlottery_stores.csv)에 누적
2) data/stores_clean.geojson 과 (선택) data/store_aliases.csv 로 매장 매칭
3) 매칭 성공은 wins.csv, 실패는 wins_unmatched.csv 로 저장
4) (옵션) scripts/compute_a3_scores.py 가 있을 경우 A3 산출 파일 생성

사용 예 (GitHub Actions):
  python scripts/scrape_and_update_requests.py \
    --repo-root . --base-draw 1184 --base-date 2025-08-09
"""
from __future__ import annotations

import os
import re
import csv
import json
import subprocess
from datetime import date, datetime
from typing import Dict, List, Tuple

import requests
import pandas as pd
from lxml import html  # cssselect 필요

# ---------------------- 대상 URL ----------------------
BASE_URL = (
    "https://www.dhlottery.co.kr/store.do?method=topStore"
    "&pageGubun=L645&drwNo={drwNo}"
)
LOTTO_JSON = (
    "https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={drwNo}"
)

# ---------------------- 공용 유틸 ----------------------
def get_draw_date(n: int) -> str:
    """회차 → 추첨일(YYYY-MM-DD). 실패 시 빈 문자열."""
    try:
        r = requests.get(LOTTO_JSON.format(drwNo=n), timeout=10)
        if r.ok:
            d = r.json().get("drwNoDate")
            if d:
                return d
    except Exception:
        pass
    return ""


def _is_choice(s: str) -> bool:
    return any(x in s for x in ("자동", "수동", "반자동"))


def _looks_addr(s: str) -> bool:
    # 주소스러운 키워드 포함 여부로 판정 (대충 맞으면 됨)
    return bool(re.search(r"(시|군|구|로|길|동|면|리|번지|호)\s*\d*", s))


def _clean(cell: str) -> str:
    return re.sub(r"\s+", " ", (cell or "")).strip()


def _guess_columns(tds: List[str]) -> Tuple[str, str, str] | None:
    """
    tds: tr 한 줄에서 추출한 텍스트 리스트.
    규칙:
      - 첫 칸이 순번(숫자)이면 제거
      - '자동/수동/반자동' 포함칸은 choice
      - 주소처럼 보이는 칸은 address
      - 남은 것 중 가장 긴 텍스트를 name
    """
    cells = [_clean(x) for x in tds if _clean(x)]
    if not cells:
        return None

    # 앞 칸이 순번이면 제거
    if re.fullmatch(r"\d+", cells[0]):
        cells = cells[1:]
        if not cells:
            return None

    choice = next((c for c in cells if _is_choice(c)), "")
    addr = next((c for c in cells if _looks_addr(c)), "")

    leftovers = [c for c in cells if c not in (choice, addr)]
    leftovers = [c for c in leftovers if "dhlottery.co.kr" not in c]
    name = max(leftovers, key=len) if leftovers else ""

    if not addr:
        for c in cells:
            if c != choice and _looks_addr(c):
                addr = c
                break

    if not name or not addr:
        return None
    return name, choice, addr


def fetch_table(drw_no: int):
    """한 회차 페이지에서 1/2등 표를 파싱해 (name, choice, addr, rank) 목록과 draw_date를 반환."""
    url = BASE_URL.format(drwNo=drw_no)
    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    doc = html.fromstring(r.text)

    draw_date = get_draw_date(drw_no)
    rows: List[Tuple[str, str, str, int]] = []

    for t in doc.cssselect("table.tbl_data"):
        # 표 제목/헤더에서 1등/2등 힌트
        head = " ".join(t.xpath(".//caption//text()") + t.xpath(".//th//text()"))
        rank_hint = 1 if "1등" in head else (2 if "2등" in head else 0)

        for tr in t.xpath(".//tbody/tr"):
            tds = ["".join(td.xpath(".//text()")).strip() for td in tr.xpath("./td")]
            if not tds:
                continue
            guessed = _guess_columns(tds)
            if not guessed:
                continue
            name, choice, addr = guessed

            # 셀 내용에 1등/2등이 있으면 보정
            flat = " ".join(tds)
            rank = rank_hint
            if "1등" in flat:
                rank = 1
            if "2등" in flat and rank == 0:
                rank = 2
            if rank not in (1, 2):
                continue

            # 광고/URL 같은 이상치 제거
            if "dhlottery.co.kr" in name or "dhlottery.co.kr" in addr:
                continue

            rows.append((name, choice, addr, rank))

    return rows, draw_date


def norm(s: str) -> str:
    """매칭을 위한 문자열 정규화."""
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)  # 모든 공백 제거
    s = re.sub(r"\(.*?\)", "", s)  # 괄호 설명 제거
    for tok in ["복권방", "복권", "로또", "편의점", "CU", "GS25", "세븐일레븐", "미니스톱"]:
        s = s.replace(tok, "")
    s = s.replace("-", "")
    return s


def build_index(geojson_path: str) -> Dict[str, str]:
    """마스터 GeoJSON → (정규화된 name|address -> store_id) 인덱스."""
    with open(geojson_path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    idx: Dict[str, str] = {}
    for ft in gj.get("features", []):
        p = ft.get("properties", {}) or {}
        key = norm(p.get("name", "")) + "|" + norm(p.get("address", ""))
        if key and p.get("store_id"):
            idx[key] = p["store_id"]
    return idx


def load_aliases(path: str) -> Dict[str, str]:
    """
    (선택) 수동 매핑 파일: data/store_aliases.csv
    헤더: alias_name,alias_address,store_id
    """
    m: Dict[str, str] = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                key = norm(row.get("alias_name", "")) + "|" + norm(
                    row.get("alias_address", "")
                )
                if key and row.get("store_id"):
                    m[key] = row["store_id"]
    return m


# ---------------------- 메인 루틴 ----------------------
def main(repo_root: str, base_draw: int, base_date: str):
    data_dir = os.path.join(repo_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    base_d = datetime.strptime(base_date, "%Y-%m-%d").date()
    today = date.today()
    est = base_draw + ((today - base_d).days // 7)  # 오늘 기준 예상 최신 회차

    stores_csv = os.path.join(data_dir, "dhlottery_stores.csv")
    if os.path.exists(stores_csv):
        df = pd.read_csv(stores_csv)
    else:
        df = pd.DataFrame(
            columns=["draw", "draw_date", "rank", "name", "choice_type", "address"]
        )

    have = set(df["draw"].unique().tolist()) if not df.empty else set()
    # 기존에 없는 회차만 추가 수집 (처음엔 최신 1회차만)
    to_fetch = [d for d in range(min(have | {est}), est + 1) if d not in have] if have else [est]

    all_rows: List[Dict[str, str | int]] = []
    for drw in to_fetch:
        print(f"[SCRAPE] {drw}")
        rows, dd = fetch_table(drw)
        for name, choice, addr, rank in rows:
            all_rows.append(
                {
                    "draw": drw,
                    "draw_date": dd,
                    "rank": rank,
                    "name": name,
                    "choice_type": choice,
                    "address": addr,
                }
            )

    if all_rows:
        df = pd.concat([df, pd.DataFrame(all_rows)], ignore_index=True)

    df.sort_values(["draw", "rank", "name"], inplace=True)
    df.to_csv(stores_csv, index=False, encoding="utf-8")
    print(f"[SAVE] {stores_csv} ({len(df)} rows)")

    # ----- 매칭: GeoJSON + (옵션) alias -----
    geo = os.path.join(data_dir, "stores_clean.geojson")
    wins = os.path.join(data_dir, "wins.csv")
    um = os.path.join(data_dir, "wins_unmatched.csv")

    if os.path.exists(geo):
        idx = build_index(geo)
        alias = load_aliases(os.path.join(data_dir, "store_aliases.csv"))
        W: List[Dict[str, str | int]] = []
        U: List[Dict[str, str | int]] = []

        for _, r in df.iterrows():
            key = norm(str(r["name"])) + "|" + norm(str(r["address"]))
            sid = alias.get(key) or idx.get(key, "")
            row = {
                "store_id": sid,
                "date": r["draw_date"],
                "rank": int(r["rank"]),
                "draw_no": int(r["draw"]),
                "name": r["name"],
                "address": r["address"],
            }
            (W if sid else U).append(row)

        pd.DataFrame(W).to_csv(wins, index=False, encoding="utf-8")
        pd.DataFrame(U).to_csv(um, index=False, encoding="utf-8")
        print(f"[SAVE] {wins} ({len(W)} rows), {um} ({len(U)} rows)")

        # ----- A3 산출 (선택) -----
        a3_script = os.path.join(repo_root, "scripts", "compute_a3_scores.py")
        if W and os.path.exists(a3_script):
            out_geo = os.path.join(data_dir, "stores_clean.a3.geojson")
            out_sum = os.path.join(data_dir, "scores_a3_summary.csv")
            subprocess.check_call(
                [
                    "python",
                    a3_script,
                    "--geojson",
                    geo,
                    "--events",
                    wins,
                    "--out-geojson",
                    out_geo,
                    "--out-summary",
                    out_sum,
                ]
            )
            print(f"[A3] wrote {out_geo}, {out_sum}")
        else:
            print("[A3] skipped (wins empty or compute_a3_scores.py not found)")
    else:
        print("[MATCH] skipped (stores_clean.geojson not found)")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", required=True)
    p.add_argument("--base-draw", type=int, required=True)
    p.add_argument("--base-date", type=str, required=True)
    a = p.parse_args()
    main(a.repo_root, a.base_draw, a.base_date)
