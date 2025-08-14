#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, csv, json, subprocess
from datetime import date, datetime
from typing import List, Tuple, Dict
import requests, pandas as pd
from lxml import html

BASE_URL = "https://www.dhlottery.co.kr/store.do?method=topStore&pageGubun=L645&drwNo={drwNo}"
LOTTO_JSON = "https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={drwNo}"

def get_draw_date(n:int)->str:
    try:
        r=requests.get(LOTTO_JSON.format(drwNo=n),timeout=10)
        if r.ok:
            d=r.json().get("drwNoDate")
            if d: return d
    except Exception: pass
    return ""

def fetch_table(n:int):
    r=requests.get(BASE_URL.format(drwNo=n),timeout=15,headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    doc=html.fromstring(r.text)
    rows=[]; draw_date=get_draw_date(n)

    tables=doc.cssselect("table.tbl_data")
    for t in tables:
        rank_hint=0
        head=" ".join(t.xpath(".//caption//text()")+t.xpath(".//th//text()"))
        if "1등" in head: rank_hint=1
        if "2등" in head and rank_hint==0: rank_hint=2
        for tr in t.xpath(".//tbody/tr"):
            tds=[("".join(td.xpath(".//text()"))).strip() for td in tr.xpath("./td")]
            if len(tds)>=3:
                name,choice,addr=tds[0],tds[1],tds[2]
                rank=rank_hint
                for c in tds:
                    if "1등" in c: rank=1
                    if "2등" in c: rank=2
                if rank in (1,2):
                    rows.append((name,choice,addr,rank))
    return rows, draw_date

def norm(s:str)->str:
    s=(s or "").strip()
    s=re.sub(r"\s+","",s)
    s=s.replace("편의점","")
    return s

def build_index(geojson_path:str)->Dict[str,str]:
    with open(geojson_path,"r",encoding="utf-8") as f: gj=json.load(f)
    idx={}
    for ft in gj.get("features",[]):
        p=ft.get("properties",{})
        idx[norm(p.get("name",""))+"|"+norm(p.get("address",""))]=p.get("store_id")
    return idx

def main(repo_root:str, base_draw:int, base_date:str):
    data_dir=os.path.join(repo_root,"data"); os.makedirs(data_dir,exist_ok=True)
    base_d=datetime.strptime(base_date,"%Y-%m-%d").date()
    today=date.today()
    est=base_draw+((today-base_d).days//7)

    stores_csv=os.path.join(data_dir,"dhlottery_stores.csv")
    if os.path.exists(stores_csv): df=pd.read_csv(stores_csv)
    else: df=pd.DataFrame(columns=["draw","draw_date","rank","name","choice_type","address"])

    have=set(df["draw"].unique().tolist()) if not df.empty else set()
    to_fetch=[d for d in range(min(have|{est}),est+1) if d not in have] if have else [est]

    all_rows=[]
    for drw in to_fetch:
        print(f"[SCRAPE] {drw}")
        rows,dd=fetch_table(drw)
        for name,choice,addr,rank in rows:
            all_rows.append({"draw":drw,"draw_date":dd,"rank":rank,"name":name,"choice_type":choice,"address":addr})

    if all_rows:
        df=pd.concat([df,pd.DataFrame(all_rows)],ignore_index=True)

    df.sort_values(["draw","rank","name"],inplace=True)
    df.to_csv(stores_csv,index=False,encoding="utf-8")
    print(f"[SAVE] {stores_csv} ({len(df)} rows)")

    geo=os.path.join(data_dir,"stores_clean.geojson")
    wins=os.path.join(data_dir,"wins.csv"); um=os.path.join(data_dir,"wins_unmatched.csv")
    if os.path.exists(geo):
        idx=build_index(geo); W=[]; U=[]
        for _,r in df.iterrows():
            key=norm(str(r["name"]))+"|"+norm(str(r["address"]))
            sid=idx.get(key,"")
            row={"store_id":sid,"date":r["draw_date"],"rank":int(r["rank"]),
                 "draw_no":int(r["draw"]),"name":r["name"],"address":r["address"]}
            (W if sid else U).append(row)
        pd.DataFrame(W).to_csv(wins,index=False,encoding="utf-8")
        pd.DataFrame(U).to_csv(um,index=False,encoding="utf-8")
        print(f"[SAVE] {wins}, {um}")
        # A3 (둘 다 있을 때만)
        if W:
            out_geo=os.path.join(data_dir,"stores_clean.a3.geojson")
            out_sum=os.path.join(data_dir,"scores_a3_summary.csv")
            subprocess.check_call([
              "python", os.path.join(repo_root,"scripts","compute_a3_scores.py"),
              "--geojson", geo, "--events", wins,
              "--out-geojson", out_geo, "--out-summary", out_sum
            ])

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--repo-root",required=True)
    p.add_argument("--base-draw",type=int,required=True)
    p.add_argument("--base-date",type=str,required=True)
    a=p.parse_args()
    main(a.repo_root,a.base_draw,a.base_date)
