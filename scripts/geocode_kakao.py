#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, time, csv, json, requests, urllib.parse

API = "https://dapi.kakao.com/v2/local/search/address.json"

def geocode(addr, key):
    url = f"{API}?query={urllib.parse.quote(addr)}"
    r = requests.get(url, headers={"Authorization": f"KakaoAK {key}"}, timeout=10)
    r.raise_for_status()
    docs = r.json().get("documents", [])
    if not docs: 
        return "", ""
    d = docs[0]
    return d.get("y",""), d.get("x","")  # lat, lng

def main():
    key = os.environ.get("KAKAO_API_KEY")
    if not key:
        raise SystemExit("KAKAO_API_KEY env missing")

    src = "data/sellers_master.csv"   # 또는 stores_master.csv
    dst = "data/stores_master.csv"    # lat/lng 채워질 파일

    rows = []
    with open(src, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    out = []
    for i, row in enumerate(rows, 1):
        addr = row.get("address","")
        name = row.get("name","")
        print(f"[GEO] {i}/{len(rows)} {name} {addr}")
        lat, lng = "", ""
        if addr:
            lat, lng = geocode(addr, key)
            time.sleep(0.15)  # 속도 완화
        out.append({
            "store_id": "",
            "name": name,
            "address": addr,
            "lat": lat,
            "lng": lng
        })

    with open(dst, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["store_id","name","address","lat","lng"])
        w.writeheader(); w.writerows(out)
    print(f"[WRITE] {dst} rows={len(out)}")

if __name__ == "__main__":
    main()
