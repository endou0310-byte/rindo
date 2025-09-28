#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
roads.geojson からポップアップ表示用の林道名をユニーク抽出して集計
出力: data/derived/rindo_names.csv / .json
"""

import json, os, csv, re
from collections import defaultdict
from urllib.parse import urlparse

IN1 = "view/data/out/roads.geojson"
IN2 = "data/out/roads.geojson"
OUT_DIR = "data/derived"
os.makedirs(OUT_DIR, exist_ok=True)

def normalize_name(s: str) -> str:
    if not s: return ""
    t = s.strip()
    # よくある接尾辞や表記ゆれを軽く正規化
    t = re.sub(r"[　\s]", "", t)            # 全角/半角スペース除去
    t = re.sub(r"林道", "", t)               # 「林道」を一旦外す
    t = re.sub(r"(線|せん)$", "", t)        # 末尾の「線」を外す
    t = re.sub(r"[（）\(\)･・‐-—―\-ｰ]", "", t)  # 括弧や横棒類
    return t

def host_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

path = IN1 if os.path.exists(IN1) else IN2
with open(path, "r", encoding="utf-8") as f:
    gj = json.load(f)

bucket = defaultdict(lambda: {
    "name_originals": set(),
    "norm_name": "",
    "prefs": set(),
    "domains": set(),
    "segments": 0,
    "events": 0,
})

for ft in gj.get("features", []):
    prop = ft.get("properties", {}) or {}
    name = prop.get("name") or prop.get("rindo_name") or ""
    if not name: 
        continue
    norm = normalize_name(name)
    key  = norm or name
    b = bucket[key]
    b["name_originals"].add(name)
    b["norm_name"] = norm or name
    if prop.get("pref"):
        b["prefs"].add(prop["pref"])
    if prop.get("source_url"):
        b["domains"].add(host_of(prop["source_url"]))
    b["segments"] += 1
    # event が紐付く＝規制情報があるとみなす
    if prop.get("status") or prop.get("event_id"):
        b["events"] += 1

rows = []
for k, v in bucket.items():
    rows.append({
        "norm_name": v["norm_name"],
        "name_examples": " / ".join(sorted(v["name_originals"]))[:200],
        "n_segments": v["segments"],
        "n_events": v["events"],
        "prefs": ",".join(sorted(v["prefs"])) if v["prefs"] else "",
        "domains": ",".join(sorted(v["domains"])) if v["domains"] else "",
    })

rows.sort(key=lambda r: (-r["n_events"], -r["n_segments"], r["norm_name"]))

# CSV
csv_path = os.path.join(OUT_DIR, "rindo_names.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else 
        ["norm_name","name_examples","n_segments","n_events","prefs","domains"])
    w.writeheader()
    for r in rows: w.writerow(r)

# JSON
json_path = os.path.join(OUT_DIR, "rindo_names.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=2)

print("wrote:", csv_path)
print("wrote:", json_path)
