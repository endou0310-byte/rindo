# scripts/check_yamanashi_match.py
import json, unicodedata, re, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
roads = ROOT / "data" / "out" / "roads.geojson"
reg   = ROOT / "data" / "out" / "yamanashi_registry.json"

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    # 【カナ】/［ ］/〔 〕/[] の括弧内を除去
    s = re.sub(r"[【［\[\{〔].*?[】］\]\}〕]", "", s)
    # () の括弧内も除去
    s = re.sub(r"[（(].*?[)）]", "", s)
    # 長音等の統一
    s = s.replace("－","-").replace("―","-").replace("–","-")
    # ヶ/ヵ の表記差を吸収
    s = s.replace("ヶ","ケ").replace("ヵ","カ")
    # よくある接尾語・ノイズ
    s = re.sub(r"(森林管理道)", "", s)
    s = re.sub(r"(県営|市営|町営|村営)", "", s)
    s = re.sub(r"(本線|幹線)$", "", s)
    s = re.sub(r"(支線?)$", "", s)  # “支”だけも落とす
    s = s.replace("林道","").replace("線","")
    # 残りの記号・空白
    s = re.sub(r"[（）()・･‐\-—―ｰ\s　]", "", s)
    return s

# ---- データ読込
r = json.loads(roads.read_text(encoding="utf-8"))
g = json.loads(reg.read_text(encoding="utf-8"))
yn_dict = g.get("by_norm") or {}
yn_keys = set(yn_dict.keys())

# 山梨(19)の線を正規化名で集計
osm_norms = {}
for f in r.get("features", []):
    p = f.get("properties") or {}
    if str(p.get("pref_code")) != "19":
        continue
    raw = p.get("name") or p.get("rindo_name") or p.get("display_name") or ""
    nm = norm_name(raw)
    if not nm: 
        continue
    osm_norms[nm] = osm_norms.get(nm, 0) + 1

matched       = sorted(osm_norms.keys() & yn_keys)
unmatched_osm = sorted(osm_norms.keys() - yn_keys)
unmatched_yn  = sorted(yn_keys - osm_norms.keys())

print(f"OSM(山梨) 正規化名: {len(osm_norms)} 種")
print(f"山梨レジストリ keys: {len(yn_keys)} 種")
print(f"一致: {len(matched)} / 未一致OSM: {len(unmatched_osm)} / 未一致レジストリ: {len(unmatched_yn)}")

print("\n=== 未一致OSM(上位30) ===")
for n in unmatched_osm[:30]:
    print("-", n, f"(count={osm_norms[n]})")

print("\n=== 未一致レジストリ(上位30) ===")
for n in unmatched_yn[:30]:
    # 参考に元の表示名とURLも出す（先頭のみ）
    row = (yn_dict[n] or [{}])[0]
    print("-", n, "=>", row.get("name"), row.get("url"))
