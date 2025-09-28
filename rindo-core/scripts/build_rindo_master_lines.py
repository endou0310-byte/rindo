# scripts/build_rindo_master_lines.py
# 全国一括 “ほぼ林道だけ（≈98%精度）” 抽出スクリプト
# 方式:
#  1) 名称に「林道」を含む → 無条件採用（長さフィルタ免除）
#  2) それ以外はスコアリングで判定（タグ/未舗装/森林系用途/長さ/NGワード/都市系service）
#  3) 200m 未満は基本除外（ただし 1) は免除）
#
# チューニング箇所:
#  - MIN_LEN_M, SCORE_THRESHOLD
#  - POSITIVE/NEGATIVE ルール群（下の定数）
#
# 依存: pyosmium (osmium)
# 入力: japan-latest.osm.pbf（自動検出 or 環境変数 PBF_PATH）
# 出力: data/out/rindo_master_lines.geojson

import json, glob, math, pathlib, os, sys
import osmium

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)
GEOJSON_OUT = OUT_DIR / "rindo_master_lines.geojson"

# -------------------------
# PBF 検出
# -------------------------
def resolve_pbf() -> pathlib.Path:
    # 優先: 環境変数
    if os.environ.get("PBF_PATH"):
        p = pathlib.Path(os.environ["PBF_PATH"])
        if p.exists():
            return p
    # Windows 既定パス例
    ascii_pref = pathlib.Path(r"C:\rindo-pbf\japan-latest.osm.pbf")
    if ascii_pref.exists():
        return ascii_pref
    # リポジトリ内
    default = ROOT / "data" / "pbf" / "japan-latest.osm.pbf"
    if default.exists():
        return default
    # 親ディレクトリ直下の *.osm.pbf
    cand = sorted(glob.glob(str(ROOT.parent / "*.osm.pbf")))
    if cand:
        print(f"[INFO] Using detected PBF: {cand[0]}")
        return pathlib.Path(cand[0])
    raise FileNotFoundError("OSM PBF not found. Set PBF_PATH or place *.osm.pbf")

PBF_PATH = resolve_pbf()

# -------------------------
# ルール定義（調整ポイント）
# -------------------------
MIN_LEN_M = 200.0          # 無名候補に適用する最小長
SCORE_THRESHOLD = 10        # これ以上で採用（名称「林道」は無条件採用）

OK_HIGHWAYS = {"track", "service", "unclassified"}

URBAN_SERVICE = {
    "parking", "parking_aisle", "driveway", "alley", "emergency_access",
    "bus", "drive-through", "industrial", "yard", "car_wash", "garages"
}

# 舗装・未舗装の簡易判定
UNPAVED_WORDS = ("unpaved", "ground", "dirt", "gravel", "compacted", "fine_gravel", "sand")
PAVED_WORDS   = ("asphalt", "paved", "concrete", "paving_stones", "sett", "chipseal")

# NGワード（農業/用水/管理道 等）
NAME_HARD_NEG = ("用水", "水路", "用排水", "排水路", "農道", "水利", "畦", "管理用道路", "管理道", "農業用", "暗渠")

# 要注意ワード（紛れやすい）
NAME_SOFT_NEG = ("作業道", "工事用道路", "伐採道")

# 森林系用途/制限の陽性タグ
FORESTRY_POS = (
    ("service", "forest_service"),
    ("access", "forestry"),
    ("motor_vehicle", "forestry"),
)

# tracktype の陽性（未舗装度）
TRACK_POS = ("grade3", "grade4", "grade5")

# -------------------------
# 幾何ツール
# -------------------------
def dist_m(a, b) -> float:
    # a,b = [lon,lat]
    dx = (a[0] - b[0]) * 111320 * math.cos(math.radians((a[1] + b[1]) / 2))
    dy = (a[1] - b[1]) * 110540
    return (dx*dx + dy*dy) ** 0.5

def quick_length(coords) -> float:
    if len(coords) < 2:
        return 0.0
    length_m = 0.0
    for i in range(len(coords) - 1):
        length_m += dist_m(coords[i], coords[i+1])
        # 早期終了で高速化
        if length_m >= MIN_LEN_M:
            break
    return length_m

# -------------------------
# 判定ロジック
# -------------------------
def score_rindo(tags: dict) -> tuple[bool, int, list[str]]:
    """
    返値: (採用/棄却, 合計スコア, 理由の配列)
    ※ 名称に「林道」を含む場合は無条件採用（スコアは計上もするが長さ免除）
    """
    reasons = []
    score = 0

    h = (tags.get("highway") or "").lower()
    if h not in OK_HIGHWAYS:
        return (False, score, ["highwayが対象外"])

    name = (tags.get("name") or "")
    tracktype = (tags.get("tracktype") or "").lower()
    access = (tags.get("access") or "").lower()
    mv = (tags.get("motor_vehicle") or "").lower()
    service = (tags.get("service") or "").lower()
    surface = (tags.get("surface") or "").lower()
    waterway = (tags.get("waterway") or "").lower()
    landuse = (tags.get("landuse") or "").lower()
    natural = (tags.get("natural") or "").lower()

    # 即除外（用水/水路/農業系）
    if waterway:
        return (False, score, ["waterwayタグで除外"])
    if any(k in name for k in NAME_HARD_NEG):
        return (False, score, ["名称に農業/用水系NGワード"])

    if service in {"irrigation", "drainage", "agricultural"}:
        return (False, score, ["serviceが農業/用水系"])

    # 名称に「林道」→無条件採用（強ポジ）
    if "林道" in name:
        score += 10
        reasons.append("名称=林道")
        return (True, score, reasons)  # 長さは後段で免除扱い

    # 都市系の service は強い負点
    if service in URBAN_SERVICE:
        score -= 10
        reasons.append(f"都市系service={service}")

    # track + 未舗装度
    if h == "track":
        if any(g in tracktype for g in TRACK_POS):
            score += 5
            reasons.append(f"tracktype={tracktype}")
        # surface 未舗装
        if any(w in surface for w in UNPAVED_WORDS):
            score += 3
            reasons.append(f"surface(未舗装)={surface}")
        if any(w in surface for w in PAVED_WORDS):
            score -= 2
            reasons.append(f"surface(舗装)={surface}")

    # service/unclassified でも森林系用途があれば加点
    for k, v in FORESTRY_POS:
        if (tags.get(k) or "").lower() == v:
            score += 5
            reasons.append(f"{k}={v}")

    # 位置のヒント（way 自身に landuse/natural が付くケースは稀だが保険）
    if landuse == "forest" or natural == "wood":
        score += 3
        reasons.append("forest/wood上")

    # 要注意ワードは小さく減点（“作業道”等）
    if any(k in name for k in NAME_SOFT_NEG):
        score -= 2
        reasons.append("名称に要注意ワード")

    # highway 種別の素点
    if h == "track":
        score += 2
        reasons.append("highway=track")
    elif h == "unclassified":
        score += 1
        reasons.append("highway=unclassified")
    elif h == "service":
        score += 0  # neutral

    return (score >= SCORE_THRESHOLD, score, reasons)

# -------------------------
# ハンドラ
# -------------------------
class RindoHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.features = []

    def way(self, w: "osmium.osm.Way"):
        tags = dict(w.tags)
        ok, score, reasons = score_rindo(tags)
        if not ok and "林道" not in (tags.get("name") or ""):
            return

        coords = [[n.lon, n.lat] for n in w.nodes]
        if len(coords) < 2:
            return

        # 名称「林道」以外には長さフィルタ適用
        named_rindo = ("林道" in (tags.get("name") or ""))
        if not named_rindo:
            length_m = quick_length(coords)
            if length_m < MIN_LEN_M:
                return
        else:
            length_m = quick_length(coords)  # 記録はしておく

        self.features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "id": int(w.id),
                "name": tags.get("name") or "林道(名称不明)",
                "highway": tags.get("highway"),
                "tracktype": tags.get("tracktype"),
                "service": tags.get("service"),
                "surface": tags.get("surface"),
                "access": tags.get("access"),
                "motor_vehicle": tags.get("motor_vehicle"),
                "landuse": tags.get("landuse"),
                "natural": tags.get("natural"),
                "len_m_est": round(length_m, 1),
                "score": score,
                "reasons": reasons,
                "source": "OSM"
            }
        })

# SimpleHandler が動かない環境向けフォールバック
def run_with_reader(pbf_path: pathlib.Path):
    feats = []
    rd = osmium.io.Reader(str(pbf_path))
    for obj in rd:
        if not isinstance(obj, osmium.osm.Way):
            continue
        tags = {t.k: t.v for t in obj.tags}
        ok, score, reasons = score_rindo(tags)
        if not ok and "林道" not in (tags.get("name") or ""):
            continue
        coords = [[n.lon, n.lat] for n in obj.nodes]
        if len(coords) < 2:
            continue
        named_rindo = ("林道" in (tags.get("name") or ""))
        length_m = quick_length(coords)
        if (not named_rindo) and length_m < MIN_LEN_M:
            continue
        feats.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "id": int(obj.id),
                "name": tags.get("name") or "林道(名称不明)",
                "highway": tags.get("highway"),
                "tracktype": tags.get("tracktype"),
                "service": tags.get("service"),
                "surface": tags.get("surface"),
                "access": tags.get("access"),
                "motor_vehicle": tags.get("motor_vehicle"),
                "landuse": tags.get("landuse"),
                "natural": tags.get("natural"),
                "len_m_est": round(length_m, 1),
                "score": score,
                "reasons": reasons,
                "source": "OSM"
            }
        })
    rd.close()
    return feats

# -------------------------
# メイン
# -------------------------
def main():
    print(f"[INFO] Reading PBF: {PBF_PATH}")
    print(f"[INFO] MIN_LEN_M={MIN_LEN_M}, SCORE_THRESHOLD={SCORE_THRESHOLD}")

    try:
        h = RindoHandler()
        h.apply_file(str(PBF_PATH), locations=True)
        feats = h.features
    except TypeError:
        print("[WARN] SimpleHandler failed. Falling back to Reader loop.")
        feats = run_with_reader(PBF_PATH)

    GEOJSON_OUT.write_text(
        json.dumps({"type": "FeatureCollection", "features": feats}, ensure_ascii=False),
        encoding="utf-8"
    )
    print(f"[OK] {len(feats)} features → {GEOJSON_OUT}")
    if feats:
        # 参考: サンプル理由を数件表示
        for f in feats[:5]:
            print("[SAMPLE]", f["properties"]["name"], f["properties"]["score"], f["properties"]["reasons"])

if __name__ == "__main__":
    main()
