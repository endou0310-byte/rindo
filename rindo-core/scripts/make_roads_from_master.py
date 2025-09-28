#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pathlib
import re
import unicodedata
import os
import html

# rindo-core/scripts/ 配下にある前提
ROOT = pathlib.Path(__file__).resolve().parents[1]
IN_MASTER = ROOT / "data" / "out" / "rindo_master_lines.geojson"
IN_EVENTS = ROOT / "data" / "out" / "reg_events.json"
OUT = ROOT / "data" / "out" / "roads.geojson"
YN_REG = ROOT / "data" / "out" / "yamanashi_registry.json"

# 県名→県コード（マスター側に pref_code が無い/不正時の保険）
PREF_NAME2CODE = {
    "北海道":"01","青森県":"02","岩手県":"03","宮城県":"04","秋田県":"05","山形県":"06","福島県":"07",
    "茨城県":"08","栃木県":"09","群馬県":"10","埼玉県":"11","千葉県":"12","東京都":"13","神奈川県":"14",
    "新潟県":"15","富山県":"16","石川県":"17","福井県":"18","山梨県":"19","長野県":"20","岐阜県":"21",
    "静岡県":"22","愛知県":"23","三重県":"24","滋賀県":"25","京都府":"26","大阪府":"27","兵庫県":"28",
    "奈良県":"29","和歌山県":"30","鳥取県":"31","島根県":"32","岡山県":"33","広島県":"34","山口県":"35",
    "徳島県":"36","香川県":"37","愛媛県":"38","高知県":"39","福岡県":"40","佐賀県":"41","長崎県":"42",
    "熊本県":"43","大分県":"44","宮崎県":"45","鹿児島県":"46","沖縄県":"47",
}

def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = html.unescape(s)
    s = re.sub(r"[【［\[\{〔].*?[】］\]\}〕]", "", s)      # 【フリガナ】等を除去
    s = re.sub(r"[（(].*?[)）]", "", s)
    s = s.replace("－", "-").replace("―", "-").replace("–", "-")
    s = s.replace("ヶ", "ケ").replace("ヵ", "カ")          # ヶ/ヵ を正規化
    s = re.sub(r"(森林管理道)", "", s)
    s = re.sub(r"(県営|市営|町営|村営)", "", s)
    s = re.sub(r"(本線|幹線)$", "", s)
    s = re.sub(r"(支線?)$", "", s)                        # “支”も落とす
    s = s.replace("林道", "").replace("線", "")
    s = re.sub(r"[（）()・･‐\-—―ｰ\s　]", "", s)
    return s

def status_code(jp_or_en: str) -> str:
    """日本語/英語の状態文字列から open / regulated / closed"""
    if not jp_or_en:
        return "open"
    t = str(jp_or_en)
    tl = t.lower()
    if "closed" in tl: return "closed"
    if "regulated" in tl or "restriction" in tl or "traffic control" in tl: return "regulated"
    if "open" in tl or "reopen" in tl: return "open"
    if "通行止" in t: return "closed"
    if any(k in t for k in ["規制", "片側交互通行", "交互", "う回", "一部"]): return "regulated"
    if any(k in t for k in ["解除", "通行可", "復旧"]): return "open"
    return "open"

SEV = {"open": 1, "regulated": 2, "closed": 3}

def load_events(path: pathlib.Path):
    """
    reg_events.json を読み、(pref_code, norm_name) ごとの
      best: 最強(=closed>regulated>open) 代表 1件
      all_by_key: そのキー配下の全件
      by_name: norm_name 単独の索引（県コード不明時の保険）
    を返す。
    """
    data = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {"events": []}
    best, all_by_key, by_name = {}, {}, {}

    for e in data.get("events", []):
        nm = norm_name(e.get("norm_name") or e.get("name") or "")
        pc = str(e.get("pref_code") or "").zfill(2)
        if not nm or not pc:
            continue

        code = status_code(e.get("status") or e.get("status_jp") or "")
        cand = {
            "norm_name": nm,
            "pref_code": pc,
            "status": code,
            "status_jp": str(e.get("status_jp") or ""),
            "source_url": e.get("source_url") or "",
            "updated_at": e.get("updated_at") or "",
        }
        key = f"{pc}|{nm}"

        all_by_key.setdefault(key, []).append(cand)

        cur = best.get(key)
        if (cur is None) or (SEV[cand["status"]] > SEV[cur["status"]]) \
           or (SEV[cand["status"]] == SEV[cur["status"]] and cand["updated_at"] > cur["updated_at"]):
            best[key] = cand

    for v in best.values():
        by_name.setdefault(v["norm_name"], []).append(v)

    return best, by_name, all_by_key

def _normalize_pref_code(pc_raw, pref_name: str, nm: str, by_name: dict) -> str:
    """マスターの pref_code が欠落/不正なときの正規化＋フォールバック。"""
    pc = ("" if pc_raw is None else str(pc_raw)).strip()
    if pc.lower() in ("", "0", "00", "none", "null"):
        pc = ""
    else:
        try:
            pc = f"{int(pc):02d}"
        except Exception:
            pc = ""

    if not pc and pref_name:
        pc = PREF_NAME2CODE.get(pref_name, "") or ""

    if not pc and nm:
        cands = by_name.get(nm) or []
        if len(cands) == 1:
            pc = cands[0].get("pref_code") or ""

    return pc

# === 山梨の“公式リンク＋規制情報”レジストリ（list.php + 各kisei.php をスクレイプ） ===
def load_yamanashi_registry(path: pathlib.Path = YN_REG):
    """
    形式:
    {
      "source": "https://.../list.php",
      "generated": true,
      "by_norm": {
        "小武川": [ {"id":"1","name":"林道 小武川 線","norm":"小武川","url":"https://.../kisei.php?id=1",
                     "status":"regulated/closed/open", "reg_from":"YYYY-MM-DD", "reg_to":"YYYY-MM-DD",
                     "reg_reason":"...", "updated_at":"YYYY-MM-DD"}, ... ],
        ...
      }
    }
    """
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj.get("by_norm") or {}
    except Exception:
        return {}

def main():
    master = json.loads(IN_MASTER.read_text(encoding="utf-8"))
    ev_best, ev_by_name, ev_all = load_events(IN_EVENTS)
    yn_registry = load_yamanashi_registry()  # ★山梨公式リンク+規制情報（norm名→配列）

    feats_out = []
    matched = 0

    for f in master.get("features", []):
        p = f.get("properties", {}) or {}
        raw_name = p.get("name") or p.get("rindo_name") or p.get("display_name") or ""
        nm = norm_name(raw_name)

        pc_raw = p.get("pref_code")
        pref_name = p.get("pref_name") or p.get("pref") or ""
        pc = _normalize_pref_code(pc_raw, pref_name, nm, ev_by_name)

        # まず (pref_code|norm_name) でイベント候補を取得
        key = f"{pc}|{nm}" if (pc and nm) else None
        events = ev_all.get(key, []) if key else []

        # 代表 status 判定用の集計
        reg_cnt = 0
        cls_cnt = 0

        def _is_closed(v: str) -> bool:
            if not v: return False
            t = str(v).lower()
            return ("closed" in t) or ("通行止" in v)

        def _is_reg(v: str) -> bool:
            if not v: return False
            t = str(v).lower()
            if ("regulated" in t) or ("restriction" in t):
                return True
            if any(k in v for k in ("規制","片側","交互","徐行","重量","幅員","速度","チェーン","一部")):
                return True
            return False

        for e in events:
            if pc and str(e.get("pref_code")) != str(pc):
                continue
            st_e = e.get("status") or e.get("status_jp") or ""
            if _is_closed(st_e):
                cls_cnt += 1
            elif _is_reg(st_e):
                reg_cnt += 1

        # フォールバック：名前だけ一致（県コード無視）
        if reg_cnt == 0 and cls_cnt == 0 and nm:
            fb = ev_by_name.get(nm, []) or []
            fb_reg = fb_cls = 0
            for e in fb:
                st_e = e.get("status") or e.get("status_jp") or ""
                if _is_closed(st_e):
                    fb_cls += 1
                elif _is_reg(st_e):
                    fb_reg += 1
            if fb_cls > 0 or fb_reg > 0:
                reg_cnt, cls_cnt = fb_reg, fb_cls
                events = fb
                # 県コード未確定なら、山梨(19)が含まれていれば固定
                if not pc and any(e.get("pref_code") == "19" for e in fb):
                    pc = "19"
                    key = f"{pc}|{nm}"

        # 代表（最悪＝closed > regulated > open）
        rep = ev_best.get(key) if key else None
        if (not rep) and nm:
            for cand in ev_by_name.get(nm, []) or []:
                if (not rep) or (SEV[(cand.get("status") or "open")] > SEV[(rep.get("status") or "open")]) \
                   or (SEV[(cand.get("status") or "open")] == SEV[(rep.get("status") or "open")] and (cand.get("updated_at") or "") > (rep.get("updated_at") or "")):
                    rep = cand

        # 最終 status（ベース：既存イベント）
        st = "closed" if cls_cnt > 0 else ("regulated" if reg_cnt > 0 else "open")

        # --- ★山梨 公式リンク＋規制情報（kisei.php?id=…）の付与／上書き ---
        official = {}
        yn_rows = yn_registry.get(nm) or []
        if yn_rows:
            # URL は必ず付与（先頭を採用）
            y0 = yn_rows[0]
            official["yamanashi"] = {
                "name": y0.get("name") or raw_name,
                "url":  y0.get("url") or "",
                "note": "pref.yamanashi list.php registry match by normalized name"
            }
            # 規制情報は closed > regulated > open の優先で上書き
            yn_status = None
            yn_best = None
            for r in yn_rows:
                s = (r.get("status") or "").lower()
                if not s: continue
                if (yn_status is None) or (SEV.get(s,1) > SEV.get(yn_status,1)):
                    yn_status = s
                    yn_best = r
            if yn_status:
                st = yn_status  # ★県サイトで上書き
                # 補助属性も書き込む
                if yn_best:
                    for k in ("reg_from","reg_to","reg_reason","updated_at"):
                        v = yn_best.get(k)
                        if v: p[k] = v

        # properties 構築
        props = {
            "id": p.get("id"),
            "name": raw_name,
            "norm_name": nm,
            "pref_code": pc,
            "pref_name": pref_name,
            "highway": p.get("highway"),
            "tracktype": p.get("tracktype"),
            "access": p.get("access"),
            "motor_vehicle": p.get("motor_vehicle"),
            "service": p.get("service"),
            "surface": p.get("surface"),
            "status": st,                # ← 色分けはここを見る（closed/regulated/open）
            "regulated": reg_cnt,
            "closed": cls_cnt,
            "status_jp": (rep.get("status_jp") if rep else "") or "",
            "source_url": (rep.get("source_url") if rep else "") or "",
            "source": "OSM",
        }
        if official:
            props["official"] = official  # ← map.html でリンク表示に利用

        # 県サイトから補助属性を付けていたら反映（上で p[...] に入れている）
        for k in ("reg_from","reg_to","reg_reason","updated_at"):
            if p.get(k): props[k] = p[k]

        feats_out.append({
            "type": "Feature",
            "geometry": f.get("geometry"),
            "properties": props,
        })

    OUT.write_text(
        json.dumps({"type": "FeatureCollection", "features": feats_out},
                   ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"[OK] roads.geojson written: {len(feats_out)} features  matched={matched}  → {OUT}")

if __name__ == "__main__":
    main()
