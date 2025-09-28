# scripts/scrape_yamanashi_registry.py
import re, json, unicodedata, time, html, datetime
from html import unescape
from urllib.parse import urljoin, urlparse, parse_qs
import httpx
from bs4 import BeautifulSoup

BASE = "https://www.pref.yamanashi.jp/rindoujyouhou/"
LIST_URL = urljoin(BASE, "list.php")
OUT = "data/out/yamanashi_registry.json"

def _norm_name(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    s = unicodedata.normalize("NFKC", s)
    # 【フリガナ】や各種角括弧内を除去
    s = re.sub(r"[【［\[\{〔].*?[】］\]\}〕]", "", s)
    # () の括弧内も除去
    s = re.sub(r"[（(].*?[)）]", "", s)
    # 空白・記号の統一
    s = re.sub(r"\s+", "", s)
    s = s.replace("－","-").replace("―","-").replace("–","-")
    # ヶ/ヵのゆらぎ
    s = s.replace("ヶ","ケ").replace("ヵ","カ")
    # ノイズ除去
    s = re.sub(r"(森林管理道)", "", s)
    s = re.sub(r"(県営|市営|町営|村営)", "", s)
    s = re.sub(r"(本線|幹線)$", "", s)
    s = re.sub(r"(支線?)$", "", s)  # “支”単独も落とす
    s = s.replace("林道","").replace("線","")
    # 仕上げ
    s = re.sub(r"[（）()・･‐\-—―ｰ\s　]", "", s)
    return s

# ---- 県ページから規制情報を抽出 -------------------------------------------

def _status_from_text(t: str) -> str:
    """本文テキストから status を推定（closed > regulated > open）"""
    if re.search(r"通行止|全面?通行止|車両通行止", t):
        return "closed"
    if re.search(r"規制|片側交互|時間規制|重量|幅員|迂回|チェーン|一部", t):
        return "regulated"
    if re.search(r"解除|開通|通行可", t):
        return "open"
    return ""  # 不明

def _era_year(ystr: str) -> int:
    """令和/R を西暦に（R1=2019）"""
    if not ystr:
        return datetime.date.today().year
    if ystr.startswith(("令和", "R", "r")):
        n = int(re.sub(r"\D+", "", ystr))
        return 2018 + n
    return int(ystr)

def _pick_dates(t: str):
    """
    ざっくり期間抽出： [年]月日 ～ [年]月日 / “当分の間”は reg_to空
    返り値: (reg_from, reg_to)
    """
    t = t.replace("〜","~").replace("－","-")
    # 2025年9月28日 / 令和7年9月28日 / R7.9.28 / 9月28日
    y = r"(20\d{2}|令和\d+|R\d+)?"
    d = r"(\d{1,2})月(\d{1,2})日"
    m = re.findall(y + d, t)
    reg_from = reg_to = ""
    if m:
        y1, m1, d1 = m[0]
        reg_from = f"{_era_year(y1):04d}-{int(m1):02d}-{int(d1):02d}"
        if len(m) >= 2:
            y2, m2, d2 = m[1]
            reg_to   = f"{_era_year(y2):04d}-{int(m2):02d}-{int(d2):02d}"
    if not reg_to and re.search(r"当分の間|未定", t):
        reg_to = ""
    return reg_from, reg_to

def _pick_reason(t: str) -> str:
    m = re.search(r"(崩落|落石|土砂崩れ|路肩|路面|倒木|凍結|工事|冠水|災害|台風|雪崩|地震|豪雨)", t)
    return m.group(1) if m else ""

def _pick_updated(t: str) -> str:
    m = re.search(r"(最終更新日|更新日|掲載日).{0,10}?(\d{4})年(\d{1,2})月(\d{1,2})日", t)
    if not m:
        return ""
    y, mo, da = int(m.group(2)), int(m.group(3)), int(m.group(4))
    return f"{y:04d}-{mo:02d}-{da:02d}"

def _fetch_text(c: httpx.Client, url: str) -> str:
    r = c.get(url, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    # 表の列や段落テキストをまとめて検索できるようにスペース区切りへ
    return re.sub(r"\s+", " ", soup.get_text(separator=" "))

# --------------------------------------------------------------------------

def main():
    with httpx.Client(follow_redirects=True, timeout=20.0) as c:
        r = c.get(LIST_URL)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # テーブルの「林道名」列から <a href="kisei.php?id=...">名</a> を拾う
        rows = []  # [{id, name, norm, url, ...（後でstatus等を付与）}]
        for a in soup.select('a[href*="kisei.php?id="]'):
            name = (a.get_text() or "").strip()
            href = a.get("href") or ""
            url = urljoin(BASE, href)
            # id 抜き出し
            q = parse_qs(urlparse(url).query)
            _id = (q.get("id") or [""])[0]
            if not _id:
                continue
            norm = _norm_name(name)
            if not norm:
                continue
            rows.append({"id": _id, "name": name, "norm": norm, "url": url})

        # --- 各路線の詳細ページをクロールして status/期間/理由/更新日 を採取 ---
        for row in rows:
            try:
                time.sleep(0.25)  # 優しめに
                txt = _fetch_text(c, row["url"])
                st = _status_from_text(txt)
                if st:
                    row["status"] = st
                fr, to = _pick_dates(txt)
                if fr: row["reg_from"] = fr
                if to: row["reg_to"] = to
                rsn = _pick_reason(txt)
                if rsn: row["reg_reason"] = rsn
                upd = _pick_updated(txt)
                if upd: row["updated_at"] = upd
            except Exception:
                # 失敗は無視して URL だけ残す
                pass

    # 同一normは配列で保持
    by_norm = {}
    for row in rows:
        by_norm.setdefault(row["norm"], []).append(row)

    out = {"source": LIST_URL, "generated": True, "by_norm": by_norm}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[YAMANASHI] registry written: {OUT} (keys={len(by_norm)})")

if __name__ == "__main__":
    main()
