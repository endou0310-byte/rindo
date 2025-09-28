#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
monitor_agencies.py  v0.6.0
- 入口URLをキューに入れて同一ドメインを限定クロール（max_depth）
- watch が無い場合でも auto_seeds から「林道/通行止/規制」系のリンクを自動発見
- watch_patterns に一致するURLや .html/.pdf/.jpg/.png を発見
- 変更検知: ETag/Last-Modified/length/SHA-1 で unchanged をスキップ
- HTML: 既存の表/リスト抽出 + 本文フォールバック正規表現
- PDF: pdfminer → テキスト薄い場合は OCR（pytesseract + pdf2image）
- 画像(JPG/PNG): OCR→正規表現
- reg_events.json へマージ（pref + pref_code + source_url + updated_at 付与）
"""
import argparse, datetime, hashlib, io, json, re, unicodedata, urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from selectolax.parser import HTMLParser
import re
import unicodedata

def _norm_name_jp(s: str) -> str:
    """林道名 → マッチ用の素朴な正規化（monitor と揃える）"""
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", s)
    # 括弧や余計な記号・スペースを落とし、末尾の「林道/線/支線」などを除去
    t = re.sub(r"[（(].*?[)）]", "", t)
    t = re.sub(r"[　\s・･‐\-—―ｰ]", "", t)
    t = t.replace("森林管理道", "").replace("林道", "")
    t = re.sub(r"(支線|線|せん|道)$", "", t)
    return t

# === Yamanashi helpers ===
import re, unicodedata
from html import unescape

def _yn_norm_name(s: str) -> str:
    """林道名を roads/make_roads と揃う形に正規化"""
    if not s: return ""
    t = unicodedata.normalize("NFKC", s)
    t = unescape(t)
    # まず「林道名の列」から来る余計な語を削除
    t = t.replace("県営","").replace("市営","").replace("町営","").replace("村営","")
    t = t.replace("森林管理道","")
    # カッコ・ルビ等
    t = re.sub(r"【[^】]*】","", t)
    t = re.sub(r"[（(].*?[)）]","", t)
    # 区切り記号や空白
    t = re.sub(r"[・･‐\-—―ｰ／/､,，、\s]+","", t)
    # 接尾語の削除
    t = t.replace("林道","")
    t = re.sub(r"(支線)?線$","", t)
    return t

def _yn_split_names(raw: str) -> list[str]:
    """『小森川／本谷釜瀬・御岳』のようなセルを名前配列に分割"""
    if not raw: return []
    # タグ消し
    val = re.sub(r"(?is)<[^>]+>", " ", raw)
    val = unicodedata.normalize("NFKC", unescape(val))
    # 全角/半角スラッシュ・読点・中黒・空白で分割
    parts = re.split(r"[／/、,，・･\s]+", val)
    parts = [p.strip() for p in parts if p.strip()]
    # 末尾「線」「林道」などは _yn_norm_name 側で落ちる
    return parts

# --- 可搬 PDF テキスト抽出（pdfminerが無ければ空文字） ---
try:
    from pdfminer_high_level import extract_text as pdf_extract_text  # 互換名に合わせたい場合はここで調整
except Exception:  # pragma: no cover
    try:
        from pdfminer.high_level import extract_text as pdf_extract_text  # type: ignore
    except Exception:
        pdf_extract_text = None  # type: ignore

# --- Prefecture master (JIS X 0401, zero-padded 2 digits) ---
PREF_NAME2CODE = {
    "北海道":"01","青森県":"02","岩手県":"03","宮城県":"04","秋田県":"05","山形県":"06","福島県":"07",
    "茨城県":"08","栃木県":"09","群馬県":"10","埼玉県":"11","千葉県":"12","東京都":"13","神奈川県":"14",
    "新潟県":"15","富山県":"16","石川県":"17","福井県":"18","山梨県":"19","長野県":"20","岐阜県":"21",
    "静岡県":"22","愛知県":"23","三重県":"24","滋賀県":"25","京都府":"26","大阪府":"27","兵庫県":"28",
    "奈良県":"29","和歌山県":"30","鳥取県":"31","島根県":"32","岡山県":"33","広島県":"34","山口県":"35",
    "徳島県":"36","香川県":"37","愛媛県":"38","高知県":"39","福岡県":"40","佐賀県":"41","長崎県":"42",
    "熊本県":"43","大分県":"44","宮崎県":"45","鹿児島県":"46","沖縄県":"47",
}
PREF_CODE2NAME = {v: k for k, v in PREF_NAME2CODE.items()}

# ---- 山梨県サイト専用 ------------------------------------------------

YAMANASHI_HOSTS = {"www.pref.yamanashi.jp", "pref.yamanashi.jp"}

def canonical_url(url: str) -> str:
    """#アンカー（#...）と area_id を除去して正規化"""
    u = url.split('#', 1)[0]
    sp = urllib.parse.urlsplit(u)
    q = [(k, v) for (k, v) in urllib.parse.parse_qsl(sp.query, keep_blank_values=True) if k != 'area_id']
    return urllib.parse.urlunsplit((sp.scheme, sp.netloc, sp.path, urllib.parse.urlencode(q, doseq=True), ""))

def _norm_jp(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^(山梨県|県営|県|市町村)?", "", s)  # 軽いノイズ除去
    return s

def _make_event_yamanashi(name: str, status: str, url: str, now_iso: str) -> dict:
    name = _norm_jp(name or "")
    # 「林道」が無いときは末尾に付けてマッチ率を上げる
    if "林道" not in name:
        name = name.strip() + " 林道"
    return {
        "id": hashlib.sha1(f"{name}|{url}".encode("utf-8")).hexdigest()[:16],
        "pref": "山梨県",
        "pref_code": PREF_NAME2CODE["山梨県"],
        "name": name,
        "status": status,  # "closed" / "regulated" / "open"
        "reason": None, "from": None, "to": None,
        "source_url": canonical_url(url),
        "updated_at": now_iso,
    }

# === REPLACE: parse_yamanashi_kisai =========================================
# === Yamanashi (山梨県) ===

import re
from html import unescape

_YMN_NUM_PAT = re.compile(r"(?:\d+(?:\.\d+)?(?:km/?h|kmh|km|m|t|ｔ|％|%)|[０-９]+(?:ｋｍ/ｈ|ｋｍ|ｍ|ｔ))")
_YMN_CLOSED_PAT = re.compile(r"(通行止|全面通行止)")
_YMN_OPEN_PAT = re.compile(r"(規制はありません|規制なし)")
# “規制”キーワード。速度/幅員/重量など数値ルールが出ている場合も規制扱い
_YMN_REG_PAT = re.compile(r"(規制|片側|交互|迂回|う回|チェーン|チェーン規制)")

def _yn_norm_name(s: str) -> str:
    """イベントとマスターで合わせやすい素朴正規化（make_roads_from_master.py と同等）"""
    if not s:
        return ""
    s = unescape(s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[（(].*?[)）]", "", s)  # 括弧内削除
    s = s.replace("－", "-").replace("―", "-").replace("–", "-")
    s = re.sub(r"(森林管理道)", "", s)
    s = re.sub(r"(県営|市営|町営|村営)", "", s)
    s = s.replace("林道", "").replace("支線", "").replace("線", "")
    s = re.sub(r"[（）()・･‐\-—―ｰ\s　]", "", s)
    return s

def _yn_pick_status(page_text: str) -> tuple[str, str]:
    """
    文字列から (status, status_jp) を返す:
      - “通行止”があれば closed
      - “規制はありません”や“規制なし”があれば open
      - 上記以外で “規制” キーワード or 数値(3.6m, 20km/h など) があれば regulated
      - どれでもなければ open
    """
    t = page_text
    if _YMN_CLOSED_PAT.search(t):
        return "closed", "通行止"
    if _YMN_OPEN_PAT.search(t):
        return "open", "規制はありません"
    if _YMN_REG_PAT.search(t) or _YMN_NUM_PAT.search(t):
        return "regulated", "規制"
    return "open", "開放"

def parse_yamanashi_list(html: str, url: str, now_iso: str) -> list[dict]:
    """
    https://www.pref.yamanashi.jp/rindoujyouhou/list.php
    一覧から個別ページへのリンクを取り出す（イベントは作らない）。
    戻り値: {"name": aテキスト, "norm_name": 推定名, "url": 絶対URL} の配列
    """
    from bs4 import BeautifulSoup
    import re
    from html import unescape

    sou = BeautifulSoup(html, "html.parser")
    items = []
    for a in sou.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/rindoujyouhou/kisei\.php\?id=\d+$", href, re.IGNORECASE):
            text = unescape(a.get_text(" ", strip=True))
            name = text
            # aテキストからざっくり路線名を拾う（なくてもOK）
            m = re.search(r"林道\s*([^【\s]+)", text)
            if m:
                name = m.group(1)
            norm_name = re.sub(r"【[^】]*】", "", name)
            norm_name = re.sub(r"[県市]営|森林管理道|林道|支線|線|\s+", "", norm_name)
            items.append({
                "name": text,
                "norm_name": norm_name,
                "url": urllib.parse.urljoin(url, href),
            })
    # 重複除去（URLキー）
    seen = set()
    uniq = []
    for it in items:
        u = it["url"]
        if u in seen: 
            continue
        seen.add(u)
        uniq.append(it)
    return uniq


def parse_yamanashi_kisai(html: str, url: str, now_iso: str) -> list[dict]:
    """
    kisei.php?id=… の詳細ページからイベント生成。
    - '林道名' セルを分割して 1 路線 = 1 イベント
    - 本文から status 判定（通行止 > 規制 > 開放）
    - 必ず norm_name を付与する
    """
    import re
    from html import unescape

    text = unescape(html)

    # --- 1) 林道名セルを抽出 ---
    names_blk = ""
    # <th>林道名</th><td>…</td> を優先
    m = re.search(r"(?is)<th[^>]*>\s*林道名\s*</th>\s*<td[^>]*>(?P<td>.*?)</td>", html)
    if m:
        names_blk = m.group("td")
    else:
        # ラベル＋テキスト形式: 「林道名：…」
        m2 = re.search(r"林道名[^：:]*[:：]\s*(?P<val>.+?)(?:<|[\r\n])", html)
        if m2: names_blk = m2.group("val")

    raw_names = _yn_split_names(names_blk)
    # タイトル等からの最後の保険
    if not raw_names:
        t = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
        if t:
            title = re.sub(r"<[^>]+>", "", t.group(1))
            title = re.sub(r"規制.*$", "", title)
            raw_names = [title.strip()]

    # --- 2) status 判定（通行止 > 規制 > 開放） ---
    plain = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    plain = re.sub(r"(?is)<[^>]+>", " ", plain)
    plain = re.sub(r"\s+", " ", unescape(plain))
    is_closed = re.search(r"(通行止|全面通行止|通年通行止)", plain)
    has_reg = re.search(r"(規制(?!はありません)|片側|交互|う回|迂回|チェーン|徐行|幅員|速度|重量)", plain) \
              or re.search(r"\b\d+(?:\.\d+)?\s*(?:km/?h|㎞/?h|m|t)\b", plain)
    is_open = re.search(r"(規制はありません|規制なし)", plain)

    if is_closed:
        status = "closed"
    elif has_reg and not is_open:
        status = "regulated"
    else:
        status = "open"

    # --- 3) イベント化（必ず norm_name を付与） ---
    events = []
    seen = set()
    for nm in (raw_names or [""]):
        norm = _yn_norm_name(nm)
        if not norm: continue
        key = ("19", norm)
        if key in seen: continue
        seen.add(key)
        events.append({
            "pref": "山梨県",
            "pref_code": "19",
            "name": nm if nm else norm,
            "norm_name": norm,
            "status": status,
            "source_url": url,
            "updated_at": now_iso,
        })
    return events
# ---- 基本設定 ---------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
REG_FILE   = ROOT / "registry" / "agencies.json"
OUT_FILE   = ROOT / "data" / "out" / "reg_events.json"
STATE_FILE = ROOT / "data" / "monitor" / "state.json"
PAGES_DIR  = ROOT / "data" / "monitor" / "pages"
FILES_DIR  = ROOT / "data" / "monitor" / "files"

TIMEOUT = 20
UA = "rindo-monitor/0.6.0"
JST = datetime.timezone(datetime.timedelta(hours=9), "JST")

# ---------- 自動発見のヒント ----------
KEYWORDS_DEF = ["林道", "森林管理道", "通行止", "通行規制", "規制", "解除", "道路", "交通規制", "土砂", "落石", "崩落", "災害"]
PATH_HINTS_DEF = [
    r"/rindou", r"/rindo", r"/rindoujyouhou", r"/forest", r"/rin", r"/ringyo", r"/ringyou",
    r"/road", r"/douro", r"/kisei", r"/bosai", r"/news", r"/oshirase", r"/koho"
]

# ---------- ステータス/理由/期間パターン ----------
P_STATUS = [
    (re.compile(r"(全面|全線)?通行止(め)?"), "通行止"),
    (re.compile(r"(通行規制|規制|片側交互|大型車通行止)"), "規制"),
    (re.compile(r"(解除|通行可|通行可能)"), "解除"),
]
REASONS = ["落石","倒木","崩落","土砂(崩れ|流出)","凍結","積雪","台風","豪雨","地震","崩土","工事","補修","点検","伐採","路肩損傷","路面損傷"]
P_REASON = re.compile("|".join(REASONS))
P_RANGE  = re.compile(r"(?P<from>\d{4}[./-]\d{1,2}[./-]\d{1,2})(?:[^0-9]{0,6})(?:～|~|−|-|—|–|至|まで|から|より)(?P<to>\d{4}[./-]\d{1,2}[./-]\d{1,2}|未定|当面の間|当面|未定)")
P_DATE_SINGLE = re.compile(r"(?P<d>\d{4}[./-]\d{1,2}[./-]\d{1,2})")

# 名前抽出（森林管理道も対象）
P_RINDO_1 = re.compile(r"(林道|森林管理道)(?P<n>[\w０-９0-9一-龥ぁ-んァ-ヶ々ー・･\- \u3000]{2,30}?)(?:線|せん|道)?")
P_RINDO_2 = re.compile(r"(?P<n>[\w０-９0-9一-龥ぁ-んァ-ヶ々ー・･\- \u3000]{2,30}?)(?:林道|森林管理道|林道線)")
NAME_HEAD_RE = re.compile(r"(路線名|林道名|森林管理道名|名称|路線|路線等|路線番号)")
BAD_NAME_SUBSTR = ("路線名","市町村","現在","管理","センター","注意","について","お知らせ")

# ---------- 共通ユーティリティ ----------
def now_iso() -> str:
    return datetime.datetime.now(JST).isoformat(timespec="seconds")

def ensure_pref_fields(ev: dict, pref_name: str) -> dict:
    ev["pref"] = pref_name
    ev["pref_code"] = PREF_NAME2CODE.get(pref_name, "")
    return ev

def sha1b(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding='utf-8-sig'))  # BOM 対応
    except Exception:
        return default

def save_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    # ここを UTF-8 with BOM で保存するように変更（PowerShell が既定で正しく読める）
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _abs(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href)

def _same_domain(url: str, domains: List[str]) -> bool:
    host = urllib.parse.urlparse(url).netloc
    return any(host.endswith(d) for d in domains)

# ---------- 正規化/判定 ----------
def norm_rindo_name(s: str) -> str:
    if not s: return ""
    t = re.sub(r"[　\s]", "", s)
    t = t.replace("林道","").replace("森林管理道","")
    t = re.sub(r"(線|せん|道)$", "", t)
    t = re.sub(r"[（）()・･‐\-—―ｰ]", "", t)
    return t

def pick_status(text: str) -> Optional[str]:
    for rx, lab in P_STATUS:
        if rx.search(text or ""):
            return lab
    return None

def pick_reason(text: str) -> Optional[str]:
    m = P_REASON.search(text or "")
    return m.group(0) if m else None

def pick_range(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = P_RANGE.search(text or "")
    if m:
        return (m.group("from"), m.group("to"))
    m2 = P_DATE_SINGLE.search(text or "")
    return ((m2.group("d"), None) if m2 else (None, None))

def pick_names(text: str) -> List[str]:
    names = set()
    for m in P_RINDO_1.finditer(text or ""):
        names.add(norm_rindo_name(m.group("n")))
    for m in P_RINDO_2.finditer(text or ""):
        names.add(norm_rindo_name(m.group("n")))
    clean = []
    for n in names:
        if not n or len(n) < 2:
            continue
        if any(b in n for b in BAD_NAME_SUBSTR):
            continue
        clean.append(n)
    return clean

# ---------- HTML 抽出（表→リスト→本文） ----------
def harvest_from_html(html: str) -> List[Dict[str, Any]]:
    doc = HTMLParser(html or "")
    events: List[Dict[str, Any]] = []

    # table 優先（名前列推定あり）
    for tbl in doc.css("table"):
        rows = tbl.css("tr")
        if not rows:
            continue
        headers = [ (th.text() or "").strip() for th in rows[0].css("th,td") ]
        name_idx = None
        for i, h in enumerate(headers):
            if NAME_HEAD_RE.search(h or ""):
                name_idx = i
                break
        if name_idx is None:  # 林道語が多い列を推定
            cand = []
            for i in range(min(6, max(1, len(headers) or 1))):
                cnt = 0
                for tr in rows[1:8]:
                    cells = [ (td.text() or "").strip() for td in tr.css("th,td") ]
                    if len(cells) > i and ("林道" in cells[i] or "管理道" in cells[i]):
                        cnt += 1
                cand.append((cnt, i))
            cand.sort(reverse=True)
            if cand and cand[0][0] > 0:
                name_idx = cand[0][1]

        for tr in rows[1:]:
            cells = [ (td.text() or "").strip() for td in tr.css("th,td") ]
            if len(cells) < 2:
                continue
            row_text = " ".join(cells)
            st = pick_status(row_text)
            if not st:
                continue
            nm_raw = ""
            if name_idx is not None and len(cells) > name_idx:
                nm_raw = cells[name_idx]
            if not nm_raw:
                got = pick_names(row_text)
                nm_raw = (got[0] if got else "")
            nm = norm_rindo_name(nm_raw)
            if (not nm) or len(nm) < 2 or any(b in nm for b in BAD_NAME_SUBSTR):
                continue
            f, t = pick_range(row_text)
            events.append({
                "name": nm + "林道",
                "norm_name": nm,
                "status": st,
                "reason": pick_reason(row_text),
                "from": f, "to": t,
                "snippet": row_text[:160]
            })

    # li / p
    for sel in ("li", "p"):
        for node in doc.css(sel):
            line = (node.text() or "").strip()
            if not line:
                continue
            if ("林道" not in line and "管理道" not in line) or not re.search(r"(通行止|規制|解除|通行可|通行可能)", line):
                continue
            st = pick_status(line) or "規制"
            f, t = pick_range(line)
            for n in pick_names(line) or []:
                nm = norm_rindo_name(n)
                if (not nm) or len(nm) < 2 or any(b in nm for b in BAD_NAME_SUBSTR):
                    continue
                events.append({
                    "name": nm + "林道",
                    "norm_name": nm,
                    "status": st,
                    "reason": pick_reason(line),
                    "from": f, "to": t,
                    "snippet": line[:160]
                })

    # 本文行フォールバック
    body = doc.text(separator="\n")
    for line in (body or "").splitlines():
        line = (line or "").strip()
        if not line:
            continue
        if ("林道" not in line and "管理道" not in line) or not re.search(r"(通行止|規制|解除|通行可|通行可能)", line):
            continue
        st = pick_status(line) or "規制"
        f, t = pick_range(line)
        for n in pick_names(line) or []:
            nm = norm_rindo_name(n)
            if (not nm) or len(nm) < 2 or any(b in nm for b in BAD_NAME_SUBSTR):
                continue
            events.append({
                "name": nm + "林道",
                "norm_name": nm,
                "status": st,
                "reason": pick_reason(line),
                "from": f, "to": t,
                "snippet": line[:160]
            })
    return events

# ---------- フォールバック: プレーンテキストから抽出 ----------
def extract_generic_jp(text: str, base_url: str = "", pref: str = "") -> List[Dict[str, Any]]:
    rx = re.compile(
        r"(?P<name>[\u4E00-\u9FFFぁ-んァ-ヶ0-9一二三四五六七八九十〇・\-]+?(?:支)?線).{0,8}?"
        r"(?P<status>通行止|通行規制|片側交互通行|一部通行|う回|迂回|解除|通行可)"
    )
    out = []
    for m in rx.finditer(text or ""):
        raw = m.group("name")
        stj = m.group("status")
        code = ("closed" if "通行止" in stj else
                "regulated" if any(k in stj for k in ("規制","片側交互通行","一部")) else
                "open" if any(k in stj for k in ("解除","通行可")) else "")
        nm = norm_rindo_name(raw)
        if code and nm:
            out.append({
                "pref": pref,
                "name": nm + "林道",
                "norm_name": nm,
                "status": code,
                "reason": None,
                "from": None, "to": None,
                "snippet": (raw + " " + stj)[:160],
                "source_url": base_url
            })
    return out

# ---------- PDF/画像：抽出ヘルパ ----------
def extract_text_from_pdf_bytes(data: bytes) -> str:
    if not pdf_extract_text:
        return ""
    try:
        return (pdf_extract_text(io.BytesIO(data)) or "").strip()
    except Exception:
        return ""

def ocr_bytes_image_to_text(b: bytes) -> str:
    try:
        from PIL import Image
        import pytesseract, io as _io
        img = Image.open(_io.BytesIO(b))
        return pytesseract.image_to_string(img, lang="jpn").strip()
    except Exception:
        return ""

def ocr_pdf_bytes_to_text(b: bytes) -> str:
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
        pages = convert_from_bytes(b, dpi=300)
        txt = "\n".join(pytesseract.image_to_string(p, lang="jpn") for p in pages)
        return txt.strip()
    except Exception:
        return ""

# ---------- HTTP ----------
def conditional_get(client: httpx.Client, url: str, st: Dict[str, Any]) -> Tuple[str, Optional[bytes], Dict[str,str]]:
    headers = {"User-Agent": UA}
    if st.get("etag"): headers["If-None-Match"] = st["etag"]
    if st.get("last_modified"): headers["If-Modified-Since"] = st["last_modified"]
    try:
        r = client.get(url, headers=headers, timeout=TIMEOUT, follow_redirects=True)
    except Exception:
        return "", None, {}
    if r.status_code == 304:
        return ((r.headers.get("content-type") or "").lower(), None, dict(r.headers))
    ct = (r.headers.get("content-type") or "").lower()
    body = r.content
    changed = False
    if r.headers.get("etag") and r.headers.get("etag") != st.get("etag"): changed = True
    elif r.headers.get("last-modified") and r.headers.get("last-modified") != st.get("last_modified"): changed = True
    elif str(len(body)) != str(st.get("length")): changed = True
    elif sha1b(body) != st.get("sha1"): changed = True
    return (ct, body if changed else None, dict(r.headers))

def make_event_id(pref: str, norm_name: str, source_url: str) -> str:
    return hashlib.sha1(f"{pref}|{norm_name}|{source_url}".encode("utf-8")).hexdigest()[:16]

def merge_events(base, new_events, pref, source_url):
    base.setdefault("events", [])
    idx = {e.get("id"): i for i, e in enumerate(base["events"]) if e.get("id")}
    now = now_iso()

    for ev in new_events:
        raw_name = ev.get("name") or ev.get("norm_name") or ""
        norm = ev.get("norm_name") or norm_rindo_name(raw_name)
        if not (raw_name or norm):
            continue

        # ★詳細側URLを優先。ID/保存とも正規化URLで
        src_raw = ev.get("source_url") or source_url
        src = canonical_url(src_raw)

        eid = make_event_id(pref, norm, src)
        pref_code = PREF_NAME2CODE.get(pref)

        payload = {
            "id": eid,
            "pref": pref,
            "pref_code": pref_code,
            "name": raw_name or norm,
            "norm_name": norm,          # ★必ず保存
            "status": ev.get("status"),
            "reason": ev.get("reason"),
            "from": ev.get("from"),
            "to": ev.get("to"),
            "source_url": src,
            "updated_at": now,
        }

        if eid in idx:
            base["events"][idx[eid]].update({k: v for k, v in payload.items() if v is not None})
        else:
            base["events"].append(payload)

    base["updated"] = now
    return base


# ---------- 発見（リンク探索） ----------
def discover_links(html: str, base_url: str, patterns: List[str], crawl: Dict[str, Any], auto: bool=False) -> List[str]:
    doc = HTMLParser(html or "")
    allow_re = [re.compile(p) for p in crawl.get("allow", [])]
    deny_re  = [re.compile(p) for p in crawl.get("deny",  [])]
    patt_re  = [re.compile(p) for p in (patterns or [])]

    kw = crawl.get("keywords") or KEYWORDS_DEF
    hints = [re.compile(h, re.I) for h in (crawl.get("path_hints") or PATH_HINTS_DEF)]

    out: List[str] = []
    for a in doc.css("a[href]"):
        href = a.attributes.get("href") or ""
        u = _abs(base_url, href)
        if deny_re and any(r.search(u) for r in deny_re):  # deny 先
            continue
        if allow_re and not any(r.search(u) for r in allow_re):
            continue

        hit = False
        if patt_re and any(r.search(u) for r in patt_re):
            hit = True
        elif auto:
            text = (a.text() or "")
            if any(k in text for k in kw) or any(h.search(u) for h in hints):
                hit = True

        if hit:
            out.append(u)

    return out

# ---------- メイン ----------
def run(dry_run=False, full=False, save_html=False, save_pdf=False, limit: Optional[int]=None):
    agencies = load_json(REG_FILE, [])
    state = load_json(STATE_FILE, {})
    out    = load_json(OUT_FILE, {"updated": now_iso(), "events": []})

    # 入口URL
    targets = []
    for ag in agencies:
        pref = ag.get("pref") or ""
        watch = ag.get("watch") or []
        seeds = ag.get("auto_seeds") or []
        if watch:
            for u in watch:
                targets.append({"pref": pref, "url": u, "agency": ag, "auto": False})
        elif seeds:
            for u in seeds:
                targets.append({"pref": pref, "url": u, "agency": ag, "auto": True})
    if limit:
        targets = targets[:limit]
    if not targets:
        print("監視対象がありません。registry/agencies.json に 'watch' か 'auto_seeds' を追加してください。")
        return

    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    total_changes = 0
    with httpx.Client(http2=True, headers={"User-Agent": UA}) as client:
        for ti, t in enumerate(targets, 1):
            ag   = t["agency"]
            pref = t["pref"]
            auto = bool(t.get("auto"))
            domains   = ag.get("domains") or []
            patterns  = ag.get("watch_patterns") or []
            crawl     = ag.get("crawl") or {}
            allow_def = [r"\.html?$", r"\.php(?:\?.*)?$", r"\.pdf$", r"\.jpe?g$", r"\.png$"]
            if not crawl.get("allow"):
                crawl["allow"] = allow_def
            max_depth = int(crawl.get("max_depth", 1))
            same_dom  = bool(crawl.get("same_domain", True))

            # BFS キュー
            queue: List[Tuple[str,int]] = [(t["url"], 0)]
            seen: set[str] = set()

            while queue:
                url, depth = queue.pop(0)
                if url in seen:
                    continue
                seen.add(url)

                st = state.get(url, {})
                print(f"[{ti}/{len(targets)}] d={depth}  {url}")

                # 取得
                if full:
                    try:
                        r = client.get(url, timeout=TIMEOUT, follow_redirects=True)
                        ct = (r.headers.get("content-type") or "").lower()
                        body = r.content
                        hdr  = dict(r.headers)
                    except Exception:
                        continue
                else:
                    ct, body, hdr = conditional_get(client, url, st)

                if body is None:
                    print("  └ unchanged")
                    continue

                # スナップショット
                hname = hashlib.sha1(url.encode()).hexdigest()[:10]
                if save_html and "text/html" in ct:
                    (PAGES_DIR / f"{hname}.html").write_bytes(body)
                if save_pdf and "application/pdf" in ct:
                    (PAGES_DIR / f"{hname}.pdf").write_bytes(body)

                events: List[Dict[str,Any]] = []
                more_links: List[str] = []

                # HTML
                if "text/html" in ct:
                    try:
                        html = body.decode("utf-8", "ignore")
                    except Exception:
                        html = body.decode("cp932", "ignore")

                    host = urllib.parse.urlparse(url).netloc
                    if host in YAMANASHI_HOSTS:
                        if url.lower().endswith(".pdf"):
                            events = []  # 山梨のPDFは無視
                        elif host.endswith("pref.yamanashi.jp") and "/rindoujyouhou/kisei.php" in url:
                            events = parse_yamanashi_kisai(html, url, now_iso())
                        else:
                            links = parse_yamanashi_list(html, url, now_iso())
                            if depth < max_depth:
                                for it in links:
                                    u2 = it.get("url")
                                    if u2 and u2 not in seen:
                                        more_links.append(u2)
                            events = []  # 一覧からはイベントを作らない
                    else:
                        events = harvest_from_html(html)

                    if not events:  # フォールバック
                        text = HTMLParser(html).text(separator="\n")
                        events = extract_generic_jp(text, base_url=canonical_url(url), pref=pref)

                    # 次URL enqueue
                    if depth < max_depth:
                        cand = discover_links(html, url, patterns, crawl, auto=auto)
                        for u2 in cand:
                            if same_dom and not _same_domain(u2, domains):
                                continue
                            if u2 not in seen:
                                more_links.append(u2)

                # PDF
                elif "application/pdf" in ct or url.lower().endswith(".pdf"):
                    url = canonical_url(url)
                    save_to = FILES_DIR / (hashlib.sha1(url.encode()).hexdigest()[:16] + ".pdf")
                    save_to.write_bytes(body)
                    txt = extract_text_from_pdf_bytes(body)
                    if len(txt) < 30:  # ほぼ無文字→OCR
                        txt = ocr_pdf_bytes_to_text(body)
                    events = extract_generic_jp(txt, base_url=url, pref=pref)

                # 画像
                elif any(ext in url.lower() for ext in (".jpg",".jpeg",".png")) or "image/" in ct:
                    txt = ocr_bytes_image_to_text(body)
                    events = extract_generic_jp(txt, base_url=canonical_url(url), pref=pref)

                else:
                    print("  └ unsupported content-type:", ct)

                print(f"  └ extracted {len(events)} events")
                if events and not dry_run:
                    out = merge_events(out, events, pref=pref, source_url=url)
                    total_changes += 1

                # state 更新
                state[url] = {"etag": hdr.get("etag"),
                              "last_modified": hdr.get("last-modified"),
                              "length": len(body), "sha1": sha1b(body),
                              "checked_at": now_iso()}

                # 次URL enqueue
                for u2 in more_links:
                    queue.append((u2, depth+1))

    if not dry_run and total_changes:
        save_json(OUT_FILE, out); print("wrote:", OUT_FILE)
    save_json(STATE_FILE, state); print("state:", STATE_FILE, "(updated)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--save-html", action="store_true")
    ap.add_argument("--save-pdf", action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    run(dry_run=args.dry_run, full=args.full, save_html=args.save_html, save_pdf=args.save_pdf, limit=args.limit)
