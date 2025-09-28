# rindo-core/scripts/build_agencies_watchlist.py
# 使い方(例):
#   python .\scripts\build_agencies_watchlist.py --seeds .\registry\seeds.saitama.json --watch-top 5 --depth 2 --max-pages 80
#
# 目的:
#   seeds.json で与えた自治体サイトを起点に同一ドメイン(複数可)を軽くクロールし、
#   「通行止め/道路規制/林道」等のキーワードでスコアリングした上位URLを
#   registry/agencies.json に書き出す(既存あればマージ)。
#
# 依存:
#   pip install httpx[http2] selectolax
#
from __future__ import annotations

import argparse
import collections
import dataclasses
import json
import re
import sys
import time
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

ROOT = None  # 実行CWDが rindo-core 前提で相対パスを使う

# -----------------------------
# 設定(キーワード/正規表現)
# -----------------------------
KW_URL = [
    r"rindo", r"rin(d|do)", r"/forest", r"/forestry", r"/ringyo", r"/mori", r"/shinrin",
    r"/road", r"/roads?", r"/kisei", r"/kōtsū", r"/traffic", r"/toshi(d|ke)",
    r"tsūkō", r"tsuukou", r"kou(tsu|do)", r"tuukou", r"通行", r"通行止", r"通行止め",
    r"通行規制", r"交通規制", r"道路", r"林道", r"林業", r"森林", r"お知らせ", r"緊急", r"規制"
]
KW_TEXT = [
    r"林道", r"通行止め?", r"通行規制", r"全面通行止め", r"片側交互通行",
    r"道路情報", r"道路規制", r"通行情報", r"交通規制", r"土砂崩れ", r"落石", r"倒木",
    r"工事", r"災害", r"復旧", r"通行止解除", r"解除", r"規制情報", r"林道情報"
]
RE_URL = re.compile("|".join(KW_URL), re.IGNORECASE)
RE_TEXT = re.compile("|".join(KW_TEXT))
RE_DATE = re.compile(
    r"(20\d{2})\s*[./年-]\s*(\d{1,2})\s*[./月-]\s*(\d{1,2})\s*(?:日)?"
)
RE_PDF = re.compile(r"\.pdf($|\?)", re.IGNORECASE)

# polite
TIMEOUT = httpx.Timeout(15.0, connect=10.0)
HEADERS = {"User-Agent": "rindo-watchlist-builder/1.0 (+local)"}

@dataclasses.dataclass
class Seed:
    pref: str
    agency_type: str  # "prefecture" | "city" | "town" | "village" | ...
    site: str
    city: str | None = None
    extra_domains: list[str] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class Candidate:
    url: str
    score: float
    title: str = ""
    is_pdf: bool = False

def norm_site(u: str) -> str:
    return (u or "").strip()

def host_of(u: str) -> str:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return ""
    return h.lower()

def within_allowed(url: str, allowed: set[str]) -> bool:
    h = host_of(url)
    return (h in allowed) or any(h.endswith("." + d) for d in allowed)

def extract_links(base_url: str, html: str) -> list[str]:
    tree = HTMLParser(html)
    out = []
    for a in tree.css("a"):
        href = (a.attributes.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        absu = urljoin(base_url, href)
        out.append(absu)
    return out

def page_title(html: str) -> str:
    tree = HTMLParser(html)
    t = tree.css_first("title")
    if t and t.text():
        return t.text().strip()
    h1 = tree.css_first("h1")
    return h1.text().strip() if h1 else ""

def page_text(html: str) -> str:
    tree = HTMLParser(html)
    # 不要UIを軽く間引き
    for sel in ("script", "style", "nav", "header", "footer"):
        for n in tree.css(sel):
            n.decompose()
    return tree.text(separator=" ").strip()

def date_recency_bonus(text: str) -> float:
    # 今年～昨年なら+、古いなら控えめ
    bonus = 0.0
    for m in RE_DATE.finditer(text):
        y = int(m.group(1))
        if y >= 2024:
            bonus = max(bonus, 3.0)
        elif y >= 2022:
            bonus = max(bonus, 1.5)
    return bonus

def score_page(url: str, html: str | None) -> Candidate:
    base = 0.0
    url_hit = bool(RE_URL.search(url))
    if url_hit:
        base += 2.0
    is_pdf = bool(RE_PDF.search(url))
    title = ""
    if html:
        title = page_title(html)
        txt = (title + "\n" + page_text(html)) if html else ""
        hits = len(RE_TEXT.findall(txt))
        base += min(hits * 0.8, 8.0)
        if any(k in title for k in ("通行", "規制", "林道", "道路", "お知らせ")):
            base += 1.5
        base += date_recency_bonus(txt)
    else:
        # PDFはURL名のみでざっくり
        base += 1.0
    return Candidate(url=url, score=base, title=title, is_pdf=is_pdf)

def crawl_one(client: httpx.Client, seed: Seed, depth: int, max_pages: int, per_host_limit: int = 80) -> list[Candidate]:
    start = seed.site
    allowed = {host_of(start)} | set(seed.extra_domains)
    q = collections.deque([(start, 0)])
    seen = set()
    per_host = collections.Counter()
    cands: dict[str, Candidate] = {}

    while q and len(seen) < max_pages:
        url, d = q.popleft()
        if url in seen:
            continue
        seen.add(url)
        if not within_allowed(url, allowed):
            continue
        h = host_of(url)
        if per_host[h] >= per_host_limit:
            continue
        per_host[h] += 1

        try:
            r = client.get(url, timeout=TIMEOUT, headers=HEADERS, follow_redirects=True)
        except Exception:
            continue

        ctype = (r.headers.get("content-type") or "").lower()
        html = None
        if "text/html" in ctype or "<html" in r.text[:200].lower():
            html = r.text
            cand = score_page(url, html)
            cands[url] = cand
            if d < depth:
                for link in extract_links(url, html):
                    if link not in seen:
                        q.append((link, d + 1))
        elif "application/pdf" in ctype or RE_PDF.search(url):
            cand = score_page(url, None)
            cands[url] = cand
        else:
            # 他のMIMEは無視
            pass

    # スコアで降順
    return sorted(cands.values(), key=lambda c: c.score, reverse=True)

def read_json(path: str) -> dict | list:
    import pathlib
    p = pathlib.Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def write_json(path: str, data) -> None:
    import pathlib
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def load_seeds(seeds_path: str) -> list[Seed]:
    raw = read_json(seeds_path)
    if not isinstance(raw, list):
        raise SystemExit(f"seeds must be a JSON array: {seeds_path}")
    seeds: list[Seed] = []
    for o in raw:
        seeds.append(Seed(
            pref=o.get("pref"),
            agency_type=o.get("agency_type"),
            site=norm_site(o.get("site")),
            city=o.get("city"),
            extra_domains=o.get("extra_domains") or []
        ))
    return seeds

def merge_into_agencies(existing: list[dict] | None, seeds: list[Seed], results: dict[str, list[Candidate]], watch_top: int) -> list[dict]:
    # キー: (pref, agency_type, city or "")
    def key_of(s: Seed) -> tuple:
        return (s.pref, s.agency_type, s.city or "")
    def key_of_obj(o: dict) -> tuple:
        return (o.get("pref"), o.get("agency_type"), o.get("city") or "")

    out: dict[tuple, dict] = {}
    # 既存を先に
    if isinstance(existing, list):
        for o in existing:
            out[key_of_obj(o)] = o

    # 追加/更新
    for s in seeds:
        k = key_of(s)
        obj = out.get(k) or {
            "pref": s.pref,
            "agency_type": s.agency_type,
            "city": s.city,
            "domains": [],
            "watch": []
        }
        # domains
        new_domains = set(obj.get("domains") or [])
        new_domains.add(host_of(s.site))
        for c in results.get(s.site, []):
            h = host_of(c.url)
            if h:
                new_domains.add(h)
        # watch
        picks = [c.url for c in results.get(s.site, [])][:watch_top]
        new_watch = list(dict.fromkeys((obj.get("watch") or []) + picks))  # 重複排除

        obj["domains"] = sorted(d for d in new_domains if d)
        obj["watch"] = new_watch
        out[k] = obj

    return list(out.values())

def main():
    ap = argparse.ArgumentParser(description="Build agencies.json watch list by crawling seeds and scoring candidate pages.")
    ap.add_argument("--seeds", required=True, help="Path to seeds JSON (array).")
    ap.add_argument("--depth", type=int, default=2, help="Crawl depth (default: 2)")
    ap.add_argument("--max-pages", type=int, default=80, help="Max pages per agency (default: 80)")
    ap.add_argument("--watch-top", type=int, default=5, help="Top-N URLs to add to 'watch' (default: 5)")
    ap.add_argument("--agencies-out", default="registry/agencies.json", help="Output agencies.json path")
    ap.add_argument("--dry-run", action="store_true", help="Do not write agencies.json, just print.")
    args = ap.parse_args()

    seeds = load_seeds(args.seeds)
    if not seeds:
        print("no seeds.")
        return 0

    results: dict[str, list[Candidate]] = {}
    with httpx.Client(http2=True, headers=HEADERS) as client:
        for s in seeds:
            print(f"[crawl] {s.pref} / {s.agency_type} / {s.city or '-'} :: {s.site}")
            try:
                cands = crawl_one(client, s, depth=args.depth, max_pages=args.max_pages)
            except Exception as e:
                print("  ! error:", e)
                cands = []
            # デバッグ出力(上位のみ)
            for c in cands[:10]:
                print(f"    score={c.score:>4.1f}  {'[PDF] ' if c.is_pdf else ''}{c.url}  {(' :: '+c.title) if c.title else ''}")
            results[s.site] = cands
            time.sleep(0.4)  # polite

    existing = read_json(args.agencies_out)
    merged = merge_into_agencies(existing, seeds, results, watch_top=args.watch_top)

    if args.dry_run:
        print("\n--- merged agencies (dry-run) ---")
        print(json.dumps(merged, ensure_ascii=False, indent=2))
    else:
        write_json(args.agencies_out, merged)
        print(f"\n[OK] wrote {args.agencies_out}  (items={len(merged)})")
    return 0

if __name__ == "__main__":
    sys.exit(main())
