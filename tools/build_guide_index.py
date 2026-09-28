#!/usr/bin/env python3
"""Build dist/guide-index.json - a machine-readable list of every PUBLIC guide
page on the DF / BnB site, for Brewmaster (the Discord bot) and anything else
that needs to find a guide without scraping menus.

WHERE EACH FIELD COMES FROM (nothing is hand-typed)
---------------------------------------------------
  Page list, title, category, tags, blurb
      tsv/guide_index.tsv  - the site's own source of truth. The WordPress
      importer in the child theme creates and moves pages from this file, and
      nav.json is built from it.
  Live URL (domain)
      The WordPress REST API root (SITE_URL/wp-json/ -> "home"). Change the
      domain in WordPress and the next build picks it up.
  "updated"
      The page's "modified" date from the WordPress REST API.
  Public or not
      Every URL is requested as a logged-out visitor. Pages that bounce to the
      login screen (still being written / members-only) are LEFT OUT and appear
      automatically once they are made public. 404s are left out and reported.
  "data"
      data/guide_index_data_map.json - which dist/ files each page actually
      loaded when it was opened in a real browser (see --help for re-recording).
      Pages not in that map fall back to the files most pages with the same
      site template use.
  "keywords"
      TSV tags + item / reward names from the page's *_by_page.json data.

OUTPUTS
-------
  dist/guide-index.json          the index the bot reads
  dist/guide-index-report.json   what was skipped and why (gated / 404 / unmapped)

USAGE
-----
  python tools/build_guide_index.py                 # full build + live URL check
  python tools/build_guide_index.py --no-check      # skip the live check (offline test)
  SITE_URL=https://example.com python tools/build_guide_index.py
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV = os.path.join(REPO, "tsv", "guide_index.tsv")
DIST = os.path.join(REPO, "dist")
DATA_MAP = os.path.join(REPO, "data", "guide_index_data_map.json")
OUT = os.path.join(DIST, "guide-index.json")
REPORT = os.path.join(DIST, "guide-index-report.json")

SITE_URL = os.environ.get("SITE_URL", "https://theduchessflame.com").rstrip("/")
UA = "DFBNB-guide-index/1.0 (+https://github.com/DuchessFlame/dfbnb-data)"

# Portal / tooling sections - never public guides.
SKIP_CATEGORIES = {"admin", "member", "staff"}
SKIP_TEMPLATES = {"external-link"}

# Tag tokens that describe page plumbing, not content.
GENERIC_TAGS = {"page", "top", "sub", "guide", "category-hub", "hub", "info-page",
                "template", "checklist-hub"}

# *_by_page.json files: keyed by the page's last URL segment.
BY_PAGE_FILES = [
    "dist/events/events_rewards_by_page.json",
    "dist/activities/activities_rewards_by_page.json",
    "dist/seasonal_events/seasonal_events_rewards_by_page.json",
]

# Data feeds the bot reads directly (not tied to one page).
FEEDS = {
    "calendar": {
        "file": "dist/home-events.json",
        "what": "Event calendar (current + upcoming events, from src/home/events.tsv) "
                "and the Minerva rotation rules (anchor date, location order, cycle).",
    },
    "minerva_plans": {
        "file": "dist/minerva/minerva_plans.json",
        "what": "Minerva's plan lists (what she sells in each list).",
    },
    "axolotl": {
        "file": "dist/axolotl-rotations.json",
        "what": "Monthly axolotl rotation.",
    },
}
RAW_BASE = "https://raw.githubusercontent.com/DuchessFlame/dfbnb-data/refs/heads/main/"


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):  # noqa: D401 - stop at the first hop
        return None


_OPEN = urllib.request.build_opener()
_OPEN_NR = urllib.request.build_opener(_NoRedirect)


def http_get(url: str, follow=True, tries=5):
    """Return (status, headers, body_bytes). Backs off on 429 / 5xx."""
    opener = _OPEN if follow else _OPEN_NR
    wait = 5
    for attempt in range(tries):
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with opener.open(req, timeout=40) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503, 504) and attempt < tries - 1:
                time.sleep(int(e.headers.get("Retry-After") or wait))
                wait = min(wait * 2, 120)
                continue
            return e.code, dict(e.headers or {}), b""
        except Exception:
            if attempt < tries - 1:
                time.sleep(wait)
                wait = min(wait * 2, 120)
                continue
            return -1, {}, b""
    return -1, {}, b""


def site_home() -> str:
    st, _, body = http_get(SITE_URL + "/wp-json/")
    if st == 200:
        try:
            home = json.loads(body).get("home") or ""
            if home.startswith("http"):
                return home.rstrip("/")
        except ValueError:
            pass
    print(f"WARN: could not read {SITE_URL}/wp-json/ - using SITE_URL as-is", file=sys.stderr)
    return SITE_URL


def wp_pages(home: str) -> dict:
    """path -> modified date (YYYY-MM-DD) for every published WP page."""
    out, page = {}, 1
    while True:
        st, hdr, body = http_get(
            f"{home}/wp-json/wp/v2/pages?per_page=100&page={page}&_fields=link,modified")
        if st != 200:
            break
        rows = json.loads(body)
        if not rows:
            break
        for r in rows:
            path = "/" + r["link"].split("://", 1)[-1].split("/", 1)[-1]
            out[path] = (r.get("modified") or "")[:10]
        hl = {k.lower(): v for k, v in hdr.items()}
        total = int(hl.get("x-wp-totalpages") or page)
        if page >= total:
            break
        page += 1
    return out


def check_url(url: str) -> str:
    """ok | login | notfound | redirect:<path> | error:<code>"""
    st, hdr, _ = http_get(url, follow=False)
    if st == 200:
        return "ok"
    if st in (301, 302, 303, 307, 308):
        loc = {k.lower(): v for k, v in hdr.items()}.get("location", "")
        if "wp-login" in loc:
            return "login"
        return "redirect:" + re.sub(r"^https?://[^/]+", "", loc)
    if st in (404, 410):
        return "notfound"
    return f"error:{st}"


# --------------------------------------------------------------------------- #
# Content helpers
# --------------------------------------------------------------------------- #
def first_sentence(text: str) -> str:
    text = " ".join((text or "").split())
    m = re.match(r"(.+?[.!?])(\s|$)", text)
    return (m.group(1) if m else text)[:300]


def tag_keywords(tags: str, template: str) -> list:
    out = []
    for t in (tags or "").split(","):
        t = t.strip().lower()
        if not t or t in GENERIC_TAGS or t == template or t.endswith("-hub"):
            continue
        out.append(t.replace("-", " "))
    return out


def load_json(rel):
    p = os.path.join(REPO, rel)
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def by_page_names() -> dict:
    """slug -> (source file, [item / reward names])"""
    out = {}
    for rel in BY_PAGE_FILES:
        d = load_json(rel)
        if not isinstance(d, dict):
            continue
        d = d.get("byPage", d)
        for slug, entry in d.items():
            names, own = [], (entry.get("name") if isinstance(entry, dict) else "") or ""

            def walk(x):
                if isinstance(x, dict):
                    n = x.get("name")
                    if isinstance(n, str) and n and n != own:
                        names.append(n)
                    for v in x.values():
                        walk(v)
                elif isinstance(x, list):
                    for v in x:
                        walk(v)

            walk(entry)
            out.setdefault(slug, (rel, []))
            out[slug][1].extend(names)
    return out


def clean_name(n: str) -> str:
    n = re.sub(r"^(Plan|Recipe|Player Title|Mod|Diagram):\s*", "", n.strip())
    n = re.sub(r"\s*\[[A-Z]+:[0-9A-F]+\]$", "", n)
    n = n.strip()
    # Drop form IDs (00417C40), editor IDs and fragments like ".44" - not search words.
    if re.fullmatch(r"[0-9A-Fa-f]{6,8}", n) or len(n) < 3 or "_" in n or not re.search(r"[A-Za-z]{2}", n):
        return ""
    return n


_TITLE_NOISE = re.compile(
    r"\b(all rewards|rewards|location guide|locations guide|spawn locations|locations|"
    r"guide|checklist|calculator|how to obtain)\b", re.I)


def title_subject(title: str) -> str:
    """'Brain Fungus Location Guide' -> 'Brain Fungus'; 'Season 25: X - Scoreboard' -> 'Season 25: X'."""
    t = title.split(" - ")[0] if " - " in title else title
    t = _TITLE_NOISE.sub("", t)
    return " ".join(t.replace("–", " ").split()).strip(" :-")


def uniq(seq, limit=None):
    seen, out = set(), []
    for s in seq:
        k = s.lower()
        if s and k not in seen:
            seen.add(k)
            out.append(s)
            if limit and len(out) >= limit:
                break
    return out


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-check", action="store_true", help="skip the live URL check (offline test)")
    ap.add_argument("--rate", type=float, default=3.0, help="max live-check requests per second")
    args = ap.parse_args()

    with open(TSV, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    home = SITE_URL if args.no_check else site_home()
    wp = {} if args.no_check else wp_pages(home)
    data_map = load_json("data/guide_index_data_map.json") or {}
    data_map = data_map.get("pages", data_map)
    names = by_page_names()

    # Fallback data files per site template, learnt from the recorded map.
    tpl_files = collections.defaultdict(collections.Counter)
    tpl_count = collections.Counter()
    tsv_by_url = {r["url"]: r for r in rows}
    for path, files in data_map.items():
        r = tsv_by_url.get(path)
        if r and files:
            tpl_count[r["template"]] += 1
            for fl in files:
                tpl_files[r["template"]][fl] += 1

    def fallback_files(template):
        n = tpl_count.get(template, 0)
        if n < 3:
            return []
        return sorted(fl for fl, c in tpl_files[template].items() if c / n >= 0.8)

    report = {"login_only": [], "not_found": [], "redirected": [], "errors": [],
              "skipped_portal_or_external": [], "not_in_wordpress": [], "unmapped_data": []}
    guides, seen = [], set()
    delay = 1.0 / max(args.rate, 0.1)

    for r in rows:
        url = (r.get("url") or "").strip()
        brand = (r.get("brand") or "").strip().lower()
        cat = (r.get("topCategory") or "").strip()
        if not url.startswith("/") or "#" in url or url in seen:
            if not url.startswith("/") or "#" in url:
                report["skipped_portal_or_external"].append(url)
            continue
        seen.add(url)
        if (r.get("status") or "").lower() != "published" or (r.get("visibility") or "").lower() != "public":
            continue
        if cat.lower() in SKIP_CATEGORIES or (r.get("template") or "") in SKIP_TEMPLATES:
            report["skipped_portal_or_external"].append(url)
            continue
        if brand not in ("df", "bnb"):
            continue

        full = home + url
        if not args.no_check:
            if wp and url not in wp:
                report["not_in_wordpress"].append(url)  # the site can still serve it; checked below
            state = check_url(full)
            time.sleep(delay)
            checked = len(seen)
            if checked % 200 == 0:
                print(f"  checked {checked} URLs...", flush=True)
            if state == "login":
                report["login_only"].append(url); continue
            if state == "notfound":
                report["not_found"].append(url); continue
            if state.startswith("redirect:"):
                report["redirected"].append({"url": url, "to": state[9:]}); continue
            if state != "ok":
                report["errors"].append({"url": url, "result": state}); continue

        template = (r.get("template") or "").strip()
        slug = url.rstrip("/").rsplit("/", 1)[-1]

        files = list(data_map.get(url) or [])
        src, item_names = names.get(slug, (None, []))
        if src and not files:
            files.append(src)
        if not files and url not in data_map:
            files = fallback_files(template)
        if not files and r.get("nodeType") == "page":
            report["unmapped_data"].append(url)

        kw = [title_subject(r.get("title") or "")]
        if (r.get("subCategory") or "").strip():
            kw.append(r["subCategory"].strip())
        kw += tag_keywords(r.get("tags,nodeType") or r.get("tags") or "", template)
        kw += [clean_name(n) for n in item_names]
        kw = uniq(kw, limit=20)

        guides.append({
            "title": (r.get("title") or "").strip(),
            "url": full,
            "site": brand,
            "category": cat or (r.get("subCategory") or "").strip(),
            "type": "hub" if r.get("nodeType") in ("top", "sub") else "guide",
            "keywords": kw,
            "summary": first_sentence(r.get("blurb") or ""),
            "data": sorted(set(files)),
            "updated": wp.get(url) or "",
        })

    out = {
        "generated": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "count": len(guides),
        "site": home,
        "feeds": {k: {**v, "url": RAW_BASE + v["file"]} for k, v in FEEDS.items()},
        "guides": guides,
    }
    os.makedirs(DIST, exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
        f.write("\n")
    report = {"generated": out["generated"], **{k: v for k, v in report.items()}}
    with open(REPORT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
        f.write("\n")

    per_site = collections.Counter(g["site"] for g in guides)
    print(f"guide-index.json: {len(guides)} entries  " + "  ".join(f"{k}={v}" for k, v in sorted(per_site.items())))
    for k in ("login_only", "not_found", "redirected", "errors", "unmapped_data"):
        print(f"  {k}: {len(report[k])}")
    if not guides:
        sys.exit("ERROR: index is empty - refusing to continue")


if __name__ == "__main__":
    main()
