#!/usr/bin/env python3
"""Build dist/titles/title_art_index.json - which picture every title should show.

Any page that lists a Player or C.A.M.P. title as a reward (event, activity,
season, raid ... pages) looks the title up in this index through
df-bnb-title-art.js. The index answers one question per title: "what image is
on the server right now?"

For every title in dist/titles_player.json and dist/titles_camp.json:

  1. Check the shared title folders FIRST
       /guide-images/titles/titles-player/<stem>.avif
       /guide-images/titles/titles-camp/<stem>.avif
     where <stem> is the entitlement name with "_ENTM_" dropped (the rule every
     title already on the site uses), then the lower-cased record EDID.
  2. Then the URL build_titles_json.py chose for the Titles checklist (a season
     or mini-season copy), if it has one.
  3. Nothing on the server -> the matching blank name tag:
       Player Title Prefix Blank.avif / Suffix / Prefix-Suffix
       CAMP Title Prefix Blank.avif   / Suffix / Prefix-Suffix
     A CAMP blank that is not uploaded yet falls back to the player blank.

Also writes dist/titles/title_art_missing.json - the to-do list of titles
still on a blank, with the path the real art should be uploaded to.

Every candidate is HEAD-checked against the live site, so the index only ever
points at files the server really serves. A title shows its blank until the
real art is uploaded, and the next run picks the real one up.

Runs monthly (.github/workflows/check-title-art.yml), and can be run by hand
straight after an upload:

  python src/build_title_art_index.py
  python src/build_title_art_index.py --dry-run     # report only, write nothing
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

TAG = "[build_title_art_index]"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DIST = os.path.join(REPO_ROOT, "dist")
DEFAULT_SITE = "https://www.buffsnbrew.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36 dfbnb-title-art-check")

TITLES_ROOT = "/wp-content/uploads/guide-images/titles/"
FOLDER = {"player": "titles-player/", "camp": "titles-camp/"}

# Blank name tags, per title type and affix. Filenames are exactly as uploaded.
BLANK_FILES = {
    "player": {
        "prefix": "Player Title Prefix Blank.avif",
        "suffix": "Player Title Suffix Blank.avif",
        "both":   "Player Title Prefix-Suffix Blank.avif",
    },
    "camp": {
        "prefix": "CAMP Title Prefix Blank.avif",
        "suffix": "CAMP Title Suffix Blank.avif",
        "both":   "CAMP Title Prefix-Suffix Blank.avif",
    },
}


def log(msg):
    print("{} {}".format(TAG, msg))


def url_path(root, filename):
    """Site-relative URL with the filename percent-encoded (the blanks have spaces)."""
    return root + urllib.parse.quote(filename)


def norm_title(s):
    """'Rip Daring's' / "Rip Daring’s" / '"Rip Daring's"' -> 'rip darings'."""
    s = str(s or "").lower().replace("’", "'").replace("‘", "'")
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def affix_of(item):
    p, s = bool(item.get("isPrefix")), bool(item.get("isSuffix"))
    if p and s:
        return "both"
    if p:
        return "prefix"
    if s:
        return "suffix"
    a = str(item.get("affixType") or "").lower()
    if "prefix" in a and "suffix" in a:
        return "both"
    if "prefix" in a:
        return "prefix"
    if "suffix" in a:
        return "suffix"
    return "both"


def candidate_urls(kind, item):
    """Where this title's real art could be, in the order to check it."""
    out = []
    folder = TITLES_ROOT + FOLDER[kind]
    dbg = item.get("debug") or {}
    ents = [dbg.get("imageEntitlementEdid")] + list(dbg.get("entitlementEdids") or [])
    for ent in ents:
        ent = str(ent or "").strip()
        if ent:
            stem = re.sub(r"^zzz+_?", "", ent.lower()).replace("_entm_", "_")
            out.append(folder + stem + ".avif")
    edid = str(item.get("edid") or "").strip()
    if edid:
        out.append(folder + re.sub(r"^zzz+_?", "", edid.lower()) + ".avif")
    given = str(item.get("imageUrl") or "").strip()
    if given:
        out.append(re.sub(r"\.webp$", ".avif", given, flags=re.IGNORECASE))
    seen, uniq = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def head_ok(site, path, timeout=20, attempts=4):
    """True when the live site serves this path as an image (redirects followed).

    Only a real answer from the server counts as "missing" (404/410, or a page
    that is not an image). Timeouts, resets and 5xx are retried, so a flaky
    connection never flips a hosted title back to its blank.
    """
    url = site.rstrip("/") + path
    method = "HEAD"
    for n in range(attempts):
        req = urllib.request.Request(url, method=method, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ctype = (resp.headers.get("Content-Type") or "").lower()
                return resp.status == 200 and ctype.startswith("image/")
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                return False
            if e.code == 405 and method == "HEAD":
                method = "GET"    # server refuses HEAD - ask with GET instead
                continue
        except Exception:
            pass
        time.sleep(1.5 * (n + 1))
    raise RuntimeError("no answer from the server for " + url)


def load_items(dist_dir, name):
    path = os.path.join(dist_dir, name)
    with open(path, encoding="utf-8") as fh:
        return (json.load(fh) or {}).get("items") or []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", default=DEFAULT_DIST)
    ap.add_argument("--site", default=DEFAULT_SITE)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    titles = []   # (kind, item)
    for kind, fname in (("player", "titles_player.json"), ("camp", "titles_camp.json")):
        items = load_items(args.dist, fname)
        log("{}: {} titles".format(fname, len(items)))
        titles.extend((kind, it) for it in items)

    # Every URL to check, once.
    to_check = set()
    for kind, it in titles:
        to_check.update(candidate_urls(kind, it))
    blank_paths = {k: {a: url_path(TITLES_ROOT, f) for a, f in v.items()} for k, v in BLANK_FILES.items()}
    for v in blank_paths.values():
        to_check.update(v.values())

    log("HEAD-checking {} URLs against {}".format(len(to_check), args.site))
    hosted = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(head_ok, args.site, u): u for u in sorted(to_check)}
        for f in concurrent.futures.as_completed(futs):
            hosted[futs[f]] = bool(f.result())   # raises if the site never answered

    # Blanks: CAMP falls back to the player blank of the same affix until uploaded.
    blanks = {}
    for kind in ("player", "camp"):
        blanks[kind] = {}
        for affix in ("prefix", "suffix", "both"):
            own = blank_paths[kind][affix]
            if hosted.get(own):
                blanks[kind][affix] = own
            else:
                blanks[kind][affix] = blank_paths["player"][affix]
                if kind == "player":
                    log("WARNING: player blank not on the server: " + own)

    entries = {}
    real = blank = 0
    missing = []
    for kind, it in titles:
        affix = affix_of(it)
        url = next((u for u in candidate_urls(kind, it) if hosted.get(u)), None)
        is_real = bool(url)
        if not url:
            url = blanks[kind][affix]
            missing.append({"type": kind, "affix": affix, "title": it.get("title"),
                            "edid": it.get("edid"),
                            "expected": (candidate_urls(kind, it) or [""])[0]})
        real += is_real
        blank += not is_real
        rec = {"url": url, "real": is_real, "affix": affix, "edid": it.get("edid") or ""}
        names = {norm_title(it.get(k)) for k in ("title", "titleMale", "titleFemale")}
        for n in names:
            if not n:
                continue
            key = kind + "|" + n
            bucket = entries.setdefault(key, [])
            if not any(r["edid"] == rec["edid"] for r in bucket):
                bucket.append(rec)

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = {
        "generatedAt": now,
        "site": args.site,
        "note": ("Built by src/build_title_art_index.py (monthly). Keys are "
                 "'<player|camp>|<normalised title>'. real=false means the "
                 "blank name tag is showing until the real art is uploaded."),
        "blanks": blanks,
        "counts": {"titles": len(titles), "real": real, "blank": blank},
        "titles": dict(sorted(entries.items())),
    }
    todo = {
        "generatedAt": now,
        "note": ("Titles still showing a blank name tag. Upload the art to the "
                 "'expected' path and the next monthly run switches to it."),
        "count": len(missing),
        "missing": sorted(missing, key=lambda m: (m["type"], m["affix"], str(m["title"]))),
    }
    log("real art: {}  blank: {}".format(real, blank))

    if args.dry_run:
        log("dry run - nothing written")
        return
    out_dir = os.path.join(args.dist, "titles")
    os.makedirs(out_dir, exist_ok=True)
    for name, data, indent in (("title_art_index.json", out, 1),
                               ("title_art_missing.json", todo, 2)):
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=indent, ensure_ascii=False)
            fh.write("\n")
        log("wrote " + path)


if __name__ == "__main__":
    main()
