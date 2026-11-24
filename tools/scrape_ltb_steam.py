#!/usr/bin/env python3
r"""
tools/scrape_ltb_steam.py - catch new Fallout 76 Limited Time Bundles from Steam.

Limited Time Bundles are real-money platform DLC. They are NOT in SeventySix.esm or
Fallout76.exe as bundles (only their contents are, as ENTM records), so the game
exports can't find them. Steam lists every bundle on sale as DLC of Fallout 76
(app 1151340), with its contents in the store text, so this reads that.

  1. Steam appdetails for 1151340 -> the DLC app ids on sale right now.
  2. appdetails for each DLC -> name, release date, contents.
  3. Each content line is matched to its ENTM record (newest tsv/ENTM_Export_*.tsv)
     by name, so it gets the real formId / edid / picture name.
  4. New bundles are ADDED to data/ltb_found.json. Nothing is ever removed:
     Steam drops a bundle when it leaves the store, but the page keeps it.

build_atom_shop_json.py merges data/ltb_found.json into dist/atom_shop.json
(data["ltb"]), skipping any bundle already in its hand-written LTB_BUNDLES list.
Run daily by .github/workflows/scrape-ltb.yml.

  python tools/scrape_ltb_steam.py            # update data/ltb_found.json
  python tools/scrape_ltb_steam.py --dry-run  # print what it would add

Steam only covers the PC release. A bundle sold only on PlayStation or Xbox will
not show up here; add those to LTB_BUNDLES in build_atom_shop_json.py by hand.
"""
import argparse
import csv
import datetime
import glob
import html
import json
import os
import re
import sys
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(REPO, "data", "ltb_found.json")
LISTING = os.path.join(REPO, "data", "server_listing.tsv")
GAME_APP = 1151340
API = "https://store.steampowered.com/api/appdetails?appids={}&cc=us&l=english"
ATOMS_IMG = "/wp-content/uploads/season_images/utility/score_currency_atoms.avif"
REPAIR_IMG = "/wp-content/uploads/season_images/utility/atx_utility_repairkit_basic.avif"
SCRAP_IMG = "/wp-content/uploads/season_images/utility/atx_utility_scrapkit_basic.avif"
TAG = "[ltb-steam]"

# Words that describe a store listing rather than name the item.
GENERIC = {"set", "outfit", "headwear", "and", "the", "a", "collector", "bundle",
           "paint", "skin", "plus", "with", "your", "own", "pack", "kit"}


def get_json(url, tries=3):
    for n in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "dfbnb-data ltb scraper"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as exc:  # Steam rate-limits now and then
            if n == tries - 1:
                raise
            print(f"{TAG} retry {url}: {exc}")
            time.sleep(5 * (n + 1))


def norm(s):
    s = (s or "").lower().replace("armour", "armor")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def clean_name(name):
    """'Fallout 76: Mojave Bundle' / 'Fallout 76 - Deathclaw Pet Bundle' -> 'Mojave Bundle'."""
    return re.sub(r"^\s*fallout\s*76\s*[:\-–—]\s*", "", name or "", flags=re.I).strip()


def parse_date(s):
    for fmt in ("%d %b, %Y", "%b %d, %Y", "%d %B, %Y", "%B %d, %Y"):
        try:
            return datetime.datetime.strptime((s or "").strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return ""


# ------------------------------------------------------------------ ENTM

def newest_entm():
    files = glob.glob(os.path.join(REPO, "tsv", "ENTM_Export_*.tsv"))
    try:
        sys.path.insert(0, os.path.join(REPO, "src"))
        import tsv_source
        p = tsv_source.newest("ENTM_Export_*.tsv", required=False)
        if p:
            return p
    except Exception:
        pass
    months = {m: i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July",
                                          "August", "September", "October", "November", "December"], 1)}

    def key(p):
        m = re.search(r"ENTM_Export_([A-Za-z]+)_(\d{4})", os.path.basename(p))
        return (int(m.group(2)), months.get(m.group(1), 0)) if m else (0, 0)
    return max(files, key=key) if files else None


def load_entm():
    path = newest_entm()
    rows = []
    if not path:
        print(f"{TAG} WARNING: no ENTM export found; items will have no edid/picture")
        return rows
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if not edid or edid.lower().startswith(("zzz", "del", "cut")):
                continue
            rows.append({"formId": (r.get("FormID") or "").strip(), "edid": edid,
                         "name": (r.get("FULL") or "").strip(), "etdi": (r.get("ETDI") or "").strip(),
                         "desc": re.sub(r"\s*-\s*[A-Z .,'!]{12,}-?\s*$", "", (r.get("DESC") or "").strip()).strip(),
                         "n": norm(r.get("FULL")), "flat": norm(r.get("FULL")).replace(" ", "")})
    print(f"{TAG} ENTM: {os.path.basename(path)} ({len(rows)} records)")
    return rows


def entm_item(r):
    img = re.sub(r"\.dds$", ".avif", r["etdi"], flags=re.I) if r["etdi"] else ""
    return {"name": r["name"], "desc": r.get("desc", ""), "formId": r["formId"], "edid": r["edid"], "imageUrl": img}


def match_items(line, entm):
    """A store line -> one or more ENTM items. Returns [] when nothing matches."""
    # Player titles: 'Player Title Prefix – "Advictoriam"' -> the PlayerTitles ENTM with that word.
    t = re.search(r"player title.*?[\"\u201c\u201d']([^\"\u201c\u201d']+)[\"\u201c\u201d']", line, re.I)
    if t:
        w = norm(t.group(1)).replace(" ", "")
        hits = [r for r in entm if "playertitles" in r["edid"].lower() and w and w in r["edid"].lower().replace("_", "")]
        hits.sort(key=lambda r: (not r["edid"].startswith("ATX_"), r["edid"]))
        if hits:
            it = entm_item(hits[0])
            it["name"] = 'Player Title: "' + t.group(1).strip() + '"'
            return [it]
    n = norm(line)
    exact = [r for r in entm if r["n"] == n]
    if not exact and "(" in line:   # "Ad Victoriam (Super Sledge)" -> "Ad Victoriam"
        n2 = norm(re.sub(r"\(.*?\)", "", line))
        exact = [r for r in entm if r["n"] == n2]
    if exact:
        exact.sort(key=lambda r: (not r["edid"].startswith("ATX_"), r["edid"]))
        return [entm_item(exact[0])]
    # "Ulysses Outfit & Headwear set" -> every ENTM named "Ulysses ..."
    words = [w for w in re.split(r"\s+", re.sub(r"[^A-Za-z0-9\s]", "", line)) if w]
    core = [w.lower() for w in words if w.lower() not in GENERIC]
    if not core:
        return []
    head = core[0]
    if len(head) < 4:
        return []
    hits = [r for r in entm if head in r["flat"].split() or r["flat"].startswith(head)
            or (" " + head) in (" " + r["n"])]
    hits = [r for r in hits if r["edid"].startswith(("ATX_", "SCORE_")) and "_ENTM_" in r["edid"]]
    # the other distinctive words must be in the name too
    rest = [w for w in core[1:] if len(w) > 3]
    if rest:
        tighter = [r for r in hits if all(w in r["n"] for w in rest)]
        hits = tighter or hits
    if not hits or len(hits) > 8:
        return []
    seen, out = set(), []
    for r in sorted(hits, key=lambda r: r["name"]):
        if r["name"] in seen:
            continue
        seen.add(r["name"])
        out.append(entm_item(r))
    return out


# ------------------------------------------------------------------ store text

def content_lines(desc_html, short):
    """The bundle's contents as plain lines."""
    t = re.sub(r"(?i)<\s*(br|/p|/li|/h\d|li|p|h\d)[^>]*>", "\n", desc_html or "")
    t = html.unescape(re.sub(r"<[^>]+>", " ", t))
    lines = [re.sub(r"\s+", " ", l).strip(" -–•*") for l in t.split("\n")]
    lines = [l for l in lines if l]
    items = [l for l in lines if len(l) <= 70 and not l.endswith((".", "!", ":"))]
    if len(items) >= 2:
        return items
    # Prose only: "Purchase 800 Atoms (+300 bonus) and get the X, plus the Y."
    prose = html.unescape(re.sub(r"<[^>]+>", " ", (desc_html or "") + " " + (short or "")))
    out = []
    m = re.search(r"(\d[\d,]*)\s*Atoms\s*\(\s*\+\s*(\d[\d,]*)\s*bonus\s*\)", prose, re.I)
    if m:
        out.append(f"{m.group(1)} (+{m.group(2)} Bonus) Atoms")
    m = re.search(r"get the (.+?)(?:\.|$)", prose, re.I)
    if m:
        for part in re.split(r",\s*plus the\s+|,\s*and the\s+|\s+and the\s+|,\s*the\s+|\s+plus the\s+", m.group(1)):
            part = part.strip(" ,")
            if part:
                out.append(part)
    return out


def build_bundle(appid, d, entm):
    name = clean_name(d.get("name"))
    items = []
    for line in content_lines(d.get("detailed_description"), d.get("short_description")):
        low = line.lower()
        if re.search(r"\batoms?\b", low) and re.search(r"\d", low):
            items.append({"name": line, "desc": "In-game currency for the Atomic Shop.", "formId": "", "edid": "",
                          "imageUrl": ATOMS_IMG})
            continue
        if "repair kit" in low or "scrap kit" in low:
            items.append({"name": line, "desc": "", "formId": "", "edid": "",
                          "imageUrl": REPAIR_IMG if "repair" in low else SCRAP_IMG})
            continue
        found = match_items(line, entm)
        if found:
            items.extend(found)
        else:
            items.append({"name": line, "desc": "", "formId": "", "edid": "", "imageUrl": ""})
    return {
        "id": slug(name),
        "name": name,
        "released": parse_date((d.get("release_date") or {}).get("date")),
        "update": None,
        "platforms": ["Steam", "PlayStation", "Xbox"],
        "imageUrl": "",
        "source": "steam",
        "steamAppId": int(appid),
        "items": items,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    found = {"bundles": []}
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as fh:
            found = json.load(fh)
    have_ids = {b.get("steamAppId") for b in found["bundles"]} | {b.get("id") for b in found["bundles"]}
    # Bundles already written by hand in build_atom_shop_json.py (LTB_BUNDLES) are skipped too.
    try:
        with open(os.path.join(REPO, "src", "build_atom_shop_json.py"), encoding="utf-8") as fh:
            src = fh.read()
        have_ids |= set(re.findall(r'"id":\s*"([^"]+)"', src))
        have_ids |= {slug(n) for n in re.findall(r'"name":\s*"([^"]*Bundle[^"]*)"', src)}
    except OSError:
        pass

    game = get_json(API.format(GAME_APP))[str(GAME_APP)]
    dlc = (game.get("data") or {}).get("dlc") or []
    print(f"{TAG} Steam lists {len(dlc)} Fallout 76 DLC on sale: {dlc}")
    entm = None
    added = []
    for appid in dlc:
        if appid in have_ids:
            continue
        time.sleep(1.5)
        resp = get_json(API.format(appid)).get(str(appid)) or {}
        d = resp.get("data") or {}
        if not resp.get("success") or d.get("type") != "dlc":
            continue
        if entm is None:
            entm = load_entm()
        b = build_bundle(appid, d, entm)
        if b["id"] in have_ids or slug(b["name"]) in have_ids:
            continue
        added.append(b)
        print(f"{TAG} NEW  {b['name']} ({b['released']}) - {len(b['items'])} items")
        for i in b["items"]:
            print(f"{TAG}        {i['name']:45} {i['edid'] or '(no ENTM match)':55} {i['imageUrl']}")

    if not added:
        print(f"{TAG} nothing new")
        return
    if args.dry_run:
        return
    found["bundles"].extend(added)
    found["updated"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(found, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(f"{TAG} wrote {len(added)} new bundle(s) to {os.path.relpath(OUT, REPO)}")

    # Pictures the page will ask for that aren't on the server yet.
    listed = set()
    if os.path.exists(LISTING):
        with open(LISTING, encoding="utf-8") as fh:
            listed = {l.split("\t")[0].split("/bundle-images/")[-1] for l in fh if "/bundle-images/" in l}
    need = sorted({i["imageUrl"] for b in added for i in b["items"]
                   if i["imageUrl"] and not i["imageUrl"].startswith("/") and i["imageUrl"] not in listed})
    if need:
        print(f"{TAG} PICTURES NEEDED in guide-images/atom-shop/bundle-images/:")
        for f in need:
            print(f"{TAG}   {f}")

    # data/ltb_alerts.json: one entry per new bundle, for Mia to pick up off GitHub and tell
    # Kat about (what was added, which pictures to make/upload). Mia remembers which ids she
    # has already told Kat about, so entries are never removed here.
    alerts_path = os.path.join(REPO, "data", "ltb_alerts.json")
    alerts = {"alerts": []}
    if os.path.exists(alerts_path):
        with open(alerts_path, encoding="utf-8") as fh:
            alerts = json.load(fh)
    for b in added:
        pics = sorted({i["imageUrl"] for i in b["items"]
                       if i["imageUrl"] and not i["imageUrl"].startswith("/") and i["imageUrl"] not in listed})
        alerts["alerts"].append({
            "id": b["id"], "name": b["name"], "released": b["released"], "steamAppId": b["steamAppId"],
            "found": found["updated"], "items": [i["name"] for i in b["items"]],
            "unmatched": [i["name"] for i in b["items"] if not i["edid"] and not i["imageUrl"]],
            "picturesNeeded": pics, "uploadTo": "/wp-content/uploads/guide-images/atom-shop/bundle-images/",
            "page": "https://theduchessflame.com/df/atom-shop/limited-time-bundles/",
        })
    with open(alerts_path, "w", encoding="utf-8") as fh:
        json.dump(alerts, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


if __name__ == "__main__":
    main()
