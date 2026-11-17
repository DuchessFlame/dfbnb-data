#!/usr/bin/env python3
r"""
build_terminal_locations.py - the DF / Score Challenges "Level N Terminal Locations"
pages (Duchess, 17 Nov 2026). One page per hacking level, in the Deathclaw Egg
top card + Fixed Spawn Locations look, drawn by df-bnb-farming-non-perishable-guide.js
(data.layout = "locations": no Used For / source expands, the regions ARE the page).

  out: dist/farming_spawns/level-<N>-terminals_spawns.json   (+ the dist/pts/ copy)
  geo: data/terminal_locations/geo/level-<N>-terminals.json
  maps: <Guides and Stuff>\.Score Challenges\Terminal Spawn Locations\Level <N> Terminal Locations\
        -> /wp-content/uploads/guide-images/score-challenges/terminal-spawn-locations/Level-<N>/            (committed; CI needs no DB)
  url: /df/score-challenges/level-<N>-terminal-locations/

WHICH TERMINALS. Every placed TERM reference whose lock (REFR XLOC) is a hacking
level. xEdit's names for the XLOC Level byte, and the in-game level:
    0  "Novice 1"   -> Level 0      50 "Advanced" -> Level 1
    25 "Novice 25"  -> Level 0      75 "Expert"   -> Level 2
                                    100 "Master"  -> Level 3
(251+ = Requires Terminal / Barred / Chained / Inaccessible / Requires Key: not hackable,
not listed). Mappalachia's Position.lockLevel carries exactly this mapping (Level0-3).

Checked 17 Nov 2026 against the live SeventySix.esm (1.7.27.11) read directly:
every hackable terminal in a real cell is in Mappalachia (1.7.26.13) at the same
level; the only ESM-only ones are dev/test cells and the cut Vault 94 raid cells.
Re-check after a patch that adds locations with  --check-esm <scan.json>
(the scan is tools-side: every TERM REFR's XLOC, see the 17 Nov chat).

  python src/build_terminal_locations.py            # all four levels
  python src/build_terminal_locations.py 2          # just Level 2
"""
import collections
import datetime
import json
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import cryptids as C
from spawns_engine import build as ebuild
from spawns_engine.geo import Geo
import challenge_required

DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "farming_spawns")
PTS_OUT_DIR = os.path.join(DIST, "pts", "farming_spawns")
GEO_DIR = os.path.join(REPO, "data", "terminal_locations", "geo")
# Server: guide-images/score-challenges/terminal-spawn-locations/Level-<N>/ (Duchess's
# FileZilla folders, 17 Nov 2026 - capital L, the server is case-sensitive).
MAP_ROOT = "/wp-content/uploads/guide-images/score-challenges/terminal-spawn-locations/"
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", C.MAPPALACHIA_DB)
ALL_REGIONS = C.ALL_REGIONS
APPALACHIA_SPACE = C.APPALACHIA_SPACE
LEVELS = [0, 1, 2, 3]
# Hacking skill comes from three separate Intelligence cards, each "Gain +1 hacking
# skill" (PERK_Export: Hacker01 "Hacker", HackerExpert01 "Hacker Expert",
# HackerMaster01 "Hacker Master"), so Level N needs the first N of them equipped.
PERK_CARDS = ["Hacker", "Hacker Expert", "Hacker Master"]


def perk_line(n):
    if not n:
        return " Anyone can hack these: no perk card needed."
    cards = PERK_CARDS[:n]
    names = cards[0] if n == 1 else ", ".join(cards[:-1]) + " and " + cards[-1]
    return (f" You need the {names} perk card{'s' if n > 1 else ''} equipped to hack one"
            + (" (each card adds 1 to your hacking skill)." if n > 1 else "."))


def page_slug(n):
    return f"level-{n}-terminals"


def resolve(n, geo, cur, cache):
    """{instance: (x, y, region, marker, "terminal")} for one level, filling the geo
    cache from the DB; without a DB, straight from the committed cache."""
    key = page_slug(n)
    seen = {}
    if cur is None:
        for k, e in cache.items():
            inst = k.rsplit(":", 1)[-1]
            if inst.isdigit():
                seen[int(inst)] = (e.get("x"), e.get("y"), e.get("region", ""),
                                   e.get("marker", ""), "terminal")
        return seen
    cache.clear()
    q = ("SELECT p.instanceFormID, p.spaceFormID, p.x, p.y, p.referenceFormID, "
         "e.displayName, e.editorID FROM Position p JOIN Entity e "
         "ON e.entityFormID = p.referenceFormID "
         "WHERE e.signature = 'TERM' AND p.lockLevel = ?")
    for inst, space, x, y, ref, full, edid in cur.execute(q, (f"Level{n}",)):
        region, marker, _ = geo.resolve(space, x, y)
        cache[f"{key}:{inst}"] = {"page": key, "base": ref, "base_edid": edid,
                                  "variant": (full or "").strip() or "Terminal",
                                  "space": space, "x": round(x, 1), "y": round(y, 1),
                                  "region": region, "marker": marker,
                                  "source_type": "terminal", "tier": "fixed"}
        seen[inst] = (round(x, 1), round(y, 1), region, marker, "terminal")
    return seen


def label_regions(regions_out, key, cache):
    """Each terminal block is named after the terminal (its in-game name), numbered
    within that name per marker; the marker gets a "Terminals here:" breakdown."""
    for reg in regions_out:
        for loc in reg["locations"]:
            spawns = loc.get("spawns") or []
            names = []
            for sp in spawns:
                rec = cache.get(f"{key}:{int(sp['ref'], 16)}") or {}
                names.append(rec.get("variant") or "Terminal")
            tot = collections.Counter(names)
            seen = collections.Counter()
            for sp, v in zip(spawns, names):
                seen[v] += 1
                sp["label"] = f"{v} #{seen[v]}" if tot[v] > 1 else v
            loc["breakdown"] = [{"label": v, "count": c, "source_type": "terminal"}
                                for v, c in sorted(tot.items(), key=lambda kv: (-kv[1], kv[0]))]


# ── Used For: every live challenge a Level N terminal counts towards ──────────
# By the challenge's stat (CHAL SNAM): the level's own stat, plus "Computers Hacked",
# which any hacked terminal adds to. Read from dist/challenges/challenges.json.
LEVEL_STATS = {0: "Easy Computers Hacked", 1: "Average Computers Hacked",
               2: "Hard Computers Hacked", 3: "Very Hard Computers Hacked"}
ANY_STAT = "Computers Hacked"
TYPE_ORDER = {"Daily": 0, "Weekly": 1, "Event": 2, "Pioneer Scouts": 4, "Lifetime": 5}


def _walk(items, parent=None):
    for c in items or []:
        if isinstance(c, dict):
            yield c, parent
            yield from _walk(c.get("children"), c)


def used_for(n):
    path = os.path.join(DIST, "challenges", "challenges.json")
    try:
        pages = json.load(open(path, encoding="utf-8")).get("pages") or {}
    except Exception as e:
        print(f"  [warn] no challenges.json ({e}); Used For left empty")
        return []
    hits = {}
    for pk, pl in pages.items():
        if pk == "cut":
            continue
        items = pl if isinstance(pl, list) else (pl.get("challenges") or pl.get("items") or [])
        for c, parent in _walk(items):
            snam = c.get("snam") or ""
            if c.get("is_cut") or snam not in (LEVEL_STATS[n], ANY_STAT):
                continue
            name = c.get("full") or c.get("edid")
            season = challenge_required.season_label(c.get("edid"))
            if pk.startswith("scouts-"):
                typ = "Pioneer Scouts"
                if parent and parent.get("full"):
                    name = f"{parent['full']} - {name}"
            else:
                typ = c.get("scope") or pk.title()
            key = c.get("edid") or c.get("form_id")
            if key in hits:
                continue
            hits[key] = {"type": typ, "name": name, "required": c.get("required") or 1,
                         "season": season, "edid": c.get("edid", ""), "page": pk}
    rows = list(hits.values())
    # One line per count: the nine lifetime "Hack Terminals" tiers stay nine lines.
    rows.sort(key=lambda r: (TYPE_ORDER.get(r["type"], 9), r["name"].lower(), r["season"],
                             int(r["required"]) if str(r["required"]).isdigit() else 0))
    return rows


def existing_slots(path):
    keep = {}
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in doc.get("regions", []):
        for loc in reg.get("locations", []):
            k = (reg.get("region"), loc.get("marker"))
            keep[k] = {f: loc.get(f, "") for f in ("image_top", "directions", "image_bottom")}
            keep[k]["spawns"] = {sp["ref"]: {f: sp.get(f, "") for f in
                                             ("image_top", "directions", "image_bottom")}
                                 for sp in loc.get("spawns", []) if sp.get("ref")}
    return keep, doc


def build_one(n, geo, cur):
    key = page_slug(n)
    out_path = os.path.join(OUT_DIR, f"{key}_spawns.json")
    geo_path = os.path.join(GEO_DIR, f"{key}.json")
    prev = existing_slots(out_path)
    keep, old = prev if prev else ({}, {})
    cache = ebuild.load_cache(geo_path)
    seen = resolve(n, geo, cur, cache)
    if cur is not None:
        ebuild.save_cache(cache, geo_path)

    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep, exclude_types=())
    label_regions(regions_out, key, cache)

    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": datetime.date.today().isoformat(),
                    "source": "Mappalachia Position table, TERM refs by lockLevel (cached in "
                              "data/terminal_locations/geo/), checked against SeventySix.esm "
                              "XLOC (src/build_terminal_locations.py)",
                    "source_totals": src_totals, "unresolved": unresolved}
    doc["set"] = "terminals"
    doc["layout"] = "locations"
    doc["slug"] = key
    doc["name"] = f"Level {n} Terminals"
    doc["page_title"] = f"Score Challenges - Level {n} Terminal Locations"
    perk = perk_line(n)
    doc["intro_note"] = (f"Every Level {n} terminal in Fallout 76, grouped by region and then by "
                         f"location. Use them for any challenge that asks you to hack a skill "
                         f"level {n} terminal.{perk}")
    doc["count_noun"] = ["terminal", "terminals"]
    doc["count_subject"] = f"Level {n}"
    doc["section_label"] = f"Level {n} terminal locations"
    doc["breakdown_lead"] = "Terminals here:"
    doc["in_place_caption"] = "Terminal in place"
    doc["empty_region_note"] = f"There are no Level {n} terminals in this region."
    doc["map_base"] = MAP_ROOT + f"Level-{n}/"
    doc["map_ext"] = ".jpg"
    doc["maps_expand"] = True
    doc["map_labels"] = {"full": "Full 4K terminal map", "full_desc": f"Every Level {n} terminal",
                         "region_desc": f"Level {n} terminals"}
    # Stamped by finish_terminal_maps.py once the maps are made; kept across rebuilds.
    for k in ("full_map", "map_regions", "interior_maps"):
        doc[k] = old.get(k) or ([] if k != "full_map" else "")
    doc["used_for"] = {"challenges_heading": "Challenges", "challenges": used_for(n),
                       "recipes": None}
    doc["regions"] = regions_out

    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    for d in (OUT_DIR, PTS_OUT_DIR):
        os.makedirs(d, exist_ok=True)
        open(os.path.join(d, os.path.basename(out_path)), "w", encoding="utf-8").write(txt)
    ok = all(len(l.get("spawns") or []) == l["count"] for r in regions_out for l in r["locations"])
    per = {r["region"]: sum(l["count"] for l in r["locations"]) for r in regions_out if r["locations"]}
    print(f"  level {n}: terminals:{placements:>4} markers:{total:>3} "
          f"unresolved:{sum(unresolved.values())} slots-ok:{ok}  {per}")
    if unresolved:
        print("    unresolved:", dict(unresolved))
    return out_path


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    want = [int(a) for a in argv if a.isdigit()] or LEVELS
    geo = con = cur = None
    if os.path.exists(MAPPALACHIA_DB):
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)
        cur = con.cursor()
    else:
        print("[terminals] no Mappalachia DB - rebuilding from the committed geo caches")
    for n in want:
        build_one(n, geo, cur)
    if con:
        con.close()


if __name__ == "__main__":
    main()
