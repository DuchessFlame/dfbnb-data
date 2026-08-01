#!/usr/bin/env python3
r"""
build_nuka_cola_spawns_json.py — generate the per-variant spawn JSON for the
Nuka Cola "{Type} Locations" pages on buffsnbrew.com.

Generative: reads placements straight from the Mappalachia DB (no xEdit export
step needed for these items), resolves each placement to a region + nearest map
marker using the SAME geometry the collectables pipeline uses
(crossref_mappalachia_markers), groups by region, and writes:

    dist/nuka_cola_spawns_<slug>.json      (one per variant)
    dist/nuka_cola_spawns_manifest.json    (index of all variants)

MERGE, DON'T CLOBBER: image_top / directions / image_bottom are hand-authored.
On every rebuild the existing dist file is loaded first and any non-empty
hand-filled field is preserved for a matching (region, marker) key. Only the
spawn set / counts / coords are refreshed. New spawns appear automatically with
empty slots for you to fill; removed spawns drop out.

Membership + FormIDs live in nuka_cola_spawns_config.py — edit that, not this.

Env:
    MAPPALACHIA_DB   default D:\Mappalachia\data\mappalachia.db (same as the other builders)

Usage:
    python src/build_nuka_cola_spawns_json.py            # all variants
    python src/build_nuka_cola_spawns_json.py wild dark  # only these slugs (bare or full)
"""

import os, sys, json, sqlite3, datetime
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from nuka_cola_spawns_config import VARIANTS, ALL_REGIONS, APPALACHIA_SPACE
# Reuse the exact region/marker geometry the collectables pipeline uses.
from crossref_mappalachia_markers import (
    load_mappalachia, region_for_xy, nearest_marker, MARKER_REGION_OVERRIDES,
)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
SOURCE_TAG = "Mappalachia DB (Position table, direct pull)"


def pull_positions(cur, formids):
    """Every placed instance of the given base FormIDs in the Appalachia worldspace.

    Position.referenceFormID = the base object (our leveled item);
    Position.instanceFormID  = the unique placed REFR (kept as provenance).
    """
    if not formids:
        return []
    q = ("SELECT x, y, z, instanceFormID FROM Position "
         "WHERE spaceFormID=? AND referenceFormID IN (%s)" % ",".join("?" * len(formids)))
    return cur.execute(q, (APPALACHIA_SPACE, *formids)).fetchall()


def resolve_xy(rings, markers, x, y):
    """(region, marker) for an exterior Appalachia placement — mirrors crossref.resolve()."""
    region = region_for_xy(rings, x, y)
    marker, mx, my = nearest_marker(markers, x, y)
    if not region and mx is not None:
        region = region_for_xy(rings, mx, my)
    if not region and marker in MARKER_REGION_OVERRIDES:
        region = MARKER_REGION_OVERRIDES[marker]
    return region, marker


def load_existing(path):
    """Map (region, marker) -> {image_top, directions, image_bottom} for hand-authored preserve."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in old.get("regions", []):
        for loc in reg.get("locations", []):
            key = (reg.get("region", ""), loc.get("marker", ""))
            keep[key] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
            }
    return keep


def build_variant(v, rings, markers, cur, generated):
    path = os.path.join(DIST, f"nuka_cola_spawns_{v['slug'].replace('-locations','')}.json")
    keep = load_existing(path)

    # group placements -> (region, marker)
    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None})
    unresolved = 0
    for x, y, z, ref in pull_positions(cur, v["source_formids"]):
        region, marker = resolve_xy(rings, markers, x, y)
        if not region and not marker:
            unresolved += 1
            continue
        g = grouped[(region, marker)]
        g["count"] += 1
        if ref is not None:
            g["refs"].append(f"{int(ref):06X}")
        if g["coords"] is None:
            g["coords"] = [round(x, 1), round(y, 1)]

    # bucket by region, keep all ten
    by_region = defaultdict(list)
    for (region, marker), g in grouped.items():
        region = region if region in ALL_REGIONS else region  # (blank stays blank -> unlisted)
        prev = keep.get((region, marker), {})
        by_region[region].append({
            "marker": marker,
            "count": g["count"],
            "image_top": prev.get("image_top", ""),
            "directions": prev.get("directions", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "refs": sorted(set(g["refs"])),
            "coords": g["coords"],
        })

    regions_out = []
    for region in ALL_REGIONS:
        locs = sorted(by_region.get(region, []), key=lambda l: l["marker"].lower())
        regions_out.append({"region": region, "locations": locs})

    total = sum(len(r["locations"]) for r in regions_out)
    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "source_formids": v["source_formids"], "unresolved": unresolved},
        "set": v["slug"].replace("-locations", ""),
        "name": v["name"],
        "page_title": f"{v['name']} Locations",
        "blurb": v["blurb"],
        "regions": regions_out,
    }
    os.makedirs(DIST, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"set": doc["set"], "name": v["name"], "slug": v["slug"], "file": os.path.basename(path),
            "locations": total, "unresolved": unresolved,
            "has_source": bool(v["source_formids"])}


def main(argv):
    wanted = {a.replace("-locations", "") for a in argv[1:]}
    variants = [v for v in VARIANTS if not wanted or v["slug"].replace("-locations", "") in wanted]

    need_db = any(v["source_formids"] for v in variants)
    rings = markers = None
    con = cur = None
    if need_db:
        if not os.path.exists(MAPPALACHIA_DB):
            sys.exit(f"[nuka-cola] Mappalachia DB not found: {MAPPALACHIA_DB}")
        rings, markers = load_mappalachia()
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()

    generated = datetime.date.today().isoformat()
    manifest = []
    for v in variants:
        info = build_variant(v, rings, markers, cur, generated)
        manifest.append(info)
        flag = "" if info["has_source"] else "  (no source — hand-author)"
        print(f"  {info['file']:<46} {info['locations']:>3} locations"
              f"{('  ['+str(info['unresolved'])+' unresolved]') if info['unresolved'] else ''}{flag}")

    if not wanted:  # only rewrite the manifest on a full run
        mpath = os.path.join(DIST, "nuka_cola_spawns_manifest.json")
        json.dump({"_meta": {"generated": generated}, "sets": manifest},
                  open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"  manifest -> {os.path.basename(mpath)} ({len(manifest)} variants)")

    if con:
        con.close()


if __name__ == "__main__":
    main(sys.argv)
