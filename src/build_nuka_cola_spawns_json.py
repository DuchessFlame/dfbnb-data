#!/usr/bin/env python3
r"""
build_nuka_cola_spawns_json.py — generate the per-variant spawn JSON for the
Nuka Cola "{Type} Locations" pages on buffsnbrew.com.

Generative: reads placements straight from the Mappalachia DB (no xEdit export
step needed for these items), resolves each placement to a region + sub-location
via nuka_cola_spawns_geo (Appalachia exterior by marker/polygon; Atlantic City &
The Pitt by their instanced space names; other interiors by name), groups by
region, and writes:

    dist/nuka_cola_spawns_<slug>.json      (one per variant)
    dist/nuka_cola_spawns_manifest.json    (index of all variants)

MERGE, DON'T CLOBBER: image_top / directions / image_bottom are hand-authored.
On every rebuild the existing dist file is loaded first and any non-empty
hand-filled field is preserved for a matching (region, sub-location) key. Only the
spawn set / counts / coords are refreshed. New spawns appear automatically with
empty slots; removed spawns drop out.

Membership + FormIDs live in nuka_cola_spawns_config.py. Region tweaks live in
nuka_cola_spawns_geo.py. Edit those, not this.

Env:
    MAPPALACHIA_DB   default D:\Mappalachia\data\mappalachia.db

Usage:
    python src/build_nuka_cola_spawns_json.py            # all variants
    python src/build_nuka_cola_spawns_json.py wild dark  # only these slugs
"""

import os, sys, json, sqlite3, datetime
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from nuka_cola_spawns_config import VARIANTS, ALL_REGIONS
from nuka_cola_spawns_geo import Geo

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
SOURCE_TAG = "Mappalachia DB (Position table, all spaces)"


def pull_positions(cur, formids):
    """Every placed instance of the given base FormIDs, in EVERY space (exterior + interiors
    + Atlantic City / The Pitt instances). Position.referenceFormID = base object (our leveled
    item); Position.instanceFormID = the unique placed REFR (kept as provenance)."""
    if not formids:
        return []
    q = ("SELECT x, y, z, instanceFormID, spaceFormID FROM Position "
         "WHERE referenceFormID IN (%s)" % ",".join("?" * len(formids)))
    return cur.execute(q, tuple(formids)).fetchall()


def load_existing(path):
    """(region, sub_location) -> {image_top, directions, image_bottom} for hand-authored preserve."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in old.get("regions", []):
        for loc in reg.get("locations", []):
            keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
            }
    return keep


def build_variant(v, geo, cur, generated):
    path = os.path.join(DIST, f"nuka_cola_spawns_{v['slug'].replace('-locations','')}.json")
    keep = load_existing(path)

    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None})
    unresolved = defaultdict(int)  # interior name -> count, for the "needs a region" report
    for x, y, z, ref, space in pull_positions(cur, v["source_formids"]):
        region, sub, how = geo.resolve(space, x, y)
        if how == "interior-unresolved" or region not in ALL_REGIONS:
            unresolved[sub or f"space {space}"] += 1
            continue
        g = grouped[(region, sub)]
        g["count"] += 1
        if ref is not None:
            g["refs"].append(f"{int(ref):06X}")
        if g["coords"] is None:
            g["coords"] = [round(x, 1), round(y, 1)]

    by_region = defaultdict(list)
    for (region, sub), g in grouped.items():
        prev = keep.get((region, sub), {})
        by_region[region].append({
            "marker": sub,
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
                  "source_formids": v["source_formids"],
                  "unresolved": {k: unresolved[k] for k in sorted(unresolved)}},
        "set": v["slug"].replace("-locations", ""),
        "name": v["name"],
        "page_title": f"{v['name']} Locations",
        "blurb": v["blurb"],
        "regions": regions_out,
    }
    os.makedirs(DIST, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"set": doc["set"], "name": v["name"], "slug": v["slug"], "file": os.path.basename(path),
            "locations": total, "unresolved": sum(unresolved.values()),
            "has_source": bool(v["source_formids"])}


def main(argv):
    wanted = {a.replace("-locations", "") for a in argv[1:]}
    variants = [v for v in VARIANTS if not wanted or v["slug"].replace("-locations", "") in wanted]

    need_db = any(v["source_formids"] for v in variants)
    geo = con = cur = None
    if need_db:
        if not os.path.exists(MAPPALACHIA_DB):
            sys.exit(f"[nuka-cola] Mappalachia DB not found: {MAPPALACHIA_DB}")
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()

    generated = datetime.date.today().isoformat()
    manifest = []
    for v in variants:
        info = build_variant(v, geo, cur, generated)
        manifest.append(info)
        extra = f"  [{info['unresolved']} unresolved]" if info["unresolved"] else ""
        flag = "" if info["has_source"] else "  (no source — hand-author)"
        print(f"  {info['file']:<46} {info['locations']:>3} locations{extra}{flag}")

    if not wanted:
        mpath = os.path.join(DIST, "nuka_cola_spawns_manifest.json")
        json.dump({"_meta": {"generated": generated}, "sets": manifest},
                  open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"  manifest -> {os.path.basename(mpath)} ({len(manifest)} variants)")

    if con:
        con.close()


if __name__ == "__main__":
    main(sys.argv)
