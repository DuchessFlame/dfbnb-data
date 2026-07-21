#!/usr/bin/env python3
r"""
build_collectable_spawns_json.py — dist JSON for the "{Item} Spawn Locations" pages.

Pipeline position (see the spawn-guide skill):
  xEdit MISC2/ACTI2 export  ->  crossref_mappalachia_markers (region + nearest marker)
  ->  THIS script  ->  dist/collectable_spawns_<set>.json (+ manifest)  ->  renderer.

What it does:
  * Calls crossref_mappalachia_markers.resolve_dataset() to get every placement resolved to a
    region + nearest map marker (from the Mappalachia DB locally, or the committed geo_cache.json
    in CI). No Mappalachia DB is required here.
  * Groups per set -> all ten regions (A-Z, empty ones included) -> one location per marker (A-Z),
    with a count, the source ref FormIDs and a representative coordinate.
  * Emits empty image_top / directions / image_bottom slots for the user to hand-fill, and
    MERGES — never clobbers — any non-empty hand-authored values on rebuild (§4d).

Channels: reads tsv/, writes dist/. No PTS path logic — the PTS workflow normalizes
tsv/pts -> tsv, wipes dist, runs this, and relocates dist/* -> dist/pts/.

Env overrides:
  INPUT_GLOB     export TSVs to resolve      default <repo>/tsv/*2_Export_*.tsv
  MAPPALACHIA_DB Mappalachia SQLite          default D:\Mappalachia\data\mappalachia.db
  OUT_DIR        output dir                  default <repo>/dist
"""

import os, json, datetime
from collections import defaultdict

import crossref_mappalachia_markers as xref

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(REPO, "dist"))

# All ten regions every page renders, in A-Z order (empty ones still appear).
REGIONS_AZ = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]

# Markers render A-Z within a region, EXCEPT where one marker's directions start
# from another marker's spawn — then the referenced marker must render first.
# Maps (region, marker) -> the string to sort that marker by instead of its name.
# Give the dependent marker a key that sorts just after the marker it depends on.
MARKER_SORT_OVERRIDES = {
    # "Nuka-World On Tour" directions begin at the Rollins Labor Camp spawn,
    # so Rollins must render immediately above it (else they read out of order).
    ("Ash Heap", "Nuka-World On Tour"): "rollins labor camp~1",
    # "Dolly Sods Campground" Spawn 1 continues from the Dolly Sods Lookout cabin
    # bathroom, so the Lookout must render immediately above the Campground.
    ("The Mire", "Dolly Sods Campground"): "dolly sods lookout~1",
}

def marker_sort_key(region, marker):
    return MARKER_SORT_OVERRIDES.get((region, marker), marker.lower())

# Per-set display metadata. Unknown slugs fall back to a humanised slug.
SET_META = {
    "pint-sized-slasher-masks": {
        "name": "Pint-Sized Slasher Masks",
        "page_title": "Pint-Sized Slasher Masks Spawn Locations",
        "blurb": "Every known spawn location for the Pint-Sized Slasher Masks, grouped by region.",
    },
    "coloured-baseball-bats": {
        "name": "Coloured Baseball Bats",
        "page_title": "Coloured Baseball Bats Spawn Locations",
        "blurb": "Every known spawn location for the coloured baseball bats, grouped by region.",
    },
    "misc-clean-items": {
        "name": "Misc & Clean Items",
        "page_title": "Misc & Clean Items Spawn Locations",
        "blurb": "Every known spawn location for the misc & clean items, grouped by region.",
    },
    "musical-instruments": {
        "name": "Musical Instruments",
        "page_title": "Musical Instruments Spawn Locations",
        "blurb": "Every known spawn location for the musical instruments, grouped by region.",
    },
    "plushies": {
        "name": "Plushies",
        "page_title": "Plushies Spawn Locations",
        "blurb": "Every known spawn location for the plushies, grouped by region.",
    },
    "robot-models": {
        "name": "Robot Models",
        "page_title": "Robot Models Spawn Locations",
        "blurb": "Every known spawn location for the robot models, grouped by region.",
    },
}

# Sets that share the export/cross-ref pipeline but belong to a DIFFERENT category and are
# built elsewhere. "treasure-maps" is its own category — its dig sites come from the ACTI2
# export and are emitted by build_treasure_maps_json.py (treasure_map_locations), NOT here.
SKIP_SETS = {"treasure-maps"}


def meta_for(slug):
    if slug in SET_META:
        return SET_META[slug]
    name = slug.replace("-", " ").title()
    return {"name": name, "page_title": f"{name} Spawn Locations",
            "blurb": f"Every known spawn location for {name}, grouped by region."}


def as_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def load_existing_handfills(path):
    """Return {(region, marker): {image_top, directions, image_bottom}} for non-empty
    hand-authored values in an existing dist JSON, so a rebuild never loses them."""
    keep = {}
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in data.get("regions", []):
        rname = reg.get("region", "")
        for loc in reg.get("locations", []):
            saved = {k: loc.get(k, "") for k in ("image_top", "directions", "image_bottom")
                     if loc.get(k)}
            if saved:
                keep[(rname, loc.get("marker", ""))] = saved
    return keep


def build_set(slug, rows):
    """Build one set's JSON dict from its resolved rows."""
    meta = meta_for(slug)
    out_path = os.path.join(OUT_DIR, f"collectable_spawns_{slug}.json")
    handfills = load_existing_handfills(out_path)

    # region -> marker -> aggregate
    by_region = defaultdict(lambda: defaultdict(lambda: {"count": 0, "refs": [], "coord": None}))
    for r in rows:
        region = r.get("region") or ""            # may be "" for a polygon-gap placement
        marker = r.get("marker") or "(unknown location)"
        agg = by_region[region][marker]
        agg["count"] += 1
        ref = (r.get("ref_formid") or "").strip()
        if ref:
            agg["refs"].append(ref)
        if agg["coord"] is None:
            x, y = as_float(r.get("x")), as_float(r.get("y"))
            if x is not None and y is not None:
                agg["coord"] = [x, y]

    regions_out = []
    total = 0
    for region in REGIONS_AZ:
        locs = []
        for marker in sorted(by_region.get(region, {}), key=lambda m: marker_sort_key(region, m)):
            agg = by_region[region][marker]
            total += agg["count"]
            hf = handfills.get((region, marker), {})
            locs.append({
                "marker": marker,
                "count": agg["count"],
                "image_top": hf.get("image_top", ""),
                "directions": hf.get("directions", ""),
                "image_bottom": hf.get("image_bottom", ""),
                "refs": agg["refs"],
                "coords": agg["coord"] or [],
            })
        regions_out.append({"region": region, "locations": locs})

    # placements that resolved to a marker but NO region (polygon gaps) — surface them so
    # they can be hand-assigned, rather than silently dropped.
    orphan_markers = sorted(by_region.get("", {}), key=lambda m: m.lower())

    return {
        "_meta": {
            "generated": datetime.date.today().isoformat(),
            "source": "xEdit MISC2/ACTI2 export + Mappalachia (regions & markers)",
        },
        "set": slug,
        "name": meta["name"],
        "page_title": meta["page_title"],
        "blurb": meta["blurb"],
        "total": total,
        "regions": regions_out,
        "unplaced": [{"marker": m, "count": by_region[""][m]["count"]} for m in orphan_markers],
    }, out_path


def main():
    resolved_rows, cache, db_ok = xref.resolve_dataset(verbose=True)
    if not resolved_rows:
        print("[collectable_spawns] no resolved placements — nothing to build.")
        return

    # Local runs (Mappalachia DB present) refresh the committed geo cache so the CI patch/PTS
    # builds — which have no DB — can re-resolve every ref by FormID from data/.../geo_cache.json.
    if db_ok:
        xref.save_geo_cache(cache)
        print(f"[collectable_spawns] refreshed geo cache ({len(cache)} refs) for CI rebuilds.")

    by_set = defaultdict(list)
    for r in resolved_rows:
        by_set[r.get("set") or ""].append(r)

    os.makedirs(OUT_DIR, exist_ok=True)
    manifest_sets = []
    for slug in sorted(by_set):
        if not slug or slug in SKIP_SETS:
            continue
        data, out_path = build_set(slug, by_set[slug])
        json.dump(data, open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        region_counts = {reg["region"]: len(reg["locations"]) for reg in data["regions"] if reg["locations"]}
        manifest_sets.append({
            "set": slug, "name": data["name"], "page_title": data["page_title"],
            "total": data["total"], "regions": region_counts,
            "unplaced": len(data["unplaced"]),
        })
        unpl = f"  ({len(data['unplaced'])} unplaced)" if data["unplaced"] else ""
        print(f"[collectable_spawns] {slug}: {data['total']} placements across "
              f"{len(region_counts)} region(s){unpl} -> {os.path.basename(out_path)}")

    manifest = {
        "_meta": {"generated": datetime.date.today().isoformat()},
        "sets": manifest_sets,
    }
    mpath = os.path.join(OUT_DIR, "collectable_spawns_manifest.json")
    json.dump(manifest, open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[collectable_spawns] wrote manifest with {len(manifest_sets)} set(s) -> "
          f"{os.path.basename(mpath)}")


if __name__ == "__main__":
    main()
