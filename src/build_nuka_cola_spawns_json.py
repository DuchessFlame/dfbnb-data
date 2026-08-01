#!/usr/bin/env python3
r"""
build_nuka_cola_spawns_json.py — generate the per-variant spawn JSON for the
Nuka Cola "{Type} Locations" pages on buffsnbrew.com.

WIDEST source resolution (Aug 2026 rewrite):
  nuka_cola_spawns_sources.get_sources() reads the committed game-file exports
  (LVLI_Entries + LVLI_Refs + ALCH refs) and returns, per flavour, EVERY world
  source that can yield the drink — the full leveled-list closure plus the
  containers / vending machines / collectrons / NPCs / direct REFRs that hold or
  place those lists. This script then looks up each source's world coordinates in
  the Mappalachia Position table and resolves region + sub-location via
  nuka_cola_spawns_geo, tagging every placement with its source_type.

  So a rebuild now captures vending machines, mystery machines, collectrons,
  containers, quest rewards and direct placements — not just the flavour's own
  leveled item.

MERGE, DON'T CLOBBER: image_top / directions / image_bottom are hand-authored and
preserved for a matching (region, sub-location) key on every rebuild.

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
import nuka_cola_spawns_sources as sources

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
SOURCE_TAG = "Game-file exports (LVLI/ALCH) + Mappalachia Position (all spaces)"
SQL_CHUNK = 900  # stay under SQLite's 999-variable limit


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def pull_by_base(cur, base_ints):
    """Placements of each holder base: (x, y, z, instanceFormID, spaceFormID, referenceFormID)."""
    out = []
    for chunk in _chunks(base_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID, referenceFormID FROM Position "
             "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


def pull_by_instance(cur, inst_ints):
    """Specific placed REFRs (drink placed directly): (x, y, z, instanceFormID, spaceFormID)."""
    out = []
    for chunk in _chunks(inst_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID FROM Position "
             "WHERE instanceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


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


def build_variant(v, geo, cur, generated, tbls):
    slug = v["slug"].replace("-locations", "")
    path = os.path.join(DIST, f"nuka_cola_spawns_{slug}.json")
    keep = load_existing(path)

    # Full source closure from the game-file exports (no DB needed for this part).
    extra = [f"{int(x):08X}" for x in v.get("source_formids", [])]
    src = sources.get_sources(slug, extra_seed_formids=extra, tables=tbls)
    base_map = {int(fid, 16): meta["source_type"] for fid, meta in src["placed_bases"].items()}
    direct_ints = {int(fid, 16) for fid in src["direct_refrs"]}

    # Every world placement, tagged with source_type, deduped by instanceFormID.
    placements = {}  # instanceFormID -> (x, y, z, space, source_type)
    for x, y, z, inst, space, ref in pull_by_base(cur, base_map.keys()):
        placements.setdefault(inst, (x, y, z, space, base_map.get(ref, "loot-list")))
    for x, y, z, inst, space in pull_by_instance(cur, direct_ints):
        placements.setdefault(inst, (x, y, z, space, "direct"))

    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None,
                                   "sources": defaultdict(int)})
    unresolved = defaultdict(int)
    for inst, (x, y, z, space, stype) in placements.items():
        region, sub, how = geo.resolve(space, x, y)
        if how == "interior-unresolved" or region not in ALL_REGIONS:
            unresolved[sub or f"space {space}"] += 1
            continue
        g = grouped[(region, sub)]
        g["count"] += 1
        g["sources"][stype] += 1
        if inst is not None:
            g["refs"].append(f"{int(inst):06X}")
        if g["coords"] is None:
            g["coords"] = [round(x, 1), round(y, 1)]

    by_region = defaultdict(list)
    for (region, sub), g in grouped.items():
        prev = keep.get((region, sub), {})
        by_region[region].append({
            "marker": sub,
            "count": g["count"],
            "sources": dict(sorted(g["sources"].items())),
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
    src_totals = defaultdict(int)
    for r in regions_out:
        for loc in r["locations"]:
            for t, n in loc["sources"].items():
                src_totals[t] += n
    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "source_formids": v.get("source_formids", []),
                  "lists_in_closure": len(src["lvli_closure"]),
                  "source_totals": dict(sorted(src_totals.items())),
                  "unresolved": {k: unresolved[k] for k in sorted(unresolved)}},
        "set": slug,
        "name": v["name"],
        "page_title": f"{v['name']} Locations",
        "blurb": v["blurb"],
        "regions": regions_out,
    }
    os.makedirs(DIST, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"set": slug, "name": v["name"], "slug": v["slug"], "file": os.path.basename(path),
            "locations": total, "unresolved": sum(unresolved.values()),
            "placements": sum(loc["count"] for r_ in regions_out for loc in r_["locations"]),
            "has_source": bool(base_map or direct_ints)}


def main(argv):
    wanted = {a.replace("-locations", "") for a in argv[1:]}
    variants = [v for v in VARIANTS if not wanted or v["slug"].replace("-locations", "") in wanted]

    if not os.path.exists(MAPPALACHIA_DB):
        # CI-safe: DB is large and local-only. Leave the committed dist/ artifacts untouched
        # rather than failing — rebuild locally with the DB to refresh (hand-fills preserved).
        print(f"[nuka-cola] Mappalachia DB not found at {MAPPALACHIA_DB} — "
              f"skipping rebuild, leaving committed dist/ untouched (normal in CI).")
        return

    tbls = sources.load_tables()
    geo = Geo(MAPPALACHIA_DB)
    con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()

    generated = datetime.date.today().isoformat()
    manifest = []
    for v in variants:
        info = build_variant(v, geo, cur, generated, tbls)
        manifest.append(info)
        extra = f"  [{info['unresolved']} unresolved]" if info["unresolved"] else ""
        flag = "" if info["has_source"] else "  (no world source)"
        print(f"  {info['file']:<46} {info['locations']:>4} locations / "
              f"{info['placements']:>5} placements{extra}{flag}")

    if not wanted:
        mpath = os.path.join(DIST, "nuka_cola_spawns_manifest.json")
        json.dump({"_meta": {"generated": generated},
                   "sets": [{k: m[k] for k in ("set", "name", "slug", "file",
                                               "locations", "unresolved", "has_source")}
                            for m in manifest]},
                  open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"  manifest -> {os.path.basename(mpath)} ({len(manifest)} variants)")

    con.close()


if __name__ == "__main__":
    main(sys.argv)
