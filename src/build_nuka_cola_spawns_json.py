#!/usr/bin/env python3
r"""
build_nuka_cola_spawns_json.py — generate the per-variant spawn JSON for the
Nuka Cola "{Type} Locations" pages on buffsnbrew.com.

WIDEST, AUTOMATED source resolution (Aug 2026):
  nuka_cola_spawns_sources.get_sources() reads the committed game-file exports
  (LVLI_Entries + LVLI_Refs + ALCH refs) and returns, per flavour, EVERY world
  source that can yield the drink — the full leveled-list closure plus the
  containers / vending machines / collectrons / NPCs / direct REFRs that hold or
  place those lists.

  Coordinates live only in the local Mappalachia Position table (459 MB, not in
  CI), so — exactly like build_npc_spawns.py and build_collectable_spawns_json.py —
  a LOCAL run with the DB resolves every placement and writes a committed cache:

      data/nuka_cola_spawns/geo_cache.json   {instanceFormID: {base, space, x, y, region, marker}}

  Every later run WITHOUT the DB (GitHub CI, a scheduled task) rebuilds the JSON
  straight from that cache + the committed TSVs — fully automated. The cache only
  needs a fresh local DB pass when the game adds new placements.

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
from nuka_cola_spawns_sources import DRINK_ALCH, TSV
try:
    from build_farming_used_for import build_consumption
except Exception:
    build_consumption = None

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
GEO_CACHE = os.environ.get("NUKA_GEO_CACHE",
                           os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json"))
SPECIAL_SOURCES = os.path.join(REPO, "data", "nuka_cola_spawns", "special_sources.json")
SOURCE_TAG = "Game-file exports (LVLI/ALCH) + Mappalachia Position (cached for CI)"
SQL_CHUNK = 900  # stay under SQLite's 999-variable limit


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def pull_by_base(cur, base_ints):
    out = []
    for chunk in _chunks(base_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID, referenceFormID FROM Position "
             "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


def pull_by_instance(cur, inst_ints):
    out = []
    for chunk in _chunks(inst_ints):
        q = ("SELECT x, y, z, instanceFormID, spaceFormID FROM Position "
             "WHERE instanceFormID IN (%s)" % ",".join("?" * len(chunk)))
        out.extend(cur.execute(q, tuple(chunk)).fetchall())
    return out


def load_cache():
    try:
        return json.load(open(GEO_CACHE, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache):
    os.makedirs(os.path.dirname(GEO_CACHE), exist_ok=True)
    json.dump(cache, open(GEO_CACHE, "w", encoding="utf-8"), ensure_ascii=False)


_SPECIAL_CACHE = None
def load_special():
    """Curated no-coordinate sources (Collectrons, Resource Generators, dedicated /
    instanced Vendors) keyed by variant slug. These CAN'T come from Mappalachia
    placements (ATX/CAMP/event items have no world coords), so they live in a
    committed data file — same pattern as the vendor_overrides.tsv in the vendor
    pipeline. Missing file -> {} (pages just omit those expands)."""
    global _SPECIAL_CACHE
    if _SPECIAL_CACHE is None:
        try:
            _SPECIAL_CACHE = json.load(open(SPECIAL_SOURCES, encoding="utf-8"))
        except Exception:
            _SPECIAL_CACHE = {}
    return _SPECIAL_CACHE


def compute_farming_tips(cons):
    """Deterministic Farming Tips for a non-perishable drink (spawn-guide skill
    §9g). Drinks don't spoil and no perk/magazine raises their yield; only
    weight-reduction perks/mods apply, computed from the consumption weight."""
    if not cons:
        return {}
    w = cons.get("weight") or 0
    obj = cons.get("object_type") or "Drink"
    r2 = lambda x: round(x, 2)
    return {
        "spoils": False,
        "spoil_duration_hours": None,
        "base_weight": w,
        "object_type": obj,
        "yield_perk": None,
        "weight_perk": "traveling_pharmacy" if obj == "Chem" else "thru_hiker",
        "weight_perk_ranks": [
            {"rank": 1, "reduction": "45%", "weight": r2(w * 0.55)},
            {"rank": 2, "reduction": "90%", "weight": r2(w * 0.10)},
        ],
        "backpack_mod": "chemists" if obj == "Chem" else "grocers",
        "backpack_weight": r2(w * 0.10),
        "armour_mod": "thru_hikers",
        "armour_mod_per_piece": 0.20,
        "armour_mod_weights": [
            {"pieces": 1, "reduction": "20%", "weight": r2(w * 0.80)},
            {"pieces": 2, "reduction": "40%", "weight": r2(w * 0.60)},
            {"pieces": 3, "reduction": "60%", "weight": r2(w * 0.40)},
            {"pieces": 4, "reduction": "80%", "weight": r2(w * 0.20)},
            {"pieces": 5, "reduction": "90% cap", "weight": r2(w * 0.10)},
        ],
        "magazines_affect_yield": False,
        "good_with_salt": False,
    }


def build_used_for(slug, name):
    """(used_for, farming_tips) for a variant. used_for.consumption comes from the
    farming pipeline's build_consumption (the item's own ALCH effects/weight/diet);
    farming_tips is computed from it. Empty dicts when there's no drink ALCH."""
    fids = DRINK_ALCH.get(slug) or []
    if not fids or build_consumption is None:
        return {}, {}
    cons = None
    for fid in fids:
        cons = build_consumption(fid, TSV, item_name=name)
        if cons:
            break
    if not cons:
        return {}, {}
    return {"consumption": cons}, compute_farming_tips(cons)


def load_existing(path):
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


def resolve_placements(v, tbls, geo, cur, cache, db_ok):
    """Yield (instanceFormID_int, x, y, region, marker, source_type) for one flavour.

    DB present  -> query Position, resolve via geo, and record into cache.
    DB absent   -> reconstruct from the committed cache + the TSV source closure.
    """
    slug = v["slug"].replace("-locations", "")
    extra = [f"{int(x):08X}" for x in v.get("source_formids", [])]
    src = sources.get_sources(slug, extra_seed_formids=extra, tables=tbls)
    base_type = {int(fid, 16): meta["source_type"] for fid, meta in src["placed_bases"].items()}
    direct_ints = {int(fid, 16) for fid in src["direct_refrs"]}
    lists_n = len(src["lvli_closure"])

    seen = {}
    if db_ok:
        for x, y, z, inst, space, ref in pull_by_base(cur, base_type.keys()):
            if inst in seen:
                continue
            region, marker, how = geo.resolve(space, x, y)
            cache[str(inst)] = {"base": ref, "space": space, "x": round(x, 1),
                                "y": round(y, 1), "region": region, "marker": marker}
            seen[inst] = (x, y, region, marker, base_type.get(ref, "loot-list"))
        for x, y, z, inst, space in pull_by_instance(cur, direct_ints):
            if inst in seen:
                continue
            region, marker, how = geo.resolve(space, x, y)
            cache[str(inst)] = {"base": None, "space": space, "x": round(x, 1),
                                "y": round(y, 1), "region": region, "marker": marker}
            seen[inst] = (x, y, region, marker, "direct")
    else:
        for key, e in cache.items():
            inst = int(key)
            base = e.get("base")
            if base in base_type:
                seen[inst] = (e.get("x"), e.get("y"), e.get("region", ""),
                              e.get("marker", ""), base_type[base])
        for inst in direct_ints:
            e = cache.get(str(inst))
            if e and inst not in seen:
                seen[inst] = (e.get("x"), e.get("y"), e.get("region", ""),
                              e.get("marker", ""), "direct")

    return seen, lists_n


def build_variant(v, tbls, geo, cur, cache, db_ok, generated):
    slug = v["slug"].replace("-locations", "")
    path = os.path.join(DIST, f"nuka_cola_spawns_{slug}.json")
    keep = load_existing(path)

    seen, lists_n = resolve_placements(v, tbls, geo, cur, cache, db_ok)

    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None,
                                   "sources": defaultdict(int)})
    unresolved = defaultdict(int)
    for inst, (x, y, region, marker, stype) in seen.items():
        if region not in ALL_REGIONS:
            unresolved[marker or f"instance {inst:06X}"] += 1
            continue
        g = grouped[(region, marker)]
        g["count"] += 1
        g["sources"][stype] += 1
        g["refs"].append(f"{int(inst):06X}")
        if g["coords"] is None and x is not None and y is not None:
            g["coords"] = [x, y]

    by_region = defaultdict(list)
    for (region, marker), g in grouped.items():
        prev = keep.get((region, marker), {})
        by_region[region].append({
            "marker": marker,
            "count": g["count"],
            "sources": dict(sorted(g["sources"].items())),
            "image_top": prev.get("image_top", ""),
            "directions": prev.get("directions", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "refs": sorted(set(g["refs"])),
            "coords": g["coords"] or [],
        })

    regions_out = []
    for region in ALL_REGIONS:
        locs = sorted(by_region.get(region, []), key=lambda l: l["marker"].lower())
        regions_out.append({"region": region, "locations": locs})

    total = sum(len(r["locations"]) for r in regions_out)
    placements = sum(loc["count"] for r in regions_out for loc in r["locations"])
    src_totals = defaultdict(int)
    for r in regions_out:
        for loc in r["locations"]:
            for t, n in loc["sources"].items():
                src_totals[t] += n

    _uf, _tips = build_used_for(slug, v["name"])
    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "source_formids": v.get("source_formids", []),
                  "lists_in_closure": lists_n,
                  "source_totals": dict(sorted(src_totals.items())),
                  "unresolved": {k: unresolved[k] for k in sorted(unresolved)}},
        "set": slug,
        "name": v["name"],
        "page_title": f"{v['name']} Locations",
        "blurb": v["blurb"],
        "regions": regions_out,
        "used_for": _uf,
        "farming_tips": _tips,
        "special_sources": load_special().get(slug, {}),
    }
    os.makedirs(DIST, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"set": slug, "name": v["name"], "slug": v["slug"], "file": os.path.basename(path),
            "locations": total, "placements": placements,
            "unresolved": sum(unresolved.values()), "has_source": bool(seen)}


def main(argv):
    wanted = {a.replace("-locations", "") for a in argv[1:]}
    variants = [v for v in VARIANTS if not wanted or v["slug"].replace("-locations", "") in wanted]

    tbls = sources.load_tables()
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = load_cache()

    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[nuka-cola] Mappalachia DB found — resolving placements and refreshing geo cache.")
    elif cache:
        print(f"[nuka-cola] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[nuka-cola] No Mappalachia DB and no geo cache — cannot build. "
              "Run once locally with MAPPALACHIA_DB set to seed data/nuka_cola_spawns/geo_cache.json.")
        return

    generated = datetime.date.today().isoformat()
    manifest = []
    for v in variants:
        info = build_variant(v, tbls, geo, cur, cache, db_ok, generated)
        manifest.append(info)
        extra = f"  [{info['unresolved']} unresolved]" if info["unresolved"] else ""
        flag = "" if info["has_source"] else "  (no world source)"
        print(f"  {info['file']:<46} {info['locations']:>4} locations / "
              f"{info['placements']:>5} placements{extra}{flag}")

    if db_ok:
        save_cache(cache)
        print(f"[nuka-cola] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")

    if not wanted:
        mpath = os.path.join(DIST, "nuka_cola_spawns_manifest.json")
        json.dump({"_meta": {"generated": generated},
                   "sets": [{k: m[k] for k in ("set", "name", "slug", "file",
                                               "locations", "unresolved", "has_source")}
                            for m in manifest]},
                  open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"  manifest -> {os.path.basename(mpath)} ({len(manifest)} variants)")

    if con:
        con.close()


if __name__ == "__main__":
    main(sys.argv)
