#!/usr/bin/env python3
r"""
spawns_configs.nuka_cola — the Nuka-Cola family driver for the shared spawns engine.

This is the thin, per-family layer: it owns the drink seed map (DRINK_ALCH), the
computed Used For / Farming Tips wiring, the curated coordless special_sources
merge, and the exact output-doc shape for the Nuka "{Type} Locations" pages. All
the heavy lifting (LVLI closure, geo, region grouping, DB/cache) lives in
spawns_engine.

A NEW drink variant = one entry in nuka_cola_spawns_config.VARIANTS + its ALCH
FormID in DRINK_ALCH below. No new code.
"""

import os, sys, json, sqlite3, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from nuka_cola_spawns_config import VARIANTS, ALL_REGIONS
from spawns_engine.geo import Geo
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine.classify import nuka_classify
try:
    from build_farming_used_for import build_consumption
except Exception:
    build_consumption = None

# Base drink ALCH FormIDs (8-hex, load order) — seeds for the up-walk, alongside
# each flavour's source_formids in nuka_cola_spawns_config.
DRINK_ALCH = {
    "nuka-cola":            ["0004835D"],
    "nuka-cola-cherry":     ["00048360"],
    "nuka-cola-cranberry":  ["00598DCD"],
    "nuka-cola-dark":       ["00113294"],
    "nuka-cola-grape":      ["00113292"],
    "nuka-cola-orange":     ["00113298"],
    "nuka-cola-quantum":    ["0004835F"],
    "nuka-cola-wild":       ["0011329B"],
    "nuka-cola-twist":      ["00660864"],
    "nukashine":            ["0047BC14", "0047BC08"],           # fresh + vintage
    "nuka-cola-vaccinated": [],                                  # quest activator, not placed
    "sunset-sarsaparilla":  ["00832CA7", "00837E07", "00837E08"],
}

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
TSV = esources.TSV
GEO_CACHE = os.environ.get("NUKA_GEO_CACHE",
                           os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json"))
SPECIAL_SOURCES = os.path.join(REPO, "data", "nuka_cola_spawns", "special_sources.json")
SOURCE_TAG = "Game-file exports (LVLI/ALCH) + Mappalachia Position (cached for CI)"


_SPECIAL_CACHE = None
def load_special():
    """Curated no-coordinate sources (Collectrons, Resource Generators, dedicated/
    instanced Vendors) keyed by variant slug. ATX/CAMP/event items have no world
    coords, so they live in a committed data file. Missing file -> {}."""
    global _SPECIAL_CACHE
    if _SPECIAL_CACHE is None:
        try:
            _SPECIAL_CACHE = json.load(open(SPECIAL_SOURCES, encoding="utf-8"))
        except Exception:
            _SPECIAL_CACHE = {}
    return _SPECIAL_CACHE


def compute_farming_tips(cons):
    """Deterministic Farming Tips for a non-perishable drink (spawn-guide §9g)."""
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
    """(used_for, farming_tips) for a variant, from the farming pipeline's
    build_consumption. Empty dicts when there's no drink ALCH."""
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


def build_variant(v, tbls, geo, cur, cache, db_ok, generated):
    slug = v["slug"].replace("-locations", "")
    path = os.path.join(DIST, f"nuka_cola_spawns_{slug}.json")
    keep = ebuild.load_existing(path)

    extra = [f"{int(x):08X}" for x in v.get("source_formids", [])]
    item_records = [{"formid": f, "sig": "ALCH"} for f in DRINK_ALCH.get(slug, [])]
    src = esources.get_sources(item_records, tbls, nuka_classify,
                               extra_closure_seeds=extra,
                               placed_sigs=esources.PLACED_SIGS_DEFAULT)
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)

    _uf, _tips = build_used_for(slug, v["name"])
    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "source_formids": v.get("source_formids", []),
                  "lists_in_closure": lists_n,
                  "source_totals": src_totals,
                  "unresolved": unresolved},
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


def run(argv):
    wanted = {a.replace("-locations", "") for a in argv[1:]}
    variants = [v for v in VARIANTS if not wanted or v["slug"].replace("-locations", "") in wanted]

    tbls = esources.load_tables()
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = ebuild.load_cache(GEO_CACHE)

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
        ebuild.save_cache(cache, GEO_CACHE)
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
    run(sys.argv)
