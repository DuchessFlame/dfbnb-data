#!/usr/bin/env python3
r"""
spawns_configs.plants — the Farming PLANTS family driver (BNB, /bnb/farming/plants/).

Plants are HARVESTABLE FLORA, not creatures and not an ALCH/LVLI item closure, so this
driver seeds each page from the game's FLOR export (one FLOR record per placeable flora
base) and resolves WHERE that flora grows straight from the Mappalachia DB. It reuses the
shared spawn primitives from spawns_configs.cryptids (geo resolve, _record, group_regions,
label_spawns, attach_breakdowns, used_for) and the two-tier "guaranteed vs weighted"
model — but with a plant-appropriate tiering (see plants_tier):

  Fixed Spawn Locations   — GUARANTEED points: directly-placed flora REFRs (static, always
                            there) + Mappalachia NPC-table flora points whose spawnWeight
                            >= 1.0 AND that are the sole occupant of the point's pool.
  Chance to Spawn Locations — WEIGHTED points: shared/weighted Mappalachia spawn-pool points
                            (real % chance from spawnWeight). NO 100-fixed-spawn cap — every
                            placement the DB returns is resolved (spawn-guide, per request).

Placement resolution runs TWO passes, deduped by instanceFormID:
  1. NPC-name pass (PRIMARY)  — Mappalachia NPC table joined to Position by the flora's
     display name(s) (npcName), carrying spawnWeight + shared-pool tiering.
  2. Static-base pass (FALLBACK) — Position rows whose referenceFormID is one of the flora's
     own FLOR base FormIDs (directly placed flora the NPC table doesn't carry). Guaranteed.

ROSTER IS GENERATIVE. The page list comes from guide_index.tsv (the 43 committed
`bnb-sc-farming-plants-*` sub-cards → slug / title / guide URL), and each plant's flora
records are matched from the FLOR export by EDID/FULL tokens. The ALIASES table below only
carries the handful of editorial names that don't auto-resolve (Glowing Resin = Sap, Lure
Weed = Angler Plant, Prickeye = Slipper Cactus, the *-Blossom wild variants, …). New FLOR
records for an existing plant are picked up automatically.

Naming convention (shared with meat + insects): each plant is a sub-category CARD named by
the plant; the guide PAGE sits under it at /bnb/farming/plants/<slug>/<slug>-guide/, titled
"{name} Location Guide" (both read from guide_index.tsv, never hand-constructed).

Output: dist/plants.json (hub) + dist/plants/<slug>.json per page. Geo caches under
data/plant_spawns/<slug>.json so CI (no Mappalachia DB) reproduces the spawns.

Usage:
    python src/build_spawns.py plants [--pts] [slug ...]
"""

import os, re, csv, sys, sqlite3, datetime
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import tsv_source
from spawns_engine.geo import Geo
from spawns_engine import build as ebuild
from spawns_configs import cryptids as C

DIST = os.path.join(REPO, "dist")
URL_BASE = "/bnb/farming/plants/"
GUIDE_INDEX = os.path.join(REPO, "tsv", "guide_index.tsv")
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", C.MAPPALACHIA_DB)
APPALACHIA_SPACE = C.APPALACHIA_SPACE
ALL_REGIONS = C.ALL_REGIONS
SOURCE_TAG = ("Game-file exports (FLOR/NPC) + challenges.json + Mappalachia "
              "Position/Entity (cached for CI)")

# ── editorial name → flora aliases ────────────────────────────────────────────
# Only the plants whose site name doesn't auto-match a FLOR EDID/FULL. Each entry:
#   edid        include EDID stems (normalised, digit-agnostic contains-match)
#   exclude     EDID stems to reject (keeps base vs wild/blossom/diseased separate)
#   mappalachia npcName candidate strings for the Mappalachia NPC-table pass
# A plant with no entry uses its singularised title as the EDID token and its title
# as the npcName candidate. FLOR FULL-name exact match is always tried too.
ALIASES = {
    "carrot":               {"edid": ["carrot"], "exclude": ["wildcarrot"]},
    "wild-carrot-flower":    {"edid": ["wildcarrotflower"], "mappalachia": ["Wild Carrot Flower"]},
    "gourd":                {"edid": ["gourd"], "exclude": ["wildgourd"]},
    "gourd-blossom":         {"edid": ["wildgourdvine", "wildgourdflower"],
                              "mappalachia": ["Wild Gourd Blossom", "Gourd Blossom"]},
    "melon":                {"edid": ["melon"], "exclude": ["wildmelon", "gswildmelon"]},
    "melon-blossom":         {"edid": ["wildmelonvine"], "exclude": ["gswild"],
                              "mappalachia": ["Wild Melon Blossom", "Melon Blossom"]},
    "tato":                 {"edid": ["tato"], "exclude": ["wildtato"]},
    "tato-blossom":          {"edid": ["wildtatoplant"],
                              "mappalachia": ["Wild Tato Blossom", "Tato Blossom"]},
    "prickeye":             {"edid": ["slippercactus"], "exclude": ["pepper"],
                              "mappalachia": ["Prickeye"]},
    "snaptail-reed":         {"edid": ["snaptail"], "exclude": ["florarad"],
                              "mappalachia": ["Snaptail"]},
    "starlight-berries":     {"edid": ["starlightcreeper"], "exclude": ["florarad"],
                              "mappalachia": ["Starlight Creeper", "Starlight Berry"]},
    "soot-flower":           {"edid": ["sootflower"], "exclude": ["toxicsoot"]},
    "toxic-soot-flower":     {"edid": ["toxicsootflower"], "mappalachia": ["Toxic Soot Flower"]},
    "cranberries":          {"edid": ["cranberry"], "exclude": ["diseased"]},
    "diseased-cranberries":  {"edid": ["diseased"], "mappalachia": ["Diseased Cranberry"]},
    "blackberries":         {"edid": ["blackberry"], "exclude": ["florarad"]},
    "firecracker-berries":   {"edid": ["firecracker"]},
    "mega-sloth-mushroom":   {"edid": ["fungusmegasloth", "megasloth"]},
    "mutated-fern":          {"edid": ["fern"]},
    "glowing-fungus":        {"edid": ["fungusglowing", "glowingfungus"]},
    "brain-fungus":          {"edid": ["fungusbrain", "brainfungus"]},
    "gut-shroom":            {"edid": ["gutshroom"]},
    "strangler-bloom":       {"edid": ["stranglerbloom"], "mappalachia": ["Strangler Bloom"]},
    "strangler-pod":         {"edid": ["stranglerpod"], "mappalachia": ["Strangler Pod"]},
    "glowing-resin":         {"edid": ["florasap"], "mappalachia": ["Glowing Resin"]},
    "lure-weed":             {"edid": ["anglerplant"], "mappalachia": ["Lure Weed"]},
    "silt-bean":             {"edid": ["siltbean"]},
    "ginseng-root":          {"edid": ["ginseng"], "exclude": ["nuka"]},
    "ash-rose":              {"edid": ["ashrose"], "exclude": ["radrose"]},
    "firecap":               {"edid": ["firecap"], "exclude": ["florarad"]},
}


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _sing(n):
    return re.sub(r"ies$", "y", n) if n.endswith("ies") else re.sub(r"s$", "", n)


# ── roster (from guide_index.tsv — the committed site structure) ──────────────
def load_roster():
    """Return [{slug, name, url}] for every plant sub-card in guide_index.tsv, with
    the child guide page's URL (…/<slug>/<slug>-guide/). Generative: the roster is
    exactly what the site publishes, never a second hand-list to keep in sync."""
    by_id, subs = {}, []
    with open(GUIDE_INDEX, encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            by_id[r["id"]] = r
            if r.get("parentId") == "bnb-tc-farming-plants" and r.get("nodeType") == "sub":
                subs.append(r)
    roster = []
    for r in subs:
        slug = r["slug"]
        # the child guide page (nodeType=page) carries the real content URL
        page = next((p for p in by_id.values()
                     if p.get("parentId") == r["id"] and p.get("nodeType") == "page"), None)
        url = (page or {}).get("url") or f"{URL_BASE}{slug}/{slug}-guide/"
        roster.append({"slug": slug, "name": r["title"], "url": url})
    roster.sort(key=lambda p: p["name"].lower())
    return roster


# ── flora resolution (FLOR export) ────────────────────────────────────────────
def load_flor(tsv_root=None):
    root = tsv_root or os.path.join(REPO, "tsv")
    path = tsv_source.newest(os.path.join(root, "FLOR_Export_*.tsv"), required=False)
    rows = []
    if path:
        with open(path, encoding="utf-8", errors="replace") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
    return rows, path


def resolve_flora(slug, name, flor):
    """Match a plant to its FLOR base records → (flora[list of {form_id,edid,full}],
    mappalachia_names[list]). EDID/FULL token match with per-plant ALIASES."""
    cfg = ALIASES.get(slug, {})
    inc = cfg.get("edid") or [_sing(_norm(name))]
    exc = cfg.get("exclude") or []
    mapp = list(cfg.get("mappalachia") or [name])
    names = {_norm(n) for n in mapp} | {_norm(name)}
    flora = []
    for r in flor:
        edid = r.get("FLOR_EDID", "")
        full = r.get("FLOR_FULL", "")
        e, fu = _norm(edid), _norm(full)
        if (any(t in e for t in inc) or (fu and fu in names)) \
                and not any(x in e for x in exc):
            flora.append({"form_id": (r.get("FLOR_FormID") or "").upper(),
                          "edid": edid, "full": full})
    # widen npcName candidates with the clean FULL names we actually matched
    for fr in flora:
        clean = re.sub(r"\b(plant|planter|vine|bush)\b", "", fr["full"], flags=re.I).strip()
        for cand in (fr["full"], clean):
            if cand and cand not in mapp:
                mapp.append(cand)
    return flora, mapp


# ── placement passes ──────────────────────────────────────────────────────────
def _npc_pass(mapp_names, slug, cur, geo, cache, seen, pool_size):
    """PRIMARY: Mappalachia NPC-table flora points (npcName join Position), carrying
    spawnWeight + shared-pool flag for two-tier routing."""
    for nm in mapp_names:
        q = ("SELECT n.instanceFormID, n.spaceFormID, p.x, p.y, n.spawnWeight "
             "FROM NPC n JOIN Position p "
             "ON p.instanceFormID = n.instanceFormID AND p.spaceFormID = n.spaceFormID "
             "WHERE n.npcName = ?")
        for inst, space, x, y, sw in cur.execute(q, (nm,)):
            if inst in seen[slug]:
                continue
            region, marker, _ = geo.resolve(space, x, y)
            shared = (pool_size.get(inst, 1) or 1) > 1
            C._record(cache, seen, slug, inst, space, None, x, y, region, marker,
                      "spawn", weight=sw, shared=shared)


def _static_pass(flora_ids, slug, cur, geo, cache, seen):
    """FALLBACK: directly-placed flora REFRs (Position by FLOR base FormID). These are
    always-there flora → guaranteed static fixed spawns."""
    base_ints = [int(fid, 16) for fid in flora_ids if fid]
    if not base_ints:
        return
    for x, y, z, inst, space, ref in ebuild.pull_by_base(cur, base_ints):
        if inst in seen[slug]:
            continue
        region, marker, _ = geo.resolve(space, x, y)
        C._record(cache, seen, slug, inst, space, ref, x, y, region, marker, "static")


def _rebuild_from_cache(cache, slug, seen):
    """DB absent → reconstruct this plant's placements from the committed geo cache
    (page-scoped keys `<slug>:<inst>`, written by C._record)."""
    for key, e in cache.items():
        if not isinstance(e, dict) or e.get("page") != slug:
            continue
        inst = key.rsplit(":", 1)[-1]
        if inst.isdigit():
            seen[slug][int(inst)] = (e.get("x"), e.get("y"), e.get("region", ""),
                                     e.get("marker", ""), e.get("source_type", "spawn"))


# ── two-tier split (plant-appropriate) ────────────────────────────────────────
def plants_tier(seen_slug, cache, slug, name):
    """Split placements into GUARANTEED (Fixed Spawn Locations) and WEIGHTED
    (Chance to Spawn Locations). Static flora REFRs are always guaranteed; NPC-table
    points are guaranteed only when spawnWeight >= 1.0 and sole occupant. No cap."""
    guaranteed, weighted = {}, []
    for inst, tup in seen_slug.items():
        x, y, region, marker, stype = tup
        rec = cache.get(f"{slug}:{inst}", {})
        weight = rec.get("weight")
        shared = rec.get("shared", False)
        is_g = (stype in ("static", "direct")) or (
            stype == "spawn" and weight is not None and weight >= 0.999 and not shared)
        if is_g:
            guaranteed[inst] = tup
        else:
            weighted.append({"region": region, "marker": marker, "weight": weight})
    by = {}
    for w in weighted:
        marker = w["marker"] or "Unknown location"
        e = by.setdefault((w["region"], marker),
                          {"region": w["region"], "marker": marker,
                           "weights": [], "count": 0})
        e["count"] += 1
        if w["weight"]:
            e["weights"].append(w["weight"])
    rows = []
    for e in by.values():
        wmax = max(e["weights"]) if e["weights"] else None
        rows.append({"marker": e["marker"], "region": e["region"],
                     "chance_value": round(wmax, 4) if wmax else None,
                     "chance_display": C._pct(wmax) if wmax else "possible",
                     "variants": [name], "count": e["count"]})
    rows.sort(key=lambda r: r["marker"].lower())
    return guaranteed, rows


# ── per-plant build ───────────────────────────────────────────────────────────
def build_plant(pg, flor, geo, cur, db_ok, cache_path, generated):
    slug, name = pg["slug"], pg["name"]
    flora, mapp = resolve_flora(slug, name, flor)
    flora_ids = [fr["form_id"] for fr in flora]

    cache = ebuild.load_cache(cache_path)
    # prune cache to this plant (page-scoped keys)
    for k in [k for k, v in cache.items()
              if isinstance(v, dict) and v.get("page") not in (slug,)]:
        cache.pop(k, None)

    seen = {slug: {}}
    if db_ok:
        pool_size = {}
        for inst, n in cur.execute("SELECT instanceFormID, COUNT(DISTINCT npcName) "
                                   "FROM NPC WHERE spaceFormID = ? GROUP BY instanceFormID",
                                   (APPALACHIA_SPACE,)):
            pool_size[inst] = n
        _npc_pass(mapp, slug, cur, geo, cache, seen, pool_size)   # PRIMARY
        _static_pass(flora_ids, slug, cur, geo, cache, seen)      # FALLBACK
    else:
        _rebuild_from_cache(cache, slug, seen)

    guaranteed, chance_spawns = plants_tier(seen[slug], cache, slug, name)

    keep = _keep_from_doc(os.path.join(OUT_DIR, slug + ".json"))
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        guaranteed, ALL_REGIONS, keep or {})
    C.label_spawns(regions_out, name)
    C.attach_breakdowns(regions_out)

    pg_tokens = {"name": name, "tokens": [name.lower()]}
    uf = C.used_for(pg_tokens, pg["url"])

    if db_ok:
        ebuild.save_cache(cache, cache_path)

    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "source_totals": src_totals, "unresolved": unresolved,
                  "mappalachia_names": mapp},
        "set": "plants", "slug": slug, "name": name,
        "page_title": f"{name} Location Guide", "url": pg["url"],
        "blurb": _blurb(name, flora, placements, chance_spawns),
        "flora": flora,
        "used_for": uf,
        "farming_tips": None,
        "events_activities": [],
        "fixed_spawns": {"regions": regions_out, "total_markers": total,
                         "total_placements": placements},
        "chance_spawns": {"locations": chance_spawns, "total": len(chance_spawns),
                          "note": None},
    }
    # assertion: spawns[] == count on every guaranteed location
    bad = [(r["region"], l["marker"]) for r in regions_out
           for l in r["locations"] if len(l.get("spawns") or []) != l["count"]]
    if bad:
        raise AssertionError(f"[{slug}] spawns/count mismatch at {bad[:5]}")
    return doc, placements, len(flora), len(chance_spawns)


def _keep_from_doc(path):
    try:
        return ebuild.load_existing(path)
    except Exception:
        return {}


def _blurb(name, flora, placements, chance):
    tail = []
    if placements:
        tail.append(f"{placements} guaranteed spawn point" + ("" if placements == 1 else "s"))
    if chance:
        tail.append(f"{len(chance)} chance-spawn location" + ("" if len(chance) == 1 else "s"))
    head = f"Where {name} grows in Fallout 76"
    return head + (" — " + ", ".join(tail) + "." if tail else ".")


# ── entry point ──────────────────────────────────────────────────────────────
OUT_DIR = os.path.join(DIST, "plants")
HUB_FILE = os.path.join(DIST, "plants.json")
GEO_DIR = os.path.join(REPO, "data", "plant_spawns")


def run(argv=None):
    global OUT_DIR
    import json
    args = [a for a in (argv or []) if a != "plants"]
    pts = "--pts" in args
    slug_filter = {a for a in args if not a.startswith("-")}

    out_dir = os.path.join(DIST, "pts", "plants") if pts else OUT_DIR
    hub_file = os.path.join(DIST, "pts", "plants.json") if pts else HUB_FILE
    geo_dir = os.path.join(GEO_DIR, "pts") if pts else GEO_DIR
    tsv_root = os.path.join(REPO, "tsv", "pts") if pts else os.path.join(REPO, "tsv")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(geo_dir, exist_ok=True)

    OUT_DIR = out_dir  # so _keep_from_doc reads the right prior docs

    generated = datetime.date.today().isoformat()
    roster = load_roster()
    if slug_filter:
        roster = [p for p in roster if p["slug"] in slug_filter]
    flor, flor_path = load_flor(tsv_root if pts else None)
    print(f"[plants] {len(roster)} plant pages; FLOR export: "
          f"{os.path.basename(flor_path) if flor_path else 'MISSING'}"
          + (" (PTS channel)" if pts else ""))

    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[plants] Mappalachia DB found — resolving placements + refreshing geo caches.")
    else:
        print("[plants] No Mappalachia DB — rebuilding from committed geo caches (CI mode).")

    hub = []
    for pg in roster:
        cache_path = os.path.join(geo_dir, pg["slug"] + ".json")
        doc, placements, n_flora, n_chance = build_plant(
            pg, flor, geo, cur, db_ok, cache_path, generated)
        json.dump(doc, open(os.path.join(out_dir, pg["slug"] + ".json"), "w",
                            encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"  {pg['slug']:<22} flora:{n_flora:>2} fixed:{placements:>4} "
              f"chance:{n_chance:>3} uf:{len(doc['used_for']):>2}"
              + ("   <-- 0 flora (needs alias)" if n_flora == 0 else ""))
        hub.append({"slug": pg["slug"], "name": pg["name"], "url": pg["url"],
                    "counts": {"flora": n_flora, "fixed_spawns": placements,
                               "chance_spawns": n_chance}})

    hub_doc = {"_meta": {"generated": generated, "source": SOURCE_TAG},
               "name": "Farming - Plants", "page_title": "Farming - Plants", "url": URL_BASE,
               "blurb": ("Every farmable plant in Fallout 76 — where each flora grows, its "
                         "guaranteed spawn points and its chance-to-spawn locations. "
                         "Pick a plant below."),
               "plants": hub}
    json.dump(hub_doc, open(hub_file, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[plants] wrote {hub_file} ({len(hub)} pages) + per-page docs in {out_dir}")
    if con:
        con.close()

    try:
        from patchlog_utils import write_empty_patchlog_feed
        write_empty_patchlog_feed("dist", "patchlog_latest_bnb_farming_plants.json",
                                  current_count=len(hub))
    except Exception:
        pass


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
