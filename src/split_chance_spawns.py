#!/usr/bin/env python3
r"""
split_chance_spawns.py — Fixed Spawn Locations vs Chance to Spawn Locations.

THE RULE (spawn-guide 9k). A world point is a FIXED SPAWN only when the leveled
list placed there is DEDICATED to the item: if something spawns, it can only be
this item. `LPI_Chems_Addictol` -> `LL_Chems_Addictol` -> Addictol is dedicated.
`LPI_Chems_Prewar` -> `LL_Chems_Prewar_Same` rolls nine different chems, so its
306 world points are NOT Addictol spawns — Addictol is just one of the things
that might be lying there. Those become **Chance to Spawn Locations**.

The list's own ChanceNone is deliberately NOT part of the test. The Deathclaw Egg
world spawn is 50% and is still a fixed Deathclaw Egg spawn, because nothing else
can ever be standing at that point. Dedication is about WHAT spawns, not how often.

Why this exists as a post-step as well as an engine fix: the fix itself lives in
spawns_engine.sources.dedicated_lists + spawns_engine.build (so every future build
is right), but re-running the spawn builders rewrites the whole doc and drops the
used_for / vendor_list / producer-card / treasure-map joins until
build_farming_used_for.py --all is re-run. This step repairs the committed dist in
place, touching only `regions` and `chance_spawns`. Idempotent: a doc whose direct
placements are all dedicated is left exactly as it was.

Run:  python src/split_chance_spawns.py [dist_dir]
Wired into build_farming_used_for.main() next to chem_loot_collapse.
"""
import json
import os
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_engine import sources as esources
from spawns_engine import build as ebuild


# ── the dedication test ──────────────────────────────────────────────────────
def dedicated_refs(item_records, tbls, dedication_seeds=None):
    """Every world REFR that is a genuine fixed spawn of this item set:
    REFRs placing a list dedicated to the set, plus the item's own placed bases.

    `dedication_seeds` widens the dedication test only (whole-category pages — see
    spawns_engine.sources.get_sources)."""
    seeds = {str(r["formid"]).upper() for r in item_records}
    closure = esources._closure(seeds, tbls["c2p"])
    ded = esources.dedicated_lists(dedication_seeds or seeds, closure, tbls["p2c"])
    ok = set()
    for lv in ded:
        for rf, _edid, rsig in tbls["lvli_refs"].get(lv, ()):
            if rsig == "REFR":
                ok.add(int(rf, 16))
    for rec in item_records:
        fid = str(rec["formid"]).upper()
        refs = (tbls["misc_refs"].get(fid, []) if rec.get("sig") == "MISC"
                else tbls["alch_refs"].get(fid, []))
        ok |= {int(x[0], 16) for x in refs if x[2] == "REFR"}
    return ok


def chance_points(item_records, tbls, ok_refs, geo_cache, all_regions):
    """{region: {marker: [ref, ...]}} for every placed REFR that is NOT a fixed spawn.

    Derived from the SOURCE data (LVLI closure + geo cache), not from whatever is
    currently sitting in the doc — so this rebuilds correctly no matter how many
    times it runs, and repairs a doc whose points were already moved by an earlier
    version of this script."""
    seeds = {str(r["formid"]).upper() for r in item_records}
    closure = esources._closure(seeds, tbls["c2p"])
    refs = set()
    for lv in closure:
        for rf, _edid, rsig in tbls["lvli_refs"].get(lv, ()):
            if rsig == "REFR":
                refs.add(int(rf, 16))

    regions = set(all_regions or ())
    out = defaultdict(dict)
    for inst in refs:
        if inst in ok_refs:
            continue                       # a genuine fixed spawn — belongs above
        e = geo_cache.get(str(inst))
        if not e:
            continue                       # never resolved to a place
        region, marker = e.get("region", ""), e.get("marker", "")
        if not region or not marker or (regions and region not in regions):
            continue
        out[region].setdefault(marker, []).append(f"{inst:06X}")
    return out


def chance_block(moved, order, doc):
    """The `chance_spawns` payload: regions A-Z as the doc orders them, markers A-Z,
    each carrying its refs so the per-item region tile can be drawn from them."""
    for reg in moved:
        if reg not in order:
            order.append(reg)
    regions_out = []
    for reg in order:
        markers = moved.get(reg)
        if not markers:
            continue
        regions_out.append({
            "region": reg,
            "markers": [{"name": m, "placements": len(markers[m]),
                         "refs": sorted(set(markers[m]))}
                        for m in sorted(markers, key=lambda m: m.lower())],
            "placements": sum(len(v) for v in markers.values()),
        })
    return {
        "regions": regions_out,
        "total_markers": sum(len(r["markers"]) for r in regions_out),
        "total": sum(r["placements"] for r in regions_out),
        # per-ITEM map folder; the region tile filename is appended by the renderer
        "map_base": ebuild.image_base(doc, doc.get("set") or "", doc.get("name") or ""),
    }


# ── the split ────────────────────────────────────────────────────────────────
def split(doc, ok_refs, drop_nests=False, moved=None):
    """Strip non-fixed placements from `regions`, then (re)build `chance_spawns`.
    Returns the number of doc changes (0 = already correct).

    `moved` is the source-derived {region: {marker: [refs]}} from chance_points(); when
    given it REPLACES chance_spawns outright, so the block is rebuilt from the game
    data every run rather than accumulated from whatever the doc happened to hold.

    `drop_nests` removes `nest` placements as well, for a page that never declared a
    nest as its source (spawns_engine.classify.make_farming_classify). There a
    Deathclaw Nest is an ordinary container, so it belongs in Containers with its
    rng76 rate — the same treatment chem_loot_collapse gives every other container.
    The Containers row appears after the next build_farming_used_for.py --all."""
    stripped = defaultdict(dict)   # region -> marker -> [ref, ...]
    n_moved = 0
    n_nest = 0

    for region in doc.get("regions", []) or []:
        kept_locs = []
        for loc in region.get("locations", []) or []:
            spawns = loc.get("spawns") or []
            kept = []
            for s in spawns:
                ref = s.get("ref") or ""
                if drop_nests and s.get("source_type") == "nest":
                    n_nest += 1
                    continue
                if s.get("source_type") == "direct" and ref:
                    try:
                        is_fixed = int(ref, 16) in ok_refs
                    except ValueError:
                        is_fixed = True
                    if not is_fixed:
                        # keep the ref — the per-item region tile is drawn from it
                        stripped[region.get("region", "")].setdefault(
                            loc.get("marker", ""), []).append(ref)
                        n_moved += 1
                        continue
                kept.append(s)
            if len(kept) == len(spawns):
                kept_locs.append(loc)
                continue
            if not kept:
                continue                      # marker had nothing but chance points
            loc["spawns"] = kept
            loc["count"] = len(kept)
            loc["sources"] = dict(sorted(Counter(s.get("source_type") for s in kept).items()))
            loc["refs"] = sorted({s.get("ref", "") for s in kept if s.get("ref")})
            kept_locs.append(loc)
        region["locations"] = kept_locs

    # Region order follows the doc's own `regions` list (already the canonical
    # ALL_REGIONS order); markers are A-Z inside each region.
    order = [r.get("region", "") for r in doc.get("regions", []) or []]
    source = moved if moved is not None else stripped
    block = chance_block(dict(source), order, doc) if source else None

    changed = n_moved + n_nest
    if block and block != doc.get("chance_spawns"):
        doc["chance_spawns"] = block
        changed = changed or 1

    if not changed:
        return 0

    doc["total"] = sum(l.get("count", 0) for r in doc.get("regions", []) or []
                       for l in r.get("locations", []) or [])
    meta = doc.get("_meta")
    if isinstance(meta, dict):
        totals = Counter()
        for r in doc.get("regions", []) or []:
            for l in r.get("locations", []) or []:
                totals.update(l.get("sources") or {})
        meta["source_totals"] = dict(sorted(totals.items()))
    return changed


# ── per-family seed resolution ───────────────────────────────────────────────
def _farming_targets():
    """{dist filename -> (item_records, drop_nests)} for dist/farming_spawns/*.

    drop_nests mirrors make_farming_classify: a nest is a distinct source only on a
    page whose config declares it (drop_rates.containers.marker_label)."""
    from farming_spawns_config import ALL_SETS
    out = {}
    for c in ALL_SETS:
        label = ((c.get("drop_rates") or {}).get("containers") or {}).get("marker_label")
        out[f"{c['slug']}_spawns.json"] = (c["items"], not label)
    return out


def _drink_targets(paths):
    """{dist filename -> item_records} for the Nuka-Cola drink docs. Seeds go
    through drink_alch() — never index DRINK_ALCH directly (spawn-guide 9k)."""
    from spawns_configs.nuka_cola import drink_alch
    out = {}
    for p in paths:
        slug = os.path.basename(p)[len("nuka_cola_spawns_"):-len(".json")]
        fids = drink_alch(slug)
        if fids:
            out[os.path.basename(p)] = [{"formid": f, "sig": "ALCH"} for f in fids]
    return out


def _collectable_targets(module):
    """(item_records, dedication_seeds) for the DF collectable location guides."""
    try:
        mod = __import__(f"spawns_configs.{module}", fromlist=["load_item_records"])
        recs = mod.load_item_records()[0]
        wide = set()
        if hasattr(mod, "load_dedication_seeds"):
            wide = mod.load_dedication_seeds() | {r["formid"].upper() for r in recs}
        return recs, (wide or None)
    except Exception as exc:                                    # pragma: no cover
        print(f"  [warn] {module} seeds unavailable ({exc})")
        return None, None


# ── geo caches (where the chance points' coords/markers live) ────────────────
REPO = os.path.dirname(HERE)
_GEO_CACHE = {}


def geo_cache(path):
    if path not in _GEO_CACHE:
        try:
            _GEO_CACHE[path] = json.load(open(path, encoding="utf-8"))
        except Exception:
            _GEO_CACHE[path] = {}
    return _GEO_CACHE[path]


def farming_geo(slug):
    return geo_cache(os.path.join(REPO, "data", "farming_spawns",
                                  f"geo_cache_{slug.replace('-', '_')}.json"))


def drinks_geo():
    return geo_cache(os.path.join(REPO, "data", "nuka_cola_spawns", "geo_cache.json"))


# ── driver ───────────────────────────────────────────────────────────────────
def _apply(path, item_records, tbls, log, dedication_seeds=None, drop_nests=False,
           geo=None):
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:
        return 0
    ok = dedicated_refs(item_records, tbls, dedication_seeds)
    moved = None
    if geo:
        all_regions = [r.get("region", "") for r in doc.get("regions", []) or []]
        moved = chance_points(item_records, tbls, ok, geo, all_regions)
    n = split(doc, ok, drop_nests, moved)
    if n:
        json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        cs = (doc.get("chance_spawns") or {}).get("total", 0)
        log.append((os.path.basename(path), cs, doc["total"]))
    return n


def run(dist_dir="dist"):
    import glob
    tbls = esources.load_tables()
    log = []

    root = os.path.join(dist_dir, "farming_spawns")
    if os.path.isdir(root):
        targets = _farming_targets()
        for fn in sorted(os.listdir(root)):
            if fn.endswith("_spawns.json") and fn in targets:
                recs, drop_nests = targets[fn]
                slug = fn[:-len("_spawns.json")]
                _apply(os.path.join(root, fn), recs, tbls, log, None, drop_nests,
                       farming_geo(slug))

    drinks = sorted(glob.glob(os.path.join(dist_dir, "nuka_cola_spawns_*.json")))
    if drinks:
        dgeo = drinks_geo()
        for fn, recs in _drink_targets(drinks).items():
            _apply(os.path.join(dist_dir, fn), recs, tbls, log, None, False, dgeo)

    for module, sub in (("bobbleheads", "bobblehead_spawns"),
                        ("magazines", "magazine_spawns")):
        d = os.path.join(dist_dir, sub)
        if not os.path.isdir(d):
            continue
        recs, wide = _collectable_targets(module)
        if not recs:
            continue
        for fn in sorted(os.listdir(d)):
            if fn.endswith(".json"):
                _apply(os.path.join(d, fn), recs, tbls, log, wide)

    if log:
        print("[split_chance_spawns] moved shared-loot-pool points out of Fixed Spawn:")
        for fn, n, total in log:
            print(f"  {fn:<46} {n:>5} chance  |  {total:>5} fixed")
    print(f"[split_chance_spawns] {len(log)} docs changed (dist={dist_dir})")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "dist")
