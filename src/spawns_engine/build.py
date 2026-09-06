#!/usr/bin/env python3
r"""
spawns_engine.build — the shared build core for every "{Item} Spawn Locations"
page: Mappalachia Position lookups, geo-cache IO, hand-authored field
preservation, placement resolution, and region/marker grouping.

Coordinates live only in the local Mappalachia Position table (~480 MB, not in
CI). A LOCAL run with the DB resolves every placement and writes a committed
geo cache; every later run WITHOUT the DB rebuilds straight from that cache plus
the committed TSVs — fully automated.

Each item FAMILY (drinks, farming/eggs) supplies its own thin driver
(spawns_configs.*) that calls get_sources() with its classifier, then these
helpers, then assembles its exact output doc. The heavy lifting lives here once.
"""

import os, json, re
from collections import defaultdict

SQL_CHUNK = 900  # stay under SQLite's 999-variable limit


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ── DB queries ───────────────────────────────────────────────────────────────
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


# ── geo cache ────────────────────────────────────────────────────────────────
def load_cache(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(cache, open(path, "w", encoding="utf-8"), ensure_ascii=False)


# ── hand-authored field preservation ─────────────────────────────────────────
def load_existing(path):
    """Load hand-authored fields from the previous build to preserve on rebuild.

    Marker-level slots are keyed (region, marker); PER-SPAWN slots are keyed by the
    placement's ref inside `spawns`, so a marker holding several spawns keeps each
    one's own photos/directions even as the placement list grows or reorders."""
    keep = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in old.get("regions", []):
        for loc in reg.get("locations", []):
            spawns = {}
            for sp in loc.get("spawns") or []:
                ref = sp.get("ref") or ""
                saved = {k: sp.get(k, "") for k in ("image_top", "directions", "image_bottom")
                         if sp.get(k)}
                if ref and saved:
                    spawns[ref] = saved
            keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                "image_top": loc.get("image_top", ""),
                "directions": loc.get("directions", ""),
                "image_bottom": loc.get("image_bottom", ""),
                "spawns": spawns,
            }
    return keep


# ── placement resolution ─────────────────────────────────────────────────────
def resolve_placements(src, geo, cur, cache, db_ok):
    """Given a get_sources() result, return
        (seen: {instanceFormID: (x, y, region, marker, source_type)}, lists_n).

    DB present -> query Position, resolve via geo, record into cache.
    DB absent  -> reconstruct from the committed cache + the TSV source closure.
    """
    base_type = {int(fid, 16): meta["source_type"] for fid, meta in src["placed_bases"].items()}
    # A placed REFR is only a FIXED spawn when the list behind it is dedicated to this
    # item (spawns_engine.sources.dedicated_lists). A shared loot pool's point is real
    # but what appears there is a gamble -> "chance", which group_regions keeps out of
    # Fixed Spawn Locations and group_chance renders separately.
    direct_type = {int(fid, 16): ("direct" if meta.get("dedicated", True) else "chance")
                   for fid, meta in src["direct_refrs"].items()}
    direct_ints = set(direct_type)
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
            seen[inst] = (x, y, region, marker, direct_type[inst])
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
                              e.get("marker", ""), direct_type[inst])

    return seen, lists_n


# ── region / marker grouping ─────────────────────────────────────────────────
CHANCE_TYPES = ("chance",)     # shared-loot-pool points -> Chance to Spawn Locations

# The Chance to Spawn map is PER ITEM, per region — a shared blank region tile would
# be useless, because it would have to carry every farming page's points at once.
# Files follow the site's established spawn-photo convention:
#   /wp-content/uploads/guide-images/<category>/<item>/<region-slug>-chance-map.avif
UPLOADS = "/wp-content/uploads/guide-images/"
_IMAGE_BASE_RE = re.compile(r"(/wp-content/uploads/guide-images/[^/\"']+/[^/\"']+/)")

# slug prefix / membership -> category folder. Same families the guide folders use.
_EGG_SLUGS = {"deathclaw-egg", "frog-egg", "mirelurk-egg", "mothman-egg",
              "radscorpion-egg", "radtoad-egg"}


def region_slug(name):
    """'Ash Heap' -> 'ash-heap'. The filename half of a region map."""
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-",
                                     str(name or "").lower()).lstrip("-"))


def image_base(doc, slug, name="", category=None):
    """The item's guide-images folder, as a site-absolute path ending in '/'.

    Rule 1 (authoritative): if the doc already carries a hand-authored spawn photo
    under guide-images/<cat>/<item>/, reuse THAT folder. The live site is the truth —
    e.g. deathclaw-egg's folder is `deathclaw-eggs`, plural, which no slug rule would
    have guessed.
    Rule 2: derive it — category from the slug family, item folder from the display
    name. Only used for items that have no photos on the page yet.
    """
    blob = json.dumps(doc) if isinstance(doc, (dict, list)) else str(doc or "")
    m = _IMAGE_BASE_RE.search(blob)
    if m:
        return m.group(1)

    if category is None:
        if slug.startswith("chems-"):
            category = "farming-chems"
        elif slug in _EGG_SLUGS:
            category = "farming-eggs"
        elif slug.startswith("nuka") or slug in ("sunset-sarsaparilla", "nukashine"):
            category = "farming-nuka-cola"
        else:
            category = "farming-non-perishable"
    item = region_slug(name) or region_slug(slug)
    return f"{UPLOADS}{category}/{item}/" if item else ""


# Above this many placements a page is DENSE: the renderers stop drawing a slot per
# spawn and show one marker-level slot instead (df-bnb-farming-plants.js /
# -meat.js DENSE_PAGE). Keep the two in step.
DENSE_PAGE = 250


def compact_spawns(regions_out):
    """Strip the per-spawn slots a dense page will never show, in place.

    On a dense page the renderer already collapses to ONE marker-level photo slot,
    so the per-spawn `spawns[]` entries are invisible — but they still ship. Once the
    plants pipeline started resolving leveled-placed flora, Blackberries went to
    13,342 placements and a 3.9 MB document of empty slots nobody can see.

    What survives: every entry that carries authored content (a photo or directions),
    because that is real work and must never be thrown away, and the marker-level
    `refs` / `coords`, which is what render_spawn_maps plots. Only blank placeholders
    go. Returns the number of entries dropped."""
    dropped = 0
    for reg in regions_out:
        for loc in reg.get("locations") or []:
            spawns = loc.get("spawns") or []
            kept = [sp for sp in spawns
                    if sp.get("image_top") or sp.get("directions") or sp.get("image_bottom")]
            if len(kept) != len(spawns):
                dropped += len(spawns) - len(kept)
                loc["spawns"] = kept
                # The builders assert spawns[] == count on every location, which is a
                # good guard against silently losing a placement. Flag the ones that
                # were trimmed deliberately so that check still catches real drops.
                loc["spawns_compacted"] = True
    return dropped


def group_regions(seen, all_regions, keep, exclude_types=CHANCE_TYPES):
    """Group resolved placements into the per-region location lists. Returns
        (regions_out, src_totals, unresolved, total, placements)
    with the exact shapes the pre-refactor builds emitted.

    `exclude_types` are held back for group_chance() — a point whose list is a shared
    loot pool is not a fixed spawn and must never be counted here."""
    exclude = set(exclude_types or ())
    grouped = defaultdict(lambda: {"count": 0, "refs": [], "coords": None,
                                   "sources": defaultdict(int), "places": []})
    unresolved = defaultdict(int)
    for inst, (x, y, region, marker, stype) in seen.items():
        if stype in exclude:
            continue
        if region not in all_regions:
            unresolved[marker or f"instance {inst:06X}"] += 1
            continue
        g = grouped[(region, marker)]
        g["count"] += 1
        g["sources"][stype] += 1
        ref = f"{int(inst):06X}"
        g["refs"].append(ref)
        # Every placement is kept individually so a marker with N spawns can render
        # N photo/map slots instead of one shared pair (see `spawns` below).
        g["places"].append({"ref": ref, "source_type": stype,
                            "coords": [x, y] if x is not None and y is not None else []})
        if g["coords"] is None and x is not None and y is not None:
            g["coords"] = [x, y]

    by_region = defaultdict(list)
    for (region, marker), g in grouped.items():
        prev = keep.get((region, marker), {})
        prev_spawns = prev.get("spawns") or {}
        # One `spawns` entry per placement: its own map shot, directions and
        # in-place photo. `label` is left blank here — a family driver may set a
        # nicer one (e.g. "Deathclaw Nest #2"); the renderer falls back to
        # "Spawn N". Slots are preserved across rebuilds by ref.
        places = sorted(g["places"], key=lambda p: (p["source_type"], p["ref"]))
        spawns = []
        for p in places:
            hf = prev_spawns.get(p["ref"], {})
            spawns.append({
                "label": "",
                "ref": p["ref"],
                "source_type": p["source_type"],
                "coords": p["coords"],
                "image_top": hf.get("image_top", ""),
                "directions": hf.get("directions", ""),
                "image_bottom": hf.get("image_bottom", ""),
            })
        by_region[region].append({
            "marker": marker,
            "count": g["count"],
            "sources": dict(sorted(g["sources"].items())),
            "image_top": prev.get("image_top", ""),
            "directions": prev.get("directions", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "refs": sorted(set(g["refs"])),
            "coords": g["coords"] or [],
            "spawns": spawns,
        })

    regions_out = []
    for region in all_regions:
        locs = sorted(by_region.get(region, []), key=lambda l: l["marker"].lower())
        regions_out.append({"region": region, "locations": locs})

    total = sum(len(r["locations"]) for r in regions_out)
    placements = sum(loc["count"] for r in regions_out for loc in r["locations"])
    src_totals = defaultdict(int)
    for r in regions_out:
        for loc in r["locations"]:
            for t, n in loc["sources"].items():
                src_totals[t] += n

    return (regions_out, dict(sorted(src_totals.items())),
            {k: unresolved[k] for k in sorted(unresolved)}, total, placements)


def group_chance(seen, all_regions, chance_types=CHANCE_TYPES):
    """Build the `chance_spawns` block: the world points whose leveled list is a
    SHARED loot pool, so the item is only one of several things that can appear.

    Deliberately NAMES ONLY on the page. These points are far too numerous and too
    low-odds to photograph individually (Addictol alone has 307 of them), so the
    expand lists marker names A-Z inside each region A-Z and links out to that
    ITEM's map for that region. No photo slots — that is what makes Fixed Spawn
    Locations worth reading.

    Each marker still carries its `refs`, because the map is per ITEM: a shared blank
    region tile would be useless (it would have to carry every farming page's points
    at once). `render_spawn_maps.py` resolves these refs through the geo cache to draw
    `03 Region Tiles/<RegionSlug>_<slug>_chance.jpg`. Refs are data for the map
    builder, never rendered on the page.
    """
    types = set(chance_types or ())
    by_region = defaultdict(lambda: defaultdict(list))
    total = 0
    for inst, (_x, _y, region, marker, stype) in seen.items():
        if stype not in types or region not in all_regions or not marker:
            continue
        by_region[region][marker].append(f"{int(inst):06X}")
        total += 1

    regions_out = []
    for region in all_regions:
        markers = by_region.get(region)
        if not markers:
            continue
        regions_out.append({
            "region": region,
            "markers": [{"name": m, "placements": len(markers[m]),
                         "refs": sorted(set(markers[m]))}
                        for m in sorted(markers, key=lambda m: m.lower())],
            "placements": sum(len(v) for v in markers.values()),
        })
    return {"regions": regions_out,
            "total_markers": sum(len(r["markers"]) for r in regions_out),
            "total": total}
