#!/usr/bin/env python3
r"""
spawns_configs.bobbleheads — the Bobblehead family driver for the shared spawns engine.

PAGES (DF brand, under /df/collectables/)

    /df/collectables/bobbleheads/location-guide/            <- HUB
        The full spawn-guide root-expand set EXCEPT the marker dump: Used For
        (one When-Consumed sub-expand per bobblehead), Collectrons, Containers,
        Creatures, Events & Activities, Resource Generators, Vendors, and a
        Fixed Spawn Locations expand that renders the REGION INDEX (links out).

    /df/collectables/bobbleheads/location-guide/<region>/   <- TEN REGION PAGES
        Fixed Spawn Locations ONLY, filtered to that region — every marker A-Z,
        one Map location / Directions / Item in place block PER SPAWN (§9k).

RENDERER: no new JS. Both page types are drawn by the existing
`df-bnb-farming-non-perishable-guide.js` (the Deathclaw Egg guide renderer), which
already has `mountRegionPage()` for per-region location pages and
`renderRegionIndex()` for the hub. DF brand colours come free — the shell flips
its tokens on a /df/ path. Only the path matcher + JSON route were added there.
Consequently THIS BUILDER MUST EMIT THE FARMING DOC SHAPE: one file carrying
`regions[]`, `fixed_spawn_index`, `drop_rates`, `vendor_list`, `events_activities`
and `item_breakdown`.

SEEDS — the world lists carry the MISC `_Pickup` records, NOT the ALCH `_Potion`
records. `LL_Loot_Bobbleheads` (004FC08F) is 20x `BobbleHead_*_Pickup:MISC` plus
`GHL_LLS_GlowingBobbleheads`; the ALCH potion is what you get after picking one up.
Seeding ALCH alone finds ZERO world spawns — it must be BOTH. Membership resolves
by EDID regex against the committed MISC/ALCH exports, never a hardcoded FormID list.

SPAWN RATE — a bobblehead point is NOT guaranteed. `LPI_Loot_Bobbleheads`
(0001911D) carries `LVLG_ChanceNoneGlobal = LPI_Chance_BobbleHeads_ECON`, an
economy-tuned GLOB currently at 80.0, so each placed point has a
`1 - 80/100 = 20%` chance of holding a bobblehead on any given server. The figure
is READ from `LVLI_Math.ListChanceNoneResolved` at build time, never typed, so it
tracks any Bethesda economy retune automatically (drop-rate-engine §3b).

Usage:
    python src/build_spawns.py bobbleheads
"""

import os, re, csv, sys, glob, json, sqlite3, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_engine.geo import Geo
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine import events as eevents
from spawns_engine.classify import nuka_classify
import tsv_source          # one resolver for every export selection

try:
    from build_farming_used_for import build_consumption
except Exception:
    build_consumption = None

# ── constants ────────────────────────────────────────────────────────────────
SET_SLUG = "bobbleheads"
SET_NAME = "Bobbleheads"
PAGE_TITLE = "Bobbleheads Location Guide"
# Re-homed Aug 2026: bobbleheads live under Farming - Consumables (one home), not
# /df/collectables/. Same proven page structure (…/location-guide/ hub + region
# children), just under the consumables path. Renderer matches both (old=redirect).
URL_BASE = "/df/farming/consumables/bobbleheads/location-guide/"

# The world spawn-point list. Everything else (rate, entries, member items) is
# read from the exports — this is the one seed the page can't infer.
WORLD_LIST_ID = "0001911D"
WORLD_LIST_EDID = "LPI_Loot_Bobbleheads"

ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]

REGION_SLUGS = {
    "Ash Heap": "ash-heap", "Atlantic City": "atlantic-city",
    "Burning Springs": "burning-springs", "Cranberry Bog": "cranberry-bog",
    "Forest": "forest", "Savage Divide": "savage-divide",
    "Skyline Valley": "skyline-valley", "The Mire": "the-mire",
    "The Pitt": "the-pitt", "Toxic Valley": "toxic-valley",
}

# Live bobblehead pickups. `zzz_Babylon_*` (cut Vault 51 / Nuclear Winter build)
# and `BACKUP_*` (deprecated pre-launch MISC) are excluded on purpose — they are
# flagged isCut in dist/collectables_bobbleheads.json and place nothing live.
PICKUP_EDID_RE = re.compile(r"^(GHL_Glowing)?BobbleHead_\w+_Pickup$")

# QA / debug / cut-content holders inherit loot lists but aren't reachable in game.
DEV_EDID_RE = re.compile(r"(^qa|^test|_test|debug|zzz_|babylon)", re.I)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "bobblehead_spawns")
OUT_FILE = os.path.join(OUT_DIR, "bobbleheads_spawns.json")
TSV = esources.TSV
GEO_CACHE = os.environ.get(
    "BOBBLEHEAD_GEO_CACHE",
    os.path.join(REPO, "data", "bobblehead_spawns", "geo_cache.json"))
SOURCE_TAG = "Game-file exports (LVLI/MISC/ALCH) + Mappalachia Position (cached for CI)"

# classify() source_type -> the renderer expand it belongs in. `direct` is absent
# on purpose: those are the world spawn points and live in Fixed Spawn Locations.
CONTAINER_TYPES = {"container", "loot-list"}


# ── file helpers ─────────────────────────────────────────────────────────────
def _newest(pattern):
    return tsv_source.newest(os.path.join(TSV, pattern), required=False)


# ── seeds ────────────────────────────────────────────────────────────────────
def load_item_records():
    """Every live bobblehead record that seeds the closure: the MISC `_Pickup`
    world objects (what the leveled lists place) AND the ALCH `_Potion`
    consumables (what vendors / boxes / event rewards hand out).

    Returns (records, pickup_names, alch_items) where alch_items is
    [{form_id, name}] for the Used For per-item breakdown."""
    recs, pickup_names, alch_items = [], {}, []

    misc = _newest("MISC_Export_*.tsv")
    if not misc:
        raise FileNotFoundError(f"no MISC_Export_*.tsv in {TSV}")
    with open(misc, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if PICKUP_EDID_RE.match(edid):
                fid = (r.get("FormID") or "").strip().upper()
                full = (r.get("FULL") or "").strip()
                recs.append({"formid": fid, "sig": "MISC", "edid": edid, "full": full})
                pickup_names[fid] = full

    # ALCH potions come from the already-built collectables JSON so the guide and
    # the Bobbleheads Checklist can never disagree on membership.
    try:
        bh = json.load(open(os.path.join(DIST, "collectables_bobbleheads.json"),
                            encoding="utf-8"))
        for grp in bh.get("groups", []):
            for it in grp.get("items", []):
                if it.get("isCut"):
                    continue
                fid = (it.get("formId") or "").strip().upper()
                if not fid:
                    continue
                recs.append({"formid": fid, "sig": "ALCH", "edid": it.get("edid", "")})
                alch_items.append({"form_id": fid, "edid": it.get("edid", ""),
                                   "group": grp.get("name", "")})
    except Exception as e:
        print(f"[bobbleheads] [warn] collectables_bobbleheads.json unreadable ({e}); "
              "ALCH seeds skipped — world spawns are unaffected.")

    return recs, pickup_names, alch_items


def alch_display_names():
    """ALCH FormID -> FULL, so the Used For sub-expands are named the way the game
    names them ('Bobblehead: Small Guns'), never the editor ID."""
    out = {}
    alch = _newest("ALCH_Export_*.tsv")
    if not alch:
        return out
    with open(alch, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            fid = (r.get("ALCH_FormID") or "").strip().upper()
            if fid:
                out[fid] = (r.get("FULL") or "").strip()
    return out


# ── world spawn rate (read from the export, never typed) ─────────────────────
def world_spawn_rate():
    """Per-point spawn chance for WORLD_LIST_ID, from LVLI_Math's resolved list
    ChanceNone. Returns (rate, glob_edid, chance_none_value) — rate is None when
    the export can't be read, so the page shows no rate rather than a guess."""
    math = _newest("LVLI_Export_*_LVLI_Math.tsv")
    if not math:
        return None, "", None
    with open(math, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if (r.get("LVLI_FormID") or "").strip().upper() != WORLD_LIST_ID:
                continue
            try:
                cn = float(r.get("ListChanceNoneResolved") or "")
            except ValueError:
                return None, "", None
            glob_tok = (r.get("ListChanceNoneGlobal") or "").split(":")
            glob_edid = glob_tok[1] if len(glob_tok) >= 2 else ""
            return max(0.0, 1.0 - cn / 100.0), glob_edid, cn
    return None, "", None


def pct(v, places=2):
    if v is None:
        return ""
    s = f"{v * 100:.{places}f}".rstrip("0").rstrip(".")
    return (s or "0") + "%"


def r6(v):
    """Round a probability for output — float math leaks 0.19999999999999996."""
    return None if v is None else round(float(v), 6)


# ── display names for leveled-list sources ───────────────────────────────────
# DISPLAY ONLY — never used for routing. Leveled lists carry no FULL name, so the
# generic prettifier can only produce editor-speak ("LL Scavenger Bobble Heads").
# These regexes put the player-facing name on the handful of sources that reach
# the page. Routing stays 100% EDID/signature driven (spawn-guide §9k).
DISPLAY_OVERRIDES = [
    (re.compile(r"^LL_BobbleheadBox_Loot_(List_)?Glowing$|^LL_BobbleheadBox_Loot_Glowing$", re.I),
     "Glowing Bobblehead Box"),
    (re.compile(r"^LL_BobbleheadBox_Loot(_Normal)?$", re.I), "Bobblehead Box"),
    (re.compile(r"^MILE_LL_Scavenger_BobbleHeads$", re.I), "Milepost Zero scavenger (bobblehead pool)"),
    (re.compile(r"^MILE_LL_Scavenger_Master$", re.I), "Milepost Zero scavenger"),
    (re.compile(r"^BURN_LLV_Vendor_RE_Dr_", re.I), "Burning Springs doctor"),
    (re.compile(r"^Burn_Doctor_VendorChest", re.I), "Burning Springs doctor"),
    (re.compile(r"^GHL_LLS_GlowingBobbleheads$", re.I), "Glowing bobblehead pool"),
]
_GLUE_RE = re.compile(r"\bBobble Heads?\b")


def prettify(edid):
    if not edid:
        return ""
    for rx, name in DISPLAY_OVERRIDES:
        if rx.search(edid):
            return name
    s = re.sub(r"^(?:(?:LL[SVEDIC]?|LPI|LLD|BURN|GHL|MILE|ATX|NWOT|RE)_)+", "", edid)
    s = re.sub(r"_+", " ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    return re.sub(r"\s+", " ", _GLUE_RE.sub("Bobbleheads", s)).strip()


# ── hand-authored slot preservation ──────────────────────────────────────────
def load_keep():
    """Preserve hand-authored photos/directions across rebuilds (spawn-guide §4d).
    Marker slots key on (region, marker); per-spawn slots key on the placement ref."""
    return ebuild.load_existing(OUT_FILE)


def attach_labels(regions_out):
    """Name each per-spawn block. Every bobblehead placement is the same
    LPI spawn point, so they number within their marker: `Bobblehead Spawn #1..N`.
    A marker holding a single spawn drops the `#1` (§9k). Vendor placements are
    skipped — they render in the flat vendor table, not as spawn blocks."""
    for reg in regions_out:
        for loc in reg["locations"]:
            spawns = [s for s in (loc.get("spawns") or [])
                      if s.get("source_type") != "vendor"]
            single = len(spawns) <= 1
            for i, sp in enumerate(spawns, 1):
                sp["label"] = "Bobblehead Spawn" if single else f"Bobblehead Spawn #{i}"


def attach_breakdowns(regions_out, rate_display):
    """One breakdown row per marker so the renderer's 'Collectable here:' note
    states the count and the per-point chance."""
    for reg in regions_out:
        for loc in reg["locations"]:
            n = sum(v for k, v in (loc.get("sources") or {}).items() if k != "vendor")
            if not n:
                continue
            loc["breakdown"] = [{
                "label": "bobblehead spawn point" + ("" if n == 1 else "s"),
                "count": n,
                "rate_key": "world_spawns",
                "rate_display": rate_display,
            }]


# ── non-location sources ─────────────────────────────────────────────────────
def build_sources(src, tbls, appearance_fn, targets):
    """Return (containers_node, vendor_lists, closure_container_rows).

    Containers: leveled lists in the closure that are loot boxes / scavenger pools.
    Vendors: vendor-typed placed bases, joined to their stock list for a rate."""
    parent_edid = tbls["parent_edid"]
    # rng76 keys on the list FormID, but placed_bases records the holder's stock
    # list by EDID, so invert the map once.
    edid_to_fid = {}
    for fid, edid in parent_edid.items():
        if edid:
            edid_to_fid.setdefault(edid, fid)

    def rate_of(list_ref):
        """list_ref may be a FormID or an EDID."""
        if not appearance_fn or not list_ref:
            return None
        lid = list_ref if re.fullmatch(r"[0-9A-Fa-f]{8}", str(list_ref)) \
            else edid_to_fid.get(list_ref)
        if not lid:
            return None
        try:
            p = appearance_fn(lid, targets)
            return r6(p) if p and p > 0 else None
        except Exception:
            return None

    boxes = []
    seen_names = set()
    for lv in sorted(src["lvli_closure"]):
        edid = parent_edid.get(lv) or ""
        if not edid or DEV_EDID_RE.search(edid):
            continue
        if not re.search(r"(box|scavenger|cache|loot_?bag)", edid, re.I):
            continue
        name = prettify(edid)
        if name in seen_names:
            continue
        seen_names.add(name)
        boxes.append({"name": name, "edid": edid, "list_id": lv,
                      "rate": rate_of(lv), "rate_display": pct(rate_of(lv))})
    boxes.sort(key=lambda b: (-(b["rate"] or 0), b["name"].lower()))

    vendors = []
    for fid, meta in sorted(src["placed_bases"].items()):
        if meta.get("source_type") != "vendor":
            continue
        edid, via = meta.get("edid") or "", meta.get("via") or ""
        if DEV_EDID_RE.search(edid) or DEV_EDID_RE.search(via):
            continue
        r = rate_of(via) if via else None
        vendors.append({"base": fid.upper(), "name": prettify(edid) or prettify(via),
                        "stock_list": via, "rate": r, "rate_display": pct(r)})
    return boxes, vendors


def container_node(boxes):
    """drop_rates.containers — the renderer draws one rate + note block, so the
    box list is folded into the note (rate = the best resolved box chance)."""
    if not boxes:
        return None
    best = boxes[0]
    others = ", ".join(b["name"] for b in boxes[1:])
    note = ("opening a " + best["name"]
            + (" — also from the " + others if others else "")
            + ". Bobblehead boxes are handed out by the Scoreboard, Giuseppe the "
              "Stamp Vendor, public events, the Gleaming Depths raid and the "
              "U Mine It treasure maps rather than found in world containers.")
    return {"rate": r6(best["rate"]), "rate_display": best["rate_display"] or "",
            "rate_source": "computed" if best["rate"] else None,
            "container_edid": best["edid"], "note": note,
            "boxes": boxes}


def vendor_rows(vendors, seen, cache):
    """Flat vendor_list (spawn-guide §9e) — one row per merchant, located through
    the same geo pass as every other placement."""
    placed = {}
    for inst, (x, y, region, marker, stype) in seen.items():
        if stype == "vendor":
            base = (cache.get(str(inst)) or {}).get("base")
            placed.setdefault(base, []).append((region, marker))
    rows = []
    for v in vendors:
        # A random-encounter merchant (`_RE_` in its stock list) has no fixed
        # placement — its chest base sits in a holding cell, so the resolved
        # marker would be a lie. Report it as roaming instead.
        roaming = "_RE_" in (v.get("stock_list") or "")
        spots = [("", "")] if roaming else (placed.get(int(v["base"], 16)) or [("", "")])
        for region, marker in spots:
            if roaming:
                marker, region, vtype = "Random encounter", "", "Random encounter vendor"
            else:
                vtype = ("Train station vendor" if marker.endswith("Station")
                         else "Settlement vendor")
            rows.append({
                "name": v["name"], "marker": marker, "region": region,
                "vendor_type": vtype,
                "rate_lines": [v["rate_display"]] if v["rate_display"] else [],
                "rate_display": v["rate_display"] or "",
                "rate_value": v["rate"] or 0, "count": 1,
                "stock_list": v["stock_list"],
            })
    rows.sort(key=lambda r: (-r["rate_value"], (r["marker"] or "").lower()))
    return rows


def build_item_breakdown(alch_items, names):
    """Used For — one When-Consumed sub-expand per bobblehead, built from its own
    ALCH record (renderMultiItemUsedFor in the renderer)."""
    if build_consumption is None:
        return []
    out = []
    for it in alch_items:
        fid = it["form_id"]
        name = names.get(fid) or prettify(it.get("edid", "")) or fid
        try:
            cons = build_consumption(fid, TSV, item_name=name)
        except Exception:
            cons = None
        out.append({"name": name, "form_id": fid, "group": it.get("group", ""),
                    "consumption": cons})
    out.sort(key=lambda b: ((b["group"] or ""), (b["name"] or "").lower()))
    return out


# ── entry point ──────────────────────────────────────────────────────────────
def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()

    item_records, pickup_names, alch_items = load_item_records()
    targets = {r["formid"].upper() for r in item_records}
    print(f"[bobbleheads] {len(item_records)} seed records "
          f"({sum(1 for r in item_records if r['sig'] == 'MISC')} MISC pickups + "
          f"{sum(1 for r in item_records if r['sig'] == 'ALCH')} ALCH potions)")

    rate, glob_edid, cn_value = world_spawn_rate()
    rate_display = pct(rate)
    print(f"[bobbleheads] world spawn chance {rate_display or 'unresolved'} "
          f"(GLOB {glob_edid or 'n/a'} = {cn_value} ChanceNone)")

    tbls = esources.load_tables()
    src = esources.get_sources(item_records, tbls, nuka_classify)
    print(f"[bobbleheads] closure {len(src['lvli_closure'])} lists · "
          f"{len(src['direct_refrs'])} direct placements · "
          f"{len(src['placed_bases'])} holder bases")

    appearance_fn = None
    try:
        import rng76
        _res = rng76.Rng76Data.from_tsv_root(TSV).resolver
        appearance_fn = lambda lid, t: _res.appearance_prob(lid, t)
        print("[bobbleheads] rng76 loaded — container / vendor / event rates computed.")
    except Exception as e:
        print(f"[bobbleheads] [warn] rng76 unavailable ({e}); source rates blank.")

    db_ok = os.path.exists(MAPPALACHIA_DB)
    cache = ebuild.load_cache(GEO_CACHE)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[bobbleheads] Mappalachia DB found — resolving placements and refreshing geo cache.")
    elif cache:
        print(f"[bobbleheads] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[bobbleheads] No Mappalachia DB and no geo cache — cannot build. Run once "
              "locally with MAPPALACHIA_DB set to seed data/bobblehead_spawns/geo_cache.json.")
        return

    keep = load_keep()
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)
    # Shared-loot-pool points held back by group_regions — names only (group_chance).
    chance_spawns = ebuild.group_chance(seen, ALL_REGIONS)
    attach_labels(regions_out)
    attach_breakdowns(regions_out, rate_display)

    events_activities = eevents.detect(src["lvli_closure"], tbls["parent_edid"],
                                       c2p=tbls["c2p"])
    eevents.resolve_event_rates(events_activities, targets, appearance_fn)

    boxes, vendors = build_sources(src, tbls, appearance_fn, targets)
    vlist = vendor_rows(vendors, seen, cache)

    # Region index — what the hub's Fixed Spawn Locations expand renders instead of
    # 271 marker sub-expands (they live on the ten region pages).
    index = []
    world_total = 0
    for r in regions_out:
        n = sum(sum(v for k, v in (l.get("sources") or {}).items() if k != "vendor")
                for l in r["locations"])
        markers = sum(1 for l in r["locations"]
                      if any(k != "vendor" for k in (l.get("sources") or {})))
        world_total += n
        index.append({"region": r["region"], "slug": REGION_SLUGS[r["region"]],
                      "url": f"{URL_BASE}{REGION_SLUGS[r['region']]}/",
                      "count": markers, "spawns": n})

    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG,
                  "lists_in_closure": lists_n, "source_totals": src_totals,
                  "unresolved": unresolved,
                  "world_list": {"form_id": WORLD_LIST_ID, "edid": WORLD_LIST_EDID}},
        "set": SET_SLUG,
        "name": SET_NAME,
        "page_title": PAGE_TITLE,
        "blurb": (f"Every bobblehead spawn point in Fallout 76 — {world_total} placed "
                  f"points across {sum(i['count'] for i in index)} map markers, each with "
                  f"a {rate_display or 'chance'} chance of holding one. Pick a region for "
                  "its full spawn list, or read on for the boxes, vendors and events "
                  "that hand bobbleheads over."),
        "drop_rates": {
            "world_spawns": {
                "rate": r6(rate), "rate_display": rate_display,
                "rate_source": "computed" if rate is not None else None,
                "list_id": WORLD_LIST_ID, "list_edid": WORLD_LIST_EDID,
                "chance_none_glob": glob_edid, "chance_none_value": cn_value,
                "note": (f"Each placed point rolls the {WORLD_LIST_EDID} list, whose "
                         f"ChanceNone is driven by the {glob_edid} economy global "
                         f"(currently {cn_value}). Which bobblehead you get is a separate "
                         "even roll across the 21-item pool."),
            },
            "containers": container_node(boxes),
            "collectrons": None,
            "resource_generators": None,
            "creatures": None,
        },
        "vendor_list": vlist,
        "events_activities": events_activities,
        "item_breakdown": build_item_breakdown(alch_items, alch_display_names()),
        "fixed_spawn_index": {"base": URL_BASE, "regions": index},
        "regions": regions_out,
        "chance_spawns": chance_spawns,
    }
    json.dump(doc, open(OUT_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    for i in index:
        print(f"  {i['slug']:<18} {i['count']:>4} markers / {i['spawns']:>4} spawns  "
              f"-> {i['url']}")
    print(f"  {os.path.basename(OUT_FILE):<28} "
          f"containers: {len(boxes)} · vendors: {len(vlist)} · "
          f"events: {len(events_activities)} · used-for items: {len(doc['item_breakdown'])}")

    manifest = os.path.join(DIST, "bobblehead_spawns_manifest.json")
    json.dump({"_meta": {"generated": generated, "source": SOURCE_TAG},
               "set": SET_SLUG, "name": SET_NAME, "page_title": PAGE_TITLE,
               "url_base": URL_BASE, "file": "bobblehead_spawns/bobbleheads_spawns.json",
               "totals": {"spawns": world_total, "markers": sum(i["count"] for i in index),
                          "regions": len(ALL_REGIONS), "items": len(pickup_names),
                          "spawn_chance": rate_display},
               "regions": index, "unresolved": unresolved},
              open(manifest, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  {os.path.basename(manifest):<28} manifest")

    if unresolved:
        print(f"[bobbleheads] [warn] {sum(unresolved.values())} placement(s) with no region: "
              f"{dict(list(unresolved.items())[:8])}")

    # ── assertions (spawn-guide §9k: spawns[] must cover every placement) ─────
    bad = [(r["region"], l["marker"]) for r in regions_out for l in r["locations"]
           if len(l.get("spawns") or []) != l["count"]]
    if bad:
        raise AssertionError(f"spawns/count mismatch at {bad[:5]}")
    print(f"[bobbleheads] OK — {placements} placements across {total} markers, "
          f"every marker's spawns[] matches its count.")

    if db_ok:
        ebuild.save_cache(cache, GEO_CACHE)
        print(f"[bobbleheads] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
