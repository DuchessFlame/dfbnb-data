#!/usr/bin/env python3
r"""
spawns_configs.magazines — the Magazine family driver for the shared spawns engine.

PAGES (DF brand, under /df/farming/consumables/)

    /df/farming/consumables/magazines/location-guide/            <- HUB
        The full spawn-guide root-expand set EXCEPT the marker dump: Containers,
        Collectrons, Creatures, Events & Activities, Resource Generators, Vendors,
        and a Fixed Spawn Locations expand that renders the REGION INDEX (links out).
        NO "Used For / When Consumed" block — magazines are not eaten; each issue
        grants a timed perk when read, so there is no food-consumption breakdown.

    /df/farming/consumables/magazines/location-guide/<region>/   <- TEN REGION PAGES
        Fixed Spawn Locations ONLY, filtered to that region — every marker A-Z,
        one Map location / Directions / Item in place block PER SPAWN (spawn-guide 9k).

RENDERER: no new JS. Both page types are drawn by the existing
`df-bnb-farming-non-perishable-guide.js`, whose DF_GUIDES map already routes
`magazines` -> `magazine_spawns/magazines_spawns.json`. Consequently THIS BUILDER
MUST EMIT THE FARMING DOC SHAPE: `regions[]`, `fixed_spawn_index`, `drop_rates`,
`vendor_list`, `events_activities`.

SEEDS — the world list places the magazine BOOK records directly (a magazine has
no separate MISC-pickup / ALCH-potion split the way a bobblehead does). The world
spawn list `LPI_Loot_Magazines` (00322788) is entries of `Magazine_<Series>NN_Book:BOOK`.
Membership resolves by EDID regex against the committed BOOK export, never a
hardcoded FormID list. `zzz_Babylon_*` (cut Nuclear Winter build) and the
`recipe_mod_*_Magazine_*` weapon-mag recipes are excluded — they place nothing live.

SPAWN RATE — read from `LVLI_Math.ListChanceNoneResolved` for WORLD_LIST_ID at
build time, never typed, so it tracks any Bethesda economy retune automatically
(drop-rate-engine 3b). Each placed point currently has a 20% chance of holding a
magazine (LPI_Chance_Magazines_ECON = 80.0 ChanceNone), read not assumed.

Usage:
    python src/build_spawns.py magazines
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

# ── constants ────────────────────────────────────────────────────────────────
SET_SLUG = "magazines"
SET_NAME = "Magazines"
PAGE_TITLE = "Magazines Location Guide"
# Canonical consumables home; the renderer's parseDF expects the /location-guide/
# segment + bare region slugs (same as bobbleheads). NOTE: nav.json currently lists
# magazines at /df/farming/consumables/magazines/<region>-magazines/ (no
# location-guide) — that is a stale earlier layout and must be corrected to match
# this URL_BASE for the region children to route.
URL_BASE = "/df/farming/consumables/magazines/location-guide/"

# The world spawn-point list. Everything else (rate, entries, member items) is
# read from the exports — this is the one seed the page can't infer.
WORLD_LIST_ID = "00322788"
WORLD_LIST_EDID = "LPI_Loot_Magazines"

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

# Live reading-magazine issues only: Magazine_<Series>NN_Book. Excludes the
# recipe_mod_*_Magazine_* weapon-mag recipes (start recipe_/zzz) and zzz_Babylon_*
# cut Nuclear Winter issues (caught by DEV_EDID_RE too).
PICKUP_EDID_RE = re.compile(r"^Magazine_[A-Za-z]+\d+_Book$")
# Every BOOK the game calls a magazine — the numbered issues above PLUS the four
# holotape magazines. Used ONLY to widen the fixed-spawn dedication test (see
# spawns_engine.sources.get_sources dedication_seeds): LPI_Loot_Magazines hands out
# both, so measured against the 96 numbered issues alone it would read as a shared
# loot pool and every magazine spawn point would be demoted to a chance point.
CATEGORY_EDID_RE = re.compile(r"^Magazine_", re.I)

# QA / debug / cut-content holders inherit loot lists but aren't reachable in game.
DEV_EDID_RE = re.compile(r"(^qa|^test|_test|debug|zzz_|babylon|^recipe_)", re.I)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "magazine_spawns")
OUT_FILE = os.path.join(OUT_DIR, "magazines_spawns.json")
TSV = esources.TSV
GEO_CACHE = os.environ.get(
    "MAGAZINE_GEO_CACHE",
    os.path.join(REPO, "data", "magazine_spawns", "geo_cache.json"))
SOURCE_TAG = "Game-file exports (LVLI/BOOK) + Mappalachia Position (cached for CI)"

CONTAINER_TYPES = {"container", "loot-list"}


# ── file helpers ─────────────────────────────────────────────────────────────
def _newest(pattern, exclude=None):
    return tsv_source.newest(os.path.join(TSV, pattern), exclude=exclude, required=False)


# ── seeds ────────────────────────────────────────────────────────────────────
def load_dedication_seeds():
    """FormIDs of every magazine BOOK (numbered issues + holotape magazines), for
    the fixed-spawn dedication test only. Keyword-driven, no FormIDs typed."""
    out = set()
    book = _newest("BOOK_Export_*.tsv", exclude="_Locations")
    if not book:
        return out
    with open(book, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if CATEGORY_EDID_RE.match(edid) and not DEV_EDID_RE.search(edid):
                out.add((r.get("FormID") or "").strip().upper())
    return out


def load_item_records():
    """Every live magazine issue that seeds the closure: the BOOK records the
    leveled lists place. Returns (records, book_names) where book_names is
    {FormID -> FULL} for display."""
    recs, book_names = [], {}
    book = _newest("BOOK_Export_*.tsv", exclude="_Locations")
    if not book:
        raise FileNotFoundError(f"no BOOK_Export_*.tsv in {TSV}")
    with open(book, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if PICKUP_EDID_RE.match(edid) and not DEV_EDID_RE.search(edid):
                fid = (r.get("FormID") or "").strip().upper()
                full = (r.get("FULL") or "").strip()
                recs.append({"formid": fid, "sig": "BOOK", "edid": edid, "full": full})
                book_names[fid] = full
    return recs, book_names


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
    return None if v is None else round(float(v), 6)


# ── display names for leveled-list sources ───────────────────────────────────
DISPLAY_OVERRIDES = [
    (re.compile(r"box", re.I), None),  # handled by prettify fallback
]
_GLUE_RE = re.compile(r"\bMagazines?\b")


def prettify(edid):
    if not edid:
        return ""
    s = re.sub(r"^(?:(?:LL[SVEDIC]?|LPI|LLD|BURN|GHL|MILE|ATX|NWOT|RE)_)+", "", edid)
    s = re.sub(r"_+", " ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    return re.sub(r"\s+", " ", _GLUE_RE.sub("Magazines", s)).strip()


# ── hand-authored slot preservation ──────────────────────────────────────────
def load_keep():
    return ebuild.load_existing(OUT_FILE)


def attach_labels(regions_out):
    """Name each per-spawn block. Every magazine placement is the same
    LPI spawn point, so they number within their marker: `Magazine Spawn #1..N`.
    A marker holding a single spawn drops the `#1`. Vendor placements are skipped."""
    for reg in regions_out:
        for loc in reg["locations"]:
            spawns = [s for s in (loc.get("spawns") or [])
                      if s.get("source_type") != "vendor"]
            single = len(spawns) <= 1
            for i, sp in enumerate(spawns, 1):
                sp["label"] = "Magazine Spawn" if single else f"Magazine Spawn #{i}"


def attach_breakdowns(regions_out, rate_display):
    for reg in regions_out:
        for loc in reg["locations"]:
            n = sum(v for k, v in (loc.get("sources") or {}).items() if k != "vendor")
            if not n:
                continue
            loc["breakdown"] = [{
                "label": "magazine spawn point" + ("" if n == 1 else "s"),
                "count": n,
                "rate_key": "world_spawns",
                "rate_display": rate_display,
            }]


# ── non-location sources ─────────────────────────────────────────────────────
def build_sources(src, tbls, appearance_fn, targets):
    parent_edid = tbls["parent_edid"]
    edid_to_fid = {}
    for fid, edid in parent_edid.items():
        if edid:
            edid_to_fid.setdefault(edid, fid)

    def rate_of(list_ref):
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
        if not re.search(r"(box|scavenger|cache|loot_?bag|magazinerack)", edid, re.I):
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


def container_types_node(boxes):
    """drop_rates.containers.types — the container-TYPE + rate model: one row per
    lootable container type that can hold a magazine, with the rng76 drop rate.
    The renderer draws "open a {type} -> X% chance", never a location dump."""
    if not boxes:
        return None
    types = [{"name": b["name"], "rate": r6(b["rate"]), "rate_display": b["rate_display"] or ""}
             for b in boxes]
    return {"types": types}


def vendor_rows(vendors, seen, cache):
    placed = {}
    for inst, (x, y, region, marker, stype) in seen.items():
        if stype == "vendor":
            base = (cache.get(str(inst)) or {}).get("base")
            placed.setdefault(base, []).append((region, marker))
    rows = []
    for v in vendors:
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


# ── entry point ──────────────────────────────────────────────────────────────
def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()

    item_records, book_names = load_item_records()
    targets = {r["formid"].upper() for r in item_records}
    print(f"[magazines] {len(item_records)} seed BOOK records "
          f"(live Magazine_*_Book issues)")

    rate, glob_edid, cn_value = world_spawn_rate()
    rate_display = pct(rate)
    print(f"[magazines] world spawn chance {rate_display or 'unresolved'} "
          f"(GLOB {glob_edid or 'n/a'} = {cn_value} ChanceNone)")

    tbls = esources.load_tables()
    src = esources.get_sources(item_records, tbls, nuka_classify,
                               dedication_seeds=load_dedication_seeds() | set(targets))
    print(f"[magazines] closure {len(src['lvli_closure'])} lists · "
          f"{len(src['direct_refrs'])} direct placements · "
          f"{len(src['placed_bases'])} holder bases")

    appearance_fn = None
    try:
        import rng76
        _res = rng76.Rng76Data.from_tsv_root(TSV).resolver
        appearance_fn = lambda lid, t: _res.appearance_prob(lid, t)
        print("[magazines] rng76 loaded — container / vendor / event rates computed.")
    except Exception as e:
        print(f"[magazines] [warn] rng76 unavailable ({e}); source rates blank.")

    db_ok = os.path.exists(MAPPALACHIA_DB)
    cache = ebuild.load_cache(GEO_CACHE)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[magazines] Mappalachia DB found — resolving placements and refreshing geo cache.")
    elif cache:
        print(f"[magazines] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[magazines] No Mappalachia DB and no geo cache — fixed spawns will be "
              "EMPTY. Run once locally with MAPPALACHIA_DB set to seed "
              "data/magazine_spawns/geo_cache.json (the non-geo sections still build).")

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
        "blurb": (f"Every magazine spawn point in Fallout 76 — {world_total} placed "
                  f"points across {sum(i['count'] for i in index)} map markers. Pick a "
                  "region for its full spawn list, or read on for the boxes, vendors and "
                  "events that hand magazines over. Each issue grants a timed perk when "
                  "read (no food-consumption breakdown)."),
        "drop_rates": {
            "world_spawns": {
                "rate": r6(rate), "rate_display": rate_display,
                "rate_source": "computed" if rate is not None else None,
                "list_id": WORLD_LIST_ID, "list_edid": WORLD_LIST_EDID,
                "chance_none_glob": glob_edid, "chance_none_value": cn_value,
                "note": (f"Each placed point rolls the {WORLD_LIST_EDID} list. Which "
                         "magazine issue you get is an even roll across the live pool."),
            },
            "containers": container_types_node(boxes),
            "collectrons": None,
            "resource_generators": None,
            "creatures": None,
        },
        "vendor_list": vlist,
        "events_activities": events_activities,
        # NO item_breakdown / used_for — magazines grant perks, not a When-Consumed block.
        "item_breakdown": [],
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
          f"events: {len(events_activities)} · issues: {len(book_names)}")

    manifest = os.path.join(DIST, "magazine_spawns_manifest.json")
    json.dump({"_meta": {"generated": generated, "source": SOURCE_TAG},
               "set": SET_SLUG, "name": SET_NAME, "page_title": PAGE_TITLE,
               "url_base": URL_BASE, "file": "magazine_spawns/magazines_spawns.json",
               "totals": {"spawns": world_total, "markers": sum(i["count"] for i in index),
                          "regions": len(ALL_REGIONS), "items": len(book_names),
                          "spawn_chance": rate_display},
               "regions": index, "unresolved": unresolved},
              open(manifest, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  {os.path.basename(manifest):<28} manifest")

    if unresolved:
        print(f"[magazines] [warn] {sum(unresolved.values())} placement(s) with no region: "
              f"{dict(list(unresolved.items())[:8])}")

    bad = [(r["region"], l["marker"]) for r in regions_out for l in r["locations"]
           if len(l.get("spawns") or []) != l["count"]]
    if bad:
        raise AssertionError(f"spawns/count mismatch at {bad[:5]}")
    print(f"[magazines] OK — {placements} placements across {total} markers, "
          f"every marker's spawns[] matches its count.")

    if db_ok:
        ebuild.save_cache(cache, GEO_CACHE)
        print(f"[magazines] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
