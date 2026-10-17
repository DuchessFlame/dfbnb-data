#!/usr/bin/env python3
r"""
spawns_configs.robot_models — Robot Models (DF Collectables): one driver, two pages.

PAGES (DF brand)
    /df/collectables/robot-models/spawn-locations/   <- spawn guide (farming renderer)
    /df/collectables/robot-models/checklist/         <- checklist (df-bnb-robot-models.js)

OUTPUT
    dist/robot_model_spawns/robot_models_spawns.json   farming doc shape (spawn-guide 9k):
        regions[] (Fixed Spawn Locations, one slot block per spawn), chance_spawns,
        drop_rates.containers, vendor_list, events_activities, used_for.challenges.
    dist/collectables_robot_models.json                the checklist rows: one per
        robot model, with its How to Obtain routes (world spawns, containers,
        events, challenges) — every number read from the exports, nothing typed.

SEEDS — every MISC whose EDID is BotModel<Name> (no CharGen_ / Chess_ copies). Names
come from the MISC FULL. The three Robot Parts Models share one FULL in game, so
their EDID number is added to tell them apart: BotModelParts01 -> "Robot Parts Model
A (01)" (01 -> A, 02 -> B, 03 -> C), the same names the old Wix guide used.

Hand-written content (directions + photos per spawn) lives in
data/robot_model_spawns/spawn_notes.json, keyed by the placement REF FormID, so a
rebuild can never drop it.

Usage:
    python src/build_spawns.py robot-models
"""

import os, re, csv, sys, json, sqlite3, datetime
from collections import defaultdict

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
import tsv_source

SET_SLUG = "robot-models"
SET_NAME = "Robot Models"
PAGE_TITLE = "Collectables - Robot Models Spawn Locations"
SPAWN_URL = "/df/collectables/robot-models/spawn-locations/"
CHECKLIST_URL = "/df/collectables/robot-models/checklist/"

ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]

PICKUP_EDID_RE = re.compile(r"^BotModel[A-Za-z]+\d*$")
PARTS_RE = re.compile(r"^BotModelParts(\d\d)$")
DEV_EDID_RE = re.compile(r"(^qa|^test|_test|debug|zzz_|babylon|^cut_)", re.I)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "robot_model_spawns")
OUT_FILE = os.path.join(OUT_DIR, "robot_models_spawns.json")
CHECKLIST_FILE = os.path.join(DIST, "collectables_robot_models.json")
TSV = esources.TSV
DATA_DIR = os.path.join(REPO, "data", "robot_model_spawns")
GEO_CACHE = os.environ.get("ROBOT_MODEL_GEO_CACHE", os.path.join(DATA_DIR, "geo_cache.json"))
NOTES_FILE = os.path.join(DATA_DIR, "spawn_notes.json")
# Item pictures (checklist thumbs + Item Image). Server paths; the page shows a
# placeholder until the file is uploaded.
IMAGE_BASE = "/wp-content/uploads/guide-images/collectables/robot-models/"
MAP_BASE = IMAGE_BASE
APPALACHIA_SPACE = 2480661   # the open-world worldspace; only its spawns get a region tile
SOURCE_TAG = "Game-file exports (MISC/LVLI/CHAL) + Mappalachia Position (cached for CI)"


def _newest(pattern, exclude=None):
    return tsv_source.newest(os.path.join(TSV, pattern), exclude=exclude, required=False)


def pct(v, places=4):
    import rate_format
    return rate_format.fmt_rate(v, places)


def r6(v):
    return None if v is None else round(float(v), 6)


def slugify(s):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", str(s).lower())).strip("-")


def display_name(edid, full):
    m = PARTS_RE.match(edid)
    if m:
        n = int(m.group(1))
        letter = chr(ord("A") + n - 1) if 1 <= n <= 26 else ""
        return f"{full} {letter} ({m.group(1)})".replace("  ", " ")
    return full


def load_item_records():
    """Every live BotModel MISC: [{formid, sig, edid, full, name, keywords, value, weight}]."""
    misc = _newest("MISC_Export_*.tsv")
    if not misc:
        raise FileNotFoundError(f"no MISC_Export_*.tsv in {TSV}")
    recs = []
    with open(misc, encoding="utf-8", errors="replace") as f:
        rd = csv.reader(f, delimiter="\t")
        head = next(rd)
        ix = {h: i for i, h in enumerate(head)}
        for row in rd:
            g = lambda k: (row[ix[k]] if k in ix and ix[k] < len(row) else "").strip()
            edid = g("EDID")
            if not PICKUP_EDID_RE.match(edid) or DEV_EDID_RE.search(edid):
                continue
            full = g("FULL")
            if not full:
                continue
            refs = [row[i] for i in range(ix.get("Ref1", len(row)), len(row)) if row[i]]
            recs.append({"formid": g("FormID").upper(), "sig": "MISC", "edid": edid,
                         "full": full, "name": display_name(edid, full),
                         "keywords": g("Keywords"), "value": g("Value"), "weight": g("Weight"),
                         "refs": refs})
    recs.sort(key=lambda r: r["name"].lower())
    return recs, os.path.basename(misc)


def load_notes():
    try:
        return json.load(open(NOTES_FILE, encoding="utf-8"))
    except Exception:
        return {}


def prettify(edid):
    if not edid:
        return ""
    s = re.sub(r"^(?:(?:LL[SVEDIC]?|LLI|LPI|LLD|SCORE|ATX|NWOT|RE)_)+", "", edid)
    s = re.sub(r"_+", " ", s)
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).strip()


def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()
    items, misc_file = load_item_records()
    by_fid = {r["formid"]: r for r in items}
    targets = set(by_fid)
    print(f"[robot-models] {len(items)} robot models from {misc_file}: "
          + ", ".join(r["name"] for r in items))

    tbls = esources.load_tables()
    seeds = [{"formid": r["formid"], "sig": "MISC", "edid": r["edid"]} for r in items]
    src = esources.get_sources(seeds, tbls, nuka_classify, place_item_bases=True,
                               dedication_seeds=set(targets))
    print(f"[robot-models] closure {len(src['lvli_closure'])} lists · "
          f"{len(src['direct_refrs'])} direct placements · {len(src['placed_bases'])} holder bases")

    appearance_fn = None
    try:
        import rng76
        _res = rng76.Rng76Data.from_tsv_root(TSV).resolver
        appearance_fn = lambda lid, t: _res.appearance_prob(lid, t)
        print("[robot-models] rng76 loaded.")
    except Exception as e:
        print(f"[robot-models] [warn] rng76 unavailable ({e}); source rates blank.")

    db_ok = os.path.exists(MAPPALACHIA_DB)
    cache = ebuild.load_cache(GEO_CACHE)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB)
        cur = con.cursor()
        print("[robot-models] Mappalachia DB found — resolving placements.")
    elif cache:
        print(f"[robot-models] No DB — rebuilding from committed geo cache ({len(cache)}).")
    else:
        print("[robot-models] [warn] no Mappalachia DB and no geo cache — fixed spawns EMPTY.")

    keep = ebuild.load_existing(OUT_FILE)
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)

    # Which robot model stands at each placement (per-spawn label + per-item counts).
    def base_of(inst):
        e = cache.get(str(inst)) or {}
        b = e.get("base") if e.get("base") is not None else e.get("ref")
        try:
            return f"{int(b):08X}" if isinstance(b, int) else str(b or "").upper().zfill(8)
        except Exception:
            return ""

    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)
    chance_spawns = ebuild.group_chance(seen, ALL_REGIONS)
    chance_spawns["map_base"] = MAP_BASE

    notes = load_notes()
    per_item_spawns = defaultdict(int)
    per_item_markers = defaultdict(set)
    notes_used = 0
    for reg in regions_out:
        for loc in reg["locations"]:
            counts = defaultdict(int)
            names = []
            for sp in loc["spawns"]:
                fid = base_of(int(sp["ref"], 16))
                nm = by_fid.get(fid, {}).get("name") or "Robot Model"
                names.append(nm)
                counts[nm] += 1
                if fid in by_fid:
                    per_item_spawns[fid] += 1
                    per_item_markers[fid].add((reg["region"], loc["marker"]))
            seenc = defaultdict(int)
            for sp, nm in zip(loc["spawns"], names):
                seenc[nm] += 1
                sp["label"] = nm if counts[nm] == 1 else f"{nm} #{seenc[nm]}"
                n = notes.get("spawns", {}).get(sp["ref"]) or {}
                for k in ("image_top", "directions", "image_bottom"):
                    if n.get(k):
                        sp[k] = n[k]
                if n:
                    notes_used += 1
            mk = notes.get("markers", {}).get(f"{reg['region']}|{loc['marker']}") or {}
            for k in ("directions", "note", "image_top", "image_bottom"):
                if mk.get(k):
                    loc[k] = mk[k]
            loc["breakdown"] = [{"label": nm, "count": c, "rate_key": "world_spawns",
                                 "rate_display": "100%"}
                                for nm, c in sorted(counts.items())]

    # Containers / vendors / events
    parent_edid = tbls["parent_edid"]
    edid_to_fid = {}
    for f, e in parent_edid.items():
        if e:
            edid_to_fid.setdefault(e, f)

    def rate_of(list_ref, tset):
        if not appearance_fn or not list_ref:
            return None
        lid = list_ref if re.fullmatch(r"[0-9A-Fa-f]{8}", str(list_ref)) else edid_to_fid.get(list_ref)
        if not lid:
            return None
        try:
            p = appearance_fn(lid, tset)
            return r6(p) if p and p > 0 else None
        except Exception:
            return None

    try:
        import build_farming_used_for as B
        _cont_names = B._load_cont_names(TSV)
        _live = B.live_cont_bases(TSV)

        def container_types(closure, tset, _tsv, app):
            return {"types": B.container_types(closure, tset, app, _cont_names,
                                               tbls["lvli_refs"], parent_edid,
                                               placed_bases=_live)}
    except Exception as e:
        print(f"[robot-models] [warn] container helper unavailable ({e})")
        container_types = None
    cont_types = None
    if container_types is not None:
        try:
            cont_types = container_types(src["lvli_closure"], targets, TSV, appearance_fn)
        except Exception as e:
            print(f"[robot-models] [warn] container_types failed ({e}); falling back.")
            cont_types = None
    boxes = []
    if cont_types is None:
        names_seen = set()
        for lv in sorted(src["lvli_closure"]):
            edid = parent_edid.get(lv) or ""
            if not edid or DEV_EDID_RE.search(edid):
                continue
            if not re.search(r"(box|scavenger|cache|loot_?bag|lunchbox)", edid, re.I):
                continue
            nm = prettify(edid)
            if nm in names_seen:
                continue
            names_seen.add(nm)
            r = rate_of(lv, targets)
            if r:
                boxes.append({"name": nm, "list_id": lv, "edid": edid, "rate": r,
                              "rate_display": pct(r)})
        boxes.sort(key=lambda b: (-(b["rate"] or 0), b["name"].lower()))
        cont_types = {"types": [{"name": b["name"], "rate": b["rate"],
                                 "rate_display": b["rate_display"]} for b in boxes]}

    vendors = []
    for f, meta in sorted(src["placed_bases"].items()):
        if meta.get("source_type") != "vendor":
            continue
        via = meta.get("via") or ""
        if DEV_EDID_RE.search(meta.get("edid") or "") or DEV_EDID_RE.search(via):
            continue
        r = rate_of(via, targets)
        vendors.append({"name": prettify(meta.get("edid") or via), "marker": "", "region": "",
                        "vendor_type": "Vendor", "rate_lines": [pct(r)] if r else [],
                        "rate_display": pct(r) or "", "rate_value": r or 0, "count": 1,
                        "stock_list": via})

    events_activities = eevents.detect(src["lvli_closure"], parent_edid, c2p=tbls["c2p"])
    eevents.resolve_event_rates(events_activities, targets, appearance_fn)
    events_activities = [e for e in events_activities if e.get("rate_value") or e.get("rate_display")]

    # Challenges (CHAL joined through CNDF / EDID stem — collectable_challenges).
    challenges = None
    try:
        import collectable_challenges as cchal
        chal_rows, cndf, _sets = cchal.context()
        challenges = cchal.build_for_items(list(targets), [r["edid"] for r in items], chal_rows, cndf)
    except Exception as e:
        print(f"[robot-models] [warn] challenges join failed ({e})")
    chal_items = (challenges or {}).get("items") or []
    # Per-item challenge list: a challenge counts an item when its CNDF names it.
    per_item_chal = defaultdict(list)
    if chal_items:
        try:
            import collectable_challenges as cchal
            for r in items:
                b = cchal.build_for_items([r["formid"]], [r["edid"]], chal_rows, cndf)
                for c in (b or {}).get("items") or []:
                    per_item_chal[r["formid"]].append(c)
        except Exception:
            pass

    used_for_challenges = []
    for c in chal_items:
        scope = c.get("scope") or ""
        used_for_challenges.append({"name": c.get("name"), "edid": c.get("edid"),
                                    "type": scope.title() if scope else "Challenge",
                                    "required": c.get("required"),
                                    "counts": [by_fid[f]["name"] for f in by_fid
                                               if any(x.get("edid") == c.get("edid")
                                                      for x in per_item_chal[f])]})

    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG, "misc_export": misc_file,
                  "lists_in_closure": lists_n, "source_totals": src_totals,
                  "unresolved": unresolved},
        "set": SET_SLUG,
        "name": SET_NAME,
        "page_title": PAGE_TITLE,
        "blurb": ("Every place a robot model sits in the world, sorted by region. "
                  "Each one is a fixed spawn, so if it's missing, someone on your "
                  "server has already taken it or it's still in your pick-up history. "
                  "Keep grenades and explosives away from them - the models get knocked "
                  "flying."),
        "items": [{"form_id": r["formid"], "edid": r["edid"], "name": r["name"],
                   "spawns": per_item_spawns.get(r["formid"], 0)} for r in items],
        "drop_rates": {
            "world_spawns": {"rate": 1.0, "rate_display": "100%", "rate_source": "computed",
                             "note": "Every fixed spawn is the robot model itself, placed in the world."},
            "containers": cont_types if (cont_types and cont_types.get("types")) else None,
            "collectrons": None,
            "resource_generators": None,
            "creatures": None,
        },
        # recipes: None -> the renderer leaves the Recipes & Cooking block off
        # (nothing cooks with a robot model).
        "used_for": {"challenges": used_for_challenges, "recipes": None},
        "info_notes": [{
            "title": f"Which robot models count for the {c['name'].split(' - ')[0]} challenge",
            "body": ("These count: " + ", ".join(c["counts"]) + ". "
                     + ("These don't: " + ", ".join(sorted(set(r["name"] for r in items) - set(c["counts"])))
                        + "." if set(r["name"] for r in items) - set(c["counts"]) else "")),
        } for c in used_for_challenges if c.get("counts")],
        "vendor_list": vendors,
        "events_activities": events_activities,
        "item_breakdown": [],
        "map_base": MAP_BASE,
        # Full 4K render (render_spawn_maps.py -> 01 Full Maps (4096)/robot-models.jpg)
        # and the regions that have a tile in 03 Region Tiles. The renderer only shows
        # a map link once a HEAD check finds the file on the server.
        "full_map": MAP_BASE + "robot-models.jpg",
        "map_regions": [r["region"] for r in regions_out
                        if any((cache.get(str(int(sp["ref"], 16))) or {}).get("space") == APPALACHIA_SPACE
                               for l in r["locations"] for sp in l["spawns"])],
        "map_ext": ".jpg",
        "regions": regions_out,
        "chance_spawns": chance_spawns,
    }
    json.dump(doc, open(OUT_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    bad = [(r["region"], l["marker"]) for r in regions_out for l in r["locations"]
           if len(l.get("spawns") or []) != l["count"]]
    if bad:
        raise AssertionError(f"spawns/count mismatch at {bad[:5]}")

    # ── checklist rows ──────────────────────────────────────────────────────
    cont_rows_by_item = {}
    for r in items:
        rows = []
        tset = {r["formid"]}
        if container_types is not None:
            try:
                ct = container_types(src["lvli_closure"], tset, TSV, appearance_fn) or {}
                rows = [{"name": t["name"], "rate_display": t.get("rate_display", "")}
                        for t in ct.get("types", [])]
            except Exception:
                rows = []
        cont_rows_by_item[r["formid"]] = rows

    ev_by_item = {}
    for r in items:
        evs = eevents.detect(src["lvli_closure"], parent_edid, c2p=tbls["c2p"])
        eevents.resolve_event_rates(evs, {r["formid"]}, appearance_fn)
        ev_by_item[r["formid"]] = [{"name": e.get("name"), "type": e.get("type"),
                                    "rate_display": e.get("rate_display", "")}
                                   for e in evs if e.get("rate_value")]

    # Drop conditions on the leveled-list path to each item (LVLI entry Cond1..N):
    # every entry, inside the item's closure, that points at the item or at another
    # closure list. Empty -> "None" on the page (the files were read, nothing gates it).
    closure = {str(x).upper() for x in src["lvli_closure"]}
    conds_by_item = defaultdict(set)
    entries = _newest("LVLI_Export_*_LVLI_Entries.tsv")
    if entries:
        with open(entries, encoding="utf-8", errors="replace") as f:
            rd = csv.reader(f, delimiter="\t")
            head = next(rd)
            ix = {h: i for i, h in enumerate(head)}
            cidx = [ix[h] for h in head if re.fullmatch(r"Cond\d+", h)]
            for row in rd:
                lid = (row[ix["LVLI_FormID"]] if len(row) > ix["LVLI_FormID"] else "").upper()
                if lid not in closure:
                    continue
                ref = (row[ix["LVLO_Reference"]] if len(row) > ix["LVLO_Reference"] else "")[:8].upper()
                cs = [row[i].strip() for i in cidx if i < len(row) and row[i].strip()]
                if not cs:
                    continue
                if ref in by_fid:
                    conds_by_item[ref].update(cs)
                elif ref in closure:
                    for f2 in by_fid:
                        conds_by_item[f2].update(cs)

    def tradeable(kw):
        return not re.search(r"untradeable|notrade|no_trade|questitem", kw or "", re.I)

    rows = []
    for r in items:
        fid = r["formid"]
        markers = sorted(per_item_markers.get(fid, set()))
        rows.append({
            "id": fid,
            "name": r["name"],
            "full": r["full"],
            "edid": r["edid"],
            "form_id": fid,
            "keywords": r["keywords"],
            "value": r["value"],
            "weight": r["weight"],
            "tradeable": tradeable(r["keywords"]),
            "image": IMAGE_BASE + slugify(r["name"]) + ".avif",
            "source": "World Spawn" if per_item_spawns.get(fid) else (
                "Containers" if cont_rows_by_item.get(fid) else "Other"),
            "obtain": {
                "world_spawns": {"spawns": per_item_spawns.get(fid, 0),
                                 "locations": len(markers),
                                 "places": [{"region": a, "marker": b} for a, b in markers]},
                "containers": cont_rows_by_item.get(fid, []),
                "events": ev_by_item.get(fid, []),
                "drop_conditions": sorted(conds_by_item.get(fid, set())),
                "conditions_checked": bool(entries),
                "challenges": [{"name": c.get("name"), "type": (c.get("scope") or "").title(),
                                "required": c.get("required")} for c in per_item_chal.get(fid, [])],
            },
            "cut": False,
        })
    checklist = {"_meta": {"generated": generated, "source": SOURCE_TAG,
                           "spawn_guide": SPAWN_URL},
                 "set": SET_SLUG, "name": SET_NAME, "count": len(rows),
                 "challenges": used_for_challenges, "items": rows}
    json.dump(checklist, open(CHECKLIST_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"[robot-models] fixed: {placements} spawns at {total} markers · "
          f"chance: {chance_spawns.get('total', 0)} · containers: "
          f"{len((cont_types or {}).get('types', []))} · vendors: {len(vendors)} · "
          f"events: {len(events_activities)} · challenges: {len(chal_items)} · notes applied: {notes_used}")
    for r in items:
        print(f"    {r['name']:<28} {per_item_spawns.get(r['formid'], 0):>3} spawns")
    if unresolved:
        print(f"[robot-models] [warn] unresolved placements: {unresolved}")
    if db_ok:
        ebuild.save_cache(cache, GEO_CACHE)
        print(f"[robot-models] geo cache saved ({len(cache)}).")
    if con:
        con.close()
    return OUT_FILE


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv[1:])
