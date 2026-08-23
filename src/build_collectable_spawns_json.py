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

import os, re, csv, json, datetime
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
        # Full-region overview map offered as a download in the intro card. Same
        # uploads folder as the per-marker mask images.
        "full_map": "/wp-content/uploads/guide-images/collectables/Slasher-Mask-Locations/slasher_masks.jpg",
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
    # Pint-Sized Phantoms grave sites — a TSV-sourced set (see GRAVE_* below). It
    # renders through the SAME df-bnb-collectables-spawns.js engine + collect
    # tracking as the mask pages, but lives under Treasure Maps, so its source is
    # tsv/phantom_grave_sites.tsv (regions pre-resolved) rather than resolve_dataset.
    "pint-sized-phantom-graves": {
        "name": "Pint-Sized Phantoms' Grave Sites",
        "page_title": "Pint-Sized Phantoms' Grave Site Locations",
        "blurb": "Every grave site for the Pint-Sized Phantoms' treasure map, grouped by region.",
    },
    # Two more TSV-sourced dig-site sets that render through the SAME
    # df-bnb-collectables-spawns.js engine + collect tracking, but live under
    # Treasure Maps. Regions are pre-resolved in their committed TSVs (see
    # DIG_SETS / build_dig_set), so CI needs no Mappalachia DB.
    "treasure-maps-locations": {
        "name": "Treasure Map Locations",
        "page_title": "Treasure Map Dig Locations",
        "blurb": "Every treasure-map dig site in Appalachia, grouped by region.",
    },
    "u-mine-it": {
        "name": "U Mine It Dig Locations",
        "page_title": "U Mine It (Lucky Strike) Dig Locations",
        "blurb": "Every U-Mine-It / Lucky Strike dig site in Appalachia, grouped by region.",
    },
}

# ── Pint-Sized Phantoms grave sites (TSV-sourced collectables-spawns set) ──────
# Two inputs, deliberately separated, because they change for different reasons and
# on different clocks:
#
#   tsv/phantom_grave_sites.tsv   PLACEMENTS — machine-generated from the newest REFR
#                                 export by build_phantom_grave_sites_tsv.py. Bethesda
#                                 churns ref FormIDs and markers; this file follows them.
#   tsv/phantom_grave_notes.tsv   EDITORIAL — hand-authored directions + the three photo
#                                 slots, keyed on the grave's identity, NOT on its ref.
#
# The two are joined here by grave key. That key is the thing the player is told to
# find and is the only stable identity a grave has:
#
#     "7"                             -> numbered grave 7, wherever Bethesda moves it
#     "@Philippi Battlefield Cemetery" -> an unnumbered grave, keyed on its marker
#
# This matters. Before the split, the editorial columns lived inside the generated
# file and were carried across rebuilds keyed on ref_formid. When graves 05/07/09 were
# re-placed under new refs (00930AA0/AA7/AAE, EDIDs SDOW__GraveNN) the join key changed,
# the merge missed, and the directions + three photo paths were silently deleted by a
# routine rebuild. A placement rebuild can no longer touch editorial content at all,
# because it no longer writes it.
GRAVE_SLUG = "pint-sized-phantom-graves"
GRAVE_BASE = "008F1672"          # SDOW_MQ02_Graves_GraveActivator01 ("Disturbed Grave")

# Read the placements for THIS channel — tsv/pts/ on a PTS build, tsv/ on live —
# falling back to live when PTS has none. That fallback is safe in this direction
# only: a PTS page showing live data is merely behind, while a live page showing
# PTS data is wrong, which is how the grave page shipped graves that weren't there.
import tsv_source as _ts
GRAVE_CHANNEL = _ts.channel_of()
GRAVE_TSV = _ts.derived_read("phantom_grave_sites.tsv", GRAVE_CHANNEL)
GRAVE_NOTES_TSV = os.path.join(REPO, "tsv", "phantom_grave_notes.tsv")
# The grave TSV uses "The Forest"; the ten canonical regions call it "Forest".
GRAVE_REGION_ALIASES = {"the forest": "Forest"}


def grave_key(site_number, marker):
    """Stable editorial key for a grave: its number, else '@<marker>'."""
    sn = (site_number or "").strip()
    if sn:
        return sn
    return "@" + (marker or "").strip()


def load_grave_notes():
    """tsv/phantom_grave_notes.tsv -> {grave_key: {directions, photo_*}}.

    Hand-authored and never written by any build script. Missing file is not an
    error — the page simply renders without directions or photos.
    """
    notes = {}
    if not os.path.exists(GRAVE_NOTES_TSV):
        return notes
    with open(GRAVE_NOTES_TSV, encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        idx = {h.strip(): i for i, h in enumerate(header)}
        for line in f:
            if not line.strip():
                continue
            cols = line.rstrip("\n").split("\t")

            def cell(name):
                i = idx.get(name)
                return cols[i].strip() if i is not None and i < len(cols) else ""

            key = cell("grave")
            if not key:
                continue
            notes[key] = {c: cell(c) for c in
                          ("directions", "photo_region", "photo_approach", "photo_spawn")}
    return notes

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


def load_existing_top(path):
    """Return hand-added top-level fields (blurb_quote, full_map) from an existing
    dist JSON so a rebuild never drops values that aren't derived from SET_META."""
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}
    return {k: data[k] for k in ("blurb_quote", "full_map") if data.get(k)}


def build_set(slug, rows):
    """Build one set's JSON dict from its resolved rows."""
    meta = meta_for(slug)
    out_path = os.path.join(OUT_DIR, f"collectable_spawns_{slug}.json")
    handfills = load_existing_handfills(out_path)
    existing_top = load_existing_top(out_path)
    # SET_META is canonical; fall back to any hand-added value already in dist.
    full_map = meta.get("full_map") or existing_top.get("full_map", "")
    blurb_quote = meta.get("blurb_quote") or existing_top.get("blurb_quote", "")

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
        "blurb_quote": blurb_quote,
        "full_map": full_map,
        "total": total,
        "regions": regions_out,
        "unplaced": [{"marker": m, "count": by_region[""][m]["count"]} for m in orphan_markers],
    }, out_path


def load_grave_spawn_handfills(path):
    """{(region, marker, label): {image_region, image_top, directions, image_bottom}} for
    non-empty hand-authored spawn values in an existing grave JSON, so a rebuild never
    loses them. image_region is grave-only — the third photo slot (see GRAVE_SLOTS)."""
    keep = {}
    try:
        data = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in data.get("regions", []):
        rname = reg.get("region", "")
        for loc in reg.get("locations", []):
            marker = loc.get("marker", "")
            for sp in loc.get("spawns", []) or []:
                label = sp.get("label", "")
                saved = {k: sp.get(k, "") for k in
                         ("image_region", "image_top", "directions", "image_bottom")
                         if sp.get(k)}
                if saved:
                    keep[(rname, marker, label)] = saved
    return keep


def _read_grave_rows():
    """Parse tsv/phantom_grave_sites.tsv into a list of row dicts."""
    rows = []
    with open(GRAVE_TSV, encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        idx = {h.strip(): i for i, h in enumerate(header)}

        def cell(cols, name):
            i = idx.get(name)
            return cols[i].strip() if i is not None and i < len(cols) else ""

        for line in f:
            if not line.strip():
                continue
            cols = line.rstrip("\n").split("\t")
            rows.append({
                "region": cell(cols, "region"),
                "site_number": cell(cols, "site_number"),
                "ref_formid": cell(cols, "ref_formid"),
                "marker": cell(cols, "closest_fast_travel"),
                # Provenance: which export (or manual verification) this row came from.
                "source_export": cell(cols, "source_export"),
                "x": cell(cols, "x"), "y": cell(cols, "y"),
            })
    return rows


def build_grave_set():
    """Reshape the grave-sites TSV into a collectable_spawns_<slug>.json dict + path."""
    meta = meta_for(GRAVE_SLUG)
    out_path = os.path.join(OUT_DIR, f"collectable_spawns_{GRAVE_SLUG}.json")
    notes = load_grave_notes()
    handfills = load_grave_spawn_handfills(out_path)
    existing_top = load_existing_top(out_path)
    full_map = meta.get("full_map") or existing_top.get("full_map", "")
    blurb_quote = meta.get("blurb_quote") or existing_top.get("blurb_quote", "")

    def region_norm(r):
        return GRAVE_REGION_ALIASES.get((r or "").strip().lower(), (r or "").strip())

    def site_sort(sn):
        try:
            return (0, int(sn))
        except (TypeError, ValueError):
            return (1, 0)          # unnumbered graves sort last within a marker

    # region -> marker -> list of grave rows
    by_region = defaultdict(lambda: defaultdict(list))
    for r in _read_grave_rows():
        region = region_norm(r["region"])
        marker = r["marker"] or "(unknown location)"
        by_region[region][marker].append(r)

    regions_out, total = [], 0
    for region in REGIONS_AZ:
        locs = []
        for marker in sorted(by_region.get(region, {}), key=lambda m: marker_sort_key(region, m)):
            graves = sorted(by_region[region][marker], key=lambda r: site_sort(r["site_number"]))
            spawns = []
            for g in graves:
                label = f"Grave Site #{g['site_number']}" if g["site_number"] else "Grave Site"
                # Editorial content comes from phantom_grave_notes.tsv, keyed on the
                # grave's identity. handfills (keyed region+marker+label, read back out
                # of the previously published JSON) is only a legacy safety net for
                # anything not yet migrated into the notes file — it is never written.
                nt = notes.get(grave_key(g["site_number"], marker), {})
                hf = handfills.get((region, marker, label), {})
                spawns.append({
                    "label": label,
                    # image_region is the grave page's extra first slot (Region map).
                    "image_region": nt.get("photo_region") or hf.get("image_region", ""),
                    "image_top": nt.get("photo_approach") or hf.get("image_top", ""),
                    "directions": nt.get("directions") or hf.get("directions", ""),
                    "image_bottom": nt.get("photo_spawn") or hf.get("image_bottom", ""),
                    "refs": [g["ref_formid"]] if g["ref_formid"] else [],
                })
            total += len(spawns)
            locs.append({"marker": marker, "count": len(spawns), "spawns": spawns})
        regions_out.append({"region": region, "locations": locs})

    # Provenance, not build time. "generated" is when this file was written and says
    # nothing about whether the data is current; "observed" is the placement export the
    # rows actually came from. Where rows disagree, the OLDEST wins — a set is only as
    # fresh as its stalest row.
    sources = sorted({r["source_export"] for r in _read_grave_rows() if r["source_export"]})
    data = {
        "_meta": {
            "generated": datetime.date.today().isoformat(),
            "observed": sources[0] if sources else "unknown",
            "observed_all": sources,
            "source": "tsv/phantom_grave_sites.tsv (placements) + tsv/phantom_grave_notes.tsv "
                      "(directions/photos, keyed on grave identity) — reshaped for the "
                      "collectables-spawns engine",
        },
        "set": GRAVE_SLUG,
        "name": meta["name"],
        "page_title": meta["page_title"],
        "blurb": meta["blurb"],
        "blurb_quote": blurb_quote,
        "full_map": full_map,
        "total": total,
        "regions": regions_out,
        "unplaced": [],
    }
    return data, out_path


# ── TSV-sourced dig-site sets (Treasure Maps locations + U Mine It) ──────────
# Both live under Treasure Maps and render through the SAME collectables-spawns
# engine + collect tracking. Their committed TSVs already carry region +
# closest_fast_travel, so CI needs no Mappalachia DB.
DIG_SETS = {
    # Same channel rule as the graves TSV above: read this channel's file, fall
    # back to live if PTS hasn't generated one.
    "treasure-maps-locations": _ts.derived_read("treasure_map_dig_sites.tsv", GRAVE_CHANNEL),
    "u-mine-it": _ts.derived_read("u_mine_it_dig_sites.tsv", GRAVE_CHANNEL),
}


def _dig_label(ref_edid, site_number, idx):
    """Per-site label. A hand-set site_number wins; else a treasure-map mound is named
    by its map number; else generic 'Dig Site #N' within the marker."""
    sn = str(site_number or "").strip()
    if sn:
        return f"Dig Site #{sn}"
    m = re.search(r"(\d+)\s*$", ref_edid or "")
    if m and "treasuremapmound" in (ref_edid or "").lower():
        return f"Treasure Map {int(m.group(1)):02d}"
    return f"Dig Site #{idx + 1}"


def _read_dig_rows(tsv_path):
    with open(tsv_path, newline="", encoding="utf-8") as f:
        return [{k: (r.get(k) or "").strip() for k in
                 ("region", "site_number", "ref_edid", "ref_formid",
                  "closest_fast_travel", "directions", "photo_approach", "photo_spawn")}
                for r in csv.DictReader(f, delimiter="\t")]


def build_dig_set(slug):
    """Reshape a committed dig-site TSV into collectable_spawns_<slug>.json — the
    per-spawn, 2-photo (Map location / Directions / Item in place) layout the grave
    page uses. Editorial directions/photos live IN the TSV (merge-preserved by the
    TSV generator) and are also merge-preserved from any published JSON by identity."""
    tsv_path = DIG_SETS[slug]
    meta = meta_for(slug)
    out_path = os.path.join(OUT_DIR, f"collectable_spawns_{slug}.json")
    handfills = load_grave_spawn_handfills(out_path)     # (region,marker,label) safety net
    existing_top = load_existing_top(out_path)
    full_map = meta.get("full_map") or existing_top.get("full_map", "")
    blurb_quote = meta.get("blurb_quote") or existing_top.get("blurb_quote", "")

    by_region = defaultdict(lambda: defaultdict(list))
    for r in _read_dig_rows(tsv_path):
        region = r["region"] or ""
        marker = r["closest_fast_travel"] or "(unknown location)"
        by_region[region][marker].append(r)

    regions_out, total = [], 0
    for region in REGIONS_AZ:
        locs = []
        for marker in sorted(by_region.get(region, {}), key=lambda m: marker_sort_key(region, m)):
            spawns = []
            for i, g in enumerate(by_region[region][marker]):
                label = _dig_label(g["ref_edid"], g["site_number"], i)
                hf = handfills.get((region, marker, label), {})
                spawns.append({
                    "label": label,
                    "image_top": g["photo_approach"] or hf.get("image_top", ""),
                    "directions": g["directions"] or hf.get("directions", ""),
                    "image_bottom": g["photo_spawn"] or hf.get("image_bottom", ""),
                    "refs": [g["ref_formid"]] if g["ref_formid"] else [],
                })
            total += len(spawns)
            locs.append({"marker": marker, "count": len(spawns), "spawns": spawns})
        regions_out.append({"region": region, "locations": locs})

    data = {
        "_meta": {
            "generated": datetime.date.today().isoformat(),
            "source": f"{os.path.relpath(tsv_path, REPO)} (placements + editorial, regions "
                      "pre-resolved) — reshaped for the collectables-spawns engine",
        },
        "set": slug, "name": meta["name"], "page_title": meta["page_title"],
        "blurb": meta["blurb"], "blurb_quote": blurb_quote, "full_map": full_map,
        "total": total, "regions": regions_out, "unplaced": [],
    }
    return data, out_path


def _manifest_entry(data):
    region_counts = {reg["region"]: len(reg["locations"]) for reg in data["regions"] if reg["locations"]}
    return {"set": data["set"], "name": data["name"], "page_title": data["page_title"],
            "total": data["total"], "regions": region_counts, "unplaced": len(data["unplaced"])}


def build_graves_only():
    """Build ONLY the grave-sites set (no Mappalachia resolve) and merge its manifest
    entry in place, leaving every other set's JSON and manifest row untouched."""
    if not os.path.exists(GRAVE_TSV):
        print(f"[collectable_spawns] {GRAVE_TSV} not found — nothing to build.")
        return
    os.makedirs(OUT_DIR, exist_ok=True)
    data, out_path = build_grave_set()
    json.dump(data, open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    rc = {reg["region"]: len(reg["locations"]) for reg in data["regions"] if reg["locations"]}
    print(f"[collectable_spawns] {GRAVE_SLUG}: {data['total']} grave sites across "
          f"{len(rc)} region(s) -> {os.path.basename(out_path)}")

    # Placement patch log. Built from REFR export vs REFR export — the only place
    # a grave being added, removed or re-placed is visible. Diffing this file
    # against its own previous revision (what every other feed used to do) could
    # never see it: with no new export the output never moved, so the page showed
    # a cheerful "nothing changed" for five weeks while sending players to graves
    # that were not there.
    try:
        import patchlog_utils as _pl
        _pl.write_export_patchlog(
            dist_dir=OUT_DIR,
            feed_name="patchlog_latest_df_placements.json",
            record_type="REFR",
            pattern="REFR_Placements_*.tsv",
            key_col="RefFormID",
            name_cols=("RefEDID", "RefFormID"),
            fields={"X": "X", "Y": "Y", "Z": "Z"},
            # Only the graves' own base object — not every placement in Appalachia.
            scope=lambda r: (r.get("BaseFormID", "") or "").strip().upper() == GRAVE_BASE,
            current_count=data["total"],
        )
    except Exception as e:
        print(f"[collectable_spawns] placement patch log skipped: {e}")

    mpath = os.path.join(OUT_DIR, "collectable_spawns_manifest.json")
    try:
        manifest = json.load(open(mpath, encoding="utf-8"))
    except Exception:
        manifest = {"_meta": {"generated": datetime.date.today().isoformat()}, "sets": []}
    sets = [s for s in manifest.get("sets", []) if s.get("set") != GRAVE_SLUG]
    sets.append(_manifest_entry(data))
    manifest["sets"] = sets
    json.dump(manifest, open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[collectable_spawns] merged {GRAVE_SLUG} into manifest ({len(sets)} set(s)).")


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

    # Pint-Sized Phantoms grave sites — TSV-sourced, so it isn't in resolved_rows.
    # Built here too (not just via --graves-only) so a full rebuild keeps it current.
    if os.path.exists(GRAVE_TSV):
        gdata, gpath = build_grave_set()
        json.dump(gdata, open(gpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        manifest_sets.append(_manifest_entry(gdata))
        grc = {reg["region"]: len(reg["locations"]) for reg in gdata["regions"] if reg["locations"]}
        print(f"[collectable_spawns] {GRAVE_SLUG}: {gdata['total']} grave sites across "
              f"{len(grc)} region(s) -> {os.path.basename(gpath)}")

    # TSV-sourced dig-site sets (Treasure Maps locations + U Mine It). Regions are
    # pre-resolved in their committed TSVs, so these build with no Mappalachia DB.
    for slug in DIG_SETS:
        if not os.path.exists(DIG_SETS[slug]):
            print(f"[collectable_spawns] {slug}: {os.path.basename(DIG_SETS[slug])} not found — skipped.")
            continue
        ddata, dpath = build_dig_set(slug)
        json.dump(ddata, open(dpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        manifest_sets.append(_manifest_entry(ddata))
        drc = {reg["region"]: len(reg["locations"]) for reg in ddata["regions"] if reg["locations"]}
        print(f"[collectable_spawns] {slug}: {ddata['total']} dig sites across "
              f"{len(drc)} region(s) -> {os.path.basename(dpath)}")

    manifest = {
        "_meta": {"generated": datetime.date.today().isoformat()},
        "sets": manifest_sets,
    }
    mpath = os.path.join(OUT_DIR, "collectable_spawns_manifest.json")
    json.dump(manifest, open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[collectable_spawns] wrote manifest with {len(manifest_sets)} set(s) -> "
          f"{os.path.basename(mpath)}")


def _rebuild_manifest_from_docs():
    """Rebuild the manifest from EVERY committed collectable_spawns_<set>.json on disk,
    so a targeted build (graves-only / dig-only, or a run missing an export) never drops
    a set that already has a doc."""
    import glob
    sets = []
    for f in sorted(glob.glob(os.path.join(OUT_DIR, "collectable_spawns_*.json"))):
        if f.endswith("_manifest.json"):
            continue
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        if isinstance(d, dict) and d.get("set") and "regions" in d:
            sets.append(_manifest_entry(d))
    mpath = os.path.join(OUT_DIR, "collectable_spawns_manifest.json")
    json.dump({"_meta": {"generated": datetime.date.today().isoformat()}, "sets": sets},
              open(mpath, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[collectable_spawns] manifest rebuilt from {len(sets)} on-disk set(s): "
          f"{[s['set'] for s in sets]}")


def build_dig_only():
    """Build ONLY the TSV-sourced dig-site sets (Treasure Maps locations + U Mine It) —
    no Mappalachia resolve — then rebuild the manifest from all on-disk docs so the mask
    / grave sets are preserved."""
    os.makedirs(OUT_DIR, exist_ok=True)
    for slug in DIG_SETS:
        if not os.path.exists(DIG_SETS[slug]):
            print(f"[collectable_spawns] {slug}: {os.path.basename(DIG_SETS[slug])} not found — skipped.")
            continue
        data, path = build_dig_set(slug)
        json.dump(data, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        drc = {reg["region"]: len(reg["locations"]) for reg in data["regions"] if reg["locations"]}
        print(f"[collectable_spawns] {slug}: {data['total']} dig sites across "
              f"{len(drc)} region(s) -> {os.path.basename(path)}")
    _rebuild_manifest_from_docs()


if __name__ == "__main__":
    import sys
    if "--graves-only" in sys.argv[1:]:
        build_graves_only()
    elif "--dig-only" in sys.argv[1:]:
        build_dig_only()
    else:
        main()
