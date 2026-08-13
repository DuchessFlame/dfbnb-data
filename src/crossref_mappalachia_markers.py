#!/usr/bin/env python3
r"""
crossref_mappalachia_markers.py — resolve nearest map marker + region for collectable spawns.

Takes the xEdit location export(s) produced by tools/ExportCollectableLocationsToTSV.pas
(rows of set / item / section / worldspace / cell / x / y / z / ref_formid) and, per placed
reference, resolves:

  * REGION  — point-in-polygon of the ref X/Y against the Mappalachia region tilings
              (the same SubRegion families build_npc_spawns.py uses; includes Skyline Valley
              via the "Storm" region and Burning Springs).
  * MARKER  — nearest Mappalachia map marker (Euclidean) to the ref X/Y; becomes the
              spawn's sub-expand name.

The Mappalachia DB only covers the Appalachia worldspace. The two instanced expedition
worldspaces — Atlantic City (XPDAC*) and The Pitt (XPDPitt*) — have NO map markers in the
DB, so those refs are resolved from a hand-maintained map (data/collectable_spawns/
manual_regions.json), giving a per-district sub-marker like "Atlantic City - The Boardwalk".

Outputs (both committed so CI can rebuild without the 459 MB DB):
  * tsv/collectable_locations_resolved_<yyyy-mm-dd>.tsv — input rows + region/marker/resolved_by
  * data/collectable_spawns/geo_cache.json             — {ref_formid: {region, marker, ...}}

Inputs (override with env vars):
  MAPPALACHIA_DB    default D:\Mappalachia\data\mappalachia.db
  INPUT_GLOB        default <repo>/tsv/*_CollectableLocations_*.tsv   (newest per section)
  MANUAL_REGIONS    default <repo>/data/collectable_spawns/manual_regions.json
  OUT_TSV           default <repo>/tsv/collectable_locations_resolved_<date>.tsv
  GEO_CACHE         default <repo>/data/collectable_spawns/geo_cache.json

When the DB is absent (CI), the committed geo_cache is used to re-resolve refs by FormID and
only newly-seen refs are reported as unresolved.
"""

import os, re, csv, glob, json, sqlite3, datetime
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
INPUT_GLOB     = os.environ.get("INPUT_GLOB",     os.path.join(REPO, "tsv", "*2_Export_*.tsv"))
MANUAL_REGIONS = os.environ.get("MANUAL_REGIONS", os.path.join(REPO, "data", "collectable_spawns", "manual_regions.json"))
GEO_CACHE      = os.environ.get("GEO_CACHE",      os.path.join(REPO, "data", "collectable_spawns", "geo_cache.json"))
OUT_TSV        = os.environ.get("OUT_TSV",        os.path.join(
                     REPO, "tsv", f"collectable_locations_resolved_{datetime.date.today().isoformat()}.tsv"))

WORLDSPACE = 2480661  # Appalachia (the only worldspace with markers/regions in the DB)

# SubRegion editor-ID family -> friendly region name (mirrors build_npc_spawns.py).
REGION_FAMILIES = {
    "ForestSubRegion": "Forest",
    "ToxicValleySubRegion": "Toxic Valley",
    "CranberrySubRegion": "Cranberry Bog",
    "SwampSubRegion": "The Mire",
    "MountainSubRegion": "Savage Divide",
    "MountainRemovalSubRegion": "Ash Heap",
    "StormSubRegion": "Skyline Valley",
    "BurningSpringsSubRegion": "Burning Springs",
}

# All ten regions the pages render, so we can validate resolved names.
ALL_REGIONS = {
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
}

# ── LCTN location-keyword → region ───────────────────────────────────────────────
# Every FO76 Location (LCTN) record carries a `LocRegion*` keyword tagging which
# map region it belongs to (tsv/LCTN_Export_*_LCTN.tsv, KW_* columns). This is the
# authoritative, coordinate-free region tag — it resolves interiors AND outdoor
# landmarks that fall in a region-polygon gap, WITHOUT any point-in-polygon math.
LOCREGION_KEYWORD_TO_REGION = {
    "Mountain": "Savage Divide",
    "Whitespring": "Savage Divide",          # sub-region inside Savage Divide
    "ForestFloodlands": "Forest",
    "SwampForest": "The Mire",
    "CranberryBog": "Cranberry Bog",
    "Storm": "Skyline Valley",
    "BurningSprings": "Burning Springs",
    "MTR": "Ash Heap",                       # Mountain-Removal (Mount Blair mining)
    "ToxicValley": "Toxic Valley",
    "AC": "Atlantic City",
    "Pitt": "The Pitt",
}
# When a location carries several LocRegion keywords, prefer the most specific map
# region over the broad "Mountain" (e.g. Whitespring locations also tag Mountain).
LOCREGION_PRIORITY = [
    "CranberryBog", "ToxicValley", "BurningSprings", "Storm", "MTR",
    "SwampForest", "ForestFloodlands", "AC", "Pitt", "Whitespring", "Mountain",
]


def _norm_name(s):
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _edid_core(eid, is_location=True):
    """Reduce an EDID to a comparable core token: strip Loc.../.../Location wrapper,
    trailing digits, lowercase. e.g. 'LocMountainsGarrahanHQLocation' -> 'mountainsgarrahanhq',
    space 'GarrahanMiningHQ01' -> 'garrahanmininghq'."""
    e = eid or ""
    if is_location:
        e = re.sub(r"^Loc", "", e)
        e = re.sub(r"Location$", "", e)
    e = re.sub(r"\d+$", "", e)
    return e.lower()


def load_lctn_regions(tsv_root=None):
    """Build the LCTN-derived region maps from the newest LCTN export.

    Returns (by_name, by_core):
      by_name : { normalised LCTN FULL name -> region }
      by_core : { LCTN EDID core token       -> region }
    A location with no direct LocRegion keyword inherits its PNAM parent's region
    (walked up to 6 hops). Only the 10 canonical map regions are emitted.
    Returns ({}, {}) if the export is absent (callers fall back to the old logic)."""
    root = tsv_root or os.path.join(REPO, "tsv")
    hits = sorted(glob.glob(os.path.join(root, "LCTN_Export_*_LCTN.tsv")),
                  key=os.path.getmtime, reverse=True)
    if not hits:
        return {}, {}
    path = hits[0]

    recs = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        rd = csv.DictReader(f, delimiter="\t")
        kw_cols = [c for c in (rd.fieldnames or []) if re.fullmatch(r"KW_\d+", c or "")]
        for row in rd:
            fid_raw = (row.get("LCTN_FormID") or "").strip()
            if not fid_raw:
                continue
            try:
                fid = int(fid_raw, 16)
            except ValueError:
                continue
            regs = []
            for c in kw_cols:
                m = re.search(r"LocRegion([A-Za-z]+)", row.get(c) or "")
                if m and m.group(1) in LOCREGION_KEYWORD_TO_REGION:
                    regs.append(m.group(1))
            pnam = (row.get("PNAM_ParentLocation") or "").split(":")[0].strip()
            recs[fid] = {"full": row.get("LCTN_FULL") or "", "edid": row.get("LCTN_EDID") or "",
                         "regs": regs, "pnam": pnam}

    def region_of(fid, depth=0):
        r = recs.get(fid)
        if not r or depth > 6:
            return ""
        for kw in LOCREGION_PRIORITY:
            if kw in r["regs"]:
                return LOCREGION_KEYWORD_TO_REGION[kw]
        if r["pnam"]:
            try:
                return region_of(int(r["pnam"], 16), depth + 1)
            except ValueError:
                return ""
        return ""

    by_name, by_core = {}, {}
    for fid, r in recs.items():
        reg = region_of(fid)
        if reg not in ALL_REGIONS:
            continue
        nm = _norm_name(r["full"])
        if nm:
            by_name.setdefault(nm, reg)
            if nm.startswith("the "):
                by_name.setdefault(nm[4:], reg)
        core = _edid_core(r["edid"], is_location=True)
        if core:
            by_core.setdefault(core, reg)
    return by_name, by_core


def lctn_region_for(by_name, by_core, display_name="", space_edid=""):
    """Resolve a region from the LCTN maps by display name (exact, then 'the'-less),
    then by editorID core (space core == LCTN core, then longest substring match).
    Returns '' when nothing matches."""
    nm = _norm_name(display_name)
    if nm in by_name:
        return by_name[nm]
    if nm.startswith("the ") and nm[4:] in by_name:
        return by_name[nm[4:]]
    core = _edid_core(space_edid, is_location=False)
    if core and core in by_core:
        return by_core[core]
    if core:
        best = None
        for c, reg in by_core.items():
            if len(c) >= 5 and (core in c or c in core):
                if best is None or len(c) > best[0]:
                    best = (len(c), reg)
        if best:
            return best[1]
    return ""

# Map markers that sit in a Mappalachia region-polygon GAP (roads, water, borders) so
# point-in-polygon returns no region — even for items placed right on them. Assign the
# correct region here once and any placement nearest that marker resolves globally, on
# every page. Source of truth: the marker's in-game PNAM parent location in xEdit.
#   e.g. The Crater's PNAM is SubRegionToxicValley04Location "Toxic Valley".
MARKER_REGION_OVERRIDES = {
    "The Crater": "Toxic Valley",
    # NE map-edge cluster (Old Danielson Cabin, Hillside Cavern, Point Repose, The Bullengrube…)
    # falls outside every SubRegion polygon; nearest covered marker is Mysterious Cave (Savage
    # Divide). Add siblings here if items ever spawn nearest them.
    "Old Danielson Cabin": "Savage Divide",
    # Ghoul Within update — new far-north Savage Divide landmarks. Their exterior
    # placements sit past the SubRegion polygons, so the nearest-polygon fallback
    # would otherwise mis-assign them to The Mire. (fallout.wiki; user-confirmed.)
    "Radiant Hills": "Savage Divide",
    "Hillside Cavern": "Savage Divide",
}


# ── Mappalachia geometry ─────────────────────────────────────────────────────────
def load_mappalachia():
    con = sqlite3.connect(MAPPALACHIA_DB)
    cur = con.cursor()
    fam_ids = defaultdict(list)
    for fid, eid in cur.execute("SELECT regionFormID, regionEditorID FROM Region"):
        m = re.match(r"([A-Za-z]+SubRegion)\d", eid or "")
        if m and m.group(1) in REGION_FAMILIES:
            fam_ids[REGION_FAMILIES[m.group(1)]].append(fid)
    rings = {}
    for reg, ids in fam_ids.items():
        rr = defaultdict(list)
        q = ("SELECT regionFormID, subRegionIndex, coordIndex, x, y FROM RegionPoints "
             f"WHERE regionFormID IN ({','.join(map(str, ids))}) "
             "ORDER BY regionFormID, subRegionIndex, coordIndex")
        for rfid, si, ci, x, y in cur.execute(q):
            rr[(rfid, si)].append((x, y))
        rings[reg] = [v for v in rr.values() if len(v) >= 3]
    markers = [(l, x, y) for (x, y, l) in
               cur.execute("SELECT x, y, label FROM MapMarker WHERE spaceFormID=? AND label<>''",
                           (WORLDSPACE,))]
    con.close()
    return rings, markers


def pip(rings_list, px, py):
    inside = False
    for ring in rings_list:
        n = len(ring); j = n - 1
        for i in range(n):
            xi, yi = ring[i]; xj, yj = ring[j]
            if ((yi > py) != (yj > py)) and \
               (px < (xj - xi) * (py - yi) / ((yj - yi) or 1e-9) + xi):
                inside = not inside
            j = i
    return inside


def _nearest_region(rings, px, py):
    """Region whose polygon boundary is CLOSEST to (px, py) — used only when the
    point is outside every region polygon (roads/water/borders/map-edge gaps).
    Distance is min squared distance to any ring vertex (cheap; runs on the rare
    fallback path only)."""
    best_reg, best_d = "", 1e30
    for reg, rl in rings.items():
        for ring in rl:
            for x, y in ring:
                d = (x - px) ** 2 + (y - py) ** 2
                if d < best_d:
                    best_d, best_reg = d, reg
    return best_reg


def region_for_xy(rings, px, py, nearest=False):
    """Point-in-polygon region for (px, py). With nearest=True, a point that falls
    outside every region polygon is assigned the CLOSEST polygon's region instead
    of '' — the coordinate-based gap fallback."""
    for reg, rl in rings.items():
        if pip(rl, px, py):
            return reg
    if nearest:
        return _nearest_region(rings, px, py)
    return ""


def nearest_marker(markers, px, py):
    best, bx, by, bd = "", None, None, 1e30
    for l, x, y in markers:
        d = (x - px) ** 2 + (y - py) ** 2
        if d < bd:
            bd, best, bx, by = d, l, x, y
    return best, bx, by


# ── inputs ───────────────────────────────────────────────────────────────────────
def newest_per_section(paths):
    """Given many <SECTION>2_Export_<date>.tsv (MISC2_Export_…, ACTI2_Export_…), keep the
    newest file per SECTION so re-running an export doesn't double-count."""
    by_sec = {}
    for p in paths:
        base = os.path.basename(p)
        m = re.match(r"(.+?)_(?:Export|CollectableLocations)_", base)
        sec = m.group(1) if m else base
        if sec not in by_sec or os.path.getmtime(p) > os.path.getmtime(by_sec[sec]):
            by_sec[sec] = p
    return sorted(by_sec.values())


def read_rows(paths):
    rows = []
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                rows.append(r)
    return rows


def load_manual_regions():
    try:
        data = json.load(open(MANUAL_REGIONS, encoding="utf-8"))
        return data.get("spaces", data)
    except Exception as e:
        print(f"[crossref] WARN: no manual_regions map ({MANUAL_REGIONS}): {e}")
        return {}


def load_geo_cache():
    try:
        return json.load(open(GEO_CACHE, encoding="utf-8"))
    except Exception:
        return {}


def save_geo_cache(cache):
    os.makedirs(os.path.dirname(GEO_CACHE), exist_ok=True)
    json.dump(cache, open(GEO_CACHE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def as_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


# ── resolve one ref ───────────────────────────────────────────────────────────────
def resolve(row, rings, markers, manual, cache):
    """Return (region, marker, resolved_by)."""
    ws = (row.get("worldspace") or "").strip()
    cell = (row.get("cell") or "").strip()
    ref = (row.get("ref_formid") or "").strip().upper()
    px, py = as_float(row.get("x")), as_float(row.get("y"))

    # 1) instanced expedition worldspaces (Atlantic City / The Pitt) — manual map,
    #    keyed by worldspace editorID first, then cell editorID as a fallback.
    for key in (ws, cell):
        if key and key in manual:
            e = manual[key]
            return e["region"], e["marker"], "manual"

    # 2) Appalachia worldspace (includes Skyline Valley) — resolve from coords.
    is_appalachia = ("appalachia" in ws.lower()) or (ws == "" and px is not None and py is not None
                                                     and abs(px) > 2000)  # exterior-scale coords
    if rings is not None and markers is not None and px is not None and py is not None and is_appalachia:
        region = region_for_xy(rings, px, py)
        marker, mx, my = nearest_marker(markers, px, py)
        # If the ref falls in a polygon gap (road/water/border), inherit the region
        # of its nearest marker so a placement never comes back region-less.
        if not region and mx is not None:
            region = region_for_xy(rings, mx, my)
        # Marker itself in a gap (e.g. The Crater) — use the explicit override.
        if not region and marker in MARKER_REGION_OVERRIDES:
            region = MARKER_REGION_OVERRIDES[marker]
        if region or marker:
            return region, marker, "coords"

    # 3) interior placement — coords are cell-local and can't be cross-referenced, but the
    #    xEdit export captured the game location name (CELL FULL / WRLD name). Use it as the
    #    marker so the spawn is still named (e.g. "Vault 63"); region stays blank for a hand
    #    glance unless the name matches the manual map.
    loc = (row.get("loc_name") or "").strip()
    if loc:
        for e in manual.values():
            if loc == e.get("display") or loc == e.get("marker"):
                return e["region"], e["marker"], "loc_name"
        return "", loc, "loc_name"

    # 4) DB absent (CI) — fall back to the committed cache by ref FormID.
    if ref in cache:
        c = cache[ref]
        return c.get("region", ""), c.get("marker", ""), "cache"

    # 5) unresolved — hand-author (no coords, no location name, no cache).
    return "", "", "unresolved"


# ── main ────────────────────────────────────────────────────────────────────────
PASSTHROUGH = ["set", "item_formid", "edid", "name", "section", "worldspace", "cell",
               "x", "y", "z", "ref_formid", "holder", "loc_name", "loc_source"]
OUT_FIELDS = PASSTHROUGH + ["region", "marker", "resolved_by"]


def resolve_dataset(input_glob=None, verbose=True):
    """Resolve every placement in the matching export TSVs to region + marker.

    Returns (resolved_rows, cache, db_ok). Pure read of inputs + Mappalachia DB / committed
    geo cache — no files are written, so build_collectable_spawns_json.py can import and reuse
    this without side effects. The CLI main() wraps this to also write the resolved TSV + cache.
    """
    paths = newest_per_section(sorted(glob.glob(input_glob or INPUT_GLOB)))
    if not paths:
        if verbose:
            print(f"[crossref] no input files matched {input_glob or INPUT_GLOB}")
        return [], load_geo_cache(), False
    if verbose:
        print("[crossref] inputs:")
        for p in paths:
            print("   ", os.path.basename(p))

    rows = read_rows(paths)
    manual = load_manual_regions()
    cache = load_geo_cache()

    db_ok = os.path.isfile(MAPPALACHIA_DB)
    if db_ok:
        rings, markers = load_mappalachia()
        if verbose:
            print(f"[crossref] Mappalachia DB found — resolving from coords, refreshing geo cache "
                  f"({len(markers)} markers, {len(rings)} region tilings).")
    else:
        rings = markers = None
        if verbose:
            print(f"[crossref] Mappalachia DB not found at {MAPPALACHIA_DB} — CI mode: using committed geo cache.")

    resolved_rows = []
    for row in rows:
        region, marker, how = resolve(row, rings, markers, manual, cache)
        out = {k: (row.get(k) or "") for k in PASSTHROUGH}
        out["region"], out["marker"], out["resolved_by"] = region, marker, how
        resolved_rows.append(out)
        # refresh cache from fresh (coords/manual) resolutions only
        if how in ("coords", "manual"):
            ref = (row.get("ref_formid") or "").strip().upper()
            if ref:
                cache[ref] = {"region": region, "marker": marker,
                              "worldspace": row.get("worldspace", ""),
                              "resolved_by": how}
    return resolved_rows, cache, db_ok


def main():
    resolved_rows, cache, db_ok = resolve_dataset()
    if not resolved_rows:
        return

    out_fields = OUT_FIELDS
    by_method = defaultdict(int)
    unresolved = []
    for r in resolved_rows:
        by_method[r["resolved_by"]] += 1
        if r["resolved_by"] == "unresolved":
            unresolved.append(r)

    os.makedirs(os.path.dirname(OUT_TSV), exist_ok=True)
    with open(OUT_TSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, delimiter="\t")
        w.writeheader()
        w.writerows(resolved_rows)

    if db_ok:
        save_geo_cache(cache)

    # ── report ──
    print(f"[crossref] {len(resolved_rows)} placements resolved "
          f"({dict(by_method)}) -> {os.path.basename(OUT_TSV)}")
    per_set = defaultdict(lambda: defaultdict(int))
    for r in resolved_rows:
        per_set[r["set"]][r["region"] or "(unresolved)"] += 1
    for s in sorted(per_set):
        parts = ", ".join(f"{reg}:{n}" for reg, n in sorted(per_set[s].items()))
        print(f"   {s}: {parts}")
    if unresolved:
        print(f"[crossref] {len(unresolved)} unresolved placement(s) need hand-authoring "
              f"(interior local coords or an unmapped instanced space):")
        seen = set()
        for r in unresolved:
            key = (r.get("worldspace", ""), r.get("cell", ""))
            if key in seen:
                continue
            seen.add(key)
            print(f"     set={r.get('set','')}  worldspace='{r.get('worldspace','')}'  cell='{r.get('cell','')}'")
        # flag any resolved region name that isn't one of the ten (typo guard)
    bad = {r["region"] for r in resolved_rows if r["region"] and r["region"] not in ALL_REGIONS}
    if bad:
        print(f"[crossref] WARNING: resolved region names not in the ten-region set: {sorted(bad)}")


if __name__ == "__main__":
    main()
