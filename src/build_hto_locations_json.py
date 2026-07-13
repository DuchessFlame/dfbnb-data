#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_hto_locations_json.py
Build HTO (Infestations) location JSON for buffsnbrew.com.

Two data-sourcing modes:

  CURATED (default / live):
    Source of truth is the CURATED_LOCATIONS allowlist below -- the dev-verified
    list of confirmed infestation spawn locations. Formids are kept where known;
    map coordinates are looked up from data/mappalachia_coords.json.
    To add/remove a spawn location, edit CURATED_LOCATIONS.

  GENERATIVE (--pts or --generative):
    Reads the LCTN TSV from the active TSV_DIR, finds every LCTN record tagged
    with the LocTypeHostileTakeover keyword (008A1490), excludes workshops
    (LocTypeWorkshop 000234F1 or "Workshop" in EDID/name), resolves regions
    from EDID prefixes (with PNAM fallback), and looks up map coordinates from
    data/mappalachia_coords.json. This way, if Bethesda adds or removes HTO
    locations on PTS, the page reflects it automatically.

Output paths:
  (default / live)  -> dist/infestations/hto_locations.json
  --pts             -> dist/pts/infestations/hto_locations.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.

Flags:
  --pts          PTS mode (tsv/pts/ input, dist/pts/ output, generative)
  --generative   Generative mode without PTS paths (for the normalize-then-
                 relocate PTS workflow that reads from tsv/ and relocates to
                 dist/pts/ after build)
"""

import json, os, sys, glob, csv, sqlite3
from pathlib import Path
from collections import Counter

PTS = "--pts" in sys.argv
GENERATIVE = PTS or "--generative" in sys.argv

_REPO_ROOT  = Path(__file__).resolve().parent.parent
TSV_DIR     = _REPO_ROOT / "tsv" / ("pts" if PTS else "")
DIST_DIR    = (_REPO_ROOT / "dist" / "pts" / "infestations") if PTS \
              else (_REPO_ROOT / "dist" / "infestations")
COORDS_JSON = _REPO_ROOT / "data" / "mappalachia_coords.json"

_MAPPALACHIA_CANDIDATES = [
    Path(r"D:\Mappalachia\data\mappalachia.db"),
    _REPO_ROOT.parent / "Mappalachia" / "data" / "mappalachia.db",
]

KEYWORD_FORMID = "008A1490"
WORKSHOP_KEYWORD = "000234F1"  # LocTypeWorkshop

EDID_REGION_MAP = {
    "LocForest":     "The Forest",
    "SubForest":     "The Forest",
    "LocToxic":      "Toxic Valley",
    "LocSwamp":      "The Mire",
    "LocMountains":  "Savage Divide",
    "LocMountain":   "Savage Divide",
    "LocMTR":        "Ash Heap",
    "LocCranberry":  "Cranberry Bog",
    "LocWhitespring":"Savage Divide",
}

PNAM_REGION_MAP = {
    "Mire":          "The Mire",
    "The Ash Heap":  "Ash Heap",
}

REGION_ORDER = ["Ash Heap","Cranberry Bog","Savage Divide","The Forest","The Mire","Toxic Valley","Unknown"]

# -- Curated, dev-verified spawn locations (source of truth) --
# (name, region, formid)  -- formid "" where unknown (new locations).
CURATED_LOCATIONS = [
    # Cranberry Bog
    ("Fort Defiance",                    "Cranberry Bog", "00004143"),
    ("Appalachian Antiques",             "Cranberry Bog", "00004148"),
    ("The General's Steakhouse",         "Cranberry Bog", "0000E131"),
    ("Ranger District Office",           "Cranberry Bog", "0009A026"),
    # The Forest
    ("Aaronholt Homestead",              "The Forest",    "00004145"),
    ("Bolton Greens",                    "The Forest",    "00096ABB"),
    ("Charleston Trainyard",             "The Forest",    "0010CCE9"),
    ("The Giant Teapot",                 "The Forest",    "0009A04F"),
    ("Greg's Mine Supply",               "The Forest",    "00091F0A"),
    ("Morgantown",                       "The Forest",    "00048318"),
    ("Morgantown Trainyard",             "The Forest",    "0008CD53"),
    ("New Gad",                          "The Forest",    "0009A24F"),
    ("Poseidon Energy Plant WV-06",      "The Forest",    ""),
    ("Summersville Dam",                 "The Forest",    "002E8048"),
    ("Summersville Docks",               "The Forest",    "0009A18D"),
    ("Tyler County Fairgrounds",         "The Forest",    "000A1B7E"),
    # Savage Divide
    ("Huntersville",                     "Savage Divide", "0008FEE3"),
    ("New Appalachian Central Trainyard","Savage Divide", "00095519"),
    ("Sons of Dane Compound",            "Savage Divide", "0010A0B8"),
    ("Palace of the Winding Path",       "Savage Divide", "00096A63"),
    ("Pleasant Valley Cabins",           "Savage Divide", "000193A9"),
    ("Pleasant Valley Ski Resort",       "Savage Divide", "000193A7"),
    ("Seneca Rocks Visitor Center",      "Savage Divide", "0009A0D1"),
    ("Sunnytop Ski Lanes",               "Savage Divide", "00007332"),
    ("The Whitespring Golf Club",        "Savage Divide", "0009A451"),
    # Ash Heap
    ("AMS Testing Site",                 "Ash Heap",      "003919F5"),
    ("Beckley",                          "Ash Heap",      "00329729"),
    ("Brim Quarry",                      "Ash Heap",      "00217A8D"),
    ("Lewisburg",                        "Ash Heap",      "0009056E"),
    ("Mount Blair Trainyard",            "Ash Heap",      "00093D5F"),
    ("Welch",                            "Ash Heap",      "000B4871"),
    # The Mire
    ("Berkeley Springs",                 "The Mire",      "0000414B"),
    ("Dyer Chemical",                    "The Mire",      "00055E01"),
    ("Harpers Ferry",                    "The Mire",      "00070368"),
    # Toxic Valley
    ("Grafton",                          "Toxic Valley",  "0006DE2D"),
    ("Grafton Steel",                    "Toxic Valley",  "0006D2F2"),
]


def newest(pattern):
    hits = sorted(glob.glob(str(TSV_DIR / pattern)))
    return hits[-1] if hits else None

def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except (UnicodeDecodeError, KeyError):
            continue
    return []

def region_from_edid(edid):
    for prefix, region in EDID_REGION_MAP.items():
        if edid.startswith(prefix):
            return region
    return "Unknown"


def build_from_lctn():
    """Read LCTN TSV and extract locations tagged with LocTypeHostileTakeover.

    Returns the same list-of-dicts structure as the curated path so the rest
    of build() can treat both sources identically.
    """
    lctn_path = newest("LCTN_Export_*_LCTN.tsv")
    if not lctn_path:
        print(f"ERROR: No LCTN_Export_*_LCTN.tsv found in {TSV_DIR}")
        sys.exit(1)

    rows = read_tsv(lctn_path)
    print(f"Read {len(rows)} LCTN records from {os.path.basename(lctn_path)}")

    locations = []
    skipped_workshops = []

    for row in rows:
        # Scan keyword columns (KW_1 .. KW_13) for HTO and Workshop keywords
        has_hto = False
        has_workshop = False
        for i in range(1, 14):
            kw = row.get(f"KW_{i}", "")
            if KEYWORD_FORMID in kw:
                has_hto = True
            if WORKSHOP_KEYWORD in kw:
                has_workshop = True

        if not has_hto:
            continue

        edid   = row.get("LCTN_EDID", "")
        name   = row.get("LCTN_FULL", "")
        formid = row.get("LCTN_FormID", "")

        # Skip records with no display name (test/debug stubs)
        if not name:
            continue

        # Exclude workshops -- keyword, EDID, or display name
        if has_workshop or "Workshop" in edid or "Workshop" in name:
            skipped_workshops.append(f"{name} [{formid}] ({edid})")
            continue

        # Determine region from EDID prefix
        region = region_from_edid(edid)
        if region == "Unknown":
            # Fallback: parse PNAM_ParentLocation
            # Format: "FormID:EDID:DisplayRegion" e.g. "002F706D:SubRegionSwamp06Location:Mire"
            pnam = row.get("PNAM_ParentLocation", "")
            if ":" in pnam:
                pnam_region = pnam.rsplit(":", 1)[-1].strip()
                region = PNAM_REGION_MAP.get(pnam_region, pnam_region)

        locations.append({
            "formid": formid,
            "name":   name,
            "region": region,
            "workshop": False,
        })

    print(f"HTO locations found: {len(locations)}")
    if skipped_workshops:
        print(f"Workshops excluded ({len(skipped_workshops)}):")
        for w in skipped_workshops:
            print(f"  {w}")

    return locations


def load_coords():
    """Load coordinate lookup. Prefer static JSON; fall back to Mappalachia DB."""
    if COORDS_JSON.exists():
        print(f"Using coords lookup: {COORDS_JSON.name}")
        with open(COORDS_JSON, encoding="utf-8") as f:
            lookup = json.load(f)
        print(f"  {len(lookup)} map markers loaded")
        return lookup

    # Fallback: regenerate from Mappalachia DB
    for p in _MAPPALACHIA_CANDIDATES:
        if p.exists():
            print(f"Coords JSON missing -- rebuilding from {p}")
            db = sqlite3.connect(str(p))
            cur = db.cursor()
            cur.execute("SELECT label, x, y FROM MapMarker WHERE label != '' ORDER BY label")
            lookup = {}
            for label, x, y in cur.fetchall():
                if label not in lookup:
                    lookup[label] = {"x": round(x), "y": round(y)}
            db.close()
            COORDS_JSON.parent.mkdir(parents=True, exist_ok=True)
            with open(COORDS_JSON, "w", encoding="utf-8") as f:
                json.dump(lookup, f, indent=1, ensure_ascii=False)
            print(f"  Wrote {len(lookup)} markers to {COORDS_JSON.name}")
            return lookup

    env = os.environ.get("MAPPALACHIA_DB")
    if env and os.path.exists(env):
        print(f"Coords JSON missing -- rebuilding from {env}")
        db = sqlite3.connect(env)
        cur = db.cursor()
        cur.execute("SELECT label, x, y FROM MapMarker WHERE label != '' ORDER BY label")
        lookup = {}
        for label, x, y in cur.fetchall():
            if label not in lookup:
                lookup[label] = {"x": round(x), "y": round(y)}
        db.close()
        COORDS_JSON.parent.mkdir(parents=True, exist_ok=True)
        with open(COORDS_JSON, "w", encoding="utf-8") as f:
            json.dump(lookup, f, indent=1, ensure_ascii=False)
        print(f"  Wrote {len(lookup)} markers to {COORDS_JSON.name}")
        return lookup

    print("WARNING: No coords source found. Coordinates will be 0,0.")
    return {}


def build():
    mode = "PTS" if PTS else ("GENERATIVE" if GENERATIVE else "LIVE")
    print(f"build_hto_locations_json.py  [{mode}]")
    print(f"  TSV_DIR:  {TSV_DIR}")
    print(f"  DIST_DIR: {DIST_DIR}")
    print("=" * 60)

    if GENERATIVE:
        locations = build_from_lctn()
    else:
        locations = [
            {"formid": fid, "name": name, "region": region, "workshop": False}
            for (name, region, fid) in CURATED_LOCATIONS
        ]
        print(f"Curated spawn locations: {len(locations)}")

    coords_lookup = load_coords()
    coords_found = 0
    for loc in locations:
        name = loc["name"]
        match = coords_lookup.get(name)
        if not match:
            for label, coords in coords_lookup.items():
                if label.startswith(name) or name in label:
                    match = coords
                    break
        if match:
            loc["x"] = match["x"]
            loc["y"] = match["y"]
            coords_found += 1
        else:
            loc["x"] = 0
            loc["y"] = 0
            print(f"  WARNING: No coords for '{name}'")

    print(f"Coordinates matched: {coords_found}/{len(locations)}")

    locations.sort(key=lambda l: (
        REGION_ORDER.index(l["region"]) if l["region"] in REGION_ORDER else 99,
        l["name"]
    ))

    output = {
        "_meta": {
            "generator": "build_hto_locations_json.py",
            "source": "LCTN TSV (LocTypeHostileTakeover keyword)" if GENERATIVE
                      else "curated allowlist (dev-verified)",
            "keyword_edid": "LocTypeHostileTakeover",
            "total_locations": len(locations),
            "coords_source": "Mappalachia" if coords_found else "none",
            "detection_radius": 18000,
            "isPts": PTS,
        },
        "locations": [
            {"id": i+1, "formid": l["formid"], "name": l["name"],
             "region": l["region"], "x": l["x"], "y": l["y"],
             "workshop": l["workshop"]}
            for i, l in enumerate(locations)
        ],
    }

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIST_DIR / "hto_locations.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nOutput: {out_path}")
    rc = Counter(l["region"] for l in locations)
    print(f"  {len(locations)} locations, {len(rc)} regions")
    for r in REGION_ORDER:
        if r in rc:
            print(f"  {r}: {rc[r]}")
    print("Done.")


if __name__ == "__main__":
    build()
