#!/usr/bin/env python3
"""
build_hto_locations_json.py
Build HTO (Infestations) location JSON for buffsnbrew.com.

Reads the LocTypeHostileTakeover keyword (008A1490) refs from TSV to get
the game-defined list of infestation spawn locations, then looks up map
coordinates from data/mappalachia_coords.json (static, committed to repo).

Output: dist/infestations/hto_locations.json
"""

import json, os, glob, csv, sqlite3
from pathlib import Path
from collections import Counter

_REPO_ROOT  = Path(__file__).resolve().parent.parent
TSV_DIR     = _REPO_ROOT / "tsv"
DIST_DIR    = _REPO_ROOT / "dist" / "infestations"
COORDS_JSON = _REPO_ROOT / "data" / "mappalachia_coords.json"

_MAPPALACHIA_CANDIDATES = [
    Path(r"D:\Mappalachia\data\mappalachia.db"),
    _REPO_ROOT.parent / "Mappalachia" / "data" / "mappalachia.db",
]

KEYWORD_FORMID = "008A1490"

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

REGION_ORDER = ["Ash Heap","Cranberry Bog","Savage Divide","The Forest","The Mire","Toxic Valley","Unknown"]


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
            print(f"Coords JSON missing — rebuilding from {p}")
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
        print(f"Coords JSON missing — rebuilding from {env}")
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
    print("build_hto_locations_json.py")
    print("=" * 60)

    refs_path = newest("KYWD_Export_*_Refs.tsv")
    if not refs_path:
        print("ERROR: No KYWD_Export_*_Refs.tsv found in", TSV_DIR)
        return
    print(f"Reading: {os.path.basename(refs_path)}")

    rows = read_tsv(refs_path)
    locations = []
    for row in rows:
        if row.get("KeywordFormID") != KEYWORD_FORMID:
            continue
        if row.get("RefSignature") != "LCTN":
            continue
        locations.append({
            "formid":   row["RefFormID"],
            "edid":     row["RefEDID"],
            "name":     row["RefName"],
            "index":    int(row["RefIndex"]),
            "region":   region_from_edid(row["RefEDID"]),
            "workshop": "Workshop" in row["RefEDID"],
        })

    print(f"Found {len(locations)} LCTN entries on keyword {KEYWORD_FORMID}")
    if not locations:
        print("ERROR: No locations found.")
        return

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
            "keyword": KEYWORD_FORMID,
            "keyword_edid": "LocTypeHostileTakeover",
            "total_locations": len(locations),
            "coords_source": "Mappalachia" if coords_found else "none",
            "detection_radius": 18000,
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
