#!/usr/bin/env python3
"""
build_hto_locations_json.py
Build HTO (Infestations) location JSON for buffsnbrew.com.

Reads the LocTypeHostileTakeover keyword (008A1490) refs from TSV to get
the game-defined list of infestation spawn locations, then cross-references
Mappalachia's SQLite database for map coordinates.

Output: dist/infestations/hto_locations.json
Consumed by: df-bnb-infestations.js (locations page)

Data sources:
  - KYWD_Export_*_Refs.tsv  → keyword 008A1490 → LCTN form IDs + names
  - Mappalachia mappalachia.db → MapMarker table → x/y positions
"""

import json, os, glob, csv, sqlite3
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_DIR    = _REPO_ROOT / "tsv"
DIST_DIR   = _REPO_ROOT / "dist" / "infestations"

# Mappalachia DB — check common locations
_MAPPALACHIA_CANDIDATES = [
    Path(r"D:\Mappalachia\data\mappalachia.db"),
    _REPO_ROOT / "mappalachia" / "mappalachia.db",
    _REPO_ROOT.parent / "Mappalachia" / "data" / "mappalachia.db",
]

KEYWORD_FORMID = "008A1490"  # LocTypeHostileTakeover

# ── Region mapping from LCTN EditorID prefix ────────────────────────────────
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

# ── Helpers ─────────────────────────────────────────────────────────────────

def newest(pattern):
    """Pick the newest TSV matching a glob pattern."""
    hits = sorted(glob.glob(str(TSV_DIR / pattern)))
    return hits[-1] if hits else None

def read_tsv(path):
    """Read a TSV file, return list of dicts."""
    if not path or not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except (UnicodeDecodeError, KeyError):
            continue
    return []

def find_mappalachia_db():
    """Locate the Mappalachia SQLite database."""
    for p in _MAPPALACHIA_CANDIDATES:
        if p.exists():
            return str(p)
    # Try environment variable
    env = os.environ.get("MAPPALACHIA_DB")
    if env and os.path.exists(env):
        return env
    return None

def region_from_edid(edid):
    """Derive the Appalachian region from a LCTN EditorID."""
    for prefix, region in EDID_REGION_MAP.items():
        if edid.startswith(prefix):
            return region
    return "Unknown"

def is_workshop(edid):
    """Check if a location is a workshop from its EditorID."""
    return "Workshop" in edid

# ── Main build ──────────────────────────────────────────────────────────────

def build():
    print("build_hto_locations_json.py")
    print("=" * 60)

    # 1. Read keyword refs
    refs_path = newest("KYWD_Export_*_Refs.tsv")
    if not refs_path:
        print("ERROR: No KYWD_Export_*_Refs.tsv found in", TSV_DIR)
        return
    print(f"Reading keyword refs: {os.path.basename(refs_path)}")

    rows = read_tsv(refs_path)
    locations = []
    for row in rows:
        if row.get("KeywordFormID") != KEYWORD_FORMID:
            continue
        if row.get("RefSignature") != "LCTN":
            continue
        locations.append({
            "formid":  row["RefFormID"],
            "edid":    row["RefEDID"],
            "name":    row["RefName"],
            "index":   int(row["RefIndex"]),
            "region":  region_from_edid(row["RefEDID"]),
            "workshop": is_workshop(row["RefEDID"]),
        })

    print(f"Found {len(locations)} LCTN entries on keyword {KEYWORD_FORMID}")

    if not locations:
        print("ERROR: No locations found. Check that keyword 008A1490 exists in the KYWD refs TSV.")
        return

    # 2. Cross-reference Mappalachia for coordinates
    db_path = find_mappalachia_db()
    coords_found = 0

    if db_path:
        print(f"Using Mappalachia DB: {db_path}")
        db = sqlite3.connect(db_path)
        cur = db.cursor()

        for loc in locations:
            name = loc["name"]
            # Try exact match first, then partial
            cur.execute("SELECT x, y, label FROM MapMarker WHERE label = ?", (name,))
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT x, y, label FROM MapMarker WHERE label LIKE ?", (name + "%",))
                row = cur.fetchone()
            if not row:
                cur.execute("SELECT x, y, label FROM MapMarker WHERE label LIKE ?", ("%" + name + "%",))
                row = cur.fetchone()

            if row:
                loc["x"] = round(row[0])
                loc["y"] = round(row[1])
                loc["mapLabel"] = row[2]
                coords_found += 1
            else:
                loc["x"] = 0
                loc["y"] = 0
                loc["mapLabel"] = ""
                print(f"  WARNING: No MapMarker match for '{name}' (edid: {loc['edid']})")

        db.close()
        print(f"Coordinates matched: {coords_found}/{len(locations)}")
    else:
        print("WARNING: Mappalachia DB not found. Coordinates will be 0,0.")
        print("  Set MAPPALACHIA_DB env var or place mappalachia.db at one of:")
        for p in _MAPPALACHIA_CANDIDATES:
            print(f"    {p}")
        for loc in locations:
            loc["x"] = 0
            loc["y"] = 0
            loc["mapLabel"] = ""

    # 3. Sort by region then name
    region_order = ["Ash Heap", "Cranberry Bog", "Savage Divide", "The Forest", "The Mire", "Toxic Valley", "Unknown"]
    locations.sort(key=lambda l: (region_order.index(l["region"]) if l["region"] in region_order else 99, l["name"]))

    # 4. Build output
    output = {
        "_meta": {
            "generator": "build_hto_locations_json.py",
            "keyword": KEYWORD_FORMID,
            "keyword_edid": "LocTypeHostileTakeover",
            "total_locations": len(locations),
            "coords_source": "Mappalachia" if db_path else "none",
            "detection_radius": 18000,
        },
        "locations": [],
    }

    for i, loc in enumerate(locations):
        entry = {
            "id": i + 1,
            "formid": loc["formid"],
            "name": loc["name"],
            "region": loc["region"],
            "x": loc["x"],
            "y": loc["y"],
            "workshop": loc["workshop"],
        }
        output["locations"].append(entry)

    # 5. Write JSON
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIST_DIR / "hto_locations.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nOutput: {out_path}")
    print(f"  {len(output['locations'])} locations across {len(set(l['region'] for l in locations))} regions")

    # Summary by region
    from collections import Counter
    rc = Counter(l["region"] for l in locations)
    for region in region_order:
        if region in rc:
            print(f"  {region}: {rc[region]}")

    print("\nDone.")

if __name__ == "__main__":
    build()
