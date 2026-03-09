#!/usr/bin/env python3
"""
build_allies_pets_weather_json.py

Reads xEdit TSV exports and builds the dist/ JSON files for:
  - weather_stations.json
  - allies.json
  - pets.json
  - pet_furniture.json
  - pet_apparel.json
  - cryos.json
  - fridges.json
  - repair_bots.json

Inputs (from data/tsv/ in the repo):
  COBJ_Export.tsv
  ENTM_Export.tsv
  FURN_Export_FURN.tsv
  FLST_Export_Entries.tsv

Usage:
  python build_allies_pets_weather_json.py [--tsv-dir data/tsv] [--out-dir dist]
"""

import csv
import glob
import json
import os
import re
import argparse
from pathlib import Path

# ---------------------------------------------------------------------------
# CLI args + env var resolution (matches dfbnb-patch-build.yml pattern)
#
# TSV paths are resolved in priority order:
#   1. Explicit env vars set by the workflow picker step
#      (COBJ_TSV, ENTM_TSV, FURN_TSV, FLST_ENTRIES_TSV)
#   2. Glob search inside --tsv-dir for the newest matching file
#   3. Bare filename fallback inside --tsv-dir (for local dev)
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("--tsv-dir", default="tsv",  help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
args = parser.parse_args()

TSV_DIR = Path(args.tsv_dir)
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _newest_glob(pattern):
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def _resolve_tsv(env_var, glob_pattern, fallback_name):
    """Resolve a TSV path via env var → glob → bare fallback."""
    v = os.environ.get(env_var, "").strip()
    if v and Path(v).exists():
        return Path(v)
    found = _newest_glob(str(TSV_DIR / glob_pattern))
    if found:
        return Path(found)
    fallback = TSV_DIR / fallback_name
    if fallback.exists():
        return fallback
    raise FileNotFoundError(
        f"Cannot find TSV for {env_var}. "
        f"Tried env var, glob '{glob_pattern}', and '{fallback_name}' in {TSV_DIR}"
    )


COBJ_PATH = _resolve_tsv("COBJ_TSV",         "COBJ_Export_*.tsv",         "COBJ_Export.tsv")
ENTM_PATH = _resolve_tsv("ENTM_TSV",         "ENTM_Export_*.tsv",         "ENTM_Export.tsv")
FURN_PATH = _resolve_tsv("FURN_TSV",         "FURN_Export_*_FURN.tsv",    "FURN_Export_FURN.tsv")
FLST_PATH = _resolve_tsv("FLST_ENTRIES_TSV", "FLST_Export_*_Entries.tsv", "FLST_Export_Entries.tsv")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_tsv(path, encoding="utf-8"):
    """Load a TSV file by path. Returns list of dicts."""
    rows = []
    with open(path, encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Strip surrounding quotes that xEdit sometimes adds
            rows.append({k: v.strip().strip('"') for k, v in row.items()})
    return rows


def save_json(filename, data):
    path = OUT_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {path}  ({len(data.get('items', data))} items)")


def clean_desc(raw):
    """Strip the boilerplate CAMP MODE suffix from ENTM DESC fields."""
    if not raw:
        return ""
    text = raw.strip()
    # Remove trailing "- C.A.M.P. ITEMS APPEAR WHILE IN C.A.M.P. MODE. -" block
    text = re.sub(
        r"\s*-\s*C\.?A\.?M\.?P\.?\s*ITEMS\s+APPEAR\s+WHILE\s+IN\s+C\.?A\.?M\.?P\.?\s+"
        r"(EDIT\s+)?MODE\.?\s*-\s*",
        " ", text, flags=re.IGNORECASE
    )
    # Collapse multiple spaces
    text = re.sub(r"  +", " ", text).strip()
    return text


def is_cut(edid):
    """Return True if the EDID marks cut/unreleased content."""
    s = str(edid or "").strip().upper()
    return s.startswith(("ZZZ", "ZZZZ", "DEL_", "DELETE_", "CUT_"))


def storefront_img_url(ecil_val, folder=""):
    """
    Convert a .dds ECIL image value to a storefront webp URL.
    ECIL values look like: ATX_CAMP_Utility_WeatherStation_Standard_Clear_C1.dds
    Storefront URLs follow the pattern:
      /wp-content/uploads/storefront/<folder>/<lowercase_name_no_ext>.webp
    """
    if not ecil_val or not ecil_val.strip():
        return None
    name = os.path.splitext(os.path.basename(ecil_val.strip()))[0].lower()
    if not name:
        return None
    base = "/wp-content/uploads/storefront"
    if folder:
        return f"{base}/{folder}/{name}.webp"
    return f"{base}/{name}.webp"


def xalg_to_source(xalg):
    """Map XALG flag string to a human-readable obtain source."""
    s = str(xalg or "").lower()
    if "fallout 1st" in s or "f1st" in s:
        return "Fallout 1st"
    if "premium" in s:
        return "Atom Shop"
    return ""


# ---------------------------------------------------------------------------
# Load TSVs
# ---------------------------------------------------------------------------

print("Loading TSVs…")
print(f"  COBJ: {COBJ_PATH}")
print(f"  ENTM: {ENTM_PATH}")
print(f"  FURN: {FURN_PATH}")
print(f"  FLST: {FLST_PATH}")

cobj_rows      = load_tsv(COBJ_PATH)
entm_rows      = load_tsv(ENTM_PATH)
furn_rows      = load_tsv(FURN_PATH)
flst_entries   = load_tsv(FLST_PATH)

# ---------------------------------------------------------------------------
# Build lookup maps
# ---------------------------------------------------------------------------

# ENTM by FormID
entm_by_id   = {r["FormID"]: r for r in entm_rows}
# ENTM by EDID (for suffix matching)
entm_by_edid = {r["EDID"]: r for r in entm_rows}

# COBJ: map CNAM_FormID → list of COBJ rows (multiple COBJs can share a CNAM)
cobj_by_cnam = {}
for r in cobj_rows:
    cnam = r.get("CNAM_FormID", "").strip()
    if cnam:
        cobj_by_cnam.setdefault(cnam, []).append(r)

# FURN by FormID
furn_by_id = {r["FURN_FormID"]: r for r in furn_rows}

# FLST entries by FLST FormID
flst_by_list = {}
for r in flst_entries:
    flst_by_list.setdefault(r["FLST_FormID"], []).append(r)


def get_entm_for_cobj(cobj_row):
    """
    Find the ENTM record that owns a COBJ via ECIL columns.
    We search all ENTM rows whose ECIL_1..ECIL_N contain the COBJ FormID.
    (Expensive but only done for a small set of items.)
    """
    cobj_id = cobj_row["COBJ_FormID"]
    for entm in entm_rows:
        count = int(entm.get("ECIL_Count", 0) or 0)
        for i in range(1, count + 1):
            ecil = entm.get(f"ECIL_{i}", "").strip()
            if ecil == cobj_id:
                return entm
    return None


def ecil_images(entm_row, folder=""):
    """Extract carousel images from ENTM ECIL columns."""
    images = []
    count = int(entm_row.get("ECIL_Count", 0) or 0)
    for i in range(1, count + 1):
        val = entm_row.get(f"ECIL_{i}", "").strip()
        if val and val.lower().endswith(".dds"):
            url = storefront_img_url(val, folder)
            if url:
                images.append(url)
    return images


def xalg_source_from_furn(form_id):
    """Get obtain source from FURN XALG flags."""
    furn = furn_by_id.get(form_id, {})
    return xalg_to_source(furn.get("XALG_Flags", ""))


# ---------------------------------------------------------------------------
# WEATHER STATIONS
# ---------------------------------------------------------------------------
# Source: FLST 006EE8F0 (ATX_Weather_FormList_WeatherStations) → 18 ACTI entries
# ENTM matched by EDID suffix after ATX_Weather_WeatherStation_ /
# SCORE_*_Weather_WeatherStation_

WEATHER_FLST = "006EE8F0"

# Suffix extractor for ACTI EDID
_ACTI_PREFIX_RE = re.compile(
    r"^(?:SCORE_S\d+_)?ATX_Weather_WeatherStation_(.+)$",
    re.IGNORECASE
)
_ENTM_PREFIX_RE = re.compile(
    r"^(?:SCORE_S\d+_)?ATX_ENTM_CAMP_Utility_WeatherStation_(.+)$",
    re.IGNORECASE
)

# Build suffix → ENTM map
entm_by_suffix = {}
for entm in entm_rows:
    m = _ENTM_PREFIX_RE.match(entm.get("EDID", ""))
    if m:
        entm_by_suffix[m.group(1).lower()] = entm


def build_weather_stations():
    entries = flst_by_list.get(WEATHER_FLST, [])
    items = []

    for entry in sorted(entries, key=lambda r: int(r.get("EntryIndex", 0))):
        acti_id   = entry["Entry_FormID"]
        acti_edid = entry["Entry_EDID"]
        acti_full = entry["Entry_FULL"]

        # Match ENTM by EDID suffix
        m = _ACTI_PREFIX_RE.match(acti_edid)
        suffix = m.group(1).lower() if m else None
        entm = entm_by_suffix.get(suffix) if suffix else None

        desc        = clean_desc(entm.get("DESC", ""))        if entm else ""
        entm_id     = entm["FormID"]                          if entm else ""
        display     = entm.get("FULL", acti_full)             if entm else acti_full
        nnam        = entm.get("NNAM", "")                    if entm else ""
        xalg_flag   = entm.get("XALG", "")                   if entm else ""
        source      = xalg_to_source(xalg_flag) or "Atom Shop"

        # Carousel images from ECIL
        carousel = ecil_images(entm, "camp-utility") if entm else []
        image_url = carousel[0] if carousel else ""

        # Obtain source label
        if "scoreboard" in acti_edid.lower() or acti_edid.upper().startswith("SCORE_"):
            source = "Scoreboard"
            season_m = re.match(r"SCORE_S(\d+)_", acti_edid, re.IGNORECASE)
            season_num = int(season_m.group(1)) if season_m else None
        else:
            season_num = None

        items.append({
            "formId":       acti_id,
            "actiFormId":   acti_id,
            "entmFormId":   entm_id,
            "edid":         acti_edid,
            "displayName":  display or acti_full,
            "shortName":    nnam,
            "description":  desc,
            "obtainSource": source,
            "seasonNumber": season_num,
            "howToObtain":  f"Season {season_num} Scoreboard" if season_num else "Atom Shop",
            "dropRate":     "—",
            "tradeable":    not bool(season_num),
            "imageUrl":     image_url,
            "imageCarousel": carousel,
            "cutContent":   is_cut(acti_edid),
        })

    # Sort A-Z
    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# REPAIR BOTS
# ---------------------------------------------------------------------------
# 4 ENTM skins, each maps to a FURN pod.
# ENTM → COBJ → FURN (pod)
# Base COBJ: 007AE545 builds the station via LVLI

REPAIR_BOT_ENTM_IDS = [
    "007AE546",   # Enclave
    "0082BED5",   # Company
    "0084920E",   # Santa's Helper
    "008571F1",   # Emergency Technician
]

REPAIR_BOT_FURN_IDS = {
    "007AE546": "007AE499",   # Enclave pod FURN
    "0082BED5": "008109D1",   # Company pod FURN
    "0084920E": "0084920C",   # Santa's Helper FURN
    "008571F1": "00884077",   # Emergency Tech FURN
}


def build_repair_bots():
    items = []
    for entm_id in REPAIR_BOT_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        furn_id  = REPAIR_BOT_FURN_IDS.get(entm_id, "")
        furn     = furn_by_id.get(furn_id, {})
        source   = xalg_to_source(furn.get("XALG_Flags", "")) or "Atom Shop"

        desc      = clean_desc(entm.get("DESC", ""))
        display   = entm.get("FULL", "")
        carousel  = ecil_images(entm, "camp-utility")
        image_url = carousel[0] if carousel else ""

        items.append({
            "formId":       furn_id or entm_id,
            "entmFormId":   entm_id,
            "furnFormId":   furn_id,
            "edid":         entm.get("EDID", ""),
            "furnEdid":     furn.get("FURN_EDID", ""),
            "displayName":  display,
            "description":  desc,
            "obtainSource": source,
            "howToObtain":  "Atom Shop",
            "dropRate":     "—",
            "seasonNumber": None,
            "tradeable":    True,
            "imageUrl":     image_url,
            "imageCarousel": carousel,
            "xalgFlags":    furn.get("XALG_Flags", ""),
            "cutContent":   is_cut(entm.get("EDID", "")),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# CAMP ALLIES
# ---------------------------------------------------------------------------
# Sourced from COBJ list. Two types:
#   1. Quest/companion allies — FURN in Babylon_WorkshopBlacklist (no XALG)
#   2. Premium/scoreboard allies — XALG = Premium

# Known ally COBJ → FURN mapping (from session research)
ALLY_COBJ_FURN = {
    # Quest/companion placed (no plan)
    "0054EB64": {"furn": "0055F6D2", "name": "U.S.S.A. Console",       "obtain": "Companion Quest"},
    "00568E4E": {"furn": "0056FBA4", "name": "Raider Punk's Radio",     "obtain": "Companion Quest"},
    "00569CC1": {"furn": "0057469B", "name": "Beckett's Bar",           "obtain": "Companion Quest"},
    "00585CDB": {"furn": "00585CE0", "name": "Forager's Chair",         "obtain": "Companion Quest"},
    "00585CC2": {"furn": "005856C7", "name": "Wanderer's Guitar",       "obtain": "Companion Quest"},
    "0061E077": {"furn": "0061E042", "name": "Sam's Workbench",         "obtain": "Companion Quest"},
    # Premium / Scoreboard
    "005C60D0": {"furn": "005C4208", "name": "Solomon's Medic Station", "obtain": "Atom Shop"},
    "005DBE64": {"furn": "005EED4D", "name": "Yasmin's Cooking Stove",  "obtain": "Atom Shop"},
    "0061E078": {"furn": "0063164E", "name": "Daphne's Toy Box",        "obtain": "Scoreboard"},
    "0061E079": {"furn": "0061F6F3", "name": "Maul's Cauldron",         "obtain": "Scoreboard"},
    "0062F75B": {"furn": "0062F75A", "name": "Xerxo's Spaceship",       "obtain": "Atom Shop"},
    "0063164B": {"furn": "0063164E", "name": "Katherine's Research Desk","obtain": "Atom Shop"},
    "00674961": {"furn": "0067E70B", "name": "Leo's Desk",              "obtain": "Scoreboard"},
    "0068D3D2": {"furn": "0068D3D5", "name": "Scarberry's Shrine",      "obtain": "Scoreboard"},
    "006A4360": {"furn": "006A4363", "name": "Joey's Stage",            "obtain": "Scoreboard"},
    "006DC965": {"furn": "006DC969", "name": "Grandma's Chair",         "obtain": "Scoreboard"},
    "0073506D": {"furn": "0073507C", "name": "Del Lawson's Squire Bag", "obtain": "Atom Shop"},
    "0073E940": {"furn": "0073E943", "name": "Adelaide's Table",        "obtain": "Scoreboard"},
    "007FDC17": {"furn": "007FDC1B", "name": "Dottie's Strange Boxes",  "obtain": "Atom Shop"},
}

# ENTM → COBJ link: scan ENTM ECIL cols for our COBJ FormIDs
_ally_entm_cache = {}

def find_entm_for_cobj_id(cobj_id):
    if cobj_id in _ally_entm_cache:
        return _ally_entm_cache[cobj_id]
    for entm in entm_rows:
        count = int(entm.get("ECIL_Count", 0) or 0)
        for i in range(1, count + 1):
            if entm.get(f"ECIL_{i}", "").strip() == cobj_id:
                _ally_entm_cache[cobj_id] = entm
                return entm
    _ally_entm_cache[cobj_id] = None
    return None


def build_allies():
    items = []
    for cobj_id, meta in ALLY_COBJ_FURN.items():
        furn_id  = meta["furn"]
        furn     = furn_by_id.get(furn_id, {})
        entm     = find_entm_for_cobj_id(cobj_id)

        display  = meta["name"]
        obtain   = meta["obtain"]
        source   = obtain

        desc     = clean_desc(entm.get("DESC", "")) if entm else ""
        entm_id  = entm["FormID"] if entm else ""
        carousel = ecil_images(entm, "camp-allies") if entm else []
        img      = carousel[0] if carousel else ""

        # Use FURN XALG to refine source if available
        xalg = furn.get("XALG_Flags", "")
        if xalg:
            source = xalg_to_source(xalg) or source

        items.append({
            "formId":           furn_id or cobj_id,
            "cobjFormId":       cobj_id,
            "entmFormId":       entm_id,
            "furnFormId":       furn_id,
            "edid":             furn.get("FURN_EDID", ""),
            "displayName":      display,
            "description":      desc,
            "obtainSource":     source,
            "howToObtain":      obtain,
            "dropRate":         "—",
            "seasonNumber":     None,
            "tradeable":        (source != "Scoreboard"),
            "imageUrl":         img,
            "imageCarousel":    carousel,
            "xalgFlags":        xalg,
            "buffsAndFunctions": "",   # Populated manually in the JSON
            "inventory":        "",    # Populated manually in the JSON
            "cutContent":       is_cut(furn.get("FURN_EDID", "")),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# CAMP PETS
# ---------------------------------------------------------------------------
# 18 pet spawn furniture records from CAMPPets_SpawnFurniture* FURNs
# Each pet is a FURN (spawn bed/house) + associated ENTM

# Known pet spawn FURN → ENTM mapping patterns (EDID suffix matching)
# FURN EDID: CAMPPets_SpawnFurniture_Cat_Bed_GreyTabby
# ENTM EDID: ATX_ENTM_CAMP_Pets_SpawnFurniture_Cat_Bed_GreyTabby (hypothetical)
# We'll match by collecting all FURN rows with CAMPPets_SpawnFurniture in EDID

PET_ANIMAL_MAP = {
    "_cat_": "cat", "_cats_": "cat",
    "_dog_": "dog", "_dogs_": "dog",
    "_radhog_": "radhog", "_radhogs_": "radhog",
}


def animal_from_edid(edid):
    s = str(edid or "").lower()
    for k, v in PET_ANIMAL_MAP.items():
        if k in s:
            return v
    # Fallback for Rooter/Mongrel
    if "mongrel" in s: return "dog"
    if "rooter"  in s: return "radhog"
    return "other"


def build_pets():
    # Get all spawn FURN records
    spawn_furns = [
        r for r in furn_rows
        if "campPets_SpawnFurniture".lower() in r.get("FURN_EDID", "").lower()
        or "CAMPPets_SpawnFurniture" in r.get("FURN_EDID", "")
    ]

    items = []
    for furn in spawn_furns:
        furn_id   = furn["FURN_FormID"]
        furn_edid = furn["FURN_EDID"]
        furn_full = furn.get("FURN_FULL", "")
        xalg      = furn.get("XALG_Flags", "")
        source    = xalg_to_source(xalg) or "Atom Shop"
        animal    = animal_from_edid(furn_edid)

        # Try to find ENTM for this FURN
        entm = find_entm_for_cobj_id(furn_id)  # reuse ECIL scanner
        # Also try direct EDID-based lookup
        if not entm:
            suffix = re.sub(r"^(?:SCORE_S\d+_)?CAMPPets_SpawnFurniture_", "", furn_edid, flags=re.IGNORECASE)
            for candidate in entm_rows:
                cedid = candidate.get("EDID", "")
                if suffix.lower() in cedid.lower() and "pets" in cedid.lower():
                    entm = candidate
                    break

        desc      = clean_desc(entm.get("DESC", "")) if entm else ""
        entm_id   = entm["FormID"] if entm else ""
        entm_full = entm.get("FULL", "").strip() if entm else ""
        carousel  = ecil_images(entm, "camp-pets") if entm else []

        # Pet image = first carousel (the actual pet), Home image = second (bed/house)
        pet_img  = carousel[0] if len(carousel) > 0 else ""
        home_img = carousel[1] if len(carousel) > 1 else ""

        # Season number from FURN EDID (e.g. SCORE_S19_CAMPPets_...)
        season_m   = re.match(r"SCORE_S(\d+)_", furn_edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None

        # Scoreboard items (SCORE_*) are non-tradeable; ATX_ Atom Shop items are tradeable
        if season_num:
            source    = "Scoreboard"
            how       = f"Season {season_num} Scoreboard"
            tradeable = False
        else:
            how       = source  # "Atom Shop"
            tradeable = True

        items.append({
            "formId":       furn_id,
            "entmFormId":   entm_id,
            "edid":         furn_edid,
            # displayName = the actual pet name (ENTM FULL), e.g. "German Shepherd"
            "displayName":  entm_full or furn_full or furn_edid,
            # homeName = the furniture item name (FURN FULL), e.g. "German Shepherd House"
            "homeName":     furn_full,
            "description":  desc,
            "animalType":   animal,
            "obtainSource": source,
            "howToObtain":  how,
            "dropRate":     "1–3 Star Legendary",
            "seasonNumber": season_num,
            "tradeable":    tradeable,
            "imageUrl":     pet_img,
            "homeImageUrl": home_img,
            "imageCarousel": carousel,
            "xalgFlags":    xalg,
            "cutContent":   is_cut(furn_edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# PET FURNITURE
# ---------------------------------------------------------------------------
# Idle furniture (standalone buyable items): FURN EDID contains CAMPPets_IdleFurniture

def build_pet_furniture():
    idle_furns = [
        r for r in furn_rows
        if "campPets_IdleFurniture".lower() in r.get("FURN_EDID", "").lower()
        or "CAMPPets_IdleFurniture" in r.get("FURN_EDID", "")
    ]

    items = []
    for furn in idle_furns:
        furn_id   = furn["FURN_FormID"]
        furn_edid = furn["FURN_EDID"]
        furn_full = furn.get("FURN_FULL", "")
        xalg      = furn.get("XALG_Flags", "")
        source    = xalg_to_source(xalg) or "Atom Shop"
        animal    = animal_from_edid(furn_edid)

        entm = find_entm_for_cobj_id(furn_id)
        if not entm:
            suffix = re.sub(r"^(?:SCORE_S\d+_)?CAMPPets_IdleFurniture_", "", furn_edid, flags=re.IGNORECASE)
            for candidate in entm_rows:
                cedid = candidate.get("EDID", "")
                if suffix.lower() in cedid.lower() and "pets" in cedid.lower():
                    entm = candidate
                    break

        desc      = clean_desc(entm.get("DESC", "")) if entm else ""
        entm_id   = entm["FormID"] if entm else ""
        entm_full = entm.get("FULL", "").strip() if entm else ""
        carousel  = ecil_images(entm, "camp-pets") if entm else []
        img       = carousel[0] if carousel else ""

        season_m   = re.match(r"SCORE_S(\d+)_", furn_edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source    = "Scoreboard"
            how       = f"Season {season_num} Scoreboard"
            tradeable = False
        else:
            how       = source
            tradeable = True

        items.append({
            "formId":       furn_id,
            "entmFormId":   entm_id,
            "edid":         furn_edid,
            "displayName":  entm_full or furn_full or furn_edid,
            "description":  desc,
            "animalType":   animal,
            "obtainSource": source,
            "howToObtain":  how,
            "dropRate":     "—",
            "seasonNumber": season_num,
            "tradeable":    tradeable,
            "imageUrl":     img,
            "imageCarousel": carousel,
            "xalgFlags":    xalg,
            "cutContent":   is_cut(furn_edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# PET APPAREL
# ---------------------------------------------------------------------------
# 9 COBJ items crafted at Armor Workbench

PET_APPAREL_ENTM_IDS = [
    "0078B503",  # Red Bow Collar (Cat)
    "0078B502",  # Red Bow Collar (Dog)
    "007B285E",  # Rusted Chain Collar (Dog)
    "007DBEF0",  # Leather Collar (Cat)
    "00840E0F",  # Responders Bandana (Dog)
    "00852177",  # Nose Ring (Radhog)
    "00853B81",  # Rusty Nails Collar (Cat)
    "00853B82",  # Rusty Nails Collar (Dog)
    "00868F71",  # Sooie-Heart Ring (Radhog)
]

PET_APPAREL_ANIMAL = {
    "0078B503": "cat", "0078B502": "dog",
    "007B285E": "dog", "007DBEF0": "cat",
    "00840E0F": "dog", "00852177": "radhog",
    "00853B81": "cat", "00853B82": "dog",
    "00868F71": "radhog",
}


def build_pet_apparel():
    items = []
    for entm_id in PET_APPAREL_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        desc     = clean_desc(entm.get("DESC", ""))
        display  = entm.get("FULL", "")
        xalg     = entm.get("XALG", "")
        source   = xalg_to_source(xalg) or "Atom Shop"
        animal   = PET_APPAREL_ANIMAL.get(entm_id, "other")
        carousel = ecil_images(entm, "camp-pets")
        img      = carousel[0] if carousel else ""
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        # Apparel is craftable — non-tradeable if scoreboard, tradeable if ATX
        tradeable  = (season_num is None)

        items.append({
            "formId":       entm_id,
            "entmFormId":   entm_id,
            "edid":         edid,
            "displayName":  display,
            "description":  desc,
            "animalType":   animal,
            "obtainSource": source,
            "howToObtain":  "Craft at Armor Workbench",
            "dropRate":     "—",
            "seasonNumber": season_num,
            "tradeable":    tradeable,
            "imageUrl":     img,
            "imageCarousel": carousel,
            "xalgFlags":    xalg,
            "cutContent":   is_cut(edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# CRYOS
# ---------------------------------------------------------------------------

CRYO_ENTM_IDS = [
    "006C2F42",  # Military Cryo-Freezer
    "00773E30",  # Nuka-Cola Cryo-Freezer
    "007D70D2",  # Fishing Cooler Cryo Freezer
]

CRYO_GOLDVENDOR = {
    "006C2F42": "00732A95",
}


def build_cryos():
    items = []
    for entm_id in CRYO_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        desc     = clean_desc(entm.get("DESC", ""))
        display  = entm.get("FULL", "")
        xalg     = entm.get("XALG", "")
        source   = xalg_to_source(xalg) or "Atom Shop"
        carousel = ecil_images(entm, "camp-utility")
        img      = carousel[0] if carousel else ""
        gv       = CRYO_GOLDVENDOR.get(entm_id, "")
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        how        = f"Season {season_num} Scoreboard" if season_num else ("Gold Bullion vendor" if gv else "Atom Shop")
        tradeable  = not bool(season_num)

        items.append({
            "formId":         entm_id,
            "entmFormId":     entm_id,
            "goldVendorFormId": gv,
            "edid":           edid,
            "displayName":    display,
            "description":    desc,
            "obtainSource":   "Scoreboard" if season_num else source,
            "howToObtain":    how,
            "dropRate":       "—",
            "seasonNumber":   season_num,
            "tradeable":      tradeable,
            "imageUrl":       img,
            "imageCarousel":  carousel,
            "spoilageReduction": "100% (no spoilage)",
            "xalgFlags":      xalg,
            "cutContent":     is_cut(edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# FRIDGES
# ---------------------------------------------------------------------------

FRIDGE_ENTM_IDS = [
    # Upright fridge skins
    "0056B910",  # Blood Spattered
    "005DBDBD",  # Beer Barrel
    "005F56CB",  # Vault-Tec
    "00692CCB",  # Meat Locker
    "006F7E74",  # Sugar Bombs
    "007703CC",  # Camping Cooler
    "007DC658",  # Gone Fission Camping Cooler
    "0082F826",  # Pink Modern Home
    # Chest/cooler style
    "005A18C6",  # The Cooler
    "005A1B8B",  # Nuka-Cola Cooler
    "005A3584",  # The Ice Box
    "005ADDA0",  # Red Rocket Cooler
    "0079A1CF",  # Nuka-Cola Quantum Cooler
]


def build_fridges():
    items = []
    for entm_id in FRIDGE_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        desc     = clean_desc(entm.get("DESC", ""))
        display  = entm.get("FULL", "")
        xalg     = entm.get("XALG", "")
        source   = xalg_to_source(xalg) or "Atom Shop"
        carousel = ecil_images(entm, "camp-utility")
        img      = carousel[0] if carousel else ""

        items.append({
            "formId":       entm_id,
            "entmFormId":   entm_id,
            "edid":         entm.get("EDID", ""),
            "displayName":  display,
            "description":  desc,
            "obtainSource": source,
            "howToObtain":  source,
            "dropRate":     "—",
            "seasonNumber": None,
            "tradeable":    True,
            "imageUrl":     img,
            "imageCarousel": carousel,
            "spoilageReduction": "-50%",
            "xalgFlags":    xalg,
            "cutContent":   is_cut(entm.get("EDID", "")),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# RUN ALL BUILDERS
# ---------------------------------------------------------------------------

print("\nBuilding JSON files…")

save_json("weather_stations.json", build_weather_stations())
save_json("repair-bots.json",      build_repair_bots())
save_json("allies.json",           build_allies())
save_json("pets.json",             build_pets())
save_json("pet-furniture.json",    build_pet_furniture())
save_json("pet-apparel.json",      build_pet_apparel())
save_json("cryos.json",            build_cryos())
save_json("fridges.json",          build_fridges())

print("\nDone.")
