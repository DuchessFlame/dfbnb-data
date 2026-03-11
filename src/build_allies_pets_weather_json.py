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
BOOK_PATH = _resolve_tsv("BOOK_TSV",         "BOOK_Export_*.tsv",         "BOOK_Export.tsv")

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
print(f"  BOOK: {BOOK_PATH}")

cobj_rows      = load_tsv(COBJ_PATH)
entm_rows      = load_tsv(ENTM_PATH)
furn_rows      = load_tsv(FURN_PATH)
flst_entries   = load_tsv(FLST_PATH)
book_rows      = load_tsv(BOOK_PATH)

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

# ---------------------------------------------------------------------------
# BOOK: tradeable-via-plan lookups
# A BOOK (plan) is non-tradeable if any of its ReferencedBy Ref columns
# contains "Untradable" or "Untradeable" — these are untradeable leveled lists
# (e.g. LL_DailyOps_Rewards_HighLVL_Chase_RareUntradable).
# ---------------------------------------------------------------------------

book_by_id: dict = {}
for r in book_rows:
    ref_vals = [r.get(f"Ref{i}", "") for i in range(1, 44)]
    is_untrad = any("Untradable" in v or "Untradeable" in v for v in ref_vals)
    book_by_id[r["FormID"]] = {
        "full":          r.get("FULL", ""),
        "edid":          r.get("EDID", ""),
        "is_untradeable": is_untrad,
    }

# COBJ: CNAM FormID → {gnam_fid, gnam_full} (direct crafted-item → plan-book)
# Only rows with a non-empty GNAM_FormID are stored.
cobj_gnam_by_cnam: dict = {}
# CondProxy COBJs (used by GoldVendor / scoreboard unlock proxy):
#   EDID: SCORE_workshop_CondProxy_co_Category<Type>_<NameToken>_GoldVendor
# Indexed by lower-case <NameToken> stripped of the GoldVendor suffix.
condproxy_gnam_by_token: dict = {}

for r in cobj_rows:
    gnam = r.get("GNAM_FormID", "").strip().strip('"')
    if not gnam:
        continue
    gnam_full = r.get("GNAM_FULL", "").strip().strip('"')
    cnam = r.get("CNAM_FormID", "").strip().strip('"')
    cobj_edid = r.get("COBJ_EDID", "").strip().strip('"')

    if cnam:
        cobj_gnam_by_cnam.setdefault(cnam, {"gnam_fid": gnam, "gnam_full": gnam_full})

    if "CondProxy" in cobj_edid:
        token = re.sub(
            r"^.*?CondProxy_co_Category\w+_", "", cobj_edid, flags=re.IGNORECASE
        )
        token = re.sub(r"_GoldVendor.*$", "", token, flags=re.IGNORECASE).lower()
        if token:
            condproxy_gnam_by_token.setdefault(token, {"gnam_fid": gnam, "gnam_full": gnam_full})


def plan_for_cnam(cnam_id: str) -> tuple[str, str]:
    """Return (gnam_fid, gnam_full) for a craftable-item FormID, or ('','')."""
    r = cobj_gnam_by_cnam.get(cnam_id, {})
    return r.get("gnam_fid", ""), r.get("gnam_full", "")


def plan_for_condproxy_token(edid_hint: str) -> tuple[str, str]:
    """
    Return (gnam_fid, gnam_full) by matching the EDID hint against
    CondProxy name tokens.  Tries progressively shorter suffix matches.
    """
    key = edid_hint.lower()
    # Direct lookup
    if key in condproxy_gnam_by_token:
        r = condproxy_gnam_by_token[key]
        return r["gnam_fid"], r["gnam_full"]
    # Substring search (e.g. "acboardwalk" inside a longer token)
    for token, r in condproxy_gnam_by_token.items():
        if key and key in token:
            return r["gnam_fid"], r["gnam_full"]
    return "", ""


def tradeable_from_plan(gnam_fid: str) -> bool:
    """True only if a plan book exists AND is not in an untradeable leveled list."""
    if not gnam_fid:
        return False
    return not book_by_id.get(gnam_fid, {}).get("is_untradeable", True)


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
    """Build weather station list directly from ENTM records (no FLST needed)."""
    # Atlantic City Fog has a Gold Bullion plan but the CondProxy token regex
    # mislabels it — hardcode tradeable status here keyed by ENTM FormID.
    WEATHER_TRADEABLE = {
        "0073ABA6": True,   # Weather Control Station (Atlantic City Fog) — Gold Bullion plan
    }

    items = []

    for entm in entm_rows:
        edid = entm.get("EDID", "")
        if "WeatherStation" not in edid:
            continue
        if is_cut(edid):
            continue

        entm_id   = entm["FormID"]
        display   = entm.get("FULL", "").strip() or edid
        nnam      = entm.get("NNAM", "").strip()
        desc      = clean_desc(entm.get("DESC", ""))
        xalg_flag = entm.get("XALG", "")
        source    = xalg_to_source(xalg_flag) or "Atom Shop"

        # Use ETDI for the icon image (not ECIL_1 which has _C1 suffix)
        _etdi     = entm.get("ETDI", "").strip()
        carousel  = ecil_images(entm, "camp-utility")
        image_url = storefront_img_url(_etdi, "camp-utility") if _etdi else (carousel[0] if carousel else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"

        # Tradeable: hardcoded map wins, then CondProxy plan lookup fallback
        if entm_id in WEATHER_TRADEABLE:
            _ws_tradeable = WEATHER_TRADEABLE[entm_id]
            _ws_plan_name = "Plan: Weather Control Station (Atlantic City Fog)" if entm_id == "0073ABA6" else ""
        else:
            _ws_edid_key = re.sub(
                r"^(?:SCORE_S\d+_)?(?:ATX_|SCORE_)?ENTM_CAMP_Utility_WeatherStation_",
                "", edid, flags=re.IGNORECASE
            ).lower()
            _ws_gnam_fid, _ws_plan_name = plan_for_condproxy_token(_ws_edid_key)
            if not _ws_gnam_fid:
                _ws_gnam_fid, _ws_plan_name = plan_for_condproxy_token(
                    re.sub(r"^(?:SCORE_S\d+_)?(?:ATX_|SCORE_)?ENTM_", "", edid, flags=re.IGNORECASE).lower()
                )
            _ws_tradeable = tradeable_from_plan(_ws_gnam_fid)

        items.append({
            "formId":       entm_id,
            "entmFormId":   entm_id,
            "edid":         edid,
            "displayName":  display,
            "shortName":    nnam,
            "description":  desc,
            "obtainSource": source,
            "seasonNumber": season_num,
            "howToObtain":  f"Season {season_num} Scoreboard" if season_num else
                            ("Gold Bullion Vendor" if entm_id == "0073ABA6" else "Atom Shop"),
            "dropRate":     "—",
            "tradeable":    _ws_tradeable,
            "planName":     _ws_plan_name,
            "imageUrl":     image_url,
            "imageCarousel": carousel,
            "buildInfo":    "Power Required: Yes\nShares Limit With: All Weather Control Stations",
            "cutContent":   False,
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# REPAIR BOTS
# ---------------------------------------------------------------------------
# Source: ENTM_Export_March_2026.tsv — all ATX_ENTM_CAMP_Utility_RepairBot_* records.
# All four skins are Atom Shop (XALG flag 000000001 = Premium/ATX).
# No base/default repair bot exists as a separate ENTM entry.
# Add new FormIDs here as new skins are released.

REPAIR_BOT_ENTM_IDS = [
    "007AE546",  # Enclave Repair Bot
    "0082BED5",  # Company Repair Bot
    "0084920E",  # Santa's Helper Repair Bot
    "008571F1",  # Emergency Technician Repair Bot
]

# ENTM FormID → FURN FormID.
# Repair bot source (Atom Shop) is resolved directly from the ENTM XALG flag,
# so FURN lookup is not required for source detection. The dict is kept for
# completeness in case FURN records are needed in future (e.g. for plan FormIDs).
REPAIR_BOT_FURN_IDS: dict = {}


def build_repair_bots():
    items = []
    for entm_id in REPAIR_BOT_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        # FURN lookup kept for forward-compat; currently no FURN IDs are mapped.
        furn_id  = REPAIR_BOT_FURN_IDS.get(entm_id, "")
        furn     = furn_by_id.get(furn_id, {})

        # Source: prefer FURN XALG_Flags if a FURN exists, otherwise fall back
        # to the ENTM XALG field. All current skins are Atom Shop.
        xalg_raw = furn.get("XALG_Flags", "") or entm.get("XALG", "")
        source   = xalg_to_source(xalg_raw) or "Atom Shop"

        # Season number from EDID (none currently, kept for future scoreboards)
        edid       = entm.get("EDID", "")
        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"

        desc      = clean_desc(entm.get("DESC", ""))
        display   = entm.get("FULL", "")
        _etdi     = entm.get("ETDI", "").strip()
        carousel  = ecil_images(entm, "camp-utility")
        image_url = storefront_img_url(_etdi, "camp-utility") if _etdi else (carousel[0] if carousel else "")

        items.append({
            "formId":       furn_id or entm_id,
            "entmFormId":   entm_id,
            "furnFormId":   furn_id,
            "edid":         edid,
            "furnEdid":     furn.get("FURN_EDID", ""),
            "displayName":  display,
            "description":  desc,
            "obtainSource": source,
            "howToObtain":  f"Season {season_num} Scoreboard" if season_num else "Atom Shop",
            "dropRate":     "—",
            "seasonNumber": season_num,
            "tradeable":    False,  # no plan book exists — account-bound CAMP skin
            "planName":     "",
            "imageUrl":     image_url,
            "imageCarousel": carousel,
            "xalgFlags":    xalg_raw,
            "cutContent":   is_cut(edid),
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
    "0062F75B": {"furn": "0062F75A", "name": "Xerxo's Spaceship",              "obtain": "Atom Shop"},
    # Katherine Swan: COBJ 0063164B, FURN 0063164E (ATX_CAMP_Astronomer_KatherineFurniture_CampObject)
    # ENTM 0062F44F — ETDI base "SCORE_S7_CAMP_Ally_KatherineSwan" matches FURN prefix OK
    "0063164B": {"furn": "0063164E", "name": "Katherine's Research Desk",      "obtain": "Atom Shop"},
    # Leo Petrov: FURN EDID "SCORE_S11_CAMP_Ally_NukaAgent_Leo_Desk_FURN"
    # ENTM ETDI "SCORE_S11_CAMP_Ally_LeoPetrov.dds" — prefix mismatch, use entm_override
    "00674961": {"furn": "0067E70B", "name": "Leo's Desk",                     "obtain": "Scoreboard",
                 "entm_override": "00674962"},
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
    """Find ENTM by checking if cobj_id appears in ECIL fields (works for pets/furniture)."""
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


# ---------------------------------------------------------------------------
# Ally ENTM lookup: match by ETDI prefix vs FURN EDID
# ENTM ETDI = "SCORE_S16_CAMP_Ally_Adelaide.dds"
# FURN EDID = "SCORE_S16_CAMP_Ally_Adelaide_Table_FURN"
# The ETDI base (without .dds) is a leading prefix of the FURN EDID (case-insensitive).
# ---------------------------------------------------------------------------
_ally_entm_by_etdi: dict = {}
for _entm in entm_rows:
    _etdi = _entm.get("ETDI", "").strip()
    if _etdi and "CAMP_Ally" in _entm.get("EDID", ""):
        _base = re.sub(r"\.dds$", "", _etdi, flags=re.IGNORECASE).lower()
        _ally_entm_by_etdi[_base] = _entm


def find_ally_entm_for_furn(furn_edid: str):
    """Find ally ENTM by matching ETDI base as prefix of the FURN EDID."""
    key = furn_edid.lower()
    # Try progressively shorter prefixes of the furn edid to find a match
    for etdi_base, entm in _ally_entm_by_etdi.items():
        if key.startswith(etdi_base):
            return entm
    return None


def build_allies():
    items = []
    for cobj_id, meta in ALLY_COBJ_FURN.items():
        furn_id  = meta["furn"]
        furn     = furn_by_id.get(furn_id, {})

        # Resolve ENTM: entm_override wins (for items like Leo where ETDI≠FURN prefix)
        # then ETDI-prefix match, then ECIL scan fallback
        entm_override_id = meta.get("entm_override", "")
        if entm_override_id:
            entm = entm_by_id.get(entm_override_id, {})
        else:
            furn_edid_val = furn.get("FURN_EDID", "")
            entm = find_ally_entm_for_furn(furn_edid_val) if furn_edid_val else None
            if not entm:
                entm = find_entm_for_cobj_id(cobj_id)

        display  = meta["name"]
        obtain   = meta["obtain"]
        source   = obtain

        desc     = clean_desc(entm.get("DESC", "")) if entm else ""
        entm_id  = entm["FormID"] if entm else ""

        # Use ETDI for the primary icon image (not ECIL_1 which has _C1 suffix)
        _etdi    = entm.get("ETDI", "").strip() if entm else ""
        carousel = ecil_images(entm, "camp-allies") if entm else []
        img      = storefront_img_url(_etdi, "camp-allies") if _etdi else (carousel[0] if carousel else "")

        # Use FURN XALG to refine source if available
        xalg = furn.get("XALG_Flags", "")
        if xalg:
            source = xalg_to_source(xalg) or source

        # Season from FURN or ENTM EDID
        _ally_furn_edid = furn.get("FURN_EDID", "")
        _season_edid    = _ally_furn_edid or (entm.get("EDID", "") if entm else "")
        season_m        = re.match(r"SCORE_S(\d+)_", _season_edid, re.IGNORECASE)
        season_num      = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
            obtain = f"Season {season_num} Scoreboard"

        # Tradeable via plan
        _ally_token = re.sub(
            r"^(?:SCORE_S\d+_)?(?:ATX_|SCORE_)?CAMP_(?:Ally_)?", "",
            _ally_furn_edid, flags=re.IGNORECASE
        )
        _ally_token = re.sub(r"_(?:FURN|Table|Chair|Workbench|Bar|Stage|Shrine|Console|"
                             r"Radio|Guitar|Stove|Box|Boxes|Bag|Cauldron|Spaceship|Desk).*$",
                             "", _ally_token, flags=re.IGNORECASE).lower().replace("_", "")
        _ally_gnam_fid, _ally_plan_name = plan_for_condproxy_token(_ally_token)
        _ally_tradeable = tradeable_from_plan(_ally_gnam_fid)

        items.append({
            "formId":           furn_id or cobj_id,
            "cobjFormId":       cobj_id,
            "entmFormId":       entm_id,
            "furnFormId":       furn_id,
            "edid":             _ally_furn_edid,
            "displayName":      display,
            "description":      desc,
            "obtainSource":     source,
            "howToObtain":      obtain,
            "dropRate":         "—",
            "seasonNumber":     season_num,
            "tradeable":        _ally_tradeable,
            "planName":         _ally_plan_name,
            "imageUrl":         img,
            "imageCarousel":    carousel,
            "xalgFlags":        xalg,
            "buffsAndFunctions": "",
            "inventory":        "",
            "cutContent":       is_cut(_ally_furn_edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# CAMP PETS
# ---------------------------------------------------------------------------
# Source of truth: KYWD_Export_March_2026_Refs.tsv
# Per-pet keywords give exact FURN FormIDs; ENTM FormIDs confirmed by ENTM export.
#
# FURN = the spawn bed/house/kennel placed in CAMP (workshop furniture item)
# ENTM = the storefront entry that carries displayName, description, images
# NPC_ = the actual pet actor (lives in game world, not used in JSON)

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
    if "mongrel" in s: return "dog"
    if "rooter"  in s: return "radhog"
    return "other"


# ---------------------------------------------------------------------------
# Authoritative FURN → ENTM map for spawn pets.
# Derived from per-pet KYWD refs (CampPets_*, ATX_CAMPPets_*, SCORE_*).
# Regex EDID matching is unreliable — many FURNs have no bed-type segment
# (e.g. SpawnFurniture_Cat_RoboPaw, SpawnFurniture_Cat_Lykoi,
#        SpawnFurniture_Dog_MongrelDogHouse) which breaks suffix extraction.
# ---------------------------------------------------------------------------
SPAWN_FURN_TO_ENTM = {
    "0077D81F": "0078B500",  # Grey Tabby Cat     (cat)
    "0077D820": "0078B501",  # German Shepherd     (dog)
    "007A19B6": "007A19B5",  # White Shepherd      (dog)  S19 Scoreboard
    "007A19C5": "007A19C4",  # Bombay Cat          (cat)  S19 Scoreboard
    "007AE521": "007AE520",  # Sphynx Cat          (cat)
    "007B28F8": "007B28F7",  # Rottweiler          (dog)
    "007DC475": "007DC474",  # Wild Cat            (cat)
    "00804BAC": "00804BAA",  # Farm Cat            (cat)
    "0082A99E": "0082A99D",  # RoboPaw Steel Dog   (dog)
    "0082BCB6": "0082BCB4",  # RoboPaw Steel Cat   (cat)
    "008335D9": "008335DA",  # Sable Shepherd      (dog)  S22 Scoreboard
    "0083646D": "0083646C",  # Ragdoll Cat         (cat)  S22 Scoreboard
    "0084132B": "0084132A",  # Mongrel             (dog)
    "0084FB8B": "0084FB8A",  # Radhog              (radhog)
    "00853B83": "00853B80",  # Lykoi Cat           (cat)  S23 Scoreboard
    "0085B0CA": "0085B0C9",  # RoboPaw Blue Dog    (dog)
    "0089A8C5": "0089A8C4",  # Rooter Radhog       (radhog)
    "008A5DF5": "008A5DF4",  # Glowing Cat         (cat)  S24 Scoreboard
}

# ---------------------------------------------------------------------------
# Authoritative ENTM ID list for pet idle furniture.
# Source: KYWD ATX_Entitlement_Filter_Store_CAMP_Pet_Furniture (008538E5)
# plus RadHog-specific items confirmed from ENTM export.
# Switched to ENTM-first approach (same pattern as pet apparel) because
# some items lack or have inconsistent FURN EDID IdleFurniture patterns.
# ---------------------------------------------------------------------------
PET_FURNITURE_ENTM_IDS = [
    # Cat items
    "0078B4FE",  # Cat Tree
    "007A27AD",  # Catctus Scratching Post      (S19 Scoreboard)
    "007DC478",  # Knick Knack Table
    "0082EF12",  # Mushroom Scratching Post     (S22 Scoreboard)
    "00853B8A",  # Skeletal Scratching Post     (S23 Scoreboard)
    # Dog items
    "0078B4FF",  # Dog Dirt Pile
    "007A27AC",  # Dog Leaf Pile                (S19 Scoreboard)
    "007B290F",  # Mr. Fuzzy Chew Toy
    "007B2910",  # Junkyard Food Bowl
    "00804F59",  # Raider Skull Pile
    # Radhog items
    "00852172",  # Scratching Post (RadHog)
    "00853B8A",  # Skeletal Scratching Post — shared cat/radhog? keep once
    "00868F73",  # Feeding Trough (RadHog)
]
# Deduplicate while preserving order
PET_FURNITURE_ENTM_IDS = list(dict.fromkeys(PET_FURNITURE_ENTM_IDS))

# Animal type for each furniture ENTM (for grouping on page)
PET_FURNITURE_ANIMAL = {
    "0078B4FE": "cat",
    "007A27AD": "cat",
    "007DC478": "cat",
    "0082EF12": "cat",
    "00853B8A": "cat",
    "0078B4FF": "dog",
    "007A27AC": "dog",
    "007B290F": "dog",
    "007B2910": "dog",
    "00804F59": "dog",
    "00852172": "radhog",
    "00868F73": "radhog",
}


def build_pets():
    items = []
    for furn_id, entm_id in SPAWN_FURN_TO_ENTM.items():
        # Look up FURN record
        furn     = next((r for r in furn_rows if r.get("FURN_FormID") == furn_id), {})
        furn_edid = furn.get("FURN_EDID", "")
        furn_full = furn.get("FURN_FULL", "")
        xalg      = furn.get("XALG_Flags", "")
        source    = xalg_to_source(xalg) or "Atom Shop"
        animal    = animal_from_edid(furn_edid or entm_id)

        # Look up ENTM record
        entm      = entm_by_id.get(entm_id, {})
        desc      = clean_desc(entm.get("DESC", ""))
        entm_full = entm.get("FULL", "").strip()
        carousel  = ecil_images(entm, "camp-pets") if entm else []

        # Use ETDI for primary pet image, second carousel image for bed/home
        _etdi    = entm.get("ETDI", "").strip() if entm else ""
        pet_img  = storefront_img_url(_etdi, "camp-pets") if _etdi else (carousel[0] if carousel else "")
        home_img = carousel[1] if len(carousel) > 1 else (carousel[0] if carousel else "")

        # Season number: check FURN EDID first, then ENTM EDID
        season_m   = re.match(r"SCORE_S(\d+)_", furn_edid or entm.get("EDID", ""), re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None

        if season_num:
            source = "Scoreboard"
            how    = f"Season {season_num} Scoreboard"
        else:
            how    = source

        items.append({
            "formId":        furn_id,
            "entmFormId":    entm_id,
            "edid":          furn_edid,
            "displayName":   entm_full or furn_full or furn_edid,
            "homeName":      furn_full,
            "description":   desc,
            "animalType":    animal,
            "obtainSource":  source,
            "howToObtain":   how,
            "dropRate":      "—",
            "seasonNumber":  season_num,
            "tradeable":     False,
            "planName":      "",
            "imageUrl":      pet_img,
            "homeImageUrl":  home_img,
            "imageCarousel": carousel,
            "xalgFlags":     xalg,
            "cutContent":    is_cut(furn_edid),
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# PET FURNITURE
# ---------------------------------------------------------------------------
# Idle furniture items. Sourced from PET_FURNITURE_ENTM_IDS hardcoded list
# (derived from KYWD ATX_Entitlement_Filter_Store_CAMP_Pet_Furniture + RadHog extras).
# Uses ENTM-first approach (same as pet apparel) for reliable lookup.

def build_pet_furniture():
    items = []
    for entm_id in PET_FURNITURE_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        edid      = entm.get("EDID", "")
        desc      = clean_desc(entm.get("DESC", ""))
        display   = entm.get("FULL", "")
        xalg      = entm.get("XALG", "")
        source    = xalg_to_source(xalg) or "Atom Shop"
        animal    = PET_FURNITURE_ANIMAL.get(entm_id, animal_from_edid(edid))
        carousel  = ecil_images(entm, "camp-pets")

        # Use ETDI for primary image (icon), ECIL for carousel
        _etdi = entm.get("ETDI", "").strip()
        img   = storefront_img_url(_etdi, "camp-pets") if _etdi else (carousel[0] if carousel else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
            how    = f"Season {season_num} Scoreboard"
        else:
            how    = source

        items.append({
            "formId":        entm_id,
            "entmFormId":    entm_id,
            "edid":          edid,
            "displayName":   display,
            "description":   desc,
            "animalType":    animal,
            "obtainSource":  source,
            "howToObtain":   how,
            "dropRate":      "—",
            "seasonNumber":  season_num,
            "tradeable":     False,
            "planName":      "",
            "imageUrl":      img,
            "imageCarousel": carousel,
            "xalgFlags":     xalg,
            "cutContent":    is_cut(edid),
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
        # Pet apparel imageUrl uses ETDI (the icon .dds), NOT the ECIL_1 carousel image.
        # ECIL_1 has a _C1 suffix which maps to a different file than the uploaded texture icon.
        _etdi    = entm.get("ETDI", "").strip()
        img      = storefront_img_url(_etdi, "camp-pets") if _etdi else (carousel[0] if carousel else "")
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        # Apparel is craftable — non-tradeable if scoreboard, tradeable if ATX
        tradeable  = (season_num is None)

        # Tradeable via plan: look up the ARMO CNAM for this ENTM via COBJ EDID suffix,
        # then check if that COBJ has a GNAM plan book.
        # All current pet apparel items have no COBJ GNAM → non-tradeable.
        _ap_edid_suffix = re.sub(
            r"^(?:SCORE_S\d+_)?(?:ATX_|SCORE_)?ENTM_(?:Apparel_)?",
            "", edid, flags=re.IGNORECASE
        ).lower()
        _ap_gnam_fid, _ap_plan_name = plan_for_condproxy_token(_ap_edid_suffix)
        _ap_tradeable = tradeable_from_plan(_ap_gnam_fid)

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
            "tradeable":    _ap_tradeable,  # False for all current items (no plan books exist)
            "planName":     _ap_plan_name,
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
        _etdi    = entm.get("ETDI", "").strip()
        carousel = ecil_images(entm, "camp-utility")
        img      = storefront_img_url(_etdi, "camp-utility") if _etdi else (carousel[0] if carousel else "")
        gv       = CRYO_GOLDVENDOR.get(entm_id, "")
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        how        = f"Season {season_num} Scoreboard" if season_num else ("Gold Bullion vendor" if gv else "Atom Shop")
        tradeable  = not bool(season_num)

        # Tradeable via plan: match ENTM EDID suffix against CondProxy tokens
        _cryo_key = re.sub(
            r"^(?:SCORE_S\d+_)?(?:ATX_|SCORE_)?ENTM_CAMP_Utility_",
            "", edid, flags=re.IGNORECASE
        ).lower()
        _cryo_gnam_fid, _cryo_plan_name = plan_for_condproxy_token(_cryo_key)
        _cryo_tradeable = tradeable_from_plan(_cryo_gnam_fid)

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
            "tradeable":      _cryo_tradeable,
            "planName":       _cryo_plan_name,
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
    # ── Upright refrigerators ──
    "0055FA66",  # Refrigerator (base)
    "0055FA5F",  # Bloody Arktos Refrigerator
    "0055FA60",  # Nuka-Cola Refrigerator
    "0055FA61",  # Stainless Steel Refrigerator
    "0056B910",  # Blood Spattered Refrigerator
    "005DBDBD",  # Beer Barrel Fridge
    "005F56CB",  # Vault-Tec Refrigerator
    "00692CCB",  # The Meat Locker
    "006F7E74",  # Sugar Bombs Refrigerator
    "007703CC",  # Camping Cooler           (S17 Scoreboard)
    "007DC658",  # Gone Fission Camping Cooler (S21 Scoreboard)
    "0082F826",  # Pink Modern Home Refrigerator (S22 Scoreboard)
    # ── Chest / cooler style (50% spoilage) ──
    "005A18C6",  # The Cooler
    "005A1B8B",  # Nuka-Cola Cooler
    "005A3584",  # The Ice Box
    "005ADDA0",  # Red Rocket Cooler
    "0079A1CF",  # Nuka-Cola Quantum Cooler
    # ── Beer kegs / dispensers ──
    "0058BCF9",  # Beer Keg
    "00773E31",  # Beer Mystery Machine
    "00795285",  # Blue Ridge Beer Keg Set  (S18 Scoreboard)
]


def build_fridges():
    items = []
    for entm_id in FRIDGE_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        edid     = entm.get("EDID", "")
        desc     = clean_desc(entm.get("DESC", ""))
        display  = entm.get("FULL", "")
        xalg     = entm.get("XALG", "")
        source   = xalg_to_source(xalg) or "Atom Shop"
        _etdi    = entm.get("ETDI", "").strip()
        carousel = ecil_images(entm, "camp-utility")
        img      = storefront_img_url(_etdi, "camp-utility") if _etdi else (carousel[0] if carousel else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
            how    = f"Season {season_num} Scoreboard"
        else:
            how = source

        items.append({
            "formId":        entm_id,
            "entmFormId":    entm_id,
            "edid":          edid,
            "displayName":   display,
            "description":   desc,
            "obtainSource":  source,
            "howToObtain":   how,
            "dropRate":      "—",
            "seasonNumber":  season_num,
            "tradeable":     False,
            "planName":      "",
            "imageUrl":      img,
            "imageCarousel": carousel,
            "spoilageReduction": "-50%",
            "xalgFlags":     xalg,
            "cutContent":    is_cut(edid),
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
