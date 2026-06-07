#!/usr/bin/env python3
"""
build_camp_json.py
Reads ENTM_Export TSV → outputs dist/camp.json for the df-bnb-camp module.

Covers: camp-pets, camp-pet-furniture, camp-pet-apparel, weather-machines
All sourced from ENTM records; ACTI used only for fallback checks.

Run:  python3 build_camp_json.py
Output: ../dist/camp.json
"""

import csv, json, os, re, sys
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

# ─── PATHS ───────────────────────────────────────────────────────────────────

SCRIPT_DIR  = Path(__file__).resolve().parent
TSV_DIR     = SCRIPT_DIR.parent / "tsv"
DIST_DIR    = SCRIPT_DIR.parent / "dist"
DIST_DIR.mkdir(parents=True, exist_ok=True)

def latest_tsv(pattern):
    """Return the most recent TSV matching a glob pattern, or None."""
    matches = sorted(TSV_DIR.glob(pattern))
    return matches[-1] if matches else None

ENTM_TSV = latest_tsv("ENTM_Export*.tsv")
COBJ_TSV = latest_tsv("COBJ_Export*.tsv")

if not ENTM_TSV:
    print("[ERROR] No ENTM_Export*.tsv found in tsv/", file=sys.stderr)
    sys.exit(1)
if not COBJ_TSV:
    print("[ERROR] No COBJ_Export*.tsv found in tsv/", file=sys.stderr)
    sys.exit(1)

# ─── STOREFRONT CONFIG ───────────────────────────────────────────────────────

STOREFRONT = "/wp-content/uploads/storefront"

# pageType → subfolder for WebP images
FOLDER = {
    "camp-pets":           "camp-pets",
    "camp-pet-furniture":  "camp-pet-furniture",
    "camp-pet-apparel":    "camp-pet-apparel",
    "weather-machines":    "weather-machines",
}

# ─── SEASON NAMES ─────────────────────────────────────────────────────────────
# Update from fallout76_seasons.tsv as needed.
# Key = season number as string, value = season name for display.
SEASONS = {
    "15": "Expeditions: Atlantic City",
    "19": "Invaders from Beyond",
    "20": "Test Your Metal",
    "21": "Zorbo's Revenge",
    "22": "Boardwalk Paradise",
    "23": "Nuka-World on Tour",
    "24": "RIP Darling & The Cryptids from Beyond the Cosmos",
}

# ─── HARDCODED OBTAIN OVERRIDES (keyed by ENTM FormID) ───────────────────────
# Use for items whose obtain source can't be read from game data alone.
OBTAIN_OVERRIDES = {
    "0084132A": {  # Mongrel — Fallout 1st free claim
        "howToObtain": "Free to claim from the Atom Shop for Fallout 1st members.",
        "tradeable":   False,
    },
    "0073ABA6": {  # Atlantic City Fog — Gold Bullion
        "howToObtain": "Gold Bullion - Samuel - Cautious - 1250 Bullion",
        "tradeable":   True,
    },
}

# ─── WEATHER STATION BUILD INFO ───────────────────────────────────────────────
# VMAD properties (power data) are not exported in the current ACTI TSV.
# Hardcoded: all weather stations require 2 power, 1 per CAMP, 0 per workshop.
# NOTE: if VMAD export is added in a future xEdit run, replace this with dynamic lookup.
WEATHER_BUILD_INFO = (
    "Power Required: 2\n"
    "Build Limit per Camp: 1\n"
    "Build Limit per Workshop: 0\n"
    "Shares Limit With: All Weather Control Stations"
)

# ─── CUT CONTENT ─────────────────────────────────────────────────────────────

# "ZZZ" without the underscore too — Bethesda's deprecated records aren't
# consistent (e.g. zzzATX_ENTM_CAMP_Utility_WeatherStation_Falls_Leaves),
# and the old "ZZZ_" check let that one through as a duplicate Fall station.
CUT_PREFIXES = ("ZZZ", "DEL_", "CUT_", "POST_")

def is_cut(edid):
    u = str(edid or "").upper().strip()
    return any(u.startswith(p) for p in CUT_PREFIXES)

# ─── IMAGE URL ────────────────────────────────────────────────────────────────

def img_url(etdi, page_type):
    if not etdi or not etdi.strip():
        return ""
    name = os.path.splitext(os.path.basename(etdi.strip()))[0].lower()
    folder = FOLDER.get(page_type, page_type)
    return f"{STOREFRONT}/{folder}/{name}.webp"

# ─── CLEAN DESCRIPTION ───────────────────────────────────────────────────────

# Boilerplate suffixes in DESC that are noise for display
_BOILERPLATE = re.compile(
    r'\s*[-–—]\s*C\.?A\.?M\.?P\.?\s+(?:ITEMS|PETS)\s+APPEAR.*$|'
    r'\s*[-–—]\s*APPAREL\s+IS\s+CRAFTABLE.*$|'
    r'\s*[-–—]\s*CAMP\s+PET\s+CLOTHING.*$|'
    r'\s*\bThis\s+Weather\s+Control\s+Station\s+will\s+change.*$|'
    r'\s*\bWeather\s+Control\s+Station\s+will\s+change.*$',
    re.IGNORECASE | re.DOTALL
)

def clean_desc(raw):
    s = str(raw or "").strip()
    s = _BOILERPLATE.sub("", s).strip().strip("-–—").strip()
    return s

# ─── DISPLAY NAME ─────────────────────────────────────────────────────────────

def clean_name(full):
    """Strip trailing animal qualifier: 'Red Bow Collar (Dog)' → 'Red Bow Collar'"""
    s = str(full or "").strip()
    s = re.sub(r'\s*\([^)]+\)\s*$', '', s).strip()
    return s

def weather_friendly(full):
    """'Weather Control Station (Alien Invasion)' → 'Alien Invasion Weather Control Station'"""
    s = str(full or "").strip()
    m = re.search(r'\((.+?)\)\s*$', s)
    if m:
        skin = m.group(1).strip()
        base = re.sub(r'\s*\([^)]+\)\s*$', '', s).strip()
        return f"{skin} {base}"
    return s

# ─── PAGE CLASSIFICATION ─────────────────────────────────────────────────────

def classify_page(edid):
    e = edid.upper()
    if '_APPAREL_CAMPPETS_' in e:
        return 'camp-pet-apparel'
    if '_CAMP_CAMPPETS_EMOTE_' in e:
        return None  # skip emotes
    if '_CAMP_CAMPPETS_IDLEFURNITURE_' in e or '_CAMP_CAMPPETS_FURNITURE_' in e:
        return 'camp-pet-furniture'
    if '_CAMP_CAMPPETS_' in e:
        return 'camp-pets'
    if '_CAMP_UTILITY_WEATHERSTATION_' in e:
        return 'weather-machines'
    return None

# ─── ANIMAL TYPE ─────────────────────────────────────────────────────────────

def animal_type(edid):
    e = edid.upper()
    if '_DOG_' in e:   return 'Dog'
    if '_CAT_' in e or '_LYKOI' in e: return 'Cat'
    if '_RADHOG_' in e or 'RADHOG' in e or '_ROOTER' in e: return 'RadHog'
    return None

# ─── SEASON / OBTAIN SOURCE ───────────────────────────────────────────────────

def resolve_obtain(edid, form_id, xalg):
    """Returns (howToObtain, seasonNumber, tradeable)"""

    # Hardcoded override wins
    override = OBTAIN_OVERRIDES.get(form_id)
    if override:
        return (
            override.get("howToObtain", "—"),
            override.get("seasonNumber", None),
            override.get("tradeable", False),
        )

    # XALG 000000001 = account-bound entitlement
    non_tradeable = bool(xalg and xalg.strip() == "000000001")

    # Score / Scoreboard season
    m = re.match(r'^(?:ZZZ_)?SCORE_S(\d+)_', edid, re.IGNORECASE)
    if m:
        snum = m.group(1)
        sname = SEASONS.get(snum, "")
        if sname:
            how = f"Purchase with tickets from the {sname} Scoreboard (Season {snum})"
        else:
            how = f"Purchase with tickets from the Season {snum} Scoreboard (Season {snum})"
        return how, snum, False   # scoreboard = non-tradeable

    # ATX = Atom Shop
    if edid.upper().startswith("ATX_"):
        return (
            "Can be purchased separately or with certain bundles from the Atom Shop.",
            None,
            False,  # all camp ATX items non-tradeable (no plans that can be traded)
        )

    return "—", None, None

# ─── CRAFT INFO ───────────────────────────────────────────────────────────────

def parse_components(fvpa):
    """'Steel:3 | Cloth:2' → 'Steel x3, Cloth x2'"""
    # FVPA format changed Apr-2026: EDID:qty → EDID:qty:keyword_EDID
    # Split on ":" and take parts[0]=EDID, parts[1]=qty, ignore parts[2+]
    if not fvpa or not fvpa.strip():
        return ""
    parts = []
    for seg in fvpa.split("|"):
        seg = seg.strip()
        if ":" in seg:
            tokens = seg.split(":")
            mat = tokens[0].strip()
            qty = tokens[1].strip() if len(tokens) >= 2 else "1"
            parts.append(f"{mat} x{qty}")
        elif seg:
            parts.append(seg)
    return ", ".join(parts)

def craft_bench_from_desc(desc):
    d = str(desc or "").upper()
    if "ARMOR WORKBENCH" in d:
        return "Armor Workbench"
    return None

# ─── LOAD TSVs ────────────────────────────────────────────────────────────────

def load_tsv(path):
    rows = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                rows.append(row)
    except FileNotFoundError:
        print(f"[WARN] TSV not found: {path}", file=sys.stderr)
    return rows

# ─── MAIN BUILD ───────────────────────────────────────────────────────────────

def build():
    entm_rows = load_tsv(ENTM_TSV)
    cobj_rows = load_tsv(COBJ_TSV)

    # Build COBJ lookup: CNAM FormID → {fvpa, edid}
    # For apparel: COBJ links clothing item (CNAM) → recipe components (FVPA)
    cobj_by_cnam = {}
    for c in cobj_rows:
        cnam = c.get("CNAM_FormID", "").strip()
        if cnam:
            cobj_by_cnam[cnam] = c

    # Find apparel MOD/item FormIDs that link back to ENTM via COBJ
    # COBJ EDID for apparel: ATX_co_Clothes_CAMPPets_* or SCORE_*_co_Clothes_CAMPPets_*
    # These COBJ records have CNAM pointing to the clothing mod item, not the ENTM.
    # We'll index COBJ by EDID pattern for apparel component lookup.
    apparel_cobj = {}  # apparel COBJ EDIDs keyed by ENTM-derived token
    for c in cobj_rows:
        edid = c.get("COBJ_EDID", "").strip().strip('"')
        if re.search(r'co_Clothes_CAMPPets_', edid, re.IGNORECASE):
            fvpa = parse_components(c.get("FVPA", "").strip().strip('"'))
            # Store by EDID
            apparel_cobj[edid.lower()] = fvpa

    def get_apparel_components(entm_edid):
        """Match ENTM EDID to COBJ by common token"""
        # ENTM: ATX_ENTM_Apparel_CAMPPets_Dog_Neckwear_CollarRedBow
        # COBJ: ATX_co_Clothes_CAMPPets_Dog_Neckwear_CollarRedBow
        # Strip the ENTM/Apparel → co/Clothes and try to match
        token = re.sub(r'^(?:SCORE_S\d+_)?ATX_ENTM_Apparel_', '', entm_edid, flags=re.IGNORECASE)
        token = re.sub(r'^SCORE_S\d+_ENTM_Apparel_', '', token, flags=re.IGNORECASE)
        # token = "CAMPPets_Dog_Neckwear_CollarRedBow"
        for cobj_edid, fvpa in apparel_cobj.items():
            if token.lower() in cobj_edid:
                return fvpa
        return ""

    items = []

    for row in entm_rows:
        edid    = row.get("EDID", "").strip()
        form_id = row.get("FormID", "").strip()
        full    = row.get("FULL", "").strip()
        desc    = row.get("DESC", "").strip()
        etdi    = row.get("ETDI", "").strip()
        xalg    = row.get("XALG", "").strip()

        page_type = classify_page(edid)
        if page_type is None:
            continue

        # Skip placeholders
        if "DevOnly" in edid or "DoNotUse" in edid:
            continue
        # Skip blank-named items (no FULL)
        if not full and not is_cut(edid):
            continue

        cut = is_cut(edid)
        how, season_num, tradeable = resolve_obtain(edid, form_id, xalg)
        description = clean_desc(desc)
        image = img_url(etdi, page_type)

        item = {
            "formId":        form_id,
            "edid":          edid,
            "displayName":   full,          # raw name; JS cleans qualifier suffix
            "pageType":      page_type,
            "description":   description,
            "howToObtain":   how,
            "dropRate":      "N/A",
            "seasonNumber":  season_num,
            "tradeable":     tradeable,
            "imageUrl":      image,
            "craftBench":    None,
            "craftComponents": None,
            "cutContent":    cut,
        }

        if page_type == "camp-pets":
            item["animalType"] = animal_type(edid)

        elif page_type == "camp-pet-furniture":
            item["animalType"] = animal_type(edid)

        elif page_type == "camp-pet-apparel":
            item["animalType"] = animal_type(edid)
            item["craftBench"] = craft_bench_from_desc(desc)
            item["craftComponents"] = get_apparel_components(edid)

        elif page_type == "weather-machines":
            item["displayName"]  = weather_friendly(full)
            item["description"]  = description
            item["buildInfo"]    = WEATHER_BUILD_INFO
            item["craftBench"]   = "C.A.M.P. Workshop"
            item["craftComponents"] = "Circuitry x2, Rubber x2, Steel x4, Screw x2"

        items.append(item)

    # Sort: by pageType order, then alphabetically within
    PAGE_ORDER = ["camp-pets", "camp-pet-furniture", "camp-pet-apparel", "weather-machines"]
    items.sort(key=lambda x: (
        PAGE_ORDER.index(x["pageType"]) if x["pageType"] in PAGE_ORDER else 99,
        x["displayName"]
    ))

    out = {
        "_generated": __import__("datetime").date.today().isoformat(),
        "_category":  "CAMP",
        "_note":      "Built by build_camp_json.py from ENTM TSV exports.",
        "items":      items,
    }

    out_path = DIST_DIR / "camp.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    counts = {}
    for it in items:
        counts[it["pageType"]] = counts.get(it["pageType"], 0) + 1
    print(f"[camp] Written {len(items)} items → {out_path}")
    for pt, n in sorted(counts.items()):
        print(f"  {pt}: {n}")

    # Note: patchlog feed for camp items is written by build_allies_pets_weather_json.py
    # to patchlog_latest_df_camp.json. This script skips patchlog generation since
    # it's a partial view of the same data.

if __name__ == "__main__":
    build()
