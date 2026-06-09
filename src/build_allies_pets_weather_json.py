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
  ACTI_Export_ACTI.tsv

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

from patchlog_utils import write_patchlog_feed

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


_MONTH_ORD = {"jan": 1, "feb": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
              "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8,
              "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def _tsv_date_key(path):
    """(year, month) from the TSV filename so the chronologically newest export
    wins — plain alphabetical sorting picks May/March over June."""
    bn = re.split(r"[\\/]", path)[-1]
    m = re.search(r"(Jan|Feb|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|Sep|Oct|Nov|Dec)[a-z]*[_-](\d{4})",
                  bn, re.IGNORECASE)
    if m:
        return (int(m.group(2)), _MONTH_ORD.get(m.group(1).lower(), 0))
    return (0, 0)


def _newest_glob(pattern, exclude_suffix=None):
    matches = glob.glob(pattern)
    if exclude_suffix:
        matches = [m for m in matches if not m.lower().endswith(exclude_suffix.lower())]
    if not matches:
        return None
    return sorted(matches, key=_tsv_date_key)[-1]


def _resolve_tsv(env_var, glob_pattern, fallback_name, exclude_suffix=None):
    """Resolve a TSV path via env var → glob → bare fallback."""
    v = os.environ.get(env_var, "").strip()
    if v and Path(v).exists():
        return Path(v)
    found = _newest_glob(str(TSV_DIR / glob_pattern), exclude_suffix=exclude_suffix)
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
BOOK_PATH = _resolve_tsv("BOOK_TSV",         "BOOK_Export_*.tsv",         "BOOK_Export.tsv",
                         exclude_suffix="_locations.tsv")
ACTI_PATH = _resolve_tsv("ACTI_TSV",         "ACTI_Export_*_ACTI.tsv",    "ACTI_Export_ACTI.tsv")
# WTHR TSV — optional. Only weather station fishing classification uses it;
# if absent the builder falls back to an empty classification.
try:
    WTHR_PATH = _resolve_tsv("WTHR_TSV",     "WTHR_Export_*_WTHR.tsv",    "WTHR_Export_WTHR.tsv")
except FileNotFoundError:
    WTHR_PATH = None

# NPC PRPS TSV — optional. Only repair bot NPC stats (Health/AP/Perception)
# use it; if absent the builder falls back to the hardcoded June 2026 values.
try:
    NPC_PRPS_PATH = _resolve_tsv("NPC_PRPS_TSV", "NPC_Export_*_PRPS.tsv", "NPC_Export_PRPS.tsv")
except FileNotFoundError:
    NPC_PRPS_PATH = None

# GLOB TSV — optional. Used to resolve gold-vendor plan prices
# (BOOK BVGO → Econ_GoldVendor_Tier_NN → FLTV). If absent, gold-vendor
# How to Obtain blocks fall back to omitting the price line.
try:
    GLOB_PATH = _resolve_tsv("GLOB_TSV", "GLOB_Export_*.tsv", "GLOB_Export.tsv")
except FileNotFoundError:
    GLOB_PATH = None

# Seasons TSV — optional, falls back gracefully if missing
_SEASONS_PATH = TSV_DIR / "fallout76_seasons.tsv"

# Build SeasonNumber -> SeasonName lookup
# e.g. {1: "The Legendary Run", 19: "The Distant Reborn", ...}
SEASON_NAMES: dict = {}
if _SEASONS_PATH.exists():
    with open(_SEASONS_PATH, encoding="utf-8", errors="replace", newline="") as _sf:
        for _row in csv.DictReader(_sf, delimiter="\t"):
            _num  = _row.get("SeasonNumber", "").strip()
            _name = _row.get("SeasonName",   "").strip()
            if _num.isdigit() and _name:
                SEASON_NAMES[int(_num)] = _name


def scoreboard_how(season_num):
    """Return the full howToObtain string for a scoreboard item."""
    name = SEASON_NAMES.get(season_num, "")
    # Reward model changed at Season 16 (Duel with the Devil, 26 Mar 2024):
    # S1-S15 was claim-as-you-rank; S16+ is the ticket/Season Pass system.
    verb = "Claim from" if (season_num or 0) <= 15 else "Purchase with tickets from"
    if name:
        # Avoid "the The Big Score Scoreboard" when the season name
        # already starts with "The".
        article = "" if name.lower().startswith("the ") else "the "
        return f"{verb} {article}{name} Scoreboard (Season {season_num})"
    return f"{verb} the Season {season_num} Scoreboard"


# Standard Atom Shop howToObtain string used across all builders.
ATX_HOW = "Can be purchased with certain bundles from the Atom Shop."

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_tsv(path, encoding="utf-8"):
    """Load a TSV file by path. Returns list of dicts."""
    rows = []
    with open(path, encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Strip surrounding quotes that xEdit sometimes adds. Guard against
            # None values: DictReader returns None for missing trailing cells
            # on short rows (common in wide exports like WTHR where unused
            # KW_N columns are absent on records with few keywords).
            rows.append({
                k: ((v or "").strip().strip('"'))
                for k, v in row.items()
                if k is not None
            })
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


def safe_int(raw, default=0):
    """int() that tolerates the malformed cells some exports produce
    (e.g. a ReferencedBy value spilling into ECIL_Count)."""
    try:
        return int(float(str(raw).strip() or default))
    except (ValueError, TypeError):
        return default


def is_cut(edid):
    """Return True if the EDID marks cut/unreleased content."""
    s = str(edid or "").strip().upper()
    return s.startswith(("ZZZ", "ZZZZ", "DEL_", "DELETE_", "CUT_"))


def storefront_img_url(ecil_val, folder=""):
    """
    Convert a .dds ECIL image value to a storefront avif URL.
    ECIL values look like: ATX_CAMP_Utility_WeatherStation_Standard_Clear_C1.dds
    Storefront URLs follow the pattern:
      /wp-content/uploads/storefront/<folder>/<lowercase_name_no_ext>.avif
    """
    if not ecil_val or not ecil_val.strip():
        return None
    name = os.path.splitext(os.path.basename(ecil_val.strip()))[0].lower()
    if not name:
        return None
    base = "/wp-content/uploads/storefront"
    if folder:
        return f"{base}/{folder}/{name}.avif"
    return f"{base}/{name}.avif"


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
print(f"  ACTI: {ACTI_PATH}")
print(f"  WTHR: {WTHR_PATH if WTHR_PATH else '(not found — weather-station fishing classification disabled)'}")
print(f"  NPC PRPS: {NPC_PRPS_PATH if NPC_PRPS_PATH else '(not found — repair bot NPC stats falling back to June 2026 constants)'}")

cobj_rows      = load_tsv(COBJ_PATH)
entm_rows      = load_tsv(ENTM_PATH)
furn_rows      = load_tsv(FURN_PATH)
flst_entries   = load_tsv(FLST_PATH)
book_rows      = load_tsv(BOOK_PATH)
acti_rows      = load_tsv(ACTI_PATH)
wthr_rows      = load_tsv(WTHR_PATH) if WTHR_PATH else []
npc_prps_rows  = load_tsv(NPC_PRPS_PATH) if NPC_PRPS_PATH else []
glob_rows      = load_tsv(GLOB_PATH) if GLOB_PATH else []

# GLOB: FormID → float value AND EDID → float value (gold vendor tiers etc.)
glob_fltv_by_id:   dict = {}
glob_fltv_by_edid: dict = {}
for r in glob_rows:
    _v = (r.get("FLTV") or "").strip()
    if not _v:
        continue
    if r.get("FormID"):
        glob_fltv_by_id[r["FormID"].strip()] = _v
    if r.get("EDID"):
        glob_fltv_by_edid[r["EDID"].strip().lower()] = _v

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

# NPC PRPS: map NPC FormID → {ActorValue name → value} (repair bot stats)
npc_prps_by_id = {}
for r in npc_prps_rows:
    fid = r.get("NPC_FormID", "").strip()
    av  = r.get("ActorValue_Name", "").strip() or r.get("ActorValue_EDID", "").strip()
    if fid and av:
        npc_prps_by_id.setdefault(fid, {})[av] = r.get("Value", "").strip()

# ACTI by EDID — used to pull PRPS properties (e.g. PowerRequired) for weather stations
acti_by_edid = {r["ACTI_EDID"]: r for r in acti_rows}

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

# Gold-vendor LVLI ref pattern inside BOOK ReferencedBy columns, e.g.
#   005A0AC5:W05_LLV_GoldVendor_Settler_Samuel_1_Cautious:LVLI
# → faction "Settler", vendor "Samuel", reputation rank "Cautious"
_GOLD_VENDOR_LVLI_RE = re.compile(
    r"W05_LLV_GoldVendor_(?P<faction>[A-Za-z]+)_(?P<vendor>[A-Za-z]+)_\d+_(?P<rank>[A-Za-z]+)",
    re.IGNORECASE,
)
# BVGO column, e.g. "Econ_GoldVendor_Tier_10 [GLOB:005A504D]"
_BVGO_RE = re.compile(r"^\s*(?P<edid>\S+)\s*\[GLOB:(?P<fid>[0-9A-Fa-f]+)\]")

book_by_id: dict = {}
for r in book_rows:
    ref_vals = [r.get(f"Ref{i}", "") for i in range(1, 44)]
    is_untrad = any("Untradable" in v or "Untradeable" in v for v in ref_vals)

    # Gold-vendor data (vendor + rank from refs, price tier from BVGO)
    _gv_vendor, _gv_rank = "", ""
    for v in ref_vals:
        m = _GOLD_VENDOR_LVLI_RE.search(v or "")
        if m:
            _gv_vendor, _gv_rank = m.group("vendor"), m.group("rank")
            break
    _gv_price = ""
    _bvgo_m = _BVGO_RE.match(r.get("BVGO", "") or "")
    if _bvgo_m:
        _fltv = (glob_fltv_by_id.get(_bvgo_m.group("fid").upper().zfill(8))
                 or glob_fltv_by_id.get(_bvgo_m.group("fid"))
                 or glob_fltv_by_edid.get(_bvgo_m.group("edid").lower()))
        if _fltv:
            try:
                _gv_price = str(int(float(_fltv)))
            except (ValueError, TypeError):
                _gv_price = _fltv

    book_by_id[r["FormID"]] = {
        "full":            r.get("FULL", ""),
        "edid":            r.get("EDID", ""),
        "is_untradeable":  is_untrad,
        "gold_vendor":     _gv_vendor,
        "gold_rank":       _gv_rank,
        "gold_price":      _gv_price,
    }


def plan_purchase_block(book_fid: str) -> str:
    """
    How to Obtain block for a physical purchasable plan (user template,
    June 2026):

        Plan: {BOOK plan name}
        {Vendor} - {Reputation rank} reputation   (gold-bullion vendors)
        {price} Gold Bullion

    All three lines are data-derived from the BOOK record (FULL, the
    W05_LLV_GoldVendor_* LVLI ref, and the BVGO price-tier GLOB). Lines
    whose data is missing are omitted. Returns "" if the BOOK is unknown.
    Caps/Stamps vendor plans: no reliable purchase-price field in the
    current exports — extend here when one becomes available.
    """
    b = book_by_id.get(book_fid)
    if not b:
        return ""
    lines = []
    full = (b["full"] or "").strip()
    if full:
        lines.append(full if full.lower().startswith("plan") else f"Plan: {full}")
    if b["gold_vendor"]:
        lines.append(f"{b['gold_vendor']} - {b['gold_rank']} reputation"
                     if b["gold_rank"] else b["gold_vendor"])
    if b["gold_price"]:
        lines.append(f"{b['gold_price']} Gold Bullion")
    return "\n".join(lines)

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
        count = safe_int(entm.get("ECIL_Count", 0))
        for i in range(1, count + 1):
            ecil = entm.get(f"ECIL_{i}", "").strip()
            if ecil == cobj_id:
                return entm
    return None


def ecil_images(entm_row, folder=""):
    """Extract carousel images from ENTM ECIL columns.

    Some exports (June 2026+) concatenate every carousel filename into ECIL_1
    with no separator ('Foo_C1.ddsFoo_C2.dds') and leave ECIL_2+ empty, so
    each cell is regex-split on .dds boundaries instead of being taken whole.
    """
    images = []
    count = safe_int(entm_row.get("ECIL_Count", 0))
    for i in range(1, count + 1):
        val = entm_row.get(f"ECIL_{i}", "").strip()
        if not val:
            continue
        for dds in re.findall(r"[^\\/:*?\"<>|]+?\.dds", val, flags=re.IGNORECASE):
            url = storefront_img_url(dds, folder)
            if url:
                images.append(url)
    return images


def xalg_source_from_furn(form_id):
    """Get obtain source from FURN XALG flags."""
    furn = furn_by_id.get(form_id, {})
    return xalg_to_source(furn.get("XALG_Flags", ""))


def acti_prps_value(acti_edid: str, av_name: str) -> str:
    """
    Return the float value (as a string) for a named Actor Value from an ACTI
    record's PRPS properties, or '' if not found.

    The ACTI TSV stores properties as dynamic column groups:
      Prop_1_AV | Prop_1_Val | Prop_1_Curve
      Prop_2_AV | Prop_2_Val | Prop_2_Curve  …

    We scan all Prop_N_AV columns for a case-insensitive match on av_name.
    """
    row = acti_by_edid.get(acti_edid, {})
    if not row:
        return ""
    prop_count = int(row.get("PropCount", 0) or 0)
    for i in range(1, prop_count + 1):
        if row.get(f"Prop_{i}_AV", "").strip().lower() == av_name.lower():
            return row.get(f"Prop_{i}_Val", "").strip()
    return ""


def furn_prps_value(furn_id: str, av_name: str) -> str:
    """
    Return the float value (as a string) for a named Actor Value from a FURN
    record's PRPS properties, or '' if not found. Same dynamic column layout
    as acti_prps_value:  Prop_1_AV | Prop_1_Val | Prop_1_Curve …
    The Prop_N_AV cells hold the AV EDID directly (e.g. ATX_RepairBot_RepairRate).
    """
    row = furn_by_id.get(furn_id, {})
    if not row:
        return ""
    prop_count = int(row.get("PropCount", 0) or 0)
    for i in range(1, prop_count + 1):
        if row.get(f"Prop_{i}_AV", "").strip().lower() == av_name.lower():
            return row.get(f"Prop_{i}_Val", "").strip()
    return ""


def fmt_num(raw: str) -> str:
    """'2.000000' → '2', '0.5000' → '0.5', '' → ''."""
    s = str(raw or "").strip()
    if not s:
        return ""
    try:
        f = float(s)
        return str(int(f)) if f == int(f) else str(f)
    except (ValueError, TypeError):
        return s


def fvpa_to_text(fvpa: str) -> str:
    """
    Convert a COBJ FVPA component string to display text.
      'c_Circuitry:1:COBJ_Workshop_Circuitry|c_Steel:3:COBJ_Workshop_Steel'
      → 'Circuitry ×1\nSteel ×3'
    """
    parts = []
    for chunk in str(fvpa or "").split("|"):
        bits = chunk.strip().split(":")
        if len(bits) < 2:
            continue
        name = re.sub(r"^c_", "", bits[0].strip())
        # CamelCase → spaced words (e.g. 'NuclearMaterial' → 'Nuclear Material')
        name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
        qty  = bits[1].strip()
        if name and qty:
            parts.append(f"{name} ×{qty}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# WEATHER STATIONS — fishing-weather classification from WTHR keywords
# ---------------------------------------------------------------------------
# Source: FLST 006EE8F0 (ATX_Weather_FormList_WeatherStations) → 18 ACTI entries
# ENTM matched by EDID suffix after ATX_Weather_WeatherStation_ /
# SCORE_*_Weather_WeatherStation_
#
# Fishing classification is derived from WTHR record keywords, cross-referenced
# against the CNDF fishing conditions (Fishing_IsCamp*Weather_Condition). Each
# CNDF tests for the presence of specific keywords via GetCurrentCAMPWeatherHasKeyword.
# By categorising each WTHR's keywords we can label what a player would fish in:
#
#   FISHING_GLOWING_KWS   — Fishing_IsCampGlowingWeather_Condition
#   FISHING_SANDSTORM_KWS — Fishing_IsCampSandstormWeather_Condition
#   FISHING_RAINY_KWS     — Fishing_IsCampRainyWeather_Condition
#   (anything else)       — Fishing_IsCampAnyFallbackWeather_Condition
#
# ENTM → WTHR linkage is hardcoded for now (manual mapping keyed by ENTM EDID
# suffix). Once the ACTI export picks up VMAD script properties, this will be
# replaced by a data-driven station→WTHR lookup via the weather-station
# Papyrus scripts.

WEATHER_FLST = "006EE8F0"

# Keyword EDIDs (without the FormID prefix) that each fishing CNDF tests for.
FISHING_GLOWING_KWS   = {"s_wt_StormRad", "s_wt_StormNuke"}
FISHING_SANDSTORM_KWS = {"s_wt_Sandstorm"}
FISHING_RAINY_KWS     = {
    "s_wt_StormMistyRainy",
    "s_wt_StormRain",
    "s_wt_StormRainOcclusion",
    "ATX_Weather_WeatherTypeKW_ThunderStorm",
}

def _wthr_kw_edids(wthr_row):
    """Extract the set of keyword EDIDs from a WTHR_Export row.

    KW_N cells look like '0017698E:s_wt_StormRad:KYWD'; we only need the middle
    segment. Empty cells are skipped. Returns a set for fast membership tests.
    """
    kws = set()
    # WTHR export currently emits up to 7 KW columns (KW_1..KW_7)
    for i in range(1, 20):
        cell = wthr_row.get(f"KW_{i}", "").strip()
        if not cell:
            continue
        parts = cell.split(":")
        if len(parts) >= 2 and parts[1]:
            kws.add(parts[1])
    return kws

def _classify_wthr_keywords(kw_set):
    """Map a WTHR's keyword set to a fishing catch-condition label.

    The fishing catch lists (Fishing_LL_*) test exactly these CAMP-weather
    conditions, in this precedence:
      Fishing_IsCampGlowingWeather_Condition  — s_wt_StormRad OR s_wt_StormNuke
      Fishing_IsCampRainyWeather_Condition    — rain-type keywords
      Fishing_IsCampAnyFallbackWeather_Condition — anything else ("No Weather")
    s_wt_Sandstorm is only tested by the Burning Springs sandstorm fishing
    CHALLENGES (Fishing_IsCampSandstormWeather_Condition) — no catch table.

    Returns one of: 'Nuke Storm', 'Rad Storm', 'Sandstorm', 'Rain',
    'No Weather'. Nuke before Rad so the Nuke Zone station (which carries
    BOTH keywords) labels as Nuke Storm; both trigger the same glowing
    catch table.
    """
    if "s_wt_StormNuke" in kw_set:
        return "Nuke Storm"
    if "s_wt_StormRad" in kw_set:
        return "Rad Storm"
    if kw_set & FISHING_SANDSTORM_KWS:
        return "Sandstorm"
    if kw_set & FISHING_RAINY_KWS:
        return "Rain"
    return "No Weather"

# Display overrides where the data-accurate label needs a word of explanation
# (verified against WTHR keywords, June 2026 TSVs).
FISHING_LABEL_OVERRIDES = {
    "atx_weather_rainbow":
        "Rad Storm (yes, really — the Rainbow weather carries the rad storm keyword)",
    "atx_weather_rainbowlightrain":
        "No Weather (despite the name — no rain keyword on this weather)",
    "atx_weather_burningsandstorm":
        "Sandstorm (counts for the Burning Springs sandstorm fishing challenges — "
        "no special catch table)",
}

# Build the WTHR classification lookups: by FormID and by lowercase EDID.
wthr_class_by_fid  = {}
wthr_class_by_edid = {}
for _w in wthr_rows:
    _fid  = _w.get("WTHR_FormID", "").strip()
    _edid = _w.get("WTHR_EDID", "").strip()
    if not _fid and not _edid:
        continue
    _kws  = _wthr_kw_edids(_w)
    _cls  = _classify_wthr_keywords(_kws)
    if _fid:
        wthr_class_by_fid[_fid.upper()] = _cls
    if _edid:
        wthr_class_by_edid[_edid.lower()] = _cls

# ENTM EDID suffix (lower-cased, after stripping SCORE_S##_ + ATX_/ENTM_CAMP_Utility_WeatherStation_)
# → the WTHR EDID whose keywords define the fishing bonus at that station.
#
# This is a manual table for now. When the ACTI VMAD export lands we can
# derive this automatically from the weather station's Papyrus script
# properties, but the mapping is ~18 entries and rarely changes.
ENTM_SUFFIX_TO_WTHR_EDID = {
    "standard_clear":           "NewWeatherClear_DONOTUSE",   # Generic clear weather
    "standard_radstorm":        "NewWeatherRadstorm",
    "snowman_snow":             "NewWeatherRain",             # Snowman → rain-type weather
    "xpdacboardwalk":           "ATX_Weather_XPD_AC_Boardwalk_Fog",
    "thunderstorm":             "ATX_Weather_Thunderstorm",
    "storm_skylinevalley":      "ATX_Weather_Storm_DeadZone_New",
    "mothman":                  "ATX_Weather_MothmanEquinox",
    "halloween":                "ATX_Weather_HalloweenOvercast01",
    "fallfoliage":              "ATX_Weather_FallFoliage",
    "nukezone":                 "NewWeatherPostNukeBlast",
    "snowaurora":               "ATX_Weather_SnowAurora01",
    "verdantpollen":            "ATX_Weather_VerdantPollen",
    "fireworks":                "ATX_Weather_Fireworks",
    "standard_lightrain":       "ATX_Weather_LightRain",
    "burningnight":             "ATX_Weather_BurningNight",
    "burningsandstorm":         "ATX_Weather_BurningSandStorm",
    "outwaste":                 "ATX_Weather_Outwaste",
    "invasion":                 "ATX_Weather_Invasion",
    "rainbow":                  "ATX_Weather_Rainbow",
    "rainbowlightrain":         "ATX_Weather_RainbowLightRain",
}


# Suffix extractor for ACTI EDID
_ACTI_PREFIX_RE = re.compile(
    r"^(?:SCORE_S\d+_)?ATX_Weather_WeatherStation_(.+)$",
    re.IGNORECASE
)
_ENTM_PREFIX_RE = re.compile(
    r"^(?:SCORE_S\d+_)?(?:ATX_)?ENTM_CAMP_Utility_WeatherStation_(.+)$",
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
    # The plan (0075FF95) has NonPlayerTradable keyword — plan is NOT tradeable.
    WEATHER_TRADEABLE = {
        "0073ABA6": False,  # Weather Control Station (Atlantic City Fog) — Gold Bullion plan, NonPlayerTradable
    }

    # Fishing-weather classification is derived at build time by:
    #   1. Looking up the WTHR record that a given weather station activates
    #      (ENTM_SUFFIX_TO_WTHR_EDID table above — to be replaced with a
    #      VMAD-derived map once the ACTI export gains script-property data).
    #   2. Classifying that WTHR's keywords via _classify_wthr_keywords()
    #      using the fishing-CNDF keyword sets.
    #
    # Labels emitted: 'Radstorm', 'Sandstorm', 'Rainy', 'Clear', 'Cloudy',
    # or 'Generic CAMP Weather'.

    # ── Physical plan books per station ──
    # ENTM FormID → BOOK FormID. The CondProxy token lookup false-positives
    # on weather stations, so the BOOK link stays a manual override — but the
    # How to Obtain wording (plan name / vendor / rank / price) is derived
    # from the BOOK record via plan_purchase_block().
    WEATHER_PLAN_BOOKS = {
        "0073ABA6": "0075FF95",  # Atlantic City Fog — gold-vendor plan (Samuel)
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
        image_url = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-utility") if _etdi else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"

        # Tradeable: ALL weather stations are non-tradeable.
        # They are entitlement-based items (Atom Shop / Scoreboard / Gold Bullion).
        # AC Fog plan (0075FF95) explicitly has NonPlayerTradable keyword.
        # The CondProxy lookup can false-positive on some stations (e.g. Halloween,
        # Mothman), so we hardcode False for all and only look up plan names.
        # Plan name: only AC Fog has a confirmed purchasable plan book (BOOK FormID 0075FF95).
        # All other weather stations are entitlement-based with no player-purchasable plan.
        # CondProxy token lookups false-positive on some stations — hard-clear for non-AC-Fog.
        _ws_tradeable = False
        _ws_book = book_by_id.get(WEATHER_PLAN_BOOKS.get(entm_id, ""), {})
        _ws_plan_name = (_ws_book.get("full") or "").strip()
        if _ws_plan_name and not _ws_plan_name.lower().startswith("plan"):
            _ws_plan_name = f"Plan: {_ws_plan_name}"

        # ── Fishing-weather classification: ENTM suffix → WTHR → keywords ──
        _suffix_m    = _ENTM_PREFIX_RE.match(edid)
        _edid_suffix = _suffix_m.group(1).lower() if _suffix_m else ""
        _wthr_edid   = ENTM_SUFFIX_TO_WTHR_EDID.get(_edid_suffix, "")
        if _wthr_edid and wthr_class_by_edid:
            _fishing = wthr_class_by_edid.get(_wthr_edid.lower(), "")
            _fishing = FISHING_LABEL_OVERRIDES.get(_wthr_edid.lower(), _fishing)
            if not _fishing:
                # WTHR EDID was mapped but not found in the current WTHR TSV —
                # surface a build-time warning so we notice stale mappings.
                print(f"  [WARN] ENTM suffix '{_edid_suffix}' maps to WTHR "
                      f"'{_wthr_edid}' but that WTHR wasn't found in the TSV")
        else:
            _fishing = ""
            if _edid_suffix:
                # NEW station not in ENTM_SUFFIX_TO_WTHR_EDID — it will still
                # appear on the page but its Fishing line shows "—" until a
                # one-line suffix→WTHR mapping is added above.
                print(f"  [WARN] NEW weather station suffix '{_edid_suffix}' has no "
                      f"WTHR mapping — Fishing label will show '—' until mapped")

        # ── How to Obtain ──
        # Scoreboard / Atom Shop use the standard templates. Stations with a
        # physical plan get the plan-purchase block (gold-merge convention:
        # scoreboard line first, then the plan/vendor/price lines).
        _plan_block = plan_purchase_block(WEATHER_PLAN_BOOKS.get(entm_id, ""))
        if _plan_block:
            _how = f"{scoreboard_how(season_num)}\nOR\n{_plan_block}" if season_num else _plan_block
        elif season_num:
            _how = scoreboard_how(season_num)
        else:
            _how = ATX_HOW

        # ── Build Information — PowerRequired read from ACTI PRPS ──
        # The ACTI EDID for weather stations follows the pattern:
        #   ATX_Weather_WeatherStation_<Suffix>  (or SCORE_S##_ prefix)
        # We derive it from the ENTM EDID by swapping the ENTM prefix.
        _acti_suffix    = re.sub(
            r"^(?:SCORE_S\d+_)?(?:ATX_)?ENTM_CAMP_Utility_WeatherStation_",
            "", edid, flags=re.IGNORECASE
        )
        _season_prefix_m = re.match(r"^(SCORE_S\d+_)", edid, re.IGNORECASE)
        _season_prefix   = _season_prefix_m.group(1) if _season_prefix_m else ""
        _acti_edid       = f"{_season_prefix}ATX_Weather_WeatherStation_{_acti_suffix}"

        _power_raw = acti_prps_value(_acti_edid, "PowerRequired")
        try:
            _power_val = str(int(float(_power_raw))) if _power_raw else "—"
        except (ValueError, TypeError):
            _power_val = _power_raw or "—"

        # Build limit and flamingo units are workshop-system globals with no
        # per-record game field — these are the only values that stay hardcoded.
        # `Fishing Weather` is appended so the Technical-section renderer picks
        # it up without needing a new Wix field mapping. Falls back to an
        # em-dash when the ENTM isn't in our suffix→WTHR map (unknown or newly
        # added station) or when the WTHR TSV is missing.
        _fishing_line = _fishing if _fishing else "—"
        _build_info = (
            f"Build Limit per Camp: 1\n"
            f"Build Limit per Workshop: 0\n"
            f"Power Required: {_power_val}\n"
            f"Flamingo Units: 1\n"
            f"Fishing Weather: {_fishing_line}"
        )

        items.append({
            "formId":               entm_id,
            "entmFormId":           entm_id,
            "edid":                 edid,
            "displayName":          display,
            "shortName":            nnam,
            "description":          desc,
            "obtainSource":         source,
            "seasonNumber":         season_num,
            "howToObtain":          _how,
            "dropRate":             "N/A",
            "tradeable":            _ws_tradeable,
            "planName":             _ws_plan_name,
            "imageUrl":             image_url,
            "imageCarousel":        carousel,
            "buildInfo":            _build_info,
            "craftingRequirements": "Circuitry ×2\nRubber ×2\nSteel ×4\nScrew ×2",
            "fishingCondition":     _fishing,    # Back-compat: same as fishingWeather
            "fishingWeather":       _fishing,    # Catch label: No Weather/Rad Storm/Nuke Storm/Sandstorm/Rain
            "wthrEdid":             _wthr_edid,  # WTHR record whose keywords drive the label
            "cutContent":           False,
        })

    items.sort(key=lambda x: x["displayName"])
    return {"items": items}


# ---------------------------------------------------------------------------
# REPAIR BOTS
# ---------------------------------------------------------------------------
# Source: ENTM_Export_March_2026.tsv — all ATX_ENTM_CAMP_Utility_RepairBot_* records.
# All four skins are Atom Shop (XALG flag 000000001 = Premium/ATX).
# No base/default repair bot exists as a separate ENTM entry.
#
# AUTO-DISCOVERY (titles.js pattern): bots are found by EDID — every skin's
# storefront record matches ENTM_CAMP_Utility_RepairBot_<Suffix>, and the
# pod FURN matches RepairBot(_)?Pod_<Suffix>. New skins appear automatically
# on the next TSV drop; unmatched parts log a [WARN].

def _rb_norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

REPAIR_BOT_ENTM_IDS = []
_rb_suffix_by_entm = {}
for _e in entm_rows:
    _m = re.search(r"ENTM_CAMP_Utility_RepairBot_(.+)$", _e.get("EDID", ""), re.I)
    if _m and not is_cut(_e.get("EDID", "")):
        REPAIR_BOT_ENTM_IDS.append(_e["FormID"])
        _rb_suffix_by_entm[_e["FormID"]] = _rb_norm(_m.group(1))
REPAIR_BOT_ENTM_IDS.sort()

# Pod FURN per suffix (the pod carries PRPS: ATX_RepairBot_RepairRate etc.)
_rb_furn_by_suffix = {}
for _f in furn_rows:
    _m = re.search(r"RepairBot_?Pod_(.+)$", _f.get("FURN_EDID", ""), re.I)
    if _m:
        _rb_furn_by_suffix[_rb_norm(_m.group(1))] = _f.get("FURN_FormID", "")

REPAIR_BOT_FURN_IDS = {}
for _eid, _sfx in _rb_suffix_by_entm.items():
    _fid = _rb_furn_by_suffix.get(_sfx, "")
    if not _fid:
        print(f"  [WARN] repair bot '{_sfx}': no pod FURN matched — pod stats will be blank")
    REPAIR_BOT_FURN_IDS[_eid] = _fid

# Bot NPC record + race per suffix (NPC actor TSV isn't loaded here — manual).
# A new bot without an entry still builds; it just logs a [WARN] and leaves
# the NPC fields blank until the suffix is added below.
_RB_NPC_BY_SUFFIX = {
    "enclave":       {"npcFormId": "007AE49A", "npcEdid": "ATX_RepairBot_Enclave",       "race": "Protectron"},
    "company":       {"npcFormId": "008109D4", "npcEdid": "ATX_RepairBot_Company",       "race": "Protectron"},
    "santashelper":  {"npcFormId": "0084920D", "npcEdid": "ATX_RepairBot_SantasHelper",  "race": "Protectron"},
    "emergencytech": {"npcFormId": "008571F7", "npcEdid": "ATX_RepairBot_EmergencyTech", "race": "Mr. Handy"},
}
REPAIR_BOT_NPC_INFO = {}
for _eid, _sfx in _rb_suffix_by_entm.items():
    _info = _RB_NPC_BY_SUFFIX.get(_sfx)
    if not _info:
        print(f"  [WARN] repair bot '{_sfx}': no NPC info — add to _RB_NPC_BY_SUFFIX")
        _info = {"npcFormId": "", "npcEdid": "", "race": ""}
    REPAIR_BOT_NPC_INFO[_eid] = _info

# NOTE: bot NPC combat stats (Health/AP/Perception) were dropped from the
# page output June 2026 — the npc_prps_by_id map stays available for future
# stat displays (e.g. pets).

# Crafting COBJ: ATX_workshop_co_CategoryResources_RepairBot (007AE545).
# Its CNAM is the LVLI ATX_workshop_LL_RepairBots (007AE547) — entries are
# entitlement-gated so the one COBJ crafts whichever skin you own.
REPAIR_BOT_LVLI_ID = "007AE547"

# Build limit GLOBs (GLOB_Export_Jun_2026):
#   008020FB ATX_WorkshopCount_RepairBot_CAMP = 1
#   008020FA ATX_WorkshopCount_RepairBot      = 1
REPAIR_BOT_LIMIT_CAMP     = "1"
REPAIR_BOT_LIMIT_WORKSHOP = "1"

# Per-bot howToObtain overrides. The Enclave Repair Bot is NOT an Atoms
# purchase — it shipped in the real-money Enclave Armory Bundle
# (dist/atom_shop.json "ltb" section: released 2024-12-03, Gleaming Depths,
# Steam / PlayStation / Xbox).
REPAIR_BOT_HOW_OVERRIDE = {
    "007AE546": ("Real-money Limited Time Bundle: Enclave Armory Bundle "
                 "(released 3 December 2024 with the Gleaming Depths update)."),
}

# Hand-uploaded page images (June 2026) — these live in guide-images, NOT the
# storefront folder, and the filename suffixes are not uniform (base / _c1 /
# _c2 / _l), so each bot maps to an explicit file list. First entry = primary.
REPAIR_BOT_IMG_BASE = "/wp-content/uploads/guide-images/camp-items/repair-bots/"
REPAIR_BOT_IMAGES = {
    "0082BED5": ["atx_camp_utility_repairbot_company.avif",
                 "atx_camp_utility_repairbot_company_c2.avif"],
    "008571F1": ["atx_camp_utility_repairbot_emergencytech_c1.avif",
                 "atx_camp_utility_repairbot_emergencytech_l.avif"],
    "007AE546": ["atx_camp_utility_repairbot_enclave.avif",
                 "atx_camp_utility_repairbot_enclave_c2.avif"],
    "0084920E": ["atx_camp_utility_repairbot_santashelper.avif",
                 "atx_camp_utility_repairbot_santashelper_c2.avif"],
}


def build_repair_bots():
    # Crafting requirements — parsed from the shared COBJ FVPA components.
    # Falls back to the June 2026 values if the COBJ row is missing.
    _craft = ""
    for _cobj in cobj_by_cnam.get(REPAIR_BOT_LVLI_ID, []):
        _craft = fvpa_to_text(_cobj.get("FVPA", ""))
        if _craft:
            break
    if not _craft:
        _craft = "Circuitry ×1\nCopper ×1\nGears ×1\nSteel ×3"

    items = []
    for entm_id in REPAIR_BOT_ENTM_IDS:
        entm = entm_by_id.get(entm_id, {})
        if not entm:
            continue

        furn_id  = REPAIR_BOT_FURN_IDS.get(entm_id, "")
        furn     = furn_by_id.get(furn_id, {})
        npc_info = REPAIR_BOT_NPC_INFO.get(entm_id, {})

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

        # Images: prefer the hand-uploaded guide-images set; fall back to the
        # storefront ECIL-derived URLs for any future bot not yet mapped.
        _hand = REPAIR_BOT_IMAGES.get(entm_id)
        if _hand:
            carousel  = [REPAIR_BOT_IMG_BASE + f for f in _hand]
            image_url = carousel[0]
        else:
            _etdi     = entm.get("ETDI", "").strip()
            carousel  = ecil_images(entm, "camp-utility")
            image_url = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-utility") if _etdi else "")

        # --- Output data (from the pod FURN PRPS) ---
        repair_rate = fmt_num(furn_prps_value(furn_id, "ATX_RepairBot_RepairRate")) or "2"
        budget_mult = fmt_num(furn_prps_value(furn_id, "WorkshopBudgetObjectMultiplier")) or "5"

        output_info = (
            "Automatically repairs damaged objects in your C.A.M.P. while deployed.\n"
            "Cannot rebuild objects that have been completely destroyed."
        )

        # --- Build information (weather-station buildInfo format) ---
        build_info = (
            f"Build Limit per Camp: {REPAIR_BOT_LIMIT_CAMP}\n"
            f"Build Limit per Workshop: {REPAIR_BOT_LIMIT_WORKSHOP}\n"
            f"Power Required: 0\n"
            f"Flamingo Units: {budget_mult}\n"
            f"Shelter Placement: No"
        )

        npc_id = npc_info.get("npcFormId", "")

        technical_notes = "\n".join([
            f"Pod EDID: {furn.get('FURN_EDID', '') or '—'}",
            f"Pod FormID: {furn_id or '—'}",
            f"Bot NPC EDID: {npc_info.get('npcEdid', '') or '—'}",
            f"Bot NPC FormID: {npc_id or '—'}",
            f"Bot Race: {npc_info.get('race', '') or '—'}",
            f"Repair Rate AV (ATX_RepairBot_RepairRate): {repair_rate}",
        ])

        items.append({
            "formId":       furn_id or entm_id,
            "entmFormId":   entm_id,
            "furnFormId":   furn_id,
            "edid":         edid,
            "furnEdid":     furn.get("FURN_EDID", ""),
            "npcFormId":    npc_id,
            "npcEdid":      npc_info.get("npcEdid", ""),
            "race":         npc_info.get("race", ""),
            "displayName":  display,
            "description":  desc,
            "obtainSource": source,
            "howToObtain":  REPAIR_BOT_HOW_OVERRIDE.get(entm_id)
                            or (scoreboard_how(season_num) if season_num else ATX_HOW),
            "dropRate":     "N/A",
            "seasonNumber": season_num,
            "tradeable":    False,  # no plan book exists — account-bound CAMP skin
            "planName":     "",
            "imageUrl":     image_url,
            "imageCarousel": carousel,
            "outputInfo":   output_info,
            "repairRate":   f"{repair_rate}% per hour",
            "buildInfo":    build_info,
            "craftingRequirements": _craft,
            "technicalNotes": technical_notes,
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

# Drift check: warn when a NEW ally camp object exists that isn't mapped.
# Ally COBJ EDIDs match COMP_Constructible_CampObject (or LiteAlly for the
# Initiate). Allies need a manual COBJ→FURN pair, so new ones are flagged.
_known_ally_cobjs = set(ALLY_COBJ_FURN)
_ALLY_DRIFT_EXCLUDE = {
    # Cut content (confirmed by Duchess, 7 Jun 2026) — never shipped:
    "005856BA",  # Beggar
    "00585CD1",  # Hunter (Chopping Block)
    "00620A16",  # Doberman (Doghouse)
}
for _c in cobj_rows:
    _ce = _c.get("COBJ_EDID", "")
    if (re.search(r"COMP_Constructible_CampObject|LiteAlly", _ce, re.I)
            and not is_cut(_ce)):
        _cid = _c.get("COBJ_FormID", "")
        if _cid and _cid not in _known_ally_cobjs and _cid not in _ALLY_DRIFT_EXCLUDE:
            print(f"  [WARN] NEW ally camp object not in ALLY_COBJ_FURN: {_cid} {_ce} "
                  f"— add the COBJ→FURN mapping")

# ENTM → COBJ link: scan ENTM ECIL cols for our COBJ FormIDs
_ally_entm_cache = {}

def find_entm_for_cobj_id(cobj_id):
    """Find ENTM by checking if cobj_id appears in ECIL fields (works for pets/furniture)."""
    if cobj_id in _ally_entm_cache:
        return _ally_entm_cache[cobj_id]
    for entm in entm_rows:
        count = safe_int(entm.get("ECIL_Count", 0))
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


# Fallback ally ENTM match by OWNER NAME + season. Ally items are named
# "Owner's Thing" and the ENTM EDID carries the owner name
# (e.g. "Adelaide's Table" -> SCORE_S16_ENTM_CAMP_Ally_Adelaide). The
# prefix/ETDI match misses these because the FURN EDID word order differs.
_ALLY_ENTMS_TOK = [e for e in entm_rows
                   if re.search(r"ENTM_CAMP_Ally", e.get("EDID", ""), re.IGNORECASE)
                   and not is_cut(e.get("EDID", ""))]


def find_ally_entm_by_token(display_name, season_num):
    owner = re.split(r"[’']s\b", display_name or "", maxsplit=1)[0]
    words = [w.lower() for w in re.split(r"[^A-Za-z]+", owner) if len(w) >= 4]
    if not words:
        return None
    fallback = None
    for e in _ALLY_ENTMS_TOK:
        ed = e.get("EDID", "").lower()
        if any(w in ed for w in words):
            if season_num and re.search(rf"_s0*{season_num}_", ed):
                return e
            fallback = fallback or e
    return fallback


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
            if not entm:
                _sm = re.search(r"SCORE_S(\d+)_", (furn_edid_val or ""), re.IGNORECASE)
                entm = find_ally_entm_by_token(meta["name"], int(_sm.group(1)) if _sm else None)

        display  = meta["name"]
        obtain   = meta["obtain"]
        source   = obtain

        desc     = clean_desc(entm.get("DESC", "")) if entm else ""
        entm_id  = entm["FormID"] if entm else ""

        # Use ETDI for the primary icon image (not ECIL_1 which has _C1 suffix)
        _etdi    = entm.get("ETDI", "").strip() if entm else ""
        carousel = ecil_images(entm, "camp-allies") if entm else []
        img      = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-allies") if _etdi else "")

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
            obtain = scoreboard_how(season_num)
        elif obtain == "Atom Shop":
            obtain = ATX_HOW

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
            "dropRate":         "N/A",
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
    "_deathclaw_": "deathclaw",
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
    "008AFDFF": "008AFE00",  # Goodboy             (dog)
    "008AFF66": "008AFF64",  # Cyprus Cat          (cat)  S25 Scoreboard
    "008B1207": "008B1206",  # Mr Pebbles          (cat)
    "008B1550": "008B154F",  # Gruyere             (radhog)
    "008B1D6D": "008B1D6C",  # Glowing Dog         (dog)  S25 Scoreboard
    "008B1F0B": "008B1F0A",  # Glowing Hog         (radhog) S25 Scoreboard
    "008A52AF": "008A52B4",  # Deathclaw           (deathclaw) — Deathclaw Cage spawn
}

# Drift check: warn when a NEW pet ENTM exists that isn't mapped above.
# Pets need a manual FURN→ENTM pair (regex suffix matching is unreliable for
# spawn furniture), so new pets are flagged here instead of auto-added.
_known_pet_entms = set(SPAWN_FURN_TO_ENTM.values())
for _e in entm_rows:
    _ed = _e.get("EDID", "")
    if (re.search(r"ENTM_CAMP_CAMPPets_", _ed, re.I)
            and not re.search(r"Furniture|Apparel|Emote", _ed, re.I)
            and not is_cut(_ed)
            and _e["FormID"] not in _known_pet_entms):
        print(f"  [WARN] NEW pet not in SPAWN_FURN_TO_ENTM: {_e['FormID']} {_ed} "
              f"— add the spawn FURN→ENTM pair")

# ---------------------------------------------------------------------------
# Authoritative ENTM ID list for pet idle furniture.
# Source: KYWD ATX_Entitlement_Filter_Store_CAMP_Pet_Furniture (008538E5)
# plus RadHog-specific items confirmed from ENTM export.
# Switched to ENTM-first approach (same pattern as pet apparel) because
# some items lack or have inconsistent FURN EDID IdleFurniture patterns.
# ---------------------------------------------------------------------------
# AUTO-DISCOVERY: every pet idle-furniture storefront record matches
# CAMPPets_(Idle)Furniture in its ENTM EDID. New items appear automatically;
# unknown animal types log a [WARN] (add to PET_FURNITURE_ANIMAL below).
PET_FURNITURE_ENTM_IDS = sorted({
    _e["FormID"] for _e in entm_rows
    if re.search(r"CAMPPets_(Idle)?Furniture", _e.get("EDID", ""), re.I)
    and not is_cut(_e.get("EDID", ""))
})

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
        pet_img  = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-pets") if _etdi else "")
        home_img = carousel[1] if len(carousel) > 1 else (carousel[0] if carousel else "")

        # Season number: check FURN EDID first, then ENTM EDID
        season_m   = re.match(r"SCORE_S(\d+)_", furn_edid or entm.get("EDID", ""), re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None

        if season_num:
            source = "Scoreboard"
            how    = scoreboard_how(season_num)
        else:
            how    = ATX_HOW

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
            "dropRate":      "N/A",
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
        img   = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-pets") if _etdi else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
            how    = scoreboard_how(season_num)
        else:
            how    = ATX_HOW

        items.append({
            "formId":        entm_id,
            "entmFormId":    entm_id,
            "edid":          edid,
            "displayName":   display,
            "description":   desc,
            "animalType":    animal,
            "obtainSource":  source,
            "howToObtain":   how,
            "dropRate":      "N/A",
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

# AUTO-DISCOVERY: every pet apparel storefront record matches
# ENTM_Apparel_CAMPPets_ in its EDID; animal comes from the EDID token.
PET_APPAREL_ENTM_IDS = sorted({
    _e["FormID"] for _e in entm_rows
    if re.search(r"ENTM_Apparel_CAMPPets_", _e.get("EDID", ""), re.I)
    and not is_cut(_e.get("EDID", ""))
})

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
        animal   = PET_APPAREL_ANIMAL.get(entm_id, animal_from_edid(entm.get("EDID", "")))
        carousel = ecil_images(entm, "camp-pets")
        # Pet apparel imageUrl: ETDI is the reliable base icon filename.
        # ECIL_1 has a _C1 suffix which does NOT match the uploaded texture icon.
        # ETDI must take priority; fall back to ECIL carousel only if ETDI is missing.
        _etdi    = entm.get("ETDI", "").strip()
        img      = storefront_img_url(_etdi, "camp-pets") if _etdi else (carousel[0] if carousel else "")
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
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
            "dropRate":     "N/A",
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

# AUTO-DISCOVERY: cryo freezer storefront records match CAMP_Utility_*Cryo*Fre*
# in the ENTM EDID (covers the in-data "CryoFrezer" typo too).
CRYO_ENTM_IDS = sorted({
    _e["FormID"] for _e in entm_rows
    if re.search(r"CAMP_Utility_.*Cryo.*Fre", _e.get("EDID", ""), re.I)
    and not is_cut(_e.get("EDID", ""))
})

# ENTM FormID → gold-vendor plan BOOK FormID (was the CondProxy COBJ
# 00732A95 pre-June-2026; now the BOOK 00732A91 so plan_purchase_block()
# can derive the plan name / vendor / rank / price from the record).
# NOTE: the old hardcoded "Samuel" line was wrong — the BOOK's vendor LVLI
# is W05_LLV_GoldVendor_Raider_Mortimer_1_Cautious (Mortimer, not Samuel).
CRYO_PLAN_BOOKS = {
    "006C2F42": "00732A91",  # Military Cryo-Freezer
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
        img      = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-utility") if _etdi else "")
        gv       = CRYO_PLAN_BOOKS.get(entm_id, "")
        edid     = entm.get("EDID", "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        # How to Obtain — standard templates; gold-merge convention puts the
        # scoreboard line first, then the data-derived plan/vendor/price block.
        _gv_block = plan_purchase_block(gv) if gv else ""
        if season_num and _gv_block:
            how = f"{scoreboard_how(season_num)}\nOR\n{_gv_block}"
        elif season_num:
            how = scoreboard_how(season_num)
        elif _gv_block:
            how = _gv_block
        else:
            how = ATX_HOW
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
            "dropRate":       "N/A",
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

# AUTO-DISCOVERY: fridges/coolers match ENTM_CAMP_Utility_Refrigerator*;
# beer kegs/dispensers match ENTM_CAMP_Utility_*Beer* (Beer Keg, Beer
# Mystery Machine, Blue Ridge Beer Keg). Cryos are a separate page.
FRIDGE_ENTM_IDS = sorted({
    _e["FormID"] for _e in entm_rows
    if (re.search(r"ENTM_CAMP_Utility_Refrigerator", _e.get("EDID", ""), re.I)
        or re.search(r"ENTM_CAMP_Utility_\w*Beer", _e.get("EDID", ""), re.I))
    and not re.search(r"Cryo", _e.get("EDID", ""), re.I)
    and not is_cut(_e.get("EDID", ""))
})


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
        img      = carousel[0] if carousel else (storefront_img_url(_etdi, "camp-utility") if _etdi else "")

        season_m   = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
        season_num = int(season_m.group(1)) if season_m else None
        if season_num:
            source = "Scoreboard"
            how    = scoreboard_how(season_num)
        else:
            how = ATX_HOW

        items.append({
            "formId":        entm_id,
            "entmFormId":    entm_id,
            "edid":          edid,
            "displayName":   display,
            "description":   desc,
            "obtainSource":  source,
            "howToObtain":   how,
            "dropRate":      "N/A",
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

weather_data = build_weather_stations()
repair_bots_data = build_repair_bots()
allies_data = build_allies()
pets_data = build_pets()
pet_furn_data = build_pet_furniture()
pet_appr_data = build_pet_apparel()
cryos_data = build_cryos()
fridges_data = build_fridges()

save_json("weather_stations.json", weather_data)
save_json("repair-bots.json",      repair_bots_data)
save_json("allies.json",           allies_data)
save_json("pets.json",             pets_data)
save_json("pet-furniture.json",    pet_furn_data)
save_json("pet-apparel.json",      pet_appr_data)
save_json("cryos.json",            cryos_data)
save_json("fridges.json",          fridges_data)

# Generate patchlog feed (all camp-related items combined)
def combine_camp_items(*dicts):
    """Combine all items from multiple data structures."""
    items = []
    for d in dicts:
        items.extend(d.get("items", []))
    return items

camp_items = combine_camp_items(
    allies_data, pets_data, pet_furn_data, pet_appr_data,
    cryos_data, fridges_data, repair_bots_data
)

write_patchlog_feed(
    dist_dir=str(OUT_DIR),
    feed_name="patchlog_latest_df_camp.json",
    current_items=camp_items,
    key_field="formId",
    name_field="displayName,edid",
    compare_fields=["displayName", "description", "obtainSource"],
    prev_json_path="dist/allies.json",  # Using allies.json as reference
    items_extractor=lambda d: d.get("items", []),
)

print("\nDone.")
