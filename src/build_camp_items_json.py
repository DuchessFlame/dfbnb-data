from __future__ import annotations

"""
DF/BNB — Camp Items JSON Builder
Produces:
  dist/collectrons.json          → /bnb/camp-items/collectrons/collectrons/
  dist/resource_producers.json   → /bnb/camp-items/resource-producers/food|junk|other/

Data sources (all in dfbnb-data/tsv/):
  RESO_Export_*.tsv
  CONT_Export_*.tsv
  ENTM_Export_*.tsv
  COBJ_Export_*.tsv
  BOOK_Export_*.tsv
  LVLI_Export_*_LVLI_List.tsv
  LVLI_Export_*_LVLI_Entries.tsv
  GLOB_Export_*.tsv          (optional — needed for intervals + Gold Bullion prices)
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")

# FOOD subcategory keywords (matched against RESO EDID + ENTM DESC lowercase)
FOOD_KEYWORDS = (
    "milk", "cake", "egg", "tea", "coffee", "candy", "popcorn", "beer",
    "pemmican", "apple", "turkey", "cookie", "mirelurk", "_food", "snack",
    "fruit", "brew", "honey", "soup", "spice", "seasoning", "stew", "muffin",
    "pie", "roast", "salmon", "noodle", "chili", "cheese", "tato", "razorgrain",
    "brahmin", "slocum", "oven", "kettle", "steamer", "edible",
)

# JUNK subcategory keywords
JUNK_KEYWORDS = (
    "_wood", "lumber", "_oil", "flora", "_junk", "_scrap", "acid",
    "cement", "fertilizer", "nuclear", "_bone", "steel", "lead", "glass",
    "cloth", "ceramic", "leather", "copper", "aluminum", "plastic",
    "material", "dumpster", "crate", "salvage", "military", "cargo",
    "apothecary", "butterfly", "morbid",
)

# Vendor name extraction from LVLI EDID
VENDOR_MAP = {
    "samuel":  "Samuel",
    "regs":    "Regs",
    "freeman": "Freeman",
    "radcliff": "Radcliff",
    "gold":    "a Gold Bullion Vendor",
}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()

def today_ymd() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")

def safe_float(s: str, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(str(s or "").strip())
    except Exception:
        return default

def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(str(s or "").strip())
    except Exception:
        return default

def starts_cut(edid: str) -> bool:
    e = (edid or "").strip().upper()
    return any(e.startswith(p) for p in CUT_PREFIXES)

def clean_str(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return re.sub(r"\s{2,}", " ", s).strip()

def clean_desc(s: str) -> str:
    """Remove boilerplate from DESC fields."""
    s = clean_str(s)
    # Remove trailing C.A.M.P. boilerplate after the real desc
    s = re.sub(r"\s*-\s*C\.A\.M\.P\. ITEMS APPEAR.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*This item cannot be built inside a Shelter\.?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*AVAILABLE FOR GOLD BULLION.*$", "", s, flags=re.IGNORECASE)
    # Collapse leftover whitespace
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def formid8(s: str) -> str:
    s = (s or "").strip().upper().replace("0X", "")
    if len(s) > 8:
        s = s[-8:]
    return s.zfill(8)

def extract_formid_from_ref(ref_str: str) -> Optional[str]:
    """Extract leading 8-hex FormID from strings like '00771DC4:SomeName:TYPE'."""
    m = re.match(r"^([0-9A-Fa-f]{8}):", (ref_str or "").strip())
    return m.group(1).upper() if m else None

def extract_avif_formid(field: str) -> Optional[str]:
    """Parse '00771DC6:SCORE_S17_ResourceAV_Collectron_Scoutmaster:AVIF' → '00771DC6'."""
    if not field:
        return None
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    if m:
        return m.group(1).upper()
    # bracket notation: [AVIF:00771DC6]
    m2 = re.search(r"\[AVIF:([0-9A-Fa-f]{8})\]", field)
    return m2.group(1).upper() if m2 else None

def extract_glob_formid(field: str) -> Optional[str]:
    """Parse GLOB FormID from fields like '00555DB0:SomeEdid:GLOB' or '[GLOB:XXXXXXXX]'."""
    if not field:
        return None
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    if m:
        return m.group(1).upper()
    m2 = re.search(r"\[GLOB:([0-9A-Fa-f]{8})\]", field)
    return m2.group(1).upper() if m2 else None

# ---------------------------------------------------------------------------
# TSV loading
# ---------------------------------------------------------------------------

def read_tsv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [dict(r) for r in reader]

def load_tsvs(patterns: List[str], tsv_root: Optional[str] = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for pat in patterns:
        paths = glob.glob(pat, recursive=True)
        if not paths and tsv_root:
            paths = glob.glob(os.path.join(tsv_root, "**", pat), recursive=True)
        for p in sorted(set(paths)):
            rows.extend(read_tsv(p))
    return rows

def _autofill(tsv_root: Optional[str], given: Optional[List[str]], globs: List[str]) -> List[str]:
    if given:
        return given
    if not tsv_root:
        return []
    hits: List[str] = []
    for g in globs:
        hits.extend(glob.glob(os.path.join(tsv_root, g), recursive=True))
    return sorted(set(hits))

# ---------------------------------------------------------------------------
# Index builders
# ---------------------------------------------------------------------------

def build_index(rows: List[Dict[str, str]], key_field: str) -> Dict[str, Dict[str, str]]:
    idx: Dict[str, Dict[str, str]] = {}
    for r in rows:
        k = (r.get(key_field) or "").strip().upper()
        if k:
            idx[k] = r
    return idx

def glob_fltv(glob_rows: List[Dict[str, str]], glob_formid: str) -> Optional[float]:
    """Return FLTV float for a GLOB FormID, or None."""
    fid = (glob_formid or "").strip().upper()
    for r in glob_rows:
        if (r.get("FormID") or "").strip().upper() == fid:
            return safe_float(r.get("FLTV") or r.get("DATA") or "")
    return None

def interval_display(hours: float) -> str:
    total_seconds = hours * 3600
    total_minutes = int(total_seconds // 60)
    secs = round(total_seconds % 60)
    if total_minutes == 0:
        return f"{secs} sec"
    if secs == 0:
        return f"{total_minutes} min"
    return f"{total_minutes} min {secs} sec"

# ---------------------------------------------------------------------------
# LVLI drop rate calculation (First-match / GetRandomPercent chain)
# ---------------------------------------------------------------------------

RE_RAND_PCT = re.compile(r"GetRandomPercent[^0-9]*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)

def parse_rand_pct_threshold(cond: str) -> Optional[float]:
    """Extract the >= threshold from 'Subject.GetRandomPercent(...) 11000000 98.000000'."""
    m = RE_RAND_PCT.search((cond or "").strip())
    return float(m.group(1)) if m else None

def compute_drop_table(lvli_formid: str, lvli_entry_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Compute First-match drop rates for a LVLI.
    Returns list of {item, formId, chance} sorted by EntryIndex.
    """
    entries = [r for r in lvli_entry_rows
               if (r.get("LVLI_FormID") or "").strip().upper() == lvli_formid.upper()]

    if not entries:
        return []

    # Sort by EntryIndex
    def _idx(r: Dict[str, str]) -> int:
        return safe_int(r.get("EntryIndex") or r.get("EntryIndex") or "0")

    entries.sort(key=_idx)

    drops: List[Dict[str, Any]] = []
    remaining = 1.0

    for r in entries:
        ref = clean_str(r.get("LVLO_Reference") or "")
        ref_fid = extract_formid_from_ref(ref)
        ref_parts = ref.split(":")
        item_name = ref_parts[1] if len(ref_parts) >= 2 else ref
        item_fid = ref_fid or ""

        # Parse condition
        cond_count = safe_int(r.get("CondCount") or "0")
        threshold: Optional[float] = None
        if cond_count > 0:
            cond1 = clean_str(r.get("Cond1") or "")
            threshold = parse_rand_pct_threshold(cond1)

        if threshold is not None:
            base = (100.0 - threshold) / 100.0
            chance = base * remaining
            remaining *= (1.0 - base)
        else:
            # No condition — guaranteed fallback (takes whatever remains)
            chance = remaining
            remaining = 0.0

        drops.append({
            "item": item_name,
            "formId": item_fid,
            "chance": round(chance * 100, 5),
        })

    return drops

# ---------------------------------------------------------------------------
# CONT property parsing
# ---------------------------------------------------------------------------

def parse_cont_properties(cont_row: Dict[str, str]) -> Dict[str, Any]:
    """
    Extract capacity, flamingo units, power required from CONT property strings.
    Property format: 'ActorValue=FORMID:EDID:AVIF | Value=N.N | CurveTable=...'
    """
    result: Dict[str, Any] = {
        "capacity": None,
        "powerRequired": False,
        "flamingoUnits": None,
        "lockable": None,
    }

    # Keywords
    kw_blob = " ".join(
        str(cont_row.get(f"KW{i}") or "") for i in range(1, 12)
    ).lower()

    if "workshopcanpowered" in kw_blob or "workshopcanbepowered" in kw_blob:
        result["powerRequired"] = True
    if "containertakeonly" in kw_blob:
        result["lockable"] = True

    # Properties
    prop_blob = " ".join(
        str(cont_row.get(f"Prop{i}") or "") for i in range(1, 7)
    )

    # CarryWeight -> capacity
    m = re.search(r"CarryWeight[^|]*\|\s*Value\s*=\s*([\d.]+)", prop_blob, re.IGNORECASE)
    if m:
        result["capacity"] = int(float(m.group(1)))

    # WorkshopBudgetObjectMultiplier -> flamingo units
    m = re.search(r"WorkshopBudgetObjectMultiplier[^|]*\|\s*Value\s*=\s*([\d.]+)", prop_blob, re.IGNORECASE)
    if m:
        result["flamingoUnits"] = int(float(m.group(1)))

    return result

# ---------------------------------------------------------------------------
# COBJ — crafting recipe data
# ---------------------------------------------------------------------------

def find_cobj_for_cont(cobj_rows: List[Dict[str, str]], cont_formid: str) -> Optional[Dict[str, str]]:
    """Find the build COBJ where CNAM_FormID matches the CONT FormID."""
    fid = (cont_formid or "").strip().upper()
    for r in cobj_rows:
        if (r.get("CNAM_FormID") or "").strip().upper() == fid:
            return r
    return None

def find_condproxy_cobj(cobj_rows: List[Dict[str, str]], name_token: str) -> Optional[Dict[str, str]]:
    """Find the CondProxy COBJ for a collectron/resource by name token."""
    tok = (name_token or "").lower()
    for r in cobj_rows:
        edid = (r.get("COBJ_EDID") or r.get("EDID") or "").lower()
        if "condproxy" in edid and tok in edid:
            return r
    return None

def find_goldvendor_cobj(cobj_rows: List[Dict[str, str]], name_token: str) -> Optional[Dict[str, str]]:
    """Find the GoldVendor plan COBJ for a collectron."""
    tok = (name_token or "").lower()
    for r in cobj_rows:
        edid = (r.get("COBJ_EDID") or r.get("EDID") or "").lower()
        if "goldvendor" in edid and tok in edid:
            return r
    return None

def parse_season_from_edid(edid: str) -> Optional[int]:
    """Extract season number from EDID like 'SCORE_S17_...'."""
    m = re.search(r"SCORE[_-]?S(\d+)[_-]", (edid or ""), re.IGNORECASE)
    return int(m.group(1)) if m else None

def parse_crafting_components(fvpa: str) -> List[Dict[str, Any]]:
    """Parse 'Circuitry:2 | Glass:2 | Gear:1 | Steel:2' into list of {item, count}."""
    result = []
    for part in re.split(r"\s*\|\s*", (fvpa or "").strip()):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^([^:]+):(\d+)$", part)
        if m:
            result.append({"item": m.group(1).strip(), "count": int(m.group(2))})
        else:
            result.append({"item": part, "count": 1})
    return result

# ---------------------------------------------------------------------------
# BOOK — plan data (tradeable, gold bullion, vendor)
# ---------------------------------------------------------------------------

def parse_book_for_plan(book_row: Optional[Dict[str, str]], glob_rows: List[Dict[str, str]]) -> Dict[str, Any]:
    """Extract plan name, tradeable flag, gold bullion price + vendor from BOOK row."""
    result: Dict[str, Any] = {
        "planName": None,
        "tradeable": None,
        "goldBullionPrice": None,
        "vendor": None,
        "vendorLvliEdid": None,
    }
    if not book_row:
        return result

    result["planName"] = clean_str(book_row.get("FULL") or "")

    # Tradeable check
    blob = " ".join(str(v) for v in book_row.values()).lower()
    non_trade = "nonplayertradeable" in blob or "nonplayertradable" in blob or "unsellableobject" in blob
    result["tradeable"] = not non_trade

    # BVGO -> GLOB -> price
    bvgo = clean_str(book_row.get("BVGO") or "")
    if bvgo:
        glob_fid = extract_glob_formid(bvgo)
        if glob_fid:
            price = glob_fltv(glob_rows, glob_fid)
            if price is not None:
                result["goldBullionPrice"] = int(price)

    # Vendor from Ref* -> LVLI with GoldVendor in EDID
    ref_count = safe_int(book_row.get("ReferencedByCount") or "0")
    for i in range(1, ref_count + 1):
        ref = clean_str(book_row.get(f"Ref{i}") or "")
        if ":LVLI" not in ref.upper():
            continue
        ref_edid_parts = ref.split(":")
        if len(ref_edid_parts) >= 2:
            edid_part = ref_edid_parts[1].lower()
            if "goldvendor" in edid_part:
                result["vendorLvliEdid"] = ref_edid_parts[1]
                # Extract vendor name
                for vk, vname in VENDOR_MAP.items():
                    if vk in edid_part:
                        result["vendor"] = vname
                        break
                if not result["vendor"]:
                    result["vendor"] = "a Gold Bullion Vendor"
                break

    return result

# ---------------------------------------------------------------------------
# Obtain method resolution
# ---------------------------------------------------------------------------

def resolve_obtain(
    entm_edid: str,
    entm_xalg: str,
    book_data: Dict[str, Any],
    cobj_condproxy: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Determine how to obtain an item.
    Returns dict with: method, seasonNumber, goldBullionPrice, vendor,
                       tradeable, planName, display, badge
    badge: "atom" | "scoreboard" | "gold" | "f1" | "default"
    """
    edid_l = (entm_edid or "").lower()
    edid_u = (entm_edid or "").upper()
    season = parse_season_from_edid(entm_edid)

    result: Dict[str, Any] = {
        "method": "unknown",
        "seasonNumber": season,
        "goldBullionPrice": book_data.get("goldBullionPrice"),
        "vendor": book_data.get("vendor"),
        "tradeable": book_data.get("tradeable"),
        "planName": book_data.get("planName"),
        "display": "",
        "badge": "default",
    }

    # Fallout 1st
    if "atx_f1_" in edid_l or "_f1_entm_" in edid_l:
        result["method"] = "f1"
        result["badge"] = "f1"
        result["display"] = "Free to claim from the Atom Shop for Fallout 1st members."
        return result

    # Atom Shop (pure ATX, non-season)
    if edid_u.startswith("ATX_") and not edid_u.startswith("ATX_COMMUNITY"):
        result["method"] = "atom"
        result["badge"] = "atom"
        result["display"] = "Purchase from the Atom Shop."
        return result

    # Community
    if "community" in edid_l or "atx_community" in edid_l:
        result["method"] = "community"
        result["badge"] = "default"
        result["display"] = "Awarded through a Bethesda community event or promotion."
        return result

    # Season Scoreboard
    if season:
        # Gold Bullion sub-path
        if result["goldBullionPrice"] and result["vendor"]:
            result["method"] = "gold"
            result["badge"] = "gold"
            price = result["goldBullionPrice"]
            vendor = result["vendor"]
            result["display"] = (
                f"Purchase from {vendor} for {price} Gold Bullion. "
                f"Requires unlocking Season {season} Scoreboard."
            )
        else:
            result["method"] = "scoreboard"
            result["badge"] = "scoreboard"
            result["display"] = f"Unlock via the Season {season} Scoreboard."
        return result

    # Default / base game
    result["method"] = "default"
    result["badge"] = "default"
    result["display"] = "Available in the base game."
    return result

# ---------------------------------------------------------------------------
# Subcategory for resource producers
# ---------------------------------------------------------------------------

def resource_subcategory(reso_edid: str, entm_desc: str) -> str:
    """Classify into 'food', 'junk', or 'other'."""
    blob = ((reso_edid or "") + " " + (entm_desc or "")).lower()

    for kw in FOOD_KEYWORDS:
        if kw in blob:
            return "food"
    for kw in JUNK_KEYWORDS:
        if kw in blob:
            return "junk"
    return "other"

# ---------------------------------------------------------------------------
# ENTM matching
# ---------------------------------------------------------------------------

def _name_token(edid: str) -> str:
    """Extract the 'unique name' part from an EDID for fuzzy matching."""
    s = (edid or "").lower()
    # Remove common prefixes
    for prefix in ("score_s\\d+_", "atx_f1_", "atx_"):
        s = re.sub(f"^{prefix}", "", s)
    # Remove ENTM/CAMP/Utility/Resource markers
    s = re.sub(r"^(entm_|camp_|utility_|resource_|resources?_|collector_|collectron_)+", "", s)
    s = re.sub(r"_(entm|camp|utility|resource|collector|collectron)_", "_", s)
    return s

def find_entm_for_reso(entm_rows: List[Dict[str, str]], reso_edid: str) -> Optional[Dict[str, str]]:
    """
    Find the best matching ENTM for a RESO entry by EDID token matching.
    Strategy:
    1. Extract unique name from RESO EDID
    2. Find ENTM containing both 'Collectron'/'Collector' and that name token
    """
    reso_tok = _name_token(reso_edid).replace("_", " ").strip()
    if not reso_tok:
        return None

    # Also try full suffix after "Collectron_" or "Collector_"
    collectron_suffix = ""
    m = re.search(r"(?:_Collectron_|_Collector_)(.+)$", reso_edid, re.IGNORECASE)
    if m:
        collectron_suffix = m.group(1).lower()

    best: Optional[Dict[str, str]] = None
    best_score = 0

    for r in entm_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid or starts_cut(edid):
            continue
        edid_l = edid.lower()

        # Must be a camp utility/collector/collectron ENTM
        if "_entm_" not in edid_l:
            continue
        if "camp" not in edid_l:
            continue

        score = 0

        # Exact suffix match
        if collectron_suffix and collectron_suffix in edid_l:
            score += 10

        # Token match (word by word)
        for word in reso_tok.split():
            if len(word) >= 4 and word in edid_l:
                score += 2

        # Prefer same season
        reso_season = parse_season_from_edid(reso_edid)
        entm_season = parse_season_from_edid(edid)
        if reso_season and reso_season == entm_season:
            score += 5
        elif reso_season and entm_season and reso_season != entm_season:
            score -= 3

        if score > best_score:
            best_score = score
            best = r

    return best if best_score >= 4 else None

def find_cont_via_avif(cont_rows: List[Dict[str, str]], avif_formid: str) -> Optional[Dict[str, str]]:
    """Find CONT that references a specific AVIF FormID in its properties."""
    if not avif_formid:
        return None
    avif_u = avif_formid.strip().upper()
    for r in cont_rows:
        props = " ".join(str(r.get(f"Prop{i}") or "") for i in range(1, 7)).upper()
        if avif_u in props:
            return r
    return None

def find_book_by_edid_token(book_rows: List[Dict[str, str]], name_token: str) -> Optional[Dict[str, str]]:
    """Find plan BOOK whose EDID or FULL contains the name token."""
    tok = (name_token or "").lower()
    for r in book_rows:
        edid = (r.get("EDID") or "").lower()
        full = (r.get("FULL") or "").lower()
        if tok in edid or tok in full:
            return r
    return None

# ---------------------------------------------------------------------------
# Image URL helper
# ---------------------------------------------------------------------------

def image_webp_url(entm_row: Optional[Dict[str, str]], subfolder: str) -> Optional[str]:
    """
    Build the final WEBP URL on the website.
    Convention: /wp-content/uploads/storefront/{subfolder}/{entm_edid_lower}.webp
    """
    if not entm_row:
        return None
    edid = (entm_row.get("EDID") or "").strip().lower()
    if not edid:
        return None
    return f"/wp-content/uploads/storefront/{subfolder}/{edid}.webp"

def image_carousel_urls(entm_row: Optional[Dict[str, str]], subfolder: str) -> List[str]:
    """Build carousel image URLs from ECIL_* fields."""
    if not entm_row:
        return []
    # ECIL_* fields contain DDS filenames (without path)
    folder = (entm_row.get("ETIP") or "").strip()  # e.g. Textures/ATX/Storefront/Camp/Utility/
    count = safe_int(entm_row.get("ECIL_Count") or "0")
    edid = (entm_row.get("EDID") or "").strip().lower()
    result = []
    for i in range(1, count + 1):
        dds = clean_str(entm_row.get(f"ECIL_{i}") or "")
        if dds:
            # Convert DDS filename to expected WEBP name
            base = os.path.splitext(dds)[0].lower()
            result.append(f"/wp-content/uploads/storefront/{subfolder}/{base}.webp")
    return result

def release_date_from_edid(edid: str) -> Optional[str]:
    """
    Best-effort release date from season number.
    Returns None if unknown — will be set to today by build script.
    """
    # Known season approximate dates (extend as needed)
    SEASON_DATES = {
        1:  "2020-06-30", 2:  "2020-09-15", 3:  "2020-12-15",
        4:  "2021-03-23", 5:  "2021-06-29", 6:  "2021-09-21",
        7:  "2021-12-14", 8:  "2022-04-19", 9:  "2022-07-05",
        10: "2022-10-04", 11: "2023-01-03", 12: "2023-04-04",
        13: "2023-07-18", 14: "2023-10-03", 15: "2024-01-23",
        16: "2024-04-30", 17: "2024-07-30", 18: "2024-10-08",
        19: "2025-01-28", 20: "2025-04-29", 21: "2025-07-29",
        22: "2025-10-28", 23: "2026-01-27", 24: "2026-04-29",
    }
    sn = parse_season_from_edid(edid)
    if sn and sn in SEASON_DATES:
        return SEASON_DATES[sn]
    edid_l = (edid or "").lower()
    if edid_l.startswith("atx_"):
        return None  # unknown ATX release date — will be today
    return None

def load_previous_release_dates(dist_path: str) -> Dict[str, str]:
    if not dist_path or not os.path.exists(dist_path):
        return {}
    try:
        with open(dist_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: Dict[str, str] = {}
        for it in (data.get("items") or []):
            fid = (str(it.get("formId") or "")).strip().upper()
            rd = (str(it.get("releaseDate") or "")).strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", rd):
                out[fid] = rd
        return out
    except Exception:
        return {}

# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_item(
    reso_row: Dict[str, str],
    entm_row: Optional[Dict[str, str]],
    cont_row: Optional[Dict[str, str]],
    cobj_row: Optional[Dict[str, str]],
    book_row: Optional[Dict[str, str]],
    glob_rows: List[Dict[str, str]],
    lvli_entry_rows: List[Dict[str, str]],
    is_collectron: bool,
    subfolder: str,
    prev_release_dates: Dict[str, str],
    today: str,
) -> Optional[Dict[str, Any]]:

    reso_edid = clean_str(reso_row.get("EDID") or "")
    reso_fid = clean_str(reso_row.get("FormID") or "")

    if starts_cut(reso_edid):
        return None

    # --- Display name + description from ENTM (preferred) or CONT ---
    if entm_row:
        display_name = clean_str(entm_row.get("FULL") or entm_row.get("NNAM") or "")
        description = clean_desc(entm_row.get("DESC") or "")
    elif cont_row:
        display_name = clean_str(cont_row.get("FULL") or "")
        description = ""
    else:
        display_name = reso_edid  # last resort
        description = ""

    if not display_name:
        return None

    # --- Production interval from GLOB ---
    interval_hours: Optional[float] = None
    interval_str: Optional[str] = None
    nam4 = clean_str(reso_row.get("NAM4_Interval") or "")
    if nam4:
        glob_fid = extract_glob_formid(nam4)
        if glob_fid:
            fltv = glob_fltv(glob_rows, glob_fid)
            if fltv is not None:
                interval_hours = fltv
                interval_str = interval_display(fltv)

    # --- Drop table from LVLI ---
    drops: List[Dict[str, Any]] = []
    lvli_fid_raw = clean_str(reso_row.get("NAM2_Produce") or "")
    if lvli_fid_raw:
        lvli_fid = extract_formid_from_ref(lvli_fid_raw)
        if lvli_fid:
            drops = compute_drop_table(lvli_fid, lvli_entry_rows)

    # --- CONT properties ---
    cont_props = parse_cont_properties(cont_row) if cont_row else {
        "capacity": None, "powerRequired": False, "flamingoUnits": None, "lockable": None
    }

    # --- COBJ crafting components ---
    components: List[Dict[str, Any]] = []
    if cobj_row:
        fvpa = clean_str(cobj_row.get("FVPA") or "")
        components = parse_crafting_components(fvpa)

    # --- BOOK plan info ---
    book_data = parse_book_for_plan(book_row, glob_rows)

    # --- Obtain method ---
    entm_edid = clean_str(entm_row.get("EDID") or reso_edid) if entm_row else reso_edid
    entm_xalg = clean_str(entm_row.get("XALG") or "") if entm_row else ""
    condproxy = None  # could add CondProxy lookup here
    obtain = resolve_obtain(entm_edid, entm_xalg, book_data, condproxy)

    # --- Images ---
    main_image = image_webp_url(entm_row, subfolder)
    carousel = image_carousel_urls(entm_row, subfolder)
    image_dds = clean_str(entm_row.get("ETDI") or "") if entm_row else ""
    image_folder = clean_str(entm_row.get("ETIP") or "") if entm_row else ""

    # --- Release date ---
    fid8_up = formid8(reso_fid)
    if fid8_up in prev_release_dates:
        release_date = prev_release_dates[fid8_up]
    else:
        release_date = release_date_from_edid(entm_edid) or today

    # --- Season number (for display) ---
    season = parse_season_from_edid(entm_edid)

    # --- Subcategory (resource producers only) ---
    subcategory: Optional[str] = None
    if not is_collectron:
        subcategory = resource_subcategory(reso_edid, description)

    cont_fid = clean_str(cont_row.get("FormID") or "") if cont_row else ""
    entm_fid = clean_str(entm_row.get("FormID") or "") if entm_row else ""

    item: Dict[str, Any] = {
        "formId": reso_fid,
        "edid": reso_edid,
        "contFormId": cont_fid,
        "entmFormId": entm_fid,
        "displayName": display_name,
        "description": description,
        "imageUrl": main_image,
        "imageCarousel": carousel,
        "imageDds": image_dds,
        "imageFolder": image_folder,
        "production": {
            "intervalHours": interval_hours,
            "intervalDisplay": interval_str,
            "drops": drops,
        },
        "station": {
            "capacity": cont_props["capacity"],
            "powerRequired": cont_props["powerRequired"],
            "flamingoUnits": cont_props["flamingoUnits"],
            "lockable": cont_props["lockable"],
        },
        "crafting": {
            "components": components,
        },
        "howToObtain": obtain,
        "seasonNumber": season,
        "releaseDate": release_date,
        "cutContent": starts_cut(reso_edid),
    }

    if not is_collectron:
        item["subcategory"] = subcategory

    return item

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Build collectrons + resource producers JSON")
    ap.add_argument("--tsv-root", default=None,
                    help="Root folder for TSV auto-discovery (e.g. dfbnb-data/tsv)")
    ap.add_argument("--reso",  action="append")
    ap.add_argument("--cont",  action="append")
    ap.add_argument("--entm",  action="append")
    ap.add_argument("--cobj",  action="append")
    ap.add_argument("--book",  action="append")
    ap.add_argument("--lvli-list",    action="append")
    ap.add_argument("--lvli-entries", action="append")
    ap.add_argument("--glob",  action="append")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    root = args.tsv_root

    reso_paths  = _autofill(root, args.reso,  ["**/RESO_Export*.tsv"])
    cont_paths  = _autofill(root, args.cont,  ["**/CONT_Export*.tsv"])
    entm_paths  = _autofill(root, args.entm,  ["**/ENTM_Export*.tsv"])
    cobj_paths  = _autofill(root, args.cobj,  ["**/COBJ_Export*.tsv"])
    book_paths  = _autofill(root, args.book,  ["**/BOOK_Export*.tsv"])
    lvli_list_paths    = _autofill(root, args.lvli_list,    ["**/*LVLI*List*.tsv"])
    lvli_entry_paths   = _autofill(root, args.lvli_entries, ["**/*LVLI*Entries*.tsv", "**/*LVLI*Math*.tsv"])
    glob_paths  = _autofill(root, args.glob,  ["**/GLOB_Export*.tsv"])

    missing = []
    for name, paths in [("RESO", reso_paths), ("CONT", cont_paths),
                        ("ENTM", entm_paths), ("COBJ", cobj_paths),
                        ("BOOK", book_paths), ("LVLI Entries", lvli_entry_paths)]:
        if not paths:
            missing.append(name)

    if missing:
        print(f"[WARN] Missing TSV sources: {', '.join(missing)}", file=sys.stderr)

    if not glob_paths:
        print("[WARN] No GLOB TSV found — production intervals and Gold Bullion prices will be null.", file=sys.stderr)

    # Load all rows
    print("Loading TSVs...", file=sys.stderr)
    reso_rows        = load_tsvs(reso_paths)
    cont_rows        = load_tsvs(cont_paths)
    entm_rows        = load_tsvs(entm_paths)
    cobj_rows        = load_tsvs(cobj_paths)
    book_rows        = load_tsvs(book_paths)
    lvli_entry_rows  = load_tsvs(lvli_entry_paths)
    glob_rows        = load_tsvs(glob_paths)

    print(f"  RESO: {len(reso_rows)}  CONT: {len(cont_rows)}  ENTM: {len(entm_rows)}  "
          f"COBJ: {len(cobj_rows)}  BOOK: {len(book_rows)}  "
          f"LVLI_Entries: {len(lvli_entry_rows)}  GLOB: {len(glob_rows)}", file=sys.stderr)

    os.makedirs(args.outdir, exist_ok=True)

    today = today_ymd()
    prev_col_dates  = load_previous_release_dates(os.path.join(args.outdir, "collectrons.json"))
    prev_res_dates  = load_previous_release_dates(os.path.join(args.outdir, "resource_producers.json"))

    # Build AVIF → CONT lookup
    print("Building AVIF → CONT index...", file=sys.stderr)
    # For each CONT, extract its property AVIF FormIDs
    avif_to_cont: Dict[str, Dict[str, str]] = {}
    for r in cont_rows:
        edid = (r.get("EDID") or "").strip()
        if starts_cut(edid):
            continue
        # Only consider station-type containers (have WorkshopCollectorObject or similar)
        kw_blob = " ".join(str(r.get(f"KW{i}") or "") for i in range(1, 12)).lower()
        if "workshop" not in kw_blob and "collectron" not in edid.lower() and "collector" not in edid.lower():
            continue
        prop_blob = " ".join(str(r.get(f"Prop{i}") or "") for i in range(1, 7))
        for m in re.finditer(r"ActorValue=([0-9A-Fa-f]{8}):", prop_blob):
            avif_to_cont[m.group(1).upper()] = r

    # Build CONT FormID → COBJ lookup
    cont_to_cobj: Dict[str, Dict[str, str]] = {}
    for r in cobj_rows:
        cnam = (r.get("CNAM_FormID") or "").strip().upper()
        if cnam:
            cont_to_cobj[cnam] = r

    # Build BOOK FormID index
    book_by_fid = build_index(book_rows, "FormID")

    collectron_items: List[Dict[str, Any]] = []
    resource_items:   List[Dict[str, Any]] = []

    print("Processing RESO entries...", file=sys.stderr)
    for reso_row in reso_rows:
        reso_edid = clean_str(reso_row.get("EDID") or "")
        reso_fid  = clean_str(reso_row.get("FormID") or "")

        if not reso_edid or starts_cut(reso_edid):
            continue

        # Filter: must be a resource/collectron station RESO
        # RESO EDID pattern: ATX_Resource_*, SCORE_S##_Resource_*
        if not re.search(r"_Resource_", reso_edid, re.IGNORECASE):
            continue

        is_collectron = bool(re.search(r"Collectron", reso_edid, re.IGNORECASE))

        # Find CONT via AVIF
        avif_fid = extract_avif_formid(clean_str(reso_row.get("NAM1_ActorValue") or ""))
        cont_row = avif_to_cont.get(avif_fid, None) if avif_fid else None

        # Find ENTM by token matching
        entm_row = find_entm_for_reso(entm_rows, reso_edid)

        # Find COBJ for CONT
        cobj_row = None
        if cont_row:
            cont_fid = (cont_row.get("FormID") or "").strip().upper()
            cobj_row = cont_to_cobj.get(cont_fid)

        # Find BOOK (the plan/recipe)
        book_row: Optional[Dict[str, str]] = None
        if cobj_row:
            gnam_fid = (cobj_row.get("GNAM_FormID") or "").strip().upper()
            if gnam_fid:
                book_row = book_by_fid.get(gnam_fid)
        if not book_row:
            # Try by name token
            m = re.search(r"(?:_Collectron_|_Collector_|_Resource_)(.+)$", reso_edid, re.IGNORECASE)
            if m:
                tok = m.group(1).lower()
                book_row = find_book_by_edid_token(book_rows, tok)

        subfolder = "camp-items-collectrons" if is_collectron else "camp-items-resource-producers"
        prev_dates = prev_col_dates if is_collectron else prev_res_dates

        item = build_item(
            reso_row, entm_row, cont_row, cobj_row, book_row,
            glob_rows, lvli_entry_rows, is_collectron, subfolder, prev_dates, today
        )

        if item is None:
            continue

        if is_collectron:
            collectron_items.append(item)
        else:
            resource_items.append(item)

    # Sort by display name (cut content last)
    def _sort_key(x: Dict[str, Any]) -> Tuple:
        return (x.get("cutContent", False), (x.get("displayName") or "").lower())

    collectron_items.sort(key=_sort_key)
    resource_items.sort(key=_sort_key)

    # Write outputs
    col_out = {
        "generatedAt": now_iso(),
        "type": "collectrons",
        "count": len(collectron_items),
        "items": collectron_items,
    }
    res_out = {
        "generatedAt": now_iso(),
        "type": "resource_producers",
        "count": len(resource_items),
        "items": resource_items,
    }

    col_path = os.path.join(args.outdir, "collectrons.json")
    res_path = os.path.join(args.outdir, "resource_producers.json")

    with open(col_path, "w", encoding="utf-8") as f:
        json.dump(col_out, f, ensure_ascii=False, indent=2)
    with open(res_path, "w", encoding="utf-8") as f:
        json.dump(res_out, f, ensure_ascii=False, indent=2)

    print(f"[OK] collectrons.json: {len(collectron_items)} items", file=sys.stderr)
    print(f"[OK] resource_producers.json: {len(resource_items)} items", file=sys.stderr)
    print(f"[OK] Written to: {args.outdir}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
