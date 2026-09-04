from __future__ import annotations

"""
DF/BNB - Camp Items JSON Builder
Produces:
  dist/collectrons.json          -> /bnb/camp-items/collectrons/collectrons/
  dist/resource_producers.json   -> /bnb/camp-items/resource-producers/food|junk|other/
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from patchlog_utils import write_patchlog_feed, diff_item_lists

# Shared drop-rate engine — single source of truth. This module used to carry
# its own standalone copy of the LVLI resolver; it now delegates to rng76.py
# (the same engine build_drop_rates.py uses) so the two can never drift.
try:
    from rng76 import Rng76Data, read_tsv_columns
except ImportError:
    _SRC_DIR = Path(__file__).resolve().parent
    if str(_SRC_DIR) not in sys.path:
        sys.path.insert(0, str(_SRC_DIR))
    from rng76 import Rng76Data, read_tsv_columns

import tsv_source          # one resolver for every export selection
import gold_vendor         # generative Gold Bullion route (ENTM -> vendor plan)
# Populated once in main() from the TSV root; resolve_drops_via_rng76() reads it.
_RNG_RESOLVER = None

# AVIF EDID (upper) -> FULL name. The AVIF FULL is the in-game label for what a
# station actually produces ("Junk", "Ore", "Party Supplies", "Holiday Cheer").
# Populated in main() from AVIF_Export_*.tsv; read by build_station_item.
_AVIF_FULL_BY_EDID = {}


def resource_name_for_reso(reso_row):
    """The produced-resource label for one RESO row, via its ActorValue -> AVIF FULL."""
    avif_edid = extract_avif_edid(clean_str(reso_row.get("NAM1_ActorValue") or ""))
    if not avif_edid:
        return ""
    return _AVIF_FULL_BY_EDID.get(avif_edid.strip().upper(), "")

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")
EXCLUDE_PATTERNS = re.compile(r"repair|repairbot", re.IGNORECASE)

FOOD_KEYWORDS = (
    "milk", "cake", "egg", "tea", "coffee", "candy", "popcorn", "beer",
    "pemmican", "apple", "turkey", "cookie", "mirelurk", "_food", "snack",
    "fruit", "brew", "honey", "soup", "spice", "seasoning", "stew", "muffin",
    "pie", "roast", "salmon", "noodle", "chili", "cheese", "tato", "razorgrain",
    "brahmin", "slocum", "oven", "kettle", "steamer", "edible",
)

JUNK_KEYWORDS = (
    "_wood", "lumber", "_oil", "flora", "_junk", "_scrap", "acid",
    "cement", "fertilizer", "nuclear", "_bone", "steel", "lead", "glass",
    "cloth", "ceramic", "leather", "copper", "aluminum", "plastic",
    "material", "dumpster", "crate", "salvage", "military", "cargo",
    "apothecary", "butterfly", "morbid",
)

VENDOR_MAP = {
    "samuel": "Samuel", "regs": "Regs", "freeman": "Freeman",
    "radcliff": "Radcliff", "gold": "a Gold Bullion Vendor",
}

MODE_SUFFIXES = (
    "_MeleeAndAmmo", "_AlcoholAndChems", "_WeaponsAndAmmo",
    "_Proletariat", "_Revolutionary", "_Party", "_Treats", "_Junk", "_All",
)

MODE_DISPLAY_MAP = {
    "MeleeAndAmmo": "Weapons & Ammo", "WeaponsAndAmmo": "Weapons & Ammo",
    "AlcoholAndChems": "Alcohol & Chems", "Proletariat": "Proletariat",
    "Revolutionary": "Revolutionary", "Party": "Party Supplies",
    "Treats": "Treats", "Electronics": "Electronics",
    "Electronics_Junk": "Junkyard", "All": "All",
}

SEASON_DATES = {
    1: "2020-06-30", 2: "2020-09-15", 3: "2020-12-15",
    4: "2021-03-23", 5: "2021-06-29", 6: "2021-09-21",
    7: "2021-12-14", 8: "2022-04-19", 9: "2022-07-05",
    10: "2022-10-04", 11: "2023-01-03", 12: "2023-04-04",
    13: "2023-07-18", 14: "2023-10-03", 15: "2024-01-23",
    16: "2024-04-30", 17: "2024-07-30", 18: "2024-10-08",
    19: "2025-01-28", 20: "2025-04-29", 21: "2025-07-29",
    22: "2025-10-28", 23: "2026-01-27", 24: "2026-04-29",
}

RE_RAND_PCT = re.compile(r"GetRandomPercent.*\)\s+[0-9A-Fa-f]+\s+(\d+\.\d+)\s*$", re.IGNORECASE)
RE_RAND_PCT_GLOB_EDID = re.compile(r"GetRandomPercent.*\)\s+[0-9A-Fa-f]+\s+(\S+)\s*$", re.IGNORECASE)
RE_GLOB_IN_COND = re.compile(r"\[GLOB:([0-9A-Fa-f]{8})\]", re.IGNORECASE)

# --- Season theme lookup (for scoreboard wording) ---
# Loaded once in main() from fallout76_seasons.tsv.
_SEASON_THEMES: Dict[int, str] = {}

def _load_season_themes(tsv_root: str):
    """Load season number → theme name from fallout76_seasons.tsv."""
    global _SEASON_THEMES
    path = os.path.join(tsv_root, "fallout76_seasons.tsv")
    if not os.path.isfile(path):
        print("[WARN] fallout76_seasons.tsv not found — scoreboard wording will be generic.", file=sys.stderr)
        return
    for row in read_tsv(path):
        sn = safe_int(row.get("SeasonNumber"))
        name = (row.get("SeasonName") or "").strip()
        if sn and name:
            # Drop leading "The" per camp-item-expands skill spec
            if name.lower().startswith("the "):
                name = name[4:]
            _SEASON_THEMES[sn] = name

def now_iso():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

def today_ymd():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

def safe_float(s, default=None):
    try: return float(str(s or "").strip())
    except Exception: return default

def safe_int(s, default=0):
    try: return int(str(s or "").strip())
    except Exception: return default

def starts_cut(edid):
    e = (edid or "").strip().upper()
    return any(e.startswith(p) for p in CUT_PREFIXES)

def is_excluded(edid):
    if starts_cut(edid): return True
    if EXCLUDE_PATTERNS.search(edid or ""): return True
    return False

def item_status(edid, modes=None, has_reso=True):
    """Three states, per the house rule:
         cut        - the record is explicitly retired (zzz / CUT / DEL prefix)
         unreleased - it exists as an entitlement but has no RESO record and no
                      leveled list behind it, so nothing in game produces from it
         live       - everything else
    """
    if starts_cut(edid): return "cut"
    if not has_reso: return "unreleased"
    if modes is not None and not any((m or {}).get("lvliFormId") for m in modes):
        return "unreleased"
    return "live"

# Images already uploaded elsewhere on the site are REUSED, never duplicated —
# storage costs money. The Atom Shop "request an item" set already hosts main
# tiles for several collectrons; keyed on ENTM EDID so the match is exact.
_REUSABLE_IMAGE_BY_EDID = {}

def load_reusable_images(outdir):
    idx = {}
    for fname in ("atom_shop.json", "bundles.json"):
        try:
            with open(os.path.join(outdir, fname), encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        for it in data.get("items", []):
            e = clean_str(it.get("edid") or "")
            u = clean_str(it.get("imageUrl") or "")
            if e and u and u.lower().endswith((".avif", ".webp", ".png", ".jpg")):
                idx.setdefault(e.upper(), u)
    return idx

def clean_str(s):
    if s is None: return ""
    s = str(s).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return re.sub(r"\s{2,}", " ", s).strip()

def clean_desc(s):
    s = clean_str(s)
    s = re.sub(r"\s*-\s*C\.A\.M\.P\. ITEMS APPEAR.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*THIS ITEM APPEARS WHILE.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*This item cannot be built inside (?:of )?a Shelter\.?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*AVAILABLE FOR GOLD BULLION.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*APPAREL IS CRAFTABLE.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return re.sub(r"\s*-\s*$", "", s).strip()

def formid8(s):
    s = (s or "").strip().upper().replace("0X", "")
    if len(s) > 8: s = s[-8:]
    return s.zfill(8)

def extract_formid_from_ref(ref_str):
    m = re.match(r"^([0-9A-Fa-f]{8}):", (ref_str or "").strip())
    return m.group(1).upper() if m else None

def extract_ref_parts(ref_str):
    parts = (ref_str or "").strip().split(":")
    fid = parts[0].upper() if parts else ""
    name = parts[1] if len(parts) >= 2 else ""
    rectype = parts[2] if len(parts) >= 3 else ""
    return (fid, name, rectype)

def extract_avif_edid(field):
    if not field: return None
    field = field.strip()
    m = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)\s', field)
    if m: return m.group(1)
    parts = field.split(":")
    if len(parts) >= 3 and parts[2].strip().upper() == "AVIF": return parts[1].strip()
    if len(parts) >= 2: return parts[1].strip()
    return None

def extract_glob_formid(field):
    if not field: return None
    m2 = re.search(r"\[GLOB:([0-9A-Fa-f]{8})\]", field)
    if m2: return m2.group(1).upper()
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    return m.group(1).upper() if m else None

def extract_lvli_formid(field):
    if not field: return None
    m2 = re.search(r"\[LVLI:([0-9A-Fa-f]{8})\]", field)
    if m2: return m2.group(1).upper()
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    return m.group(1).upper() if m else None

def parse_season_from_edid(edid):
    m = re.search(r"SCORE[_-]?S(\d+)[_-]", (edid or ""), re.IGNORECASE)
    return int(m.group(1)) if m else None

def prettify_edid_name(s):
    """Turn a raw RESO/EDID token into a readable display name for producers
    that have no ENTM/CONT FULL name (e.g. 'AdhesiveResource' -> 'Adhesive',
    'FoodPackagedResource01' -> 'Food Packaged')."""
    t = re.sub(r"^(ATX_F1_|ATX_|SCORE_S\d+_|SCORE_\w+?_|F1_|PETS_|W05_|SSE_|ENTM_)", "", s or "", flags=re.IGNORECASE)
    t = re.sub(r"_?(Resource\d*|Collector|Collectron|_resource)$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"_+", " ", t)
    t = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t or (s or "")

# --- Drop-name prettifier ---
# rng76 resolves each dropped FormID to its record FULL name (ALCH/MISC/WEAP/
# ARMO/…). Two record families can't resolve that way on the current export set,
# so rng76 falls back to a humanised EDID and the collectron loot tables show a
# raw-looking token instead of the real in-game name:
#   * AMMO — no AMMO_Export_*.tsv is published, so ammo shows as "Ammo556" etc.
#   * a couple of base crafting components humanised as "c Xxx".
# Mirror the reward pages (see _AMMO_DISPLAY in build_activities_rewards_json.py)
# and give those a clean display name. We ONLY rewrite names that are still a
# humanised fallback (start with "Ammo"/"c ") so a genuine FULL name is never
# clobbered, and we never invent a name — anything that can't be mapped cleanly
# keeps its honest cleaned-EDID fallback and is reported to stderr.
_AMMO_DISPLAY = {
    "Ammo10mm":          "10mm Round",
    "Ammo2mmEC":         "2mm Electromagnetic Cartridge",
    "Ammo308Caliber":    ".308 Round",
    "Ammo38Caliber":     ".38 Round",
    "Ammo44":            ".44 Round",
    "Ammo45Caliber":     ".45 Round",
    "Ammo50Caliber":     ".50 Round",
    "Ammo50CaliberBall": ".50 Caliber Ball",
    "Ammo556":           "5.56 Round",
    "Ammo5mm":           "5mm Round",
    "AmmoRRSpike":       "Railway Spike",
}

_UNRESOLVED_DROP_NAMES = set()

def prettify_drop_name(name, edid, formid=""):
    """Clean a humanised ammo/component fallback into a real in-game name.
    Leaves an already-resolved FULL name untouched (only acts on names that are
    still a humanised EDID fallback)."""
    n = (name or "").strip()
    e = (edid or "").strip()
    # AMMO family: rng76 humanises the EDID because no AMMO export is loaded.
    if n.lower().startswith("ammo"):
        if e in _AMMO_DISPLAY:
            return _AMMO_DISPLAY[e]
        base = re.sub(r"^Ammo[_ ]*", "", e or n)
        base = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", base).replace("_", " ")
        base = re.sub(r"\s{2,}", " ", base).strip()
        if base:
            return base
        _UNRESOLVED_DROP_NAMES.add((formid, e, n))
        return n
    # Base crafting component humanised as "c Xxx" (from a c_ EDID).
    if re.match(r"^c\s", n):
        base = re.sub(r"^c[_ ]", "", e or n)
        base = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", base).replace("_", " ")
        base = re.sub(r"\s{2,}", " ", base).strip()
        if base:
            return base
    return n

# --- TSV loading ---
def read_tsv(path):
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        return [dict(r) for r in csv.DictReader(f, delimiter="\t")]

MONTH_ORD = {"jan":1,"feb":2,"mar":3,"march":3,"apr":4,"april":4,"may":5,"jun":6,"june":6,
    "jul":7,"july":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def _tsv_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)

def _find_latest_tsv(tsv_root, glob_pattern):
    import glob as globmod
    hits = globmod.glob(os.path.join(tsv_root, glob_pattern), recursive=True)
    if not hits: return None
    return sorted(hits, key=_tsv_date_key, reverse=True)[0]

def load_latest_tsv(tsv_root, given, glob_pattern, columns=None):
    """Load the newest export matching *glob_pattern*.

    *columns* narrows the read to the named columns. Use it on very wide
    exports — GLOB carries one "RefN" back-reference column per referencing
    record (5,500+ of them) and reading it whole costs ~1.6 GB, against a few
    MB for the three columns this script actually uses.
    """
    def _read(path):
        return read_tsv_columns(path, columns) if columns else read_tsv(path)
    if given:
        rows = []
        for p in given:
            if os.path.isfile(p): rows.extend(_read(p))
        return rows
    if not tsv_root: return []
    path = _find_latest_tsv(tsv_root, glob_pattern)
    return _read(path) if path else []

# --- Index builders ---
def build_index(rows, key_field):
    idx = {}
    for r in rows:
        k = (r.get(key_field) or "").strip().upper()
        if k: idx[k] = r
    return idx

def build_multi_index(rows, key_field):
    idx = {}
    for r in rows:
        k = (r.get(key_field) or "").strip().upper()
        if k: idx.setdefault(k, []).append(r)
    return idx

def glob_fltv(glob_index, glob_formid):
    r = glob_index.get((glob_formid or "").strip().upper())
    return safe_float(r.get("FLTV") or r.get("DATA") or "") if r else None

def glob_fltv_by_edid(glob_edid_index, edid):
    r = glob_edid_index.get((edid or "").strip())
    return safe_float(r.get("FLTV") or r.get("DATA") or "") if r else None

def interval_display(hours):
    total_seconds = hours * 3600
    mins = int(total_seconds // 60)
    secs = round(total_seconds % 60)
    if mins == 0: return "{} sec".format(secs)
    if secs == 0: return "{} min".format(mins)
    return "{} min {} sec".format(mins, secs)

# --- LVLI drop rate calculation (delegated to rng76.py) ---
# This module previously carried a standalone re-implementation of the engine
# (waterfall / pick-one / first-match / independent). That copy diverged from
# rng76.py — it never read the ChanceNoneCurve column, had no MinLvl-GLOB guard
# and no list-level ChanceNone — so it could emit wrong rates. Per the
# drop-rate-engine skill's "Standalone Copy Rule", produce LVLIs are now
# resolved through the shared rng76 engine (one source of truth with
# build_drop_rates.py). The rng76 resolver also reads a separate LVLI Math TSV,
# which the old copy did not.
def resolve_drops_via_rng76(resolver, lvli_formid):
    """Resolve a produce LVLI to [{item, name, formId, chance}] via rng76.
    `chance` is a 0-100 percentage. Returns [] when the list is unresolved or
    the engine is unavailable."""
    fid = (lvli_formid or "").strip().upper()
    if not fid or resolver is None:
        return []
    out = []
    for it in resolver.resolve_deep(fid):
        rate = float(it.get("dropRate") or 0.0)
        edid = it.get("edid") or ""
        _fid = (it.get("formid") or "").upper()
        out.append({
            "item":   edid or it.get("name") or it.get("formid") or "",
            "name":   prettify_drop_name(it.get("name") or "", edid, _fid),
            "formId": _fid,
            "chance": round(rate * 100.0, 5),
        })
    return out

def consolidate_drops(drops):
    merged = {}
    for d in drops:
        key = d["formId"]
        if key in merged:
            # Use max instead of sum: duplicate entries represent quantity, not extra probability
            merged[key]["chance"] = round(max(merged[key]["chance"], d["chance"]), 5)
        else:
            merged[key] = dict(d)
    result = sorted(merged.values(), key=lambda x: (-x["chance"], (x.get("item") or "").lower()))
    for r in result: r.pop("recType", None)
    return result

# --- CONT ---
def parse_cont_properties(cont_row):
    result = {"capacity": None, "powerRequired": False, "flamingoUnits": None, "lockable": None}
    kw_blob = " ".join(str(cont_row.get("KW{}".format(i)) or "") for i in range(1, 12)).lower()
    if "workshopcanpowered" in kw_blob or "workshopcanbepowered" in kw_blob:
        result["powerRequired"] = True
    if "containertakeonly" in kw_blob:
        result["lockable"] = True
    prop_count = safe_int(cont_row.get("PropertyCount") or "0")
    for i in range(1, min(prop_count, 6) + 1):
        av = (cont_row.get("Prop_{}_AV".format(i)) or "").strip().lower()
        val = safe_float(cont_row.get("Prop_{}_Val".format(i)) or "")
        if av == "carryweight" and val is not None: result["capacity"] = int(val)
        elif av == "workshopbudgetobjectmultiplier" and val is not None: result["flamingoUnits"] = int(val)
        elif av == "powerrequired" and val is not None and val > 0: result["powerRequired"] = True
    return result

def extract_cont_avif_edids(cont_row):
    edids = set()
    prop_count = safe_int(cont_row.get("PropertyCount") or "0")
    for i in range(1, min(prop_count, 6) + 1):
        av = (cont_row.get("Prop_{}_AV".format(i)) or "").strip()
        if av and "ResourceAV" in av: edids.add(av)
    return edids

def build_avif_edid_to_cont(cont_rows):
    idx = {}
    for r in cont_rows:
        if starts_cut((r.get("EDID") or "").strip()): continue
        for ae in extract_cont_avif_edids(r): idx[ae.upper()] = r
    return idx

# --- COBJ ---
def _parse_fvpa_qty(raw):
    """Failsafe for curve-table-driven component counts. Returns (qty, scaled):
    a positive int for a fixed count, or (None, True) when the count is
    curve-driven (exported as 0 / blank / non-numeric). Keeps a stray
    level-scaled material from rendering as a misleading ×0 / ×1."""
    try:
        n = int(str(raw).strip())
    except (ValueError, TypeError):
        return None, True
    return (n, False) if n > 0 else (None, True)


def parse_crafting_components(fvpa):
    result = []
    for part in re.split(r"\s*\|\s*", (fvpa or "").strip()):
        part = part.strip()
        if not part: continue
        tokens = part.split(":")
        name = tokens[0].strip()
        if name.startswith("c_"): name = name[2:]
        count, scaled = _parse_fvpa_qty(tokens[1]) if len(tokens) >= 2 else (1, False)
        if name:
            if scaled: result.append({"item": name, "count": None, "scaled": True})
            else:      result.append({"item": name, "count": count})
    return result


def fvpa_to_array(fvpa):
    """Parse COBJ FVPA string to [{"name": "...", "qty": N}, ...].
    Curve-driven counts emit qty=None + scaled=True (failsafe)."""
    result = []
    for part in re.split(r"\s*\|\s*", (fvpa or "").strip()):
        part = part.strip()
        if not part: continue
        tokens = part.split(":")
        name = tokens[0].strip()
        if name.startswith("c_"): name = name[2:]
        name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
        qty, scaled = _parse_fvpa_qty(tokens[1]) if len(tokens) >= 2 else (1, False)
        if name:
            if scaled: result.append({"name": name, "qty": None, "scaled": True})
            else:      result.append({"name": name, "qty": qty})
    return result

def build_cobj_cnam_index(cobj_rows):
    idx = {}
    for r in cobj_rows:
        cnam = (r.get("CNAM_FormID") or "").strip().upper()
        if cnam and cnam != "00000000": idx[cnam] = r
    return idx


# Grouped workshop items (exercise equipment, containers, decor, etc.) keep
# their components on a shared recipe whose CNAM is a leveled list, while the
# per-item COBJ/CondProxy row carries only the plan name. _COBJ_FVPA_BY_CNAM
# maps every CNAM FormID that owns components to its FVPA so an empty-FVPA row
# can borrow them via its ReferencedBy LVLI(s). _CATEGORY_RECIPE_FVPA maps a
# shared category recipe's CNAM EDID (e.g. ATX_workshop_LL_Collectrons) to its
# FVPA — every collectron is an entry in that leveled list and shares the one
# recipe, but the per-item rows don't carry it, so we assign by category.
# Populated once in main().
_COBJ_FVPA_BY_CNAM = {}
_CATEGORY_RECIPE_FVPA = {}
# Workshop CAMP-build recipes matched by EDID name when there's no structural
# link from the item to its recipe (collectors / resource producers keep the
# recipe on a created-object FormID, not the container). Each entry is
# (name_key, fvpa). Only recipes crafted at the CAMP build bench
# (BNAM WorkshopWorkbench*) are eligible, which excludes cooking/brewing/chem
# recipes that merely share the item's name.
_WORKSHOP_RECIPES = []

# Structural EDID words to drop before name-matching. Only structural/category
# tokens — never product or material words, so e.g. "BrahminMilkMachine" and
# "BabeBrahmin" don't both collapse to "brahmin".
_RECIPE_NOISE = set((
    "score workshop co camp entm reso resource resources collector collectron "
    "utility generators generator decorations decoration containers container "
    "empty atx community f1 the copy condproxy cond proxy category "
    "categoryresources goldvendor vendor w05 babylon"
).split())
_RECIPE_MIN_KEY = 7  # min shared key length to accept a name match


def recipe_name_key(edid):
    """Distinctive lower-case name key for a COBJ/item EDID: split camelCase,
    drop structural words and numeric tokens, keep the rest joined."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", edid or "")
    s = re.sub(r"[^A-Za-z0-9]", " ", s).lower()
    out = []
    for w in s.split():
        if w in _RECIPE_NOISE:
            continue
        if re.fullmatch(r"s?\d+|w\d+|\d+mm|\d+|\d+caliber|556", w):
            continue
        if len(w) >= 3:
            out.append(w)
    return "".join(out)


def init_cobj_lvli_index(cobj_rows):
    _COBJ_FVPA_BY_CNAM.clear()
    _CATEGORY_RECIPE_FVPA.clear()
    _WORKSHOP_RECIPES.clear()
    for r in cobj_rows:
        cnam = (r.get("CNAM_FormID") or "").strip().upper()
        fv = (r.get("FVPA") or "").strip()
        if cnam and fv:
            _COBJ_FVPA_BY_CNAM.setdefault(cnam, fv)
        cnam_edid = (r.get("CNAM_EDID") or "").strip()
        if cnam_edid and fv:
            _CATEGORY_RECIPE_FVPA.setdefault(cnam_edid, fv)
    # Second pass (needs the LVLI index above for effective_fvpa).
    for r in cobj_rows:
        if "WorkshopWorkbench" not in (r.get("BNAM_EDID") or ""):
            continue
        fv = effective_fvpa(r)
        if not fv:
            continue
        k = recipe_name_key(r.get("COBJ_EDID") or "")
        if len(k) >= _RECIPE_MIN_KEY:
            _WORKSHOP_RECIPES.append((k, fv))


def match_workshop_recipe(*edids):
    """FVPA of the CAMP-build recipe whose EDID name best matches any of the
    item's EDIDs (entm / reso / cont). Returns '' if nothing clears the
    threshold. Best = longest shared key, tie-broken by closest length so a
    specific recipe wins over a generic one."""
    best_fv, best_len, best_diff = "", 0, 999
    for edid in edids:
        ik = recipe_name_key(edid)
        if len(ik) < _RECIPE_MIN_KEY:
            continue
        for rk, fv in _WORKSHOP_RECIPES:
            if ik in rk or rk in ik:
                L = min(len(ik), len(rk))
                diff = abs(len(ik) - len(rk))
                if L > best_len or (L == best_len and diff < best_diff):
                    best_fv, best_len, best_diff = fv, L, diff
    return best_fv


# CNAM EDID of the shared leveled-list recipe every collectron is built from.
COLLECTRON_RECIPE_CNAM_EDID = "ATX_workshop_LL_Collectrons"


def collectron_shared_fvpa():
    """FVPA shared by all collectrons (Circuitry/Copper/Gears/Steel), resolved
    from the COBJ whose CNAM is the collectron leveled list. '' if not found."""
    return _CATEGORY_RECIPE_FVPA.get(COLLECTRON_RECIPE_CNAM_EDID, "")


def refby_lvli_fids(cobj_row):
    """FormIDs of LVLI leveled lists referencing this COBJ row, from
    ReferencedBy_Flat + Ref_1..Ref_37. Entry format '<FID>:<EDID>:<TYPE>'."""
    fids = []
    blobs = [cobj_row.get("ReferencedBy_Flat", "")]
    blobs += [cobj_row.get("Ref_{}".format(i), "") for i in range(1, 38)]
    for blob in blobs:
        for piece in (blob or "").split("|"):
            bits = piece.split(":")
            if len(bits) >= 3 and bits[2].strip() == "LVLI" and bits[0].strip():
                fids.append(bits[0].strip().upper())
    return fids


def effective_fvpa(cobj_row):
    """FVPA for a COBJ row: its own, else borrowed from the shared LVLI recipe
    (CONDPROXY -> ReferencedBy LVLI -> COBJ whose CNAM == that LVLI)."""
    if not cobj_row:
        return ""
    fv = clean_str(cobj_row.get("FVPA") or "").strip()
    if fv:
        return fv
    for fid in refby_lvli_fids(cobj_row):
        if fid in _COBJ_FVPA_BY_CNAM:
            return _COBJ_FVPA_BY_CNAM[fid]
    return ""

# --- BOOK ---
def parse_book_for_plan(book_row, glob_index):
    result = {"planName": None, "tradeable": None, "goldBullionPrice": None, "vendor": None}
    if not book_row: return result
    result["planName"] = clean_str(book_row.get("FULL") or "")
    blob = " ".join(str(v) for v in book_row.values()).lower()
    result["tradeable"] = not ("nonplayertradeable" in blob or "nonplayertradable" in blob or "unsellableobject" in blob)
    bvgo = clean_str(book_row.get("BVGO") or "")
    if bvgo:
        gf = extract_glob_formid(bvgo)
        if gf:
            price = glob_fltv(glob_index, gf)
            if price is not None: result["goldBullionPrice"] = int(price)
    ref_count = safe_int(book_row.get("ReferencedByCount") or "0")
    for i in range(1, min(ref_count, 30) + 1):
        ref = clean_str(book_row.get("Ref{}".format(i)) or book_row.get("Ref_{}".format(i)) or "")
        if ":LVLI" not in ref.upper(): continue
        rp = ref.split(":")
        if len(rp) >= 2 and "goldvendor" in rp[1].lower():
            for vk, vn in VENDOR_MAP.items():
                if vk in rp[1].lower(): result["vendor"] = vn; break
            if not result["vendor"]: result["vendor"] = "a Gold Bullion Vendor"
            break
    return result

# --- Obtain ---
def resolve_obtain(entm_edid, book_data):
    edid_l = (entm_edid or "").lower()
    season = parse_season_from_edid(entm_edid)
    result = {"method": "unknown", "seasonNumber": season, "goldBullionPrice": book_data.get("goldBullionPrice"),
        "vendor": book_data.get("vendor"), "tradeable": book_data.get("tradeable"),
        "planName": book_data.get("planName"), "display": "", "badge": "default"}
    if "atx_f1_" in edid_l or "_f1_entm_" in edid_l:
        result.update(method="f1", badge="f1", display="Free to claim from the Atom Shop for Fallout 1st members.")
        return result
    if "community" in edid_l:
        result.update(method="community", display="Awarded through a Bethesda community event or promotion.")
        return result
    if season:
        theme = _SEASON_THEMES.get(season, "")
        sb_label = "{} Scoreboard (Season {})".format(theme, season) if theme else "Season {} Scoreboard".format(season)
        if result["goldBullionPrice"] and result["vendor"]:
            result.update(method="gold", badge="gold",
                display="Purchase from {} for {} Gold Bullion. Requires unlocking the {}.".format(
                    result["vendor"], result["goldBullionPrice"], sb_label))
        else:
            # Camp-item-expands skill: S≤15 "Claim from", S≥16 "Purchase with tickets from"
            if season <= 15:
                result.update(method="scoreboard", badge="scoreboard",
                    display="Claim from the {}".format(sb_label))
            else:
                result.update(method="scoreboard", badge="scoreboard",
                    display="Purchase with tickets from the {}".format(sb_label))
        return result
    if (entm_edid or "").upper().startswith("ATX_"):
        result.update(method="atom", badge="atom", display="Purchase from the Atom Shop.")
        return result
    result.update(method="default", display="Available in the base game.")
    return result

def _thousands(value):
    """1250 -> "1,250". Left untouched when the value is not a plain number."""
    try:
        return "{:,}".format(int(str(value).replace(",", "").strip()))
    except (ValueError, TypeError):
        return str(value)


# --- 9-route obtain routes (camp-item-expands spec) ---
OBTAIN_ROUTE_ORDER = [
    "Caps", "Stamps", "Scoreboard", "Gold Bullion",
    "Atom Shop", "Limited Time Bundle", "Events & Activities",
    "Quests", "Challenges",
]

def build_obtain_routes(obtain, book_data):
    """Build the 9-route obtainRoutes array per the camp-item-expands skill spec.
    Each route: {route, populated, lines[], tradeable, dropRate}."""
    method = obtain.get("method", "unknown")
    tradeable = obtain.get("tradeable")
    routes = []
    for route_label in OBTAIN_ROUTE_ORDER:
        entry = {"route": route_label, "populated": False, "lines": [], "tradeable": None, "dropRate": None}
        if route_label == "Scoreboard" and method == "scoreboard":
            entry["populated"] = True
            entry["lines"] = [obtain.get("display", "")]
            entry["tradeable"] = tradeable
            entry["dropRate"] = "N/A"
        elif route_label == "Gold Bullion" and method == "gold":
            entry["populated"] = True
            # One labelled line per fact ("Plan: X" / "Vendor: Y" / "Cost: Z") —
            # the renderer splits these into aligned sub-rows. See the
            # camp-item-expands skill, "Route detail formats".
            lines = []
            plan = obtain.get("planName")
            # GNAM_FULL values usually already read "Plan: X", so strip the
            # leading "Plan:" rather than doubling it — otherwise the route
            # renders "Plan: Plan: Auto-Miner Collectron".
            if plan:
                plan = re.sub(r"^\s*plan\s*:\s*", "", str(plan), flags=re.I).strip()
                if plan:
                    lines.append("Plan: {}".format(plan))
            vendor = obtain.get("vendor", "")
            if vendor: lines.append("Vendor: {}".format(vendor))
            price = obtain.get("goldBullionPrice")
            if price: lines.append("Cost: {} Gold Bullion".format(_thousands(price)))
            if not lines: lines = [obtain.get("display", "")]
            entry["lines"] = lines
            entry["tradeable"] = tradeable
            entry["dropRate"] = "N/A"
        elif route_label == "Atom Shop" and method in ("atom", "f1"):
            entry["populated"] = True
            entry["lines"] = [obtain.get("display", "Can be purchased with certain bundles from the Atom Shop.")]
            entry["tradeable"] = tradeable
            entry["dropRate"] = "N/A"
        elif route_label == "Events & Activities" and method == "community":
            entry["populated"] = True
            entry["lines"] = [obtain.get("display", "")]
            entry["tradeable"] = tradeable
            entry["dropRate"] = "N/A"
        elif route_label == "Caps" and method == "default":
            # Base game items typically available via plan vendor
            pass  # leave as N/A unless we have specific vendor data
        routes.append(entry)
    return routes

# --- Subcategory ---
def resource_subcategory(reso_edid, entm_desc):
    blob = ((reso_edid or "") + " " + (entm_desc or "")).lower()
    for kw in FOOD_KEYWORDS:
        if kw in blob: return "food"
    for kw in JUNK_KEYWORDS:
        if kw in blob: return "junk"
    return "other"

# --- Output-type buckets (resource generators) ---
# Root expands on the Resource Generators page are grouped by what a producer
# OUTPUTS. The bucket is derived generatively from the produced items (majority
# vote), with a producer-EDID fallback for stations that resolve to no drops.
# Empty buckets are still rendered (handled on the front end).
BUCKET_ORDER = [
    "Raw Ingredients", "Cooked Dishes", "Candy", "Drinks",
    "Dirty Water", "Boiled Water", "Purified Water", "Toxic Goo",
    "Ammo", "Chems", "Scrap & Crafting Materials", "Misc Junk",
]

_CHEM_ITEMS = {
    "stimpak", "stimpakdiluted", "superstimpak", "radaway", "radx", "psycho",
    "psychobuff", "psychotats", "buffout", "bufftats", "mentats", "berrymentats",
    "grapementats", "orangementats", "medx", "daytripper", "fury", "overdrive",
    "xcell", "calmex", "daddyo", "bloodpack", "bloodpackirradiated", "healingsalve",
    "ratpoison",
}
_CANDY_ITEMS = {"sugarbombs", "fancyladsnackcakes", "dandyboyapples"}
_DRINK_TOKENS = ("brew_", "whiskey", "bourbon", "beer", "lager", "pilsner", "ale_",
    "nukacola", "nukavictory", "nukaquantum", "_tea", "companytea", "coffee")

def _item_bucket(edid):
    """Classify a single produced item EDID into one output-type bucket."""
    el = (edid or "").strip().lower()
    base = re.sub(r"_prewar.*$", "", el)
    base = re.sub(r"_clean$", "", base)
    # Waters / goo
    if "watertoxicgoo" in el: return "Toxic Goo"
    if "waterdirty" in el: return "Dirty Water"
    if "waterboiled" in el: return "Boiled Water"
    if "waterpurified" in el: return "Purified Water"
    # Ammo (incl. fusion-core and plasma-core rechargers)
    if el.startswith("ammo") or "fusioncore" in el or "plasmacore" in el: return "Ammo"
    # Candy
    if "candy" in el or base in _CANDY_ITEMS: return "Candy"
    # Drinks (soda + alcohol + tea/coffee)
    if any(t in el for t in _DRINK_TOKENS): return "Drinks"
    # Chems
    if base in _CHEM_ITEMS or "stimpak" in el or "mentats" in el or "bloodpack" in el:
        return "Chems"
    # Cooked dishes / prepared food
    if any(t in el for t in ("cooked", "jerky", "gourmet", "soup", "tasty", "hotdog",
            "smores", "pemmican", "popcorn", "cakeslice", "pieslice", "mudcookie",
            "macandcheese", "instamash", "salisburysteak", "dogfood", "slimecake")):
        return "Cooked Dishes"
    # Raw ingredients
    if "scrap" not in el and any(t in el for t in ("meat", "herb", "_egg", "egg_",
            "milk", "honey", "cookingflavor", "cookingoil", "_food", "fishing_fish_meal",
            "fishing_bait", "flora", "fruit", "mothmanegg")):
        return "Raw Ingredients"
    # Scrap / crafting materials (incl. smelted ore)
    if (el.startswith("c_") and el.endswith("_scrap")) or "chem_smelting_ore" in el or "_scrap" in el:
        return "Scrap & Crafting Materials"
    return "Misc Junk"

def classify_producer_bucket(drops, *edids):
    """Bucket a producer by majority vote over its produced items; fall back to
    producer-EDID keywords when the station resolves to no drops."""
    from collections import Counter
    votes = Counter(_item_bucket(d.get("item") or "") for d in (drops or []))
    if votes:
        return sorted(votes.items(), key=lambda kv: (-kv[1], BUCKET_ORDER.index(kv[0])))[0][0]
    blob = " ".join(e for e in edids if e).lower()
    if "steamboiler" in blob or "boiler" in blob: return "Boiled Water"
    if "dirtywater" in blob: return "Dirty Water"
    if "purifiedwater" in blob or "waterresource" in blob or "cooler" in blob: return "Purified Water"
    if "toxicgoo" in blob: return "Toxic Goo"
    if "ammo" in blob: return "Ammo"
    if "food" in blob or "meat" in blob: return "Raw Ingredients"
    if "scrap" in blob or "junk" in blob or "ore" in blob: return "Scrap & Crafting Materials"
    return "Misc Junk"

# --- Image + release date ---
# Collectron art was re-cut as .avif and now lives under
# /wp-content/uploads/guide-images/camp-items/collectrons/, named after the ENTM
# texture handles rather than the ENTM EDID: the main tile is the ETDI stem plus
# the "_l" (large) suffix the storefront textures carry, and each carousel frame
# is its ECIL stem. Resource producers still point at the older
# /uploads/storefront/... .webp set — leave them until that art is redone too.
IMAGE_SETS = {
    "collectrons": {
        "base": "/wp-content/uploads/guide-images/camp-items/collectrons",
        "ext": ".avif",
        "main_from": "etdi",
    },
}

def _image_set(subfolder):
    return IMAGE_SETS.get(subfolder, {
        "base": "/wp-content/uploads/storefront/{}".format(subfolder),
        "ext": ".webp",
        "main_from": "edid",
    })

def image_webp_url(entm_row, subfolder):
    if not entm_row: return None
    # Reuse an image already hosted elsewhere on the site before inventing a new
    # path — no point uploading the same tile twice.
    reuse = _REUSABLE_IMAGE_BY_EDID.get(clean_str(entm_row.get("EDID") or "").upper())
    if reuse: return reuse
    cfg = _image_set(subfolder)
    if cfg["main_from"] == "etdi":
        stem = os.path.splitext(clean_str(entm_row.get("ETDI") or ""))[0].lower()
        if not stem: return None
        # A few ETDI handles already carry the "_l" (large) suffix — SirLoin is
        # one — so only append it when it isn't there, or the name doubles up
        # into "..._sirloin_l_l".
        if not stem.endswith("_l"): stem += "_l"
        return "{}/{}{}".format(cfg["base"], stem, cfg["ext"])
    edid = (entm_row.get("EDID") or "").strip().lower()
    return "{}/{}{}".format(cfg["base"], edid, cfg["ext"]) if edid else None

def split_ecil(entm_row):
    """Carousel frame names for one ENTM row.

    The export packs EVERY frame into ECIL_1 as one unseparated run of .dds
    names ("..._C1.dds..._C2.dds..._C3.dds"), which is why the carousel URLs
    used to come out concatenated into a single broken path. Split them back
    apart on the .dds boundary, and still read the numbered columns in case a
    future export starts filling them properly."""
    names = []
    for i in range(1, safe_int(entm_row.get("ECIL_Count") or "0") + 1):
        raw = clean_str(entm_row.get("ECIL_{}".format(i)) or "")
        if raw:
            names.extend(m.group(0) for m in re.finditer(r".+?\.dds", raw, re.IGNORECASE))
    seen, out = set(), []
    for n in names:
        if n.lower() not in seen:
            seen.add(n.lower()); out.append(n)
    return out

def image_carousel_urls(entm_row, subfolder):
    if not entm_row: return []
    cfg = _image_set(subfolder)
    return ["{}/{}{}".format(cfg["base"], os.path.splitext(n)[0].lower(), cfg["ext"])
            for n in split_ecil(entm_row)]

def release_date_from_edid(edid):
    sn = parse_season_from_edid(edid)
    return SEASON_DATES.get(sn) if sn else None

def load_previous_release_dates(dist_path):
    if not dist_path or not os.path.exists(dist_path): return {}
    try:
        with open(dist_path, "r", encoding="utf-8") as f: data = json.load(f)
        out = {}
        for it in (data.get("items") or []):
            fid = str(it.get("formId") or "").strip().upper()
            rd = str(it.get("releaseDate") or "").strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", rd): out[fid] = rd
        return out
    except Exception: return {}

# --- RESO filtering ---
# The bare ScrapResource* family (ScrapResourceGold, ScrapResourceCopper,
# ScrapResourceCircuitry, ...) are raw junk-scrap actor values, NOT stations.
# They were being grouped as collectrons, which is where the bogus "Scavenging
# Station" entry came from — it was ScrapResourceGold, while the real gold
# station (ATX_Resource_Collectron_Gold) fell off the page entirely.
RAW_SCRAP_RESOURCE = re.compile(r"^(ScrapResource|ScavengeResource)", re.IGNORECASE)

def is_camp_resource(reso_edid):
    edid_u = (reso_edid or "").strip().upper()
    if RAW_SCRAP_RESOURCE.match((reso_edid or "").strip()):
        return False
    # Standard ATX / SCORE / F1 prefixed camp items
    if re.match(r"^(ATX_|SCORE_S\d+_|ATX_F1_)", edid_u):
        if any(kw in edid_u for kw in ("RESOURCE", "COLLECTRON", "COLLECTOR", "BEEHIVE",
                                         "CHICKENCOOP", "BRAHMIN", "MORBIDWELL", "TOXICBOB",
                                         "PEPPINO", "SIRLOIN", "LIBERATED", "EVIDENCE")):
            return True
    # Pets resource producers
    if re.match(r"^PETS_", edid_u) and "RESOURCE" in edid_u: return True
    # Catch-all for any RESO with "Resource" or "Collector" anywhere in EDID
    if "RESOURCE" in edid_u or "COLLECTOR" in edid_u or "COLLECTRON" in edid_u:
        return True
    # Explicit whitelist for edge cases
    if edid_u in ("ATX_MORBIDWELL",): return True
    return False

def is_collectron_edid(reso_edid):
    return bool(re.search(r"Collectron", reso_edid or "", re.IGNORECASE))

# --- ENTM matching ---
def find_entm_via_cont_refs(cont_row, entm_formid_index):
    for i in range(1, min(safe_int(cont_row.get("ReferencedByCount") or "0"), 1225) + 1):
        ref = clean_str(cont_row.get("Ref_{}".format(i)) or "")
        if ref and ":ENTM" in ref.upper():
            fid = extract_formid_from_ref(ref)
            if fid and fid in entm_formid_index: return entm_formid_index[fid]
    return None

def find_entm_via_cobj_refs(cont_formid, cobj_rows, entm_formid_index):
    cont_fid = (cont_formid or "").strip().upper()
    if not cont_fid: return None
    for r in cobj_rows:
        if (r.get("CNAM_FormID") or "").strip().upper() != cont_fid: continue
        all_refs = []
        rf = clean_str(r.get("ReferencedBy_Flat") or "")
        if rf: all_refs.append(rf)
        for i in range(1, min(safe_int(r.get("ReferencedByCount") or "0"), 37) + 1):
            ref = clean_str(r.get("Ref_{}".format(i)) or "")
            if ref: all_refs.append(ref)
        for ref_str in all_refs:
            for seg in ref_str.split("|"):
                seg = seg.strip()
                if ":ENTM" in seg.upper():
                    fid = extract_formid_from_ref(seg)
                    if fid and fid in entm_formid_index: return entm_formid_index[fid]
    return None

def _name_token(edid):
    s = (edid or "").lower()
    for prefix in (r"score_s\d+_", r"atx_f1_", r"atx_community\d*_\w+_", r"atx_"):
        s = re.sub("^{}".format(prefix), "", s)
    s = re.sub(r"^(entm_|camp_|utility_|resource_|resources?_|collector_|collectron_)+", "", s)
    return re.sub(r"_(entm|camp|utility|resource|collector|collectron)_", "_", s)

# Collectron entitlement EDIDs are highly regular:
#   {prefix}_ENTM_CAMP_Utility_Collectron_{Token}
# so a direct token index beats the fuzzy scorer below, which was failing for 13
# of them — and a failed ENTM match meant no FULL name (the page fell back to a
# prettified RESO EDID, e.g. "Peppino Collectron Station" instead of "Peppino the
# Clown Collectron Station") AND no imageUrl at all.
_COLLECTRON_ENTM_TOKEN = re.compile(r"_ENTM_CAMP_Utility_Collectron_(.+)$", re.IGNORECASE)

def build_collectron_entm_index(entm_rows):
    """token(lower) -> ENTM row. Cut/dupe rows are indexed too; callers decide."""
    idx = {}
    for r in entm_rows:
        m = _COLLECTRON_ENTM_TOKEN.search(clean_str(r.get("EDID") or ""))
        if not m: continue
        tok = m.group(1).strip().lower()
        if "duplicate" in tok:            # ATX_..._GoldBotDUPLICATE000 carries the WRONG FULL
            continue
        # A live row always beats a zzz-prefixed cut row for the same token.
        if tok in idx and starts_cut(clean_str(r.get("EDID") or "")):
            continue
        idx[tok] = r
    return idx

# Station EDIDs that don't share any token with their entitlement.
COLLECTRON_TOKEN_ALIASES = {
    "scrapall": "scavenger",   # ATX_Resource_Scrap_CollectronAll -> Scavenger
    "scrap":    "scavenger",
}

# Generic words that carry no identity — stripped before tokenising.
_GENERIC_TOKENS = {"atx", "score", "zzz", "entm", "camp", "utility", "resource",
                   "resourceav", "collectron", "collector", "station", "f1",
                   "community2020", "atlas", "co"}

def _norm_token(s):
    """Comparison form: lowercase, letters+digits only. Collapses the
    camelCase/underscore mismatch between MrFarmhand and mr_farmhand."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _token_candidates(src):
    """Every contiguous run of identity-bearing words in an EDID, longest first.
    Splits on underscores AND camelCase so ATX_PeppinoResource_Collector and
    ATX_Resource_Collectron_BoS_MissionSupplies both yield usable tokens."""
    s = re.sub(r"_?S\d+_", "_", src or "", flags=re.IGNORECASE)
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)          # camelCase -> snake
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", s) if p]
    kept = [p.lower() for p in parts if p.lower() not in _GENERIC_TOKENS]
    out = []
    for i in range(len(kept)):
        for j in range(len(kept), i, -1):
            out.append(_norm_token("".join(kept[i:j])))
    return [t for t in sorted(set(out), key=len, reverse=True) if t]

def find_entm_via_collectron_token(reso_edid, cont_edid, entm_token_idx):
    """Match a station to its entitlement by token, longest candidate first.

    Matching is EXACT on the normalised token, with one narrow allowance for the
    "...Bot" entitlement suffix (station says 'Gold', entitlement says 'GoldBot').
    Loose prefix matching is deliberately NOT used: it silently bound Red Rocket
    to Auto-Miner on 'auto' and Nuka Quantum to Nuka-Cola on 'nuka'."""
    norm_idx = {}
    for k, r in entm_token_idx.items():
        norm_idx.setdefault(_norm_token(k), r)
    cands = []
    for src in (cont_edid or "", reso_edid or ""):
        cands.extend(_token_candidates(src))
    for t in sorted(set(cands), key=len, reverse=True):
        t = _norm_token(COLLECTRON_TOKEN_ALIASES.get(t, t))
        if len(t) < 3: continue
        for probe in (t, t + "bot", t + "collectron", t + "station"):
            if probe in norm_idx: return norm_idx[probe]
    return None

def build_entitlement_only_items(entm_token_idx, covered_edids, today):
    """Collectron entitlements with NO RESO record behind them.

    They exist in the store data but nothing in game produces from them, so the
    RESO-driven grouping above can never emit them. Surface them anyway, flagged
    per the house rule: zzz/CUT/DEL prefix -> cut, otherwise -> unreleased."""
    out = []
    for _tok, r in sorted(entm_token_idx.items()):
        edid = clean_str(r.get("EDID") or "")
        if not edid or edid.upper() in covered_edids: continue
        name = clean_str(r.get("FULL") or "")
        if not name: continue
        fid = clean_str(r.get("FormID") or "")
        status = "cut" if starts_cut(edid) else "unreleased"
        # Even entitlement-only (cut/unreleased) collectrons must carry the same
        # obtainRoutes + buildInfo shape as the RESO-backed ones, or the camp-items
        # JSON contract (verify_camp_items_json.py, which requires non-empty
        # 'obtainRoutes' and 'buildInfo' on every collectron) fails the build.
        # Derive them from the ENTM EDID we have; there is no RESO/container here,
        # so buildInfo uses the standard defaults (1 per camp, non-powered, no
        # flamingo cost) and the obtain routes come out all-N/A unless the EDID
        # itself names an Atom Shop / F1 / scoreboard source.
        _book_data = {"planName": None, "tradeable": None,
                      "goldBullionPrice": None, "vendor": None}
        _obtain = resolve_obtain(edid, _book_data)
        _obtain_routes = build_obtain_routes(_obtain, _book_data)
        _build_info = ("Build Limit per Camp: {}\nBuild Limit per Workshop: {}\n"
                       "Power Required: {}\nFlamingo Units: {}").format(1, 0, "No", "—")
        out.append({
            "formId": fid, "edid": edid,
            "resoFormId": "", "contFormId": "", "entmFormId": fid,
            "displayName": name,
            "description": clean_desc(r.get("DESC") or ""),
            "imageUrl": image_webp_url(r, "collectrons"),
            "imageCarousel": image_carousel_urls(r, "collectrons"),
            "imageDds": clean_str(r.get("ETDI") or ""),
            "imageFolder": clean_str(r.get("ETIP") or ""),
            "isCollectron": True,
            "production": {"intervalHours": None, "intervalDisplay": None,
                           "resourceName": "", "resourceEdid": "",
                           "drops": [], "modes": []},
            "station": {"capacity": None, "powerRequired": False,
                        "flamingoUnits": None, "lockable": None},
            "crafting": {"components": []}, "craftingRequirements": [],
            "howToObtain": _obtain, "obtainRoutes": _obtain_routes,
            "buildInfo": _build_info,
            "seasonNumber": _obtain.get("seasonNumber"),
            "cutContent": status == "cut",
            "status": status,
            "releaseDate": "",
        })
    return out

def find_entm_by_token_match(entm_rows, reso_edid):
    reso_tok = _name_token(reso_edid).replace("_", " ").strip()
    if not reso_tok: return None
    collectron_suffix = ""
    m = re.search(r"(?:_Collectron_?|_Collector_?|_Resource_?)(.+)$", reso_edid, re.IGNORECASE)
    if m: collectron_suffix = m.group(1).lower().rstrip("_").replace("_resource", "").replace("_collector", "")
    best, best_score = None, 0
    for r in entm_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid or starts_cut(edid): continue
        edid_l = edid.lower()
        if "_entm_" not in edid_l or "camp" not in edid_l: continue
        score = 0
        if collectron_suffix and collectron_suffix in edid_l: score += 10
        for word in reso_tok.split():
            if len(word) >= 4 and word in edid_l: score += 2
        rs = parse_season_from_edid(reso_edid)
        es = parse_season_from_edid(edid)
        if rs and rs == es: score += 5
        elif rs and es and rs != es: score -= 3
        if score > best_score: best_score, best = score, r
    return best if best_score >= 4 else None

# --- Terminal mode grouping ---
def detect_mode_name(reso_edid):
    m = re.search(r"(?:Collectron|Collector)_([A-Za-z_]+?)(?:_resource)?$", (reso_edid or "").strip(), re.IGNORECASE)
    if m:
        suffix = m.group(1)
        if suffix in MODE_DISPLAY_MAP: return MODE_DISPLAY_MAP[suffix]
        name = re.sub(r"([a-z])([A-Z])", r"\1 \2", suffix)
        return name.replace("_", " ").replace("And", "&").strip()
    return "Default"

def _reso_station_base(reso_edid):
    edid = (reso_edid or "").strip()
    for suffix in MODE_SUFFIXES:
        if edid.endswith(suffix): return edid[:-len(suffix)]
    return edid

def group_reso_by_station(reso_rows, avif_edid_to_cont):
    eligible = []
    for reso_row in reso_rows:
        reso_edid = clean_str(reso_row.get("EDID") or "")
        if not reso_edid or is_excluded(reso_edid) or not is_camp_resource(reso_edid): continue
        avif_edid = extract_avif_edid(clean_str(reso_row.get("NAM1_ActorValue") or ""))
        cont_row = avif_edid_to_cont.get((avif_edid or "").upper()) if avif_edid else None
        cont_fid = (cont_row.get("FormID") or "").strip().upper() if cont_row else None
        eligible.append((_reso_station_base(reso_edid), cont_fid, reso_row))
    base_to_cont = {}
    for base_edid, cont_fid, _ in eligible:
        if cont_fid and base_edid not in base_to_cont: base_to_cont[base_edid] = cont_fid
    groups = {}
    for base_edid, cont_fid, reso_row in eligible:
        gk = cont_fid or base_to_cont.get(base_edid) or "BASE_{}".format(base_edid)
        groups.setdefault(gk, []).append(reso_row)
    return groups

# --- Core builder ---
def build_station_item(reso_rows, cont_row, entm_row, cobj_row, book_row,
    glob_index, glob_edid_index, entries_index, list_index, is_collectron, subfolder, prev_release_dates, today):
    primary_reso = reso_rows[0]
    for r in reso_rows:
        edid = clean_str(r.get("EDID") or "")
        if not re.search(r"_(MeleeAndAmmo|AlcoholAndChems|WeaponsAndAmmo|Proletariat|Revolutionary|Party|Treats|Electronics_Junk|All)$", edid, re.IGNORECASE):
            primary_reso = r; break
    primary_edid = clean_str(primary_reso.get("EDID") or "")
    primary_fid = clean_str(primary_reso.get("FormID") or "")
    if entm_row:
        display_name = clean_str(entm_row.get("FULL") or entm_row.get("NNAM") or "")
        description = clean_desc(entm_row.get("DESC") or "")
    elif cont_row:
        display_name = clean_str(cont_row.get("FULL") or "")
        description = ""
    else:
        display_name, description = prettify_edid_name(primary_edid), ""
    if not display_name: return None
    modes = []
    for reso_row in reso_rows:
        re_edid = clean_str(reso_row.get("EDID") or "")
        re_fid = clean_str(reso_row.get("FormID") or "")
        mode_name = detect_mode_name(re_edid) if len(reso_rows) > 1 else "Default"
        lvli_fid = extract_lvli_formid(clean_str(reso_row.get("NAM2_Produce") or ""))
        drops = consolidate_drops(resolve_drops_via_rng76(_RNG_RESOLVER, lvli_fid)) if lvli_fid else []
        modes.append({"name": mode_name, "resoFormId": re_fid, "lvliFormId": lvli_fid or "",
                      "resourceName": resource_name_for_reso(reso_row), "drops": drops})
    interval_hours, interval_str = None, None
    nam4 = clean_str(primary_reso.get("NAM4_Interval") or "")
    if nam4:
        gfid = extract_glob_formid(nam4)
        if gfid:
            fltv = glob_fltv(glob_index, gfid)
            if fltv is not None: interval_hours, interval_str = fltv, interval_display(fltv)
    cont_props = parse_cont_properties(cont_row) if cont_row else {"capacity": None, "powerRequired": False, "flamingoUnits": None, "lockable": None}
    _cobj_fvpa = effective_fvpa(cobj_row)
    if not _cobj_fvpa and is_collectron:
        # Every collectron is built from the one shared leveled-list recipe; the
        # per-item COBJ rows don't carry the FVPA, so assign it by category.
        _cobj_fvpa = collectron_shared_fvpa()
    if not _cobj_fvpa:
        # Collectors / resource producers keep their recipe on a created-object
        # FormID, not the container, so cobj_idx misses them. Match the CAMP
        # build recipe by EDID name (workshop bench only — see match_workshop_recipe).
        _cobj_fvpa = match_workshop_recipe(
            clean_str(entm_row.get("EDID") or "") if entm_row else "",
            primary_edid,
            clean_str(cont_row.get("EDID") or "") if cont_row else "",
        )
    components = parse_crafting_components(_cobj_fvpa) if _cobj_fvpa else []
    book_data = parse_book_for_plan(book_row, glob_index)
    entm_edid = clean_str(entm_row.get("EDID") or primary_edid) if entm_row else primary_edid
    obtain = resolve_obtain(entm_edid, book_data)
    obtain_routes = build_obtain_routes(obtain, book_data)
    cont_fid = clean_str(cont_row.get("FormID") or "") if cont_row else ""
    entm_fid = clean_str(entm_row.get("FormID") or "") if entm_row else ""
    fid_key = formid8(entm_fid or primary_fid)
    release_date = prev_release_dates.get(fid_key) or release_date_from_edid(entm_edid) or today
    # Flat drops list for backward compat: merge all mode drops, dedup by formId
    flat_drops = []
    seen_drop_fids = set()
    for m in modes:
        for d in m.get("drops", []):
            if d["formId"] not in seen_drop_fids:
                seen_drop_fids.add(d["formId"])
                flat_drops.append(d)
    flat_drops.sort(key=lambda d: -d.get("chance", 0))
    # What the station produces, as the game labels it (AVIF FULL). Prefer the
    # primary RESO; multi-mode stations whose every RESO carries a mode suffix
    # (Fasnacht Party/Treats) fall back to the first mode that resolved a name.
    resource_edid = (extract_avif_edid(clean_str(primary_reso.get("NAM1_ActorValue") or "")) or "")
    resource_name = resource_name_for_reso(primary_reso)
    if not resource_name:
        resource_name = next((m.get("resourceName") for m in modes if m.get("resourceName")), "")
    season_num = obtain.get("seasonNumber")
    item = {
        "formId": entm_fid or primary_fid, "edid": entm_edid,
        "resoFormId": primary_fid, "contFormId": cont_fid,
        "entmFormId": entm_fid,
        "displayName": display_name, "description": description,
        "imageUrl": image_webp_url(entm_row, subfolder),
        "imageCarousel": image_carousel_urls(entm_row, subfolder),
        "imageDds": clean_str(entm_row.get("ETDI") or "") if entm_row else "",
        "imageFolder": clean_str(entm_row.get("ETIP") or "") if entm_row else "",
        "isCollectron": is_collectron,
        "production": {
            "intervalHours": interval_hours, "intervalDisplay": interval_str,
            "resourceName": resource_name, "resourceEdid": resource_edid,
            "drops": flat_drops, "modes": modes,
        },
        "station": cont_props,
        "crafting": {"components": components},
        "craftingRequirements": fvpa_to_array(_cobj_fvpa) if _cobj_fvpa else [],
        "howToObtain": obtain,
        "obtainRoutes": obtain_routes,
        "buildInfo": "Build Limit per Camp: {}\nBuild Limit per Workshop: {}\nPower Required: {}\nFlamingo Units: {}".format(
            1, 0,
            "Yes" if cont_props.get("powerRequired") else "No",
            cont_props.get("flamingoUnits") or "—",
        ),
        "seasonNumber": season_num,
        "releaseDate": release_date,
        "cutContent": starts_cut(primary_edid),
        "status": item_status(primary_edid, modes, has_reso=True),
    }
    if not is_collectron:
        item["subcategory"] = resource_subcategory(primary_edid, description)
        item["bucket"] = classify_producer_bucket(flat_drops, primary_edid, entm_edid)
    return item

# --- Main ---
def main():
    ap = argparse.ArgumentParser(description="Build collectrons + resource producers JSON")
    ap.add_argument("--tsv-root", default=None)
    ap.add_argument("--reso", action="append"); ap.add_argument("--cont", action="append")
    ap.add_argument("--entm", action="append"); ap.add_argument("--cobj", action="append")
    ap.add_argument("--book", action="append"); ap.add_argument("--lvli-list", action="append")
    ap.add_argument("--lvli-entries", action="append"); ap.add_argument("--glob", action="append")
    ap.add_argument("--avif", action="append")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    root = args.tsv_root
    print("Loading TSVs...", file=sys.stderr)
    reso_rows = load_latest_tsv(root, args.reso, "**/RESO_Export_*.tsv")
    cont_rows = load_latest_tsv(root, args.cont, "**/CONT_Export_*.tsv")
    entm_rows = load_latest_tsv(root, args.entm, "**/ENTM_Export_*.tsv")
    cobj_rows = load_latest_tsv(root, args.cobj, "**/COBJ_Export_*.tsv")
    book_rows = load_latest_tsv(root, args.book, "**/BOOK_Export_*.tsv")
    lvli_list_rows = load_latest_tsv(root, args.lvli_list, "**/*LVLI*List*.tsv")
    lvli_entry_rows = load_latest_tsv(root, args.lvli_entries, "**/*LVLI*Entries*.tsv")
    glob_rows = load_latest_tsv(root, args.glob, "**/GLOB_Export_*.tsv",
                                columns=("FormID", "EDID", "FLTV"))
    avif_rows = load_latest_tsv(root, args.avif, "**/AVIF_Export_*.tsv")
    # AVIF FULL is the produced-resource label shown on the item head pill.
    global _AVIF_FULL_BY_EDID
    _AVIF_FULL_BY_EDID = {}
    for r in avif_rows:
        ae = clean_str(r.get("EDID") or "")
        full = clean_str(r.get("FULL") or "")
        if ae and full:
            _AVIF_FULL_BY_EDID[ae.upper()] = full
    if not avif_rows:
        print("[WARN] Missing: AVIF (produced-resource names will be blank)", file=sys.stderr)
    for name, rows in [("RESO", reso_rows), ("CONT", cont_rows), ("ENTM", entm_rows),
                        ("COBJ", cobj_rows), ("BOOK", book_rows), ("LVLI Entries", lvli_entry_rows)]:
        if not rows: print("[WARN] Missing: {}".format(name), file=sys.stderr)
    print("  RESO:{} CONT:{} ENTM:{} COBJ:{} BOOK:{} LVLI_L:{} LVLI_E:{} GLOB:{}".format(
        len(reso_rows), len(cont_rows), len(entm_rows), len(cobj_rows),
        len(book_rows), len(lvli_list_rows), len(lvli_entry_rows), len(glob_rows)), file=sys.stderr)
    # Load season themes for scoreboard wording (≤15 "Claim from" / ≥16 "Purchase with tickets from")
    if root:
        _load_season_themes(root)
    # Build the shared rng76 resolver once from the same TSV root. Produce
    # LVLIs are resolved through this (see resolve_drops_via_rng76).
    global _RNG_RESOLVER
    if root:
        print("Loading rng76 drop-rate engine...", file=sys.stderr)
        _RNG_RESOLVER = Rng76Data.from_tsv_root(root).resolver
    else:
        print("[WARN] No --tsv-root; drop rates will be empty.", file=sys.stderr)
    os.makedirs(args.outdir, exist_ok=True)
    global _REUSABLE_IMAGE_BY_EDID
    _REUSABLE_IMAGE_BY_EDID = load_reusable_images(args.outdir)
    print("  reusable images found (already hosted): {}".format(len(_REUSABLE_IMAGE_BY_EDID)), file=sys.stderr)
    today = today_ymd()
    prev_col = load_previous_release_dates(os.path.join(args.outdir, "collectrons.json"))
    prev_res = load_previous_release_dates(os.path.join(args.outdir, "resource_producers.json"))
    print("Building indices...", file=sys.stderr)
    glob_index = build_index(glob_rows, "FormID")
    glob_edid_index = build_index(glob_rows, "EDID")
    list_index = build_index(lvli_list_rows, "LVLI_FormID")
    entries_index = build_multi_index(lvli_entry_rows, "LVLI_FormID")
    entm_fid_idx = build_index(entm_rows, "FormID")
    avif_to_cont = build_avif_edid_to_cont(cont_rows)
    collectron_entm_idx = build_collectron_entm_index(entm_rows)
    print("  collectron entitlements indexed: {}".format(len(collectron_entm_idx)), file=sys.stderr)
    cobj_idx = build_cobj_cnam_index(cobj_rows)
    init_cobj_lvli_index(cobj_rows)
    book_idx = build_index(book_rows, "FormID")
    cont_idx = build_index(cont_rows, "FormID")
    print("Grouping RESO...", file=sys.stderr)
    station_groups = group_reso_by_station(reso_rows, avif_to_cont)
    print("  {} groups, {} RESOs".format(len(station_groups), sum(len(v) for v in station_groups.values())), file=sys.stderr)
    col_items, res_items, seen = [], [], set()
    for cont_key, grp in station_groups.items():
        primary_edid = clean_str(grp[0].get("EDID") or "")
        cont_row = cont_idx.get(cont_key) if not cont_key.startswith("BASE_") else None
        if not cont_row:
            for rr in grp:
                ae = extract_avif_edid(clean_str(rr.get("NAM1_ActorValue") or ""))
                if ae:
                    cont_row = avif_to_cont.get(ae.upper())
                    if cont_row: break
        entm_row = None
        if cont_row: entm_row = find_entm_via_cont_refs(cont_row, entm_fid_idx)
        if cont_row and not entm_row:
            entm_row = find_entm_via_cobj_refs((cont_row.get("FormID") or "").strip().upper(), cobj_rows, entm_fid_idx)
        if not entm_row:
            # Direct collectron token index first — it is exact where the fuzzy
            # scorer below guesses (and often gave up entirely).
            entm_row = find_entm_via_collectron_token(
                primary_edid,
                clean_str(cont_row.get("EDID") or "") if cont_row else "",
                collectron_entm_idx)
        if not entm_row: entm_row = find_entm_by_token_match(entm_rows, primary_edid)
        cobj_row = cobj_idx.get((cont_row.get("FormID") or "").strip().upper()) if cont_row else None
        book_row = None
        if cobj_row:
            gf = (cobj_row.get("GNAM_FormID") or "").strip().upper()
            if gf and gf != "00000000": book_row = book_idx.get(gf)
        if not book_row:
            m = re.search(r"(?:_Collectron_|_Collector_|_Resource_)(.+?)(?:_resource)?$", primary_edid, re.IGNORECASE)
            if m:
                tok = m.group(1).lower()
                for br in book_rows:
                    if tok in (br.get("EDID") or "").lower() or tok in (br.get("FULL") or "").lower():
                        book_row = br; break
        # Collectron classification. Three positive signals, checked in order:
        #   S1  a RESO EDID literally contains "Collectron"
        #   S2  the ENTM is a "_Utility_Collectron_" record
        #   S3  the station container EDID names a "_Collectron_" station
        is_col = any(is_collectron_edid(clean_str(r.get("EDID") or "")) for r in grp)
        # S1 is loose. Bethesda ships a couple of STATIC resource generators whose
        # RESO EDID still contains "Collectron" but which are NOT robot collectrons:
        #   * 0067F3B2 ATX_Resource_Collectron_TreeSapBucket  -> "Tree Sap Collector"
        #   * 0068E77E ATX_Resource_Collectron_RadstagFieldDressingStation
        #                                       -> "Radstag Field Dressing Station"
        # Both sit on a bare "ATX_Collector_*" WorkshopCollectorObject container, not
        # on a Collectron Station, and produce a raw resource on an interval like any
        # generator. A genuine collectron's container EDID always names a Collectron
        # (…Collectron_Station_…, …RobotCollectron…, …CollectronStation…) — note
        # "Collector" is NOT a substring of "Collectron". So when S1 is the only
        # signal and the container is a bare Collector object (EDID has "Collector"
        # but NOT "Collectron"), demote it to the resource-producers page. This never
        # touches GoldBot, RedRocket, the FETCH Junkyard Dog or any real collectron —
        # they all carry a Collectron container (S3) or Collectron ENTM (S2).
        if is_col and cont_row:
            cont_edid = clean_str(cont_row.get("EDID") or "")
            if (re.search(r"Collector", cont_edid, re.IGNORECASE)
                    and not re.search(r"Collectron", cont_edid, re.IGNORECASE)):
                is_col = False
        if not is_col and entm_row:
            entm_edid = clean_str(entm_row.get("EDID") or "")
            if re.search(r"_Utility_Collectron_", entm_edid, re.IGNORECASE):
                is_col = True
        if not is_col and cont_row:
            cont_edid = clean_str(cont_row.get("EDID") or "")
            if re.search(r"_Collectron_", cont_edid, re.IGNORECASE):
                is_col = True
        sf = "collectrons" if is_col else "camp-items-resource-producers"
        item = build_station_item(grp, cont_row, entm_row, cobj_row, book_row,
            glob_index, glob_edid_index, entries_index, list_index, is_col, sf, prev_col if is_col else prev_res, today)
        if item is None: continue
        fid = item["formId"]
        if fid in seen: continue
        seen.add(fid)
        (col_items if is_col else res_items).append(item)
    # Entitlement-only collectrons (no RESO): cut or unreleased, appended so the
    # page is a complete list rather than only what the RESO grouping found.
    covered = {(i.get("edid") or "").upper() for i in col_items}
    extra = build_entitlement_only_items(collectron_entm_idx, covered, today)
    if extra:
        print("  entitlement-only collectrons added: {}".format(
            ", ".join("{} [{}]".format(e["displayName"], e["status"]) for e in extra)), file=sys.stderr)
        col_items.extend(extra)
    # Live first, then unreleased, then cut; alphabetical within each band.
    _rank = {"live": 0, "unreleased": 1, "cut": 2}
    sk = lambda x: (_rank.get(x.get("status"), 0 if not x.get("cutContent") else 2),
                    (x.get("displayName") or "").lower())
    col_items.sort(key=sk); res_items.sort(key=sk)
    # Generative Gold Bullion route (src/gold_vendor.py). This builder had no
    # gold-vendor handling of any kind, so every collectron and resource
    # producer a gold vendor sells read "N/A" — which the page contract states
    # as "we checked, it isn't sold there".
    _gv = gold_vendor.index()
    for _label, _items in [("collectron", col_items),
                           ("resource producer", res_items)]:
        _gv.apply_to_items(_items, _label)
        _gv.report_unstocked(_items, _label)

    for fname, typ, items in [("collectrons.json", "collectrons", col_items),
                               ("resource_producers.json", "resource_producers", res_items)]:
        out = {"generatedAt": now_iso(), "type": typ, "count": len(items), "items": items}
        with open(os.path.join(args.outdir, fname), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print("[OK] {}: {} items".format(fname, len(items)), file=sys.stderr)
    # Patchlog
    combined = col_items + res_items
    prev_combined = []
    try:
        import subprocess
        for jf in ["dist/collectrons.json", "dist/resource_producers.json"]:
            try:
                cmd = ["git", "show", "HEAD^:" + jf]
                o = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=30)
                prev_combined.extend(json.loads(o.decode("utf-8")).get("items", []))
            except Exception:
                pass
    except Exception:
        pass
    entry = diff_item_lists(
        prev_items=prev_combined, curr_items=combined, key_field="formId",
        name_field="displayName,edid",
        compare_fields=["displayName", "description", "production", "imageUrl"])
    with open(os.path.join(args.outdir, "patchlog_latest_bnb_camp_items.json"), "w", encoding="utf-8") as f:
        json.dump({"entries": [entry]}, f, ensure_ascii=False, indent=2)
    print("[patchlog] current={} added={} removed={} changed={}".format(
        entry['current'], len(entry["added"]), len(entry["removed"]), len(entry["changed"])), file=sys.stderr)
    if _UNRESOLVED_DROP_NAMES:
        print("[drop-names] {} dropped item(s) have no resolvable FULL name — "
              "showing cleaned EDID (add the matching record export to fix):".format(
                  len(_UNRESOLVED_DROP_NAMES)), file=sys.stderr)
        for fid, edid, shown in sorted(_UNRESOLVED_DROP_NAMES):
            print("           {} {} -> '{}'".format(fid, edid, shown), file=sys.stderr)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
