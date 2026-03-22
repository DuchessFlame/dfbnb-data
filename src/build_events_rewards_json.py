#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_events_rewards_json.py

Builds:
  dist/events/events_rewards.json
  dist/events/events_rewards_by_page.json
  dist/patchlogs/patchlog_latest_df_events.json

baseRewards schema (new):
  {
    "tiers": [
      {
        "tier":          ""           # "" | "gold" | "silver" | "bronze" | "mutated"
        "xp":            471,         # XP at level 50, or null
        "xpFormID":      "...",
        "caps":          320,         # caps value from GLOB, or null
        "capsFormID":    "...",
        "legendaryRank": 1,           # int or null
        "lvliFormID":    "...",
        "poolTypes":     [{"type":"caps","label":"Caps"}, ...],
        "titles":        [{"title":"Shepherd","kind":"player","isPrefix":true,"isSuffix":true}]
      }
    ]
  }
"""

import csv, glob, json, os, re
from collections import defaultdict
from pathlib import Path

DIST_DIR     = Path("dist/events")
PATCHLOG_DIR = Path("dist/patchlogs")

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def newest(pattern):
    files = glob.glob(pattern)
    if not files: raise FileNotFoundError(pattern)
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]

def read_tsv(path):
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

def pick(row, *keys, default=""):
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return v
    return default

def pct(x):
    return round(max(0.0, float(x)) * 100, 6)

def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"<.*?>", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()

def strip_trailing_slash(p):
    p = (p or "").strip()
    if p != "/" and p.endswith("/"): p = p[:-1]
    return p

def parse_ref(ref):
    s = (ref or "").strip()
    if not s: return ("", "")
    if ":" in s:
        parts = s.split(":")
        return (parts[0].strip(), parts[-1].strip())
    return (s, "")

def title_case_words(s):
    return " ".join(w.capitalize() if w else w for w in s.split())

LVLI_LABEL_OVERRIDES = {
    # key: EXACT lowercase EDID match → display label
}

# Pattern-based label rules: (regex, replacement_func_or_string)
# Applied in order; first match wins.
LVLI_LABEL_PATTERNS = [
    # Exact well-known EDIDs
    (r"^RA_LL_Rewards_Activities$",           "Activity Rewards"),
    (r"^RA_LL_Rewards_EnclaveActivities$",    "Enclave Activity Rewards"),
    # Enclave Urban Scout Armour (raw EDIDs contain ScoutUniform / ScoutArmor)
    (r"(?i)scout_?uniform|scout_?armor",      "Enclave Urban Scout Armour"),
    # Enclave Plasma Gun mod boxes
    (r"(?i)enclave.*plasmagun|plasmagun.*all", "Enclave Plasma Gun Mod Boxes"),
    # Legendary items: generative star-count labels from Rank/Star suffixes
    # e.g. RA_LL_Rewards_LegendaryItems_Rank1 → "Legendary Items (1★)"
    (r"(?i)LegendaryItems.*?_Rank(\d+)$",
     lambda m: f"Legendary Items ({m.group(1)}\u2605)"),
    # e.g. LLS_Loot_Legendary1Star → "Legendary Items (1★)"
    (r"(?i)Legendary(\d+)Star$",
     lambda m: f"Legendary Items ({m.group(1)}\u2605)"),
    # Legendary sub-pools: armour, power armour, weapons
    (r"(?i)LegendaryItems_LL_Armor_All",       "Legendary Armour"),
    (r"(?i)LegendaryItems_LL_PowerArmor_All",  "Legendary Power Armour"),
    (r"(?i)LegendaryItems_LL_Weapons_Any",     "Legendary Weapons (All Types)"),
    (r"(?i)LegendaryItems_LL_Weapons_Melee",   "Legendary Melee Weapons"),
    (r"(?i)LegendaryItems_LL_Weapons_Ranged",  "Legendary Ranged Weapons"),
    # Region reward pools (public-facing names, not data-miner jargon)
    (r"(?i)progression_?items",               "Regional Loot"),
    (r"(?i)allregions_?grabbag|all_?regions",  "Regional Loot Pool"),
    (r"(?i)forest_?grabbag|forest_?grab_?bag", "Forest Rewards"),
    (r"(?i)toxicvalley_?grabbag|toxic_?valley_?grab", "Toxic Valley Rewards"),
    (r"(?i)savagedivide_?grabbag|savage_?divide_?grab", "Savage Divide Rewards"),
    (r"(?i)ashheap_?grabbag|ash_?heap_?grab",  "Ash Heap Rewards"),
    (r"(?i)mire_?grabbag|mire_?grab_?bag",     "The Mire Rewards"),
    (r"(?i)cranberrybog_?grabbag|cranberry_?bog_?grab", "Cranberry Bog Rewards"),
    (r"(?i)skylinevalley_?grabbag|skyline_?valley_?grab", "Skyline Valley Rewards"),
    (r"(?i)burningsprings_?grabbag|burning_?springs_?grab", "Burning Springs Rewards"),
    # Regional schematics
    (r"(?i)regional_?schematics",             "Regional Plans"),
    # Corpse Flower Seeds
    (r"(?i)corpseflower.*seeds",              "Corpse Flower Seeds"),
    # U-Mine-It Maps
    (r"(?i)umineit|u_?mine_?it",             "U-Mine-It Maps"),
    # Stimpak
    (r"(?i)chems_stimpak$",                   "Stimpak"),
]

def prettify_lvli_label(edid):
    """Convert an LVLI EDID to a human-readable label for tree display."""
    t = (edid or "").strip()
    if not t: return ""

    # Check exact overrides first
    if t.lower() in LVLI_LABEL_OVERRIDES:
        return LVLI_LABEL_OVERRIDES[t.lower()]

    # Check pattern rules (label can be a string or a callable(match) → string)
    for pat, label in LVLI_LABEL_PATTERNS:
        m = re.search(pat, t)
        if m:
            return label(m) if callable(label) else label

    # Generic cleanup: strip LVLI naming prefixes (order matters — longest first)
    t = re.sub(r"^(RA_LLS?_Rewards_Activities|RA_LLS?_Rewards|RA_LLS?|RA_LL_Rewards|RA_LL|LLS?_Rewards|LLS?|RA|LL|QuestReward|Quest_Reward|Rewards)_+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^LL_", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^FF\d+_(?:Reward_)?", "", t, flags=re.IGNORECASE)

    # Strip quest/event ID prefixes (may be nested): E01b_, Ffz17_, Mtns04_, Bo_, Bs02_, Burn_, etc.
    # Only strip tokens that look like quest IDs (contain digits, or are known short codes).
    # Don't strip regular English words like "Tea", "Wheel", "Love", "Robots".
    for _ in range(3):
        # Tokens with digits are always quest IDs: E01b_, Ffz17_, Mtns04_, Bs02_, Cbz13_, Sr01_, etc.
        t = re.sub(r"^[A-Za-z]+\d+[A-Za-z]?_", "", t)
        # Known short non-numeric quest ID prefixes (2-4 chars, known codes only)
        t = re.sub(r"^(?:Bo|EN|Burn|Gwws)_", "", t, flags=re.IGNORECASE)
        t = re.sub(r"^LLS?_", "", t, flags=re.IGNORECASE)
        t = re.sub(r"^LL_", "", t, flags=re.IGNORECASE)

    # Split on underscores and CamelCase
    t = t.replace("__", "_").replace("_", " ")
    t = re.sub(r"([a-z])([A-Z])", r"\1 \2", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Remove stray "LL" / "LLS" words left mid-label
    t = re.sub(r"\bLLS?\b\s*", "", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Semantic replacements
    t = re.sub(r"\bPublic Events\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bPublic Event Rewards Rewards\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"(?i)\bUmine\s*It\b", "U-Mine-It", t)

    t = title_case_words(t)
    return t.strip()

def simplify_condition(cond_str):
    """Convert verbose xEdit condition strings to human-readable summaries."""
    s = (cond_str or "").strip()
    if not s:
        return ""

    # Extract quest name from patterns like: EN02_MQ_Us "One of Us" [QUST:000293A3]
    quest_match = re.search(r'"([^"]+)"\s*\[QUST:', s)
    quest_name = quest_match.group(1) if quest_match else ""

    # GetQuestCompleted → "Requires: <Quest Name>"
    if "GetQuestCompleted" in s and quest_name:
        return f"Requires: {quest_name}"

    # HasLearnedRecipe → check comparison value (= 1 means MUST know, = 0 means must NOT know)
    # and resolve the COBJ reference to a human-readable recipe/plan name.
    if "HasLearnedRecipe" in s:
        # Extract COBJ FormID from e.g. "co_Weapon_Ranged_GatlingPlasma [COBJ:00311432]"
        cobj_match = re.search(r'\[COBJ:([0-9A-Fa-f]+)\]', s)
        # Extract COBJ EDID as fallback
        cobj_edid_match = re.search(r'HasLearnedRecipe\([^,]*,\s*[^,]*,\s*(\w+)', s)
        # Extract comparison value (last number in the raw string, e.g. "10000000 1.000000")
        comp_match = re.search(r'(\d+\.\d+)\s*$', s)
        comp_val = float(comp_match.group(1)) if comp_match else 1.0

        # Resolve recipe name from COBJ
        recipe_name = ""
        if cobj_match:
            cobj_fid = cobj_match.group(1)
            cobj_entry = cobj_by_formid.get(cobj_fid)
            if cobj_entry:
                recipe_name = cobj_entry.get("created_name", "")
        if not recipe_name and cobj_edid_match:
            cobj_edid = cobj_edid_match.group(1)
            cobj_entry = cobj_by_edid.get(cobj_edid)
            if cobj_entry:
                recipe_name = cobj_entry.get("created_name", "")
        if not recipe_name and cobj_edid_match:
            # Humanize EDID as last resort
            recipe_name = humanize_edid(cobj_edid_match.group(1))

        if comp_val >= 1.0:
            # = 1 means player MUST have learned the recipe
            if recipe_name:
                return f"Requires Plan: {recipe_name} to be learned"
            return "Requires the base plan to be learned"
        else:
            # = 0 means player must NOT have learned it yet
            if recipe_name:
                return f"Won\u2019t drop if you\u2019ve already learned Plan: {recipe_name}"
            return "Won\u2019t drop if you\u2019ve already learned this recipe"

    # GetRandomPercent → handled by entryRate and pill display, omit from conditions
    if "GetRandomPercent" in s:
        return ""

    # GetLevel → "Requires player level X+"
    if "GetLevel" in s:
        level_match = re.search(r'(\d+)\.0+\s*$', s)
        if level_match:
            return f"Requires player level {level_match.group(1)}+"
        return ""

    # GetGlobalValue → extract GLOB name and make readable
    if "GetGlobalValue" in s:
        glob_match = re.search(r'(\w+)\s*\[GLOB:', s)
        if glob_match:
            glob_edid = glob_match.group(1)
            # Prettify: LTT_RA_Rewards_Activities_DoubleLegendaryItem_Toggle
            pretty = re.sub(r"^(LTT_|RA_|Rewards_|Activities_)+", "", glob_edid)
            pretty = pretty.replace("_", " ").strip()
            # Split CamelCase
            pretty = re.sub(r"([a-z])([A-Z])", r"\1 \2", pretty)
            pretty = re.sub(r"\s+", " ", pretty)
            pretty = title_case_words(pretty)
            pretty = re.sub(r"(?i)\bUmine\s*It\s*Map\b", "U-Mine-It Map", pretty)
            pretty = re.sub(r"(?i)\bU-mine-it\b", "U-Mine-It", pretty)
            # Clean up "Toggle" suffix if redundant
            pretty = re.sub(r"\s+Toggle$", "", pretty, flags=re.IGNORECASE)
            return f"Toggle: {pretty}"
        return ""

    # Fallback: strip raw numeric flags at end and clean up
    s = re.sub(r'\s+[01]{8}\s+[\d.]+$', '', s)
    s = re.sub(r'\s+[01]{8}\s+\S+\s*$', '', s)
    # Strip "Subject." prefix and parameter noise
    s = re.sub(r'^Subject\.', '', s)
    s = re.sub(r'\(00 00 00.*?\)', '()', s)
    return s.strip() if s.strip() else ""

def simplify_conditions(conditions):
    """Simplify a list of condition strings, removing empty results."""
    result = []
    for c in (conditions or []):
        s = simplify_condition(c)
        if s and s not in result:
            result.append(s)
    return result

def parse_randompercent_multiplier(conditions_text):
    mult = 1.0
    # Match "GetRandomPercent <= N" (standard <= format)
    for m in re.finditer(r"GetRandomPercent\s*<=\s*(\d+(?:\.\d+)?)", conditions_text or "", flags=re.IGNORECASE):
        try:
            n = max(0, min(100, float(m.group(1))))
            mult *= n / 100.0
        except ValueError:
            pass
    # Match raw GMRW Conditions format: "GetRandomPercent <flags> <value>"
    # e.g. "GetRandomPercent 10100000 10.000000"
    for m in re.finditer(r"GetRandomPercent\s+\d+\s+(\d+(?:\.\d+)?)", conditions_text or "", flags=re.IGNORECASE):
        try:
            n = max(0, min(100, float(m.group(1))))
            mult *= n / 100.0
        except ValueError:
            pass
    return mult

# --------------------------------------------------
# Load TSVs
# --------------------------------------------------

QUEST        = read_tsv(newest("tsv/QUEST_Export_*.tsv"))
GMRW         = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
LVLI_LIST    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_List.tsv"))
LVLI_ENTRIES = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Entries.tsv"))
LVLI_MATH    = read_tsv(newest("tsv/LVLI_Export_*_LVLI_Math.tsv"))
# BOOK: exclude Locations sub-export (has no FULL column)
_book_files = [f for f in glob.glob("tsv/BOOK_Export_*.tsv")
               if "_Locations" not in f]
if not _book_files:
    raise FileNotFoundError("tsv/BOOK_Export_*.tsv (non-Locations)")
_book_files.sort(key=lambda x: os.path.getmtime(x))
BOOK         = read_tsv(_book_files[-1])
# ARMO: exclude SLOTS and ObjectTemplate sub-exports (no ARMO_FULL column)
_armo_files = [f for f in glob.glob("tsv/ARMO_Export_*.tsv")
               if "_SLOTS" not in f and "_ObjectTemplate" not in f]
if not _armo_files:
    raise FileNotFoundError("tsv/ARMO_Export_*.tsv (non-SLOTS)")
_armo_files.sort(key=lambda x: os.path.getmtime(x))
ARMO         = read_tsv(_armo_files[-1])
GLOB         = read_tsv(newest("tsv/GLOB_Export_*.tsv"))
GUIDE        = read_tsv(newest("tsv/guide_index.tsv"))

try:    MISC = read_tsv(newest("tsv/MISC_Export_*.tsv"))
except FileNotFoundError: MISC = []
try:    WEAP = read_tsv(newest("tsv/WEAP_Export_*.tsv"))
except FileNotFoundError: WEAP = []
try:    ALCH = read_tsv(newest("tsv/ALCH_Export_*.tsv"))
except FileNotFoundError: ALCH = []
try:    AMMO = read_tsv(newest("tsv/AMMO_Export_*.tsv"))
except FileNotFoundError: AMMO = []
try:    CREA = read_tsv(newest("tsv/CREA_Export_*.tsv"))
except FileNotFoundError: CREA = []
try:    CURV = read_tsv(newest("tsv/CURV_Export_*.tsv"))
except FileNotFoundError: CURV = []
try:    CURV_POINTS = read_tsv(newest("tsv/CURV_Export_*_POINTS.tsv"))
except FileNotFoundError: CURV_POINTS = []
try:    PLYT = read_tsv(newest("tsv/PLYT_Export_*.tsv"))
except FileNotFoundError: PLYT = []
try:    CMPT = read_tsv(newest("tsv/CMPT_Export_*.tsv"))
except FileNotFoundError: CMPT = []
try:    WEAP_OT = read_tsv(newest("tsv/WEAP_Export_*_ObjectTemplate.tsv"))
except FileNotFoundError: WEAP_OT = []
try:    ARMO_OT = read_tsv(newest("tsv/ARMO_Export_*_ObjectTemplate.tsv"))
except FileNotFoundError: ARMO_OT = []
try:    COBJ = read_tsv(newest("tsv/COBJ_Export_*.tsv"))
except FileNotFoundError: COBJ = []

# --------------------------------------------------
# Index: WEAP Object Template (mod slots for named/unique weapons)
# --------------------------------------------------

# Map weapon FormID → list of mod slot dicts [{label, value, includeIndex}]
# Used to display weapon breakdowns like:
#   Appearance: Lawbringer | 1★ Adrenal | 2★ Rapid | 3★ Swift
#   Receiver: Standard | Grip: Standard | Sights: Iron Sights | Barrel: Long

# Slot labels inferred from OMOD EDID keywords or attach point index
_MOD_SLOT_LABELS = {
    "appearance": "Appearance",
    "paint":      "Appearance",
    "weapon_paint": "Appearance",
    "legendary1":   "Legendary 1★",
    "legendary_weapon1": "Legendary 1★",
    "legendary2":   "Legendary 2★",
    "legendary_weapon2": "Legendary 2★",
    "legendary3":   "Legendary 3★",
    "legendary_weapon3": "Legendary 3★",
    "legendary4":   "Legendary 4★",
    "legendary_weapon4": "Legendary 4★",
    "legendary5":   "Legendary 5★",
    "legendary_weapon5": "Legendary 5★",
    "receiver":  "Receiver",
    "grip":      "Grip",
    "scope":     "Sights",
    "ironsights": "Sights",
    "sights":    "Sights",
    "barrel":    "Barrel",
    "magazine":  "Magazine",
    "muzzle":    "Muzzle",
    "stock":     "Stock",
}

def _classify_mod_slot(mod_ref_str):
    """Classify a mod OMOD reference string into a human-readable slot label + value."""
    if not mod_ref_str:
        return None, None
    # mod_ref_str looks like: ATX_mod_44_Weapon_Paint_Lawbringer "Lawbringer" [OMOD:008599F7]
    # or: mod_Legendary_Weapon1_Adrenal "Adrenal" [OMOD:0080F549]
    parts = mod_ref_str.strip()
    # Extract quoted display name
    display_name = ""
    m = re.search(r'"([^"]+)"', parts)
    if m:
        display_name = m.group(1)
    # Extract EDID (everything before the first quote or bracket)
    edid = re.split(r'["\[]', parts)[0].strip()
    edid_lower = edid.lower()

    # Match slot label from EDID keywords
    label = None
    for keyword, slot_label in _MOD_SLOT_LABELS.items():
        if keyword in edid_lower:
            label = slot_label
            break
    if not label:
        label = "Mod"

    if display_name:
        value = display_name
    else:
        # Humanize raw EDID: strip mod_ prefix and common boilerplate, title-case
        h = re.sub(r"^mod_", "", edid, flags=re.IGNORECASE)
        h = re.sub(r"\s*\[OMOD:[0-9A-Fa-f]+\]", "", h)
        h = h.replace("_", " ").strip()
        value = h if h else edid
    return label, value

def _mod_slot_sort_key(slot):
    """Sort mod slots: Legendary stars first, then Unique/Custom, then everything else."""
    label = slot.get("label", "")
    if label.startswith("Legendary"):
        # Extract star number for sub-sorting (1★ before 2★ etc.)
        m = re.search(r"(\d)", label)
        return (0, int(m.group(1)) if m else 0)
    if label.lower() in ("unique", "custom"):
        return (1, 0)
    # Everything else (Lining, Receiver, Material, Appearance, etc.) by includeIndex
    return (2, slot.get("includeIndex", 0))

# Group OT rows by (FormID, CombinationIndex), then pick the best combo per weapon.
# "Best" = the combination with the most legendary slots (i.e. the named/unique variant).
_weap_combos = defaultdict(lambda: defaultdict(list))  # {fid: {combo_idx: [slots]}}
for r in WEAP_OT:
    fid = pick(r, "WEAP_FormID", "FormID")
    mod_ref = pick(r, "Include_Mod", "Mod")
    if not fid or not mod_ref:
        continue
    label, value = _classify_mod_slot(mod_ref)
    if label and value:
        combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
        inc_idx = int(pick(r, "IncludeIndex", default="0") or 0)
        _weap_combos[fid][combo_idx].append({
            "label": label,
            "value": value,
            "includeIndex": inc_idx,
        })

weap_mod_slots_by_formid = {}
for fid, combos in _weap_combos.items():
    # Pick the combination with the most legendary slots
    best_combo_idx = max(
        combos.keys(),
        key=lambda ci: sum(1 for s in combos[ci] if "Legendary" in s["label"])
    )
    best_slots = combos[best_combo_idx]
    # Only keep weapons that have at least one legendary slot (i.e. named/unique weapons)
    has_legendary = any("Legendary" in s["label"] for s in best_slots)
    if has_legendary:
        best_slots.sort(key=_mod_slot_sort_key)
        weap_mod_slots_by_formid[fid] = best_slots

# --------------------------------------------------
# Index: ARMO Object Template (mod slots for named/unique armour)
# --------------------------------------------------

# Armour slot labels — same pattern as weapons but armour-specific keywords
_ARMOR_MOD_SLOT_LABELS = {
    "legendary_armor1": "Legendary 1★",
    "legendary_armor2": "Legendary 2★",
    "legendary_armor3": "Legendary 3★",
    "legendary_armor4": "Legendary 4★",
    "legendary_armor5": "Legendary 5★",
    "legendary1":       "Legendary 1★",
    "legendary2":       "Legendary 2★",
    "legendary3":       "Legendary 3★",
    "legendary4":       "Legendary 4★",
    "legendary5":       "Legendary 5★",
    "paint":      "Appearance",
    "material_paint": "Appearance",
    "lining":     "Lining",
    "material_0": "Material",
    "material_1": "Material",
    "material_2": "Material",
    "material_3": "Material",
    "material_4": "Material",
    "size_a":     "Weight Class",
    "size_b":     "Weight Class",
    "size_c":     "Weight Class",
    "custom":     "Unique",
}

def _classify_armor_mod_slot(mod_ref_str):
    """Classify an armour OMOD reference string into a human-readable slot label + value."""
    if not mod_ref_str:
        return None, None
    parts = mod_ref_str.strip()
    display_name = ""
    m = re.search(r'"([^"]+)"', parts)
    if m:
        display_name = m.group(1)
    edid = re.split(r'["\[]', parts)[0].strip()
    edid_lower = edid.lower()

    label = None
    for keyword, slot_label in _ARMOR_MOD_SLOT_LABELS.items():
        if keyword in edid_lower:
            label = slot_label
            break
    if not label:
        # Fallback: Null/No Misc linings → skip, otherwise generic "Mod"
        if "lining_null" in edid_lower or "no misc" in (display_name or "").lower():
            return None, None
        label = "Mod"

    if display_name:
        value = display_name
    else:
        h = re.sub(r"^mod_", "", edid, flags=re.IGNORECASE)
        h = re.sub(r"\s*\[OMOD:[0-9A-Fa-f]+\]", "", h)
        h = h.replace("_", " ").strip()
        value = h if h else edid
    return label, value

_armo_combos = defaultdict(lambda: defaultdict(list))
for r in ARMO_OT:
    fid = pick(r, "ARMO_FormID", "FormID")
    mod_ref = pick(r, "Include_Mod", "Mod")
    if not fid or not mod_ref:
        continue
    label, value = _classify_armor_mod_slot(mod_ref)
    if label and value:
        combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
        inc_idx = int(pick(r, "IncludeIndex", default="0") or 0)
        _armo_combos[fid][combo_idx].append({
            "label": label,
            "value": value,
            "includeIndex": inc_idx,
        })

armo_mod_slots_by_formid = {}
for fid, combos in _armo_combos.items():
    best_combo_idx = max(
        combos.keys(),
        key=lambda ci: sum(1 for s in combos[ci] if "Legendary" in s["label"])
    )
    best_slots = combos[best_combo_idx]
    has_legendary = any("Legendary" in s["label"] for s in best_slots)
    if has_legendary:
        best_slots.sort(key=_mod_slot_sort_key)
        armo_mod_slots_by_formid[fid] = best_slots

# --------------------------------------------------
# Index: GLOB
# --------------------------------------------------

glob_vals = {}
for r in GLOB:
    fid  = pick(r, "GLOB_FormID", "FormID")
    fltv = pick(r, "GLOB_FLTV", "FLTV")
    if fid and fltv:
        try: glob_vals[fid] = float(fltv)
        except ValueError: pass

# --------------------------------------------------
# Index: COBJ (constructible objects / recipes)
# Maps COBJ FormID → {edid, created_name, created_formid}
# Used to resolve HasLearnedRecipe conditions to human-readable names
# --------------------------------------------------

cobj_by_formid = {}
cobj_by_edid = {}
for r in COBJ:
    fid  = pick(r, "COBJ_FormID", "FormID")
    edid = pick(r, "COBJ_EDID", "EDID")
    cnam_full = pick(r, "CNAM_FULL", "FULL")
    cnam_fid  = pick(r, "CNAM_FormID")
    if fid:
        entry = {"edid": edid or "", "created_name": cnam_full or "", "created_formid": cnam_fid or ""}
        cobj_by_formid[fid] = entry
        if edid:
            cobj_by_edid[edid] = entry

# --------------------------------------------------
# Index: item names
# --------------------------------------------------

book_names = {}
for r in BOOK:
    fid = pick(r, "BOOK_FormID", "FormID")
    full = pick(r, "BOOK_FULL", "FULL")
    if fid and full: book_names[fid] = full

armo_names = {}
for r in ARMO:
    fid = pick(r, "ARMO_FormID", "FormID")
    full = pick(r, "ARMO_FULL", "FULL")
    if fid and full: armo_names[fid] = full

misc_names = {}
for r in MISC:
    # MISC TSVs use various column name conventions across exports
    fid  = pick(r, "MISC_FormID", "FormID", "FormId")
    full = pick(r, "MISC_FULL", "FULL - Name", "FULL", "Name")
    if fid and full: misc_names[fid] = full

weap_names = {}
for r in WEAP:
    fid  = pick(r, "WEAP_FormID", "FormID")
    full = pick(r, "WEAP_FULL", "FULL - Name", "FULL")
    if fid and full: weap_names[fid] = full

alch_names = {}
for r in ALCH:
    fid  = pick(r, "ALCH_FormID", "FormID")
    full = pick(r, "ALCH_FULL", "FULL - Name", "FULL")
    if fid and full: alch_names[fid] = full

ammo_names = {}
for r in AMMO:
    fid  = pick(r, "AMMO_FormID", "FormID")
    full = pick(r, "AMMO_FULL", "FULL - Name", "FULL")
    if fid and full: ammo_names[fid] = full

crea_names = {}
for r in CREA:
    fid  = pick(r, "CREA_FormID", "FormID")
    full = pick(r, "CREA_FULL", "FULL")
    if fid and full: crea_names[fid] = full

# Friendly display names for hardcoded known FormIDs
KNOWN_FID_NAMES = {
    "0000000F": "Caps",
    "005652F9": "Legendary Module",
    "005A5443": "Treasury Note",
    "007FDC33": "Improved Bait",
    "003F7410": "Legendary Scrip",
    "0072D4FC": "Bobblehead Crate",
}

def humanize_edid(edid):
    """Convert an EDID like 'DLC04_HandMadeGun' or 'CombatShotgun' to a readable name."""
    if not edid:
        return edid
    s = edid
    # Strip common prefixes (including all DLC prefixes like DLC03_, DLC04_, etc.)
    for pfx in ["LL_Weapon_", "LL_Armor_", "LPI_Weapon_", "LPI_Armor_",
                 "LL_", "LPI_", "POST_"]:
        if s.startswith(pfx):
            s = s[len(pfx):]
    # Strip DLC0N_ prefixes generically (DLC01_, DLC02_, DLC03_, etc.)
    s = re.sub(r"^DLC\d+_", "", s)
    # Split CamelCase and underscores into words
    s = re.sub(r"_", " ", s)
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Build EDID-to-name index from all loaded TSVs (for items without FULL names)
edid_to_name = {}
for r in BOOK:
    edid = pick(r, "BOOK_EDID", "EDID")
    full = pick(r, "BOOK_FULL", "FULL")
    if edid and full:
        edid_to_name[edid] = full
for r in ARMO:
    edid = pick(r, "ARMO_EDID", "EDID")
    full = pick(r, "ARMO_FULL", "FULL")
    if edid and full:
        edid_to_name[edid] = full
for r in MISC:
    edid = pick(r, "MISC_EDID", "EDID")
    full = pick(r, "MISC_FULL", "FULL - Name", "FULL", "Name")
    if edid and full:
        edid_to_name[edid] = full
for r in WEAP:
    edid = pick(r, "WEAP_EDID", "EDID")
    full = pick(r, "WEAP_FULL", "FULL - Name", "FULL")
    if edid and full:
        edid_to_name[edid] = full
for r in ALCH:
    edid = pick(r, "ALCH_EDID", "EDID")
    full = pick(r, "ALCH_FULL", "FULL - Name", "FULL")
    if edid and full:
        edid_to_name[edid] = full
for r in AMMO:
    edid = pick(r, "AMMO_EDID", "EDID")
    full = pick(r, "AMMO_FULL", "FULL - Name", "FULL")
    if edid and full:
        edid_to_name[edid] = full

def resolve_name_for_formid(formid, edid=None):
    if not formid: return formid
    name = (KNOWN_FID_NAMES.get(formid)
         or book_names.get(formid)
         or armo_names.get(formid)
         or misc_names.get(formid)
         or weap_names.get(formid)
         or alch_names.get(formid)
         or ammo_names.get(formid)
         or crea_names.get(formid))
    if name:
        return name
    # Try EDID-based lookup
    if edid:
        name = edid_to_name.get(edid)
        if name:
            return name
        # Humanize EDID as last resort
        return humanize_edid(edid)
    return formid

# --------------------------------------------------
# Index: LVLI
# --------------------------------------------------

lvli_edid_by_formid = {}
lvli_list_by_formid = {}
for r in LVLI_LIST:
    fid = pick(r, "LVLI_FormID", "FormID")
    edid = pick(r, "LVLI_EDID", "EDID")
    if fid:
        lvli_list_by_formid[fid] = r
        if edid: lvli_edid_by_formid[fid] = edid

lvli_math_by_entry = {}
for r in LVLI_MATH:
    try: key = (r["LVLI_FormID"], r["EntryIndex"])
    except KeyError: continue
    lvli_math_by_entry[key] = r

lvli_entries_by_list = defaultdict(list)
for r in LVLI_ENTRIES:
    if "LVLI_FormID" in r:
        lvli_entries_by_list[r["LVLI_FormID"]].append(r)

def _resolve_chance_none(math_row, field_prefix="Entry"):
    """
    Resolve ChanceNone for an LVLI entry/list, checking GLOB references
    when the pre-computed 'Resolved' column is 0.

    Priority:
      1. <prefix>ChanceNoneResolved (if non-zero, trust it)
      2. <prefix>ChanceNoneGlobal → extract GLOB FormID → look up glob_vals
      3. <prefix>ChanceNoneCurve  → extract GLOB FormID → look up glob_vals
      4. Fall back to 0.0 (= 100% drop chance)

    Returns a float in 0-100 space (e.g. 95.0 means 95% chance of nothing).
    """
    resolved = float(math_row.get(f"{field_prefix}ChanceNoneResolved") or 0)
    if resolved > 0:
        return resolved

    # Check GLOB references when Resolved is 0
    for col in (f"{field_prefix}ChanceNoneGlobal", f"{field_prefix}ChanceNoneCurve"):
        ref = (math_row.get(col) or "").strip()
        if not ref:
            continue
        # Extract GLOB FormID from "00829437:SpawnChance_Cnone_...:GLOB"
        glob_fid = ref.split(":")[0] if ":" in ref else ref
        if glob_fid in glob_vals:
            return glob_vals[glob_fid]

    return 0.0


_lvli_cache = {}

def compute_lvli(list_id):
    if not list_id: return {}
    if list_id in _lvli_cache: return _lvli_cache[list_id]
    results = {}
    for e in lvli_entries_by_list.get(list_id, []):
        idx = e.get("EntryIndex")
        if idx is None: continue
        math = lvli_math_by_entry.get((list_id, idx))
        if not math: continue
        sub        = (math.get("SubLVLI_FormID") or "").strip()
        list_none  = _resolve_chance_none(math, "List") / 100.0
        entry_pres = float(math.get("EntryPresenceChance") or 1)
        entry_none = _resolve_chance_none(math, "Entry") / 100.0
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)
        apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)
        chance = (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori
        if sub:
            for k, v in compute_lvli(sub).items():
                results[k] = results.get(k, 0) + v * chance
        else:
            ref = (e.get("LVLO_Reference") or "").strip()
            qty_raw = (e.get("LVLO_Count") or e.get("Count") or "1").strip()
            try: qty = int(float(qty_raw))
            except (ValueError, TypeError): qty = 1
            if ":" in ref:
                fid = ref.split(":")[0]
                results[fid] = results.get(fid, 0) + chance
    _lvli_cache[list_id] = results
    return results

def compute_lvli_with_region(list_id, depth=0, seen=None, inherited_region=None):
    """
    Like compute_lvli but returns a list of dicts with {formid, chance, region, lctn}
    so that items in regional schematic pools can be tagged with their source region.
    Only resolves one level of sub-LVLI for region detection; deeper levels fall back
    to standard compute_lvli for performance.
    """
    if seen is None: seen = set()
    if list_id in seen or depth > 8: return []
    seen = seen | {list_id}
    results = []
    for e in lvli_entries_by_list.get(list_id, []):
        idx = e.get("EntryIndex")
        if idx is None: continue
        math = lvli_math_by_entry.get((list_id, idx))
        if not math: continue
        sub        = (math.get("SubLVLI_FormID") or "").strip()
        list_none  = _resolve_chance_none(math, "List") / 100.0
        entry_pres = float(math.get("EntryPresenceChance") or 1)
        entry_none = _resolve_chance_none(math, "Entry") / 100.0
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)
        apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)
        chance = (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori
        if sub:
            # Detect region from sub-LVLI EDID
            sub_edid = lvli_edid_by_formid.get(sub, "").lower()
            region = inherited_region
            for substr, rname in REGION_BY_SUBLVLI_EDID.items():
                if substr in sub_edid:
                    region = rname
                    break
            for item in compute_lvli_with_region(sub, depth + 1, seen, region):
                results.append({
                    "formid": item["formid"],
                    "chance": item["chance"] * chance,
                    "region": item["region"] or inherited_region,
                    "lctn":   item["lctn"],
                })
        else:
            ref = (e.get("LVLO_Reference") or "").strip()
            if ":" in ref:
                fid = ref.split(":")[0]
                results.append({
                    "formid": fid,
                    "chance": chance,
                    "region": inherited_region,
                    "lctn":   None,
                })
    return results

# --------------------------------------------------
# Activity Events: Helper functions
# --------------------------------------------------

def parse_lvlf_flags(flags_str):
    """
    Parse LVLF_Flags positional bit string from xEdit export.

    xEdit's GetEditValue on a flags field returns a bit string where
    character position N (left-to-right, 0-indexed) corresponds to bit N:
      position 0 = Calculate from all levels <= PC's level (Level Filter)
      position 1 = Calculate for each item in count (For Each)
      position 2 = Use All
      position 6 = First Match

    Examples: "001" → Use All, "11" → Level Filter + For Each,
              "0000001" → First Match
    """
    flags_str = (flags_str or "").strip()
    if not flags_str:
        return {"use_all": False, "for_each": False, "level_filter": False, "first_match": False}

    def bit_set(pos):
        return pos < len(flags_str) and flags_str[pos] == '1'

    return {
        "level_filter": bit_set(0),
        "for_each":     bit_set(1),
        "use_all":      bit_set(2),
        "first_match":  bit_set(6),
    }

def _extract_grp_threshold(raw_conds):
    """Extract the GetRandomPercent <= X threshold from raw condition strings.
    Handles both literal numbers (e.g. 20.000000) and GLOB references ([GLOB:XXXXXXXX]).
    Returns the threshold float (e.g. 20.0, 25.0) or None if no GRP condition found."""
    for cond in raw_conds:
        if "GetRandomPercent" not in cond:
            continue
        # Try GLOB reference first: [GLOB:XXXXXXXX]
        glob_match = re.search(r'\[GLOB:([0-9A-Fa-f]+)\]', cond)
        if glob_match:
            glob_fid = glob_match.group(1)
            if glob_fid in glob_vals:
                return glob_vals[glob_fid]
        # Try literal number (last number in the string)
        parts = cond.strip().split()
        for part in reversed(parts):
            try:
                return float(part)
            except ValueError:
                continue
    return None


def resolve_lvli_items_deep(list_id, depth=0, seen=None):
    """
    Resolve an LVLI to leaf items with full quantity/probability tracking.
    Returns list of dicts: {formid, name, qty, dropRate, edid, sig, conditions}
    Recurses deeply (max 50 levels) and preserves individual entry probabilities.
    Applies normalization for pick-one lists where xEdit apriori values are 1.0.
    """
    if seen is None:
        seen = set()
    if list_id in seen or depth > 50:
        return []
    seen = seen | {list_id}

    # Check if this list is Use All (each entry fires independently)
    list_row = lvli_list_by_formid.get(list_id)
    flags = parse_lvlf_flags(pick(list_row, "LVLF_Flags", default="") if list_row else "")
    is_use_all = flags["use_all"]
    is_first_match = flags["first_match"]

    # Pre-scan for first_match cascading probabilities:
    # "Use first object that matches" checks entries IN ORDER, picking the first
    # whose GetRandomPercent condition passes.  xEdit can't calculate the net
    # probability per entry, so we do it here.
    first_match_rates = {}  # EntryIndex -> net probability (0-1)
    if is_first_match:
        all_entries = lvli_entries_by_list.get(list_id, [])
        thresholds = []
        for _e in all_entries:
            _idx = _e.get("EntryIndex")
            if _idx is None:
                continue
            _conds = []
            for _ci in range(1, 11):
                _cv = (_e.get(f"Cond{_ci}") or "").strip()
                if _cv:
                    _conds.append(_cv)
            thresholds.append((_idx, _extract_grp_threshold(_conds)))
        if any(t is not None for _, t in thresholds):
            prev = 0.0
            for _idx, thresh in thresholds:
                if thresh is not None:
                    first_match_rates[_idx] = max((thresh - prev) / 100.0, 0.0)
                    prev = thresh
                else:
                    first_match_rates[_idx] = max((100.0 - prev) / 100.0, 0.0)

    items = []
    for entry in lvli_entries_by_list.get(list_id, []):
        idx = entry.get("EntryIndex")
        if idx is None:
            continue

        math = lvli_math_by_entry.get((list_id, idx))
        if not math:
            continue

        # Extract probability components (resolve GLOBs when xEdit left them unresolved)
        list_none  = _resolve_chance_none(math, "List") / 100.0
        entry_pres = float(math.get("EntryPresenceChance") or 1)
        entry_none = _resolve_chance_none(math, "Entry") / 100.0
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)
        apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)

        drop_rate = (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori

        # Override with cascading probability for first_match lists
        if idx in first_match_rates:
            drop_rate = first_match_rates[idx]

        # Get quantity (try multiple columns)
        qty = 1
        qty_raw = entry.get("LVIV_Quantity") or entry.get("LVLO_Count") or entry.get("Count") or "1"
        try:
            qty = int(float(qty_raw))
        except (ValueError, TypeError):
            qty = 1

        # Check for quantity global override
        qty_glob_ref = (entry.get("LVIG_QuantityGlobal") or "").strip()
        if qty_glob_ref:
            glob_fid = qty_glob_ref.split(":")[0] if ":" in qty_glob_ref else qty_glob_ref
            if glob_fid in glob_vals:
                qty = int(glob_vals[glob_fid])

        # Collect conditions from Cond1-Cond10
        conditions = []
        for i in range(1, 11):
            cond_key = f"Cond{i}"
            if cond_key in entry:
                cond_val = (entry.get(cond_key) or "").strip()
                if cond_val:
                    conditions.append(cond_val)

        sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
        ref = (entry.get("LVLO_Reference") or "").strip()
        ref_sig = ref.split(":")[-1].upper() if ref.count(":") >= 2 else ""

        if sub_lvli:
            # Recurse into sub-LVLI and apply parent drop rate
            sub_items = resolve_lvli_items_deep(sub_lvli, depth + 1, seen)
            for sub_item in sub_items:
                items.append({
                    "formid": sub_item["formid"],
                    "name": sub_item["name"],
                    "qty": sub_item["qty"],
                    "dropRate": sub_item["dropRate"] * drop_rate,
                    "edid": sub_item["edid"],
                    "sig": sub_item.get("sig", ""),
                    "conditions": conditions + (sub_item.get("conditions") or []),
                })
        else:
            # Leaf item
            if ":" in ref:
                fid = ref.split(":")[0]
                edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
                name = resolve_name_for_formid(fid, edid)
                items.append({
                    "formid": fid,
                    "name": name,
                    "qty": qty,
                    "dropRate": drop_rate,
                    "edid": edid,
                    "sig": ref_sig,
                    "conditions": conditions,
                })

    # Normalize for pick-one lists.  A pick-one list selects exactly one
    # entry, so the individual probabilities MUST sum to 1.0 (100%).
    # xEdit exports often report apriori=1.0 for every entry making the
    # raw total equal to the entry count — we normalise unconditionally
    # whenever the total deviates from 1.0 by more than a tiny epsilon.
    if not is_use_all and not is_first_match and items:
        total_rate = sum(item["dropRate"] for item in items)
        if total_rate > 0 and abs(total_rate - 1.0) > 0.0001:
            for item in items:
                item["dropRate"] = item["dropRate"] / total_rate

    return items

# Build a set of unique/named item identifiers from Object Template custom mods.
# These are used to detect LVLIs that drop named/legendary item variants
# (e.g. "LL_Armor_EnclaveScoutUniform_Torso_Urban_LastBastion" → "LastBastion").
_unique_ot_names = set()
for fid, slots in armo_mod_slots_by_formid.items():
    for slot in slots:
        if (slot["label"] or "").lower() in ("unique", "custom"):
            # Extract the identifier from the value (e.g. "Last Bastion" → "lastbastion")
            raw = (slot["value"] or "").replace(" ", "").lower()
            if raw and len(raw) > 3:
                _unique_ot_names.add(raw)
for fid, slots in weap_mod_slots_by_formid.items():
    for slot in slots:
        if (slot["label"] or "").lower() in ("unique", "custom"):
            raw = (slot["value"] or "").replace(" ", "").lower()
            if raw and len(raw) > 3:
                _unique_ot_names.add(raw)

def _is_unique_lvli(lvli_edid):
    """Check if an LVLI EDID matches a known unique/named item variant."""
    edid_lower = (lvli_edid or "").replace("_", "").lower()
    return any(name in edid_lower for name in _unique_ot_names)


def build_lvli_tree_node(list_id, depth=0, seen=None):
    """
    Builds a hierarchical tree representation of an LVLI for rendering as expandable sections.
    Returns a nested dict structure with type, formid, edid, label, useAll, children[], items[].
    Max recursion depth: 15. Tracks visited LVLIs via seen set to prevent cycles.
    """
    if seen is None:
        seen = set()
    if list_id in seen or depth > 15:
        return None
    seen = seen | {list_id}

    # Look up list metadata
    list_row = lvli_list_by_formid.get(list_id)
    if not list_row:
        return None

    flags = parse_lvlf_flags(pick(list_row, "LVLF_Flags", default=""))
    is_use_all = flags["use_all"]
    is_first_match = flags["first_match"]

    edid = lvli_edid_by_formid.get(list_id, "")
    label = prettify_lvli_label(edid)

    children = []
    items = []

    # Collect raw (0-1) probabilities for all entries first, then normalize
    raw_entries = []  # list of (type, raw_rate, data_dict, raw_conditions)

    # Process each entry
    for entry in lvli_entries_by_list.get(list_id, []):
        idx = entry.get("EntryIndex")
        if idx is None:
            continue

        math = lvli_math_by_entry.get((list_id, idx))
        if not math:
            continue

        # EntryAprioriChance_NoSublist is the pre-computed drop rate in 0-1 space.
        # It SHOULD incorporate: (1 - listCN/100) * (1 - entryCN/100) * condChance
        # BUT xEdit often fails to resolve GLOB references in ChanceNone fields,
        # leaving them at 0 and inflating apriori.  We correct by applying any
        # unresolved GLOB ChanceNone as a post-hoc multiplier.
        apriori = float(math.get("EntryAprioriChance_NoSublist") or 0)

        # Correct for unresolved GLOB ChanceNone
        resolved_list_cn  = float(math.get("ListChanceNoneResolved") or 0)
        resolved_entry_cn = float(math.get("EntryChanceNoneResolved") or 0)
        actual_list_cn    = _resolve_chance_none(math, "List")
        actual_entry_cn   = _resolve_chance_none(math, "Entry")
        # If the resolved value was 0 but the GLOB gives a real value, apply correction
        glob_correction = 1.0
        if actual_list_cn > 0 and resolved_list_cn == 0:
            glob_correction *= (1.0 - actual_list_cn / 100.0)
        if actual_entry_cn > 0 and resolved_entry_cn == 0:
            glob_correction *= (1.0 - actual_entry_cn / 100.0)
        entry_drop_rate = apriori * glob_correction

        # Get quantity
        qty = 1
        qty_raw = entry.get("LVIV_Quantity") or entry.get("LVLO_Count") or entry.get("Count") or "1"
        try:
            qty = int(float(qty_raw))
        except (ValueError, TypeError):
            qty = 1

        # Check for quantity global override
        qty_glob_ref = (entry.get("LVIG_QuantityGlobal") or "").strip()
        if qty_glob_ref:
            glob_fid = qty_glob_ref.split(":")[0] if ":" in qty_glob_ref else qty_glob_ref
            if glob_fid in glob_vals:
                qty = int(glob_vals[glob_fid])

        # Collect conditions from Cond1-Cond10
        conditions = []
        for i in range(1, 11):
            cond_key = f"Cond{i}"
            if cond_key in entry:
                cond_val = (entry.get(cond_key) or "").strip()
                if cond_val:
                    conditions.append(cond_val)

        # For UseAll lists, if entry has a GetRandomPercent condition (with a GLOB
        # or literal threshold), use that as the effective entry rate.  xEdit can't
        # resolve GLOB-referenced conditions, leaving apriori=1.0 (100%).
        if is_use_all and conditions:
            grp_thresh = _extract_grp_threshold(conditions)
            if grp_thresh is not None:
                entry_drop_rate = grp_thresh / 100.0

        sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
        ref = (entry.get("LVLO_Reference") or "").strip()
        ref_sig = ref.split(":")[-1].upper() if ref.count(":") >= 2 else ""

        # Simplify conditions for display
        display_conditions = simplify_conditions(conditions)

        if sub_lvli:
            # Recurse into sub-LVLI
            sub_node = build_lvli_tree_node(sub_lvli, depth + 1, seen)
            if sub_node and (sub_node.get("children") or sub_node.get("items")):
                if display_conditions:
                    sub_node["conditions"] = display_conditions
                raw_entries.append(("child", entry_drop_rate, sub_node, conditions))
        else:
            # Leaf item
            if ":" in ref:
                fid = ref.split(":")[0]
                ref_edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
                name = resolve_name_for_formid(fid, ref_edid)
                item_data = {
                    "formid": fid,
                    "edid": ref_edid,
                    "name": name,
                    "qty": qty,
                    "sig": ref_sig,
                }
                if display_conditions:
                    item_data["conditions"] = display_conditions
                # Attach modSlots ONLY if this LVLI is a named/unique variant
                # (detected by matching the LVLI EDID against known unique OT names).
                # Regular LVLI pools drop the base version (OT CombinationIndex 0).
                if _is_unique_lvli(edid):
                    if ref_sig == "ARMO" and fid in armo_mod_slots_by_formid:
                        item_data["modSlots"] = armo_mod_slots_by_formid[fid]
                    elif ref_sig == "WEAP" and fid in weap_mod_slots_by_formid:
                        item_data["modSlots"] = weap_mod_slots_by_formid[fid]
                raw_entries.append(("item", entry_drop_rate, item_data, conditions))

    # ── First-match cascading probability ──
    # When a list has the "Use first object that matches all conditions" flag,
    # entries are checked IN ORDER. Each entry may have a GetRandomPercent <= X
    # condition. The game rolls ONE random number (1-100) and picks the FIRST
    # entry whose condition matches.  This means:
    #   Entry 1 (<= 20): net 20%
    #   Entry 2 (<= 38): net 18% (38 - 20, since <=20 already consumed)
    #   Entry 3 (none):  net 62% (100 - 38, catches everything else)
    # xEdit can't calculate this, so it gives all entries apriori=1.  We fix it here.
    if is_first_match and raw_entries:
        thresholds = [_extract_grp_threshold(raw_conds) for (_, _, _, raw_conds) in raw_entries]
        # Only apply cascading logic if at least one entry has a GetRandomPercent condition
        if any(t is not None for t in thresholds):
            prev_threshold = 0.0
            new_entries = []
            for i, (etype, rate, data, raw_conds) in enumerate(raw_entries):
                if thresholds[i] is not None:
                    net_p = (thresholds[i] - prev_threshold) / 100.0
                    prev_threshold = thresholds[i]
                else:
                    # No condition = catches everything remaining
                    net_p = (100.0 - prev_threshold) / 100.0
                new_entries.append((etype, max(net_p, 0.0), data, raw_conds))
            raw_entries = new_entries

    # Normalize for pick-one lists: all raw_rates must sum to 1.0 (100%)
    total_rate = sum(r for (_, r, _, _) in raw_entries)
    if not is_use_all and not is_first_match and raw_entries and total_rate > 0 and abs(total_rate - 1.0) > 0.0001:
        for i, (etype, raw_rate, data, raw_conds) in enumerate(raw_entries):
            raw_entries[i] = (etype, raw_rate / total_rate, data, raw_conds)

    # Convert to final output with percentages
    for etype, rate, data, _raw_conds in raw_entries:
        rate_pct = round(rate * 100, 6)
        if etype == "child":
            data["entryRate"] = rate_pct
            children.append(data)
        else:
            data["dropRate"] = rate_pct
            items.append(data)

    result = {
        "type": "lvli",
        "formid": list_id,
        "edid": edid,
        "label": label,
        "useAll": is_use_all,
    }
    if children:
        result["children"] = children
    if items:
        result["items"] = items

    # Flag region-based nodes so JS can adjust display (no fake 12.5%, add note)
    edid_lower = (edid or "").lower()
    if "allregions" in edid_lower or "grabbag" in edid_lower:
        result["isRegionPool"] = True
    if "progressionitems" in edid_lower:
        result["isRegionLoot"] = True

    return result

def decompose_activities_lvli(formid):
    """
    Takes the RA_LL_Rewards_Activities LVLI (Use All list) and routes each sub-entry
    to the correct bucket based on EDID patterns.
    Returns dict of reward buckets.
    """
    buckets = {
        "caps": {"items": [], "dropRate": 0.0, "qty": 0},
        "legendary_items": {"items": [], "dropRate": 0.0},
        "scrip": {"items": []},
        "u_mine_it_maps": {"items": [], "dropRate": 0.0},
        "regional_schematics": None,
        "progression_items": None,
        "chems": [],
    }

    entries = lvli_entries_by_list.get(formid, [])
    for entry in entries:
        idx = entry.get("EntryIndex")
        if idx is None:
            continue

        math = lvli_math_by_entry.get((formid, idx))
        if not math:
            continue

        # Get the sub-LVLI reference
        ref = (entry.get("LVLO_Reference") or "").strip()
        if not ":" in ref:
            continue

        ref_fid = ref.split(":")[0]
        ref_edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
        ref_edid_lower = ref_edid.lower()

        # Collect parent-level conditions (Cond1-Cond10 on the Activities entry)
        parent_conditions = []
        for i in range(1, 11):
            cond_key = f"Cond{i}"
            cond_val = (entry.get(cond_key) or "").strip()
            if cond_val:
                parent_conditions.append(cond_val)

        # Collect parent-level ChanceNone GLOB/Curve
        chance_none_glob = (entry.get("LVOC_ChanceNoneCurve") or "").strip()
        if not chance_none_glob:
            chance_none_glob = (entry.get("LVOG_ChanceNoneGlobal") or "").strip()

        # Route based on EDID pattern
        if "_caps" in ref_edid_lower:
            # Caps bucket
            sub_items = resolve_lvli_items_deep(ref_fid)
            if sub_items:
                for item in sub_items:
                    buckets["caps"]["items"].append(item)
                    buckets["caps"]["qty"] = item.get("qty", 0)
                    buckets["caps"]["dropRate"] = item.get("dropRate", 0.0)

        elif "_legendaryitems" in ref_edid_lower:
            # Legendary items bucket
            sub_items = resolve_lvli_items_deep(ref_fid)
            for item in sub_items:
                buckets["legendary_items"]["items"].append(item)
                if not buckets["legendary_items"]["dropRate"]:
                    buckets["legendary_items"]["dropRate"] = item.get("dropRate", 0.0)

        elif "_scrip" in ref_edid_lower:
            # Scrip bucket
            sub_items = resolve_lvli_items_deep(ref_fid)
            for item in sub_items:
                buckets["scrip"]["items"].append(item)

        elif "_umineitmap" in ref_edid_lower or "_umineitmaps" in ref_edid_lower:
            # U-Mine-It Maps bucket — capture parent conditions + ChanceNone GLOB
            sub_items = resolve_lvli_items_deep(ref_fid)
            for item in sub_items:
                buckets["u_mine_it_maps"]["items"].append(item)
                if not buckets["u_mine_it_maps"]["dropRate"]:
                    buckets["u_mine_it_maps"]["dropRate"] = item.get("dropRate", 0.0)
            buckets["u_mine_it_maps"]["conditions"] = parent_conditions
            if chance_none_glob:
                buckets["u_mine_it_maps"]["chanceNoneGlob"] = chance_none_glob

        elif "_regionalschematics" in ref_edid_lower:
            # Regional Schematics bucket
            if buckets["regional_schematics"] is None:
                buckets["regional_schematics"] = {"items": []}
            sub_items = resolve_lvli_items_deep(ref_fid)
            buckets["regional_schematics"]["items"].extend(sub_items)

        elif "_progressionitems" in ref_edid_lower:
            # Progression Items bucket (GrabBag)
            if buckets["progression_items"] is None:
                buckets["progression_items"] = {"items": []}
            sub_items = resolve_lvli_items_deep(ref_fid)
            buckets["progression_items"]["items"].extend(sub_items)

        elif "ll_chems_stimpak" in ref_edid_lower or "_chems_" in ref_edid_lower:
            # Chems bucket
            sub_items = resolve_lvli_items_deep(ref_fid)
            if sub_items:
                for item in sub_items:
                    item["conditions"] = parent_conditions + (item.get("conditions") or [])
                buckets["chems"].extend(sub_items)
            else:
                # Fallback: LL_Chems_Stimpak has empty ref in TSV (xEdit export gap)
                # The actual item is Stimpak (ALCH 00023736)
                buckets["chems"].append({
                    "formid": "00023736", "name": "Stimpak",
                    "qty": 1, "dropRate": 1.0, "edid": "Stimpak",
                    "sig": "ALCH", "conditions": parent_conditions,
                })

    return buckets

def resolve_region_grabbag(grabbag_formid, activity_region_names):
    """
    Resolve QuestReward_LLS_AllRegions_GrabBag into per-region item lists.
    activity_region_names: list of display names like ["The Mire", "Ash Heap"]
    Returns {"availableRegions": [...], "byRegion": {...}}
    """
    # Map EDID substrings → display name
    region_map = {
        "forest":        "Forest",
        "toxicvalley":   "Toxic Valley",
        "savagedivide":  "Savage Divide",
        "ashheap":       "Ash Heap",
        "mire":          "The Mire",
        "cranberrybog":  "Cranberry Bog",
        "skylinevalley": "Skyline Valley",
        "burningsprings":"Burning Springs",
    }

    # Build a normalised set for matching: "themire" → "The Mire", "ashheap" → "Ash Heap"
    norm_to_display = {}
    for display_name in activity_region_names:
        norm = display_name.lower().replace(" ", "").replace("the", "")
        norm_to_display[norm] = display_name
    # Also add un-stripped versions
    for display_name in activity_region_names:
        norm_to_display[display_name.lower().replace(" ", "")] = display_name

    by_region = {}
    available = []

    entries = lvli_entries_by_list.get(grabbag_formid, [])
    for entry in entries:
        ref = (entry.get("LVLO_Reference") or "").strip()
        if ":" not in ref:
            continue

        ref_fid = ref.split(":")[0]
        ref_edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
        ref_edid_lower = ref_edid.lower()

        # Detect region from EDID
        detected_display = None
        for edid_key, display_name in region_map.items():
            if edid_key in ref_edid_lower:
                # Check if this region is in our activity regions
                norm_key = display_name.lower().replace(" ", "").replace("the", "")
                norm_key2 = display_name.lower().replace(" ", "")
                if norm_key in norm_to_display or norm_key2 in norm_to_display:
                    detected_display = display_name
                break

        if detected_display:
            if detected_display not in available:
                available.append(detected_display)
            # Resolve the region GrabBag entries
            region_entries = lvli_entries_by_list.get(ref_fid, [])
            region_items = []
            # Count entries per sub-LVLI for weighting
            ref_counts = defaultdict(int)
            for re_entry in region_entries:
                re_ref = (re_entry.get("LVLO_Reference") or "").strip()
                if re_ref:
                    ref_counts[re_ref] += 1

            total_entries = len(region_entries)
            seen_refs = set()
            for re_entry in region_entries:
                re_ref = (re_entry.get("LVLO_Reference") or "").strip()
                if not re_ref or ":" not in re_ref:
                    continue
                re_fid = re_ref.split(":")[0]
                re_edid = re_ref.split(":")[1] if len(re_ref.split(":")) > 1 else ""
                re_sig = re_ref.split(":")[-1] if re_ref.count(":") >= 2 else ""

                # Determine category from actual item signature first, then EDID
                re_edid_lower = re_edid.lower()
                re_sig_upper = re_sig.upper() if re_sig else ""
                if re_sig_upper == "AMMO":
                    category = "Ammo"
                elif re_sig_upper == "WEAP":
                    category = "Weapon"
                elif re_sig_upper in ("ARMO",):
                    category = "Armor"
                elif re_sig_upper == "BOOK":
                    category = "Schematic"
                elif "weapon" in re_edid_lower:
                    category = "Weapon"
                elif "armor" in re_edid_lower or "armour" in re_edid_lower:
                    category = "Armor"
                elif "schematic" in re_edid_lower or "recipe" in re_edid_lower:
                    category = "Schematic"
                else:
                    category = "Other"

                # Weight = count of this ref / total entries (duplicate entries = weighting)
                weight = ref_counts[re_ref] / total_entries if total_entries > 0 else 0

                # Only add once per unique ref (weight captures duplicates)
                if re_ref in seen_refs:
                    continue
                seen_refs.add(re_ref)

                if re_sig.upper() == "LVLI":
                    # Resolve sub-LVLI to leaf items
                    sub_items = resolve_lvli_items_deep(re_fid)
                    for si in sub_items:
                        # Determine category from actual leaf item signature
                        si_sig = si.get("sig", "").upper()
                        if si_sig == "AMMO":
                            si_cat = "Ammo"
                        elif si_sig == "WEAP":
                            si_cat = "Weapon"
                        elif si_sig in ("ARMO",):
                            si_cat = "Armor"
                        elif si_sig == "BOOK":
                            si_cat = "Schematic"
                        else:
                            si_cat = category  # fallback to parent EDID-based category
                        region_items.append({
                            "name": si.get("name", ""),
                            "formid": si.get("formid", ""),
                            "edid": si.get("edid", ""),
                            "dropRate": round(weight * 100, 2),
                            "qty": si.get("qty", 1),
                            "category": si_cat,
                            "conditions": si.get("conditions", []),
                        })
                else:
                    # Direct item reference
                    name = resolve_name_for_formid(re_fid)
                    region_items.append({
                        "name": name,
                        "formid": re_fid,
                        "edid": re_edid,
                        "dropRate": round(weight * 100, 2),
                        "qty": 1,
                        "category": category,
                        "conditions": [],
                    })

            by_region[detected_display] = region_items

    return {
        "availableRegions": available,
        "byRegion": by_region,
    }

def build_activity_data(gmrw_rows, event_key, region_locations):
    """
    Main function to build activityData JSON for an activity event.
    Processes GMRW rows and builds structured reward data.
    """
    activity_data = {
        "baseRewards": {
            "xp": None,
            "xpFormID": None,
            "caps": None,
            "capsFormID": None,
            "legendaryItems": {"rank": None, "dropRate": 0.0, "conditions": []},
            "legendaryModules": None,
            "legendaryScrip": {"items": [], "expectedValue": 0.0},
            "treasuryNotes": None,
            "uMineItMaps": {"dropRate": None, "conditions": []},
        },
        "chemRewards": [],
        "uniqueEventRewards": [],
        "regionRewards": {"availableRegions": [], "byRegion": {}},
        "planRewards": [],
    }

    # Extract region display names from region_locations (e.g., "The Mire", "Ash Heap")
    activity_region_names = list(dict.fromkeys(
        loc.get("region", "").strip()
        for loc in region_locations
        if loc.get("region", "").strip()
    ))

    # ── Collect ALL unique XP entries across GMRW rows (by FormID) ──────────
    # Some events have multiple GMRW records at different quest stages, each
    # awarding XP (e.g. Back on the Beat: stage 600/1000/1401 give 24 XP,
    # stage 5000 gives 193 XP).  We gather them all so the front-end can
    # display every stage's XP via the xpByStage array.
    #
    # XP can come from two sources:
    #   XPCT_XPCurveTable  – curve-based, scales with player level (success/stages)
    #   NAM7_XPGlobal      – flat GLOB value, often used for failure rewards
    #                         (identified by "Failure" in the GLOB EDID)
    _xp_seen_formids = set()
    _xp_entries = []          # list of {gmrwFormID, edid, stageNum, xpCurve, xpValue, hasItems, isFailure}
    _CUT_RE = re.compile(r'(?:^|[_\-])(?:CUT|DEL|ZZZ|DVCT|DVDT|DVPT)(?:[_\-]|$)', re.IGNORECASE)

    # Process GMRW rows
    for rr in gmrw_rows:
        # XP — collect every unique GMRW record's XP (from XPCT or NAM7)
        xpct = (rr.get("XPCT_XPCurveTable") or "").strip()
        nam7 = (rr.get("NAM7_XPGlobal") or "").strip()
        gmrw_fid = (rr.get("FormID") or "").strip()

        if gmrw_fid and gmrw_fid not in _xp_seen_formids:
            # Skip cut / deprecated / removed GMRW records
            _gmrw_edid = (rr.get("EDID") or "").strip()
            if _CUT_RE.search(_gmrw_edid):
                _xp_seen_formids.add(gmrw_fid)   # mark seen so we don't re-check
                continue
            xpv = None
            xp_ref = None
            is_failure = False

            if xpct:
                xpv = xp_at_level(xpct)
                xp_ref = xpct.split(":")[0]
            elif nam7:
                # NAM7 is a flat GLOB value (not curve-based)
                nam7_fid = nam7.split(":")[0]
                if nam7_fid in glob_vals:
                    xpv = int(glob_vals[nam7_fid])
                    xp_ref = nam7_fid
                # Detect failure XP: GLOB EDID contains "Failure"
                is_failure = "failure" in nam7.lower()

            if xpv is not None and xpv > 0:
                _xp_seen_formids.add(gmrw_fid)
                edid = (rr.get("EDID") or "").strip()
                rewarded = (rr.get("RewardedItem") or "").strip()
                # Extract stage number from EDID (e.g. "QuestReward_RS02_Beat_Stage1000_01")
                _stage_m = re.search(r'Stage(\d+)', edid, re.IGNORECASE)
                stage_num = int(_stage_m.group(1)) if _stage_m else 0
                _xp_entries.append({
                    "gmrwFormID": gmrw_fid,
                    "edid":       edid,
                    "stageNum":   stage_num,
                    "xpCurve":    xp_ref,
                    "xpValue":    xpv,
                    "hasItems":   bool(rewarded),
                    "isFailure":  is_failure,
                })

        # Backward-compat: still set the single xp field (first XPCT value found)
        if xpct and not activity_data["baseRewards"]["xp"]:
            xpv_single = xp_at_level(xpct)
            if xpv_single is not None:
                activity_data["baseRewards"]["xp"] = xpv_single
                activity_data["baseRewards"]["xpFormID"] = xpct.split(":")[0]

        # Caps
        caps_ref = (rr.get("NAM8_CapsGlobal") or "").strip()
        if caps_ref and not activity_data["baseRewards"]["caps"]:
            caps_fid = caps_ref.split(":")[0]
            if caps_fid in glob_vals:
                activity_data["baseRewards"]["caps"] = int(glob_vals[caps_fid])
                activity_data["baseRewards"]["capsFormID"] = caps_fid

        # Legendary Rank
        qrlr = (rr.get("QRLR_LegendaryItemRewardRank") or "").strip()
        if qrlr and qrlr not in ("0", ""):
            activity_data["baseRewards"]["legendaryItems"]["rank"] = int(qrlr)

        # RewardedItem LVLI
        rewarded = (rr.get("RewardedItem") or "").strip()
        if not rewarded:
            continue

        formid, kind = parse_ref(rewarded)
        if kind.upper() != "LVLI":
            continue

        lvli_edid = lvli_edid_by_formid.get(formid, "")
        lvli_edid_lower = lvli_edid.lower()

        # Decompose activities list
        if "rewards_activities" in lvli_edid_lower or "ra_ll_rewards" in lvli_edid_lower:
            buckets = decompose_activities_lvli(formid)

            # Process caps
            if buckets["caps"]["items"]:
                for item in buckets["caps"]["items"]:
                    activity_data["baseRewards"]["caps"] = item.get("qty", 0)

            # Process legendary items — the LVLI drop rate is structural, not the actual
            # chance. Legendary items are always awarded at the rank from GMRW.
            # Don't override rank set from GMRW QRLR field.
            if buckets["legendary_items"]["items"]:
                # If GMRW didn't set rank, try to infer from the entry structure
                if not activity_data["baseRewards"]["legendaryItems"]["rank"]:
                    activity_data["baseRewards"]["legendaryItems"]["rank"] = 1
                # Legendary items are always awarded — dropRate is not from LVLI
                activity_data["baseRewards"]["legendaryItems"]["dropRate"] = None

            # Process scrip
            if buckets["scrip"]["items"]:
                qty_counts = defaultdict(float)
                for item in buckets["scrip"]["items"]:
                    qty_counts[item.get("qty", 0)] += item.get("dropRate", 0.0)

                for qty, drop_rate in sorted(qty_counts.items()):
                    activity_data["baseRewards"]["legendaryScrip"]["items"].append({
                        "qty": qty,
                        "dropRate": pct(drop_rate),
                    })

            # Process U-Mine-It Maps
            if buckets["u_mine_it_maps"]["items"]:
                for item in buckets["u_mine_it_maps"]["items"]:
                    activity_data["baseRewards"]["uMineItMaps"]["dropRate"] = pct(item.get("dropRate", 0.0))
                # Use parent-level conditions from the activities entry
                umap_conds = buckets["u_mine_it_maps"].get("conditions", [])
                if umap_conds:
                    activity_data["baseRewards"]["uMineItMaps"]["conditions"] = umap_conds
                # Include ChanceNone GLOB reference if present
                umap_glob = buckets["u_mine_it_maps"].get("chanceNoneGlob", "")
                if umap_glob:
                    activity_data["baseRewards"]["uMineItMaps"]["chanceNoneGlob"] = umap_glob

            # Calculate scrip expected value
            scrip_items = activity_data["baseRewards"]["legendaryScrip"]["items"]
            if scrip_items:
                ev = sum(s["qty"] * s["dropRate"] / 100 for s in scrip_items)
                activity_data["baseRewards"]["legendaryScrip"]["expectedValue"] = round(ev, 2)

            # Process chems
            for item in buckets["chems"]:
                activity_data["chemRewards"].append({
                    "name": item.get("name", ""),
                    "formid": item.get("formid", ""),
                    "dropRate": pct(item.get("dropRate", 0.0)),
                    "qty": item.get("qty", 1),
                    "conditions": item.get("conditions", []),
                })

            # Process regional schematics → Plan Rewards
            # Only include BOOK items (plans/recipes), not weapons/armor from deeper resolution
            if buckets["regional_schematics"] and buckets["regional_schematics"]["items"]:
                seen_plans = set()
                for item in buckets["regional_schematics"]["items"]:
                    sig = item.get("sig", "").upper()
                    name = item.get("name", "")
                    edid = item.get("edid", "")
                    # Filter: only BOOKs, or items whose EDID/name indicates a plan/recipe
                    is_book = sig == "BOOK"
                    is_plan_edid = any(kw in edid.lower() for kw in ["recipe", "plan", "schematic"]) if edid else False
                    is_plan_name = name.startswith(("Plan:", "Recipe:"))
                    if not (is_book or is_plan_edid or is_plan_name):
                        continue
                    # Dedup by formid
                    fid = item.get("formid", "")
                    if fid in seen_plans:
                        continue
                    seen_plans.add(fid)
                    activity_data["planRewards"].append({
                        "name": name,
                        "formid": fid,
                        "edid": edid,
                        "dropRate": pct(item.get("dropRate", 0.0)),
                        "qty": item.get("qty", 1),
                        "isPlan": True,
                    })

            # Process progression items → Region Rewards
            if buckets["progression_items"] and buckets["progression_items"]["items"]:
                # The progression items chain goes: ProgressionItems → AllRegions_GrabBag
                # Find the GrabBag LVLI by looking at sub-entries
                prog_formid = None
                for pe in lvli_entries_by_list.get(formid, []):
                    pe_ref = (pe.get("LVLO_Reference") or "").strip()
                    if "_progressionitems" in pe_ref.lower():
                        prog_fid = pe_ref.split(":")[0]
                        # Now find the GrabBag inside the ProgressionItems LVLI
                        for sub_e in lvli_entries_by_list.get(prog_fid, []):
                            sub_ref = (sub_e.get("LVLO_Reference") or "").strip()
                            if "grabbag" in sub_ref.lower() or "allregions" in sub_ref.lower():
                                prog_formid = sub_ref.split(":")[0]
                                break
                        break

                if prog_formid:
                    region_data = resolve_region_grabbag(prog_formid, activity_region_names)
                    activity_data["regionRewards"] = region_data

        # Handle quest rewards (titles/books)
        elif "_questrewards" in lvli_edid_lower or "_ll_quest" in lvli_edid_lower:
            sub_items = resolve_lvli_items_deep(formid)
            for item in sub_items:
                edid_str = item.get("edid", "")
                sig_str = item.get("sig", "").upper()
                # Try title lookup (BOOK → PLYT/CMPT)
                title_result = book_edid_to_title(edid_str) if edid_str else None
                if title_result:
                    kind_str, td = title_result
                    display_name = td.get("title", "")
                    kind_type = "player_title" if kind_str == "player" else "camp_title"
                    kind_prefix = "Player Title" if kind_str == "player" else "Camp Title"
                    # Get ChanceNone GLOB if present on the entry
                    chance_glob = (
                        lvli_entries_by_list.get(formid, [{}])[0].get("LVOG_ChanceNoneGlobal") or ""
                    ).strip()
                    chance_conds = list(item.get("conditions", []))
                    if chance_glob:
                        glob_edid = chance_glob.split(":")[1] if ":" in chance_glob else chance_glob
                        chance_conds.append(f"ChanceNone GLOB: {glob_edid}")
                    activity_data["uniqueEventRewards"].append({
                        "name": f"{kind_prefix}: {display_name}" if display_name else item.get("name", ""),
                        "formid": item.get("formid", ""),
                        "edid": edid_str,
                        "dropRate": pct(item.get("dropRate", 0.0)) if item.get("dropRate", 0.0) < 1.0 else None,
                        "qty": item.get("qty", 1),
                        "kind": kind_type,
                        "conditions": chance_conds,
                    })
                elif sig_str == "BOOK" or "recipe" in edid_str.lower():
                    # Non-title BOOK quest reward (plan) → UER
                    activity_data["uniqueEventRewards"].append({
                        "name": item.get("name", ""),
                        "formid": item.get("formid", ""),
                        "edid": edid_str,
                        "dropRate": pct(item.get("dropRate", 0.0)) if item.get("dropRate", 0.0) < 1.0 else None,
                        "qty": item.get("qty", 1),
                        "kind": "plan",
                        "conditions": item.get("conditions", []),
                    })
                else:
                    # Other quest reward → UER
                    activity_data["uniqueEventRewards"].append({
                        "name": item.get("name", ""),
                        "formid": item.get("formid", ""),
                        "edid": edid_str,
                        "dropRate": pct(item.get("dropRate", 0.0)) if item.get("dropRate", 0.0) < 1.0 else None,
                        "qty": item.get("qty", 1),
                        "kind": None,
                        "conditions": item.get("conditions", []),
                    })

        # All other LVLI - resolve to unique event rewards
        else:
            sub_items = resolve_lvli_items_deep(formid)
            for item in sub_items:
                dr = item.get("dropRate", 0.0)
                item_name = item.get("name", "")
                # Skip items with zero/negligible drop rate
                if dr <= 0.0:
                    continue
                # Skip items whose name didn't resolve (still raw FormID hex)
                if re.fullmatch(r"[0-9A-Fa-f]{8}", item_name):
                    continue
                is_plan = item_name.startswith(("Plan:", "Recipe:"))
                activity_data["uniqueEventRewards"].append({
                    "name": item_name,
                    "formid": item.get("formid", ""),
                    "edid": item.get("edid", ""),
                    "dropRate": pct(dr) if dr < 1.0 else None,
                    "qty": item.get("qty", 1),
                    "kind": "plan" if is_plan else None,
                    "conditions": item.get("conditions", []),
                })
                if is_plan:
                    activity_data["planRewards"].append({
                        "name": item_name,
                        "formid": item.get("formid", ""),
                        "dropRate": pct(dr) if dr < 1.0 else None,
                        "qty": item.get("qty", 1),
                    })

    # ── Build reward tree for xEdit-style display ──
    reward_tree = []
    seen_lvli_tree = set()
    for rr in gmrw_rows:
        rewarded = (rr.get("RewardedItem") or "").strip()
        if not rewarded:
            continue
        formid, kind = parse_ref(rewarded)

        # Extract GMRW-level conditions
        cond_text = (rr.get("Conditions") or "").strip()
        tier_func = (rr.get("TierConditionFunc") or "").strip()
        tier_val = (rr.get("TierConditionValue") or "").strip()

        if tier_func.lower() == "getrandompercent" and tier_val:
            try:
                gmrw_mult = max(0.0, min(1.0, float(tier_val) / 100.0))
            except (ValueError, TypeError):
                gmrw_mult = 1.0
            gmrw_cond_display = f"GetRandomPercent <= {tier_val}"
        else:
            gmrw_mult = parse_randompercent_multiplier(cond_text)
            gmrw_cond_display = cond_text if gmrw_mult < 1.0 else ""

        if kind.upper() == "LVLI":
            if formid in seen_lvli_tree:
                continue
            seen_lvli_tree.add(formid)
            tree_node = build_lvli_tree_node(formid)
            if tree_node and (tree_node.get("children") or tree_node.get("items")):
                tree_node["gmrwDropRate"] = round(gmrw_mult * 100, 6)
                if gmrw_cond_display:
                    tree_node["gmrwConditions"] = [gmrw_cond_display]
                reward_tree.append(tree_node)
        else:
            # Non-LVLI direct reward (e.g. a single BOOK)
            name = resolve_name_for_formid(formid)
            reward_tree.append({
                "type": "leaf",
                "formid": formid,
                "name": name or formid,
                "qty": 1,
                "dropRate": round(gmrw_mult * 100, 6),
                "conditions": [gmrw_cond_display] if gmrw_cond_display else [],
                "edid": rewarded.split(":")[1] if ":" in rewarded else "",
                "sig": kind.upper(),
            })

    # ── Inject missing Chem rewards into the Activity Rewards tree node ──
    # Some LVLI entries (e.g. LL_Chems_Stimpak) have incomplete TSV exports and
    # resolve to empty tree nodes.  If we have chemRewards data, inject them as
    # items into the Activity Rewards tree node so they appear on the page.
    if activity_data["chemRewards"]:
        for tree_node in reward_tree:
            if tree_node.get("edid", "") == "RA_LL_Rewards_Activities" and tree_node.get("useAll"):
                # Check if chems are already present
                child_edids = {c.get("edid", "").lower() for c in tree_node.get("children", [])}
                has_chems = any("ll_chems" in e or "chems_stimpak" in e for e in child_edids)
                if not has_chems:
                    chem_items = []
                    for cr in activity_data["chemRewards"]:
                        chem_items.append({
                            "formid": cr.get("formid", ""),
                            "edid": "",
                            "name": cr.get("name", ""),
                            "qty": cr.get("qty", 1),
                            "sig": "ALCH",
                            "dropRate": 100.0,
                        })
                    if chem_items:
                        chem_node = {
                            "type": "lvli",
                            "formid": "0052B10C",
                            "edid": "LL_Chems_Stimpak",
                            "label": "Chems",
                            "useAll": False,
                            "items": chem_items,
                            "entryRate": 100.0,
                            "conditions": simplify_conditions(
                                [c for cr in activity_data["chemRewards"] for c in cr.get("conditions", [])]
                            ),
                        }
                        tree_node["children"].append(chem_node)
                break

    activity_data["rewardTree"] = reward_tree

    # Sort plan rewards alphabetically
    activity_data["planRewards"].sort(key=lambda x: (x.get("name") or "").lower())

    # Attach weapon/armour mod-slot breakdowns (from ObjectTemplate TSVs)
    for uer_item in activity_data["uniqueEventRewards"]:
        fid = uer_item.get("formid", "")
        if not fid:
            continue
        slots = None
        if fid in weap_mod_slots_by_formid:
            slots = weap_mod_slots_by_formid[fid]
        elif fid in armo_mod_slots_by_formid:
            slots = armo_mod_slots_by_formid[fid]
        if slots:
            # Strip the includeIndex (internal sorting key) before emitting
            uer_item["modSlots"] = [
                {"label": s["label"], "value": s["value"]}
                for s in slots
            ]

    # Sort unique event rewards: titles first, then others
    def _uer_sort_key(item):
        kind = item.get("kind") or ""
        is_title = "title" in kind
        return (0 if is_title else 1, (item.get("name") or "").lower())
    activity_data["uniqueEventRewards"].sort(key=_uer_sort_key)

    # ── Build xpByStage / xpFailed when multiple GMRW records award XP ──────
    # Sort by stage number, label the one with items as "Event Completion XP",
    # mark entries with isFailure as "Event Failure XP", and number the rest
    # as stage checkpoints.
    #
    # Two output paths:
    #   a) xpByStage array  – when there are 3+ entries, or 2 non-failure entries
    #   b) xpSuccess/xpFailed – simple success+failure pair (1 success, 1 failure)
    #      The JS renderXpExpand already supports both.

    failure_entries = [e for e in _xp_entries if e["isFailure"]]
    success_entries = [e for e in _xp_entries if not e["isFailure"]]

    if failure_entries:
        # Pick the failure XP value (use the first; in practice there's only one)
        fail_xp = failure_entries[0]["xpValue"]
        activity_data["baseRewards"]["xpFailed"] = fail_xp

    if len(success_entries) > 1:
        # Multiple success stages → use xpByStage
        success_entries.sort(key=lambda e: e["stageNum"])

        # Identify the completion stage: entry with items and highest XP
        completion_idx = None
        for i, entry in enumerate(success_entries):
            if entry["hasItems"]:
                if completion_idx is None or entry["xpValue"] > success_entries[completion_idx]["xpValue"]:
                    completion_idx = i
        if completion_idx is None:
            completion_idx = len(success_entries) - 1

        xp_by_stage = []
        checkpoint_counter = 1
        # Count how many checkpoints there are (non-completion, non-failure)
        num_checkpoints = sum(1 for i, e in enumerate(success_entries) if i != completion_idx)
        for i, entry in enumerate(success_entries):
            if i == completion_idx:
                label = "Event Completion XP"
            elif num_checkpoints == 1:
                label = "Checkpoint XP"
                checkpoint_counter += 1
            else:
                label = f"Checkpoint {checkpoint_counter} XP"
                checkpoint_counter += 1
            xp_by_stage.append({
                "label":       label,
                "xp":          entry["xpValue"],
                "gmrwFormID":  entry["gmrwFormID"],
                "curveFormID": entry["xpCurve"],
            })

        # Append failure entry to the xpByStage array if present
        for fe in failure_entries:
            xp_by_stage.append({
                "label":       "Event Failure XP",
                "xp":          fe["xpValue"],
                "gmrwFormID":  fe["gmrwFormID"],
                "curveFormID": fe["xpCurve"],
            })

        activity_data["baseRewards"]["xpByStage"] = xp_by_stage

        # Set the main xp field to the completion value for backward compat
        comp = success_entries[completion_idx]
        activity_data["baseRewards"]["xp"] = comp["xpValue"]
        activity_data["baseRewards"]["xpFormID"] = comp["xpCurve"]

    elif len(success_entries) == 1 and failure_entries:
        # Simple success+failure pair → use xpSuccess/xpFailed
        # (the JS renderXpExpand handles this case separately)
        succ = success_entries[0]
        activity_data["baseRewards"]["xpSuccess"] = succ["xpValue"]
        # Make sure main xp is set to the success value
        activity_data["baseRewards"]["xp"] = succ["xpValue"]
        activity_data["baseRewards"]["xpFormID"] = succ["xpCurve"]

    return activity_data

# --------------------------------------------------
# Index: CURV / XP at level 50
# --------------------------------------------------

curv_by_formid = {}
for r in CURV:
    fid = pick(r, "CURV_FormID", "FormID")
    if fid: curv_by_formid[fid] = {"formid": fid, "edid": pick(r, "CURV_EDID", "EDID")}

_curv_pts = defaultdict(list)
for r in CURV_POINTS:
    fid = pick(r, "CURV_FormID", "FormID")
    try: _curv_pts[fid].append((float(r.get("X") or 0), float(r.get("Y") or 0)))
    except (ValueError, TypeError): pass

def xp_at_level(curv_ref, level=50):
    fid = curv_ref.split(":")[0] if ":" in curv_ref else curv_ref
    pts = _curv_pts.get(fid)
    if not pts: return None
    return int(sorted(pts, key=lambda p: abs(p[0] - level))[0][1])

# --------------------------------------------------
# Index: PLYT / CMPT
# --------------------------------------------------

plyt_by_edid = {}
for r in PLYT:
    edid = pick(r, "EDID - Editor ID", "EDID")
    if edid:
        plyt_by_edid[edid] = {
            "title":    pick(r, "ANAM - Male Title", "ANAM"),
            "isPrefix": str(pick(r, "PTPR - Is Prefix", "PTPR")).lower() == "true",
            "isSuffix": str(pick(r, "PTSU - Is Suffix", "PTSU")).lower() == "true",
        }

cmpt_by_edid = {}
for r in CMPT:
    edid = pick(r, "EDID")
    if edid:
        cmpt_by_edid[edid] = {
            "title":    pick(r, "ANAM - Title", "ANAM"),
            "isPrefix": str(pick(r, "PTPR - Is Prefix", "PTPR")).lower() == "true",
            "isSuffix": str(pick(r, "PTSU - Is Suffix", "PTSU")).lower() == "true",
        }

def book_edid_to_title(book_edid):
    # Remove _Recipe_ from BOOK EDID to match PLYT/CMPT EDIDs
    base = book_edid.replace("_Recipe_", "_")
    # Try PLYT (player titles): "PlayerTitle_Prefix_X" → "PlayerTitles_Prefix_X"
    s = base.replace("Title_", "Titles_")
    if s in plyt_by_edid: return ("player", plyt_by_edid[s])
    # Try CMPT (camp titles): "CampTitle_Prefix_X" → "CAMPTitles_Prefix_X"
    # Handle case variations: CampTitle_, CAMPTitle_, camptitle_
    s2 = re.sub(r"(?i)CampTitle_", "CAMPTitles_", base)
    if s2 in cmpt_by_edid: return ("camp", cmpt_by_edid[s2])
    # Also try just replacing Title_ → Titles_ on the base (handles CampTitle_ → CampTitles_)
    s3 = base.replace("Title_", "Titles_")
    # Normalize case: try upper-casing CAMP prefix
    s3 = re.sub(r"(?i)^camptitles_", "CAMPTitles_", s3)
    if s3 in cmpt_by_edid: return ("camp", cmpt_by_edid[s3])
    return None

# --------------------------------------------------
# LVLI pool classification
# --------------------------------------------------

KNOWN_FIDS = {
    "0000000F": ("Caps",             "caps"),
    "005652F9": ("Legendary Module", "legendary_modules"),
    "005A5443": ("Treasury Note",    "treasury_notes"),
    "007FDC33": ("Improved Bait",    "improved_bait"),
    "003F7410": ("Legendary Scrip",  "legendary_scrip"),
    "0072D4FC": ("Bobblehead Crate", "bobblehead"),
}
FLUX_EDIDS = {
    "c_NukeFlora_Blue_scrap", "c_NukeFlora_Orange_scrap",
    "c_NukeFlora_Purple_scrap", "c_NukeFlora_Red_scrap", "c_NukeFlora_Yellow_scrap",
}
POOL_TYPE_ORDER = [
    "caps", "chems", "improved_bait", "legendary_item", "legendary_modules",
    "legendary_scrip", "treasury_notes", "bobblehead", "treasure_maps",
    "flux", "player_title", "camp_title",
]

def classify_item_ref(ref_str):
    if not ref_str: return None
    parts = ref_str.split(":")
    fid   = parts[0]
    edid  = parts[1] if len(parts) > 1 else ""
    sig   = parts[-1] if len(parts) > 2 else ""
    if fid in KNOWN_FIDS: return KNOWN_FIDS[fid]
    if sig.upper() == "LGDI": return ("Legendary Item", "legendary_item")
    if edid in FLUX_EDIDS: return ("Flux", "flux")
    if "TreasuryNote" in edid or "Treasury_Note" in edid: return ("Treasury Note", "treasury_notes")
    if "LegendaryModule" in edid: return ("Legendary Module", "legendary_modules")
    if "Stimpak" in edid or edid.lower().startswith("ll_chems"): return ("Chems", "chems")
    if "Map" in edid and ("Lucky" in edid or "mtrz" in edid.lower()): return ("Treasure Map", "treasure_maps")
    if "PlayerTitle" in edid and "Recipe" in edid:
        result = book_edid_to_title(edid)
        if result: return (result[1]["title"], "player_title")
    if "CAMPTitle" in edid and "Recipe" in edid:
        result = book_edid_to_title(edid)
        if result: return (result[1]["title"], "camp_title")
    return None

def classify_pool(lvli_fid):
    types_seen  = {}
    titles      = []
    titles_seen = set()

    def _walk(fid, depth=0, seen=None):
        if seen is None: seen = set()
        if fid in seen or depth > 6: return
        seen.add(fid)
        for entry in lvli_entries_by_list.get(fid, []):
            ref = (entry.get("LVLO_Reference") or "").strip()
            if not ref: continue
            parts    = ref.split(":")
            ref_fid  = parts[0]
            ref_type = parts[-1].upper() if len(parts) > 1 else ""
            if ref_type == "LVLI":
                _walk(ref_fid, depth + 1, seen.copy())
            else:
                c = classify_item_ref(ref)
                if not c: continue
                label, tkey = c
                if tkey not in types_seen:
                    types_seen[tkey] = "Player Title" if tkey == "player_title" else \
                                       "Camp Title"   if tkey == "camp_title"   else label
                if tkey in ("player_title", "camp_title"):
                    edid = parts[1] if len(parts) > 1 else ""
                    if edid and edid not in titles_seen:
                        result = book_edid_to_title(edid)
                        if result:
                            titles_seen.add(edid)
                            kind, td = result
                            titles.append({
                                "title":    td["title"],
                                "kind":     kind,
                                "isPrefix": td["isPrefix"],
                                "isSuffix": td["isSuffix"],
                            })

    _walk(lvli_fid)
    pool_types = []
    for key in POOL_TYPE_ORDER:
        if key in types_seen:
            pool_types.append({"type": key, "label": types_seen[key]})
    for key, label in types_seen.items():
        if not any(p["type"] == key for p in pool_types):
            pool_types.append({"type": key, "label": label})
    return pool_types, titles

# --------------------------------------------------
# Index: GMRW
# --------------------------------------------------

gmrw_rows_by_id     = defaultdict(list)
gmrw_rows_by_parent = defaultdict(list)

for r in GMRW:
    gmrw_fid   = pick(r, "FormID", "GMRW_FormID")
    parent_ref = (r.get("ParentQuestLink") or "").strip()
    parent_fid = parent_ref.split(":")[0] if ":" in parent_ref else parent_ref
    if gmrw_fid: gmrw_rows_by_id[gmrw_fid].append(r)
    if parent_fid: gmrw_rows_by_parent[parent_fid].append(r)

def get_gmrw_rows_for_quest(q):
    qid  = pick(q, "QUEST_FormID", "FormID")
    seen_fids = set()   # tracks GMRW FormIDs already pulled via a source
    all_rows = []
    # 1) GMRW rows parented directly to this quest — add ALL rows (multiple per FormID)
    parent_rows = gmrw_rows_by_parent.get(qid, [])
    all_rows.extend(parent_rows)
    for r in parent_rows:
        seen_fids.add((r.get("FormID") or "").strip())
    # 2) Legacy fallback: GMRWRef0-9 on QUEST row
    for i in range(10):
        ref = q.get(f"GMRWRef{i}")
        if not ref: continue
        gmrw_fid = ref.split(":")[0] if ":" in ref else ref
        if gmrw_fid in seen_fids: continue
        seen_fids.add(gmrw_fid)
        all_rows.extend(gmrw_rows_by_id.get(gmrw_fid, []))
    # 3) Cross-quest mapping: some events have GMRW rewards parented to a different quest
    cross_gmrw_fids = CROSS_QUEST_GMRW.get(qid, [])
    for gmrw_fid in cross_gmrw_fids:
        if gmrw_fid in seen_fids: continue
        seen_fids.add(gmrw_fid)
        all_rows.extend(gmrw_rows_by_id.get(gmrw_fid, []))
    # 4) Ref1 fallback: some GMRW rows have no ParentQuestLink but Ref1 points to the quest
    if not all_rows:
        for gmrw_fid, gmrw_list in gmrw_rows_by_id.items():
            for r in gmrw_list:
                for ri in range(1, 6):
                    ref_col = r.get(f"Ref{ri}", "")
                    if ref_col and qid in ref_col:
                        if gmrw_fid not in seen_fids:
                            seen_fids.add(gmrw_fid)
                            all_rows.extend(gmrw_rows_by_id.get(gmrw_fid, []))
                if all_rows: break
            if all_rows: break
    return all_rows

# --------------------------------------------------
# Base Rewards builder
# --------------------------------------------------

TIER_ORDER = ("", "gold", "silver", "bronze", "mutated")

def build_base_rewards(gmrw_rows):
    by_tier = defaultdict(list)
    for r in gmrw_rows:
        by_tier[(r.get("TierLabel") or "").strip()].append(r)

    tiers = []
    for tier_key in TIER_ORDER:
        if tier_key not in by_tier: continue
        rows = by_tier[tier_key]

        # XP at level 50
        xp_val = xp_formid = None
        for r in rows:
            xpct = (r.get("XPCT_XPCurveTable") or "").strip()
            if xpct:
                fid = xpct.split(":")[0]
                xp_val = xp_at_level(fid)
                xp_formid = fid
                break

        # Caps
        caps_val = caps_formid = None
        for r in rows:
            caps_ref = (r.get("NAM8_CapsGlobal") or "").strip()
            if caps_ref:
                fid = caps_ref.split(":")[0]
                if fid in glob_vals:
                    caps_val    = int(glob_vals[fid])
                    caps_formid = fid
                break

        # Legendary rank
        leg_rank = None
        for r in rows:
            qrlr = (r.get("QRLR_LegendaryItemRewardRank") or "").strip()
            if qrlr and qrlr not in ("0", ""):
                try: leg_rank = int(qrlr)
                except ValueError: pass
                break

        # LVLI pool
        lvli_fid = pool_types = titles = None
        for r in rows:
            item_ref = (r.get("RewardedItem") or "").strip()
            if not item_ref: continue
            fid, sig = parse_ref(item_ref)
            if sig.upper() == "LVLI" and fid:
                lvli_fid = fid
                pool_types, titles = classify_pool(fid)
                break
        if pool_types is None: pool_types = []
        if titles     is None: titles     = []

        tiers.append({
            "tier":          tier_key,
            "xp":            xp_val,
            "xpFormID":      xp_formid,
            "caps":          caps_val,
            "capsFormID":    caps_formid,
            "legendaryRank": leg_rank,
            "lvliFormID":    lvli_fid,
            "poolTypes":     pool_types,
            "titles":        titles,
            "conditions":    [c for r in rows for c in
                              (r.get("TierConditions") or r.get("TierConditionFunc")
                               or r.get("Conditions") or "").split("|")
                              if c.strip()],
        })
    return {"tiers": tiers}

# --------------------------------------------------
# Misc helpers
# --------------------------------------------------

def add_free(free, label, value, meta=None):
    if value is None: return
    if isinstance(value, str) and value.strip() == "": return
    row = {"label": label, "value": value}
    if meta: row["meta"] = meta
    free.append(row)

def merge_conditions(*conds):
    out, seen = [], set()
    for c in conds:
        s = (c or "").strip()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def humanize_party_crasher_name(raw):
    s = (raw or "").strip()
    if not s: return "Party Crasher"
    formid = s.split(":")[0] if ":" in s else ""
    if formid and formid in crea_names: return crea_names[formid].strip()
    edid = s.split(":", 1)[1] if ":" in s else s
    edid = re.sub(r"^Lvl", "", edid)
    edid = re.sub(r"_?PartyCrasher$", "", edid)
    edid = re.sub(r"_", " ", edid).strip()
    edid = re.sub(r"(?<!^)(?=[A-Z])", " ", edid).strip()
    return edid if edid else "Party Crasher"

# --------------------------------------------------
# Guide mapping
# --------------------------------------------------

reward_pages = []
for r in GUIDE:
    slug = (r.get("slug") or "").strip()
    if not slug.endswith("-all-rewards"): continue
    url   = strip_trailing_slash(r.get("url") or "")
    title = (r.get("title") or "").strip()
    base_title = title
    if base_title.lower().endswith(" all rewards"):
        base_title = base_title[:-len(" all rewards")].strip()
    reward_pages.append({"slug": slug, "url": url, "title": title,
                          "eventTitle": base_title, "eventKey": norm_name(base_title)})

reward_pages_by_key = defaultdict(list)
for p in reward_pages:
    if p["eventKey"]: reward_pages_by_key[p["eventKey"]].append(p)

# --------------------------------------------------
# Quest indexing
# --------------------------------------------------

quest_by_key = defaultdict(list)
for q in QUEST:
    qid  = pick(q, "QUEST_FormID", "FormID")
    name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name",
                "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)
    quest_by_key[norm_name(name)].append(q)
    # Also index by EDID so alias lookups can match EDID-based keys
    edid = pick(q, "QUEST_EDID", "EDID", default="")
    edid_key = norm_name(edid)
    if edid_key and edid_key != norm_name(name):
        quest_by_key[edid_key].append(q)

EVENT_KEY_ALIASES = {
    "arealblast":        ["enclaveactivityarealblast"],
    "botsonparade":      ["enclaveactivitybotsonparade"],
    "droppedconnection": ["enclaveactivitydroppedconnection"],
    # Guide title → QUEST name mismatches
    "fasnachtdayparade": ["fasnachtday", "eventfasnachtday", "e01ffasnacht"],
    "gearingup":         ["gearinup", "eventgearinup", "burne01gear"],
    "oneviolentnight":   ["eventoneviolentnight", "mtns04night"],
    "encryptid":         ["eventencryptid", "e01bencryptid"],
    "distinguishedguests": ["eventdistinguishedguests", "mtnm04guest"],
    "lodebaring":        ["eventlodebaring", "mtr08lode"],
    "meatweek":          ["eventgrahmsmeatcook", "e02ameatbbq", "meatcook"],
    "seismicactivity":   ["eventseismicactivity", "e09alauncher"],
    "sinkholesolutions": ["eventsinkholesolutions", "burne02sinkhole"],
    "campfiretales":     ["eventcampfiretales", "e01ctales"],
    "treasurehunter":    ["seasonaltreasurehunter", "e04treasurehunter"],
    "poweringuppowerstation": ["poweringupthundermt", "poweringup", "mtr07power", "mtr07earth", "earthmover"],
    "caravansmilepostzero":   ["milecaravanintro", "milepostzero"],
    # Daily Ops → two modes (Uplink + Decryption)
    "dailyops":          ["dailyopsmode01quest", "dailyopsmode02quest"],
    # Holiday Scorched → Spotlight quest
    "holidayscorched":   ["spotlightholiday2018"],
    # Gleaming Depths — each stage maps to a raid encounter module quest
    "gleamingdepthsstage1": ["rd01enc01bot", "rd01questrewardenc01"],
    "gleamingdepthsstage2": ["rd01enc02drill", "rd01questrewardenc02"],
    "gleamingdepthsstage3": ["rd01enc04encladesquad", "rd01enc04enclavesquad", "rd01questrewardenc03"],
    "gleamingdepthsstage4": ["rd01enc05researchlab", "rd01questrewardenc04"],
    "gleamingdepthsstage5": ["rd01enc06scorchtongue", "rd01questrewardenc05"],
    # The Pitt Expeditions — two missions
    "thepittexpos":      ["xpdpitt01mission", "xpdpitt02mission", "thepittuniondues", "thepittfromashestofire"],
    # Atlantic City Expeditions — placeholder (no expedition quests in current TSV)
    "atlanticcityexpos": ["xpdac"],
}

# Explicit cross-quest GMRW mappings: some events have their GMRW rewards
# parented to a DIFFERENT quest (e.g. a region boss quest that aggregates
# sub-event rewards). Map event quest FormID → list of GMRW FormIDs.
CROSS_QUEST_GMRW = {
    # Dangerous Pastimes — GMRW lives under Neurological Warfare (006AD506)
    "00733DB5": ["0077082C"],  # QuestReward_Storm_DangerousPastimes_Stage9000
    # Gearing Up — main rewards also live under Neurological Warfare
    "007F1E8A": ["0080A089"],  # Burn_E01_QuestReward_GearinUp
    # Sinkhole Solutions — GMRW 00837FEB has no parent, Ref1 points to quest
    "0080BFD0": ["00837FEB"],  # Burn_E02_QuestReward_Sinkhole
}

# Container-based seasonal events: rewards come from opening containers
# (pails, gifts, treat bags) not from GMRW quest rewards. Map event key to
# a list of {"title": ..., "lvliFormID": ...} pool defs that should be injected
# when GMRW returns nothing useful.
CONTAINER_LOOT_EVENTS = {
    "treasurehunter": {
        "description": "Rewards from Mole Miner Pails (found/crafted). Tiers represent pail quality.",
        "pools": [
            {"title": "Ornate Mole Miner Pail (Found)",   "lvliFormID": "005D805A", "tier": ""},
            {"title": "Regular Mole Miner Pail (Found)",   "lvliFormID": "005D8054", "tier": ""},
            {"title": "Dusty Mole Miner Pail (Found)",     "lvliFormID": "005D8056", "tier": ""},
            {"title": "Ornate Mole Miner Pail (Crafted)",  "lvliFormID": "005D8053", "tier": ""},
            {"title": "Regular Mole Miner Pail (Crafted)", "lvliFormID": "005D8059", "tier": ""},
            {"title": "Dusty Mole Miner Pail (Crafted)",   "lvliFormID": "005D8055", "tier": ""},
        ],
    },
    "holidayscorched": {
        "description": "Rewards from Holiday Gifts dropped by Holiday Scorched. Tiers represent gift quality.",
        "pools": [
            {"title": "Large Holiday Gift (Found)",   "lvliFormID": "005DCA88", "tier": ""},
            {"title": "Medium Holiday Gift (Found)",  "lvliFormID": "005DCA8A", "tier": ""},
            {"title": "Small Holiday Gift (Found)",   "lvliFormID": "005DCA89", "tier": ""},
            {"title": "Large Holiday Gift (Crafted)",  "lvliFormID": "005DCA85", "tier": ""},
            {"title": "Medium Holiday Gift (Crafted)", "lvliFormID": "005DCA87", "tier": ""},
            {"title": "Small Holiday Gift (Crafted)",  "lvliFormID": "005DCA86", "tier": ""},
        ],
    },
    "halloweenscorched": {
        "description": "Rewards from Spooky Treat Bags dropped by Spooky Scorched.",
        "pools": [
            {"title": "Spooky Treat Bag",  "lvliFormID": "0062038D", "tier": ""},
        ],
    },
}

# Events that were cut or have no in-game rewards — suppress warnings.
CUT_EVENTS = {"caravansmilepostzero"}

# Enclave activity quest FormIDs — used to detect enclave events and inject
# the shared activities LVLI (008A9106) when it isn't already in GMRW rewards.
ENCLAVE_QUEST_FIDS = set()  # populated from alias keys at runtime
ENCLAVE_ACTIVITIES_LVLI = "008A9106"

# --------------------------------------------------
# Activity region / location — read from xlsx
# Falls back gracefully if the file is missing.
# --------------------------------------------------

def _load_region_location_tsv(path="tsv/events_region_location.tsv"):
    """
    Reads events_region_location.tsv and returns a dict:
      { norm_name(activity_name): [ {"region": str, "location": str}, ... ] }
    Columns: "Activity / Event Name", "Region", "Location / LCTN"
    """
    from collections import defaultdict
    out = defaultdict(list)
    try:
        rows = read_tsv(path)
        for row in rows:
            name     = str(row.get("Activity / Event Name") or "").strip()
            region   = str(row.get("Region") or "").strip()
            location = str(row.get("Location / LCTN") or "").strip()
            if not name: continue
            # Strip leading prefix so norm_name matches the event_key from the guide slug
            bare = re.sub(r"^(enclave\s+)?activity:\s*", "", name, flags=re.IGNORECASE).strip()
            bare = re.sub(r"^event:\s*", "", bare, flags=re.IGNORECASE).strip()
            key  = norm_name(bare)
            if key:
                out[key].append({"region": region, "location": location})
    except FileNotFoundError:
        print(f"[WARN] Region/location TSV not found at {path} — region data will be empty.")
    except Exception as e:
        print(f"[WARN] Could not read region/location TSV: {e}")
    return dict(out)

ACTIVITY_REGION_LOCATIONS = _load_region_location_tsv()

# Maps known regional sub-LVLI EDID substrings to region display names.
# Used to tag items in regional schematic pools with their region.
REGION_BY_SUBLVLI_EDID = {
    "regionforest":        "Forest",
    "regionashheap":       "Ash Heap",
    "regioncranberrybog":  "Cranberry Bog",
    "regionmire":          "Mire",
    "regionsavagedivide":  "Savage Divide",
    "regiontoxicvalley":   "Toxic Valley",
    "regionskylinevalley": "Skyline Valley",
    "regionburningsprings":"Burning Springs",
}

def _quest_has_gmrw(q):
    """Return True if this quest row has at least one non-empty GMRWRef."""
    for i in range(10):
        ref = (q.get(f"GMRWRef{i}") or "").strip()
        if ref: return True
    return False

def _quest_sort_key(q):
    """Sort key that prefers quests with GMRWRefs and penalises Master/CUT quests."""
    edid = (q.get("QUEST_EDID") or q.get("EDID") or "").lower()
    is_master_or_cut = ("_master" in edid or "cut_" in edid or edid.endswith("_misc"))
    has_gmrw = _quest_has_gmrw(q)
    # Also check if GMRW rows exist via parent link
    qid = pick(q, "QUEST_FormID", "FormID")
    has_gmrw_parent = bool(gmrw_rows_by_parent.get(qid))
    # Priority: (0) has GMRWRef + not master, (1) has GMRW parent, (2) has GMRWRef, (3) not master, (4) rest
    score = 0
    if has_gmrw and not is_master_or_cut: score = 0
    elif has_gmrw_parent: score = 1
    elif has_gmrw: score = 2
    elif not is_master_or_cut: score = 3
    else: score = 4
    return (score, edid)

def find_quest_candidates_for_key(event_key):
    event_key = (event_key or "").strip()
    if not event_key: return []
    # Always collect from ALL sources: direct match + aliases + substring
    c = list(quest_by_key.get(event_key, []))
    # Always try aliases too (even when direct match exists)
    for pref in EVENT_KEY_ALIASES.get(event_key, []):
        for qkey, rows in quest_by_key.items():
            if qkey.startswith(pref): c.extend(rows)
    # Substring fallback only when both direct + alias came up empty
    if not c:
        for qkey, rows in quest_by_key.items():
            if event_key in qkey: c.extend(rows)
    # Deduplicate by FormID
    seen = set()
    deduped = []
    for q in c:
        fid = pick(q, "QUEST_FormID", "FormID")
        if fid not in seen:
            seen.add(fid)
            deduped.append(q)
    # Sort so quests with actual reward data come first
    deduped.sort(key=_quest_sort_key)
    return deduped

# --------------------------------------------------
# Event builder
# --------------------------------------------------

events  = []
by_page = {}

for key, pages in sorted(reward_pages_by_key.items()):
    candidates = find_quest_candidates_for_key(key)

    # Skip cut/removed events entirely
    if key in CUT_EVENTS:
        event = {
            "questFormID": "", "name": pages[0]["eventTitle"] or "Event",
            "gameName": "", "freeRewards": [], "conditionalRewards": [],
            "baseRewards": {"tiers": []}, "regionLocations": [],
            "pools": [], "banners": [], "scenarios": [],
            "isCutContent": True,
            "warnings": [{"title": "Cut Content",
                          "message": f"'{pages[0]['eventTitle']}' was cut from the game and has no reward data."}]
        }
    elif not candidates:
        event = {
            "questFormID": "", "name": pages[0]["eventTitle"] or "Event",
            "gameName": "", "freeRewards": [], "conditionalRewards": [],
            "baseRewards": {"tiers": []}, "regionLocations": ACTIVITY_REGION_LOCATIONS.get(key, []),
            "pools": [], "banners": [], "scenarios": [],
            "warnings": [{"title": "Missing QUEST match",
                          "message": f"No QUEST row matched guide title '{pages[0]['eventTitle']}'."}]
        }
    else:
        candidates.sort(key=_quest_sort_key)
        q         = candidates[0]
        qid       = pick(q, "QUEST_FormID", "FormID")
        game_name = pick(q, "FULL - Name", "QUEST_FULL - Name", "QUEST_FULL_Name",
                         "FULL", "QUEST_FULL", "EDID", "QUEST_EDID", default=qid)

        is_public = str(q.get("IsPublicEvent") or q.get("PublicEvent") or "0").strip() == "1"

        event = {
            "questFormID": qid, "name": pages[0]["eventTitle"] or game_name,
            "gameName": game_name, "isPublicEvent": is_public,
            "description": pick(q, "DESC - Description", "DESC", default=""),
            "regionLocations": ACTIVITY_REGION_LOCATIONS.get(key, []),
            "freeRewards": [], "conditionalRewards": [], "baseRewards": {"tiers": []},
            "pools": [], "banners": [], "scenarios": [],
        }

        # Invaders flag
        if str(q.get("InvadersTakeOver") or "0").strip() == "1":
            event["banners"].append({
                "type": "notice", "style": "invaders",
                "lines": ["Invaders Event Take Over",
                          "This event can be taken over by Invaders from Beyond."]
            })

        # Party Crashers
        pc_count = int(q.get("PartyCrasherCount") or 0)
        for i in range(pc_count):
            npc_raw  = q.get(f"PartyCrasher_NPC_{i}")
            glob_raw = q.get(f"PartyCrasher_GLOB_{i}")
            if not npc_raw or not glob_raw: continue
            glob_fid = glob_raw.split(":")[0] if ":" in str(glob_raw) else str(glob_raw)
            if glob_fid not in glob_vals: continue
            event["banners"].append({
                "type": "notice", "style": "party-crasher",
                "lines": [f"Party Crasher: {humanize_party_crasher_name(npc_raw)}",
                          f"{pct(glob_vals[glob_fid])}% chance to spawn at the end of the event."]
            })

        # Flag enclave activities based on quest key aliases
        is_enclave_activity = any(
            key in norm_name(game_name or "")
            for key in ["enclaveactivity", "enclave_activity"]
        ) or any(
            "enclave" in alias
            for aliases in EVENT_KEY_ALIASES.values()
            for alias in aliases
            if norm_name(game_name or "") in alias or alias in norm_name(game_name or "")
        )
        if is_enclave_activity:
            event["isEnclaveActivity"] = True

        # GMRW
        gmrw_rows = get_gmrw_rows_for_quest(q)
        if gmrw_rows:
            event["baseRewards"] = build_base_rewards(gmrw_rows)

        # Detect if this is an activity event
        is_activity = False
        quest_edid = pick(q, "QUEST_EDID", "EDID", default="").lower()
        # Activities use RA_LL_Rewards_Activities or similar patterns
        for rr in gmrw_rows:
            rewarded = (rr.get("RewardedItem") or "").strip()
            if "rewards_activities" in rewarded.lower() or "ra_ll_rewards" in rewarded.lower():
                is_activity = True
                break
        # Also check URL slug pattern
        if not is_activity:
            for p in pages:
                if "/activit" in (p.get("url") or "").lower():
                    is_activity = True
                    break

        if is_activity:
            event["type"] = "activity"
            event["activityData"] = build_activity_data(gmrw_rows, key, event.get("regionLocations", []))

        # freeRewards (legacy / base tier only, for backward compat)
        pool_seen = set()
        free_seen = set()   # dedup: track (label, value) pairs already added
        for rr in gmrw_rows:
            tier_label = (rr.get("TierLabel") or "").strip()

            if tier_label == "":
                xpct = (rr.get("XPCT_XPCurveTable") or "").strip()
                if xpct:
                    xpv = xp_at_level(xpct.split(":")[0])
                    if xpv is not None and ("XP", xpv) not in free_seen:
                        free_seen.add(("XP", xpv))
                        add_free(event["freeRewards"], "XP", xpv,
                                 meta={"source": "GMRW", "curveFormID": xpct.split(":")[0]})
                caps_ref = (rr.get("NAM8_CapsGlobal") or "").strip()
                if caps_ref:
                    fid = caps_ref.split(":")[0]
                    if fid in glob_vals:
                        cv = int(glob_vals[fid])
                        if ("Caps", cv) not in free_seen:
                            free_seen.add(("Caps", cv))
                            add_free(event["freeRewards"], "Caps", cv,
                                     meta={"source": "GMRW", "globFormID": fid})
                qrlr = (rr.get("QRLR_LegendaryItemRewardRank") or "").strip()
                if qrlr and qrlr not in ("0", ""):
                    if ("Legendary Reward Rank", qrlr) not in free_seen:
                        free_seen.add(("Legendary Reward Rank", qrlr))
                        add_free(event["freeRewards"], "Legendary Reward Rank", qrlr,
                                 meta={"source": "GMRW"})

            # Pools (all tiers)
            rewarded = (rr.get("RewardedItem") or "").strip()
            if not rewarded: continue
            formid, kind = parse_ref(rewarded)
            count = (rr.get("RewardedItemCount") or "").strip() or "1"
            conds = merge_conditions(
                rr.get("Conditions"),
                rr.get("TierConditionFunc"),
                rr.get("ConditionGlobs"),
            )
            tier_func  = (rr.get("TierConditionFunc")  or "").strip()
            tier_val   = (rr.get("TierConditionValue")  or "").strip()

            # Compute cond_mult BEFORE synthesis to avoid double-counting.
            # When TierConditionFunc/Value exist, use them directly.
            # The raw Conditions column (e.g. "GetRandomPercent 10100000 5.0") and the
            # synthesised canonical form both match the regex — running both through
            # parse_randompercent_multiplier multiplies the penalty twice.
            if tier_func.lower() == "getrandompercent" and tier_val:
                try:
                    _cond_mult_canon = max(0.0, min(1.0, float(tier_val) / 100.0))
                except (ValueError, TypeError):
                    _cond_mult_canon = 1.0
            else:
                _raw_for_mult = " | ".join(
                    c for c in [rr.get("Conditions"), rr.get("ConditionGlobs")]
                    if (c or "").strip()
                )
                _cond_mult_canon = parse_randompercent_multiplier(_raw_for_mult)

            # Synthesise canonical "GetRandomPercent <= N" into conds for display only
            # (does NOT feed back into cond_mult — that's already computed above).
            if tier_func.lower() == "getrandompercent" and tier_val:
                try:
                    synth = f"GetRandomPercent <= {float(tier_val):.6f}"
                    if synth not in conds:
                        conds = list(conds) + [synth]
                except (ValueError, TypeError):
                    pass

            if kind.upper() == "LVLI":
                pool_key = (formid, rr.get("RewardIndex") or "")
                if pool_key in pool_seen: continue
                pool_seen.add(pool_key)
                lvli_edid = lvli_edid_by_formid.get(formid, "")
                label     = prettify_lvli_label(lvli_edid) or prettify_lvli_label(rewarded.replace(":", "_"))
                cond_mult = _cond_mult_canon  # pre-computed above, avoids double-counting
                lvli_edid_lower = lvli_edid.lower()

                # Detect special pool types for JS routing
                is_regional_schematics = (
                    "rewards_activities_regionalschematics" in lvli_edid_lower
                    or "regional_schematics" in lvli_edid_lower
                    or "regionalschematics" in lvli_edid_lower
                )
                is_progression_items = (
                    "rewards_activities_progressionitems" in lvli_edid_lower
                    or "progression_items" in lvli_edid_lower
                    or "progressionitems" in lvli_edid_lower
                )
                # Enclave routing flags — stamp the MAIN enclave pools so JS can
                # route them to their dedicated expands without EDID string matching.
                # Conditional pools sharing the same EDID keywords (Last Bastion chest,
                # Gatling Plasma plans) are distinguished by cond_mult < 1.0 and must
                # NOT get these flags — they route to Unique Event Rewards instead.
                is_enclave_armour = (
                    cond_mult >= 1.0 and (
                        "scoutuniform" in lvli_edid_lower
                        or "scout_uniform" in lvli_edid_lower
                        or "scoutarmor" in lvli_edid_lower
                    )
                )
                is_enclave_plasma = (
                    "enclave_plasmagun" in lvli_edid_lower
                    or "enclaveplasmagun" in lvli_edid_lower
                    or "plasmagun_all" in lvli_edid_lower
                )

                if is_regional_schematics:
                    # Use region-aware walk so each item gets a region tag
                    region_items_raw = compute_lvli_with_region(formid)
                    seen_fids = {}
                    for ri in region_items_raw:
                        fid2 = ri["formid"]
                        ch2  = ri["chance"] * cond_mult
                        if fid2 not in seen_fids or ch2 > seen_fids[fid2]["dropRate"] / 100:
                            nm2 = resolve_name_for_formid(fid2)
                            seen_fids[fid2] = {
                                "formid":   fid2,
                                "name":     nm2,
                                "dropRate": pct(ch2),
                                "qty":      1,
                                "isPlan":   any(n.startswith(("Plan:", "Recipe:")) for n in [nm2] if n),
                                "region":   ri["region"] or "",
                                "lctn":     ri["lctn"] or "",
                            }
                    items = sorted(seen_fids.values(),
                                   key=lambda x: (x["name"] or "", x["formid"] or ""))
                else:
                    probs = compute_lvli(formid)
                    # Normalise: pick-one lists must sum to 1.0 (100%).
                    # xEdit exports often give apriori=1.0 for every entry, so
                    # we normalise whenever the total deviates from 1.0.
                    _total = sum(probs.values())
                    if _total > 0 and abs(_total - 1.0) > 0.0001:
                        probs = {k: v / _total for k, v in probs.items()}
                    items = sorted([
                        {
                            "formid": fid,
                            "name": resolve_name_for_formid(fid),
                            "dropRate": pct(ch * cond_mult),
                            "qty": 1,
                            "isPlan": any(
                                n.startswith(("Plan:", "Recipe:"))
                                for n in [resolve_name_for_formid(fid)]
                                if n
                            ),
                        }
                        for fid, ch in probs.items()
                    ], key=lambda x: (x["name"] or "", x["formid"] or ""))

                pt, ttl = classify_pool(formid)
                pool_entry = {
                    "title": label or "Reward Pool", "lvliFormID": formid, "lvliEdid": lvli_edid,
                    "tier": tier_label, "count": count, "conditions": conds,
                    "poolChance": pct(cond_mult), "poolTypes": pt, "items": items,
                    "itemCount": len(items),
                }
                if is_regional_schematics: pool_entry["isRegionalSchematics"] = True
                if is_progression_items:   pool_entry["isProgressionItems"]   = True
                if is_enclave_armour:      pool_entry["isEnclaveArmour"]      = True
                if is_enclave_plasma:      pool_entry["isEnclavePlasmaGun"]   = True
                event["pools"].append(pool_entry)
            else:
                nm = resolve_name_for_formid(formid) if formid else rewarded
                is_plan = nm.startswith(("Plan:", "Recipe:")) if nm else False
                cond_mult_item = _cond_mult_canon  # pre-computed above, avoids double-counting
                if cond_mult_item < 1.0:
                    # Conditional drop (e.g. GetRandomPercent) — goes to conditionalRewards
                    cond_entry = {
                        "formid":     formid,
                        "name":       nm,
                        "qty":        count,
                        "poolChance": round(cond_mult_item * 100, 6),
                        "isPlan":     is_plan,
                        "conditions": conds,
                        "source":     "GMRW",
                    }
                    # Detect player/camp title and add kind + affix fields
                    edid_parts = rewarded.split(":") if ":" in rewarded else []
                    item_edid  = edid_parts[1] if len(edid_parts) > 1 else ""
                    title_result = book_edid_to_title(item_edid) if item_edid else None
                    if title_result:
                        kind_str, td = title_result
                        cond_entry["kind"] = "player_title" if kind_str == "player" else "camp_title"
                        if td.get("isPrefix"): cond_entry["affix"] = "Prefix"
                        elif td.get("isSuffix"): cond_entry["affix"] = "Suffix"
                    # Mark non-tradeable for BOOKs (plans/titles are never tradeable)
                    item_sig = edid_parts[-1].upper() if edid_parts else ""
                    if item_sig == "BOOK" or is_plan or title_result:
                        cond_entry["tradeable"] = False
                    event["conditionalRewards"].append(cond_entry)
                else:
                    add_free(event["freeRewards"], "Guaranteed Reward", f"{nm} x{count}",
                             meta={"source": "GMRW", "rewardedItem": rewarded, "conditions": conds,
                                   "name": nm, "qty": count, "isPlan": is_plan, "isUnique": not is_plan})

        event["pools"].sort(key=lambda p: (p.get("title") or "", p.get("lvliFormID") or ""))

        # For enclave activities: ensure the shared activities LVLI 008A9106 is present
        if event.get("isEnclaveActivity"):
            # If 008A9106 was already added by the GMRW loop (without the flag), stamp it now.
            for _p in event["pools"]:
                if _p["lvliFormID"] == ENCLAVE_ACTIVITIES_LVLI:
                    _p["isEnclaveActivities"] = True
            has_act = any(p["lvliFormID"] == ENCLAVE_ACTIVITIES_LVLI for p in event["pools"])
            if not has_act:
                act_edid  = lvli_edid_by_formid.get(ENCLAVE_ACTIVITIES_LVLI, "")
                act_probs = compute_lvli(ENCLAVE_ACTIVITIES_LVLI)
                act_items = sorted([
                    {
                        "formid": fid,
                        "name": resolve_name_for_formid(fid),
                        "dropRate": pct(ch),
                        "qty": 1,
                        "isPlan": resolve_name_for_formid(fid).startswith(("Plan:", "Recipe:")),
                    }
                    for fid, ch in act_probs.items()
                ], key=lambda x: (x["name"] or "", x["formid"] or ""))
                pt, ttl = classify_pool(ENCLAVE_ACTIVITIES_LVLI)
                event["pools"].append({
                    "title": "Enclave Activity Rewards",
                    "lvliFormID": ENCLAVE_ACTIVITIES_LVLI,
                    "lvliEdid": act_edid,
                    "tier": "", "count": "1", "conditions": [],
                    "poolChance": 100.0, "poolTypes": pt, "items": act_items,
                    "itemCount": len(act_items),
                    "isEnclaveActivities": True,
                })

    # Container-based seasonal events: inject LVLI pools when GMRW yields none
    if key in CONTAINER_LOOT_EVENTS and not event.get("pools"):
        cle = CONTAINER_LOOT_EVENTS[key]
        if cle.get("description"):
            event["containerLootDescription"] = cle["description"]
        event["isContainerLoot"] = True
        for pool_def in cle.get("pools", []):
            formid = pool_def["lvliFormID"]
            lvli_edid = lvli_edid_by_formid.get(formid, "")
            probs = compute_lvli(formid)
            _total = sum(probs.values())
            if _total > 0 and abs(_total - 1.0) > 0.0001:
                probs = {k: v / _total for k, v in probs.items()}
            items = sorted([
                {
                    "formid": fid,
                    "name": resolve_name_for_formid(fid),
                    "dropRate": pct(ch),
                    "qty": 1,
                    "isPlan": any(
                        n.startswith(("Plan:", "Recipe:"))
                        for n in [resolve_name_for_formid(fid)]
                        if n
                    ),
                }
                for fid, ch in probs.items()
            ], key=lambda x: (x["name"] or "", x["formid"] or ""))
            pt, ttl = classify_pool(formid)
            event["pools"].append({
                "title": pool_def["title"],
                "lvliFormID": formid,
                "lvliEdid": lvli_edid,
                "tier": pool_def.get("tier", ""),
                "count": "1",
                "conditions": [],
                "poolChance": 100.0,
                "poolTypes": pt,
                "items": items,
                "itemCount": len(items),
                "isContainerLoot": True,
            })
        # Remove the "Missing QUEST match" warning since we now have data
        event["warnings"] = [w for w in event.get("warnings", [])
                             if w.get("title") != "Missing QUEST match"]

    for p in pages:
        if p["slug"]: by_page[p["slug"]] = event
        if p["url"]:
            by_page[p["url"]] = event
            by_page[strip_trailing_slash(p["url"])] = event
    events.append(event)

# --------------------------------------------------
# Write output
# --------------------------------------------------

DIST_DIR.mkdir(parents=True, exist_ok=True)
PATCHLOG_DIR.mkdir(parents=True, exist_ok=True)

with open(DIST_DIR / "events_rewards.json", "w", encoding="utf-8") as f:
    json.dump({"events": events}, f, separators=(",", ":"))
with open(DIST_DIR / "events_rewards_by_page.json", "w", encoding="utf-8") as f:
    json.dump({"byPage": by_page}, f, separators=(",", ":"))
with open(PATCHLOG_DIR / "patchlog_latest_df_events.json", "w", encoding="utf-8") as f:
    json.dump({"built": True}, f)

print(f"Events Rewards build complete. events={len(events)} byPage={len(by_page)}")
