#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_treasure_maps_json.py
===========================
Builds: dist/treasure_maps.json

Reads xEdit TSV exports and resolves the Treasure Map + U Mine It LVLI
hierarchies into structured JSON with drop rates.

Uses rng76 engine for deep LVLI resolution — flattens nested leveled lists
to leaf items with calculated drop rates per the drop-rate-engine skill rules.

Key LVLIs:
  LL_TreasureMap (0038B471)           - FirstMatch by region (5 entries)
  LLS_TreasureMap_{FF,TV,MTN,MTR,CB_SF} - pick-one map pools per region
  LL_TreasureMap_Reward (001A7220)    - UseAll reward chain (shared pools)
  LLS_TreasureMap_Reward_Base (0050CC2C) - region-specific reward branch
  MTRz05_LL_01_MinerMine (0032AD5F)   - UseAll Independent (max_count=0)
  MTRz05_LL_02_ProspectorMine (0032AD60)
  MTRz05_LL_03_ExcavatorMine (0032AD61)
  MTRZ05_Vendor_LuckyMaps (0086A8AF)  - pick-one lucky map vendor pool

Usage:
  python build_treasure_maps_json.py
"""

import csv, glob, json, os, re, sys
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

# ── rng76 engine import ──────────────────────────────────────────────────
_SRC = Path(__file__).resolve().parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from rng76 import (
    Rng76Data, safe_float, humanize_edid, fmt_pct,
)
import tsv_source          # one resolver for every export selection

_REPO_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR   = _REPO_ROOT / "dist"
TSV_DIR    = _REPO_ROOT / "tsv"

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)

def newest(pattern, exclude_substrings=None):
    # Delegate to tsv_source. The old basename tie-break handed same-date ties to
    # the companion, so a bare "ALCH_Export_*.tsv" resolved to ..._Effects.tsv —
    # no FULL column, no Keywords_Flat, one row per magic effect.
    hit = tsv_source.newest(str(TSV_DIR / pattern),
                            exclude=exclude_substrings, required=False)
    if not hit:
        raise FileNotFoundError(f"No files matching {pattern} in {TSV_DIR}")
    return hit

# xEdit writes one "RefN" back-reference column per referencing record. The GLOB
# export carries 5,577 of them against 5 real columns, so reading it whole costs
# ~1.6 GB for data nothing here looks at. Dropping them on the way in is the
# difference between the build running and being OOM-killed. Note the underscore:
# the COBJ/BOOK exports use "Ref_1".."Ref_37" and those ARE read, so only the
# unsuffixed "RefN" form is dropped.
_RE_BACKREF_COL = re.compile(r"^Ref\d+$")


def _rows_without_backrefs(handle):
    reader = csv.reader(handle, delimiter="\t")
    try:
        header = next(reader)
    except StopIteration:
        return []
    idx = [(i, name) for i, name in enumerate(header)
           if not _RE_BACKREF_COL.match(name)]
    return [{name: (row[i] if i < len(row) else "") for i, name in idx}
            for row in reader]


def read_tsv(path):
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return _rows_without_backrefs(f)
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return _rows_without_backrefs(f)

def pick(row, *keys, default=""):
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return v
    return default


# === PORTED from build_activities_rewards_json.py (OBTS mod-slot resolution) — KEEP IN SYNC. Duplicated per the project's standalone-copy rule. ===

# --- TSV loaders for object-template + omod + mgef data -------------------
try:    WEAP_OT = read_tsv(newest("WEAP_Export_*_ObjectTemplate.tsv"))
except FileNotFoundError: WEAP_OT = []
try:    ARMO_OT = read_tsv(newest("ARMO_Export_*_ObjectTemplate.tsv"))
except FileNotFoundError: ARMO_OT = []
try:    MGEF_DATA = read_tsv(newest("MGEF_Export_*March*.tsv"))
except FileNotFoundError:
    try:    MGEF_DATA = read_tsv(newest("MGEF_Export_*.tsv"))
    except FileNotFoundError: MGEF_DATA = []
# Load ALL OMOD exports and merge — different exports may have different DESC fields
OMOD_DATA = []
for _omod_f in tsv_source.all_matching(str(TSV_DIR / "OMOD_Export_*.tsv")):
    try:    OMOD_DATA.extend(read_tsv(_omod_f))
    except Exception: pass

# --------------------------------------------------
# Index: WEAP Object Template (mod slots for named/unique weapons)
# --------------------------------------------------

# Slot labels inferred from OMOD EDID keywords or attach point index
_MOD_SLOT_LABELS = {
    "appearance": "Appearance",
    "paint":      "Appearance",
    "weapon_paint": "Appearance",
    "legendary1":   "1★ Legendary",
    "legendary_weapon1": "1★ Legendary",
    "legendary2":   "2★ Legendary",
    "legendary_weapon2": "2★ Legendary",
    "legendary3":   "3★ Legendary",
    "legendary_weapon3": "3★ Legendary",
    "legendary4":   "4★ Legendary",
    "legendary_weapon4": "4★ Legendary",
    "legendary5":   "5★ Legendary",
    "legendary_weapon5": "5★ Legendary",
    "custom":    "Unique",
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
    """Classify a mod OMOD reference string into a human-readable slot label + value + OMOD FormID."""
    if not mod_ref_str:
        return None, None, ""
    parts = mod_ref_str.strip()
    display_name = ""
    m = re.search(r'"([^"]+)"', parts)
    if m:
        display_name = m.group(1)
    omod_fid = ""
    m2 = re.search(r'\[OMOD:([0-9A-Fa-f]+)\]', parts)
    if m2:
        omod_fid = m2.group(1).upper()
    edid = re.split(r'["\[]', parts)[0].strip()
    edid_lower = edid.lower()

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
        h = re.sub(r"^mod_", "", edid, flags=re.IGNORECASE)
        h = re.sub(r"\s*\[OMOD:[0-9A-Fa-f]+\]", "", h)
        h = h.replace("_", " ").strip()
        value = h if h else edid
    return label, value, omod_fid

def _mod_slot_sort_key(slot):
    """Sort mod slots: Legendary stars first, then Unique/Custom, then everything else."""
    label = slot.get("label", "")
    if "Legendary" in label:
        m = re.search(r"(\d)", label)
        return (0, int(m.group(1)) if m else 0)
    if label.lower() in ("unique", "custom"):
        return (1, 0)
    return (2, slot.get("includeIndex", 0))


# --------------------------------------------------
# Junk mod filtering + custom name prettification
# --------------------------------------------------

_JUNK_MOD_VALUES = {
    "no upgrade",
    "default appearance",
    "no muzzle",
    "no customization",
    "no sights",
    "no custom",
    "standard ironsights",
}

_JUNK_MOD_PATTERNS = re.compile(
    r"(?i)"
    r"(?:range\s*offset\s*for\s*.+)"
    r"|(?:.*dummynoeffect.*)"
    r"|(?:modcol\s*.+)"
    r"|(?:\w+\s+\w+\s+nozzle)"
)

def _is_junk_mod(value):
    """Return True if a mod display value is engine junk / placeholder."""
    if not value:
        return True
    v = value.strip().lower()
    if v in _JUNK_MOD_VALUES:
        return True
    if _JUNK_MOD_PATTERNS.fullmatch(v):
        return True
    return False


_CUSTOM_NAME_CLEANUP = {
    "black diamond":                "Black Diamond",
    "perfect storm":                "Perfect Storm",
    "civil unrest":                 "Civil Unrest",
    "all rise":                     "All Rise",
    "voice of set":                 "Voice of Set",
    "slug buster":                  "Slug Buster",
    "anti scorchbeast training pistol": "Anti-Scorchbeast Training Pistol",
    "makeshift ronin blade":        "Makeshift Ronin Blade",
    "blade of bastet":              "Blade of Bastet",
    "grant's saber":                "Grant's Saber",
    "sword of surrender":           "Sword of Surrender",
    "burrows' bane":                "Burrows' Bane",
    "frigid blaze":                 "Frigid Blaze",
    "hellstorm":                    "Hellstorm",
    "kingfisher":                   "Kingfisher",
    "mind over matter":             "Mind Over Matter",
    "motherlode":                   "Motherlode",
    "mechanic's best friend":       "Mechanic's Best Friend",
    "mechanic friend":              "Mechanic's Best Friend",
    "fancy shotgun":                "Fancy Shotgun",
    "fancy revolver":               "Fancy Revolver",
    "the guarantee":                "The Guarantee",
    "whistle in the dark":          "Whistle in the Dark",
    "commander's charge":           "Commander's Charge",
    "final word":                   "Final Word",
    "last bastion":                 "Last Bastion",
    "sole survivor":                "Sole Survivor",
    "salt of the earth":            "Salt of the Earth",
    "night light":                  "Night Light",
    "cursed":                       None,
    "melee knife dmgvscryptid":     "Cryptid Slayer",
    "ranged flamer cursed":         "Cursed",
    "boiling point name":           "Boiling Point",
    "ranged lasergun cursed":       "Cursed",
    "ranged 10mmsmg cursed":        "Cursed",
    "melee pickaxe cursed":         "Cursed",
    "melee shovel cursed":          "Cursed",
    "incendiary":                   "Perfect Storm",
    "head hunter":                  "Head Hunter",
    "dangerous":                    "Ogua Gauntlet",
    "red terror":                   "Red Terror",
    "crimson sky":                  "Crimson Sky",
    "luca's switchblade":           "Luca's Switchblade",
    "elder's mark":                 "Elder's Mark",
    "cultist piercer":              "Cultist Piercer",
    "holy fire":                    "Holy Fire",
    "cursed harpoon gun":           "Cursed Harpoon Gun",
    "cursed shovel":                "Cursed Shovel",
    "cursed pickaxe":               "Cursed Pickaxe",
    "love tap":                     "Love Tap",
    "love-tap":                     "Love Tap",
    "whackersmacker":               "Whacker Smacker",
    "whacker smacker":              "Whacker Smacker",
    "rat bat":                      "Rat Bat",
    "molerat bat":                  "Rat Bat",
    "tillberg's tornado":           "Tillberg's Tornado",
}

_CUSTOM_MOD_DESC_OVERRIDES = {
    "black diamond":       "Splits base damage into ~96 Physical + ~96 Cryo per hit (L50). No DoT — all cryo is direct on-hit.",
    "perfect storm":       "Deals ~21 Fire Damage on impact + Burning DoT (17 dmg × 3 ticks). Burns stack per bullet.",
    "civil unrest":        "+50 AP",
    "all rise":            "+50 HP",
}


def _clean_custom_name(raw_value):
    """
    Clean up a Custom/Unique mod display value into a human-readable weapon prefix.
    Returns the cleaned name, or None if the value shouldn't be used as a prefix.
    """
    if not raw_value:
        return None
    v = raw_value.strip()
    v = re.sub(r"(?i)\s*(?:custom\s*mod|custom\s*name|special\s*effect|paint)$", "", v).strip()
    if not v:
        return None
    key = v.lower()
    if key in _CUSTOM_NAME_CLEANUP:
        return _CUSTOM_NAME_CLEANUP[key]
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 '\-]+", v):
        return v
    h = re.sub(r"(?i)^(?:melee|ranged|custom|weapon|armor|armour)[_ ]*", "", v)
    h = re.sub(r"(?i)[_ ]?(?:name|cursed|dmgvs\w+)$", "", h)
    h = h.replace("_", " ").strip()
    if h and len(h) > 2:
        return h.title()
    return None


def _apply_custom_prefix(cur_name, custom_prefix):
    """Build the final display name from base name and custom prefix, avoiding redundancy."""
    if not custom_prefix or not cur_name:
        return custom_prefix or cur_name or ""
    norm_prefix = re.sub(r"[\s\-]", "", custom_prefix.lower())
    norm_name = re.sub(r"[\s\-]", "", cur_name.lower())
    if norm_prefix in norm_name:
        return cur_name
    if norm_name in norm_prefix:
        return custom_prefix
    return f"{custom_prefix} {cur_name}"


_JUNK_OMOD_DESCS = {"no customization", ""}

def _filter_and_clean_modslots(slots, item_name=None):
    """
    Filter junk mods from a modSlots list and optionally extract the custom name.
    Returns (cleaned_slots, custom_prefix_or_None, custom_description_or_None).
    """
    cleaned = []
    custom_slots = []
    custom_omod_fids = []
    for s in slots:
        label = s.get("label", "")
        value = s.get("value", "")
        if _is_junk_mod(value):
            continue
        if label.lower() in ("unique", "custom"):
            cname = _clean_custom_name(value)
            if cname:
                custom_slots.append((cname, value, s))
            ofid = s.get("omod_fid", "")
            if ofid:
                custom_omod_fids.append(ofid)
            continue
        cleaned.append(s)
    custom_prefix = None
    chosen_idx = None
    _NAME_SUFFIXES = re.compile(r"(?i)\s*(?:custom\s*mod|custom\s*name)\s*$")
    if custom_slots:
        names_only = [c[0] for c in custom_slots]
        good_idxs = [i for i, (c, r, s) in enumerate(custom_slots) if c.lower() != "cursed"]
        if not good_idxs:
            good_idxs = list(range(len(custom_slots)))
        def _name_score(idx):
            c, raw, s = custom_slots[idx]
            has_name_suffix = 1 if _NAME_SUFFIXES.search(raw) else 0
            return (-has_name_suffix, len(c))
        best_idx = min(good_idxs, key=_name_score)
        custom_prefix = custom_slots[best_idx][0]
        chosen_idx = best_idx
        _EFFECT_STRIP = re.compile(r"(?i)\s*(?:special\s*effect|custom\s*mod|custom\s*name|paint)\s*$")
        for i, (cname, raw_val, slot) in enumerate(custom_slots):
            if i == chosen_idx:
                continue
            display = _EFFECT_STRIP.sub("", raw_val).strip() or raw_val.strip()
            if custom_prefix and display.lower() == custom_prefix.lower():
                continue
            cleaned.append({"label": "Custom", "value": display,
                            "omod_fid": slot.get("omod_fid", "")})

    custom_desc = None
    for ofid in custom_omod_fids:
        d = omod_desc_by_formid.get(ofid, "")
        if d and d.strip().lower() not in _JUNK_OMOD_DESCS:
            if custom_desc is None or len(d) > len(custom_desc):
                custom_desc = d
    if not custom_desc and custom_prefix:
        custom_desc = mgef_desc_by_name.get(custom_prefix.lower())

    return cleaned, custom_prefix, custom_desc


# Group OT rows by (FormID, CombinationIndex), storing slots + combo metadata.
_weap_combos = defaultdict(lambda: defaultdict(list))
_weap_combo_names = defaultdict(dict)
_weap_combo_keywords = defaultdict(lambda: defaultdict(set))
for r in WEAP_OT:
    fid = pick(r, "WEAP_FormID", "FormID")
    mod_ref = pick(r, "Include_Mod", "Mod")
    if not fid or not mod_ref:
        continue
    label, value, omod_fid = _classify_mod_slot(mod_ref)
    if label and value:
        combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
        inc_idx = int(pick(r, "IncludeIndex", default="0") or 0)
        _weap_combos[fid][combo_idx].append({
            "label": label,
            "value": value,
            "includeIndex": inc_idx,
            "omod_fid": omod_fid,
        })
    combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
    combo_full = (pick(r, "Combination_FULL", default="") or "").strip()
    if combo_full:
        _weap_combo_names[fid][combo_idx] = combo_full
    mod_edid = re.split(r'["\[]', (mod_ref or ""))[0].strip().lower()
    m = re.match(r"mod_custom_(\w+)", mod_edid)
    if m:
        kw = re.sub(r"_", "", m.group(1)).lower()
        _weap_combo_keywords[fid][combo_idx].add(kw)

weap_mod_slots_by_formid = {}       # fid → best combo slots (fallback)
weap_mod_slots_by_variant = {}      # fid → {variant_key_lower: slots}

for fid, combos in _weap_combos.items():
    variants = {}
    best_combo_idx = None
    best_legendary_count = -1
    for ci, slots in combos.items():
        has_legendary = any("Legendary" in s["label"] for s in slots)
        if not has_legendary:
            continue
        legendary_count = sum(1 for s in slots if "Legendary" in s["label"])
        sorted_slots = sorted(slots, key=_mod_slot_sort_key)
        keys = set()
        cname = _weap_combo_names.get(fid, {}).get(ci, "")
        if cname:
            keys.add(re.sub(r"[^a-z0-9]", "", cname.lower()))
        for kw in _weap_combo_keywords.get(fid, {}).get(ci, set()):
            keys.add(kw)
        for k in keys:
            variants[k] = sorted_slots
        if legendary_count > best_legendary_count or (
            legendary_count == best_legendary_count and (best_combo_idx is None or ci > best_combo_idx)
        ):
            best_legendary_count = legendary_count
            best_combo_idx = ci
    if best_combo_idx is not None:
        best_slots = sorted(combos[best_combo_idx], key=_mod_slot_sort_key)
        weap_mod_slots_by_formid[fid] = best_slots
    if variants:
        weap_mod_slots_by_variant[fid] = variants

# --------------------------------------------------
# Index: ARMO Object Template (mod slots for named/unique armour)
# --------------------------------------------------

_ARMOR_MOD_SLOT_LABELS = {
    "legendary_armor1": "1★ Legendary",
    "legendary_armor2": "2★ Legendary",
    "legendary_armor3": "3★ Legendary",
    "legendary_armor4": "4★ Legendary",
    "legendary_armor5": "5★ Legendary",
    "legendary1":       "1★ Legendary",
    "legendary2":       "2★ Legendary",
    "legendary3":       "3★ Legendary",
    "legendary4":       "4★ Legendary",
    "legendary5":       "5★ Legendary",
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
    """Classify an armour OMOD reference string into a human-readable slot label + value + OMOD FormID."""
    if not mod_ref_str:
        return None, None, ""
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
        if "lining_null" in edid_lower or "no misc" in (display_name or "").lower():
            return None, None, ""
        label = "Mod"

    omod_fid = ""
    m2 = re.search(r'\[OMOD:([0-9A-Fa-f]+)\]', parts)
    if m2:
        omod_fid = m2.group(1).upper()

    if display_name:
        value = display_name
    else:
        h = re.sub(r"^mod_", "", edid, flags=re.IGNORECASE)
        h = re.sub(r"\s*\[OMOD:[0-9A-Fa-f]+\]", "", h)
        h = h.replace("_", " ").strip()
        value = h if h else edid
    return label, value, omod_fid

_armo_combos = defaultdict(lambda: defaultdict(list))
_armo_combo_names = defaultdict(dict)
_armo_combo_keywords = defaultdict(lambda: defaultdict(set))
for r in ARMO_OT:
    fid = pick(r, "ARMO_FormID", "FormID")
    mod_ref = pick(r, "Include_Mod", "Mod")
    if not fid or not mod_ref:
        continue
    label, value, omod_fid = _classify_armor_mod_slot(mod_ref)
    if label and value:
        combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
        inc_idx = int(pick(r, "IncludeIndex", default="0") or 0)
        _armo_combos[fid][combo_idx].append({
            "label": label,
            "value": value,
            "includeIndex": inc_idx,
            "omod_fid": omod_fid,
        })
    combo_idx = int(pick(r, "CombinationIndex", default="0") or 0)
    combo_full = (pick(r, "Combination_FULL", default="") or "").strip()
    if combo_full:
        _armo_combo_names[fid][combo_idx] = combo_full
    mod_edid = re.split(r'["\[]', (mod_ref or ""))[0].strip().lower()
    m = re.match(r"mod_custom_(\w+)", mod_edid)
    if m:
        kw = re.sub(r"_", "", m.group(1)).lower()
        _armo_combo_keywords[fid][combo_idx].add(kw)

armo_mod_slots_by_formid = {}
armo_mod_slots_by_variant = {}

for fid, combos in _armo_combos.items():
    variants = {}
    best_combo_idx = None
    best_legendary_count = -1
    for ci, slots in combos.items():
        has_legendary = any("Legendary" in s["label"] for s in slots)
        if not has_legendary:
            continue
        legendary_count = sum(1 for s in slots if "Legendary" in s["label"])
        sorted_slots = sorted(slots, key=_mod_slot_sort_key)
        keys = set()
        cname = _armo_combo_names.get(fid, {}).get(ci, "")
        if cname:
            keys.add(re.sub(r"[^a-z0-9]", "", cname.lower()))
        for kw in _armo_combo_keywords.get(fid, {}).get(ci, set()):
            keys.add(kw)
        for k in keys:
            variants[k] = sorted_slots
        if legendary_count > best_legendary_count or (
            legendary_count == best_legendary_count and (best_combo_idx is None or ci > best_combo_idx)
        ):
            best_legendary_count = legendary_count
            best_combo_idx = ci
    if best_combo_idx is not None:
        best_slots = sorted(combos[best_combo_idx], key=_mod_slot_sort_key)
        armo_mod_slots_by_formid[fid] = best_slots
    if variants:
        armo_mod_slots_by_variant[fid] = variants

# --------------------------------------------------
# Index: OMOD descriptions for custom mod display
# Keyed by OMOD FormID → DESC text (from OMOD export TSV)
# --------------------------------------------------

omod_desc_by_formid = {}
for r in OMOD_DATA:
    fid = pick(r, "OMOD_FormID", "FormID")
    desc = pick(r, "DESC")
    if fid and desc:
        fid_s = fid.strip()
        desc_s = desc.strip()
        if fid_s not in omod_desc_by_formid or len(desc_s) > len(omod_desc_by_formid[fid_s]):
            omod_desc_by_formid[fid_s] = desc_s

# Fallback: MGEF descriptions keyed by FULL name (lowercased)
mgef_desc_by_name = {}
for r in MGEF_DATA:
    full = pick(r, "FULL")
    dnam = pick(r, "DNAM_MagicItemDescription")
    if full and dnam:
        mgef_desc_by_name[full.strip().lower()] = dnam.strip()

# Known unique/named OT display names (for _is_unique_lvli matching).
# Scan BOTH the fallback (by_formid) combos AND every named variant combo so
# variant-only uniques (e.g. Double-Barrel "Salt Of The Earth", whose fallback
# combo is a different named variant) are still recognised.
_unique_ot_names = set()
def _collect_unique_names(_slots):
    for _slot in _slots:
        if (_slot["label"] or "").lower() in ("unique", "custom"):
            _raw = (_slot["value"] or "").replace(" ", "").lower()
            if _raw and len(_raw) > 3:
                _unique_ot_names.add(_raw)
for _fid, _slots in armo_mod_slots_by_formid.items():
    _collect_unique_names(_slots)
for _fid, _slots in weap_mod_slots_by_formid.items():
    _collect_unique_names(_slots)
for _fid, _variants in weap_mod_slots_by_variant.items():
    for _vk, _slots in _variants.items():
        _collect_unique_names(_slots)
for _fid, _variants in armo_mod_slots_by_variant.items():
    for _vk, _slots in _variants.items():
        _collect_unique_names(_slots)

def _is_unique_lvli(lvli_edid):
    """Check if an LVLI EDID matches a known unique/named item variant."""
    edid_lower = (lvli_edid or "").replace("_", "").lower()
    return any(name in edid_lower for name in _unique_ot_names)


def _resolve_variant_modslots(item_fid, item_sig, lvli_edid, fallback=True):
    sig = (item_sig or "").upper()
    edid_norm = re.sub(r"[^a-z0-9]", "", (lvli_edid or "").lower())
    if sig == "WEAP":
        variants = weap_mod_slots_by_variant.get(item_fid, {})
        fb = weap_mod_slots_by_formid.get(item_fid)
    elif sig == "ARMO":
        variants = armo_mod_slots_by_variant.get(item_fid, {})
        fb = armo_mod_slots_by_formid.get(item_fid)
    else:
        return None
    if variants and edid_norm:
        for vkey, slots in variants.items():
            if vkey in edid_norm:
                return slots
    return fb if fallback else None


def _attach_modslots(item, variant_edid):
    """If this WEAP/ARMO reward came from a unique variant sub-LVLI, resolve its
    OBTS combo and attach modSlots / customModName / customModDescription and the
    unique display name. variant_edid is the intermediate sub-LVLI EDID
    (e.g. LL_Weapon_Ranged_LightningGun_NightLight)."""
    sig = (item.get("sig") or "").upper()
    if sig not in ("WEAP", "ARMO"):
        return item
    fid = (item.get("form_id") or "").upper()
    if not _is_unique_lvli(variant_edid):
        return item
    resolved = _resolve_variant_modslots(fid, sig, variant_edid, fallback=False)
    if not resolved:
        return item
    raw = [{"label": s["label"], "value": s["value"], "omod_fid": s.get("omod_fid", "")} for s in resolved]
    cleaned, custom_prefix, custom_desc = _filter_and_clean_modslots(raw, item.get("name"))
    if cleaned:
        item["modSlots"] = cleaned
    if custom_prefix:
        item["name"] = _apply_custom_prefix(item.get("name", ""), custom_prefix)
        item["customModName"] = custom_prefix
        desc = custom_desc or _CUSTOM_MOD_DESC_OVERRIDES.get(custom_prefix.lower())
        if desc:
            item["customModDescription"] = desc
    return item

# === END PORTED block ===


# ============================================================
# Region definitions
# ============================================================

# Map-name prefixes → region key + location URL
MAP_PREFIX_TO_REGION = OrderedDict([
    ("Forest",        {"key": "forest",        "name": "Forest",        "url": "/df/treasure-maps/forest/"}),
    ("Toxic Valley",  {"key": "toxic_valley",  "name": "Toxic Valley",  "url": "/df/treasure-maps/toxic-valley/"}),
    ("Ash Heap",      {"key": "ash_heap",      "name": "Ash Heap",      "url": "/df/treasure-maps/ash-heap/"}),
    ("Cranberry Bog", {"key": "cranberry_bog", "name": "Cranberry Bog", "url": "/df/treasure-maps/cranberry-bog/"}),
    ("Mire",          {"key": "the_mire",      "name": "The Mire",      "url": "/df/treasure-maps/the-mire/"}),
    ("Savage Divide", {"key": "savage_divide", "name": "Savage Divide", "url": "/df/treasure-maps/savage-divide/"}),
])

# Reward pool FormIDs per region (from LLS_TreasureMap_Reward_Base)
# Note: Cranberry Bog and The Mire share the same reward pool (0050CC2D) but
# are displayed as separate regions for clarity.
REWARD_REGION_FORMIDS = {
    "forest":       "0050CC2E",
    "toxic_valley": "0050CC31",
    "ash_heap":     "0050CC30",
    "cranberry_bog":"0050CC2D",
    "the_mire":     "0050CC2D",
    "savage_divide":"0050CC2F",
}

# Shared reward pool FormIDs (from LL_TreasureMap_Reward [001A7220])
SHARED_POOLS = OrderedDict([
    ("caps",         {"name": "Caps",             "formid": "0050CC2A"}),
    ("recipes",      {"name": "Recipes",          "formid": "004F6DAD"}),
    ("weapon_mods",  {"name": "Weapon Mod Plans",  "formid": "003D73D8"}),
    ("armour_mods",  {"name": "Armour Mod Plans",  "formid": "000673A7"}),
])

# Region display order (alphabetical; "The Mire" sorts under T)
REGION_ORDER = [
    "ash_heap", "burning_springs", "cranberry_bog", "forest",
    "savage_divide", "skyline_valley", "the_mire", "toxic_valley",
]

# Map drop region LVLIs (for extracting all unique maps)
# Note: cranberry_bog and the_mire share the same drop pool (LLS_TreasureMap_CB_SF)
# in the game, but the maps within it are displayed under separate region groups
# based on their name prefix.
MAP_POOL_FORMIDS = OrderedDict([
    ("forest",       "003D0CD5"),
    ("toxic_valley", "003D0CD6"),
    ("ash_heap",     "003D0CD7"),
    ("cranberry_bog","003D0CD8"),
    ("the_mire",     "003D0CD8"),
    ("savage_divide","003D0CD9"),
])

# ============================================================
# U Mine It definitions (unchanged)
# ============================================================

UMINE_TIERS = OrderedDict([
    ("miner",      {"formid": "0032AD5F", "edid": "MTRz05_LL_01_MinerMine",      "name": "Miner Map"}),
    ("prospector", {"formid": "0032AD60", "edid": "MTRz05_LL_02_ProspectorMine",  "name": "Prospector Map"}),
    ("excavator",  {"formid": "0032AD61", "edid": "MTRz05_LL_03_ExcavatorMine",   "name": "Excavator Map"}),
])

# ── Lucky Strike shared quest reward pools (fire on every quest completion) ──
# QuestReward_LLS_Aid_All [LVLI:0043934D] is a pick-one of 6 regional aid pools.
# Each entry's "ChanceNone" slot is actually a MinLvl GLOB (xEdit export trap —
# see drop-rate-engine skill section 5 "MinLvl GLOBs"). At Lv50 every region is
# unlocked, so the pick-one resolves to 1/6 = 16.667% per region pool.
#
# Each LL_Aid_<Region> LVLI is then resolved deep via rng76 to flatten its
# items, and the per-region rates are scaled by the parent 1/6 pick.
UMINE_AID_PARENT_FORMID = "0043934D"  # QuestReward_LLS_Aid_All
UMINE_AID_REGIONS = OrderedDict([
    ("forest",         {"formid": "003CC4A3", "edid": "QuestReward_LLS_Aid_Forest",        "name": "Forest",        "min_lvl": 1}),
    ("toxic_valley",   {"formid": "003CC4A5", "edid": "QuestReward_LLS_Aid_ToxicValley",   "name": "Toxic Valley",  "min_lvl": 10}),
    ("savage_divide",  {"formid": "003CC4A1", "edid": "QuestReward_LLS_Aid_SavageDivide",  "name": "Savage Divide", "min_lvl": 15}),
    ("ash_heap",       {"formid": "003CC4A0", "edid": "QuestReward_LLS_Aid_AshHeap",       "name": "Ash Heap",      "min_lvl": 25}),
    ("the_mire",       {"formid": "003CC4A4", "edid": "QuestReward_LLS_Aid_Mire",          "name": "The Mire",      "min_lvl": 30}),
    ("cranberry_bog",  {"formid": "003CC4A2", "edid": "QuestReward_LLS_Aid_CranberryBog",  "name": "Cranberry Bog", "min_lvl": 35}),
])

# LL_Scrap_Acid [LVLI:007AC791] is the second shared pool — pick-one of 3
# acid quantity variants (×1, ×2, ×3) at 33.333% each. resolve_deep handles
# the quantity variants automatically.
UMINE_ACID_FORMID = "007AC791"  # LL_Scrap_Acid


# ============================================================
# Pint-Sized Phantoms — "Secrets to the Grave" (Slasher seasonal)
# ============================================================
# A mini-quest treasure-map line, modelled on Lucky Strike (u_mine_it):
# the player obtains the Pint-Sized Phantoms' Map, then uses it to find and
# dig up the grave sites the Pint-Sized Phantoms disturbed across Appalachia.
# Digging a grave rolls the repeatable reward pools; the one-time story quest
# chain (MQ01/02/04/05) unlocks the cosmetic "Slasher" plans.
#
# Every record comes from the SDOW_* ("Slasher" / Secrets to the Grave)
# seasonal plugin. If the current TSV export predates that content the map
# BOOK won't resolve and the whole block is omitted (see
# build_pint_sized_phantoms) — so this stays inert on the live channel until
# the Slasher season ships, and populates automatically on the PTS channel.

PHANTOM_MAP_FORMID = "008F15E4"          # SDOW_MQ02_SlasherMap → "Pint-Sized Phantoms' Map"
PHANTOM_MAP_EDID   = "SDOW_MQ02_SlasherMap"
PHANTOM_QUEST_NAME = "Secrets to the Grave"
PHANTOM_QUEST_EDID = "SDOW_MQ02_Graves"
PHANTOM_REPEATABLE_QUEST = "SDOW_SQ01_Graves_Repeatable"

# Repeatable grave-dig loot table SDOW_LL_SQ01_RepeatableRewards [0090312D] —
# a UseAll list with max_count=0, so every entry rolls independently at its own
# fire chance each time you dig a grave (drop-rate-engine §3a/3c). Entry fire
# rates are read from the list itself (ChanceNone × any GetRandomPercent gate)
# rather than hard-coded, so a re-export picks up any tuning automatically.
PHANTOM_REPEATABLE_FORMID = "0090312D"   # SDOW_LL_SQ01_RepeatableRewards (UseAll parent)
PHANTOM_JOURNAL_FORMID     = "008F2AFD"  # SDOW_MQ02_Graves_LL_QuestRelatedItems (journal pages)

# XP + Caps for a grave dig come from the repeatable quest-reward GMRW
# (SDOW_SQ01_QuestRewards → quest SDOW_SQ01_Graves_Repeatable "Laid to Unrest").
PHANTOM_XP_CURVE_FORMID = "00876404"     # CT_Player_XP_Universal_Tier16 (level-scaled)
PHANTOM_CAPS_GLOB       = "0090312C"     # Caps_SQ01_GraveDiggingRepeatable (flat, FLTV)

# The Progression-Items entry funnels into the all-regions grab bag; the game
# rolls the region you're standing in, so it's shown as one sub-pool broken
# into per-region sub-expands (ABC-ordered by region name).
PHANTOM_PROGRESSION_FORMID = "0086A8C3"  # RA_LL_Rewards_Activities_ProgressionItems
PHANTOM_REGION_GRABBAGS = [
    ("Ash Heap",        "00434509"),
    ("Burning Springs", "0081EAD6"),
    ("Cranberry Bog",   "0043450B"),
    ("Forest",          "00434506"),
    ("Savage Divide",   "00434508"),
    ("Skyline Valley",  "0081EAD8"),
    ("The Mire",        "0043450A"),
    ("Toxic Valley",    "00434507"),
]
# Friendly names for the repeatable sub-pools (by referenced FormID). Anything
# not listed falls back to the resolved BOOK/LVLI name.
PHANTOM_POOL_NAMES = {
    "008FAE2A": "Raw Meat",
    "008F863B": "Blood Packs",
    "008F863A": "Skulls",
    "0090312F": "Throwing Knives",
    "0086A8C3": "Progression Items",
    "0090312E": "Rare Rewards",
    "00903130": "Ultra-Rare Rewards",
}
# Journal-page branches (quest-related pool SDOW_MQ02_Graves_LL_QuestRelatedItems).
PHANTOM_JOURNAL_BRANCHES = [
    ("008F2AFC", "First Journal Page",
     "Drops when you have not yet collected any journal pages"),
    ("008F15EA", "Later Journal Pages",
     "Drops once you have collected your first page"),
]

# How the player obtains the map itself (both are FirstMatch pools that only
# fire while the Slasher seasonal content toggle is active).
PHANTOM_MAP_SOURCES = [
    {"source": "Activity Rewards", "list": "SDOW_LL_Rewards_Activities_SlasherMaps",
     "formid": "008F2B68",
     "notes": "Drops from Slasher seasonal activities while the event is active"},
    {"source": "Public Event Rewards", "list": "SDOW_LL_Rewards_PublicEvents_SlasherMaps",
     "formid": "00904724",
     "notes": "Drops on public event completion while the event is active"},
]


# ============================================================
# Helpers
# ============================================================

def simplify_condition(cond_str):
    """Convert verbose xEdit condition strings to human-readable summaries.

    Subset of the activities builder's simplifier — just the conditions that
    actually appear on treasure-map / U-Mine-It LVLIs (Ghoul gating, FO1st,
    learned-recipe). Other functions return "" so they're filtered out.
    """
    s = (cond_str or "").strip()
    if not s:
        return ""

    # GetIsPlayerGhoul → Ghoul / Human character restriction.
    # Trailing "0.000000" = comparison value 0 (must NOT be ghoul → Human only).
    # Trailing "1.000000" = comparison value 1 (must be ghoul → Ghoul only).
    if "GetIsPlayerGhoul" in s:
        if re.search(r'0\.0+\s*$', s):
            return "Human character only"
        return "Ghoul character only"

    # IsPlayerFO1Member → Fallout 1st membership check
    if "IsPlayerFO1Member" in s:
        return "Requires Fallout 1st membership"

    # HasLearnedRecipe → check comparison value
    if "HasLearnedRecipe" in s:
        if re.search(r'0\.0+\s*$', s):
            return "Won’t drop if you’ve already learned this plan"
        return "Requires the base plan to be learned"

    # GetRandomPercent / GetLevel are baked into rates — hide
    if "GetRandomPercent" in s or "GetLevel" in s:
        return ""

    # Internal/engine-only checks — hide
    for _fn in ("GetGlobalValue", "GetInCurrentLocation", "GetInCell",
                "LocationAliasIsLocation", "GetIsAliasRef", "HasEntitlement",
                "GetItemCount", "GetValue", "GetNumTimesCompletedQuest",
                "IsActivePlayer", "GetVMQuestVariable", "GetStageDone",
                "HasKeyword", "GetRemainingQuestTimeSeconds"):
        if _fn in s:
            return ""

    # Raw GLOB / QUST refs without a known wrapper — hide
    if re.match(r'^[0-9A-Fa-f]{8}:', s) and (":GLOB" in s or ":QUST" in s):
        return ""

    # Fallback: strip raw numeric flags at end and clean up
    s = re.sub(r'\s+[01]{8}\s+[\d.]+$', '', s)
    s = re.sub(r'^Subject\.', '', s)
    s = re.sub(r'\(00 00 00.*?\)', '()', s)
    return s.strip() if s.strip() else ""


def simplify_conditions(conditions):
    """Simplify a list of condition strings, dropping empty/internal results."""
    result = []
    for c in (conditions or []):
        s = simplify_condition(c)
        if s and s not in result:
            result.append(s)
    return result


def aggregate_items(items):
    """Aggregate resolved leaf items by formid, sum drop rates, track qty range."""
    agg = {}
    for it in items:
        fid = it["formid"]
        dr = it["dropRate"]
        if dr < 0.000001:
            continue
        if fid not in agg:
            agg[fid] = {
                "formid": fid,
                "name": it["name"],
                "edid": it.get("edid", ""),
                "sig": it.get("sig", ""),
                "drop_rate_raw": 0.0,
                "qty_min": it["qty"],
                "qty_max": it["qty"],
                "conditions": it.get("conditions", []),
            }
        agg[fid]["drop_rate_raw"] += dr
        agg[fid]["qty_min"] = min(agg[fid]["qty_min"], it["qty"])
        agg[fid]["qty_max"] = max(agg[fid]["qty_max"], it["qty"])
    # Cap individual rates at 1.0
    for v in agg.values():
        if v["drop_rate_raw"] > 1.0:
            v["drop_rate_raw"] = 1.0
    return sorted(agg.values(), key=lambda x: (-x["drop_rate_raw"], x["name"]))


def aggregate_items_split_qty(items):
    """Like aggregate_items, but keys on (FormID, quantity) instead of FormID
    alone, and drops qty-0 leaves.

    Used for the Pint-Sized Phantoms pools. Several of those lists are pick-one
    LVLIs where the SAME item appears at several quantities — e.g. the Skulls
    list SDOW_LL_SQ01_Junk_Skull [008F863A] is x5/x4/x3/x2/x1 Skull at
    18/16/14/12/10%, and Throwing Knives is x1/x2/x3 at 33.33% each. Keying on
    FormID alone (aggregate_items) collapsed those into a single "x1-5 @ 70%"
    row and hid the per-quantity odds. Keying on (FormID, quantity) keeps each
    quantity as its own row, while genuine duplicates (same item AND quantity
    from two branches) still merge and sum.

    A qty-0 leaf is a blank/padding entry (the "give 0 of X" outcome used to pad
    a pick-one list); it is not a real drop, so it is dropped rather than folded
    into a sibling's rate/range.

    NOTE: this is a display-aggregation change only — the drop-rate maths in
    rng76.py are unchanged. The Event-Reward standalone build
    (build_activities_rewards_json.py) uses a different, FormID-keyed model
    (compute_lvli) that does not carry quantities and does not build the
    Pint-Sized Phantoms page, so there is no formula divergence to mirror.
    """
    agg = {}
    for it in items:
        dr = it["dropRate"]
        if dr < 0.000001:
            continue
        if str(it["qty"]).strip() in ("0", "0.0"):
            continue
        key = (it["formid"], it["qty"])
        if key not in agg:
            agg[key] = {
                "formid": it["formid"],
                "name": it["name"],
                "edid": it.get("edid", ""),
                "sig": it.get("sig", ""),
                "drop_rate_raw": 0.0,
                "qty_min": it["qty"],
                "qty_max": it["qty"],
                "conditions": it.get("conditions", []),
            }
        agg[key]["drop_rate_raw"] += dr
        agg[key]["qty_min"] = min(agg[key]["qty_min"], it["qty"])
        agg[key]["qty_max"] = max(agg[key]["qty_max"], it["qty"])
    for v in agg.values():
        if v["drop_rate_raw"] > 1.0:
            v["drop_rate_raw"] = 1.0
    return sorted(agg.values(), key=lambda x: (-x["drop_rate_raw"], x["name"]))


def format_item(agg_item):
    """Format an aggregated item for JSON output."""
    qty_min = agg_item["qty_min"]
    qty_max = agg_item["qty_max"]
    qty_str = f"{qty_min}-{qty_max}" if qty_min != qty_max else str(qty_min)

    # Determine tradeable status from raw conditions BEFORE simplification
    # (NonPlayerTradable etc. are stripped by simplify_condition)
    name = agg_item.get("name", "")
    sig = agg_item.get("sig", "")
    raw_conditions = agg_item.get("conditions", [])
    tradeable = True
    for c in raw_conditions:
        if "NonPlayerTrad" in c or "Untradab" in c or "Untradea" in c:
            tradeable = False
            break

    return {
        "name": name,
        "form_id": agg_item["formid"],
        "edid": agg_item["edid"],
        "sig": sig,
        "drop_rate": fmt_pct(agg_item["drop_rate_raw"] * 100),
        "drop_rate_raw": round(agg_item["drop_rate_raw"], 6),
        "qty": qty_str,
        "conditions": simplify_conditions(raw_conditions),
        "tradeable": tradeable,
    }


def has_toggle_condition(entry):
    for c in entry.get("conditions", []):
        if "GetGlobalValue" in c and "Toggle" in c and "1.000000" in c:
            return True
    return False

def has_learned_recipe_condition(entry):
    for c in entry.get("conditions", []):
        if "HasLearnedRecipe" in c:
            return True
    return False

def get_toggle_glob_info(entry):
    for c in entry.get("conditions", []):
        m = re.search(r'(\w+_Toggle)\s*\[GLOB:([0-9A-Fa-f]{8})\]', c)
        if m:
            return m.group(1), m.group(2)
    return None, None

def get_quest_condition(entry):
    for c in entry.get("conditions", []):
        if "PlayerHasQuest" in c:
            m = re.search(r'"([^"]+)"\s*\[QUST:', c)
            if m:
                return f"Requires quest: {m.group(1)}"
            m2 = re.search(r'(\w+)\s*\[QUST:', c)
            if m2:
                return f"Requires quest: {humanize_edid(m2.group(1))}"
    return None


# ============================================================
# Build: Treasure Map Names (grouped by dig-location region)
# ============================================================

def build_map_names(entries_idx, books):
    """Collect all unique treasure maps from all region pools,
    group by dig-location region (name prefix), add location URLs."""

    # Gather all unique maps from every pool
    all_maps = {}
    for rkey, fid in MAP_POOL_FORMIDS.items():
        for e in entries_idx.entries(fid):
            mfid = e["ref_formid"]
            if mfid not in all_maps:
                all_maps[mfid] = {
                    "name": books.name(mfid),
                    "form_id": mfid,
                    "edid": books.edid(mfid),
                }

    # Group by name prefix → region
    region_maps = defaultdict(list)
    for m in sorted(all_maps.values(), key=lambda x: x["name"]):
        name = m["name"]
        parts = name.split(" Treasure Map ")
        prefix = parts[0] if len(parts) == 2 else "Unknown"
        info = MAP_PREFIX_TO_REGION.get(prefix)
        if info:
            m["location_url"] = info["url"]
            region_maps[info["key"]].append(m)

    return region_maps


# ============================================================
# Build: Flattened Shared Reward Pools (rng76 deep resolve)
# ============================================================

def build_shared_rewards(resolver):
    """Resolve each shared reward pool to flattened leaf items."""
    pools = []
    for pool_key, pool_info in SHARED_POOLS.items():
        raw_items = resolver.resolve_deep(pool_info["formid"])
        agg = aggregate_items(raw_items)
        pools.append({
            "pool_key": pool_key,
            "pool_name": pool_info["name"],
            "pool_formid": pool_info["formid"],
            "item_count": len(agg),
            "items": [format_item(it) for it in agg],
        })
    return pools


# ============================================================
# Build: Flattened Region-Specific Rewards (rng76 deep resolve)
# ============================================================

def build_region_rewards(resolver):
    """Resolve each region's specific reward pool to flattened leaf items."""
    region_items = {}
    for rkey, fid in REWARD_REGION_FORMIDS.items():
        raw_items = resolver.resolve_deep(fid)
        agg = aggregate_items(raw_items)
        region_items[rkey] = [format_item(it) for it in agg]
    return region_items


# ============================================================
# Build: Per-Region Mod Boxes (weapon + armour MISC mod items)
# ============================================================
#
# The treasure-map drop tree for weapon and armour mods passes through
# a FirstMatch-by-region LVLI which routes to a region-specific subtree.
# rng76's resolve_deep follows ONE branch (the first entry, Forest), so
# the JSON only ever shows Forest's mod items. The other regions' mod
# subtrees are never visited.
#
# Fix: walk each region's branch directly, then multiply rates by the
# probability of REACHING the FirstMatch parent in the first place. That
# probability is identical for every region (it's the path from the
# treasure map reward down to the FirstMatch list — region-independent).
#
# The path coefficient is computed empirically by comparing Forest's
# items in the full resolve to the same items resolved directly from
# Forest's branch entry. Since Forest is the FirstMatch's first entry
# (cum_fail = 1.0), the ratio is exactly the path coefficient.

PER_REGION_MOD_BRANCHES = OrderedDict([
    # branch_key -> (top_pools, {region_key: child_entry_formid})
    # top_pools: list of LVLI form IDs that lead to this branch's FirstMatch.
    #   The 4 entries inside LL_TreasureMap_Reward fire INDEPENDENTLY (UseAll
    #   with max_count > 4) so an item that's reachable via more than one of
    #   those entries gets multiple independent chances per dig. Mod boxes
    #   appear via two of those entries:
    #     - 003D73D8  weapon-mods-recipes pool   (cn-gated waterfall)
    #     - 004F6DAD  the all-recipes pool       (pick-one of 4 sub-pools)
    #   Both ultimately land in 003D73DA → 003136F5 (FirstMatch by region),
    #   and the per-dig rate is the SUM of contributions from each path.
    ("weapon_mods", {
        "top_pools": ["003D73D8", "004F6DAD"],
        "branches": OrderedDict([
            ("forest",         "003136C0"),  # LL_Recipes_Mods_Weapons_Any_RegionForest
            ("toxic_valley",   "00313696"),  # LL_Recipes_Mods_Weapons_Any_RegionToxicValley
            ("ash_heap",       "003136E6"),  # LL_Recipes_Mods_Weapons_Any_RegionAshHeap
            ("cranberry_bog",  "003136E7"),  # LL_Recipes_Mods_Weapons_Any_RegionCranberryBog
            ("the_mire",       "003136C1"),  # LL_Recipes_Mods_Weapons_Any_RegionMire
            ("savage_divide",  "00313695"),  # LL_Recipes_Mods_Weapons_Any_RegionSavageDivide
        ]),
    }),
    ("armour_mods", {
        # Armour mods sit behind 000673A7 directly AND behind 004F6DAD via
        # its armour-recipes sub-pool 003D73D9 — same dual-path setup.
        "top_pools": ["000673A7", "004F6DAD"],
        "branches": OrderedDict([
            ("forest",         "004F6816"),
            ("toxic_valley",   "004F682D"),
            ("ash_heap",       "004F6829"),
            ("cranberry_bog",  "004F682A"),
            ("the_mire",       "004F682B"),
            ("savage_divide",  "004F682C"),
        ]),
    }),
])


_MOD_BOX_EDID_RE = re.compile(r'^(?:dlc\d+_)?miscmod_mod', re.IGNORECASE)

# More specific patterns for dig-reward categorisation. Armour patterns are
# checked BEFORE the generic weapon pattern so that, e.g.,
# `recipe_mod_armor_RaiderMod_*` lands in armour_mod_plans, not weapon_mod_plans.
_ARMOUR_PLAN_EDID_RE   = re.compile(r'^recipe_mod_(armor|powerarmor|underarmor)', re.IGNORECASE)
_WEAPON_PLAN_EDID_RE   = re.compile(r'^recipe_mod_', re.IGNORECASE)
_ARMOUR_MODBOX_EDID_RE = re.compile(r'^(?:dlc\d+_)?miscmod_mod_(armor|powerarmor|underarmor)', re.IGNORECASE)

def _is_mod_box(item_dict):
    """A mod box is a MISC item whose EDID starts with 'miscmod_mod'
    (optionally prefixed with 'DLC<n>_' for Wastelanders/NW content).
    Excludes test/debug/cut prefixes like 'zzz_', 'CUT_', 'POST_', 'test_'."""
    edid = (item_dict.get("edid", "") or "")
    return bool(_MOD_BOX_EDID_RE.match(edid))


def _is_branch_mod_leaf(item_dict):
    """Items the per-region mod walker is responsible for: MISC mod boxes
    AND BOOK 'recipe_mod_*' plans. Both kinds of leaf live inside the
    per-region weapon/armour mod sub-trees. Other leaves (random WEAP/ARMO
    that may sit alongside) are not handled here — they flow through the
    shared-pool path or the region-specific reward path instead."""
    edid = (item_dict.get("edid", "") or "")
    sig  = (item_dict.get("sig", "") or "").upper()
    if _MOD_BOX_EDID_RE.match(edid):
        return True
    if sig == "BOOK" and _WEAPON_PLAN_EDID_RE.match(edid):
        return True
    return False


def categorize_dig_item(item):
    """Categorise a flattened dig-reward item into one of:
        weapon_mod_plan  - weapon mod plans (BOOK) and weapon mod boxes (MISC)
        armour_mod_plan  - armour / PA / underarmour mod plans + matching mod boxes
        recipe_plan      - general recipes & plans (workshop, food, whole-armour, etc.)
        region_bonus     - non-plan loot themed to the region (WEAP, ARMO, ALCH, AMMO, junk MISC)

    The buckets map directly to the user-facing Dig Rewards sub-expands."""
    edid = (item.get("edid", "") or "")
    sig = (item.get("sig", "") or "").upper()

    # Loose mod boxes (MISC items the player can apply directly to gear).
    if _MOD_BOX_EDID_RE.match(edid):
        if _ARMOUR_MODBOX_EDID_RE.match(edid):
            return "armour_mod_plan"
        return "weapon_mod_plan"

    # BOOK = plans and recipes.
    if sig == "BOOK":
        if _ARMOUR_PLAN_EDID_RE.match(edid):
            return "armour_mod_plan"
        if _WEAPON_PLAN_EDID_RE.match(edid):
            # Every recipe_mod_* that isn't armour-prefixed is a weapon mod plan.
            return "weapon_mod_plan"
        return "recipe_plan"

    # Everything else (WEAP / ARMO / ALCH / AMMO / generic MISC) is the
    # region-themed bonus loot pulled from LLS_TreasureMap_Reward_Region<X>.
    return "region_bonus"


def _compute_path_coeff(resolver, top_pools, forest_entry_id):
    """Empirically derive the total path-to-FirstMatch coefficient.

    Forest is the first entry in the FirstMatch list, so its cum_fail
    multiplier is 1.0 within each top pool's resolve. Forest items in
    resolve_deep(top_pool_id) have rate = path_coeff_for_that_pool ×
    within_forest_rate. Dividing by within_forest_rate (from a direct
    resolve of Forest's branch entry) gives path_coeff per pool.

    LL_TreasureMap_Reward is UseAll independent (max_count > entry count),
    so multiple top pools that route to the same FirstMatch contribute
    INDEPENDENTLY — the total per-dig coefficient is the SUM of each
    top pool's coefficient.

    Uses median ratio per pool to absorb floating-point noise.
    """
    forest_items = resolver.resolve_deep(forest_entry_id)
    forest_by_id = {it["formid"]: it["dropRate"] for it in forest_items}

    total_coeff = 0.0
    for top_pool_id in top_pools:
        full_items = resolver.resolve_deep(top_pool_id)
        ratios = []
        for it in full_items:
            fid = it["formid"]
            ftr = forest_by_id.get(fid)
            if ftr is not None and ftr > 0 and it["dropRate"] > 0:
                ratios.append(it["dropRate"] / ftr)
        if ratios:
            ratios.sort()
            total_coeff += ratios[len(ratios) // 2]
    return total_coeff


def build_per_region_mod_items(resolver):
    """For each region, return the list of mod-box items (raw rng76 format)
    with rates corrected to per-dig probability."""
    per_region_raw = defaultdict(list)
    for branch_name, cfg in PER_REGION_MOD_BRANCHES.items():
        top_pools     = cfg["top_pools"]
        branches      = cfg["branches"]
        forest_entry  = branches["forest"]
        path_coeff    = _compute_path_coeff(resolver, top_pools, forest_entry)
        if path_coeff <= 0:
            continue
        for region_key, entry_fid in branches.items():
            items = resolver.resolve_deep(entry_fid)
            for it in items:
                # Keep mod-box (MISC) and recipe_mod_* plan (BOOK) leaves —
                # both live inside the per-region branch and need the path
                # coefficient applied. Forest's are also handled here: they
                # are stripped from the shared pools by
                # filter_branch_mod_leaves_from_shared so they aren't
                # double-counted. Other leaves (any WEAP/ARMO that might
                # sit alongside) are left to the shared-pool path.
                if not _is_branch_mod_leaf(it):
                    continue
                scaled = dict(it)
                scaled["dropRate"] = it["dropRate"] * path_coeff
                per_region_raw[region_key].append(scaled)
    return per_region_raw


def filter_branch_mod_leaves_from_shared(shared_pools):
    """Strip per-region branch mod leaves from shared reward pools — both
    MISC mod boxes ('miscmod_mod*') and BOOK 'recipe_mod_*' plans are served
    per-region by build_per_region_mod_items, so leaving them in shared
    pools would cause Forest's items to be counted twice (once via the
    shared pool's resolve_deep walk of Forest's branch, once via the
    per-region collector). The per-region collector applies the empirically
    derived path coefficient that already sums both top_pool contributions,
    so it produces the correct per-dig rate for every region (including
    Forest)."""
    for pool in shared_pools:
        kept = [it for it in pool["items"] if not _is_branch_mod_leaf(it)]
        pool["items"] = kept
        pool["item_count"] = len(kept)
    return shared_pools


# ============================================================
# Per-Region Dig Rewards (categorised + caps)
# ============================================================
#
# Mirrors the JS `collectItemsForRegion` + `categorizeDigItems` logic but runs
# at build time so the JSON consumer just renders pre-bucketed lists.
#
# Categories (each bucket is a sub-expand on the Treasure Maps page):
#   recipes_plans     - general workshop / food / whole-weapon / whole-armour plans
#   weapon_mod_plans  - "Plan: <weapon> <mod>" BOOKs + loose weapon mod boxes
#   armour_mod_plans  - armour / PA / underarmour mod plans
#   region_bonus      - region-themed bonus loot (food, water, ammo, etc.)
#
# Plus the only other dig reward we have data for:
#   caps              - sourced from LL_Caps_TreasureMap (LLS_Caps_High, qty 3-7)
#
# NOTE on XP: there is NO treasure-map XP record in any current TSV export
# (QUEST, GMRW, GLOB, CURV, AVIF). If/when an XP source is found in the data
# (e.g. a curve referenced from a script, a GMRW entry tied to the mound
# activator, etc.) add it here. Until then, do not surface XP — no data, no
# claim.

# Region keyword -> our region key. Mirrors REGION_TOKENS in the JS so the
# Python categoriser sees the same regions the JS does.
_REGION_TOKENS = {
    "Forest":            "forest",
    "ForestFloodlands":  "forest",
    "ToxicValley":       "toxic_valley",
    "AshHeap":           "ash_heap",
    "Mountains":         "savage_divide",
    "CranberryBog":      "cranberry_bog",
    "Mire":              "the_mire",
    "SwampForest":       "the_mire",
    "BurningSprings":    "burning_springs",
    "SkylineValley":     "skyline_valley",
}
_DROP_REGIONS = ["forest", "toxic_valley", "ash_heap",
                 "cranberry_bog", "the_mire", "savage_divide"]
_REGION_LOC_RE = re.compile(r'Region(\w+?)Location')


def _regions_for_item(item):
    """Return the list of region keys an item is gated to via
    `Subject.GetInCurrentLocation(...Region<X>Location...)` conditions.
    Items with no region condition are treated as available in all regions."""
    hits = []
    for c in (item.get("conditions") or []):
        if "GetInCurrentLocation" not in c:
            continue
        m = _REGION_LOC_RE.search(c)
        if m and m.group(1) in _REGION_TOKENS:
            hits.append(_REGION_TOKENS[m.group(1)])
    return hits if hits else list(_DROP_REGIONS)


def _strip_region_conditions(conds):
    """Drop GetInCurrentLocation conditions — already implied by the region
    this list is rendered under, so they'd be noise in the output."""
    return [c for c in (conds or []) if "GetInCurrentLocation" not in c]


def build_dig_rewards(region_key, region, shared_pools):
    """Build the categorised dig-rewards payload for a single region.

    Returns a dict with caps metadata and four flat item lists:
    recipes_plans, weapon_mod_plans, armour_mod_plans, region_bonus."""
    by_id = {}
    caps_item = None

    def add(it):
        fid = it["form_id"]
        if fid not in by_id:
            by_id[fid] = {
                "name":          it["name"],
                "form_id":       fid,
                "edid":          it.get("edid", ""),
                "sig":           it.get("sig", ""),
                "qty":           it.get("qty", "1"),
                "drop_rate_raw": 0.0,
                "drop_rate":     "",
                "conditions":    _strip_region_conditions(it.get("conditions", [])),
                "tradeable":     it.get("tradeable", True) is not False,
            }
        by_id[fid]["drop_rate_raw"] += float(it.get("drop_rate_raw", 0) or 0)

    # 1) Items from shared reward pools that match this region. The "caps"
    #    pool is special-cased and lifted into its own field.
    for pool in shared_pools or []:
        if pool.get("pool_key") == "caps":
            if pool.get("items"):
                caps_item = pool["items"][0]
            continue
        for it in pool.get("items", []):
            if region_key in _regions_for_item(it):
                add(it)

    # 2) Region-specific reward items (region bonus loot + per-region mod boxes)
    for it in (region.get("region_reward_items") or []):
        add(it)

    # Cap rates at 1.0 and format as percent strings
    items = []
    for it in by_id.values():
        rate = min(it["drop_rate_raw"], 1.0)
        it["drop_rate_raw"] = round(rate, 6)
        it["drop_rate"]     = fmt_pct(rate * 100)
        items.append(it)

    # Categorise into the four buckets
    buckets = {
        "recipes_plans":    [],
        "weapon_mod_plans": [],
        "armour_mod_plans": [],
        "region_bonus":     [],
    }
    cat_to_bucket = {
        "recipe_plan":     "recipes_plans",
        "weapon_mod_plan": "weapon_mod_plans",
        "armour_mod_plan": "armour_mod_plans",
        "region_bonus":    "region_bonus",
    }
    for it in items:
        buckets[cat_to_bucket[categorize_dig_item(it)]].append(it)

    # Sort each bucket alphabetically by display name
    for k in buckets:
        buckets[k].sort(key=lambda x: (x.get("name") or "").lower())

    caps_payload = None
    if caps_item:
        caps_payload = {
            "name":          caps_item.get("name", "Caps"),
            "form_id":       caps_item.get("form_id", "0000000F"),
            "edid":          caps_item.get("edid", "Caps001"),
            "qty":           caps_item.get("qty", "3-7"),
            "drop_rate":     caps_item.get("drop_rate", "100%"),
            "drop_rate_raw": caps_item.get("drop_rate_raw", 1.0),
            "scales_with_buffs": False,
            "note":          "Does not scale with player level or other buffs",
        }

    return {
        "caps":             caps_payload,
        "recipes_plans":    buckets["recipes_plans"],
        "weapon_mod_plans": buckets["weapon_mod_plans"],
        "armour_mod_plans": buckets["armour_mod_plans"],
        "region_bonus":     buckets["region_bonus"],
    }


# ============================================================
# Build: Combined Region Output
# ============================================================

REGION_NAMES = OrderedDict([
    ("forest",          "Forest"),
    ("toxic_valley",    "Toxic Valley"),
    ("ash_heap",        "Ash Heap"),
    ("cranberry_bog",   "Cranberry Bog"),
    ("the_mire",        "The Mire"),
    ("savage_divide",   "Savage Divide"),
    ("burning_springs", "Burning Springs"),
    ("skyline_valley",  "Skyline Valley"),
])

REGION_LOCATION_URLS = {
    "forest":          "/df/treasure-maps/forest/",
    "toxic_valley":    "/df/treasure-maps/toxic-valley/",
    "ash_heap":        "/df/treasure-maps/ash-heap/",
    "cranberry_bog":   "/df/treasure-maps/cranberry-bog/",
    "the_mire":        "/df/treasure-maps/the-mire/",
    "savage_divide":   "/df/treasure-maps/savage-divide/",
    "burning_springs": "/df/treasure-maps/burning-springs/",
    "skyline_valley":  "/df/treasure-maps/skyline-valley/",
}


def build_regions(entries_idx, books, resolver, shared_pools):
    """Build the full region data: maps + region-specific rewards + mod boxes
    + categorised dig_rewards (caps / recipes / weapon mods / armour mods /
    region bonus)."""
    map_groups = build_map_names(entries_idx, books)
    region_rewards = build_region_rewards(resolver)
    per_region_mods = build_per_region_mod_items(resolver)

    regions = OrderedDict()
    for rkey in REGION_ORDER:
        maps = map_groups.get(rkey, [])
        rewards = region_rewards.get(rkey, [])
        # Append per-region mod-box items (aggregated + formatted) to this
        # region's reward items. Items without a region condition are added
        # so the JS picks them up under this region only.
        mod_raw = per_region_mods.get(rkey, [])
        if mod_raw:
            mod_agg = aggregate_items(mod_raw)
            rewards = list(rewards) + [format_item(it) for it in mod_agg]
        region_data = {
            "name": REGION_NAMES[rkey],
            "location_url": REGION_LOCATION_URLS.get(rkey, ""),
            "map_count": len(maps),
            "maps": maps,
            "region_reward_items": rewards,
        }
        # dig_rewards is the structured per-region payload the JS renders:
        # caps + categorised plan/recipe/mod buckets.
        region_data["dig_rewards"] = build_dig_rewards(rkey, region_data, shared_pools)
        regions[rkey] = region_data
    return regions


# ============================================================
# Build: U Mine It (uses manual resolution — not rng76)
# ============================================================

class LvliListIndex:
    def __init__(self, rows):
        self._d = {}
        for r in rows:
            fid = pick(r, "LVLI_FormID", default="").strip()
            if not fid:
                continue
            self._d[fid] = {
                "edid": pick(r, "LVLI_EDID", default=""),
                "flags": pick(r, "LVLF_Flags", default=""),
                "count": int(safe_float(pick(r, "EntryCount", "LLCT_Count", default="0"))),
                "maxValue": safe_float(pick(r, "LVMV_MaxValue", default="0")),
                "maxGlob": pick(r, "LVMG_MaxGlobal", default=""),
            }

    def get(self, fid):
        return self._d.get(fid.strip())

    def parse_flags(self, flags_str):
        f = flags_str or ""
        return {
            "useAll": len(f) > 2 and f[2] == '1',
            "firstMatch": len(f) > 6 and f[6] == '1',
        }

    def max_count(self, fid, globs):
        info = self.get(fid)
        if not info:
            return 0
        mg = info["maxGlob"]
        if mg:
            mg_fid = mg.split(":")[0] if ":" in mg else mg
            val = globs.fltv(mg_fid)
            if val is not None:
                return int(val)
        mv = info["maxValue"]
        if mv:
            return int(mv)
        return 0


class LvliEntriesIndex:
    def __init__(self, rows):
        self._d = defaultdict(list)
        for r in rows:
            pfid = pick(r, "LVLI_FormID", default="").strip()
            ref_raw = pick(r, "LVLO_Reference", default="")
            if not pfid or not ref_raw:
                continue
            ref_fid = ref_raw.split(":")[0] if ":" in ref_raw else ref_raw
            ref_edid = ref_raw.split(":")[1] if ref_raw.count(":") >= 1 else ""
            ref_sig = ref_raw.split(":")[2] if ref_raw.count(":") >= 2 else ""
            conds = []
            for ci in range(1, 11):
                c = pick(r, f"Cond{ci}", default="")
                if c:
                    conds.append(c)
            self._d[pfid].append({
                "index": int(safe_float(pick(r, "EntryIndex", default="0"))),
                "ref_formid": ref_fid, "ref_edid": ref_edid, "ref_sig": ref_sig,
                "chanceNoneValue": safe_float(pick(r, "LVOV_ChanceNoneValue", default="0")),
                "chanceNoneGlob": pick(r, "LVOG_ChanceNoneGlobal", default=""),
                "quantity": safe_float(pick(r, "LVIV_Quantity", default="1")),
                "condCount": int(safe_float(pick(r, "CondCount", default="0"))),
                "conditions": conds,
            })
        for fid in self._d:
            self._d[fid].sort(key=lambda e: e["index"])

    def entries(self, fid):
        return self._d.get(fid.strip(), [])


class GlobLookup:
    FALLBACKS = {"0043B770": 75.0, "0089EA90": 75.0}

    def __init__(self, rows):
        self._by_formid = {}
        fltv_key = None
        if rows:
            keys = list(rows[0].keys())
            for c in ("FLTV", "DATA"):
                if c in keys:
                    fltv_key = c
                    break
            if fltv_key is None and len(keys) >= 3:
                fltv_key = keys[2]
        for r in rows:
            fid = pick(r, "FormID", "GLOB_FormID", default="").strip()
            fltv = safe_float(r.get(fltv_key, "0") if fltv_key else "0", 0.0)
            edid = pick(r, "EDID", default="")
            if fid:
                self._by_formid[fid] = {"fltv": fltv, "edid": edid}

    def fltv(self, formid):
        formid = formid.strip()
        if formid in self._by_formid:
            return self._by_formid[formid]["fltv"]
        if formid in self.FALLBACKS:
            return self.FALLBACKS[formid]
        return None

    def edid(self, formid):
        formid = formid.strip()
        return self._by_formid.get(formid, {}).get("edid", "")


class BookLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid

    def edid(self, formid):
        e = self._by_formid.get(formid.strip())
        return e["edid"] if e else ""


class MiscLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid


class AlchLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid:
                self._by_formid[fid] = {
                    "full": pick(r, "FULL", default=""),
                    "edid": pick(r, "EDID", default=""),
                }

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]:
            return e["full"]
        if e and e["edid"]:
            return humanize_edid(e["edid"])
        return formid


def resolve_chance_none(entry, globs):
    cn_glob = entry.get("chanceNoneGlob", "")
    if cn_glob:
        glob_fid = cn_glob.split(":")[0] if ":" in cn_glob else cn_glob
        glob_edid = globs.edid(glob_fid)
        if "MinLvl" in glob_edid:
            return 0.0
        val = globs.fltv(glob_fid)
        if val is not None:
            return val
    cn_val = entry.get("chanceNoneValue", 0.0)
    if cn_val:
        return cn_val
    return 0.0


# ============================================================
# Build: U Mine It tiers (manual resolution for toggle conditions)
# ============================================================

def build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch):
    def resolve_name(entry):
        fid, sig, edid = entry["ref_formid"], entry["ref_sig"], entry["ref_edid"]
        if sig == "BOOK":
            return books.name(fid)
        if sig == "MISC":
            return misc.name(fid)
        if sig == "ALCH":
            return alch.name(fid)
        return humanize_edid(edid) if edid else fid

    tiers = OrderedDict()
    for tier_key, tier_info in UMINE_TIERS.items():
        fid = tier_info["formid"]
        lvli_info = list_idx.get(fid)
        entries = entries_idx.entries(fid)
        max_c = list_idx.max_count(fid, globs) if lvli_info else 0

        rewards = []
        for e in entries:
            cn = resolve_chance_none(e, globs)
            rate = 1.0 - (cn / 100.0)

            toggle_name, toggle_fid = get_toggle_glob_info(e)
            conditions, notes = [], []

            if has_toggle_condition(e) and toggle_name:
                conditions.append(f"Requires {humanize_edid(toggle_name)} = ON")
                if toggle_fid:
                    notes.append(f"Toggle GLOB: {toggle_name} [{toggle_fid}]")
            if has_learned_recipe_condition(e):
                conditions.append("Won't drop if you've already learned this recipe")
            qc = get_quest_condition(e)
            if qc:
                conditions.append(qc)
            if cn > 0:
                notes.append(f"ChanceNone: {cn}")
                cn_glob_str = e.get("chanceNoneGlob", "")
                if cn_glob_str:
                    gfid = cn_glob_str.split(":")[0]
                    gedid = cn_glob_str.split(":")[1] if ":" in cn_glob_str else ""
                    notes.append(f"ChanceNone via GLOB {gedid} [{gfid}]")

            tradeable = True
            if "PlayerTitle" in e["ref_edid"] or "CampTitle" in e["ref_edid"]:
                tradeable = False

            rewards.append({
                "name": resolve_name(e),
                "edid": e["ref_edid"],
                "form_id": e["ref_formid"],
                "sig": e["ref_sig"],
                "quantity": int(e["quantity"]),
                "drop_rate": fmt_pct(rate * 100),
                "drop_rate_raw": round(rate, 6),
                "tradeable": tradeable,
                "conditions": conditions,
                "notes": notes,
            })

        # Inject Waste Acid into the tier rewards so it appears in the
        # per-tier Junk & Scrap sub-expand instead of a separate shared pool.
        # LL_Scrap_Acid (007AC791) is a pick-one of 3 qty variants (×1/×2/×3)
        # at 33.333% each. The JS UMINE_EXPAND table breaks it into 3 rows.
        rewards.append({
            "name": "Waste Acid",
            "edid": "c_Acid_scrap",
            "form_id": "001BF72D",
            "sig": "MISC",
            "quantity": 1,
            "drop_rate": "100%",
            "drop_rate_raw": 1.0,
            "tradeable": True,
            "conditions": [],
            "notes": [
                "Source: LL_Scrap_Acid [007AC791] — shared quest reward",
                "Pick-one of 3 qty variants (×1/×2/×3 at 33.33% each)",
            ],
        })

        tiers[tier_key] = {
            "name": tier_info["name"],
            "edid": tier_info["edid"],
            "form_id": fid,
            "list_type": "UseAll Independent (max_count=0)",
            "entry_count": len(entries) + 1,  # +1 for injected Waste Acid
            "rewards": rewards,
            "notes": [
                f"UseAll list, max_count={max_c} -> Independent",
                "Each entry fires independently at its own rate",
                "rate = 1 - (ChanceNone / 100)",
                "Waste Acid injected from shared LL_Scrap_Acid pool",
            ],
        }
    return tiers


# ============================================================
# Build: U Mine It shared quest reward pools (Aid only)
# ============================================================
#
# These pools fire alongside the tier-specific mining LVLI on every Lucky
# Strike quest completion (regardless of tier). Each is resolved deep via
# rng76 so the JSON carries flattened leaf items the website can render
# directly under each region sub-expand without any additional lookups.
#
# Aid: parent LVLI 0043934D (QuestReward_LLS_Aid_All) is a pick-one across
# 6 regional sub-LVLIs gated by LVLV_MinimumLevel (Forest 1, Toxic Valley
# 10, Savage Divide 15, Ash Heap 25, Mire 30, Cranberry Bog 35). The list
# has the LVLF "Calculate from all levels" flag set, so at any player
# level it picks uniformly from every unlocked regional pool. We surface
# each tier as its own sub-expand labelled by player-level range, since
# regions are an implementation detail and confuse readers expecting a
# "your region" gate.
#
# MISC items inside the regional Aid LVLIs (Waste Acid etc.) are scrap
# components — they get hoisted out of the Aid pool into a separate
# Junk & Scrap shared pool so the Aid expand only carries true ALCH
# consumables.
#
# Acid: pick-one of 3 quantity variants on LL_Scrap_Acid. resolve_deep
# handles the qty variants automatically.
def build_u_mine_it_shared_pools(resolver):
    """Resolve the shared Aid + Acid + Junk pools fired on every Lucky Strike completion."""

    # ── Aid: tier-labelled sub-pools (one per region) ────────────────────
    # Build the level-range label from each region's min_lvl + the next
    # region's min_lvl - 1. Last region gets "X+" since there's no upper bound.
    # Items are split: ALCH stays in Aid, MISC is collected for the Junk pool.
    region_keys = list(UMINE_AID_REGIONS.keys())
    aid_regions = []
    junk_items_raw = []  # accumulator for MISC items pulled out of regional Aid LVLIs

    for i, rkey in enumerate(region_keys):
        rinfo = UMINE_AID_REGIONS[rkey]
        min_lvl = rinfo["min_lvl"]
        # Tier label: "Player Level 25 to 29" or "Player Level 35+" for the last tier
        if i + 1 < len(region_keys):
            next_min = UMINE_AID_REGIONS[region_keys[i + 1]]["min_lvl"]
            tier_label = f"Player Level {min_lvl} to {next_min - 1}"
        else:
            tier_label = f"Player Level {min_lvl}+"

        raw = resolver.resolve_deep(rinfo["formid"])
        # Split ALCH (Aid) vs MISC (Junk & Scrap). Anything else stays in Aid
        # by default — no surprises if a new sig appears.
        alch_raw = []
        for it in raw:
            sig = (it.get("sig") or "").upper()
            if sig == "MISC":
                junk_items_raw.append(it)
            else:
                alch_raw.append(it)

        agg = aggregate_items(alch_raw)
        aid_regions.append({
            "region_key": rkey,
            "name": rinfo["name"],         # kept for legacy/debug, not displayed
            "tier_label": tier_label,       # e.g. "Player Level 25 to 29"
            "edid": rinfo["edid"],
            "form_id": rinfo["formid"],
            "min_lvl": min_lvl,
            "tier_drop_rate": "100%",       # one tier always rolls per quest
            "tier_drop_rate_raw": 1.0,
            "item_count": len(agg),
            "items": [format_item(it) for it in agg],
        })

    aid_pool = {
        "key": "aid",
        "name": "Aid Items",
        "form_id": UMINE_AID_PARENT_FORMID,
        "edid": "QuestReward_LLS_Aid_All",
        "list_type": "Pick-one of 6 regional LVLIs (MinLvl-gated)",
        "drop_rate": "100%",
        "drop_rate_raw": 1.0,
        "blurb": f"Regional loot pool · {len(region_keys)} player level tiers · one tier always rolls",
        "regions": aid_regions,
        "notes": [
            "Parent LVLI 0043934D is a pick-one across 6 LL_Aid_<Region> sub-pools",
            "LVLF flag 0x01 (Level Filter) set → picks uniformly from every unlocked tier",
            "Each tier is shown as 100% because exactly one tier is always rolled",
            "MISC items (Waste Acid etc.) are injected into per-tier Junk & Scrap",
        ],
    }

    # ── Waste Acid (LL_Scrap_Acid) is now injected directly into each
    # tier's rewards in build_u_mine_it() so it appears inside the per-tier
    # Junk & Scrap sub-expand. No separate shared pool needed.
    # MISC items from regional Aid LVLIs (junk_items_raw) are also dropped
    # here — if any appear in future xEdit exports they should be handled
    # similarly (injected into tier rewards).

    return [aid_pool]


# ============================================================
# Build: Lucky Maps (unchanged)
# ============================================================

def build_lucky_maps(entries_idx, books):
    vendor_fid = "0086A8AF"
    vendor_entries = entries_idx.entries(vendor_fid)
    count = len(vendor_entries)
    items = []
    for e in vendor_entries:
        fid = e["ref_formid"]
        qc = get_quest_condition(e)
        items.append({
            "name": books.name(fid),
            "edid": books.edid(fid),
            "form_id": fid,
            "drop_rate": fmt_pct((1.0 / count) * 100) if count > 0 else "0%",
            "conditions": [qc] if qc else [],
        })
    drop_sources = [
        {"source": "Vendor (Purveyor Murmrgh)", "list": "MTRZ05_Vendor_LuckyMaps",
         "notes": "Pick-one from 3 maps, requires Lucky Strike quest not active"},
        {"source": "Activity Rewards", "list": "RA_LL_Rewards_Activities_UMineItMaps",
         "notes": "ChanceNone via GLOB LTT_RA_Rewards_Activities_UMineItMap_DropRate (runtime toggle)"},
        {"source": "Public Event Rewards", "list": "RA_LLS_Loot_UmineItMaps",
         "notes": "Requires Lucky Strike quest not active"},
        {"source": "Safe Containers", "list": "Various LLE_Safe_* lists",
         "notes": "Excavator Map only; ChanceNone 70-95 depending on safe difficulty"},
        {"source": "Motherlode Container (Rare)", "list": "MTR05_LLI_MotherlodeContainer_Rare",
         "notes": "Excavator Map; ChanceNone 70"},
    ]
    return {
        "vendor_list": "MTRZ05_Vendor_LuckyMaps",
        "vendor_formid": vendor_fid,
        "items": items,
        "drop_sources": drop_sources,
        "notes": [
            "Lucky Maps start the U Mine It questline (Lucky Strike)",
            "Three tiers: Miner Map -> Prospector Map -> Excavator Map",
            "Each tier leads to progressively better mining rewards",
        ],
    }


def build_teammate_reward(entries_idx):
    fid = "001A721E"
    entries = entries_idx.entries(fid)
    count = len(entries)
    items = []
    for e in entries:
        items.append({
            "name": humanize_edid(e["ref_edid"]),
            "edid": e["ref_edid"],
            "form_id": e["ref_formid"],
            "sig": e["ref_sig"],
            "drop_rate": fmt_pct((1.0 / count) * 100) if count > 0 else "0%",
        })
    return {
        "list_edid": "LL_TreasureMap_TeammateReward",
        "list_formid": fid,
        "list_type": "Pick-one",
        "items": items,
        "notes": [
            "Teammates get a random reward when a team member digs a treasure mound",
            f"Pick-one from {count} entries (equal chance each)",
        ],
    }


def _phantom_pretty_name(item, books):
    """Prefer the BOOK FULL name for plan/recipe leaves so they read as
    'Plan: …' rather than a humanised EDID."""
    if (item.get("sig") or "").upper() == "BOOK":
        nm = books.name(item.get("form_id", ""))
        if nm and nm != item.get("form_id", ""):
            return nm
    return item.get("name", "")


def _phantom_items(resolver, books, formid):
    """Resolve one LVLI to flattened, aggregated, formatted leaf items with the
    BOOK-preferred display names.

    Uses aggregate_items_split_qty so same-item / different-quantity pick-one
    lists (Skulls, Throwing Knives) render one row per quantity rather than a
    single merged "×1-5" row."""
    items = [format_item(it) for it in aggregate_items_split_qty(resolver.resolve_deep(formid))]
    for it in items:
        it["name"] = _phantom_pretty_name(it, books)
    return items


def _phantom_internal_blurb(items):
    """Style-guide blurb describing how a resolved pool rolls internally."""
    n = len(items)
    noun = "item" if n == 1 else "items"
    total = sum(it["drop_rate_raw"] for it in items)
    if n == 1:
        return ("Guaranteed drop · 1 item" if total >= 0.99
                else "Chance drop · 1 item")
    if total > 1.05:
        return f"Each item rolls independently · {n} {noun}"
    if total >= 0.99:
        return f"Guaranteed drop of one item · {n} {noun}"
    return f"Chance drop of one item · {n} {noun}"


def _phantom_fire_rate(entry):
    """Independent-entry fire chance for a RepeatableRewards entry:
    (1 - ChanceNone) × conditionChance (from any GetRandomPercent gate).
    The list is UseAll/max_count=0 so each entry rolls on its own each dig."""
    try:
        cn = float(entry.get("LVOV_ChanceNoneValue") or 0.0)
    except (TypeError, ValueError):
        cn = 0.0
    cond_chance = 1.0
    for i in range(1, 11):
        c = entry.get(f"Cond{i}") or ""
        if "GetRandomPercent" in c:
            m = re.search(r"([\d.]+)\s*$", c.strip())
            if m:
                cond_chance = float(m.group(1)) / 100.0
    return max(0.0, min(1.0, (1.0 - cn / 100.0) * cond_chance))


def _fire_pct(rate):
    """Pretty percent for a pool-level fire rate (e.g. 0.15 → '15%')."""
    pct = rate * 100.0
    if abs(pct - round(pct)) < 1e-6:
        return f"{int(round(pct))}%"
    return f"{pct:.2f}".rstrip("0").rstrip(".") + "%"


def _phantom_repeatable_pools(resolver, books):
    """Build the ABC-ordered Repeatable-Rewards sub-pools from the UseAll list.

    Each entry becomes a pool carrying its own per-dig fire rate. The
    Progression-Items entry is expanded into per-region sub-pools."""
    lvli = resolver.lvli
    pools = []
    for e in lvli.entries_by_list.get(PHANTOM_REPEATABLE_FORMID, []):
        ref = e.get("LVLO_Reference", "")
        parts = ref.split(":")
        fid = parts[0] if parts else ref
        sig = parts[2].upper() if len(parts) >= 3 else ""
        fire = _phantom_fire_rate(e)
        name = PHANTOM_POOL_NAMES.get(fid) or books.name(fid) or fid

        pool = {
            "name": name,
            "form_id": fid,
            "fire_rate": round(fire, 6),
            "fire_rate_pct": _fire_pct(fire),
        }

        if fid == PHANTOM_PROGRESSION_FORMID:
            # Regional loot pool — the game rolls the region you dig in.
            regions = []
            for rname, rfid in PHANTOM_REGION_GRABBAGS:
                ritems = _phantom_items(resolver, books, rfid)
                regions.append({"region": rname,
                                "item_count": len(ritems),
                                "items": ritems})
            pool["regional"] = True
            pool["regions"] = regions
            pool["item_count"] = sum(r["item_count"] for r in regions)
            pool["blurb"] = ("Regional loot pool — rewards depend on which region "
                             "you dig in · " + str(len(regions)) + " regions")
        elif sig == "BOOK":
            # Single recipe/plan (e.g. shovel paint) — guaranteed once the gate
            # passes, so its internal rate is 100%.
            pool["items"] = [{
                "name": name, "form_id": fid, "edid": e.get("LVLI_EDID", ""),
                "sig": "BOOK", "drop_rate": "100%", "drop_rate_raw": 1.0,
                "qty": "1", "conditions": [], "tradeable": False,
            }]
            pool["item_count"] = 1
            pool["blurb"] = _phantom_internal_blurb(pool["items"])
        else:
            items = _phantom_items(resolver, books, fid)
            # Attach OBTS mod-slot data for unique weapon/armour sub-LVLIs.
            # Map leaf form_id -> variant sub-LVLI EDID by scanning this pool's
            # direct entries: any direct entry that is a unique LVLI gets resolved
            # one level so its leaf item form_ids inherit that variant edid.
            variant_edid_map = {}
            for de in resolver.lvli.entries_by_list.get(fid, []):
                dref = (de.get("LVLO_Reference") or "").strip()
                dparts = dref.split(":")
                sub_fid = dparts[0] if dparts else dref
                sub_edid = dparts[1] if len(dparts) >= 2 else ""
                sub_sig = dparts[2].upper() if len(dparts) >= 3 else ""
                if sub_sig != "LVLI" or not _is_unique_lvli(sub_edid):
                    continue
                for leaf in _phantom_items(resolver, books, sub_fid):
                    variant_edid_map[(leaf.get("form_id") or "").upper()] = sub_edid
            if variant_edid_map:
                for it in items:
                    vedid = variant_edid_map.get((it.get("form_id") or "").upper())
                    if vedid:
                        _attach_modslots(it, vedid)
            pool["items"] = items
            pool["item_count"] = len(items)
            pool["blurb"] = _phantom_internal_blurb(items)

        pools.append(pool)

    pools.sort(key=lambda p: p["name"].lower())
    return pools


def _loc_to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _load_location_sites(filename):
    """Load an author-maintained site-locations TSV and return a region-grouped
    list for the front-end location renderer (grave sites / dig sites pages).

    Columns: region, site_number, ref_edid, ref_formid, closest_fast_travel,
    directions, photo_approach, photo_spawn, x, y, z. Region + closest_fast_travel
    are PRECOMPUTED (the CI build server has no Mappalachia DB, so they can't be
    derived here); directions + photo URLs are author-supplied and survive
    rebuilds because they live in this committed TSV, not in datamined output.
    Regions are ABC-ordered; sites within a region sort by number (blank last).
    Returns [] when the file is absent so the block simply omits.

    Channel-scoped: reads tsv/pts/ on a PTS build, tsv/ on live, falling back to
    live when PTS has no copy. Live pages must never render PTS placements.
    """
    path = Path(tsv_source.derived_read(filename, tsv_source.channel_of()))
    if not path.exists():
        return []
    groups = OrderedDict()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            region = (row.get("region") or "").strip()
            if not region:
                continue
            num_raw = (row.get("site_number") or "").strip()
            try:
                num = int(num_raw)
            except ValueError:
                num = None
            groups.setdefault(region, []).append({
                "number": num,
                "closest_fast_travel": (row.get("closest_fast_travel") or "").strip(),
                "directions": (row.get("directions") or "").strip(),
                "photo_approach": (row.get("photo_approach") or "").strip(),
                "photo_spawn": (row.get("photo_spawn") or "").strip(),
                "ref_edid": (row.get("ref_edid") or "").strip(),
                "coords": {
                    "x": _loc_to_float(row.get("x")),
                    "y": _loc_to_float(row.get("y")),
                    "z": _loc_to_float(row.get("z")),
                },
            })
    out = []
    for region in sorted(groups.keys()):
        sites = sorted(groups[region], key=lambda s: (s["number"] is None, s["number"] or 0))
        out.append({"region": region, "sites": sites})
    return out


def build_pint_sized_phantoms(resolver, books):
    """Build the Pint-Sized Phantoms mini-quest block.

    Returns None when the Slasher map BOOK isn't in the current TSV export, so
    the key is simply omitted on channels that predate the seasonal content.
    """
    map_name = books.name(PHANTOM_MAP_FORMID)
    if not map_name or map_name == PHANTOM_MAP_FORMID:
        print("    (Slasher map BOOK not in this export — skipping phantom block)")
        return None

    # ── Experience & Caps (from the repeatable quest-reward GMRW) ──
    xp50 = resolver.curvs.interpolate(PHANTOM_XP_CURVE_FORMID, 50) or 0.0
    experience = {
        "curve_formid": PHANTOM_XP_CURVE_FORMID,
        "tier": "Tier16",
        "xp_level_50": int(round(xp50)),
        "scales_with_level": True,
    }
    caps_amt = resolver.globs.value(PHANTOM_CAPS_GLOB) or 0.0
    caps = {"amount": int(round(caps_amt)), "guaranteed": True,
            "glob_formid": PHANTOM_CAPS_GLOB}

    # ── Repeatable Rewards (UseAll — each pool rolls per dig) ──
    repeatable_pools = _phantom_repeatable_pools(resolver, books)

    # ── Quest Related (journal pages) — active-quest keyword trigger, then a
    # first-page / later-pages branch based on how many you've collected. ──
    branches = []
    for bfid, bname, bcond in PHANTOM_JOURNAL_BRANCHES:
        bitems = _phantom_items(resolver, books, bfid)
        branches.append({"name": bname, "condition": bcond,
                         "item_count": len(bitems), "items": bitems})

    return {
        "map": {
            "name": map_name,
            "edid": PHANTOM_MAP_EDID,
            "form_id": PHANTOM_MAP_FORMID,
        },
        "quest": {
            "name": PHANTOM_QUEST_NAME,
            "edid": PHANTOM_QUEST_EDID,
            "repeatable_quest": PHANTOM_REPEATABLE_QUEST,
        },
        "experience": experience,
        "caps": caps,
        "repeatable_rewards": {
            "name": "Repeatable Rewards",
            "list_edid": "SDOW_LL_SQ01_RepeatableRewards",
            "list_formid": PHANTOM_REPEATABLE_FORMID,
            "blurb": str(len(repeatable_pools)) + " reward lists · each list rolled when you dig a grave",
            "pools": repeatable_pools,
        },
        "quest_related": {
            "name": "Quest Related",
            "list_formid": PHANTOM_JOURNAL_FORMID,
            "trigger": "Only rolls while the “Secrets to the Grave” quest is active",
            "warning": "Journal pages advance Secrets to the Grave and stop dropping once you have collected them all",
            "branches": branches,
        },
        "map_sources": PHANTOM_MAP_SOURCES,
        "grave_sites": _load_location_sites("phantom_grave_sites.tsv"),
        "notes": [
            "The Pint-Sized Phantoms' Map is a mini-quest treasure line for the Slasher seasonal event",
            "Use the map to find and dig up the grave sites the Pint-Sized Phantoms disturbed",
            "Grave-dig rewards are repeatable · XP scales with level, Caps are flat",
            "GMRW conditions NOT baked in — handled by website JS",
        ],
    }


def main():
    print("[build_treasure_maps_json.py] Starting build...")
    lvli_list_path = newest("LVLI_Export_*_LVLI_List.tsv")
    lvli_entries_path = newest("LVLI_Export_*_LVLI_Entries.tsv")
    book_path = newest("BOOK_Export_*.tsv", exclude_substrings=["Locations"])
    glob_path = newest("GLOB_Export_*.tsv")
    source_files = [os.path.basename(p) for p in [lvli_list_path, lvli_entries_path, book_path, glob_path]]
    for label, p in [("LVLI List", lvli_list_path), ("LVLI Entries", lvli_entries_path),
                     ("BOOK", book_path), ("GLOB", glob_path)]:
        print(f"  {label}: {os.path.basename(p)}")
    list_idx = LvliListIndex(read_tsv(lvli_list_path))
    entries_idx = LvliEntriesIndex(read_tsv(lvli_entries_path))
    globs = GlobLookup(read_tsv(glob_path))
    books = BookLookup(read_tsv(book_path))
    misc, alch = MiscLookup([]), AlchLookup([])
    try:
        misc_path = newest("MISC_Export_*.tsv")
        misc = MiscLookup(read_tsv(misc_path))
        source_files.append(os.path.basename(misc_path))
    except FileNotFoundError:
        pass
    try:
        alch_path = newest("ALCH_Export_*.tsv")
        alch = AlchLookup(read_tsv(alch_path))
        source_files.append(os.path.basename(alch_path))
    except FileNotFoundError:
        pass

    print("  Loading rng76 engine...")
    rng_data = Rng76Data.from_tsv_root(str(TSV_DIR))
    resolver = rng_data.resolver

    # Shared pools are built first because build_regions now needs them
    # (per-region dig_rewards walk shared pools to attach the matching items).
    print("  Building shared reward pools...")
    shared_pools = build_shared_rewards(resolver)
    # Mod-box items AND BOOK recipe_mod_* plans are served per-region (see
    # build_per_region_mod_items); remove them from shared pools to avoid
    # double-counting Forest's leaves on the website (the shared-pool
    # resolve_deep only walks Forest's FirstMatch branch).
    shared_pools = filter_branch_mod_leaves_from_shared(shared_pools)
    print("  Building regions (deep LVLI resolution)...")
    regions = build_regions(entries_idx, books, resolver, shared_pools)
    print("  Building U Mine It...")
    tiers = build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch)
    print("  Building U Mine It shared pools (Aid + Acid)...")
    umine_shared = build_u_mine_it_shared_pools(resolver)
    print("  Building Lucky Maps...")
    lucky = build_lucky_maps(entries_idx, books)
    print("  Building teammate reward...")
    teammate = build_teammate_reward(entries_idx)
    print("  Building Pint-Sized Phantoms (Slasher grave sites)...")
    phantoms = build_pint_sized_phantoms(resolver, books)

    total_maps = sum(r["map_count"] for r in regions.values())
    total_shared = sum(p["item_count"] for p in shared_pools)
    total_region_items = sum(len(r["region_reward_items"]) for r in regions.values())

    output = {
        "shared_reward_pools": shared_pools,
        "regions": regions,
        "teammate_reward": teammate,
        # Dig-site locations for the /treasure-maps/locations/ page (df-bnb-treasure-maps.js
        # buildLocationGroups). Author-maintained tsv/treasure_map_dig_sites.tsv, generated
        # from the ACTI2 mound export by build_treasure_map_dig_sites_tsv.py; directions/photos
        # are hand-filled there and survive rebuilds. Empty [] until that TSV is committed.
        "treasure_map_locations": _load_location_sites("treasure_map_dig_sites.tsv"),
        "u_mine_it": {"tiers": tiers, "shared_pools": umine_shared, "lucky_maps": lucky},
        "meta": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "generator": "build_treasure_maps_json.py",
            "source_files": source_files,
            "notes": [
                "Drop rates resolved via rng76 engine (deep LVLI flattening)",
                "Shared reward pools apply to ALL treasure map digs",
                "Region-specific rewards vary by dig location",
                "Mod boxes (miscmod_mod*) are computed per region, not via shared pools",
                "GMRW conditions NOT baked in - handled by website JS",
                "Burning Springs and Skyline Valley are empty placeholders",
                "Per-region dig_rewards: caps + categorised plan/mod buckets",
                "Caps sourced from LL_Caps_TreasureMap (LLS_Caps_High, qty 3-7, 100%)",
                "No XP field — there is no treasure-map XP record in any current TSV export",
            ],
        },
    }

    # Slasher seasonal mini-quest — only present once the SDOW content is in
    # the export (populates on the PTS channel, inert on live until it ships).
    if phantoms:
        output["pint_sized_phantoms"] = phantoms

    os.makedirs(str(DIST_DIR), exist_ok=True)
    out_path = DIST_DIR / "treasure_maps.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n  Output: {out_path}")
    print(f"  Regions: {len(regions)}, Maps: {total_maps}")
    print(f"  Shared pools: {len(shared_pools)}, Shared items: {total_shared}")
    print(f"  Region-specific items: {total_region_items}")
    aid_tier_count = sum(len(p.get("regions", [])) for p in umine_shared if p["key"] == "aid")
    aid_item_count = sum(
        sum(r["item_count"] for r in p["regions"])
        for p in umine_shared if p["key"] == "aid"
    )
    print(f"  U Mine It tiers: {len(tiers)}, Lucky Maps: {len(lucky['items'])}")
    print(f"  U Mine It shared pools: Aid ({aid_tier_count} tiers, {aid_item_count} items)")
    print(f"  Waste Acid injected into each tier's Junk & Scrap sub-expand")
    if phantoms:
        _pools = phantoms["repeatable_rewards"]["pools"]
        _pp = sum(p.get("item_count", 0) for p in _pools)
        print(f"  Pint-Sized Phantoms: XP L50 {phantoms['experience']['xp_level_50']}, "
              f"Caps {phantoms['caps']['amount']}, {len(_pools)} repeatable pools ({_pp} items), "
              f"{len(phantoms['quest_related']['branches'])} journal branches")
    print("[build_treasure_maps_json.py] Done.")

    patchlog_dir = DIST_DIR / "patchlogs"
    os.makedirs(str(patchlog_dir), exist_ok=True)
    write_empty_patchlog_feed(str(DIST_DIR), "patchlog_latest_df_treasure_maps.json", current_count=total_maps)


if __name__ == "__main__":
    main()
