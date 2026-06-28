#!/usr/bin/env python3
"""
build_healing_calculator_json.py
================================
Extracts ALL healing-related data from Fallout 76 TSV exports and outputs
dist/calculators/healing_calculator.json for the Healing Calculator page.

Reads from:
  - GLOB_Export_*.tsv   (food/water heal magnitudes, durations, heal-rate bonuses)
  - SPEL_Export_*_EFFECTS.tsv  (spell magnitudes for perks, mutations, legendaries)
  - PERK_Export_*.tsv   (perk card metadata -- names, descriptions, ranks)
  - ALCH_Export_*.tsv   (consumables -- Stimpaks, Blood Packs, etc.)
  - ENCH_Export_*.tsv   (legendary enchantments -- Vampire's, Solar Armor, etc.)
  - ACTI_Export_*_ACTI.tsv  (activators -- CAMP healing items with VMAD spell refs)
  - FURN_Export_*_FURN.tsv  (furniture -- Sympto-Matic, etc.)

No external dependencies -- runs on stdlib only.

Usage (local):   python src/build_healing_calculator_json.py
Usage (CI):      python src/build_healing_calculator_json.py
"""

import csv
import glob as globmod
import json
import os
import re
import sys
from datetime import date

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV_DIR = os.path.join(ROOT, "tsv")
OUT_DIR = os.path.join(ROOT, "dist", "calculators")
OUT_PATH = os.path.join(OUT_DIR, "healing_calculator.json")

# ---------------------------------------------------------------------------
# Month ordering for sorting TSV filenames by embedded month/year
# ---------------------------------------------------------------------------
_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def _filename_date_key(path):
    """Extract (year, month_number) from filenames like GLOB_Export_March_2026.tsv."""
    base = os.path.basename(path).lower()
    m = re.search(r'_([a-z]+)_(\d{4})', base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)


def find_newest_tsv(pattern):
    """Find the newest TSV file matching a glob pattern under TSV_DIR."""
    files = globmod.glob(os.path.join(TSV_DIR, pattern))
    if not files:
        return None
    # Sort by embedded date first, then by mtime as tiebreaker
    return max(files, key=lambda p: (_filename_date_key(p), os.path.getmtime(p)))


# ---------------------------------------------------------------------------
# TSV reading helpers
# ---------------------------------------------------------------------------
def read_tsv(filepath):
    """Read a TSV file with encoding fallback. Returns list of rows (list of strings)."""
    if not filepath or not os.path.exists(filepath):
        return []
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                lines = f.read().strip().split("\n")
            return [line.split("\t") for line in lines]
        except (UnicodeDecodeError, IOError):
            continue
    print(f"  [WARNING] Could not read {filepath} with any encoding", file=sys.stderr)
    return []


def read_tsv_dicts(filepath):
    """Read a TSV file into a list of dicts keyed by header row."""
    if not filepath or not os.path.exists(filepath):
        return []
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as fh:
                return list(csv.DictReader(fh, delimiter="\t"))
        except (UnicodeDecodeError, IOError):
            continue
    print(f"  [WARNING] Could not read {filepath} with any encoding", file=sys.stderr)
    return []


def safe_float(val, default=0.0):
    """Parse a float, stripping quotes and whitespace. Returns default on failure."""
    if val is None:
        return default
    try:
        return float(str(val).strip().strip('"'))
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    """Parse an int, stripping quotes and whitespace. Returns default on failure."""
    if val is None:
        return default
    try:
        return int(float(str(val).strip().strip('"')))
    except (ValueError, TypeError):
        return default


def clean_str(val):
    """Strip quotes and whitespace from a string value."""
    if val is None:
        return ""
    return str(val).strip().strip('"')


# ---------------------------------------------------------------------------
# 1. GLOB extraction
# ---------------------------------------------------------------------------
def extract_globs(tsv_path):
    """Extract healing-related GLOBs: food magnitudes, durations, heal rate bonuses, etc."""
    print(f"  Reading GLOBs from: {os.path.basename(tsv_path or 'NONE')}")
    rows = read_tsv(tsv_path)
    if not rows:
        print("  [WARNING] No GLOB data found -- using defaults", file=sys.stderr)
        return _default_globs()

    # GLOB columns: FormID, EDID, FLTV, ReferencedByCount, Ref1...
    # Header row tells us column positions
    header = rows[0] if rows else []
    edid_col = 1   # EDID is always column index 1
    fltv_col = 2   # FLTV (float value) is always column index 2

    # Build lookup: EDID -> float value
    glob_map = {}
    for row in rows[1:]:
        if len(row) > fltv_col:
            edid = row[edid_col].strip()
            val = safe_float(row[fltv_col])
            if edid:
                glob_map[edid] = val

    # Extract food heal magnitudes (standard)
    food_mag = {}
    food_mag_map = {
        "Tiny":      "SURV_Food_Heal_Mag_1_Tiny",
        "Small":     "SURV_Food_Heal_Mag_2_Small",
        "Medium":    "SURV_Food_Heal_Mag_3_Medium",
        "Large":     "SURV_Food_Heal_Mag_4_Large",
        "VeryLarge": "SURV_Food_Heal_Mag_5_VeryLarge",
        "Huge":      "SURV_Food_Heal_Mag_5_Huge",
        "Gourmet":   "SURV_Food_Heal_Mag_5_Gourmet",
    }
    for tier, edid in food_mag_map.items():
        food_mag[tier] = glob_map.get(edid, 0.0)
    print(f"    Food magnitudes: {food_mag}")

    # Extract food heal magnitudes (improved -- Herbivore/Carnivore)
    food_mag_improved = {}
    food_mag_improved_map = {
        "Tiny":      "SURV_Food_Heal_Mag_1_Tiny_Improved",
        "Small":     "SURV_Food_Heal_Mag_2_Small_Improved",
        "Medium":    "SURV_Food_Heal_Mag_3_Medium_Improved",
        "Large":     "SURV_Food_Heal_Mag_4_Large_Improved",
        "VeryLarge": "SURV_Food_Heal_Mag_5_VeryLarge_Improved",
        "Huge":      "SURV_Food_Heal_Mag_5_HugeImproved",
    }
    for tier, edid in food_mag_improved_map.items():
        food_mag_improved[tier] = glob_map.get(edid, 0.0)
    print(f"    Food magnitudes (improved): {food_mag_improved}")

    # Extract food heal durations
    food_dur = {
        "standard": glob_map.get("SURV_Food_Heal_Dur_Standard", 25.0),
        "water":    glob_map.get("SURV_Food_Heal_Dur_Water", 10.0),
    }
    print(f"    Food durations: {food_dur}")

    # Extract water/nuka heal magnitudes
    water_mag = {
        "Water":         glob_map.get("SURV_Food_Heal_Mag_Water", 1.0),
        "Purified":      glob_map.get("SURV_Food_Heal_Mag_WaterPurified", 2.5),
        "NukaSmall":     glob_map.get("SURV_Food_Heal_Mag_6_NukaCola_Small", 4.0),
        "NukaMedium":    glob_map.get("SURV_Food_Heal_Mag_6_NukaCola_Medium", 10.0),
        "NukaLarge":     glob_map.get("SURV_Food_Heal_Mag_6_NukaCola_Large", 15.0),
        "NukaVeryLarge": glob_map.get("SURV_Food_Heal_Mag_6_NukaCola_VeryLarge", 20.0),
    }
    print(f"    Water magnitudes: {water_mag}")

    # Extract food heal-rate bonuses (FortifyHealRate from food)
    food_healrate = {
        "Low":    glob_map.get("SURV_Food_Effect_HealthRegen_Mag_1_Low", 0.075),
        "Medium": glob_map.get("SURV_Food_Effect_HealthRegen_Mag_2_Medium", 0.125),
        "High":   glob_map.get("SURV_Food_Effect_HealthRegen_Mag_3_High", 0.250),
    }
    print(f"    Food heal-rate bonuses: {food_healrate}")

    # Extract food max-HP bonuses
    food_maxhp = {
        "Low":    glob_map.get("SURV_Food_Effect_HealthMax_Mag_1_Low", 15.0),
        "Medium": glob_map.get("SURV_Food_Effect_HealthMax_Mag_2_Medium", 30.0),
        "High":   glob_map.get("SURV_Food_Effect_HealthMax_Mag_3_High", 40.0),
    }
    print(f"    Food max-HP bonuses: {food_maxhp}")

    # Misc healing-related GLOBs
    misc = {}
    misc_keys = [
        "LGND_ExecuteHealthThreshold",
        "ATX_BloodTransfusionPump_MagnitudeGhoulGiveHealth",
        "ATX_BloodTransfusionPump_MagnitudeHumanRemoveRads",
        "ATX_BloodTransfusionPump_MagnitudeGhoulGiveRads",
        "ATX_CooldownDuration_BloodTransfusionPump",
        "XPD_AC_OvergrownPollinatorHealRatio",
        "XPD_AC_OvergrownPollinatorHealCloakDistance",
        "BURN_HealingSpear_HealDuration",
        "BURN_HealingSpear_HealAmount",
        "BURN_HealingSpear_HealCooldownTimer",
        "BURN_HealingSpear_HealAmount_PowerAttack",
        "BURN_HealingSpear_HealDuration_PowerAttack",
        "BOUNTY_LegendaryHealthBonus",
    ]
    for key in misc_keys:
        if key in glob_map:
            misc[key] = glob_map[key]

    return {
        "foodHealMagnitudes": food_mag,
        "foodHealMagnitudesImproved": food_mag_improved,
        "foodHealDurations": food_dur,
        "waterHealMagnitudes": water_mag,
        "foodHealRateBonuses": food_healrate,
        "foodMaxHpBonuses": food_maxhp,
        "misc": misc,
    }


def _default_globs():
    """Fallback GLOB values if TSV is missing."""
    return {
        "foodHealMagnitudes": {
            "Tiny": 0.16, "Small": 0.40, "Medium": 0.80,
            "Large": 1.20, "VeryLarge": 2.40, "Huge": 8.00, "Gourmet": 10.00,
        },
        "foodHealMagnitudesImproved": {
            "Tiny": 0.32, "Small": 0.60, "Medium": 1.00,
            "Large": 1.80, "VeryLarge": 3.60, "Huge": 10.00,
        },
        "foodHealDurations": {"standard": 25.0, "water": 10.0},
        "waterHealMagnitudes": {
            "Water": 1.0, "Purified": 2.5, "NukaSmall": 4.0,
            "NukaMedium": 10.0, "NukaLarge": 15.0, "NukaVeryLarge": 20.0,
        },
        "foodHealRateBonuses": {"Low": 0.075, "Medium": 0.125, "High": 0.250},
        "foodMaxHpBonuses": {"Low": 15.0, "Medium": 30.0, "High": 40.0},
        "misc": {},
    }


# ---------------------------------------------------------------------------
# 2. SPEL extraction
# ---------------------------------------------------------------------------
def extract_spels(effects_path):
    """Extract healing-related spell effects from SPEL_Export_*_EFFECTS.tsv."""
    print(f"  Reading SPELs from: {os.path.basename(effects_path or 'NONE')}")
    rows = read_tsv(effects_path)
    if not rows:
        print("  [WARNING] No SPEL data found", file=sys.stderr)
        return {}

    header = rows[0]
    # Build column index map
    col = {}
    for i, h in enumerate(header):
        col[h.strip()] = i

    formid_c = col.get("SPEL_FormID", 0)
    edid_c = col.get("SPEL_EDID", 1)
    full_c = col.get("SPEL_FULL", 2)
    eff_idx_c = col.get("EffectIndex", 3)
    mgef_edid_c = col.get("EFID_MGEF_EDID", 5)
    mgef_full_c = col.get("EFID_MGEF_FULL", 6)
    mag_c = col.get("EFIT_Magnitude", 11)
    dur_c = col.get("EFIT_Duration", 13)
    avif_edid_c = col.get("MAGA_AVIF_EDID", 10)

    # Collect all effects grouped by SPEL EDID
    spel_effects = {}
    for row in rows[1:]:
        if len(row) <= mag_c:
            continue
        edid = row[edid_c].strip()
        if not edid:
            continue
        formid = row[formid_c].strip()
        full = row[full_c].strip() if len(row) > full_c else ""
        eff_idx = safe_int(row[eff_idx_c]) if len(row) > eff_idx_c else 0
        mgef_edid = row[mgef_edid_c].strip() if len(row) > mgef_edid_c else ""
        mgef_full = row[mgef_full_c].strip() if len(row) > mgef_full_c else ""
        mag = safe_float(row[mag_c])
        dur = safe_int(row[dur_c]) if len(row) > dur_c else 0
        avif = row[avif_edid_c].strip() if len(row) > avif_edid_c else ""

        if edid not in spel_effects:
            spel_effects[edid] = {
                "formid": formid,
                "full": full,
                "effects": [],
            }
        spel_effects[edid]["effects"].append({
            "index": eff_idx,
            "mgef_edid": mgef_edid,
            "mgef_full": mgef_full,
            "magnitude": mag,
            "duration": dur,
            "avif": avif,
        })

    return spel_effects


# ---------------------------------------------------------------------------
# 3. PERK extraction
# ---------------------------------------------------------------------------
def extract_perks(tsv_path):
    """Extract healing-related perks from PERK_Export_*.tsv."""
    print(f"  Reading PERKs from: {os.path.basename(tsv_path or 'NONE')}")
    rows = read_tsv(tsv_path)
    if not rows:
        print("  [WARNING] No PERK data found", file=sys.stderr)
        return {}

    header = rows[0]
    col = {}
    for i, h in enumerate(header):
        col[h.strip()] = i

    formid_c = col.get("PERK_FormID", 0)
    edid_c = col.get("PERK_EDID", 1)
    full_c = col.get("FULL", 2)
    desc_c = col.get("DESC", 3)
    playable_c = col.get("DATA_Playable", 6)

    # Collect unique perks by EDID
    perk_map = {}
    for row in rows[1:]:
        if len(row) <= desc_c:
            continue
        edid = clean_str(row[edid_c])
        if not edid:
            continue
        # Skip zzz_ prefixed (deprecated/Babylon) and creature perks
        if edid.startswith("zzz_") or edid.startswith("cr"):
            continue
        formid = clean_str(row[formid_c])
        full = clean_str(row[full_c])
        desc = clean_str(row[desc_c])
        playable = clean_str(row[playable_c]) if len(row) > playable_c else ""

        if edid not in perk_map:
            perk_map[edid] = {
                "formid": formid,
                "full": full,
                "desc": desc,
                "playable": playable.lower() == "true",
            }

    return perk_map


# ---------------------------------------------------------------------------
# 4. ALCH extraction
# ---------------------------------------------------------------------------
def extract_alch(tsv_path):
    """Extract healing consumables from ALCH_Export_*.tsv.

    Handles two TSV formats:
      - Combined format: a single file with both header columns and effect columns
        (e.g. ALCH_Export_March_2026.tsv)
      - Split format: the main file has only header/ref columns, and a companion
        _Effects.tsv file has the effects (e.g. ALCH_Export_June_2026.tsv +
        ALCH_Export_June_2026_Effects.tsv)
    """
    print(f"  Reading ALCHs from: {os.path.basename(tsv_path or 'NONE')}")
    if not tsv_path:
        print("  [WARNING] No ALCH data found", file=sys.stderr)
        return {}

    # Detect format by checking if main file has MGEF columns
    rows = read_tsv_dicts(tsv_path)
    if not rows:
        print("  [WARNING] No ALCH data found", file=sys.stderr)
        return {}

    has_effects = "MGEF_EDID" in rows[0]

    if has_effects:
        # Combined format: effects are in the same file
        return _extract_alch_combined(rows)
    else:
        # Split format: look for companion _Effects.tsv file
        return _extract_alch_split(tsv_path, rows)


def _extract_alch_combined(rows):
    """Parse ALCH data from a combined single-file format (header + effects in one)."""
    alch_map = {}
    for row in rows:
        edid = clean_str(row.get("ALCH_EDID", ""))
        if not edid or edid.startswith("zzz_"):
            continue

        if edid not in alch_map:
            alch_map[edid] = {
                "formid": clean_str(row.get("ALCH_FormID", "")),
                "full": clean_str(row.get("FULL", "")),
                "desc": clean_str(row.get("DESC", "")),
                "weight": safe_float(row.get("Weight")),
                "value": safe_int(row.get("Value")),
                "effects": [],
            }

        mgef_edid = clean_str(row.get("MGEF_EDID", ""))
        mgef_full = clean_str(row.get("MGEF_FULL", ""))
        mag = safe_float(row.get("EFIT_Magnitude"))
        dur = safe_int(row.get("EFIT_Duration"))
        eff_idx = safe_int(row.get("EffectIndex"))
        magg_edid = clean_str(row.get("MAGG_GLOB_EDID", ""))
        durg_edid = clean_str(row.get("DURG_GLOB_EDID", ""))

        if mgef_edid:
            alch_map[edid]["effects"].append({
                "index": eff_idx,
                "mgef_edid": mgef_edid,
                "mgef_full": mgef_full,
                "magnitude": mag,
                "duration": dur,
                "magg_glob": magg_edid,
                "durg_glob": durg_edid,
            })

    return alch_map


def _extract_alch_split(main_tsv_path, main_rows):
    """Parse ALCH data from split format: main file + companion _Effects.tsv."""
    # Build item metadata from main file
    alch_map = {}
    for row in main_rows:
        edid = clean_str(row.get("ALCH_EDID", ""))
        if not edid or edid.startswith("zzz_"):
            continue
        if edid not in alch_map:
            alch_map[edid] = {
                "formid": clean_str(row.get("ALCH_FormID", "")),
                "full": clean_str(row.get("FULL", "")),
                "desc": clean_str(row.get("DESC", "")),
                "weight": safe_float(row.get("Weight")),
                "value": safe_int(row.get("Value")),
                "effects": [],
            }

    # Find companion Effects file
    # e.g. ALCH_Export_June_2026.tsv -> ALCH_Export_June_2026_Effects.tsv
    base, ext = os.path.splitext(main_tsv_path)
    effects_path = base + "_Effects" + ext
    if not os.path.exists(effects_path):
        # Try alternate patterns
        effects_path2 = base + "_effects" + ext
        if os.path.exists(effects_path2):
            effects_path = effects_path2
        else:
            print(f"  [WARNING] No companion Effects file found at {effects_path}",
                  file=sys.stderr)
            return alch_map

    print(f"  Reading ALCH effects from: {os.path.basename(effects_path)}")
    eff_rows = read_tsv_dicts(effects_path)
    if not eff_rows:
        print("  [WARNING] Could not read ALCH Effects file", file=sys.stderr)
        return alch_map

    for row in eff_rows:
        edid = clean_str(row.get("ALCH_EDID", ""))
        if not edid or edid.startswith("zzz_"):
            continue

        # Create entry if missing from main file
        if edid not in alch_map:
            alch_map[edid] = {
                "formid": clean_str(row.get("ALCH_FormID", "")),
                "full": "",
                "desc": "",
                "weight": 0.0,
                "value": 0,
                "effects": [],
            }

        mgef_edid = clean_str(row.get("MGEF_EDID", ""))
        mgef_full = clean_str(row.get("MGEF_FULL", ""))
        mag = safe_float(row.get("EFIT_Magnitude"))
        dur = safe_int(row.get("EFIT_Duration"))
        eff_idx = safe_int(row.get("EffectIndex"))
        magg_edid = clean_str(row.get("MAGG_GLOB_EDID", ""))
        durg_edid = clean_str(row.get("DURG_GLOB_EDID", ""))

        if mgef_edid:
            alch_map[edid]["effects"].append({
                "index": eff_idx,
                "mgef_edid": mgef_edid,
                "mgef_full": mgef_full,
                "magnitude": mag,
                "duration": dur,
                "magg_glob": magg_edid,
                "durg_glob": durg_edid,
            })

    return alch_map


# ---------------------------------------------------------------------------
# 5. ENCH extraction
# ---------------------------------------------------------------------------
def extract_enchs(tsv_path):
    """Extract healing-related enchantments from ENCH_Export_*.tsv.

    Handles two formats:
      - Legacy format with Effects_Flat column (a pipe-delimited summary string)
      - New format with individual Effect_N_MGEF_EID / Effect_N_Magnitude columns
    """
    print(f"  Reading ENCHs from: {os.path.basename(tsv_path or 'NONE')}")
    rows = read_tsv(tsv_path)
    if not rows:
        print("  [WARNING] No ENCH data found", file=sys.stderr)
        return {}

    header = rows[0]
    col = {}
    for i, h in enumerate(header):
        col[h.strip()] = i

    formid_c = col.get("ENCH_FormID", 0)
    edid_c = col.get("ENCH_EDID", 1)
    full_c = col.get("ENCH_FULL", 2)

    # Detect format
    has_effects_flat = "Effects_Flat" in col
    has_individual_effects = "Effect_1_MGEF_EID" in col

    ench_map = {}
    for row in rows[1:]:
        if len(row) <= edid_c:
            continue
        edid = row[edid_c].strip()
        if not edid:
            continue
        formid = row[formid_c].strip()
        full = row[full_c].strip() if len(row) > full_c else ""

        if has_effects_flat:
            effects_c = col["Effects_Flat"]
            effects_flat = row[effects_c].strip() if len(row) > effects_c else ""
            ench_map[edid] = {
                "formid": formid,
                "full": full,
                "effects_flat": effects_flat,
            }
        elif has_individual_effects:
            # New format: Effect_N_MGEF_EID, Effect_N_Magnitude, Effect_N_Duration
            effects_list = []
            for n in range(1, 21):  # Support up to 20 effects
                eid_key = f"Effect_{n}_MGEF_EID"
                mag_key = f"Effect_{n}_Magnitude"
                dur_key = f"Effect_{n}_Duration"
                if eid_key not in col:
                    break
                eid_c = col[eid_key]
                if len(row) <= eid_c:
                    break
                eid_val = row[eid_c].strip()
                if not eid_val:
                    continue
                # Strip FormID prefix if present (e.g. "00563B9F:V94_Solar_TeamHealSelfEffect")
                if ":" in eid_val:
                    eid_val = eid_val.split(":", 1)[1]
                mag_val = safe_float(row[col[mag_key]]) if mag_key in col and len(row) > col[mag_key] else 0.0
                dur_val = safe_int(row[col[dur_key]]) if dur_key in col and len(row) > col[dur_key] else 0

                effects_list.append({
                    "mgef_edid": eid_val,
                    "mgef_full": "",  # Not available in this format
                    "magnitude": mag_val,
                    "duration": dur_val,
                })

            # Build a synthetic effects_flat for compatibility with parse_ench_effects
            flat_parts = []
            for eff in effects_list:
                flat_parts.append(
                    f"MGEF={eff['mgef_edid']}[];FULL={eff['mgef_full']};"
                    f"Mag={eff['magnitude']};Dur={eff['duration']}"
                )
            ench_map[edid] = {
                "formid": formid,
                "full": full,
                "effects_flat": " | ".join(flat_parts),
                "_parsed_effects": effects_list,
            }
        else:
            ench_map[edid] = {
                "formid": formid,
                "full": full,
                "effects_flat": "",
            }

    return ench_map


def parse_ench_effects(ench_entry):
    """Parse enchantment effects from an ench_map entry.

    Accepts either:
      - A string (effects_flat from legacy format)
      - A dict with '_parsed_effects' key (new format) or 'effects_flat' key
    """
    if isinstance(ench_entry, str):
        effects_flat = ench_entry
    elif isinstance(ench_entry, dict):
        if "_parsed_effects" in ench_entry:
            return ench_entry["_parsed_effects"]
        effects_flat = ench_entry.get("effects_flat", "")
    else:
        return []

    effects = []
    if not effects_flat:
        return effects
    for part in effects_flat.split(" | "):
        eff = {}
        # Extract MGEF EDID
        m = re.search(r'MGEF=([^\[;]+)', part)
        if m:
            eff["mgef_edid"] = m.group(1).strip()
        # Extract FULL name
        m = re.search(r'FULL=([^;]+)', part)
        if m:
            eff["mgef_full"] = m.group(1).strip()
        # Extract Magnitude
        m = re.search(r'Mag=([\d.]+)', part)
        if m:
            eff["magnitude"] = float(m.group(1))
        # Extract Duration
        m = re.search(r'Dur=(\d+)', part)
        if m:
            eff["duration"] = int(m.group(1))
        if eff:
            effects.append(eff)
    return effects


# ---------------------------------------------------------------------------
# 6. CAMP Items extraction (ACTI + FURN → VMAD script → SPEL)
# ---------------------------------------------------------------------------

# Known CAMP healing ACTI records with VMAD script-property → SPEL chain
_CAMP_ACTI_DEFS = [
    {
        "edid": "SCORE_S24_HealingArch",
        "id": "camp-healing-arch",
        "name": "Healing Arch",
        "script_name": "WorkshopDeconArchScript",
        "spell_prop": "DeconArchSpell",
        "notes": "CAMP placement required. Season 24 Scoreboard reward. Requires power (30).",
    },
    {
        "edid": "ATX_FloorDecor_BloodTransfusionPump",
        "id": "camp-blood-transfusion-pump",
        "name": "Blood Transfusion Pump",
        "script_name": "ATX_BloodTransfusionPumpScript",
        "spell_prop": "BuffSpell",
        "notes": "CAMP placement required. Atomic Shop item.",
    },
    {
        "edid": "ATX_PlayerBuff_BodyScanner",
        "id": "camp-body-scanner",
        "name": "Body Scanner",
        "script_name": "WorkshopDeconArchScript",
        "spell_prop": "DeconArchSpell",
        "notes": ("CAMP placement required. Atomic Shop item. "
                  "Functions as Decontamination Shower. Requires power (30)."),
    },
    {
        "edid": "WorkshopDeconArch01",
        "id": "camp-decontamination-shower",
        "name": "Decontamination Shower",
        "script_name": "WorkshopDeconArchScript",
        "spell_prop": "DeconArchSpell",
        "notes": "CAMP placement required. Craftable (Science rank 2). Requires power (30).",
    },
]

# CAMP healing FURN records (FURN TSV lacks VMAD columns — spell EDID hardcoded)
_CAMP_FURN_DEFS = [
    {
        "edid": "Workshop_Symptomatic",
        "id": "camp-sympto-matic",
        "name": "Sympto-Matic",
        "spell_edid": "SymptoMaticDiseaseCure",
        "notes": "CAMP placement required. Wastelanders quest reward (An Ounce of Prevention).",
    },
]


def _parse_vmad_scripts(vmad_str):
    """Parse a VMAD_Scripts column value into structured data.

    Format: ScriptName::PropName=FormID:EDID:Type##ScriptName::PropName=Value
    Returns: {script_name: {prop_name: {"formid", "edid", "type"} or {"raw": str} or None}}
    """
    result = {}
    if not vmad_str:
        return result
    for entry in vmad_str.split("##"):
        entry = entry.strip()
        if not entry or "::" not in entry:
            continue
        script_prop, _, value = entry.partition("=")
        script_name, _, prop_name = script_prop.partition("::")
        if not script_name or not prop_name:
            continue
        if script_name not in result:
            result[script_name] = {}
        parts = value.split(":")
        if len(parts) >= 3:
            result[script_name][prop_name] = {
                "formid": parts[0],
                "edid": parts[1],
                "type": parts[2],
            }
        elif value:
            result[script_name][prop_name] = {"raw": value}
        else:
            result[script_name][prop_name] = None
    return result


def build_camp_items(acti_tsv, furn_tsv, spel_effects, globs):
    """Build CAMP healing items by reading ACTI/FURN TSVs, resolving VMAD→SPEL chains.

    For ACTI records: parses VMAD_Scripts to find the spell FormID/EDID, then
    looks up the spell in spel_effects to get magnitude/duration.
    For FURN records (Sympto-Matic): FURN TSV lacks VMAD columns, so the spell
    EDID is hardcoded and verified against spel_effects.
    """
    print("  Building CAMP healing items...")

    # --- Read ACTI TSV and find target records ---
    acti_rows = read_tsv(acti_tsv)
    acti_header = acti_rows[0] if acti_rows else []
    acti_col = {h.strip(): i for i, h in enumerate(acti_header)}
    a_formid_c = acti_col.get("ACTI_FormID", 0)
    a_edid_c = acti_col.get("ACTI_EDID", 1)
    a_full_c = acti_col.get("ACTI_FULL", 2)
    a_vmad_c = acti_col.get("VMAD_Scripts", -1)

    target_acti = {d["edid"] for d in _CAMP_ACTI_DEFS}
    acti_lookup = {}
    for row in acti_rows[1:]:
        if len(row) <= a_edid_c:
            continue
        edid = row[a_edid_c].strip()
        if edid in target_acti:
            acti_lookup[edid] = {
                "formid": row[a_formid_c].strip(),
                "full": row[a_full_c].strip() if len(row) > a_full_c else "",
                "vmad": row[a_vmad_c].strip() if a_vmad_c >= 0 and len(row) > a_vmad_c else "",
            }

    # --- Read FURN TSV and find target records ---
    furn_rows = read_tsv(furn_tsv)
    furn_header = furn_rows[0] if furn_rows else []
    furn_col = {h.strip(): i for i, h in enumerate(furn_header)}
    f_formid_c = furn_col.get("FURN_FormID", 0)
    f_edid_c = furn_col.get("FURN_EDID", 1)
    f_full_c = furn_col.get("FURN_FULL", 2)

    target_furn = {d["edid"] for d in _CAMP_FURN_DEFS}
    furn_lookup = {}
    for row in furn_rows[1:]:
        if len(row) <= f_edid_c:
            continue
        edid = row[f_edid_c].strip()
        if edid in target_furn:
            furn_lookup[edid] = {
                "formid": row[f_formid_c].strip(),
                "full": row[f_full_c].strip() if len(row) > f_full_c else "",
            }

    misc = globs.get("misc", {})
    items = []

    # --- Process ACTI items: VMAD → SPEL resolution ---
    for defn in _CAMP_ACTI_DEFS:
        acti = acti_lookup.get(defn["edid"])
        if not acti:
            print(f"    [WARNING] ACTI '{defn['edid']}' not found in TSV",
                  file=sys.stderr)
            items.append({
                "id": defn["id"], "name": defn["name"], "edid": defn["edid"],
                "formId": "", "recordType": "ACTI", "spellEdid": "",
                "hpPerSec": 0.0, "duration": 0, "combat": False,
                "source": "camp",
                "notes": defn["notes"] + " [TSV record not found]",
                "effects": [],
            })
            continue

        # Parse VMAD_Scripts to find the spell property
        vmad = _parse_vmad_scripts(acti["vmad"])
        script_data = vmad.get(defn["script_name"], {})
        prop_data = script_data.get(defn["spell_prop"])
        spell_edid = prop_data["edid"] if prop_data and "edid" in prop_data else None
        spell_formid = prop_data["formid"] if prop_data and "formid" in prop_data else ""

        if not spell_edid:
            print(f"    [WARNING] {defn['name']}: VMAD spell property not resolved",
                  file=sys.stderr)
            items.append({
                "id": defn["id"], "name": defn["name"], "edid": defn["edid"],
                "formId": acti["formid"], "recordType": "ACTI", "spellEdid": "",
                "hpPerSec": 0.0, "duration": 0, "combat": False,
                "source": "camp",
                "notes": defn["notes"] + " [VMAD not resolved]",
                "effects": [],
            })
            continue

        print(f"    {defn['name']}: VMAD -> {spell_edid} [{spell_formid}]")

        # Look up SPEL effects
        spell_info = spel_effects.get(spell_edid, {})
        effects = spell_info.get("effects", [])

        item = {
            "id": defn["id"],
            "name": defn["name"],
            "edid": defn["edid"],
            "formId": acti["formid"],
            "recordType": "ACTI",
            "spellEdid": spell_edid,
            "spellFormId": spell_formid,
            "hpPerSec": 0.0,
            "duration": 0,
            "combat": False,
            "source": "camp",
            "notes": defn["notes"],
            "effects": [],
        }

        # Collect effects and identify healing
        for eff in effects:
            mgef = eff.get("mgef_edid", "")
            full = eff.get("mgef_full", "")
            mag = eff.get("magnitude", 0.0)
            dur = eff.get("duration", 0)
            item["effects"].append({
                "mgef_edid": mgef, "mgef_full": full,
                "magnitude": mag, "duration": dur,
            })
            if "RestoreHealth" in mgef and mag > 0:
                item["hpPerSec"] = mag
                item["duration"] = dur

        # --- Special: Blood Transfusion Pump (GLOB-driven magnitudes) ---
        if defn["edid"] == "ATX_FloorDecor_BloodTransfusionPump":
            ghoul_hp = misc.get(
                "ATX_BloodTransfusionPump_MagnitudeGhoulGiveHealth", 160.0)
            human_rads = misc.get(
                "ATX_BloodTransfusionPump_MagnitudeHumanRemoveRads", 300.0)
            ghoul_rads = misc.get(
                "ATX_BloodTransfusionPump_MagnitudeGhoulGiveRads", 140.0)
            cooldown = misc.get(
                "ATX_CooldownDuration_BloodTransfusionPump", 900.0)
            item["healType"] = "instant"
            item["ghoulHpRestore"] = ghoul_hp
            item["humanRadRemoval"] = human_rads
            item["ghoulRadDamage"] = ghoul_rads
            item["cooldown"] = int(cooldown)
            item["notes"] += (
                f" Ghoul players: restores {ghoul_hp:.0f} HP"
                f" + {ghoul_rads:.0f} rads."
                f" Human players: removes {human_rads:.0f} rads."
                f" {int(cooldown)}s cooldown."
            )

        # --- Special: Decon Shower / Body Scanner (rad removal) ---
        elif spell_edid == "DeconArchSpell":
            rad_mag = 0.0
            for eff in effects:
                if "DeconMist" in eff.get("mgef_edid", ""):
                    rad_mag = eff.get("magnitude", 200.0)
            item["healType"] = "radRemoval"
            item["radRemoval"] = rad_mag
            item["notes"] += (
                f" Removes ~{rad_mag:.0f} rads. Chance to cure mutations."
            )

        # --- Special: Healing Arch (continuous RestoreHealth) ---
        elif "HealingArch" in spell_edid:
            item["healType"] = "healOverTime"
            item["notes"] += (
                " Continuously heals while standing in the arch."
            )

        items.append(item)

    # --- Process FURN items (hardcoded spell EDID) ---
    for defn in _CAMP_FURN_DEFS:
        furn = furn_lookup.get(defn["edid"])
        spell_edid = defn["spell_edid"]
        spell_info = spel_effects.get(spell_edid, {})
        effects = spell_info.get("effects", [])
        formid = furn["formid"] if furn else ""
        print(f"    {defn['name']}: hardcoded -> {spell_edid} [{formid}]")

        item = {
            "id": defn["id"],
            "name": defn["name"],
            "edid": defn["edid"],
            "formId": formid,
            "recordType": "FURN",
            "spellEdid": spell_edid,
            "hpPerSec": 0.0,
            "duration": 0,
            "combat": False,
            "source": "camp",
            "notes": defn["notes"],
            "effects": [],
        }

        for eff in effects:
            mgef = eff.get("mgef_edid", "")
            full = eff.get("mgef_full", "")
            mag = eff.get("magnitude", 0.0)
            dur = eff.get("duration", 0)
            item["effects"].append({
                "mgef_edid": mgef, "mgef_full": full,
                "magnitude": mag, "duration": dur,
            })
            if "RestoreHealth" in mgef and mag > 0:
                item["hpPerSec"] = mag
                item["duration"] = dur

        if "DiseaseCure" in spell_edid:
            item["healType"] = "diseaseCure"
            item["notes"] += " Cures all diseases (no HP heal)."

        items.append(item)

    print(f"    Total CAMP items: {len(items)}")
    return items


# ---------------------------------------------------------------------------
# Build the output JSON
# ---------------------------------------------------------------------------
def build_food_items(globs):
    """Build food healing items from GLOB data."""
    mags = globs["foodHealMagnitudes"]
    mags_imp = globs["foodHealMagnitudesImproved"]
    dur_standard = globs["foodHealDurations"]["standard"]

    food_tiers = [
        ("Tiny",      "e.g. Corn Soup, Razorgrain Soup"),
        ("Small",     "e.g. Cranberry Relish, Iguana Soup"),
        ("Medium",    "e.g. Deathclaw Steak, Mirelurk Cake"),
        ("Large",     "e.g. Cranberry Cobbler, Yao Guai Roast"),
        ("VeryLarge", "e.g. Company Tea, Brain Bombs"),
        ("Huge",      "e.g. Pepperoni Roll, S'mores"),
        ("Gourmet",   "e.g. Gourmet-tier recipes"),
    ]

    items = []
    for tier, desc in food_tiers:
        mag = mags.get(tier, 0.0)
        mag_imp = mags_imp.get(tier, 0.0)
        dur = dur_standard
        item = {
            "id": f"food-{tier.lower()}",
            "name": f"{tier} Heal Food",
            "description": desc,
            "hpPerSec": mag,
            "duration": dur,
            "totalHp": round(mag * dur, 2),
            "combat": True,
            "source": "food",
            "tier": tier,
        }
        if mag_imp > 0:
            item["hpPerSecImproved"] = mag_imp
            item["totalHpImproved"] = round(mag_imp * dur, 2)
            item["notes"] = "Improved values apply with Herbivore/Carnivore mutation"
        else:
            item["notes"] = "No improved variant found in data"
        items.append(item)

    return items


def build_water_items(globs):
    """Build water/nuka healing items from GLOB data."""
    water_mags = globs["waterHealMagnitudes"]
    dur_water = globs["foodHealDurations"]["water"]

    water_tiers = [
        ("Water",         "Boiled Water, Dirty Water",     dur_water),
        ("Purified",      "Purified Water",                dur_water),
        ("NukaSmall",     "Nuka-Cola",                     dur_water),
        ("NukaMedium",    "Nuka-Cola Cherry, Cranberry",   dur_water),
        ("NukaLarge",     "Nuka-Cola Dark, Grape, Orange", dur_water),
        ("NukaVeryLarge", "Nuka-Cola Quantum",             dur_water),
    ]

    items = []
    for tier, desc, dur in water_tiers:
        mag = water_mags.get(tier, 0.0)
        items.append({
            "id": f"water-{tier.lower()}",
            "name": f"{tier} Water/Nuka",
            "description": desc,
            "hpPerSec": mag,
            "duration": dur,
            "totalHp": round(mag * dur, 2),
            "combat": True,
            "source": "water",
            "tier": tier,
        })

    return items


def build_stimpak_items(alch_map):
    """Build stimpak/medical items from ALCH data."""
    items = []

    # Define the stimpak variants we want to extract
    stimpak_defs = [
        {
            "alch_edid": "StimpakDiluted",
            "id": "stimpak-diluted",
            "name": "Stimpak: Diluted",
        },
        {
            "alch_edid": "Stimpak",
            "id": "stimpak-regular",
            "name": "Stimpak",
        },
        {
            "alch_edid": "SuperStimpak",
            "id": "stimpak-super",
            "name": "Stimpak: Super",
        },
        {
            "alch_edid": "HealingSalve",
            "id": "stimpak-healing-salve",
            "name": "Healing Salve",
        },
        {
            "alch_edid": "Bloodpack",
            "id": "stimpak-blood-pack",
            "name": "Blood Pack",
        },
        {
            "alch_edid": "BloodpackGlowing",
            "id": "stimpak-glowing-blood-pack",
            "name": "Glowing Blood Pack",
        },
        {
            "alch_edid": "BloodpackIrradiated",
            "id": "stimpak-irradiated-blood-pack",
            "name": "Irradiated Blood Pack",
        },
        {
            "alch_edid": "ResuscitationKit",
            "id": "stimpak-resuscitation-kit",
            "name": "Resuscitation Kit",
        },
    ]

    for sdef in stimpak_defs:
        alch = alch_map.get(sdef["alch_edid"])
        if not alch:
            print(f"    [WARNING] ALCH '{sdef['alch_edid']}' not found -- skipping", file=sys.stderr)
            continue

        # Find healing effects: StimpakRestoreHealth, RestoreHealthFood, RestoreHealthChem
        heal_effects = [
            e for e in alch["effects"]
            if e["mgef_edid"] in (
                "StimpakRestoreHealth", "RestoreHealthFood",
                "RestoreHealthChem", "RestoreHealthGeneric",
                "RestoreHealthBloodPack",
            )
        ]

        # Stimpaks typically have two effects: a burst (high mag, short dur)
        # and a heal-over-time (lower mag, longer dur)
        burst_mag = 0.0
        burst_dur = 0
        hot_mag = 0.0
        hot_dur = 0

        if len(heal_effects) >= 2:
            # Sort by duration ascending -- shorter duration = burst
            sorted_effs = sorted(heal_effects, key=lambda e: e["duration"])
            # The one with shorter duration is the burst
            if sorted_effs[0]["duration"] < sorted_effs[1]["duration"]:
                burst_mag = sorted_effs[0]["magnitude"]
                burst_dur = sorted_effs[0]["duration"]
                hot_mag = sorted_effs[1]["magnitude"]
                hot_dur = sorted_effs[1]["duration"]
            else:
                burst_mag = sorted_effs[1]["magnitude"]
                burst_dur = sorted_effs[1]["duration"]
                hot_mag = sorted_effs[0]["magnitude"]
                hot_dur = sorted_effs[0]["duration"]
        elif len(heal_effects) == 1:
            eff = heal_effects[0]
            if eff["duration"] <= 5:
                burst_mag = eff["magnitude"]
                burst_dur = eff["duration"]
            else:
                hot_mag = eff["magnitude"]
                hot_dur = eff["duration"]

        total_hp = round(burst_mag * max(burst_dur, 1) + hot_mag * max(hot_dur, 1), 2)

        item = {
            "id": sdef["id"],
            "name": sdef["name"],
            "burstMagnitude": burst_mag,
            "burstDuration": burst_dur,
            "hotMagnitude": hot_mag,
            "hotDuration": hot_dur,
            "totalHp": total_hp,
            "combat": True,
            "source": "stimpak",
        }
        items.append(item)

    return items


def build_perk_items(perk_map, spel_effects):
    """Build perk card items from PERK and SPEL data."""
    items = []

    # --- Life Giver ---
    items.append({
        "id": "perk-life-giver",
        "name": "Life Giver",
        "ranks": [
            {"rank": 1, "healRateBonus": 0, "maxHpBonus": 0,
             "notes": "Gain more max HP per point in END. Curve-driven from Endurance."},
        ],
        "combat": True,
        "source": "perk",
        "condition": None,
        "notes": "Max HP bonus is driven by a curve based on Endurance stat.",
    })

    # --- Photosynthetic ---
    photo_ranks = []
    photo_spell = spel_effects.get("AbPerkPhotosynthetic", {})
    photo_effs = photo_spell.get("effects", [])
    heal_rate_effs = [e for e in photo_effs
                      if "FortifyHealRate" in e.get("mgef_edid", "")]
    heal_rate_effs.sort(key=lambda e: e["index"])
    for i, eff in enumerate(heal_rate_effs):
        photo_ranks.append({
            "rank": i + 1,
            "healRateBonus": eff["magnitude"],
        })
    if not photo_ranks:
        photo_ranks = [
            {"rank": 1, "healRateBonus": 0.3},
            {"rank": 2, "healRateBonus": 0.6},
        ]
        print("    [INFO] Photosynthetic: using default values", file=sys.stderr)

    items.append({
        "id": "perk-photosynthetic",
        "name": "Photosynthetic",
        "ranks": photo_ranks,
        "combat": True,
        "source": "perk",
        "condition": "Daytime only (6am-6pm)",
    })

    # --- Storm Chaser ---
    storm_ranks = []
    storm_spell = spel_effects.get("AbPerkStormChaser", {})
    storm_effs = storm_spell.get("effects", [])
    storm_heal = [e for e in storm_effs
                  if "FortifyHealRate" in e.get("mgef_edid", "")]
    storm_heal.sort(key=lambda e: e["index"])
    for i, eff in enumerate(storm_heal):
        storm_ranks.append({
            "rank": i + 1,
            "healRateBonus": eff["magnitude"],
        })
    if not storm_ranks:
        storm_ranks = [
            {"rank": 1, "healRateBonus": 0.5},
            {"rank": 2, "healRateBonus": 1.0},
        ]
        print("    [INFO] Storm Chaser: using default values", file=sys.stderr)

    items.append({
        "id": "perk-storm-chaser",
        "name": "Storm Chaser",
        "ranks": storm_ranks,
        "combat": True,
        "source": "perk",
        "condition": "Outside during rain or Rad Storms only",
    })

    # --- Homebody ---
    homebody_ranks = []
    homebody_spell = spel_effects.get("AbPerkHomebody", {})
    homebody_effs = homebody_spell.get("effects", [])
    homebody_heal = [e for e in homebody_effs
                     if "FortifyHealRate" in e.get("mgef_edid", "")]
    homebody_heal.sort(key=lambda e: e["index"])
    # Homebody rank 1 is limb regen, rank 2 is heal rate
    for i, eff in enumerate(homebody_heal):
        homebody_ranks.append({
            "rank": i + 1,
            "healRateBonus": eff["magnitude"],
        })
    if not homebody_ranks:
        homebody_ranks = [
            {"rank": 1, "healRateBonus": 0, "notes": "Limb regen only at rank 1"},
            {"rank": 2, "healRateBonus": 2.0},
        ]
        print("    [INFO] Homebody: using default values", file=sys.stderr)

    items.append({
        "id": "perk-homebody",
        "name": "Homebody",
        "ranks": homebody_ranks,
        "combat": False,
        "source": "perk",
        "condition": "While in C.A.M.P. or Workshop",
        "notes": "Also improves well rested benefits and grants limb regen.",
    })

    # --- Born Survivor ---
    born_ranks = []
    born_spell = spel_effects.get("zzzPerkBornSurvivorSpell", {})
    born_effs = born_spell.get("effects", [])
    born_heal = [e for e in born_effs
                 if "StimpakRestoreHealth" in e.get("mgef_edid", "")
                 or "RestoreHealth" in e.get("mgef_edid", "")]
    born_heal.sort(key=lambda e: e["index"])
    # Group by rank -- each Born Survivor rank has different mag values
    # Effects at indices 0,1,2 correspond to ranks 1,2,3
    for i, eff in enumerate(born_heal):
        born_ranks.append({
            "rank": i + 1,
            "autoStimpakHp": eff["magnitude"],
            "duration": eff["duration"],
            "notes": f"Auto-Stimpak at low HP, {eff['magnitude']} HP over {eff['duration']}s",
        })
    if not born_ranks:
        born_ranks = [
            {"rank": 1, "autoStimpakHp": 6.0, "duration": 5,
             "notes": "Below 20% HP, auto-use Stimpak every 20s"},
            {"rank": 2, "autoStimpakHp": 8.4, "duration": 5,
             "notes": "Below 30% HP, auto-use Stimpak every 20s"},
            {"rank": 3, "autoStimpakHp": 10.8, "duration": 5,
             "notes": "Below 40% HP, auto-use Stimpak every 20s"},
        ]
        print("    [INFO] Born Survivor: using default values", file=sys.stderr)

    items.append({
        "id": "perk-born-survivor",
        "name": "Born Survivor",
        "ranks": born_ranks,
        "combat": True,
        "source": "perk",
        "condition": "Falling below HP threshold",
    })

    # --- First Aid ---
    first_aid_spell = spel_effects.get("AbPerkFirstAid", {})
    first_aid_effs = first_aid_spell.get("effects", [])
    items.append({
        "id": "perk-first-aid",
        "name": "First Aid",
        "ranks": [
            {"rank": 1, "stimpakBonus": 0,
             "notes": "Stimpaks restore more HP based on INT. Curve-driven."},
        ],
        "combat": True,
        "source": "perk",
        "condition": None,
        "notes": "Stimpak healing bonus scales with Intelligence.",
    })

    # --- Ghoulish ---
    items.append({
        "id": "perk-ghoulish",
        "name": "Ghoulish",
        "ranks": [
            {"rank": 1, "notes": "Radiation now regenerates your lost Health."},
            {"rank": 2, "notes": "Radiation now regenerates more of your lost Health."},
            {"rank": 3, "notes": "Radiation now regenerates even more of your lost Health."},
        ],
        "combat": True,
        "source": "perk",
        "condition": "While taking radiation damage",
        "notes": "Converts a portion of incoming radiation into health regeneration.",
    })

    # --- Bloodsucker ---
    items.append({
        "id": "perk-bloodsucker",
        "name": "Bloodsucker",
        "ranks": [
            {"rank": 1, "notes": "Blood Packs satisfy thirst, no rads, heal 2x."},
            {"rank": 2, "notes": "Blood Packs satisfy more thirst, no rads, heal 100% more."},
            {"rank": 3, "notes": "Blood Packs greatly satisfy thirst, no rads, heal 150% more."},
        ],
        "combat": True,
        "source": "perk",
        "condition": None,
        "notes": "Modifies Blood Pack healing. Also applies to Cannibal.",
    })

    # --- Cannibal ---
    cannibal_spell = spel_effects.get("PerkCannibalHeal", {})
    cannibal_effs = cannibal_spell.get("effects", [])
    cannibal_heal = [e for e in cannibal_effs
                     if "RestoreHealth" in e.get("mgef_edid", "")]
    cannibal_ranks = []
    for i, eff in enumerate(cannibal_heal):
        cannibal_ranks.append({
            "rank": i + 1,
            "restoreHp": eff["magnitude"],
            "duration": eff["duration"],
        })
    if not cannibal_ranks:
        cannibal_ranks = [
            {"rank": 1, "restoreHp": 15.0, "duration": 5},
            {"rank": 2, "restoreHp": 30.0, "duration": 5},
        ]

    items.append({
        "id": "perk-cannibal",
        "name": "Cannibal",
        "ranks": cannibal_ranks,
        "combat": True,
        "source": "perk",
        "condition": "Eating corpses (human, Ghoul, Super Mutant, Scorched, Mole Miner)",
    })

    # --- Friendly Fire ---
    items.append({
        "id": "perk-friendly-fire",
        "name": "Friendly Fire",
        "ranks": [
            {"rank": 1, "notes": "Teammates hit by flame weapons regen health briefly."},
            {"rank": 2, "notes": "Teammates hit by flame weapons regen more health briefly."},
            {"rank": 3, "notes": "Teammates hit by flame weapons regen even more health."},
        ],
        "combat": True,
        "source": "perk",
        "condition": "Hit teammate with flame weapon (no grenades)",
    })

    # --- Spiritual Healer ---
    spirit_spell = spel_effects.get("PerkSpiritualHealer", {})
    spirit_effs = spirit_spell.get("effects", [])
    spirit_heal = [e for e in spirit_effs
                   if "RestoreHealth" in e.get("mgef_edid", "")]
    spirit_heal.sort(key=lambda e: e["index"])
    spirit_ranks = []
    for i, eff in enumerate(spirit_heal):
        spirit_ranks.append({
            "rank": i + 1,
            "restoreHp": eff["magnitude"],
            "duration": eff["duration"],
        })
    if not spirit_ranks:
        spirit_ranks = [
            {"rank": 1, "restoreHp": 6.0, "duration": 5},
            {"rank": 2, "restoreHp": 8.0, "duration": 7},
            {"rank": 3, "restoreHp": 10.0, "duration": 10},
        ]

    items.append({
        "id": "perk-spiritual-healer",
        "name": "Spiritual Healer",
        "ranks": spirit_ranks,
        "combat": True,
        "source": "perk",
        "condition": "On a team; scales with CHA",
        "notes": "Regenerate HP while on a team, based on Charisma.",
    })

    # --- E.M.T. ---
    items.append({
        "id": "perk-emt",
        "name": "E.M.T.",
        "ranks": [
            {"rank": 1, "notes": "Auto-revive downed teammates every 3 min, grant increased healing 1 min."},
            {"rank": 2, "notes": "Revived players get improved health regen for 30s."},
            {"rank": 3, "notes": "Revived players get high health regen for 60s."},
        ],
        "combat": True,
        "source": "perk",
        "condition": "After reviving a teammate",
    })

    return items


def build_mutation_items(spel_effects):
    """Build mutation items from SPEL data."""
    items = []

    # --- Healing Factor ---
    hf_spell = spel_effects.get("Mutation_HealingFactor", {})
    hf_effs = hf_spell.get("effects", [])

    heal_rate = 3.0
    chem_penalty = 55.0
    heal_rate_improved = 3.75

    for eff in hf_effs:
        if "FortifyHealRate" in eff.get("mgef_edid", ""):
            if eff["magnitude"] > heal_rate:
                heal_rate_improved = eff["magnitude"]
            else:
                heal_rate = eff["magnitude"]
        if "ReduceChemEffect" in eff.get("mgef_edid", ""):
            chem_penalty = eff["magnitude"]

    items.append({
        "id": "mutation-healing-factor",
        "name": "Healing Factor",
        "healRateBonus": heal_rate,
        "healRateBonusImproved": heal_rate_improved,
        "combat": False,
        "source": "mutation",
        "notes": f"Also reduces chem effectiveness by {chem_penalty}%. "
                 f"Improved value ({heal_rate_improved}) applies with Strange in Numbers.",
        "condition": "Out of combat only",
    })

    return items


def build_legendary_items(ench_map, spel_effects):
    """Build legendary effect items from ENCH and SPEL data."""
    items = []

    # --- Vampire's (weapon) ---
    vamp_spell = spel_effects.get("Legendary_VampireRegen", {})
    vamp_effs = vamp_spell.get("effects", [])
    vamp_hp = 1.0
    vamp_dur = 2
    for eff in vamp_effs:
        if "RestoreHealth" in eff.get("mgef_edid", ""):
            vamp_hp = eff["magnitude"]
            vamp_dur = eff["duration"]
            break

    items.append({
        "id": "legendary-vampires",
        "name": "Vampire's",
        "hpPerHit": vamp_hp,
        "healDuration": vamp_dur,
        "combat": True,
        "source": "legendary-weapon",
        "notes": f"Restores {vamp_hp} HP over {vamp_dur}s per hit.",
    })

    # --- Medic's (weapon) ---
    medic_spell = spel_effects.get("PerkMedicSpell", {})
    medic_effs = medic_spell.get("effects", [])
    medic_hp = 5.0
    medic_dur = 1
    for eff in medic_effs:
        if "Medic" in eff.get("mgef_edid", "") or "Vampire" in eff.get("mgef_edid", ""):
            medic_hp = eff["magnitude"]
            medic_dur = eff["duration"]
            break

    items.append({
        "id": "legendary-medics",
        "name": "Medic's",
        "hpOnCrit": medic_hp,
        "healDuration": medic_dur,
        "combat": True,
        "source": "legendary-weapon",
        "notes": f"V.A.T.S. crits heal you and your team for {medic_hp} HP.",
    })

    # --- Crits Heal Team ---
    crits_spell = spel_effects.get("Legendary_CritsHealTeamSpell", {})
    crits_effs = crits_spell.get("effects", [])
    crits_hp = 5.0
    for eff in crits_effs:
        if "RestoreHealth" in eff.get("mgef_edid", ""):
            crits_hp = eff["magnitude"]
            break

    items.append({
        "id": "legendary-crits-heal-team",
        "name": "Medic (Critical)",
        "hpPerCrit": crits_hp,
        "combat": True,
        "source": "legendary-weapon",
        "notes": f"V.A.T.S. critical hits heal you and your group for {crits_hp} HP.",
    })

    # --- Health Regen (armor) ---
    items.append({
        "id": "legendary-health-regen",
        "name": "Health Regeneration",
        "healRateBonus": 0,
        "combat": True,
        "source": "legendary-armor",
        "notes": "Slowly regenerate health. Heal rate is curve-driven by player level.",
    })

    # --- Auto Stimpak (armor) ---
    items.append({
        "id": "legendary-auto-stimpak",
        "name": "Auto Stimpak",
        "combat": True,
        "source": "legendary-armor",
        "notes": "Automatically uses a Stimpak when health is low. Duration 1s.",
    })

    # --- Increase Healing / Healer's (armor) ---
    items.append({
        "id": "legendary-increase-healing",
        "name": "Healer's",
        "combat": True,
        "source": "legendary-armor",
        "notes": "Increases healing from all sources via LegendaryIncreaseHealingPerk.",
    })

    # --- Blood Sacrifice ---
    bs_data = []
    for tier in range(1, 5):
        spell_key = f"LGN_BloodSacrifice_Spell0{tier}"
        bs_spell = spel_effects.get(spell_key, {})
        bs_effs = bs_spell.get("effects", [])
        heal_rate_val = 0.0
        restore_hp = 0.0
        dur = 0
        for eff in bs_effs:
            edid = eff.get("mgef_edid", "")
            if "HealRate" in edid or "HealthRegen" in edid or "FortifyHealRate" in edid:
                heal_rate_val = eff["magnitude"]
                dur = eff["duration"]
            if "RestoreHealth" in edid:
                restore_hp = eff["magnitude"]
                if not dur:
                    dur = eff["duration"]
        if heal_rate_val or restore_hp:
            bs_data.append({
                "tier": tier,
                "healRate": heal_rate_val,
                "restoreHp": restore_hp,
                "duration": dur,
            })

    items.append({
        "id": "legendary-blood-sacrifice",
        "name": "Blood Sacrifice",
        "tiers": bs_data if bs_data else [
            {"tier": 1, "healRate": 25.0, "restoreHp": 5.0, "duration": 8},
            {"tier": 2, "healRate": 30.0, "restoreHp": 5.0, "duration": 10},
            {"tier": 3, "healRate": 35.0, "restoreHp": 5.0, "duration": 12},
            {"tier": 4, "healRate": 40.0, "restoreHp": 5.0, "duration": 14},
        ],
        "combat": True,
        "source": "legendary-armor",
        "notes": "Teammate goes down: you heal over time. Cooldown 300s.",
    })

    # --- Retribution ---
    ret_data = []
    for tier in range(1, 5):
        spell_key = f"LGN_RetributionBuffSpell0{tier}"
        ret_spell = spel_effects.get(spell_key, {})
        ret_effs = ret_spell.get("effects", [])
        restore_hp = 0.0
        dur = 0
        for eff in ret_effs:
            if "RestoreHealth" in eff.get("mgef_edid", ""):
                restore_hp = eff["magnitude"]
                dur = eff["duration"]
                break
        if restore_hp:
            ret_data.append({
                "tier": tier,
                "restoreHpPerSec": restore_hp,
                "duration": dur,
            })

    items.append({
        "id": "legendary-retribution",
        "name": "Retribution",
        "tiers": ret_data if ret_data else [
            {"tier": 1, "restoreHpPerSec": 1.0, "duration": 15},
            {"tier": 2, "restoreHpPerSec": 2.0, "duration": 15},
            {"tier": 3, "restoreHpPerSec": 3.0, "duration": 15},
            {"tier": 4, "restoreHpPerSec": 4.0, "duration": 15},
        ],
        "combat": True,
        "source": "legendary-armor",
        "notes": "On kill: regenerate health and AP for 15s.",
    })

    return items


def build_equipment_items(ench_map, spel_effects):
    """Build equipment items (Solar Armor, etc.) from ENCH and SPEL data."""
    items = []

    # --- Solar Armor (Set Bonus) ---
    solar_ench = ench_map.get("ench_Armor_Set_V94_Solar", {})
    solar_effs = parse_ench_effects(solar_ench)
    solar_self_heal = 4.0
    solar_team_heal = 35.0
    for eff in solar_effs:
        full = eff.get("mgef_full", "")
        edid = eff.get("mgef_edid", "")
        mag = eff.get("magnitude", 0)
        if "Solar Armor Heal" in full or "TeamHealSelfEffect" in edid:
            solar_self_heal = mag
        if "Cloak" in full or "TeamHealCloakEffect" in edid:
            solar_team_heal = mag

    # Also check SPEL for Solar Armor
    solar_spell = spel_effects.get("V94_Solar_TeamHealSpell", {})
    solar_spell_effs = solar_spell.get("effects", [])
    for eff in solar_spell_effs:
        if "SolarArmorHeal" in eff.get("mgef_edid", "") or \
           "Solar Armor Heal" in eff.get("mgef_full", ""):
            solar_self_heal = eff["magnitude"]

    items.append({
        "id": "equip-solar-armor",
        "name": "Solar Armor (Set Bonus)",
        "hpPerSec": solar_self_heal,
        "teamHealCloak": solar_team_heal,
        "combat": True,
        "source": "armor-set",
        "condition": "Above 60% HP",
        "notes": f"Self heal {solar_self_heal} HP/s. Cloak heals nearby teammates at {solar_team_heal} HP.",
    })

    return items


def build_consumable_items(spel_effects):
    """Build bobblehead and magazine items from SPEL data."""
    items = []

    # --- Bobblehead: Medicine ---
    bobble_spell = spel_effects.get("BobbleHead_MedicineSpell", {})
    bobble_effs = bobble_spell.get("effects", [])
    bobble_mag = 30.0
    bobble_dur = 3600
    for eff in bobble_effs:
        if "Stimpak" in eff.get("mgef_full", "") or "Stimpak" in eff.get("mgef_edid", ""):
            bobble_mag = eff["magnitude"]
            bobble_dur = eff["duration"]
            break

    items.append({
        "id": "bobblehead-medicine",
        "name": "Bobblehead: Medicine",
        "stimpakBonus": bobble_mag,
        "duration": bobble_dur,
        "combat": True,
        "source": "bobblehead",
        "notes": f"+{bobble_mag}% Stimpak healing for {bobble_dur}s ({bobble_dur // 60} min).",
    })

    # --- Awesome Tales 6 (Magazine) ---
    mag_spell = spel_effects.get("Magazine_AwesomeTales06Spell", {})
    mag_effs = mag_spell.get("effects", [])
    mag_healrate = 0.2
    mag_dur = 7200
    for eff in mag_effs:
        if "HealRate" in eff.get("mgef_edid", "") or "Heal Rate" in eff.get("mgef_full", ""):
            mag_healrate = eff["magnitude"]
            mag_dur = eff["duration"]
            break

    items.append({
        "id": "magazine-awesome-tales-6",
        "name": "Awesome Tales 6",
        "healRateBonus": mag_healrate,
        "duration": mag_dur,
        "combat": True,
        "source": "magazine",
        "notes": f"+{mag_healrate} heal rate for {mag_dur}s ({mag_dur // 3600}h).",
    })

    return items


def build_food_healrate_items(globs):
    """Build food heal-rate buff items from GLOB data."""
    bonuses = globs["foodHealRateBonuses"]
    items = []

    tier_examples = {
        "Low": "e.g. Corn Soup, Simple foods",
        "Medium": "e.g. Cranberry Relish, Steeped Thistle Tea",
        "High": "e.g. Brain Bombs, Company Tea",
    }

    for tier, bonus in bonuses.items():
        items.append({
            "id": f"food-healrate-{tier.lower()}",
            "name": f"{tier} Heal Rate Food",
            "healRateBonus": bonus,
            "combat": True,
            "source": "food",
            "tier": tier,
            "description": tier_examples.get(tier, ""),
        })

    return items


def build_food_maxhp_items(globs):
    """Build food max-HP buff items from GLOB data."""
    bonuses = globs["foodMaxHpBonuses"]
    items = []

    for tier, bonus in bonuses.items():
        items.append({
            "id": f"food-maxhp-{tier.lower()}",
            "name": f"{tier} Max HP Food",
            "maxHpBonus": bonus,
            "combat": True,
            "source": "food",
            "tier": tier,
        })

    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("[build_healing_calculator_json.py] Starting...")

    # Find newest TSV files
    glob_tsv = find_newest_tsv("GLOB_Export_*.tsv")
    spel_effects_tsv = find_newest_tsv("SPEL_Export_*_EFFECTS.tsv")
    perk_tsv = find_newest_tsv("PERK_Export_*.tsv")
    alch_tsv = find_newest_tsv("ALCH_Export_*.tsv")
    ench_tsv = find_newest_tsv("ENCH_Export_*.tsv")
    acti_tsv = find_newest_tsv("ACTI_Export_*_ACTI.tsv")
    furn_tsv = find_newest_tsv("FURN_Export_*_FURN.tsv")

    print(f"  GLOB: {os.path.basename(glob_tsv) if glob_tsv else 'NOT FOUND'}")
    print(f"  SPEL: {os.path.basename(spel_effects_tsv) if spel_effects_tsv else 'NOT FOUND'}")
    print(f"  PERK: {os.path.basename(perk_tsv) if perk_tsv else 'NOT FOUND'}")
    print(f"  ALCH: {os.path.basename(alch_tsv) if alch_tsv else 'NOT FOUND'}")
    print(f"  ENCH: {os.path.basename(ench_tsv) if ench_tsv else 'NOT FOUND'}")
    print(f"  ACTI: {os.path.basename(acti_tsv) if acti_tsv else 'NOT FOUND'}")
    print(f"  FURN: {os.path.basename(furn_tsv) if furn_tsv else 'NOT FOUND'}")

    # Extract raw data from TSVs
    globs = extract_globs(glob_tsv)
    spel_effects = extract_spels(spel_effects_tsv)
    perk_map = extract_perks(perk_tsv)
    alch_map = extract_alch(alch_tsv)
    ench_map = extract_enchs(ench_tsv)

    print(f"\n  Extracted: {len(spel_effects)} SPEL records, "
          f"{len(perk_map)} PERK records, {len(alch_map)} ALCH records, "
          f"{len(ench_map)} ENCH records")

    # Build structured output
    food_items = build_food_items(globs)
    water_items = build_water_items(globs)
    stimpak_items = build_stimpak_items(alch_map)
    perk_items = build_perk_items(perk_map, spel_effects)
    mutation_items = build_mutation_items(spel_effects)
    legendary_items = build_legendary_items(ench_map, spel_effects)
    equipment_items = build_equipment_items(ench_map, spel_effects)
    camp_items = build_camp_items(acti_tsv, furn_tsv, spel_effects, globs)
    consumable_items = build_consumable_items(spel_effects)
    food_healrate_items = build_food_healrate_items(globs)
    food_maxhp_items = build_food_maxhp_items(globs)

    output = {
        "generated": str(date.today()),
        "sources": {
            "base": {
                "label": "Base HP Regeneration",
                "items": [
                    {
                        "id": "base-hp-regen",
                        "name": "Base HP Regeneration",
                        "description": "Passive health regeneration outside of combat",
                        "hpPerSec": 0,
                        "isPercentBased": True,
                        "percentPerSec": 0.5,
                        "duration": None,
                        "combat": False,
                        "source": "base",
                        "notes": "Scales with max HP. Only active outside combat.",
                    }
                ],
            },
            "food": {
                "label": "Food & Drink",
                "items": food_items,
            },
            "water": {
                "label": "Water & Nuka-Cola",
                "items": water_items,
            },
            "stimpaks": {
                "label": "Stimpaks & Medical",
                "items": stimpak_items,
            },
            "perks": {
                "label": "Perk Cards",
                "items": perk_items,
            },
            "mutations": {
                "label": "Mutations",
                "items": mutation_items,
            },
            "legendary": {
                "label": "Legendary Effects",
                "items": legendary_items,
            },
            "equipment": {
                "label": "Equipment & Power Armor",
                "items": equipment_items,
            },
            "camp": {
                "label": "CAMP Items",
                "items": camp_items,
            },
            "team": {
                "label": "Team & Social",
                "items": [
                    {
                        "id": "team-medic-perk",
                        "name": "Team Medic (Perk)",
                        "description": "Stimpaks also heal nearby teammates",
                        "combat": True,
                        "source": "perk",
                        "notes": "Heal amount depends on perk rank and stimpak type.",
                    },
                ],
            },
            "consumables": {
                "label": "Bobbleheads & Magazines",
                "items": consumable_items,
            },
            "foodHealRate": {
                "label": "Food Heal Rate Buffs",
                "items": food_healrate_items,
            },
            "foodMaxHp": {
                "label": "Food Max HP Buffs",
                "items": food_maxhp_items,
            },
        },
        "globals": {
            "foodHealMagnitudes": globs["foodHealMagnitudes"],
            "foodHealMagnitudesImproved": globs["foodHealMagnitudesImproved"],
            "foodHealDurations": globs["foodHealDurations"],
            "foodHealRateBonuses": globs["foodHealRateBonuses"],
            "foodMaxHpBonuses": globs["foodMaxHpBonuses"],
            "waterHealMagnitudes": globs["waterHealMagnitudes"],
        },
        "mechanics": {
            "healRateDescription": (
                "HealRate is a multiplier applied to base HP regeneration. "
                "A HealRate of 1.0 means 100% of base regen speed."
            ),
            "combatNote": (
                "Most passive regeneration effects are disabled during combat. "
                "Food heal-over-time, Stimpaks, and Vampire's continue to work in combat."
            ),
            "stackingNote": (
                "Multiple food heal-over-time effects do NOT stack -- only the most "
                "recent food/drink applies. Heal Rate bonuses from perks, mutations, "
                "and legendary effects DO stack additively."
            ),
        },
    }

    # Ensure output directory exists
    os.makedirs(OUT_DIR, exist_ok=True)

    # Write JSON
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Count items
    total_items = sum(
        len(cat.get("items", []))
        for cat in output["sources"].values()
    )
    print(f"\n  Wrote {OUT_PATH}")
    print(f"  Total items: {total_items} across {len(output['sources'])} categories")
    print("[build_healing_calculator_json.py] Done.")


if __name__ == "__main__":
    main()