#!/usr/bin/env python3
"""
build_load_screens_json.py
==========================
Generates dist/load_screens.json from the xEdit LSCR TSV export.

Input file (in tsv/ folder or pass via --data-dir):
  LSCR_Export_*.tsv

Output:
  dist/load_screens.json — categorised load screen entries for buffsnbrew.com

Usage:
  python build_load_screens_json.py
  python build_load_screens_json.py --data-dir tsv --out dist/load_screens.json
"""

import csv
import json
import os
import re
import sys
import argparse
from collections import defaultdict

from patchlog_utils import diff_item_lists, _write_json, _git_show_json

# ─────────────────────────────────────────────────────────────────────────────
#  EDID PREFIX → CATEGORY MAPPING
# ─────────────────────────────────────────────────────────────────────────────
#
# Order matters — first match wins.  More specific prefixes must come before
# broader ones (e.g. "CUT_" before "Creature_").
#
# Prefixes tested with startswith(); the EDID is stripped of quotes first.
# Entries that match none of these fall into "General Gameplay".

PREFIX_CATEGORIES = [
    # Cut / deprecated content  — must be checked first
    # NOTE: DELETED_Perk_ stays in Perks (not cut) — only zzz/CUT/GHL are cut
    ("zzz",                         "Cut Content"),
    ("CUT_",                        "Cut Content"),
    ("GHL00_",                      "Cut Content"),   # Ghoul Blackout (cut quest)

    # Explicit categories by prefix
    ("Armor_",                      "Armour & Power Armour"),
    ("Chems_",                      "Chems, Food & Aid"),
    ("Creature_",                   "Creatures"),
    ("Event_",                      "Events"),
    ("Events_",                     "Events"),        # Events_Hordes, Events_Workshops, etc.
    ("E05_Caravan_",                "Events"),
    ("E07A_Mothman_",               "Events"),
    ("Storm_E01_",                  "Events"),
    ("Faction_",                    "Factions"),
    ("GeneralGameplay_",           "General Gameplay"),
    ("Lore_",                       "Lore"),
    ("Magazine_",                   "Magazines & Collectibles"),
    ("Mutations_",                  "Mutations"),
    ("Perk_",                       "Perks & S.P.E.C.I.A.L."),
    ("SPECIAL_",                    "Perks & S.P.E.C.I.A.L."),
    ("DELETED_Perk_",               "Perks & S.P.E.C.I.A.L."),
    ("Region_",                     "Locations"),
    ("Vault_",                      "Vaults"),
    ("Weapon_",                     "Weapons"),

    # Location-style entries from specific content packs
    ("XPD_AC",                      "Locations"),     # Atlantic City locations
    ("XPD_Pitt_",                   "Locations"),     # The Pitt locations
    ("Nukashine_",                  "Locations"),
    ("EnclaveResearchFacility_",    "Locations"),
    ("FortAtlas",                   "Locations"),
    ("MakeshiftVault_",            "Locations"),
    ("OrwellOrchardsBunker_",       "Locations"),
    ("MILE_",                       "Locations"),

    # Vault 76 exit
    ("aa_Vault76Exit_",             "Vaults"),

    # Wastelanders (W05) sub-categorisation
    ("W05_Location_",              "Locations"),
    ("W05_Enemy_",                  "General Gameplay"),
    ("W05_GeneralGameplay_",       "General Gameplay"),

    # Misc that lands in General Gameplay
    ("Survival_",                   "General Gameplay"),
    ("SharedQuests_",              "General Gameplay"),
    ("SheltersLoadScreen_",        "General Gameplay"),
    ("Photomode_",                  "General Gameplay"),
    ("Fishing_",                    "General Gameplay"),
]

CATEGORY_ORDER = [
    "Armour & Power Armour",
    "Chems, Food & Aid",
    "Creatures",
    "Events",
    "Factions",
    "General Gameplay",
    "Locations",
    "Lore",
    "Magazines & Collectibles",
    "Mutations",
    "Perks & S.P.E.C.I.A.L.",
    "Vaults",
    "Weapons",
    "Cut Content",
]


def classify(edid):
    """Return the category name for an EDID string."""
    e = edid.strip('"').strip()
    # DELETED_Perk_ must be checked BEFORE the generic CUT/zzz prefixes
    if e.startswith("DELETED_Perk_"):
        return "Perks & S.P.E.C.I.A.L."
    for prefix, cat in PREFIX_CATEGORIES:
        if e.startswith(prefix):
            return cat
    return "General Gameplay"


# ─────────────────────────────────────────────────────────────────────────────
#  DISPLAY NAME EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

# Exact EDID → display name overrides.
# Use these for entries where algorithmic name generation doesn't produce
# the right result. Checked first — if the EDID is here, we skip all logic.
NAME_OVERRIDES = {
    # Events with non-obvious names
    "E05_Caravan_LoadScreen":           "Blue Ridge Caravan",
    "E07A_Mothman_LoadScreen":          "The Mothman Equinox",
    "Storm_E01_Dangerous_LoadScreen":   "Dangerous Pastimes",

    # Events — names matched from QUEST TSV cross-reference
    "Event_E03A_Mischief":              "Mischief Night",
    "Event_MN2_Mischief":               "Mischief Night (Rapidan Camp)",
    "Event_E07B_Invaders":              "Invaders from Beyond",
    "Event_76ExitEventQuest":           "Vault 76 Exit Event",
    "Event_RE_Scene_MTNZ05_Messenger":  "Messenger Event",
    "Event_PowerPlantEvent":            "Power Plant Event",
    "Event_TW003":                      "Manhunt",
    "Event_TW006":                      "Protest March",
    "Event_TW009":                      "The Battle that Never Was",
    "Event_TW043":                      "Patrol Duty",
    "Event_TWZ07":                      "Grafton Day",
    "Event_BoSZ03":                     "Distant Thunder",
    "Event_ENz01_Above":                "Dropped Connection",
    "Event_ENz04_Bots":                 "Bots on Parade",
    "Event_ENs02_Blast":                "A Real Blast",
    "Event_CB06_ASAM":                  "Surface to Air",
    "Event_CBZ09_Census":               "Census Violence",
    "Event_CBZ13_Robots":               "AWOL Armaments",
    "Event_FF09_Reaper":                "Fertile Soil",
    "Event_FF11_Raid":                  "Collision Course",
    "Event_FF12_Bell":                  "The Bell Tolls",
    "Event_FFZ11_Pack":                 "Leader of the Pack",
    "Event_FFZ16_Swatter":              "Fly Swatter",
    "Event_FSS01_Trap":                 "It's a Trap",
    "Event_MTR05_Mother_Breach":        "Breach and Clear",
    "Event_MTR10_Battle":               "Battle Bots",
    "Event_RS02_Beat":                  "Back on the Beat",
    "Event_SFZ08_Fear":                 "Irrational Fear",

    # Photomode
    "Photomode_PersonalLoadScreen":     "Photomode Personal Loading Screens",

    # Fishing
    "Fishing_FishermansRest_Loadscreen": "Fishing Tips",

    # Vault 76 exit
    "aa_Vault76Exit_01":                "Vault 76 Exit",

    # Nukashine
    "Nukashine_LoadScreen":             "Load Screen",

    # Locations where suffix-stripping breaks prefix-stripping
    "XPD_AC02_Blackout_LoadScreen":     "AC02 Blackout",
    "GHL00_Quest_Blackout_LoadScreen":  "Ghoul Blackout Quest",
}

# Suffixes to strip from EDID before name extraction
STRIP_SUFFIXES = ["_LoadScreen", "_Loadscreen", "_loadscreen"]

# Category-prefix stripping table.  For each category we list the prefixes
# to strip (in order — first match wins, longest checked first automatically).
# After stripping, the remainder is CamelCase-split and underscores → spaces.
#
# Within a category the strip list is sorted longest-first at runtime.
CATEGORY_STRIP = {
    "Armour & Power Armour": [
        "Armor_",
    ],
    "Chems, Food & Aid": [
        "Chems_",
    ],
    "Creatures": [
        "Creature_",
    ],
    "Cut Content": [
        "zzzBabylon_Loadscreen_",
        "zzz_Event_",
        "zzz",
        "GHL00_Quest_Blackout_",
        "CUT_Quickplay_",
        "CUT_Vault_",
        "CUT_",
    ],
    "Events": [
        # Specific sub-prefixes (strip the code prefix, keep the name)
        "Event_Storm_", "Event_BURN_", "Event_MOON_",
        "Event_MTNM04_", "Event_MTNM03_",
        "Event_MN2_", "Event_SSE_",
        "Event_E01B_Herd",   # Free Range
        "Event_E01B_", "Event_E01C_", "Event_E01F_",
        "Event_E02A_", "Event_E03A_", "Event_E05_",
        "Event_E06_", "Event_E07B_", "Event_E08A_", "Event_E08B_",
        "Event_E09A_", "Event_E09B_", "Event_E09C_", "Event_E09D_",
        "Event_ENs02_",
        "Event_GQ_",       # GQ_ → strip, keep rest (Horde, WorkshopAttack)
        "Event_FSS02_",
        "Event_BS02_",
        "Event_",
        "Events_",
    ],
    "Factions": [
        "Faction_",
    ],
    "General Gameplay": [
        "GeneralGameplay_",
        "W05_Enemy_",
        "W05_GeneralGameplay_",
        "Survival_",
        "SharedQuests_",
        "SheltersLoadScreen_",
    ],
    "Locations": [
        "XPD_AC02_Blackout_",
        "XPD_AC_",
        "XPD_Pitt_",
        "EnclaveResearchFacility_",
        "FortAtlasSubstructure_",
        "FortAtlas_",
        "MakeshiftVault_",
        "OrwellOrchardsBunker_",
        "MILE_MN2_",
        "Region_",
        "W05_Location_",
    ],
    "Lore": [
        "Lore_",
    ],
    "Magazines & Collectibles": [
        "Magazine_",
    ],
    "Mutations": [
        "Mutations_",
    ],
    "Perks & S.P.E.C.I.A.L.": [
        "DELETED_Perk_",
        "Perk_",
        "SPECIAL_01_", "SPECIAL_02_", "SPECIAL_03_",
        "SPECIAL_04_", "SPECIAL_05_", "SPECIAL_06_",
    ],
    "Vaults": [
        "Vault_",
    ],
    "Weapons": [
        "Weapon_",
    ],
}

# Sort each category's strip list longest-first
for _cat in CATEGORY_STRIP:
    CATEGORY_STRIP[_cat].sort(key=len, reverse=True)


def _camel_split(s):
    """Split CamelCase into words: 'DeathclawGauntlet' → 'Deathclaw Gauntlet'."""
    return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', s)


def extract_name(edid, lsst=None, category=None):
    """
    Build a human-readable display name from the EDID.

    For LSST-only event entries, the first double-space chunk of the LSST
    text is the actual in-game event name — use that instead.
    """
    e = edid.strip('"').strip()

    # 1. Check overrides first
    if e in NAME_OVERRIDES:
        return NAME_OVERRIDES[e]

    # 2. For event entries that have LSST, use the first double-space chunk
    #    as the event name (it's the actual in-game title)
    if lsst and category == "Events":
        chunks = lsst.split("  ")
        if chunks and chunks[0].strip():
            return chunks[0].strip()

    # 3. Strip known suffixes from the working EDID
    for sfx in STRIP_SUFFIXES:
        if e.endswith(sfx):
            e = e[:-len(sfx)]

    # 4. Strip the category prefix
    strips = CATEGORY_STRIP.get(category, [])
    for pfx in strips:
        if e.startswith(pfx):
            e = e[len(pfx):]
            break

    # 4b. For Events, strip remaining internal code prefixes (CB02_, FF01_, etc.)
    #     Also strip RE_Scene_ and any secondary code prefix after it.
    if category == "Events":
        e = re.sub(r'^RE_Scene_', '', e)
        e = re.sub(r'^[A-Za-z]{2,4}\d{1,3}_', '', e)

    # 5. Handle Power Armour names specially
    #    "PowerArmor_T45" → "T-45 Power Armour"
    if "Armor_" in edid and "PowerArmor_" in edid:
        # After stripping "Armor_", we have "PowerArmor_X"
        pa_match = re.match(r'PowerArmor_(.+)', e)
        if pa_match:
            rest = pa_match.group(1)
            rest = _camel_split(rest).replace("_", " ").strip()
            # T-series
            rest = re.sub(r'^(T)(\d+)', r'\1-\2', rest)
            if rest.lower() == "frame":
                return "Power Armour Frame"
            elif rest.lower() == "raider":
                return "Power Armor Raider"
            else:
                return f"{rest} Power Armour"

    # 6. T-series fix for standalone T## patterns
    e = re.sub(r'^(T)(\d+)', r'\1-\2', e)

    # 7. CamelCase split
    e = _camel_split(e)

    # 8. Replace underscores with spaces
    e = e.replace("_", " ")

    # 9. Collapse multiple spaces
    e = re.sub(r'\s+', ' ', e).strip()

    return e


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN BUILD
# ─────────────────────────────────────────────────────────────────────────────

def build_load_screens(tsv_path):
    """Read the LSCR TSV and return the structured JSON data."""

    categories = defaultdict(list)

    with open(tsv_path, encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            form_id = row.get("LSCR_FormID", "").strip().strip('"')
            edid    = row.get("LSCR_EDID", "").strip().strip('"')
            desc    = row.get("DESC_Description", "").strip().strip('"')
            lsst    = row.get("LSST_LoadingScreenText", "").strip().strip('"')

            if not form_id or not edid:
                continue

            # Normalise empty strings to None
            if not desc:
                desc = None
            if not lsst:
                lsst = None

            # Skip entries with neither DESC nor LSST
            if not desc and not lsst:
                continue

            cat = classify(edid)
            name = extract_name(edid, lsst, cat)

            categories[cat].append({
                "formId":  form_id,
                "edid":    edid,
                "name":    name,
                "desc":    desc,
                "category": cat,
                "hasLsst": lsst is not None,
                "lsst":    lsst,
            })

    # Sort items within each category alphabetically by name
    for cat in categories:
        categories[cat].sort(key=lambda x: x["name"].lower())

    # Build output in fixed category order
    output_categories = []
    total = 0
    for cat_name in CATEGORY_ORDER:
        items = categories.get(cat_name, [])
        if not items:
            continue
        total += len(items)
        output_categories.append({
            "name":  cat_name,
            "count": len(items),
            "items": items,
        })

    # Catch any categories not in the fixed order
    for cat_name in sorted(categories.keys()):
        if cat_name not in CATEGORY_ORDER:
            items = categories[cat_name]
            total += len(items)
            output_categories.append({
                "name":  cat_name,
                "count": len(items),
                "items": items,
            })

    return {
        "generated":  __import__("datetime").date.today().isoformat(),
        "totalCount": total,
        "categories": output_categories,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate load_screens.json from LSCR TSV export"
    )
    parser.add_argument(
        "--data-dir", default="tsv",
        help="Folder containing TSV files (default: tsv)"
    )
    parser.add_argument(
        "--out", default="dist/load_screens.json",
        help="Output JSON path (default: dist/load_screens.json)"
    )
    parser.add_argument(
        "--tsv", default=None,
        help="Override LSCR TSV path directly"
    )
    args = parser.parse_args()

    # Resolve TSV path
    def find_tsv(directory, keyword):
        for fname in sorted(os.listdir(directory), reverse=True):
            if keyword.lower() in fname.lower() and fname.endswith(".tsv"):
                return os.path.join(directory, fname)
        raise FileNotFoundError(
            f'No TSV matching "{keyword}" in {directory}'
        )

    tsv_path = args.tsv or find_tsv(args.data_dir, "LSCR")
    print(f"LSCR TSV: {tsv_path}")

    print("Building load screen data…")
    data = build_load_screens(tsv_path)
    print(f"  {data['totalCount']} entries across {len(data['categories'])} categories")
    for cat in data["categories"]:
        lsst_count = sum(1 for i in cat["items"] if i["hasLsst"])
        lsst_str = f" ({lsst_count} with LSST)" if lsst_count else ""
        print(f"    {cat['name']}: {cat['count']}{lsst_str}")

    # Write output
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    # Write output
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)

    size_kb = os.path.getsize(args.out) / 1024
    print(f"\nSaved → {args.out}  ({size_kb:.0f} KB)")

    # ── Patchlog feed ──────────────────────────────────────────────────────
    dist_base = os.path.dirname(args.out) or "dist"
    prev_json = _git_show_json("HEAD^", args.out)

    def extract_all_items(d):
        if not d:
            return []
        items = []
        for cat in d.get("categories", []):
            items.extend(cat.get("items", []))
        return items

    entry = diff_item_lists(
        prev_items=extract_all_items(prev_json),
        curr_items=extract_all_items(data),
        key_field="formId",
        name_field="name",
        compare_fields=["name", "desc", "category", "hasLsst", "lsst"],
    )
    feed = {"entries": [entry]}
    feed_path = os.path.join(dist_base, "patchlog_latest_load_screens.json")
    _write_json(feed_path, feed)
    a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])
    print(
        f"[patchlog] patchlog_latest_load_screens.json: "
        f"current={entry['current']}  added={a}  removed={r}  changed={c}"
    )


if __name__ == "__main__":
    main()
