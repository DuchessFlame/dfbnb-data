"""
build_weak_spot_multipliers_json.py
-----------------------------------
Generates dist/calculators/weak_spot_multipliers.json from the BPTD TSV export.

1.  Reads the full BPTD TSV (exported by the xEdit script).
2.  Uses the RefBy_RACE_Names column (pipe-separated creature names from
    Referenced By RACE records) as the primary source for enemy display names.
3.  Falls back to the EDID_TO_NAMES mapping when RefBy_RACE_Names is empty
    or when an EDID needs a custom display name (e.g. "Human Enemies"
    instead of the RACE name, or one EDID mapping to multiple entries).
4.  Filters out internal / non-targetable body parts (Root, COM, Camera, etc.).
5.  Deduplicates parts that share the same display name within an enemy.
6.  Merges in any manually curated entries from the existing JSON that have
    no matching EDID in the TSV (so hand-entered enemies are preserved until
    their BPTDs are exported).

Usage:
  python src/build_weak_spot_multipliers_json.py
"""

import json
import os
import csv
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV_PATH = os.path.join(ROOT, "tsv", "BPTD_Export_May_2026.tsv")
OUT_PATH = os.path.join(ROOT, "dist", "calculators", "weak_spot_multipliers.json")


# ---------------------------------------------------------------------------
# EDID → Display Name(s) override mapping
# ---------------------------------------------------------------------------
# This dict overrides the RefBy_RACE_Names from the TSV.  Use it when:
#   - The RACE FULL name is ugly or too technical (e.g. "HumanRace" → "Human Enemies")
#   - One EDID should appear under multiple display names (e.g. Dog + Wolf)
#   - A creature needs a custom label (e.g. "Encrypted Assaultron" variant)
#
# If an EDID is listed here, ONLY these names are used (RefBy is ignored).
# If an EDID is NOT here, the script uses the first RefBy_RACE_Names value.
# If an EDID is NOT here AND has no RefBy_RACE_Names, it is SKIPPED.
# ---------------------------------------------------------------------------

EDID_TO_NAMES = {
    # Overrides where RACE name isn't player-friendly or needs splitting
    "HumanBodyPartData":                ["Human Enemies"],
    "ScorchedBodyPartData":             ["Scorched"],
    "ViciousDogBodyPartData":           ["Dog", "Wolf"],
    "FeralGhoulBodyPartData":           ["Feral Ghoul", "Feral Ghoul Glowing One"],
    "AssaultronBodyPartData":           ["Assaultron", "Encrypted Assaultron"],
    "GorillaBodyPartData":              ["Mega Sloth"],
    "FEVHoundBodyPartData":             ["Mutant Hound"],
    "PowerArmorBodyPartData":           ["Power Armor Enemies"],
    "OwlBodyPartData":                  ["Owlet"],
    "RadSquirrelBodyPartData":          ["Squirrel"],
    "TurretTripodBodyPartData":         ["Turret Tripod", "Turret Workshop"],
    "DLC01_TurretBubbleBodyPartData":   ["Turret Defender"],
    "MrHandyCreateABotBodyPartData":    ["Mr Handy"],  # merges into same entry as MrHandyBodyPartData

    # Skip these — internal/companion records, not enemies
    "DefaultBodyPartData":              [],
    "DogmeatBodyPartData":              [],
}

# EDIDs to completely skip (no output at all)
SKIP_EDIDS = {edid for edid, names in EDID_TO_NAMES.items() if not names}


# ---------------------------------------------------------------------------
# Part-name overrides per EDID
# ---------------------------------------------------------------------------
# Some BPTN names in the TSV are internal/ugly. This dict lets you remap
# specific part names for specific EDIDs before they hit the output.
# Format:  EDID → { "TSV Part Name": "Display Name" }
# ---------------------------------------------------------------------------

PART_NAME_OVERRIDES = {
    "VertibirdBodyPartData": {
        "LeftWing": "Left Wing",
        "RightWing": "Right Wing",
    },
    "FeralGhoulBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "DeathclawBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "MoleMinerBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "WendigoBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "GorillaBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "SupermutantBehemothBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "MirelurkKingBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "DLC03_AnglerBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "RadSquirrelBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "OwlBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "FoxBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "GraftonMonsterBodyPartData": {
        "LeftFoot": "Left Foot",
        "RightFoot": "Right Foot",
    },
    "PowerArmorBodyPartData": {
        "LFoot": "Left Foot",
        "RFoot": "Right Foot",
    },
    "ScorchedBodyPartData": {
        "RaiderLeftFoot": "Left Foot",
        "RaiderRightFoot": "Right Foot",
    },
    "HumanBodyPartData": {
        "RaiderLeftFoot": "Left Foot",
        "RaiderRightFoot": "Right Foot",
    },
    "StingwingBodyPartData": {
        "Left Wings": "Left Wing",
        "Right Wings": "Right Wing",
    },
    "DLC03_FogCrawlerPartData": {
        "Left Foot": "Left Legs",
        "Right Foot": "Right Legs",
    },
    "HoneyBeastBodyPartData": {
        "Left foot": "Left Foot",
    },
    "MirelurkQueenBodyPartData": {
        "Left Foot": "Left Legs",
        "Right Foot": "Right Legs",
    },
    "mirelurkHunterBodyPartData": {
        "Left foot": "Left Foot",
    },
}


# ---------------------------------------------------------------------------
# Encrypted Assaultron overrides — different multipliers to the base Assaultron
# ---------------------------------------------------------------------------

ENCRYPTED_ASSAULTRON_OVERRIDES = {
    "Head":              0.33,
    "Combat Inhibitor":  1.00,
}


# ---------------------------------------------------------------------------
# Internal / non-targetable part types to filter out
# ---------------------------------------------------------------------------

SKIP_PART_TYPES = {
    "Root", "COM", "Camera", "Eye", "Weapon", "Pelvis",
    "LookAt", "Face Target Source",
}

SKIP_PART_NAMES_EXACT = {
    "Root", "Headtracking", "Camera", "Weapon",
    "FaceTargetSource", "QuestTargetNode", "LookAt",
}

# Creature-prefixed Root/COM names (e.g. "DogmeatRoot", "DeathclawCOM")
SKIP_PART_NAME_SUFFIXES = ["Root", "COM"]


def should_skip_part(part_name, part_type):
    """Return True if this part should be excluded from the calculator."""
    if part_type in SKIP_PART_TYPES:
        return True
    if part_name in SKIP_PART_NAMES_EXACT:
        return True
    for suffix in SKIP_PART_NAME_SUFFIXES:
        if part_name.endswith(suffix) and len(part_name) > len(suffix):
            return True
    return False


def should_skip_zero_stats(health_pct, to_hit_chance):
    """Skip parts that are clearly internal (0 health, 0 to-hit)."""
    try:
        h = int(health_pct) if health_pct else 0
        t = int(to_hit_chance) if to_hit_chance else 0
        return h == 0 and t == 0
    except ValueError:
        return False


def get_display_names(edid, refby_race_names):
    """
    Determine the display name(s) for a BPTD record.

    Priority:
      1. EDID_TO_NAMES override (if EDID is in the dict)
      2. RefBy_RACE_Names from the TSV (first RACE name with a FULL value)
      3. Fall back to cleaning up the EDID itself
    """
    # If there's an explicit override, use it (even if empty → skip)
    if edid in EDID_TO_NAMES:
        return EDID_TO_NAMES[edid]

    # Use RefBy_RACE_Names if available
    if refby_race_names:
        # Pipe-separated, pick the first non-empty name
        names = [n.strip() for n in refby_race_names.split("|") if n.strip()]
        if names:
            return [names[0]]  # Use the first RACE's FULL name

    # Last resort: clean up the EDID (strip "BodyPartData" suffix, add spaces)
    fallback = edid.replace("BodyPartData", "").replace("PartData", "")
    # Remove DLC prefixes
    for prefix in ("DLC01_", "DLC02_", "DLC03_", "DLC04_"):
        fallback = fallback.replace(prefix, "")
    if fallback:
        return [fallback]

    return []


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_from_tsv():
    """Parse the BPTD TSV and return {enemy_name: [{limb, multiplier}, ...]}."""
    raw = {}  # name → list of (limb, mult)

    with open(TSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")

        # Check if the new RefBy columns exist
        has_refby = False
        fieldnames = reader.fieldnames or []
        if "RefBy_RACE_Names" in fieldnames:
            has_refby = True

        for row in reader:
            edid = row.get("BPTD_EDID", "").strip()

            # Skip explicitly excluded EDIDs
            if edid in SKIP_EDIDS:
                continue

            part_name = row.get("BPTN_PartName", "").strip()
            part_type = row.get("PartType", "").strip()
            health_pct = row.get("HealthPercent", "").strip()
            to_hit = row.get("ToHitChance", "").strip()

            if not part_name:
                continue

            # Get display names — from override map or RefBy_RACE_Names
            refby_names = row.get("RefBy_RACE_Names", "").strip() if has_refby else ""
            display_names = get_display_names(edid, refby_names)
            if not display_names:
                continue

            # Apply part-name overrides
            overrides = PART_NAME_OVERRIDES.get(edid, {})
            if part_name in overrides:
                part_name = overrides[part_name]

            # Skip internal parts
            if should_skip_part(part_name, part_type):
                continue
            if should_skip_zero_stats(health_pct, to_hit):
                continue

            try:
                mult = round(float(row.get("DamageMult", "1.000000").strip()), 2)
            except ValueError:
                mult = 1.00

            for name in display_names:
                if name not in raw:
                    raw[name] = []
                raw[name].append((part_name, mult))

    # Apply Encrypted Assaultron overrides
    if "Encrypted Assaultron" in raw:
        updated = []
        for limb, mult in raw["Encrypted Assaultron"]:
            if limb in ENCRYPTED_ASSAULTRON_OVERRIDES:
                mult = ENCRYPTED_ASSAULTRON_OVERRIDES[limb]
            updated.append((limb, mult))
        raw["Encrypted Assaultron"] = updated

    # Deduplicate: keep first occurrence of each limb name per enemy
    enemies = {}
    for name, parts in raw.items():
        seen = set()
        deduped = []
        for limb, mult in parts:
            if limb not in seen:
                seen.add(limb)
                deduped.append({"limb": limb, "multiplier": mult})
        # Sort: highest multiplier first, then alphabetical
        deduped.sort(key=lambda p: (-p["multiplier"], p["limb"]))
        enemies[name] = deduped

    return enemies


def merge_manual_entries(tsv_enemies):
    """
    Load the existing JSON and preserve any enemies that have no match
    in the TSV (i.e. hand-curated entries like raid bosses, DLC creatures
    not yet exported, etc.).
    """
    if not os.path.isfile(OUT_PATH):
        return tsv_enemies

    with open(OUT_PATH, "r", encoding="utf-8") as f:
        existing = json.load(f)

    existing_enemies = existing.get("enemies", {})
    tsv_names = set(tsv_enemies.keys())

    merged = dict(tsv_enemies)
    preserved_count = 0
    for name, parts in existing_enemies.items():
        if name not in tsv_names:
            merged[name] = parts
            preserved_count += 1

    if preserved_count:
        print(f"[weak_spot] Preserved {preserved_count} manually curated entries not in TSV")

    return merged


def main():
    if not os.path.isfile(TSV_PATH):
        print(f"[weak_spot] ERROR: No TSV at {TSV_PATH}")
        print("[weak_spot] Run the xEdit BPTD export script first.")
        return

    print(f"[weak_spot] Building from TSV: {TSV_PATH}")
    tsv_enemies = build_from_tsv()
    print(f"[weak_spot] Parsed {len(tsv_enemies)} enemies from TSV")

    # Merge in manually curated entries that don't have a TSV match
    all_enemies = merge_manual_entries(tsv_enemies)

    # Sort alphabetically
    sorted_enemies = dict(sorted(all_enemies.items(), key=lambda kv: kv[0].lower()))

    output = {
        "generated": str(date.today()),
        "source": "BPTD_Export_May_2026.tsv",
        "enemies": sorted_enemies
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[weak_spot] Written {len(sorted_enemies)} enemies -> {OUT_PATH}")


if __name__ == "__main__":
    main()
