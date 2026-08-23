#!/usr/bin/env python3
"""
build_buff_stations_json.py

Reads xEdit TSV exports and builds dist/buff-stations.json for the
DF/BNB Buff Stations page (df-bnb-camp-items.js, page type "buff-stations").

AUTO-DISCOVERY (titles.js pattern): membership is derived from the
buff-furniture KEYWORDS in the game data, so new stations Bethesda adds
appear automatically on the next TSV drop. Manual tables below only hold
overrides (names, obtain text, ENTM matches) and known exclusions.

Groups (root expands), display order:
  SPECIAL spelled out: Strength, Perception, Endurance, Charisma,
  Intelligence, Agility, Luck — then ABC:
  Experience (XP), Unique Buffs, Utility, Well Rested, Well Tuned
Items granting two buffs carry both group keys in buffTypes (automatic —
an item with both the Agility and Perception keywords lands in both).

Inputs (from tsv/ in the repo):
  FURN_Export_*_FURN.tsv      (env: FURN_TSV)
  ACTI_Export_*_ACTI.tsv      (env: ACTI_TSV)
  ENTM_Export_*.tsv           (env: ENTM_TSV)
  KYWD_Export_*_Refs.tsv      (env: KYWD_REFS_TSV)
  PERK_Export_*.tsv           (env: PERK_TSV)
  COBJ_Export_*.tsv           (env: COBJ_TSV)
  fallout76_seasons.tsv

Usage:
  python build_buff_stations_json.py [--tsv-dir tsv] [--out-dir dist]
"""

import argparse
import csv
import glob
import json
import os
import re
from pathlib import Path
import tsv_source          # one resolver for every export selection

parser = argparse.ArgumentParser()
parser.add_argument("--tsv-dir", default="tsv",  help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
args = parser.parse_args()

TSV_DIR = Path(args.tsv_dir)
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "buff-stations.json"


_MONTH_ORD = {"jan": 1, "feb": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
              "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8,
              "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def _tsv_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def _newest_glob(pattern):
    matches = glob.glob(pattern)
    if not matches:
        return None
    return sorted(matches, key=_tsv_date_key)[-1]


def _resolve_tsv(env_var, glob_pattern, fallback_name):
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


FURN_PATH      = _resolve_tsv("FURN_TSV",      "FURN_Export_*_FURN.tsv", "FURN_Export_FURN.tsv")
ACTI_PATH      = _resolve_tsv("ACTI_TSV",      "ACTI_Export_*_ACTI.tsv", "ACTI_Export_ACTI.tsv")
ENTM_PATH      = _resolve_tsv("ENTM_TSV",      "ENTM_Export_*.tsv",      "ENTM_Export.tsv")
KYWD_REFS_PATH = _resolve_tsv("KYWD_REFS_TSV", "KYWD_Export_*_Refs.tsv", "KYWD_Export_Refs.tsv")
PERK_PATH      = _resolve_tsv("PERK_TSV",      "PERK_Export_*.tsv",      "PERK_Export.tsv")
COBJ_PATH      = _resolve_tsv("COBJ_TSV",      "COBJ_Export_*.tsv",      "COBJ_Export.tsv")
SEASONS_PATH   = TSV_DIR / "fallout76_seasons.tsv"
try:
    LVLI_ENTRIES_PATH = _resolve_tsv("LVLI_ENTRIES_TSV", "*LVLI*Entries*.tsv", "LVLI_Entries.tsv")
except FileNotFoundError:
    LVLI_ENTRIES_PATH = None

print("Loading TSVs…")
for _n, _p in [("FURN", FURN_PATH), ("ACTI", ACTI_PATH), ("ENTM", ENTM_PATH),
               ("KYWD Refs", KYWD_REFS_PATH), ("PERK", PERK_PATH), ("COBJ", COBJ_PATH)]:
    print(f"  {_n}: {_p}")

IMG_BASE = "/wp-content/uploads/guide-images/camp-items/buff-stations/"
ATX_HOW  = "Can be purchased with certain bundles from the Atom Shop."


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


# ---------------------------------------------------------------- TSV loads
FURN = {}
for r in rows(FURN_PATH):
    FURN[r["FURN_FormID"]] = r

ACTI = {}
for r in rows(ACTI_PATH):
    ACTI[r["ACTI_FormID"]] = {"FURN_EDID": r["ACTI_EDID"],
                              "FURN_FULL": r["ACTI_FULL"],
                              "XALG_Flags": r.get("XALG_Flags", "")}

ENTM_BY_EDID = {}
ENTM_BY_FULL = {}
for r in rows(ENTM_PATH):
    ENTM_BY_EDID[r["EDID"]] = r
    full = (r["FULL"] or "").strip().lower()
    if full:
        ENTM_BY_FULL.setdefault(full, []).append(r)

SEASON_NAMES = {}
if SEASONS_PATH.exists():
    for r in rows(SEASONS_PATH):
        if r["SeasonNumber"].strip().isdigit():
            SEASON_NAMES[int(r["SeasonNumber"])] = r["SeasonName"].strip()

# Homebody perk (PERK 00393F6E Homebody01) — stats verified June 2026:
# SPEL 00393F6F AbPerkHomebody = Heal Rate +2, Limb Regeneration +200;
# SURV_WellRested2 carries 7200s/10800s paired rows (2h base, 3h with perk).
HOMEBODY_LINE = ("Homebody Perk: while in your C.A.M.P. or workshop — "
                 "Heal Rate +2 and Limb Regeneration +200. "
                 "Also extends the Comfy Bed Well Rested buff from 2 hours to 3 hours.")

# COBJ — crafting components + plan names, matched by created-object FormID.
# PLAN_NAME_BY_NORMEDID lets a grouped/proxy component recipe (FVPA present,
# GNAM_FULL empty) borrow the plan FULL name from its sibling CondProxy row,
# which carries the plan name but no components. Both rows normalise to the same
# EDID once the CondProxy_/ATX_/_b suffixes are stripped.
COBJ_BY_CNAM = {}
ALL_COBJ_ROWS = []
PLAN_NAME_BY_NORMEDID = {}


def _norm_cobj_edid(edid):
    e = (edid or "").lower()
    e = re.sub(r"^(atx_|z+_|f1_|score_s\d+_|score_)", "", e)
    e = e.replace("condproxy_", "")
    e = re.sub(r"_(goldvendor|a|b|c)$", "", e)
    return e


for r in rows(COBJ_PATH):
    ALL_COBJ_ROWS.append(r)
    cn = r.get("CNAM_FormID", "").strip()
    if cn:
        COBJ_BY_CNAM.setdefault(cn, []).append(r)
    _gf = (r.get("GNAM_FULL") or "").strip().strip('"')
    if _gf:
        _ne = _norm_cobj_edid(r.get("COBJ_EDID", ""))
        if _ne:
            PLAN_NAME_BY_NORMEDID.setdefault(_ne, _gf)


def plan_name_for_row(c):
    """Plan FULL name for a COBJ row: its own GNAM_FULL, else a sibling's
    (same normalised EDID) plan name. Empty string if none resolves."""
    plan = (c.get("GNAM_FULL") or "").strip().strip('"')
    if plan:
        return plan
    return PLAN_NAME_BY_NORMEDID.get(_norm_cobj_edid(c.get("COBJ_EDID", "")), "")


# Instruments (and other grouped CAMP objects) are crafted from a recipe whose
# CNAM is a leveled list; each individual FURN is an ENTRY of that list. So the
# component recipe for, e.g., Steel Guitar is found by: FURN FormID -> the LVLI
# it belongs to -> the COBJ whose CNAM == that LVLI -> its FVPA.
# LVLI_BY_MEMBER maps each member FormID to the LVLI FormIDs it appears in.
LVLI_BY_MEMBER = {}
if LVLI_ENTRIES_PATH:
    for r in rows(LVLI_ENTRIES_PATH):
        lvli = (r.get("LVLI_FormID") or "").strip().upper()
        # Only index workshop build leveled lists, so membership can't pull a
        # recipe from an unrelated list (loot tables, quest rewards, etc.).
        if "workshop" not in (r.get("LVLI_EDID") or "").lower():
            continue
        ref = (r.get("LVLO_Reference") or "").strip()
        member = ref.split(":")[0].strip().upper() if ref else ""
        if lvli and member:
            LVLI_BY_MEMBER.setdefault(member, []).append(lvli)


def crafting_via_lvli_membership(fid):
    """For a FURN that is an entry of a workshop leveled-list recipe (e.g.
    instruments), return (fvpa_str, plan_name) from the COBJ whose CNAM is that
    LVLI. ('', '') if the FURN isn't a grouped-recipe member."""
    for lvli in LVLI_BY_MEMBER.get((fid or "").upper(), []):
        for c in COBJ_BY_CNAM.get(lvli, []):
            fv = (c.get("FVPA") or "").strip()
            if fv:
                return fv, plan_name_for_row(c)
    return "", ""


def _refby_lvli_fids(c):
    """FormIDs of the LVLI leveled lists that reference this COBJ row, parsed
    from ReferencedBy_Flat + Ref_1..Ref_37. Entry format
    '<FID>:<EDID>:<TYPE>'; we keep the FIDs whose TYPE is LVLI."""
    fids = []
    blobs = [c.get("ReferencedBy_Flat", "")] + [c.get(f"Ref_{i}", "") for i in range(1, 38)]
    for blob in blobs:
        for piece in (blob or "").split("|"):
            bits = piece.split(":")
            if len(bits) >= 3 and bits[2].strip() == "LVLI" and bits[0].strip():
                fids.append(bits[0].strip())
    return fids


def _components_via_lvli(c):
    """Grouped workshop items (e.g. exercise equipment, containers) place their
    components on a shared recipe whose CNAM is a leveled list, while the
    per-item CondProxy row carries only the plan name. When a matched COBJ row
    has no FVPA of its own, follow its ReferencedBy LVLI(s) to that shared
    recipe (COBJ whose CNAM == the LVLI FormID) and borrow its FVPA. Returns the
    FVPA string, or '' if nothing resolves."""
    for fid in _refby_lvli_fids(c):
        for sib in COBJ_BY_CNAM.get(fid, []):
            fv = (sib.get("FVPA") or "").strip()
            if fv:
                return fv
    return ""


def _row_fvpa(c):
    """FVPA for a COBJ row: its own, else borrowed from the shared LVLI recipe."""
    fv = (c.get("FVPA") or "").strip()
    return fv if fv else _components_via_lvli(c)


def _parse_fvpa_qty(raw):
    """Failsafe for curve-table-driven component counts. Returns (qty, scaled):
    a positive int when the count is fixed, or (None, True) when it's
    curve-driven (exported as 0 / blank / non-numeric). Camp recipes use plain
    integers today, but some armour/weapon mod counts are curve-scaled and this
    keeps a stray one from rendering as a misleading ×0."""
    try:
        n = int(str(raw).strip())
    except (ValueError, TypeError):
        return None, True
    return (n, False) if n > 0 else (None, True)


def fvpa_to_text(fvpa):
    parts = []
    for chunk in (fvpa or "").split("|"):
        bits = chunk.split(":")
        if len(bits) >= 2 and bits[0].strip():
            name = bits[0].strip()
            name = re.sub(r"^c_", "", name)
            name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
            qty, scaled = _parse_fvpa_qty(bits[1])
            parts.append(f"{name} ×(varies)" if scaled else f"{name} ×{qty}")
    return "\n".join(parts)


def fvpa_to_array(fvpa):
    """Parse COBJ FVPA string to [{"name": "...", "qty": N}, ...].
    Curve-driven counts emit qty=None + scaled=True (failsafe)."""
    result = []
    for chunk in (fvpa or "").split("|"):
        bits = chunk.strip().split(":")
        if len(bits) >= 2 and bits[0].strip():
            name = bits[0].strip()
            name = re.sub(r"^c_", "", name)
            name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
            qty, scaled = _parse_fvpa_qty(bits[1])
            if scaled:
                result.append({"name": name, "qty": None, "scaled": True})
            else:
                result.append({"name": name, "qty": qty})
    return result


# ---------------------------------------------------------------- groups
# No blurbs on group headers (user requirement, 7 Jun 2026) — the group
# expand shows the label + count only.
GROUPS = [
    {"key": "strength",     "label": "Strength"},
    {"key": "perception",   "label": "Perception"},
    {"key": "endurance",    "label": "Endurance"},
    {"key": "charisma",     "label": "Charisma"},
    {"key": "intelligence", "label": "Intelligence"},
    {"key": "agility",      "label": "Agility"},
    {"key": "luck",         "label": "Luck"},
    {"key": "experience",   "label": "Experience (XP)"},
    {"key": "unique",       "label": "Unique Buffs"},
    {"key": "utility",      "label": "Utility"},
    {"key": "wellrested",   "label": "Well Rested"},
    {"key": "welltuned",    "label": "Well Tuned"},
]
GROUP_ORDER = {g["key"]: i for i, g in enumerate(GROUPS)}

STAT_NAME = {"strength": "Strength", "perception": "Perception",
             "endurance": "Endurance", "charisma": "Charisma",
             "intelligence": "Intelligence", "agility": "Agility", "luck": "Luck"}

SPECIAL_KW = {
    "strength":     ("005B359F ATX_FurnituretypeStrength",     "005B519D ATX_BuffStrength"),
    "perception":   ("0065015D ATX_FurnituretypePerception",   "00650156 ATX_BuffPerception"),
    "endurance":    ("00644EA3 ATX_FurnituretypeEndurance",    "00644EA7 ATX_BuffEndurance"),
    "charisma":     ("0065015A ATX_FurnituretypeCharisma",     "0061F6AC ATX_BuffCharisma"),
    "intelligence": ("0065015B ATX_FurnituretypeIntelligence", "00650155 ATX_BuffIntelligence"),
    "agility":      ("005EDEE0 ATX_FurnituretypeAgility",      "005EDEE5 ATX_BuffAgility"),
    "luck":         ("0065015C ATX_FurnituretypeLuck",         "0060497D ATX_BuffLuck"),
}

DUR_NOTE = ("Duration Global: ATX_SPECIAL_BuffDurationGlobal (0065015E) = 1800s\n"
            "Magnitude Global: ATX_SPECIAL_BuffMagnitudeGlobal (0065015F) = +2")

WT_TECH = ["Instrument Keyword: 0050CD11 FurnitureTypeInstrument",
           "Buff Spell: 0050CD15 SURV_WellTunedSpell “Well Tuned”",
           "Buff Effect: 0050CD14 SURV_WellTunedEffect — +25 AP regen, 3600s"]
WT_OUT = ("Well Tuned: +25 Action Point regeneration for 60 minutes.\n"
          "Solo buff — applies to the player only.")

RESTED_OUT = "Rested: +5% XP for 60 minutes — solo buff (player only)."

# ================================================================ AUTO-DISCOVERY
# Buff-furniture keyword → page group(s). An item carrying two of these
# keywords (e.g. a pinball machine with Agility + Perception) automatically
# lands in BOTH groups — no manual dual-stat list needed.
KW_TO_GROUPS = {
    "005B359F": ["strength"],
    "0065015D": ["perception"],
    "00644EA3": ["endurance"],
    "0065015A": ["charisma"],
    "0065015B": ["intelligence"],
    "005EDEE0": ["agility"],
    "0065015C": ["luck"],
    "0050CD11": ["welltuned"],                  # FurnitureTypeInstrument
    "005A4E2B": ["wellrested", "experience"],   # ATX_FurnituretypeRested (Rested = +5% XP)
    "0076B52B": ["experience"],                 # FurnitureTypeXPBonus
    "0089ADB2": ["unique"],                     # FurnituretypeAccuracy (Phoropter)
    "00897407": ["unique"],                     # LoveHurts ChemDuration (Lethal Loveseat)
    "008B1D60": ["unique"],                     # WeaponsExpert GatheringBuff (Rip statue)
    "005D98A6": ["utility"],                    # FurnitureTypeDiseaseCure (Sympto-matic)
}

# Records that carry a buff keyword but must NOT be on the page.
# World-placed objects (REFR only, no buildable COBJ) and quest/companion props.
EXCLUDED = {
    "006B8E0F",  # Casino Pool Table — world object, Expedition: Atlantic City
    "0078CA7D",  # Pool Table — world object, Storm questline
    "00692563",  # Communal Campfire — event-placed, crafting recipe is cut
    "006D1DD7",  # Arm Wrestle Machine — world object, EXP17
    "006B8E0E",  # Drum Set — world object, EXP17
    "00684B85",  # Moon_CampfireGuitar — event-placed world guitar
    "00769104",  # AC_MQ01 Tuba — quest prop, EDID says NONPLAYER
    "005856C7",  # Wanderer's Guitar — companion camp object, not buildable
    "0061E075",  # ATX_COMP_Mechanic guitar — companion camp object
    "0052A8F9",  # CharGen guitar
    "00830006",  # Ice Bath — ZZZ deprecated (S22)
}

CUT_RE = re.compile(r"^(zzz|test|chargen|post_)", re.I)

# Items that grant a buff but carry NO detectable buff keyword — added manually.
MANUAL_ITEMS = [
    {"fid": "007B2841", "groups": ["utility"],
     "entm": "ATX_ENTM_CAMP_FloorDecor_BloodTransfusionPump", "how": None,
     "buff": "Restores health and removes rads when used.\n"
             "Ghoul characters gain rads instead.\n15 minute cooldown between uses."},
    {"fid": "008B1D5A", "groups": ["unique"],
     "entm": "ATX_ENTM_CAMP_Utility_Sharpening", "how": None,
     "buff": "Sharpening Stone: 5% chance to gain a Blood Pack on melee kills for 30 minutes.",
     "spell": "008D1C9C “Sharpening Stone”"},
    {"fid": "0068D3D5", "groups": ["experience"],
     "entm": None,
     "how": "Placed and used by Lite Ally: Steven Scarberry (Season 12 Scoreboard) — "
            "the ally applies the buff to your team.",
     "name": "Scarberry’s Shrine",
     "buff": "+5% XP for 60 minutes — team buff, applied by your companion."},
]

# Gold-vendor re-releases of scoreboard items are the SAME item with a second
# purchase route — they merge into the base entry instead of getting their own
# sub-expand (user requirement, 7 Jun 2026). The base item's How to Obtain
# lists the scoreboard first, then the gold bullion route with vendor name,
# reputation rank needed for it to appear in their inventory, and cost.
# Source: BOOK VendorList LVLI (W05_LLV_GoldVendor_*) + Econ_GoldVendor_Tier
# GLOBs. Same item with a DIFFERENT SKIN stays a separate sub-expand
# (beds/sleeping bags excepted — they stay aggregated).
GOLD_MERGED = {
    "005F56CA": "0056744C",   # Weight Bench (Gold Vendor)      → Weight Bench
    "00609E0A": "005D80E0",   # Antique Speed Bag (Gold Vendor) → Antique Speed Bag
}

# Gold bullion line appended to the base item's How to Obtain.
GOLD_HOW = {
    "0056744C": "Gold Bullion: Plan: Weight Bench — sold by Mortimer at The Crater "
                "for 1,250 gold bullion. Requires Raiders reputation rank Cautious "
                "to appear in his inventory. Available once per character.",
    "005D80E0": "Gold Bullion: Plan: Speed Bag — sold by Mortimer at The Crater "
                "for 1,250 gold bullion. Requires Raiders reputation rank Cautious "
                "to appear in his inventory. Available once per character.",
}

# Correct plan names for the merged items (the auto COBJ token-fallback
# previously mismatched these to an unrelated GoldVendor recipe).
PLAN_OVERRIDES = {
    "0056744C": "Plan: Weight Bench",
    "005D80E0": "Plan: Speed Bag",
}

# Extra Technical lines for merged gold-vendor records
GOLD_TECH = {
    "0056744C": ["Gold Vendor FURN: SCORE_S2_Furniture_Weightbench_GoldVendor (005F56CA)",
                 "Gold Vendor Plan: SCORE_Recipe_workshop_CAMP_Utility_WeightBench_GoldVendor (005F56C8)",
                 "Vendor List: 005A0EE5 W05_LLV_GoldVendor_Raider_Mortimer_1_Cautious",
                 "Price Global: 005A504D Econ_GoldVendor_Tier_10 = 1250"],
    "005D80E0": ["Gold Vendor FURN: SCORE_S3_Antique_Speed_Bag_GoldVendor (00609E0A)",
                 "Gold Vendor Plan: SCORE_Recipe_workshop_CAMP_Utility_SpeedBag_GoldVendor (00609E06)",
                 "Vendor List: 005A0EE5 W05_LLV_GoldVendor_Raider_Mortimer_1_Cautious",
                 "Price Global: 005A504D Econ_GoldVendor_Tier_10 = 1250"],
}

# ---------------------------------------------------------------- overrides
NAME_OVERRIDES = {
    "00668025": "Nuka-lele (Quantum)",
    "0062C3E2": "Pipe Organ (Atom Shop)",
    "00678D16": "Resonator Guitar B",
    "007AE386": "Accordion (Free)",
    "00691B73": "Violin",
    "00684BAA": "Vault Girl Arm Wrestle Machine",
    "007267B6": "Hollywood Vanity (Free)",
    "006FA9F0": "Stars and Strikes Bowling Arcade Machine",
}

HOW_OVERRIDES = {
    "007CF731": "Plan: Cosmic Capture — rare reward drop from the Invaders from Beyond seasonal event.",
    "008B12E0": "Purchase with tickets from the Weapons Expert Mini Season (2026) reward shop.",
    "008B1553": "Purchase with tickets from the Weapons Expert Mini Season (2026) reward shop.",
    "00897409": "Purchase with tickets from the Love Hurts Mini Season reward shop.",
    "00766AA3": "Plan: Stargazer's Telescope — sold by the Milepost Zero vendors (imported goods).",
    "0079B885": "Plan: Cauldron Hot Tub — event-specific reward during the Mischief Night event.",
    "0078C78E": "Plan: Rusty Saxophone — Milepost Zero reward.",
    "00755EB1": "Reward from the Mothman Equinox seasonal event. Not craftable.",
    "007AE386": "Free reward — unlocked for all players.",
    "007267B6": "Free reward — unlocked for all players.",
    "005D98A5": ATX_HOW,   # Sympto-matic — Atom Shop item with unbranded EDID
    "006628D8": "Available to Fallout 1st members.",
}

# Free-from-ATX unlocks have no tradeable plan; Milepost saxophone plan is
# untradeable per the published data set.
TRADEABLE_OVERRIDES = {
    "007AE386": False,  # Accordion (Free)
    "007267B6": False,  # Hollywood Vanity (Free)
    "0078C78E": False,  # Rusty Saxophone
}

# ENTM matches the FULL-name auto-lookup can't resolve (shared or differing names)
ENTM_OVERRIDES = {
    "0056744C": "SCORE_S2_ENTM_Utility_WeightBench",
    "005D80E0": "SCORE_S3_ENTM_CAMP_FloorDecor_ExerciseEquipment_AntiqueSpeedBag",
    "006628D9": "ATX_ENTM_CAMP_Furniture_Table_Special_9BallTable_D_WoodGreen",
    "006628DB": "ATX_ENTM_CAMP_Furniture_Table_Special_9BallTable_C_Green",
    "00684BAA": "ATX_ENTM_CAMP_Utility_ArmWrestleMachine_VaultGirl",
    "0068DF0A": "ATX_ENTM_CAMP_Furniture_SkullsFirePit",
    "00691B73": "SCORE_S12_ENTM_CAMP_Furniture_Instrument_ViolinChair",
    "0062C3E2": "ATX_ENTM_CAMP_Furniture_Instrument_Pipe_Organ",
    "00678D16": "ATX_ENTM_CAMP_Furniture_Instrument_ResonatorGuitar",
    "006FA9F0": "ATX_ENTM_CAMP_Furniture_BowlingArcadeMachine_StarsAndStrikes",
    "007AE386": "ATX_ENTM_CAMP_Furniture_Instrument_Accordion",  # free version shares the ATX art
}

# Custom Output text for non-SPECIAL, non-instrument, non-rested items
BUFF_TEXT = {
    "0089ADB0": "Accuracy Boost: +25% V.A.T.S. Accuracy for 2 hours.",
    "00897409": "Rush of Love: +25% Chem Duration for 60 minutes.",
    "008B1553": "Rip’s Bounty: cryptids may drop extra scraps for 60 minutes.",
    "005D98A5": "Cures all diseases when used.",
    "00755EB1": "+5% XP for 60 minutes — solo buff (player only).",
}

SPELL_NOTES = {
    "0089ADB0": "0089ADB5 “Accuracy Boost”",
    "00897409": "00897408 “Rush of Love”",
    "008B1553": "008B1D5E “Rip’s Bounty”",
}
KW_NOTES = {
    "0089ADB0": "0089ADB2 FurnituretypeAccuracy",
    "00897409": "00897407 LoveHurts_FurnituretypeChemDuration",
    "005D98A5": "005D98A6 FurnitureTypeDiseaseCure",
    "008B1553": "008B1D60 WeaponsExpert_StatueTypeItem_GatheringBuff",
    "00755EB1": "0076B52B FurnitureTypeXPBonus",
}

# ---------------------------------------------------------------- discovery
discovered = {}  # fid -> set(groups)
for r in rows(KYWD_REFS_PATH):
    k = r["KeywordFormID"]
    if k not in KW_TO_GROUPS:
        continue
    fid = r["RefFormID"]
    if fid in EXCLUDED or fid in GOLD_MERGED:
        continue
    rec = FURN.get(fid) or ACTI.get(fid)
    if not rec:
        continue
    edid = rec["FURN_EDID"]
    if CUT_RE.match(edid) or "nonplayer" in edid.lower():
        continue
    discovered.setdefault(fid, set()).update(KW_TO_GROUPS[k])

for spec in MANUAL_ITEMS:
    discovered.setdefault(spec["fid"], set()).update(spec["groups"])
MANUAL_BY_FID = {m["fid"]: m for m in MANUAL_ITEMS}

# Bed name lists (for the three aggregate bed entries)
BED_KW = {"003CD038": "sleepingbag", "003CD037": "mattress", "003CD036": "comfy"}
BED_CUT_RE = re.compile(r"^(zzz|test|chargen|post_|npc|workshopnpc)", re.I)
bed_names = {"sleepingbag": [], "mattress": [], "comfy": []}
for r in rows(KYWD_REFS_PATH):
    k = r["KeywordFormID"]
    if k in BED_KW:
        fr = FURN.get(r["RefFormID"])
        if not fr:
            continue
        edid, full = fr["FURN_EDID"], (fr["FURN_FULL"] or "").strip()
        if not full or BED_CUT_RE.match(edid):
            continue
        lst = bed_names[BED_KW[k]]
        if full not in lst:
            lst.append(full)
for v in bed_names.values():
    v.sort()

# ---------------------------------------------------------------- helpers
def clean_desc(desc):
    s = (desc or "").strip()
    s = re.sub(r"\s*-\s+[A-Z0-9’'.,&%/!:()\- ]{12,}\s+-\s*", " ", s)
    return re.sub(r"\s{2,}", " ", s).strip()


def season_from_edid(*edids):
    for e in edids:
        m = re.search(r"SCORE_S(\d+)_", e or "", re.I)
        if m:
            return int(m.group(1))
    return None


def score_how(season):
    name = SEASON_NAMES.get(season, "")
    # Season 16 (Duel with the Devil) cutoff: S1-S15 = claim-as-you-rank,
    # S16+ = ticket/Season Pass system.
    verb = "Claim from" if (season or 0) <= 15 else "Purchase with tickets from"
    if name:
        # Drop the leading "The " of the theme so we emit a single lowercase
        # article ("...the Big Score Scoreboard"); a mid-name "The" is kept.
        if name.lower().startswith("the "):
            name = name[4:]
        return f"{verb} the {name} Scoreboard (Season {season})"
    return f"{verb} the Season {season} Scoreboard"


# ---------------------------------------------------------------------------
# OBTAIN ROUTES (camp-item-expands spec — fixed 9-route How to Obtain list,
# plus buff-station extra routes for sources outside the standard taxonomy:
# base-game craftables, Fallout 1st, free unlocks, aggregated beds).
# ---------------------------------------------------------------------------
OBTAIN_ROUTE_ORDER = [
    "Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
    "Limited Time Bundle", "Events & Activities", "Quests", "Challenges",
]
# Extra rows appended after the 9 standard routes, only when populated.
EXTRA_ROUTE_ORDER = ["Base Game", "Fallout 1st", "Free Unlock",
                     "Various Sources", "Other"]


def _route_entry(name, lines, tradeable, drop_rate):
    lines = [ln for ln in lines if str(ln).strip()]
    return {
        "route":     name,
        "populated": bool(lines),
        "lines":     lines,
        "tradeable": tradeable if lines else None,
        "dropRate":  (drop_rate if lines else None),
    }


def classify_segment(seg):
    """Map one How-to-Obtain text segment to a route name + cleaned text."""
    s = seg.strip()
    low = s.lower()
    if low.startswith("gold bullion:"):
        return "Gold Bullion", s.split(":", 1)[1].strip()
    # Multi-source aggregate (beds) — checked before Scoreboard because the
    # text itself names "...Scoreboard bed plans all qualify".
    if low.startswith("various") or "all qualify" in low:
        return "Various Sources", s
    if (low.startswith(("claim from", "purchase with tickets"))
            or "scoreboard" in low or "mini season" in low):
        return "Scoreboard", s
    if s == ATX_HOW or "atom shop" in low:
        return "Atom Shop", s
    if low.startswith("base game"):
        return "Base Game", s
    if low.startswith("free reward") or "unlocked for all" in low:
        return "Free Unlock", s
    if "fallout 1st" in low:
        return "Fallout 1st", s
    if ("milepost" in low or "seasonal event" in low or "event-specific" in low
            or "reward drop from" in low or "equinox" in low
            or "mischief night" in low):
        return "Events & Activities", s
    return "Other", s


def buff_obtain_routes(how, tradeable):
    """Build the 9 standard routes (N/A where empty) plus any populated extra
    routes, by classifying each segment of the resolved How-to-Obtain text."""
    segs = [seg for seg in re.split(r"\n\n+", how or "") if seg.strip()]
    std, extra = {}, {}
    for seg in segs:
        route, text = classify_segment(seg)
        lines = [ln for ln in text.split("\n") if ln.strip()]
        bucket = std if route in OBTAIN_ROUTE_ORDER else extra
        if route in bucket:
            bucket[route] = (bucket[route][0] + lines, tradeable, "N/A")
        else:
            bucket[route] = (lines, tradeable, "N/A")

    routes = []
    for name in OBTAIN_ROUTE_ORDER:
        if name in std:
            lines, trad, drop = std[name]
            routes.append(_route_entry(name, lines, trad, drop))
        else:
            routes.append(_route_entry(name, [], None, None))
    for name in EXTRA_ROUTE_ORDER:
        if name in extra:
            lines, trad, drop = extra[name]
            entry = _route_entry(name, lines, trad, drop)
            if entry["populated"]:
                routes.append(entry)
    return routes


def entm_lookup(fid, full_name, furn_edid=""):
    spec = ENTM_OVERRIDES.get(fid)
    if spec:
        return ENTM_BY_EDID.get(spec)
    cands = [r for r in ENTM_BY_FULL.get((full_name or "").lower(), [])
             if not r["EDID"].upper().startswith(("ZZZ", "POST_", "DEL_", "CUT_"))]
    if not cands:
        return None
    # Guard: a BASE-GAME record (unbranded EDID) must not adopt a paid
    # variant's ENTM just because the display names collide (e.g. base
    # Pipe Organ vs the Atom Shop Pipe Organ). FROMATX records are the
    # exception — they're free copies of the ATX item and share its art.
    e = (furn_edid or "").upper()
    fr = FURN.get(fid) or ACTI.get(fid) or {}
    is_premium = "Premium" in (fr.get("XALG_Flags") or "")
    branded = (e.startswith(("ATX_", "SCORE_", "MILE_", "F1_"))
               or "FROMATX" in e or is_premium)
    if not branded and cands[0]["EDID"].upper().startswith(("ATX_", "SCORE_")):
        return None
    return cands[0]


def image_for(entm, furn_edid):
    if entm:
        tex = (entm.get("ETDI") or "").strip()
        if tex.lower().endswith(".dds"):
            return IMG_BASE + tex[:-4].lower() + ".avif"
    return IMG_BASE + (furn_edid or "").lower() + ".avif"


def crafting_for(fid, furn_edid):
    for c in COBJ_BY_CNAM.get(fid, []):
        fv = _row_fvpa(c)
        plan = plan_name_for_row(c)
        if fv or plan:
            return fvpa_to_text(fv), plan, fvpa_to_array(fv)
    token = re.sub(r"^(ATX_|SCORE_S\d+_|SCORE_)", "", furn_edid or "").split("_")[-1].lower()
    if len(token) >= 6:
        # Scan every COBJ row (CondProxy rows have no CNAM, so they're not in
        # COBJ_BY_CNAM). Prefer a token-matched row that actually yields
        # components — its own FVPA, or borrowed from a shared LVLI recipe —
        # over a bare name-only proxy row.
        fallback = None
        for c in ALL_COBJ_ROWS:
            if token in c.get("COBJ_EDID", "").lower():
                fv = _row_fvpa(c)
                plan = plan_name_for_row(c)
                if fv:
                    return fvpa_to_text(fv), plan, fvpa_to_array(fv)
                if fallback is None and plan:
                    fallback = ("", plan, [])
        if fallback:
            return fallback
    # Last resort: grouped recipes (instruments, etc.) where the FURN is an
    # entry of a workshop leveled-list whose CNAM recipe carries the components.
    fv, plan = crafting_via_lvli_membership(fid)
    if fv:
        return fvpa_to_text(fv), plan, fvpa_to_array(fv)
    return "", "", []


def auto_how(fid, furn_edid, entm, premium, season):
    """EDID/flag-driven obtain text. New items get a sensible default and an
    [INFO] line in the build log so the wording can be reviewed."""
    if fid in HOW_OVERRIDES:
        return HOW_OVERRIDES[fid]
    e = (furn_edid or "").upper()
    if season:
        return score_how(season)
    if e.startswith(("ATX_", "F1_ATX_")) or (entm and entm["EDID"].upper().startswith("ATX_")):
        return ATX_HOW
    if e.startswith("MILE_"):
        return "Milepost Zero reward."
    if not premium and e.startswith(("INSTRUMENT_", "FURNITURE_")):
        return "Base game — craftable at a C.A.M.P. or workshop after learning the plan."
    print(f"  [INFO] auto obtain text unknown for {fid} {furn_edid} — review wording")
    return "—"


def build_info_for(fid):
    """Generative Build Information from the FURN record: Power Required (from
    the PowerRequired property or a WorkshopCanBePowered/PowerConnection
    keyword) and Flamingo Units (the WorkshopBudgetObjectMultiplier property —
    the item's C.A.M.P. budget cost). Per-camp / per-workshop build limits are
    not present in the FURN export, so they are not emitted (no fabrication)."""
    r = FURN.get(fid)
    if not r:
        return ""
    flamingo = None
    power = False
    for i in range(1, 11):
        av = (r.get(f"Prop_{i}_AV") or "").strip()
        val = (r.get(f"Prop_{i}_Val") or "").strip()
        if av == "WorkshopBudgetObjectMultiplier":
            try:
                flamingo = int(float(val))
            except ValueError:
                pass
        elif av == "PowerRequired":
            try:
                if float(val) > 0:
                    power = True
            except ValueError:
                pass
    for i in range(1, 11):
        kw = (r.get(f"KW_{i}") or "").strip().lower()
        if "workshopcanbepowered" in kw or "workshoppowerconnection" in kw:
            power = True
    lines = [f"Power Required: {'Yes' if power else 'No'}"]
    if flamingo is not None:
        lines.append(f"Flamingo Units: {flamingo}")
    return "\n".join(lines)


def build_output(fid, groups):
    if fid in BUFF_TEXT:
        lines = [BUFF_TEXT[fid]]
    elif "welltuned" in groups:
        lines = [WT_OUT]
    elif "wellrested" in groups:
        lines = [RESTED_OUT]
    else:
        stats = [STAT_NAME[g] for g in groups if g in STAT_NAME]
        if stats:
            joined = " and ".join(f"+2 {s}" for s in stats)
            lines = [f"{joined} for 30 minutes.", "Solo buff — applies to the player only."]
        else:
            lines = []
    if "wellrested" in groups:
        lines += ["", HOMEBODY_LINE]
    spec = MANUAL_BY_FID.get(fid)
    if spec and spec.get("buff") and fid not in BUFF_TEXT:
        lines = [spec["buff"]]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------- build
items_out = []

for fid in sorted(discovered):
    groups = sorted(discovered[fid], key=lambda g: GROUP_ORDER.get(g, 99))
    fr = FURN.get(fid) or ACTI.get(fid)
    furn_edid = fr["FURN_EDID"] if fr else ""
    base_name = (fr["FURN_FULL"].strip() if fr and fr["FURN_FULL"] else "")
    spec = MANUAL_BY_FID.get(fid, {})

    entm = entm_lookup(fid, NAME_OVERRIDES.get(fid) or spec.get("name") or base_name,
                       furn_edid) \
        if not spec.get("entm") else ENTM_BY_EDID.get(spec["entm"])
    display = (NAME_OVERRIDES.get(fid) or spec.get("name") or base_name
               or ((entm["FULL"] or "").strip() if entm else "")
               or furn_edid or fid)
    desc = clean_desc(entm["DESC"]) if entm else ""
    season = season_from_edid(furn_edid, entm["EDID"] if entm else "")

    premium = "Premium" in ((entm and entm["XALG_Flags"]) or "") or \
              "Premium" in ((fr and fr.get("XALG_Flags")) or "")
    f1 = "Fallout 1st" in ((fr and fr.get("XALG_Flags")) or "")
    tradeable = False if (premium or f1 or (furn_edid or "").upper().startswith(("ATX_", "SCORE_"))) else True
    if fid in TRADEABLE_OVERRIDES:
        tradeable = TRADEABLE_OVERRIDES[fid]

    how = spec.get("how") or auto_how(fid, furn_edid, entm, premium, season)
    # Merged gold-vendor route: scoreboard line first, then the gold bullion
    # line (vendor, reputation rank, cost).
    if fid in GOLD_HOW:
        how = f"{how}\n\n{GOLD_HOW[fid]}"
    obtain_routes = buff_obtain_routes(how, tradeable)
    crafting, plan, crafting_arr = crafting_for(fid, furn_edid)
    if fid in PLAN_OVERRIDES:
        plan = PLAN_OVERRIDES[fid]

    tech = [f"EDID: {furn_edid}", f"FormID: {fid}"] if furn_edid else [f"FormID: {fid}"]
    # rename first line label for FURN records (historic format)
    tech = [f"FURN EDID: {furn_edid}", f"FURN FormID: {fid}"] if FURN.get(fid) else \
           [f"EDID: {furn_edid}", f"FormID: {fid}"]
    if entm:
        tech += [f"ENTM EDID: {entm['EDID']}", f"ENTM FormID: {entm['FormID']}"]
    g0 = [g for g in groups if g in SPECIAL_KW]
    for g in g0:
        kw, sp = SPECIAL_KW[g]
        tech.append(f"{STAT_NAME[g]} Keyword: {kw}")
        tech.append(f"{STAT_NAME[g]} Spell: {sp}")
    if g0:
        tech += DUR_NOTE.split("\n")
    if "welltuned" in groups:
        tech += WT_TECH
    if "wellrested" in groups:
        tech.append("Keyword: 005A4E2B ATX_FurnituretypeRested")
    if fid in KW_NOTES:
        tech.append(f"Keyword: {KW_NOTES[fid]}")
    if fid in SPELL_NOTES:
        tech.append(f"Spell: {SPELL_NOTES[fid]}")
    if spec.get("spell"):
        tech.append(f"Spell: {spec['spell']}")
    if fid in GOLD_TECH:
        tech += GOLD_TECH[fid]

    items_out.append({
        "formId": fid,
        "entmFormId": entm["FormID"] if entm else "",
        "edid": furn_edid,
        "displayName": display,
        "description": desc,
        "obtainSource": "",
        "howToObtain": how,
        "obtainRoutes": obtain_routes,
        "dropRate": "N/A",
        "seasonNumber": season,
        "tradeable": tradeable,
        "planName": plan,
        "imageUrl": image_for(entm, furn_edid),
        "outputInfo": build_output(fid, groups),
        "buildInfo": build_info_for(fid),
        "craftingRequirements": crafting_arr,
        "technicalNotes": "\n".join(tech),
        "buffTypes": groups,
        # All items use the 4-sub-expand layout now — Well Tuned instruments
        # included (user requirement, 7 Jun 2026).
        "singleExpand": False,
        # Output stacking rows (rendered as aligned label/value rows).
        # Well Tuned stacks with other AP regen buffs but not with itself.
        "outputRows": (["Stacks with other buffs: Yes", "Stacks on itself: No"]
                       if "welltuned" in groups else []),
        "cutContent": False,
    })

# ----- aggregate bed entries -------------------------------------------------
BED_ENTRIES = [
    ("003CD038", "Sleeping Bags",
     "Rested: +5% XP for 60 minutes — solo buff (player only).",
     "0005C528 SURV_WellRested", "003CD038 BedTypeSleepingBag", "sleepingbag"),
    ("003CD037", "Mattresses",
     "Rested: +5% XP for 60 minutes — solo buff (player only).",
     "0005C528 SURV_WellRested", "003CD037 BedTypeMattress", "mattress"),
    ("003CD036", "Comfy Beds",
     "Well Rested: +5% XP and +2 Agility for 2 hours (3 hours with the Homebody perk) — solo buff (player only).",
     "003CD033 SURV_WellRested2", "003CD036 BedTypeComfy", "comfy"),
]
for fid, label, buff, spell, kwline, bkey in BED_ENTRIES:
    names = bed_names[bkey]
    tech = [f"Bed Type Keyword: {kwline}", f"Buff Spell: {spell}", "",
            f"Counts As ({len(names)}):"] + names
    items_out.append({
        "formId": fid, "entmFormId": "", "edid": kwline.split(" ")[1],
        "displayName": label,
        "description": f"Any bed that counts as a {label[:-1].lower()} grants this buff when you sleep in it. "
                       "The full list of qualifying items is in the Technical section.",
        "obtainSource": "", "howToObtain": "Various — base game, Atom Shop and Scoreboard bed plans all qualify.",
        "obtainRoutes": buff_obtain_routes("Various — base game, Atom Shop and Scoreboard bed plans all qualify.", None),
        "dropRate": "N/A", "seasonNumber": None, "tradeable": None, "planName": "",
        "imageUrl": IMG_BASE + label.lower().replace(" ", "_") + ".avif",
        "outputInfo": buff + "\n\n" + HOMEBODY_LINE,
        "craftingRequirements": [],
        "technicalNotes": "\n".join(tech),
        "buffTypes": ["wellrested", "experience"],
        "singleExpand": False, "cutContent": False,
    })

# ---------------------------------------------------------------- write
data = {"groups": GROUPS, "items": items_out}
OUT.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
counts = {}
for it in items_out:
    for g in it["buffTypes"]:
        counts[g] = counts.get(g, 0) + 1
print(f"Wrote {len(items_out)} items -> {OUT}")
for g in GROUPS:
    print(f"  {g['label']}: {counts.get(g['key'], 0)}")
missing = [it["displayName"] for it in items_out if not it["entmFormId"] and it["formId"] not in
           ("003CD038", "003CD037", "003CD036")]
print("No ENTM match (desc/image fall back to FURN):", ", ".join(missing) or "none")
