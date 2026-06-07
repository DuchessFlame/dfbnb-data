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

parser = argparse.ArgumentParser()
parser.add_argument("--tsv-dir", default="tsv",  help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
args = parser.parse_args()

TSV_DIR = Path(args.tsv_dir)
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "buff-stations.json"


def _newest_glob(pattern):
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


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

# COBJ — crafting components + plan names, matched by created-object FormID
COBJ_BY_CNAM = {}
for r in rows(COBJ_PATH):
    cn = r.get("CNAM_FormID", "").strip()
    if cn:
        COBJ_BY_CNAM.setdefault(cn, []).append(r)


def fvpa_to_text(fvpa):
    parts = []
    for chunk in (fvpa or "").split("|"):
        bits = chunk.split(":")
        if len(bits) >= 2 and bits[0].strip():
            name = bits[0].strip()
            name = re.sub(r"^c_", "", name)
            name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
            parts.append(f"{name} ×{bits[1].strip()}")
    return "\n".join(parts)


# ---------------------------------------------------------------- groups
GROUPS = [
    {"key": "strength",     "label": "Strength",
     "blurb": "+2 Strength for 30 minutes — solo buff (player only)."},
    {"key": "perception",   "label": "Perception",
     "blurb": "+2 Perception for 30 minutes — solo buff (player only)."},
    {"key": "endurance",    "label": "Endurance",
     "blurb": "+2 Endurance for 30 minutes — solo buff (player only)."},
    {"key": "charisma",     "label": "Charisma",
     "blurb": "+2 Charisma for 30 minutes — solo buff (player only)."},
    {"key": "intelligence", "label": "Intelligence",
     "blurb": "+2 Intelligence for 30 minutes — solo buff (player only)."},
    {"key": "agility",      "label": "Agility",
     "blurb": "+2 Agility for 30 minutes — solo buff (player only)."},
    {"key": "luck",         "label": "Luck",
     "blurb": "+2 Luck for 30 minutes — solo buff (player only)."},
    {"key": "experience",   "label": "Experience (XP)",
     "blurb": "Stations and furniture that grant a +5% XP buff."},
    {"key": "unique",       "label": "Unique Buffs",
     "blurb": "One-of-a-kind buff stations."},
    {"key": "utility",      "label": "Utility",
     "blurb": "Stations with a useful function rather than a timed buff."},
    {"key": "wellrested",   "label": "Well Rested",
     "blurb": "Beds and furniture that grant the Rested / Well Rested buff."},
    {"key": "welltuned",    "label": "Well Tuned",
     "blurb": "Musical instruments — +25 AP regeneration for 60 minutes "
              "(solo buff, player only)."},
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

# ---------------------------------------------------------------- overrides
NAME_OVERRIDES = {
    "005F56CA": "Weight Bench (Gold Vendor)",
    "00609E0A": "Antique Speed Bag (Gold Vendor)",
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
    "005F56CA": "Purchase the plan for gold bullion (available once per character).",
    "00609E0A": "Purchase the plan for gold bullion (available once per character).",
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
    "005F56CA": "SCORE_S2_ENTM_Utility_WeightBench",
    "005D80E0": "SCORE_S3_ENTM_CAMP_FloorDecor_ExerciseEquipment_AntiqueSpeedBag",
    "00609E0A": "SCORE_S3_ENTM_CAMP_FloorDecor_ExerciseEquipment_AntiqueSpeedBag",
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
    if fid in EXCLUDED:
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
    if name:
        return f"Purchase with tickets from the {name} Scoreboard (Season {season})"
    return f"Purchase with tickets from the Season {season} Scoreboard"


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
        txt = fvpa_to_text(c.get("FVPA", ""))
        plan = (c.get("GNAM_FULL") or "").strip()
        if txt or plan:
            return txt, plan
    token = re.sub(r"^(ATX_|SCORE_S\d+_|SCORE_)", "", furn_edid or "").split("_")[-1].lower()
    if len(token) >= 6:
        for clist in COBJ_BY_CNAM.values():
            for c in clist:
                if token in c.get("COBJ_EDID", "").lower():
                    return fvpa_to_text(c.get("FVPA", "")), (c.get("GNAM_FULL") or "").strip()
    return "", ""


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
    crafting, plan = crafting_for(fid, furn_edid)

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

    items_out.append({
        "formId": fid,
        "entmFormId": entm["FormID"] if entm else "",
        "edid": furn_edid,
        "displayName": display,
        "description": desc,
        "obtainSource": "",
        "howToObtain": how,
        "dropRate": "N/A",
        "seasonNumber": season,
        "tradeable": tradeable,
        "planName": plan,
        "imageUrl": image_for(entm, furn_edid),
        "outputInfo": build_output(fid, groups),
        "craftingRequirements": crafting,
        "technicalNotes": "\n".join(tech),
        "buffTypes": groups,
        "singleExpand": "welltuned" in groups,
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
        "dropRate": "N/A", "seasonNumber": None, "tradeable": None, "planName": "",
        "imageUrl": IMG_BASE + label.lower().replace(" ", "_") + ".avif",
        "outputInfo": buff + "\n\n" + HOMEBODY_LINE,
        "craftingRequirements": "",
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
