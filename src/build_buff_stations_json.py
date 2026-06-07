#!/usr/bin/env python3
"""
build_buff_stations_json.py

Reads xEdit TSV exports and builds dist/buff-stations.json for the
DF/BNB Buff Stations page (df-bnb-camp-items.js, page type "buff-stations").

Groups (root expands), display order:
  SPECIAL spelled out: Strength, Perception, Endurance, Charisma,
  Intelligence, Agility, Luck — then ABC:
  Experience (XP), Unique Buffs, Utility, Well Rested, Well Tuned
Items granting two buffs carry both group keys in buffTypes.

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
    """Resolve a TSV path via env var -> glob -> bare fallback."""
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

# Bed name lists from bed-type keyword refs
BED_KW = {"003CD038": "sleepingbag", "003CD037": "mattress", "003CD036": "comfy"}
bed_names = {"sleepingbag": [], "mattress": [], "comfy": []}
CUT_RE = re.compile(r"^(zzz|test|chargen|post_|npc|workshopnpc)", re.I)
for r in rows(KYWD_REFS_PATH):
    k = r["KeywordFormID"]
    if k in BED_KW:
        fr = FURN.get(r["RefFormID"])
        if not fr:
            continue
        edid, full = fr["FURN_EDID"], (fr["FURN_FULL"] or "").strip()
        if not full or CUT_RE.match(edid):
            continue
        lst = bed_names[BED_KW[k]]
        if full not in lst:
            lst.append(full)
for v in bed_names.values():
    v.sort()

# Homebody perk (PERK 00393F6E Homebody01)
HOMEBODY_DESC = ""
for r in rows(PERK_PATH):
    if r["PERK_EDID"] == "Homebody01":
        HOMEBODY_DESC = r["DESC"].strip()
        break
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

# ---------------------------------------------------------------- item list
# (furnFormId, groups, entmEdid or None, source key or literal howToObtain)
# source keys: atx / score / gold / f1st, anything else = literal text
I = []
def item(fid, groups, entm=None, how=None, **kw):
    I.append(dict(fid=fid, groups=groups, entm=entm, how=how, **kw))

# Strength
item("0056744C", ["strength"], "SCORE_S2_ENTM_Utility_WeightBench", "score")
item("005F56CA", ["strength"], "SCORE_S2_ENTM_Utility_WeightBench", "gold",
     name="Weight Bench (Gold Vendor)")
# Agility
item("005D80E0", ["agility"], "SCORE_S3_ENTM_CAMP_FloorDecor_ExerciseEquipment_AntiqueSpeedBag", "score")
item("00609E0A", ["agility"], "SCORE_S3_ENTM_CAMP_FloorDecor_ExerciseEquipment_AntiqueSpeedBag", "gold",
     name="Antique Speed Bag (Gold Vendor)")
item("00804F5B", ["agility"], "ATX_ENTM_CAMP_FloorDecor_ExerciseEquipment_RaiderSpeedBag", "atx")
item("007CF731", ["agility"], None,
     "Plan: Cosmic Capture — rare reward drop from the Invaders from Beyond seasonal event.")
item("006F459F", ["agility", "perception"], "ATX_ENTM_CAMP_Furniture_BoardwalkBonanzaPinballMachine", "atx")
item("0076E0FF", ["agility", "perception"], "ATX_ENTM_CAMP_Furniture_GameCabinet_AstroAttack", "atx")
item("007DB370", ["agility", "perception"], "ATX_ENTM_CAMP_Furniture_VaultTecPinballMachine", "atx")
item("008B12E0", ["agility", "perception"], "SCORE_MiniSeason_2026_WeaponsExpert_ENTM_CAMP_Furniture_RipDaringPinballMachine",
     "Purchase with tickets from the Weapons Expert Mini Season (2026) reward shop.")
item("0076A6C0", ["agility", "perception"], "ATX_ENTM_CAMP_Furniture_AtomicRoller", "atx")
item("007CEA86", ["agility", "luck"], "ATX_ENTM_CAMP_FloorDecor_FiveFingerFiletTable", "atx")
# Endurance
item("00644EA2", ["endurance"], "SCORE_S8_ENTM_CAMP_Utility_ExerciseBike", "score")
# Charisma
item("006628DA", ["charisma"], "SCORE_S15_ENTM_CAMP_Furniture_Table_Special_9BallTable_B_Blue", "score")
item("006628D8", ["charisma"], "ATX_F1_ENTM_CAMP_Furniture_Table_Special_9BallTable_Red", "f1st")
item("006628D9", ["charisma"], "ATX_ENTM_CAMP_Furniture_Table_Special_9BallTable_D_WoodGreen", "atx")
item("006628DB", ["charisma"], "ATX_ENTM_CAMP_Furniture_Table_Special_9BallTable_C_Green", "atx")
item("00840E70", ["charisma"], "ATX_ENTM_CAMP_Furniture_Table_Special_9BallTable_Rustic_HandmadePoolTable", "atx")
item("00646D80", ["charisma"], "SCORE_S9_ENTM_CAMP_Utility_ArmWrestleMachine", "score")
item("00684BAA", ["charisma"], "ATX_ENTM_CAMP_Utility_ArmWrestleMachine_VaultGirl", "atx",
     name="Vault Girl Arm Wrestle Machine")
item("007DBEAA", ["charisma"], "SCORE_S21_ENTM_CAMP_Utility_ArmWrestleMachine_Poseidon", "score")
item("006A274D", ["charisma"], "SCORE_S13_ENTM_CAMP_Utility_HollywoodVanity", "score")
item("007267B6", ["charisma"], None, "atx", name="Hollywood Vanity (Free)")
item("006E739E", ["charisma"], "ATX_ENTM_CAMP_Furniture_ShoeshineMachine", "atx")
item("006A910B", ["charisma", "luck"], "ATX_ENTM_Structure_Furniture_BowlingAlleyLane", "atx")
item("006EB2BF", ["charisma", "luck"], "ATX_TP_ENTM_Structure_Furniture_BowlingAlleyLane_Americana", "atx")
# Perception
item("006628DF", ["perception"], None, "atx", name="Radiation Glove Box")
item("00766AA3", ["perception"], None,
     "Plan: Stargazer's Telescope — sold by the Milepost Zero vendors (imported goods).",
     name="Stargazer's Telescope")
# Intelligence
item("007D6A59", ["intelligence"], "ATX_ENTM_CAMP_FloorDecor_SummoningCircle", "atx")
# Luck
item("006C50B8", ["luck", "perception"], "ATX_ENTM_CAMP_Furniture_BowlingArcadeMachine", "atx")
item("006FA9F0", ["luck", "perception"], "ATX_ENTM_CAMP_Furniture_BowlingArcadeMachine_StarsAndStrikes", "atx")
item("006FCB2E", ["luck", "perception"], "ATX_ENTM_CAMP_Furniture_BowlingArcadeMachine_RollingStars", "atx")
item("008B12DC", ["luck", "perception"], "SCORE_S25_ENTM_CAMP_Furniture_CamdenClawMachine", "score")
# Unique Buffs
item("0089ADB0", ["unique"], "SCORE_S24_ENTM_CAMP_Utility_Phoropter", "score",
     buff="Accuracy Boost: +25% V.A.T.S. Accuracy for 2 hours.",
     spell="0089ADB5 “Accuracy Boost”", kwline="0089ADB2 FurnituretypeAccuracy")
item("00897409", ["unique"], "SCORE_MiniSeason_LoveHurts_ENTM_CAMP_Furniture_Chair_LethalSeat",
     "Purchase with tickets from the Love Hurts Mini Season reward shop.",
     buff="Rush of Love: +25% Chem Duration for 60 minutes.",
     spell="00897408 “Rush of Love”", kwline="00897407 LoveHurts_FurnituretypeChemDuration")
item("008B1D5A", ["unique"], "ATX_ENTM_CAMP_Utility_Sharpening", "atx",
     buff="Sharpening Stone: 5% chance to gain a Blood Pack on melee kills for 30 minutes.",
     spell="008D1C9C “Sharpening Stone”")
item("008B1553", ["unique"], "SCORE_MiniSeason_2026_WeaponsExpert_ENTM_CAMP_Furniture_RipBoyStatue",
     "Purchase with tickets from the Weapons Expert Mini Season (2026) reward shop.",
     buff="Rip’s Bounty: cryptids may drop extra scraps for 60 minutes.",
     spell="008B1D5E “Rip’s Bounty”")
# Utility
item("005D98A5", ["utility"], None, "atx", name="Sympto-matic",
     buff="Cures all diseases when used.",
     kwline="005D98A6 FurnitureTypeDiseaseCure")
item("007B2841", ["utility"], "ATX_ENTM_CAMP_FloorDecor_BloodTransfusionPump", "atx",
     buff="Restores health and removes rads when used.\n"
          "Ghoul characters gain rads instead.\n15 minute cooldown between uses.")
# Experience (XP)
item("00755EB1", ["experience"], None,
     "Reward from the Mothman Equinox seasonal event. Not craftable.",
     name="Sacred Mothman Tome",
     buff="+5% XP for 60 minutes — solo buff (player only).",
     kwline="0076B52B FurnitureTypeXPBonus")
item("0068D3D5", ["experience"], None,
     "Placed and used by Lite Ally: Steven Scarberry (Season 12 Scoreboard) — "
     "the ally applies the buff to your team.",
     name="Scarberry’s Shrine",
     buff="+5% XP for 60 minutes — team buff, applied by your companion.")
# Well Rested — rested-type furniture (also give the XP buff → both groups)
for fid, entm, how in [
    ("005A2C4B", "ATX_ENTM_CAMP_Decoration_Communal_Firepit", "atx"),
    ("0060212D", "ATX_ENTM_CAMP_Decoration_HotTub", "atx"),
    ("0060EC27", "ATX_ENTM_CAMP_Decoration_VaultTecSpa", "atx"),
    ("00677B9B", "SCORE_S11_ENTM_CAMP_Furniture_CappyHotTub", "score"),
    ("0068380D", "SCORE_S20_ENTM_CAMP_Decoration_GooTub", "score"),
    ("0068DF0A", "ATX_ENTM_CAMP_Furniture_SkullsFirePit", "atx"),
    ("0079B885", None, "Plan: Cauldron Hot Tub — event-specific reward during the "
                       "Mischief Night event."),
]:
    item(fid, ["wellrested", "experience"], entm, how,
         buff="Rested: +5% XP for 60 minutes — solo buff (player only).",
         kwline="005A4E2B ATX_FurnituretypeRested",
         homebody=True)

# ---------------------------------------------------------------- helpers
def clean_desc(desc):
    """Strip storefront boilerplate segments like '- C.A.M.P. ITEMS APPEAR ... -'."""
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

def entm_lookup(spec, full_name):
    if spec:
        return ENTM_BY_EDID.get(spec)
    cands = [r for r in ENTM_BY_FULL.get((full_name or "").lower(), [])
             if not r["EDID"].upper().startswith(("ZZZ", "POST_", "DEL_", "CUT_"))]
    return cands[0] if cands else None

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
    # fall back: COBJ EDID token match
    token = re.sub(r"^(ATX_|SCORE_S\d+_|SCORE_)", "", furn_edid or "").split("_")[-1].lower()
    if len(token) >= 6:
        for clist in COBJ_BY_CNAM.values():
            for c in clist:
                if token in c.get("COBJ_EDID", "").lower():
                    return fvpa_to_text(c.get("FVPA", "")), (c.get("GNAM_FULL") or "").strip()
    return "", ""

DUR_NOTE = ("Duration Global: ATX_SPECIAL_BuffDurationGlobal (0065015E) = 1800s\n"
            "Magnitude Global: ATX_SPECIAL_BuffMagnitudeGlobal (0065015F) = +2")

def build_output(groups, spec):
    """Output box text — the buff(s) the item gives."""
    if "buff" in spec and spec["buff"]:
        lines = [spec["buff"]]
    else:
        stats = [STAT_NAME[g] for g in groups if g in STAT_NAME]
        if stats:
            joined = " and ".join(f"+2 {s}" for s in stats)
            lines = [f"{joined} for 30 minutes.", "Solo buff — applies to the player only."]
        else:
            lines = []
    if spec.get("homebody"):
        lines.append("")
        lines.append(HOMEBODY_LINE)
    return "\n".join(lines).strip()

# ---------------------------------------------------------------- build
items_out = []

def emit(fid, groups, display, desc, how, season, tradeable, entm, furn_edid,
         output, tech_extra, crafting, plan, single=False):
    rec = {
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
        "outputInfo": output,
        "craftingRequirements": crafting,
        "technicalNotes": tech_extra,
        "buffTypes": groups,
        "singleExpand": single,
        "cutContent": False,
    }
    items_out.append(rec)

for spec in I:
    fid = spec["fid"]
    fr = FURN.get(fid) or ACTI.get(fid)
    furn_edid = fr["FURN_EDID"] if fr else ""
    base_name = (fr["FURN_FULL"].strip() if fr and fr["FURN_FULL"] else "")
    entm = entm_lookup(spec.get("entm"), spec.get("name") or base_name)
    display = (spec.get("name") or base_name
               or ((entm["FULL"] or "").strip() if entm else "")
               or furn_edid or fid)
    desc = clean_desc(entm["DESC"]) if entm else ""
    season = season_from_edid(furn_edid, entm["EDID"] if entm else "")

    how = spec.get("how")
    if how == "atx":
        how = ATX_HOW
    elif how == "score":
        how = score_how(season) if season else "Scoreboard reward."
    elif how == "gold":
        how = "Purchase the plan for gold bullion (available once per character)."
    elif how == "f1st":
        how = "Available to Fallout 1st members."

    premium = "Premium" in ((entm and entm["XALG_Flags"]) or "") or \
              "Premium" in ((fr and fr["XALG_Flags"]) or "")
    f1 = "Fallout 1st" in ((fr and fr["XALG_Flags"]) or "")
    tradeable = False if (premium or f1 or (furn_edid or "").upper().startswith(("ATX_", "SCORE_"))) else True

    crafting, plan = crafting_for(fid, furn_edid)

    tech = [f"FURN EDID: {furn_edid}", f"FURN FormID: {fid}"]
    if entm:
        tech.append(f"ENTM EDID: {entm['EDID']}")
        tech.append(f"ENTM FormID: {entm['FormID']}")
    g0 = [g for g in spec["groups"] if g in SPECIAL_KW]
    for g in g0:
        kw, sp = SPECIAL_KW[g]
        tech.append(f"{STAT_NAME[g]} Keyword: {kw}")
        tech.append(f"{STAT_NAME[g]} Spell: {sp}")
    if g0:
        tech.extend(DUR_NOTE.split("\n"))
    if spec.get("kwline"):
        tech.append(f"Keyword: {spec['kwline']}")
    if spec.get("spell"):
        tech.append(f"Spell: {spec['spell']}")

    emit(fid, spec["groups"], display, desc, how, season, tradeable, entm,
         furn_edid, build_output(spec["groups"], spec), "\n".join(tech),
         crafting, plan)

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
    out = buff + "\n\n" + HOMEBODY_LINE
    items_out.append({
        "formId": fid, "entmFormId": "", "edid": kwline.split(" ")[1],
        "displayName": label,
        "description": f"Any bed that counts as a {label[:-1].lower()} grants this buff when you sleep in it. "
                       "The full list of qualifying items is in the Technical section.",
        "obtainSource": "", "howToObtain": "Various — base game, Atom Shop and Scoreboard bed plans all qualify.",
        "dropRate": "N/A", "seasonNumber": None, "tradeable": None, "planName": "",
        "imageUrl": IMG_BASE + label.lower().replace(" ", "_") + ".avif",
        "outputInfo": out, "craftingRequirements": "",
        "technicalNotes": "\n".join(tech),
        "buffTypes": ["wellrested", "experience"],
        "singleExpand": False, "cutContent": False,
    })

# ----- instruments (Well Tuned) ---------------------------------------------
INSTRUMENTS = [
    # (furnFormId, entmEdid or None, override name or None, source)
    ("0000CFA7", None, "Acoustic Guitar", "base"),
    ("0000C22A", None, "Banjo", "base"),
    ("00006F48", None, "Steel Guitar", "base"),
    ("00044DBC", None, "Snare Drum", "base"),
    ("0010F30F", None, "Grand Piano", "base"),
    ("0010F3BC", None, "Upright Piano", "base"),
    ("0034D1F5", None, "Tuba", "base"),
    ("0037B7F4", None, "Bass", "base"),
    ("003B7692", None, "Frame Drum", "base"),
    ("001199CA", None, "Chemical Barrel", "base"),
    ("0033AE56", None, "Metal Barrel", "base"),
    ("004E0FA9", None, "Mouth Harp", "base"),
    ("00662764", None, "Nuka-lele", "base"),
    ("00668025", None, "Nuka-lele (Quantum)", "base"),
    ("006381B4", None, "Pipe Organ", "base"),
    ("0055A338", "ATX_ENTM_CAMP_Furniture_Instrument_Theremin", None, "atx"),
    ("005D1C15", "ATX_ENTM_CAMP_Furniture_Instrument_Orgatronic", None, "atx"),
    ("005FD994", "ATX_ENTM_CAMP_Furniture_Instrument_DrumSet", None, "atx"),
    ("0062C3E2", "ATX_ENTM_CAMP_Furniture_Instrument_Pipe_Organ", "Pipe Organ (Atom Shop)", "atx"),
    ("0064E9E7", "ATX_ENTM_CAMP_Furniture_Instrument_DrumSet_Skull", None, "atx"),
    ("0065BFF1", "ATX_ENTM_CAMP_Furniture_Instrument_ResonatorGuitar", None, "atx"),
    ("00678D16", "ATX_ENTM_CAMP_Furniture_Instrument_ResonatorGuitar", "Resonator Guitar B", "atx"),
    ("00664E7D", "ATX_ENTM_CAMP_Furniture_Instrument_HamboneChair", None, "atx"),
    ("0066F201", "SCORE_S16_ENTM_CAMP_Furniture_Instrument_HomemadeXylophone", None, "score"),
    ("00691B73", "SCORE_S12_ENTM_CAMP_Furniture_Instrument_ViolinChair", "Violin", "score"),
    ("00728409", "ATX_ENTM_CAMP_Furniture_Instrument_Xylophone", None, "atx"),
    ("007746A5", "ATX_ENTM_CAMP_Furniture_Instrument_Accordion", None, "atx"),
    ("007AE386", None, "Accordion (Free)", "Free reward — unlocked for all players."),
    ("0078C78E", None, "Rusty Saxophone", "Plan: Rusty Saxophone — Milepost Zero reward."),
    ("007AE566", None, "Chemical Barrel Drum (Blue)", "score"),
    ("007AE563", None, "Radioactive Barrel Drum", "score"),
    ("007AE564", None, "Metal Barrel Drum", "score"),
    ("007AE565", None, "Chemical Barrel Drum", "score"),
]
WT_TECH = ["Instrument Keyword: 0050CD11 FurnitureTypeInstrument",
           "Buff Spell: 0050CD15 SURV_WellTunedSpell “Well Tuned”",
           "Buff Effect: 0050CD14 SURV_WellTunedEffect — +25 AP regen, 3600s"]
WT_OUT = ("Well Tuned: +25 Action Point regeneration for 60 minutes.\n"
          "Solo buff — applies to the player only.")

for fid, entm_edid, name_over, src in INSTRUMENTS:
    fr = FURN.get(fid)
    furn_edid = fr["FURN_EDID"] if fr else ""
    base_name = (fr["FURN_FULL"].strip() if fr and fr["FURN_FULL"] else "")
    display = name_over or base_name or furn_edid
    entm = entm_lookup(entm_edid, base_name)
    desc = clean_desc(entm["DESC"]) if entm else ""
    season = season_from_edid(furn_edid, entm["EDID"] if entm else "")
    if src == "base":
        how = "Base game — craftable at a C.A.M.P. or workshop after learning the plan."
    elif src == "atx":
        how = ATX_HOW
    elif src == "score":
        how = score_how(season) if season else "Scoreboard reward."
    else:
        how = src
    premium = "Premium" in ((entm and entm["XALG_Flags"]) or "") or \
              "Premium" in ((fr and fr["XALG_Flags"]) or "")
    tradeable = not premium and src == "base"
    crafting, plan = crafting_for(fid, furn_edid)
    tech = [f"FURN EDID: {furn_edid}", f"FURN FormID: {fid}"]
    if entm:
        tech += [f"ENTM EDID: {entm['EDID']}", f"ENTM FormID: {entm['FormID']}"]
    tech += WT_TECH
    emit(fid, ["welltuned"], display, desc, how, season, tradeable, entm,
         furn_edid, WT_OUT, "\n".join(tech), crafting, plan, single=True)

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
