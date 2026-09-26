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
import camp_config       # hand-maintained tables live in data/camp/*.json
import gold_vendor       # generative Gold Bullion route (ENTM -> vendor plan)
import reusable_images   # art the site already hosts — season_images first
from cut_content import CUT_OBTAIN_TEXT

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
try:
    GLOB_PATH = _resolve_tsv("GLOB_TSV", "GLOB_Export_*.tsv", "GLOB_Export.tsv")
except FileNotFoundError:
    GLOB_PATH = None

print("Loading TSVs…")
for _n, _p in [("FURN", FURN_PATH), ("ACTI", ACTI_PATH), ("ENTM", ENTM_PATH),
               ("KYWD Refs", KYWD_REFS_PATH), ("PERK", PERK_PATH), ("COBJ", COBJ_PATH)]:
    print(f"  {_n}: {_p}")

IMG_BASE = "/wp-content/uploads/guide-images/camp-items/buff-stations/"

# Art the site already serves, read out of dist/ (season manifests first).
# Scoreboard items resolve to their season_images tile before anything else,
# so no second copy is ever uploaded into camp-items/buff-stations/.
HOSTED = reusable_images.build_index(str(OUT_DIR))
print("  " + HOSTED.summary())
ATX_HOW  = "Can be purchased with certain bundles from the Atom Shop."

# Override tables (groups, exclusions, name/how/ENTM overrides, gold-vendor
# merges, beds). One entry gets added most seasons — see data/camp/buff_stations.json.
_CFG = camp_config.load("buff_stations", {"ATX_HOW": ATX_HOW,
                                           "CUT_OBTAIN_TEXT": CUT_OBTAIN_TEXT})


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


# ---------------------------------------------------------------- TSV loads
FURN = {}
for r in rows(FURN_PATH):
    FURN[r["FURN_FormID"]] = r

ACTI = {}
for r in rows(ACTI_PATH):
    # Keep the whole row (KW_n, Prop_n_*, VMAD_Scripts): script-driven buff
    # discovery and the interaction type read them. The FURN_* aliases let
    # FURN and ACTI records share one code path.
    _a = dict(r)
    _a.update({"FURN_EDID": r["ACTI_EDID"], "FURN_FULL": r["ACTI_FULL"],
               "XALG_Flags": r.get("XALG_Flags", "")})
    ACTI[r["ACTI_FormID"]] = _a

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
# Output & Effects renders outputRows as an aligned label/value table, so the
# buff is split into one fact per row instead of a run-on sentence.
SOLO_ROW = "Applies To: You only (solo buff)"
HOMEBODY_ROWS = ["Homebody Perk: Heal Rate +2 and Limb Regeneration +200 while in your C.A.M.P. or workshop",
                 "Homebody Bonus: Comfy Bed Well Rested lasts 3 hours instead of 2"]

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
# LVLI FormID -> its entries [(FormID, EDID, signature)], same workshop filter.
# Drives "Shares a Build Limit with": everything on one build list is built
# from one recipe, so it shares that recipe's WorkshopCount limit.
LVLI_ENTRIES_BY_LIST = {}
LVLI_EDID = {}
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
            bits = [b.strip() for b in ref.split(":")]
            LVLI_ENTRIES_BY_LIST.setdefault(lvli, []).append(
                (member, bits[1] if len(bits) > 1 else "", bits[2] if len(bits) > 2 else ""))
            LVLI_EDID[lvli] = (r.get("LVLI_EDID") or "").strip()


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
GROUPS = _CFG["groups"]
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
KW_TO_GROUPS = _CFG["kw_to_groups"]

# Records that carry a buff keyword but must NOT be on the page.
# World-placed objects (REFR only, no buildable COBJ) and quest/companion props.
EXCLUDED = set(_CFG["excluded"])
STATUS_OVERRIDES = _CFG.get("status_overrides", {})

# FURN EDID -> a wp-content URL for art the site already hosts elsewhere (Atom
# Shop request tiles, season_images/season-N, event galleries). Used verbatim,
# so nothing has to be re-uploaded into IMG_BASE. Lookup is case-insensitive;
# see data/camp/buff_stations.json.
IMAGE_OVERRIDES = {k.lower(): v for k, v in _CFG.get("image_overrides", {}).items()}

CUT_RE = re.compile(r"^(zzz|test|chargen|post_)", re.I)

# Items that grant a buff but carry NO detectable buff keyword — added manually.
MANUAL_ITEMS = _CFG["manual_items"]

# Gold-vendor re-releases of scoreboard items are the SAME item with a second
# purchase route — they merge into the base entry instead of getting their own
# sub-expand (user requirement, 7 Jun 2026). The base item's How to Obtain
# lists the scoreboard first, then the gold bullion route with vendor name,
# reputation rank needed for it to appear in their inventory, and cost.
# Source: BOOK VendorList LVLI (W05_LLV_GoldVendor_*) + Econ_GoldVendor_Tier
# GLOBs. Same item with a DIFFERENT SKIN stays a separate sub-expand
# (beds/sleeping bags excepted — they stay aggregated).
GOLD_MERGED = _CFG["gold_merged"]
# One ENTM -> several FURN versions: merged-away FURN -> kept FURN.
VARIANT_MERGED = _CFG.get("variant_merged", {})
VARIANT_INFO = _CFG.get("variant_info", {})

# Gold bullion line appended to the base item's How to Obtain.
# One labelled line per fact (camp-item-expands "Route detail formats") — the
# renderer turns "Label: value" lines into aligned sub-rows.
GOLD_HOW = _CFG["gold_how"]

# Correct plan names for the merged items (the auto COBJ token-fallback
# previously mismatched these to an unrelated GoldVendor recipe).
PLAN_OVERRIDES = _CFG["plan_overrides"]

# Extra Technical lines for merged gold-vendor records
GOLD_TECH = _CFG["gold_tech"]

# ---------------------------------------------------------------- overrides
NAME_OVERRIDES = _CFG["name_overrides"]

HOW_OVERRIDES = _CFG["how_overrides"]

# Free-from-ATX unlocks have no tradeable plan; Milepost saxophone plan is
# untradeable per the published data set.
TRADEABLE_OVERRIDES = _CFG["tradeable_overrides"]

# ENTM matches the FULL-name auto-lookup can't resolve (shared or differing names)
ENTM_OVERRIDES = _CFG["entm_overrides"]

# The season that actually awards the item, when the EDID disagrees. A bonus
# rewards page re-offers items built for an earlier season, so SCORE_Sxx_ in
# the EDID is the asset's origin season and not always the granting one.
# Overriding here fixes seasonNumber, the How to Obtain line and the Scoreboard
# route together, because all three derive from this number. imageUrl is NOT
# affected - it resolves from the EDID, which is correct as-is.
SEASON_OVERRIDES = {k: int(v) for k, v in _CFG.get("season_overrides", {}).items()}

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
    if fid in EXCLUDED or fid in GOLD_MERGED or fid in VARIANT_MERGED:
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
BED_CUT_RE = re.compile(r"^(zzz|test|chargen|post_|npc)", re.I)  # WorkshopNpc* are the buildable workshop beds (the only Mattress records) — keep them
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
    # 1. Scoreboard tile already hosted in season_images — always wins.
    tex = ((entm or {}).get("ETDI") or "").strip()
    season = (HOSTED.find_season(edid=(entm or {}).get("EDID") or "", texture=tex)
              or HOSTED.find_season(edid=furn_edid or "", texture=furn_edid or ""))
    if season:
        return season
    # 2. Hand-picked override (atom-shop tiles, mini-season galleries, beds).
    override = IMAGE_OVERRIDES.get((furn_edid or "").lower())
    if override:
        return override
    # 3. Any other hosted art (Atom Shop / bundles).
    hosted = HOSTED.find(edid=(entm or {}).get("EDID") or "", texture=tex)
    if hosted:
        return hosted
    if entm:
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
    not present in the FURN export, so they are not emitted (no fabrication).
    ACTI stations (script-cast buffs) carry the same KW_n / Prop_n columns."""
    r = FURN.get(fid) or ACTI.get(fid)
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


# ---------------------------------------------------------------- build limits
# Generative, from the game data (user requirement, 25 Sep 2026):
#   FURN -> the COBJ that builds it (CNAM == the FURN, or CNAM == a workshop
#   leveled list the FURN is an entry of) -> the WorkshopCount GLOBs that
#   reference that COBJ. "..._CAMP" / "..._Camp" is the C.A.M.P. limit, the
#   unsuffixed one the workshop limit. A recipe with only one unsuffixed global
#   (the communal firepit / hot tub counts) applies it to both.
# Everything on the same build list — or built by another recipe tied to the
# same global — counts against the same limit, so it is listed under
# "Shares a Build Limit with".
_DEAD_RE = re.compile(r"^(zzz|del_|cut_|test)", re.I)
WSCOUNT_BY_COBJ = {}     # COBJ FormID -> [(GLOB FormID, EDID, value)]
COBJS_BY_GLOB = {}       # GLOB FormID -> [COBJ FormID]
if GLOB_PATH:
    with open(GLOB_PATH, encoding="utf-8", errors="replace", newline="") as _f:
        # Stream it: GLOB is thousands of columns wide (project-wide-tsv-exports).
        _hdr = _f.readline().rstrip("\r\n").split("\t")
        _ref0 = next((i for i, h in enumerate(_hdr) if h.lower().startswith("ref")
                      and h.lower() != "referencedbycount"), 4)
        for _line in _f:
            _p = _line.rstrip("\r\n").split("\t")
            if len(_p) < 3 or "workshopcount" not in _p[1].lower() or _DEAD_RE.match(_p[1]):
                continue
            try:
                _val = int(float(_p[2]))
            except ValueError:
                continue
            for _x in _p[_ref0:]:
                _b = _x.split(":")
                if len(_b) >= 3 and _b[2] == "COBJ" and not _DEAD_RE.match(_b[1]):
                    WSCOUNT_BY_COBJ.setdefault(_b[0], []).append((_p[0], _p[1], _val))
                    COBJS_BY_GLOB.setdefault(_p[0], []).append(_b[0])
COBJ_BY_FID = {r.get("COBJ_FormID", ""): r for r in ALL_COBJ_ROWS}


def _record_name(fid, edid=""):
    """Player-facing name of a build-list entry: this page's own name for it,
    then the FURN/ACTI FULL, then the plan that builds it (STAT entries such
    as the base-game PoolTable01 carry no FULL), then a spaced-out EDID."""
    if NAME_OVERRIDES.get(fid):
        return NAME_OVERRIDES[fid]
    rec = FURN.get(fid) or ACTI.get(fid)
    full = ((rec or {}).get("FURN_FULL") or "").strip()
    if full:
        return full
    for c in COBJ_BY_CNAM.get(fid, []) + [c for c in ALL_COBJ_ROWS
                                          if f"_{(edid or '').lower()}" in c.get("COBJ_EDID", "").lower()][:3]:
        plan = re.sub(r"^Plan:\s*", "", plan_name_for_row(c) or "").strip()
        if plan:
            return plan
    e = re.sub(r"\d+$", "", edid or fid)
    return re.sub(r"([a-z])([A-Z])", r"\1 \2", e) or fid


def _list_members(lvli, seen=None):
    """Every record on a build list, nested lists expanded."""
    seen = seen if seen is not None else set()
    if lvli in seen:
        return []
    seen.add(lvli)
    out = []
    for fid, edid, sig in LVLI_ENTRIES_BY_LIST.get(lvli, []):
        if sig == "LVLI":
            out += _list_members(fid, seen)
        else:
            out.append((fid, edid))
    return out


def _build_lists(fid):
    """Workshop build lists (direct, then parent lists) that a recipe builds."""
    out, todo, seen = [], list(LVLI_BY_MEMBER.get((fid or "").upper(), [])), set()
    while todo:
        l = todo.pop(0)
        if l in seen:
            continue
        seen.add(l)
        if COBJ_BY_CNAM.get(l):
            out.append(l)
        todo += LVLI_BY_MEMBER.get(l, [])
    return out


def build_limits_for(fid, display):
    """(camp, workshop, shares, tech_lines) for one buff station."""
    lists = _build_lists(fid)
    cobjs = [c.get("COBJ_FormID", "") for c in COBJ_BY_CNAM.get(fid, [])]
    for l in lists:
        cobjs += [c.get("COBJ_FormID", "") for c in COBJ_BY_CNAM.get(l, [])]
    cobjs = [c for c in dict.fromkeys(cobjs)
             if c and not _DEAD_RE.match(COBJ_BY_FID.get(c, {}).get("COBJ_EDID", ""))]
    globs = {}
    for c in cobjs:
        for g in WSCOUNT_BY_COBJ.get(c, []):
            globs[g[0]] = g
    camp = [g[2] for g in globs.values() if g[1].lower().endswith("_camp")]
    shop = [g[2] for g in globs.values() if not g[1].lower().endswith("_camp")]
    if shop and not camp and len(shop) == 1:
        camp = shop[:]
    # Shared: the rest of each build list, plus whatever other recipes tied to
    # the same global build.
    members = []
    for l in lists:
        members += _list_members(l)
    for g in globs:
        for c in COBJS_BY_GLOB.get(g, []):
            if c in cobjs:
                continue
            cn = (COBJ_BY_FID.get(c, {}).get("CNAM_FormID") or "").strip()
            if not cn:
                continue
            if cn in LVLI_ENTRIES_BY_LIST:
                members += _list_members(cn)
            else:
                members.append((cn, COBJ_BY_FID[c].get("CNAM_EDID", "")))
    own = (display or "").strip().lower()
    names = []
    for mfid, medid in members:
        if mfid == fid or _DEAD_RE.match(medid or "") or "nonplayer" in (medid or "").lower():
            continue
        # A gold-vendor or merged variant record of this same item.
        if GOLD_MERGED.get(mfid) == fid or VARIANT_MERGED.get(mfid) == fid:
            continue
        nm = _record_name(mfid, medid)
        # A gold-vendor / variant copy of this same item is not "another" item.
        if nm.strip().lower() == own or nm in names:
            continue
        names.append(nm)
    names.sort(key=str.lower)
    tech = [f"Build List: {l} {LVLI_EDID.get(l, '')}".rstrip() for l in lists]
    tech += [f"Build Limit Global: {g[0]} {g[1]} = {g[2]}"
             for g in sorted(globs.values(), key=lambda x: x[1].lower())]
    return (min(camp) if camp else None, min(shop) if shop else None, names, tech)


_DUR_RE = re.compile(r"^(.*?) for (\d+ (?:minutes?|hours?))(?:\s*[—-]\s*solo buff \(player only\))?\.?$")


def split_buff(text):
    """'Accuracy Boost: +25% V.A.T.S. Accuracy for 2 hours.' ->
    ('Accuracy Boost: +25% V.A.T.S. Accuracy', ['Duration: 2 hours']).
    Text that doesn't fit the pattern is returned untouched."""
    m = _DUR_RE.match(text.strip())
    if not m:
        return text.strip(), []
    rows = [f"Duration: {m.group(2)}"]
    if "solo buff" in text.lower():
        rows.append(SOLO_ROW)
    return m.group(1).strip(), rows


def build_output(fid, groups):
    """Returns (outputInfo, outputRows): the buff itself as one short line,
    then one fact per row (duration, who it applies to, perks, stacking)."""
    spec = MANUAL_BY_FID.get(fid)
    rows = []
    if spec and spec.get("buff") and fid not in BUFF_TEXT:
        info, rows = split_buff(spec["buff"])
        rows = rows + list(spec.get("rows") or [])
    elif fid in BUFF_TEXT:
        info, rows = split_buff(BUFF_TEXT[fid])
    elif "welltuned" in groups:
        info, rows = "Well Tuned: +25 Action Point regeneration", ["Duration: 60 minutes", SOLO_ROW]
    elif "wellrested" in groups:
        info, rows = "Rested: +5% XP", ["Duration: 60 minutes", SOLO_ROW]
    else:
        stats = [STAT_NAME[g] for g in groups if g in STAT_NAME]
        if stats:
            info = " and ".join(f"+2 {s}" for s in stats)
            rows = ["Duration: 30 minutes", SOLO_ROW]
        else:
            info = ""
    if "wellrested" in groups:
        rows += HOMEBODY_ROWS
    if "welltuned" in groups:
        rows += ["Stacks With Other Buffs: Yes", "Stacks On Itself: No"]
    return info.strip(), rows


# ------------------------------------------------ script-driven discovery
# Some stations carry NO buff keyword: an activator script casts the buff
# (e.g. Mechanical Derby Game: OnActivateCastSpell::SpellToCast=ATX_BuffIntelligence).
# Read the VMAD_Scripts column (ACTI export; FURN too once its export carries
# it — until then this is a no-op for FURN), take every script property that
# points at a buff SPEL, and resolve the SPEL's timed "Fortify <SPECIAL>"
# effects to page groups. Keyword discovery wins; this only adds records the
# keyword pass did not find. Same cut / exclusion filters, plus an obtain route
# (a live COBJ, a workshop build list with a recipe, or an ENTM) so world /
# expedition / Atlantic City copies stay off the page.
_VMAD_CFG = _CFG.get("vmad_discovery", {})
_VMAD_PROPS = set(_VMAD_CFG.get("spell_props", ["SpellToCast", "BuffSpell"]))
_VMAD_PREFIXES = tuple(x.lower() for x in _VMAD_CFG.get("spell_edid_prefixes", ["ATX_Buff"]))
_VMAD_SKIP_RE = re.compile(r"^(zzz|test|chargen|post_|del_|cut_)", re.I)
_VMAD_VAL_RE = re.compile(r"^([0-9A-Fa-f]{8}):([^:]*):SPEL$")
_FORTIFY_RE = re.compile(r"^fortify\s+(\w+)$", re.I)
_STAT_BY_NAME = {v.lower(): k for k, v in STAT_NAME.items()}

SPEL_FX_BY_FID = {}
for _r in rows(_resolve_tsv("SPEL_EFFECTS_TSV", "SPEL_Export_*_EFFECTS.tsv", "SPEL_Export_EFFECTS.tsv")):
    SPEL_FX_BY_FID.setdefault((_r.get("SPEL_FormID") or "").strip().upper(), []).append(_r)


def vmad_spells(vmad):
    """[(script, property, SPEL FormID, SPEL EDID)] for every VMAD property
    ('Script::Prop=FormID:EDID:SPEL##...') that points at a buff spell."""
    out = []
    for part in (vmad or "").split("##"):
        if "::" not in part or "=" not in part:
            continue
        script, rest = part.split("::", 1)
        prop, val = rest.split("=", 1)
        m = _VMAD_VAL_RE.match(val.strip())
        if not m:
            continue
        sfid, sedid = m.group(1).upper(), m.group(2)
        if prop.strip() in _VMAD_PROPS or sedid.lower().startswith(_VMAD_PREFIXES):
            out.append((script.strip(), prop.strip(), sfid, sedid))
    return out


def spell_groups(sfid):
    """(groups, seconds, spell FULL) from the SPEL's timed Fortify effects."""
    groups, dur, full = [], 0, ""
    for fx in SPEL_FX_BY_FID.get(sfid, []):
        full = full or (fx.get("SPEL_FULL") or "").strip()
        try:
            secs = int(float(fx.get("EFIT_Duration") or 0))
        except ValueError:
            secs = 0
        m = _FORTIFY_RE.match((fx.get("EFID_MGEF_FULL") or "").strip())
        if secs > 0 and m and m.group(1).lower() in _STAT_BY_NAME:
            g = _STAT_BY_NAME[m.group(1).lower()]
            if g not in groups:
                groups.append(g)
            dur = max(dur, secs)
    return groups, dur, full


def has_obtain_route(fid, full, edid):
    if any(not _DEAD_RE.match(c.get("COBJ_EDID", "")) for c in COBJ_BY_CNAM.get(fid, [])):
        return True
    if _build_lists(fid):
        return True
    return entm_lookup(fid, full, edid) is not None


VMAD_FOUND = {}   # fid -> {"script","prop","fid","edid","full","secs"}
for _table in (ACTI, FURN):
    for _fid, _rec in _table.items():
        _vm = _rec.get("VMAD_Scripts") or ""
        if not _vm or _fid in discovered:
            continue
        if _fid in EXCLUDED or _fid in GOLD_MERGED or _fid in VARIANT_MERGED:
            continue
        _edid = _rec.get("FURN_EDID") or ""
        if _VMAD_SKIP_RE.match(_edid) or "nonplayer" in _edid.lower():
            continue
        _groups, _hit = [], None
        for _script, _prop, _sfid, _sedid in vmad_spells(_vm):
            _g, _secs, _sfull = spell_groups(_sfid)
            if _g:
                _groups += [g for g in _g if g not in _groups]
                _hit = _hit or {"script": _script, "prop": _prop, "fid": _sfid,
                                "edid": _sedid, "full": _sfull, "secs": _secs}
        if not _groups:
            continue
        _full = (_rec.get("FURN_FULL") or "").strip()
        if not has_obtain_route(_fid, NAME_OVERRIDES.get(_fid) or _full, _edid):
            print(f"  [INFO] script buff {_fid} {_edid} skipped — no COBJ / build list / ENTM")
            continue
        discovered[_fid] = set(_groups)
        VMAD_FOUND[_fid] = _hit
        print(f"  Script-cast buff: {_fid} {_edid} ({_full}) -> {', '.join(_groups)} "
              f"via {_hit['script']}::{_hit['prop']} = {_hit['edid']}")


# ------------------------------------------------ interaction type
# How the player uses the station, straight from the record:
#   FURN            -> the player enters the furniture and plays an animation
#                      (the AnimFurn* keyword names the animation set)
#   ACTI            -> instant: the buff is cast on the button press
#   ACTI + BlockPlayerActivation -> walk-through (a trigger fires it)
# machine_animates flags activators whose OBJECT animates (derby games, slot
# machines) — the player still does not.
_INT_CFG = _CFG.get("interaction", {})
_INT_LABELS = _INT_CFG.get("labels", {"animation": "Plays an animation",
                                      "instant": "Instant on activate",
                                      "walkthrough": "Walk-through"})
_INT_WALK_KW = {k.lower() for k in _INT_CFG.get("walkthrough_keywords", ["BlockPlayerActivation"])}
_INT_MACHINE_KW = {k.lower() for k in _INT_CFG.get("machine_anim_keywords", ["AllowNonActorToAnimateOnServer"])}
_INT_MACHINE_SCRIPTS = [s.lower() for s in _INT_CFG.get("machine_anim_scripts", ["RandomResultPowered"])]
_INT_ANIM_RE = re.compile(_INT_CFG.get("anim_keyword_re", r"^(ATX_)?Anims?Furn"), re.I)


def _kw_edids(rec):
    out = []
    for k, v in (rec or {}).items():
        if k.startswith("KW_") and v:
            bits = v.split(":")
            out.append(bits[1] if len(bits) > 1 else v)
    return out


def _interaction(kind, anim=None, machine=False):
    return {"type": kind, "label": _INT_LABELS.get(kind, kind),
            "anim_keyword": anim, "machine_animates": bool(machine)}


def interaction_for(fid):
    fr = FURN.get(fid)
    if fr is not None:
        anim = next((k for k in _kw_edids(fr) if _INT_ANIM_RE.match(k)), None)
        return _interaction("animation", anim)
    ar = ACTI.get(fid)
    if ar is not None:
        kws = {k.lower() for k in _kw_edids(ar)}
        vm = (ar.get("VMAD_Scripts") or "").lower()
        machine = bool(kws & _INT_MACHINE_KW) or any(s in vm for s in _INT_MACHINE_SCRIPTS)
        if kws & _INT_WALK_KW:
            return _interaction("walkthrough", None, machine)
        return _interaction("instant", None, machine)
    return None


# ---------------------------------------------------------------- build
items_out = []
ATX_SIBLING = {}   # fid -> the extra Atom Shop ENTM found for it
# Scoreboard / event items later sold in the Atomic Shop, each with its
# official source — see atom_shop_releases in data/camp/buff_stations.json.
ATOM_SHOP_RELEASES = _CFG.get("atom_shop_releases", {})

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
    season = SEASON_OVERRIDES.get(fid, season_from_edid(furn_edid, entm["EDID"] if entm else ""))

    premium = "Premium" in ((entm and entm["XALG_Flags"]) or "") or \
              "Premium" in ((fr and fr.get("XALG_Flags")) or "")
    f1 = "Fallout 1st" in ((fr and fr.get("XALG_Flags")) or "")
    tradeable = False if (premium or f1 or (furn_edid or "").upper().startswith(("ATX_", "SCORE_"))) else True
    if fid in TRADEABLE_OVERRIDES:
        tradeable = TRADEABLE_OVERRIDES[fid]

    how = spec.get("how") or auto_how(fid, furn_edid, entm, premium, season)
    status = STATUS_OVERRIDES.get(fid, "")
    # Merged gold-vendor route: scoreboard line first, then the gold bullion
    # line (vendor, reputation rank, cost).
    if fid in GOLD_HOW:
        how = f"{how}\n\n{GOLD_HOW[fid]}"
    if status:
        tradeable = False   # unreleased / cut: nothing to trade
    # Atom Shop re-release: a scoreboard / event item that ALSO has its own
    # plain ATX_ entitlement under the same name (e.g. Weight Bench, sold in
    # the Cash Plus Interest bundle alongside its Season 2 record). F1_ / TP_
    # entitlements are Fallout 1st claims and promo codes, not shop sales.
    if (not status and fid in ATOM_SHOP_RELEASES
            and "atom shop" not in (how or "").lower()):
        how = f"{how}\n\n{ATX_HOW}" if how and how != "—" else ATX_HOW
    if not status and "atom shop" not in (how or "").lower():
        _own = (entm or {}).get("FormID", "")
        for _e in ENTM_BY_FULL.get(display.strip().lower(), []):
            _ed = (_e.get("EDID") or "").upper()
            if (_e.get("FormID") != _own and _ed.startswith("ATX_")
                    and not _ed.startswith(("ATX_F1_", "ATX_TP_"))):
                how = f"{how}\n\n{ATX_HOW}" if how and how != "—" else ATX_HOW
                ATX_SIBLING[fid] = _e
                break
    obtain_routes = buff_obtain_routes(how, tradeable)
    output_info, output_rows = build_output(fid, groups)
    if fid in VARIANT_INFO:
        output_rows = ["Versions: " + ", ".join(VARIANT_INFO[fid]["versions"])] + output_rows
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
        if fid not in VMAD_FOUND:
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
    if fid in VMAD_FOUND:
        _v = VMAD_FOUND[fid]
        if not any(SPECIAL_KW[g][1].startswith(_v["fid"]) for g in g0):
            tech.append(f"Buff Spell: {_v['fid']} {_v['edid']}"
                        + (f" \u201c{_v['full']}\u201d" if _v['full'] else ""))
        tech.append(f"Buff Script: {_v['script']} ({_v['prop']})")
    if fid in GOLD_TECH:
        tech += GOLD_TECH[fid]
    if fid in VARIANT_INFO:
        tech += VARIANT_INFO[fid]["tech"]
    if fid in ATOM_SHOP_RELEASES:
        tech.append(f"Atom Shop Release: {ATOM_SHOP_RELEASES[fid]}")
    if fid in ATX_SIBLING:
        tech.append(f"Atom Shop ENTM: {ATX_SIBLING[fid]['FormID']} {ATX_SIBLING[fid]['EDID']}")
    lim_camp, lim_ws, lim_shares, lim_tech = build_limits_for(fid, display)
    tech += lim_tech
    _bi = [f"Build Limit CAMP: {lim_camp if lim_camp is not None else '—'}",
           f"Build Limit Workshop: {lim_ws if lim_ws is not None else '—'}",
           f"Shares a Build Limit with: {', '.join(lim_shares) if lim_shares else '—'}"]
    _pw = build_info_for(fid)
    if _pw:
        _bi.append(_pw)

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
        "outputInfo": output_info,
        "buildInfo": "\n".join(_bi),
        "craftingRequirements": crafting_arr,
        "technicalNotes": "\n".join(tech),
        "buffTypes": groups,
        # All items use the 4-sub-expand layout now — Well Tuned instruments
        # included (user requirement, 7 Jun 2026).
        "singleExpand": False,
        # Output stacking rows (rendered as aligned label/value rows).
        # Well Tuned stacks with other AP regen buffs but not with itself.
        "outputRows": output_rows,
        # "unreleased" / "cut" -> head pill; see status_overrides in the config.
        "status": status,
        "cutContent": False,
        # How the station is used: animation / instant / walk-through.
        "interaction": interaction_for(fid),
    })

# ----- aggregate bed entries -------------------------------------------------
BED_ENTRIES = _CFG["bed_entries"]
for fid, label, buff, spell, kwline, bkey in BED_ENTRIES:
    names = bed_names[bkey]
    if bkey == "comfy":
        bed_info = "Well Rested: +5% XP and +2 Agility"
        bed_rows = ["Duration: 2 hours (3 hours with the Homebody perk)", SOLO_ROW]
    else:
        bed_info, bed_rows = split_buff(buff)
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
        "imageUrl": (IMAGE_OVERRIDES.get(kwline.split(" ")[1].lower())
                     or IMG_BASE + label.lower().replace(" ", "_") + ".avif"),
        "outputInfo": bed_info,
        "outputRows": bed_rows + HOMEBODY_ROWS,
        "craftingRequirements": [],
        "technicalNotes": "\n".join(tech),
        "buffTypes": ["wellrested", "experience"],
        "singleExpand": False, "cutContent": False,
        # Sleeping in the bed is a furniture animation.
        "interaction": _interaction("animation"),
    })

# ------------------------------------------------- Gold Bullion (generative)
# Every station a gold vendor sells, resolved from the game data rather than
# the gold_merged/gold_how tables, which only ever covered the Weight Bench and
# the Antique Speed Bag. See src/gold_vendor.py for the ENTM -> plan chain.
_GV = gold_vendor.index()
_GV.apply_to_items(items_out, "buff station")
_GV.report_unstocked(items_out)

# ------------------------------------------------- fixed row schema
# Every item carries the SAME labelled rows in Output & Effects, Build
# Information and the record block, in the same order. A value the game data
# does not give us prints "—" rather than the row disappearing, so two items
# side by side never show different headings (user requirement, 23 Sep 2026).
# Item-specific facts that are not one of the fixed rows (variant list, ghoul
# note, cooldown) become plain sentences under the table — no extra headings.
OUTPUT_FIXED = ["Duration", "Applies To", "Stacks With Other Buffs", "Stacks On Itself"]
BUILD_FIXED  = ["Build Limit CAMP", "Build Limit Workshop", "Shares a Build Limit with",
                "Power Required", "Flamingo Units"]
TECH_FIXED   = ["EDID", "FormID", "ENTM EDID", "ENTM FormID", "Buff Keyword",
                "Buff Spell", "Buff Effect", "Duration Global", "Magnitude Global"]
TECH_ALIASES = {"FURN EDID": "EDID", "FURN FormID": "FormID",
                "Instrument Keyword": "Buff Keyword", "Bed Type Keyword": "Buff Keyword",
                "Keyword": "Buff Keyword", "Spell": "Buff Spell"}
for _s in STAT_NAME.values():
    TECH_ALIASES[f"{_s} Keyword"] = "Buff Keyword"
    TECH_ALIASES[f"{_s} Spell"] = "Buff Spell"
# Groups whose buff is one shared spell per type: using a second source of
# the same buff refreshes the timer rather than stacking, and it runs
# alongside any different buff.
SPELL_BUFF_GROUPS = set(SPECIAL_KW) | {"welltuned", "wellrested"}


def _split_row(line):
    if ": " not in line:
        return None, line
    k, v = line.split(": ", 1)
    return k.strip(), v.strip()


def _as_note(label, value):
    if label == "Versions":
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return f"Comes in {len(parts)} versions: {', '.join(parts)}."
    if label == "Ghoul Characters":
        return f"Ghoul characters: {value[:1].lower() + value[1:]}."
    if label == "Cooldown":
        return f"Cooldown of {value}."
    return f"{label}: {value}"


def normalize_fixed_rows(it):
    # Output & Effects
    vals, notes = {}, []
    for line in it.get("outputRows") or []:
        k, v = _split_row(line)
        if k in OUTPUT_FIXED:
            vals.setdefault(k, v)
        elif k and k.startswith("Homebody"):
            continue   # perk effect, not the item's; Comfy Beds keeps it in Duration
        elif k:
            notes.append(_as_note(k, v))
        elif v.strip():
            notes.append(v.strip())
    if set(it.get("buffTypes") or []) & SPELL_BUFF_GROUPS:
        vals.setdefault("Stacks With Other Buffs", "Yes")
        vals.setdefault("Stacks On Itself", "No")
    it["outputRows"] = [f"{k}: {vals.get(k) or '—'}" for k in OUTPUT_FIXED]
    it["outputNotes"] = notes

    # Build Information (Tradeable and Season are rendered from their own
    # fields; the renderer prints "—" for those on this page too)
    b = {}
    for line in str(it.get("buildInfo") or "").split("\n"):
        k, v = _split_row(line)
        if k:
            b.setdefault(k, v)
    it["buildInfo"] = "\n".join(f"{k}: {b.get(k) or '—'}" for k in BUILD_FIXED)

    # Record block: fixed core rows, then any extra records this item really
    # has (gold-vendor refs, version FURNs, the bed "Counts As" list).
    core, extras = {k: [] for k in TECH_FIXED}, []
    lines = str(it.get("technicalNotes") or "").split("\n")
    for i, line in enumerate(lines):
        if not line.strip() or line.startswith("Counts As"):
            extras += [l for l in lines[i:] if l.strip()]
            break
        k, v = _split_row(line)
        k = TECH_ALIASES.get(k, k)
        if k in core:
            if v not in core[k]:
                core[k].append(v)
        else:
            extras.append(line)
    out = [f"{k}: {', '.join(core[k]) or '—'}" for k in TECH_FIXED]
    it["technicalNotes"] = "\n".join(out + extras)


for _it in items_out:
    normalize_fixed_rows(_it)


# ------------------------------------------------- Rested family (generative)
# Output & Effects for every Rested / Well Rested source, read from the game
# data: the buff name (SPEL FULL), the XP line (MGEF DNAM), the duration
# (EFIT), the Homebody stat bonus (Fortify* effects on a spell the Homebody
# PERK references) and the CAMP-ally variants (Kindred Spirit / Lover's
# Embrace). Only the source -> spell link and the plain-English conditions
# are hand-kept, in data/camp/buff_stations.json (rested_* keys).
#
# SURV_WellRested2 carries paired 7200s / 10800s rows for both the XP and the
# Agility effect; the condition choosing between them is not in the export.
# The shorter row is used, which matches the in-game testing in Duchess's
# Well Rested guide (2 hours, Homebody adds +2 Agility for 2 hours).
SPEL_FX_PATH = _resolve_tsv("SPEL_EFFECTS_TSV", "SPEL_Export_*_EFFECTS.tsv", "SPEL_Export_EFFECTS.tsv")
MGEF_PATH    = _resolve_tsv("MGEF_TSV",         "MGEF_Export_*.tsv",         "MGEF_Export.tsv")
print(f"  SPEL effects: {SPEL_FX_PATH}\n  MGEF: {MGEF_PATH}")

SPEL_FX = {}
for r in rows(SPEL_FX_PATH):
    SPEL_FX.setdefault(r["SPEL_EDID"].strip(), []).append(r)
MGEF_DESC = {r["MGEF_FormID"].strip(): (r.get("DNAM_MagicItemDescription") or "").strip()
             for r in rows(MGEF_PATH)}


def _secs(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def fmt_duration(sec):
    if sec and sec % 3600 == 0:
        h = sec // 3600
        return f"{h} hour{'s' if h != 1 else ''}"
    m = round(sec / 60)
    return f"{m} minute{'s' if m != 1 else ''}"


def rested_spell(edid):
    fx = SPEL_FX.get(edid) or []
    if not fx:
        print(f"  [WARN] Rested spell {edid} not in the SPEL export")
        return None
    xp = [e for e in fx if "XP" in MGEF_DESC.get(e["EFID_MGEF_FormID"].strip(), "")]
    stats = {}
    for e in fx:
        full = (e.get("EFID_MGEF_FULL") or "").strip()
        mag = float(e.get("EFIT_Magnitude") or 0)
        if full.startswith("Fortify ") and mag > 0:
            stat = full[len("Fortify "):]
            d = _secs(e["EFIT_Duration"])
            if stat not in stats or d < stats[stat][1]:
                stats[stat] = (mag, d)
    x = min(xp, key=lambda e: _secs(e["EFIT_Duration"])) if xp else None
    return {
        "fid": fx[0]["SPEL_FormID"].strip(), "edid": edid,
        "name": (fx[0].get("SPEL_FULL") or "").strip() or edid,
        "xp": re.sub(r"\s*Bonus\s*", " ", MGEF_DESC.get(x["EFID_MGEF_FormID"].strip(), "")).strip() if x else "",
        "dur": _secs(x["EFIT_Duration"]) if x else 0,
        "mgef": f"{x['EFID_MGEF_FormID'].strip()} {x['EFID_MGEF_EDID'].strip()}" if x else "",
        "stats": stats,
    }


def _stat_text(stats):
    return " and ".join(f"+{int(m) if m == int(m) else m} {s}" for s, (m, _d) in stats.items())


# Homebody: the playable PERK whose references include a Rested spell.
HOMEBODY = None
_rested_edids = set(_CFG.get("rested_spells", {}).values())
for r in rows(PERK_PATH):
    if r["PERK_EDID"].startswith("zzz"):
        continue
    refs = [r.get(f"Ref_{i}", "") for i in range(1, 60)]
    spells = {x.split(":")[1] for x in refs if x.endswith(":SPEL") and x.count(":") == 2}
    if spells & _rested_edids:
        HOMEBODY = {"fid": r["PERK_FormID"], "edid": r["PERK_EDID"],
                    "name": r["FULL"].strip(), "spells": spells}
        break

RESTED_SPELLS = _CFG.get("rested_spells", {})
RESTED_ALLY   = _CFG.get("rested_ally_spells", [])
RESTED_ALLY_SOURCES = set(_CFG.get("rested_ally_sources", []))
RESTED_RULES  = _CFG.get("rested_rules", "")
BED_KEY_BY_FID = {e[0]: e[5] for e in _CFG.get("bed_entries", [])}


def _homebody_note(sp):
    if HOMEBODY and sp["stats"] and sp["edid"] in HOMEBODY["spells"]:
        d = min(dd for _m, dd in sp["stats"].values())
        return f"With the {HOMEBODY['name']} perk card equipped: also {_stat_text(sp['stats'])} for {fmt_duration(d)}."
    return ""


def _set_row(lines, label, value):
    return [f"{label}: {value}" if l.startswith(label + ": ") else l for l in lines]


def apply_rested(it):
    key = BED_KEY_BY_FID.get(it["formId"]) or ("furniture" if "wellrested" in (it.get("buffTypes") or []) else None)
    if not key or key not in RESTED_SPELLS:
        return
    sp = rested_spell(RESTED_SPELLS[key])
    if not sp:
        return
    main = f"{sp['name']}: {sp['xp']}"
    if sp["stats"] and not (HOMEBODY and sp["edid"] in HOMEBODY["spells"]):
        main += f" and {_stat_text(sp['stats'])}"
    it["outputInfo"] = main
    it["outputRows"] = _set_row(_set_row(_set_row(_set_row(it["outputRows"],
        "Duration", fmt_duration(sp["dur"])),
        "Applies To", "You only (solo buff)"),
        "Stacks With Other Buffs", "Yes"),
        "Stacks On Itself", "No")

    notes = [n for n in [_homebody_note(sp)] if n]
    tech_extra = []
    if key in RESTED_ALLY_SOURCES:
        for a in RESTED_ALLY:
            asp = rested_spell(a["spell"])
            if not asp:
                continue
            line = (f"{asp['name']}: {asp['xp']} for {fmt_duration(asp['dur'])} "
                    f"when you sleep in a bed you own while {a['when']}.")
            if asp["stats"] and HOMEBODY and asp["edid"] in HOMEBODY["spells"]:
                line += f" With {HOMEBODY['name']}: also {_stat_text(asp['stats'])}."
            notes.append(line)
            tech_extra.append(f"Ally Spell: {asp['fid']} {asp['edid']} “{asp['name']}”")
    if RESTED_RULES:
        notes.append(RESTED_RULES)
    it["outputNotes"] = notes + [n for n in it.get("outputNotes") or [] if n not in notes]

    t = str(it.get("technicalNotes") or "").split("\n")
    t = _set_row(t, "Buff Spell", f"{sp['fid']} {sp['edid']} “{sp['name']}”")
    t = _set_row(t, "Buff Effect", f"{sp['mgef']} — {sp['xp']}, {sp['dur']}s")
    if HOMEBODY and sp["edid"] in HOMEBODY["spells"]:
        tech_extra.insert(0, f"Homebody Perk: {HOMEBODY['fid']} {HOMEBODY['edid']}")
    # extras go straight after the fixed core (before a bed's Counts As list)
    cut = len(TECH_FIXED)
    it["technicalNotes"] = "\n".join(t[:cut] + tech_extra + t[cut:])


for _it in items_out:
    apply_rested(_it)

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


# ------------------------------------------ Well Rested Buffs guide (generative)
# dist/well-rested-buffs.json feeds the SPECIALs & Stats - Well Rested Buffs
# guide (df-bnb-well-rested-buffs.js). It reuses the Rested family resolved
# above, so the guide and the Buff Stations page can never disagree:
#   - buff name / XP / duration / Homebody stats  -> SPEL + MGEF + PERK
#   - which bed types give which buff             -> rested_spells + bed_entries
#   - the Well Rested furniture list              -> the live wellrested items
#   - Kindred Spirit / Lover's Embrace allies     -> dist/allies.json (live
#     allies, split on the romanceable flag that builder reads from the TSVs)
# The only hand-kept part is rested_guide_no_ally_buff (script-decided).
# CI runs build_allies_pets_weather_json.py before this script, so
# dist/allies.json is current by the time we read it.
WR_OUT = OUT_DIR / "well-rested-buffs.json"


def _wr_buff(edid):
    sp = rested_spell(edid)
    if not sp:
        return None
    out = {
        "name": sp["name"],
        "xp": sp["xp"],
        "duration": fmt_duration(sp["dur"]),
        "durationSec": sp["dur"],
        "spell": f"{sp['fid']} {sp['edid']}",
        "homebody": None,
    }
    if sp["stats"] and HOMEBODY and sp["edid"] in HOMEBODY["spells"]:
        d = min(dd for _m, dd in sp["stats"].values())
        out["homebody"] = {
            "stats": [{"stat": s, "value": int(m) if m == int(m) else m}
                      for s, (m, _dd) in sp["stats"].items()],
            "text": _stat_text(sp["stats"]),
            "duration": fmt_duration(d),
            "durationSec": d,
        }
    return out


def _wr_bed_types(edid):
    return [e[1] for e in _CFG.get("bed_entries", [])
            if RESTED_SPELLS.get(e[5]) == edid]


_wr_no_buff = dict(_CFG.get("rested_guide_no_ally_buff", {}))
_wr_allies = {"kindred": [], "lovers": [], "none": []}
_allies_path = OUT_DIR / "allies.json"
if _allies_path.exists():
    for a in json.loads(_allies_path.read_text(encoding="utf-8")).get("items", []):
        if a.get("cutContent"):
            continue
        name = (a.get("displayName") or "").strip()
        if not name:
            continue
        if name in _wr_no_buff:
            _wr_allies["none"].append(name)
        elif (a.get("buffsAndEffects") or {}).get("romanceable"):
            _wr_allies["lovers"].append(name)
        else:
            _wr_allies["kindred"].append(name)
    for k in _wr_allies:
        _wr_allies[k].sort(key=str.lower)
else:
    print(f"  [WARN] {_allies_path} missing — ally lists left empty")

_wr_rested = _wr_buff(RESTED_SPELLS.get("sleepingbag", ""))
_wr_well = _wr_buff(RESTED_SPELLS.get("furniture", ""))
if _wr_rested:
    _wr_rested["bedTypes"] = _wr_bed_types(RESTED_SPELLS.get("sleepingbag", ""))
if _wr_well:
    _wr_well["bedTypes"] = _wr_bed_types(RESTED_SPELLS.get("furniture", ""))
    _wr_well["furniture"] = sorted(
        [{"name": it["displayName"], "formId": it["formId"], "imageUrl": it.get("imageUrl", "")}
         for it in items_out
         if "wellrested" in (it.get("buffTypes") or [])
         and not it.get("cutContent")
         and it["formId"] not in BED_KEY_BY_FID],
        key=lambda x: x["name"].lower())

_wr_ally_keys = {"COMP_WellRested3_KindredSpirit": ("kindredSpirit", "kindred"),
                 "COMP_WellRested3_LoversEmbrace": ("loversEmbrace", "lovers")}
wr = {
    "homebody": ({"name": HOMEBODY["name"], "formId": HOMEBODY["fid"], "edid": HOMEBODY["edid"]}
                 if HOMEBODY else None),
    "rested": _wr_rested,
    "wellRested": _wr_well,
}
for a in RESTED_ALLY:
    key, bucket = _wr_ally_keys.get(a["spell"], (None, None))
    if not key:
        continue
    b = _wr_buff(a["spell"])
    if b:
        b["allies"] = _wr_allies[bucket]
    wr[key] = b
wr["noAllyBuff"] = _wr_allies["none"]

WR_OUT.write_text(json.dumps(wr, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Wrote Well Rested Buffs guide data -> {WR_OUT}")
for k in ("rested", "wellRested", "kindredSpirit", "loversEmbrace"):
    b = wr.get(k) or {}
    print(f"  {k}: {b.get('name')} {b.get('xp')} {b.get('duration')}"
          f" | Homebody: {(b.get('homebody') or {}).get('text', '-')}"
          f" | allies: {len(b.get('allies', []))} furniture: {len(b.get('furniture', []))}")
