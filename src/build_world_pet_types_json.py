#!/usr/bin/env python3
"""
build_world_pet_types_json.py

Reads the xEdit TSV exports and builds dist/world_pet_types.json — the data feed
for the World Pets → Types page (/df/pets/world-pets/world-pet-types/).

Architecture (locked, matches every other camp page):
  xEdit TSV exports  →  this script  →  dist/world_pet_types.json  →  df-bnb-world-pets.js
The JSON holds DATA FIELDS only. All prose / rendered HTML lives in the renderer
as templates — never in here.

Inputs (from tsv/ in the repo):
  NPC_Export_*.tsv   — skin actors (CAMPPets_Actor_*). Source of the species/skin list.
  ENTM_Export_*.tsv  — store entitlements. Per-skin store DESC text.
  ALCH_Export_*.tsv (+ _Effects) — the six pet foods + effect magnitudes.
  COBJ_Export_*.tsv  — food crafting recipes (FVPA ingredients).
  GLOB_Export_*.tsv  — buff magnitudes (WorldPets_Buff_*), unlock levels, gift interval,
                       SURV food-effect magnitude globals.

Curve-table values (base health, Pet Combat Prowess multipliers, total Pet XP) are
resolved from data/curvetables/json/WorldPets/ — verified against those CURV files and
embedded below as constants (cited inline). Tier labels are provisional (pre-release
June 2026 feature).

Usage:
  python build_world_pet_types_json.py [--tsv-dir tsv] [--out-dir dist]
"""

import csv
import glob
import json
import os
import re
import argparse
from pathlib import Path

from patchlog_utils import write_patchlog_feed
import tsv_source          # one resolver for every export selection

# ---------------------------------------------------------------------------
# CLI args + env var resolution (matches dfbnb build-workflow pattern)
#
# TSV paths resolved in priority order:
#   1. Explicit env vars set by the workflow picker (NPC_TSV, ENTM_TSV, …)
#   2. Newest-by-Month_Year glob inside --tsv-dir
#   3. Bare filename fallback inside --tsv-dir (local dev)
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("--tsv-dir", default="tsv",  help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
parser.add_argument("--pts", action="store_true", help="Build PTS variant (reads from tsv/pts/)")
args = parser.parse_args()

TSV_DIR = Path(args.tsv_dir)
PTS_MODE = args.pts
if PTS_MODE:
    TSV_DIR = TSV_DIR / "pts"
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)


_MONTH_ORD = {"jan": 1, "feb": 2, "mar": 3, "march": 3, "apr": 4, "april": 4,
              "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8,
              "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def _tsv_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def _newest_glob(pattern, exclude_suffix=None):
    matches = glob.glob(pattern)
    if exclude_suffix:
        matches = [m for m in matches if not m.lower().endswith(exclude_suffix.lower())]
    if not matches:
        return None
    return sorted(matches, key=_tsv_date_key)[-1]


def _resolve_tsv(env_var, glob_pattern, fallback_name, exclude_suffix=None):
    """Resolve a TSV path via env var → glob → bare fallback."""
    v = os.environ.get(env_var, "").strip()
    if v and Path(v).exists():
        return Path(v)
    found = _newest_glob(str(TSV_DIR / glob_pattern), exclude_suffix=exclude_suffix)
    if found:
        return Path(found)
    fallback = TSV_DIR / fallback_name
    if fallback.exists():
        return fallback
    raise FileNotFoundError(
        f"Cannot find TSV for {env_var}. "
        f"Tried env var, glob '{glob_pattern}', and '{fallback_name}' in {TSV_DIR}"
    )


# The NPC actor export and its _PRPS / _Refs siblings share the NPC_Export_ prefix;
# exclude the siblings so the bare actor table is picked.
NPC_PATH  = _resolve_tsv("NPC_TSV",  "NPC_Export_*.tsv",  "NPC_Export.tsv",
                         exclude_suffix="_prps.tsv")
ENTM_PATH = _resolve_tsv("ENTM_TSV", "ENTM_Export_*.tsv", "ENTM_Export.tsv")
ALCH_PATH = _resolve_tsv("ALCH_TSV", "ALCH_Export_*.tsv", "ALCH_Export.tsv",
                         exclude_suffix="_effects.tsv")
ALCH_EFFECTS_PATH = _resolve_tsv("ALCH_EFFECTS_TSV", "ALCH_Export_*_Effects.tsv",
                                 "ALCH_Export_Effects.tsv")
COBJ_PATH = _resolve_tsv("COBJ_TSV", "COBJ_Export_*.tsv", "COBJ_Export.tsv")
GLOB_PATH = _resolve_tsv("GLOB_TSV", "GLOB_Export_*.tsv", "GLOB_Export.tsv")

# Optional TSVs — combat data (WEAP DNAM for per-species attack stats, EMOT for commands)
try:
    WEAP_DNAM_PATH = _resolve_tsv("WEAP_DNAM_TSV", "WEAP_Export_*_DNAM.tsv",
                                  "WEAP_Export_DNAM.tsv")
except FileNotFoundError:
    WEAP_DNAM_PATH = None
try:
    EMOT_PATH = _resolve_tsv("EMOT_TSV", "EMOT_Export_*.tsv", "EMOT_Export.tsv")
except FileNotFoundError:
    EMOT_PATH = None

# The _NPC_PATH glob can still grab NPC_Export_*_Refs.tsv (doesn't end _prps). Guard:
if NPC_PATH.name.lower().endswith(("_refs.tsv", "_prps.tsv")):
    _bare = _newest_glob(str(TSV_DIR / "NPC_Export_*.tsv"))
    # fall through to the first NPC_Export that is neither _Refs nor _PRPS
    cands = [p for p in glob.glob(str(TSV_DIR / "NPC_Export_*.tsv"))
             if not p.lower().endswith(("_refs.tsv", "_prps.tsv"))]
    if cands:
        NPC_PATH = Path(sorted(cands, key=_tsv_date_key)[-1])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_tsv(path):
    rows = []
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            rows.append({k: ((v or "").strip().strip('"'))
                         for k, v in row.items() if k is not None})
    return rows


def fmt_num(raw):
    """'2.000000' -> 2 (int), '0.400000' -> 0.4 (float), '' -> None."""
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        f = float(s)
        return int(f) if f == int(f) else f
    except (ValueError, TypeError):
        return None


print("Loading TSVs…")
print(f"  NPC:  {NPC_PATH}")
print(f"  ENTM: {ENTM_PATH}")
print(f"  ALCH: {ALCH_PATH}")
print(f"  ALCH effects: {ALCH_EFFECTS_PATH}")
print(f"  COBJ: {COBJ_PATH}")
print(f"  GLOB: {GLOB_PATH}")
if WEAP_DNAM_PATH:
    print(f"  WEAP DNAM: {WEAP_DNAM_PATH}")
if EMOT_PATH:
    print(f"  EMOT: {EMOT_PATH}")

npc_rows          = load_tsv(NPC_PATH)
entm_rows         = load_tsv(ENTM_PATH)
alch_rows         = load_tsv(ALCH_PATH)
alch_effect_rows  = load_tsv(ALCH_EFFECTS_PATH)
cobj_rows         = load_tsv(COBJ_PATH)
glob_rows         = load_tsv(GLOB_PATH)

# GLOB lookup: EDID (lower) -> numeric FLTV
glob_by_edid = {}
for r in glob_rows:
    ed = (r.get("EDID") or "").strip()
    v = fmt_num(r.get("FLTV"))
    if ed and v is not None:
        glob_by_edid[ed.lower()] = v


def glob_val(edid, default=None):
    return glob_by_edid.get(str(edid).lower(), default)


# WEAP DNAM — per-species attack stats (speed, reach, delay)
weap_dnam_rows = load_tsv(WEAP_DNAM_PATH) if WEAP_DNAM_PATH else []
weap_by_edid = {}
for r in weap_dnam_rows:
    ed = (r.get("WEAP_EDID") or "").strip()
    if ed:
        weap_by_edid[ed.lower()] = r

# EMOT — pet command emotes
emot_rows = load_tsv(EMOT_PATH) if EMOT_PATH else []


# ---------------------------------------------------------------------------
# OBTAIN ROUTES — site-standard fixed 9-route shape (camp-item-expands spec)
# ---------------------------------------------------------------------------
OBTAIN_ROUTE_ORDER = [
    "Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
    "Limited Time Bundle", "Events & Activities", "Quests", "Challenges",
]

ATX_LINE = "Purchased from the Atomic Shop — sold individually or as part of a bundle."


def make_obtain_routes(populated):
    """Build the fixed 9-route array. `populated` maps route -> (lines, tradeable,
    dropRate). Routes not in the map are emitted dimmed (N/A, no pills)."""
    routes = []
    for name in OBTAIN_ROUTE_ORDER:
        if name in populated:
            lines, tradeable, drop = populated[name]
            lines = [ln for ln in lines if str(ln).strip()]
        else:
            lines, tradeable, drop = [], None, None
        routes.append({
            "route":     name,
            "populated": bool(lines),
            "lines":     lines,
            "tradeable": tradeable if lines else None,
            "dropRate":  drop if lines else None,
        })
    return routes


# ---------------------------------------------------------------------------
# ENTM description join
# ---------------------------------------------------------------------------
SPECIES_SEG_RE = re.compile(r"_(Cat|Dog|RadHog|Radhog|Deathclaw)_", re.IGNORECASE)

entm_by_edid_lc = {}
for r in entm_rows:
    ed = (r.get("EDID") or "").strip()
    if ed:
        entm_by_edid_lc.setdefault(ed.lower(), r)

# Only CAMP-pet skin entitlements are valid DESC sources — never apparel /
# furniture / idle-furniture / emote / dev records.
def _is_pet_skin_entm(edid):
    e = edid.lower()
    if "camppets" not in e:
        return False
    if any(bad in e for bad in ("apparel", "furniture", "idlefurniture",
                                "emote", "devonly", "placeholder")):
        return False
    return True


pet_entm_rows = [r for r in entm_rows if _is_pet_skin_entm(r.get("EDID", ""))]


def clean_pet_desc(raw):
    """Clean an ENTM DESC for display: cut the store boilerplate from the
    ' - C.A.M.P.' marketing tag onward, drop the outdated legendary-items line,
    collapse runs of spaces."""
    t = (raw or "").strip()
    if not t or t.upper() == "TBD":
        return ""
    m = re.search(r"\s+-\s+C\.?A\.?M\.?P\.?", t)
    if m:
        t = t[:m.start()]
    t = t.replace("Generates 1, 2, or 3 Star Legendary Items", "")
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def resolve_desc(actor_edid, actor_full):
    """Resolve a skin's store description from ENTM. Chain:
      1. derived EDID  (CAMPPets_Actor_ -> ENTM_CAMP_CAMPPets_), with ATX_ toggle
      2. strip the species segment from the derived EDID
      3. ENTM EDID ends with the actor's last skin token
      4. exact FULL display-name match (pet-skin ENTMs only)
    Returns (desc_text, matched_edid) — both '' when nothing resolves."""
    derived = actor_edid.replace("CAMPPets_Actor_", "ENTM_CAMP_CAMPPets_")

    # Candidate EDIDs to try directly (case-insensitive).
    cands = [derived]
    # toggle leading ATX_ either way
    if derived.lower().startswith("atx_"):
        cands.append(derived[4:])
    else:
        cands.append("ATX_" + derived)
    # 2) strip species segment, e.g. ..._Cat_Lykoi -> ..._Lykoi
    stripped = SPECIES_SEG_RE.sub("_", derived)
    if stripped != derived:
        cands.append(stripped)
        if stripped.lower().startswith("atx_"):
            cands.append(stripped[4:])
        else:
            cands.append("ATX_" + stripped)

    for c in cands:
        row = entm_by_edid_lc.get(c.lower())
        if row:
            d = clean_pet_desc(row.get("DESC", ""))
            if d:
                return d, row.get("EDID", "")

    # 3) endswith the last skin token
    last_tok = actor_edid.split("_")[-1].lower()
    if len(last_tok) >= 4:
        for row in pet_entm_rows:
            if row["EDID"].lower().endswith(last_tok):
                d = clean_pet_desc(row.get("DESC", ""))
                if d:
                    return d, row.get("EDID", "")

    # 4) exact FULL match (skin entitlements only)
    full = (actor_full or "").strip().lower()
    if full:
        for row in pet_entm_rows:
            if (row.get("FULL", "").strip().lower() == full):
                d = clean_pet_desc(row.get("DESC", ""))
                if d:
                    return d, row.get("EDID", "")

    return "", ""


# ---------------------------------------------------------------------------
# SPECIES + SKIN discovery from NPC actors
# ---------------------------------------------------------------------------
# Filter: CAMPPets_Actor_{Cat|Dog|RadHog|Deathclaw}_*  ; exclude debug/sandbox/
# cut (zzz) actors and FULL=TBD/empty. Dedupe: when both a _Template and a
# _Standard actor share a species (Radhog), the _Standard is the canonical base
# and the _Template is dropped — Deathclaw keeps its _Template (no _Standard).
ACTOR_RE = re.compile(r"CAMPPets_Actor_(Cat|Dog|RadHog|Radhog|Deathclaw)_",
                      re.IGNORECASE)

# Manual override (camp-item-expands philosophy: tables hold overrides only).
# The two skins that have no own store entitlement and ARE the species' bundled
# default — shown with a base-skin note instead of fabricated store text, and
# their obtain routes left dimmed. Keyed by actor EDID (lower).
TRUE_BASE_ACTORS = {
    "camppets_actor_cat_tabby",            # Grey Tabby Cat
    "atx_camppets_actor_deathclaw_template",  # Deathclaw (base/template)
}

BASE_OBTAIN_NOTE = ("Base skin — included with the C.A.M.P. Pets system; "
                    "not sold separately.")
BASE_DESC_NOTE = ("The default skin for this species — bundled with the "
                  "C.A.M.P. Pets system rather than sold in the store.")


def species_key(raw):
    r = raw.lower()
    if r == "radhog":
        return "radhog"
    return r  # cat / dog / deathclaw


def classify_source(edid):
    """(source, season|None) from the actor EDID prefix."""
    m = re.match(r"SCORE_S(\d+)_", edid, re.IGNORECASE)
    if m:
        return "Scoreboard", int(m.group(1))
    if re.match(r"ATX_", edid, re.IGNORECASE):
        return "Atom Shop", None
    return "Base", None


# Collect raw skin actors per species
raw_skins = {}   # species_key -> list of actor rows (dicts with form/edid/full)
for r in npc_rows:
    edid = r.get("EDID", "")
    m = ACTOR_RE.search(edid)
    if not m:
        continue
    el = edid.lower()
    if any(bad in el for bad in ("_debug_", "_sandbox", "sandbox")):
        continue
    if el.startswith("zzz"):
        continue
    full = (r.get("FULL", "") or "").strip()
    if not full or full.upper() == "TBD":
        continue
    sk = species_key(m.group(1))
    raw_skins.setdefault(sk, []).append({
        "formId": r.get("FormID", "").strip(),
        "edid":   edid,
        "full":   full,
    })

# Dedupe _Template when a _Standard sibling exists for the species.
for sk, lst in raw_skins.items():
    has_standard = any(s["edid"].lower().endswith("_standard") for s in lst)
    if has_standard:
        raw_skins[sk] = [s for s in lst if not s["edid"].lower().endswith("_template")]


# ---------------------------------------------------------------------------
# Static species metadata (signature buff identities, diet, activity, haul).
# Magnitudes are read from GLOB below — never hardcoded here.
# ---------------------------------------------------------------------------
DEATHCLAW_MEAT_HAUL = [
    "Dogmeat", "Cat Meat", "Mole Rat Meat", "Bloodbug Meat", "Squirrel",
    "Radroach Meat", "Bloatfly Meat", "Mirelurk Meat", "Iguana", "Stingwing",
    "Glowing Meat", "Rad Rat Meat", "Rad Ant Meat", "Chicken", "Rabbit",
    "Hermit Crab Meat", "Wolf Meat",
]

SPECIES_META = {
    "cat": {
        "name": "Cat", "diet": "Carnivore", "buffSpell": "Cat Pet",
        "sig1": "Bait Finder", "sig2": "Mega Bits",
        "sig1Glob": "Cat_Bait", "sig2Glob": "Cat_MegaBits",
        "activity": "Catch Fish with an active Pet Cat",
        "treasure": {"item": "Improved Fishing Bait", "isList": False},
        "foodTypes": ["Meat"],
    },
    "deathclaw": {
        "name": "Deathclaw", "diet": "Carnivore", "buffSpell": "Deathclaw Pet",
        "sig1": "Meat Lover", "sig2": "Fun-Festation",
        "sig1Glob": "Deathclaw_Meat", "sig2Glob": "Deathclaw_Infestation",
        "activity": "Kill Infestation enemies with an active Pet Deathclaw",
        "treasure": {"item": "random Meat", "isList": True, "list": DEATHCLAW_MEAT_HAUL},
        "foodTypes": ["Meat"],
    },
    "dog": {
        "name": "Dog", "diet": "Omnivore", "buffSpell": "Dog Pet",
        "sig1": "Stimpak Fetcher", "sig2": "Bounty Sniffer",
        "sig1Glob": "Dog_Stimpak", "sig2Glob": "Dog_Bounty",
        "activity": "Complete Bounty Hunts with an active Pet Dog",
        "treasure": {"item": "Stimpak", "isList": False},
        "foodTypes": ["Meat", "Plant"],
    },
    "radhog": {
        "name": "Radhog", "diet": "Omnivore", "buffSpell": "Radhog Pet",
        "sig1": "Rad Magnet", "sig2": "Nuke Harvest",
        "sig1Glob": "Radhog_Rad", "sig2Glob": "Radhog_NukeHarvest",
        "activity": "Collect Flux with an active Pet Radhog",
        "treasure": {"item": "RadAway", "isList": False},
        "foodTypes": ["Meat", "Plant"],
    },
}

SPECIES_ABC = ["cat", "deathclaw", "dog", "radhog"]


# ---------------------------------------------------------------------------
# COMBAT — per-species WEAP stats + command emotes from EMOT
# ---------------------------------------------------------------------------

# WEAP EDID -> species key mapping
WEAP_SPECIES_MAP = {
    "crunarmedworldpet_cat":       "cat",
    "crunarmedworldpet_dog":       "dog",
    "crunarmedworldpet_radhog":    "radhog",
    "crunarmedworldpet_deathclaw": "deathclaw",
}

# Attack type labels (from Bethesda article: dogs bite, deathclaws/cats scratch, radhogs ram)
ATTACK_LABELS = {
    "cat":       "Scratch",
    "dog":       "Bite",
    "radhog":    "Ram",
    "deathclaw": "Scratch",
}

# NPC DNAM_CalcHealth base health multipliers (from NPC records)
NPC_BASE_HEALTH = {
    "cat":       15,
    "dog":       190,
    "radhog":    450,
    "deathclaw": 450,
}


def build_species_combat():
    """Per-species combat stats from WEAP DNAM TSV."""
    combat = {}
    for edid_lc, sk in WEAP_SPECIES_MAP.items():
        row = weap_by_edid.get(edid_lc)
        if not row:
            continue
        combat[sk] = {
            "weapFormId":    (row.get("WEAP_FormID") or "").strip(),
            "weapEdid":      (row.get("WEAP_EDID") or "").strip(),
            "attackType":    ATTACK_LABELS.get(sk, "Melee"),
            "speed":         fmt_num(row.get("DNAM_Speed")),
            "reach":         fmt_num(row.get("DNAM_Reach")),
            "minRange":      fmt_num(row.get("DNAM_MinRange")),
            "maxRange":      fmt_num(row.get("DNAM_MaxRange")),
            "attackDelay":   fmt_num(row.get("DNAM_AttackDelaySeconds")),
            "critDmgMult":   fmt_num(row.get("DNAM_DamageOutOfRangeMult")),
            "npcBaseHealth": NPC_BASE_HEALTH.get(sk),
        }
    return combat


def build_commands():
    """Pet command emotes from EMOT TSV."""
    cmds = []
    for r in emot_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid.startswith("WorldPets_Emote_Command_"):
            continue
        cmd_name = edid.replace("WorldPets_Emote_Command_", "")
        cmds.append({
            "formId":    (r.get("FormID") or "").strip(),
            "edid":      edid,
            "name":      (r.get("FULL") or "").strip(),
            "animation": (r.get("SNAM") or "").strip(),
            "command":   cmd_name,
        })
    cmds.sort(key=lambda c: c["formId"])
    return cmds


def buff_ranks(glob_token, n=3):
    """[rank1, rank2, rank3] from WorldPets_Buff_{token}01/02/03 GLOBs."""
    out = []
    for i in range(1, n + 1):
        v = glob_val(f"WorldPets_Buff_{glob_token}0{i}")
        out.append(v)
    return out


def build_species():
    species_combat = build_species_combat()
    species = []
    for sk in SPECIES_ABC:
        meta = SPECIES_META[sk]
        skins_raw = raw_skins.get(sk, [])

        skins = []
        for s in sorted(skins_raw, key=lambda x: x["full"].lower()):
            edid = s["edid"]
            el = edid.lower()
            is_true_base = el in TRUE_BASE_ACTORS
            source, season = classify_source(edid)
            if is_true_base:
                source, season = "Base", None

            # description
            if is_true_base:
                desc, _matched = BASE_DESC_NOTE, ""
            else:
                desc, _matched = resolve_desc(edid, s["full"])
                if not desc:
                    desc = BASE_DESC_NOTE  # graceful fallback, never fabricate

            # obtain routes — populate exactly the source route (all bound).
            obtain_note = ""
            if source == "Scoreboard" and season:
                routes = make_obtain_routes({
                    "Scoreboard": ([f"Purchase with tickets from the Season {season} Scoreboard."],
                                   False, "N/A")})
            elif source == "Atom Shop":
                routes = make_obtain_routes({"Atom Shop": ([ATX_LINE], False, "N/A")})
            else:  # Base — all dimmed + a base note
                routes = make_obtain_routes({})
                obtain_note = BASE_OBTAIN_NOTE

            skin = {
                "name":         s["full"],
                "formId":       s["formId"],
                "edid":         edid,
                "obtainSource": source,
                "tradeable":    False,
                "description":  desc,
                "obtainRoutes": routes,
            }
            if season:
                skin["seasonNumber"] = season
            if obtain_note:
                skin["obtainNote"] = obtain_note
            skins.append(skin)

        species.append({
            "key":       sk,
            "name":      meta["name"],
            "diet":      meta["diet"],
            "buffSpell": meta["buffSpell"],
            "sig1":      meta["sig1"],
            "sig2":      meta["sig2"],
            "sig1Mag":   buff_ranks(meta["sig1Glob"]),
            "sig2Mag":   buff_ranks(meta["sig2Glob"]),
            "activity":  meta["activity"],
            "treasure":  dict(meta["treasure"], qtyByRank=[2, 4, 6]),
            "foodTypes": meta["foodTypes"],
            "skins":     skins,
            "combat":    species_combat.get(sk, {}),
        })
    return species


# ---------------------------------------------------------------------------
# FOODS — ALCH + ALCH effects + COBJ recipes
# ---------------------------------------------------------------------------
INGREDIENT_ALIASES = {"Molerat": "Mole Rat"}


def pretty_ingredient(token):
    t = re.sub(r"^c_", "", token)
    t = re.sub(r"^CookingFlavor_", "", t)
    t = re.sub(r"(VegetableFruit|Vegetable)$", "", t)
    t = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", t)
    for k, v in INGREDIENT_ALIASES.items():
        t = t.replace(k, v)
    return t.strip()


def fvpa_ingredients(fvpa):
    out = []
    for chunk in str(fvpa or "").split("|"):
        bits = chunk.strip().split(":")
        if len(bits) < 2 or not bits[0].strip():
            continue
        name = pretty_ingredient(bits[0].strip())
        try:
            qty = int(float(bits[1]))
        except (ValueError, TypeError):
            qty = 1
        if name:
            out.append({"name": name, "qty": qty})
    return out


# COBJ recipe by produced-item EDID (CNAM_EDID)
cobj_by_cnam_edid = {}
for r in cobj_rows:
    ce = (r.get("CNAM_EDID") or "").strip()
    if ce and "PetFood" in ce:
        cobj_by_cnam_edid[ce] = r

# ALCH effect magnitude globals — resolve the per-effect magnitude from the
# MAGG_GLOB / EFIT value. These read the same four SURV globals for every food.
RESTORE_HUNGER = glob_val("SURV_Food_RestoreHunger_Mag_2_Small")   # 360
HEAL_PER_SEC   = glob_val("SURV_Food_Heal_Mag_2_Small")            # 0.4
HEAL_DUR_SEC   = glob_val("SURV_Food_Heal_Dur_Standard")           # 25
RADIATION      = glob_val("SURV_Food_RadiationDamage_Mag_0_Token") # 1


def food_type_from_keywords(kw_flat):
    if "PETS_PetFoodType_Meat" in kw_flat:
        return "Meat"
    if "PETS_PetFoodType_Plant" in kw_flat:
        return "Plant"
    return ""


def build_foods():
    foods = []
    for r in alch_rows:
        edid = (r.get("ALCH_EDID") or "").strip()
        if not re.match(r"^PETS_PetFood_(Meat|Plant)\d$", edid):
            continue
        kw = r.get("Keywords_Flat", "")
        ftype = food_type_from_keywords(kw)
        cobj = cobj_by_cnam_edid.get(edid, {})
        heal_total = None
        if HEAL_PER_SEC is not None and HEAL_DUR_SEC is not None:
            ht = HEAL_PER_SEC * HEAL_DUR_SEC
            heal_total = int(ht) if ht == int(ht) else round(ht, 2)
        foods.append({
            "name":    r.get("FULL", "").strip(),
            "type":    ftype,
            "formId":  r.get("ALCH_FormID", "").strip(),
            "edid":    edid,
            "cobj":    cobj.get("COBJ_FormID", "").strip(),
            "ingredients": fvpa_ingredients(cobj.get("FVPA", "")),
            "effects": {
                "restoreHunger": RESTORE_HUNGER,
                "healPerSec":    HEAL_PER_SEC,
                "healDurSec":    HEAL_DUR_SEC,
                "healTotal":     heal_total,
                "radiation":     RADIATION,
            },
            "sell": fmt_num(r.get("Value")),
        })
    # Stable order: Meat 1-3, then Plant 1-3 (by EDID).
    foods.sort(key=lambda f: f["edid"])
    foods.sort(key=lambda f: (0 if f["type"] == "Meat" else 1))
    return foods


# ---------------------------------------------------------------------------
# BUFFS (all-pet) + STATS — magnitudes from GLOB, curve values cited from CURV
# ---------------------------------------------------------------------------
def build_buffs():
    return {
        # Signature buff unlock levels — WorldPets_PetLevelling_Level_PRK_Buff0{n}Rank0{r}
        "signatureUnlockLevels": {
            "sig1": [glob_val("WorldPets_PetLevelling_Level_PRK_Buff01Rank01"),
                     glob_val("WorldPets_PetLevelling_Level_PRK_Buff01Rank02"),
                     glob_val("WorldPets_PetLevelling_Level_PRK_Buff01Rank03")],
            "sig2": [glob_val("WorldPets_PetLevelling_Level_PRK_Buff02Rank01"),
                     glob_val("WorldPets_PetLevelling_Level_PRK_Buff02Rank02"),
                     glob_val("WorldPets_PetLevelling_Level_PRK_Buff02Rank03")],
        },
        "allPet": {
            # Fortify Carry Weight — magnitudes WorldPets_Buff_AnyPet_CarryCapacity0{r}
            # unlock levels WorldPets_PetLevelling_Level_PRK_CarryCapacity0{r}
            "fortifyCarryWeight": {
                "ranks":  [glob_val("WorldPets_Buff_AnyPet_CarryCapacity01"),
                           glob_val("WorldPets_Buff_AnyPet_CarryCapacity02"),
                           glob_val("WorldPets_Buff_AnyPet_CarryCapacity03")],
                "levels": [glob_val("WorldPets_PetLevelling_Level_PRK_CarryCapacity01"),
                           glob_val("WorldPets_PetLevelling_Level_PRK_CarryCapacity02"),
                           glob_val("WorldPets_PetLevelling_Level_PRK_CarryCapacity03")],
            },
            # Pet Combat Prowess — damage GLOBs WorldPets_Buff_AnyPet_PetProwess_Damage0{n}
            # upgrade levels WorldPets_PetLevelling_Level_UPG_PetProwess0{n}.
            # damageMult / takenMult resolved from CURV:
            #   WorldPets_PetProwess_DamageMult0.json         -> 1,2,3.5,5.5,8
            #   WorldPets_PetProwess_IncomingDamageMult0.json -> 1,0.8,0.6,0.4,0.2
            "petCombatProwess": {
                "levels":     [glob_val("WorldPets_PetLevelling_Level_UPG_PetProwess01"),
                               glob_val("WorldPets_PetLevelling_Level_UPG_PetProwess02"),
                               glob_val("WorldPets_PetLevelling_Level_UPG_PetProwess03"),
                               glob_val("WorldPets_PetLevelling_Level_UPG_PetProwess04")],
                "damage":     [glob_val("WorldPets_Buff_AnyPet_PetProwess_Damage01"),
                               glob_val("WorldPets_Buff_AnyPet_PetProwess_Damage02"),
                               glob_val("WorldPets_Buff_AnyPet_PetProwess_Damage03"),
                               glob_val("WorldPets_Buff_AnyPet_PetProwess_Damage04")],
                "damageMult": [2, 3.5, 5.5, 8],
                "takenMult":  [0.8, 0.6, 0.4, 0.2],
            },
        },
    }


def build_stats():
    return {
        "combat": {
            "minLvl": glob_val("WorldPets_MinLVL", 1),
            "maxLvl": glob_val("WorldPets_MaxLVL", 150),
        },
        "progressionMax": 200,
        # Base health — CURV WorldPets_PetProwess_Health0.json (key levels).
        "baseHealth": [
            {"lvl": 1,   "hp": 1981},
            {"lvl": 50,  "hp": 19871},
            {"lvl": 100, "hp": 48628},
            {"lvl": 150, "hp": 114518},
        ],
        # Base damage per hit — CURVs WorldPets_PetProwess_Damage{0-4}.json (key levels).
        # Each row: combat level -> [prowess 0, prowess 1, prowess 2, prowess 3, prowess 4].
        "baseDamage": [
            {"lvl": 1,   "dmg": [40, 80, 121, 161, 201]},
            {"lvl": 50,  "dmg": [62, 125, 187, 249, 311]},
            {"lvl": 100, "dmg": [97, 194, 291, 388, 485]},
            {"lvl": 150, "dmg": [151, 302, 453, 604, 755]},
        ],
        # Damage dealt multiplier — CURV WorldPets_PetProwess_DamageMult0.json.
        # Keyed by pet progression level (steps at 50/100/150/200).
        "damageMult": [
            {"progLvl": "1-49",    "mult": 1},
            {"progLvl": "50-99",   "mult": 2},
            {"progLvl": "100-149", "mult": 3.5},
            {"progLvl": "150-199", "mult": 5.5},
            {"progLvl": "200",     "mult": 8},
        ],
        # Incoming damage multiplier — CURV WorldPets_PetProwess_IncomingDamageMult0.json.
        "incomingDamageMult": [
            {"progLvl": "1-49",    "mult": 1},
            {"progLvl": "50-99",   "mult": 0.8},
            {"progLvl": "100-149", "mult": 0.6},
            {"progLvl": "150-199", "mult": 0.4},
            {"progLvl": "200",     "mult": 0.2},
        ],
        # All six WorldPets_Resist_* curves read 0 (placeholders, June 2026).
        "resistances": 0,
        # Total Pet XP to 200 ~ 303,770 (CURV 008AFDF9 XP curve). No daily cap —
        # Pet XP ticks once per minute while you earn player XP (12k/day cap removed).
        "levelling": {"totalXp": 303770},
        "commands": build_commands(),
        "immunities": ["Radiation", "Disease", "Fall damage"],
    }


# ---------------------------------------------------------------------------
# Assemble + write
# ---------------------------------------------------------------------------
data = {
    "meta": {"export": "June 2026", "featureAdded": "Jun 2026",
             "channel": "pts" if PTS_MODE else "live"},
    "routes": list(OBTAIN_ROUTE_ORDER),
    "foods": build_foods(),
    "buffs": build_buffs(),
    "stats": build_stats(),
    "playerGift": {
        "interval": glob_val("WorldPets_ConsumableGiftInterval"),
        "note": "Each species haul delivery also includes one random player gift.",
    },
    "species": build_species(),
}

out_path = OUT_DIR / ("world_pet_types_pts.json" if PTS_MODE else "world_pet_types.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
    f.write("\n")

_skin_total = sum(len(sp["skins"]) for sp in data["species"])
print(f"  Wrote {out_path}")
print(f"  Species: {len(data['species'])}  Skins: {_skin_total}  Foods: {len(data['foods'])}")
for sp in data["species"]:
    n_desc = sum(1 for s in sp["skins"] if s["description"] != BASE_DESC_NOTE)
    print(f"    {sp['name']:<10} skins={len(sp['skins']):<2} withStoreDesc={n_desc}")

# Patchlog feed — flat list of skins keyed by formId.
all_skins = []
for sp in data["species"]:
    for s in sp["skins"]:
        all_skins.append({"formId": s["formId"], "name": f"{sp['name']}: {s['name']}",
                          "description": s["description"]})
try:
    write_patchlog_feed(
        dist_dir=str(OUT_DIR),
        feed_name="patchlog_latest_world_pet_types.json",
        current_items=all_skins,
        key_field="formId",
        name_field="name",
        compare_fields=["name", "description"],
        prev_json_path="dist/world_pet_types.json",
        items_extractor=lambda d: [
            {"formId": s["formId"], "name": f"{sp['name']}: {s['name']}",
             "description": s.get("description", "")}
            for sp in d.get("species", []) for s in sp.get("skins", [])
        ],
    )
except Exception as e:
    print(f"  [patchlog] skipped: {e}")

print("Done.")
