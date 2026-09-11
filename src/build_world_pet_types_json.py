#!/usr/bin/env python3
"""
build_world_pet_types_json.py

Reads the xEdit TSV exports and builds dist/world_pet_types.json — the data feed
for the World Pets species / leveling / guide pages (/df/pets/world-pets/*).

Architecture (locked, matches every other camp page):
  xEdit TSV exports  →  this script  →  dist/world_pet_types.json  →  df-bnb-world-pets.js
The JSON holds DATA FIELDS only. All prose / rendered HTML lives in the renderer
as templates — never in here.

Inputs (from tsv/ in the repo):
  NPC_Export_*.tsv   — skin actors (CAMPPets_Actor_*). Source of the species/skin list.
  ENTM_Export_*.tsv  — store entitlements. Per-skin store DESC text.
  ALCH_Export_*.tsv (+ _Effects) — the six pet foods + effect magnitudes.
  COBJ_Export_*.tsv  — food crafting recipes (FVPA ingredients).
  GLOB_Export_*.tsv  — buff magnitudes (WorldPets_Buff_*, WorldPets_ConsumableBuff_*),
                       gift interval, reward amounts, SURV food-effect globals.
  Sept 2026 PTS rework (all optional — a live tree without World Pets data
  simply ships those fields empty):
  SPEL_Export_*_HEADER / _EFFECTS — buff names, per-rank MGEF, Pet Rested bonus.
  MGEF_Export_*.tsv   — buff description templates (<MAG>).
  LVLI_Export_*_LVLI_Entries.tsv — finder / second-buff items + per-rank qty GLOBs.
  FLST_Export_*_Entries.tsv — WorldPets_LvRewards_<Species> (30 rewards each).
  GMRW_Export_*.tsv   — reward amounts; links back to the retired level CHALs.
  CHAL_Export_*.tsv   — retired "Species - Reach Level N" records (level per slot).
  MESG_Help_Export_*.tsv — the in-game Pets help pages.
  MISC_Export_*.tsv   — display names for MISC reward items (streamed, filtered).

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


def _optional_tsv(env_var, glob_pattern, fallback_name, exclude_suffix=None):
    """Same resolver as the required tables, but a missing table returns None.
    The Sept 2026 PTS rework moved buff magnitudes, reward lists, the Pet Rested
    bonus and the help copy into records this build never needed before; on a
    LIVE tree that has no World Pets data these simply resolve to nothing and
    every dependent field ships as null / [] instead of crashing the build."""
    try:
        return _resolve_tsv(env_var, glob_pattern, fallback_name, exclude_suffix)
    except FileNotFoundError:
        return None


# Sept 2026 PTS rework — reward track, buff ranks, Pet Rested, help copy.
MGEF_PATH       = _optional_tsv("MGEF_TSV", "MGEF_Export_*.tsv", "MGEF_Export.tsv")
SPEL_EFF_PATH   = _optional_tsv("SPEL_EFFECTS_TSV", "SPEL_Export_*_EFFECTS.tsv",
                                "SPEL_Export_EFFECTS.tsv")
SPEL_HEAD_PATH  = _optional_tsv("SPEL_HEADER_TSV", "SPEL_Export_*_HEADER.tsv",
                                "SPEL_Export_HEADER.tsv")
FLST_ENT_PATH   = _optional_tsv("FLST_ENTRIES_TSV", "FLST_Export_*_Entries.tsv",
                                "FLST_Export_Entries.tsv")
GMRW_PATH       = _optional_tsv("GMRW_TSV", "GMRW_Export_*.tsv", "GMRW_Export.tsv")
LVLI_ENT_PATH   = _optional_tsv("LVLI_ENTRIES_TSV", "LVLI_Export_*_LVLI_Entries.tsv",
                                "LVLI_Export_LVLI_Entries.tsv")
MESG_HELP_PATH  = _optional_tsv("MESG_HELP_TSV", "MESG_Help_Export_*.tsv",
                                "MESG_Help_Export.tsv")
CHAL_PATH       = _optional_tsv("CHAL_TSV", "CHAL_Export_*.tsv", "CHAL_Export.tsv")
MISC_PATH       = _optional_tsv("MISC_TSV", "MISC_Export_*.tsv", "MISC_Export.tsv")

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

for _lbl, _p in (("MGEF", MGEF_PATH), ("SPEL effects", SPEL_EFF_PATH),
                 ("SPEL header", SPEL_HEAD_PATH), ("FLST entries", FLST_ENT_PATH),
                 ("GMRW", GMRW_PATH), ("LVLI entries", LVLI_ENT_PATH),
                 ("MESG help", MESG_HELP_PATH), ("CHAL", CHAL_PATH), ("MISC", MISC_PATH)):
    print(f"  {_lbl}: {_p if _p else '(not found — dependent fields ship empty)'}")


def _load_filtered(path, keep):
    """Stream a (possibly huge) TSV and keep only rows where keep(row) is true."""
    if not path:
        return []
    out = []
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            row = {k: ((v or "").strip().strip('"')) for k, v in row.items() if k is not None}
            if keep(row):
                out.append(row)
    return out


def _wp(s):
    return "worldpets" in str(s or "").lower()


mgef_rows      = _load_filtered(MGEF_PATH, lambda r: _wp(r.get("EDID")))
spel_eff_rows  = _load_filtered(SPEL_EFF_PATH, lambda r: _wp(r.get("SPEL_EDID")))
spel_head_rows = _load_filtered(SPEL_HEAD_PATH, lambda r: _wp(r.get("SPEL_EDID")))
flst_ent_rows  = _load_filtered(FLST_ENT_PATH, lambda r: _wp(r.get("FLST_EDID")))
gmrw_rows      = _load_filtered(GMRW_PATH, lambda r: _wp(r.get("EDID")))
lvli_ent_rows  = load_tsv(LVLI_ENT_PATH) if LVLI_ENT_PATH else []
mesg_rows      = _load_filtered(MESG_HELP_PATH, lambda r: _wp(r.get("MESG_EDID")))
chal_rows      = _load_filtered(CHAL_PATH, lambda r: _wp(r.get("EDID")))

mgef_by_edid = {r.get("EDID", ""): r for r in mgef_rows}
spel_head_by_edid = {r.get("SPEL_EDID", ""): r for r in spel_head_rows}
spel_effects = {}                       # spell EDID -> [effect rows in index order]
for r in spel_eff_rows:
    spel_effects.setdefault(r.get("SPEL_EDID", ""), []).append(r)
for _k in spel_effects:
    spel_effects[_k].sort(key=lambda r: int(fmt_num(r.get("EffectIndex")) or 0))
flst_entries = {}                       # FLST EDID -> [entry rows in index order]
for r in flst_ent_rows:
    flst_entries.setdefault(r.get("FLST_EDID", ""), []).append(r)
for _k in flst_entries:
    flst_entries[_k].sort(key=lambda r: int(fmt_num(r.get("EntryIndex")) or 0))
gmrw_by_edid = {r.get("EDID", ""): r for r in gmrw_rows}
lvli_entries = {}                       # LVLI EDID -> [entry rows]
for r in lvli_ent_rows:
    lvli_entries.setdefault(r.get("LVLI_EDID", ""), []).append(r)
mesg_by_edid = {r.get("MESG_EDID", ""): r for r in mesg_rows}
chal_by_edid = {r.get("EDID", ""): r for r in chal_rows}

# GLOB reverse index — which GLOB feeds a given record (Ref1..RefN "FID:EDID:SIG").
glob_refs_by_target = {}
for r in glob_rows:
    ed = (r.get("EDID") or "").strip()
    v = fmt_num(r.get("FLTV"))
    if not ed or v is None:
        continue
    for k, ref in r.items():
        if k and k.startswith("Ref") and k[3:].isdigit() and ref:
            parts = ref.split(":")
            if len(parts) >= 2:
                glob_refs_by_target.setdefault(parts[1], []).append((ed, v))

# Display names for LVLO / GMRW item references: ALCH FULL, MISC FULL.
item_full_by_edid = {}
for r in alch_rows:
    ed = (r.get("ALCH_EDID") or "").strip()
    if ed and r.get("FULL"):
        item_full_by_edid[ed] = r["FULL"].strip()
_WANTED_MISC = set()
for _rows in lvli_entries.values():
    for _e in _rows:
        _ref = _e.get("LVLO_Reference", "")
        if _ref.endswith(":MISC"):
            _WANTED_MISC.add(_ref.split(":")[1])
for _g in gmrw_rows:
    _ref = _g.get("RewardedItem", "")
    if _ref.endswith(":MISC"):
        _WANTED_MISC.add(_ref.split(":")[1])
for r in _load_filtered(MISC_PATH, lambda r: r.get("EDID") in _WANTED_MISC):
    if r.get("FULL"):
        item_full_by_edid[r["EDID"]] = r["FULL"]


def ref_parts(ref):
    """'00117DF9:SuperStimpak:ALCH' -> ('00117DF9', 'SuperStimpak', 'ALCH')."""
    p = str(ref or "").split(":")
    if len(p) >= 3:
        return p[0], p[1], p[2]
    return "", str(ref or ""), ""


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
    # Store boilerplate starts at " - C.A.M.P. PETS …" — sometimes at the very
    # start of the DESC (Hunting Dog), which left the whole blurb as boilerplate.
    m = re.search(r"(?:^|\s+)-\s+C\.?A\.?M\.?P\.?", t)
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
# Species identities. Only the EDID tokens that tie a species to its records
# live here — every NAME, magnitude, item and interval below is read from the
# exports (SPEL / MGEF / GLOB / LVLI / ENTM). The *Fallback strings are only
# used when a tree has no World Pets SPEL records at all (live, pre-release).
#
# Sept 2026 PTS rework (vs the June dump this build was first written for):
#   Radhog  Rad Magnet (RadAway/hr)       -> Goo-Getter  (Toxic Goo / 30 min)
#           Nuke Harvest (% 2x Flux)      -> Rad-Dration (HRF / 30 min in a nuke zone)
#   Deathclaw Meat Lover (meat/hr)        -> Dark Titan  (Black Titanium / 30 min)
#   Cat     Mega Bits % 2x on craft       -> +N Fish Bits per fish caught
#   Dog     Bounty Sniffer % extra        -> +1 legendary (1-3*) per Grunt Hunt
# The entitlement / reward-list EDIDs still carry the OLD names (RadMagnet,
# NukeHarvest, MeatLover) — so token matching accepts both.
# ---------------------------------------------------------------------------
SPECIES_META = {
    "cat": {
        "name": "Cat", "cap": "Cat", "diet": "Carnivore", "buffSpell": "Cat Pet",
        "sig1Fallback": "Bait Finder", "sig2Fallback": "Mega Bits",
        "sig1Tokens": ["BaitsFinder", "BaitFinder", "Bait"],
        "sig2Tokens": ["MegaBits"],
        "sig1Lvli": "WorldPets_LL_ConsumableBuff_Cat",
        "sig2Lvli": "WorldPets_LL_Buff2_Cat",
        "sig2Trigger": "catch",
        "activity": "Catch Fish with an active Pet Cat",
        "foodTypes": ["Meat"],
    },
    "deathclaw": {
        "name": "Deathclaw", "cap": "Deathclaw", "diet": "Carnivore", "buffSpell": "Deathclaw Pet",
        "sig1Fallback": "Dark Titan", "sig2Fallback": "Fun-Festation",
        "sig1Tokens": ["DarkTitan", "MeatLover", "Meat"],
        "sig2Tokens": ["FunFestation", "Infestation"],
        "sig1Lvli": "WorldPets_LL_ConsumableBuff_Deathclaw",
        "sig2Lvli": "HTO_crLLD_Boss",
        "sig2Trigger": "infestation",
        "activity": "Kill Infestation enemies with an active Pet Deathclaw",
        "foodTypes": ["Meat"],
    },
    "dog": {
        "name": "Dog", "cap": "Dog", "diet": "Omnivore", "buffSpell": "Dog Pet",
        "sig1Fallback": "Stimpak Fetcher", "sig2Fallback": "Bounty Sniffer",
        "sig1Tokens": ["StimpakFetcher", "Stimpak"],
        "sig2Tokens": ["BountySniffer", "Bounty"],
        "sig1Lvli": "WorldPets_LL_ConsumableBuff_Dog",
        "sig2Lvli": "Burn_BountyHuntDaily_LL_BountySnifferRewards",
        "sig2Trigger": "grunt-hunt",
        "activity": "Complete Grunt Hunts with an active Pet Dog",
        "foodTypes": ["Meat", "Plant"],
    },
    "radhog": {
        "name": "Radhog", "cap": "Radhog", "diet": "Omnivore", "buffSpell": "Radhog Pet",
        "sig1Fallback": "Goo-Getter", "sig2Fallback": "Rad-Dration",
        "sig1Tokens": ["GooGetter", "RadMagnet", "Rad"],
        "sig2Tokens": ["RadDration", "NukeHarvest"],
        "sig1Lvli": "WorldPets_LL_ConsumableBuff_Radhog",
        "sig2Lvli": "WorldPets_LL_Buff2_Radhog",
        "sig2Trigger": "nuke-zone",
        "activity": "Collect Flux with an active Pet Radhog",
        "foodTypes": ["Meat", "Plant"],
    },
}

SPECIES_ABC = ["cat", "deathclaw", "dog", "radhog"]

RANK_RE = re.compile(r"0?([1-4])$")


def _secs_to_minutes(v):
    if v is None:
        return None
    return int(round(float(v) / 60.0))


def spell_name(edid, fallback):
    r = spel_head_by_edid.get(edid)
    return (r.get("SPEL_FULL") if r and r.get("SPEL_FULL") else fallback)


def spell_rank_descs(edid):
    """Per-rank MGEF description templates for a species buff spell (effect
    index 0..2 = rank 1..3). Keeps the raw <MAG> token — the renderer fills it."""
    out = []
    for e in spel_effects.get(edid, []):
        m = mgef_by_edid.get(e.get("EFID_MGEF_EDID", ""), {})
        out.append((m.get("DNAM_MagicItemDescription") or "").strip())
    return out


def legendary_stars_of(ref_edid, ref_sig):
    """Star rating of a legendary reward reference: from a '<n>Star' token in the
    EDID, or by following an LVLI whose entries are all LGDI *_Rank<n> items."""
    m = re.search(r"(\d)Star", ref_edid)
    if m:
        return int(m.group(1))
    if ref_sig == "LVLI":
        ranks = set()
        for e in lvli_entries.get(ref_edid, []):
            _f, ed, sig = ref_parts(e.get("LVLO_Reference"))
            mm = re.search(r"_Rank(\d)$", ed)
            if sig == "LGDI" and mm:
                ranks.add(int(mm.group(1)))
        if len(ranks) == 1:
            return ranks.pop()
    return None


def item_label(ref):
    """Readable name for an LVLO reference."""
    _fid, ed, sig = ref_parts(ref)
    if sig in ("ALCH", "MISC") and ed in item_full_by_edid:
        return item_full_by_edid[ed]
    stars = legendary_stars_of(ed, sig)
    if stars:
        return f"{stars}\u2605 legendary item"
    return ed


def lvli_rank_rows(lvli_edid, glob_filter="WorldPets_"):
    """Rank rows from a species buff LVLI. The rank is taken from the entry's
    QUANTITY GLOB suffix (…01/02/03) rather than its HasEntitlement condition:
    the Sept 2026 Dog rank-3 entry has a NULL entitlement reference, so
    condition-matching alone would lose it (see the rank rule below). Returns [{rank, item, qty, qtyStatic,
    entitlement}] sorted by rank."""
    rows = []
    for e in lvli_entries.get(lvli_edid, []):
        qg = e.get("LVIG_QuantityGlobal", "")
        _gf, g_ed, _gs = ref_parts(qg)
        if glob_filter not in g_ed:
            continue
        ent = ""
        for i in range(1, 11):
            c = e.get(f"Cond{i}", "")
            mm = re.search(r"HasEntitlement\([^,]*,[^,]*,\s*(\S+)", c)
            if mm and "NULL" not in mm.group(1) and c.rstrip().endswith("1.000000"):
                ent = mm.group(1)
                break
        # Rank: the entitlement suffix when there is one (Dog Bounty Sniffer
        # reuses the rank-1 qty GLOB on all three entries), else the qty GLOB
        # suffix (Dog Stimpak Fetcher rank 3 has a NULL entitlement).
        m = RANK_RE.search(ent) if ent else None
        m = m or RANK_RE.search(g_ed)
        if not m:
            continue
        rank = int(m.group(1))
        rows.append({
            "rank":        rank,
            "item":        item_label(e.get("LVLO_Reference")),
            "itemEdid":    ref_parts(e.get("LVLO_Reference"))[1],
            "qty":         glob_val(g_ed),
            "qtyStatic":   fmt_num(e.get("LVIV_Quantity")),
            "entitlement": ent,
            # True when the entry's entitlement gate points at NULL (a data bug —
            # Dog Stimpak Fetcher rank 3, Sept 2026 PTS).
            "entitlementMissing": any("HasEntitlement" in e.get(f"Cond{i}", "") and
                                      "NULL" in e.get(f"Cond{i}", "") for i in range(1, 11)),
        })
    rows.sort(key=lambda r: r["rank"])
    return rows


def build_species_buffs(sk, meta):
    """Current (Sept 2026 PTS) buff package for one species, fully resolved."""
    cap = meta["cap"]
    sig1_spell = f"WorldPets_{cap}Buff_Buff01"
    sig2_spell = f"WorldPets_{cap}Buff_Buff02"
    sig1 = {
        "name":        spell_name(sig1_spell, meta["sig1Fallback"]),
        "spell":       sig1_spell,
        "descs":       spell_rank_descs(sig1_spell),
        "intervalMin": _secs_to_minutes(glob_val("WorldPets_ConsumableGiftInterval")),
        "ranks":       lvli_rank_rows(meta["sig1Lvli"]),
    }
    sig2 = {
        "name":        spell_name(sig2_spell, meta["sig2Fallback"]),
        "spell":       sig2_spell,
        "descs":       spell_rank_descs(sig2_spell),
        "trigger":     meta["sig2Trigger"],
        "intervalMin": (_secs_to_minutes(glob_val("WorldPets_Radhog_Buff2GiftInterval"))
                        if sk == "radhog" else None),
        "ranks":       lvli_rank_rows(meta["sig2Lvli"]),
    }
    return {"sig1": sig1, "sig2": sig2}


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
            # Attack animation length (s) — the real per-species cadence field.
            "animAttackSec": fmt_num(row.get("DNAM_AnimAttackSeconds")),
            # CRDT is the crit multiplier; DNAM_DamageOutOfRangeMult is not.
            "critDmgMult":   fmt_num(row.get("CRDT_CritDamageMult")),
            "outOfRangeMult": fmt_num(row.get("DNAM_DamageOutOfRangeMult")),
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
                # No store text → ship empty. (The old fallback reused the base-skin
                # note, which claimed a store skin was "bundled with the system".)

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

        buffs = build_species_buffs(sk, meta)
        s1, s2 = buffs["sig1"], buffs["sig2"]
        s1_first = s1["ranks"][0] if s1["ranks"] else {}
        species.append({
            "key":       sk,
            "name":      meta["name"],
            "diet":      meta["diet"],
            "buffSpell": meta["buffSpell"],
            # Legacy flat fields — kept so an older renderer still reads something
            # sensible. The authoritative data is `buffs` below.
            "sig1":      s1["name"],
            "sig2":      s2["name"],
            "sig1Mag":   [r["qty"] for r in s1["ranks"]] or [None, None, None],
            "sig2Mag":   [r["qty"] for r in s2["ranks"]] or [None, None, None],
            "activity":  meta["activity"],
            "treasure":  {"item": s1_first.get("item", ""), "isList": False,
                          "qtyByRank": [r["qty"] for r in s1["ranks"]]},
            "buffs":     buffs,
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
# ---------------------------------------------------------------------------
# REWARD TRACK — Sept 2026 PTS: FLST WorldPets_LvRewards_<Species> (30 GMRW each)
#
# The June/Aug "Species - Reach Level N" CHAL records are retired (zzz_) and the
# rewards are now claimed in the Pip-Boy Pet Progression Menu. The GMRW records
# do not carry a level, so each reward is matched to the retired challenge slot
# it replaced: directly when the GMRW still references that CHAL
# (levelConfirmed = true), otherwise by reward kind (levelConfirmed = false).
# Levels come from the CHAL FULL ("Dog - Reach Level 40") with the retired
# WorldPets_PetLevelling_Level_* GLOBs as fallback.
# ---------------------------------------------------------------------------
LEVELLING_CHAL_RE = re.compile(
    r"^(?:zzz_?)?WorldPets_PetLevelling_(Cat|Dog|Radhog|Deathclaw)_(.+)$", re.IGNORECASE)
REACH_LEVEL_RE = re.compile(r"Reach Level\s+(\d+)", re.IGNORECASE)

# Retired-slot -> GLOB suffix (the GLOB and CHAL spellings drifted apart).
SLOT_GLOB_ALIAS = {
    "CUR_Atom01": "CUR_Atoms01", "CUR_Atom02": "CUR_Atoms02",
    "CUR_PerkCards01": "CUR_PerkCard01",
}


def _level_for_slot(sp_cap, slot):
    for pre in ("", "zzz_", "zzz"):
        r = chal_by_edid.get(f"{pre}WorldPets_PetLevelling_{sp_cap}_{slot}")
        if r:
            m = REACH_LEVEL_RE.search(r.get("FULL", ""))
            if m:
                return int(m.group(1))
    g = SLOT_GLOB_ALIAS.get(slot, slot)
    for pre in ("WorldPets_PetLevelling_Level_", "zzz_WorldPets_PetLevelling_Level_"):
        v = glob_val(pre + g)
        if v is not None:
            return int(v)
    return None


def _gmrw_chal_slot(g):
    """Retired CHAL slot the GMRW record itself still references, if any."""
    for k, ref in g.items():
        if k.startswith("Ref") and ref and ref.endswith(":CHAL"):
            m = LEVELLING_CHAL_RE.match(ref.split(":")[1])
            if m:
                return m.group(2)
    return None


def _kind_slot(meta, kind, rest):
    """Retired CHAL slot inferred from the reward's own EDID tokens."""
    rank = RANK_RE.search(rest)
    n = rank.group(1) if rank else "1"
    if kind == "BUFF":
        if any(t.lower() in rest.lower() for t in ["PetCarrier"]):
            return f"PRK_CarryCapacity0{n}"
        if any(t.lower() in rest.lower() for t in meta["sig1Tokens"]):
            return f"PRK_Buff01Rank0{n}"
        return f"PRK_Buff02Rank0{n}"
    if kind == "PERK":
        return f"UPG_PetProwess0{n}"
    if kind == "ICON":
        return "COS_PlayerIcon01" if rest.startswith("01") else "COS_PlayerIcon02"
    if kind == "TITL":
        m = re.match(r"(Prefix|Suffix)0?(\d)", rest)
        if m:
            return f"COS_PlayerTitle{m.group(1)}0{m.group(2)}"
    if kind == "COSM":
        if "Backpack" in rest and "Flair" not in rest:
            return "COS_Backpack01"
        if "Medallion" in rest or "Flair" in rest:
            return "COS_BackpackFlair01"
        if "TamerOutfit" in rest or "Outfit" in rest:
            return "COS_PlayerOutfit01"
        return "COS_PetEquipment02" if rest.startswith("02") else "COS_PetEquipment01"
    if kind == "CNCY":
        # Only reached when the GMRW no longer points at a retired CHAL.
        if "Atoms02" in rest:
            return "CUR_Atom02"
        if "Caps" in rest:
            return "CUR_Atom01"          # took the old Lv 15 Atoms slot (inferred)
    return None


GMRW_RE = re.compile(r"^WorldPets_LvReward_(All|Cat|Dog|Radhog|Deathclaw)_(CNCY|BUFF|PERK|COSM|ICON|TITL)_(.+)$")

KIND_TYPE = {"BUFF": "buff", "PERK": "buff", "COSM": "cosmetic", "ICON": "cosmetic",
             "TITL": "cosmetic", "CNCY": "currency"}


def _gmrw_amount(g):
    """(amount, unit) for a currency / consumable GMRW."""
    fid = g.get("FormID", "")
    item_ref = g.get("RewardedItem", "")
    if item_ref:
        _f, ed, _s = ref_parts(item_ref)
        qty = fmt_num(g.get("RewardedItemCount"))
        return qty, item_full_by_edid.get(ed, ed)
    cur = g.get("QRCO_CurrencyObject", "")
    cap_glob = g.get("NAM8_CapsGlobal", "")
    if cap_glob:
        _f, ged, _s = ref_parts(cap_glob)
        unit = "Caps"
        if "PerkScrap" in cur:
            unit = "Perk Coins"
        return glob_val(ged), unit
    # Atoms rewards carry no amount in the GMRW columns — the GLOB that feeds
    # this GMRW (GLOB Ref list) holds it.
    for ged, v in glob_refs_by_target.get(g.get("EDID", ""), []):
        unit = "Atoms" if "Atom" in ged else ("Perk Coins" if "PerkCoin" in ged else "")
        return v, unit
    return None, ""


def _entm_for(gmrw_edid):
    ed = gmrw_edid.replace("WorldPets_LvReward_", "WorldPets_ENTM_", 1)
    return entm_by_edid_lc.get(ed.lower())


def build_reward_track():
    track = {}
    for sk in SPECIES_ABC:
        meta = SPECIES_META[sk]
        cap = meta["cap"]
        buffs = build_species_buffs(sk, meta)
        rows = []
        for e in flst_entries.get(f"WorldPets_LvRewards_{cap}", []):
            gmrw_edid = e.get("Entry_EDID", "")
            m = GMRW_RE.match(gmrw_edid)
            if not m:
                continue
            _scope, kind, rest = m.groups()
            g = gmrw_by_edid.get(gmrw_edid, {})
            direct = _gmrw_chal_slot(g) if g else None
            slot = direct or _kind_slot(meta, kind, rest)
            level = _level_for_slot(cap, slot) if slot else None
            entm = _entm_for(gmrw_edid) or {}
            row = {
                "level":          level,
                "levelConfirmed": bool(direct),
                "slot":           slot,
                "type":           KIND_TYPE.get(kind, "cosmetic"),
                "kind":           kind,
                "gmrw":           gmrw_edid,
                "gmrwFormId":     e.get("Entry_FormID", ""),
                "entitlement":    entm.get("EDID", ""),
                "name":           "",
                "texture":        (entm.get("ETIP", "") + entm.get("ETDI", "")) if entm.get("ETDI") else "",
            }
            rk = RANK_RE.search(rest)
            rank = int(rk.group(1)) if rk else None
            if kind == "BUFF":
                if "PetCarrier" in rest:
                    row["buff"] = "carry"
                    row["name"] = f"Pet Carrier {rank}"
                elif slot and slot.startswith("PRK_Buff01"):
                    row["buff"] = "sig1"
                    row["name"] = f"{buffs['sig1']['name']} {rank}"
                else:
                    row["buff"] = "sig2"
                    row["name"] = f"{buffs['sig2']['name']} {rank}"
                row["rank"] = rank
            elif kind == "PERK":
                row["buff"] = "prowess"
                row["rank"] = rank
                row["name"] = f"Pet Prowess {rank}"
            elif kind == "CNCY":
                amt, unit = _gmrw_amount(g) if g else (None, "")
                row["amount"] = amt
                row["unit"] = unit
                row["name"] = (f"{amt:,} {unit}" if isinstance(amt, (int, float)) and unit
                               else unit or rest)
            else:
                nnam = (entm.get("NNAM") or "").strip()
                full = (entm.get("FULL") or "").strip()
                if kind == "TITL":
                    row["titlePart"] = "prefix" if "Prefix" in rest else "suffix"
                    row["name"] = nnam or rest.split("_")[-1]
                elif kind == "ICON":
                    row["name"] = full or nnam or rest
                else:
                    row["name"] = re.sub(r"^DC\b", "Deathclaw", nnam or full or rest)
                    row["cosmetic"] = ("backpack" if slot == "COS_Backpack01" else
                                       "flair" if slot == "COS_BackpackFlair01" else
                                       "outfit" if slot == "COS_PlayerOutfit01" else "pet")
            rows.append(row)
        rows.sort(key=lambda r: (r["level"] is None, r["level"] or 0))
        track[sk] = rows
    alt = [glob_val("WorldPets_AltLvReward_All_Caps01"), glob_val("WorldPets_AltLvReward_All_Caps02")]
    return {
        "source": "FLST WorldPets_LvRewards_<Species> + GMRW + ENTM",
        "claimedIn": "Pip-Boy PETS - Pet Progression Menu",
        "species": track,
        "unusedAltCaps": [a for a in alt if a is not None],
    }


def build_buffs():
    """All-pet buffs (carry / prowess) + legacy signatureUnlockLevels, with the
    unlock levels read off the new reward track."""
    track = REWARD_TRACK["species"].get("dog") or next(iter(REWARD_TRACK["species"].values()), [])

    def lv(buff, rank):
        for r in track:
            if r.get("buff") == buff and r.get("rank") == rank:
                return r.get("level")
        return None

    def g2(new, old):
        v = glob_val(new)
        return v if v is not None else glob_val(old)

    return {
        "signatureUnlockLevels": {
            "sig1": [lv("sig1", i) for i in (1, 2, 3)],
            "sig2": [lv("sig2", i) for i in (1, 2, 3)],
        },
        "allPet": {
            "fortifyCarryWeight": {
                "name":   "Pet Carrier",
                "ranks":  [g2(f"WorldPets_Buff_All_PetCarrier0{i}",
                              f"WorldPets_Buff_AnyPet_CarryCapacity0{i}") for i in (1, 2, 3)],
                "levels": [lv("carry", i) for i in (1, 2, 3)],
            },
            # Pet Combat Prowess — damage GLOBs WorldPets_Buff_All_PetProwess0{n}_Damage
            # (Sept 2026: 100/250/450/700, was 200/350/550/800).
            # damageMult / takenMult resolved from CURV (June JSON — no PTS POINTS export yet):
            #   WorldPets_PetProwess_DamageMult0.json         -> 1,2,3.5,5.5,8
            #   WorldPets_PetProwess_IncomingDamageMult0.json -> 1,0.8,0.6,0.4,0.2
            "petCombatProwess": {
                "levels":     [lv("prowess", i) for i in (1, 2, 3, 4)],
                "damage":     [g2(f"WorldPets_Buff_All_PetProwess0{i}_Damage",
                                  f"WorldPets_Buff_AnyPet_PetProwess_Damage0{i}") for i in (1, 2, 3, 4)],
                "damageMult": [2, 3.5, 5.5, 8],
                "takenMult":  [0.8, 0.6, 0.4, 0.2],
            },
        },
    }


# ---------------------------------------------------------------------------
# LEVELLING — Pet XP rules, the Pet Rested daily bonus, and the in-game help copy
# ---------------------------------------------------------------------------
# CURV 008AFDF9 CT_WorldPets_XP_LevelingProgression -> WorldPets_PetXp_02.json.
# y = Pet XP to advance one level at level x (linear between points). The PTS
# CURV POINTS table is not exported yet, so these are the June JSON values.
XP_CURVE = [[1, 0], [2, 210], [5, 250], [10, 300], [15, 350], [20, 400], [25, 450],
            [30, 500], [35, 550], [40, 600], [45, 650], [50, 700], [55, 775], [60, 850],
            [65, 925], [70, 1000], [75, 1075], [80, 1150], [85, 1225], [90, 1300],
            [95, 1375], [100, 1450], [105, 1525], [110, 1600], [115, 1675], [120, 1750],
            [125, 1825], [130, 1900], [135, 1975], [140, 2050], [145, 2125], [150, 2200],
            [155, 2300], [160, 2400], [165, 2500], [170, 2600], [175, 2700], [180, 2800],
            [185, 2900], [190, 3000], [195, 3100], [200, 3200]]


def _xp_at(lvl):
    for (x0, y0), (x1, y1) in zip(XP_CURVE, XP_CURVE[1:]):
        if x0 <= lvl <= x1:
            return y0 + (y1 - y0) * (lvl - x0) / (x1 - x0)
    return XP_CURVE[-1][1]


def _help_paras(edid):
    """MESG help body split into paragraphs (the export flattens line breaks to
    runs of spaces). Keeps the in-game wording verbatim."""
    r = mesg_by_edid.get(edid)
    if not r:
        return []
    txt = r.get("DESC_Description", "")
    return [p.strip() for p in re.split(r"\s{3,}", txt) if p.strip()]


def build_levelling():
    rested_eff = (spel_effects.get("WorldPets_WellRested") or [{}])[0]
    mag = fmt_num(rested_eff.get("EFIT_Magnitude"))
    dur = fmt_num(rested_eff.get("EFIT_Duration"))
    rested_mgef = mgef_by_edid.get(rested_eff.get("EFID_MGEF_EDID", ""), {})
    total = int(round(sum(_xp_at(l) for l in range(2, 201))))
    return {
        "totalXp":  total,
        "xpCurve":  XP_CURVE,
        # "every time you earn XP, your Pet will also earn Pet XP. This is limited
        # at once every 1 minute" — HelpWorldPetsLeveling (MESG 008B251E).
        "tickSeconds": 60 if mesg_by_edid.get("HelpWorldPetsLeveling") else None,
        "rested": ({
            "name":       spell_name("WorldPets_WellRested", "Pet Rested"),
            "desc":       (rested_mgef.get("DNAM_MagicItemDescription") or "").strip(),
            "multiplier": (mag + 1) if isinstance(mag, (int, float)) else None,
            "durationSec": dur,
            "durationHours": (round(dur / 3600, 2) if isinstance(dur, (int, float)) else None),
            "resets":     "daily challenge reset",
        } if rested_eff else None),
        "help": {
            "overview": _help_paras("HelpWorldPets"),
            "leveling": _help_paras("HelpWorldPetsLeveling"),
            "buffs":    _help_paras("HelpWorldPetsBuffs"),
            "controls": _help_paras("HelpWorldPetsControls_PC"),
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
        # Legacy mirror of LEVELLING["totalXp"] (the full block is top-level `levelling`).
        "levelling": {"totalXp": LEVELLING["totalXp"]},
        "commands": build_commands(),
        "immunities": ["Radiation", "Disease", "Fall damage"],
    }


# ---------------------------------------------------------------------------
# Assemble + write
# ---------------------------------------------------------------------------
REWARD_TRACK = build_reward_track()
LEVELLING = build_levelling()

try:
    _export_label = tsv_source.export_date(str(GLOB_PATH)).strftime("%B %Y")
except Exception:
    _export_label = ""

data = {
    "meta": {"export": _export_label, "featureAdded": "Jun 2026",
             "channel": "pts" if PTS_MODE else "live",
             # False on a tree with no World Pets reward lists (live, pre-release) —
             # the renderer uses it to show the "PTS only" notice instead of blanks.
             "hasWorldPetsData": bool(any(REWARD_TRACK["species"].values()))},
    "routes": list(OBTAIN_ROUTE_ORDER),
    "foods": build_foods(),
    "buffs": build_buffs(),
    "rewardTrack": REWARD_TRACK,
    "levelling": LEVELLING,
    "stats": build_stats(),
    "playerGift": {
        "interval": glob_val("WorldPets_ConsumableGiftInterval"),
        "intervalMin": _secs_to_minutes(glob_val("WorldPets_ConsumableGiftInterval")),
        "note": "The finder buff delivers its item on this timer while the pet is active.",
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
    n_desc = sum(1 for s in sp["skins"] if s["description"] and s["description"] != BASE_DESC_NOTE)
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
