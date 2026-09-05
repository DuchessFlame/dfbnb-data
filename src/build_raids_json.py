#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_raids_json.py — Gleaming Depths All Rewards (single consolidated page)

Replaces the five per-stage "Gleaming Depths Stage N All Rewards" pages with ONE
page carrying six root expands:

    General Stage Rewards   (the 10 pools shared by all five stages)
    Stage 1 Rewards  …  Stage 5 Rewards   (the stage-unique pools)

Where the data comes from
-------------------------
The old builder flattened `events_rewards.json`, which lost the LVLI structure and
left ~160 items per stage carrying raw FormIDs as names. This builder instead walks
the raid leveled lists directly with the shared rng76 engine:

    RD01_LL_Raids_Rewards_Enc01  007965E1
    RD01_LL_Raids_Rewards_Enc02  007965E2
    RD01_LL_Raids_Rewards_Enc03  007965E3
    RD01_LL_Raids_Rewards_Enc04  007965E4
    RD01_LL_Raids_Rewards_Enc05  007965E5

Each direct entry of those lists is a sub-LVLI = one reward pool. A pool whose
FormID appears under more than one stage is SHARED (bobbleheads, magazines, ammo,
chems, aid, rads, components, resources, mutations, utility) and moves to the
General section with a per-stage chance column — the pool chance genuinely differs
per stage (Bobbleheads: 5 / 10 / 15 / 25 / 25). Everything else is stage-unique and
becomes that stage's tickable checklist.

Speed-run trophies (RD01_LLS_Raids_Rewards_Enc0N_Trophies) and the stage-5 Drifter
Activation Key Card hang off the quest's GMRW rather than the Enc list, so they are
looked up by EDID and appended to their stage.

Rates
-----
All rate maths is rng76's (drop-rate-engine skill owns the rules). This script does
no rate arithmetic of its own beyond aggregating duplicate quantity rows for one
FormID: the game lists e.g. Stimpak x6 / x5 / x4 / x3 as four entries, and the
checklist wants one row reading "x3-6" with the summed chance of getting any of them.

Two modes (mirrors build_daily_ops_json.py):
  (default / live)  -> dist/raids/raids_rewards.json
  --pts             -> dist/pts/raids/raids_rewards.json

Usage:
  python src/build_raids_json.py
  python src/build_raids_json.py --pts
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_this_dir = Path(__file__).resolve().parent
for _p in [_this_dir, _this_dir / "src", _this_dir.parent / "src"]:
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from rng76 import Rng76Data, pct, read_tsv, newest, pick
from patchlog_utils import write_empty_patchlog_feed


PTS = "--pts" in sys.argv

_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_ROOT = str(_REPO_ROOT / "tsv")

PAGE_SLUG = "gleaming-depths-all-rewards"
PAGE_PATH = "/df/raids/gleaming-depths/gleaming-depths-all-rewards"

# Image convention: files are uploaded as <EDID lowercased>.avif under this folder.
# A missing file 404s and the renderer falls back to its placeholder slot, so a new
# reward needs no code change — just the upload.
IMAGE_BASE = "/wp-content/uploads/guide-images/raids/gleaming-depths/"


# ---------------------------------------------------------------------------
# STAGE TABLE
# ---------------------------------------------------------------------------

STAGES: List[Dict[str, Any]] = [
    {
        "stage": 1,
        "encFormID": "007965E1",
        "questFormID": "00772A47",
        "gameName": "Guardian Bot Module",
        "bossName": "EN06 Guardian Bot",
        "speedrunSeconds": 148,
        "trophyEdid": "RD01_LLS_Raids_Rewards_Enc01_Trophies",
    },
    {
        "stage": 2,
        "encFormID": "007965E2",
        "questFormID": "0078F7A1",
        "gameName": "Drill Complex Module",
        "bossName": "Tunnel Boring Drill",
        "speedrunSeconds": 220,
        "trophyEdid": "RD01_LLS_Raids_Rewards_Enc02_Trophies",
    },
    {
        "stage": 3,
        "encFormID": "007965E3",
        "questFormID": "0078B59E",
        "gameName": "Enclave Squad Module",
        "bossName": "Enclave Epsilon Squad",
        "speedrunSeconds": 139,
        "trophyEdid": "RD01_LLS_Raids_Rewards_Enc03_Trophies",
    },
    {
        "stage": 4,
        "encFormID": "007965E4",
        "questFormID": "00788127",
        "gameName": "Enclave Research Lab Module",
        "bossName": "Ultragenetic Stalker",
        "speedrunSeconds": 254,
        "trophyEdid": "RD01_LLS_Raids_Rewards_Enc04_Trophies",
    },
    {
        "stage": 5,
        "encFormID": "007965E5",
        "questFormID": "00786D41",
        "gameName": "Scorchtongue Module",
        "bossName": "Ultracite Terror",
        "speedrunSeconds": 134,
        "trophyEdid": "RD01_LLS_Raids_Rewards_Enc05_Trophies",
        "extraPoolEdids": ["P62_LLS_Rewards_TheDrifter_ActivationKeyCard"],
    },
]

STAGE_COUNT = len(STAGES)


# ---------------------------------------------------------------------------
# POOL DISPLAY LABELS
#
# The raid LVLI set is small and fixed, so hand-written labels beat generic EDID
# prettification here. Keyed by the EDID with the Enc0N_ stage prefix stripped, so
# one entry covers all five stage copies of the same pool.
# ---------------------------------------------------------------------------

POOL_LABELS: Dict[str, str] = {
    # Shared — General Stage Rewards
    "RD01_LLS_Raids_Rewards_BobbleHeads":               "Bobbleheads",
    "RD01_LLS_Raids_Rewards_Magazines":                 "Magazines",
    "RD01_LLS_Raids_Rewards_Resources":                 "Currency & Resources",
    "RD01_LLS_Raids_Rewards_Contextual_AmmoType_All":   "Ammunition",
    "RD01_LLS_Raids_Rewards_Mutations":                 "Mutation Serums",
    "RD01_LLS_Raids_Rewards_Chems":                     "Chems",
    "RD01_LLS_Raids_Rewards_Utility":                   "Repair Kits",
    "RD01_LLS_Raids_Rewards_Aid":                       "Aid",
    "RD01_LLS_Raids_Rewards_Rads":                      "Rad Treatment",
    "RD01_LLS_Raids_Rewards_Components":                "Crafting Components",
    # Stage-unique (stage prefix stripped)
    "LegendaryShards":                     "Legendary Mods & Items",
    "PowerArmor_Vulcan":                   "Vulcan Power Armour",
    "PowerArmor_Vulcan_Mods":              "Vulcan Power Armour Mod Plans",
    "PowerArmor_Vulcan_Recipes":           "Vulcan Power Armour Plans",
    "Weapons_Valkyrie":                    "Valkyrie",
    "Weapons_DrillFist":                   "Drill Fist",
    "Weapons_DrillFistMods":               "Drill Fist Mod Plans",
    "Weapons_ResolveBreaker":              "Resolve Breaker",
    "Weapons_StrikeBreaker":               "Flatliner",
    "Weapons_Cauterizer":                  "Cauterizer",
    "Weapons_LicketySplit":                "Lickety-Split",
    "Weapons_BoilingPoint":                "Boiling Point",
    "Weapons_UltraciteTerrorSword":        "Ultracite Terror Sword",
    "ScoutArmor_EnclaveEpsilonSquad":      "Enclave Epsilon Squad Scout Armour Paint",
    "CombatArmor_EnclaveEpsilonSquad":     "Enclave Epsilon Squad Combat Armour Paint",
    "Workshop_PowerGenerator":             "Ultracite Generator",
    "BackPackMod_Canteen":                 "Canteen Backpack Mod",
    "UnderArmor_SecretOperative":          "Enclave Secret Operative Underarmour",
    "Trophies":                            "Speed Run Trophy",
    "P62_LLS_Rewards_TheDrifter_ActivationKeyCard": "Drifter Activation Key Card",
}

# Order the shared pools appear inside General Stage Rewards.
GENERAL_ORDER = [
    "RD01_LLS_Raids_Rewards_Resources",
    "RD01_LLS_Raids_Rewards_Contextual_AmmoType_All",
    "RD01_LLS_Raids_Rewards_Aid",
    "RD01_LLS_Raids_Rewards_Chems",
    "RD01_LLS_Raids_Rewards_Rads",
    "RD01_LLS_Raids_Rewards_Mutations",
    "RD01_LLS_Raids_Rewards_Utility",
    "RD01_LLS_Raids_Rewards_Components",
    "RD01_LLS_Raids_Rewards_BobbleHeads",
    "RD01_LLS_Raids_Rewards_Magazines",
]

_ENC_PREFIX_RE = re.compile(r"^RD01_LLS_Raids_Rewards_Enc0\d_", re.IGNORECASE)


def pool_key(edid: str) -> str:
    """Strip the Enc0N_ stage prefix so all five copies of a pool share a label key."""
    stripped = _ENC_PREFIX_RE.sub("", edid or "")
    return stripped or (edid or "")


def pool_label(edid: str) -> str:
    key = pool_key(edid)
    if key in POOL_LABELS:
        return POOL_LABELS[key]
    if edid in POOL_LABELS:
        return POOL_LABELS[edid]
    # Fallback: humanise the leftover EDID rather than showing raw data-miner text.
    words = re.sub(r"[_]+", " ", key).strip()
    words = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", words)
    return words.title()


def pool_id(stage_or_scope: str, edid: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", pool_key(edid).lower()).strip("-")
    return f"{stage_or_scope}-{base}"


# ---------------------------------------------------------------------------
# STAR GLYPH
#
# Legendary mod shards ship their star rating as the in-game glyph "¬" repeated
# once per star (¬¬¬¬ Radioactive-Powered). drop-rate-engine section 16 puts the
# rating BEFORE the word, using ★ — so "¬¬¬¬ X" becomes "4★ X".
# ---------------------------------------------------------------------------

_STAR_RE = re.compile(r"^(¬+)\s*")


def destar(name: str) -> Tuple[str, Optional[int]]:
    m = _STAR_RE.match(name or "")
    if not m:
        return (name or "").strip(), None
    stars = len(m.group(1))
    return f"{stars}★ {name[m.end():].strip()}", stars


# ---------------------------------------------------------------------------
# LGDI NAMES
#
# A legendary-item placeholder record has no FULL name, so rng76 humanises the
# EDID and you get "Legendary Items Weapons Ranged Rank3". drop-rate-engine
# section 16 fixes the house format: the rating always comes BEFORE the word
# Legendary, written with the ★ glyph — "3★ Legendary Ranged Weapon".
# ---------------------------------------------------------------------------

_LGDI_RE = re.compile(
    r"^(?:RD01_)?LegendaryItems_(?P<body>.*?)_Rank(?P<rank>\d)$", re.IGNORECASE
)

_LGDI_BODY = {
    "weapons_ranged": "Ranged Weapon",
    "weapons_melee": "Melee Weapon",
    "weapons_any": "Weapon",
    "weapons": "Weapon",
    "powerarmor": "Power Armour Piece",
    "armor": "Armour Piece",
    "powerarmor_enclavevulcan_armleft": "Vulcan Left Arm",
    "powerarmor_enclavevulcan_armright": "Vulcan Right Arm",
    "powerarmor_enclavevulcan_legleft": "Vulcan Left Leg",
    "powerarmor_enclavevulcan_legright": "Vulcan Right Leg",
    "powerarmor_enclavevulcan_torso": "Vulcan Torso",
    "powerarmor_enclavevulcan_helmet": "Vulcan Helmet",
}


def lgdi_name(edid: str) -> Tuple[str, Optional[int]]:
    """('3★ Legendary Ranged Weapon', 3) — or ('', None) when the EDID isn't one."""
    if (edid or "").lower() == "legendaryitems_special_allitems":
        return "Legendary Item (any type)", None
    m = _LGDI_RE.match(edid or "")
    if not m:
        return "", None
    rank = int(m.group("rank"))
    body = m.group("body").lower()
    label = _LGDI_BODY.get(body)
    if not label:
        label = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", m.group("body")).replace("_", " ").strip()
    return f"{rank}★ Legendary {label}", rank


# Records with no FULL name anywhere in the exports — there is no UTIL/ATX TSV in
# tsv/, so rng76 can only humanise the EDID. Three rows, named by hand from the
# in-game item. Delete an entry the day its export lands.
NAME_OVERRIDES: Dict[str, str] = {
    "0041ADEB": "Repair Kit",
    "0041ADEC": "Improved Repair Kit",
    "0054B4EC": "Scrap Kit",
}


# ---------------------------------------------------------------------------
# KEYWORD FLAGS — tradeable / unsellable
# NonPlayerTradable [KYWD:00499F7A] → cannot be traded with other players
# UnsellableObject  [KYWD:003D4327] → cannot be sold to NPC vendors
# ---------------------------------------------------------------------------

_NON_PLAYER_TRADABLE_KW = "00499f7a"
_UNSELLABLE_OBJECT_KW = "003d4327"


def load_keyword_flags(tsv_root: str) -> Tuple[Set[str], Set[str]]:
    non_tradable: Set[str] = set()
    unsellable: Set[str] = set()
    try:
        rows = read_tsv(newest(os.path.join(tsv_root, "KYWD_Export_*_Refs.tsv")))
    except (FileNotFoundError, IndexError):
        return non_tradable, unsellable
    for r in rows:
        kw = (r.get("KeywordFormID") or "").strip().lower()
        fid = (r.get("RefFormID") or "").strip().lower()
        if not fid:
            continue
        if kw == _NON_PLAYER_TRADABLE_KW:
            non_tradable.add(fid)
        elif kw == _UNSELLABLE_OBJECT_KW:
            unsellable.add(fid)
    return non_tradable, unsellable


# ---------------------------------------------------------------------------
# COBJ / OMOD — what a plan actually teaches, and the mod's effect
# ---------------------------------------------------------------------------

_MAT_RE = re.compile(r"^([A-Za-z0-9_]+?):(\d+)")


def _humanise_component(edid: str) -> str:
    s = re.sub(r"^c_", "", edid or "")
    s = re.sub(r"_scrap$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    return s.replace("_", " ").strip().title()


def parse_materials(fvpa: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for chunk in (fvpa or "").split("|"):
        m = _MAT_RE.match(chunk.strip())
        if not m:
            continue
        out.append({"name": _humanise_component(m.group(1)), "qty": int(m.group(2))})
    return out


def load_cobj(tsv_root: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, str]]:
    """
    Read COBJ once and return three indexes:

      by_plan       GNAM (plan) FormID -> what that plan unlocks + its recipe
      name_by_fid   CNAM FormID -> the crafted object's display name
      name_by_cobj  COBJ FormID -> the crafted object's display name

    name_by_fid exists because AMMO records carry no FULL name in the xEdit export
    — rng76 falls back to humanising the EDID, so 10mm Round resolves as "Ammo10mm".
    The crafting recipe for the same FormID does carry the real name, so ammo rows
    borrow it. name_by_cobj resolves the COBJ references inside HasLearnedRecipe
    conditions.
    """
    by_plan: Dict[str, Dict[str, Any]] = {}
    name_by_fid: Dict[str, str] = {}
    name_by_cobj: Dict[str, str] = {}
    try:
        rows = read_tsv(newest(os.path.join(tsv_root, "COBJ_Export_*.tsv")))
    except (FileNotFoundError, IndexError):
        return by_plan, name_by_fid, name_by_cobj
    for r in rows:
        cobj_fid = (r.get("COBJ_FormID") or "").strip().upper()
        cnam_fid = (r.get("CNAM_FormID") or "").strip().upper()
        cnam_name = (r.get("CNAM_FULL") or "").strip()
        if cnam_fid and cnam_name:
            name_by_fid.setdefault(cnam_fid, cnam_name)
        if cobj_fid and cnam_name:
            name_by_cobj.setdefault(cobj_fid, cnam_name)
        gnam = (r.get("GNAM_FormID") or "").strip().upper()
        if not re.fullmatch(r"[0-9A-F]{8}", gnam):
            continue
        by_plan[gnam] = {
            "cobjFormID": cobj_fid,
            "createdFormID": cnam_fid,
            "createdEdid": (r.get("CNAM_EDID") or "").strip(),
            "createdName": cnam_name,
            "workbench": (r.get("BNAM_FULL") or r.get("BNAM_EDID") or "").strip(),
            "materials": parse_materials(r.get("FVPA") or ""),
        }
    return by_plan, name_by_fid, name_by_cobj


def load_omod(tsv_root: str) -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[Dict[str, str]]]]:
    """(omod by FormID, its property rows by FormID). The properties matter because
    a misc power-armour mod's only property is the dn_HasMisc_* keyword that ties it
    to its perk — see the EffectIndex block."""
    out: Dict[str, Dict[str, str]] = {}
    props: Dict[str, List[Dict[str, str]]] = {}
    try:
        rows = read_tsv(newest(os.path.join(tsv_root, "OMOD_Export_*.tsv"),
                               exclude_substrings=["_Properties"]))
    except (FileNotFoundError, IndexError):
        return out, props
    for r in rows:
        fid = (r.get("OMOD_FormID") or "").strip().upper()
        if not fid:
            continue
        out[fid] = {
            "name": (r.get("FULL") or "").strip(),
            "desc": (r.get("DESC") or "").strip(),
            "edid": (r.get("OMOD_EDID") or "").strip(),
            "attach": (r.get("AttachPoint_Name") or r.get("AttachPoint_EDID") or "").strip(),
        }
    try:
        for r in read_tsv(newest(os.path.join(tsv_root, "OMOD_Export_*_Properties.tsv"))):
            fid = (r.get("OMOD_FormID") or "").strip().upper()
            if fid:
                props.setdefault(fid, []).append(r)
    except (FileNotFoundError, IndexError):
        pass
    return out, props


# ---------------------------------------------------------------------------
# MOD EFFECT TEXT — the OMOD is not where the effect lives
#
# A power-armour misc mod's OMOD record carries a FULL name, an empty DESC, and a
# single property that ADDs a keyword (dn_HasMisc_Tesla, dn_HasMisc_Kinetic …).
# That keyword is SHARED by every power-armour set's copy of the same mod — the
# Vulcan, T-60, T-45, Raider, Ultracite, Excavator, Hellcat and Union versions of
# Kinetic Dynamo all add dn_HasMisc_Kinetic — so the effect is authored once,
# away from the OMOD, and every set inherits it.
#
# Three places actually hold the text:
#
#   1. OMOD DESC          — populated for cosmetic mods only (the headlamps).
#   2. PERK DESC          — a hidden perk whose entry-point conditions test
#                           WornApparelHasKeywordCount(dn_HasMisc_*). This is how
#                           Optimized Bracers gets "Power attacks cost 25% less".
#   3. ENCH → MGEF DNAM   — the enchantment named after the mod; its magic effect's
#                           DNAM_MagicItemDescription is the in-game string, e.g.
#                           Medic Pump → "Automatically uses a Stimpak when Below
#                           50% Health". SPEL records work the same way.
#
# Nothing in the exports draws an explicit OMOD → ENCH line, so (3) is matched by
# name: the OMOD's FULL against the ENCH/SPEL FULL and against the trailing token
# of its EDID (RD01_..._Helmet_Misc_VATSChance ↔ EnchPowerArmor_VATSChance), plus
# a short alias table for the handful the data names differently. Every hit records
# where it came from in `effectSource`, so a bad match is visible on the page rather
# than silently wrong.
#
# Magnitude placeholders (<mag>, <+MAG>, <ITEM1>) are left VERBATIM per the
# rewards-style-guide rule; the enchantment's magnitude/area ride alongside as
# separate fields instead of being substituted into the sentence.
# ---------------------------------------------------------------------------

# Mods whose enchantment is authored under a different name than the mod itself.
# Each one verified by hand against the record pair named in the comment.
EFFECT_ALIASES: Dict[str, str] = {
    # OMOD "Stealth Boy" -> ENCH EnchPowerArmor_StealthScript "Stealth Field"
    "stealthboy": "stealthfield",
    # OMOD "Jet Pack"    -> ENCH EnchPowerArmor_Jetpack "Jetpack"
    "jetpack": "jetpack",
    # OMOD "V.A.T.S. Matrix Overlay" -> ENCH EnchPowerArmor_VATSChance "Targeting Matrix"
    "vatsmatrixoverlay": "targetingmatrix",
    # OMOD "Canteen" (backpack) -> ENCH RD01_EnchBackpack_Effect_Canteen
    "canteen": "canteenbackpackmod",
}

_PLACEHOLDER_RE = re.compile(r"<[^>]{1,24}>")


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _edid_tail(edid: str) -> str:
    """Trailing token of an EDID: RD01_Mod_..._Misc_VATSChance -> 'vatschance'."""
    parts = [p for p in (edid or "").split("_") if p]
    return _norm_key(parts[-1]) if parts else ""


class EffectIndex:
    """Resolves an OMOD to its in-game effect description. See the block above."""

    def __init__(self, tsv_root: str) -> None:
        self.mgef: Dict[str, Dict[str, str]] = {}
        self.by_key: Dict[str, List[Dict[str, Any]]] = {}
        self.perk_by_keyword: Dict[str, Dict[str, str]] = {}
        self._load(tsv_root)

    # -- loading ---------------------------------------------------------

    def _load(self, tsv_root: str) -> None:
        try:
            for r in read_tsv(newest(os.path.join(tsv_root, "MGEF_Export_*.tsv"))):
                fid = (r.get("MGEF_FormID") or "").strip().upper()
                if fid:
                    self.mgef[fid] = r
        except (FileNotFoundError, IndexError):
            pass

        # ENCH — one row per enchantment, effects in numbered columns.
        try:
            for r in read_tsv(newest(os.path.join(tsv_root, "ENCH_Export_*.tsv"))):
                effects = []
                for i in range(1, 8):
                    ref = (r.get(f"Effect_{i}_MGEF_FID") or "").strip()
                    if not ref:
                        continue
                    effects.append({
                        "mgef": ref.split(":")[0].strip().upper(),
                        "magnitude": (r.get(f"Effect_{i}_Magnitude") or "").strip(),
                        "area": (r.get(f"Effect_{i}_Area") or "").strip(),
                    })
                if effects:
                    self._register(
                        kind="ENCH",
                        edid=(r.get("ENCH_EDID") or "").strip(),
                        full=(r.get("ENCH_FULL") or "").strip(),
                        effects=effects,
                        desc="",
                    )
        except (FileNotFoundError, IndexError):
            pass

        # SPEL — header carries the name and DESC, effects live in the twin file.
        try:
            spel_effects: Dict[str, List[Dict[str, Any]]] = {}
            for r in read_tsv(newest(os.path.join(tsv_root, "SPEL_Export_*_EFFECTS.tsv"))):
                fid = (r.get("SPEL_FormID") or "").strip().upper()
                mg = (r.get("EFID_MGEF_FormID") or "").strip().upper()
                if fid and mg:
                    spel_effects.setdefault(fid, []).append({
                        "mgef": mg,
                        "magnitude": (r.get("EFIT_Magnitude") or "").strip(),
                        "area": (r.get("EFIT_Area") or "").strip(),
                    })
            for r in read_tsv(newest(os.path.join(tsv_root, "SPEL_Export_*_HEADER.tsv"))):
                fid = (r.get("SPEL_FormID") or "").strip().upper()
                self._register(
                    kind="SPEL",
                    edid=(r.get("SPEL_EDID") or "").strip(),
                    full=(r.get("SPEL_FULL") or "").strip(),
                    effects=spel_effects.get(fid, []),
                    desc=(r.get("SPEL_DESC") or "").strip(),
                )
        except (FileNotFoundError, IndexError):
            pass

        # PERK — take the NEWEST export that still carries the entry-point
        # condition columns. The Aug 2026 export shipped without them (and with
        # an empty PerkConditions_Flat); the xEdit script was rewritten on
        # 2026-09-05 so later exports have them again.
        #
        # Ordered by rng76's filename date key, not alphabetically: sorting
        # "PERK_Export_<Month>_<Year>" as text puts March above July and August,
        # so an alphabetical scan picked the right file for the wrong reason and
        # would have kept picking it after the export was fixed.
        try:
            import glob as _glob
            candidates = _glob.glob(os.path.join(tsv_root, "PERK_Export_*.tsv"))
            try:
                from rng76 import _filename_date_key as _date_key
            except ImportError:
                _date_key = os.path.basename
            candidates.sort(key=lambda p: (_date_key(p), os.path.basename(p)))
            for path in reversed(candidates):
                rows = read_tsv(path)
                if not rows or "Cond_1" not in rows[0]:
                    continue
                kw_re = re.compile(r"\[KYWD:([0-9A-Fa-f]{8})\]")
                for r in rows:
                    conds = " ".join(
                        v for k, v in r.items()
                        if k and (k.startswith("Cond_") or k == "PerkConditions_Flat") and v
                    )
                    if "dn_Has" not in conds:
                        continue
                    desc = (r.get("DESC") or "").strip().strip('"')
                    for m in kw_re.finditer(conds):
                        key = m.group(1).upper()
                        cur = self.perk_by_keyword.get(key)
                        if cur is None or (not cur.get("desc") and desc):
                            self.perk_by_keyword[key] = {
                                "edid": (r.get("PERK_EDID") or "").strip().strip('"'),
                                "name": (r.get("FULL") or "").strip().strip('"'),
                                "desc": desc,
                            }
                break
        except (FileNotFoundError, IndexError):
            pass

    def _register(self, kind, edid, full, effects, desc) -> None:
        rec = {"kind": kind, "edid": edid, "full": full, "effects": effects, "desc": desc}
        for key in {_norm_key(full), _edid_tail(edid)}:
            if key and len(key) > 2:
                self.by_key.setdefault(key, []).append(rec)

    # -- lookup ----------------------------------------------------------

    # A full-name match ("Medic Pump" == "Medic Pump") is strong enough on its own.
    # An EDID-tail match is not: single tokens like INT or Bleed collide across the
    # whole game, and matching on them alone pulled a Nuka-World clothing
    # enchantment onto Internal Database and the Vault 94 bleed set onto Rusty
    # Knuckles. So a tail match must also land in the same domain as the mod.
    _DOMAIN_RE = re.compile(r"PowerArmor|^RD01|Backpack", re.IGNORECASE)

    def _first_described(self, recs: List[Dict[str, Any]],
                         require_domain: bool = False) -> Optional[Dict[str, Any]]:
        for rec in recs:
            if require_domain and not self._DOMAIN_RE.search(rec.get("edid") or ""):
                continue
            if rec.get("desc"):
                return {"text": rec["desc"],
                        "source": f"{rec['kind']} {rec['edid']}"}
            for eff in rec.get("effects", []):
                mg = self.mgef.get(eff["mgef"])
                if not mg:
                    continue
                dnam = (mg.get("DNAM_MagicItemDescription") or "").strip()
                if not dnam:
                    continue
                out: Dict[str, Any] = {
                    "text": dnam,
                    "source": f"{rec['kind']} {rec['edid']} → MGEF {mg.get('EDID', '')}",
                }
                if _PLACEHOLDER_RE.search(dnam):
                    mag = eff.get("magnitude")
                    area = eff.get("area")
                    if mag and mag not in ("0", "0.000000"):
                        out["magnitude"] = mag
                    if area and area not in ("0", "0.000000"):
                        out["area"] = area
                return out
        return None

    def for_omod(self, omod_row: Dict[str, str],
                 prop_rows: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        if not omod_row:
            return None

        desc = (omod_row.get("desc") or "").strip()
        name = (omod_row.get("name") or "").strip()
        edid = (omod_row.get("edid") or "").strip()

        # 1. The OMOD's own description (cosmetic mods).
        if desc and desc != name:
            return {"text": desc, "source": f"OMOD {edid}"}

        # 2. A hidden perk keyed on the mod's shared dn_HasMisc_* keyword.
        kw_re = re.compile(r"\[KYWD:([0-9A-Fa-f]{8})\]")
        for pr in prop_rows or []:
            for m in kw_re.finditer(pr.get("Value1") or ""):
                perk = self.perk_by_keyword.get(m.group(1).upper())
                if perk and perk.get("desc"):
                    return {"text": perk["desc"], "source": f"PERK {perk['edid']}"}

        # 3. The enchantment or spell named after the mod. Name keys are trusted;
        #    EDID-tail keys have to prove they're in the same domain.
        keys: List[Tuple[str, bool]] = []
        nk = _norm_key(name)
        if len(nk) >= 5:
            keys.append((EFFECT_ALIASES.get(nk, nk), False))
        tail = _edid_tail(edid)
        if len(tail) >= 5:
            keys.append((EFFECT_ALIASES.get(tail, tail), True))
        seen: Set[str] = set()
        for key, needs_domain in keys:
            if not key or key in seen:
                continue
            seen.add(key)
            hit = self._first_described(self.by_key.get(key, []), needs_domain)
            if hit:
                return hit
        return None


def load_legendary_effects(repo_root: Path) -> Dict[str, str]:
    """name(lower)+star -> effect text, from dist/legendary_mods.json."""
    out: Dict[str, str] = {}
    p = repo_root / "dist" / "legendary_mods.json"
    if not p.exists():
        return out
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return out
    for grp in data.get("groups", []):
        for mod in grp.get("mods", []):
            name = (mod.get("name") or "").strip().lower()
            star = mod.get("star")
            eff = (mod.get("effect") or "").strip()
            if name and eff:
                out[f"{name}|{star}"] = eff
                out.setdefault(name, eff)
    return out


# ---------------------------------------------------------------------------
# CONDITIONS
#
# Raw xEdit condition strings are unreadable on a guide page, and only eight
# distinct functions appear anywhere in the raid pools. Each is mapped by hand.
#
# Reading the comparison follows the house convention set by simplify_condition()
# in build_activities_rewards_json.py: use the trailing comparison VALUE, never the
# flag bit-string in front of it. So `HasLearnedRecipe(...) … 0.000000` means the
# player must NOT have learned it — the plan stops dropping once you know it — and
# `… 1.000000` means the plan is required first. Anything with no confident reading
# renders as nothing rather than as invented wording.
# ---------------------------------------------------------------------------

_COND_FN_RE = re.compile(r"^(?:\w+\.)?(\w+)\(")
_COND_VAL_RE = re.compile(r"([-\d]+\.\d+)\s*$")
_COBJ_REF_RE = re.compile(r"\[COBJ:([0-9A-Fa-f]{8})\]")
_KW_REF_RE = re.compile(r"([A-Za-z0-9_]+)\s*\[KYWD:[0-9A-Fa-f]{8}\]")
_GLOB_REF_RE = re.compile(r"([A-Za-z0-9_]+)\s*\[GLOB:[0-9A-Fa-f]{8}\]")
_AMMO_KW_RE = re.compile(r"^IsAmmoType_(.+)$")


def simplify_condition(raw: str, cobj_names: Dict[str, str]) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    fm = _COND_FN_RE.match(s)
    fn = fm.group(1) if fm else ""
    vm = _COND_VAL_RE.search(s)
    val = float(vm.group(1)) if vm else 1.0

    if fn == "HasLearnedRecipe":
        name = ""
        cm = _COBJ_REF_RE.search(s)
        if cm:
            name = cobj_names.get(cm.group(1).upper(), "")
        if name.startswith(("Plan: ", "Recipe: ")):
            name = name.split(": ", 1)[1]
        if val >= 1.0:
            return (f"Requires Plan: {name} to be learned" if name
                    else "Requires the base plan to be learned")
        return (f"Won’t drop if you’ve already learned Plan: {name}" if name
                else "Won’t drop if you’ve already learned this recipe")

    if fn == "WornHasKeyword":
        # Contextual ammo: the keyword names the ammo type, which is the row's own
        # name anyway. Phrasing it per-item avoids mangling calibre EDIDs
        # (IsAmmoType_308Caliber would read as "308Caliber", not ".308").
        km = _KW_REF_RE.search(s)
        if not km or not _AMMO_KW_RE.match(km.group(1)):
            return ""
        return "Only drops while a weapon that uses this ammunition is equipped"

    if fn == "GetGlobalValue":
        gm = _GLOB_REF_RE.search(s)
        if not gm:
            return ""
        # Drifter (P62) GLOBs are cut-content plumbing — hidden everywhere else on
        # the site (build_activities_rewards_json.simplify_condition), so hidden here.
        if re.search(r"P62|Drifter", gm.group(1), re.IGNORECASE):
            return ""
        pretty = re.sub(r"^ContextualAmmo_", "", gm.group(1))
        pretty = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", pretty).replace("_", " ").strip()
        return f"Toggle: {pretty}"

    if fn == "GetItemCount" and val < 1.0:
        return "Won’t drop if you already hold one"

    # GetRandomPercent is already expressed by the drop rate. IsTrueForConditionForm
    # points at a CNDF whose pass/fail sense cannot be read from the export, and the
    # rows carrying it also carry a HasLearnedRecipe that says the same thing in
    # plain words. GetIsInExpedition / GetIsInDailyOps gate the shared contextual
    # ammo list for other game modes and mean nothing inside a raid. All hidden.
    return ""


def simplify_conditions(raws: List[str], cobj_names: Dict[str, str]) -> List[str]:
    out: List[str] = []
    for raw in raws or []:
        s = simplify_condition(raw, cobj_names)
        if s and s not in out:
            out.append(s)
    return out


# ---------------------------------------------------------------------------
# ITEM ASSEMBLY
# ---------------------------------------------------------------------------

def image_urls(edid: str, formid: str) -> List[str]:
    stem = (edid or formid or "").strip().lower()
    if not stem:
        return []
    return [IMAGE_BASE + stem + ".avif"]


def build_outputs(
    item: Dict[str, Any],
    cobj_by_plan: Dict[str, Dict[str, Any]],
    omod: Dict[str, Dict[str, str]],
    omod_props: Dict[str, List[Dict[str, str]]],
    effects: Optional["EffectIndex"],
    leg_effects: Dict[str, str],
    stars: Optional[int],
) -> Optional[Dict[str, Any]]:
    """
    'Output & Effects' payload.

    A plan has no effect of its own, so for a mod plan we follow the chain the game
    uses — plan (BOOK) -> COBJ -> created object -> OMOD — and surface the mod's
    name, attach point and description. Where the game data carries no effect text
    (most Vulcan misc mods only add a keyword), the field is simply absent rather
    than filled with invented wording.
    """
    out: Dict[str, Any] = {}
    sig = (item.get("sig") or "").upper()
    fid = (item.get("formid") or "").upper()
    edid = item.get("edid") or ""

    # Legendary mod shard → real effect text from dist/legendary_mods.json
    if sig == "MISC" and edid.lower().startswith("legendaryshard_"):
        plain = re.sub(r"^\d★\s*", "", item.get("name") or "").strip().lower()
        eff = leg_effects.get(f"{plain}|{stars}") or leg_effects.get(plain)
        out["kind"] = "legendaryShard"
        out["teaches"] = item.get("name")
        if eff:
            out["effect"] = eff
        if stars:
            out["star"] = stars
        return out

    # Plan / recipe → what it unlocks
    if sig == "BOOK":
        c = cobj_by_plan.get(fid)
        if not c:
            return None
        out["kind"] = "plan"
        created_name = c.get("createdName") or c.get("createdEdid")
        created_fid = c.get("createdFormID") or ""
        om = omod.get(created_fid)
        if om:
            out["teaches"] = om.get("name") or created_name
            if om.get("attach"):
                out["attachPoint"] = om["attach"]
            # The effect is rarely on the OMOD itself — EffectIndex walks the
            # OMOD → keyword → PERK and OMOD → ENCH/SPEL → MGEF trails for it.
            if effects is not None:
                hit = effects.for_omod(om, omod_props.get(created_fid, []))
                if hit:
                    out["effect"] = hit["text"]
                    out["effectSource"] = hit["source"]
                    if hit.get("magnitude"):
                        out["effectMagnitude"] = hit["magnitude"]
                    if hit.get("area"):
                        out["effectArea"] = hit["area"]
        else:
            out["crafts"] = created_name
        if c.get("workbench"):
            out["workbench"] = c["workbench"]
        if c.get("materials"):
            out["materials"] = c["materials"]
        return out or None

    return None


def assemble_items(
    lvli_fid: str,
    resolver,
    non_tradable: Set[str],
    unsellable: Set[str],
    cobj_by_plan: Dict[str, Dict[str, Any]],
    cobj_names: Dict[str, str],
    cobj_cond_names: Dict[str, str],
    omod: Dict[str, Dict[str, str]],
    omod_props: Dict[str, List[Dict[str, str]]],
    effects: Optional["EffectIndex"],
    leg_effects: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Resolve one pool and collapse duplicate quantity rows.

    The game lists a single item once per possible quantity (Stimpak x6, x5, x4, x3).
    A checklist wants one tickable row, so rows sharing a FormID are merged: chances
    add (they are mutually exclusive outcomes of the same pick) and the quantities
    become a min-max range.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    for raw in resolver.resolve_deep(lvli_fid):
        fid = (raw.get("formid") or "").upper()
        if not fid:
            continue
        qty = int(raw.get("qty") or 0) or 1
        if fid not in merged:
            order.append(fid)
            name, stars = destar(raw.get("name") or fid)
            edid = raw.get("edid") or ""
            sig = (raw.get("sig") or "").upper()
            # AMMO records carry no FULL name in the export; borrow the crafting
            # recipe's name so "Ammo10mm" reads as "10mm Round".
            if sig == "AMMO" and cobj_names.get(fid):
                name = cobj_names[fid]
            if sig == "LGDI":
                lname, lrank = lgdi_name(edid)
                if lname:
                    name = lname
                    if lrank:
                        stars = lrank
            if fid in NAME_OVERRIDES:
                name = NAME_OVERRIDES[fid]
            fid_l = fid.lower()
            entry: Dict[str, Any] = {
                "formid": fid,
                "name": name,
                "edid": edid,
                "sig": sig,
                "qtyMin": qty,
                "qtyMax": qty,
                "dropRate": 0.0,
                "conditions": simplify_conditions(list(raw.get("conditions") or []), cobj_cond_names),
                "images": image_urls(edid, fid),
            }
            if stars:
                entry["star"] = stars
            if fid_l in non_tradable:
                entry["tradeable"] = False
            else:
                entry["tradeable"] = True
            if fid_l in unsellable:
                entry["unsellable"] = True
            outputs = build_outputs(entry, cobj_by_plan, omod, omod_props, effects,
                                    leg_effects, stars)
            if outputs:
                entry["outputs"] = outputs
            merged[fid] = entry
        e = merged[fid]
        e["dropRate"] += float(raw.get("dropRate") or 0.0)
        e["qtyMin"] = min(e["qtyMin"], qty)
        e["qtyMax"] = max(e["qtyMax"], qty)

    items = []
    for fid in order:
        e = merged[fid]
        raw_pct = pct(e["dropRate"]) if e["dropRate"] <= 1.0 else e["dropRate"] * 100.0
        # An item that appears in more than one branch of the same tree (contextual
        # ammo hits both the "matches your equipped weapon" branch and the generic
        # fallback) can sum past 100 once the branches are flattened. Cap it and
        # flag the row so the overshoot is visible rather than silently rounded.
        if raw_pct > 100.0:
            e["rateCapped"] = round(raw_pct, 4)
            raw_pct = 100.0
        e["dropRate"] = round(raw_pct, 6)
        if e["qtyMin"] == e["qtyMax"]:
            e["qty"] = e["qtyMin"]
        items.append(e)
    return items


# ---------------------------------------------------------------------------
# SUBTITLES (rewards-style-guide blurb templates)
# ---------------------------------------------------------------------------

def pool_subtitle(flags: Dict[str, bool], n_items: int) -> str:
    unit = "item" if n_items == 1 else "items"
    if flags.get("use_all"):
        return f"Each item rolls independently · {n_items} {unit}"
    return f"Chance drop of one item · {n_items} {unit}"


def fmt_seconds(seconds: int) -> str:
    return f"{seconds // 60}:{seconds % 60:02d}"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Building Gleaming Depths All Rewards... (mode={'PTS' if PTS else 'LIVE'})")

    tsv_root = str(_REPO_ROOT / ("tsv/pts" if PTS and (_REPO_ROOT / "tsv/pts").exists() else "tsv"))
    print(f"  TSV root : {tsv_root}")

    data = Rng76Data.from_tsv_root(tsv_root)
    resolver, lvli = data.resolver, data.lvli

    non_tradable, unsellable = load_keyword_flags(tsv_root)
    cobj_by_plan, cobj_names, cobj_cond_names = load_cobj(tsv_root)
    omod, omod_props = load_omod(tsv_root)
    effects = EffectIndex(tsv_root)
    leg_effects = load_legendary_effects(_REPO_ROOT)
    print(f"  KYWD non-tradable: {len(non_tradable)}  unsellable: {len(unsellable)}")
    print(f"  COBJ by plan: {len(cobj_by_plan)}  COBJ names: {len(cobj_names)}  "
          f"OMOD: {len(omod)}  effect keys: {len(effects.by_key)}  "
          f"perk-by-keyword: {len(effects.perk_by_keyword)}  "
          f"legendary effects: {len(leg_effects)}")

    # EDID -> FormID for the off-list pools (trophies, keycard)
    fid_by_edid: Dict[str, str] = {}
    for row in lvli.list_rows:
        ed = (row.get("LVLI_EDID") or row.get("EDID") or "").strip()
        fid = (row.get("LVLI_FormID") or row.get("FormID") or "").strip().upper()
        if ed and fid:
            fid_by_edid.setdefault(ed, fid)

    # 1) Walk each stage's Enc list and record every direct entry as a pool.
    #    stage_chance is the entry's own chance of firing at all: 100 - ChanceNone.
    seen_pools: Dict[str, Dict[str, Any]] = {}   # sub FormID -> {edid, stages{n: chance}}
    stage_order: Dict[int, List[str]] = {}

    for cfg in STAGES:
        st = cfg["stage"]
        stage_order[st] = []
        for entry in lvli.entries_by_list.get(cfg["encFormID"], []):
            ref = (entry.get("LVLO_Reference") or "").strip()
            parts = ref.split(":")
            sub = parts[0].strip().upper()
            ed = parts[1].strip() if len(parts) > 1 else ""
            if not re.fullmatch(r"[0-9A-F]{8}", sub):
                continue
            try:
                cn = float(entry.get("LVOV_ChanceNoneValue") or 0.0)
            except ValueError:
                cn = 0.0
            rec = seen_pools.setdefault(sub, {"edid": ed, "stages": {}})
            rec["stages"][st] = round(100.0 - cn, 4)
            stage_order[st].append(sub)

    shared_fids = {f for f, r in seen_pools.items() if len(r["stages"]) > 1}
    print(f"  Pools: {len(seen_pools)}  shared: {len(shared_fids)}  stage-unique: {len(seen_pools) - len(shared_fids)}")

    # 2) General Stage Rewards — shared pools, one row per item, rate per stage.
    general_pools: List[Dict[str, Any]] = []
    order_index = {e: i for i, e in enumerate(GENERAL_ORDER)}
    for fid in sorted(shared_fids, key=lambda f: order_index.get(seen_pools[f]["edid"], 99)):
        rec = seen_pools[fid]
        items = assemble_items(fid, resolver, non_tradable, unsellable,
                               cobj_by_plan, cobj_names, cobj_cond_names,
                                   omod, omod_props, effects, leg_effects)
        flags = lvli.flags_for(fid)
        stage_chance = {str(s): rec["stages"].get(s, 0.0) for s in range(1, STAGE_COUNT + 1)}
        # Item chance per stage = pool chance x the item's chance inside the pool.
        for it in items:
            it["stageDropRate"] = {
                s: round(stage_chance[s] * it["dropRate"] / 100.0, 6)
                for s in stage_chance
            }
        general_pools.append({
            "poolId": pool_id("general", rec["edid"]),
            "title": pool_label(rec["edid"]),
            "lvliFormID": fid,
            "lvliEdid": rec["edid"],
            "flags": flags,
            "stageChance": stage_chance,
            "subtitle": pool_subtitle(flags, len(items)),
            "items": items,
        })

    general = {
        "title": "General Stage Rewards",
        "subtitle": (
            f"Dropped by every stage · {len(general_pools)} reward lists · "
            "chance varies per stage"
        ),
        "note": (
            "These lists are shared by all five stages — the same leveled list "
            "FormID appears under each one, so the items never change, only the "
            "chance the list fires. They are not tracked on the stage checklists."
        ),
        "pools": general_pools,
    }

    # 3) Per-stage sections — stage-unique pools plus trophy / keycard.
    stages_out: List[Dict[str, Any]] = []
    for cfg in STAGES:
        st = cfg["stage"]
        pools: List[Dict[str, Any]] = []

        for fid in stage_order[st]:
            if fid in shared_fids:
                continue
            rec = seen_pools[fid]
            items = assemble_items(fid, resolver, non_tradable, unsellable,
                                   cobj_by_plan, cobj_names, cobj_cond_names,
                                   omod, omod_props, effects, leg_effects)
            flags = lvli.flags_for(fid)
            chance = rec["stages"].get(st, 100.0)
            for it in items:
                it["stageDropRate"] = round(chance * it["dropRate"] / 100.0, 6)
            pools.append({
                "poolId": pool_id(f"stage-{st}", rec["edid"]),
                "title": pool_label(rec["edid"]),
                "lvliFormID": fid,
                "lvliEdid": rec["edid"],
                "flags": flags,
                "poolChance": chance,
                "subtitle": pool_subtitle(flags, len(items)),
                "items": items,
            })

        # Speed-run trophy — a GMRW reward on the quest, not an Enc list entry.
        extra_edids = [cfg["trophyEdid"]] + list(cfg.get("extraPoolEdids") or [])
        for ed in extra_edids:
            fid = fid_by_edid.get(ed)
            if not fid:
                print(f"  ! stage {st}: pool EDID not found: {ed}")
                continue
            items = assemble_items(fid, resolver, non_tradable, unsellable,
                                   cobj_by_plan, cobj_names, cobj_cond_names,
                                   omod, omod_props, effects, leg_effects)
            for it in items:
                it["stageDropRate"] = it["dropRate"]
            is_trophy = ed.endswith("_Trophies")
            pools.append({
                "poolId": pool_id(f"stage-{st}", ed),
                "title": pool_label(ed),
                "lvliFormID": fid,
                "lvliEdid": ed,
                "flags": lvli.flags_for(fid),
                "poolChance": 100.0,
                "subtitle": pool_subtitle(lvli.flags_for(fid), len(items)),
                "conditionSummary": (
                    f"Complete the stage in under {fmt_seconds(cfg['speedrunSeconds'])}"
                    if is_trophy else "Must be an active participant"
                ),
                "items": items,
            })

        total = sum(len(p["items"]) for p in pools)
        stages_out.append({
            "stage": st,
            "title": f"Stage {st} Rewards",
            "gameName": cfg["gameName"],
            "bossName": cfg["bossName"],
            "questFormID": cfg["questFormID"],
            "encFormID": cfg["encFormID"],
            "speedrunSeconds": cfg["speedrunSeconds"],
            "speedrunLabel": fmt_seconds(cfg["speedrunSeconds"]),
            "subtitle": f"{cfg['bossName']} · {total} tracked {'reward' if total == 1 else 'rewards'}",
            "itemCount": total,
            "pools": pools,
        })
        print(f"  Stage {st}: {len(pools)} pools, {total} tracked rewards")

    page = {
        "slug": PAGE_SLUG,
        "path": PAGE_PATH,
        "name": "Gleaming Depths",
        "pageType": "raid",
        "description": (
            "Every reward the Gleaming Depths raid can drop, split by stage. The "
            "General Stage Rewards list is shared by all five stages; each stage "
            "expand tracks only what that stage alone can give you."
        ),
        "generated": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "general": general,
        "stages": stages_out,
    }

    output: Dict[str, Any] = {"byPage": {PAGE_SLUG: page, PAGE_PATH: page}}
    if PTS:
        output["isPts"] = True

    out_dir = (_REPO_ROOT / "dist" / "pts" / "raids") if PTS else (_REPO_ROOT / "dist" / "raids")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "raids_rewards.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_items = (
        sum(len(p["items"]) for p in general_pools)
        + sum(s["itemCount"] for s in stages_out)
    )
    print(f"\n✓ Wrote {out_file}")
    print(f"✓ General pools: {len(general_pools)}  Stages: {len(stages_out)}  Items: {total_items}")
    print(f"  File size: {out_file.stat().st_size / 1024:.0f} KB")

    write_empty_patchlog_feed(str(out_dir), "patchlog_latest_df_raids.json", total_items)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
