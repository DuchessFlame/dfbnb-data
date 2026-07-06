#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
src/build_fishing_daily_rewards_json.py
---------------------------------------
Builds the reward data feed for the DF/BNB "Big Fish in a Small Pond" page
(df-bnb-fishing-guides.add.js -> renderBigFishGuide).

"Big Fish in a Small Pond" is the Pip-Boy Daily fishing quest. Speak to Captain
Raymond at Fisherman's Rest, get sent to a region, catch 7 fish, hand in. Every
reward on the page is derived GENERATIVELY from the game-data exports so the page
stays correct across patches:

  QUEST -> the quest record (FormID / EDID / display name / blurb / reward GMRW)
  GMRW  -> the reward record `Fishing_BigFish_Reward` (9 reward indices):
             - XP / Caps / Legendary Scrip flat amounts (GLOB FLTV)
             - 6 regional Yellow-Box weapon-mod LVLIs (one per assigned region)
             - the main quest-reward pool LVLI (bait / displays / hook mods /
               CAMP & recipe plans / contextual aid & ammo)
  LVLI  -> resolved through the shared rng76.py drop-rate engine so the per-pool
           and per-item percentages match every other drop-rate page on the site.

Drop-rate maths come ONLY from rng76.py (the canonical engine). This script does
NOT re-implement any formula logic, so there is no standalone-copy to keep in sync
(unlike build_activities_rewards_json.py). The only curated input is POOL_META —
the persistence / tradeable / droppable blurb legends and cross-links, which are
gameplay facts not present in the TSV data.

Two modes:
  (default / live)  reads tsv/      -> dist/fishing_big_fish.json
  --pts             reads tsv/pts/  -> dist/pts/fishing_big_fish.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.

Env overrides (used by the in-session sandbox verifier, ignored in CI):
  FBF_TSV_DIR   override the TSV directory
  FBF_OUT       override the output file path

No external dependencies beyond rng76.py (stdlib only).
"""

import os
import re
import sys
import glob
import json
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")

# Make the shared engine importable (same dir / src).
for _p in (SCRIPT_DIR, os.path.join(SCRIPT_DIR, "..", "src")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from rng76 import Rng76Data, read_tsv, newest, pick  # noqa: E402

# Reuse the fish-journal food-effect renderer verbatim so recipe buffs read exactly
# like the cooked-meal effects on the Field Journal page (same GLOB tier resolution,
# same "+X Stat for Y min" formatting). We only borrow its pure helpers.
import build_fish_journal_json as _fj  # noqa: E402

PTS = "--pts" in sys.argv

# ---------------------------------------------------------------------------
# Quest / reward identifiers (stable FormIDs — verified June 2026 export)
# ---------------------------------------------------------------------------
QUEST_FORMID = "007B95E1"
QUEST_EDID = "Fishing_BigFish"
REWARD_GMRW_EDID = "Fishing_BigFish_Reward"
MAIN_POOL_LVLI = "007DC663"          # Fishing_BigFish_LL_Quest_Rewards

# Region weapon-mod LVLI EDID -> friendly region name (the 6 lists wired into the
# GMRW for the daily). Keyed by EDID so it survives FormID churn between patches.
REGION_MOD_BY_EDID = {
    "LL_Mods_Weapons_Any_RegionForest":       "The Forest",
    "LL_Mods_Weapons_Any_RegionToxicValley":  "Toxic Valley",
    "LL_Mods_Weapons_Any_RegionSavageDivide": "Savage Divide",
    "LL_Mods_Weapons_Any_RegionMire":         "The Mire",
    "LL_Mods_Weapons_Any_RegionAshHeap":      "Ash Heap",
    "LL_Mods_Weapons_Any_RegionCranberryBog": "Cranberry Bog",
    "LL_Mods_Weapons_Any_RegionSkylineValley":"Skyline Valley",
}

# Friendly group titles + curated blurb legends, keyed by the main-pool sub-list
# EDID (or "_curved" for the direct BOOK entry). The NUMBERS are computed by the
# engine; only the descriptive legends live here.
POOL_META = {
    "Fishing_BigFish_LL_Quest_Rewards_Bait": {
        "key": "bait",
        "title": "Fishing Bait",
        "legends": ["Always drops"],
        "tradeable": True, "droppable": True,
        "note": "Handed out every single time you complete the daily.",
    },
    "_curved": {
        "key": "curvedDisplay",
        "title": "Curved Fish Display",
        "legends": ["Stops dropping once learnt"],
        "tradeable": True, "droppable": True,
        "note": "A CAMP display plaque. Drops at 100% until you learn it, then "
                "it leaves the pool.",
    },
    "Fishing_BigFish_LL_Quest_Rewards_HookMods": {
        "key": "hookMods",
        "title": "Fishing Hook Mod Plans",
        "legends": ["Stops dropping once learnt"],
        "tradeable": True, "droppable": True,
        "relatedGuide": {"label": "Reel Talk: Rod Mods, Hooks & Reels",
                          "url": "/df/fishing/reel-talk/"},
        "note": "The five hook upgrades. The pool only fires 20% of the time, and "
                "when it does it picks one plan — so each plan is ~4% per hand-in. "
                "Each plan stops dropping once you have learnt it.",
    },
    "Fishing_BigFish_LL_Quest_Rewards_Recipes": {
        "key": "campRecipePlans",
        "title": "CAMP & Recipe Plans",
        "legends": ["Keeps dropping once learnt"],
        "tradeable": True, "droppable": True,
        "note": "Drops every hand-in and KEEPS dropping even after they are "
                "learnt — perfect for collecting duplicates to trade or gift.",
    },
    "QuestReward_LLS_AllRegions_Any": {
        "key": "contextualAid",
        "title": "Contextual Aid & Ammo",
        "legends": ["Always drops"],
        "tradeable": False, "droppable": True,
        "note": "A scaling bundle of aid (stimpaks, RadAway, chems) and ammo "
                "appropriate to your level and loadout.",
    },
    "_regionalMod": {
        "key": "regionalWeaponMod",
        "title": "Regional Weapon Mod",
        "legends": ["Always drops"],
        "tradeable": True, "droppable": True,
        "note": "One random Yellow-Box weapon mod from the region Captain Raymond "
                "sent you to. You always receive exactly one.",
    },
}

# Blurb-legend key shown on the page so readers know what each tag means.
BLURB_LEGEND = [
    {"term": "Always drops",
     "desc": "Guaranteed every time you hand the daily in."},
    {"term": "Stops dropping once learnt",
     "desc": "Removed from the reward pool for that character once you have "
             "learnt the plan."},
    {"term": "Keeps dropping once learnt",
     "desc": "Continues to drop even after you have learnt it, so you can "
             "stockpile duplicates."},
    {"term": "Tradeable",
     "desc": "Can be traded to other players."},
    {"term": "Droppable",
     "desc": "Can be dropped from your inventory (e.g. to gift to a new player)."},
    {"term": "Pool chance",
     "desc": "How often this reward group appears at all on a hand-in."},
    {"term": "Per-item chance",
     "desc": "The chance of a specific item within the group, factoring the pool "
             "chance in."},
]

# Other BNB guides referenced from the DF source guide.
RELATED_GUIDES = [
    {"label": "Hook, Line & Ledger — fishing system overview",
     "url": "/df/fishing/hook-line-ledger/"},
    {"label": "Reel Talk — rod mods, hooks & reels",
     "url": "/df/fishing/reel-talk/"},
    {"label": "What's Biting? — fish, bait & weather",
     "url": "/df/fishing/whats-biting/"},
    {"label": "Linda-Lee & the Chum Trough",
     "url": "/df/fishing/linda-lee-chum-trough-rewards/"},
]

# The lifetime-challenge "additional rewards" (plushies etc.) are intentionally
# NOT enumerated here — the page links out to the all-rewards checklist instead.
ALL_REWARDS_CHECKLIST = "/df/fishing/all-rewards-checklist/"


# ---------------------------------------------------------------------------
# TSV plumbing
# ---------------------------------------------------------------------------

def tsv_dir():
    if os.environ.get("FBF_TSV_DIR"):
        return os.environ["FBF_TSV_DIR"]
    return os.path.join(SCRIPT_DIR, "..", "tsv", "pts" if PTS else "")


def out_path():
    if os.environ.get("FBF_OUT"):
        return os.environ["FBF_OUT"]
    return os.path.join(DIST_DIR, "pts", "fishing_big_fish.json") if PTS \
        else os.path.join(DIST_DIR, "fishing_big_fish.json")


def _ref_parts(ref):
    """'007DC664:EDID:LVLI' -> ('007DC664', 'EDID', 'LVLI')."""
    p = (ref or "").split(":")
    fid = p[0].strip().upper() if p else ""
    edid = p[1].strip() if len(p) > 1 else ""
    sig = p[-1].strip().upper() if len(p) > 1 else ""
    return fid, edid, sig


def _to_float(s, default=0.0):
    try:
        return float(str(s).strip())
    except (TypeError, ValueError):
        return default


def _round(x, n=4):
    return round(float(x), n)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def load_quest(root):
    """Return the quest header dict from the QUEST export."""
    try:
        rows = read_tsv(newest(os.path.join(root, "QUEST_Export_*.tsv")))
    except (FileNotFoundError, ValueError):
        rows = []
    for r in rows:
        fid = pick(r, "FormID", "QUST_FormID").strip().upper()
        edid = pick(r, "EDID", "QUST_EDID").strip()
        if fid == QUEST_FORMID or edid == QUEST_EDID:
            return {
                "formId": fid or QUEST_FORMID,
                "edid": edid or QUEST_EDID,
                "gameName": pick(r, "FULL", "Name").strip()
                            or "Daily: Big Fish in a Small Pond",
                "description": pick(r, "Description", "CNAM", "Objective").strip()
                            or "Grab a rod and scour Appalachia's waters for fish!",
            }
    return {
        "formId": QUEST_FORMID, "edid": QUEST_EDID,
        "gameName": "Daily: Big Fish in a Small Pond",
        "description": "Grab a rod and scour Appalachia's waters for fish!",
    }


def load_reward_rows(root):
    """All GMRW rows for the daily's reward record, in reward-index order."""
    rows = read_tsv(newest(os.path.join(root, "GMRW_Export_*.tsv")))
    out = [r for r in rows if pick(r, "EDID").strip() == REWARD_GMRW_EDID]
    out.sort(key=lambda r: _to_float(pick(r, "RewardIndex"), 0))
    return out


def glob_value(data, ref):
    """GLOB ref ('007DC661:EDID:GLOB' or bare FormID) -> FLTV float, or None."""
    fid, _e, _s = _ref_parts(ref)
    if not fid:
        return None
    v = data.globs.vals.get(fid)
    return v if v is not None else None


def base_rewards(data, reward_rows):
    """XP / Caps / Legendary Scrip flat amounts + the contextual ammo & aid note."""
    out = []
    seen = set()
    for r in reward_rows:
        xp = glob_value(data, pick(r, "NAM7_XPGlobal"))
        caps = glob_value(data, pick(r, "NAM8_CapsGlobal"))
        cur = pick(r, "QRCO_CurrencyObject")
        if xp is not None and "xp" not in seen:
            out.append({"label": "XP", "value": int(xp),
                        "meta": {"source": "GMRW", "globRef": pick(r, "NAM7_XPGlobal")}})
            seen.add("xp")
        if caps is not None and cur and "Caps" in cur and "caps" not in seen:
            out.append({"label": "Caps", "value": int(caps),
                        "meta": {"source": "GMRW", "globRef": pick(r, "NAM8_CapsGlobal")}})
            seen.add("caps")
        elif caps is not None and cur and "LegendaryToken" in pick(r, "NAM8_CapsGlobal") \
                and "scrip" not in seen:
            out.append({"label": "Legendary Scrip", "value": int(caps),
                        "meta": {"source": "GMRW", "globRef": pick(r, "NAM8_CapsGlobal")}})
            seen.add("scrip")
    # Contextual ammo + aid are surfaced as their own pool (see contextual aid),
    # but we also flag them in the base summary for the at-a-glance row.
    out.append({"label": "Contextual Ammo", "value": None,
                "meta": {"source": "LVLI", "note": "Scaled to your level & loadout."}})
    out.append({"label": "Aid", "value": None,
                "meta": {"source": "LVLI", "note": "Stimpaks, RadAway and chems."}})
    return out


def _prettify_ammo(name):
    """Drop the leading 'Ammo ' the TSV names carry (e.g. 'Ammo Fusion Cell'
    -> 'Fusion Cell')."""
    s = str(name).strip()
    return s[5:].strip() if s.lower().startswith("ammo ") else s


def _plan_drop_conditions(raw_conds):
    """Translate the raw CNDF strings on a plan entry into plain-English drop
    conditions. The daily plans only ever carry a self-referential
    HasLearnedRecipe check (the plan stops dropping once its recipe is learned)."""
    out = []
    for c in raw_conds or []:
        s = str(c)
        if "HasLearnedRecipe" in s or "HasPerk" in s:
            msg = "Won't drop if you've already learned this plan"
            if msg not in out:
                out.append(msg)
    return out


def _items_from_deep(items, gate):
    """rng76 resolve_deep items -> trimmed item dicts with absolute % = gate*within."""
    out = []
    for it in items:
        within = float(it.get("dropRate", 0.0))      # 0..1 within the sub-list
        d = {
            "formId": it.get("formid", ""),
            "name": it.get("name", ""),
            "qty": int(it.get("qty", 1) or 1),
            "dropRate": _round(gate * within * 100, 4),   # absolute %
            "isPlan": str(it.get("sig", "")).upper() == "BOOK"
                      and str(it.get("name", "")).lower().startswith(("plan", "recipe")),
            "sig": it.get("sig", ""),
        }
        conds = _plan_drop_conditions(it.get("conditions"))
        if conds:
            d["conditions"] = conds
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Recipe -> cooked-dish buff resolution (Task 2)
#
# A "Recipe: <Dish>" BOOK in the campRecipePlans pool teaches a cooked ALCH food of
# the same base name. We resolve that food's buff string using the SAME renderer as
# build_fish_journal_json (build_alch_effects / render_effect + GLOB tier resolution),
# then append the diet the meal satisfies. Diet in FO76 food isn't a discrete keyword
# on the record — a cooked meal counts toward Carnivore when it carries the meat
# ingredient keyword and Herbivore when it carries the vegetable/produce keyword.
# ---------------------------------------------------------------------------

# Keyword-EDID substrings that mark a meal's diet (read from the ALCH Keyword_N cols).
_DIET_KEYWORDS = [
    ("Carnivore", ("IngredientTypeMeat",)),
    ("Herbivore", ("IngredientTypeVegetable", "IngredientTypeFruit",
                   "IngredientTypeHerb", "IngredientTypeProduce")),
]


def _newest_tsv(root, prefix):
    """Newest '<prefix>*.tsv' in root, or None (mirrors build_fish_journal's picker)."""
    return _fj.newest(root, prefix, pts=PTS)


def _build_recipe_buff_map(root):
    """base-dish name (lower) -> buff string, e.g. 'fish and tatos' ->
    '+5% XP for 30 min · Carnivore'. Empty if the exports can't be read."""
    try:
        glob_map = _fj.build_glob(read_tsv(_newest_tsv(root, "GLOB_Export_")))
    except (FileNotFoundError, ValueError, TypeError):
        glob_map = {}

    # ALCH effects TSV (…_Effects.tsv) -> edid -> [rendered effect strings].
    eff_path = None
    for p in glob.glob(os.path.join(root, "ALCH_Export_*Effects*.tsv")):
        if PTS == ("PTS" in os.path.basename(p)):
            eff_path = p
    eff_map = _fj.build_alch_effects(read_tsv(eff_path) if eff_path else [], glob_map)

    # ALCH main -> per-food: FULL name + diet (from ingredient keyword columns).
    alch_rows = read_tsv(_newest_tsv(root, "ALCH_Export_")) or []
    alch_rows = [r for r in alch_rows if "ALCH_FormID" in r]
    by_edid_name, by_edid_diet, by_name_edid = {}, {}, {}
    for r in alch_rows:
        edid = (r.get("ALCH_EDID") or "").strip()
        full = (r.get("FULL") or "").strip()
        if not edid:
            continue
        by_edid_name[edid] = full
        if full:
            by_name_edid.setdefault(full.lower(), edid)
        kw_blob = " ".join(v for k, v in r.items()
                           if v and (k or "").startswith("Keyword"))
        diet = None
        for label, tokens in _DIET_KEYWORDS:
            if any(t in kw_blob for t in tokens):
                diet = label
                break
        by_edid_diet[edid] = diet

    # COBJ: map each recipe BOOK -> the ALCH it produces (robust BOOK->ALCH link).
    # A COBJ's GNAM is the required recipe BOOK ("Recipe: <Dish>"), CNAM is the
    # produced object. We key by the recipe name so a BOOK entry resolves to its meal.
    cobj_rows = read_tsv(_newest_tsv(root, "COBJ_Export_")) or []
    recipe_name_to_alch_edid = {}
    for r in cobj_rows:
        gnam = (r.get("GNAM_FULL") or "").strip()          # "Recipe: Fish and Tatos"
        out_edid = (r.get("CNAM_EDID") or "").strip()
        if gnam and out_edid and gnam.lower().startswith("recipe:"):
            recipe_name_to_alch_edid.setdefault(gnam.lower(), out_edid)

    def _buff_for_recipe(recipe_full):
        """recipe_full = 'Recipe: Fish and Tatos' -> buff string or None."""
        base = re.sub(r"^recipe:\s*", "", (recipe_full or "").strip(), flags=re.I)
        # Prefer the COBJ link (BOOK -> produced ALCH); fall back to name match.
        alch_edid = recipe_name_to_alch_edid.get((recipe_full or "").lower())
        if not alch_edid:
            alch_edid = by_name_edid.get(base.lower())
        if not alch_edid:
            return None
        effs = eff_map.get(alch_edid, [])
        if not effs:
            return None
        buff = ", ".join(effs)
        diet = by_edid_diet.get(alch_edid)
        if diet:
            buff += " · " + diet
        return buff

    return _buff_for_recipe


def build_main_pools(data, root=None):
    """Decompose Fishing_BigFish_LL_Quest_Rewards into grouped pools."""
    recipe_buff = _build_recipe_buff_map(root) if root else (lambda _n: None)
    pools = []
    entries = data.lvli.entries_by_list.get(MAIN_POOL_LVLI, [])
    entries = sorted(entries, key=lambda e: _to_float(e.get("EntryIndex"), 0))
    for e in entries:
        ref = e.get("LVLO_Reference", "")
        fid, edid, sig = _ref_parts(ref)
        cn = _to_float(e.get("LVOV_ChanceNoneValue"), 0.0)
        gate = 1.0 - cn / 100.0                      # this group's pool chance
        if sig == "LVLI":
            meta = POOL_META.get(edid)
            if not meta:
                continue
            resolved = data.resolver.resolve_deep(fid)
            # The contextual aid/ammo pool resolves to ~1700 scaled leaf entries
            # (every aid item / ammo type / level tier). Enumerating it is noise and
            # bloats the feed, so summarise by category instead of listing items.
            if meta["key"] == "contextualAid":
                # The raw list is ~1700 level-scaled leaf entries. Collapse to the
                # distinct aid (ALCH) and ammo (AMMO) items so each can be listed.
                aid_names, ammo_names = set(), set()
                for it in resolved:
                    s = str(it.get("sig", "")).upper()
                    nm = str(it.get("name", "")).strip()
                    if not nm:
                        continue
                    if s == "ALCH":
                        aid_names.add(nm)
                    elif s == "AMMO":
                        ammo_names.add(_prettify_ammo(nm))
                aid_items = [{"name": n, "type": "Aid"} for n in sorted(aid_names)]
                ammo_items = [{"name": n, "type": "Ammo"} for n in sorted(ammo_names)]
                pools.append({
                    "key": meta["key"], "title": meta["title"],
                    "lvliFormID": fid, "lvliEdid": edid,
                    "poolChance": _round(gate * 100, 2),
                    "perItem": None,
                    "legends": list(meta["legends"]),
                    "tradeable": meta["tradeable"], "droppable": meta["droppable"],
                    "note": meta.get("note", ""),
                    "summary": {"aidTypes": len(aid_items), "ammoTypes": len(ammo_items),
                                "examples": ["Stimpak", "RadAway", "Rad-X",
                                             "5mm / 10mm / .44 ammo"]},
                    "items": aid_items + ammo_items,
                })
                continue
            items = _items_from_deep(resolved, gate)
            # Recipe plans that teach a cooked dish: resolve the food buff so the page
            # can show what the recipe makes (e.g. "+5% XP for 30 min · Carnivore").
            if meta["key"] == "campRecipePlans":
                for it in items:
                    if str(it.get("name", "")).startswith("Recipe:"):
                        it["buff"] = recipe_buff(it["name"])
            # Per-item "X% each" only makes sense when items share one rate.
            rates = sorted({round(i["dropRate"], 2) for i in items})
            per_item = f"{rates[0]:g}% each" if len(rates) == 1 and len(items) > 1 else None
            pools.append({
                "key": meta["key"], "title": meta["title"],
                "lvliFormID": fid, "lvliEdid": edid,
                "poolChance": _round(gate * 100, 2),
                "perItem": per_item,
                "legends": list(meta["legends"]),
                "tradeable": meta["tradeable"], "droppable": meta["droppable"],
                "note": meta.get("note", ""),
                "relatedGuide": meta.get("relatedGuide"),
                "items": items,
            })
        elif sig == "BOOK":
            meta = POOL_META["_curved"]
            pools.append({
                "key": meta["key"], "title": meta["title"],
                "bookFormID": fid, "bookEdid": edid,
                "poolChance": _round(gate * 100, 2),
                "perItem": None,
                "legends": list(meta["legends"]),
                "tradeable": meta["tradeable"], "droppable": meta["droppable"],
                "note": meta.get("note", ""),
                "items": [{
                    "formId": fid,
                    "name": "Plan: Curved Fish Display",
                    "qty": 1, "dropRate": _round(gate * 100, 2),
                    "isPlan": True, "sig": "BOOK",
                }],
            })
    return pools


def _region_mods_list(items):
    """rng76 resolve_deep items -> sorted, de-duplicated mod list.

    Each region pool fires 100% of the time when Captain Raymond sends you there,
    so the gate is 1.0 and the absolute percent is round(within*100, 4) — the same
    convention as _items_from_deep(gate=1.0). Names that appear more than once have
    their rates summed, so a name at 0.1% twice reads 0.2%. Sorted case-insensitively.
    """
    agg = {}
    for it in items:
        nm = str(it.get("name", "")).strip()
        if not nm:
            continue
        within = float(it.get("dropRate", 0.0))      # 0..1 within the list
        agg[nm] = agg.get(nm, 0.0) + within * 100.0
    return [{"name": nm, "dropRate": _round(agg[nm], 4)}
            for nm in sorted(agg, key=str.lower)]


def build_regional_pool(data, reward_rows):
    """The single 'Regional Weapon Mod' pool with one sub-entry per region."""
    meta = POOL_META["_regionalMod"]
    regions = []
    host = None   # pool whose reward is gated by >1 stage (serves a shared region)
    for r in reward_rows:
        fid, edid, sig = _ref_parts(pick(r, "RewardedItem"))
        if sig != "LVLI" or edid not in REGION_MOD_BY_EDID:
            continue
        mods = data.resolver.resolve_deep(fid)
        mods_list = _region_mods_list(mods)
        entry = {
            "region": REGION_MOD_BY_EDID[edid],
            "lvliFormID": fid, "lvliEdid": edid,
            "possibleMods": len(mods_list),   # distinct mods in the region's pool
            "mods": mods_list,                # full de-duplicated mod list
        }
        regions.append(entry)
        # A reward gated by more than one GetStageDoneCurrentInstance stage is shared
        # by more than one region — e.g. Cranberry Bog's pool also covers Skyline
        # Valley (the stage text names the region, but the TSV drops the stage index,
        # so we detect the shared pool by its extra condition).
        if str(pick(r, "Conditions") or "").count("GetStageDoneCurrentInstance") > 1:
            host = entry
    # Skyline Valley has no pool of its own — it reuses the multi-stage pool.
    if host:
        regions.append({
            "region": "Skyline Valley",
            "lvliFormID": host["lvliFormID"], "lvliEdid": host["lvliEdid"],
            "possibleMods": host["possibleMods"], "sharesWith": host["region"],
            "mods": list(host["mods"]),   # reuse the host region's mod list (copy)
        })
    # Burning Springs is a fishing region but has no dedicated weapon-mod pool at all.
    regions.append({"region": "Burning Springs", "possibleMods": None, "noPool": True})
    # Stable display order roughly west-to-east / by progression.
    order = ["The Forest", "Toxic Valley", "Savage Divide", "Ash Heap",
             "The Mire", "Cranberry Bog", "Skyline Valley", "Burning Springs"]
    regions.sort(key=lambda x: order.index(x["region"]) if x["region"] in order else 99)
    if not regions:
        return None
    return {
        "key": meta["key"], "title": meta["title"],
        "poolChance": 100.0,
        "perItem": "1 random mod",
        "legends": list(meta["legends"]),
        "tradeable": meta["tradeable"], "droppable": meta["droppable"],
        "note": meta.get("note", ""),
        "regions": regions,
    }


def main():
    root = tsv_dir()
    try:
        gmrw_file = newest(os.path.join(root, "GMRW_Export_*.tsv"))
    except (FileNotFoundError, ValueError):
        print(f"[big-fish] No GMRW export found in {root}", file=sys.stderr)
        sys.exit(1)

    data = Rng76Data.from_tsv_root(root)
    quest = load_quest(root)
    reward_rows = load_reward_rows(root)
    if not reward_rows:
        print(f"[big-fish] No '{REWARD_GMRW_EDID}' rows in {os.path.basename(gmrw_file)}",
              file=sys.stderr)
        sys.exit(1)

    # Assemble pools: regional mod first (always-on headline), then the main pool
    # groups in reader-friendly order.
    main_pools = build_main_pools(data, root)
    regional = build_regional_pool(data, reward_rows)

    order = ["bait", "curvedDisplay", "regionalWeaponMod", "hookMods",
             "campRecipePlans", "contextualAid"]
    pools = list(main_pools)
    if regional:
        pools.append(regional)
    pools.sort(key=lambda p: order.index(p["key"]) if p["key"] in order else 99)

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": PTS,
        "source": {"gmrwTsv": os.path.basename(gmrw_file), "tsvDir": os.path.basename(root.rstrip("/")) or "tsv"},
        "quest": {
            "formId": quest["formId"], "edid": quest["edid"],
            "name": "Big Fish in a Small Pond",
            "gameName": quest["gameName"],
            "description": quest["description"],
            "fishRequired": 7,
            "npc": "Captain Raymond",
            "location": "Fisherman's Rest",
            "rewardGmrwEdid": REWARD_GMRW_EDID,
        },
        "baseRewards": base_rewards(data, reward_rows),
        "pools": pools,
        "blurbLegend": BLURB_LEGEND,
        "relatedGuides": RELATED_GUIDES,
        "additionalRewards": {
            "note": "Big Fish in a Small Pond also unlocks lifetime-challenge "
                    "rewards (plushies and more). These are tracked on the "
                    "all-rewards checklist rather than listed here.",
            "url": ALL_REWARDS_CHECKLIST,
            "label": "Fishing — All Rewards Checklist",
        },
    }

    op = out_path()
    os.makedirs(os.path.dirname(op), exist_ok=True)
    with open(op, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")

    npools = len(pools)
    nitems = sum(len(p.get("items", [])) for p in pools)
    print(f"[big-fish] OK -- {npools} pools, {nitems} items, "
          f"{len(output['baseRewards'])} base rewards "
          f"-> {os.path.relpath(op, os.path.join(SCRIPT_DIR, '..'))}")


if __name__ == "__main__":
    main()
