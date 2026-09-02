#!/usr/bin/env python3
"""
build_buffs_json.py — the BnB Buffs category builder.

Replaces the Feb-2026 gap-filler scaffold. Produces one JSON per buff page,
all consumed by df-bnb-buffs.js.

PAGES AND THEIR SHAPES
──────────────────────
Three render modes, because the three families of page answer different
questions:

  mode "effect-groups"   root expand per EFFECT, item rows inside
      /bnb/buffs/alcohol/alcohol-buffs/
      /bnb/buffs/chems/chem-buffs/
      /bnb/buffs/food/food-buffs/
      /bnb/buffs/hybrid-food/hybrid-food-buffs/
    An item appears in EVERY group it has an effect for — Nukashine is in
    Strength AND Intelligence — because the reader arrives asking "what do I
    drink for Strength?", not "what does Nukashine do?".
    Group order: SPECIAL spelled S-P-E-C-I-A-L, then everything else A-Z.
    Sub-expands: Item Image, Output & Effects, How to Obtain, Technical.

  mode "items-abc"       root expand per ITEM, A-Z
      /bnb/buffs/bobbleheads/bobblehead-buffs/
      /bnb/buffs/magazines/magazine-buffs/
      /bnb/buffs/mutations/mutation-buffs/
      /bnb/buffs/nuka-cola-products/nuka-cola-product-buffs/
    These sets are small and every reader already knows the item name they
    want ("Strength Bobblehead", "Carnivore Mutation"), so grouping by effect
    would add a click for nothing.
    Sub-expands: Item Image, How to Obtain, Output & Effects, Technical.

  mode "effect-rows"     root expand per EFFECT, the effect IS the row
      /bnb/buffs/scout-banners/scout-banner-buffs/
    There is exactly ONE Scout's Banner (ALCH 00653FCD). What varies is the
    Scout's Code it fires — Survival / Teamwork / Research / Discovery /
    Innovation, plus Scout's Courage and the First Aid HoT. So the named
    effect is the row, and each row explains that one code.

WHAT IS NOT ON A BUFF PAGE
──────────────────────────
No checkboxes, no progress bar. These are reference pages, not checklists —
you don't "collect" Strength. (df-bnb-buffs.js therefore never touches the
checklist REST endpoint.)

Survival plumbing (hunger, thirst, rads, disease chance, addiction chance)
never creates a root expand — a group holding ~every item on the page is
noise, not a filter. It still renders inside Output & Effects, so nothing is
hidden from the reader. See buffs_effects._NON_GROUPING.

Items with no groupable effect are NOT dropped. They collect in a final
"No Buff Effects" group at the very bottom, after the A-Z tail, so the page
can never silently lose an item to a normalisation miss.

DIET TAGS
─────────
Carnivore / Herbivore / Both / None, from the item's IngredientType*
keywords. Same rule as build_bnb_menu_sync.detect_mutation — imported, not
re-implemented, so the menu and the buff pages can never disagree.

DATA SOURCES (all committed exports / already-built JSON)
  tsv/ALCH_Export_*.tsv + *_Effects.tsv   items, keywords, effect rows
  tsv/MGEF_Export_*.tsv                   effect FULL + DNAM description
  tsv/GLOB_Export_*.tsv                   magnitude / duration globals
  tsv/SPEL_Export_*_{HEADER,EFFECTS}.tsv  mutations + scout banner codes
  tsv/BOOK_Export_*.tsv                   "Recipe: X" / "Plan: X" records
  dist/recipe_guide.json                  crafting recipes + spoil chains
  dist/cobj-recipes.json                  bench keywords, component lists
  dist/bnb-item-categories.json           (rules imported, not the output)
  dist/*_spawns_manifest.json             location-guide URLs to link

OUTPUT
  dist/buffs/<page>.json      one per page
  dist/buffs.json             manifest (page list + counts + generated)
  dist/patchlog_latest_bnb_buffs.json

PIPELINE
  Runs AFTER build_bnb_item_categories_json.py, build_cobj_recipes_json.py
  and build_recipe_guide_json.py (it reads their dist output). The PTS build
  normalises tsv/pts into tsv/ and relocates dist/ -> dist/pts afterwards, so
  the default --data-dir tsv / --outdir dist is correct for BOTH channels and
  no --pts flag is needed in CI.

Usage:
  python src/build_buffs_json.py
  python src/build_buffs_json.py --page alcohol
  python src/build_buffs_json.py --data-dir tsv --outdir dist
"""

from __future__ import annotations

import argparse
import csv
import datetime
import glob as _glob
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

from buffs_effects import (                                  # noqa: E402
    SPECIAL_LABELS,
    display_name,
    format_duration,
    format_magnitude,
    group_for,
    group_label,
    is_generic_full,
    is_hidden,
    polarity_of,
    sort_groups,
    substitute_mag,
)

# Category rules + ALCH loading come from the item-categories builder so the
# two can never drift. If that module ever moves, fail loudly rather than
# silently re-deriving a second (wrong) definition of "what is a chem".
from build_bnb_item_categories_json import (                 # noqa: E402
    CATEGORIES,
    edid_is_cut,
    find_latest_tsv,
    load_alch_records,
    load_kywd_refs,
    norm_fid,
    resolve_category,
)

# Diet tags — one definition, shared with the BnB menu.
try:
    from build_bnb_menu_sync import detect_mutation           # noqa: E402
except Exception:                                             # pragma: no cover
    HERBIVORE_KEYWORDS = {"IngredientTypeFruit", "IngredientTypeVegetable",
                          "IngredientTypeHerb", "FoodTypeVegHam"}
    CARNIVORE_KEYWORDS = {"IngredientTypeMeat", "IngredientTypeEgg",
                          "FoodTypeChickenMeat", "MealTypeSteak"}

    def detect_mutation(keywords: Set[str]) -> str:
        h = bool(keywords & HERBIVORE_KEYWORDS)
        c = bool(keywords & CARNIVORE_KEYWORDS)
        return "Both" if (h and c) else "Herbivore" if h else "Carnivore" if c else ""

try:
    from patchlog_utils import write_empty_patchlog_feed      # noqa: E402
except Exception:                                             # pragma: no cover
    def write_empty_patchlog_feed(outdir, name, current_count=0):
        path = os.path.join(outdir, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"items": [], "count": current_count}, f, indent=2)


# ---------------------------------------------------------------------------
# Page definitions
# ---------------------------------------------------------------------------

IMAGE_BASE = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/buffs/"

# Location guides to cross-link from How to Obtain. Filled in from the spawn
# manifests at build time where one exists; these are the fallbacks.
GUIDE_LINKS: Dict[str, Dict[str, str]] = {
    "bobbleheads": {"label": "Bobbleheads Location Guide",
                    "url": "/df/farming/consumables/bobbleheads/location-guide/"},
    "magazines":   {"label": "Magazines Location Guide",
                    "url": "/df/farming/consumables/magazines/location-guide/"},
    "scout-banners": {"label": "Scout's Banner Location Guide",
                      "url": "/df/farming/consumables/scouts-banner/location-guide/"},
}

PAGES: Dict[str, Dict[str, Any]] = {
    "alcohol": {
        "mode": "effect-groups",
        "source": "alch",
        "categories": ["alcohol"],
        "title": "Alcohol Buffs",
        "noun": "alcoholic drinks",
        "url": "/bnb/buffs/alcohol/alcohol-buffs/",
        "blurb": "Every alcoholic drink in Fallout 76 that grants a buff, grouped by the "
                 "effect it gives. SPECIAL first, then everything else A-Z. A drink with "
                 "more than one effect appears under each of them.",
    },
    "chems": {
        "mode": "effect-groups",
        "source": "alch",
        "categories": ["chems", "serums"],
        "title": "Chem Buffs",
        "noun": "chems",
        "url": "/bnb/buffs/chems/chem-buffs/",
        "blurb": "Every chem and serum that grants a buff, grouped by the effect it gives. "
                 "SPECIAL first, then everything else A-Z. A chem with more than one effect "
                 "appears under each of them.",
    },
    "food": {
        "mode": "effect-groups",
        "source": "alch",
        "categories": ["food", "canned", "prewar_candy"],
        "title": "Food Buffs",
        "noun": "food and drink items",
        "url": "/bnb/buffs/food/food-buffs/",
        "blurb": "Every food, canned meal, tea, juice and sweet that grants a buff, grouped "
                 "by the effect it gives. SPECIAL first, then everything else A-Z. A dish "
                 "with more than one effect appears under each of them.",
    },
    # NOT built by default — see SKIP_BY_DEFAULT. "Hybrid" currently resolves
    # to the single dish carrying BOTH meat and plant ingredient keywords, and
    # a one-row page reads as broken. Kept wired so it can be switched on the
    # moment we settle what /bnb/buffs/hybrid-food/ is meant to list.
    "hybrid-food": {
        "mode": "effect-groups",
        "source": "alch",
        "categories": ["food", "canned", "prewar_candy"],
        "diet_filter": "Both",
        "title": "Hybrid Food Buffs",
        "noun": "hybrid dishes",
        "url": "/bnb/buffs/hybrid-food/hybrid-food-buffs/",
        "blurb": "Dishes that carry BOTH meat and plant ingredient keywords, so Carnivore "
                 "and Herbivore both double their benefit. Grouped by effect, SPECIAL first.",
    },
    "nuka-cola": {
        "mode": "items-abc",
        "source": "alch",
        "categories": ["nuka_cola"],
        "title": "Nuka Cola Product Buffs",
        "noun": "Nuka-Cola products",
        "url": "/bnb/buffs/nuka-cola-products/nuka-cola-product-buffs/",
        "blurb": "Every Nuka-Cola product and what it does, A-Z.",
    },
    "magazines": {
        "mode": "items-abc",
        "source": "alch",
        "keyword_edid": "MagazineKeyword",
        "title": "Magazine Buffs",
        "noun": "magazines",
        "url": "/bnb/buffs/magazines/magazine-buffs/",
        "guide": "magazines",
        "blurb": "Every magazine issue and the buff it grants, A-Z.",
    },
    "bobbleheads": {
        "mode": "items-abc",
        "source": "alch",
        "keyword_edid": "BobbleheadKeyword",
        "title": "Bobblehead Buffs",
        "noun": "bobbleheads",
        "url": "/bnb/buffs/bobbleheads/bobblehead-buffs/",
        "guide": "bobbleheads",
        "blurb": "Every bobblehead and the buff it grants, A-Z.",
    },
    "mutations": {
        "mode": "items-abc",
        "source": "spel",
        "edid_prefix": "Mutation_",
        "title": "Mutation Buffs",
        "noun": "mutations",
        "url": "/bnb/buffs/mutations/mutation-buffs/",
        "blurb": "Every mutation, its positive and negative effects, and the serum that "
                 "grants it. A-Z.",
    },
    "scout-banners": {
        "mode": "effect-rows",
        "source": "spel",
        "edid_prefix": "SCORE_Banner",
        "title": "Scout Banner Buffs",
        "noun": "Scout's Codes",
        "url": "/bnb/buffs/scout-banners/scout-banner-buffs/",
        "guide": "scout-banners",
        "banner_form_id": "00653FCD",
        "blurb": "Scout's Banner fires one of its Scout's Codes when you or a teammate get "
                 "a kill. One expand per code.",
    },
}

SKIP_BY_DEFAULT = {"hybrid-food"}

# A handful of mutation effects have neither a usable FULL (it just repeats the
# mutation's name) nor a DNAM, so the EDID is all there is and it prettifies
# into something like "Twisted Muscle Limb Damage". Override table rather than
# a regex: there are four of them and they are not a pattern.
MUTATION_EFFECT_NAMES: Dict[str, str] = {
    "Mutation_TwistedMusclesEffect":    "Melee Damage",
    "Mutation_TwistedMuscleLimbDamage": "Limb Damage",
    "Mutation_FortifyJumpHeight":       "Jump Height",
    "Mutation_EggHeadEffect":           "Intelligence",
}

# ---------------------------------------------------------------------------
# Scout's Banner — the eight named effects
# ---------------------------------------------------------------------------
# The banner's SPEL tree is mostly plumbing: proc handlers, fireworks VFX, a
# client-side Detect Life mirror, a revive cooldown. Only eight of those rows
# are things a player has a name for, and five of them share the same engine
# FULL ("¢Scout's Code"), so the page cannot be derived by walking the tree —
# it has to be declared. Each entry is:
#
#   marker  the MGEF that IS the effect (what the banner grants)
#   proc    the MGEF that DOES the work (what actually happens on a kill)
#   name    the in-game name, from the banner's own DESC line:
#           "Survival. Teamwork. Research. Discovery. Innovation."
#
# If Bethesda adds a sixth Scout's Code this table is the one place to touch,
# and the build prints a warning for any SCORE_Banner marker it doesn't know.
SCOUT_CODES: List[Dict[str, str]] = [
    {"name": "Scout's Code: Survival",
     "marker": "SCORE_Banner_Medic",
     "proc":   "SCORE_Banner_Medic_RestoreHealth_Effect",
     "summary": "Heals 20% of your maximum Health."},
    {"name": "Scout's Code: Teamwork",
     "marker": "SCORE_Banner_Driven_Effect",
     "proc":   "SCORE_Banner_Driven_RestoreAP_Effect",
     "summary": "Restores 100% of your Action Points."},
    {"name": "Scout's Code: Research",
     "marker": "SCORE_Banner_Hardened_Effect",
     "proc":   "SCORE_Banner_Hardened_RepairArmor_Effect",
     "summary": "Repairs 10% condition on all equipped armour."},
    {"name": "Scout's Code: Discovery",
     "marker": "SCORE_Banner_Vision",
     "proc":   "",
     "summary": "Highlights nearby enemies through walls."},
    {"name": "Scout's Code: Innovation",
     "marker": "SCORE_Banner_Forged_Effect",
     "proc":   "SCORE_Banner_Forged_RepairWeapon_Effect",
     "summary": "Repairs 10% condition on your equipped weapon."},
    {"name": "Scout's Courage",
     "marker": "SCORE_Banner_AutoRevive_ApplyPerk_Effect",
     "proc":   "",
     "summary": "Automatically revives you if you go down, once every 5 minutes."},
    {"name": "Battle Charged",
     "marker": "SCORE_Banner_BattleCharged_Buff_Effect",
     "proc":   "SCORE_Banner_BattleCharged_OnKill_Effect",
     "summary": "Fortifies all damage you deal for a short burst after a kill."},
    {"name": "First Aid",
     "marker": "SCORE_Banner_HumanHeal_Effect",
     "proc":   "",
     "summary": "A healing-over-time trickle for everyone standing near the banner. "
                "The rate steps up with the banner's rank."},
]

# Every other MGEF on a SCORE_Banner spell. Listed so the build can tell
# "plumbing we know about" from "something new Bethesda added".
SCOUT_PLUMBING = {
    "SCORE_Banner_RewardEffect", "SCORE_Banner_RewardEffect_Human",
    "SCORE_Banner_ProcHandler_Effect", "SCORE_Banner_ProcChance_Effect",
    "SCORE_Banner_OnKill_Effect", "SCORE_Banner_Fireworks_OnKill_Effect",
    "SCORE_Banner_Vision_HighlightEnemies_Effect",
    "SCORE_Banner_Vision_HighlightEnemies_CloakEffect",
    "SCORE_Banner_Medic_RestoreHealth_Effect",
    "SCORE_Banner_Driven_RestoreAP_Effect",
    "SCORE_Banner_Hardened_RepairArmor_Effect",
    "SCORE_Banner_Forged_RepairWeapon_Effect",
    "SCORE_Banner_BattleCharged_OnKill_Effect",
    "SCORE_Banner_AutoRevive_FanFare",
    "DetectLifeOnTargetEffect", "GenericOnCooldownEffect",
}

# Bobblehead FULLs read "Bobblehead: Strength" / "Glowing Bobblehead: Strength",
# which sorts every one of them under B. The page is A-Z by what the reader is
# actually looking for — the stat — so flip it to "Strength Bobblehead".
_BOBBLE_RE = re.compile(r"^(Glowing\s+)?Bobblehead:\s*(.+)$", re.I)


def page_display_name(page_key: str, name: str) -> str:
    if page_key == "bobbleheads":
        m = _BOBBLE_RE.match(name)
        if m:
            return f"{m.group(2).strip()} Bobblehead" + (" (Glowing)" if m.group(1) else "")
    return name


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def today_ymd() -> str:
    return datetime.date.today().isoformat()


def clean(s: Any) -> str:
    return str(s if s is not None else "").strip()


def safe_num(s: Any) -> Optional[float]:
    try:
        return float(str(s).strip())
    except (TypeError, ValueError):
        return None


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def read_tsv_cols(path: str, want: Sequence[str]) -> Iterable[Dict[str, str]]:
    """Stream a TSV yielding only the requested columns.

    csv.DictReader builds a dict of every column per row; the MGEF export is
    562 columns wide and the GLOB export 5,582, which is enough to OOM a
    sandbox build (see HANDOFF: "Wide TSV exports OOM the build"). Selecting
    the indices up front keeps memory flat.
    """
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        rdr = csv.reader(f, delimiter="\t")
        try:
            hdr = next(rdr)
        except StopIteration:
            return
        idx = {w: hdr.index(w) for w in want if w in hdr}
        for row in rdr:
            yield {w: (row[i] if i < len(row) else "") for w, i in idx.items()}


def newest_export(data_dir: str, pattern: str, exclude: Optional[str] = None) -> Optional[str]:
    cands = [p for p in _glob.glob(os.path.join(data_dir, pattern))
             if not (exclude and exclude in os.path.basename(p))]
    if not cands:
        return None
    try:
        import tsv_source
        return max(cands, key=lambda p: (tsv_source.export_date(p), os.path.getmtime(p)))
    except Exception:
        return max(cands, key=os.path.getmtime)


def load_json(path: str) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Source loading
# ---------------------------------------------------------------------------

class Sources:
    """Everything the page builders read, loaded once."""

    def __init__(self, data_dir: str, dist_dir: str):
        self.data_dir = data_dir
        self.dist_dir = dist_dir
        self.warnings: List[str] = []

        self.alch_path = newest_export(data_dir, "ALCH_Export_*.tsv", exclude="_Effects")
        self.alch_eff_path = newest_export(data_dir, "ALCH_Export_*_Effects.tsv")
        self.mgef_path = newest_export(data_dir, "MGEF_Export_*.tsv")
        self.glob_path = newest_export(data_dir, "GLOB_Export_*.tsv")
        self.book_path = newest_export(data_dir, "BOOK_Export_*.tsv", exclude="_Locations")
        self.spel_hdr_path = newest_export(data_dir, "SPEL_Export_*_HEADER.tsv")
        self.spel_eff_path = newest_export(data_dir, "SPEL_Export_*_EFFECTS.tsv")

        for label, path in (("ALCH", self.alch_path), ("ALCH effects", self.alch_eff_path),
                            ("MGEF", self.mgef_path)):
            if not path:
                raise SystemExit(f"[build_buffs_json] FATAL: no {label} export found in {data_dir}")

        self.alch: Dict[str, Dict[str, Any]] = load_alch_records(self.alch_path)
        self.kywd: Dict[str, Set[str]] = {}
        kywd_path = find_latest_tsv(data_dir, "KYWD_Export_*_Refs.tsv")
        if kywd_path:
            self.kywd = load_kywd_refs(kywd_path)
        else:
            self.warnings.append("no KYWD refs export — category resolution falls back to "
                                 "the Keywords_Flat column only")

        # load_alch_records() gives keywords as FormIDs (that's what the
        # category rules match on). The buff pages need the keyword NAMES too —
        # diet detection, object type and the magazine/bobblehead page filters
        # are all written against EDIDs — plus the ENIT spoil/addiction columns
        # it doesn't carry. One extra pass over the same file.
        self.alch_extra: Dict[str, Dict[str, Any]] = {}
        for r in read_tsv_cols(self.alch_path, [
                "ALCH_FormID", "Keywords_Flat", "ENIT_Addiction_FULL",
                "ENIT_SpoiledItem_FULL", "ENIT_IsCanned", "ENIT_CannedBase_FULL",
                "ENIT_AddictionChance"]):
            fid = norm_fid(r.get("ALCH_FormID", ""))
            if not fid:
                continue
            flat = r.get("Keywords_Flat", "") or ""
            self.alch_extra[fid] = {
                "keyword_edids": set(re.findall(r"([A-Za-z_][\w]*)\s*\[[0-9A-Fa-f]{8}\]", flat)),
                "addiction_name": clean(r.get("ENIT_Addiction_FULL")),
                "addiction_chance": safe_num(r.get("ENIT_AddictionChance")),
                "spoiled_to": clean(r.get("ENIT_SpoiledItem_FULL")),
                "is_canned": clean(r.get("ENIT_IsCanned")).lower() in ("1", "true", "yes"),
                "canned_base": clean(r.get("ENIT_CannedBase_FULL")),
            }
        # Fold the extras onto the records so every downstream reader sees one
        # object rather than having to remember there are two.
        for fid, rec in self.alch.items():
            rec.update(self.alch_extra.get(fid, {"keyword_edids": set()}))

        self.mgef: Dict[str, Dict[str, str]] = {}
        for r in read_tsv_cols(self.mgef_path,
                               ["MGEF_FormID", "EDID", "FULL", "DNAM_MagicItemDescription"]):
            fid = norm_fid(r.get("MGEF_FormID", ""))
            if fid:
                self.mgef[fid] = r

        self.globs: Dict[str, float] = {}
        if self.glob_path:
            for r in read_tsv_cols(self.glob_path, ["EDID", "FLTV"]):
                v = safe_num(r.get("FLTV"))
                e = clean(r.get("EDID"))
                if e and v is not None:
                    self.globs[e] = v

        # ALCH FormID -> [effect rows], in EffectIndex order.
        self.alch_effects: Dict[str, List[Dict[str, str]]] = {}
        for r in read_tsv_cols(self.alch_eff_path, [
                "ALCH_FormID", "EffectIndex", "MGEF_FormID", "MGEF_EDID", "MGEF_FULL",
                "EFIT_Magnitude", "EFIT_Duration",
                "MAGG_GLOB_EDID", "DURG_GLOB_EDID", "CODV_CooldownValue"]):
            fid = norm_fid(r.get("ALCH_FormID", ""))
            if fid:
                self.alch_effects.setdefault(fid, []).append(r)
        for rows in self.alch_effects.values():
            rows.sort(key=lambda r: safe_num(r.get("EffectIndex")) or 0)

        # SPEL
        self.spel: Dict[str, Dict[str, str]] = {}
        self.spel_effects: Dict[str, List[Dict[str, str]]] = {}
        if self.spel_hdr_path:
            for r in read_tsv_cols(self.spel_hdr_path,
                                   ["SPEL_FormID", "SPEL_EDID", "SPEL_FULL", "SPEL_DESC"]):
                fid = norm_fid(r.get("SPEL_FormID", ""))
                if fid:
                    self.spel[fid] = r
        if self.spel_eff_path:
            for r in read_tsv_cols(self.spel_eff_path, [
                    "SPEL_FormID", "EffectIndex", "EFID_MGEF_FormID", "EFID_MGEF_EDID",
                    "EFID_MGEF_FULL", "EFIT_Magnitude", "EFIT_Duration"]):
                fid = norm_fid(r.get("SPEL_FormID", ""))
                if fid:
                    self.spel_effects.setdefault(fid, []).append(r)
            for rows in self.spel_effects.values():
                rows.sort(key=lambda r: safe_num(r.get("EffectIndex")) or 0)

        # BOOK: plan/recipe records, indexed by the item name they teach.
        self.plans_by_output: Dict[str, Dict[str, str]] = {}
        if self.book_path:
            for r in read_tsv_cols(self.book_path, ["FormID", "EDID", "FULL", "DESC"]):
                full = clean(r.get("FULL"))
                m = re.match(r"^(?:Recipe|Plan):\s*(.+)$", full, re.I)
                if m:
                    self.plans_by_output.setdefault(m.group(1).strip().lower(), r)

        # Already-built JSON
        self.recipe_guide = load_json(os.path.join(dist_dir, "recipe_guide.json")) or {}
        self.cobj = load_json(os.path.join(dist_dir, "cobj-recipes.json")) or {}
        self.recipes_by_output: Dict[str, Dict[str, Any]] = {}
        for cat in (self.recipe_guide.get("categories") or {}).values():
            for item in (cat.get("items") or []):
                out = (item.get("output") or {})
                fid = norm_fid(out.get("formId", ""))
                if fid:
                    self.recipes_by_output.setdefault(fid, item)
                nm = clean(out.get("name")).lower()
                if nm:
                    self.recipes_by_output.setdefault(nm, item)

        # Location guides from the spawn manifests (authoritative URL bases).
        for key in list(GUIDE_LINKS):
            man = load_json(os.path.join(dist_dir, f"{key.rstrip('s')}_spawns_manifest.json")) \
                or load_json(os.path.join(dist_dir, f"{key}_spawns_manifest.json"))
            if isinstance(man, dict) and man.get("url_base"):
                GUIDE_LINKS[key] = {
                    "label": clean(man.get("page_title")) or GUIDE_LINKS[key]["label"],
                    "url": man["url_base"],
                }

    # -- effect assembly ---------------------------------------------------

    def mgef_row(self, form_id: str) -> Dict[str, str]:
        return self.mgef.get(norm_fid(form_id), {})

    def resolve_mag(self, magnitude: Optional[float], mag_glob: str) -> Optional[float]:
        """The effect row usually carries the resolved magnitude already; the
        GLOB table is the fallback AND the authority when the row reads 0."""
        if magnitude:
            return magnitude
        if mag_glob and mag_glob in self.globs:
            return self.globs[mag_glob]
        return magnitude

    def build_effect(self, *, mgef_form_id: str, mgef_edid: str, mgef_full: str,
                     magnitude: Any, duration: Any,
                     mag_glob: str = "", dur_glob: str = "") -> Dict[str, Any]:
        m = self.mgef_row(mgef_form_id)
        dnam = clean(m.get("DNAM_MagicItemDescription"))
        mag = self.resolve_mag(safe_num(magnitude), clean(mag_glob))
        dur = safe_num(duration)
        if not dur and dur_glob and dur_glob in self.globs:
            dur = self.globs[dur_glob]

        eff = {
            "formId": norm_fid(mgef_form_id),
            "edid": clean(mgef_edid) or clean(m.get("EDID")),
            "full": clean(mgef_full) or clean(m.get("FULL")),
            "dnam": dnam,
            "magnitude": mag,
            "duration": dur,
            "magGlob": clean(mag_glob) or None,
            "durGlob": clean(dur_glob) or None,
        }
        eff["name"] = display_name(eff)
        eff["polarity"] = polarity_of(eff)
        eff["durationDisplay"] = format_duration(dur)
        eff["display"] = effect_display_line(eff)
        return eff


# ---------------------------------------------------------------------------
# Effect display line
# ---------------------------------------------------------------------------

def _fmt_surv(seconds: Optional[float]) -> Optional[str]:
    """A hunger/thirst magnitude is BOTH an amount and a duration — the raw
    number the game stores AND how long it keeps the meter filled. Show both,
    the way the farming guides do: "1080/18 min"."""
    if not seconds or seconds <= 0:
        return None
    m = seconds / 60.0
    mins = f"{round(m, 1):g} min" if m < 2 else f"{int(round(m))} min"
    return f"{round(seconds, 2):g}/{mins}"


def effect_display_line(eff: Dict[str, Any]) -> str:
    """One player-facing line for an effect. The DURATION is rendered
    separately by the front-end, so it is not repeated here — except for a
    hunger/thirst value, where the number IS the magnitude."""
    name = clean(eff.get("name"))
    low = name.lower()
    mag = eff.get("magnitude")
    dur = eff.get("duration")
    dnam = clean(eff.get("dnam"))
    mi = int(round(mag)) if isinstance(mag, (int, float)) else None

    if low in ("rads", "rad") or low.startswith("rad "):
        return f"+{mi} Rads" if mi else "Rads"
    if "disease chance" in low:
        return f"{mi}% chance to catch a disease" if mi is not None else "Chance to catch a disease"
    if low.startswith("quench") or ("thirst" in low and "increase" not in low):
        v = _fmt_surv(mag)
        return f"Quenches thirst ({v})" if v else "Quenches thirst"
    if "satisfy hunger" in low or ("hunger" in low and "increase" not in low and "slow" not in low):
        v = _fmt_surv(mag)
        return f"Satisfies hunger ({v})" if v else "Satisfies hunger"
    if low.startswith("increase hunger"):
        v = _fmt_surv(mag)
        return f"Increases hunger ({v})" if v else "Increases hunger"
    if low.startswith("increase thirst"):
        v = _fmt_surv(mag)
        return f"Increases thirst ({v})" if v else "Increases thirst"
    if "restore health" in low or low == "health":
        # Food heals per SECOND over its duration — show the TOTAL restored.
        total = format_magnitude(mag * dur) if (mag and dur) else format_magnitude(mag)
        return f"Restores {total} Health" if total else "Restores Health"
    if "chance addiction" in low:
        pct = format_magnitude((mag or 0) * 100) if (mag or 0) <= 1 else format_magnitude(mag)
        return f"{pct}% chance of addiction" if pct else "Chance of addiction"

    # A DNAM template ("Rads <mag> /s", "<+MAG>% Ballistic Weapon Condition
    # Cost") is the game's own wording — prefer it once the value is filled in.
    if dnam and re.search(r"<\s*[+-]?\s*\w+\s*>", dnam):
        filled = substitute_mag(dnam, mag, dur)
        # A leftover token means the template needs something we can't resolve
        # — <ITEM1.ABBR> is the game's runtime stat abbreviation, filled in by
        # the HUD. Fall through to the EDID-derived name rather than shipping
        # raw markup onto the page.
        if filled and "<" not in filled:
            # A bare "<mag>" carries no sign, so a penalty renders as "2
            # Agility" — the game supplies the minus at runtime from the
            # effect's own archetype. Put it back.
            if eff.get("polarity") == "debuff" and re.match(r"^\d", filled):
                filled = "-" + filled
            return filled

    # A token-free DNAM that already reads as a finished sentence ("Breathe
    # Underwater.") or already states its own number ("15% Reduced damage from
    # Scorched") is better than "+4 Water Breathing" — on a flag-style effect
    # the stored magnitude is an internal value, not something to show a player.
    if dnam and "<" not in dnam and (dnam.rstrip().endswith(".") or re.search(r"\d", dnam)):
        return dnam.strip()

    # "+30 Fortify Carry Weight" -> "+30 Carry Weight". The verb is implied by
    # the sign; keeping both makes every line say the same thing twice.
    bare = re.sub(r"^(Fortify|Restore|Increase|Reduce)\s+", "", name, flags=re.I)

    # The name may already carry its own number ("+25 Bleed DMG over 5 sec",
    # lifted from a DNAM) — prefixing another magnitude would read "+25 +25".
    if re.match(r"^[+-]?\d", bare):
        return bare

    if mag is not None and 0 < abs(mag) < 1:
        # A sub-1 magnitude is a MULTIPLIER (Bird Bones stores fall damage as
        # 0.9), not a quantity. Show the factor rather than rounding it to 1.
        return f"{bare} \u00d7{format_magnitude(mag)}"

    if mi:
        # The engine stores a penalty as a positive number and marks it with a
        # Reduce verb, so the sign has to come from the polarity.
        sign = "-" if (mi < 0 or (eff.get("polarity") == "debuff" and mi > 0)) else "+"
        return f"{sign}{abs(mi)} {bare}"
    return name


def sort_effects(effects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Timed buffs first, instant effects next, rads/addiction plumbing last."""
    def key(e: Dict[str, Any]) -> Tuple[int, str]:
        n = clean(e.get("name")).lower()
        if "rad" in n or "addiction" in n or "disease chance" in n:
            band = 3
        elif "hunger" in n or "thirst" in n:
            band = 2
        elif e.get("duration"):
            band = 0
        else:
            band = 1
        return (band, n)
    return sorted(effects, key=key)


# ---------------------------------------------------------------------------
# How to Obtain
# ---------------------------------------------------------------------------
# Two routes on every consumable buff page, in this order:
#
#   1. Recipe / Plan   how you CRAFT it. "Recipe: Not in Game" when the item
#                      has no COBJ recipe at all — an explicit statement that
#                      we checked, not a gap.
#   2. Fresh / Raw     how you get the UNCRAFTED one. Fermentables spoil into
#                      their fresh form, raw meat has no recipe, packaged food
#                      is loot only.
#
# Everything here is derived. When a fact isn't in the exports the route says
# so plainly rather than guessing a location.

RECIPE_NOT_IN_GAME = "Recipe: Not in Game"

_PLAN_PREFIX_RE = re.compile(r"^(?:Recipe|Plan):\s*", re.I)


def build_obtain_routes(sources: Sources, rec: Dict[str, Any],
                        page_key: str) -> List[Dict[str, Any]]:
    fid = rec["form_id"]
    name = clean(rec.get("name"))
    kw = rec.get("keyword_edids") or set()
    routes: List[Dict[str, Any]] = []

    # --- 1. Recipe / Plan -------------------------------------------------
    recipe = sources.recipes_by_output.get(fid) or \
        sources.recipes_by_output.get(name.lower())
    lines: List[str] = []
    if recipe:
        plan = sources.plans_by_output.get(name.lower())
        if plan:
            # Strip the leading "Recipe:"/"Plan:" off the BOOK FULL before
            # adding our own label — otherwise it reads "Plan: Recipe: Blight Soup".
            plan_full = _PLAN_PREFIX_RE.sub("", clean(plan.get("FULL")))
            lines.append(f"Plan: {plan_full}")
        wb = clean(recipe.get("workbench"))
        if wb:
            lines.append(f"Requires: {wb}")
        ings = recipe.get("ingredients") or []
        if ings:
            lines.append("Components: " + " / ".join(
                f"{clean(i.get('name')) or clean(i.get('edid'))} x{i.get('qty', 1)}"
                for i in ings))
        out_qty = (recipe.get("output") or {}).get("qty") or recipe.get("output_qty")
        if out_qty and int(out_qty) > 1:
            lines.append(f"Yield: {out_qty}")
        for h in (recipe.get("howToObtain") or []):
            h = clean(h)
            if not h:
                continue
            # The recipe guide stores these as raw "Quest: <EDID>" strings.
            # Say what it means rather than printing an engine id at the reader.
            qm = re.match(r"^Quest:\s*(\S+)$", h)
            lines.append(f"Note: Unlocked by a quest ({qm.group(1)})." if qm
                         else f"Note: {h}")
        spoils = clean((recipe.get("output") or {}).get("spoils_to"))
        if spoils:
            lines.append(f"Note: Ferments/spoils into {spoils}.")
    else:
        lines.append(RECIPE_NOT_IN_GAME)

    routes.append({
        "route": "Recipe / Plan",
        "populated": bool(recipe),
        "lines": lines,
        "tradeable": None,
        "dropRate": None,
    })

    # --- 2. Fresh / Raw ---------------------------------------------------
    fresh_lines: List[str] = []
    # A fermentable spoils INTO the fresh drink; the fresh drink itself is the
    # looted one. Work out which side of that pair this item is on.
    is_fermentable = "FermenterItem" in kw or name.lower().startswith("fermentable ")
    spoiled_full = clean(rec.get("spoiled_to"))
    if is_fermentable:
        fresh_lines.append("Note: This is the fermentable form — craft it, then let it "
                           "ferment into the drinkable version.")
    if spoiled_full:
        fresh_lines.append(f"Spoils into: {spoiled_full}")

    if "MealTypeRaw" in kw:
        fresh_lines.append("Raw: Harvested from creatures or picked in the world — "
                           "no recipe needed.")
    elif "MealTypePackaged" in kw:
        fresh_lines.append("Fresh: Pre-War packaged food — found as loot, never crafted "
                           "fresh.")
    elif "MealTypeCooked" in kw and not recipe:
        fresh_lines.append("Fresh: Cooked item with no player recipe — found as loot or "
                           "handed out by an event or quest.")

    guide = PAGES.get(page_key, {}).get("guide")
    if guide and guide in GUIDE_LINKS:
        g = GUIDE_LINKS[guide]
        fresh_lines.append(f"Guide: {g['label']} — {g['url']}")

    if not fresh_lines:
        fresh_lines.append("No separate fresh or raw variant — the crafted item is the "
                           "only form.")

    routes.append({
        "route": "Fresh / Raw",
        "populated": True,
        "lines": fresh_lines,
        "tradeable": None,
        "dropRate": None,
    })

    return routes


# ---------------------------------------------------------------------------
# Item assembly
# ---------------------------------------------------------------------------

def image_url(page_key: str, rec: Dict[str, Any]) -> str:
    """Convention: guide-images/buffs/<page>/<slug>.avif.

    The renderer swaps a failed load for the standard dashed placeholder, so a
    page ships correctly before the icons are uploaded and lights up on its own
    once they land — no rebuild needed.
    """
    return f"{IMAGE_BASE}{page_key}/{slugify(clean(rec.get('name')))}.avif"


def build_alch_item(sources: Sources, rec: Dict[str, Any], page_key: str,
                    mode: str) -> Dict[str, Any]:
    fid = rec["form_id"]
    raw_rows = sources.alch_effects.get(fid, [])

    effects: List[Dict[str, Any]] = []
    for r in raw_rows:
        eff = sources.build_effect(
            mgef_form_id=r.get("MGEF_FormID", ""),
            mgef_edid=r.get("MGEF_EDID", ""),
            mgef_full=r.get("MGEF_FULL", ""),
            magnitude=r.get("EFIT_Magnitude"),
            duration=r.get("EFIT_Duration"),
            mag_glob=r.get("MAGG_GLOB_EDID", ""),
            dur_glob=r.get("DURG_GLOB_EDID", ""),
        )
        if is_hidden(eff):
            continue
        g = group_for(eff)
        eff["groupKey"] = g[0] if g else None
        eff["groupLabel"] = g[1] if g else None
        effects.append(eff)
    effects = sort_effects(effects)

    group_keys: List[str] = []
    seen: Set[str] = set()
    for e in effects:
        k = e.get("groupKey")
        if k and k not in seen:
            seen.add(k)
            group_keys.append(k)
    group_keys = sort_groups(group_keys)

    kw = rec.get("keyword_edids") or set()
    diet = detect_mutation(kw) or "None"
    obj_type = ("Drink" if "ObjectTypeDrink" in kw else
                "Food" if "ObjectTypeFood" in kw else
                "Chem" if "ObjectTypeChem" in kw else
                "Serum" if "ObjectTypeSerum" in kw else
                "Magazine" if "MagazineKeyword" in kw else
                "Bobblehead" if "BobbleheadKeyword" in kw else None)

    build_rows: List[str] = []
    if obj_type:
        build_rows.append(f"Type: {obj_type}")
    if diet and diet != "None":
        build_rows.append(f"Mutation Diet: {diet}")
    w = safe_num(rec.get("weight"))
    if w is not None:
        build_rows.append(f"Weight: {format_magnitude(w)}")
    v = safe_num(rec.get("value"))
    if v is not None:
        build_rows.append(f"Value: {int(v)} caps")
    addiction = clean(rec.get("addiction_name"))
    if addiction:
        build_rows.append(f"Addiction: {addiction}")
    if "ObjectTypeCanSpoil" in " ".join(kw):
        build_rows.append("Spoils: Yes")

    id_rows = [f"EDID: {rec.get('edid')}", f"FormID: {fid}"]
    for e in effects:
        id_rows.append(f"MGEF {e['formId']}: {e['edid']}")
    if kw:
        id_rows.append("Keywords: " + ", ".join(sorted(kw)))

    return {
        "formId": fid,
        "edid": clean(rec.get("edid")),
        "name": page_display_name(page_key, clean(rec.get("name"))),
        "gameName": clean(rec.get("name")),
        "description": clean(rec.get("desc")),
        "imageUrl": image_url(page_key, rec),
        "objectType": obj_type,
        "diet": diet,
        "weight": w,
        "value": int(v) if v is not None else None,
        "effects": effects,
        "effectGroups": group_keys,
        "obtainRoutes": build_obtain_routes(sources, rec, page_key),
        "technical": {"build": build_rows, "ids": id_rows},
        "status": "cut" if edid_is_cut(clean(rec.get("edid"))) else "live",
    }


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

NO_BUFF_KEY = "no-buff-effects"
NO_BUFF_LABEL = "No Buff Effects"


def alch_records_for_page(sources: Sources, page_key: str,
                          spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Select the ALCH records that belong on one page."""
    kw_edid = spec.get("keyword_edid")
    if kw_edid:
        out = [rec for rec in sources.alch.values()
               if kw_edid in (rec.get("keyword_edids") or set())
               and clean(rec.get("name"))
               and not edid_is_cut(clean(rec.get("edid")))]
        return out

    picked: Dict[str, Dict[str, Any]] = {}
    for cat in spec.get("categories", []):
        rule = CATEGORIES.get(cat)
        if not rule:
            sources.warnings.append(f"{page_key}: unknown category rule {cat!r}")
            continue
        items, _stats = resolve_category(cat, rule, sources.kywd, sources.alch)
        for it in items:
            fid = norm_fid(it.get("form_id", ""))
            rec = sources.alch.get(fid)
            if rec:
                picked.setdefault(fid, rec)
    return list(picked.values())


def build_effect_group_page(sources: Sources, page_key: str,
                            spec: Dict[str, Any]) -> Dict[str, Any]:
    recs = alch_records_for_page(sources, page_key, spec)
    items = [build_alch_item(sources, r, page_key, spec["mode"]) for r in recs]

    diet_filter = spec.get("diet_filter")
    if diet_filter:
        items = [i for i in items if i["diet"] == diet_filter]

    items.sort(key=lambda i: i["name"].lower())

    counts: Dict[str, int] = {}
    labels: Dict[str, str] = {}
    for it in items:
        for k in it["effectGroups"]:
            counts[k] = counts.get(k, 0) + 1
            labels.setdefault(k, group_label(k))

    groups = [{"key": k, "label": labels[k], "count": counts[k]}
              for k in sort_groups(list(counts))]

    # Nothing is ever dropped for failing to normalise: items with no
    # groupable effect land in a final group after the A-Z tail.
    orphan = [i for i in items if not i["effectGroups"]]
    if orphan:
        for i in orphan:
            i["effectGroups"] = [NO_BUFF_KEY]
        groups.append({"key": NO_BUFF_KEY, "label": NO_BUFF_LABEL, "count": len(orphan)})

    return {
        "mode": "effect-groups",
        "groups": groups,
        "items": items,
    }


def build_items_abc_page(sources: Sources, page_key: str,
                         spec: Dict[str, Any]) -> Dict[str, Any]:
    if spec.get("source") == "spel":
        items = build_mutation_items(sources, page_key)
    else:
        recs = alch_records_for_page(sources, page_key, spec)
        items = [build_alch_item(sources, r, page_key, spec["mode"]) for r in recs]
    items.sort(key=lambda i: i["name"].lower())
    return {"mode": "items-abc", "groups": [], "items": items}


# --- mutations -------------------------------------------------------------

def collapse_mutation_effects(effects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fold a mutation's duplicated effect rows into one row per MGEF.

    Every mutation stores each effect at least twice: the value it gives on
    its own, and the (higher) value it gives while a Mutation Serum is
    active. Left alone that renders as "+4 Agility" followed by
    "+5 Agility", which reads as a data bug. Collapsing on the MGEF EDID and
    keeping min/max turns it into the sentence a player wants:
    "+4 Agility (+5 with a serum)".
    """
    by_edid: Dict[str, List[Dict[str, Any]]] = {}
    order: List[str] = []
    for e in effects:
        k = e["edid"] or e["formId"]
        if k not in by_edid:
            by_edid[k] = []
            order.append(k)
        by_edid[k].append(e)

    out: List[Dict[str, Any]] = []
    for k in order:
        rows = by_edid[k]
        mags = [r["magnitude"] for r in rows if r.get("magnitude") is not None]
        base = min(rows, key=lambda r: r.get("magnitude") if r.get("magnitude") is not None else 0)
        merged = dict(base)
        if mags and min(mags) != max(mags):
            merged["magnitude"] = min(mags)
            merged["serumMagnitude"] = max(mags)
            merged["display"] = effect_display_line(merged)
            hi = dict(merged, magnitude=max(mags))
            merged["display"] += f" ({effect_display_line(hi)} with a serum)"
        else:
            merged["display"] = effect_display_line(merged)
        out.append(merged)
    return out


def build_mutation_items(sources: Sources, page_key: str) -> List[Dict[str, Any]]:
    """One row per mutation.

    A mutation's SPECIAL swing lives only on its UI-dummy effect pair — the
    base value and the "with serum" value — so is_hidden() is called with
    keep_ui_dummy=True here and nowhere else (see buffs_effects.is_hidden).
    """
    out: List[Dict[str, Any]] = []
    for fid, hdr in sources.spel.items():
        edid = clean(hdr.get("SPEL_EDID"))
        if not edid.startswith("Mutation_"):
            continue
        if edid_is_cut(edid) or edid.endswith("-CUT") or "_Babylon" in edid:
            continue
        name = clean(hdr.get("SPEL_FULL"))
        if not name:
            continue
        # A mutation is real iff the game ships a serum for it. That is the
        # cleanest generative gate available: all 19 live mutations have one,
        # and it filters out the icon/flag helper spells (ChameleonStealthIcon,
        # ChameleonOnAttackFlag, ElectricMob) without a hand-maintained list —
        # a 20th mutation appears here the moment its serum lands in ALCH.
        serum = find_serum_for(sources, name)
        if not serum:
            sources.warnings.append(
                f"mutations: {edid!r} ({name!r}) has no matching serum in ALCH — "
                f"treated as a helper spell, not a mutation")
            continue

        effects: List[Dict[str, Any]] = []
        base_mags: List[float] = []
        serum_mags: List[float] = []
        for r in sources.spel_effects.get(fid, []):
            eff = sources.build_effect(
                mgef_form_id=r.get("EFID_MGEF_FormID", ""),
                mgef_edid=r.get("EFID_MGEF_EDID", ""),
                mgef_full=r.get("EFID_MGEF_FULL", ""),
                magnitude=r.get("EFIT_Magnitude"),
                duration=r.get("EFIT_Duration"),
            )
            e_edid = eff["edid"]
            # The UI-dummy pair is where the game keeps a mutation's numbers.
            # Most mutations ship TWO dummy rows (a low and a high value) — the
            # range the effect scales across — so collect both rather than
            # picking one and implying a single figure.
            if "_UIDummyWithSerum" in e_edid:
                m = eff.get("magnitude")
                if m:
                    serum_mags.append(m)
                continue
            if "_UIDummy" in e_edid:
                m = eff.get("magnitude")
                if m:
                    base_mags.append(m)
                continue
            if is_hidden(eff):
                continue
            if eff["edid"].endswith("Mutation_Treated_Effect") or \
               eff["full"] == "SURV_Mutation_Treated_Effect":
                continue
            # An effect FULL that just repeats the mutation's own name
            # ("Twisted Muscles" on both the melee and limb-damage rows) names
            # the SOURCE, not the effect — re-derive from the EDID so the two
            # rows don't render as the same thing twice.
            override = MUTATION_EFFECT_NAMES.get(eff["edid"])
            if override:
                eff["name"] = override
                eff["display"] = effect_display_line(eff)
            elif eff["full"].strip().lower() == name.strip().lower():
                eff["full"] = ""
                eff["name"] = display_name(eff)
                eff["display"] = effect_display_line(eff)

            g = group_for(eff)
            eff["groupKey"] = g[0] if g else None
            eff["groupLabel"] = g[1] if g else None
            effects.append(eff)

        # A mutation lists each effect TWICE — the base value and the value
        # you get while a serum is active (Bird Bones: +4 AGI, +5 with a
        # serum). Collapse the pair onto one row rather than printing what
        # looks like a duplicate.
        effects = collapse_mutation_effects(effects)
        effects = sort_effects(effects)

        summary: List[str] = []
        desc = clean(hdr.get("SPEL_DESC"))
        if desc:
            summary.append(desc)
        def _range(vals: List[float]) -> Optional[str]:
            if not vals:
                return None
            lo, hi = min(vals), max(vals)
            return (format_magnitude(lo) if lo == hi
                    else f"{format_magnitude(lo)} \u2013 {format_magnitude(hi)}")

        base_r, serum_r = _range(base_mags), _range(serum_mags)
        # Per-effect ranges (above) are better than one page-level number, so
        # the UI-dummy figures are only worth printing when nothing else has
        # any — a mutation whose whole payload lives on the dummy rows.
        if base_r and not any(e.get("magnitude") for e in effects):
            summary.append(f"Effect magnitude: {base_r}")
            if serum_r and serum_r != base_r:
                summary.append(f"With a Mutation Serum: {serum_r}")

        routes = [{
            "route": "Serum",
            "populated": bool(serum),
            "lines": ([f"Item: {serum['name']}",
                       "Vendor: Purchasable from the MODUS Pharmacy in the Whitespring "
                       "Bunker, or crafted at a Chemistry Station with the recipe.",
                       "Note: A serum grants the mutation with none of its negative "
                       "effects for one hour."]
                      if serum else [RECIPE_NOT_IN_GAME]),
            "tradeable": True if serum else None,
            "dropRate": None,
        }, {
            "route": "Radiation",
            "populated": True,
            "lines": ["Fresh: Take radiation damage and the game rolls a random mutation "
                      "from the pool. Starched Genes prevents new mutations, so unequip "
                      "it before farming rads.",
                      "Note: A mutation gained this way carries its negative effects "
                      "unless you also hold a serum or the Class Freak perk."],
            "tradeable": None,
            "dropRate": None,
        }]

        id_rows = [f"SPEL EDID: {edid}", f"SPEL FormID: {fid}"]
        for e in effects:
            id_rows.append(f"MGEF {e['formId']}: {e['edid']}")

        out.append({
            "formId": fid,
            "edid": edid,
            "name": f"{name} Mutation" if not name.lower().endswith("mutation") else name,
            "description": desc,
            "imageUrl": f"{IMAGE_BASE}mutations/{slugify(name)}.avif",
            "objectType": "Mutation",
            "diet": None,
            "effects": effects,
            "effectGroups": sort_groups([e["groupKey"] for e in effects if e.get("groupKey")]),
            "summaryLines": summary,
            "obtainRoutes": routes,
            "technical": {
                "build": ["Type: Mutation",
                          f"Serum: {serum['name'] if serum else 'None in game'}",
                          f"Effect magnitude: {base_r or 'n/a'}",
                          f"With a serum: {serum_r or (base_r or 'n/a')}"],
                "ids": id_rows},
            "status": "live",
        })

    # Two SPELs can carry the same FULL — Mutation_Chameleon and its
    # StealthIcon helper are both "Chameleon", and both find the Chameleon
    # Serum. One row per serum, keeping whichever spell actually has effects.
    best: Dict[str, Dict[str, Any]] = {}
    for row in out:
        key = row["name"].lower()
        cur = best.get(key)
        if cur is None or len(row["effects"]) > len(cur["effects"]):
            best[key] = row
    return list(best.values())


def find_serum_for(sources: Sources, mutation_name: str) -> Optional[Dict[str, str]]:
    want = f"{mutation_name} serum".lower()
    for rec in sources.alch.values():
        if clean(rec.get("name")).lower() == want:
            return {"name": clean(rec.get("name")), "formId": rec["form_id"]}
    return None


# --- scout banners ---------------------------------------------------------

def build_scout_banner_page(sources: Sources, page_key: str,
                            spec: Dict[str, Any]) -> Dict[str, Any]:
    """One root expand per NAMED Scout's Code.

    There is exactly one Scout's Banner item (ALCH 00653FCD), so the item is
    not the unit of interest — the code it fires is. Each row is one code:
    its marker effect (what the banner grants you) plus its proc effect (what
    actually happens on a kill), so Output & Effects carries the real numbers
    rather than the marker's placeholder magnitude of 1.

    Walking the SPEL tree can't produce this page on its own — five of the
    codes share the engine FULL "\u00a2Scout's Code" and the rest of the tree is
    proc handlers and fireworks VFX. See SCOUT_CODES for the declared table
    and SCOUT_PLUMBING for the rows deliberately left off.
    """
    banner_fid = norm_fid(spec.get("banner_form_id", ""))
    banner = sources.alch.get(banner_fid, {})
    banner_name = clean(banner.get("name")) or "Scout's Banner"
    banner_desc = clean(banner.get("desc"))
    banner_img = f"{IMAGE_BASE}scout-banners/{slugify(banner_name)}.avif"

    # Index every MGEF that appears anywhere on a SCORE_Banner spell, keeping
    # the row with the LARGEST magnitude — First Aid ships four tiers (3/6/9/12
    # HP/s) on one effect and the top tier is the one worth quoting.
    by_edid: Dict[str, Dict[str, Any]] = {}
    owner_spell: Dict[str, Tuple[str, str]] = {}
    unknown: Set[str] = set()

    for fid, hdr in sources.spel.items():
        edid = clean(hdr.get("SPEL_EDID"))
        if not edid.startswith("SCORE_Banner") or edid.lower().startswith("zzz"):
            continue
        for r in sources.spel_effects.get(fid, []):
            eff = sources.build_effect(
                mgef_form_id=r.get("EFID_MGEF_FormID", ""),
                mgef_edid=r.get("EFID_MGEF_EDID", ""),
                mgef_full=r.get("EFID_MGEF_FULL", ""),
                magnitude=r.get("EFIT_Magnitude"),
                duration=r.get("EFIT_Duration"),
            )
            e_edid = eff["edid"]
            prev = by_edid.get(e_edid)
            if prev is None or (eff.get("magnitude") or 0) > (prev.get("magnitude") or 0):
                by_edid[e_edid] = eff
                owner_spell[e_edid] = (edid, fid)
            known = any(c["marker"] == e_edid or c["proc"] == e_edid for c in SCOUT_CODES)
            if not known and e_edid not in SCOUT_PLUMBING:
                unknown.add(e_edid)

    for e in sorted(unknown):
        sources.warnings.append(
            f"scout-banners: unrecognised banner effect {e!r} — add it to "
            f"SCOUT_CODES (a new Scout's Code) or SCOUT_PLUMBING (engine only)")

    obtain = [{
        "route": "Scoreboard",
        "populated": True,
        "lines": ["Plan: Scout's Banner",
                  "Note: Purchase with tickets from the Season Scoreboard, or pick one "
                  "up from a teammate who dropped it.",
                  "Availability: Consumable \u2014 one banner per placement."],
        "tradeable": True,
        "dropRate": None,
    }]
    g = GUIDE_LINKS.get("scout-banners")
    if g:
        obtain.append({
            "route": "Fresh / Raw",
            "populated": True,
            "lines": ["Fresh: The banner has no world spawns \u2014 it is a reward "
                      "consumable only.",
                      f"Guide: {g['label']} \u2014 {g['url']}"],
            "tradeable": None,
            "dropRate": None,
        })

    # The banner-wide proc chance is context every row needs, so it is read
    # once and repeated as a Trigger line rather than becoming a row of its own.
    # The proc chance is driven by the banner's rank at runtime, so the export
    # stores 0 here. Printing "0% chance" would read as "never fires".
    proc = by_edid.get("SCORE_Banner_ProcChance_Effect")
    trigger = clean(proc.get("display")) if proc else ""
    if not trigger or trigger.startswith("0%"):
        trigger = "Fires on a kill by you or a nearby teammate."

    rows: List[Dict[str, Any]] = []
    for code in SCOUT_CODES:
        marker = by_edid.get(code["marker"])
        if not marker:
            sources.warnings.append(
                f"scout-banners: marker effect {code['marker']!r} for "
                f"{code['name']!r} is not in this SPEL export \u2014 row skipped")
            continue
        effects = [marker]
        proc_eff = by_edid.get(code["proc"]) if code["proc"] else None
        if proc_eff and proc_eff is not marker:
            # A proc magnitude of 1 with no description is an on/off flag, not
            # a quantity — "+1 Repair Equipped Weapon" is nonsense to a reader.
            if (proc_eff.get("magnitude") or 0) == 1 and not clean(proc_eff.get("dnam")):
                proc_eff["display"] = proc_eff["name"]
            effects.append(proc_eff)

        summary = [code["summary"]]
        # The DNAM may be a template ("Healing <mag> HP/s"). Fill it from the
        # marker's magnitude; if a token survives, the game resolves it at
        # runtime and we have nothing to put there, so drop the line rather
        # than print markup at the reader.
        dnam = substitute_mag(clean(marker.get("dnam")),
                              marker.get("magnitude"), marker.get("duration"))
        if "<" in dnam:
            dnam = ""
        if dnam:
            # "Survival - Heal 20% HP" \u2014 the code name is already the row title.
            short = code["name"].split(":")[-1].strip()
            cleaned = re.sub(rf"^{re.escape(short)}\s*-\s*", "", dnam).strip()
            if cleaned and cleaned.lower() not in summary[0].lower():
                summary.append(f"In-game text: {cleaned}")

        spel_edid, spel_fid = owner_spell.get(code["marker"], ("", ""))
        dur = marker.get("durationDisplay")
        build_rows = [
            f"Source: {banner_name}",
            f"Trigger: {trigger}",
            f"Duration: {dur or 'Instant'}",
        ]
        if proc_eff and proc_eff.get("durationDisplay"):
            build_rows.append(f"Effect Duration: {proc_eff['durationDisplay']}")

        id_rows = [f"ALCH FormID: {banner_fid}  ({banner.get('edid') or 'SCORE_Banner_Consumable'})"]
        if spel_edid:
            id_rows.append(f"SPEL: {spel_edid}  [{spel_fid}]")
        for e in effects:
            id_rows.append(f"MGEF {e['formId']}: {e['edid']}")

        rows.append({
            "formId": marker["formId"],
            "edid": marker["edid"],
            "name": code["name"],
            "gameName": code["name"],
            "description": banner_desc,
            "imageUrl": banner_img,
            "objectType": "Scout's Code",
            "diet": None,
            "effects": effects,
            "effectGroups": [],
            "summaryLines": summary,
            "obtainRoutes": obtain,
            "technical": {"build": build_rows, "ids": id_rows},
            "status": "live",
        })

    # Declared order, not A-Z: the five Scout's Codes read as a set (Survival,
    # Teamwork, Research, Discovery, Innovation \u2014 the order the banner's own
    # description lists them in), then the three standalone effects.
    groups = [{"key": slugify(r["name"]), "label": r["name"], "count": 1} for r in rows]
    return {"mode": "effect-rows", "groups": groups, "items": rows}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_page(sources: Sources, page_key: str) -> Dict[str, Any]:
    spec = PAGES[page_key]
    mode = spec["mode"]
    if mode == "effect-groups":
        body = build_effect_group_page(sources, page_key, spec)
    elif mode == "items-abc":
        body = build_items_abc_page(sources, page_key, spec)
    elif mode == "effect-rows":
        body = build_scout_banner_page(sources, page_key, spec)
    else:                                                     # pragma: no cover
        raise SystemExit(f"unknown mode {mode!r} for page {page_key!r}")

    return {
        "version": today_ymd(),
        "generated": now_iso(),
        "page": page_key,
        "title": spec["title"],
        "url": spec["url"],
        "blurb": spec["blurb"],
        "countNoun": spec["noun"],
        "sourceAlch": os.path.basename(sources.alch_path or ""),
        "sourceMgef": os.path.basename(sources.mgef_path or ""),
        **body,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build the BnB Buffs page JSON.")
    ap.add_argument("--data-dir", default="tsv")
    ap.add_argument("--outdir", default="dist")
    ap.add_argument("--page", action="append",
                    help="build only this page (repeatable)")
    args = ap.parse_args(argv)

    data_dir = args.data_dir
    outdir = args.outdir
    buffs_dir = os.path.join(outdir, "buffs")
    os.makedirs(buffs_dir, exist_ok=True)

    print(f"[build_buffs_json] data-dir={data_dir} outdir={outdir}")
    sources = Sources(data_dir, outdir)
    print(f"[build_buffs_json]   ALCH  {os.path.basename(sources.alch_path)} "
          f"({len(sources.alch)} records)")
    print(f"[build_buffs_json]   MGEF  {os.path.basename(sources.mgef_path)} "
          f"({len(sources.mgef)} effects)")
    print(f"[build_buffs_json]   GLOB  {os.path.basename(sources.glob_path or '-')} "
          f"({len(sources.globs)} globals)")
    print(f"[build_buffs_json]   SPEL  {os.path.basename(sources.spel_hdr_path or '-')} "
          f"({len(sources.spel)} spells)")

    wanted = args.page or [k for k in PAGES if k not in SKIP_BY_DEFAULT]
    manifest_pages: List[Dict[str, Any]] = []
    total = 0

    for key in wanted:
        if key not in PAGES:
            print(f"[build_buffs_json] WARNING unknown page {key!r} — skipped")
            continue
        page = build_page(sources, key)
        path = os.path.join(buffs_dir, f"{key}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(page, f, indent=1, ensure_ascii=False)
        n = len(page["items"])
        total += n
        ng = len(page["groups"])
        print(f"[build_buffs_json] {key:14s} {n:4d} items  {ng:3d} groups  -> {path}")
        manifest_pages.append({
            "page": key,
            "title": page["title"],
            "url": page["url"],
            "mode": page["mode"],
            "file": f"buffs/{key}.json",
            "items": n,
            "groups": ng,
        })

    manifest = {
        "version": today_ymd(),
        "generated": now_iso(),
        "_category": "Buffs",
        "pages": manifest_pages,
        "totals": {"pages": len(manifest_pages), "items": total},
        "warnings": sources.warnings,
    }
    man_path = os.path.join(outdir, "buffs.json")
    if args.page:
        # A partial build must not shrink the manifest to the pages it happened
        # to rebuild — merge onto whatever is already there.
        old = load_json(man_path) or {}
        merged = {p["page"]: p for p in (old.get("pages") or [])}
        merged.update({p["page"]: p for p in manifest_pages})
        manifest["pages"] = [merged[k] for k in PAGES if k in merged]
        manifest["totals"] = {"pages": len(manifest["pages"]),
                              "items": sum(p["items"] for p in manifest["pages"])}
        manifest["partial"] = sorted(args.page)
    with open(man_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1, ensure_ascii=False)
    print(f"[build_buffs_json] manifest -> {man_path}")

    for w in sources.warnings:
        print(f"[build_buffs_json] WARNING {w}")

    write_empty_patchlog_feed(outdir, "patchlog_latest_bnb_buffs.json",
                              current_count=total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
