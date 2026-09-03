#!/usr/bin/env python3
"""
build_farming_used_for.py — generative "Used For" builder for the Farming
"{Item} Spawn Locations" guide pages.

For every item defined in farming_spawns_config.ALL_SETS this script derives a
`used_for` block PURELY from the committed game-file exports / built JSON and
injects it into the matching dist/farming_spawns/<slug>_spawns.json:

    used_for = {
        "consumption": { object_type, weight, value, addiction, note, effects[] },
        "challenges":  [ { name, edid, page, type, required } , ... ],
                       # type = Daily / Weekly / Event / Mini Season / ...
                       # required = the challenge's target count (SNAM count)
        "recipes":     [ { name, form_id, category, workbench, mutation,
                           output_qty, item_qty, ingredients[], effects[],
                           how_to_obtain } , ... ],
    }

Data sources (all already produced upstream in CI):
  * <data-dir>/ALCH_Export_*.tsv + *_Effects.tsv  → the item's own drink/food
      effects, object type, weight and value (consumption).
  * <dist-dir>/recipe_guide.json                  → every cooking recipe that
      lists the item as an ingredient (ingredients, effects, workbench, ...).
  * <dist-dir>/cobj-recipes.json                  → bench keywords to refine the
      recipe category (Drink / Food (Meat) / Food).
  * <dist-dir>/challenges/challenges.json         → any challenge whose
      conditions reference the item's form ID.

Because the effect-name and duration formatting is shared with the farming
guide pages, we import those helpers from build_farming_guides_json.

PIPELINE / WORKFLOW
  This runs as a POST step, AFTER build_farming_spawns_json.py (which writes the
  spawn JSONs) AND AFTER build_recipe_guide_json.py (which writes
  recipe_guide.json).  In both the patch build and the PTS build the recipe
  guide is produced before this step, and the PTS build normalises tsv/pts into
  tsv/ and writes every JSON into dist/ before relocating dist/ -> dist/pts/, so
  the default --dist-dir dist / --data-dir tsv is correct for BOTH channels.
  No --pts flag is needed in CI.

USAGE
  python src/build_farming_used_for.py --all
  python src/build_farming_used_for.py --item cream
  # local PTS one-off (reads PTS dist + PTS tsv, writes dist/pts):
  python src/build_farming_used_for.py --all --dist-dir dist/pts --data-dir tsv/pts
"""

from __future__ import annotations

import argparse
import collections
import csv
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from farming_spawns_config import ALL_SETS, SETS_BY_SLUG  # noqa: E402
from spawns_engine import events as _events_engine  # noqa: E402  (Events & Activities rate chaining)
import treasure_map_sources as _treasure_maps  # noqa: E402  (Treasure Maps expand — map -> dig rate)
import farming_spawns_sources as sources  # noqa: E402  (item LVLI closure for vendor join)
import rng76  # noqa: E402  (shared LVLI engine — computes vendor appearance rates, never hardcoded)
# Reuse the exact effect-name / duration formatting used by the guide pages.
from build_farming_guides_json import (  # noqa: E402
    _clean_effect_name,
    _format_effect_duration,
    find_first,
    load_weight_backfill,
)
import glob as _glob       # noqa: E402
import tsv_source          # one resolver for every export selection


# The refactored build_farming_guides_json no longer exports the month-named
# {ALCH,BOOK,COBJ,GLOB}_GLOBS filename lists; resolve the newest matching export
# locally via tsv_source (globs + newest-by-date, works for live tsv/ and tsv/pts/).
def _newest_export(data_dir: str, glob_name: str, exclude: Optional[str] = None) -> Optional[str]:
    cands = [p for p in _glob.glob(os.path.join(data_dir, glob_name))
             if not (exclude and exclude in os.path.basename(p))]
    if not cands:
        return None
    try:
        return max(cands, key=lambda p: (tsv_source.export_date(p), os.path.getmtime(p)))
    except Exception:
        return max(cands, key=os.path.getmtime)

# Cache the weight-backfill map per data_dir (the current ALCH export lost its
# Weight column; older month-named exports still carry it — see
# build_farming_guides_json.load_weight_backfill).
_WEIGHT_BACKFILL: Dict[str, Dict[str, float]] = {}


def _weight_backfill(data_dir: str, current_main: Optional[str]) -> Dict[str, float]:
    if data_dir not in _WEIGHT_BACKFILL:
        paths = [p for p in _glob.glob(os.path.join(data_dir, "ALCH_Export_*.tsv"))
                 if "_Effects" not in os.path.basename(p) and p != current_main]
        _WEIGHT_BACKFILL[data_dir] = load_weight_backfill(paths)
    return _WEIGHT_BACKFILL[data_dir]

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ── ALCH file resolution ─────────────────────────────────────────────────────
def _resolve_alch(data_dir: str, effects: bool) -> Optional[str]:
    """Return the newest ALCH export in data_dir. Prefers the month-named live
    exports (ALCH_GLOBS order); falls back to PTS-named exports for local
    --data-dir tsv/pts runs (newest by filename)."""
    hit = _newest_export(data_dir,
                         "ALCH_Export_*_Effects.tsv" if effects else "ALCH_Export_*.tsv",
                         exclude=None if effects else "_Effects")
    if hit:
        return hit
    pat = "ALCH_Export_PTS_*_Effects.tsv" if effects else "ALCH_Export_PTS_*.tsv"
    return tsv_source.newest(os.path.join(data_dir, pat),
                             exclude=None if effects else "_Effects",
                             required=False)


def _read_tsv(path: str) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _safe_num(s: Any) -> Optional[float]:
    try:
        return float(str(s).strip())
    except (TypeError, ValueError):
        return None


# ── SURV magnitude GLOBs ─────────────────────────────────────────────────────
# Hunger / thirst magnitudes are SECONDS of the survival meter restored, and
# the food-heal magnitudes are health-per-second. Both live on GLOBs
# (SURV_Food_RestoreHunger_Mag_*, SURV_Drink_RestoreThirst_Mag_*,
# SURV_Food_Heal_Mag_*) so the page shows the REAL number, never a tier word
# like "(large)". The effect row usually already carries the resolved value in
# its magnitude column; the GLOB table is the fallback/authority.
_GLOB_CACHE: Dict[str, Dict[str, float]] = {}


def _load_globs(data_dir: str) -> Dict[str, float]:
    """EDID -> float for every GLOB in the newest GLOB export."""
    if data_dir in _GLOB_CACHE:
        return _GLOB_CACHE[data_dir]
    out: Dict[str, float] = {}
    path = _newest_export(data_dir, "GLOB_Export_*.tsv")
    if not path:
        path = tsv_source.newest(os.path.join(data_dir, "GLOB_Export_*.tsv"),
                                 required=False)
    if path:
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            rdr = csv.reader(f, delimiter="\t")
            try:
                hdr = next(rdr)
            except StopIteration:
                hdr = []
            try:
                i_e, i_v = hdr.index("EDID"), hdr.index("FLTV")
            except ValueError:
                i_e = i_v = -1
            if i_e >= 0:
                for row in rdr:
                    if len(row) > max(i_e, i_v):
                        v = _safe_num(row[i_v])
                        if v is not None and row[i_e]:
                            out[row[i_e]] = v
    _GLOB_CACHE[data_dir] = out
    return out


def _fmt_minutes(seconds: Optional[float]) -> Optional[str]:
    """Seconds -> "24 min" (or "1.5 min" for sub-2-minute values)."""
    if not seconds or seconds <= 0:
        return None
    m = seconds / 60.0
    if m < 2:
        return f"{round(m, 1):g} min"
    return f"{int(round(m))} min"


def _fmt_surv(seconds: Optional[float]) -> Optional[str]:
    """A hunger/thirst value is BOTH a magnitude and a duration — the raw
    number the game stores AND how long it keeps the meter filled. Show both:
    "1080/18 min"."""
    mins = _fmt_minutes(seconds)
    if not mins:
        return None
    return f"{round(seconds, 2):g}/{mins}"


def _fmt_amount(v: Optional[float]) -> Optional[str]:
    """1.8 -> '1.8', 45.0 -> '45'."""
    if v is None:
        return None
    return f"{round(v, 2):g}"


# ── Effect display ───────────────────────────────────────────────────────────
def _surv_seconds(magnitude: Optional[float], mag_glob: Optional[str],
                  globs: Optional[Dict[str, float]]) -> Optional[float]:
    """Resolve a hunger/thirst magnitude (seconds of meter restored)."""
    if magnitude:
        return float(magnitude)
    if mag_glob and globs:
        return globs.get(mag_glob)
    return None


def _effect_display(name: str, magnitude: Optional[float],
                    mag_glob: Optional[str],
                    duration: Optional[float] = None,
                    globs: Optional[Dict[str, float]] = None) -> str:
    """Player-friendly one-line label for an effect (the buff duration is
    rendered separately by the front-end, so it is NOT included here — but a
    hunger/thirst "how much meter this fills" value IS, because that is the
    effect's magnitude, not its duration)."""
    n = (name or "").strip()
    low = n.lower()
    mi = None
    if magnitude is not None:
        try:
            mi = int(round(float(magnitude)))
        except (TypeError, ValueError):
            mi = None

    if low in ("rads", "rad") or low.startswith("rad "):
        return f"+{mi} Rads" if mi else "Rads"
    if "disease chance" in low or low.startswith("chance"):
        return f"{mi}% chance to catch a disease" if mi is not None else "Chance to catch a disease"
    if "disease resist" in low or "disease resistance" in low:
        return f"+{mi} Disease Resistance" if mi else n
    if low.startswith("quench") or "thirst" in low and "increase" not in low:
        v = _fmt_surv(_surv_seconds(magnitude, mag_glob, globs))
        return f"Quenches thirst ({v})" if v else "Quenches thirst"
    if "satisfy hunger" in low or (("hunger" in low) and "reduc" not in low and "increase" not in low):
        v = _fmt_surv(_surv_seconds(magnitude, mag_glob, globs))
        return f"Satisfies hunger ({v})" if v else "Satisfies hunger"
    if "reduced hunger" in low or "no hunger" in low:
        return f"-{mi}% hunger rate" if mi else "Reduced hunger rate"
    if "restore health" in low or low == "restore health":
        # Food heals per SECOND over its duration — show the TOTAL restored.
        mag = magnitude if magnitude else (globs or {}).get(mag_glob or "")
        total = _fmt_amount((mag or 0) * duration) if (mag and duration) else _fmt_amount(mag)
        return f"Restores {total} Health" if total else "Restores Health"
    if "ap regen" in low or "action point regen" in low:
        return f"+{mi} Action Point regen" if mi else "+AP Regen"
    if "action point" in low or low.startswith("fortify action"):
        return f"+{mi} Action Points" if mi else n
    if mi:
        return f"+{mi} {n}"
    return n


def _sort_effects(effects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Timed buffs first, plain instant effects next, Rads last."""
    def key(e):
        is_rad = "rad" in (e.get("name") or "").lower()
        if is_rad:
            return 2
        return 0 if e.get("duration") else 1
    return sorted(effects, key=key)


# ── Consumption (item's own ALCH effects) ────────────────────────────────────
def build_consumption(formid: str, data_dir: str, item_name: str = "This item") -> Optional[Dict[str, Any]]:
    formid = (formid or "").upper()
    main_path = _resolve_alch(data_dir, effects=False)
    eff_path = _resolve_alch(data_dir, effects=True)
    if not main_path or not eff_path:
        return None

    obj_type, weight, value, addiction = None, None, None, None
    carnivore = herbivore = False
    for r in _read_tsv(main_path):
        if (r.get("ALCH_FormID") or "").strip().upper() == formid:
            kw = (r.get("Keywords_Flat") or "")
            if "ObjectTypeDrink" in kw:
                obj_type = "Drink"
            elif "ObjectTypeFood" in kw:
                obj_type = "Food"
            elif "ObjectTypeChem" in kw:
                obj_type = "Chem"
            # Mutation diet flags — Carnivore doubles meat, Herbivore doubles
            # plants. Derived from the item's IngredientType* keywords.
            carnivore = "IngredientTypeMeat" in kw
            herbivore = any(t in kw for t in ("IngredientTypeVegetable",
                            "IngredientTypeFruit", "IngredientTypeHerb",
                            "IngredientTypeProduce"))
            w = _safe_num(r.get("Weight"))
            if w is None:  # current ALCH export lost the Weight column — backfill
                w = _weight_backfill(data_dir, main_path).get(formid)
            weight = 0 if w is None else round(w, 2)
            v = _safe_num(r.get("Value"))
            value = int(v) if v is not None else None
            addiction = (r.get("ENIT_Addiction_FULL") or "").strip() or None
            break

    globs = _load_globs(data_dir)
    effects: List[Dict[str, Any]] = []
    for r in _read_tsv(eff_path):
        if (r.get("ALCH_FormID") or "").strip().upper() != formid:
            continue
        name = _clean_effect_name(r.get("MGEF_FULL") or r.get("MGEF_EDID") or "")
        if not name:
            continue
        mag = _safe_num(r.get("EFIT_Magnitude"))
        # Drop a disease-chance effect that rounds to 0% (safe to consume).
        if "disease chance" in name.lower() and (mag is None or round(mag) == 0):
            continue
        raw_dur = _safe_num(r.get("EFIT_Duration"))
        dur = _format_effect_duration(raw_dur)
        mag_glob = (r.get("MAGG_GLOB_EDID") or "").strip()
        effects.append({
            "name": name,
            "display": _effect_display(name, mag, mag_glob,
                                       duration=raw_dur, globs=globs),
            "duration": dur,
        })

    if obj_type is None and not effects:
        return None

    effects = _sort_effects(effects)
    if obj_type:
        note = (f"{item_name} is a {obj_type.lower()} consumable. It grants no "
                f"timed stat buff on its own.")
    else:
        note = f"Consuming {item_name} applies the effects below."
    return {
        "object_type": obj_type,
        "weight": weight,
        "value": value,
        "addiction": addiction,
        "herbivore": herbivore,
        "carnivore": carnivore,
        "note": note,
        "effects": effects,
    }


# ── Recipes (item used as ingredient) ────────────────────────────────────────
def _iter_recipe_entries(recipe_guide: Dict[str, Any]):
    """Yield every full recipe record (dict with an ingredient list + output)."""
    def walk(o):
        if isinstance(o, dict):
            if isinstance(o.get("ingredients"), list) and isinstance(o.get("output"), dict):
                yield o
            for v in o.values():
                yield from walk(v)
        elif isinstance(o, list):
            for v in o:
                yield from walk(v)
    yield from walk(recipe_guide.get("categories", recipe_guide))


def _bench_category_map(cobj_path: Optional[str]) -> Dict[str, str]:
    """recipe_edid → refined category ('Drink' / 'Food (Meat)' / 'Food') from
    cobj-recipes.json bench keywords."""
    out: Dict[str, str] = {}
    if not cobj_path or not os.path.exists(cobj_path):
        return out
    meta = (json.load(open(cobj_path, encoding="utf-8")).get("recipe_meta") or {})
    for _name, m in meta.items():
        edid = (m.get("edid") or "").strip()
        kws = m.get("bench_keywords") or []
        if not edid:
            continue
        if "Meal_Recipe_Drink" in kws:
            out[edid] = "Drink"
        elif "Meal_Recipe_Meat" in kws:
            out[edid] = "Food (Meat)"
        elif m.get("category") == "food":
            out[edid] = "Food"
    return out


_OBTAIN_HINTS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"MeatWeek", re.I), "Meat Week seasonal event recipe."),
    (re.compile(r"Fishing", re.I), "Fishing recipe."),
    (re.compile(r"RSVP", re.I), "Learned from a cookbook terminal (RSVP questline)."),
]

# ── Plan / recipe names (COBJ -> BOOK) ───────────────────────────────────────
# A COBJ's GNAM is its learn condition. When that record is a BOOK, its FULL is
# the in-game plan name the player actually looks for ("Recipe: Fish Chowder",
# "Plan: ..."). When GNAM points at anything else (or is empty) the recipe is
# known by default. NEVER hand-write these strings.
_PLAN_CACHE: Dict[str, Dict[str, str]] = {}


def _load_recipe_plans(data_dir: str) -> Dict[str, str]:
    """recipe COBJ EDID -> plan/recipe BOOK FULL name."""
    if data_dir in _PLAN_CACHE:
        return _PLAN_CACHE[data_dir]
    out: Dict[str, str] = {}
    book_path = _newest_export(data_dir, "BOOK_Export_*.tsv")
    cobj_path = _newest_export(data_dir, "COBJ_Export_*.tsv")
    books: Dict[str, str] = {}
    if book_path:
        for r in _read_tsv(book_path):
            fid = (r.get("FormID") or "").strip().upper()
            full = (r.get("FULL") or "").strip()
            if fid and full:
                books[fid] = full
    if cobj_path:
        for r in _read_tsv(cobj_path):
            edid = (r.get("COBJ_EDID") or "").strip()
            if not edid:
                continue
            gnam = (r.get("GNAM_FormID") or "").strip().upper()
            if gnam in books:
                out[edid] = books[gnam]
                continue
            # Not a plan — some recipes unlock off a CHAL record instead.
            g_edid = (r.get("GNAM_EDID") or "")
            g_full = (r.get("GNAM_FULL") or "").strip()
            if g_full and "challenge" in g_edid.lower():
                out[edid] = f"Unlocked by the challenge: {g_full}"
    _PLAN_CACHE[data_dir] = out
    return out


def _obtain_text(recipe: Dict[str, Any], plans: Optional[Dict[str, str]] = None) -> str:
    """The plan/recipe the player must learn, e.g. "Recipe: Fish Chowder"."""
    plan = (plans or {}).get((recipe.get("recipe_edid") or "").strip())
    if plan:
        return plan
    hto = recipe.get("howToObtain") or []
    if isinstance(hto, list) and hto:
        # Prettify a raw "Terminal: <edid>" entry, else pass real text through.
        parts = [h for h in hto if h and not h.lower().startswith("terminal:")]
        if parts:
            return " ".join(parts)
    out = recipe.get("output") or {}
    edid = " ".join([recipe.get("recipe_edid") or "", recipe.get("edid") or "",
                     out.get("edid") or ""])
    for rx, txt in _OBTAIN_HINTS:
        if rx.search(edid):
            return txt
    return "Known by default."


def build_recipes(item_name: str, recipe_guide: Dict[str, Any],
                  bench_cat: Dict[str, str],
                  data_dir: Optional[str] = None,
                  guide_urls: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    want = item_name.strip().lower()
    plans = _load_recipe_plans(data_dir) if data_dir else {}
    globs = _load_globs(data_dir) if data_dir else {}
    seen = set()
    recipes: List[Dict[str, Any]] = []
    for rec in _iter_recipe_entries(recipe_guide):
        ings = rec.get("ingredients") or []
        item_qty = None
        for i in ings:
            if (i.get("name") or "").strip().lower() == want:
                item_qty = i.get("qty")
                break
        if item_qty is None:
            continue
        out = rec.get("output") or {}
        key = rec.get("recipe_edid") or out.get("formId") or out.get("name")
        if key in seen:
            continue
        seen.add(key)

        r_edid = rec.get("recipe_edid") or ""
        effects = []
        for e in out.get("effects") or []:
            effects.append({
                "name": e.get("name"),
                "display": _effect_display(e.get("name"), e.get("magnitude"),
                                           e.get("mag_glob"),
                                           duration=e.get("duration"), globs=globs),
                "duration": e.get("dur_display"),
            })
        effects = _sort_effects(effects)

        recipes.append({
            "name": rec.get("name") or out.get("name"),
            "form_id": out.get("formId"),
            "recipe_edid": r_edid,
            "category": bench_cat.get(r_edid, (rec.get("category") or "Food").title()),
            "workbench": rec.get("workbench") or "Cooking Station",
            "output_qty": out.get("qty") or rec.get("output_qty") or 1,
            "item_qty": item_qty,
            "mutation": out.get("mutation"),
            "ingredients": _link_ings(ings, guide_urls, item_name),
            "effects": effects,
            "how_to_obtain": _obtain_text(rec, plans),
        })
    recipes.sort(key=lambda r: (r.get("name") or "").lower())
    return recipes


# ── Ingredient → farming-guide hyperlinks + How-to-Obtain (item is the OUTPUT) ──
# Every recipe ingredient that has its own farming guide page (meat / eggs /
# non-perishable / chems / nuka-cola / plants / junk …) is linked to that page.
# The lookup is built from tsv/guide_index.tsv (item-hub rows), so a new guide
# page is linkable the moment it exists — no hardcoded URLs.
_GUIDE_URL_CACHE: Optional[Dict[str, str]] = None
SITE_BASE = "https://www.buffsnbrew.com"

# Recipe ingredient names that differ from the guide page's display name.
_ING_ALIASES = {
    "bloodpack": "blood pack",
    "super stimpak": "stimpak: super",
    "stimpack": "stimpak",
}


def _load_guide_urls() -> Dict[str, str]:
    """lower(item name) → absolute guide URL, from the guide_index item-hub rows.

    Prefers the item HUB (the `sub`/`page` node), not the deeper `-guide` subpage,
    and prefers a bnb-brand page over a df one when both exist."""
    global _GUIDE_URL_CACHE
    if _GUIDE_URL_CACHE is not None:
        return _GUIDE_URL_CACHE
    out: Dict[str, str] = {}
    best: Dict[str, Tuple[int, int]] = {}   # name -> (brand_rank, url_len) chosen so far
    path = os.path.join(REPO, "tsv", "guide_index.tsv")
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                tags = (row.get("tags,nodeType") or row.get("tags") or "").lower()
                if "farming" not in tags and "non-perishable" not in tags \
                        and "nuka-cola" not in tags and "chems" not in tags \
                        and "meat" not in tags and "eggs" not in tags:
                    continue
                node = (row.get("nodeType") or "").strip()
                if node not in ("sub", "page", "top"):
                    continue
                url = (row.get("url") or "").strip()
                name = (row.get("menuTitle") or row.get("title") or "").strip()
                if not url or not name:
                    continue
                # Skip the deep "... Location Guide" subpages — link the hub instead.
                low = name.lower()
                for suff in (" location guide", " spawn locations", " meat", " guide"):
                    if low.endswith(suff):
                        low = low[: -len(suff)].strip()
                if not low:
                    continue
                brand_rank = 0 if (row.get("brand") or "").strip() == "bnb" else 1
                key = (brand_rank, len(url))
                if low not in best or key < best[low]:
                    best[low] = key
                    out[low] = SITE_BASE + url if url.startswith("/") else url
    except FileNotFoundError:
        pass
    _GUIDE_URL_CACHE = out
    return out


def _ing_url(name: Optional[str], guide_urls: Optional[Dict[str, str]]) -> Optional[str]:
    if not name or not guide_urls:
        return None
    low = name.strip().lower()
    if low in guide_urls:
        return guide_urls[low]
    alias = _ING_ALIASES.get(low)
    if alias and alias in guide_urls:
        return guide_urls[alias]
    return None


def _link_ings(ings: List[Dict[str, Any]], guide_urls: Optional[Dict[str, str]],
               main_name: Optional[str]) -> List[Dict[str, Any]]:
    main = (main_name or "").strip().lower()
    out = []
    for i in ings:
        nm = i.get("name")
        d: Dict[str, Any] = {"name": nm, "qty": i.get("qty")}
        url = _ing_url(nm, guide_urls)
        if url:
            d["url"] = url
        if main and nm and nm.strip().lower() == main:
            d["is_main"] = True
        out.append(d)
    return out


def _quest_source_note(edid: Optional[str]) -> str:
    e = edid or ""
    if "SurvivalShortcut" in e:
        return ("Produced by the <b>Survival Shortcut</b> legendary perk card — "
                "it hands you a Survival Syringe when your health runs low.")
    if "BrawlingChemist" in e:
        return ("Produced by the <b>Brawling Chemist</b> legendary perk card while "
                "you fight unarmed / one-handed.")
    if "GHL_" in e:
        return "A Ghoul-update chem, obtained through Ghoul-character content."
    if "HellsEagles" in e or e.startswith("AC_SQ01"):
        return "Reward from the Atlantic City <b>Hell&rsquo;s Eagles</b> side quest."
    if e.startswith("W05"):
        return "Obtained during the Skyline Valley questline."
    if e.startswith("MTNZ"):
        return "Obtained during a mountain / expedition questline."
    if e.startswith("SFS"):
        return "A quest formula (given during the Secrets questline)."
    if e.startswith("SFM"):
        return "An organic / quest-sourced chem."
    if e.startswith("EN07"):
        return "Quest-sourced during the High-Radiation Fluids objective."
    if e.startswith("POST"):
        return "A quest / event-sourced salve."
    if e.startswith("MoM"):
        return "A Mystery of the Mothman quest item."
    return "A quest- or event-locked chem — not craftable at a chem station."


def build_obtain(item_name: str, formid: str, is_quest: bool, item_edid: str,
                 recipe_guide: Dict[str, Any], bench_cat: Dict[str, str],
                 data_dir: Optional[str] = None,
                 guide_urls: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """The How-to-Obtain block: the crafting recipe(s) that PRODUCE this item
    (item is the recipe OUTPUT), each with hyperlinked ingredients + plan name.
    For quest-locked chems with no station recipe, a source note is returned."""
    want = item_name.strip().lower()
    want_fid = (formid or "").strip().upper()
    plans = _load_recipe_plans(data_dir) if data_dir else {}
    globs = _load_globs(data_dir) if data_dir else {}
    seen = set()
    recipes: List[Dict[str, Any]] = []
    for rec in _iter_recipe_entries(recipe_guide):
        out = rec.get("output") or {}
        oname = (out.get("name") or "").strip().lower()
        ofid = (out.get("formId") or out.get("form_id") or "").strip().upper()
        if oname != want and (not want_fid or ofid != want_fid):
            continue
        key = rec.get("recipe_edid") or ofid or oname
        if key in seen:
            continue
        seen.add(key)
        r_edid = rec.get("recipe_edid") or ""
        effects = []
        for e in out.get("effects") or []:
            effects.append({
                "name": e.get("name"),
                "display": _effect_display(e.get("name"), e.get("magnitude"),
                                           e.get("mag_glob"),
                                           duration=e.get("duration"), globs=globs),
                "duration": e.get("dur_display"),
            })
        effects = _sort_effects(effects)
        recipes.append({
            "name": rec.get("name") or out.get("name"),
            "form_id": out.get("formId"),
            "recipe_edid": r_edid,
            "category": bench_cat.get(r_edid, (rec.get("category") or "Chem").title()),
            "workbench": rec.get("workbench") or "Chemistry Station",
            "output_qty": out.get("qty") or rec.get("output_qty") or 1,
            "ingredients": _link_ings(rec.get("ingredients") or [], guide_urls, None),
            "effects": effects,
            "how_to_obtain": _obtain_text(rec, plans),
        })
    recipes.sort(key=lambda r: (r.get("name") or "").lower())
    note = None
    if not recipes and is_quest:
        note = _quest_source_note(item_edid)
    return {"recipes": recipes, "note": note}


# ── Challenges (item referenced in challenge conditions) ──────────────────────
# Each hit carries the challenge TYPE and the REQUIRED count so the page can
# render "Daily - Collect Deathclaw eggs (1)". The type is the challenge's own
# `scope` (Daily / Weekly / Event / Lifetime ...), EXCEPT when the challenge
# lives on a mini-season page (`season:<slug>`), which wins — those read as
# "Mini Season".
_MINI_SEASON = "Mini Season"


def _challenge_type(page: Optional[str], node: Dict[str, Any]) -> str:
    if (page or "").startswith("season:"):
        return _MINI_SEASON
    scope = (node.get("scope") or node.get("group") or "").strip()
    if scope:
        return scope
    return (page or "").replace("-", " ").title()


def build_challenges(formid: str, dist_dir: str) -> List[Dict[str, Any]]:
    path = os.path.join(dist_dir, "challenges", "challenges.json")
    if not os.path.exists(path):
        return []
    data = json.load(open(path, encoding="utf-8"))
    fid = (formid or "").upper()
    hits: Dict[str, Dict[str, Any]] = {}

    def cond_strings(d: Dict[str, Any]) -> List[str]:
        out = []
        for k in ("conditions", "conditions_display", "conditions_human"):
            v = d.get(k)
            if isinstance(v, list):
                out.extend(str(x) for x in v)
        return out

    def walk(o, page: Optional[str]):
        if isinstance(o, dict):
            for cs in cond_strings(o):
                if fid in cs.upper():
                    name = o.get("full") or o.get("edid")
                    key = (o.get("edid") or o.get("form_id") or name or "")
                    if name:
                        req = o.get("required")
                        try:
                            req = int(req)
                        except (TypeError, ValueError):
                            req = None
                        row = {
                            "name": name,
                            "edid": o.get("edid"),
                            "page": page,
                            "type": _challenge_type(page, o),
                            "required": req,
                        }
                        prev = hits.get(key)
                        # A mini-season listing beats the generic events page.
                        if not prev or (row["type"] == _MINI_SEASON
                                        and prev.get("type") != _MINI_SEASON):
                            hits[key] = row
                    break
            for v in o.values():
                walk(v, page)
        elif isinstance(o, list):
            for v in o:
                walk(v, page)

    pages = data.get("pages", data)
    if isinstance(pages, dict):
        for page_key, page_val in pages.items():
            walk(page_val, page_key)
    else:
        walk(pages, None)
    return list(hits.values())


# ── Computed vendor rates (rng76 — NOTHING hardcoded) ────────────────────────
# Every vendor % is the true per-reset appearance probability: "if I open this
# vendor's trade window, what's the chance a unit of the item is in stock?"
# It is resolved from the game data by rng76.appearance_prob() over the exact
# leveled lists the vendor actually rolls — never a typed-in number.

def _fmt_rate(p: float) -> str:
    """Human % for a per-roll pick rate — up to 2 decimals, trailing zeros
    trimmed (e.g. 0.3333 -> '33.33%', 0.15 -> '15%', 0.075 -> '7.5%')."""
    if p <= 0:
        return "0%"
    if p >= 0.9995:
        return "100%"
    s = f"{p * 100.0:.2f}".rstrip("0").rstrip(".")
    return f"{s}%"


class VendorRates:
    """Resolves SINGLE per-roll pick rates from a loaded rng76 dataset.

    ``vendor_rate`` walks a vendor's real inventory: it maps the vendor master's
    ``sells`` EDIDs to LVLI FormIDs, keeps only the ROOT lists (those not
    referenced as a sub-list by another sells entry), resolves each with
    rng76.pick_rate(), and takes the HIGHEST per-roll pick rate across them — the
    item's best single-roll chance in that vendor's stock, NOT a cumulative
    "will it show up" probability. ``list_rate`` does the same for explicit lists
    (generic marker heuristic + drop_rates summary block).
    """

    def __init__(self, data: "rng76.Rng76Data") -> None:
        self.res = data.resolver
        self.lvli = data.lvli
        self.edid2fid: Dict[str, str] = {}
        for fid, ed in data.lvli.edid_by_formid.items():
            self.edid2fid.setdefault(ed, fid)
        # GLOB value by EDID — lets us read a ChanceNone GLOB (e.g. a container's
        # ItemTwo slot) straight from game data instead of typing the number.
        self.glob_by_edid: Dict[str, float] = {}
        for fid, ed in data.globs.edids.items():
            v = data.globs.vals.get(fid)
            if v is not None:
                self.glob_by_edid.setdefault(ed, v)

    def glob_value(self, edid: str) -> Optional[float]:
        return self.glob_by_edid.get(edid) if edid else None

    def appearance(self, list_ids, targets: set) -> float:
        """Highest single-list appearance probability across one or more lists."""
        if isinstance(list_ids, str):
            list_ids = [list_ids]
        return max((self.res.appearance_prob(x, targets) for x in (list_ids or []) if x),
                   default=0.0)

    def roots_of(self, sells_edids: List[str]) -> List[str]:
        fids = [self.edid2fid[e] for e in (sells_edids or []) if e in self.edid2fid]
        children = set()
        for f in fids:
            for entry in self.lvli.entries_by_list.get(f, []):
                ref = (entry.get("LVLO_Reference") or "")
                if ":LVLI" in ref.upper():
                    children.add(ref.split(":")[0].upper())
        return [f for f in fids if f.upper() not in children]

    def _lines(self, list_fids: List[str], targets: set) -> List[float]:
        """One appearance probability per contributing source entry, across all
        given lists, highest first (each is its own display line)."""
        out: List[float] = []
        for lf in list_fids:
            out.extend(self.res.entry_appearances(lf, targets))
        return sorted(out, reverse=True)

    def vendor_lines(self, sells_edids: List[str], targets: set) -> List[float]:
        return self._lines(self.roots_of(sells_edids), targets)

    def list_lines(self, list_ids, targets: set) -> List[float]:
        if isinstance(list_ids, str):
            list_ids = [list_ids]
        list_ids = [x for x in (list_ids or []) if x]
        return self._lines(list_ids, targets) if list_ids else []


def _rate_fields(lines: List[float]) -> Dict[str, Any]:
    """Build the JSON rate fields from a list of per-source probabilities:
    rate_lines (each on its own display line), rate_display (joined), and
    rate_value (the highest, used only for sorting)."""
    displays = [_fmt_rate(x) for x in lines]
    return {
        "rate_lines": displays,
        "rate_display": " / ".join(displays) if displays else "0%",
        "rate_value": round(max(lines), 6) if lines else 0.0,
    }


def _target_fids(cfg: Dict[str, Any]) -> set:
    return {(i.get("formid") or "").upper() for i in (cfg.get("items") or []) if i.get("formid")}


def _general_list_id(cfg: Dict[str, Any]) -> str:
    return (((cfg.get("drop_rates") or {}).get("vendors") or {}).get("general") or {}).get("list_id", "")


def _raider_list_id(cfg: Dict[str, Any]) -> str:
    return (((cfg.get("drop_rates") or {}).get("vendors") or {}).get("raider") or {}).get("list_id", "")


# ── Harvest produce (scripted ACTI / flora FLOR — deterministic, read from data) ─
# Some "world spawns" are not leveled lists at all: a harvestable ACTI runs a
# Papyrus script that hands you the item, or a FLOR flora node yields its produce
# on harvest. There is no ChanceNone to resolve — the yield is deterministic — so
# the 100% must be READ from the produce data, never typed. This reader maps each
# harvestable base FormID to the item FormIDs it produces:
#   * ACTI: parsed from the VMAD_Scripts column already in ACTI_Export_*_ACTI.tsv
#           (e.g. MirelurkHarvestableScript::MirelurkEgg=0023E9D4:MirelurkEgg:ALCH).
#   * FLOR: parsed from FLOR_Export_*.tsv's produce column — needs the new FLOR
#           export (tools ExportFLORToTSV) to be run in xEdit. Until that TSV
#           exists the flora stay flagged not_leveled_list (never fabricated).
# A world_spawns id that is an LVLI (e.g. the mothman LPI flora lists) is walked
# down to its FLOR/ACTI children so the produce lookup still applies.

def _newest(data_dir: str, pattern: str) -> Optional[str]:
    cands = glob.glob(os.path.join(data_dir, pattern))
    return max(cands, key=tsv_source.export_key) if cands else None


class HarvestProduce:
    """base FormID (ACTI/FLOR, or an LVLI resolving to them) -> does it
    deterministically produce one of `targets` on harvest?"""

    _FORMREF = re.compile(r"([0-9A-Fa-f]{8}):[^:\t]*:[A-Za-z]{4}")

    def __init__(self, data_dir: str, lvli: Any = None) -> None:
        self.lvli = lvli
        self.acti = self._load_vmad(data_dir)   # ACTI FormID -> {produced FormIDs}
        self.flor = self._load_flor(data_dir)   # FLOR FormID -> {produced FormIDs}

    def _load_vmad(self, data_dir: str) -> Dict[str, set]:
        path = _newest(data_dir, "ACTI_Export_*_ACTI.tsv")
        out: Dict[str, set] = {}
        if not path:
            return out
        for r in _read_tsv(path):
            fid = (r.get("ACTI_FormID") or "").strip().upper()
            vmad = r.get("VMAD_Scripts") or ""
            if not fid or not vmad:
                continue
            refs = {m.upper() for m in self._FORMREF.findall(vmad)}
            if refs:
                out[fid] = refs
        return out

    def _load_flor(self, data_dir: str) -> Dict[str, set]:
        cands = [p for p in glob.glob(os.path.join(data_dir, "FLOR_Export_*.tsv"))
                 if not p.endswith("_Refs.tsv")]
        path = max(cands, key=tsv_source.export_key) if cands else None
        out: Dict[str, set] = {}
        if not path:
            return out
        for r in _read_tsv(path):
            fid = (r.get("FLOR_FormID") or "").strip().upper()
            if not fid:
                continue
            produce = ""
            for col in ("Produce", "PFIG_Produce", "Ingredient", "PFIG_Ingredient"):
                if r.get(col):
                    produce = r[col]
                    break
            refs = {m.upper() for m in self._FORMREF.findall(produce)}
            if refs:
                out[fid] = refs
        return out

    @property
    def available(self) -> bool:
        return bool(self.acti or self.flor)

    def produces(self, base_id: str, targets: set, depth: int = 0) -> bool:
        bid = (base_id or "").upper()
        if not bid:
            return False
        if self.acti.get(bid, set()) & targets:
            return True
        if self.flor.get(bid, set()) & targets:
            return True
        if self.lvli is not None and depth < 6:
            for e in self.lvli.entries_by_list.get(bid, []):
                ref = (e.get("LVLO_Reference") or "").strip()
                child = ref.split(":")[0].upper() if ref else ""
                if len(child) == 8 and self.produces(child, targets, depth + 1):
                    return True
        return False

    def any_produces(self, ids, targets: set) -> bool:
        return any(self.produces(i, targets) for i in (ids or []) if i)


def _patch_events_activities(doc: Dict[str, Any], rates: Optional["VendorRates"],
                             targets: set) -> None:
    """Fill in per-source drop rates for the Events & Activities expand.

    The engine (spawns_engine.events) already put the raw event/activity reward
    lists into doc['events_activities'] as {list_id, edid, name, type}. Here we
    resolve each list's per-roll appearance chance for the item with rng76 (never
    typed), add the rate fields + a blurb, and sort by chance (desc). Left as-is
    when rng76 is unavailable, so a build without the engine still renders (rate
    just shows blank)."""
    evs = doc.get("events_activities")
    if not (rates and isinstance(evs, list) and evs):
        return
    # Chained rate from each event/activity ROOT: rng76 walks the nested tree
    # (incl. loot-bag sub-lists) DOWN to the item via VendorRates.appearance().
    _events_engine.resolve_event_rates(evs, targets, rates.appearance)


def _patch_treasure_maps(doc: Dict[str, Any], rates: Optional["VendorRates"],
                         targets: set, dist_dir: str) -> None:
    """Fill doc['treasure_maps'] for the Treasure Maps root expand.

    One row per MAP — "if I dig up this map, what's the chance the item is in the
    haul?" — across the three families (region treasure maps, the Lucky Strike /
    U Mine It maps, the Pint-Sized Phantoms' map). Names and FormIDs are JOINED
    from dist/treasure_maps.json, so a new map needs no edit here; the rates are
    resolved with rng76 exactly like every other expand. See
    src/treasure_map_sources.py and spawn-guide skill §9l.

    A map that can't pay the item out is dropped, so an item with no map source
    leaves the key empty and the renderer shows its honest empty state.
    """
    if not (rates and targets):
        return
    maps = _treasure_maps.build_maps(
        dist_dir, targets,
        lambda list_id, t: rates.appearance([list_id], t),
        rates.lvli, _fmt_rate,
    )
    doc["treasure_maps"] = {"maps": maps}


def _patch_drop_rates(doc: Dict[str, Any], rates: Optional["VendorRates"], targets: set,
                      harvest: Optional["HarvestProduce"] = None,
                      extra_base_ids: Optional[List[str]] = None) -> None:
    """Overwrite computed drop_rate summaries with rng76-derived per-entry values,
    so headline numbers and the per-vendor table agree and nothing is typed in.

    Applies to EVERY vendor pool (any key under `vendors` with a `list_id`) and to
    a `creature_drops` pool if present — both are entry-driven, so each source
    entry becomes its own rate line. `world_spawns` / `containers` are left alone:
    their % is a list-level ChanceNone (a GLOB), not a per-entry pick, so they
    aren't resolved by entry_appearances."""
    if not rates:
        return
    dr = doc.get("drop_rates") or {}

    def _apply(node: Dict[str, Any]) -> None:
        if not (isinstance(node, dict) and node.get("list_id")):
            return
        fields = _rate_fields(rates.list_lines(node["list_id"], targets))
        if not fields["rate_lines"]:
            return  # target not reachable via this list — leave any existing text
        node["rate"] = fields["rate_value"]
        node["rate_lines"] = fields["rate_lines"]
        node["rate_display"] = fields["rate_display"]

    for node in (dr.get("vendors") or {}).values():
        _apply(node)
    if isinstance(dr.get("creature_drops"), dict):
        _apply(dr["creature_drops"])

    # world_spawns / containers are single "chance per spawn/container" values, not
    # per-entry lines — computed from game data, never typed.
    def _set(node: Dict[str, Any], p: float) -> None:
        if p > 0:
            node["rate"] = round(p, 6)
            node["rate_display"] = _fmt_rate(p).lstrip("~")
            node["rate_source"] = "computed"
        else:
            # No leveled-list path (scripted ACTI/FLOR harvest) — the export has no
            # probability field to read. Flag it; do NOT fabricate a number.
            node["rate_source"] = "not_leveled_list"

    ws = dr.get("world_spawns")
    if isinstance(ws, dict) and (ws.get("list_id") or ws.get("list_ids")):
        ids = ([ws["list_id"]] if ws.get("list_id") else []) + list(ws.get("list_ids") or [])
        p = rates.appearance(ids, targets)
        if p > 0:
            _set(ws, p)                       # leveled-list world spawn
        elif harvest and harvest.any_produces(list(ids) + list(extra_base_ids or []), targets):
            # Deterministic scripted-ACTI / flora-FLOR harvest: 100% read from the
            # produce data (VMAD script property / FLOR produce field), not typed.
            ws["rate"] = 1.0
            ws["rate_display"] = "100%"
            ws["rate_source"] = "computed"
        else:
            _set(ws, 0.0)                     # no data — flagged not_leveled_list

    cn = dr.get("containers")
    if isinstance(cn, dict):
        # Container "chance per container" = (1 - ItemTwo ChanceNone GLOB) x the
        # item's appearance inside the nest/loot sub-list. Reads the GLOB value from
        # data, so it tracks any Bethesda change to that GLOB.
        gv = rates.glob_value(cn.get("chance_none_glob", ""))
        nest = cn.get("nest_list_id") or cn.get("container_id")
        if gv is not None and nest:
            _set(cn, (1.0 - gv / 100.0) * rates.appearance(nest, targets))
        elif cn.get("list_id") or cn.get("container_id"):
            _set(cn, rates.appearance(cn.get("list_id") or cn.get("container_id"), targets))


# ── CAMP producers: Collectrons + Resource Generators ───────────────────────
# The Collectrons and Resource Generators expands are CARD lists, not prose. Each
# card is one CAMP station that can produce the item:
#
#     {Station name}                       -> links to its camp-items page
#     Obtain via: {ATX / Season / event}
#     {Item} — {rate}                      -> one row per item the page covers
#
# Entries are JOINED at build time from the canonical camp-item exports, so a new
# station appears on every item page it produces with no config edit:
#   dist/collectrons.json        -> drop_rates.collectrons.entries
#   dist/resource_producers.json -> drop_rates.resource_generators.entries
#
# ROUTING: a collectron is a pod/bot that gathers into its own stash; a resource
# generator is a machine that produces the item in place. They are DIFFERENT
# sources and never share an expand — the split follows which export the station
# lives in, which is set upstream by build_camp_items_json.py.
#
# Rates come from the same rng76 resolution the camp-item pages use (percent in
# the export), so both pages always agree. A station that lists the item but
# resolves to 0 keeps its card with rate = None (flagged, never fabricated).
CAMP_PRODUCER_SOURCES = (
    ("collectrons", "collectrons.json"),
    ("resource_generators", "resource_producers.json"),
)


def _load_camp_producers(dist_dir: str) -> Dict[str, List[Dict[str, Any]]]:
    """{'collectrons': [...], 'resource_generators': [...]} from the built camp
    exports. Missing file -> empty list (the expand falls back to its note)."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for key, fname in CAMP_PRODUCER_SOURCES:
        path = os.path.join(dist_dir, fname)
        try:
            out[key] = json.load(open(path, encoding="utf-8")).get("items", []) or []
        except (OSError, ValueError):
            out[key] = []
            print(f"  [warn] {fname} not built — {key} cards skipped")
    return out


def _producer_entries(items: List[Dict[str, Any]], targets: set) -> List[Dict[str, Any]]:
    """Cards for every station whose production drops include any target FormID."""
    entries: List[Dict[str, Any]] = []
    for st in items:
        drops = ((st.get("production") or {}).get("drops")) or []
        rows = []
        for d in drops:
            if (d.get("formId") or "").upper() not in targets:
                continue
            pct = d.get("chance")
            pct = float(pct) if isinstance(pct, (int, float)) else 0.0
            # A resolved 0 is a REAL answer, not a gap: a degenerate waterfall
            # (UseAll + max_count=1 with a ChanceNone-0 entry ordered ABOVE the
            # rarer pools) means the item can never roll. Show 0%, flag the
            # source, and explain it in the node's `note` — never hide the card
            # and never substitute a hand-figure.
            rows.append({
                "name": d.get("name") or d.get("item") or "",
                "form_id": (d.get("formId") or "").upper(),
                "rate": round(pct / 100.0, 6),
                "rate_display": _fmt_rate(pct / 100.0) if pct > 0 else "0%",
                "rate_source": "computed" if pct > 0 else "computed_zero",
            })
        if not rows:
            continue
        rows.sort(key=lambda r: (-(r["rate"] or 0.0), r["name"].lower()))
        entries.append({
            "name": st.get("displayName") or st.get("edid") or "",
            "edid": st.get("edid") or "",
            "obtain": ((st.get("howToObtain") or {}).get("display") or "").strip(),
            "interval": ((st.get("production") or {}).get("intervalDisplay") or "").strip(),
            "items": rows,
        })
    entries.sort(key=lambda e: (-(e["items"][0]["rate"] or 0.0), e["name"].lower()))
    return entries


def _patch_camp_producers(doc: Dict[str, Any], targets: set, dist_dir: str) -> None:
    """Fill drop_rates.collectrons / .resource_generators with joined station cards.

    Any hand-written `note` on those nodes is PRESERVED (it renders under the
    cards). A node with no matching station is left as-is, so an item with no
    producers still shows its honest empty state."""
    dr = doc.get("drop_rates")
    if not isinstance(dr, dict):
        return
    producers = _load_camp_producers(dist_dir)
    for key in ("collectrons", "resource_generators"):
        entries = _producer_entries(producers.get(key) or [], targets)
        if not entries:
            continue
        node = dr.get(key)
        if not isinstance(node, dict):
            node = collections.OrderedDict()
            dr[key] = node
        node["entries"] = entries


# ── Vendor list (flat, sorted by % desc) ─────────────────────────────────────
# Rate is set by vendor TYPE, not location: Vera 100% > Raider (general + raider
# pool) > everyone else (general pool).
#
# NAMES: once dist/vendors.json (the NPC2 vendor master) exists, we join the
# item's own LVLI closure against each vendor's `sells` closure to list the REAL
# named vendors that stock the item (with their marker/region). When the master
# is absent — or an item has no named seller (e.g. eggs, which no vendor sells) —
# we fall back to the marker + TYPE heuristic below. Vera is always added from
# drop_rates (the only guaranteed 100% named vendor).
RAIDER_MARKERS = {"the crater", "crater core"}


def _vera_row(cfg: Dict[str, Any], rates: Optional["VendorRates"], targets: set) -> Optional[Dict[str, Any]]:
    vend = ((cfg.get("drop_rates") or {}).get("vendors") or {})
    vera = vend.get("vera")
    if not (vera and vera.get("location")):
        return None
    # Compute Vera's rate from her own list when we can; else trust config.
    lines = (rates.list_lines(vera.get("list_id", ""), targets)
             if rates and vera.get("list_id") else [float(vera.get("rate", 1.0))])
    fields = _rate_fields(lines)
    if vera.get("qty"):
        fields["rate_display"] += f" ({vera['qty']} guaranteed)"
        fields["rate_lines"] = [fields["rate_lines"][0] + f" ({vera['qty']} guaranteed)"] \
            if fields["rate_lines"] else fields["rate_lines"]
    return {
        "name": "Vera (Blue Ridge)",
        "marker": vera["location"],
        "region": vera.get("region", ""),
        "vendor_type": "Named vendor",
        **fields,
        "count": 1,
    }


def _tier(marker: str, is_raider: bool, cfg: Dict[str, Any],
          rates: Optional["VendorRates"], targets: set):
    """(vendor_type, rate_display, rate_value) for a non-Vera vendor whose exact
    inventory tree is unknown (marker heuristic). The generic bot rolls the shared
    drink/food pool; a raider-location bot additionally rolls the raider faction
    pool, so the two combine. All numbers come from rng76 — no hardcoded tiers."""
    general = rates.list_lines(_general_list_id(cfg), targets) if rates else []
    if is_raider:
        extra = rates.list_lines(_raider_list_id(cfg), targets) if rates else []
        lines = sorted(general + extra, reverse=True)
        return "Raider vendor", _rate_fields(lines)
    if (marker or "").endswith("Station"):
        return "Train station vendor", _rate_fields(general)
    return "Settlement vendor", _rate_fields(general)


def build_vendor_list_named(cfg: Dict[str, Any], item_closure: set,
                            vendor_master: List[Dict[str, Any]],
                            rates: Optional["VendorRates"], targets: set) -> List[Dict[str, Any]]:
    """Real named vendors whose stock closure intersects the item's LVLI closure.
    Each vendor's rate is resolved from ITS OWN inventory tree via rng76 (raider
    faction pools fold in automatically when the vendor actually rolls them)."""
    rows: List[Dict[str, Any]] = []
    for v in vendor_master:
        if not item_closure & set(v.get("sells_formids") or []):
            continue
        marker = v.get("marker", "")
        ident = f"{v.get('faction','')} {v.get('edid','')} {v.get('container_base','')}".lower()
        is_raider = ("raider" in ident) or (marker.lower() in RAIDER_MARKERS)
        lines = rates.vendor_lines(v.get("sells") or [], targets) if rates else []
        # Fall back to the type heuristic only if the tree yielded nothing.
        if not lines:
            vtype, fields = _tier(marker, is_raider, cfg, rates, targets)
        else:
            vtype = ("Raider vendor" if is_raider
                     else "Train station vendor" if (marker or "").endswith("Station")
                     else "Settlement vendor")
            fields = _rate_fields(lines)
        rows.append({
            "name": v.get("name", ""),
            "marker": marker,
            "region": v.get("region", ""),
            "vendor_type": vtype,
            **fields,
            "count": 1,
        })
    return rows


def build_vendor_list_heuristic(regions: List[Dict[str, Any]], cfg: Dict[str, Any],
                                rates: Optional["VendorRates"], targets: set) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for reg in regions:
        for loc in reg.get("locations", []):
            c = (loc.get("sources") or {}).get("vendor", 0)
            if not c:
                continue
            marker = loc.get("marker", "")
            vtype, fields = _tier(marker, marker.lower() in RAIDER_MARKERS, cfg, rates, targets)
            rows.append({
                "name": marker,
                "marker": marker,
                "region": reg.get("region", ""),
                "vendor_type": vtype,
                **fields,
                "count": c,
            })
    return rows


def build_vendor_list(regions: List[Dict[str, Any]], cfg: Dict[str, Any],
                      item_closure: Optional[set] = None,
                      vendor_master: Optional[List[Dict[str, Any]]] = None,
                      rates: Optional["VendorRates"] = None) -> List[Dict[str, Any]]:
    """Prefer real named vendors (via the sells join); fall back to the marker +
    TYPE heuristic. Vera is always prepended from drop_rates."""
    targets = _target_fids(cfg)
    named = (build_vendor_list_named(cfg, item_closure, vendor_master, rates, targets)
             if (vendor_master and item_closure) else [])
    body = named if named else build_vendor_list_heuristic(regions, cfg, rates, targets)

    # Collapse exact-duplicate rows (same name+marker+region+type) into one, summing
    # count — matches the §9e "×N" pill. Distinct locations stay as separate rows.
    merged: Dict[tuple, Dict[str, Any]] = {}
    for r in body:
        key = (r["name"], r.get("marker", ""), r.get("region", ""), r["vendor_type"])
        if key in merged:
            merged[key]["count"] += r.get("count", 1)
        else:
            merged[key] = dict(r)

    rows: List[Dict[str, Any]] = []
    vera = _vera_row(cfg, rates, targets)
    if vera:
        rows.append(vera)
    rows.extend(merged.values())
    rows.sort(key=lambda r: (-r["rate_value"], (r.get("region") or "~"),
                             (r.get("marker") or "").lower(), r["name"].lower()))
    return rows


# ── Assemble + inject ────────────────────────────────────────────────────────
def build_used_for(cfg: Dict[str, Any], dist_dir: str, data_dir: str,
                   recipe_guide: Dict[str, Any], bench_cat: Dict[str, str]) -> Dict[str, Any]:
    items = cfg.get("items") or []
    formid = (items[0].get("formid") if items else "") or ""
    edid = (items[0].get("edid") if items else "") or ""
    name = cfg["name"]
    guide_urls = _load_guide_urls()
    return {
        "consumption": build_consumption(formid, data_dir, name),
        "challenges": build_challenges(formid, dist_dir),
        "recipes": build_recipes(name, recipe_guide, bench_cat, data_dir, guide_urls),
        # How to Obtain: the crafting recipe(s) that PRODUCE this item + quest note.
        "obtain": build_obtain(name, formid, bool(cfg.get("is_quest")), edid,
                               recipe_guide, bench_cat, data_dir, guide_urls),
    }


# ── Containers expand — container TYPE → rng76 appearance rate ────────────────
# The Containers expand lists each lootable world CONTAINER TYPE that can hold the
# item, with the item's drop rate for that container type: "if you open a {type},
# there is X% chance it holds this item." NOT a location list, NOT a map.
#
# Method (spawn-guide §9k Containers rule):
#   - Walk the item's LVLI up-closure. For every closure list L, take the CONT
#     bases that reference it (LVLI_Refs, sig CONT).
#   - Keep only REAL lootable containers: farming_classify("CONT", edid, via) must
#     return "container" (this drops VendorChest→Vendors, VendingMachine/collectron
#     →Resource Generators/Collectrons) AND the base must have a display name in
#     CONT_Export.
#   - Rate = rng76 appearance_prob(L, item) — the waterfall/pick-one/ChanceNone
#     resolved chance the item shows when that container's loot list is rolled once.
#     NEVER a bare ChanceNone, never hand-rolled.
#   - Aggregate by container display name. Identical rates for the same name dedupe
#     to one row; DISTINCT rates under the same name each get their own row (a "Safe"
#     that rolls three different loot lists shows three rows). Drop genuine 0%. Show
#     ALL — no cap, no minimum-rate cutoff. Sort by rate desc, then name.
_CONT_NAME_CACHE: Dict[str, Dict[str, str]] = {}
CONT_GLOBS = ["CONT_Export_*.tsv"]


def _load_cont_names(data_dir: str) -> Dict[str, str]:
    """CONT FormID (upper hex) → in-game FULL display name."""
    if data_dir in _CONT_NAME_CACHE:
        return _CONT_NAME_CACHE[data_dir]
    out: Dict[str, str] = {}
    path = _newest_export(data_dir, "CONT_Export_*.tsv")
    if path:
        for r in _read_tsv(path):
            fid = (r.get("FormID") or "").strip().upper()
            full = (r.get("FULL") or "").strip()
            if fid and full:
                out[fid] = full
    _CONT_NAME_CACHE[data_dir] = out
    return out


def container_types(closure, targets: set, appearance, cont_names: Dict[str, str],
                    lvli_refs: Dict[str, Any], parent_edid: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The Containers type+rate list. REUSABLE across families (farming / meat /
    drinks): `appearance(list_id, targets) -> float` is the family's rng76
    appearance-probability callable. Distinct rates under one name are kept as
    separate rows; identical rates dedupe; 0% dropped; sorted by rate desc, name."""
    from spawns_engine.classify import farming_classify as _classify  # lazy (avoid import cycle)
    by_name: Dict[str, Dict[float, float]] = {}
    for L in closure:
        refs = lvli_refs.get(L) or lvli_refs.get(str(L).upper()) or ()
        via = parent_edid.get(L, "")
        names = []
        for rf, red, rs in refs:
            if rs != "CONT":
                continue
            if _classify("CONT", red, via) != "container":
                continue
            nm = cont_names.get((rf or "").upper())
            if nm:
                names.append(nm)
        if not names:
            continue
        rate = appearance(L, targets)
        if not rate or rate <= 0:
            continue
        key = round(rate, 4)
        for nm in names:
            by_name.setdefault(nm, {})[key] = rate
    types = []
    for nm, rate_map in by_name.items():
        for rr in rate_map.values():
            types.append({"name": nm, "rate": rr, "rate_display": _fmt_rate(rr)})
    types.sort(key=lambda t: (-t["rate"], t["name"].lower()))
    return types


def _patch_containers(doc: Dict[str, Any], closure, targets: set,
                      rates: Optional["VendorRates"], cont_names: Dict[str, str],
                      tables: Any) -> None:
    """Set doc['drop_rates']['containers'] = {'types': [{name, rate, rate_display}]}"""
    if not rates or not tables or not closure or not targets:
        return
    types = container_types(closure, targets, lambda L, t: rates.appearance([L], t),
                            cont_names, tables.get("lvli_refs", {}), tables.get("parent_edid", {}))
    dr = doc.get("drop_rates")
    if not isinstance(dr, dict):
        dr = {}
        doc["drop_rates"] = dr
    dr["containers"] = {"types": types}


def inject(slug: str, used_for: Dict[str, Any], cfg: Dict[str, Any], dist_dir: str,
           item_closure: Optional[set] = None,
           vendor_master: Optional[List[Dict[str, Any]]] = None,
           rates: Optional["VendorRates"] = None,
           harvest: Optional["HarvestProduce"] = None,
           cont_names: Optional[Dict[str, str]] = None,
           tables: Any = None,
           closure_lists: Any = None) -> bool:
    path = os.path.join(dist_dir, "farming_spawns", f"{slug}_spawns.json")
    if not os.path.exists(path):
        print(f"  [skip] {os.path.relpath(path, REPO)} not built")
        return False
    doc = json.load(open(path, encoding="utf-8"),
                    object_pairs_hook=collections.OrderedDict)
    extra_ids = [b.get("formid") for b in (cfg.get("extra_world_bases") or []) if b.get("formid")]
    _patch_drop_rates(doc, rates, _target_fids(cfg), harvest=harvest, extra_base_ids=extra_ids)
    _patch_events_activities(doc, rates, _target_fids(cfg))
    _patch_camp_producers(doc, _target_fids(cfg), dist_dir)
    _patch_treasure_maps(doc, rates, _target_fids(cfg), dist_dir)
    # Containers expand → container-type → rng76 rate (runs AFTER _patch_drop_rates
    # so the type list is the final word on doc['drop_rates']['containers']).
    _patch_containers(doc, closure_lists, _target_fids(cfg), rates,
                      cont_names or {}, tables)
    vendor_list = build_vendor_list(doc.get("regions", []), cfg,
                                    item_closure=item_closure, vendor_master=vendor_master,
                                    rates=rates)
    out = collections.OrderedDict()
    placed = False
    farming_tips = doc.get("farming_tips")
    for k, v in doc.items():
        if k in ("used_for", "vendor_list", "farming_tips"):
            continue  # drop any old values; re-inserted in canonical spot
        out[k] = v
        if k == "drop_rates":
            if farming_tips is not None:
                out["farming_tips"] = farming_tips
            out["used_for"] = used_for
            out["vendor_list"] = vendor_list
            placed = True
    if not placed:
        # no drop_rates key — insert before regions
        regions = out.pop("regions", None)
        if farming_tips is not None:
            out["farming_tips"] = farming_tips
        out["used_for"] = used_for
        out["vendor_list"] = vendor_list
        if regions is not None:
            out["regions"] = regions
    json.dump(out, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    r = used_for.get("recipes") or []
    c = used_for.get("challenges") or []
    print(f"  {os.path.basename(path):<34} recipes:{len(r):>2}  challenges:{len(c):>2}"
          f"  consumption:{'yes' if used_for.get('consumption') else 'no'}"
          f"  vendors:{len(vendor_list):>2}")
    return True


def _load_vendor_master(dist_dir: str) -> List[Dict[str, Any]]:
    path = os.path.join(dist_dir, "vendors.json")
    try:
        return json.load(open(path, encoding="utf-8")).get("vendors", [])
    except Exception:
        return []


def run(slugs: List[str], dist_dir: str, data_dir: str) -> None:
    rg_path = os.path.join(dist_dir, "recipe_guide.json")
    if not os.path.exists(rg_path):
        sys.exit(f"ERROR: {rg_path} not found — run build_recipe_guide_json.py first.")
    recipe_guide = json.load(open(rg_path, encoding="utf-8"))
    bench_cat = _bench_category_map(os.path.join(dist_dir, "cobj-recipes.json"))

    # Vendor master (real names) + LVLI tables for the item→vendor sells join.
    vendor_master = _load_vendor_master(dist_dir)
    try:
        tables = sources.load_tables(data_dir)
    except Exception as e:
        print(f"  [warn] LVLI tables unavailable ({e}); vendor names fall back to markers.")
        tables = None
    if vendor_master:
        print(f"  vendor master: {len(vendor_master)} vendors from {os.path.join(dist_dir, 'vendors.json')}")

    # Shared rng76 engine — every vendor % is COMPUTED from game data, never hardcoded.
    rates: Optional[VendorRates] = None
    try:
        rates = VendorRates(rng76.Rng76Data.from_tsv_root(data_dir))
        print(f"  rng76 engine loaded from {data_dir} — vendor rates will be computed.")
    except Exception as e:
        print(f"  [warn] rng76 engine unavailable ({e}); vendor rates left blank.")

    # Harvest-produce reader — scripted-ACTI / flora-FLOR world spawns are
    # deterministic, so their 100% is READ from the produce data (ACTI VMAD now;
    # FLOR export when present), never typed.
    harvest: Optional[HarvestProduce] = None
    try:
        harvest = HarvestProduce(data_dir, lvli=(rates.lvli if rates else None))
        print(f"  harvest-produce: ACTI={len(harvest.acti)} scripted, FLOR={len(harvest.flor)} flora "
              f"({'FLOR export present' if harvest.flor else 'no FLOR export yet — flora stay flagged'}).")
    except Exception as e:
        print(f"  [warn] harvest-produce reader unavailable ({e}); harvestables stay not_leveled_list.")

    cont_names = _load_cont_names(data_dir)
    if cont_names:
        print(f"  container names: {len(cont_names)} CONT records for the Containers type list.")

    print(f"Building used_for  (dist={dist_dir}  data={data_dir})")
    for slug in slugs:
        cfg = SETS_BY_SLUG.get(slug)
        if not cfg:
            print(f"  [skip] unknown slug '{slug}'")
            continue
        item_closure = None
        closure_lists = None
        if tables:
            src = sources.get_sources(cfg["items"], tables)
            closure_lists = src["lvli_closure"]
            item_closure = {f.upper() for f in closure_lists}
        uf = build_used_for(cfg, dist_dir, data_dir, recipe_guide, bench_cat)
        inject(slug, uf, cfg, dist_dir, item_closure=item_closure,
               vendor_master=vendor_master, rates=rates, harvest=harvest,
               cont_names=cont_names, tables=tables, closure_lists=closure_lists)
        if cfg.get("multi_item"):
            _inject_multi_item(slug, cfg, dist_dir, data_dir)
        # Honeycomb also hosts the Honey Beast creature (it drops Honeycomb). Fold
        # the creature bundle in AFTER used_for so both survive (inject() above
        # copies unknown keys like honey_beast through). Runs in the CI --all pass.
        if slug == "honeycomb":
            try:
                import honeycomb_honey_beast
                honeycomb_honey_beast.inject(dist_dir)
            except Exception as e:
                print(f"  [warn] Honey Beast fold-in skipped for honeycomb ({e})")


def _inject_multi_item(slug, cfg, dist_dir, data_dir):
    """For a combined multi-item page (e.g. Salt/Pepper/Sugar & Spices): add an
    `item_breakdown` (per-item consumption, one Used-For sub-expand each) and a
    `fixed_spawn_index` (turns Fixed Spawn Locations into a region index linking
    the per-region pages, instead of dumping every marker)."""
    path = os.path.join(dist_dir, "farming_spawns", f"{slug}_spawns.json")
    if not os.path.exists(path):
        return
    doc = json.load(open(path, encoding="utf-8"),
                    object_pairs_hook=collections.OrderedDict)
    breakdown = []
    for it in (cfg.get("items") or []):
        breakdown.append({
            "name": it.get("full") or it.get("edid"),
            "formid": it.get("formid"),
            "consumption": build_consumption(it.get("formid"), data_dir, it.get("full") or "This item"),
        })
    base = cfg.get("region_index_base") or ""
    regions = [{"region": r.get("region"),
                "count": len(r.get("locations") or []),
                "url": (base + _region_slug(r.get("region")) + "/") if base else ""}
               for r in doc.get("regions", [])]
    doc["item_breakdown"] = breakdown
    doc["fixed_spawn_index"] = {"base": base, "regions": regions}
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  {os.path.basename(path):<34} multi_item: {len(breakdown)} items, "
          f"{len(regions)} region-index links")


def _region_slug(region):
    """'The Mire' -> 'the-mire', 'Ash Heap' -> 'ash-heap', 'The Pitt' -> 'the-pitt'.

    Canonical hyphenated slugs, matching the DF location guides (Bobbleheads,
    Magazines) and DF_REGION_SLUGS in df-bnb-farming-non-perishable-guide.js.
    The old BNB pages dropped the leading 'the' for The Mire only — an
    inconsistency with The Pitt right beside it — so 'mire' is now a legacy
    alias the renderer still accepts rather than something we emit.
    """
    return (region or "").strip().lower().replace(" ", "-")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate used_for for farming spawn guides.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--item", metavar="SLUG", help="Build one item by slug")
    g.add_argument("--all", action="store_true", help="Build every configured item")
    ap.add_argument("--dist-dir", default=os.path.join(REPO, "dist"),
                    help="Dir holding recipe_guide.json, challenges/, farming_spawns/ (default: dist)")
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"),
                    help="Dir holding ALCH exports (default: tsv)")
    args = ap.parse_args(argv)

    slugs = [c["slug"] for c in ALL_SETS] if args.all else [args.item]
    run(slugs, args.dist_dir, args.data_dir)

    # Farming - Chems: fold the huge generic medical-loot placements (15k–27k on
    # Stimpak/RadAway/Rad-X/Mentats-family docs) into note-only Containers/Creatures
    # expands, keeping only genuine fixed world points. Runs AFTER the used_for join
    # so its container/creature notes are the final word; idempotent + threshold-gated
    # so Blood-Sac-sized docs stay fully enumerated. See chem_loot_collapse.py.
    try:
        import chem_loot_collapse
        chem_loot_collapse.run(args.dist_dir)
    except Exception as e:  # pragma: no cover - collapse is best-effort
        print(f"  [warn] chem_loot_collapse skipped ({e})")


if __name__ == "__main__":
    main()
