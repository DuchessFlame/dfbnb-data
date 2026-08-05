#!/usr/bin/env python3
"""
build_farming_used_for.py — generative "Used For" builder for the Farming
"{Item} Spawn Locations" guide pages.

For every item defined in farming_spawns_config.ALL_SETS this script derives a
`used_for` block PURELY from the committed game-file exports / built JSON and
injects it into the matching dist/farming_spawns/<slug>_spawns.json:

    used_for = {
        "consumption": { object_type, weight, value, addiction, note, effects[] },
        "challenges":  [ { name, edid, page } , ... ],   # usually [] for drinks
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
# Reuse the exact effect-name / duration formatting used by the guide pages.
from build_farming_guides_json import (  # noqa: E402
    _clean_effect_name,
    _format_effect_duration,
    find_first,
    ALCH_GLOBS,
    ALCH_EFFECTS_GLOBS,
)

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ── ALCH file resolution ─────────────────────────────────────────────────────
def _resolve_alch(data_dir: str, effects: bool) -> Optional[str]:
    """Return the newest ALCH export in data_dir. Prefers the month-named live
    exports (ALCH_GLOBS order); falls back to PTS-named exports for local
    --data-dir tsv/pts runs (newest by filename)."""
    names = ALCH_EFFECTS_GLOBS if effects else ALCH_GLOBS
    hit = find_first(data_dir, names)
    if hit:
        return hit
    pat = "ALCH_Export_PTS_*_Effects.tsv" if effects else "ALCH_Export_PTS_*.tsv"
    cands = sorted(glob.glob(os.path.join(data_dir, pat)))
    # exclude the *_Effects.tsv when we want the main file
    if not effects:
        cands = [c for c in cands if not c.endswith("_Effects.tsv")]
    return cands[-1] if cands else None


def _read_tsv(path: str) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _safe_num(s: Any) -> Optional[float]:
    try:
        return float(str(s).strip())
    except (TypeError, ValueError):
        return None


# ── Effect display ───────────────────────────────────────────────────────────
_TIER_RE = re.compile(r"_Mag_\d+_([A-Za-z]+)")


def _tier_from_glob(glob_edid: Optional[str]) -> Optional[str]:
    m = _TIER_RE.search(glob_edid or "")
    return m.group(1) if m else None


def _effect_display(name: str, magnitude: Optional[float],
                    mag_glob: Optional[str]) -> str:
    """Player-friendly one-line label for an effect (duration is rendered
    separately by the front-end, so it is NOT included here)."""
    n = (name or "").strip()
    low = n.lower()
    mi = None
    if magnitude is not None:
        try:
            mi = int(round(float(magnitude)))
        except (TypeError, ValueError):
            mi = None
    tier = _tier_from_glob(mag_glob)
    tw = tier.lower() if tier else None
    if tw:
        tw = {"collosal": "colossal"}.get(tw, tw)  # fix known game-data typo

    if low in ("rads", "rad") or low.startswith("rad "):
        return f"+{mi} Rads" if mi else "Rads"
    if "disease chance" in low or low.startswith("chance"):
        return f"{mi}% chance to catch a disease" if mi is not None else "Chance to catch a disease"
    if "disease resist" in low or "disease resistance" in low:
        return f"+{mi} Disease Resistance" if mi else n
    if low.startswith("quench") or "thirst" in low and "increase" not in low:
        return f"Quenches thirst ({tw})" if tw else "Quenches thirst"
    if "satisfy hunger" in low or (("hunger" in low) and "reduc" not in low and "increase" not in low):
        return f"Satisfies hunger ({tw})" if tw else "Satisfies hunger"
    if "reduced hunger" in low or "no hunger" in low:
        return f"-{mi}% hunger rate" if mi else "Reduced hunger rate"
    if "restore health" in low or low == "restore health":
        return "Restores Health"
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
    for r in _read_tsv(main_path):
        if (r.get("ALCH_FormID") or "").strip().upper() == formid:
            kw = (r.get("Keywords_Flat") or "")
            if "ObjectTypeDrink" in kw:
                obj_type = "Drink"
            elif "ObjectTypeFood" in kw:
                obj_type = "Food"
            elif "ObjectTypeChem" in kw:
                obj_type = "Chem"
            w = _safe_num(r.get("Weight"))
            weight = 0 if (w is None or w == 0) else round(w, 2)
            v = _safe_num(r.get("Value"))
            value = int(v) if v is not None else None
            addiction = (r.get("ENIT_Addiction_FULL") or "").strip() or None
            break

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
        dur = _format_effect_duration(_safe_num(r.get("EFIT_Duration")))
        mag_glob = (r.get("MAGG_GLOB_EDID") or "").strip()
        effects.append({
            "name": name,
            "display": _effect_display(name, mag, mag_glob),
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


def _obtain_text(recipe: Dict[str, Any]) -> str:
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
    return ""


def build_recipes(item_name: str, recipe_guide: Dict[str, Any],
                  bench_cat: Dict[str, str]) -> List[Dict[str, Any]]:
    want = item_name.strip().lower()
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
                "display": _effect_display(e.get("name"), e.get("magnitude"), e.get("mag_glob")),
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
            "ingredients": [{"name": i.get("name"), "qty": i.get("qty")} for i in ings],
            "effects": effects,
            "how_to_obtain": _obtain_text(rec),
        })
    recipes.sort(key=lambda r: (r.get("name") or "").lower())
    return recipes


# ── Challenges (item referenced in challenge conditions) ──────────────────────
def build_challenges(formid: str, dist_dir: str) -> List[Dict[str, Any]]:
    path = os.path.join(dist_dir, "challenges", "challenges.json")
    if not os.path.exists(path):
        return []
    data = json.load(open(path, encoding="utf-8"))
    fid = (formid or "").upper()
    hits: List[Dict[str, Any]] = []
    seen = set()

    def cond_strings(d: Dict[str, Any]) -> List[str]:
        out = []
        for k in ("conditions", "conditions_display", "conditions_human"):
            v = d.get(k)
            if isinstance(v, list):
                out.extend(str(x) for x in v)
        return out

    def walk(o, page=None):
        if isinstance(o, dict):
            for cs in cond_strings(o):
                if fid in cs.upper():
                    name = o.get("full") or o.get("edid")
                    key = (o.get("edid") or name)
                    if name and key not in seen:
                        seen.add(key)
                        hits.append({"name": name, "edid": o.get("edid"), "page": page})
                    break
            for k, v in o.items():
                walk(v, page if page else (k if k in ("daily", "weekly", "event", "lifetime", "cut", "social") else page))
        elif isinstance(o, list):
            for v in o:
                walk(v, page)

    walk(data.get("pages", data))
    return hits


# ── Vendor list (flat, sorted by % desc) ─────────────────────────────────────
# Cream/food vendors are generic vending machines / station protectrons that all
# pull from the shared drink pool, so the game gives them no unique player-facing
# name — the identifier is the location marker + vendor TYPE. Rate is set by TYPE,
# not location: Vera 100% > Raider (general + raider pool) > everyone else (general
# drink pool). The only individually-named vendor is Vera (added from drop_rates).
# NOTE: attaching a specific NPC name per placement is NOT possible from the
# committed exports — the vendor REFR base objects are high-load-order placed refs
# absent from the standing NPC/CONT/ACTI exports. That needs a dedicated ref->base
# cross-reference (xEdit/Mappalachia) and is intentionally out of scope here.
RAIDER_MARKERS = {"the crater", "crater core"}


def build_vendor_list(regions: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    vend = ((cfg.get("drop_rates") or {}).get("vendors") or {})
    general_rd = (vend.get("general") or {}).get("rate_display", "~86%")
    raider_extra = (vend.get("raider") or {}).get("rate_display", "")
    rows: List[Dict[str, Any]] = []

    # Vera (or any named 100% vendor) first, from the drop-rate data.
    vera = vend.get("vera")
    if vera and vera.get("location"):
        rd = vera.get("rate_display", "100%")
        if vera.get("qty"):
            rd += f" ({vera['qty']} guaranteed)"
        rows.append({
            "name": "Vera (Blue Ridge)",
            "marker": vera["location"],
            "region": vera.get("region", ""),
            "vendor_type": "Named vendor",
            "rate_display": rd,
            "rate_value": float(vera.get("rate", 1.0)),
            "count": 1,
        })

    for reg in regions:
        for loc in reg.get("locations", []):
            c = (loc.get("sources") or {}).get("vendor", 0)
            if not c:
                continue
            marker = loc.get("marker", "")
            ml = marker.lower()
            if ml in RAIDER_MARKERS:
                vtype = "Raider vendor"
                rd = f"{general_rd} (+{raider_extra} Raider)" if raider_extra else general_rd
                rv = 0.94
            elif marker.endswith("Station"):
                vtype, rd, rv = "Train station vendor", general_rd, 0.86
            else:
                vtype, rd, rv = "Settlement vendor", general_rd, 0.86
            rows.append({
                "name": marker,
                "marker": marker,
                "region": reg.get("region", ""),
                "vendor_type": vtype,
                "rate_display": rd,
                "rate_value": rv,
                "count": c,
            })

    rows.sort(key=lambda r: (-r["rate_value"], r["marker"].lower()))
    return rows


# ── Assemble + inject ────────────────────────────────────────────────────────
def build_used_for(cfg: Dict[str, Any], dist_dir: str, data_dir: str,
                   recipe_guide: Dict[str, Any], bench_cat: Dict[str, str]) -> Dict[str, Any]:
    items = cfg.get("items") or []
    formid = (items[0].get("formid") if items else "") or ""
    name = cfg["name"]
    return {
        "consumption": build_consumption(formid, data_dir, name),
        "challenges": build_challenges(formid, dist_dir),
        "recipes": build_recipes(name, recipe_guide, bench_cat),
    }


def inject(slug: str, used_for: Dict[str, Any], cfg: Dict[str, Any], dist_dir: str) -> bool:
    path = os.path.join(dist_dir, "farming_spawns", f"{slug}_spawns.json")
    if not os.path.exists(path):
        print(f"  [skip] {os.path.relpath(path, REPO)} not built")
        return False
    doc = json.load(open(path, encoding="utf-8"),
                    object_pairs_hook=collections.OrderedDict)
    vendor_list = build_vendor_list(doc.get("regions", []), cfg)
    out = collections.OrderedDict()
    placed = False
    for k, v in doc.items():
        if k in ("used_for", "vendor_list"):
            continue  # drop any old values; re-inserted in canonical spot
        out[k] = v
        if k == "drop_rates":
            out["used_for"] = used_for
            out["vendor_list"] = vendor_list
            placed = True
    if not placed:
        # no drop_rates key — insert before regions
        regions = out.pop("regions", None)
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


def run(slugs: List[str], dist_dir: str, data_dir: str) -> None:
    rg_path = os.path.join(dist_dir, "recipe_guide.json")
    if not os.path.exists(rg_path):
        sys.exit(f"ERROR: {rg_path} not found — run build_recipe_guide_json.py first.")
    recipe_guide = json.load(open(rg_path, encoding="utf-8"))
    bench_cat = _bench_category_map(os.path.join(dist_dir, "cobj-recipes.json"))

    print(f"Building used_for  (dist={dist_dir}  data={data_dir})")
    for slug in slugs:
        cfg = SETS_BY_SLUG.get(slug)
        if not cfg:
            print(f"  [skip] unknown slug '{slug}'")
            continue
        uf = build_used_for(cfg, dist_dir, data_dir, recipe_guide, bench_cat)
        inject(slug, uf, cfg, dist_dir)


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


if __name__ == "__main__":
    main()
