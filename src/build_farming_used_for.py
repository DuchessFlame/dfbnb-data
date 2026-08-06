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
import farming_spawns_sources as sources  # noqa: E402  (item LVLI closure for vendor join)
import rng76  # noqa: E402  (shared LVLI engine — computes vendor appearance rates, never hardcoded)
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
    return max(cands, key=os.path.getmtime) if cands else None


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
        path = max(cands, key=os.path.getmtime) if cands else None
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
    name = cfg["name"]
    return {
        "consumption": build_consumption(formid, data_dir, name),
        "challenges": build_challenges(formid, dist_dir),
        "recipes": build_recipes(name, recipe_guide, bench_cat),
    }


def inject(slug: str, used_for: Dict[str, Any], cfg: Dict[str, Any], dist_dir: str,
           item_closure: Optional[set] = None,
           vendor_master: Optional[List[Dict[str, Any]]] = None,
           rates: Optional["VendorRates"] = None,
           harvest: Optional["HarvestProduce"] = None) -> bool:
    path = os.path.join(dist_dir, "farming_spawns", f"{slug}_spawns.json")
    if not os.path.exists(path):
        print(f"  [skip] {os.path.relpath(path, REPO)} not built")
        return False
    doc = json.load(open(path, encoding="utf-8"),
                    object_pairs_hook=collections.OrderedDict)
    extra_ids = [b.get("formid") for b in (cfg.get("extra_world_bases") or []) if b.get("formid")]
    _patch_drop_rates(doc, rates, _target_fids(cfg), harvest=harvest, extra_base_ids=extra_ids)
    vendor_list = build_vendor_list(doc.get("regions", []), cfg,
                                    item_closure=item_closure, vendor_master=vendor_master,
                                    rates=rates)
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

    print(f"Building used_for  (dist={dist_dir}  data={data_dir})")
    for slug in slugs:
        cfg = SETS_BY_SLUG.get(slug)
        if not cfg:
            print(f"  [skip] unknown slug '{slug}'")
            continue
        item_closure = None
        if tables and vendor_master:
            src = sources.get_sources(cfg["items"], tables)
            item_closure = {f.upper() for f in src["lvli_closure"]}
        uf = build_used_for(cfg, dist_dir, data_dir, recipe_guide, bench_cat)
        inject(slug, uf, cfg, dist_dir, item_closure=item_closure,
               vendor_master=vendor_master, rates=rates, harvest=harvest)


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
