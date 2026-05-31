#!/usr/bin/env python3
"""
build_recipe_guide_json.py
===========================
Builds dist/recipe_guide.json — the data file for the Recipe Guide page
at /df/plan-checklists/recipe/ on buffsnbrew.com.

Consumable recipes are classified into 5 categories:
  - food      (cooked meals, teas, soups, etc.)
  - chems     (stimpaks, chems, healing items)
  - alcohol   (beers, spirits, wines)
  - serums    (mutation serums)
  - nuka_cola (Nuka-Cola variants, sodas, juices, drinks)

Each recipe entry includes:
  - Ingredients (with pretty names and quantities)
  - How to Obtain (parsed from COBJ ReferencedBy refs)
  - Output item (effects, weight, value, spoil data)
  - Technical info (EDIDs, FormIDs, workbench, keywords)

Inputs:
  dist/farming_guides.json      — effects, output data, spoil times
  dist/cobj-recipes.json        — recipe ingredients & meta
  dist/bnb-item-categories.json — fine-grained category classification
  tsv/COBJ_Export_*.tsv         — ReferencedBy_Flat for how-to-obtain

Output:
  dist/recipe_guide.json

Usage:
  python build_recipe_guide_json.py
  python build_recipe_guide_json.py --data-dir tsv --outdir dist
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple

# Shared helpers
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402
from cut_content import is_cut  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATEGORY_LABELS = {
    "food":      "Food",
    "chems":     "Chems",
    "alcohol":   "Alcohol",
    "serums":    "Serums",
    "nuka_cola": "Nuka-Cola & Drinks",
}

CATEGORY_ORDER = ["food", "chems", "alcohol", "serums", "nuka_cola"]

# Ref record types to surface as how-to-obtain sources.
# QUST = quest, CHAL = challenge, TERM = terminal/arcade prize,
# BOOK = plan/note that teaches the recipe.
HOW_TO_OBTAIN_TYPES = {"QUST", "CHAL", "TERM", "BOOK"}

# LVLI ref EDIDs containing these substrings indicate a meaningful
# acquisition source (quest reward list, vendor table, event reward, etc.).
LVLI_MEANINGFUL = [
    "QuestReward", "Quest_Reward", "Vendor", "Event",
    "Reward", "Prize", "Token", "Treasury", "NPE_",
]

# COBJ TSV files — newest first
COBJ_GLOBS = [
    "COBJ_Export_May_2026.tsv",
    "COBJ_Export_Apr_2026.tsv",
    "COBJ_Export_March_2026.tsv",
    "COBJ_Export_Feb_2026.tsv",
]

# Runtime lookup: ingredient EDID → pretty name.
# Populated from farming_guides ingredients at load time.
_COMPONENT_PRETTY: Dict[str, str] = {}

# COBJ EDIDs matching these patterns are NOT consumable recipes even if they
# sit on a food/chem workbench. Smelting ores, workshop decorations, etc.
_SKIP_EDID_PATTERNS = [
    re.compile(r"Smelting", re.IGNORECASE),
    re.compile(r"workshop_co_", re.IGNORECASE),
    re.compile(r"ATX_workshop", re.IGNORECASE),
    re.compile(r"ATX_Resources", re.IGNORECASE),
    re.compile(r"co_Structure_", re.IGNORECASE),
    re.compile(r"co_Furniture_", re.IGNORECASE),
    re.compile(r"co_Light_", re.IGNORECASE),
    re.compile(r"co_Decorat", re.IGNORECASE),
    re.compile(r"co_Utility_", re.IGNORECASE),
    re.compile(r"co_Power_", re.IGNORECASE),
    re.compile(r"co_Defense_", re.IGNORECASE),
    re.compile(r"SCORE_.*workshop", re.IGNORECASE),
    re.compile(r"Billboard", re.IGNORECASE),
    re.compile(r"Statue", re.IGNORECASE),
    re.compile(r"CandyMachine", re.IGNORECASE),
    re.compile(r"co_Chem_Ammo_", re.IGNORECASE),
]

# BNAM (workbench) EDID → human-readable name
WORKBENCH_PRETTY: Dict[str, str] = {
    "Workbench_Crafting_Cooking":  "Cooking Station",
    "Workbench_Crafting_Chemlab":  "Chemistry Station",
    "Workbench_Crafting_Brewing":  "Brewing Station",
    "Workbench_Crafting_Fermenter": "Fermenter",
    "Workbench_Crafting_TinkersBench": "Tinker's Workbench",
    "WorkshopCanneryStation":      "Cannery Station",
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_ymd() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def clean_str(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    return s


def prettify_component(edid: str) -> str:
    """Convert c_Wood → Wood, c_FiberOptics → Fiber Optics, etc."""
    if not edid:
        return edid
    if edid in _COMPONENT_PRETTY:
        return _COMPONENT_PRETTY[edid]
    name = edid
    if name.startswith("c_"):
        name = name[2:]
    # CamelCase → spaced
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    name = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", name)
    return name


# ---------------------------------------------------------------------------
# 1. Load farming_guides.json → recipe EDID → full recipe record
# ---------------------------------------------------------------------------

def load_farming_guides(dist_dir: str) -> Dict[str, Dict[str, Any]]:
    """Build map: recipe_edid → recipe record (output, effects, ingredients)."""
    path = os.path.join(dist_dir, "farming_guides.json")
    if not os.path.isfile(path):
        print(f"WARNING: {path} not found — effects data will be empty",
              file=sys.stderr)
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"WARNING: {path} is malformed (line {e.lineno}) — "
              f"effects data will be empty", file=sys.stderr)
        return {}

    recipe_map: Dict[str, Dict[str, Any]] = {}
    pages = data.get("pages", {})
    ing_page = pages.get("ingredient-search", {})
    ingredients = ing_page.get("ingredients", [])

    for ing in ingredients:
        # Build ingredient-name lookup for prettifying later
        edid = ing.get("edid", "")
        name = ing.get("name", "")
        if edid and name:
            _COMPONENT_PRETTY[edid] = name

        for recipe in ing.get("used_in", []):
            redid = recipe.get("recipe_edid", "")
            if not redid:
                continue
            # Keep the entry with the most ingredients (richest data)
            existing = recipe_map.get(redid)
            if existing:
                existing_count = len(existing.get("all_ingredients", []))
                new_count = len(recipe.get("all_ingredients", []))
                if new_count <= existing_count:
                    continue
            recipe_map[redid] = recipe

    print(f"Loaded {len(recipe_map)} recipe records from farming_guides.json",
          file=sys.stderr)
    return recipe_map


# ---------------------------------------------------------------------------
# 2. Load bnb-item-categories.json → ALCH EDID → fine category
# ---------------------------------------------------------------------------

def load_item_categories(dist_dir: str) -> Dict[str, str]:
    """Build map: ALCH EDID → one of the 5 output categories."""
    path = os.path.join(dist_dir, "bnb-item-categories.json")
    if not os.path.isfile(path):
        print(f"WARNING: {path} not found — classification may be coarse",
              file=sys.stderr)
        return {}

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Map bnb-item-categories keys → our 5 output categories
    cat_mapping = {
        "food":         "food",
        "canned":       "food",
        "prewar_candy": "food",
        "chems":        "chems",
        "alcohol":      "alcohol",
        "serums":       "serums",
        "nuka_cola":    "nuka_cola",
    }

    lookup: Dict[str, str] = {}
    for src_cat, items in data.get("categories", {}).items():
        dest_cat = cat_mapping.get(src_cat)
        if not dest_cat:
            continue
        for item in items:
            edid = item.get("edid", "")
            if edid:
                lookup[edid] = dest_cat

    print(f"Loaded {len(lookup)} item→category mappings", file=sys.stderr)
    return lookup


# ---------------------------------------------------------------------------
# 3. Load cobj-recipes.json → recipe ingredients + meta
# ---------------------------------------------------------------------------

def load_cobj_recipes(
    dist_dir: str,
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Returns (recipes, meta, variants) from cobj-recipes.json."""
    path = os.path.join(dist_dir, "cobj-recipes.json")
    if not os.path.isfile(path):
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    recipes = data.get("recipes", {})
    meta = data.get("recipe_meta", {})
    variants = data.get("recipe_variants", {})

    print(f"Loaded {len(recipes)} recipes from cobj-recipes.json", file=sys.stderr)
    return recipes, meta, variants


# ---------------------------------------------------------------------------
# 4. Load COBJ TSV → COBJ EDID → ReferencedBy_Flat
# ---------------------------------------------------------------------------

def load_cobj_refs(data_dir: str) -> Dict[str, str]:
    """Build map: COBJ EDID → ReferencedBy_Flat string."""
    refs_map: Dict[str, str] = {}

    tsv_file = None
    for pattern in COBJ_GLOBS:
        candidates = sorted(glob.glob(os.path.join(data_dir, pattern)))
        if candidates:
            tsv_file = candidates[-1]
            break

    if not tsv_file:
        print(f"WARNING: No COBJ TSV found in {data_dir} — refs will be empty",
              file=sys.stderr)
        return refs_map

    print(f"Reading refs from {os.path.basename(tsv_file)}...", file=sys.stderr)

    try:
        with open(tsv_file, "r", encoding="utf-8", errors="replace",
                  newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                edid = clean_str(
                    row.get("COBJ_EDID", "") or row.get("EDID", "")
                )
                refs = clean_str(row.get("ReferencedBy_Flat", ""))
                if edid:
                    refs_map[edid] = refs
    except Exception as e:
        print(f"ERROR reading {tsv_file}: {e}", file=sys.stderr)

    print(f"Loaded refs for {len(refs_map)} COBJ records", file=sys.stderr)
    return refs_map


# ---------------------------------------------------------------------------
# Parse how-to-obtain from ReferencedBy_Flat
# ---------------------------------------------------------------------------

def parse_how_to_obtain(refs_flat: str) -> List[str]:
    """Parse ReferencedBy_Flat into list of how-to-obtain source strings.

    Each ref is FormID:EditorID:RecordType. We surface quest, challenge,
    terminal/arcade, book/note, and meaningful LVLI (quest-reward / vendor /
    event loot lists) references.
    """
    if not refs_flat:
        return []

    sources: List[str] = []
    seen: Set[str] = set()

    for ref in refs_flat.split("|"):
        ref = ref.strip()
        if not ref or ":" not in ref:
            continue

        parts = ref.split(":")
        if len(parts) < 3:
            continue

        form_id = parts[0].strip()
        edid = parts[1].strip()
        record_type = parts[2].strip()

        source: Optional[str] = None

        if record_type in HOW_TO_OBTAIN_TYPES:
            label = {
                "QUST": "Quest",
                "CHAL": "Challenge",
                "TERM": "Terminal",
                "BOOK": "Plan/Note",
            }.get(record_type, record_type)
            ident = edid if edid else f"[{form_id}]"
            source = f"{label}: {ident}"

        elif record_type == "LVLI" and edid:
            if any(kw in edid for kw in LVLI_MEANINGFUL):
                source = f"Loot List: {edid}"

        if source and source not in seen:
            seen.add(source)
            sources.append(source)

    return sources


# ---------------------------------------------------------------------------
# Classify recipe into fine category
# ---------------------------------------------------------------------------

def _is_workshop_recipe(cobj_edid: str) -> bool:
    """Return True if the COBJ EDID matches a non-consumable pattern
    (workshop decoration, smelting, furniture, etc.)."""
    for pat in _SKIP_EDID_PATTERNS:
        if pat.search(cobj_edid):
            return True
    return False


def classify_recipe(
    display_name: str,
    meta: Dict[str, Any],
    item_categories: Dict[str, str],
    farming_data: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Classify a recipe into one of 5 categories, or None if not consumable."""
    cnam_edid = meta.get("cnam_edid", "")
    cobj_edid = meta.get("edid", "")
    coarse_cat = meta.get("category", "other")
    keywords = meta.get("bench_keywords", [])
    bnam = meta.get("bnam_edid", "")
    kw_str = " ".join(keywords)

    # ── Skip workshop / smelting / decoration recipes ──
    if _is_workshop_recipe(cobj_edid):
        return None

    # ── bnb-item-categories lookup (most reliable) ──
    if cnam_edid and cnam_edid in item_categories:
        return item_categories[cnam_edid]

    # ── farming_guides output flags ──
    if farming_data:
        out = farming_data.get("output", {})
        if out.get("is_alcohol"):
            return "alcohol"
        if out.get("is_nuka_cola"):
            return "nuka_cola"

    # ── EDID / keyword pattern matching ──
    # Only apply heuristics when the coarse category is food or chem —
    # prevents CAMP workshop items from leaking into consumable groups.
    if coarse_cat not in ("food", "chem"):
        return None

    # Serums
    if ("MutationSerum" in cobj_edid or "MutationSerum" in cnam_edid
            or "MutationSerum" in kw_str):
        return "serums"

    # Alcohol
    if ("DrinkTypeAlcohol" in kw_str or "DrinkTypeLiquor" in kw_str
            or "RecipeFilter_Brewing" in kw_str):
        return "alcohol"
    if bnam and "Brew" in bnam and "food" not in bnam.lower():
        return "alcohol"

    # Nuka-Cola / drinks
    if ("NukaCola" in kw_str or "DrinkTypeSoda" in kw_str
            or "DrinkTypeJuice" in kw_str):
        return "nuka_cola"
    if "NukaCola" in cnam_edid:
        return "nuka_cola"

    # ── Coarse category fallback ──
    if coarse_cat == "food":
        return "food"
    if coarse_cat == "chem":
        return "chems"

    return None


# ---------------------------------------------------------------------------
# Build recipe guide
# ---------------------------------------------------------------------------

def build_recipe_guide(
    cobj_recipes: Dict[str, Dict[str, int]],
    cobj_meta: Dict[str, Dict[str, Any]],
    cobj_variants: Dict[str, List[Dict[str, Any]]],
    farming_map: Dict[str, Dict[str, Any]],
    item_categories: Dict[str, str],
    cobj_refs: Dict[str, str],
    diag: Diagnostics,
) -> Dict[str, Any]:
    """Build the recipe_guide.json payload."""

    categories: Dict[str, List[Dict[str, Any]]] = {k: [] for k in CATEGORY_ORDER}
    skipped_other = 0
    skipped_cut = 0

    for display_name, ingredients in cobj_recipes.items():
        meta = cobj_meta.get(display_name, {})
        cobj_edid = meta.get("edid", "")
        cnam_edid = meta.get("cnam_edid", "")

        # Skip cut content
        if is_cut(cobj_edid):
            skipped_cut += 1
            continue

        # Get farming_guides data for this recipe (by COBJ EDID)
        fg_data = farming_map.get(cobj_edid)

        # Classify into one of 5 categories
        cat = classify_recipe(display_name, meta, item_categories, fg_data)
        if not cat:
            skipped_other += 1
            continue

        # ── Ingredients ──
        ing_list: List[Dict[str, Any]] = []
        if fg_data and fg_data.get("all_ingredients"):
            for ing in fg_data["all_ingredients"]:
                ing_list.append({
                    "edid": ing.get("edid", ""),
                    "name": (ing.get("name", "")
                             or prettify_component(ing.get("edid", ""))),
                    "qty":  ing.get("qty", 1),
                })
        else:
            for edid, qty in sorted(ingredients.items()):
                ing_list.append({
                    "edid": edid,
                    "name": prettify_component(edid),
                    "qty":  qty,
                })

        # ── Output / effects ──
        output: Dict[str, Any]
        if fg_data:
            fg_out = fg_data.get("output", {})
            output = {
                "name":             fg_out.get("name", display_name),
                "edid":             fg_out.get("edid", cnam_edid),
                "formId":           fg_out.get("formId", ""),
                "weight":           fg_out.get("weight"),
                "value":            fg_out.get("value"),
                "mutation":         fg_out.get("mutation", ""),
                "effects":          fg_out.get("effects", []),
                "spoils_to":        fg_out.get("spoils_to", ""),
                "spoil_time":       fg_out.get("spoil_time"),
                "addiction_name":   fg_out.get("addiction_name", ""),
                "addiction_chance":  fg_out.get("addiction_chance"),
            }
        else:
            output = {
                "name":             display_name,
                "edid":             cnam_edid,
                "formId":           "",
                "weight":           None,
                "value":            None,
                "mutation":         "",
                "effects":          [],
                "spoils_to":        "",
                "spoil_time":       None,
                "addiction_name":   "",
                "addiction_chance":  None,
            }

        # ── How to obtain (from COBJ refs) ──
        refs_flat = cobj_refs.get(cobj_edid, "")
        how_to_obtain = parse_how_to_obtain(refs_flat)

        # ── Workbench ──
        workbench = ""
        if fg_data:
            workbench = fg_data.get("workbench", "")
        if not workbench:
            bnam = meta.get("bnam_edid", "")
            if bnam:
                workbench = WORKBENCH_PRETTY.get(bnam, prettify_component(bnam))

        # ── Build entry ──
        entry: Dict[str, Any] = {
            "name":         display_name,
            "recipe_edid":  cobj_edid,
            "category":     cat,
            "workbench":    workbench,
            "ingredients":  ing_list,
            "howToObtain":  how_to_obtain,
            "output":       output,
            "technical": {
                "recipe_edid":   cobj_edid,
                "output_edid":   cnam_edid,
                "output_formId": output.get("formId", ""),
                "bnam_edid":     meta.get("bnam_edid", ""),
                "cnam_edid":     cnam_edid,
                "bench_keywords": meta.get("bench_keywords", []),
            },
        }

        # Ingredient alternates (e.g., Fish Bits — any fish species)
        alt = meta.get("alternates")
        if alt:
            entry["alternates"] = alt

        # Region variants (e.g., Disease Cure — one per region)
        if display_name in cobj_variants:
            entry["region_variants"] = cobj_variants[display_name]

        categories[cat].append(entry)

    # Sort items alphabetically within each category
    for cat in categories:
        categories[cat].sort(key=lambda e: e["name"].lower())

    total = sum(len(v) for v in categories.values())

    print(f"\nRecipe Guide breakdown:", file=sys.stderr)
    for cat in CATEGORY_ORDER:
        print(f"  {CATEGORY_LABELS[cat]}: {len(categories[cat])}",
              file=sys.stderr)
    print(f"  Total consumables: {total}", file=sys.stderr)
    print(f"  Skipped (not consumable): {skipped_other}", file=sys.stderr)
    print(f"  Skipped (cut content): {skipped_cut}", file=sys.stderr)

    diag.info(
        "recipe_guide.build.summary",
        f"Built recipe guide with {total} consumable recipes "
        f"across {len(CATEGORY_ORDER)} categories.",
        context={
            "total": total,
            "by_category": {c: len(categories[c]) for c in CATEGORY_ORDER},
            "skipped_other": skipped_other,
            "skipped_cut": skipped_cut,
        },
    )

    return {
        "version":         today_ymd(),
        "generated":       now_iso(),
        "count":           total,
        "category_labels": CATEGORY_LABELS,
        "category_order":  CATEGORY_ORDER,
        "categories": {
            cat: {
                "label": CATEGORY_LABELS[cat],
                "count": len(categories[cat]),
                "items": categories[cat],
            }
            for cat in CATEGORY_ORDER
        },
    }


# ---------------------------------------------------------------------------
# Atomic write + round-trip validation
# ---------------------------------------------------------------------------

def atomic_write_json(
    payload: Dict[str, Any],
    output_path: str,
    diag: Diagnostics,
) -> None:
    """Serialize, write to temp, validate, then atomically replace."""
    try:
        serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    except Exception as e:
        diag.error("recipe_guide.serialize.failed",
                   "Failed to serialize recipe_guide payload", detail=str(e))
        diag.save()
        print(f"ERROR serializing {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    out_dir = os.path.dirname(output_path) or "."
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix="recipe_guide.", suffix=".json.tmp", dir=out_dir,
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(serialized)
            f.flush()
            try:
                os.fsync(f.fileno())
            except (OSError, AttributeError):
                pass
    except Exception as e:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        diag.error("recipe_guide.write.failed",
                   "Failed to write temp file", detail=str(e))
        diag.save()
        print(f"ERROR writing tempfile: {e}", file=sys.stderr)
        sys.exit(1)

    # Round-trip validation
    try:
        with open(tmp_path, "r", encoding="utf-8") as f:
            reparsed = json.load(f)
        if not isinstance(reparsed, dict) or "categories" not in reparsed:
            raise ValueError("Missing 'categories' key")
    except Exception as e:
        bad_path = output_path + ".bad"
        try:
            os.replace(tmp_path, bad_path)
        except OSError:
            pass
        diag.error("recipe_guide.roundtrip.failed",
                   "Round-trip validation failed", detail=str(e))
        diag.save()
        print(f"ERROR round-trip validation: {e}", file=sys.stderr)
        sys.exit(1)

    os.replace(tmp_path, output_path)
    print(f"\nWrote {output_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build recipe_guide.json for the Recipe Guide page")
    parser.add_argument("--data-dir", type=str, default="tsv",
                        help="TSV input dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist",
                        help="Output / dist dir (default: dist)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="recipe_guide", outdir=args.outdir)

    # -- Load all inputs --
    farming_map = load_farming_guides(args.outdir)
    item_categories = load_item_categories(args.outdir)
    cobj_recipes, cobj_meta, cobj_variants = load_cobj_recipes(args.outdir)
    cobj_refs = load_cobj_refs(args.data_dir)

    # -- Build --
    payload = build_recipe_guide(
        cobj_recipes, cobj_meta, cobj_variants,
        farming_map, item_categories,
        cobj_refs, diag,
    )

    # -- Write --
    output_path = os.path.join(args.outdir, "recipe_guide.json")
    atomic_write_json(payload, output_path, diag)

    diag.save()
    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
