from __future__ import annotations

"""
build_cobj_recipes_json.py
===========================
Generates dist/cobj-recipes.json from xEdit COBJ TSV export.

Reads COBJ entries and extracts crafting recipes for food, chem, and other
craftable items. Captures FNAM_Keywords so downstream builders (menu-items)
can classify each recipe as food/chem/other without needing a second TSV.

Input files (place in tsv/ folder or pass via --data-dir):
  COBJ_Export_*.tsv       (xEdit tab-separated export)

Output:
  dist/cobj-recipes.json  -> {
    "version": "YYYY-MM-DD",
    "generated": "<ISO-8601 UTC>",
    "count": N,
    "recipes":     { "Item Name": { "Ingredient": count, ... } },
    "recipe_meta": { "Item Name": {
        "edid": "co_meal_...",
        "cnam_edid": "...",
        "bnam_edid": "Workbench_Cooking" | "",   # BNAM (FO76 workbench ref)
        "bench_keywords": ["Meal_Recipe_Food", ...],
        "category": "food" | "chem" | "other",
        "source_file": "COBJ_Export_March_2026.tsv"
    } }
  }

Diagnostics:
  Writes cobj_recipes section of dist/diagnostics.json reporting:
    - TSV files that have no usable rows (column-rename / empty export)
    - Rows with a display name but no ingredients (FVPA missing)
    - Duplicate recipes across months (info only)

Usage:
  python build_cobj_recipes_json.py
  python build_cobj_recipes_json.py --data-dir /path/to/tsvs --outdir /path/to/dist
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

# Import shared diagnostics helper
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diagnostics import Diagnostics  # noqa: E402
from cut_content import is_cut  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Display-name overrides: game-data name -> public-facing name.
# The game's internal names are sometimes data-mining labels that players
# and staff won't recognise. Map them here so every downstream consumer
# (crafting tab, order log, menu page) sees the familiar name.
DISPLAY_NAME_OVERRIDES: Dict[str, str] = {
    "Slipper Cactus":           "Prickeye",
    "Vegetable Slipper Cactus": "Prickeye",
}

# FNAM_Keywords -> menu category classification.
# These patterns are matched against each extracted keyword EDID.
FOOD_KEYWORD_PATTERNS = (
    re.compile(r"^Meal_Recipe_", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Shared_Food", re.IGNORECASE),
)

CHEM_KEYWORD_PATTERNS = (
    re.compile(r"^RecipeFilter_Chem", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Shared_Healing", re.IGNORECASE),
    re.compile(r"^RecipeFilter_MutationSerum", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Stimpak", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Poison", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Bio", re.IGNORECASE),
    re.compile(r"^RecipeFilter_Brewing", re.IGNORECASE),
)

# BNAM (workbench) EDID patterns — more reliable than FNAM keywords
# because a single workbench uniquely determines the menu category.
FOOD_BENCH_PATTERNS = (
    re.compile(r"Cook", re.IGNORECASE),
    re.compile(r"Brew", re.IGNORECASE),
    re.compile(r"Food", re.IGNORECASE),
)

# Region tokens that appear in FO76 EDIDs. When multiple COBJ recipes share
# the same display name (e.g. "Disease Cure", "Healing Salve") but have
# different region tokens in their EDID, we treat them as region variants
# — one recipe per region — and surface them separately in recipe_variants.
#
# Ordered most-specific first so "CranberryBog" is detected before a
# hypothetical shorter token, and so "TheMire" wins over "Mire" when both
# would match (the game uses both spellings across different items).
REGION_TOKENS: Tuple[Tuple[str, str], ...] = (
    ("CranberryBog",  "Cranberry Bog"),
    ("TheMire",       "The Mire"),
    ("ToxicValley",   "Toxic Valley"),
    ("SavageDivide",  "Savage Divide"),
    ("AshHeap",       "Ash Heap"),
    ("Forest",        "Forest"),
    ("Mire",          "The Mire"),   # fallback spelling
)

CHEM_BENCH_PATTERNS = (
    re.compile(r"Chem", re.IGNORECASE),
    re.compile(r"Pharma", re.IGNORECASE),
    re.compile(r"Medical", re.IGNORECASE),
)


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


def should_skip(edid: str) -> bool:
    """Delegate to the shared cut_content module — single source of truth
    for cut / test / debug / never-released EDIDs."""
    return is_cut(clean_str(edid))


def detect_region(edid: str) -> str:
    """Return the human-readable region name if the EDID contains an FO76
    region token, otherwise "". Used to group region-variant recipes
    (e.g. Disease Cure - Forest, Healing Salve - The Mire).

    We match on a CamelCase-aware trailing boundary only. Leading
    boundaries would need to allow both "_Forest" (post-underscore, as in
    Disease Cure EDIDs) AND "SalveForest" (mid-CamelCase, as in Healing
    Salve EDIDs), which a single lookbehind can't express without
    re-introducing false positives in rare suffix-clash words. Trailing
    boundary (end of string / underscore / uppercase / digit) is
    sufficient in practice because each region token starts with a
    capital letter and is unique enough mid-word ("Mire", "AshHeap",
    "CranberryBog" etc.) that we haven't seen clashes."""
    e = edid or ""
    for token, display in REGION_TOKENS:
        pat = re.compile(re.escape(token) + r"(?=$|[_A-Z0-9])")
        if pat.search(e):
            return display
    return ""


def load_curve_tables(curve_dir: str) -> Dict[str, List[Dict[str, float]]]:
    """Recursively load all curve-table JSONs from *curve_dir*.

    Returns a dict keyed by lowercase filename stem (e.g. "food_1_primary")
    whose value is the sorted list of {x, y} control points.
    """
    curves: Dict[str, List[Dict[str, float]]] = {}
    if not os.path.isdir(curve_dir):
        return curves
    for dirpath, _dirs, files in os.walk(curve_dir):
        for fname in files:
            if not fname.endswith(".json"):
                continue
            stem = fname[:-5].lower()  # strip .json, lowercase
            fpath = os.path.join(dirpath, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                points = data.get("curve", [])
                if points:
                    curves[stem] = sorted(points, key=lambda p: p["x"])
            except Exception:
                pass  # skip malformed files silently
    return curves


def _curv_edid_to_stems(edid: str) -> List[str]:
    """Generate candidate filename stems from a CURV EditorID.

    Handles multiple naming conventions:
      CT_COBJ_Cooking_Food_1_Primary  -> food_1_primary  (strip CT_COBJ_Cooking_)
      COBJ_Brewing_WaterIngredient    -> brewing_wateringredient  (strip COBJ_)
      COBJ_Workshop_Wood              -> cobj_workshop_wood  (exact lowercase)
      COBJ_Ammo_Acid                  -> cobj_ammo_acid
    """
    e = edid.lower()
    candidates = [e]
    if e.startswith("ct_"):
        without_ct = e[3:]
        candidates.append(without_ct)
        # CT_COBJ_Cooking_Food_1_Primary -> strip ct_cobj_{category}_ -> food_1_primary
        parts = without_ct.split("_", 2)
        if len(parts) >= 3:
            candidates.append(parts[2])
    if e.startswith("cobj_"):
        without_cobj = e[5:]
        candidates.append(without_cobj)
        # COBJ_Brewing_WaterIngredient -> strip cobj_{category}_ -> wateringredient
        parts = without_cobj.split("_", 1)
        if len(parts) >= 2:
            candidates.append(parts[1])
    return candidates


def resolve_curve(x: float, points: List[Dict[str, float]]) -> int:
    """Evaluate a curve table at *x* using linear interpolation.

    The game uses integer X values from FVPA Count and the control points
    typically have integer X/Y, so the result is rounded to int.
    Returns 0 if points is empty.
    """
    if not points:
        return 0
    # Clamp to curve bounds
    if x <= points[0]["x"]:
        return round(points[0]["y"])
    if x >= points[-1]["x"]:
        return round(points[-1]["y"])
    # Linear interpolation between bracketing points
    for i in range(len(points) - 1):
        x0, y0 = points[i]["x"], points[i]["y"]
        x1, y1 = points[i + 1]["x"], points[i + 1]["y"]
        if x0 <= x <= x1:
            if x1 == x0:
                return round(y0)
            t = (x - x0) / (x1 - x0)
            return round(y0 + t * (y1 - y0))
    return round(points[-1]["y"])


def parse_fvpa(fvpa_str: str, curves: Optional[Dict[str, List[Dict[str, float]]]] = None) -> Dict[str, int]:
    """Parse FVPA field and resolve ingredient counts via curve tables.

    New format (with curve EDID):
      ComponentEDID:Count:CurveTableEDID|ComponentEDID:Count:CurveTableEDID|...

    Legacy format (no curve EDID):
      ComponentEDID:Count|ComponentEDID:Count|...

    When a CurveTableEDID is present and a matching curve JSON exists,
    the Count (X-axis input) is resolved to the Y-axis output — the
    actual in-game ingredient cost. Otherwise the raw Count is used.
    """
    ingredients: Dict[str, int] = {}
    fvpa_str = clean_str(fvpa_str).strip()
    if not fvpa_str:
        return ingredients
    if curves is None:
        curves = {}
    for part in fvpa_str.split("|"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        fields = part.split(":")
        mat = clean_str(fields[0]).strip()
        try:
            raw_count = int(clean_str(fields[1]).strip())
        except (ValueError, TypeError, IndexError):
            continue

        # Resolve through curve table if CURV EDID is present
        count = raw_count
        curv_edid = clean_str(fields[2]).strip() if len(fields) >= 3 else ""
        if curv_edid and curves:
            for stem in _curv_edid_to_stems(curv_edid):
                if stem in curves:
                    count = resolve_curve(raw_count, curves[stem])
                    break

        if mat and count > 0:
            ingredients[mat] = ingredients.get(mat, 0) + count
    return ingredients


def parse_fnam_keywords(fnam_str: str) -> List[str]:
    """Parse FNAM_Keywords "Name[FormID] | Name[FormID]" -> ["Name", ...]."""
    keywords: List[str] = []
    fnam_str = clean_str(fnam_str).strip()
    if not fnam_str:
        return keywords
    for part in fnam_str.split("|"):
        part = clean_str(part).strip()
        if not part:
            continue
        # strip trailing [FormID]
        part = re.sub(r"\[[0-9A-Fa-f]+\]\s*$", "", part).strip()
        if part:
            keywords.append(part)
    return keywords


def classify_category(keywords: List[str], bnam_edid: str = "") -> str:
    # BNAM wins if it matches — the workbench uniquely identifies the
    # recipe category. Only fall through to keyword-based classification
    # when BNAM is absent (older TSV exports) or ambiguous.
    if bnam_edid:
        for pat in FOOD_BENCH_PATTERNS:
            if pat.search(bnam_edid):
                return "food"
        for pat in CHEM_BENCH_PATTERNS:
            if pat.search(bnam_edid):
                return "chem"

    for kw in keywords:
        for pat in FOOD_KEYWORD_PATTERNS:
            if pat.search(kw):
                return "food"
        for pat in CHEM_KEYWORD_PATTERNS:
            if pat.search(kw):
                return "chem"
    return "other"


def read_tsv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if reader.fieldnames is None:
                print(f"ERROR: {path} has no header row", file=sys.stderr)
                return rows
            rows = [dict(r) for r in reader]
    except Exception as e:
        print(f"ERROR reading {path}: {e}", file=sys.stderr)
    return rows


def find_tsv_files(data_dir: str, pattern: str = "COBJ_Export_*.tsv") -> List[str]:
    return sorted(glob.glob(os.path.join(data_dir, pattern)))


def resolve_edid_column(row: Dict[str, str]) -> str:
    """xEdit export column names drift between months — accept either form."""
    for key in ("COBJ_EDID", "EDID"):
        if key in row and row[key] is not None:
            return row[key]
    return ""


# ---------------------------------------------------------------------------
# Recipe extraction
# ---------------------------------------------------------------------------

def build_recipes(
    per_file_rows: List[Tuple[str, List[Dict[str, str]]]],
    diag: Diagnostics,
    curves: Optional[Dict[str, List[Dict[str, float]]]] = None,
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Extract recipes + metadata from per-file COBJ rows.

    Returns (recipes, meta, variants) where:
      recipes[name]  = {ingredient: count, ...}   (deduped, best pick)
      meta[name]     = {edid, bnam_edid, category, source_file, ...}
      variants[name] = [{region, edid, ingredients, bnam_edid, ...}, ...]

    `variants` is only populated for display names that have 2+ COBJ
    recipes each with a detected region token in their EDID. This powers
    the chef portal's Crafting tab which needs to show e.g. "Disease Cure
    - Forest", "Disease Cure - The Mire" as separate rows even though the
    game's display name for all of them is just "Disease Cure".
    """
    recipes: Dict[str, Dict[str, int]] = {}
    meta: Dict[str, Dict[str, Any]] = {}
    # raw_by_name[name] -> list of (region, edid, ingredients, bnam_edid, category, source_file)
    raw_by_name: Dict[str, List[Dict[str, Any]]] = {}

    total_rows = 0
    total_skipped_cut = 0
    total_no_fvpa = 0
    total_usable = 0

    for source_file, rows in per_file_rows:
        file_usable = 0
        file_total = len(rows)
        total_rows += file_total

        for row in rows:
            edid = resolve_edid_column(row)
            display_name = clean_str(row.get("CNAM_FULL", ""))
            display_name = DISPLAY_NAME_OVERRIDES.get(display_name, display_name)
            fvpa = row.get("FVPA", "")
            fnam = row.get("FNAM_Keywords", "")
            bnam_edid = clean_str(row.get("BNAM_EDID", ""))

            if should_skip(edid):
                total_skipped_cut += 1
                continue

            if not display_name:
                continue

            ingredients = parse_fvpa(fvpa, curves)
            if not ingredients:
                total_no_fvpa += 1
                continue

            file_usable += 1
            total_usable += 1

            keywords = parse_fnam_keywords(fnam)
            category = classify_category(keywords, bnam_edid)

            # Track every recipe under its display name so we can later
            # extract region variants. This is additive to the dedup
            # logic below, not a replacement.
            region = detect_region(edid)
            raw_by_name.setdefault(display_name, []).append({
                "region": region,
                "edid": clean_str(edid),
                "ingredients": ingredients,
                "bnam_edid": bnam_edid,
                "category": category,
                "source_file": os.path.basename(source_file),
            })

            if display_name not in recipes:
                recipes[display_name] = ingredients
                meta[display_name] = {
                    "edid": clean_str(edid),
                    "cnam_edid": clean_str(row.get("CNAM_EDID", "")),
                    "bnam_edid": bnam_edid,
                    "bench_keywords": keywords,
                    "category": category,

                    "source_file": os.path.basename(source_file),
                }
            else:
                # Prefer the entry with the most ingredients (most-specific recipe)
                if len(ingredients) > len(recipes[display_name]):
                    recipes[display_name] = ingredients
                    meta[display_name] = {
                        "edid": clean_str(edid),
                        "cnam_edid": clean_str(row.get("CNAM_EDID", "")),
                        "bnam_edid": bnam_edid,
                        "bench_keywords": keywords,
                        "category": category,
    
                        "source_file": os.path.basename(source_file),
                    }
                else:
                    # Back-fill keywords / BNAM / category if earlier entry lacked them
                    existing = meta.get(display_name, {})
                    if not existing.get("bench_keywords") and keywords:
                        existing["bench_keywords"] = keywords
                    if not existing.get("bnam_edid") and bnam_edid:
                        existing["bnam_edid"] = bnam_edid
                    # Re-classify if we now have a BNAM we didn't before
                    if existing.get("category") == "other" and (bnam_edid or keywords):
                        existing["category"] = classify_category(
                            existing.get("bench_keywords") or keywords,
                            existing.get("bnam_edid") or bnam_edid,
                        )

        # Diagnostic: a TSV with zero usable rows is almost always a broken export
        if file_total > 0 and file_usable == 0:
            diag.error(
                "cobj.tsv.empty_export",
                "COBJ TSV contains no usable recipes — likely a broken or empty xEdit export.",
                detail=f"{os.path.basename(source_file)} has {file_total} rows but 0 with ingredients.",
                context={"file": os.path.basename(source_file), "rows": file_total},
            )
        elif file_total > 0 and file_usable < max(10, file_total // 50):
            diag.warning(
                "cobj.tsv.low_yield",
                "COBJ TSV produced unusually few recipes — check xEdit export columns.",
                detail=f"{os.path.basename(source_file)}: {file_usable}/{file_total} rows usable.",
                context={"file": os.path.basename(source_file), "rows": file_total, "usable": file_usable},
            )

    # ── Extract region-variant recipes ─────────────────────────────────
    # A "region variant" is a display name that has 2+ COBJ recipes, each
    # with a distinct region token in its EDID. Disease Cure and Healing
    # Salve are the two current cases — each has one recipe per FO76
    # region (Forest / Toxic Valley / Savage Divide / Ash Heap / The Mire
    # / Cranberry Bog), and sometimes two per region (chem lab + cooking
    # bench). We de-dupe on (region, ingredient_set) so we don't emit
    # two identical chem-lab-vs-cooking-lab rows for the same region;
    # the chef only needs to know which ingredients a region requires.
    variants: Dict[str, List[Dict[str, Any]]] = {}
    for display_name, entries in raw_by_name.items():
        region_entries = [e for e in entries if e["region"]]
        if len(region_entries) < 2:
            continue
        regions_seen = {e["region"] for e in region_entries}
        if len(regions_seen) < 2:
            # all "variants" are in the same region (e.g. two workbench
            # variants of one regional recipe) — not a true region split
            continue
        # Collapse duplicates per region — keep the entry with the most
        # ingredients so the most-specific recipe wins, matching the
        # dedup rule used above for `recipes`.
        by_region: Dict[str, Dict[str, Any]] = {}
        for e in region_entries:
            existing = by_region.get(e["region"])
            if existing is None or len(e["ingredients"]) > len(existing["ingredients"]):
                by_region[e["region"]] = e
        variants[display_name] = [
            by_region[r] for r in sorted(by_region.keys())
        ]

    # ── Detect ingredient alternates ───────────────────────────────────
    # An "ingredient alternates" pattern is when 2+ COBJ records share the
    # same display name AND the same recipe shape EXCEPT for ONE varying
    # ingredient slot. Canonical case: "Fish Bits" has 57 COBJ records,
    # each consuming a different fish species (Leatherback, Potbelly Kelt,
    # Alpine Sawgill, ...) but all producing the same Fish_Fishbits output.
    # In-game the player can use ANY of those fish to craft Fish Bits.
    #
    # The existing dedup logic picks one arbitrary winner, which is fine
    # for `recipes` (where we just need a representative recipe), but
    # downstream consumers (farming planner, chef portal) need the full
    # alternates list so they can display "Any Fish" or similar rather
    # than the misleading "Small Raw Savage Leatherback".
    #
    # To avoid mixing legacy/dirty entries from older TSV exports
    # (e.g. older months where ingredient slots used display names like
    # "Static Sawgill" instead of FormID-backed EDIDs), alternates are
    # detected only against entries from the SINGLE most recent source
    # file. We rank source files by parsing month-year tokens from the
    # filename so chronology — not alphabetic order — wins.
    _MONTH_TOKEN = {
        "Jan": 1, "Feb": 2, "March": 3, "Apr": 4, "April": 4,
        "May": 5, "June": 6, "July": 7, "Aug": 8, "Sept": 9,
        "Oct": 10, "Nov": 11, "Dec": 12,
    }
    def _source_file_rank(fname: str) -> Tuple[int, int]:
        m = re.search(r"_([A-Za-z]+)_(\d{4})", fname)
        if not m:
            return (0, 0)
        return (int(m.group(2)), _MONTH_TOKEN.get(m.group(1), 0))

    latest_basename = ""
    if per_file_rows:
        latest_basename = os.path.basename(
            max((sf for sf, _ in per_file_rows), key=_source_file_rank)
        )

    def _derive_alternates_label(edids: List[str]) -> str:
        """Heuristic label for an alternates group based on edid patterns.
        Returns "" if no pattern matches; downstream consumers should fall
        back to a generic "Any of N options" label in that case.
        Order matters — most specific patterns first."""
        if not edids:
            return ""
        # Seasonal Fish — most specific, check before generic Fish.
        if all(re.match(r"^SeasonalFish_Meal_", e, re.IGNORECASE) for e in edids):
            return "Seasonal Fish"
        # Fish: any prefix that contains "_Fish_Meal_" — covers Fishing_*,
        # Burn_*, and any future region prefix the game adds for fish.
        if all(re.search(r"Fish_Meal_", e) for e in edids):
            return "Fish"
        # Legendary shards (tier 2/3 variants of the same SPECIAL stat)
        if all(re.match(r"^LegendaryShard_", e, re.IGNORECASE) for e in edids):
            return "Legendary Shard"
        return ""

    alternates_count = 0
    for display_name, entries in raw_by_name.items():
        # Use only entries from the latest source file (avoid legacy noise)
        scope = [e for e in entries if e.get("source_file") == latest_basename]
        if len(scope) < 2:
            continue
        all_sets = [set(e["ingredients"].keys()) for e in scope]
        common = set.intersection(*all_sets)
        # Each entry must have all common ingredients + exactly one varying
        if not all(
            (set(e["ingredients"].keys()) & common) == common
            and len(set(e["ingredients"].keys()) - common) == 1
            for e in scope
        ):
            continue
        varying = set()
        varying_qty = None
        consistent_qty = True
        for e in scope:
            diff = set(e["ingredients"].keys()) - common
            for edid in diff:
                varying.add(edid)
                q = e["ingredients"][edid]
                if varying_qty is None:
                    varying_qty = q
                elif varying_qty != q:
                    consistent_qty = False
        if len(varying) < 2 or not consistent_qty:
            continue
        # Skip if the recipe entry we kept doesn't actually contain one of
        # the alternates (defensive — should always match in practice)
        recipe = recipes.get(display_name) or {}
        if not any(edid in recipe for edid in varying):
            continue
        alt_list = sorted(varying)
        meta.setdefault(display_name, {})["alternates"] = {
            "label": _derive_alternates_label(alt_list),
            "qty": varying_qty,
            "ingredients": alt_list,
        }
        alternates_count += 1

    print(f"Recipes extracted: {len(recipes)}", file=sys.stderr)
    print(f"Region variants: {len(variants)} items with per-region recipes", file=sys.stderr)
    print(f"Ingredient alternates: {alternates_count} items with alternate-ingredient slots", file=sys.stderr)
    print(f"Skipped (cut content): {total_skipped_cut}", file=sys.stderr)
    print(f"Rows without ingredients: {total_no_fvpa}", file=sys.stderr)

    diag.info(
        "cobj.build.summary",
        f"Extracted {len(recipes)} unique recipes from {total_rows} COBJ rows.",
        context={
            "unique_recipes": len(recipes),
            "region_variant_items": len(variants),
            "ingredient_alternates": alternates_count,
            "total_rows": total_rows,
            "skipped_cut": total_skipped_cut,
            "no_fvpa": total_no_fvpa,
        },
    )

    return recipes, meta, variants


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build cobj-recipes.json from COBJ TSV export")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV input dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
    parser.add_argument(
        "--curve-dir", type=str,
        default=os.path.join("data", "curvetables", "json"),
        help="Curve-table JSON root dir (default: data/curvetables/json)",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    diag = Diagnostics(source="cobj_recipes", outdir=args.outdir)

    tsv_files = find_tsv_files(args.data_dir, "COBJ_Export_*.tsv")
    if not tsv_files:
        diag.error(
            "cobj.tsv.none_found",
            "No COBJ_Export_*.tsv files were found.",
            detail=f"Searched {args.data_dir}",
        )
        diag.save()
        print(f"ERROR: No COBJ_Export_*.tsv files found in {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(tsv_files)} COBJ TSV file(s)", file=sys.stderr)

    per_file_rows: List[Tuple[str, List[Dict[str, str]]]] = []
    for tsv_path in tsv_files:
        print(f"Reading {os.path.basename(tsv_path)}...", file=sys.stderr)
        rows = read_tsv(tsv_path)
        per_file_rows.append((tsv_path, rows))
        print(f"  Loaded {len(rows)} rows", file=sys.stderr)

    total_rows = sum(len(r) for _, r in per_file_rows)
    print(f"Total rows: {total_rows}", file=sys.stderr)

    # Load curve-table JSONs for resolving FVPA counts
    curves = load_curve_tables(args.curve_dir)
    if curves:
        print(f"Loaded {len(curves)} curve table(s) from {args.curve_dir}", file=sys.stderr)
    else:
        print(f"WARNING: No curve tables found in {args.curve_dir} — raw FVPA counts will be used", file=sys.stderr)

    recipes, meta, variants = build_recipes(per_file_rows, diag, curves)

    output: Dict[str, Any] = {
        "version": today_ymd(),
        "generated": now_iso(),
        "count": len(recipes),
        "recipes": recipes,
        "recipe_meta": meta,
        # Region-variant recipes for items whose display name collapses
        # multiple COBJ records (Disease Cure, Healing Salve). Consumers
        # (chef portal's Crafting tab) should render one row per region
        # when an item appears in recipe_variants.
        "recipe_variants": variants,
    }

    output_path = os.path.join(args.outdir, "cobj-recipes.json")

    # Atomic write + round-trip validation.
    #
    # The dist/cobj-recipes.json shipped in April 2026 was truncated mid-
    # recipe_meta entry (file ended at "ATX_workshop_LL_Structure_Taggerdys_
    # Roofs": {). That can only happen if json.dump was interrupted after
    # opening the file in 'w' mode — the file is zeroed on open, then
    # partially populated, then the process dies, and a broken artifact
    # gets committed. The browser's JSON.parse then throws and the chef
    # portal's Crafting tab silently receives an empty recipes dict.
    #
    # Fix pattern:
    #   1. Serialize the full payload to a string first. If the payload
    #      has a circular reference or a non-serialisable type, the error
    #      happens here with zero files touched.
    #   2. Write to a same-directory temp file and fsync before rename.
    #      os.replace is atomic on every supported platform — the
    #      destination either points at the old file or the new one,
    #      never a half-written one.
    #   3. Re-parse the destination with json.loads as a sanity check
    #      before claiming success. If it can't round-trip we leave the
    #      tempfile as *.json.bad for inspection and error out loud.
    try:
        payload = json.dumps(output, indent=2, ensure_ascii=False)
    except Exception as e:
        diag.error("cobj.serialize.failed", "Failed to serialise cobj-recipes payload", detail=str(e))
        diag.save()
        print(f"ERROR serialising {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix="cobj-recipes.",
        suffix=".json.tmp",
        dir=args.outdir,
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except (OSError, AttributeError):
                # fsync may not be available on every FS (e.g. a network
                # share inside a GH Actions windows runner). It's best-
                # effort; os.replace below is still atomic.
                pass
    except Exception as e:
        # Writing the temp file itself failed — drop it and bail.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        diag.error("cobj.write.failed", "Failed to write cobj-recipes temp file", detail=str(e))
        diag.save()
        print(f"ERROR writing tempfile for {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Round-trip verification — re-parse the tempfile so a silently
    # truncated write can't reach dist/.
    try:
        with open(tmp_path, "r", encoding="utf-8") as f:
            reparsed = json.load(f)
        if not isinstance(reparsed, dict) or "recipes" not in reparsed:
            raise ValueError("re-parsed JSON missing 'recipes' key")
        if len(reparsed.get("recipes", {})) != len(recipes):
            raise ValueError(
                f"recipe count mismatch: wrote {len(recipes)}, reparsed {len(reparsed.get('recipes', {}))}"
            )
    except Exception as e:
        # Preserve the broken file for post-mortem rather than leaving
        # dist/cobj-recipes.json in its previous (possibly also broken)
        # state silently.
        bad_path = output_path + ".bad"
        try:
            os.replace(tmp_path, bad_path)
        except OSError:
            pass
        diag.error(
            "cobj.write.roundtrip_failed",
            "Wrote cobj-recipes temp file but re-parse failed — refusing to publish.",
            detail=f"{e}; bad file kept at {os.path.basename(bad_path)}",
        )
        diag.save()
        print(f"ERROR round-trip validating {tmp_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # All checks passed — publish atomically.
    try:
        os.replace(tmp_path, output_path)
        print(f"Wrote {output_path}", file=sys.stderr)
    except Exception as e:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        diag.error("cobj.publish.failed", "Failed to replace cobj-recipes.json", detail=str(e))
        diag.save()
        print(f"ERROR publishing {output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    diag.save()
    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
