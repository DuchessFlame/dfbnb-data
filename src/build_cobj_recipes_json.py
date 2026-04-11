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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")

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
    e = clean_str(edid).upper()
    return any(e.startswith(p) for p in CUT_PREFIXES)


def parse_fvpa(fvpa_str: str) -> Dict[str, int]:
    """Parse FVPA field "Material:Count | Material:Count | ..." -> {mat: count}."""
    ingredients: Dict[str, int] = {}
    fvpa_str = clean_str(fvpa_str).strip()
    if not fvpa_str:
        return ingredients
    for part in fvpa_str.split("|"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        mat, cnt = part.split(":", 1)
        mat = clean_str(mat).strip()
        try:
            count = int(clean_str(cnt).strip())
        except (ValueError, TypeError):
            continue
        if mat:
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
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, Any]]]:
    """Extract recipes + metadata from per-file COBJ rows."""
    recipes: Dict[str, Dict[str, int]] = {}
    meta: Dict[str, Dict[str, Any]] = {}

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
            fvpa = row.get("FVPA", "")
            fnam = row.get("FNAM_Keywords", "")
            bnam_edid = clean_str(row.get("BNAM_EDID", ""))

            if should_skip(edid):
                total_skipped_cut += 1
                continue

            if not display_name:
                continue

            ingredients = parse_fvpa(fvpa)
            if not ingredients:
                total_no_fvpa += 1
                continue

            file_usable += 1
            total_usable += 1

            keywords = parse_fnam_keywords(fnam)
            category = classify_category(keywords, bnam_edid)

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

    print(f"Recipes extracted: {len(recipes)}", file=sys.stderr)
    print(f"Skipped (cut content): {total_skipped_cut}", file=sys.stderr)
    print(f"Rows without ingredients: {total_no_fvpa}", file=sys.stderr)

    diag.info(
        "cobj.build.summary",
        f"Extracted {len(recipes)} unique recipes from {total_rows} COBJ rows.",
        context={
            "unique_recipes": len(recipes),
            "total_rows": total_rows,
            "skipped_cut": total_skipped_cut,
            "no_fvpa": total_no_fvpa,
        },
    )

    return recipes, meta


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build cobj-recipes.json from COBJ TSV export")
    parser.add_argument("--data-dir", type=str, default="tsv", help="TSV input dir (default: tsv)")
    parser.add_argument("--outdir", type=str, default="dist", help="Output dir (default: dist)")
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

    recipes, meta = build_recipes(per_file_rows, diag)

    output: Dict[str, Any] = {
        "version": today_ymd(),
        "generated": now_iso(),
        "count": len(recipes),
        "recipes": recipes,
        "recipe_meta": meta,
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
