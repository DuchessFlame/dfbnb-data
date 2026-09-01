#!/usr/bin/env python3
"""
build_best_build_tags_json.py
=============================
Builds dist/best-build-tags.json — the full list of C.A.M.P. Best Builds tag
options, for the "Tagging Your Best Build" section of
/df/camp/best-builds/ (rendered by df-bnb-camp-items.js).

WHY THIS EXISTS
---------------
The tag list used to be a hardcoded BEST_BUILD_TAGS array inside
df-bnb-camp-items.js. Bethesda adds tags without announcing them, so the baked
list drifted: as of the July 2026 export the game had BUILDINGS/HOTEL and
THEMES/FISHING that the page had never shown. Reading the game data instead
means a new tag appears on the page the next time an export lands.

SOURCE OF TRUTH
---------------
Two record types, both from the same xEdit export month:

  KYWD  BestBuilds_KeywordTag_<Category>_<Name>   — one record per tag.
        FULL_Name carries the in-game label, in caps ("CRANBERRY BOG").

  FLST  BestBuildsTagCategories                   — master list. Its entry
        order IS the category order.
        BestBuildsTagCategories_<Category>        — one per category. Its entry
        order IS the in-game tag order, which is why NUMBERS comes out
        Zero → One → Two → … → Hundred and not alphabetically.

Nothing here sorts. Both orders are read straight off the FLSTs, so if Bethesda
reorders or inserts a category the page follows without a code change.

  * BestBuilds_KeywordTag_Generic_Dummy is a placeholder with no FULL_Name. The
    game itself leaves it out of CampTagsFormList (363 entries vs 364 KYWDs),
    so it is dropped here too.

Inputs:
  - tsv/KYWD_Export_*.tsv                (tag keywords)
  - tsv/FLST_Export_*_List.tsv           (category counts + names)
  - tsv/FLST_Export_*_Entries.tsv        (membership + order)

Output:
  - dist/best-build-tags.json

Usage:
  python src/build_best_build_tags_json.py
  python src/build_best_build_tags_json.py --tsv-root tsv --outdir dist
  python src/build_best_build_tags_json.py --channel pts
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

import tsv_source

# ---------------------------------------------------------------------------
# EDID prefixes
# ---------------------------------------------------------------------------

KEYWORD_PREFIX  = "BestBuilds_KeywordTag_"
CATEGORY_MASTER = "BestBuildsTagCategories"
CATEGORY_PREFIX = "BestBuildsTagCategories_"

# Placeholder keyword the game keeps out of CampTagsFormList. No FULL_Name.
GENERIC_EDID = "BestBuilds_KeywordTag_Generic_Dummy"

# ---------------------------------------------------------------------------
# Display names
# ---------------------------------------------------------------------------
# The game stores labels in caps. `display` is what the page prints. Everything
# not listed here goes through title_case() below.
#
# Three reasons a tag lands in this map, and they are worth keeping separate:
#   1. Bethesda typo — the game data is misspelled. Corrected for display only;
#      `name` keeps the real value so a future fix in the game files is visible
#      as a diff instead of being silently swallowed.
#   2. Casing title_case() cannot infer (internal capitals).
#   3. DF house spelling, to match the rest of the site.

DISPLAY_OVERRIDES = {
    # 1. Bethesda typos
    "CHARTEUSE":       "Chartreuse",      # Colors
    "FOURTY":          "Forty",           # Numbers
    "PAVILLION":       "Pavilion",        # Buildings
    # 2. Internal capitals
    "CAMP McCLINTOCK": "Camp McClintock",  # Locations
    "ROBCO":           "RobCo",            # Themes
    # 3. DF house spelling
    "COLORED":         "Coloured",         # Themes
}

# Same idea for the category headings.
CATEGORY_DISPLAY_OVERRIDES = {
    "COLORS": "Colours",
}

# Category order comes from the BestBuildsTagCategories FLST, so a category
# Bethesda adds slots itself in. To pin a hand-picked order instead — the page
# ran shortest-list-first (Biomes, Weathers, Numbers, Materials, Colours,
# Habitats, Locations, Themes, Buildings) until Sept 2026 — list the raw caps
# names here. Anything the game has that isn't listed is appended, so a new
# category still shows up rather than vanishing.
CATEGORY_ORDER: list[str] = []

# Words that stay lowercase inside a multi-word label. "ON" in
# "NUKA-WORLD ON TOUR" is the only live case, but the page has always shown it
# capitalised, so the set is empty by design — kept as the hook for when a
# genuinely lowercase connector shows up.
LOWERCASE_WORDS: set[str] = set()


def title_case(raw: str) -> str:
    """ALL CAPS game label -> the page's Title Case.

    Splits on spaces and hyphens and capitalises each part, so
    "NUKA-WORLD ON TOUR" -> "Nuka-World On Tour" and "VAULT 76" -> "Vault 76".
    Parts that already carry internal capitals (McCLINTOCK) can't be inferred —
    those go in DISPLAY_OVERRIDES.
    """
    words = []
    for i, word in enumerate(raw.split(" ")):
        if i and word.lower() in LOWERCASE_WORDS:
            words.append(word.lower())
            continue
        words.append("-".join(p[:1].upper() + p[1:].lower() for p in word.split("-")))
    return " ".join(words)


def display_for(raw: str) -> str:
    return DISPLAY_OVERRIDES.get(raw, title_case(raw))


def slugify(raw: str) -> str:
    out = []
    for ch in raw.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).strip("-")


# ---------------------------------------------------------------------------
# TSV reading
# ---------------------------------------------------------------------------

def read_rows(path: str) -> list[dict]:
    """Read a tab-separated xEdit export.

    errors="replace" matches the rest of src/ — some exports carry stray cp1252
    bytes (curly apostrophes in unrelated columns) that would otherwise abort
    the whole read over a field this builder never looks at.
    """
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Build dist/best-build-tags.json")
    ap.add_argument("--tsv-root", default=None,
                    help="TSV directory (default: resolved by tsv_source)")
    ap.add_argument("--outdir", default="dist")
    ap.add_argument("--channel", default="live", choices=["live", "pts"])
    args = ap.parse_args()

    def newest(pattern, **kw):
        if args.tsv_root:
            return tsv_source.newest(os.path.join(args.tsv_root, pattern), **kw)
        return tsv_source.newest(pattern, channel=args.channel, **kw)

    kywd_path    = newest("KYWD_Export_*.tsv", exclude="Refs")
    entries_path = newest("FLST_Export_*_Entries.tsv")
    list_path    = newest("FLST_Export_*_List.tsv")

    # ---- keywords: FormID -> {editorId, name} ------------------------------
    keywords: dict[str, dict] = {}
    for row in read_rows(kywd_path):
        edid = (row.get("EDID") or "").strip()
        if not edid.startswith(KEYWORD_PREFIX) or edid == GENERIC_EDID:
            continue
        keywords[(row.get("FormID") or "").strip().upper()] = {
            "editorId": edid,
            "name":     (row.get("FULL_Name") or "").strip(),
        }

    if not keywords:
        print(f"[ERROR] No {KEYWORD_PREFIX}* records in {kywd_path}", file=sys.stderr)
        return 1

    # ---- FLST membership: EDID -> [(index, entryFormID)] -------------------
    members: dict[str, list[tuple[int, str]]] = {}
    for row in read_rows(entries_path):
        edid = (row.get("FLST_EDID") or "").strip()
        if edid != CATEGORY_MASTER and not edid.startswith(CATEGORY_PREFIX):
            continue
        try:
            idx = int(row.get("EntryIndex") or 0)
        except ValueError:
            idx = 0
        members.setdefault(edid, []).append(
            (idx, (row.get("Entry_FormID") or "").strip().upper())
        )
    for v in members.values():
        v.sort(key=lambda t: t[0])

    if CATEGORY_MASTER not in members:
        print(f"[ERROR] {CATEGORY_MASTER} FLST not found in {entries_path}",
              file=sys.stderr)
        return 1

    # ---- FLST headers: FormID -> {edid, full, count} -----------------------
    flst_by_id: dict[str, dict] = {}
    for row in read_rows(list_path):
        flst_by_id[(row.get("FLST_FormID") or "").strip().upper()] = {
            "edid":  (row.get("FLST_EDID") or "").strip(),
            "full":  (row.get("FLST_FULL") or "").strip(),
            "count": (row.get("EntryCount") or "").strip(),
        }

    # ---- build, in master-FLST order ---------------------------------------
    categories = []
    unknown_tags = []
    for _, cat_form_id in members[CATEGORY_MASTER]:
        head = flst_by_id.get(cat_form_id)
        if not head:
            print(f"[WARN]  Category {cat_form_id} in {CATEGORY_MASTER} has no "
                  f"FLST header row — skipped.", file=sys.stderr)
            continue

        cat_edid = head["edid"]
        cat_name = head["full"] or cat_edid.replace(CATEGORY_PREFIX, "").upper()

        tags = []
        for _, tag_form_id in members.get(cat_edid, []):
            kw = keywords.get(tag_form_id)
            if not kw:
                unknown_tags.append((cat_name, tag_form_id))
                continue
            tags.append({
                "formId":   tag_form_id,
                "editorId": kw["editorId"],
                "name":     kw["name"],
                "display":  display_for(kw["name"]),
            })

        categories.append({
            "id":          slugify(cat_name),
            "formId":      cat_form_id,
            "editorId":    cat_edid,
            "name":        cat_name,
            "displayName": CATEGORY_DISPLAY_OVERRIDES.get(cat_name, title_case(cat_name)),
            "count":       len(tags),
            "tags":        tags,
        })

    if CATEGORY_ORDER:
        pinned = {n: i for i, n in enumerate(CATEGORY_ORDER)}
        categories.sort(key=lambda c: pinned.get(c["name"], len(pinned)))

    for cat_name, form_id in unknown_tags:
        print(f"[WARN]  {cat_name}: entry {form_id} has no BestBuilds keyword "
              f"record — skipped.", file=sys.stderr)

    total = sum(c["count"] for c in categories)

    payload = {
        "generated":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "channel":       args.channel,
        "source": {
            "keywords":   os.path.basename(kywd_path),
            "flstList":   os.path.basename(list_path),
            "flstEntries": os.path.basename(entries_path),
        },
        "categoryCount": len(categories),
        "tagCount":      total,
        "categories":    categories,
    }

    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, "best-build-tags.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"[build_best_build_tags_json] Wrote {out_path} — "
          f"{len(categories)} categories, {total} tags "
          f"(from {os.path.basename(kywd_path)}).", file=sys.stderr)
    for c in categories:
        print(f"    {c['displayName']:<12} {c['count']:>3}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
