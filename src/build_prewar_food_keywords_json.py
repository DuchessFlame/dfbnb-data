#!/usr/bin/env python3
"""
build_prewar_food_keywords_json.py
==================================
Builds the Pre-War Food Keyword Index for
/df/score-challenges/pre-war-food-keyword-index/.

Score challenges like "Eat Pre-war Food" and "Collect Pre-war Food" don't go by
what an item looks like or where it came from. They test the item's keywords:
it counts if it carries MealTypePackaged ("Pre-war Food") OR MealTypeDogfood.
Both are OR'd on every Pre-war Food challenge (condition flag 10010000), which
is why Canned Dog Food counts. This script lists every consumable that carries
either keyword, and every live challenge that asks for Pre-war Food.

Sibling of build_costume_keywords_json.py -- same output shape, same renderer
pattern, same cut rule.

Reads from (channel-explicit -- a --pts build reads tsv/pts/ only):
  - ALCH_Export_*.tsv     Keywords_Flat per consumable, and its FULL name.
                          (The *_Effects.tsv sibling is not needed.)
  - CHAL_Export_*.tsv     Cond1..CondN, for the "Used For" challenge list.

Outputs:
  - dist/prewar_food_keywords.json        (live)
  - dist/pts/prewar_food_keywords.json    (--pts)

Output shape:
  {
    "keyword": "MealTypePackaged",
    "keywords": ["MealTypePackaged", "MealTypeDogfood"],
    "challenges": [ { "type": "Daily", "text": "Eat Pre-war Food", "epic": false }, ... ],
    "items": [ { "name": "Cram", "type": "Food" }, ... ],
    "typeCounts": { "Food": N, "Candy": N, ... },
    "_meta": { ... }
  }

Items are A-Z by name, one row per display name. Challenges are grouped
Daily / Weekly / Event / Lifetime, A-Z inside each.

No external dependencies -- stdlib only.

Usage:  python src/build_prewar_food_keywords_json.py
PTS:    python src/build_prewar_food_keywords_json.py --pts
"""

import csv
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_spin_wheel_json as spin  # noqa: E402  (shared is_cut() -- one cut rule site-wide)
import tsv_source                     # noqa: E402  (one resolver for every export selection)

ROOT = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
OUT_DIR = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")
OUT_NAME = "prewar_food_keywords.json"

# The keyword that DEFINES a Pre-war Food challenge (Used For keys off this one
# only -- "Eat Dog Food" tests MealTypeDogfood alone and is not a Pre-war Food
# challenge), and every keyword those challenges accept for an item.
KEYWORD = "MealTypePackaged"
ITEM_KEYWORDS = ("MealTypePackaged", "MealTypeDogfood")

csv.field_size_limit(10**9)


def read_tsv(path):
    """xEdit writes Windows-1252 and the PTS pull is not always re-encoded.
    UTF-8 first, cp1252 fallback -- only names are affected."""
    raw = open(path, "rb").read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"))


# ─────────────────────────────────────────────────────────────────────────────
#  Which records are real items
# ─────────────────────────────────────────────────────────────────────────────
# On top of the shared cut rule (CUT_/ZZZ_/DEL_/TEST_...): survival-mode
# templates (AAA_SURV_TEMPLATE_*) carry the keyword but are never an item.
_NOT_ITEM_EDID = [
    re.compile(r"^AAA_", re.I),
    re.compile(r"TEMPLATE", re.I),
    re.compile(r"_TEST(?:_|$)", re.I),
]

# Display names that are not a player item even though the record is clean.
_SKIP_NAMES = set()


def is_not_item(edid):
    return any(p.search(edid or "") for p in _NOT_ITEM_EDID)


# ─────────────────────────────────────────────────────────────────────────────
#  The pill: Dog Food / Candy / Cakes & Pies / Drink / Food
# ─────────────────────────────────────────────────────────────────────────────
# Straight off the item's own keywords, first match wins. Bubblegum carries
# ObjectTypeCandy too, so it reads as Candy. _TYPE_OVERRIDE fixes a single item
# by display name if the rule reads it wrong.
_TYPE_RULES = [
    ("Dog Food",     "MealTypeDogfood"),
    ("Candy",        "ObjectTypeCandy"),
    ("Cakes & Pies", "ObjectTypeCakesPies"),
    ("Drink",        "ObjectTypeDrink"),
]
_TYPE_OVERRIDE = {
    # "Item Name": "Drink",
}
TYPE_ORDER = ["Food", "Candy", "Cakes & Pies", "Drink", "Dog Food"]

_KW_TOKEN = re.compile(r"([A-Za-z0-9_]+)\s*\[[0-9A-Fa-f]{8}\]")


def parse_keywords(flat):
    """'ObjectTypeFood[00055ECC] | MealTypePackaged[0013485D]' -> {EDIDs}."""
    return set(_KW_TOKEN.findall(flat or ""))


def item_type(name, kws):
    if name in _TYPE_OVERRIDE:
        return _TYPE_OVERRIDE[name]
    for label, kw in _TYPE_RULES:
        if kw in kws:
            return label
    return "Food"


def collect_items():
    alch_path = tsv_source.newest("ALCH_Export_*.tsv", channel=CHANNEL, exclude="_Effects")
    print(f"  Reading consumables from: {os.path.basename(alch_path)}")

    by_name = {}
    dropped = Counter()
    tagged = 0
    for row in read_tsv(alch_path):
        kws = parse_keywords(row.get("Keywords_Flat"))
        if not kws & set(ITEM_KEYWORDS):
            continue
        tagged += 1
        name = (row.get("FULL") or "").strip()
        edid = (row.get("ALCH_EDID") or "").strip()
        if not name or name.upper() == "NONE":
            dropped["no name"] += 1
            continue
        if spin.is_cut(edid):
            dropped["cut"] += 1
            continue
        if is_not_item(edid):
            dropped["template"] += 1
            continue
        if name in _SKIP_NAMES:
            dropped["skip name"] += 1
            continue
        by_name.setdefault(name, []).append(item_type(name, kws))

    print(f"  ALCH records carrying {' or '.join(ITEM_KEYWORDS)}: {tagged}")
    print(f"  Dropped: {dict(dropped)}")
    items = []
    for name, types in by_name.items():
        # Same display name on several records (pre-war clean copies, event
        # copies). One item to a player; the commonest reading wins.
        items.append({"name": name, "type": Counter(types).most_common(1)[0][0]})
    items.sort(key=lambda it: it["name"].lower())
    print(f"  Pre-war Food items (unique names): {len(items)}")
    return items, os.path.basename(alch_path)


# ─────────────────────────────────────────────────────────────────────────────
#  Used For -- the live challenges that test for Pre-war Food
# ─────────────────────────────────────────────────────────────────────────────
#   Cond1 : 10010000|1.000000|HasKeyword|00 00 00|00 00|
#           MealTypePackaged "Pre-war Food" [KYWD:0013485D]|...
#
# Full-token match so a longer keyword starting with the same text can't answer.
_COND_RE = re.compile(r"\|" + KEYWORD + r"(?![A-Za-z0-9_])")
_EPIC_RE = re.compile(r"^\s*epic\s*-\s*", re.I)
CHAL_TYPE_ORDER = ["Daily", "Weekly", "Event", "Lifetime"]


def collect_challenges():
    path = tsv_source.newest("CHAL_Export_*.tsv", channel=CHANNEL, required=False)
    if not path:
        print("  [WARN] No CHAL_Export_*.tsv found -- Used For omitted", file=sys.stderr)
        return [], None
    print(f"  Reading challenges from: {os.path.basename(path)}")

    out, seen = [], set()
    for row in read_tsv(path):
        edid = (row.get("EDID") or "").strip()
        text = (row.get("FULL") or "").strip()
        if not text or text.upper() == "NONE" or spin.is_cut(edid):
            continue
        hit = any(
            col and col.startswith("Cond") and col != "CondCount" and val and _COND_RE.search(val)
            for col, val in row.items()
        )
        if not hit:
            continue
        ctype = (row.get("CNAM") or "").strip() or "Other"
        epic = bool(_EPIC_RE.match(text))
        clean = _EPIC_RE.sub("", text).strip()
        sig = (ctype, clean.lower(), epic)
        if sig in seen:
            continue
        seen.add(sig)
        out.append({"type": ctype, "text": clean, "epic": epic})

    order = {t: i for i, t in enumerate(CHAL_TYPE_ORDER)}
    out.sort(key=lambda c: (order.get(c["type"], len(order)), c["text"].lower(), c["epic"]))
    print(f"  Challenges using {KEYWORD}: {len(out)}")
    return out, os.path.basename(path)


def main():
    print("=" * 60)
    print("  Building Pre-War Food Keyword Index JSON")
    print(f"  Mode: {CHANNEL.upper()}")
    print("=" * 60)

    items, source = collect_items()
    challenges, chal_source = collect_challenges()

    if len(items) < 20:
        # A healthy export has ~35. Never publish a near-empty page over a good one.
        print(f"ERROR: only {len(items)} pre-war food items found -- not writing", file=sys.stderr)
        return 1

    counts = Counter(it["type"] for it in items)
    output = {
        "keyword": KEYWORD,
        "keywords": list(ITEM_KEYWORDS),
        "challenges": challenges,
        "items": items,
        "typeCounts": {t: counts[t] for t in TYPE_ORDER if counts.get(t)},
        "_meta": {
            "built": date.today().isoformat(),
            "channel": CHANNEL,
            "source": source,
            "challengeSource": chal_source,
            "itemCount": len(items),
            "challengeCount": len(challenges),
        },
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, OUT_NAME)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n  Written to: {out_path}")
    print(f"  Items: {len(items)}  {dict(output['typeCounts'])}")
    print(f"  Challenges: {len(challenges)}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
