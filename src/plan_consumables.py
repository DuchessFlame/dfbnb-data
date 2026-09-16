#!/usr/bin/env python3
"""
plan_consumables.py — the Recipe plan checklist, which is meant to be food,
drink, alcohol and chems and nothing else.

WHAT WAS WRONG
--------------
plan_master's `recipe` bucket is where the builder puts anything it cannot file
elsewhere. Of its 1,197 live rows only 155 are actually consumable recipes; the
other 1,042 are CAMP furniture, weapon paints, backpacks and outfits. The page
was a 1,200-row dumping ground with the food buried in it.

WHAT DECIDES
------------
The record the plan's recipe CREATES. `cnam.sig == "ALCH"` is the whole test:
ALCH is the game's consumable record, so a plan that makes one is a food, drink
or chem recipe and a plan that does not, is not. No name matching, no EditorID
guessing — a "Recipe: " prefix appears on beer steins too.

The groups then come from the ALCH record's own keywords, which is why they can
be trusted: ObjectTypeFood / Drink / Chem / Serum for the root, and
MealType* / DrinkType* / ObjectType* for the sub-group. Alcohol is pulled out of
Drinks into a root of its own because that is how a player thinks about it
(DrinkTypeAlcohol), not because the files put it there.

Fields written onto each consumable row:

    consumable_group   root expand   ("Food", "Drinks", "Alcohol", "Chems", "Serums")
    consumable_type    sub-expand    ("Cakes & Pies", "Tea", "Salves", ...)

Everything in the bucket that gets neither is, by definition, not a consumable,
and plan_subpages sends it to the CAMP page.

Usage:
    python3 src/plan_consumables.py --report-only src/plan-system/plan_master.json
    python3 src/plan_consumables.py src/plan-system/plan_master.json dist/plan_master.json
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

SCHEMA = 1
HERE = os.path.dirname(os.path.abspath(__file__))
TSV = os.path.join(os.path.dirname(HERE), "tsv")


def set_tsv_dir(path):
    """Point the ALCH pickup at another root — tsv/pts for the PTS channel."""
    global TSV
    TSV = path or os.path.join(os.path.dirname(HERE), "tsv")
    return TSV

# Root expands, in PAGE order. Each is (label, blurb, keyword).
ROOTS = [
    ("Food",    "Anything you eat. Cooked meals, the raw ingredients worth a recipe, "
                "and the sweet stuff.",                                   "ObjectTypeFood"),
    ("Drinks",  "Teas, juices and everything else you drink that will not "
                "get you drunk.",                                         "ObjectTypeDrink"),
    ("Alcohol", "Beer, wine, liquor and the moonshine. Split out of Drinks because "
                "nobody thinks of a Pickaxe Pilsner as a beverage.",       "DrinkTypeAlcohol"),
    ("Serums",  "Mutation serums — the ones that give you a mutation without the "
                "rads.",                                                  "ObjectTypeSerum"),
    ("Chems",   "Everything you inject, swallow or rub on. Stimpaks, salves, "
                "antibiotics and the rest.",                              "ObjectTypeChem"),
]

# Sub-expands per root, in page order: (label, keyword). The first match wins,
# and anything that matches none of them falls into the root's last entry, whose
# keyword is None — the catch-all.
SUBS = {
    "Food": [
        ("Cakes & Pies",   "ObjectTypeCakesPies"),
        ("Steaks",         "MealTypeSteak"),
        ("Soups & Stews",  "MealTypeSoup"),
        ("Gourmet",        "MealTypeGourmet"),
        ("Packaged",       "MealTypePackaged"),
        ("Raw",            "MealTypeRaw"),
        ("Cooked Meals",   None),
    ],
    "Drinks": [
        ("Tea",            "DrinkTypeTea"),
        ("Juice",          "DrinkTypeJuice"),
        ("Other Drinks",   None),
    ],
    "Alcohol": [
        ("Liquor",         "DrinkTypeLiquor"),
        ("Beer & Wine",    None),
    ],
    "Chems": [
        ("Stimpaks",       "ObjectTypeStimpak"),
        ("Salves",         "ObjectTypeSalve"),
        ("Antibiotics",    "ObjectTypeAntibiotics"),
        ("Rad-X",          "ObjectTypeRadX"),
        ("Other Chems",    None),
    ],
    "Serums": [
        ("Mutation Serums", None),
    ],
}

# MATCH order, which is deliberately not page order. Almost every row carries
# more than one ObjectType, so whichever is tested first wins, and the useful
# answer is rarely the broadest one:
#   * every alcohol is also ObjectTypeDrink — test Alcohol first or the root is
#     empty and all fifteen sit in Drinks, which is what happened;
#   * every serum is also ObjectTypeChem;
#   * a cake is ObjectTypeCakesPies AND ObjectTypeFood.
MATCH_ORDER = ["Alcohol", "Serums", "Food", "Drinks", "Chems"]

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _newest_alch():
    """Newest ALCH_Export_*.tsv by the month in its NAME.

    Not by mtime: tsv/ holds several ALCH exports that share a timestamp because
    they were re-downloaded together, and an mtime sort silently picked whichever
    the glob returned first.
    """
    hits = [h for h in glob.glob(os.path.join(TSV, "ALCH_Export_*.tsv"))
            if "Effects" not in os.path.basename(h)]
    if not hits:
        raise RuntimeError(f"[plan_consumables] no ALCH_Export_*.tsv in {TSV}")

    def stamp(path):
        m = re.search(r"_([A-Za-z]{3,9})_(\d{4})", os.path.basename(path))
        return (int(m.group(2)), _MONTHS.get(m.group(1)[:3].lower(), 0)) if m else (0, 0)

    return sorted(hits, key=lambda h: (stamp(h), os.path.getmtime(h)), reverse=True)[0]


def _load_keywords(path):
    """ALCH FormID -> set of its keyword EditorIDs."""
    out = {}
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            fid = (r.get("ALCH_FormID") or "").strip()
            if not fid:
                continue
            out[fid] = set(re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)\[",
                                      r.get("Keywords_Flat") or ""))
    return out


def classify(keywords):
    """(root, sub) for an ALCH's keyword set, or (None, None)."""
    by_label = {label: kw for label, _blurb, kw in ROOTS}
    root = next((label for label in MATCH_ORDER if by_label[label] in keywords), None)
    if not root:
        # An ALCH with none of the five ObjectTypes still IS a consumable — it is
        # the keywording that is thin, not the record. Food is the safe home:
        # every one of these seen so far is a meal.
        root = "Food"
    for label, kw in SUBS[root]:
        if kw is None or kw in keywords:
            return root, label
    return root, SUBS[root][-1][0]


def attach(items):
    path = _newest_alch()
    kw = _load_keywords(path)
    tally = collections.Counter()
    for it in items:
        # SELECT BY PAGE, NOT BY RECORD. `cnam.sig == "ALCH"` is still the whole
        # definition of a consumable — it is what plan_subpages tests to decide
        # the Recipe page in the first place — but asking plan_subpages rather
        # than re-testing it here means the page and the grouping cannot come to
        # different answers about a row. An ALCH plan that some future carve-out
        # moves to a page of its own stops being grouped here, automatically.
        if it.get("plan_page") != "recipe":
            it.pop("consumable_group", None)
            it.pop("consumable_type", None)
            continue
        cn = it.get("cnam") or {}
        root, sub = classify(kw.get(cn.get("formid", ""), set()))
        it["consumable_group"] = root
        it["consumable_type"] = sub
        if not it.get("cut"):
            tally[f"{root} / {sub}"] += 1
    return {"source": os.path.basename(path), "tally": dict(tally)} if tally else {}


def config():
    """Group headings for the renderer, emitted into plan_master."""
    return [{"label": label, "blurb": blurb,
             "types": [t for t, _kw in SUBS[label]]}
            for label, blurb, _kw in ROOTS]


def report(stats, where=""):
    if not stats:
        print(f"[plan_consumables] {where}no consumable plans found")
        return
    total = sum(stats["tally"].values())
    print(f"[plan_consumables] {where}{total} consumable plans from {stats['source']}")
    by_root = collections.Counter()
    for k, v in stats["tally"].items():
        by_root[k.split(" / ")[0]] += v
    for label, _b, _k in ROOTS:
        if by_root.get(label):
            print(f"    {label} ({by_root[label]})")
            for t, _kw in SUBS[label]:
                n = stats["tally"].get(f"{label} / {t}", 0)
                if n:
                    print(f"      {t}: {n}")


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        return doc, {}
    doc["consumables_schema"] = SCHEMA
    doc["consumable_groups"] = config()
    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--tsv-dir", default="")
    args = ap.parse_args()
    if args.tsv_dir:
        set_tsv_dir(args.tsv_dir)
    for path in args.paths:
        if not os.path.exists(path):
            print(f"[plan_consumables] missing: {path}", file=sys.stderr)
            continue
        _doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")


if __name__ == "__main__":
    main()
