#!/usr/bin/env python3
r"""
plan_apparel_class.py — armour or clothing? Ask the record, not the name.

WHAT THIS FIXES
---------------
plan_images.page_folder() sends anything the builder filed under the `armour`
bucket to the Body Armour page, and one of its rules says so out loud: "A
helmet is armour, whatever its record calls itself." That rule was written to
stop every Marine and Secret Service helmet being swept into apparel by the
word "Headwear", and for a set piece it is right. But it also kept 44 things
that are plainly outfits on the armour page — the Cultist robes and hoods, the
ten Flower Crowns, the Vault 63 helmets, the Cowboy Duster and Chaps, the
Wasteland Florist apron and sunhat, the Vault-Tec Jumpsuit.

Duchess, on seeing them: "this is all apparel, they give no stats and mostly no
skins for them."

THE RULE, READ OFF THE ARMO RECORD
----------------------------------
"No stats" is a thing the record says, so it is a thing that can be tested:

    resistance   DAMA_*_Curve   armour carries a resistance LADDER, not a flat
                                amount. Every DAMA_*_Amount in the export is 0,
                                including Marine Armor's — the number comes
                                from a CURVE (CT_Player_Armor_Universal_TierNN)
                                resolved against your level. Testing the Amount
                                fields says every piece of armour in the game
                                has no armour, which is how this was nearly
                                missed.
    durability   DATA_Health    100 on armour, 0 on clothing.

A record with neither protects you from nothing, so it is an outfit whatever it
is called. Names are never consulted.

AND NO SET IS LEFT SPLIT IN HALF
---------------------------------
Six pieces pass that test but must NOT move: the Marine, Arctic Marine, Chinese
Stealth, Brotherhood Recon, Civil Engineer, Secret Service and Secret Service
Drifter HELMETS. Each is genuinely statless — in game they are cosmetic — but
each is also one piece of a six- or ten-piece set whose other pieces are real
armour. Moving one piece to another page splits a set across two checklists,
which is worse for a reader than the helmet sitting with its family.

So a row moves only when its WHOLE set is clothing: no plan that shares its set
token creates a piece with stats. Every standalone outfit passes that trivially
(it is a set of one); nothing with a mod or a skin behind it moves, so nothing
is orphaned.

Runs after plan_images (it rewrites `image_dir`) and before plan_subpages
(which routes on it).

Usage:
    python3 src/plan_apparel_class.py --report-only dist/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import add_weapon_groups as awg          # newest() / read_tsv(), and the TSV root
import add_armour_groups as aag          # the anchored set-token extraction

SCHEMA = 1

FROM_FOLDER = "body-armour"
TO_FOLDER = "apparel"

# The resistance ladder. An armour piece names a curve per damage type; an
# outfit names none.
CURVES = ("DAMA_Physical_Curve", "DAMA_Energy_Curve", "DAMA_Rad_Curve",
          "DAMA_Fire_Curve", "DAMA_Cryo_Curve", "DAMA_Poison_Curve")


def load_armo():
    """ARMO FormID -> (has resistance ladder, durability)."""
    path = awg.newest("ARMO_Export_*.tsv",
                      exclude=r"_(SLOTS|ObjectTemplate|Properties|Refs|Mods)\.tsv$")
    out = {}
    for r in awg.read_tsv(path):
        fid = (r.get("ARMO_FormID") or "").strip()
        if not fid:
            continue
        curve = any((r.get(c) or "").strip() for c in CURVES)
        try:
            health = float((r.get("DATA_Health") or "0").strip() or 0)
        except ValueError:
            health = 0.0
        out[fid] = (curve, health)
    return os.path.basename(path), out


def has_stats(rec):
    """True when the record carries a resistance ladder or durability."""
    if rec is None:
        return None            # no record at all — not our call to make
    curve, health = rec
    return bool(curve) or health > 0


def set_token(item):
    """The set this piece belongs to, or None for a standalone.

    Same anchored extraction the armour grouping uses — on `Armor_` /
    `PowerArmor_`, stepping over DLC and content prefixes — so the two agree
    about what a set is. A record whose EditorID never says "armor" belongs to
    no set, which is exactly what a one-off hood is.
    """
    for src in ((item.get("cnam") or {}).get("edid", ""),
                (item.get("plan_item") or {}).get("edid", "")):
        if not src:
            continue
        tok = aag._seed_token_any(src)
        if tok:
            return aag._norm(tok)
    return None


def family(item, armoured_sets):
    """The armour set a piece belongs to, or None.

    set_token() anchors on `Armor_`, which a helmet's own record usually lacks
    (Headwear_Clothes_DLC03_Marine_Helmet, Headwear_ChineseStealthArmor,
    Headwear_Clothes_SecretService_Helmet). So a headwear piece whose EditorID
    carries a WHOLE underscore-delimited token naming a real armour set belongs
    to that set. Whole tokens only: the Vault 63 helmets end in `_Storm`, and
    "storm" is a POWER armour set, which is why power armour sets are never in
    `armoured_sets`.
    """
    tok = set_token(item)
    if tok and tok in armoured_sets:
        return tok
    cn = (item.get("cnam") or {}).get("edid", "") or ""
    toks = [t.lower() for t in cn.split("_") if t]
    if not toks or "headwear" not in toks:
        return None
    for t in toks:
        if t in armoured_sets:
            return t
    return tok


def attach(items):
    source, armo = load_armo()

    # Which SETS contain a piece with stats. Read across Body Armour AND
    # Apparel: the Enclave scout sets (Covert Scout, Solar, Thorn) were filed
    # under Apparel by the bucket and were never looked at, so 15 real armour
    # pieces with a resistance ladder and 100 durability sat on the Apparel
    # page (Duchess, 4 Oct 2026). Power armour is its own page and never counts.
    armoured_sets = set()
    pieces = []
    for it in items:
        folder = (it.get("image_dir") or "")
        if folder not in (FROM_FOLDER, TO_FOLDER):
            continue
        cn = it.get("cnam") or {}
        if cn.get("sig") != "ARMO":
            continue
        stats = has_stats(armo.get(cn.get("formid") or ""))
        pieces.append((it, stats))
        if stats:
            tok = set_token(it)
            if tok:
                armoured_sets.add(tok)

    moved, kept, to_armour = [], [], []
    for it, stats in pieces:
        folder = it.get("image_dir") or ""
        fam = family(it, armoured_sets)
        if folder == FROM_FOLDER:
            if it.get("reclassified_from") == TO_FOLDER:
                # moved here by an earlier run — keep it while it still qualifies
                if stats or (fam and fam in armoured_sets):
                    to_armour.append(it)
                continue
            if stats is False:
                if fam and fam in armoured_sets:
                    kept.append(it)          # a cosmetic helmet inside a real set
                    continue
                it["image_dir"] = TO_FOLDER
                it["reclassified_from"] = FROM_FOLDER
                moved.append(it)
        elif folder == TO_FOLDER and not it.get("reclassified_from"):
            # Real armour filed as apparel, or a statless piece of a real set.
            if stats or (stats is False and fam and fam in armoured_sets):
                it["image_dir"] = FROM_FOLDER
                it["reclassified_from"] = TO_FOLDER
                to_armour.append(it)
        elif folder == TO_FOLDER and it.get("reclassified_from") == FROM_FOLDER:
            # moved by an earlier run; re-test with the family rule
            if stats is False and fam and fam in armoured_sets:
                it["image_dir"] = FROM_FOLDER
                it.pop("reclassified_from", None)
                kept.append(it)
            else:
                moved.append(it)

    # Anything moved by an earlier run that no longer qualifies goes back —
    # these enrichers are re-run in place, and data that is not pruned lies.
    done = {id(i) for i in moved} | {id(i) for i in to_armour}
    for it in items:
        origin = it.get("reclassified_from")
        if origin and id(it) not in done:
            it.pop("reclassified_from")
            # Only undo a move this module made — a row routed elsewhere since
            # (a Legacy Nuclear Winter plan, say) just loses the stale marker.
            here = it.get("image_dir") or ""
            if here in (FROM_FOLDER, TO_FOLDER) and here != origin:
                it["image_dir"] = origin

    return {"source": source, "moved": len(moved), "kept": len(kept),
            "to_armour": len(to_armour),
            "names": sorted(i["name"] for i in moved),
            "kept_names": sorted(i["name"] for i in kept),
            "to_armour_names": sorted(i["name"] for i in to_armour)}


def report(stats, where=""):
    if not stats:
        print(f"[plan_apparel_class] {where}no armour rows, nothing to do")
        return
    print(f"[plan_apparel_class] {where}{stats['moved']} statless plans moved "
          f"body-armour -> apparel, from {stats['source']}")
    if stats.get("to_armour"):
        print(f"  apparel -> body-armour (real armour, or a piece of a real set): "
              + ", ".join(stats["to_armour_names"]))
    if stats["kept"]:
        print(f"  kept with their set (statless but one piece of a real one): "
              + ", ".join(stats["kept_names"]))


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        return doc, {}
    doc["apparel_class_schema"] = SCHEMA
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
        awg.set_tsv_dir(args.tsv_dir)
    for path in args.paths:
        if not os.path.exists(path):
            print(f"[plan_apparel_class] missing: {path}", file=sys.stderr)
            continue
        _doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")
        if args.report_only and stats:
            for n in stats["names"]:
                print("    ->", n)


if __name__ == "__main__":
    main()
