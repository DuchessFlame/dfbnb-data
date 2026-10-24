#!/usr/bin/env python3
"""
rng76_lvot_test.py — per-entry curve tables in resolve_chance_none (25 Oct 2026).

An entry's tier GLOB (LVOC) is read through THAT entry's curve table (LVOT),
carried by LVLI_Math's EntryChanceNoneCurveTable column
("!!!Wordpress - ExportLVLIToCSV.pas"). Real rows from the Government Aid Drop
crate (Container_Loot_DropCrate_Govt01, 001199F0), checked in SeventySix.esm:

  entry                                   LVOC                          LVOT
  LL_Armor_Any                            ItemThree_High_ChanceNone_Tier Container_Item3_ChanceNone
  LL_Food_Packaged                        ItemTwo_Medium_ChanceNone_Tier Container_Item2_ChanceNone
  LLS_Loot_Weapons_Ranged_Mods_Recipes_   Tier_Guaranteed (0)            (none)
  AllRegions

Asserts:
  * no column (older export)  -> exactly the old behaviour (list curve
    borrowed; the weapons entry reads 100 = always nothing, the old bug)
  * column, blank LVOT        -> the list's curve is NOT borrowed
                                 (Tier_Guaranteed -> 0 = always rolls)
  * column, LVOT set          -> that curve, even when the list's borrowed
                                 curve is a different one (food: Item2, not Item3)

Run:  python3 tests/rng76_lvot_test.py      (needs the repo's tsv/ exports)
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))

import rng76  # noqa: E402

CRATE = "001199F0"
ITEM3 = "00018445:Container_Item3_ChanceNone:CURV"
ITEM2 = "00018444:Container_Item2_ChanceNone:CURV"


def main():
    data = rng76.Rng76Data.from_tsv_root(os.path.join(ROOT, "tsv"))
    res = data.resolver
    rows = {}
    for (lid, _idx), m in data.lvli.math_by_entry.items():
        if lid == CRATE:
            rows[(m.get("LVLO_Reference") or "").split(":")[1]] = dict(m)
    weap = rows["LLS_Loot_Weapons_Ranged_Mods_Recipes_AllRegions"]
    food = rows["LL_Food_Packaged"]
    armour = rows["LL_Armor_Any"]
    for r in (weap, food, armour):
        r.pop("EntryChanceNoneCurveTable", None)

    fails = []

    def check(label, got, want):
        ok = abs(got - want) < 1e-6
        print(("ok   " if ok else "FAIL ") + f"{label}: {got:g} (want {want:g})")
        if not ok:
            fails.append(label)

    # older export, no column: unchanged behaviour
    old_weap = res.resolve_chance_none(weap, "Entry")
    check("no column: weapons entry keeps the old borrowed-curve value", old_weap, 100.0)
    old_armour = res.resolve_chance_none(armour, "Entry")

    # column present
    check("blank LVOT: Tier_Guaranteed is not run through the list's curve",
          res.resolve_chance_none(dict(weap, EntryChanceNoneCurveTable=""), "Entry"), 0.0)
    check("LVOT Item3 on the armour entry: same as before",
          res.resolve_chance_none(dict(armour, EntryChanceNoneCurveTable=ITEM3), "Entry"), old_armour)
    tier = res.globs.value(rng76.glob_formid_from_lvli_field(food["EntryChanceNoneCurve"]))
    want = res.curvs.interpolate("00018444", tier)
    check("LVOT Item2 on the food entry: its own curve, not the list's Item3",
          res.resolve_chance_none(dict(food, EntryChanceNoneCurveTable=ITEM2), "Entry"), want)

    if fails:
        print(f"\n{len(fails)} failed")
        sys.exit(1)
    print("\nall passed")


if __name__ == "__main__":
    main()
