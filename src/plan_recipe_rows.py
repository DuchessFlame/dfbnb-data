#!/usr/bin/env python3
r"""
plan_recipe_rows.py — checklist rows for recipes that have no plan book.

WHY
---
Every row on a plan checklist has, until now, started life as a BOOK: the plan
item you find, trade and learn. That is one of the ways Bethesda teaches a
recipe, and the pipeline treated it as the only one.

`plan_unlocks` reads COBJ.GNAM, where the game states the route itself, and 147
live recipes name a CHALLENGE there instead of a plan. Most pair with a plan
book — those are handled by the cut rescue in `add_recipe_unlocks.py`. The rest
have no plan book at all, and there has never been anywhere for them to go:

    Beer Pump                  <- Kill Bounties from Group 2
    Aerial Turbine             <- Complete Grunt Hunts
    Dog Tamer Outfit           <- Dog - Reach Level 190
    Spicy Eelgrass Soup        <- Catch Fall Seasonal Local Legend: "Sludge Eye"
    Guard Post                 <- learned by claiming a workshop

A player knows these the same way they know any other recipe, and tracks them
the same way. So a checklist row is a RECIPE you can learn, and a plan book is
one route to it rather than the definition of a row.

WHAT IS EXCLUDED, AND WHY
-------------------------
Emitting one row per COBJ would publish the same craftable several times over,
because a single thing a player builds is often several recipes:

  * **A recipe a plan already covers.** Skipped on the COBJ FormID first, then
    on the created record's name — "Rocket Bobber" is both a rod mod with a plan
    and a `RodBobber_Display_Rocket` souvenir without one, and two rows called
    Rocket Bobber on the fishing page is a bug however defensible the data is.
  * **Stubs with no created record.** 53 of them: condition proxies whose whole
    job is to carry the GNAM. There is nothing to name a row after.
  * **Duplicates within this set**, collapsed on the created record's name, so
    the workbench and souvenir variants of one item yield one row.

ROW SHAPE
---------
Identical to a plan row, with `plan_item: null` and an id of `RECIPE_<COBJ>`.

`kind` stays **"plan"**. It is tempting to invent `kind: "recipe"`, and it is
wrong: `selectPlanRows()` in the renderer filters every checklist on
`(it.kind || "plan") === "plan"`, and the progress store, the export poster and
the group counts all follow it. A new kind would publish these rows into a
dataset nothing reads. What sets them apart is `plan_item: null`, which the
renderer is already defensive about everywhere it touches it, plus an explicit
`recipe_only` flag for anything that wants to say so.
`type` comes from the same `classify_plan()` the plan rows use, so a recipe
lands on the page its craftable belongs to and nowhere else. The id is stable
across builds because a FormID is, which matters: it is the progress-store key,
and a reader's ticks follow it.

`tradeable` is **null** for challenge/workshop recipes, not False. A recipe with
no plan item is not a thing that can change hands at all, so both answers would
be wrong; null is what the renderer already draws as "Unknown".

SCRAP-TO-LEARN IS THE EXCEPTION: **always `tradeable: False`.**
(So is a challenge reward — see `plan_sources.apply_tradeable_rules`.)
A scrap-learned mod is not "we don't know" — the game states the route, and that
route is the only one: there is no plan book for it, so there is nothing to find,
trade, sell or drop. Printing "Unknown" there invites a reader to go looking for a
plan in someone's vendor that cannot exist. The rule is the route, not a per-mod
lookup, so it is enforced centrally in `enforce_scrap_untradeable()` and holds for
any row flagged `scrap_learn` whatever built it.
"""

import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo
import plan_images
import plan_sources
import plan_unlocks


def _norm(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _plan_name(full):
    return _norm(re.sub(r"^(?:Plan|Recipe)\s*:\s*", "", full or ""))


def build(items, tsv_dir="tsv", stats=None):
    """Return the recipe-only rows to append to *items*."""
    stats = stats if stats is not None else collections.Counter()
    tsv_dir = os.path.abspath(tsv_dir)
    bpo.TSV = tsv_dir
    bpo.SIG_INDEX = bpo.build_sig_index()
    cobj_idx = bpo.build_cobj_index()
    unlocks = plan_unlocks.RecipeUnlocks(tsv_dir, bpo.newest)

    covered_fids = {(it.get("cobj") or {}).get("formid", "").upper()
                    for it in items if it.get("cobj")}
    covered_names = {_plan_name(it.get("name")) for it in items}
    covered_names |= {_norm((it.get("cnam") or {}).get("edid")) for it in items}
    covered_names.discard("")

    rows, seen = [], {}
    for co_fid in sorted(unlocks.by_cobj):
        unlock = unlocks.proof_of_life(co_fid)
        if not unlock:
            continue
        if co_fid in covered_fids:
            stats["covered_by_a_plan"] += 1
            continue
        cobj = cobj_idx.get(co_fid) or {}
        name = (cobj.get("cnam_full") or "").strip()
        if not name:
            stats["no_created_record"] += 1
            continue
        key = _norm(name)
        if key in covered_names:
            stats["same_name_as_a_plan"] += 1
            continue
        if key in seen:
            stats["duplicate_recipe"] += 1
            continue

        cat, has_img, cnam_sig, cnam_fid, cnam_edid = bpo.classify_plan(
            cobj.get("edid") or "", cobj)
        sentence = unlocks.sentence(unlock)
        row = {
            "kind": "plan", "recipe_only": True, "brand": "df", "type": cat,
            "id": f"RECIPE_{co_fid}", "name": name,
            "has_image_box": has_img, "image_dir": "",
            "obtain": (bpo.LEARNT_DIRECT + " It is not random loot; see below."),
            "category_label": bpo.category_label(cat, has_img, cnam_sig,
                                                 physical=False),
            "obtain_routes": [],
            "obtain_unlocks": [sentence] if sentence else [],
            # No BOOK exists. Every consumer tests this for None already, because
            # a plan row with an unresolved created object has always been able
            # to carry a null `cobj`/`cnam`.
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": cobj.get("edid") or ""},
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": cnam_sig}
                     if cnam_fid else None),
            "tradeable": None, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [],
        }
        # The art folder decides which checklist page the row lands on
        # (plan_subpages keys on it), so it is resolved with the same classifier
        # the plan rows use rather than left as the bucket name. plan_images
        # overwrites it with the same answer when the art step runs.
        row["image_dir"] = plan_images.page_folder(row) or cat or ""
        row["obtain_ledger"] = plan_sources.obtain_ledger(row)
        seen[key] = row
        rows.append(row)
        stats["emitted"] += 1
        stats["by_type_" + (cat or "none")] += 1

    rows += _build_scrap(items, cobj_idx, unlocks, covered_fids, stats)
    return rows, stats


# Structural mod EDID tokens the family resolver protects — see add_weapon_groups
# / plan_images. A scrap-learned mod that turns out to be a paint or material is
# a skin, not a functional mod, but both still nest under the same weapon, so the
# distinction is left to add_weapon_groups.classify() (which reads the OMOD attach
# point) rather than second-guessed here.
def _build_scrap(items, cobj_idx, unlocks, covered_fids, stats):
    """Recipe-only rows for mods learned by SCRAPPING a weapon or armour.

    Distinct from build() above in two ways, both because a scrap mod is
    per-weapon, not one-of-a-kind:

      * It does NOT collapse on the created record's display NAME. "Hair Trigger
        Receiver" is a separate learnable for the 10mm, the Pipe and the Combat
        Rifle, and each has to be its own row so it can nest under its own
        weapon. Collapsing them the way build() collapses souvenir duplicates
        would publish one row and drop the rest. Dedup is on the created OMOD
        FormID instead — that folds only the genuine workbench duplicates (two
        COBJs making the same OMOD), keeping the shorter/earliest COBJ id.

      * The `type` is taken from the GNAM target's signature (WEAP -> weapon,
        ARMO -> armour), not from classify_plan(). A ranged weapon mod's EDID
        (`co_mod_10mm_Receiver_...`) carries no "weapon"/"gun" token, so
        classify_plan() would file it under `recipe` and it would never reach the
        weapon page; the parent item's signature is the data-driven truth.

    A scrap COBJ already carried by a real plan row (same COBJ, or a book mod
    that creates the same OMOD) is skipped, so a mod obtainable both ways is not
    doubled.
    """
    covered_cnam = {((it.get("cnam") or {}).get("formid") or "").upper()
                    for it in items if it.get("cnam")}
    covered_cnam.discard("")

    # One row per created OMOD; if several COBJs make it, keep the lowest FormID
    # so the row id is stable across builds.
    by_omod = {}
    for co_fid in unlocks.by_cobj:
        unlock = unlocks.unlock_for(co_fid)
        if not unlock or unlock.get("kind") != plan_unlocks.SCRAP:
            continue
        if co_fid in covered_fids:
            stats["scrap_covered_by_a_plan"] += 1
            continue
        cobj = cobj_idx.get(co_fid) or {}
        cnam_fid = (cobj.get("cnam_fid") or "").upper()
        if cnam_fid and cnam_fid in covered_cnam:
            stats["scrap_mod_already_a_plan"] += 1
            continue
        dedup_key = cnam_fid or co_fid
        prev = by_omod.get(dedup_key)
        if prev is None or co_fid < prev[0]:
            by_omod[dedup_key] = (co_fid, cobj, unlock)

    rows = []
    for co_fid, cobj, unlock in by_omod.values():
        name = (cobj.get("cnam_full") or "").strip()
        if not name:
            stats["scrap_no_created_record"] += 1
            continue
        cat = "armour" if unlock.get("item_sig") == "ARMO" else "weapon"
        cnam_fid = (cobj.get("cnam_fid") or "").upper()
        cnam_edid = cobj.get("cnam_edid") or ""
        sentence = unlocks.sentence(unlock)
        row = {
            "kind": "plan", "recipe_only": True, "scrap_learn": True,
            "brand": "df", "type": cat,
            "id": f"RECIPE_{co_fid}", "name": name,
            "has_image_box": False, "image_dir": "",
            "obtain": (bpo.LEARNT_DIRECT +
                       " It is learned by scrapping the base item — see below."),
            "category_label": bpo.category_label(cat, False, "OMOD", physical=False),
            "obtain_routes": [],
            "obtain_unlocks": [sentence] if sentence else [],
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": cobj.get("edid") or ""},
            # cnam.sig MUST be OMOD: add_weapon_groups / add_armour_groups key
            # their "this is a mod, find its weapon" path on it.
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": "OMOD"}
                     if cnam_fid else None),
            # Scrap-to-learn is never tradeable — see ROW SHAPE above.
            "tradeable": False, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [],
        }
        row["image_dir"] = plan_images.page_folder(row) or cat or ""
        row["obtain_ledger"] = plan_sources.obtain_ledger(row)
        rows.append(row)
        stats["scrap_emitted"] += 1
        stats["scrap_by_type_" + cat] += 1
    return rows


def attach(items, tsv_dir="tsv", stats=None):
    """Append recipe-only rows to *items*, replacing any from a previous run."""
    stats = stats if stats is not None else collections.Counter()
    before = len(items)
    items[:] = [it for it in items if not it.get("recipe_only")]
    stats["replaced"] = before - len(items)
    rows, stats = build(items, tsv_dir, stats)
    items.extend(rows)
    # Route-decided tradeability (scrap-to-learn, challenge rewards), applied to
    # every row here because attach() is the one place all three builders
    # (build_plan_obtain_json, merge_parts, add_recipe_unlocks) pass through.
    plan_sources.apply_tradeable_rules(items, stats)
    return stats


def report(stats, stream=sys.stdout):
    for key in sorted(stats):
        print(f"    {key:24s} {stats[key]}", file=stream)
