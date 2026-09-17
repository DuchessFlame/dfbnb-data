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

`tradeable` is **null**, not False. A recipe with no plan item is not a thing
that can change hands at all, so both answers would be wrong; null is what the
renderer already draws as "no pill".
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
            "obtain": ("Learned directly — there is no plan for this one. "
                       "It is not random loot; see below."),
            "category_label": bpo.category_label(cat, has_img, cnam_sig),
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

    return rows, stats


def attach(items, tsv_dir="tsv", stats=None):
    """Append recipe-only rows to *items*, replacing any from a previous run."""
    stats = stats if stats is not None else collections.Counter()
    before = len(items)
    items[:] = [it for it in items if not it.get("recipe_only")]
    stats["replaced"] = before - len(items)
    rows, stats = build(items, tsv_dir, stats)
    items.extend(rows)
    return stats


def report(stats, stream=sys.stdout):
    for key in sorted(stats):
        print(f"    {key:24s} {stats[key]}", file=stream)
