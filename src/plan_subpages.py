#!/usr/bin/env python3
"""
plan_subpages.py — plan checklist pages that are carved out of a bucket.

THE PROBLEM
-----------
plan_master's `type` is the record bucket, and it lies. The `recipe` bucket
holds 1,216 live rows of which only ~155 are actually food or chems; the rest
are CAMP furniture, weapon paints, snow globes and fishing gear that were filed
there because nothing else fitted. So several pages that guide_index.tsv has
carried since the category was built had no way to select their own rows, and
rendered empty while their plans sat on the Recipe or Weapon checklist.

THE SIGNAL
----------
plan_images.page_folder() already answers "what IS this row, to a player" — it
classifies every plan by the record it creates and the words in its EditorID,
and the result is written onto the row as `image_dir`. Its own comment says
"one page per folder". This module takes it at its word and uses that same
classification to decide which page a row belongs to, so there is ONE
classifier to maintain rather than one per page.

Carve-outs are opt-in. Only the pages listed in SUBPAGES below pull their rows
out of a bucket; every other folder value is ignored and those rows stay where
they are. That matters — `image_dir` says 857 recipe-bucket rows are CAMP
items, and evicting them before a CAMP page exists would just lose them.

Fields written onto each matching row:

    plan_page         the page slug ("snow-globes", "fishing-rod")
    plan_page_group   the group heading it renders under, or None for a flat page

Usage:
    python3 src/plan_subpages.py --report-only src/plan-system/plan_master.json
    python3 src/plan_subpages.py src/plan-system/plan_master.json dist/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

SCHEMA = 1


# ─── page definitions ───────────────────────────────────────────────────────
# folder  : the plan_images.page_folder() value that selects this page's rows.
# groups  : ordered [key, label, blurb, EditorID pattern]. The first pattern
#           that matches wins. None means a flat A–Z page with no groups.
# skip    : EditorID pattern for rows the folder claims but the page should not
#           show, with the reason. Never a silent drop — the report prints them.
# note    : [heading, body] shown under the header card. For saying out loud why
#           a page is thinner than a reader expects.

SUBPAGES = {
    "fishing-rod": {
        "title": "Fishing Rod",
        "folder": "fishing-rod",
        # Rods, bobbers and floats sit in the `recipe` bucket; reels and rod
        # upgrades in `weapon`. Neither is a recipe or a weapon.
        "groups": [
            ["rod", "Rods", "The rod itself — the skin you cast with.",
             r"Recipe_mod_FishingRod_RodBase\d*_"],
            ["bobber", "Bobbers & Floats",
             "What sits on the water. Cosmetic: the bobber does not change what you catch.",
             r"Recipe_mod_FishingRod_RodBobber_"],
            ["reel", "Reels",
             "Line upgrades, Mark 1 through Mark 4. Each tier raises the line strength "
             "you can land a fish with.",
             r"Recipe_mod_FishingRod_LineUpgrade_"],
            ["upgrade", "Rod Upgrades",
             "The one-off rod mods — drag, handle, bearing, hook and gear ratio.",
             r"Recipe_mod_FishingRod_Upgrade_"],
        ],
        # A CAMP rod stand, not rod gear. It is a workshop item and belongs with
        # the other CAMP displays.
        "skip": [[r"Workshop_Recipe_RodDisplay", "a CAMP display stand, not rod gear"]],
        # Most rods and bobbers are Atom Shop or scoreboard unlocks whose zzz_
        # recipe record the game never wires up, so they are cut and do not
        # render. Without this the page reads as broken rather than accurate.
        "note": ["Plans only.",
                 "Most rod skins and bobbers are Atom Shop or scoreboard unlocks rather than "
                 "plans you learn, so they are not tracked here. The full set of rods, bobbers "
                 "and floats is on the Fishing Rod Skins, Bobbers & Floats guide."],
    },

    "camera-mod": {
        "title": "Camera Mod",
        "folder": "camera",
        # This page RENDERS from its own dataset (dist/camera_mods.json), because
        # only four of the thirteen camera mods are learned from a plan. The
        # entry is here anyway, and it earns its place: without it those four
        # lens plans stay in the `weapon` bucket and render on the weapon page as
        # well as this one, and a plan on two checklists is a plan you can tick
        # twice. Tagging them is what takes them off the weapon page.
        "groups": None,
        "skip": [],
    },

    "snow-globes": {
        "title": "Snow Globes",
        "folder": "snowglobe",
        # Flat A–Z. Seventeen globes with nothing to group them by — they are
        # all the same kind of thing and they all come from the same handful of
        # sources, so a heading would only add a click.
        "groups": None,
        # The Skyline Valley Snow Globe Display Case and the Shenandoah
        # Snowglobe Display are furniture you stand globes ON. Both already
        # render on /df/plan-checklists/displays/, and a plan on two checklists
        # is a plan you can tick twice.
        "skip": [[r"_(Displays?|Displaycase|SnowglobeStand)", "display furniture — it is on the Displays page"]],
    },
}


def _compile(page):
    """Compile a page's patterns once, in place."""
    if page.get("_ready"):
        return page
    page["_skip"] = [(re.compile(rx, re.I), why) for rx, why in page.get("skip") or []]
    page["_groups"] = [(k, l, b, re.compile(rx, re.I))
                       for k, l, b, rx in (page.get("groups") or [])]
    page["_ready"] = True
    return page


def page_of(item):
    """(slug, group_label) for a row, or (None, None).

    Second element is None on a flat page, and on a grouped page whose patterns
    all missed — the renderer puts those under the page's last group rather than
    dropping them, because a row with no home is still a row.
    """
    folder = (item.get("image_dir") or "").strip().lower()
    edid = (item.get("plan_item") or {}).get("edid", "") or ""
    for slug, page in SUBPAGES.items():
        if page["folder"] != folder:
            continue
        _compile(page)
        for rx, _why in page["_skip"]:
            if rx.search(edid):
                return (None, None)
        if not page["_groups"]:
            return (slug, None)
        for _k, label, _blurb, rx in page["_groups"]:
            if rx.search(edid):
                return (slug, label)
        return (slug, page["_groups"][-1][1])
    return (None, None)


def skipped(item):
    """(slug, reason) when a row's folder claims it but the page skips it."""
    folder = (item.get("image_dir") or "").strip().lower()
    edid = (item.get("plan_item") or {}).get("edid", "") or ""
    for slug, page in SUBPAGES.items():
        if page["folder"] != folder:
            continue
        _compile(page)
        for rx, why in page["_skip"]:
            if rx.search(edid):
                return (slug, why)
    return (None, None)


def config():
    """The page definitions the renderer needs, as plain JSON.

    Emitted into plan_master so the group labels and blurbs live in exactly one
    place. The renderer reads them from the data rather than carrying its own
    copy, which is how the two would otherwise drift the first time a heading is
    reworded.
    """
    return {
        slug: {
            "title": page["title"],
            "groups": [{"key": k, "label": l, "blurb": b}
                       for k, l, b, _rx in (page.get("groups") or [])],
            "note": page.get("note") or None,
        }
        for slug, page in SUBPAGES.items()
    }


def attach(items):
    """Tag every carve-out row in a plan_master item list."""
    tally = collections.Counter()
    for it in items:
        slug, group = page_of(it)
        if not slug:
            it.pop("plan_page", None)
            it.pop("plan_page_group", None)
            continue
        it["plan_page"] = slug
        it["plan_page_group"] = group
        tally[slug] += 1
        if it.get("cut"):
            tally[slug + ":cut"] += 1
    return dict(tally) if tally else {}


def report(stats, where=""):
    if not stats:
        print(f"[plan_subpages] {where}no carve-out rows found")
        return
    print(f"[plan_subpages] {where}"
          + ", ".join(f"{k}={v}" for k, v in sorted(stats.items()) if not k.endswith(":cut")))
    cuts = {k[:-4]: v for k, v in stats.items() if k.endswith(":cut")}
    if cuts:
        print("  cut (not rendered): " + ", ".join(f"{k}={v}" for k, v in sorted(cuts.items())))


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        return doc, {}
    doc["plan_subpages_schema"] = SCHEMA
    doc["plan_subpages"] = config()
    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--report-only", action="store_true")
    args = ap.parse_args()

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[plan_subpages] missing: {path}", file=sys.stderr)
            continue
        doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")
        if not (args.report_only and stats):
            continue
        rows = [i for i in doc["items"] if i.get("plan_page")]
        for slug, page in SUBPAGES.items():
            mine = [i for i in rows if i["plan_page"] == slug]
            live = [i for i in mine if not i.get("cut")]
            print(f"    {page['title']} — {len(live)} shown, {len(mine) - len(live)} cut")
            labels = [g[1] for g in (page.get("groups") or [])] or [None]
            for label in labels:
                sub = [i for i in live if i["plan_page_group"] == label]
                if label:
                    print(f"      {label} ({len(sub)})")
                for i in sorted(sub, key=lambda x: x["name"]):
                    print(f"        {i['name']}  [{i['type']}]")
        for i in doc["items"]:
            slug, why = skipped(i)
            if slug:
                print(f"    SKIPPED from {slug}: {i['name']} — {why}")


if __name__ == "__main__":
    main()
