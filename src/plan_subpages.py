#!/usr/bin/env python3
"""
plan_subpages.py — which plan checklist page every plan renders on.

THE PROBLEM
-----------
plan_master's `type` is the record bucket, and it lies. The `recipe` bucket
holds 1,216 live rows of which only ~155 are actually food or chems; the rest
are CAMP furniture, weapon paints, snow globes and fishing gear that were filed
there because nothing else fitted. The `armour` bucket holds body armour and
power armour together, which is why the Power Armour page rendered empty while
its 383 plans sat on the Body Armour page. So a page cannot select on `type`.

THE SIGNAL
----------
plan_images.page_folder() already answers "what IS this row, to a player" — it
classifies every plan by the record it creates and the words in its EditorID,
and the result is written onto the row as `image_dir`. Its own comment says
"one page per folder". This module takes it at its word: that classification IS
the page routing, so there is ONE classifier to maintain rather than one per
page.

ONE PLAN, ONE PAGE
------------------
Every page in the category is listed below, not just the carve-outs, because
that is what makes the rule checkable: a plan renders where `plan_page` says
and nowhere else, so a plan on two checklists — a plan you could tick twice —
is impossible by construction rather than by everyone remembering. Row counts
per page therefore have to add up to the live roster, and `--report-only`
prints exactly that sum. A live row that comes out with NO page is a bug and is
reported as one; the only rows that legitimately get none are the explicit
`skip` entries, each of which names the page that does show it.

Fields written onto each row:

    plan_page         the page slug ("snow-globes", "power-armour", "camp")
    plan_page_group   the group heading it renders under, or None for a flat
                      page and for a page whose grouping comes from another
                      enricher (weapon / armour / consumable)

Usage:
    python3 src/plan_subpages.py --report-only dist/plan_master.json
    python3 src/plan_subpages.py dist/plan_master.json dist/pts/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

SCHEMA = 2   # 1 -> 2: every page listed, multi-folder select, `when`, `grouping`


# ─── how a page claims its rows ─────────────────────────────────────────────
# folder   : plan_images.page_folder() value(s) that select this page's rows.
#            A list when one page draws from more than one classification.
# when     : optional extra test, by name (see WHEN). Two pages can then share
#            a folder — the Recipe page takes the rows that create an ALCH and
#            the CAMP page picks up the remainder.
# groups   : ordered [key, label, blurb, rule]. `rule` is an EditorID regex, or
#            {"folder": "..."} / {"folder": [...]} to group by classification.
#            First rule that matches wins. None means a flat A-Z page.
# grouping : the page's rows are grouped two-level by a different enricher, and
#            the renderer reads THAT field instead of plan_page_group:
#              "weapon"     -> weapon_group     + weapon_role   (add_weapon_groups)
#              "armour"     -> armour_group     + armour_role   (add_armour_groups)
#              "consumable" -> consumable_group + consumable_type (plan_consumables)
# dataset  : the page renders from its own JSON, not from plan_master rows. The
#            entry still earns its place: tagging the rows is what takes them
#            off whichever bucket page would otherwise show them as well.
# skip     : EditorID rule for rows the folder claims but the page must not
#            show, with the reason and the page that does show them. Never a
#            silent drop — the report prints every one.
# note     : [heading, body] under the header card, for saying out loud why a
#            page is thinner or odder than a reader expects.
#
# ORDER MATTERS: the first page whose folder (and `when`) claims a row wins.
# Only `recipes` is contested — Recipe takes the consumables, CAMP takes the
# rest — so Recipe is listed first.

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
            # The souvenir versions: a display copy of a bobber or a rod, built
            # at a tinker's bench, not fitted to anything. They reach this page
            # because they are fishing gear by name and by folder, and before
            # this group existed they fell into Rod Upgrades — the last group
            # takes whatever the rules miss, so a misfile here looks identical
            # to a match.
            ["display", "Display Replicas",
             "Souvenir copies for the CAMP — they go on a shelf, not on a rod.",
             r"RodBobber_Display|Tinkers_Souvenir|Rod_?Display"],
        ],
        "skip": [[r"Workshop_Recipe_RodDisplay",
                  "a CAMP display stand, not rod gear — it is on the Displays page"]],
        # Rewritten 17 Sep 2026. The old wording said most rod skins and bobbers
        # were "Atom Shop or scoreboard unlocks rather than plans you learn",
        # which is what the pipeline believed when it could only read BOOK
        # records: their plans are zzz_-prefixed and unreferenced, so they read
        # as cut. COBJ.GNAM says otherwise — thirteen of them are rewards for
        # the "Catch All Regional Fish" challenges, and they are on this page.
        "note": ["Plans and recipes.",
                 "Rod gear you learn is all here, however you learn it — from a plan, or "
                 "handed over for finishing a fishing challenge. The handful still missing "
                 "are Atom Shop or scoreboard unlocks with no recipe to learn at all; "
                 "the complete set of rods, bobbers and floats is on the Fishing Rod Skins, "
                 "Bobbers & Floats guide."],
    },

    "camera-mod": {
        "title": "Camera Mod",
        "folder": "camera",
        # Renders from dist/camera_mods.json, because only four of the thirteen
        # camera mods are learned from a plan. The entry earns its place anyway:
        # without it those four lens plans stay in the `weapon` bucket and
        # render on the Weapon page as well as this one.
        "groups": None,
        "dataset": True,
        "skip": [],
    },

    "snow-globes": {
        "title": "Snow Globes",
        "folder": "snowglobe",
        # Flat A-Z. Seventeen globes with nothing to group them by — they are
        # all the same kind of thing from the same handful of sources, so a
        # heading would only add a click.
        "groups": None,
        "skip": [[r"_(Displays?|Displaycase|SnowglobeStand)",
                  "display furniture you stand globes on — it is on the Displays page"]],
    },

    "underarmour": {
        "title": "Underarmour",
        "folder": "underarmour",
        # Renders from dist/underarmour.json. Same reasoning as camera-mod: the
        # tag is what stops these 57 rows ALSO rendering on the Body Armour
        # page, which is where the `armour` bucket was putting 36 of them.
        "groups": None,
        "dataset": True,
        "skip": [],
    },

    "power-armour": {
        "title": "Power Armour",
        "folder": "power-armour",
        # The page guide_index.tsv has carried since the category was built and
        # that rendered empty the whole time, because its 383 plans are in the
        # `armour` bucket and the Body Armour page was selecting the whole
        # bucket. Same shape as the weapon page: one root expand per PA set,
        # with Mods and Skins inside it.
        "groups": None,
        "grouping": "armour",
        "skip": [],
    },

    "body-armour": {
        "title": "Body Armour",
        "folder": "body-armour",
        "groups": None,
        "grouping": "armour",
        "skip": [],
    },

    "apparel": {
        "title": "Apparel",
        "folder": "apparel",
        "groups": None,
        "skip": [],
    },

    "backpack-mod": {
        "title": "Backpack Mod",
        "folder": "backpack",
        "groups": None,
        "skip": [],
    },

    "weapon": {
        "title": "Weapon",
        "folder": "weapons",
        # Includes the weapon paints the `recipe` bucket was holding. Grouped
        # per weapon by add_weapon_groups.
        "groups": None,
        "grouping": "weapon",
        "skip": [],
    },

    "recipe": {
        "title": "Recipe",
        "folder": "recipes",
        # Food, drink, alcohol, serums and chems, and nothing else. `when`
        # is the whole definition: the record the recipe CREATES. ALCH is the
        # game's consumable record, so a plan that makes one is a consumable
        # recipe and a plan that does not, is not. No name matching — a
        # "Recipe: " prefix is on beer steins too.
        "when": "creates_alch",
        "groups": None,
        "grouping": "consumable",
        "skip": [],
    },

    "camp": {
        "title": "CAMP Plans",
        # `workshop` is everything the classifier calls CAMP furniture. The
        # `recipes` rows that reach here are the ones Recipe did not claim —
        # they create no ALCH, so they are not consumables, and the classifier
        # could not place them either. `photomode` is one plan with nowhere
        # else to go.
        "folder": ["workshop", "recipes", "photomode"],
        "groups": [
            ["camp", "C.A.M.P. Plans",
             "Everything you build in your CAMP — furniture, decor, lights, utility and "
             "structures. Flat A-Z for now: the EditorIDs only name a category for about "
             "half of these, and grouping the rest properly means reading the C.A.M.P. "
             "build-menu keywords off the FURN / ACTI / STAT records.",
             {"folder": "workshop"}],
            ["needs-sorting", "Needs Sorting",
             "Plans the classifier could not place — beer steins, paintings, ghillie suits, "
             "photo frames. They are here so they are visible and tickable rather than "
             "silently dropped, and this group is meant to empty out, not to stay.",
             {"folder": ["recipes", "photomode"]}],
        ],
        "skip": [],
    },
}


# ─── the `when` tests ───────────────────────────────────────────────────────
# Named rather than inline lambdas so SUBPAGES stays plain data that can be
# read, diffed and emitted as JSON.

WHEN = {
    "creates_alch": lambda it: ((it.get("cnam") or {}).get("sig") or "") == "ALCH",
}


def _compile(page):
    """Compile a page's patterns once, in place."""
    if page.get("_ready"):
        return page
    page["_folders"] = [f.strip().lower() for f in (
        page["folder"] if isinstance(page["folder"], list) else [page["folder"]])]
    page["_skip"] = [(re.compile(rx, re.I), why) for rx, why in page.get("skip") or []]
    page["_when"] = WHEN[page["when"]] if page.get("when") else None
    page["_groups"] = [(k, l, b, _rule(r)) for k, l, b, r in (page.get("groups") or [])]
    page["_ready"] = True
    return page


def _rule(r):
    """A group rule: a compiled EditorID regex, or a folder test."""
    if isinstance(r, dict):
        want = r.get("folder")
        want = [want] if isinstance(want, str) else list(want or [])
        want = {w.strip().lower() for w in want}
        return ("folder", want)
    return ("edid", re.compile(r, re.I))


def _matches(rule, folder, edid):
    kind, val = rule
    return (folder in val) if kind == "folder" else bool(val.search(edid))


def _rule_edid(item):
    """The EditorID the group rules match against.

    Normally the plan item's. A row for a recipe with no plan book (see
    plan_recipe_rows.py) has none, so the recipe's own EditorID stands in —
    it carries the same vocabulary the rules are written against
    (`..._Fishing_...`, `..._Workshop_...`, `..._Clothes_...`). Without this
    every such row falls into whatever a page's last group is, which is a
    silent misfile rather than a visible one.
    """
    plan = (item.get("plan_item") or {}).get("edid") or ""
    if plan:
        return plan
    return ((item.get("cobj") or {}).get("edid")
            or (item.get("cnam") or {}).get("edid") or "")


def page_of(item):
    """(slug, group_label) for a row, or (None, None).

    Second element is None on a flat page, on a page whose grouping comes from
    another enricher, and on a grouped page whose rules all missed — the last
    group takes those rather than dropping them, because a row with no home is
    still a row.
    """
    folder = (item.get("image_dir") or "").strip().lower()
    edid = _rule_edid(item)
    for slug, page in SUBPAGES.items():
        _compile(page)
        if folder not in page["_folders"]:
            continue
        if page["_when"] and not page["_when"](item):
            continue
        for rx, _why in page["_skip"]:
            if rx.search(edid):
                return (None, None)
        if not page["_groups"]:
            return (slug, None)
        for _k, label, _blurb, rule in page["_groups"]:
            if _matches(rule, folder, edid):
                return (slug, label)
        return (slug, page["_groups"][-1][1])
    return (None, None)


def skipped(item):
    """(slug, reason) when a row's folder claims it but the page skips it."""
    folder = (item.get("image_dir") or "").strip().lower()
    edid = _rule_edid(item)
    for slug, page in SUBPAGES.items():
        _compile(page)
        if folder not in page["_folders"]:
            continue
        for rx, why in page["_skip"]:
            if rx.search(edid):
                return (slug, why)
    return (None, None)


def config():
    """The page definitions the renderer needs, as plain JSON.

    Emitted into plan_master so the titles, group labels and blurbs live in
    exactly one place. The renderer reads them from the data rather than
    carrying its own copy, which is how the two would otherwise drift the first
    time a heading is reworded.
    """
    return {
        slug: {
            "title": page["title"],
            "groups": [{"key": k, "label": l, "blurb": b}
                       for k, l, b, _r in (page.get("groups") or [])],
            "grouping": page.get("grouping") or None,
            "dataset": bool(page.get("dataset")),
            "note": page.get("note") or None,
        }
        for slug, page in SUBPAGES.items()
    }


def attach(items):
    """Tag every row in a plan_master item list with the page it renders on."""
    tally = collections.Counter()
    for it in items:
        # Only plan rows are routed. The BNB skins pages read `kind: "skin"`
        # rows out of the same file and select them by bucket; tagging those
        # would put them on a plan checklist as well as their own page.
        if (it.get("kind") or "plan") != "plan":
            continue
        slug, group = page_of(it)
        if not slug:
            it.pop("plan_page", None)
            it.pop("plan_page_group", None)
            if not it.get("cut"):
                s, _why = skipped(it)
                tally["(skipped)" if s else "(NO PAGE)"] += 1
            continue
        it["plan_page"] = slug
        it["plan_page_group"] = group
        if it.get("cut"):
            tally[slug + ":cut"] += 1
        else:
            tally[slug] += 1
    return dict(tally) if tally else {}


def homeless(items):
    """Live rows that landed on no page and were not deliberately skipped."""
    out = []
    for it in items:
        if it.get("cut") or it.get("plan_page"):
            continue
        slug, _why = skipped(it)
        if not slug:
            out.append(it)
    return out


def report(stats, where=""):
    if not stats:
        print(f"[plan_subpages] {where}no rows tagged")
        return
    pages = {k: v for k, v in stats.items()
             if not k.endswith(":cut") and not k.startswith("(")}
    print(f"[plan_subpages] {where}{sum(pages.values())} live rows across "
          f"{len(pages)} pages")
    print("  " + ", ".join(f"{k}={v}" for k, v in sorted(pages.items())))
    cuts = {k[:-4]: v for k, v in stats.items() if k.endswith(":cut")}
    if cuts:
        print("  cut (not rendered): " + ", ".join(f"{k}={v}" for k, v in sorted(cuts.items())))
    if stats.get("(skipped)"):
        print(f"  skipped on purpose: {stats['(skipped)']} (each one named in --report-only)")
    if stats.get("(NO PAGE)"):
        print(f"  *** {stats['(NO PAGE)']} LIVE ROWS ON NO PAGE — see --report-only ***")


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
    ap.add_argument("--list", action="store_true", help="also print every row per page")
    args = ap.parse_args()

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[plan_subpages] missing: {path}", file=sys.stderr)
            continue
        doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")
        if not args.report_only:
            continue
        rows = doc["items"]
        live = [i for i in rows if not i.get("cut")]
        for slug, page in SUBPAGES.items():
            mine = [i for i in live if i.get("plan_page") == slug]
            print(f"    {page['title']:16s} {len(mine):5d}"
                  + (f"   [{page['grouping']} grouping]" if page.get("grouping") else "")
                  + ("   [own dataset]" if page.get("dataset") else ""))
            for g in (page.get("groups") or []):
                n = sum(1 for i in mine if i.get("plan_page_group") == g[1])
                print(f"        {g[1]} ({n})")
            if args.list:
                for i in sorted(mine, key=lambda x: x["name"]):
                    print(f"          {i['name']}  [{i['type']}]")
        for i in live:
            slug, why = skipped(i)
            if slug:
                print(f"    SKIPPED from {slug}: {i['name']} — {why}")
        lost = homeless(live)
        print(f"    TOTAL live tagged: {sum(1 for i in live if i.get('plan_page'))}"
              f" of {len(live)}   homeless: {len(lost)}")
        for i in lost:
            print(f"    *** NO PAGE: {i['name']} [{i.get('image_dir')}] "
                  f"{(i.get('plan_item') or {}).get('edid','')}")


if __name__ == "__main__":
    main()
