#!/usr/bin/env python3
r"""
build_new_plans_json.py — "New Plans" page (/df/plan-checklists/new-plans/).

WHAT IT IS
----------
A rolling "what arrived in this patch" list. It is NOT a hand-maintained page
and it is NOT cumulative: it holds exactly the plans that exist in the NEWEST
BOOK export but did not exist in the baseline it is diffed against. Upload a
newer BOOK export and the previous batch drops off on its own, replaced by
whatever that export added. Nothing to prune, nothing to date-stamp by hand.

    newest BOOK export   ──┐
                           ├── set difference ──> the plans on this page
    baseline BOOK export ──┘

TWO MODES — "new" MEANS SOMETHING DIFFERENT PER CHANNEL
-------------------------------------------------------
    LIVE  (mode "vs-previous")      "what the last patch added"
        newest live export   vs   previous live export

    PTS   (mode "vs-live")          "what's coming in the next update"
        newest PTS export    vs   newest LIVE export

The PTS page deliberately does NOT diff PTS-against-PTS. Consecutive PTS
exports are usually the same build re-exported, so a PTS-vs-PTS diff comes out
empty while the PTS is carrying dozens of plans the live game has never seen —
which is exactly what a PTS reader is there for. The PTS job selects this mode
by passing `--baseline-dir tsv`; the sanity check in that workflow asserts the
newest side is PTS and the baseline is live, so either half of the wiring
breaking fails the build rather than publishing a wrong page.

The renderer words the page from the `mode` field, so the live page says
"added by the latest game data update" and the PTS page says "not in the live
game yet".

DUPLICATE BASELINES
-------------------
An export re-uploaded from the same game build has an identical plan roster to
the one before it. Diffing against it yields nothing and blanks the page even
though the patch genuinely added plans — which is exactly what the live May and
July 2026 exports do (2,866 plans each).

So in `vs-previous` mode the baseline walks back past any export whose plan
roster is IDENTICAL to the newest, stopping at the first that actually differs.
It cannot run away: a real patch always differs, so the walk stops at the real
previous patch. Skipped exports are listed in `baseline.skipped_duplicates` and
shown on the page, so an accidental re-upload is visible rather than silent.

Every side is resolved through tsv_source, which ranks exports by the date in
the FILENAME — never by mtime. (In CI, actions/checkout stamps every file with
the checkout time, so "newest by mtime" is meaningless.)

If no usable baseline exists at all, the page is EMPTY, not "everything is
new". A first-run page listing all 2,800 plans as new would be a lie.

WHERE THE ROWS COME FROM
------------------------
The BOOK diff gives FormIDs and nothing else. Every row's content — drop
routes, rates, tradeable / stops-dropping flags, effects, image box — is taken
verbatim from the plan_master.json this same build already produced, so the
New Plans row and the row on the plan's own checklist page are byte-identical
and cannot drift. This builder resolves nothing itself and must therefore run
AFTER build_plan_obtain_json.py in every workflow.

A plan in the BOOK diff with no plan_master entry is skipped and counted in
`skipped_not_in_master` (backpack cosmetics are deliberately dropped from the
roster upstream, so a few skips are normal, not a fault).

GROUPING
--------
Groups are A–Z by label: Apparel · Armour · Backpack Mods · CAMP · Recipes ·
Weapons. Plans are A–Z by display title inside each group. Empty groups are
omitted entirely rather than rendered as a heading with nothing under it.

CAMP and Recipes are two different things and are split here even though
plan_master lumps both into its single "recipe" bucket: CAMP is placeable
(buildings, furniture, decorations, workshop objects), Recipes is consumable
(food, chems, drinks).

`group_for()` is an ordered ladder, and its SHAPE matters more than any pattern
in it:

    plan_master's apparel / armour / weapon / backpack-mod buckets are EARNED —
    the builder resolved the plan's created object to a real ARMO or WEAP
    record. That is stronger evidence than any word in an EditorID, so those
    buckets are trusted and passed straight through. Word-matching exists to
    refine the ONE bucket that was never resolved ("recipe", the catch-all) and
    to fix the single documented cross-bucket error (weapon mods filed under
    "recipe" or "armour", caught by a weapon mod SLOT name in the EditorID).

Getting that backwards is a real bug, not a nicety. An earlier cut word-matched
first and put "Plan: Trucker Uniform" under CAMP (its recipe is crafted at a
workshop) and "Plan: Thorn Armor Chest Piece" under Armour (its EditorID says
"Armor"), when plan_master had both correctly as apparel. Across the roster it
dragged 182 plans off the page they actually live on.

This is deliberately a display-time refinement for THIS page only — it does not
move any plan between checklist pages. If the same misfiling should be fixed at
the source, that belongs in classify_plan() in build_plan_obtain_json.py, and
this function can then be reduced to the bucket map.

USAGE
-----
    python src/build_new_plans_json.py --data-dir tsv --outdir dist
    python src/build_new_plans_json.py --data-dir tsv/pts --baseline-dir tsv --outdir dist
        # PTS job relocates dist/ -> dist/pts/ afterwards
"""
import os, re, csv, json, argparse, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
sys.path.insert(0, HERE)

import tsv_source

# ── the BOOK export selector, shared ─────────────────────────────────────────
# BOOK_Export_*.tsv also catches the _Locations companion, which carries no FULL
# column and would diff to "everything is new". Every caller must exclude it.
# build_underarmour_json.py imports both of these plus book_plan_ids() and
# resolve_baseline() so the two pages always agree on what "new" means.
BOOK_GLOB    = "BOOK_Export_*.tsv"
BOOK_EXCLUDE = "Locations"

# ── group definitions ────────────────────────────────────────────────────────
# key -> label. Rendered A–Z by label, so the dict order here is cosmetic.
GROUPS = {
    "apparel":      "Apparel",
    "armour":       "Armour",
    "backpack-mod": "Backpack Mods",
    "camp":         "CAMP",
    "recipe":       "Recipes",
    "weapon":       "Weapons",
}

# plan_master `type` -> group key, used as the fallback when the EDIDs are
# silent. "recipe" is absent on purpose: it is resolved by has_image_box.
_TYPE_FALLBACK = {
    "apparel":      "apparel",
    "armour":       "armour",
    "backpack-mod": "backpack-mod",
    "weapon":       "weapon",
}

# Manual last word, for the handful the data cannot settle. Key is the plan's
# BOOK FormID (the `plan_item.formid` on the row, shown in Technical); value is
# a GROUPS key. Runs FIRST and wins over everything below it.
#
# Use it only where the data genuinely does not say. Every entry here is a CAMP
# placeable whose COBJ never resolved to a placeable upstream, so it lands in
# the "recipe" catch-all and nothing in its EditorID says otherwise — the EDIDs
# below literally read "Recipe". Fixing it properly means fixing the COBJ
# resolution in build_plan_obtain_json.py; until then these three would be the
# only rows on a two-plan page, filed under the wrong heading.
GROUP_OVERRIDES = {
    "008EE1B0": "camp",   # Plan: Healing Arch  (SCORE_S24_Recipe_HealingArch_StampVendor)
    "008EE1AF": "camp",   # Plan: Phoropter     (SCORE_S24_Recipe_Phoropter_StampVendor)
    "008F5215": "camp",   # Plan: Pint-Sized Slasher Photo Frame (PTS; SDOW_Recipe_PhotoMode_Frame_SlasherFrame)
}

_RX_BACKPACK = re.compile(r"backpack", re.I)
# Weapon mod SLOT names. A mod recipe whose EditorID names one of these is a
# weapon mod, whatever bucket it landed in: armour and power-armour mods use
# Material / Lining / Torso / Arm / Leg / Helmet instead, so there is no
# overlap. This is what rescues the MG42 and Plasma Caster mod plans, which
# carry no COBJ link at all and would otherwise fall to Recipes (and, for one
# magazine mod, to Armour).
_RX_WEAP_SLOT = re.compile(
    # Trailing (?=_|$) not \b — "_" is a word character, so \b never fires
    # between "Magazine" and "_ArmorPen" and half the slots slipped through.
    r"_mod_.*?_(receiver|muzzle|barrel|magazine|grip|stock\d*|scope|sights?|"
    r"bayonet|ammo|sling|capacitor|nozzle|blade|hammer)(?=_|$)", re.I)
_RX_WORKSHOP = re.compile(r"workshop|categoryfurniture|_furniture_|_decor", re.I)
_RX_WEAPON   = re.compile(r"(^|[_\W])(weapon|melee|ranged|gun|rifle|pistol|shotgun|launcher|grenade|mine|bow|chainsaw)([_\W]|$)", re.I)
_RX_ARMOUR   = re.compile(r"(^|[_\W])(armor|armour|powerarmor|power_armor|pa)([_\W]|$)", re.I)
_RX_APPAREL  = re.compile(r"(^|[_\W])(outfit|apparel|underarmor|under_armor|dress|costume|uniform|mask|hat|helmet)([_\W]|$)", re.I)
_RX_CONSUM   = re.compile(r"(^|[_\W])(food|chem|drink|cook|meal|recipe_food|alch)([_\W]|$)", re.I)


def group_for(item):
    """Group key for one plan_master item. See GROUPING in the module docstring."""
    plan_fid  = ((item.get("plan_item") or {}).get("formid") or "").upper()
    if plan_fid in GROUP_OVERRIDES:
        return GROUP_OVERRIDES[plan_fid]

    cobj_edid = ((item.get("cobj") or {}).get("edid") or "")
    cnam      = (item.get("cnam") or {})
    cnam_edid = (cnam.get("edid") or "")
    cnam_sig  = (cnam.get("sig") or "")
    book_edid = ((item.get("plan_item") or {}).get("edid") or "")
    typ       = str(item.get("type") or "").lower()
    # The BOOK's own EditorID is always present; the COBJ/CNAM links are not.
    blob      = f"{cobj_edid} {cnam_edid} {book_edid}"

    # 1. A weapon mod SLOT name in the EditorID is the one documented
    #    cross-bucket rescue, and it runs before the buckets because it is
    #    correcting them: stamp/score-vendor weapon mods land in "recipe" and
    #    the odd magazine mod lands in "armour". Armour and power-armour mods
    #    use Material / Lining / Torso / Arm / Leg / Helmet instead, so there
    #    is no overlap and this cannot steal a genuine armour mod.
    if _RX_WEAP_SLOT.search(blob):
        return "weapon"

    # 2. RESOLVED BUCKETS WIN. apparel / armour / weapon / backpack-mod mean
    #    the upstream builder matched the plan's created object to a real ARMO
    #    or WEAP record. No word in an EditorID outranks that — see GROUPING in
    #    the module docstring for the 182 plans that moved when this was the
    #    other way round.
    if typ in _TYPE_FALLBACK:
        return _TYPE_FALLBACK[typ]

    # ── Below here the bucket is "recipe", the catch-all that resolved nothing.
    #    Only now does word-matching get a say. ───────────────────────────────

    # 3. Backpack mods that never resolved to a backpack record.
    if _RX_BACKPACK.search(blob):
        return "backpack-mod"

    # 4. A workshop / furniture COBJ is a CAMP build.
    if _RX_WORKSHOP.search(blob):
        return "camp"

    # 5. The created object's own EDID — this is what rescues co_Weapon_*
    #    stamp-vendor mods that carry no mod-slot name.
    if _RX_WEAPON.search(blob):  return "weapon"
    if _RX_APPAREL.search(blob): return "apparel"
    if _RX_ARMOUR.search(blob):  return "armour"
    if cnam_sig == "ALCH" or _RX_CONSUM.search(blob):
        return "recipe"

    # 6. Nothing said anything. The "recipe" bucket splits on whether the plan
    #    makes something you can place. has_image_box is exactly that test
    #    upstream.
    return "camp" if item.get("has_image_box") else "recipe"


def plan_title(item):
    """The title the page sorts and shows — matches planTitle() in the renderer."""
    return str(item.get("display_name") or item.get("name") or item.get("id") or "")


def book_plan_ids(path):
    """{FormID: FULL} for every learnable plan/recipe BOOK in one export.

    Same roster test as build_plan_obtain_json.py: FULL starts "Plan: " or
    "Recipe: ". Keeping the two in step matters — a row this builder considers
    new but the roster never built would render as a blank line.
    """
    out = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            full = (row.get("FULL") or "").strip()
            if not (full.startswith("Plan: ") or full.startswith("Recipe: ")):
                continue
            fid = (row.get("FormID") or "").strip().upper()
            if fid:
                out[fid] = full
    return out


def resolve_baseline(data_dir, baseline_dir, cur_ids, newest_f):
    """(baseline_file, mode, skipped) — what to diff `newest_f` against.

    Shared with build_underarmour_json.py so the two pages can never disagree
    about what "new" means. `cur_ids` is book_plan_ids(newest_f), passed in so
    the caller does not parse the newest export twice.

    mode "vs-live"     : baseline_dir given. The newest export THERE, whatever
                         its roster. No walk-back — the two roots are different
                         channels, so an identical roster is a real answer
                         ("the PTS adds nothing"), not a re-upload to skip.

    mode "vs-previous" : single root. The previous export, walking BACK past
                         any whose plan roster is identical to the newest,
                         because that is a re-upload of the same game build and
                         diffing against it blanks the page. See DUPLICATE
                         BASELINES in the module docstring. Returns None when
                         no usable baseline survives — an empty page, never
                         "everything is new".
    """
    if baseline_dir:
        base_hits = tsv_source.all_matching(
            os.path.join(baseline_dir, BOOK_GLOB), exclude=BOOK_EXCLUDE)
        if not base_hits:
            raise SystemExit(
                f"[new-plans] --baseline-dir {baseline_dir} holds no BOOK export. "
                f"Publishing every plan as new would be a lie — fix the path."
            )
        return base_hits[-1], "vs-live", []

    hits = tsv_source.all_matching(
        os.path.join(data_dir, BOOK_GLOB), exclude=BOOK_EXCLUDE)
    newest_abs = os.path.abspath(newest_f)
    older = [h for h in hits if os.path.abspath(h) != newest_abs]

    # A real patch always differs, so this stops at the real previous patch —
    # it cannot run away past one.
    skipped = []
    newest_roster = set(cur_ids)
    for cand in reversed(older):
        if set(book_plan_ids(cand)) == newest_roster:
            skipped.append(os.path.basename(cand))
            continue
        return cand, "vs-previous", skipped

    return None, "vs-previous", skipped


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=TSV,
                    help="TSV export root; tsv/pts on the PTS channel")
    ap.add_argument("--baseline-dir", default="",
                    help="diff against the newest export HERE instead of the "
                         "previous one under --data-dir. The PTS job passes "
                         "'tsv' so 'new' means new-versus-live.")
    ap.add_argument("--outdir", default=DIST,
                    help="output dir (the PTS job relocates dist/ -> dist/pts/ afterwards)")
    ap.add_argument("--master", default="",
                    help="plan_master.json to read rows from (default: <outdir>/plan_master.json)")
    ap.add_argument("--out", default="",
                    help="explicit output file (default: <outdir>/new_plans.json)")
    args = ap.parse_args(argv)

    outdir = args.outdir
    master_path = args.master or os.path.join(outdir, "plan_master.json")
    out_path    = args.out    or os.path.join(outdir, "new_plans.json")

    # ── the two BOOK exports ────────────────────────────────────────────────
    newest_f = tsv_source.newest(os.path.join(args.data_dir, BOOK_GLOB),
                                 exclude=BOOK_EXCLUDE, required=False)
    if not newest_f:
        raise SystemExit(f"[new-plans] no BOOK export found under {args.data_dir}")
    new_ids = book_plan_ids(newest_f)
    prev_f, mode, skipped_dupes = resolve_baseline(
        args.data_dir, args.baseline_dir, new_ids, newest_f)

    if prev_f:
        old_ids = book_plan_ids(prev_f)
        added   = set(new_ids) - set(old_ids)
    else:
        # No usable baseline: there is no "before", so nothing is demonstrably
        # new. An empty page beats calling all 2,800 plans new.
        old_ids = {}
        added   = set()
        print("[new-plans] no usable baseline on this channel — "
              "publishing an empty page rather than calling every plan new.")

    print(f"[new-plans] mode    : {mode}")
    print(f"[new-plans] newest  : {os.path.basename(newest_f)}  ({len(new_ids)} plans)")
    print(f"[new-plans] baseline: {os.path.basename(prev_f) if prev_f else '(none)'}  ({len(old_ids)} plans)")
    for s in skipped_dupes:
        print(f"[new-plans] skipped : {s} — identical plan roster to the newest "
              f"(re-upload of the same game build)")
    print(f"[new-plans] added   : {len(added)}")

    # ── pull the finished rows out of plan_master ───────────────────────────
    with open(master_path, encoding="utf-8") as f:
        master = json.load(f)
    by_fid = {}
    for it in master.get("items", []):
        fid = ((it.get("plan_item") or {}).get("formid") or "").upper()
        if fid:
            by_fid[fid] = it

    rows, skipped = [], []
    for fid in added:
        it = by_fid.get(fid)
        if not it:
            skipped.append(new_ids.get(fid, fid))
            continue
        # Cut plans are KEPT, not dropped. This used to `continue` here, which
        # was harmless while plan_master hardcoded "cut": false — the branch
        # never fired. Now that build_plan_obtain_json.py actually resolves the
        # flag, dropping them would silently delete rows from a page whose whole
        # job is "here is everything the export added", on a site that exists to
        # document the game files. They render with the ✕ Cut pill and the
        # renderer already keeps them out of the progress total and the export
        # poster, so they cost a reader nothing and tell a data-miner something.
        row = dict(it)                       # verbatim row — see WHERE THE ROWS COME FROM
        row["group"] = group_for(it)
        row["is_new"] = True
        rows.append(row)

    groups = []
    for key, label in sorted(GROUPS.items(), key=lambda kv: kv[1].lower()):
        members = sorted((r for r in rows if r["group"] == key),
                         key=lambda r: plan_title(r).lower())
        if members:                          # empty groups are omitted, not shown bare
            groups.append({"key": key, "label": label, "count": len(members),
                           "items": members})

    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        # The renderer words the page from this: "vs-previous" -> "added by the
        # latest game data update", "vs-live" -> "not in the live game yet".
        "mode": mode,
        "baseline": {
            "newest":   os.path.basename(newest_f),
            "previous": os.path.basename(prev_f) if prev_f else None,
            "newest_plan_count":   len(new_ids),
            "previous_plan_count": len(old_ids),
            # Re-uploads of the same game build that the walk-back stepped over.
            # Shown on the page so an accidental re-upload is visible, not silent.
            "skipped_duplicates": skipped_dupes,
        },
        "count": len(rows),
        "skipped_not_in_master": sorted(skipped),
        "groups": groups,
    }

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[new-plans] wrote {out_path}  ({len(rows)} plans in {len(groups)} groups)")
    for g in groups:
        print(f"   {g['label']}: {g['count']}")
    if skipped:
        print(f"[new-plans] {len(skipped)} added plan(s) had no plan_master row "
              f"(backpack cosmetics are dropped upstream): {', '.join(skipped[:8])}"
              + (" ..." if len(skipped) > 8 else ""))
    return out


if __name__ == "__main__":
    main()
