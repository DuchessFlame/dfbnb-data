#!/usr/bin/env python3
r"""
build_new_plans_json.py — "New Plans" page (/df/plan-checklists/new-plans/).

WHAT IT IS
----------
A rolling "what arrived in this patch" list. It is NOT a hand-maintained page
and it is NOT cumulative: it holds exactly the plans that exist in the NEWEST
BOOK export but did not exist in the one before it. Upload a newer BOOK export
and the previous batch drops off on its own, replaced by whatever that export
added. Nothing to prune, nothing to date-stamp by hand.

    newest BOOK export   ──┐
                           ├── set difference ──> the plans on this page
    previous BOOK export ──┘

Both sides come from tsv_source.newest_pair(), which ranks exports by the date
in the FILENAME — never by mtime. (In CI, actions/checkout stamps every file
with the checkout time, so "newest by mtime" is meaningless.) The same call
resolves the PTS channel when --data-dir points at tsv/pts, so the PTS page
diffs PTS-vs-PTS and never against a live export.

If only ONE BOOK export exists, the page is EMPTY, not "everything is new".
A first-run page listing all 2,800 plans as new would be a lie.

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

`group_for()` prefers the COBJ/OMOD EditorIDs over the plan_master `type`
bucket, because that bucket misfiles a known minority: weapon mods sold by the
stamp/score vendors land in "recipe" (co_Weapon_* EDIDs) and the odd magazine
mod lands in "armour". Those EDIDs say plainly what the plan is for, so this
page reads them first and falls back to the bucket only when they resolve
nothing. This is deliberately a display-time refinement for THIS page only —
it does not move any plan between checklist pages. If the same misfiling
should be fixed at the source, that belongs in classify_plan() in
build_plan_obtain_json.py, and this function can then be reduced to the
bucket map.

USAGE
-----
    python src/build_new_plans_json.py --data-dir tsv --outdir dist
    python src/build_new_plans_json.py --data-dir tsv/pts --outdir dist   # PTS job relocates dist/ later
"""
import os, re, csv, json, argparse, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
sys.path.insert(0, HERE)

import tsv_source

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
# a GROUPS key. Use it only where the EDIDs genuinely do not say — e.g. a CAMP
# build whose COBJ never resolved to a placeable upstream, so it falls to
# Recipes. Runs last and wins over everything below it.
#     "008BA7F3": "weapon",
GROUP_OVERRIDES = {}

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

    # 1. Backpack is unambiguous wherever it appears.
    if typ == "backpack-mod" or _RX_BACKPACK.search(blob):
        return "backpack-mod"

    # 2. A workshop / furniture COBJ is a CAMP build, whatever the bucket says.
    if _RX_WORKSHOP.search(blob):
        return "camp"

    # 3. A weapon mod slot in the EditorID settles it before anything else.
    if _RX_WEAP_SLOT.search(blob):
        return "weapon"

    # 4. The created object's own EDID beats the bucket: this is what rescues
    #    co_Weapon_* stamp-vendor mods from the "recipe" bucket.
    if _RX_WEAPON.search(blob):  return "weapon"
    if _RX_APPAREL.search(blob): return "apparel"
    if _RX_ARMOUR.search(blob):  return "armour"
    if cnam_sig == "ALCH" or _RX_CONSUM.search(blob):
        return "recipe"

    # 5. Nothing in the EDIDs — fall back to the page bucket.
    if typ in _TYPE_FALLBACK:
        return _TYPE_FALLBACK[typ]

    # 6. The "recipe" bucket splits on whether the plan makes something you can
    #    place. has_image_box is exactly that test upstream.
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


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=TSV,
                    help="TSV export root; tsv/pts on the PTS channel")
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
    # exclude="Locations" — BOOK_Export_*.tsv also catches the _Locations
    # companion, which carries no FULL column and would diff to "everything new".
    pattern = os.path.join(args.data_dir, "BOOK_Export_*.tsv")
    newest_f, prev_f = tsv_source.newest_pair(pattern, exclude="Locations")
    if not newest_f:
        raise SystemExit(f"[new-plans] no BOOK export found under {args.data_dir}")

    new_ids = book_plan_ids(newest_f)
    if prev_f:
        old_ids = book_plan_ids(prev_f)
        added   = set(new_ids) - set(old_ids)
    else:
        # One export only: there is no "before", so nothing is demonstrably new.
        old_ids = {}
        added   = set()
        print("[new-plans] only one BOOK export on this channel — "
              "publishing an empty page rather than calling every plan new.")

    print(f"[new-plans] newest  : {os.path.basename(newest_f)}  ({len(new_ids)} plans)")
    print(f"[new-plans] previous: {os.path.basename(prev_f) if prev_f else '(none)'}  ({len(old_ids)} plans)")
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
        if it.get("cut"):
            continue
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
        "baseline": {
            "newest":   os.path.basename(newest_f),
            "previous": os.path.basename(prev_f) if prev_f else None,
            "newest_plan_count":   len(new_ids),
            "previous_plan_count": len(old_ids),
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
