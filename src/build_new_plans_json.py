#!/usr/bin/env python3
r"""
build_new_plans_json.py -- "New Plans" page (/df/plan-checklists/new-plans/).

WHAT IT IS
----------
A "what's new" list of plans, grouped by category. It exists in two channels
and now means:

    LIVE  (mode "vs-season")   "what has been added to LIVE this season"
        newest live BOOK export  vs  the season-start baseline
        (the newest live export from BEFORE the current season's start month)

    PTS   (mode "vs-live")     "what's coming in the next update"
        newest PTS BOOK export   vs  newest LIVE BOOK export (--baseline-dir)

WHY THE LIVE PAGE IS NOW SEASON-DATED
-------------------------------------
The old live rule diffed the newest export against the *previous* export, so a
mid-season patch reset the baseline and wiped the plans an earlier patch added.
Pinning the baseline to the season start fixes that: the baseline stays fixed
for the whole season, so mid-season exports only ADD plans (accumulate), and
when the next season begins the baseline rolls forward on its own -- the list
clears and the new season starts filling.

SEASON SELECTION (option B -- show the outgoing season until the first
in-season export lands)
-----------------------------------------------------------------------
Live BOOK exports are named by month (BOOK_Export_July_2026.tsv -> 2026-07),
so all season maths here is at MONTH granularity (the "month-floor" rule):

    active season = the LATEST season whose start-month <= the newest live
                    export's month.
    baseline      = the newest live export whose month is strictly BEFORE the
                    active season's start-month.

So on the day a season starts, the page keeps showing the OUTGOING season until
an export named for the new season's month (or later) is captured; the moment
that export lands, active-season rolls forward and the baseline becomes the last
pre-season export. Example (S26 starts 15 Sep 2026):

    newest = July_2026   -> active S25, baseline May_2026   (still S25)
    newest = Sept_2026   -> active S26, baseline July_2026  (flips to S26)

IMPORTANT: the flip depends on the export FILENAME's month. The first
post-patch S26 export must be named for September or later (e.g.
BOOK_Export_Sept_2026.tsv). If it is mis-named for an earlier month it will be
attributed to the previous season.

If no export exists before the active season's start, the page is EMPTY (never
"everything is new") -- the same safety rule the old builder used.

WHERE THE ROWS COME FROM
------------------------
The BOOK diff gives FormIDs. Every row's content is taken verbatim from the
plan_master.json this same build already produced, so this builder must run
AFTER build_plan_obtain_json.py. A plan in the diff with no plan_master row is
skipped and counted in skipped_not_in_master.

CATEGORY CLASSIFICATION
-----------------------
classify_group() buckets every plan into one of:

    Weapons        actual weapons + weapon MODS (receivers, muzzles, barrels,
                   arrows, thrown weapons, ...). NOT paints.
    Apparel        outfits, hats/headwear, masks, costumes, uniforms, clothes.
                   (Hats are ARMO records but are cosmetic apparel, so a
                   headwear/hat/mask EDID overrides the armour type.)
    Armour         real armour pieces + armour mods that are not paints.
    Skins          skins / paints / wraps / camo / appearance mods -- for
                   weapons AND armour/power armour. A NEW bucket: these used to
                   land under Weapons/Armour, which was wrong.
    Backpack Mods  backpack mods.
    CAMP           placeable workshop / decor / furniture objects (incl.
                   fishing-themed decor, beer steins, bug zappers).
    Fishing        fishing tackle: rods, reels, bobbers, floats, hooks, handles
                   -- i.e. the rod and its mods (_mod_FishingRod_*). NOT
                   fishing-themed furniture/food, which stay CAMP/Recipes, and
                   NOT Floater-creature grenades, which are Weapons.
    Recipes        consumables (food / chems / drinks).

Rules are ORDERED and driven by the plan's EDID / created-object EDID / CNAM
signature / display name -- not guesswork. Fishing tackle and Skins are tested
FIRST: fishing rod mods otherwise resolve to weapon (the rod is a weapon), and
paints otherwise resolve to weapon/armour. Paints whose EDID omits "paint"
(e.g. a jetpack paint) are caught by the display-name test.

The three GROUP_OVERRIDES remain for the handful whose data genuinely does not
say (CAMP placeables filed under "recipe" upstream).

USAGE
-----
    python src/build_new_plans_json.py --data-dir tsv --outdir dist
        # LIVE: season-dated (mode vs-season)
    python src/build_new_plans_json.py --data-dir tsv/pts --baseline-dir tsv_live --outdir dist
        # PTS: newest PTS vs newest LIVE (mode vs-live); relocated to dist/pts/
"""
import os, re, csv, json, argparse, sys
from datetime import datetime, timezone, date

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
SEASONS_TSV = os.path.join(REPO, "tsv", "fallout76_seasons.tsv")
sys.path.insert(0, HERE)

import tsv_source
import plan_images                            # row art, same resolver as the other pages

# -- the BOOK export selector, shared -----------------------------------------
BOOK_GLOB    = "BOOK_Export_*.tsv"
BOOK_EXCLUDE = "Locations"

# -- group definitions --------------------------------------------------------
# key -> label. Rendered A-Z by label, so the dict order here is cosmetic.
GROUPS = {
    "apparel":      "Apparel",
    "armour":       "Armour",
    "backpack-mod": "Backpack Mods",
    "camp":         "CAMP",
    "fishing":      "Fishing",
    "recipe":       "Recipes",
    "skin":         "Skins",
    "weapon":       "Weapons",
}

# Manual last word, by the plan's BOOK FormID. Runs FIRST and wins. Each is a
# CAMP placeable whose COBJ never resolved to a placeable upstream.
GROUP_OVERRIDES = {
    "008EE1B0": "camp",   # Plan: Healing Arch
    "008EE1AF": "camp",   # Plan: Phoropter
    "008F5215": "camp",   # Plan: Pint-Sized Slasher Photo Frame
}

# ---------------------------------------------------------------------------
# CATEGORY CLASSIFIER
# ---------------------------------------------------------------------------
# Fishing tackle: the rod and its mods. Every rod / reel / bobber / float / hook
# / handle carries "_mod_FishingRod_" in its EDID. Tested FIRST because the rod
# itself is a weapon and its mods carry _Weapon_ in the created object, so
# without this they would land under Weapons. Fishing-themed FURNITURE is
# "Fishing_Workshop_*" (stays CAMP), fishing FOOD is ALCH (stays Recipes), and
# the scuba backpack is a backpack -- none of those match _mod_FishingRod_.
_RX_FISHING = re.compile(r"_mod_fishingrod", re.I)

# Skins / paints. Tested against the EDID blob AND the display name, because a
# few paints (e.g. a jetpack paint) carry no "paint" token in the EDID.
_RX_SKIN_BLOB = re.compile(r"paint|(^|_)skin(_|s|$)|wrap|camo|decal|livery|_appearance", re.I)
_RX_SKIN_NAME = re.compile(r"\b(paint|skin|wrap|camo|decal|livery)\b", re.I)

# Weapon mods. Two passes: a strict mod-slot name, and a looser "_mod_ ... slot"
# for compound names (TubeBarrel, AlienMag, Arrow, ...). The last three keywords
# rescue named mods with no slot word: AlienBlaster AlienMag, Assaultron Head,
# and Sheepsquatch weapon mods.
_RX_WEAP_SLOT = re.compile(
    r"_mod_.*?_(receiver|muzzle|barrel|magazine|grip|stock\d*|scope|sights?|"
    r"bayonet|ammo|sling|capacitor|nozzle|blade|hammer)(?=_|$)", re.I)
_RX_WEAP_MOD2 = re.compile(
    r"_mod_.*(arrow|barrel|magazine|_mag\b|mag_|ammo|receiver|muzzle|grip|stock|"
    r"scope|sight|bayonet|nozzle|capacitor|drum|clip|conversion|prime|"
    r"alienmag|assaultronhead|sheepsquatch)", re.I)

# Apparel words. Override the armour TYPE for cosmetic headwear/clothing.
_RX_APPAREL = re.compile(
    r"headwear|(^|_)clothes|outfit|costume|uniform|mask|bandana|_hood|dress|"
    r"(^|_)hat(_|\d)", re.I)

_RX_BACKPACK = re.compile(r"backpack", re.I)

# Thrown / gadget weapons whose EDID has no _weapon_ or slot. "floater" is here
# (not in Fishing): every "Floater ..." plan is a Floater-creature grenade/tube,
# i.e. a thrown weapon -- there are no fishing "floater" items in the data.
_RX_THROWN = re.compile(
    r"grenade|molotov|_mine\b|beartrap|bear_trap|flashbang|soundmaker|throwing|"
    r"dynamite|tomahawk|caltrop|frag\b|nukagrenade|floater", re.I)

# Actual weapon by name / slotless mod.
_RX_WEAPON = re.compile(
    r"(^|_)weapon(_|$)|_melee_|_ranged_|(^|_)(gun|rifle|pistol|shotgun|launcher|"
    r"grenade|mine|bow|compoundbow|chainsaw|axe|sword|knife|club|spear|bat)(_|\d|$)", re.I)

_RX_ARMOUR = re.compile(r"(^|_)(armor|armour|powerarmor|power_armor)(_|$)", re.I)
# CAMP placeables. "stein" (souvenir beer steins) and "bugzapper" are placeable
# display/decor objects, not recipes/weapons.
_RX_CAMP   = re.compile(r"workshop|furniture|floordecor|_decor|_camp_|photomode|photo_frame|frame_|stein|bugzapper|bug_zapper", re.I)
_RX_CONSUM = re.compile(
    r"(^|_)(food|chem|drink|cook|meal|nuka|brew|stew|burger|whiskey|gin|wine|"
    r"tea|coffee|soup|pie|cake|roast|meat|jerky|bake)", re.I)

# Weapon mod recognised from the EDID when the crafted object never resolved to
# a co_Weapon_ record. Some stamp/gold-vendor weapon mods (e.g. the Cryolator
# Cold Surge / Hypothermic Muzzle, Magazine and Polar Lobber mods) come through
# with an empty cobj/cnam, so the earlier _mod_ / _weapon_ tests can't see them
# and they fall to Recipes. These fire LATE (only after every resolved bucket --
# weapon, apparel, armour, CAMP -- has had its say), and _RX_NOT_WEAPON blocks
# power-armour / underarmour mods from being dragged in. WEAPMOD_SLOT catches
# a weapon mod-slot term (muzzle / magazine / capacitor / ...); WEAPON_NAME
# catches a named weapon whose mod carries no slot word (Cryolator Polar Lobber).
_RX_NOT_WEAPON   = re.compile(r"armor|armour|underarmor|power_armor", re.I)
_RX_WEAPMOD_SLOT = re.compile(
    r"muzzle|receiver|bayonet|silencer|suppressor|magazine|capacitor|nozzle|"
    r"lobber|sniperbarrel|(^|_)barrel(_|$)", re.I)
_RX_WEAPON_NAME  = re.compile(
    r"cryolator|railway|(^|_)gauss|gatling|minigun|(^|_)flamer|harpoon|crossbow|"
    r"compoundbow|alienblaster|missilelauncher|fatman|handmade|combatrifle|"
    r"combatshotgun|assaultrifle|huntingrifle|leveraction|pumpaction|doublebarrel|"
    r"blackpowder|(^|_)revolver|submachinegun|autoaxe|shishkebab|powerfist|"
    r"plasmacutter|(^|_)ripper(_|$)|chainsaw", re.I)


def classify_group(item):
    """Category key for one plan_master item. See CATEGORY CLASSIFICATION."""
    plan_fid = ((item.get("plan_item") or {}).get("formid") or "").upper()
    if plan_fid in GROUP_OVERRIDES:
        return GROUP_OVERRIDES[plan_fid]

    typ       = str(item.get("type") or "").lower()
    plan_edid = ((item.get("plan_item") or {}).get("edid") or "")
    cobj_edid = ((item.get("cobj") or {}).get("edid") or "")
    cnam      = (item.get("cnam") or {})
    cnam_edid = (cnam.get("edid") or "")
    cnam_sig  = (cnam.get("sig") or "")
    blob      = f"{cobj_edid} {cnam_edid} {plan_edid}"
    name      = re.sub(r"^(plan|recipe):\s*", "", str(item.get("name") or ""), flags=re.I)
    has_box   = bool(item.get("has_image_box"))

    # 1. FISHING tackle -- the rod and its mods (_mod_FishingRod_*). Before the
    #    weapon rules, because the rod/its mods carry _Weapon_ in the COBJ.
    if _RX_FISHING.search(blob):
        return "fishing"

    # 2. SKINS -- paints/skins leak into weapon/armour otherwise.
    if _RX_SKIN_BLOB.search(blob) or _RX_SKIN_NAME.search(name):
        return "skin"

    # 3. Weapon MODS (slot names, then looser compound-name pass).
    if _RX_WEAP_SLOT.search(blob) or _RX_WEAP_MOD2.search(blob):
        return "weapon"

    # 4. APPAREL words -- override armour-typed hats / masks / headwear.
    if _RX_APPAREL.search(blob):
        return "apparel"

    # 5. Backpack mods.
    if _RX_BACKPACK.search(blob):
        return "backpack-mod"

    # 6. Actual weapons.
    if cnam_sig == "WEAP":
        return "weapon"
    if _RX_THROWN.search(blob) or _RX_THROWN.search(name):
        return "weapon"
    if _RX_WEAPON.search(blob):
        return "weapon"

    # 7. Resolved TYPE buckets (the upstream builder matched a real record).
    if typ == "weapon":
        return "weapon"
    if typ == "apparel":
        return "apparel"
    if typ == "backpack-mod":
        return "backpack-mod"
    if typ == "armour":
        return "armour"

    # 8. Armour words.
    if _RX_ARMOUR.search(blob):
        return "armour"

    # 9. CAMP placeables.
    if _RX_CAMP.search(blob) or cnam_sig in ("ACTI", "FURN"):
        return "camp"

    # 10. Weapon mod whose crafted object never resolved (cobj/cnam empty), seen
    #     only in the EDID. LATE so the resolved buckets above win; guarded so
    #     power-armour / underarmour mods can't be dragged in.
    if not _RX_NOT_WEAPON.search(blob) and (
            _RX_WEAPMOD_SLOT.search(blob) or _RX_WEAPON_NAME.search(blob)):
        return "weapon"

    # 11. Consumable recipes.
    if cnam_sig == "ALCH" or _RX_CONSUM.search(blob):
        return "recipe"

    # 12. Fallback: a placeable (has an image box) is CAMP, else a recipe.
    return "camp" if has_box else "recipe"


# Back-compat alias: the previous module exposed group_for(); keep the name
# pointing at the new classifier in case anything still imports it.
group_for = classify_group


def plan_title(item):
    return str(item.get("display_name") or item.get("name") or item.get("id") or "")


def book_plan_ids(path):
    """{FormID: FULL} for every learnable plan/recipe BOOK in one export."""
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


# ---------------------------------------------------------------------------
# SEASON HELPERS
# ---------------------------------------------------------------------------
_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _parse_dmy(s):
    """'15/09/2026' (D/M/YYYY, no zero-pad) -> datetime.date, or None."""
    s = (s or "").strip().strip('"')
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def _fmt_display(d):
    """date -> '15 Sep 2026' (matches the site's example format)."""
    return f"{d.day} {_MONTH_ABBR[d.month - 1]} {d.year}" if d else None


def _ym(d):
    """year-month index for month-granularity comparison."""
    return d.year * 12 + (d.month - 1)


def load_seasons(path=SEASONS_TSV):
    """[{key, number, name, start(date), end(date)}], oldest first."""
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            start = _parse_dmy(r.get("StartDate"))
            end   = _parse_dmy(r.get("EndDate"))
            if not start:
                continue
            rows.append({
                "key":    (r.get("SeasonKey") or "").strip(),
                "number": (r.get("SeasonNumber") or "").strip(),
                "name":   (r.get("SeasonName") or "").strip(),
                "start":  start,
                "end":    end,
            })
    rows.sort(key=lambda s: s["start"])
    return rows


def resolve_season_baseline(data_dir, newest_f, seasons):
    """(baseline_file, season, all_before) for the live season-dated diff.

    active season = latest season whose start-month <= the newest export month.
    baseline      = newest export whose month is strictly before that start-month
                    (month-floor). None -> empty page.
    """
    hits = tsv_source.all_matching(
        os.path.join(data_dir, BOOK_GLOB), exclude=BOOK_EXCLUDE)  # oldest -> newest
    newest_ym = _ym(tsv_source.export_date(newest_f))

    active = None
    for s in seasons:                         # seasons is oldest-first
        if _ym(s["start"]) <= newest_ym:
            active = s
    if active is None:
        return None, None, []

    start_ym = _ym(active["start"])
    before = [h for h in hits if _ym(tsv_source.export_date(h)) < start_ym]
    baseline = before[-1] if before else None
    return baseline, active, before


def resolve_baseline(data_dir, baseline_dir, cur_ids, newest_f):
    """(baseline_file, mode, skipped) -- the ORIGINAL shared resolver.

    KEPT UNCHANGED because build_underarmour_json.py imports and calls this as
    bnp.resolve_baseline(data_dir, baseline_dir, cur, newest_f) and unpacks the
    (base_file, mode, skipped) triple. Do NOT change its signature or return
    shape without updating build_underarmour_json.py in the same commit.

    mode "vs-live"     : baseline_dir given -> the newest export THERE, no
                         walk-back (the two roots are different channels).
    mode "vs-previous" : single root -> the previous export, walking BACK past
                         any whose plan roster is identical to the newest
                         (a re-upload of the same game build).
    """
    if baseline_dir:
        base_hits = tsv_source.all_matching(
            os.path.join(baseline_dir, BOOK_GLOB), exclude=BOOK_EXCLUDE)
        if not base_hits:
            raise SystemExit(
                f"[new-plans] --baseline-dir {baseline_dir} holds no BOOK export. "
                f"Publishing every plan as new would be a lie -- fix the path."
            )
        return base_hits[-1], "vs-live", []

    hits = tsv_source.all_matching(
        os.path.join(data_dir, BOOK_GLOB), exclude=BOOK_EXCLUDE)
    newest_abs = os.path.abspath(newest_f)
    older = [h for h in hits if os.path.abspath(h) != newest_abs]
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
                    help="PTS only: diff against the newest export HERE (tsv_live) "
                         "instead of the season-start baseline. Presence of this "
                         "flag selects PTS 'vs-live' mode.")
    ap.add_argument("--outdir", default=DIST,
                    help="output dir (the PTS job relocates dist/ -> dist/pts/ afterwards)")
    ap.add_argument("--master", default="",
                    help="plan_master.json to read rows from (default: <outdir>/plan_master.json)")
    ap.add_argument("--out", default="",
                    help="explicit output file (default: <outdir>/new_plans.json)")
    ap.add_argument("--seasons", default=SEASONS_TSV,
                    help="season dates TSV (default: tsv/fallout76_seasons.tsv)")
    args = ap.parse_args(argv)

    outdir = args.outdir
    master_path = args.master or os.path.join(outdir, "plan_master.json")
    out_path    = args.out    or os.path.join(outdir, "new_plans.json")

    newest_f = tsv_source.newest(os.path.join(args.data_dir, BOOK_GLOB),
                                 exclude=BOOK_EXCLUDE, required=False)
    if not newest_f:
        raise SystemExit(f"[new-plans] no BOOK export found under {args.data_dir}")
    new_ids = book_plan_ids(newest_f)

    season_block = None
    baseline_block = {}

    if args.baseline_dir:
        # ---- PTS: newest PTS vs newest LIVE ---------------------------------
        prev_f, mode, _sk = resolve_baseline(
            args.data_dir, args.baseline_dir, new_ids, newest_f)  # -> mode "vs-live"
        old_ids = book_plan_ids(prev_f)
        added = set(new_ids) - set(old_ids)
        baseline_block = {
            "newest": os.path.basename(newest_f),
            "previous": os.path.basename(prev_f),
            "newest_plan_count": len(new_ids),
            "previous_plan_count": len(old_ids),
        }
        print(f"[new-plans] mode    : vs-live (PTS)")
        print(f"[new-plans] newest  : {os.path.basename(newest_f)} ({len(new_ids)})")
        print(f"[new-plans] baseline: {os.path.basename(prev_f)} ({len(old_ids)})")
    else:
        # ---- LIVE: season-dated ---------------------------------------------
        seasons = load_seasons(args.seasons)
        if seasons:
            mode = "vs-season"
            prev_f, active, _before = resolve_season_baseline(
                args.data_dir, newest_f, seasons)
            if active is None:
                # No season covers the newest export -- publish empty, not "all new".
                old_ids, added = {}, set()
                print("[new-plans] no season matches the newest export -- empty page.")
                season_block = None
            else:
                season_block = {
                    "key":    active["key"],
                    "number": active["number"],
                    "name":   active["name"],
                    "start":  active["start"].isoformat(),
                    "end":    active["end"].isoformat() if active["end"] else None,
                    "start_display": _fmt_display(active["start"]),
                    "end_display":   _fmt_display(active["end"]),
                }
                if prev_f:
                    old_ids = book_plan_ids(prev_f)
                    added = set(new_ids) - set(old_ids)
                else:
                    old_ids, added = {}, set()
                    print(f"[new-plans] no export before {active['key']} start "
                          f"-- empty page (never 'everything is new').")
                print(f"[new-plans] mode    : vs-season")
                print(f"[new-plans] season  : {active['key']} {active['name']} "
                      f"({season_block['start_display']} - {season_block['end_display']})")
                print(f"[new-plans] newest  : {os.path.basename(newest_f)} ({len(new_ids)})")
                print(f"[new-plans] baseline: "
                      f"{os.path.basename(prev_f) if prev_f else '(none)'} ({len(old_ids)})")
            baseline_block = {
                "newest": os.path.basename(newest_f),
                "previous": os.path.basename(prev_f) if prev_f else None,
                "newest_plan_count": len(new_ids),
                "previous_plan_count": len(old_ids),
            }
        else:
            # Season TSV unreadable -- fall back to the old vs-previous behaviour
            # rather than break the page.
            prev_f, mode, skipped_dupes = resolve_baseline(
                args.data_dir, "", new_ids, newest_f)  # -> mode "vs-previous"
            if prev_f:
                old_ids = book_plan_ids(prev_f)
                added = set(new_ids) - set(old_ids)
            else:
                old_ids, added = {}, set()
            baseline_block = {
                "newest": os.path.basename(newest_f),
                "previous": os.path.basename(prev_f) if prev_f else None,
                "newest_plan_count": len(new_ids),
                "previous_plan_count": len(old_ids),
                "skipped_duplicates": skipped_dupes,
            }
            print("[new-plans] WARNING season TSV unreadable -- fell back to vs-previous.")
            for s in skipped_dupes:
                print(f"[new-plans] skipped (console only): {s} -- identical roster")

    # -- pull the finished rows out of plan_master ---------------------------
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
        # Cut plans are KEPT, not dropped. The renderer marks them with the
        # "✕ Cut" pill, greys the row, hides the checkbox and leaves them out
        # of the progress total (item.cut / item.cut_reason). Dropping them
        # here would silently hide content Duchess asked to have shown.
        row = dict(it)
        row["group"] = classify_group(it)
        row["is_new"] = True
        rows.append(row)

    # Art. This page copies its rows out of plan_master, so in principle it
    # inherits whatever pictures that build resolved — but it only inherits them
    # if plan_master was written by a build that had them, and a CI run has
    # already shipped this page with every row blank for exactly that reason.
    # Resolving here as well costs nothing (no rates, no rebuild, pure lookup)
    # and makes the page independent of what ran before it.
    try:
        idx, staged = plan_images.load(outdir, args.data_dir)
        plan_images.report(plan_images.attach(rows, idx, staged))
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"[new-plans] WARNING image resolve skipped: {exc}", file=sys.stderr)

    groups = []
    for key, label in sorted(GROUPS.items(), key=lambda kv: kv[1].lower()):
        members = sorted((r for r in rows if r["group"] == key),
                         key=lambda r: plan_title(r).lower())
        if members:
            groups.append({"key": key, "label": label, "count": len(members),
                           "items": members})

    out = {
        "version": 2,
        "generated": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "season": season_block,
        "baseline": baseline_block,
        "count": len(rows),
        "skipped_not_in_master": sorted(skipped),
        "groups": groups,
    }

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[new-plans] wrote {out_path} ({len(rows)} plans in {len(groups)} groups)")
    for g in groups:
        print(f"   {g['label']}: {g['count']}")
    if skipped:
        print(f"[new-plans] {len(skipped)} added plan(s) had no plan_master row: "
              f"{', '.join(skipped[:8])}" + (" ..." if len(skipped) > 8 else ""))
    return out


if __name__ == "__main__":
    main()
