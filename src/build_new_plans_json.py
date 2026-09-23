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
import plan_pins                              # manual event-recycled-plan pins (data/new_plans_pins.tsv)

# -- the BOOK export selector, shared -----------------------------------------
BOOK_GLOB    = "BOOK_Export_*.tsv"
BOOK_EXCLUDE = "Locations"

# -- the RECIPE side ----------------------------------------------------------
# A BOOK roster diff can only ever see a new PLAN ITEM. It cannot see a patch
# that adds a recipe taught some other way — by a challenge, by claiming a
# workshop — because no BOOK is involved, and it cannot see one whose plan book
# Bethesda shipped disabled months earlier. That is exactly how the Pint-Sized
# Slasher radio and fishing bobber reached the live game without this page ever
# mentioning them. Diffing the COBJ roster as well is what closes it.
COBJ_GLOB = "COBJ_Export_*.tsv"

# -- group definitions --------------------------------------------------------
# key -> label.
#
# "backpack" is the WHOLE backpack: the pack itself, its mods, its flairs and
# its skins. They were split across "Backpack Mods", Skins and Recipes, which
# put "Cat Leveling Backpack" and "Cat Leveling Backpack Flair" in two different
# parts of the page. A reader looking for backpack things looks once.
GROUPS = {
    "apparel":      "Apparel",
    "armour":       "Armour",
    "armour-mod":   "Armour Mod",
    "backpack":     "Backpack",
    "camp":         "CAMP",
    "fishing":      "Fishing",
    "misc-display": "MISC Display",
    "recipe":       "Recipes",
    "skin":         "Skins",
    "weapon":       "Weapons",
    "weapon-mod":   "Weapon Mod",
}

# Display order on the page: A-Z by LABEL, derived from GROUPS rather than
# hand-listed, so a new category sorts itself and cannot be forgotten here.
# (It used to pair each base with its mods — Weapons, Weapon Mod, Armour,
# Armour Mod — which Duchess replaced with plain A-Z.) Sorted case-insensitively
# so "CAMP" files under C rather than ahead of every lowercase-second-letter
# label. The renderer follows this order from the JSON and holds no category
# list of its own, so this is the single source of truth.
#
# A group with no plans in it is NOT emitted at all (see the build below) —
# every heading on the page has rows under it.
GROUP_ORDER = sorted(GROUPS, key=lambda k: GROUPS[k].lower())

import new_plans_sources   # page grouping by source (see module)

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

# Display replicas -- a MISC souvenir copy of an item that also exists in usable
# form, built at a tinker's bench and put on a shelf. They read as duplicates
# otherwise: the game teaches the Pint-Sized Slasher rod bobber and its display
# copy from the same challenge, and Bethesda named the copy "Pint-Sized Slasher
# Fishing Bobber" -- so the page showed what looked like the same bobber twice,
# once under Fishing and once under CAMP. Their own group answers it in the
# heading. Deliberately narrow: only a MISC row that says Display or Replica, or
# one plan_subpages already filed under "Display Replicas". Souvenir MISC items
# that copy nothing -- beer steins, fossils, taxidermy moths -- are ordinary
# CAMP decor and stay in CAMP.
_RX_REPLICA = re.compile(r"(^|_)(display|replica)(_|\b)", re.I)


def _is_display_replica(item, cnam_sig, blob):
    """A souvenir copy of something that also exists in usable form.

    NOT keyed on cnam_sig any more. The CI build resolves these rows with an
    empty signature (only 31 rows carry ""; "MISC" appears nowhere), so the
    sig test silently stopped matching and the Pint-Sized Slasher display
    bobber fell back into Recipes. What it keys on instead:

      * `plan_page_group` -- plan_subpages already files these under "Display
        Replicas", and that decision is made from the same game data.
      * a created record that is BOTH a display and a MISC item
        (`..._Display_..._Misc`), or a name that says Replica outright.

    The `_Misc` tail is what keeps the CAMP display furniture out: a glazed pot
    is `SSE_Recipe_Workshop_Display_MediumGlazedPot` with no created record at
    all, so it stays CAMP where it belongs.
    """
    if (item.get("plan_page_group") or "") == "Display Replicas":
        return True
    cnam_edid = ((item.get("cnam") or {}).get("edid") or "")
    if re.search(r"_Display_.*_Misc$", cnam_edid, re.I):
        return True
    name = str(item.get("name") or "")
    return bool(_RX_REPLICA.search(name) and re.search(r"replica", name, re.I))

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

# Backpack: the pack, its mods, its flairs, its skins. Tested against the EDID
# blob AND the display name — "Cat Leveling Backpack Flair" says backpack only in
# its FULL name, which is why the flairs were landing in Recipes.
_RX_BACKPACK = re.compile(r"backpack|back_pack", re.I)

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
# An explicit "mod" marker in a weapon row's name/EDID: the "_mod_" path
# segment, or the standalone word "Mod"/"Mods" the game appends to a mod plan's
# FULL name ("Cryolator Polar Lobber Mod", "Cryolator Magazine Mods"). Used with
# _RX_WEAPMOD_SLOT to tell a vendor/stamp weapon MOD (empty cnam, type "weapon")
# apart from a base weapon at the type bucket.
_RX_WEAP_MODWORD = re.compile(r"_mod_|\bmods?\b", re.I)
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

    # 0. MISC DISPLAY -- souvenir copies. First, because a replica carries the
    #    real item's own words (a rod bobber replica matches the fishing rule,
    #    a weapon replica would match the weapon rules) and would be filed as
    #    the thing it copies.
    if _is_display_replica(item, cnam_sig, blob):
        return "misc-display"

    # 1. FISHING tackle -- the rod and its mods (_mod_FishingRod_*). Before the
    #    weapon rules, because the rod/its mods carry _Weapon_ in the COBJ.
    if _RX_FISHING.search(blob):
        return "fishing"

    # 2. BACKPACK -- the pack, its mods, its flairs and its skins, one group.
    #    BEFORE skins and before the mod block: a backpack paint matches the
    #    skin rule and a backpack mod is OMOD-created, so either would claim it
    #    first and split the group in two. image_dir is included because the
    #    pipeline already resolves a "Backpack Flair" into the backpack folder
    #    even when its EDID never says backpack.
    if (_RX_BACKPACK.search(blob) or _RX_BACKPACK.search(name)
            or (item.get("image_dir") or "").strip().lower() == "backpack"
            or str(item.get("type") or "").lower() == "backpack-mod"):
        return "backpack"

    # 3. SKINS -- paints/skins leak into weapon/armour otherwise.
    if _RX_SKIN_BLOB.search(blob) or _RX_SKIN_NAME.search(name):
        return "skin"

    # 4. APPAREL words -- override armour-typed hats / masks / headwear. Before
    #    the mod block: a cosmetic mask/outfit is its own category, not a mod.
    if _RX_APPAREL.search(blob):
        return "apparel"

    # 5. MODS -> Weapon Mod / Armour Mod. Every OMOD-creating recipe is a mod,
    #    and mods now live in their OWN categories rather than under Weapons /
    #    Armour (Duchess's call: base items and mods separate). cnam_sig ==
    #    "OMOD" is the data-driven signal; the _RX_WEAP_* EDID passes catch
    #    weapon mods whose created object never resolved (empty cobj/cnam). This
    #    is where the scrap-to-learn mods (weapon-typed recipe rows, cnam OMOD)
    #    land, alongside every book/vendor weapon mod that used to read "Weapons".
    #    Skins/paints already returned "skin" above, so they are not pulled in.
    # MODS -> Weapon Mod / Armour Mod. Every OMOD-creating recipe is a mod, but
    # backpack flairs, camp/fishing gizmos and cosmetic apparel are OMOD-created
    # too and have homes of their own. image_dir is the folder the pipeline
    # already resolved this row into (page_folder in plan_images), so it is the
    # authoritative "what is this" — a "Backpack Flair" whose EDID never says
    # "backpack" still lands in image_dir "backpack". Rows bound for a non-mod
    # folder fall through to the buckets below; the rest split weapon vs armour.
    folder = (item.get("image_dir") or "").strip().lower()
    _ARMOUR_FOLDERS = {"body-armour", "power-armour", "underarmour"}
    _NON_MOD_FOLDERS = {"backpack", "apparel", "workshop", "recipes",
                        "fishing-rod", "snowglobe", "photomode", "misc-display"}
    is_mod = (cnam_sig == "OMOD"
              or _RX_WEAP_SLOT.search(blob) or _RX_WEAP_MOD2.search(blob)
              # A mod-slot word (Muzzle, Magazine, Lobber, Receiver, ...) or an
              # explicit "Mod"/"_mod_" catches the vendor/stamp weapon mods whose
              # created object never resolved and whose EDID says "_co_Weapon_"
              # rather than "_mod_" (e.g. SCORE_S26_co_Weapon_Cryolator_
              # ColdSurgeMuzzle_StampVendor) — without this they read as base
              # weapons because "_weapon_" matches _RX_WEAPON below.
              or _RX_WEAPMOD_SLOT.search(blob) or _RX_WEAPMOD_SLOT.search(name)
              or _RX_WEAP_MODWORD.search(blob) or _RX_WEAP_MODWORD.search(name))
    if is_mod and typ not in ("backpack-mod", "apparel") \
            and folder not in _NON_MOD_FOLDERS:
        # The resolved folder decides weapon vs armour first; then the row's own
        # type; then the EDID as a last resort. type is trusted over an EDID
        # keyword because an "Armor Piercing" RECEIVER is a weapon mod whose EDID
        # contains "armor", which a blind _RX_ARMOUR test would misfile.
        if folder in _ARMOUR_FOLDERS or typ == "armour":
            return "armour-mod"
        if folder == "weapons" or typ == "weapon":
            return "weapon-mod"
        if _RX_ARMOUR.search(blob) or _RX_NOT_WEAPON.search(blob):
            return "armour-mod"
        return "weapon-mod"

    # 6. Actual base weapons (creates a WEAP, or a thrown / named weapon).
    if cnam_sig == "WEAP":
        return "weapon"
    if _RX_THROWN.search(blob) or _RX_THROWN.search(name):
        return "weapon"
    if _RX_WEAPON.search(blob):
        return "weapon"

    # 7. Resolved TYPE buckets (the upstream builder matched a real record).
    #    A weapon-TYPED row is ambiguous: `weapon` covers both base weapons and
    #    the vendor/stamp weapon mods whose created object never resolved (empty
    #    cnam), so step 5's OMOD test could not see them. A mod-slot word in the
    #    name/EDID (Muzzle, Magazine, Receiver, Lobber, ... or an explicit
    #    "Mod"/"_mod_") is what tells the two apart — a base weapon carries none.
    #    This is what routes "Cryolator Cold Surge Muzzle" to Weapon Mod while
    #    "Cryolator" itself stays in Weapons.
    if typ == "weapon":
        if _RX_WEAPMOD_SLOT.search(blob) or _RX_WEAPMOD_SLOT.search(name) \
                or _RX_WEAP_MODWORD.search(blob) or _RX_WEAP_MODWORD.search(name):
            return "weapon-mod"
        return "weapon"
    if typ == "apparel":
        return "apparel"
    if typ == "backpack-mod":
        return "backpack"
    if typ == "armour":
        return "armour"

    # 8. Armour words (base armour piece).
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
        return "weapon-mod"

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


def recipe_ids(path):
    """{COBJ FormID: created record name} for every recipe that makes something.

    A CNAM-less COBJ is a condition proxy or a dead stub — nothing a row could
    be named after, and nothing a reader could tick.
    """
    out = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid = (row.get("COBJ_FormID") or "").strip().upper()
            name = (row.get("CNAM_FULL") or "").strip()
            if fid and name:
                out[fid] = name
    return out


def resolve_recipe_baseline(data_dir, baseline_dir, book_baseline_f):
    """The COBJ export that pairs with the BOOK baseline this page already chose.

    Pinned to the BOOK baseline's own date rather than resolved independently,
    so both halves of the diff describe the SAME patch. Resolving them apart
    would let a COBJ export from a different month decide what counts as new,
    and the page would report recipes against a boundary its own heading does
    not describe.

    The two record types are exported separately and do not always arrive
    together — through most of September 2026 the live BOOK export was current
    while COBJ was three months stale — so the newest COBJ at or before that
    date is used, and a channel with no COBJ export simply contributes nothing.
    """
    root = baseline_dir or data_dir
    hits = tsv_source.all_matching(os.path.join(root, COBJ_GLOB))
    if not hits:
        return None
    if baseline_dir:
        return hits[-1]                       # vs-live: the newest LIVE recipes
    if not book_baseline_f:
        return None
    cutoff = tsv_source.export_date(book_baseline_f)
    older = [h for h in hits if tsv_source.export_date(h) <= cutoff]
    return older[-1] if older else None


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

    # -- the recipe half of the diff -----------------------------------------
    # Same baseline, different record type. Kept separate from `added` so the
    # console says which half found what, and so a channel missing a COBJ export
    # degrades to the old BOOK-only behaviour instead of failing.
    prev_book_f = baseline_block.get("previous")
    prev_book_path = (os.path.join(args.baseline_dir or args.data_dir, prev_book_f)
                      if prev_book_f else None)
    added_recipes = set()
    new_recipe_f = tsv_source.newest(os.path.join(args.data_dir, COBJ_GLOB),
                                     required=False)
    base_recipe_f = resolve_recipe_baseline(
        args.data_dir, args.baseline_dir, prev_book_path)
    if new_recipe_f and base_recipe_f and \
            os.path.abspath(new_recipe_f) != os.path.abspath(base_recipe_f):
        new_recipes = recipe_ids(new_recipe_f)
        old_recipes = recipe_ids(base_recipe_f)
        added_recipes = set(new_recipes) - set(old_recipes)
        print(f"[new-plans] recipes : {os.path.basename(new_recipe_f)} "
              f"({len(new_recipes)}) vs {os.path.basename(base_recipe_f)} "
              f"({len(old_recipes)}) -> {len(added_recipes)} added")
        baseline_block["recipes_newest"] = os.path.basename(new_recipe_f)
        baseline_block["recipes_previous"] = os.path.basename(base_recipe_f)
    else:
        print("[new-plans] recipes : no usable COBJ baseline — plan items only")

    # -- pull the finished rows out of plan_master ---------------------------
    with open(master_path, encoding="utf-8") as f:
        master = json.load(f)

    # -- manual pins ---------------------------------------------------------
    # THE ONE THING THAT MAKES THE PINS SURVIVE CI. The change diff
    # (plan_changes.py) only runs inside reenrich_plan_master.py, and the patch
    # workflow does NOT run reenrich — it rebuilds plan_master with
    # build_plan_obtain_json.py (no `changes` field at all) and then runs THIS
    # script. So a pin applied only in reenrich is clobbered on every CI build
    # and the New Plans page comes back with count_changed:0. Applying the pins
    # HERE, against the in-memory master rows this build reads, force-attaches
    # the same `changes` entry (and route `new` flag) the diff would have
    # produced, so the existing "changed" loop below emits the pinned plans as
    # ↻ Changed rows whether or not plan_master carries any changes of its own.
    # De-duped against real changes and inert once a pin's EndDate passes; the
    # channel (live/pts) is derived from --data-dir so a live pin never shows on
    # the PTS "what's coming next" page.
    plan_pins.report(plan_pins.apply(master.get("items", []), tsv_dir=args.data_dir),
                     stream=sys.stdout)

    by_fid, by_cobj = {}, {}
    for it in master.get("items", []):
        fid = ((it.get("plan_item") or {}).get("formid") or "").upper()
        if fid:
            by_fid[fid] = it
        co = ((it.get("cobj") or {}).get("formid") or "").upper()
        if co:
            by_cobj.setdefault(co, it)

    rows, skipped, taken = [], [], set()
    for fid in added:
        it = by_fid.get(fid)
        if not it:
            skipped.append(new_ids.get(fid, fid))
            continue
        taken.add(it.get("id"))
        # Cut plans are KEPT, not dropped. The renderer marks them with the
        # "✕ Cut" pill, greys the row, hides the checkbox and leaves them out
        # of the progress total (item.cut / item.cut_reason). Dropping them
        # here would silently hide content Duchess asked to have shown.
        row = dict(it)
        row["group"] = classify_group(it)
        row["is_new"] = True
        rows.append(row)

    # -- recipes the patch added that no plan row covered --------------------
    # A recipe whose plan book is older than the patch (or was never enabled)
    # reaches the page here and nowhere else. `taken` keeps a row that arrived
    # through its plan item from being listed twice.
    from_recipes = 0
    for co_fid in added_recipes:
        it = by_cobj.get(co_fid)
        if not it or it.get("id") in taken:
            continue
        taken.add(it.get("id"))
        row = dict(it)
        row["group"] = classify_group(it)
        row["is_new"] = True
        rows.append(row)
        from_recipes += 1
    if from_recipes:
        print(f"[new-plans] {from_recipes} row(s) found by the recipe diff alone")

    # -- plans that were already here and have since CHANGED -----------------
    # The roster diff above can only ever see a FormID that was not in the
    # previous export. A plan that has always been here and has since become
    # tradeable, or picked up a second source, keeps its FormID and is
    # invisible to it — which is the whole reason src/plan_changes.py exists.
    #
    # They go into the SAME type groups as the new plans rather than a section
    # of their own (Duchess's call), and carry `is_new: False` so the renderer
    # gives them the ↻ Changed pill instead of the ★ NEW star. A plan that is
    # both new this patch and carries change notes is only ever listed once —
    # `added` wins, because "added" is the stronger statement.
    changed = 0
    for it in master.get("items", []):
        fid = ((it.get("plan_item") or {}).get("formid") or "").upper()
        if it.get("id") in taken or not (it.get("changes") or []):
            continue
        if fid and fid in added:
            continue
        row = dict(it)
        row["group"] = classify_group(it)
        row["is_new"] = False
        rows.append(row)
        changed += 1
    if changed:
        print(f"[new-plans] {changed} plan(s) changed since the last build")

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

    # Grouped by SOURCE since 23 Sep 2026 (Duchess): Daily Ops, Minerva, Stamps,
    # Quests, Events ... — see new_plans_sources.py. The type classification
    # above still runs; it now feeds each row's type pill (`type_label`).
    groups = new_plans_sources.group_rows(rows, outdir,
                                          lambda r: plan_title(r).lower(), GROUPS,
                                          data_dir=args.data_dir)

    out = {
        "version": 2,
        "generated": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "season": season_block,
        "baseline": baseline_block,
        "count": len(rows),
        "count_new": sum(1 for r in rows if r.get("is_new")),
        "count_changed": sum(1 for r in rows if not r.get("is_new")),
        "skipped_not_in_master": sorted(skipped),
        "grouping": "source",
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
        # An empty page is a valid outcome — two exports can carry the same
        # roster. An empty page while the roster diff found plans is NOT: it
        # means plan_master was built from an older export than the BOOK export
        # this page just read, so every new plan was dropped for having no row.
        # The two look identical on the page and in the JSON, hence this line.
        # Seen 16 Sept 2026: the PTS BOOK gained 149 plans and the PTS
        # plan_master was still the 22 August build, so the page published a
        # confident "nothing new" over a patch that added 149 plans.
        if not rows:
            print(f"[new-plans] *** THE PAGE IS EMPTY BUT THE ROSTER DIFF FOUND "
                  f"{len(skipped)} PLAN(S). plan_master is older than "
                  f"{os.path.basename(newest_f)} — rebuild it, or this page "
                  f"says 'nothing new' about a patch that added "
                  f"{len(skipped)}. ***")
    return out


if __name__ == "__main__":
    main()
