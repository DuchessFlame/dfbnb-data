#!/usr/bin/env python3
r"""
build_plan_obtain_json.py — Plan Checklists "How to Obtain" pipeline.

Builds dist/plan_master.json for the DF/BNB plan-checklist pages
(apparel / armour / backpack-mod / recipe / weapon), rendered by
df-bnb-plan-checklists.js (renderChecklist). Every rate is resolved with
rng76 (never a bare ChanceNone, never a hardcoded FormID).

ROSTER
------
Learnable plans are BOOK records whose FULL name starts "Plan: " or
"Recipe: " (KYWD ObjectTypeRecipe). The BOOK FormID is what actually drops
in leveled lists, so it is the rng76 TARGET.

BOOK -> created object (category + image box)
--------------------------------------------
The LVLI entry that yields a plan carries a
`Target.HasLearnedRecipe(... co_X [COBJ:xxxxxxxx] ...)` condition. That COBJ
is the recipe the plan teaches; its CNAM (created object) FormID resolved to a
record signature tells us physical-vs-mod:
  * created OMOD (or a mod/paint recipe)          -> MOD    -> NO image box
  * created WEAP/ARMO/FURN/CONT/STAT/MSTT/ACTI/MISC -> PHYSICAL -> image box
  * Workshop/CAMP recipe (co_Workshop_*, CondProxy) -> PHYSICAL -> image box
  * consumable (ALCH food/chem)                    -> no placeable -> NO image box
When the created object can't be resolved, has_image_box is False (safe: no
broken image slot) and the fact is reported.

MULTI-ROUTE OBTAIN (container-type -> drop%, generalised)
---------------------------------------------------------
Reuses spawns_engine.sources.get_sources (LVLI up-closure + placed holders)
and the farming Containers resolver. Every distinct source/route is a line
with the plan's rng76-resolved appearance chance for that source. Distinct
rates are listed separately, identical rates dedupe, 0% is dropped, sorted by
rate desc. Buckets: container / vendor / creature / event-quest / fixed.

BACKPACK: MOD vs SKIN vs FLAIR
------------------------------
Every backpack plan is an OMOD recipe, so the backpack-mod bucket used to hold
functional mods, paint skins and cosmetic flair together. `backpack_class`
splits them on the created OMOD's EDID (`_Effect_` / `_Material_` / `_Flair`),
falling back to the plan name for the few with no CNAM link. Only class "mod"
is published — /df/plan-checklists/backpack-mod/ is a mod checklist, not a
cosmetics list. Skins and flair are dropped from the roster and counted in the
report under "backpack_cosmetic_dropped".

OUTPUT & EFFECTS (what the mod actually does)
---------------------------------------------
For any plan whose created object is an OMOD, `resolve_effects` walks
OMOD properties -> ENCH -> MGEF (or the PERK that MGEF applies) for the
readable line, and Actor Values / Damage Type Values -> CURV points for the
level-scaled numbers ("+15 at Lv 1 -> +90 at Lv 50"). Nothing is hand-written;
a mod with no resolvable properties gets no effects block rather than a guess.

FLAGS (Technical)
-----------------
tradeable          : BOOK KYWDs. NonPlayerTradable or NonDroppable -> False,
                     otherwise True. (UnsellableObject alone only blocks vendor
                     SALE, not player trade, so it does not set False.)
stops_dropping     : True  if >=1 drop entry gates on HasLearnedRecipe (removed
                            from the pool once learnt);
                     False if it appears in leveled lists but NONE gate on it;
                     None  if it appears in no leveled list (purchase/quest only
                            — unresolved, rendered honestly as "Unknown").
"""
import os, re, csv, glob, json, argparse, collections, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
sys.path.insert(0, HERE)

import rng76
import plan_sources         # cut detection, readable source names, unlock routes
import plan_unlocks         # COBJ.GNAM — what the game says unlocks each recipe
import plan_source_pill     # the one-word source tag on each row
import prune_dead_routes    # retire routes nothing in the game rolls (see module)
import plan_conditions      # drop conditions per source (LVLI entry CTDAs)
import plan_display_names   # row titles: model-first weapon paint names
import plan_recipe_rows     # rows for recipes that have no plan book at all
import plan_images           # row art: published images first, staged files second
import add_weapon_groups    # weapon page grouping: weapon -> Mods / Skins
import plan_apparel_class   # armour or clothing, off the ARMO record
import plan_route_kinds     # quest vs event vs enemy, cut double-mutation lists
import plan_subpages        # which page every plan renders on
import plan_consumables     # Recipe page grouping: Food / Drinks / Alcohol / ...
import add_armour_groups    # armour page grouping: set -> Mods / Skins
from spawns_engine import sources as ssrc
# reuse the farming Containers resolver + rate helpers + rng76 wrapper
import build_farming_used_for as bfu

# ── export selection ─────────────────────────────────────────────────────────
import tsv_source          # one resolver for every export selection


def newest(pat, root=None):
    """Newest export matching *pat*, chronologically.

    Was max(..., key=os.path.getmtime). In CI actions/checkout stamps every file
    with the checkout time, so "newest by mtime" resolved to whatever the
    filesystem felt like — a different file locally than in the build that
    actually publishes. tsv_source reads the date out of the filename and ranks
    base record files above their same-date companions.
    """
    return tsv_source.newest(os.path.join(root or TSV, pat), required=False)

def read_rows(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        yield from csv.DictReader(f, delimiter="\t")

# ── FormID -> signature index (for CNAM created-object classification) ────────
SIG_EXPORTS = {
    "WEAP":"WEAP_Export_*Base.tsv","ARMO":"ARMO_Export_*ARMOUR.tsv","OMOD":"OMOD_Export_*.tsv",
    "FURN":"FURN_Export_*FURN.tsv","CONT":"CONT_Export_*.tsv","ACTI":"ACTI_Export_*ACTI.tsv",
    "MISC":"MISC_Export_*.tsv","STAT":"STAT_Export_*.tsv","MSTT":"MSTT_Export_*.tsv",
    "ALCH":"ALCH_Export_*.tsv","BOOK":"BOOK_Export_*.tsv","KEYM":"KEYM_Export_*.tsv",
    "AMMO":"AMMO_Export_*.tsv","NOTE":"NOTE_Export_*.tsv",
}
def build_sig_index():
    out = {}
    for sig, pat in SIG_EXPORTS.items():
        f = newest(pat)
        if not f: continue
        with open(f, encoding="utf-8", errors="replace") as fh:
            r = csv.reader(fh, delimiter="\t"); hdr = next(r, None)
            if not hdr: continue
            fidcol = 0
            for i, h in enumerate(hdr):
                if h.strip() == "FormID" or h.strip().endswith("_FormID"):
                    fidcol = i; break
            for row in r:
                if len(row) > fidcol and row[fidcol].strip():
                    out.setdefault(row[fidcol].strip().upper(), sig)
    return out

# ── COBJ: FormID -> {edid, cnam_fid, cnam_edid, cnam_full, bnam_edid} ─────────
def build_cobj_index():
    f = newest("COBJ_Export_*.tsv"); out = {}
    if not f: return out
    for row in read_rows(f):
        fid = (row.get("COBJ_FormID") or "").strip().upper()
        if fid:
            out[fid] = {
                "edid": (row.get("COBJ_EDID") or "").strip(),
                "cnam_fid": (row.get("CNAM_FormID") or "").strip().upper(),
                "cnam_edid": (row.get("CNAM_EDID") or "").strip(),
                "cnam_full": (row.get("CNAM_FULL") or "").strip(),
                "bnam_edid": (row.get("BNAM_EDID") or "").strip(),
            }
    return out

# ── scoreboard vendor plans: the weapon they teach ───────────────────────────
# A scoreboard weapon's plan (Plan: Nuka-Launcher, Plan: Cold Shoulder, Plan:
# Cremator ...) is sold by the Stamp / Gold vendor and teaches its own copy of
# the recipe — SCORE_S11_co_Weapon_NukaLauncher_GoldVendor — and that copy has
# NO CNAM. With nothing created, classify_plan() filed every one of them in the
# recipe bucket and plan_images routed them to the CAMP page, so the Weapon page
# never showed the Nuka-Launcher, Cold Shoulder, Circuit Breaker, Cremator,
# Tesla Cannon, Ice Breaker, Cosmic Knife, Dom Pedro or Piercing Love at all
# (found Oct 2026).
#
# The real crafting recipe sits beside it (SCORE_S11_co_NukaLauncher creates the
# AutoGrenadeLauncher WEAP), so the created weapon is borrowed from that twin:
# strip the vendor suffix and the SCORE_/co_/Weapon_ prefixes, then take the one
# COBJ whose EditorID ends in what is left AND whose CNAM is a WEAP. Two
# candidates is no answer — the row is left alone rather than guessed. Weapons
# only: a vendor MOD recipe ("…_Nitro_Grip_StampVendor") already lands on the
# weapon page by name, and has several mod twins, so it is never matched here.
_VENDOR_SUFFIX = re.compile(r"_(StampVendor|GoldVendor)(_Copy\d+|_\d+)?$", re.I)
_VENDOR_PREFIX = re.compile(r"^(zzz_?)?SCORE_(S\d+|MiniSeason_[A-Za-z0-9]+)_", re.I)
_WEAPON_NOUN   = re.compile(r"^(scythe|sword|axe|knife|rifle|pistol|gun|launcher|hammer|"
                           r"club|bow|blade|spear|shotgun|cannon)$", re.I)
_TWIN_SKIP     = re.compile(r"vendor|nocraft|repaironly|condproxy|^zzz|^del_|^cut_", re.I)


def vendor_twin(cobj, cobj_idx):
    """The crafting COBJ a CNAM-less vendor recipe stands in for, or None."""
    edid = (cobj or {}).get("edid") or ""
    if not edid or (cobj or {}).get("cnam_fid") or not _VENDOR_SUFFIX.search(edid):
        return None
    key = _VENDOR_PREFIX.sub("", _VENDOR_SUFFIX.sub("", edid))
    key = re.sub(r"^co_", "", key, flags=re.I)
    key = re.sub(r"^Weapon_(Ranged_|Melee_)?", "", key, flags=re.I)
    # Full key first; then, when the last word is only a weapon noun, the key
    # without it — the twin is sometimes named for the reward alone
    # (HeadhunterScythe -> co_Weapon_Melee_PickAxe_HeadHunter). Never any other
    # word: "Cremator_Napalmer" minus "Napalmer" is the Cremator, and a MOD
    # plan must not be mistaken for the weapon's own plan.
    words = re.findall(r"[A-Z]?[a-z0-9]+|[A-Z]+(?![a-z])", key)
    keys = [key]
    if len(words) > 1 and _WEAPON_NOUN.match(words[-1]):
        keys.append("".join(words[:-1]))
    for k in keys:
        k = re.sub(r"[^a-z0-9]", "", k.lower())
        if len(k) < 5:
            continue
        hits = []
        for c in cobj_idx.values():
            ce = c.get("edid") or ""
            if not c.get("cnam_fid") or _TWIN_SKIP.search(ce):
                continue
            if SIG_INDEX.get(c["cnam_fid"]) != "WEAP":
                continue
            if re.sub(r"[^a-z0-9]", "", ce.lower()).endswith(k):
                hits.append(c)
        if len(hits) == 1:
            return hits[0]
        if hits:
            return None          # ambiguous: never guess
    return None


def borrow_twin_cnam(cobj, co_fid, cobj_idx):
    """`cobj` with the twin's created record filled in, or `cobj` unchanged.

    The plan's own recipe stays the one Technical names (it IS the recipe the
    plan teaches); only the created record is borrowed.
    """
    twin = vendor_twin(cobj, cobj_idx)
    if not twin:
        return cobj
    out = dict(cobj or {"formid": co_fid, "edid": ""})
    out["cnam_fid"], out["cnam_edid"] = twin["cnam_fid"], twin["cnam_edid"]
    out["cnam_full"] = twin.get("cnam_full", "")
    return out

# ── LVLI entries: BOOK -> [entry dicts]; + a global list of entries per list ──
_HLR = re.compile(r"HasLearnedRecipe\([^)]*\[COBJ:([0-9A-Fa-f]{8})\]", re.I)
def build_book_entry_index():
    f = newest("LVLI_Export_*_LVLI_Entries.tsv")
    book_entries = collections.defaultdict(list)
    for row in read_rows(f):
        ref = (row.get("LVLO_Reference") or "")
        if ":BOOK" not in ref.upper():
            continue
        book_fid = ref.split(":")[0].strip().upper()
        conds = " ".join((row.get(f"Cond{i}") or "") for i in range(1, 11))
        hlr = [m.group(1).upper() for m in _HLR.finditer(conds)]
        book_entries[book_fid].append({
            "list": (row.get("LVLI_FormID") or "").strip().upper(),
            "list_edid": (row.get("LVLI_EDID") or "").strip(),
            "has_learned_recipe": bool(hlr),
            "recipe_cobj": hlr[0] if hlr else "",
        })
    return book_entries

# ── categorisation ───────────────────────────────────────────────────────────
PHYS_SIGS = {"WEAP","ARMO","FURN","CONT","STAT","MSTT","ACTI","MISC","KEYM"}
def classify_plan(book_edid, cobj):
    """Return (category, has_image_box, cnam_sig, cnam_fid, cnam_edid)."""
    e = (book_edid or "").lower()
    cnam_fid = (cobj or {}).get("cnam_fid", "")
    cnam_edid = (cobj or {}).get("cnam_edid", "")
    co_edid = (cobj or {}).get("edid", "").lower()
    sig = SIG_INDEX.get(cnam_fid, "")
    blob = e + " " + co_edid + " " + cnam_edid.lower()

    # mods / paints — no image box
    is_mod = (sig == "OMOD") or ("recipe_mod_" in e) or ("_mod_" in co_edid) \
             or ("paint" in blob) or ("skin" in blob)
    if is_mod:
        if "backpack" in blob:                     cat = "backpack-mod"
        elif any(k in blob for k in ("armor","armour","powerarmor","power_armor","pa_")):
            cat = "armour"
        elif any(k in blob for k in ("weapon","melee","ranged","gun","rifle","pistol")):
            cat = "weapon"
        else:                                       cat = "recipe"
        return cat, False, sig, cnam_fid, cnam_edid

    # physical created object
    if sig == "WEAP":
        return "weapon", True, sig, cnam_fid, cnam_edid
    if sig == "ARMO":
        cat = "apparel" if any(k in blob for k in ("outfit","apparel","underarmor","under_armor","dress","costume","uniform")) else "armour"
        return cat, True, sig, cnam_fid, cnam_edid
    if sig in PHYS_SIGS or "recipe_workshop" in e or "co_workshop" in co_edid or "workshop_co" in co_edid:
        return "recipe", True, (sig or "FURN"), cnam_fid, cnam_edid   # CAMP/workshop placeable
    if sig == "ALCH":
        return "recipe", False, sig, cnam_fid, cnam_edid              # food/chem, no placeable

    # unresolved created object -> default recipe bucket, NO image box (honest)
    return "recipe", False, sig, cnam_fid, cnam_edid

# ── tradeable / stops_dropping ───────────────────────────────────────────────
def resolve_tradeable(kw_blob):
    k = (kw_blob or "").lower()
    if "nonplayertradable" in k or "nondroppable" in k:
        return False
    if "objecttyperecipe" in k or "unsellableobject" in k:
        return True   # standard plan: sellable-block only, still player-tradeable
    return None       # unknown -> honest "Unknown"

_CAT_NOUN = {"apparel":"Apparel","armour":"Armour","backpack-mod":"Backpack Mod",
             "recipe":"CAMP / Recipe","weapon":"Weapon"}
# The first thing a reader wants from How to Obtain is whether there is a plan
# item to go looking for at all. Both sentences say which of the two this row is
# before they say anything else, and every builder that writes `obtain` uses
# these so the two halves of the roster cannot drift apart.
PHYSICAL_PLAN = ("This is a physical plan — there is a plan item to find "
                 "and learn.")
LEARNT_DIRECT = ("This plan is learnt directly — there is no physical plan "
                 "item to find.")


def category_label(cat, has_img, cnam_sig, physical=True):
    """Human label for the Technical box.

    `physical` is False for a recipe-only row — a craftable a challenge or a
    workshop claim teaches, with no plan BOOK anywhere. Saying "(physical plan)"
    there sends a reader hunting an item that does not exist, which is the whole
    distinction this label is meant to carry.
    """
    noun = _CAT_NOUN.get(cat, cat.title())
    if not has_img:
        if cnam_sig == "OMOD" or cat == "backpack-mod":
            return f"{noun} Mod / Paint"
        if cnam_sig == "ALCH":
            return "Consumable Recipe"
        return f"{noun} (mod/recipe)"
    return f"{noun} (physical plan)" if physical else f"{noun} (learnt directly)"

def resolve_stops_dropping(entries):
    if not entries:
        return None
    if any(en["has_learned_recipe"] for en in entries):
        return True
    return False


# ── backpack: mod vs skin vs flair ───────────────────────────────────────────
# The backpack-mod bucket used to be a dumping ground. Functional mods, paint
# skins and cosmetic flair all landed in it because every one of them is an
# OMOD recipe, so classify_plan() could not tell them apart. The distinction is
# carried by the created OMOD's own EDID:
#     mod_BackPack_Effect_*      -> functional MOD  (changes how the pack works)
#     mod_BackPack_*_Material_*  -> SKIN / paint    (appearance only)
#     mod_BackPack_*_Flair*      -> cosmetic FLAIR
# A handful of plans carry no COBJ -> CNAM link at all. For those the plan's own
# FULL name is reliable: Bethesda suffixes the functional ones " Mod" and the
# cosmetic ones " Flair" ("Plan: Scrap Rat Backpack Mod" vs "Plan: Black Cloth
# Backpack"). Only class "mod" is published to /plan-checklists/backpack-mod/.
_BP_FLAIR    = re.compile(r"_flair|\bflair\b", re.I)
_BP_EFFECT   = re.compile(r"_effect_", re.I)
_BP_MATERIAL = re.compile(r"_material_", re.I)

_PLAN_PREFIX = re.compile(r"^\s*(?:Plan|Recipe)\s*:\s*", re.I)

def backpack_display_name(name):
    """Row title for the backpack-mod page: "Plan: Backpack Armor Plated Mod"
    -> "Armor Plated".

    Every row on that page is a plan for a backpack mod, so "Plan", "Backpack"
    and "Mod" appear on all nine and carry no information — they just push the
    part that differs off the right-hand side at phone width. The full in-game
    title stays on the item as `name` (rendered in How to Obtain and searchable
    there), so nothing is lost.
    """
    s = _PLAN_PREFIX.sub("", name or "")
    s = re.sub(r"\bBackpack\b", " ", s, flags=re.I)
    s = re.sub(r"\bMod\b\s*$", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -\u2013\u2014")
    return s or (name or "")

def backpack_class(cnam_edid, cobj_edid, name):
    """Return 'mod' | 'skin' | 'flair' for a backpack-mod bucket plan."""
    blob = f"{cnam_edid or ''} {cobj_edid or ''}"
    if _BP_FLAIR.search(blob) or _BP_FLAIR.search(name or ""):
        return "flair"
    if _BP_EFFECT.search(blob):
        return "mod"
    if _BP_MATERIAL.search(blob):
        return "skin"
    return "mod" if re.search(r"\bmod\s*$", (name or "").strip(), re.I) else "skin"


# ── Output & Effects ─────────────────────────────────────────────────────────
# For a plan that creates an OMOD, resolve what the mod actually DOES, in plain
# English plus the real numbers — never a hand-written blurb.
#
#   OMOD properties (OMOD_Export_*_Properties.tsv)
#     ├─ "Enchantments"       -> ENCH -> MGEF DNAM description  ─┐ the readable
#     │                                  (empty DNAM -> the PERK  │ summary line
#     │                                   that MGEF applies)     ─┘
#     ├─ "Actor Values"       -> AVIF name  + CurveTable ─┐ level-scaled numbers
#     ├─ "Damage Type Value"  -> DMGT -> resistance name  ┘ (CURV points 1..50)
#     └─ "Keywords"           -> internal plumbing, dropped
#
# Nothing here is backpack-specific: any plan whose created object is an OMOD
# gets an effects block, so armour/weapon mod plans pick it up for free.

_DMGT_LABEL = {
    "dtphysical":          "Damage Resistance",
    "dtenergy":            "Energy Resistance",
    "dtradiationexposure": "Radiation Resistance",
    "dtradiation":         "Radiation Resistance",
    "dtpoison":            "Poison Resistance",
    "dtcryo":              "Cryo Resistance",
    "dtfire":              "Fire Resistance",
}
_QUOTED  = re.compile(r'"+([^"]+)"+')
_FID_TAG = re.compile(r"\[[A-Za-z_]{4}:([0-9A-Fa-f]{8})\]")
_MAG_TOK = re.compile(r"<\s*mag\s*>", re.I)
_SKIP_MGEF = re.compile(r"^zzz|emptyeffect", re.I)
# An MGEF FULL is only a usable last-resort description when it reads as
# English. Most weapon/armour mod effects carry an internal name instead
# ("ModEnergyWeaponFireDamage", "FX Shader Fire", "Bleed dmg health"), which is
# worse than saying nothing — so a jammed CamelCase name or an obvious
# engine-side token is rejected and the mod simply gets no summary line.
_INTERNAL_FULL = re.compile(r"\b(fx|vfx|shader|dmg|dbg|test|deprecated|placeholder)\b", re.I)

def _readable_full(text):
    t = (text or "").strip()
    if " " not in t or _INTERNAL_FULL.search(t):
        return ""
    return t

def _first_quoted(s):
    m = _QUOTED.search(s or "")
    return m.group(1).strip() if m else ""

def _first_fid(s):
    m = _FID_TAG.search(s or "")
    return m.group(1).upper() if m else ""

def _num(v):
    """Trim a float to the shortest honest form and sign it: 30.0 -> '+30'."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v or "")
    s = f"{f:.2f}".rstrip("0").rstrip(".")
    if s in ("", "-"):
        s = "0"
    return s if s.startswith("-") else ("+" + s)

# CURV: EDID -> [(level, value), ...] ascending. The final point is a
# level-1000 sentinel that only repeats the cap, so it is dropped for display.
def build_curve_index():
    f = newest("CURV_Export_*_POINTS.tsv")
    out = collections.defaultdict(list)
    if not f:
        return {}
    for row in read_rows(f):
        eid = (row.get("EDID") or "").strip()
        if not eid:
            continue
        try:
            out[eid.lower()].append((float(row.get("X") or 0), float(row.get("Y") or 0)))
        except ValueError:
            continue
    for k in out:
        out[k] = sorted(p for p in out[k] if p[0] <= 100)
    return dict(out)

def _curve_points(curve_idx, curve_field):
    """Every breakpoint the curve actually defines, as [{"lv": 1, "v": "+15"}…].

    These properties scale with player level and the game interpolates between
    the points; the points ARE the ladder (typically 1/10/20/30/40/50). The page
    shows all of them rather than just the ends, so a level-22 player can read
    their own number instead of guessing between two extremes. A flat property
    (every y identical) returns no ladder — one repeated value is not a table.
    """
    eid = (curve_field or "").split("[")[0].strip()
    pts = curve_idx.get(eid.lower())
    if not pts:
        return None
    if len({y for _, y in pts}) <= 1:
        return None
    return [{"lv": int(x), "v": _num(y)} for x, y in pts]

def _curve_display(curve_idx, curve_field):
    """One-line summary of a level-scaled property: '+15 at Lv 1 → +90 at Lv 50'.
    Kept alongside the ladder as the collapsed/print fallback."""
    eid = (curve_field or "").split("[")[0].strip()
    pts = curve_idx.get(eid.lower())
    if not pts:
        return None
    lo, hi = pts[0], pts[-1]
    if lo[1] == hi[1]:
        return _num(lo[1])
    return f"{_num(lo[1])} at Lv {int(lo[0])} → {_num(hi[1])} at Lv {int(hi[0])}"

# ENCH: FormID -> {edid, full, effects[{mgef, mag, curv}]}
def build_ench_index():
    f = newest("ENCH_Export_*.tsv")
    out = {}
    if not f:
        return out
    for row in read_rows(f):
        fid = (row.get("ENCH_FormID") or "").strip().upper()
        if not fid:
            continue
        effs = []
        for i in range(1, 9):
            mg = (row.get(f"Effect_{i}_MGEF_FID") or "").strip()
            if not mg:
                continue
            # The export merges FormID and EDID into the MGEF_FID column
            # ("0042E51B:Backpack_ReduceFoodSpoilageEffect"), which shifts every
            # later Effect_N_* field one to the left — the real magnitude lands
            # in Effect_N_MGEF_EID. Take the first of the two that parses as a
            # number so a future un-merged export keeps working.
            mag = ""
            for col in (f"Effect_{i}_MGEF_EID", f"Effect_{i}_Magnitude"):
                v = (row.get(col) or "").strip()
                try:
                    float(v)
                except ValueError:
                    continue
                mag = v
                break
            effs.append({
                "mgef": mg.split(":")[0].strip().upper(),
                "mag":  mag,
                "curv": (row.get(f"Effect_{i}_CURV_EID") or "").strip(),
            })
        out[fid] = {"edid": (row.get("ENCH_EDID") or "").strip(),
                    "full": (row.get("ENCH_FULL") or "").strip(),
                    "effects": effs}
    return out

# MGEF: FormID -> {edid, full, desc}
def build_mgef_index():
    f = newest("MGEF_Export_*.tsv")
    out = {}
    if not f:
        return out
    for row in read_rows(f):
        fid = (row.get("MGEF_FormID") or "").strip().upper()
        if fid:
            out[fid] = {"edid": (row.get("EDID") or "").strip(),
                        "full": (row.get("FULL") or "").strip(),
                        "desc": (row.get("DNAM_MagicItemDescription") or "").strip()}
    return out

# PERK: MGEF FormID -> perk DESC. Several backpack mods carry an empty MGEF
# description because the real text lives on the perk the effect applies
# (ScrapRat -> "-90% Scrap Weight"). The PERK's ReferencedBy list points back at
# that MGEF, so index the reverse direction once.
def build_perk_desc_by_mgef():
    f = newest("PERK_Export_*.tsv")
    out = {}
    if not f:
        return out
    for row in read_rows(f):
        desc = (row.get("DESC") or "").strip()
        if not desc:
            continue
        for j in range(1, 41):
            v = (row.get(f"Ref_{j}") or "").strip()
            if v.upper().endswith(":MGEF"):
                out.setdefault(v.split(":")[0].strip().upper(), desc)
    return out

def _apply_magnitude(text, mag):
    """Substitute <mag> in an MGEF description.

    Bethesda stores some magnitudes as a fraction and some as whole percent:
    Refrigerated is 0.5 against the template '-<mag>% Food Spoilage Rate' (i.e.
    50%), while Pillager is 90.0 against '-<mag>% Chem Weight'. A fraction next
    to a '%' is therefore scaled; anything else is printed as stored.
    """
    if not text or not _MAG_TOK.search(text):
        return text
    try:
        f = float(mag)
    except (TypeError, ValueError):
        return _MAG_TOK.sub("", text).strip()
    if 0 < abs(f) <= 1 and "%" in text:
        f *= 100
    s = f"{f:.2f}".rstrip("0").rstrip(".") or "0"
    return _MAG_TOK.sub(s, text)

def resolve_effects(omod_fid, prop_idx, ench_idx, mgef_idx, perk_idx, curve_idx):
    """Return the Output & Effects block for an OMOD, or None if it has none."""
    props = prop_idx.get((omod_fid or "").upper()) or []
    if not props:
        return None

    summary, rows, seen = [], [], set()
    for p in props:
        pname = (p.get("PropertyName") or "").strip()
        v1    = p.get("Value1") or ""
        v2    = p.get("Value2") or ""
        curve = p.get("CurveTable") or ""

        if pname == "Keywords":
            continue                       # internal plumbing, never shown

        if pname == "Enchantments":
            ench = ench_idx.get(_first_fid(v1))
            if not ench:
                continue
            for eff in ench["effects"]:
                mg = mgef_idx.get(eff["mgef"])
                if not mg or _SKIP_MGEF.search(mg["edid"]):
                    continue
                text = (mg["desc"] or perk_idx.get(eff["mgef"], "")
                        or _readable_full(mg["full"]))
                text = _apply_magnitude(text, eff["mag"])
                # Several ENCHs carry a second, near-identical MGEF for a
                # variant (Canteen ships a ghoul-only restatement). Keep the
                # first wording and drop anything that merely extends it.
                if text and not any(text.startswith(x) or x.startswith(text)
                                    for x in summary):
                    summary.append(text)
            continue

        if pname == "Damage Type Value":
            key   = (v1.split()[0] if v1.split() else "").lower()
            label = _DMGT_LABEL.get(key) or _first_quoted(v1) or key.title()
        elif pname == "Actor Values":
            label = _first_quoted(v1) or (v1.split()[0] if v1.split() else "Value")
        else:
            continue

        value = _curve_display(curve_idx, curve)
        if value is None:
            value = _num(v2)
            if value in ("+0", "0"):
                continue                   # no curve and no flat value -> nothing to say
        k = (label, value)
        if k in seen:
            continue
        seen.add(k)
        rows.append({"label": label, "value": value,
                     "points": _curve_points(curve_idx, curve),
                     "curve": (curve.split("[")[0].strip() or None)})

    if not summary and not rows:
        return None
    return {"summary": " · ".join(summary), "rows": rows[:10]}

# OMOD: EDID (lower) -> {fid, edid}. Used only as a last-resort created-object
# resolve when a plan's COBJ is a CondProxy with no CNAM.
def build_omod_edid_index():
    f = newest("OMOD_Export_*.tsv")
    out = {}
    if not f:
        return out
    for row in read_rows(f):
        eid = (row.get("OMOD_EDID") or "").strip()
        fid = (row.get("OMOD_FormID") or "").strip().upper()
        if eid and fid:
            out.setdefault(eid.lower(), {"fid": fid, "edid": eid})
    return out

_RECIPE_TOK = re.compile(r"(?:^|_)recipe_", re.I)
_VENDOR_TAIL = re.compile(r"_(GoldVendor|AtomShop|Atom|Vendor|Purveyor|Reward|Quest)$", re.I)
_PLUGIN_PFX  = re.compile(r"^[A-Za-z0-9]{2,6}_")

def omod_from_book_edid(book_edid, omod_by_edid):
    """Recover the created OMOD from the plan BOOK's own EDID.

    Recipe BOOKs are named after the mod they teach —
    ATX_Recipe_mod_BackPack_Effect_ScrapRat_GoldVendor teaches
    ATX_mod_BackPack_Effect_ScrapRat — so when the COBJ is a CondProxy carrying
    no CNAM the mod is still recoverable by stripping the recipe wrapper and the
    vendor tail. This is an exact EDID lookup, so it either hits the right
    record or returns nothing; it never guesses.
    """
    base = _VENDOR_TAIL.sub("", _RECIPE_TOK.sub("_", book_edid or "").lstrip("_"))
    for cand in (base, _PLUGIN_PFX.sub("", base)):
        hit = omod_by_edid.get(cand.lower())
        if hit:
            return hit
    return None

# OMOD properties: FormID -> [property rows]
def build_omod_prop_index():
    f = newest("OMOD_Export_*_Properties.tsv")
    out = collections.defaultdict(list)
    if not f:
        return {}
    for row in read_rows(f):
        fid = (row.get("OMOD_FormID") or "").strip().upper()
        if fid:
            out[fid].append(row)
    return dict(out)

# ── route resolution (container-type -> drop%, generalised) ──────────────────
def humanize(edid):
    s = re.sub(r"^(LL[ESV]?_|LLD_|LL_|co_|Recipe_|recipe_)", "", edid or "")
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s).replace("_", " ")
    return re.sub(r"\s+", " ", s).strip() or (edid or "Source")

def plan_classify(sig, edid, via_edid):
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    # An NPC that holds a list carries it and drops it: a vendor's stock lives
    # in their merchant CONT, never on the NPC. Testing the words first filed
    # LLD_Creature_Robot_Assaultron as "Whitespring Spa Aloe vendor" (the Spa
    # robot is an Assaultron called LC060_WhitespringVendor_Spa_Aloe) and
    # LLD_Creature_MoleMiner as a "Mole Miner" vendor (zzzEncMoleMiner_
    # LegendaryVendor) - Assaultron Blade/Head and Mole Miner Gauntlet were
    # printed as shop stock at 41.67% / 31.43% (Oct 2026 audit).
    if sig == "NPC_": return "creature"
    if "vendorchest" in e or "vendor" in e or "vend" in e: return "vendor"
    if "questreward" in e or "quest_reward" in e or "_reward" in e or "gmrw" in e \
       or "systemic" in e or "quest" in e: return "event-quest"
    if sig == "NPC_" or "creature" in e or "lle_" in e or "_npc_" in e: return "creature"
    if sig == "REFR": return "fixed"
    if sig == "CONT": return "container"
    # The record's own type, when no word in the EditorIDs said anything. A
    # quest or quest-reward record rolling a list is a quest/event payout
    # (MILE_MoleMiner_MysteryCrate is a QUST and its EditorID never says so);
    # an activator is a placed thing you interact with (Abraxo caches).
    if sig in ("QUST", "GMRW"): return "event-quest"
    if sig == "ACTI": return "fixed"
    return "loot-list"

BUCKET_LABEL = {"container":"container","vendor":"vendor","creature":"creature",
                "event-quest":"event / quest","fixed":"fixed spawn","loot-list":"loot pool"}

# ── source naming ───────────────────────────────────────────────────────────
# Moved to plan_sources.py, which reads the QUEST export every build instead of
# carrying a hand-copied AREA_CODE map, and protects mixed-case acronyms through
# the camel split so "BoS" stops rendering as "Bo S". See that module's docstring
# for why the lookup is longest-prefix-wins and why an ambiguous prefix resolves
# to nothing rather than to a guess.
#
# QUEST_NAMES is loaded once in main() and passed down; the module-level default
# is an empty index so this file stays importable (and testable) without a TSV
# tree, in which case naming falls back to the AREA_CODE backstop alone.
QUEST_NAMES = plan_sources.QuestNames()
# GMRW FormID -> quest title; set in main() from UnlockIndex. Used to name
# "Side Quests" routes after the actual quest (plan_sources.quest_route_label).
GMRW_QUESTS = {}
# QUST FormID -> quest title; set in main() from UnlockIndex (for lists a quest
# record rolls directly, e.g. CB04_RewardContainerList <- "Mayor for a Day").
QUEST_TITLES = {}

AREA_CODE  = plan_sources.AREA_CODE          # re-exported: other builders read these
_DEV_CODES = plan_sources.DEV_CODES
_PLUMBING  = plan_sources.PLUMBING


def source_label(edid):
    """Readable name for a leveled list, or None if it is not a real source."""
    return plan_sources.source_label(edid, QUEST_NAMES)


def family_name(name):
    """Back-compat shim — source_label() reads the EditorID itself."""
    return source_label(name) or (name or "Source").strip()


# Strongest evidence first. A list held by both a vendor chest and an NPC is a
# vendor route; "loot-list" is the floor, meaning nothing said anything.
_BUCKET_RANK = ["vendor", "creature", "event-quest", "fixed", "container", "loot-list"]


def holder_bucket(via, holders):
    """What KIND of source a leveled list is, judged by what holds it.

    This used to be one call — plan_classify("", via + holder_edids, via) — and
    the empty first argument was the bug. plan_classify's ONLY test for a
    creature is `sig == "NPC_"` or the word "creature" in the blob, so throwing
    the holders' signatures away meant an NPC-held list could never be classed
    as a creature drop. It fell to "loot-list", which the caller drops as
    internal plumbing, and the route vanished.

    What that cost: the Pint-Sized Slasher event's rare recipes hang off
    SDOW_LL_Slasher_RareRecipes -> SDOW_LL_BountyDrop_BIG, whose only holder is
    the NPC SDOW_Burn_BountyTarget_BIG_Slasher — the bounty boss you kill to get
    them. Nothing in those EditorIDs says "creature", so the whole branch was
    silently discarded and four plans published as having no source at all.

    Classifying each holder on its own, with its real signature, and taking the
    strongest verdict fixes it without weakening anything: the old blob-only
    answer is still computed as the floor, so a list with no holders behaves
    exactly as before.
    """
    # NPC EditorIDs stay out of the word blob for the same reason they are
    # classified by signature: a vendor robot's name is not a shop.
    holder_blob = " ".join((redid or "") for rf, redid, rsig in holders
                           if rsig not in ("CONT", "NPC_")
                           and not plan_sources.is_dev_record(redid))
    best = plan_classify("", (via or "") + " " + holder_blob, via)
    for rf, redid, rsig in holders:
        b = plan_classify(rsig, redid, via)
        if _BUCKET_RANK.index(b) < _BUCKET_RANK.index(best):
            best = b
    return best


def entry_holders(holders):
    """The records that roll this list THEMSELVES — anything but another list.

    A leveled list held only by other leveled lists is a step inside someone
    else's roll, not a place a player can go. Its rng76 appearance is the
    chance ONCE YOU ARE ALREADY INSIDE IT, so publishing it as a route printed
    a conditional rate next to the real one:

        Plan: Nuka-Cola Balloons
          NWOT                        7.14%   <- NWOT_LL_QuestReward_Generic_Recipe
          Event: Spin The Wheel       3.57%   <- E09B_Wheel_LL_QuestReward

    The 7.14% is the 1-in-14 pick inside the generic recipe pool, which Spin
    The Wheel only reaches half the time — the real chance is the 3.57% the
    event row already shows. 3,730 rows across 1,602 plans were this (Oct
    2026 live), and it is what made the same event print twice at two rates
    (Radiation Rumble 2.5% / 1.25% on the Gamma Gun).

    Editor-only holders (DEPRECATED_ effects, BabylonExcludeList) are not
    entries either: nothing a player does reaches them.
    """
    return [(rf, redid, rsig) for rf, redid, rsig in holders
            if rsig != "LVLI" and not plan_sources.is_dev_record(redid)
            and not plan_sources._RX_DEV_RECORD.search(redid or "")]


# MGEF FormID -> what the player opens to fire it ("Holiday Gift", "Ornate
# Mole Miner Pail", "Spooky Treat Bag"). Loaded lazily from the ALCH export,
# so build_no_plan_apparel_json.py (which calls resolve_routes directly) gets
# it too without having to know about it.
OPENABLE_NAMES = None
_RX_TIER = re.compile(r"Tier_?0*(\d+)", re.I)


def load_openable_names():
    """MGEF FormID -> readable name of the consumable that carries it.

    Gifts, pails, treat bags and reward boxes give their loot through a magic
    effect: ALCH (the thing you open) -> MGEF (its effect) -> LVLI (the loot).
    The LVLI's only holder is the MGEF, whose name ("Holiday Present Tier 02
    Effect") is not what the player sees, so the route used to drop out as
    "plumbing" and only the inner lists — at their conditional rates — were
    left on the page. The ALCH FULL is the name on the item in the Pip-Boy.
    Same-named tiers ("Holiday Gift" x3) are told apart by the Tier number
    in the ALCH EditorID, so three rates never sit under one name.
    """
    out = {}
    eff = newest("ALCH_Export_*_Effects.tsv")
    base = newest("ALCH_Export_*.tsv")       # tsv_source ranks the base file first
    if not eff or not base or base.endswith("_Effects.tsv"):
        return out
    full = {}
    for r in read_rows(base):
        fid = (r.get("ALCH_FormID") or r.get("FormID") or "").strip().upper()
        nm = (r.get("FULL") or "").strip()
        if fid and nm:
            full[fid] = nm
    rows = []
    mgefs_by_name = collections.defaultdict(set)
    for r in read_rows(eff):
        afid = (r.get("ALCH_FormID") or "").strip().upper()
        aedid = (r.get("ALCH_EDID") or "").strip()
        mfid = (r.get("MGEF_FormID") or "").strip().upper()
        nm = full.get(afid)
        if not (mfid and nm) or plan_sources.is_dev_record(aedid):
            continue
        if plan_sources._RX_DEV_RECORD.search(aedid) or not plan_sources.usable_quest_name(nm):
            continue
        rows.append((mfid, aedid, nm))
        mgefs_by_name[nm].add(mfid)
    seen = {}
    for mfid, aedid, nm in rows:
        # Only where one name covers several effects (three "Holiday Gift"
        # tiers) does the tier need saying; "Ornate Mole Miner Pail" already
        # says which pail it is.
        m = _RX_TIER.search(aedid)
        if m and len(mgefs_by_name[nm]) > 1 and not re.search(r"\btier\b", nm, re.I):
            nm = f"{nm} (Tier {int(m.group(1))})"
        lst = seen.setdefault(mfid, [])
        if nm not in lst:
            lst.append(nm)
    for mfid, names in seen.items():
        names.sort(key=lambda n: (len(n), n))
        out[mfid] = " / ".join(names[:2])
    return out


def openable_name(entries):
    """Readable name of the item(s) whose effect rolls this list, or None."""
    global OPENABLE_NAMES
    if OPENABLE_NAMES is None:
        try:
            OPENABLE_NAMES = load_openable_names()
        except Exception as exc:                      # noqa: BLE001 - never fatal
            print(f"  WARNING: openable names unavailable: {exc}", file=sys.stderr)
            OPENABLE_NAMES = {}
    names, notes = [], []
    for rf, redid, rsig in entries:
        if rsig == "MGEF":
            nm = OPENABLE_NAMES.get((rf or "").upper())
            if nm and nm not in names:
                names.append(nm)
                note = plan_sources.LIMITED_TIME_NOTES.get(redid or "")
                if note and note not in notes:
                    notes.append(note)
    if not names:
        return None
    label = " / ".join(names[:2])
    # "ATLAS Donor's Provisions (Fortifying ATLAS event, Aug-Sep 2020 only)"
    return f"{label} ({'; '.join(notes)})" if notes else label


def real_entries(holders, nested_in_tree):
    """entry_holders(), minus dialogue. A list whose only non-list holders are
    INFO records AND that sits inside another list of this plan's tree is that
    list's stock being read out in dialogue (Grahm's barter topic holds
    LLV_Vendor_Recipes_Base_RandomEncounters, which his chest list also
    holds) — not a separate place to get the plan."""
    ent = entry_holders(holders)
    if nested_in_tree and ent and all(rs == "INFO" for _rf, _re, rs in ent):
        return []
    return ent


def nested_route_plan(closure, lvli_refs, parent_edid, c2p, cont_names):
    """{nested list -> [top lists]} for the nested lists that must keep a route.

    entry_holders() drops a list that only other lists hold, on the promise that
    "the list that holds it gets its own route". This checks the promise. A
    nested list is COVERED when some list above it in this plan's closure will
    itself publish a row (a named vendor / creature / event / quest list, an
    openable item, or a searchable container). A nested list that is NOT
    covered keeps its route, and the value here is the topmost lists above it
    (the ones something outside the list tree rolls) whose rng76 rate the row
    carries. Structural only — no rate is computed here.

    6 Oct 2026 audit: 39 rows lost a real source this way, e.g. Plan: Resort
    Sign / Gilded Wall Clock / Resort Lamps lost Mischief Night (White Springs)
    because the parent zzz_E03A_SpookyScorched_LL_RewardList has no usable name
    although a live GMRW pays it out. Lists whose only parents are DEPRECATED_
    keep a route here too and prune_dead_routes then retires it as a cut list,
    so the source is recorded rather than silently gone.
    """
    from spawns_engine.classify import farming_classify as _fc
    cl = set(closure)
    def holders_of(A):
        return lvli_refs.get(A) or lvli_refs.get(str(A).upper()) or ()
    def parents(A):
        return [p for p in c2p.get(str(A).upper(), ()) if p in cl]
    def is_nested(A):
        return bool(parents(A)) and not real_entries(holders_of(A), True)
    def labelled(A):
        return bool(source_label(parent_edid.get(A) or str(A)))
    def publishes(A):                      # a non-nested list that makes a row
        h = holders_of(A); via = parent_edid.get(A, "")
        if openable_name(entry_holders(h)):
            return True
        for rf, red, rs in h:
            if (rs == "CONT" and _fc("CONT", red, via) == "container"
                    and cont_names.get((rf or "").upper())
                    and not bfu._is_camp_storage(red)):
                return True
        b = holder_bucket(via, h)
        if b in ("container", "loot-list"):
            return False
        if b in ("vendor", "creature"):
            return True
        return labelled(A)
    memo_cov, memo_pub = {}, {}
    def covered(A, stack=()):
        if A in memo_cov:
            return memo_cov[A]
        res = False
        for p in parents(A):
            if p in stack:
                continue
            if makes_row(p, stack + (A,)) or (is_nested(p) and covered(p, stack + (A,))):
                res = True
                break
        memo_cov[A] = res
        return res
    def makes_row(A, stack=()):
        if A not in memo_pub:
            memo_pub[A] = ((not covered(A, stack) and labelled(A)) if is_nested(A)
                           else publishes(A))
        return memo_pub[A]
    def tops(A, seen):
        out = []
        for p in parents(A):
            if p in seen:
                continue
            seen.add(p)
            if is_nested(p):
                out.extend(tops(p, seen))
            elif real_entries(holders_of(p), bool(parents(p))):
                # Only a list something outside the tree really rolls. An orphan
                # top (nothing holds it, or only DEPRECATED_/editor records do —
                # the Systemic Taxidermy pools) is not a way in.
                out.append(p)
        return out
    plan = {}
    for L in closure:
        if is_nested(L) and not covered(L):
            t = sorted(set(tops(L, {L})))
            if t:
                plan[L] = t
    return plan


def better_vendor_label(chest, stock):
    """Name a vendor from its chest's EditorID, or its stock list's when that
    says the same thing and more. "GQ_10_VendorChest_Travelling_Workshops"
    splits to "GQ Travelling Workshops vendor" (the 10 is lost at the
    underscore); its stock list Vendor_GQ10_Travelling_Workshops reads "GQ10
    Travelling Workshops vendor" — every word of the chest's name plus the code.
    A station chest ("Rand Station (Raiders vendor)") is MORE specific than its
    faction stock ("Raiders vendor"), so the chest keeps it."""
    if not chest:
        return stock
    if stock:
        cw = set(plan_sources.route_key(chest).split())
        sw = set(plan_sources.route_key(stock).split())
        if cw and cw < sw | {w.rstrip("0123456789") for w in sw}:
            return stock
    return chest


_RX_VENDOR_ROW = re.compile(r"^(?P<head>.*?)\s*\((?:(?P<place>[^(),]+),\s*)?(?P<qual>[^(),]*\bvendor)\)$")


def group_vendor_rows(routes):
    """One row per faction's station network, not one per station.

    The Responders, Raiders, Free States, Brotherhood and Neutral vendors stand
    at a dozen stations each and every station sells from the same faction
    stock, so a plan they carry got twelve rows at one identical rate — and the
    twelve-row cap then hid every other source (Short Pew lost Carver
    Timmerman; on the 6 Oct 2026 audit 1,042 rows fell off this way once the
    "Locker" chests stopped merging). Rows that share the faction qualifier
    ("Responders vendor") and the same rate, three or more of them, become one:

        "Responders vendors (Camden Park, Charleston, Flatwoods, ...)"

    Named traders (Minerva, Regs, Grahm) never share a qualifier and rate with
    two others, so they are untouched. No rate is changed: the rows already
    agree to four places. Works on rows with `_lvli` sets (inside
    resolve_routes) and on finished rows with `lvli` lists alike.
    """
    groups = collections.OrderedDict()
    keep = []
    for r in routes:
        m = _RX_VENDOR_ROW.match(r.get("route") or "") if r.get("source_type") == "vendor" else None
        if not m:
            keep.append(r)
            continue
        qual = m.group("qual").strip()
        if not re.search(r"\(", qual) and qual.lower() != "vendor":
            groups.setdefault((qual.lower(), round(r.get("rate") or 0, 4)), []).append((r, m))
        else:
            keep.append(r)
    for (qlow, _rate), rows in groups.items():
        if len(rows) < 3:
            keep.extend(r for r, _m in rows)
            continue
        places = []
        for r, m in rows:
            pl = (m.group("place") or m.group("head") or "").strip()
            if pl and pl not in places:
                places.append(pl)
        qual = rows[0][1].group("qual").strip()
        base = dict(rows[0][0])
        base["route"] = f"{qual}s ({', '.join(sorted(places))})"
        if any("_lvli" in r for r, _m in rows):
            base["_lvli"] = set().union(*[set(r.get("_lvli") or ()) for r, _m in rows])
        if any("lvli" in r for r, _m in rows):
            base["lvli"] = sorted(set().union(*[set(r.get("lvli") or ()) for r, _m in rows]))
        keep.append(base)
    return keep


_AREA_NAMES = {v for v in plan_sources.AREA_CODE.values() if isinstance(v, str)}


_VENDOR_NAMES = None


def vendor_names():
    """plan_sources.VendorNames for this run's export root (lazy, so
    build_no_plan_apparel_json.py, which calls resolve_routes directly, gets it
    without knowing about it)."""
    global _VENDOR_NAMES
    if _VENDOR_NAMES is None or getattr(_VENDOR_NAMES, "_root", None) != TSV:
        try:
            _VENDOR_NAMES = plan_sources.VendorNames(TSV, lambda pat, root: newest(pat, root))
        except Exception as exc:                      # noqa: BLE001 - never fatal
            print(f"  WARNING: vendor NPC names unavailable: {exc}", file=sys.stderr)
            _VENDOR_NAMES = plan_sources.VendorNames(None)
        _VENDOR_NAMES._root = TSV
    return _VENDOR_NAMES


def vendor_chest_for_list(fid, lvli_refs, depth=0, seen=None):
    """The vendor chest (with a named NPC) whose stock this list is part of."""
    seen = seen if seen is not None else set()
    fid = (fid or "").upper()
    if depth > 4 or fid in seen:
        return None
    seen.add(fid)
    hs = lvli_refs.get(fid) or ()
    for rf, _re, rs in hs:
        if rs == "CONT" and vendor_names().name(rf):
            return (rf or "").upper()
    for rf, _re, rs in hs:
        if rs == "LVLI":
            c = vendor_chest_for_list(rf, lvli_refs, depth + 1, seen)
            if c:
                return c
    return None


_CHEST_EDIDS = None


def chest_edids():
    """CONT FormID -> EditorID for this run's export (vendor chest matching)."""
    global _CHEST_EDIDS
    if _CHEST_EDIDS is None or _CHEST_EDIDS[0] != TSV:
        m = {}
        p = newest("CONT_Export_*.tsv")
        if p:
            for r in read_rows(p):
                m[(r.get("FormID") or "").strip().upper()] = (r.get("EDID") or "").strip()
        _CHEST_EDIDS = (TSV, m)
    return _CHEST_EDIDS[1]


_GENERIC_FULLS = {}


def vendor_chest_full(cont_names, fid):
    """A vendor chest's FULL, only when it names THIS vendor.

    Vendor chests are hidden containers and most carry no name, but 40 of them
    (the Travelling Workshops, the Wastelanders C.A.M.P. merchants, every
    workshop vendor, Milepost Zero, Fishing, NWOT ...) are named "Locker" — the
    FULL of the base locker they were copied from. Taking the FULL first
    published 481 vendor routes as "Locker" on the 6 Oct 2026 live build and
    merged them into one row per rate (Plan: Simple Bed: "Locker 8.03%" was the
    Travelling Workshops vendor). A FULL shared by more than one CONT record is
    the base object's name, not a vendor's, so the EditorID names it instead
    ("Workshop Armor vendor", "Wastelanders - C.A.M.P. AF09 Weapon vendor").
    Counted from the export each build: nothing is hardcoded.
    """
    key = id(cont_names)
    if key not in _GENERIC_FULLS:
        _GENERIC_FULLS.clear()
        counts = collections.Counter(v for v in cont_names.values() if v)
        _GENERIC_FULLS[key] = {v for v, n in counts.items() if n > 1}
    full = cont_names.get((fid or "").upper())
    if not full or full in _GENERIC_FULLS[key]:
        return None
    return full


def collapse_routes(routes):
    """Merge rows that are the same source wearing two hats.

    A vendor reaches a plan twice — once as the CONT that is their chest, once
    as the LVLI that is their stock — so the live site renders "Minerva Gold
    Vendor Chest" and "Minerva LLV Gold Vendor" as separate rows at an identical
    100%. Same for Settler Samuel, Mortimer, Reginald and Giuseppe: 170 + 170,
    99 + 99, 90, 81 + 81 duplicate rows between them.

    Two rows collapse only when plan_sources.route_key() reduces them to the
    same content words AND their rates agree to four places — different rates
    mean genuinely different pools that happen to share a name, and those stay
    apart.

    The SHORTER label wins. Equal keys means the two labels already carry the
    same content words, so whatever makes one longer is a word route_key threw
    away as noise — "Spooky Scorched" over "Creature Scorched Spooky", "The
    Slasher - Daily Ops" over "The Slasher - Daily Ops Repeat".

    EQUAL LENGTH IS A TIE, AND THE TIE IS BROKEN ALPHABETICALLY. It has to be
    broken by something, and "first one the closure happened to yield" is not
    something: the Meat Week and Test Your Metal plans are handed out at three
    quest outcomes (Bad / Good / Best) whose rarity word route_key strips and
    whose rates are identical, so they collapse to one row whose label was
    whichever tier the build reached first. `closure` comes off a set, so that
    is not stable between runs, and two builds of the same data produced 33
    rows that differed only in the word "Best" vs "Good" — enough to make
    src/ and dist/ look like they had drifted when the data was the same.
    Alphabetical is arbitrary too, but it is arbitrary the SAME way every time,
    and a build that is not reproducible cannot be diffed.

    Collapsing genuine event tiers into one row is a separate question and is
    logged as its own job — this only stops the label flapping.
    """
    best = {}
    order = []
    for r in routes:
        k = (plan_sources.route_key(r["route"]), round(r.get("rate") or 0, 4))
        if k not in best:
            best[k] = r
            order.append(k)
        else:
            cur = best[k]["route"]
            merged = set(best[k].get("_lvli") or ()) | set(r.get("_lvli") or ())
            if (len(r["route"]), r["route"]) < (len(cur), cur):
                best[k] = r
            if merged:
                best[k]["_lvli"] = merged
    return [best[k] for k in order]


# ── worn outfits are not loot ───────────────────────────────────────────────
# An OTFT is what an NPC spawns WEARING. Killing it does not hand you the
# clothes, so a leveled list that only an OTFT holds is not a drop — but its
# EditorID ("CreatureOutfit_Festive_ScorchedOutfit") says "creature", so it was
# published as an Enemy route. 7 Oct 2026, Apparel Without Plans: Mr. Claus'
# Suit, Executioner Outfit, Dog Armor, every Super Mutant armour piece and the
# "...Scorched" jumpsuits all rested on these. Same for:
#   * "Outfit" lists nothing references at all (CreatureOutfit_Scorched_Uncommon,
#     CreatureOutfit_Spooky_Holiday_BatMask_Outfit) - nothing rolls them;
#   * an outfit list a QUST holds directly (TW002_TrailerOutfit_MutantHeavy):
#     that is a quest alias being dressed, not a reward. Reward lists are GMRW.
_RX_OUTFIT_LIST = re.compile(r"outfit|(^|_)LLO_", re.I)


def worn_only(lists, lvli_refs, c2p, parent_edid):
    """True when every way into `lists` ends at an NPC outfit or at nothing.

    Walks UP from each list. Any holder that is not a list, not an OTFT, not a
    quest dressing an alias in an outfit list, and not an editor-only record
    is a real way in -> False. At least one outfit signal (an OTFT holder or an
    outfit-named list on the way up) is required, so a plain orphan list is
    still left to prune_dead_routes exactly as before.
    """
    seen, stack, outfit = set(), [str(L).upper() for L in lists or ()], False
    if not stack:
        return False
    while stack:
        L = stack.pop()
        if L in seen:
            continue
        seen.add(L)
        edid = parent_edid.get(L, "")
        if _RX_OUTFIT_LIST.search(edid):
            outfit = True
        for rf, redid, rsig in (lvli_refs.get(L) or ()):
            if rsig == "LVLI":
                stack.append((rf or "").upper())
            elif rsig == "OTFT":
                outfit = True
            elif rsig == "QUST" and _RX_OUTFIT_LIST.search(edid):
                outfit = True
            elif plan_sources.is_dev_record(redid):
                continue
            else:
                return False
        for p in c2p.get(L, ()):
            stack.append(p)
    return outfit


def _name_all_vendors(label, chests):
    """`label` names the first trader; add any other named trader whose chest
    stocks the same list. "The Fisherman (Fishing vendor)" ->
    "The Fisherman & Captain Raymond Clark (Fishing vendor)"."""
    names = []
    for c in chests:
        n = vendor_names().name(c)
        if n and n not in names:
            names.append(n)
    if len(names) < 2 or not label.startswith(names[0]):
        return label
    return " & ".join(names) + label[len(names[0]):]


def _creature_majority(names, first):
    """The creature a list's NPCs are variants of, by in-game name.

    `names` is the FULL of every NPC record holding the list (template NPCs
    with no FULL are already left out). Each distinct name scores the number
    of records whose name contains all of its words ("Deathclaw" is inside
    "Glowing Deathclaw" and "Deathclaw Matriarch"); the best, shortest one
    wins when it covers at least a third of the records. Otherwise a single
    word carried by most records names it ("Liberator Mk I".."Mk V" ->
    "Liberator"). Otherwise None: the NPCs share nothing (ten unrelated
    Infestation bosses) and the caller names the row after the list.

    Before this the first NPC the export listed named the row, so
    LLD_Creature_Deathclaw read "Wendigo" (its first holder is the
    AudioTemplateWendigo NPC) and HTO_crLLD_Boss read "Blood Eagle
    Destroyer" (Oct 2026 audit)."""
    every = [n.strip() for n in names if n and n.strip()]
    distinct = sorted(set(every))
    if not distinct:
        return first
    if len(distinct) == 1:
        return distinct[0]
    words = {n: set(re.findall(r"[a-z0-9]+", n.lower())) for n in distinct}
    def support(c):                       # NPC records, not distinct names
        return sum(1 for d in every if words[c] <= words[d])
    best = max(distinct, key=lambda c: (support(c), -len(words[c]), -len(c)))
    if support(best) * 3 >= len(every):
        return best
    # Words most records carry, kept in the game's own spelling and order,
    # minus mark/tier tokens: "Hermit Crab", "Liberator" (not "Liberator Mk").
    wc = collections.Counter(w for d in every for w in words[d])
    common = {w for w, c in wc.items() if c * 2 > len(every) and len(w) > 2
              and not w.isdigit()}
    if common:
        for d in sorted(distinct, key=len):
            if common <= words[d]:
                kept = [t for t in re.findall(r"[A-Za-z0-9.'-]+", d)
                        if re.sub(r"[^a-z0-9]", "", t.lower()) in common]
                if kept:
                    return " ".join(kept)
    return None


def resolve_routes(target_fid, tables, rates, cont_names, npc_names=None,
                   names_only=False, skip_worn=False):
    """Routes for one plan, highest rate first.

    `names_only` returns {route name -> [LVLI FormIDs]} instead, with no rng76
    call at all. That is what lets `add_drop_conditions.py` attach conditions to
    an already-built document in minutes instead of re-running the 80-minute
    build: the names come from THIS function, so the two can never disagree
    about which list is called what.

    `skip_worn` drops rows whose lists are only ever an NPC's worn outfit (see
    worn_only). Off by default - plans never sit in outfit lists, so the plan
    pages do not need it; build_no_plan_apparel_json.py turns it on. It runs
    BEFORE the 12-row cap so outfit rows cannot push real sources off.
    """
    npc_names = npc_names or {}
    target = {target_fid}
    src = ssrc.get_sources([{"formid": target_fid, "sig": "BOOK"}], tables, plan_classify)
    closure = src["lvli_closure"]
    lvli_refs = tables["lvli_refs"]; parent_edid = tables["parent_edid"]

    # One appearance computation per list, shared by containers + the loop below.
    _memo = {}
    def app(L):
        k = str(L).upper()
        if k not in _memo:
            _memo[k] = rates.appearance([L], target)
        return _memo[k]

    routes = []
    by_name = {}    # route label -> the leveled lists that produced it
    # 1) Container types (reuse the farming resolver verbatim, memoised)
    conts = []
    if not names_only:
        try:
            conts = bfu.container_types(
                closure, target,
                lambda L, t: app(L[0] if isinstance(L, (list, tuple)) and L else L),
                cont_names, lvli_refs, parent_edid)
        except Exception:
            conts = []
    for c in conts:
        routes.append({"route": c["name"], "source_type": "container",
                       "rate": round(c["rate"], 6), "rate_display": c["rate_display"]})

    # 2) Non-container routes — resolve each closure list's rng76 appearance and
    #    name it by the list's own semantic (its leveled-list edid). This keeps
    #    the "distinct rate per source" model without dumping every placed holder
    #    (e.g. all Deathclaw NPC variants collapse to one "Deathclaw" row).
    #    Vendors stay distinct by name; creature/event/loot collapse by
    #    (bucket, family, rate) so identical-rate variants dedupe.
    seen_c = set((r["source_type"], r["route"], round(r["rate"], 4)) for r in routes)  # containers
    seen_n = {}  # (bucket, family, rate4) -> route dict (collapse variants)
    seen_v = {}  # (vendor, name, rate4) -> route dict
    quest_named = set()  # labels taken from a GMRW quest title (quest_route_label)
    c2p = tables.get("c2p") or {}
    nested_rate = nested_route_plan(closure, lvli_refs, parent_edid, c2p, cont_names)
    for L in closure:
        via = parent_edid.get(L, "")
        holders = lvli_refs.get(L) or lvli_refs.get(str(L).upper()) or ()
        in_tree = any(p in closure for p in c2p.get(str(L).upper(), ()))
        entries = real_entries(holders, in_tree)
        # A list only other lists hold is a step inside a bigger roll, and its
        # rate is conditional on that roll — see entry_holders(). The list that
        # holds it gets its own route at the real rate. Lists with no parent
        # AND no holder (orphans) keep the old path; prune_dead_routes judges them.
        #
        # ...UNLESS no list above it can publish a route (6 Oct 2026 audit): a
        # parent that is unnamed (zzz_E03A_SpookyScorched_LL_RewardList, paid by
        # the live Mischief Night GMRW) or internal plumbing would otherwise take
        # the whole source with it. Then the nested list keeps its OWN name but
        # carries the real rate — the rate of the topmost list that something
        # outside the list tree rolls — never the conditional one.
        top_lists = None
        if not entries and in_tree:
            top_lists = nested_rate.get(L)
            if not top_lists:
                continue
        # Gifts, pails, treat bags: the loot hangs off the item's magic effect.
        # Named after the item, filed as loot you open (Containers).
        opened = openable_name(entries)
        if opened:
            rate = 0.0 if names_only else app(L)
            if not names_only and (not rate or rate <= 0):
                continue
            by_name.setdefault(opened, []).append(L)
            k = ("container", opened.lower(), round(rate, 4))
            if k not in seen_n:
                seen_n[k] = {"route": opened, "source_type": "container",
                             "rate": round(rate, 6), "rate_display": bfu._fmt_rate(rate),
                             "_lvli": set()}
            seen_n[k]["_lvli"].add(L)
            continue
        # A list that sits inside a bigger roll AND is rolled directly by
        # something else gets its own row for the direct roll only — the lists
        # above it publish their own rows. So that row is judged and named by
        # the direct holders, not by the parent lists: LLS_Loot_Recipes_Armor_All
        # is paid out by the Retirement Plan daily's GMRW, but its parent
        # LLC_Creature_Armor_Boss_Cond made the row a creature drop called
        # "LLC Armor Boss Cond" on 58 armour-mod plans (Oct 2026 audit).
        if in_tree and entries:
            holders = [h for h in holders if not (h[2] == "LVLI" and h[0] in closure)]
        bucket = holder_bucket(via, holders)
        # A list a vendor's stock list ALSO holds is already on the page as
        # that vendor's row (the stock list publishes it at the real rate).
        # When the list's own way in is a quest reward, that is what it is:
        # LL_Recipes_Cooking_Tasty is paid out by a GMRW and printed as a
        # second "Whitespring Gourmet vendor" row at its inside-the-roll rate.
        own = [h for h in holders if not (h[2] == "LVLI" and h[0] in closure)]
        if (bucket == "vendor" and own
                and any(rs in ("GMRW", "QUST") for _rf, _re, rs in own)):
            kb = holder_bucket(via, own)
            if kb != "vendor":
                bucket = kb
                holders = own
        if top_lists:
            # Judge the kind of source by what rolls the top of the tree too:
            # the nested list's own holders are only lists.
            for T in top_lists:
                tb = holder_bucket(parent_edid.get(T, ""),
                                   lvli_refs.get(T) or lvli_refs.get(str(T).upper()) or ())
                if _BUCKET_RANK.index(tb) < _BUCKET_RANK.index(bucket):
                    bucket = tb
        if bucket in ("container", "loot-list"):
            continue  # containers handled above; loot-list = internal plumbing, not a world source
        rate = 0.0 if names_only else (
            max(app(T) for T in top_lists) if top_lists else app(L))   # only now do the rng76 resolve
        if not names_only and (not rate or rate <= 0):
            continue
        # name: a vendor keeps its CONT/holder name; a creature keeps the name of
        # the thing that carries it; everything else uses the list family.
        # Both halves go through source_label so the CONT (the vendor's chest)
        # and the LVLI (their stock) produce the SAME string and collapse_routes
        # can then merge them. humanize() is kept only as the last resort, since
        # it is what produced "Minerva LLV Gold Vendor" next to "Minerva Gold
        # Vendor Chest" in the first place.
        vend_name = creature_name = None
        vend_chests = []          # every named merchant chest that stocks it
        npc_fulls = []            # every NPC that carries it, by in-game name
        curated_creature = False  # a hand-checked / bounty name always wins
        for rf, redid, rsig in holders:
            # An editor-only holder is not a place a player can go, and must not
            # be prettified into one. humanize() is a last-resort fallback that
            # will name ANYTHING, so it published the cut NPC CUT_LvlSubBoss as
            # the route "CUT Lvl Sub Boss". source_label() already returns None
            # for these; this stops the fallback undoing that.
            if plan_sources.is_dev_record(redid):
                continue
            b = plan_classify(rsig, redid, via)
            if b == "vendor" and not vend_name:
                vend_name = (plan_sources.CURATED_LABELS.get(redid)
                             or vendor_chest_full(cont_names, rf)
                             or better_vendor_label(source_label(redid), source_label(via))
                             or humanize(redid))
                # The NPC who owns this chest, by the name players see
                # (plan_sources.VendorNames; NPC2_Vendors export). A stock list
                # held by another list is named after the chest that list sits in.
                chest = rf if rsig == "CONT" else (
                    vendor_chest_for_list(rf, lvli_refs) if rsig == "LVLI" else None)
                if chest:
                    vend_name = vendor_names().label(chest, vend_name)
            if b == "vendor" and rsig == "CONT" and vendor_names().name(rf):
                vend_chests.append(rf)
            if rsig == "NPC_" and npc_names.get((rf or "").upper()):
                npc_fulls.append(npc_names[(rf or "").upper()])
            if b == "creature" and not creature_name:
                curated_creature = bool(plan_sources.CURATED_LABELS.get(redid)
                                        or plan_sources.bounty_npc_label(
                                            redid, npc_names.get((rf or "").upper())))
                # An NPC's FULL name beats anything derivable from its EditorID:
                # "Pint-Sized Slasher" rather than "SDOW Burn Bounty BIG Slasher".
                creature_name = (plan_sources.CURATED_LABELS.get(redid)
                                 or plan_sources.bounty_npc_label(redid, npc_names.get((rf or "").upper()))
                                 or npc_names.get((rf or "").upper())
                                 or source_label(redid) or humanize(redid))
        # One stock list on two traders' shelves: name both, not whichever
        # chest the export happened to list first (Fishing_LL_Vendor_
        # FishermansRest is The Fisherman's AND Captain Raymond Clark's).
        if bucket == "vendor" and vend_name:
            vend_name = _name_all_vendors(vend_name, vend_chests)
        # A creature list is named after the creature its NPCs are variants
        # of, not whichever NPC the export listed first: LLD_Creature_Deathclaw's
        # first holder is AudioTemplateWendigo, so Deathclaw drops were printed
        # as "Wendigo". Bosses that share nothing name the list instead:
        # HTO_crLLD_Boss rolls for ten different Infestation bosses and read
        # "Blood Eagle Destroyer" (Oct 2026 audit).
        if creature_name and not curated_creature:
            creature_name = _creature_majority(npc_fulls, creature_name)
        if bucket == "vendor" and vend_name:
            name = vend_name
            by_name.setdefault(name, []).append(L)
            key = ("vendor", name, round(rate, 4))
            if key not in seen_c:
                seen_c.add(key)
                seen_v[key] = {"route": name, "source_type": "vendor",
                               "rate": round(rate, 6), "rate_display": bfu._fmt_rate(rate),
                               "_lvli": set()}
                routes.append(seen_v[key])
            if key in seen_v:
                seen_v[key]["_lvli"].add(L)
            continue
        # source_label reads the raw EditorID (not humanize()'d) because it needs
        # the underscore boundaries to find the area code. None = editor-only
        # list or wiring end to end, so it is not a route a player can take.
        fam = (creature_name if bucket == "creature" and creature_name
               else source_label(via or str(L)))
        if bucket == "vendor" and fam and not holders:
            # A vendor stock list nothing references (the export can't see the
            # faction that sells it): name it after its chest's NPC when the
            # EditorIDs agree, so it merges with that vendor's row.
            ch = vendor_names().orphan_chest(via, chest_edids())
            if ch:
                fam = vendor_names().label(ch, fam)
        if not fam:
            continue
        if bucket == "event-quest":
            # "Side Quests" says nothing; the GMRW that pays this list out
            # names the real quest.
            qfam = plan_sources.quest_route_label(fam, str(L).upper(), lvli_refs,
                                                  GMRW_QUESTS)
            # Still reads like an EditorID ("Schematic Armor Raider", "CB04")?
            # The quest record that rolls this list names it for a player.
            if not qfam and plan_sources.looks_like_wiring(fam):
                qfam = plan_sources.direct_quest_label(entries, GMRW_QUESTS, QUEST_TITLES)
            # A bare loot word ("Power Armor", "Chems", "SPOTLIGHT Workshop")
            # says what is in the pool, not where it comes from; the quest or
            # event whose reward record rolls the list does (6 Oct 2026).
            # Not for the system names (AREA_CODE: "Raids", "Daily Ops",
            # "Expeditions" ...) — those ARE the player-facing name, and the
            # quest behind them is a stage ("Enclave Squad Module" is a Raid).
            if (not qfam and ":" not in fam and " - " not in fam
                    and fam not in _AREA_NAMES):
                dq = plan_sources.direct_quest_label(entries, GMRW_QUESTS, QUEST_TITLES)
                if dq and not (set(plan_sources.route_key(dq).split())
                               & set(plan_sources.route_key(fam).split())):
                    qfam = dq
            if qfam:
                fam = qfam
                quest_named.add(qfam)
        by_name.setdefault(fam, []).append(L)
        k = (bucket, fam.lower(), round(rate, 4))
        if k not in seen_n:
            seen_n[k] = {"route": fam, "source_type": bucket,
                         "rate": round(rate, 6), "rate_display": bfu._fmt_rate(rate),
                         "_lvli": set()}
        seen_n[k]["_lvli"].add(L)
    routes.extend(seen_n.values())
    if names_only:
        return by_name
    routes = collapse_routes(routes)

    # Same name, different rates: say what differs (tier 2 / tier 3 ...).
    groups = collections.defaultdict(list)
    for r in routes:
        if r.get("_lvli"):
            groups[r["route"]].append(r)
    for label, rows in groups.items():
        if len(rows) < 2:
            continue
        sfx = plan_sources.tier_suffixes([sorted(r["_lvli"]) for r in rows], parent_edid)
        for r, sx in zip(rows, sfx):
            if sx:
                r["route"] = f"{label} ({sx})"
                if label in quest_named:
                    quest_named.add(r["route"])
                by_name.setdefault(r["route"], []).extend(r["_lvli"])

    if skip_worn:
        routes = [r for r in routes
                  if not worn_only(r.get("_lvli") or by_name.get(r["route"], ()),
                                   lvli_refs, c2p, parent_edid)]
    routes = group_vendor_rows(routes)
    routes.sort(key=lambda r: (-(r["rate"] or 0), r["source_type"], r["route"].lower()))
    routes = routes[:12]
    for r in routes:
        # The lists behind THIS row (this name at this rate). Was looked up by
        # name alone, so two rows sharing a name — Radiation Rumble at 2.5% and
        # 1.25% — both listed every list either rate came from, and the drop
        # conditions / dead-route checks read the wrong ones.
        own = r.pop("_lvli", None)
        r["lvli"] = sorted(set(own) if own else set(by_name.get(r["route"], ())))
        if r["route"] in quest_named:
            # Named after the quest that pays it out -- lets New Plans file it
            # under Quests now the label no longer says "Side Quests".
            r["quest_reward"] = True
    return routes   # cap: a plan's most-likely dozen sources, highest rate first

# ── main ─────────────────────────────────────────────────────────────────────
SIG_INDEX = {}
def main(argv=None):
    global TSV, DIST, SIG_INDEX, QUEST_NAMES
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="cap roster (0=all) for testing")
    ap.add_argument("--offset", type=int, default=0, help="skip the first N of the roster (for chunked builds)")
    ap.add_argument("--only", default="", help="only plans whose name contains this substring")
    ap.add_argument("--only-ids", default="",
                    help="JSON file with a list of plan BOOK FormIDs; rebuild just those "
                         "(post passes skipped, like --only). For targeted re-resolves.")
    ap.add_argument("--data-dir", default=TSV, help="TSV export root (PTS build points this at the PTS tsvs)")
    ap.add_argument("--outdir", default=DIST, help="output dir (PTS build relocates dist/ -> dist/pts/)")
    ap.add_argument("--out", default="", help="explicit output file (overrides --outdir/plan_master.json)")
    ap.add_argument("--report", default="", help="write an unresolved-flags report here")
    ap.add_argument("--no-routes", action="store_true",
                    help="skip rng76 route resolution (fast full-roster flag pass)")
    args = ap.parse_args(argv)

    TSV = args.data_dir
    DIST = args.outdir
    if not args.out:
        args.out = os.path.join(DIST, "plan_master.json")
    print("[plan-obtain] loading exports ...")
    SIG_INDEX = build_sig_index()
    cobj_idx  = build_cobj_index()
    book_ent  = build_book_entry_index()
    # Output & Effects inputs. Cheap table loads — no rng76 involved.
    prop_idx  = build_omod_prop_index()
    ench_idx  = build_ench_index()
    mgef_idx  = build_mgef_index()
    perk_idx  = build_perk_desc_by_mgef()
    curve_idx = build_curve_index()
    omod_by_edid = build_omod_edid_index()
    print(f"[plan-obtain] effects tables: {len(prop_idx)} OMOD props, "
          f"{len(ench_idx)} ENCH, {len(mgef_idx)} MGEF, {len(perk_idx)} perk-desc, "
          f"{len(curve_idx)} curves")
    tables = rates = cont_names = None
    unlock_idx = None
    npc_names = {}
    if not args.no_routes:
        tables    = ssrc.load_tables(TSV)
        rates     = bfu.VendorRates(rng76.Rng76Data.from_tsv_root(TSV))
        cont_names = bfu._load_cont_names(TSV)
        # Creature routes are named after whatever carries the loot, and the NPC
        # export is the only place its in-game name exists. Without this the row
        # reads "SDOW Burn Bounty BIG Slasher" instead of "Pint-Sized Slasher".
        npcf = newest("NPC_Export_*.tsv")
        if npcf:
            for r in read_rows(npcf):
                nfid = (r.get("FormID") or "").strip().upper()
                full = (r.get("FULL") or "").strip()
                if nfid and full and nfid not in npc_names:
                    npc_names[nfid] = full
            print(f"[plan-obtain] NPC names: {len(npc_names)}")
        # Names first: source_label() consults QUEST_NAMES on every route, so the
        # index has to exist before the first resolve_routes() call, not after.
        unlock_idx = plan_sources.UnlockIndex(TSV, lambda pat, root: newest(pat, root))
        QUEST_NAMES = unlock_idx.quest_names
        GMRW_QUESTS.clear(); GMRW_QUESTS.update(unlock_idx.gmrw_quests)
        QUEST_TITLES.clear(); QUEST_TITLES.update(getattr(unlock_idx, "_quests_by_fid", {}))
        print(f"[plan-obtain] quest names: {sum(1 for v in QUEST_NAMES.exact.values() if v)} "
              f"unambiguous prefixes, {len(QUEST_NAMES.family)} families")
    # What kind of place each route is (quest / event / enemy / corpse), read
    # off the QUEST, GMRW and NPC records — the ledger files it by this.
    route_kinds = None
    if not args.no_routes:
        try:
            route_kinds = plan_route_kinds.Ctx(TSV)
        except Exception as exc:                  # noqa: BLE001 - never fatal
            print(f"  WARNING: route kinds unavailable: {exc}", file=sys.stderr)

    # COBJ.GNAM — what unlocks each recipe, straight from the game files. Needed
    # whether or not routes are being resolved, because cut detection consults
    # it and --no-routes still publishes the cut flag.
    recipe_unlocks = plan_unlocks.RecipeUnlocks(TSV, lambda pat, root: newest(pat, root))
    print(recipe_unlocks.report())

    bf = newest("BOOK_Export_*.tsv")
    roster = []
    for row in read_rows(bf):
        full = (row.get("FULL") or "").strip()
        if not (full.startswith("Plan: ") or full.startswith("Recipe: ")):
            continue
        # BOOK ReferencedBy Ref1..RefN → first :COBJ is the recipe it teaches
        # (fallback when the plan has no HasLearnedRecipe drop entry).
        co_ref = ""
        for j in range(1, 46):
            v = (row.get(f"Ref{j}") or "").strip()
            if v.upper().endswith(":COBJ"):
                co_ref = v.split(":")[0].strip().upper(); break
        row["_co_ref"] = co_ref
        roster.append(row)
    print(f"[plan-obtain] roster: {len(roster)} plan/recipe BOOKs")

    if args.only:
        roster = [r for r in roster if args.only.lower() in (r.get("FULL") or "").lower()]
    if args.only_ids:
        with open(args.only_ids, encoding="utf-8") as _f:
            _ids = {str(x).upper() for x in json.load(_f)}
        roster = [r for r in roster if (r.get("FormID") or "").strip().upper() in _ids]
        args.only = args.only or "__ids__"      # reuse --only's "skip post passes" gates
    if args.offset:
        roster = roster[args.offset:]
    if args.limit:
        roster = roster[:args.limit]
    print(f"[plan-obtain] building {len(roster)} plans ...")

    items = []
    unresolved = {"created_object": [], "tradeable": [], "stops_dropping": [],
                  "no_routes": [], "backpack_cosmetic_dropped": [], "no_effects": [],
                  "cut": []}
    for i, row in enumerate(roster):
        fid  = (row.get("FormID") or "").strip().upper()
        edid = (row.get("EDID") or "").strip()
        name = (row.get("FULL") or "").strip()
        kwblob = " ".join((row.get(f"KW{j}") or "") for j in range(1, 8))
        entries = book_ent.get(fid, [])

        # created object: prefer the HasLearnedRecipe COBJ (authoritative), else
        # the BOOK's own ReferencedBy :COBJ link (vendor-only / no-drop plans).
        co_fid = ""
        for en in entries:
            if en["recipe_cobj"]:
                co_fid = en["recipe_cobj"]; break
        if not co_fid:
            co_fid = row.get("_co_ref") or ""
        cobj = cobj_idx.get(co_fid) if co_fid else None
        # CondProxy recipes carry no CNAM; hop to the real recipe if it points on.
        if cobj and not cobj.get("cnam_fid") and "condproxy" in cobj.get("edid","").lower():
            # find a sibling COBJ sharing the stem after 'CondProxy_' with a CNAM
            stem = re.sub(r".*condproxy_?", "", cobj["edid"], flags=re.I).lower()
            if stem:
                for cfid, c in cobj_idx.items():
                    ce = c.get("edid","").lower()
                    if c.get("cnam_fid") and stem in ce and "condproxy" not in ce:
                        co_fid, cobj = cfid, c; break
        # Neither link resolved: ask plan_unlocks, which matches on the COBJ's
        # own GNAM, then on the EditorID stem, then on the created record's
        # name — refusing any key that returns more than one candidate. That is
        # the only way an orphaned plan like the Slasher bobber (ReferencedBy 0,
        # no drop entry) ever reaches its recipe.
        if not co_fid:
            linked, how = recipe_unlocks.link(fid, edid, name)
            if linked:
                co_fid, cobj = linked, cobj_idx.get(linked) or cobj
                unresolved.setdefault("cobj_linked", []).append(f"{name} [{how}]")

        # A scoreboard vendor plan: borrow the weapon from its twin recipe.
        cobj = borrow_twin_cnam(cobj, co_fid, cobj_idx)
        # Still no created object? Recover it from the BOOK's own EDID.
        if not (cobj or {}).get("cnam_fid"):
            om = omod_from_book_edid(edid, omod_by_edid)
            if om:
                cobj = dict(cobj or {"formid": co_fid, "edid": ""})
                cobj["cnam_fid"], cobj["cnam_edid"] = om["fid"], om["edid"]
        cat, has_img, cnam_sig, cnam_fid, cnam_edid = classify_plan(edid, cobj)
        if not cnam_sig and not (cobj or {}).get("edid"):
            unresolved["created_object"].append(name)

        # Backpack bucket: publish the functional mods only. Skins and flair are
        # dropped here, before any rng76 route work is spent on them.
        bp_class = None
        if cat == "backpack-mod":
            bp_class = backpack_class(cnam_edid, (cobj or {}).get("edid"), name)
            if bp_class != "mod":
                unresolved["backpack_cosmetic_dropped"].append(f"{name} [{bp_class}]")
                continue

        # Output & Effects — only a created OMOD can have any.
        effects = None
        if cnam_sig == "OMOD" and cnam_fid:
            effects = resolve_effects(cnam_fid, prop_idx, ench_idx, mgef_idx,
                                      perk_idx, curve_idx)
            if effects is None:
                unresolved["no_effects"].append(name)

        tradeable = resolve_tradeable(kwblob)
        if tradeable is None: unresolved["tradeable"].append(name)
        stops = resolve_stops_dropping(entries)
        if stops is None: unresolved["stops_dropping"].append(name)

        # Cut content. This field shipped as a hardcoded False for the life of
        # this builder, so 291 dev leftovers rendered as though a player could
        # go and get them. The EditorID decides; the reference count only
        # corroborates. See plan_sources.cut_reason().
        refs = [v for v in ((row.get(f"Ref{j}") or "").strip() for j in range(1, 46)) if v]
        recipe_unlock = recipe_unlocks.proof_of_life(co_fid)
        cut_why = plan_sources.cut_reason(edid, refs, recipe_unlock=recipe_unlock)
        if not cut_why:
            cut_why = plan_sources.dead_plan_reason(refs, bool(cobj), recipe_unlock)
        if cut_why:
            unresolved["cut"].append(f"{name} [{edid}]")

        routes = [] if args.no_routes else resolve_routes(fid, tables, rates, cont_names, npc_names)
        if routes and route_kinds is not None:
            routes = plan_route_kinds.apply_to_routes(routes, route_kinds)

        # Non-drop routes: bought, quested, challenged, placed. Kept OUT of
        # obtain_routes so every row in that table still carries a real rng76
        # rate — see the UNLOCKS section of plan_sources.py. Cut plans are not
        # worth the lookup: nothing references them, which is why they are cut.
        unlocks = [] if (args.no_routes or cut_why or unlock_idx is None) \
                  else unlock_idx.unlocks_for(fid, edid, refs, has_routes=bool(routes))
        # The GNAM sentence is a resolved route, not an EditorID inference, so it
        # leads. It is also the only source for a plan whose BOOK nothing
        # references, which is every plan the cut rescue above just saved.
        gnam_sentence = recipe_unlocks.sentence(recipe_unlock)
        if gnam_sentence and gnam_sentence not in unlocks:
            unlocks.insert(0, gnam_sentence)

        if not args.no_routes and not routes and not unlocks and not cut_why:
            unresolved["no_routes"].append(name)

        cat_label = category_label(cat, has_img, cnam_sig)
        if cut_why:
            obtain_text = ("Cut content. This plan is still in the game files but "
                           "nothing gives it out — it cannot be obtained in game.")
        elif routes:
            obtain_text = (PHYSICAL_PLAN + " It drops from the sources below, each "
                           "with its resolved chance.")
        elif unlocks:
            obtain_text = PHYSICAL_PLAN + " It is not random loot — see below."
        else:
            obtain_text = (PHYSICAL_PLAN + " No source was resolved from the game "
                           "files — see Technical for the recipe details.")

        item = {
            "kind": "plan", "brand": "df", "type": cat,
            "id": f"PLAN_{fid}", "name": name,
            "has_image_box": has_img, "image_dir": cat,
            "obtain": obtain_text, "category_label": cat_label,
            "obtain_routes": routes,
            "obtain_unlocks": unlocks,
            "plan_item": {"formid": fid, "edid": edid},
            "cobj": ({"formid": co_fid, "edid": cobj["edid"]} if cobj else None),
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": cnam_sig} if cnam_fid else None),
            "tradeable": tradeable, "stops_dropping": stops,
            "effects": effects,
            "cut": bool(cut_why),
            "cut_reason": cut_why,
        }
        # The fixed How to Obtain table the pages draw — every route printed,
        # N/A included. Pure re-sort of the two lists above (plan_sources
        # section 4), so it costs nothing and resolves nothing new.
        item["obtain_ledger"] = plan_sources.obtain_ledger(item)

        if bp_class:
            item["backpack_class"] = bp_class
            item["display_name"] = backpack_display_name(name)
        items.append(item)
        if (i+1) % 250 == 0:
            print(f"   ... {i+1}/{len(roster)}")

    # Recipes with no plan book. Appended after the roster walk because they are
    # not in it: the roster is BOOK rows, and these craftables have none. Costs
    # no rates — they are unlock routes by definition.
    if not args.offset and not args.limit and not args.only:
        print("[plan-obtain] recipes with no plan book:")
        plan_recipe_rows.report(plan_recipe_rows.attach(items, TSV))

    # Drop Conditions: when each source gives the plan at all, off the leveled
    # list entry conditions. Pure joins over the LVLI entries export -- no rate
    # is touched -- so it runs after the roster walk, on the finished routes.
    if not args.no_routes and not args.offset and not args.limit and not args.only:
        print("[plan-obtain] drop conditions:")
        plan_conditions.report(plan_conditions.attach(items, TSV, newest))

    # Dead routes off the page: a list nothing in the game rolls (retired Minerva
    # backlog, zzz lists, unhookable pools, First Match entries that never win)
    # moves to `retired_routes` and the ledger is rebuilt. Same proof as the
    # Current Bugged Plans page (prune_dead_routes.py). Before source tags, so a
    # retired Gold Bullion route can't leave a "Gold" pill behind.
    if not args.no_routes and not args.offset and not args.limit and not args.only:
        print("[plan-obtain] dead routes:")
        prune_dead_routes.report(prune_dead_routes.attach(items, TSV))

    # The one-word "where does this come from" tag on each row and in the
    # export poster's SOURCE column. Pure string work over the routes that were
    # just resolved.
    if not args.offset and not args.limit and not args.only:
        print("[plan-obtain] source tags:")
        plan_source_pill.report(plan_source_pill.attach(items))

    # Row titles: model-first for weapon paints (plan_display_names). Pure string
    # work over the finished roster, so it runs last and costs nothing. The game's
    # own name stays in `name`; this only writes `display_name`.
    if not args.offset and not args.limit and not args.only:
        print("[plan-obtain] row titles:")
        plan_display_names.report(plan_display_names.attach(items, TSV, newest))

    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "source_files": {k: os.path.basename(newest(v) or "") for k, v in SIG_EXPORTS.items()},
        "items": items,
    }
    # Art. Reuses the picture another page already hosts where one exists, else
    # a stem staged under this page's own folder — plan_images.py. Resolution
    # only, no downloads and no file checks against the server, so it cannot
    # fail the build: on any error the rows keep their empty images list and
    # the pages render the placeholder slot.
    try:
        idx, staged = plan_images.load(os.path.dirname(args.out) or DIST,
                                       args.data_dir)
        # Legacy Nuclear Winter rewards with no plan (legacy_nw.reward_rows) —
        # added BEFORE the art pass so they get pictures and a page like any row.
        if getattr(idx, "legacy", None):
            import legacy_nw
            print(f"  legacy_nw: {legacy_nw.reward_rows(items, idx.legacy)} Nuclear Winter reward row(s) with no plan")
        import plan_sources as _ps
        print(f"  slasher routes tagged: {_ps.tag_slasher_routes(items)}")
        plan_images.report(plan_images.attach(items, idx, staged))
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: image resolve skipped: {exc}", file=sys.stderr)

    # Pages carved out of a bucket. Fishing rod gear sits in `recipe` and
    # `weapon`, snow globes in `recipe` - none of which is what they are - so
    # they get tagged here and the renderer pulls them onto their own page.
    # Reads image_dir, so it MUST run after the image resolve above. No exports
    # of its own, so it cannot fail on a missing TSV, but it is wrapped anyway
    # like everything else in this tail.
    # Armour or clothing. plan_images sends the whole `armour` bucket to the Body
    # Armour page; this corrects the rows whose record carries no resistance
    # ladder and no durability — they are outfits, and they belong on Apparel.
    # MUST run before plan_subpages, which routes on image_dir.
    try:
        add_weapon_groups.set_tsv_dir(args.data_dir)
        plan_apparel_class.report(plan_apparel_class.attach(items))
        out["apparel_class_schema"] = plan_apparel_class.SCHEMA
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: apparel reclassify skipped: {exc}", file=sys.stderr)

    try:
        plan_subpages.report(plan_subpages.attach(items))
        out["plan_subpages_schema"] = plan_subpages.SCHEMA
        out["plan_subpages"] = plan_subpages.config()
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: sub-page tagging skipped: {exc}", file=sys.stderr)

    # Weapon page grouping. /df/plan-checklists/weapon/ is one root expand per
    # weapon with Mods and Skins inside it, which needs each plan tied to the
    # weapon it belongs to. That is a join against the OMOD/WEAP/COBJ exports
    # and takes about a second, so it runs here rather than as a workflow step
    # that every channel would have to remember. Same contract as the image
    # resolve above: never fatal. If it fails the rows lose their weapon_* fields
    # and the page falls back to the flat A-Z list it had before.
    # Every enricher below reads the exports for THIS channel. Without this the
    # PTS build would group PTS plans against live records, which is the quiet
    # kind of wrong: it produces a full-looking page made of the wrong data.
    try:
        add_weapon_groups.set_tsv_dir(args.data_dir)
        plan_consumables.set_tsv_dir(args.data_dir)
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: export root override skipped: {exc}", file=sys.stderr)

    # Recipe page grouping. The page is food, drink, alcohol, serums and chems
    # and nothing else; the root and sub-expand come from the ALCH record's own
    # keywords. Reads plan_page, so it MUST run after plan_subpages.
    try:
        cstats = plan_consumables.attach(items)
        plan_consumables.report(cstats)
        if cstats:
            out["consumables_schema"] = plan_consumables.SCHEMA
            out["consumable_groups"] = plan_consumables.config()
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: consumable grouping skipped: {exc}", file=sys.stderr)

    try:
        stats = add_weapon_groups.attach(items)
        add_weapon_groups.report(stats)
        if stats:
            out["weapon_groups_schema"] = add_weapon_groups.SCHEMA
            out["weapon_groups_sources"] = stats["sources"]
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: weapon grouping skipped: {exc}", file=sys.stderr)

    # Body Armour and Power Armour, same shape as the weapon page: one root
    # expand per set with Mods and Skins inside it.
    try:
        astats = add_armour_groups.attach(items)
        add_armour_groups.report(astats)
        if astats:
            out["armour_groups_schema"] = add_armour_groups.SCHEMA
            out["armour_groups_sources"] = astats["sources"]
    except Exception as exc:                      # noqa: BLE001 - never fatal
        print(f"  WARNING: armour grouping skipped: {exc}", file=sys.stderr)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[plan-obtain] wrote {args.out}  ({len(items)} plans)")

    # /df/plan-checklists/make-your-own/ reads its own compact feed, every value
    # in it copied verbatim out of the plan_master just written. Nothing else
    # rebuilds it, so left to itself it serves the PREVIOUS roster's answers --
    # which is exactly what happened: on 20 Sept 2026 the page was still showing
    # 901 plans as "Unknown" tradeable after the roster had resolved them. It is
    # a dict copy over rows already in memory, so it costs nothing to run here,
    # and it is never fatal like the rest of this tail.
    #
    # Skipped on a partial build (--offset/--limit/--only) and on a custom --out:
    # the feed must be built from a COMPLETE roster, and a chunk is not one.
    if (not args.offset and not args.limit and not args.only
            and os.path.basename(args.out) == "plan_master.json"):
        try:
            import build_make_plan_checklist_json
            build_make_plan_checklist_json.build(os.path.dirname(args.out) or DIST)
        except Exception as exc:                  # noqa: BLE001 - never fatal
            print(f"  WARNING: make-your-own feed skipped: {exc}", file=sys.stderr)

    # category + flag summary
    from collections import Counter
    print("  categories:", dict(Counter(it["type"] for it in items)))
    print("  image box :", dict(Counter(it["has_image_box"] for it in items)))
    print("  tradeable :", dict(Counter(it["tradeable"] for it in items)))
    print("  stops_drop:", dict(Counter(it["stops_dropping"] for it in items)))
    print("  effects   :", sum(1 for it in items if it.get("effects")), "of", len(items))
    print("  backpack cosmetics dropped:", len(unresolved["backpack_cosmetic_dropped"]))
    live = [it for it in items if not it["cut"]]
    print("  cut content:", len(unresolved["cut"]), "of", len(items),
          f"({len(live)} obtainable)")
    print("  with drop routes:", sum(1 for it in live if it["obtain_routes"]),
          "| with unlock routes:", sum(1 for it in live if it.get("obtain_unlocks")),
          "| no source at all:", sum(1 for it in live
                                     if not it["obtain_routes"] and not it.get("obtain_unlocks")))
    print("  UNRESOLVED created_object:", len(unresolved["created_object"]),
          "| tradeable:", len(unresolved["tradeable"]),
          "| stops_dropping:", len(unresolved["stops_dropping"]),
          "| no_routes:", len(unresolved["no_routes"]))
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(unresolved, f, ensure_ascii=False, indent=2)
    return out

if __name__ == "__main__":
    main()
