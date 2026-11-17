#!/usr/bin/env python3
"""
build_challenge_keywords_json.py
================================
Builds the Challenge Keyword Finder for
/df/score-challenges/challenge-keyword-finder/ (Duchess, 17 Nov 2026).

Pick Daily or Weekly, pick a challenge, and the page lists what counts toward
it in plain words — the in-game names, A-Z, no EDIDs / keywords / FormIDs.

Where the names come from (game data only, never typed by hand):
  - dist/challenges/challenges.json   the Daily + Weekly challenge list, the
                                      required count (GLOB-resolved), Epic /
                                      cut flags, the raw CHAL conditions and the
                                      site's existing guide matches (guides[]).
  - CNDF_Export_*.tsv                 condition forms, expanded to the leaf.
  - KYWD_Export_*_Refs.tsv            which records carry a keyword (items,
                                      weapons, apparel, races, NPCs).
  - FLST_Export_*_Entries.tsv         form list members (events, quests, drinks).
  - NPC_Export_*.tsv                  only to drop NPC copies / pets from races.
  - COBJ_Export_*.tsv                 what a workshop recipe builds.

Hand-made, editable (tsv/):
  - challenge_counter_targets.tsv     challenges with NO conditions are counted
                                      by the game's stat counter ("Canines
                                      Killed"). This maps a counter to the actor
                                      keyword(s) it stands for.
  - challenge_name_overrides.tsv      rename a raw game name to what players see
                                      (e.g. "FEV Hound" -> "Mutant Hound"), or
                                      leave display blank to drop it.
  - challenge_guide_map.tsv           add / remove guide links per challenge.

Guides: a link is only written if the guide row in tsv/guide_index.tsv is
visibility=public AND status=published (the same rule build_challenges_json_v3.py
and functions.php use). The renderer also hides any URL the WordPress
page-visibility map restricts.

Outputs:
  - dist/challenges/challenge_keywords.json        (live)
  - dist/pts/challenges/challenge_keywords.json    (--pts)
  - audits/challenge_keywords_unresolved.tsv       conditions left off the page

Usage:  python src/build_challenge_keywords_json.py
PTS:    python src/build_challenge_keywords_json.py --pts
"""

import csv
import io
import json
import os
import re
import sys
from collections import defaultdict, OrderedDict
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_spin_wheel_json as spin  # noqa: E402  (shared is_cut() -- one cut rule site-wide)
import tsv_source                     # noqa: E402
import plan_sources                   # noqa: E402  (THE site-wide cut test: is_cut_content)
import cut_content                    # noqa: E402  (item-level cut list shared by the menu / recipe builders)

ROOT = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
OUT_DIR = os.path.join(ROOT, "dist", "pts", "challenges") if PTS else os.path.join(ROOT, "dist", "challenges")
OUT_NAME = "challenge_keywords.json"
AUDIT_PATH = os.path.join(ROOT, "audits", "challenge_keywords_unresolved%s.tsv" % ("_pts" if PTS else ""))
GUIDE_INDEX = os.path.join(ROOT, "tsv", "guide_index.tsv")
COUNTER_TSV = os.path.join(ROOT, "tsv", "challenge_counter_targets.tsv")
NAMES_TSV = os.path.join(ROOT, "tsv", "challenge_name_overrides.tsv")
GUIDE_MAP_TSV = os.path.join(ROOT, "tsv", "challenge_guide_map.tsv")
SITUATIONS_TSV = os.path.join(ROOT, "tsv", "challenge_situations.tsv")
HIDE_TSV = os.path.join(ROOT, "tsv", "challenge_finder_hide.tsv")
COUNTER_LINES_TSV = os.path.join(ROOT, "tsv", "challenge_counter_lines.tsv")
PET_TYPES_JSON = os.path.join(ROOT, "dist", *(["pts"] if PTS else []), "world_pet_types.json")

TYPES = ["Daily", "Weekly"]

csv.field_size_limit(10**9)


def read_tsv(path):
    raw = open(path, "rb").read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"))


def read_edit_tsv(path):
    """Hand-edited TSVs: '#' lines are comments."""
    if not os.path.exists(path):
        return []
    lines = [l for l in open(path, encoding="utf-8-sig").read().splitlines()
             if l.strip() and not l.lstrip().startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


# ─────────────────────────────────────────────────────────────────────────────
#  Game data
# ─────────────────────────────────────────────────────────────────────────────
def load_challenges():
    path = os.path.join(ROOT, "dist", *(["pts"] if PTS else []), "challenges", "challenges.json")
    if PTS and not os.path.exists(path):
        path = os.path.join(ROOT, "dist", "challenges", "challenges.json")
    pages = json.load(open(path, encoding="utf-8")).get("pages") or {}
    return pages, os.path.relpath(path, ROOT).replace("\\", "/")


KW_REFS = defaultdict(list)       # keyword EDID -> [(sig, fid, edid, name)]
FLST = defaultdict(list)          # FLST fid -> [(sig, fid, edid, name)]
FLST_BY_FID = {}                  # FLST fid -> (edid, name)
CNDF = {}                         # fid -> [cond strings]
COBJ = {}                         # COBJ fid -> (created edid, created name)
NPC_RACE = {}                     # npc fid -> race fid
NPC_NAME = {}                     # npc fid -> (edid, name or template name)
RECIPES = defaultdict(list)       # created ALCH fid -> [[(ingredient edid, qty)], ...] (cooking bench)
ITEM_NAME = {}                    # item / component EDID -> in-game name
WORLD_PETS = []                   # World Pet species names (Cat, Dog ...)
HIDE = set()                      # challenge EDIDs left off the finder
COUNTER_LINES = defaultdict(list) # stat counter (lower) -> [(group, text)]
MENU = {}                         # Workshop2 category keyword EDID -> [recipe fids] (game's build menu)
MENU_PATH = {}                    # Workshop2 keyword EDID -> "Defense > Turrets"
COBJ_CNAM = {}                    # recipe fid -> (CNAM fid, CNAM edid, CNAM name)
MENU_MAX = 300                    # bigger than this -> "anything from the X tab" line instead of a list
SOURCES = {}


def load_item_names():
    """EDID -> in-game name for recipe ingredients (food, junk, components)."""
    out = {}
    for pat, ecol, ncol in (("ALCH_Export_*.tsv", "ALCH_EDID", "FULL"),
                            ("MISC_Export_*.tsv", "EDID", "FULL"),
                            ("CMPO_Export_*.tsv", "CMPO_EDID", "FULL")):
        p = tsv_source.newest(pat, channel=CHANNEL, required=False,
                              exclude=("_Effects", "_Refs", "_Entries", "_Locations"))
        if not p:
            continue
        SOURCES.setdefault("ingredientNames", []).append(os.path.basename(p))
        for r in read_tsv(p):
            e, n = (r.get(ecol) or "").strip(), (r.get(ncol) or "").strip()
            for part in (r.get("MCQP") or "").split("|"):   # junk -> components it scraps into
                comp = part.split(":")[0].strip()
                if comp and e and n and not e.lower().startswith(("miscmod", "zzzmiscmod", "debug")):
                    SCRAPS_INTO[comp].add((e, n))
            if e and n and e not in out:
                out[e] = n
    return out


def load_world_pets():
    """World Pet species, from the World Pets page data (built from the game's skin actors)."""
    path = PET_TYPES_JSON if os.path.exists(PET_TYPES_JSON) else os.path.join(ROOT, "dist", "world_pet_types.json")
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        print("  [WARN] world_pet_types.json not found -- pet lists skipped", file=sys.stderr)
        return
    SOURCES["worldPets"] = os.path.relpath(path, ROOT).replace("\\", "/")
    WORLD_PETS[:] = sorted({(s.get("name") or "").strip() for s in d.get("species") or [] if s.get("name")})


def load_build_menu():
    """The game's own CAMP build menu (misc/workshop/workshop.json, pulled by
    extract_workshop_menu.py) -> which recipes sit under each Workshop2 tab."""
    p = (tsv_source.newest("WORKSHOP_Export_*.json", channel=CHANNEL, required=False)
         or tsv_source.newest("WORKSHOP_Export_*.json", channel="live", required=False))
    if not p:
        print("  [WARN] no WORKSHOP_Export -- build-menu tabs stay as names", file=sys.stderr)
        return
    SOURCES["buildMenu"] = os.path.basename(p)
    kp = tsv_source.newest("KYWD_Export_*.tsv", channel=CHANNEL, exclude=["_Refs"], required=False)
    if kp:
        for r in read_tsv(kp):
            e = (r.get("EDID") or "").strip()
            if e.startswith("Workshop2_"):
                KW_FULL[e] = (r.get("FULL_Name") or "").strip()
    for cat in json.load(open(p, encoding="utf-8")):
        m = cat["CategoryKeyword"]["FormEditorID"]
        for sub in cat.get("SubCategories") or []:
            se = sub["CategoryKeyword"]["FormEditorID"]
            fids = ["%08X" % int(r["FormID"]) for r in sub.get("Recipes") or []]
            MENU.setdefault(se, []).extend(fids)
            MENU.setdefault(m, []).extend(fids)
            MENU_PATH[se] = (m, se)
            MENU_PATH.setdefault(m, (m, ""))

    # Names for recipes that place a leveled list (traps) or a leveled turret.
    need = {COBJ_CNAM[f][0] for fl in MENU.values() for f in fl
            if f in COBJ_CNAM and not COBJ_CNAM[f][2]}
    LL_KIDS.clear()
    p = tsv_source.newest("LVLI_Export_*_LVLI_Entries.tsv", channel=CHANNEL, required=False)
    if p:
        for r in read_tsv(p):
            lf = (r.get("LVLI_FormID") or "").strip().upper()
            if lf in need:
                ref = (r.get("LVLO_Reference") or "").split(":")
                if len(ref) >= 3:
                    LL_KIDS[lf].append((ref[0].upper(), ref[1], ref[2]))
    p = tsv_source.newest("ACTI_Export_*_ACTI.tsv", channel=CHANNEL, required=False)
    if p:
        for r in read_tsv(p):
            OBJ_NAME[(r.get("ACTI_FormID") or "").strip().upper()] = (
                (r.get("ACTI_EDID") or "").strip(), (r.get("ACTI_FULL") or "").strip())
    for fid, (e, n) in NPC_NAME.items():
        if n:
            NPC_BY_EDID.setdefault(e.lower(), n)


LL_KIDS = defaultdict(list)       # LVLI fid -> [(fid, edid, sig)]
MGEF_ITEMS = defaultdict(list)    # MGEF fid -> [(ALCH fid, edid, name)] that apply it
SCRAPS_INTO = defaultdict(set)    # component EDID -> {(MISC edid, name)} whose scrap data (MCQP) gives it
KW_FULL = {}                      # Workshop2 keyword EDID -> in-game tab name
OBJ_NAME = {}                     # ACTI fid -> (edid, name)
NPC_BY_EDID = {}                  # npc edid (lower) -> name


def recipe_names(recipe_fid):
    """In-game name(s) of what one build-menu recipe places."""
    cf, ce, cn = COBJ_CNAM.get(recipe_fid, ("", "", ""))
    if cn:
        n = clean_name(cn, ce, cf, "COBJ")
        return {n} if n else set()
    out = set()
    for kf, ke, ks in LL_KIDS.get(cf, []):          # workshop_LL_* -> each object
        e, n = OBJ_NAME.get(kf, (ke, ""))
        n = clean_name(n, e, kf, ks)
        if n:
            out.add(n)
    m = re.search(r"LChar_(Turret)(.*)$", ce or "")  # leveled turret -> the Workshop turret NPC
    if m:
        tail = m.group(2).replace("_", "")
        for cand in ("WorkshopTurret" + tail, "WorkshopTurretTripod" + tail):
            if NPC_BY_EDID.get(cand.lower()):
                out.add(NPC_BY_EDID[cand.lower()])
                break
    return out


def menu_names(kw_edid):
    """Every name in a build-menu tab, or None when any recipe in it can't be
    named from the exports (then the page names the tab instead of a part-list)."""
    names = set()
    for f in MENU.get(kw_edid, []):
        n = recipe_names(f)
        if not n:
            return None
        names |= n
    return names


def load_counter_lines():
    for r in read_edit_tsv(COUNTER_LINES_TSV):
        k, g, t = ((r.get(x) or "").strip() for x in ("counter", "group", "text"))
        if k and t and g in ("target", "also"):
            COUNTER_LINES[k.lower()].append((g, t))


COUNTER_LINES_TSV = os.path.join(ROOT, "tsv", "challenge_counter_lines.tsv")
COUNTER_LINES = defaultdict(list) # stat counter (lower) -> [(group, text)]


def load_counter_lines():
    for r in read_edit_tsv(COUNTER_LINES_TSV):
        k, g, t = ((r.get(x) or "").strip() for x in ("counter", "group", "text"))
        if k and t and g in ("target", "also"):
            COUNTER_LINES[k.lower()].append((g, t))


def live_actor(edid, fid):
    """Lighter than clean_name: unique NPCs (_NPC_) and templates are real here;
    only cut, QA and test records are dropped."""
    return not cut_reason(edid, fid, "NPC_") and not re.search(r"(^QA_|_Test(?:_|$)|^Test|AnimTest)", edid or "", re.I)


CAPS_VENDORS = set()              # NPC vendors whose vendor faction trades in Caps


def load_vendors():
    """NPC2_Vendors export: vendor factions (with currency) and who holds them."""
    fp = tsv_source.newest("NPC2_Vendors_*_Factions.tsv", channel=CHANNEL, required=False)
    pp = tsv_source.newest("NPC2_Vendors_*_FactionsPlacements.tsv", channel=CHANNEL, required=False)
    if not fp or not pp:
        return
    SOURCES["vendors"] = os.path.basename(pp)
    # Caps vendor = currency Caps001, or no currency set but the shared caps
    # wallet (Grahm, Beckham: Vendor_CapsBalance_Shared_Faction).
    cur = {}
    for r in read_tsv(fp):
        c = (r.get("Currency_EDID") or "").strip()
        if not c and "CapsBalance" in (r.get("Wallet_EDID") or ""):
            c = "Caps001"
        cur[(r.get("Faction_FormID") or "").strip().upper()] = c
    for r in read_tsv(pp):
        if (r.get("Holder_Sig") or "").strip() != "NPC_":
            continue
        if cur.get((r.get("Faction_FormID") or "").strip().upper()) != "Caps001":
            continue
        e = (r.get("Holder_EDID") or "").strip()
        n = (r.get("Holder_FULL") or "").strip()
        if n and live_actor(e, (r.get("Holder_FormID") or "").strip()):
            CAPS_VENDORS.add(n)


HUMAN_GROUPS = {}                 # race fid -> {group name}: actor-type keywords its NPCs carry
_GROUP_SKIP = re.compile(r"^ActorType(Human|NPC|BossType|DiseaseCarrier)$")


def load_race_groups():
    """Group names for a race-only condition (GetIsRace HumanRace): the
    ActorType* keywords carried by that race's named NPCs, e.g. Blood Eagle."""
    by = defaultdict(lambda: defaultdict(set))
    for kw, refs in KW_REFS.items():
        if not kw.startswith("ActorType") or _GROUP_SKIP.match(kw):
            continue
        for sig, fid, edid, name in refs:
            if sig == "NPC_" and name and live_actor(edid, fid):
                by[NPC_RACE.get(fid, "")][kw].add(name)
    for race, kws in by.items():
        out = set()
        for kw, names in kws.items():
            if len(names) < 1:
                continue
            g = re.sub(r"(Generic|Unique|Friendly|Combatant|_Friendly)$", "", kw[len("ActorType"):])
            g = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", g).replace(" of ", " of ").replace("_", " ").strip()
            g = re.sub(r"\bof\b", "of", re.sub(r"(?<=[a-z])of(?=[A-Z ])", " of", g))
            if g:
                out.add(g)
        HUMAN_GROUPS[race] = out


def is_npc_vendor_challenge(full):
    """'... to a Non Player Vendor' / '... NPC vendor' -- not a player's vending machine."""
    f = full.lower().replace("non player", "npc").replace("non-player", "npc")
    return "vendor" in f and "player" not in f and "vending" not in f


def load_hidden():
    for r in read_edit_tsv(HIDE_TSV):
        e = (r.get("edid") or "").strip()
        if e:
            HIDE.add(e)


def ingredient_names(alch_fid):
    """'Mirelurk Meat (2)' style lines for every cooking recipe that makes this dish."""
    out = set()
    for parts in RECIPES.get(alch_fid, []):
        for edid, qty in parts:
            n = ITEM_NAME.get(edid) or ITEM_NAME.get(re.sub(r"^c_", "", edid)) or ""
            if not n and edid.lower().startswith("c_"):
                n = edid[2:].replace("_", " ").title()
            n = clean_name(n, edid)
            if n:
                out.add(f"{n} ({qty})" if qty and qty != "1" else n)
    return out


def load_exports():
    p = tsv_source.newest("KYWD_Export_*_Refs.tsv", channel=CHANNEL)
    SOURCES["keywords"] = os.path.basename(p)
    for r in read_tsv(p):
        kw = (r.get("KeywordEDID") or "").strip()
        if kw:
            KW_REFS[kw].append(((r.get("RefSignature") or "").strip(),
                                (r.get("RefFormID") or "").strip().upper(),
                                (r.get("RefEDID") or "").strip(),
                                (r.get("RefName") or "").strip()))

    p = tsv_source.newest("FLST_Export_*_Entries.tsv", channel=CHANNEL)
    SOURCES["formLists"] = os.path.basename(p)
    for r in read_tsv(p):
        fid = (r.get("FLST_FormID") or "").strip().upper()
        FLST_BY_FID.setdefault(fid, ((r.get("FLST_EDID") or "").strip(), (r.get("FLST_FULL") or "").strip()))
        FLST[fid].append(((r.get("Entry_Sig") or "").strip(),
                          (r.get("Entry_FormID") or "").strip().upper(),
                          (r.get("Entry_EDID") or "").strip(),
                          (r.get("Entry_FULL") or "").strip()))

    p = tsv_source.newest("CNDF_Export_*.tsv", channel=CHANNEL)
    SOURCES["conditionForms"] = os.path.basename(p)
    for r in read_tsv(p):
        fid = (r.get("FormID") or "").strip().upper()
        n = int((r.get("CondCount") or "0").strip() or 0) if (r.get("CondCount") or "").strip().isdigit() else 76
        conds = [(r.get(f"Cond{i}") or "").strip() for i in range(1, min(n, 76) + 1)]
        CNDF[fid] = [c for c in conds if c]

    p = tsv_source.newest("COBJ_Export_*.tsv", channel=CHANNEL, required=False)
    if p:
        SOURCES["recipes"] = os.path.basename(p)
        for r in read_tsv(p):
            COBJ[(r.get("COBJ_FormID") or "").strip().upper()] = (
                (r.get("CNAM_EDID") or "").strip(), (r.get("CNAM_FULL") or "").strip())
            COBJ_CNAM[(r.get("COBJ_FormID") or "").strip().upper()] = (
                (r.get("CNAM_FormID") or "").strip().upper(),
                (r.get("CNAM_EDID") or "").strip(), (r.get("CNAM_FULL") or "").strip())
            # Cooking-bench recipes -> the ingredients a "Cook <dish>" challenge uses.
            if "Cooking" in (r.get("BNAM_EDID") or "") and not cut_content.is_cut(r.get("COBJ_EDID") or ""):
                parts = []
                for bit in (r.get("FVPA") or "").split("|"):
                    b = bit.split(":")
                    if len(b) >= 2 and b[0].strip():
                        parts.append((b[0].strip(), b[1].strip()))
                if parts:
                    RECIPES[(r.get("CNAM_FormID") or "").strip().upper()].append(parts)
        ITEM_NAME.update(load_item_names())
    p = tsv_source.newest("ALCH_Export_*_Effects.tsv", channel=CHANNEL, required=False)
    if p:
        SOURCES["itemEffects"] = os.path.basename(p)
        alch_full = {}
        ap = tsv_source.newest("ALCH_Export_*.tsv", channel=CHANNEL, required=False,
                               exclude=("_Effects", "_Refs", "_Entries"))
        if ap:
            for r in read_tsv(ap):
                alch_full[(r.get("ALCH_FormID") or "").strip().upper()] = (r.get("FULL") or "").strip()
        for r in read_tsv(p):
            mf = (r.get("MGEF_FormID") or "").strip().upper()
            af = (r.get("ALCH_FormID") or "").strip().upper()
            if mf and af and alch_full.get(af):
                MGEF_ITEMS[mf].append((af, (r.get("ALCH_EDID") or "").strip(), alch_full[af]))

    p = tsv_source.newest("NPC_Export_*.tsv", channel=CHANNEL, required=False)
    if p:
        SOURCES["npcs"] = os.path.basename(p)
        for r in read_tsv(p):
            rf = (r.get("RNAM_FormID") or "").strip().upper()
            nf = (r.get("FormID") or "").strip().upper()
            if rf:
                NPC_RACE[nf] = rf
            NPC_NAME[nf] = ((r.get("EDID") or "").strip(),
                            (r.get("FULL") or "").strip() or (r.get("TPLT_Name") or "").strip())


# ─────────────────────────────────────────────────────────────────────────────
#  Name hygiene — only real, player-facing names reach the page
# ─────────────────────────────────────────────────────────────────────────────
_NOT_PLAYER = re.compile(
    r"(NON_?PLAYABLE|UN_?PLAYABLE|_TEST(?:_|$)|(?:^|_)NPC(?:_|$)|^Creature_|AudioTemplate|"
    r"CAMPPets|^PETS_|Collectron|UNUSED|ATX_COMP|_COMP_|Dummy|Template|Corpse|_Loot_|^DEL_|^zz|(?-i:^cr[A-Z_]))", re.I)
# An EDID that leaked into a name slot: underscores, or a lower-case-first camel word.
_EDID_LIKE = re.compile(r"^[A-Za-z0-9]+(?:_[A-Za-z0-9]+)+$|^[a-z]+[A-Z][A-Za-z0-9]*$")

NAME_OVERRIDE = {}


def load_name_overrides():
    for r in read_edit_tsv(NAMES_TSV):
        raw = (r.get("game_name") or "").strip()
        if raw:
            NAME_OVERRIDE[raw.lower()] = (r.get("display_name") or "").strip()


# ─────────────────────────────────────────────────────────────────────────────
#  Cut content — every site-wide cut source, so nothing cut reaches the page
#    1. plan_sources.is_cut_content()  THE site-wide test (confirmed cut families
#                                      such as Milepost Zero / The Drifter /
#                                      Vault 94, plus zzz / CUT / DEL / test),
#                                      called with live_records=True so the live
#                                      Strangler Heart / Thorn / Solar armour
#                                      (_Set_V94_ records) stays in
#                                      (plan_sources.CONFIRMED_LIVE_RECORDS).
#    2. cut_content.is_cut()           the item-level list the menu / recipe /
#                                      camp builders share (Nukashine 2, SFS01
#                                      brews, Firecracker Whiskey, spoiled food,
#                                      PTS_ potions...). Items only (ITEM_SIGS).
#                                      Its Firecracker / Mothman patterns were
#                                      tightened at the source on 17 Nov 2026, so
#                                      no page-side exceptions are needed.
#    3. spin.is_cut()                  the prefix rule the keyword indexes use
#    4. dist/cut_content.json          the Cut Content page, matched by FormID —
#                                      ONLY the Challenges / Quests / Collectables
#                                      groups, whose id is the record itself. The
#                                      Unique Weapons / Armour rows carry the BASE
#                                      weapon's FormID (Crimson Sky = 0010F0EC =
#                                      the Tesla Rifle), so matching those would
#                                      cut live base weapons.
#  A record any one of them calls cut is left off.
# ─────────────────────────────────────────────────────────────────────────────
CUT_FIDS = {}                     # FormID -> why (from the Cut Content page)
CUT_REMOVED = defaultdict(set)    # source -> {names left off because cut}
CUT_SOURCES = {}
CUT_PAGE_GROUPS = {"challenges", "quests", "collectables"}


def load_cut_page():
    path = os.path.join(ROOT, "dist", *(["pts"] if PTS else []), "cut_content.json")
    if PTS and not os.path.exists(path):
        path = os.path.join(ROOT, "dist", "cut_content.json")
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        print("  [WARN] cut_content.json not found -- FormID cross-check skipped", file=sys.stderr)
        return
    CUT_SOURCES["cutContentPage"] = os.path.relpath(path, ROOT).replace("\\", "/")
    for g in d.get("groups") or []:
        if (g.get("key") or "") not in CUT_PAGE_GROUPS:
            continue
        for it in g.get("items") or []:
            fid = str(it.get("id") or "").strip().upper()
            if re.fullmatch(r"[0-9A-F]{8}", fid):
                CUT_FIDS[fid] = it.get("why") or "cut"


def cut_reason(edid, fid="", sig=""):
    """Which cut source rejects this record, or '' if it is live."""
    e = (edid or "").strip()
    f = (fid or "").strip().upper()
    if e and plan_sources.is_cut_content(e, live_records=True):
        return "plan_sources.is_cut_content"
    if e and (sig in ITEM_SIGS or sig in ("CHAL", "QUST")) and cut_content.is_cut(e):
        return "cut_content.is_cut"
    if e and spin.is_cut(e):
        return "prefix rule"
    if f and f in CUT_FIDS:
        return "Cut Content page"
    return ""


def clean_name(name, edid="", fid="", sig=""):
    """Plain name or '' (drop). Cut / test / NPC-copy / pet records never show."""
    n = re.sub(r"\s+", " ", str(name or "")).strip()
    if not n or n.upper() == "NONE":
        return ""
    why = cut_reason(edid, fid, sig)
    if why:
        CUT_REMOVED[why].add(n)
        return ""
    if edid and _NOT_PLAYER.search(edid):
        return ""
    # "Uplink: <Alias=Location>" (Daily Ops mode quests) -> "Uplink". Only a
    # trailing ": <Alias=...>" is stripped; an alias mid-name still drops it.
    n = re.sub(r"\s*:\s*<Alias=[^>]*>\s*$", "", n)
    if n.lower() in NAME_OVERRIDE:
        return NAME_OVERRIDE[n.lower()]
    if _EDID_LIKE.match(n) or n.lower().startswith("audiotemplate") or re.search(r"[<>\[\]{}]", n):
        return ""
    return n


# ─────────────────────────────────────────────────────────────────────────────
#  Condition decode
#  raw: flags|value|Function|..|..|EDID "Name" [REC:FID]|..|..|..|RunOn|
# ─────────────────────────────────────────────────────────────────────────────
FUNC_INDEX = {"939": "IsInList", "940": "HasKeyword", "941": "HasKeyword"}
_REF = re.compile(r'^\s*([A-Za-z0-9_]*)\s*(?:"((?:[^"]|"")*)")?\s*\[(\w+):([0-9A-Fa-f]{8})\]')

ITEM_SIGS = {"ALCH", "MISC", "BOOK", "NOTE", "ARMO", "WEAP", "AMMO", "KEYM", "INGR", "FLOR", "CMPO"}
GEAR_SIGS = {"WEAP", "ARMO"}

# group keys, in page order, and their plain labels
GROUPS = OrderedDict([
    ("target", "What counts"),
    ("sub", "Any of these challenges count"),
    ("pet", "Pets that count"),
    ("ingredient", "Ingredients"),
    ("scrap", "Junk that scraps into it"),
    ("effect_items", "Items that give the effect"),
    ("group", "Groups of that race"),
    ("vendor", "NPC vendors (caps)"),
    ("weapon", "Weapons that count"),
    ("mod", "Weapon mods that count"),
    ("worn", "What to wear"),
    ("during", "When it counts"),
    ("where", "Where it counts"),
    ("also", "Also needs"),
])

UNRESOLVED = defaultdict(set)     # (function, param) -> {challenge labels}


def parse(cond):
    p = str(cond or "").split("|")
    if len(p) < 6:
        return None
    func = FUNC_INDEX.get(p[2].strip(), p[2].strip())
    param = p[5].strip()
    ref = _REF.match(param)
    if not ref and func == "IsInList":
        b = param.split()
        if len(b) == 4 and all(re.fullmatch(r"[0-9A-Fa-f]{2}", x) for x in b):
            fid = "".join(reversed(b)).upper()
            e, n = FLST_BY_FID.get(fid, ("", ""))
            ref = (e, n, "FLST", fid)
        else:
            ref = None
    elif ref:
        ref = (ref.group(1), (ref.group(2) or "").replace('""', '"'), ref.group(3), ref.group(4).upper())
    try:
        value = float(p[1])
    except ValueError:
        value = None
    return {"flags": p[0].strip(), "value": value, "func": func, "param": param,
            "ref": ref, "runon": (p[9].strip() if len(p) > 9 else "") or "Subject"}


# CTDA comparison: the first three flag characters (as this export writes them).
_OPS = {"100": "==", "000": "!=", "010": ">", "110": ">=", "001": "<", "101": "<="}

# ─────────────────────────────────────────────────────────────────────────────
#  Situational conditions -> plain "Also needs" lines (tsv/challenge_situations.tsv)
# ─────────────────────────────────────────────────────────────────────────────
SITUATIONS = []                   # (func, match rx|None, op, value|None, text, guide key)
SITUATION_GUIDE = {}              # text -> guide key


def load_situations():
    for r in read_edit_tsv(SITUATIONS_TSV):
        fn = (r.get("function") or "").strip()
        text = (r.get("text") or "").strip()
        if not fn or not text:
            continue
        m = (r.get("match") or "").strip()
        v = (r.get("value") or "").strip()
        try:
            v = float(v) if v else None
        except ValueError:
            v = None
        SITUATIONS.append((fn, re.compile(m, re.I) if m else None,
                           (r.get("op") or "any").strip() or "any", v, text, (r.get("guide") or "").strip()))


def situation_text(c):
    op = _OPS.get(c["flags"][:3], "?")
    for fn, rx, want_op, want_v, text, guide in SITUATIONS:
        if fn != c["func"]:
            continue
        if rx and not rx.search(c["param"]):
            continue
        if want_op != "any" and want_op != op:
            continue
        if want_v is not None and c["value"] != want_v:
            continue
        if guide:
            SITUATION_GUIDE[text] = guide
        return text
    return ""


def is_plain_true(c):
    """== 1 (the CTDA op bits are the first three flag characters)."""
    return c["flags"][:3] == "100" and c["value"] == 1.0


def is_plain_false(c):
    return c["flags"][:3] == "100" and c["value"] == 0.0


def leaf_conditions(conds, seen=None):
    """Expand IsTrueForConditionForm down to its leaves."""
    seen = seen or set()
    out = []
    for raw in conds:
        c = parse(raw)
        if not c:
            continue
        if (c["func"] == "IsTrueForConditionForm" and c["ref"] and is_plain_true(c)
                and re.search(r"^WorldPets_.*CheckPetActive.*IsPet", c["ref"][0], re.I)):
            # "any World Pet is out" -- the form holds no pet list, so name the species.
            out.append(dict(c, func="_WorldPetActive"))
            continue
        if c["func"] == "IsTrueForConditionForm" and c["ref"] and c["ref"][3] in CNDF and is_plain_true(c):
            fid = c["ref"][3]
            if fid in seen:
                continue
            seen.add(fid)
            out.extend(leaf_conditions(CNDF[fid], seen))
        else:
            out.append(c)
    return out


# Cooked food inherits its ingredients' keywords. For a "Plants Harvested"
# challenge only the raw plant can be picked, so cooked records are left off.
_COOKED = re.compile(r"Cooked|Tasty|Gourmet|Soup|Stew|Juice|Tea$|Cobbler|Jam|Relish|Cannery|Pie|Bread|"
                     r"Caramel|Lemonade|Salad|Specimen|Scrap|_scrap|Perk$|Keywords$", re.I)
_COOKED_NAME = re.compile(r"\b(Tea|Soup|Stew|Salad|Pickled|Preserved|Ham|Beans|Noodle|Oil|Feed|Juice|Pie|"
                          r"InstaMash|Cup)\b", re.I)
HARVEST = {"on": False}
EXTRA = defaultdict(set)          # extra groups gathered while resolving (mods ...)
SCRAPPING = {"on": False}         # "Components Added": list the junk that scraps into the component
COOKING = {"on": False}           # a "Cook <dish>" challenge: list the dish's ingredients
ING = {}                          # dish name -> {ingredient lines}


def keyword_names(kw_edid, sigs):
    names = set()
    for sig, fid, edid, name in KW_REFS.get(kw_edid, []):
        if HARVEST["on"] and (_COOKED.search(edid) or _COOKED_NAME.search(name)):
            continue
        if sig in sigs:
            n = clean_name(name, edid, fid, sig)
            if n:
                names.add(n)
    return names


NPC_SPAWNED = set()               # NPC fids something spawns: placed (ACHR), LVLN, WAVE, QUST, FLST
RACE_NPCS = defaultdict(set)      # race fid -> {npc fid}


def load_npc_spawns():
    import glob
    p = tsv_source.newest("NPC_Export*Refs*.tsv", channel=CHANNEL, required=False)
    if not p:
        return
    SOURCES["npcRefs"] = os.path.basename(p)
    for r in read_tsv(p):
        # Enemies spawn from encounter leveled lists (LVLN) and event waves (WAVE).
        # Placed / quest NPCs can be friendly, so they don't count as proof.
        if (r.get("RefBy_Signature") or "").strip() in ("LVLN", "WAVE") \
                and not re.match(r"(ATX_|CAMP|Workshop)", (r.get("RefBy_EDID") or ""), re.I):
            NPC_SPAWNED.add((r.get("NPC_FormID") or "").strip().upper())
    for f, rf in NPC_RACE.items():
        RACE_NPCS[rf].add(f)


def spawned_race_npcs(race_fids):
    """Named NPCs of these races that the game actually spawns, minus cut / test / corpses."""
    out = set()
    for rf in race_fids:
        for f in RACE_NPCS.get(rf, ()):
            if f not in NPC_SPAWNED:
                continue
            e, n = NPC_NAME.get(f, ("", ""))
            if not n or re.search(r"Corpse|^Loot_|_Loot_|AudioTemplate|^ATX_|CAMP|Workshop|Collectron|Vendor|"
                                  r"Companion|Ally|Tamed|_Pet_|PetCat|Friendly", e + " " + n, re.I):
                continue
            if live_actor(e, f) and not _EDID_LIKE.match(n):
                out.add(n)
    return out


def actor_names(kw_edid, races=None, allow_template=False):
    """Actors carrying an actor keyword: the races that carry it (by race name)
    plus NPCs that carry it directly. Races whose only NPCs are pets / copies
    are dropped by clean_name() on the race EDID."""
    names = set()
    for sig, fid, edid, name in KW_REFS.get(kw_edid, []):
        if sig == "RACE":
            if races is not None and fid not in races:
                continue
            n = clean_name(name, edid, fid, sig)
            if n:
                names.add(n)
                names |= spawned_race_npcs({fid})   # named NPCs of that race count too
        elif sig == "NPC_" and fid != "00000007":
            if races is not None and NPC_RACE.get(fid) not in races:
                continue
            n = clean_name(name, re.sub(r"Template", "", edid) if allow_template else edid, fid, sig)
            if n:
                names.add(n)
    return names


def is_actor_keyword(kw_edid):
    return any(sig in ("RACE", "NPC_") and fid != "00000007" for sig, fid, _, _ in KW_REFS.get(kw_edid, []))


def flst_names(fid, seen=None):
    seen = seen or set()
    if fid in seen:
        return set()
    seen.add(fid)
    names = set()
    for sig, efid, edid, name in FLST.get(fid, []):
        if sig == "FLST":
            names |= flst_names(efid, seen)
            continue
        n = clean_name(name, edid, efid, sig)
        if n:
            names.add(n)
    return names


def ref_name(ref):
    edid, name, rec, fid = ref
    if not name and rec == "NPC_" and fid in NPC_NAME:
        name = NPC_NAME[fid][1]
        edid = re.sub(r"_DailyOps$", "", edid)
    return clean_name(name, edid, fid, rec)


def resolve(c, label, used_keywords):
    """-> (group, set(names)) or None. Unresolvable conditions are recorded."""
    func, ref, runon = c["func"], c["ref"], c["runon"]
    target = runon in ("Target", "Target List")
    miss = lambda: UNRESOLVED[(func, re.sub(r"\s+", " ", c["param"])[:120])].add(label)

    if func in ("IsFalloutWorlds",):
        return None
    if func == "_WorldPetActive":
        return ("pet", set(WORLD_PETS)) if WORLD_PETS else (miss() or None)
    sit = situation_text(c)
    if func == "HasActiveMagicEffect" and ref and ref[2] == "MGEF" and is_plain_true(c):
        items = {clean_name(n, e, f, "ALCH") for f, e, n in MGEF_ITEMS.get(ref[3], [])} - {""}
        if items:   # the items in the game that give this effect
            EXTRA["effect_items"] |= items
    if sit:
        return ("also", {sit})
    if not is_plain_true(c):
        if not is_plain_false(c):          # "not X" conditions narrow, they don't add
            miss()
        return None
    if not ref:
        miss()
        return None
    edid, name, rec, fid = ref
    if cut_reason(edid, fid, rec):
        CUT_REMOVED[cut_reason(edid, fid, rec)].add(name or edid)
        return None

    if func in ("GetIsID", "GetIsForm"):
        n = ref_name(ref)
        if not n:
            miss(); return None
        if rec == "WEAP" and not target:
            return ("weapon", {n})
        if rec == "CMPO" and SCRAPPING["on"] and SCRAPS_INTO.get(edid):
            EXTRA["scrap"] |= {clean_name(jn, je, "", "MISC") for je, jn in SCRAPS_INTO[edid]
                               if not jn.lower().startswith("scrap this")} - {""}
        if rec == "ALCH" and COOKING["on"] and RECIPES.get(fid):
            ING[n] = ingredient_names(fid)
        return ("target", {n})

    if func == "GetIsRace":
        n = ref_name(ref)
        return ("race", {(fid, n)}) if n else (miss() or None)

    if func in ("GetIsClass", "GetInFaction"):
        n = re.sub(r"\s+Faction$", "", ref_name(ref))
        return ("narrow", {n}) if n else (miss() or None)

    if func == "HasActiveMagicEffect" and EXTRA.get("effect_items"):
        return ("also", {"While under the effect of " + (name or edid)})

    if func == "HasKeyword" and target:
        used_keywords.add(edid)
        if is_actor_keyword(edid):
            return ("actor_kw", {edid}) if actor_names(edid) else (miss() or None)
        # Harvest-type keywords sit on the plant (FLOR) and on everything cooked
        # from it; the plant is what counts, so FLOR names win when there are any.
        names = (keyword_names(edid, {"FLOR"}) or keyword_names(edid, ITEM_SIGS)
                 or keyword_names(edid, {"MSTT", "ACTI"}))   # world objects: car wrecks (ExplodableCar)
        return ("target", names) if names else (miss() or None)

    if func in ("WornHasKeyword", "WornInOrOutOfPowerArmorHasKeyword", "CHAL_DoesTargetWeaponHaveKeyword"):
        used_keywords.add(edid)
        weap = keyword_names(edid, {"WEAP"})
        mods = {n for n in keyword_names(edid, {"OMOD"}) if not re.search(r"TEMPLATE|PARENT|Custom Name", n, re.I)}
        if not re.search(r"Damage$", edid):   # ma_* / WeaponType* refs on mods = mods that FIT, not mods that ADD
            mods = set()
        if mods:   # a weapon mod that adds the damage keyword makes any weapon it fits count
            EXTRA["mod"] |= mods
        if weap:
            return ("weapon", weap)
        armo = keyword_names(edid, {"ARMO"})
        if armo:
            return ("worn", armo)
        miss(); return None

    if func in ("CHAL_IsTargetWorkshopRecipeInCategory", "IsTargetWorkshopRecipeInCategory"):
        n = clean_name(name)                # the build-menu category ("Wall Decor")
        if edid in MENU:                    # Workshop2 tab -> everything built from it
            names = menu_names(edid)
            if names and len(names) <= MENU_MAX:
                return ("target", names)
            main, sub = MENU_PATH.get(edid, ("", ""))
            tab = " > ".join(clean_name(KW_FULL.get(x, "")) or x.split("_")[-1] for x in (main, sub) if x)
            return ("target", {f"Anything from the {tab} tab of the build menu"})
        return ("target", {n}) if n else (miss() or None)

    if func == "CHAL_IsTargetWorkshopRecipe":
        cedid, cname = COBJ.get(fid, ("", ""))
        n = clean_name(cname, cedid, fid, "COBJ")
        return ("target", {n}) if n else (miss() or None)

    if func == "IsInList":
        names = flst_names(fid)
        if not names:
            miss(); return None
        return ("target", names) if target else ("during", names)

    if func == "PlayerHasQuest":
        n = ref_name(ref)
        return ("during", {n}) if n else (miss() or None)

    if func in ("GetInCurrentLocation", "GetInWorldspace", "IsInLocation"):
        n = ref_name(ref)
        return ("where", {n}) if n else (miss() or None)

    miss()
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  Counter map — challenges the game counts by stat, not by condition
# ─────────────────────────────────────────────────────────────────────────────
COUNTER = {}


def load_counter_map():
    for r in read_edit_tsv(COUNTER_TSV):
        counter = (r.get("counter") or "").strip()
        kws = [k.strip() for k in (r.get("keywords") or "").split(",") if k.strip()]
        if counter and kws:
            COUNTER[counter.lower()] = kws


# ─────────────────────────────────────────────────────────────────────────────
#  Guides
# ─────────────────────────────────────────────────────────────────────────────
GUIDES_BY_URL, GUIDES_BY_ID, HIDDEN_URLS = {}, {}, set()


def norm_url(u):
    u = str(u or "").strip()
    return ("/" + u.strip("/") + "/") if u else ""


def load_guides():
    for r in read_tsv(GUIDE_INDEX):
        url = norm_url(r.get("url"))
        if not url:
            continue
        public = (r.get("visibility") or "").strip().lower() == "public" and \
                 (r.get("status") or "").strip().lower() == "published"
        g = {"id": (r.get("id") or "").strip(), "title": (r.get("title") or "").strip(), "url": url,
             "brand": (r.get("brand") or "").strip(), "cat": (r.get("topCategory") or "").strip(),
             "sub": (r.get("subCategory") or "").strip()}
        if public:
            GUIDES_BY_URL[url] = g
            if g["id"]:
                GUIDES_BY_ID[g["id"]] = g
        else:
            HIDDEN_URLS.add(url)
            if g["id"]:
                HIDDEN_URLS.add(g["id"])


# Keyword-driven guide links: a challenge that tests for one of these keywords
# gets the Score Challenges index page that lists every item carrying it.
KEYWORD_GUIDES = [
    (re.compile(r"^ClothingTypeCostume$"), "df-score-costume-keyword-index"),
    (re.compile(r"^MealType(Packaged|Dogfood)$"), "df-score-pre-war-food-keyword-index"),
    (re.compile(r"^WeaponType"), "df-score-weapon-keyword-index"),
]
GUIDE_MAP = []


def load_guide_map():
    for r in read_edit_tsv(GUIDE_MAP_TSV):
        m = (r.get("match") or "").strip()
        g = (r.get("guide") or "").strip()
        act = (r.get("action") or "add").strip().lower()
        if m and g:
            GUIDE_MAP.append((re.compile(r"(?<![A-Za-z])" + re.escape(m) + r"(?![A-Za-z])", re.I), g, act))


def lookup_guide(key):
    key = key.strip()
    if key.startswith("/"):
        return GUIDES_BY_URL.get(norm_url(key))
    return GUIDES_BY_ID.get(key)


def guides_for(name, existing, used_keywords, stats):
    out = OrderedDict()
    removed = set()
    for rx, g, act in GUIDE_MAP:
        if act == "remove" and rx.search(name):
            gg = lookup_guide(g)
            removed.add(gg["url"] if gg else norm_url(g))
    for rx, g, act in GUIDE_MAP:
        if act != "remove" and rx.search(name):
            gg = lookup_guide(g)
            if gg:
                out[gg["url"]] = gg
            elif g in HIDDEN_URLS or norm_url(g) in HIDDEN_URLS:
                stats["hidden"] += 1
    for kw in used_keywords:
        for rx, gid in KEYWORD_GUIDES:
            if rx.search(kw):
                gg = GUIDES_BY_ID.get(gid)
                if gg:
                    out[gg["url"]] = gg
    for e in existing or []:
        u = norm_url(e.get("url"))
        if u in GUIDES_BY_URL:
            out.setdefault(u, GUIDES_BY_URL[u])
        elif u:
            stats["hidden"] += 1
    # brand / cat / sub let the page apply the WordPress visibility maps too
    return [{"title": g["title"], "url": g["url"], "brand": g["brand"], "cat": g["cat"], "sub": g["sub"]}
            for u, g in out.items() if u not in removed]


# ─────────────────────────────────────────────────────────────────────────────
#  Build
# ─────────────────────────────────────────────────────────────────────────────
_EPIC = re.compile(r"^\s*epic\s*-\s*", re.I)


def walk(items):
    for c in items or []:
        if isinstance(c, dict):
            yield c
            yield from walk(c.get("children"))


def challenge_rows(items, ctype, stats):
    rows = OrderedDict()   # label -> row
    for it in walk(items):
        edid = it.get("edid") or ""
        full = (it.get("full") or it.get("name") or "").strip()
        if not full or full.upper() == "NONE":
            continue
        if edid in HIDE:
            stats["hiddenChallenges"].append(f"{ctype}: {full} [{edid}]")
            continue
        why = "challenges.json is_cut" if it.get("is_cut") else cut_reason(edid, it.get("form_id"), "CHAL")
        if why:
            stats["cutChallenges"].append((why, full, edid))
            continue
        epic = bool(it.get("is_epic")) or bool(_EPIC.match(full))
        base = _EPIC.sub("", full).strip()
        name = ("Epic - " + base) if epic else base
        req = it.get("required")
        req = str(req) if req not in (None, "") else ""
        label = f"{name} ({req})" if req else name

        used_kw = set()
        groups = defaultdict(set)
        HARVEST["on"] = "harvest" in str(it.get("snam") or "").lower()
        SCRAPPING["on"] = str(it.get("snam") or "").strip().lower() == "components added"
        COOKING["on"] = bool(re.match(r"(Epic - )?Cook\b", full, re.I))
        ING.clear()
        EXTRA.clear()
        for c in leaf_conditions(it.get("conditions") or []):
            r = resolve(c, f"{ctype}: {label}", used_kw)
            if r:
                groups[r[0]] |= r[1]
        for k, v in EXTRA.items():
            groups[k] |= v
        if it.get("is_meta"):    # a META challenge counts its sub-challenges
            for ch in it.get("children") or []:
                cn = (ch.get("full") or "").strip()
                if cn and not ch.get("is_cut") and not cut_reason(ch.get("edid"), ch.get("form_id"), "CHAL"):
                    groups["sub"].add(cn)
        if len(ING) == 1:
            groups["ingredient"] |= next(iter(ING.values()))
        elif ING:   # more than one dish counts: one line per dish
            for dish, parts in ING.items():
                groups["ingredient"].add(f"{dish}: " + ", ".join(sorted(parts, key=str.lower)))
        # Actor conditions are AND-ed: "Blood Eagle keyword AND (Human or Ghoul
        # race)" means Blood Eagles, not every human. Keyword + race -> the
        # keyword's actors of those races; a faction / class names the target on
        # its own; a race list alone lists the races.
        races = groups.pop("race", set())
        kws = groups.pop("actor_kw", set())
        narrow = groups.pop("narrow", set())
        race_fids = {f for f, _ in races} or None
        race_names = {n for _, n in races}
        for kw in kws:
            hit = actor_names(kw, race_fids) if race_fids else actor_names(kw)
            if not hit and race_names:
                # Keyword set on a template the NPC export can't tie to a race
                # ("Glowing" + Trog): keep the keyword's actors named after the race.
                hit = {n for n in actor_names(kw, allow_template=True)
                       if any(re.search(r"\b" + re.escape(rn) + r"s?\b", n, re.I) for rn in race_names)}
            groups["target"] |= (hit or race_names)
        if narrow:
            groups["target"] |= narrow
        elif races and not kws and not groups.get("target"):
            groups["target"] |= {n for _, n in races}
        if not any(groups.values()):
            for g, t in COUNTER_LINES.get(str(it.get("snam") or "").strip().lower(), []):
                groups[g].add(t)
        sn = str(it.get("snam") or "").strip()
        if sn in ("Vendor Items Sold", "Vendor Items Purchased") and is_npc_vendor_challenge(full):
            groups["vendor"] |= CAPS_VENDORS
        if races and not kws and not narrow:
            for rf, rn in races:
                if rn in ("Human", "Ghoul"):          # people: name the groups (Blood Eagle, Cultist ...)
                    groups["group"] |= HUMAN_GROUPS.get(rf, set())
                else:                                 # creatures / robots: the spawned NPCs of that race
                    groups["target"] |= spawned_race_npcs({rf})
        if not groups.get("target"):
            for kw in COUNTER.get(str(it.get("snam") or "").strip().lower(), []):
                used_kw.add(kw)
                groups["target"] |= actor_names(kw)

        row = rows.get(label)
        if not row:
            row = rows[label] = {"label": label, "name": name, "required": req, "epic": epic,
                                 "stat": str(it.get("snam") or "").strip(),
                                 "_groups": defaultdict(set), "_guides": [], "_kw": set()}
        for k, v in groups.items():
            row["_groups"][k] |= v
        row["_kw"] |= used_kw
        row["_guides"].extend(it.get("guides") or [])

    out = []
    for label, row in rows.items():
        groups = []
        for k in GROUPS:
            if not row["_groups"].get(k):
                continue
            g = {"label": GROUPS[k], "names": sorted(row["_groups"][k], key=str.lower)}
            if k == "also":
                links = {}
                for t in g["names"]:
                    gg = lookup_guide(SITUATION_GUIDE.get(t, "")) if SITUATION_GUIDE.get(t) else None
                    if gg:   # public + published only (lookup_guide only knows those)
                        links[t] = {"title": gg["title"], "url": gg["url"], "brand": gg["brand"],
                                    "cat": gg["cat"], "sub": gg["sub"]}
                if links:
                    g["links"] = links
            groups.append(g)
        guides = guides_for(row["name"], row["_guides"], row["_kw"], stats)
        if not guides:
            stats["noGuides"].append(f"{ctype}: {label}")
        if not groups:
            stats["noNames"].append(f"{ctype}: {label}")
        rec = {"label": label, "name": row["name"], "required": row["required"],
               "epic": row["epic"], "groups": groups, "guides": guides}
        if not groups and row.get("stat") and row["stat"].upper() != "NONE":
            rec["stat"] = row["stat"]      # page: "counted by the game's X stat, no other condition"
        out.append(rec)
    out.sort(key=lambda r: (r["label"].lower()))
    return out


def main():
    print("=" * 60)
    print("  Building Challenge Keyword Finder JSON")
    print(f"  Mode: {CHANNEL.upper()}")
    print("=" * 60)
    pages, chal_source = load_challenges()
    load_exports()
    load_cut_page()
    load_name_overrides()
    load_counter_map()
    load_guides()
    load_guide_map()
    load_situations()
    load_world_pets()
    load_hidden()
    load_build_menu()
    load_counter_lines()
    load_vendors()
    load_race_groups()
    load_npc_spawns()

    stats = {"hidden": 0, "noGuides": [], "noNames": [], "cutChallenges": [], "hiddenChallenges": []}
    out = OrderedDict()
    for t in TYPES:
        pl = pages.get(t.lower()) or {}
        items = pl if isinstance(pl, list) else (pl.get("items") or pl.get("challenges") or [])
        out[t] = challenge_rows(items, t, stats)
        print(f"  {t}: {len(out[t])} challenges")

    if sum(len(v) for v in out.values()) < 50:
        print("ERROR: too few challenges -- not writing", file=sys.stderr)
        return 1

    output = {
        "types": TYPES,
        "challenges": out,
        "_meta": {
            "built": date.today().isoformat(),
            "channel": CHANNEL,
            "challengeSource": chal_source,
            **SOURCES,
            **CUT_SOURCES,
            "cutChallengesDropped": len(stats["cutChallenges"]),
            "hiddenChallenges": len(stats["hiddenChallenges"]),
            "cutNamesDropped": len(set().union(*CUT_REMOVED.values())) if CUT_REMOVED else 0,
            "counts": {t: len(out[t]) for t in TYPES},
            "withoutGuides": len(stats["noGuides"]),
            "withoutNames": len(stats["noNames"]),
        },
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, OUT_NAME)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(output, f, ensure_ascii=False, indent=1)
        f.write("\n")

    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
    with open(AUDIT_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write("# Conditions left off the Challenge Keyword Finder (no plain in-game name).\n")
        f.write("function\tparam\tchallenges\n")
        for (fn, param), labels in sorted(UNRESOLVED.items()):
            f.write(f"{fn}\t{param}\t{' | '.join(sorted(labels))}\n")
        f.write("\n# Cut challenges left out of the dropdowns (source\tname [EDID])\n")
        for why, full, edid in sorted(stats["cutChallenges"]):
            f.write(f"CUT_CHALLENGE\t{why}\t{full} [{edid}]\n")
        f.write("\n# Challenges hidden by tsv/challenge_finder_hide.tsv\n")
        for l in stats["hiddenChallenges"]:
            f.write(f"HIDDEN\t\t{l}\n")
        f.write("\n# Cut records left out of the lists (source\tname)\n")
        for why, names in sorted(CUT_REMOVED.items()):
            for n in sorted(names, key=str.lower):
                f.write(f"CUT_NAME\t{why}\t{n}\n")
        f.write("\n# Challenges with no guide link\n")
        for l in stats["noGuides"]:
            f.write(f"NO_GUIDE\t\t{l}\n")
        f.write("\n# Challenges with nothing narrower than the name (page says 'anything that matches')\n")
        for l in stats["noNames"]:
            f.write(f"NO_NAMES\t\t{l}\n")

    print(f"\n  Written to: {path}")
    print(f"  Unresolved condition kinds: {len(UNRESOLVED)}  (see {os.path.relpath(AUDIT_PATH, ROOT)})")
    print(f"  Without guides: {len(stats['noGuides'])}   Without names: {len(stats['noNames'])}")
    print(f"  Guide links dropped as not public: {stats['hidden']}")
    print(f"  Cut challenges dropped: {len(stats['cutChallenges'])}   "
          f"cut names dropped: {sum(len(v) for v in CUT_REMOVED.values())} "
          + str({k: len(v) for k, v in CUT_REMOVED.items()}))
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
