#!/usr/bin/env python3
r"""
build_quest_unique_rewards_json.py - the unique rewards from every quest, part by part.

Writes dist/quest_unique_rewards.json:

    { "schema": 1, "generated": "YYYY-MM-DD", "quests": [
        { "name": "The Mistress of Mystery",
          "aliases": ["mistress of mystery", "order of mysteries", ...],
          "url": null,
          "parts": [ { "name": "Into the Mystery",
                       "rewards": [ { "name": "Veil of Secrets", "formid": "00168291",
                                      "dropRate": 100, "conditions": [] } ] } ] } ] }

One entry per quest, or per questline (QUESTLINES below). A questline's parts are its
quests in play order, each named by its in-game quest name. A quest that hands out
rewards part-way through gets one part per reward stage, named by the in-game
objective the player has just finished (never a stage number).

What counts as unique (the same rule as the activity reward lists):
    kept   plans and recipes, apparel, unique weapons and armour, player / CAMP titles
    never  aid, ammo, junk, magazines, bobbleheads, scrip, treasury notes, regional
           supply lists, cut items (is_cut_content: zzz_ / CUT_ / POST_ ...)
    shared pools: a chance-based item (under 100%) reached only through the game's shared
           world-loot lists (SHARED_POOL: LL_ / LLS_ ... and the regional QuestReward_LLS_
           lists) is everyday loot and is left out.
    everyday loot: a chance-based item (under 100%) that turns up in the leveled reward
           lists of more than SPREAD_LIMIT different quests and activity reward pages
           (counted together) is a shared pool and is left out. Fixed rewards always stay.
Drop rates come from rng76 (the drop-rate-engine rules); a fixed reward is 100.

Inputs (newest export of each, chosen by tsv_source):
    tsv/QUEST_Export_*.tsv                    quest names and types
    tsv/QUST2_Export_*_RewardQuests.tsv       objectives and which stage gives which reward
    tsv/GMRW_Export_*.tsv                     the reward records
    tsv/LVLI_Export_*  (+ GLOB, CURV, names)  through rng76
    tsv/COBJ_Export_*.tsv                     unique gear a quest has you make at a workbench
    tsv/PLYT_Export_*.tsv, CMPT_Export_*.tsv  title names
    dist/apparel_formids.json, dist/unique_weapons_armour/*.json, dist/guide-index.json,
    dist/activities/by_page/_index.json       (quests that already have a reward page are skipped)

Usage:  python src/build_quest_unique_rewards_json.py [--check]
        --check  build and print the summary but don't write the file
"""
from __future__ import annotations

import collections
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.dont_write_bytecode = True
os.chdir(REPO)

import plan_sources  # noqa: E402  site-wide cut test
import rng76         # noqa: E402  the one drop-rate engine
import tsv_source    # noqa: E402  chronological export picking

OUT = os.path.join("dist", "quest_unique_rewards.json")
SPREAD_LIMIT = 5

# Quest types that are quests. Events, activities, daily ops, raids, expeditions and
# the rest have their own reward pages.
KEEP_TYPES = {"Primary", "Secondary", "Side Quest", "Miscellaneous", "Daily"}

# Questlines kept as ONE entry. "quests" are EditorIDs in play order.
QUESTLINES = [
    {
        "name": "The Mistress of Mystery",
        "aliases": ["mistress of mystery", "order of mysteries", "the order of mysteries"],
        "quests": ["MoM00", "MoM01", "MoM02", "MoM02A", "MoM02B", "MoM02C", "MoM03", "MoM04"],
    },
    {
        "name": "Steel Dawn",
        "aliases": ["steel dawn questline", "brotherhood steel dawn"],
        "quests": ["BS01_MQ00_Breadcrumb", "BS01_MQ01_Trust", "BS01_MQ02_Invention", "BS01_MQ03_FieldTesting",
                   "BS01_MQ04_Arms", "BS01_MQ05_Raiders", "BS01_MQ06_Settlers", "BS01_MQ07_Over",
                   "BS01_MQ08_Defense"],
    },
    {
        "name": "Steel Reign",
        "aliases": ["steel reign questline", "brotherhood steel reign"],
        "quests": ["BS02_MQ01_Penance", "BS02_MQ02_Missing", "BS02_MQ03_Blue", "BS02_MQ04_Conscience",
                   "BS02_MQ05_Catalyst"],
    },
    {
        "name": "Wastelanders",
        "aliases": ["wastelanders main quest", "wastelanders questline"],
        "quests": ["W05_MQ_001P_Wayward", "W05_MQ_002P_Radical", "W05_MQ_003P_Muscle", "W05_MQ_004P_Crane",
                   "W05_MQ_101P", "W05_MQ_101P_A", "W05_MQ_101P_B", "W05_MQ_102P",
                   "W05_MQS_201P", "W05_MQS_202P", "W05_MQS_203P", "W05_MQS_204P", "W05_MQS_205P",
                   "W05_MQR_201P", "W05_MQR_202P", "W05_MQR_203P", "W05_MQR_204P", "W05_MQR_205P",
                   "W05_MQA_206P"],
    },
    {
        "name": "Skyline Valley",
        "aliases": ["skyline valley main quest", "skyline valley questline"],
        "quests": ["Storm_MQ01_Breadcrumb", "Storm_MQ02_IntroPt1", "Storm_MQ03_IntroPt2", "Storm_MQ04_HugoPt1",
                   "Storm_MQ05_HugoPt2", "Storm_MQ06_HugoPt3", "Storm_MQ07_OberlinPt1", "Storm_MQ08_OberlinPt2",
                   "Storm_MQ09_OberlinPt3", "Storm_MQ13_Finale", "Storm_MQ13_Finale_Pt2"],
    },
    {
        # not plain "Atlantic City": that is what people type for the Atlantic City Expos
        "name": "Atlantic City Main Quest",
        "aliases": ["atlantic city questline", "atlantic city quests"],
        "quests": ["AC_MQ01_Opportunity", "AC_MQ02_Stage", "AC_MQ03_HonorBound", "AC_MQ04_Sins"],
    },
    {
        "name": "Burning Springs Main Quest",
        "aliases": ["burning springs questline", "burning springs quests"],
        "quests": ["BURN_SQ01", "Burn_MQ03_RustKingMid", "BURN_SQ02_OutroP1", "BURN_SQ02_OutroP2"],
    },
    {
        "name": "Free States",
        "aliases": ["free states questline", "free states quests"],
        "quests": ["FS01_MQ_Warn", "FS02_MQ_Reassembly", "FS03_MQ_Fruition"],
    },
    {
        "name": "Enclave",
        "aliases": ["enclave questline", "enclave quests"],
        "quests": ["EN01_MQ_Bunker", "EN01_Misc", "EN02_MQ_Us", "EN05_MQ_Officer", "EN05_Basic",
                   "EN05_ObstacleCourse", "EN05_MarksmanshipTraining", "EN05_PatriotismCourse",
                   "EN05_CombatCourse", "EN07_MQ_Death", "EN07_MQ_CodeHunt"],
    },
]

# Unnamed helper quests whose rewards belong to a named quest (helper -> named quest).
HELPER_QUESTS = {
    "MoMDress": "MoM01",      # the Garb of Mysteries is claimed during Initiate of Mysteries
}

# Items a quest hands out by script rather than through a reward record (quest -> items).
SCRIPT_REWARDS = {
    "MoM00": ["00168291"],    # Veil of Secrets
}

# Shared world-loot lists (the game's generic LL_ / LLS_ ... families and the regional
# QuestReward_LLS_ supply lists). A chance-based item reached only through one of these
# is everyday loot, not a quest's own reward. Quest-specific lists are named after
# their quest (MoM04_LL_QuestReward, AC_MQ03_LL_Reward ...) and are kept.
SHARED_POOL = re.compile(r"^(?:LL|LLS|LLI|LLD|LLC|LLE|Legendary_LL|QuestReward_LLS|RA_LL)_", re.I)

# Armour and power armour sets count only when a quest hands them out as its own reward
# (Duchess, 9 Nov 2026: Key To the Past's Raider armour and Raider Power Armor). A quest's
# own armour list can still go through the game's one-piece armour lists
# (LL_Armor_Leather_ArmLeft -> Light / Medium / Heavy), so those don't count as a shared pool
# for armour. Broad lists (LLS_Armor_Piecemeal_Any, Legendary_LL_Armor_...) still do.
ARMOUR_PIECE_LIST = re.compile(r"^LL_Armor_[A-Za-z0-9]+_(?:Arm|Leg|Torso|Chest|Helm|Head)[A-Za-z0-9_]*$", re.I)

# The objective just finished is skipped when it is only a walk to the next place
# ("Go Outside the Mine Entrance"): the one before it is what earned the reward.
_TRAVEL = re.compile(r"^(?:go|leave|head|travel|return)\b", re.I)

# A reward record tier with one of these is for teammates / other players, not you.
_TEAMMATE = re.compile(r"IsActivePlayer\s+[01]{8}\s+0(?:\.0+)?\b")
_PLACEHOLDER = re.compile(r"<[^>]*>")
_STAGE_IN_EDID = re.compile(r"_Stage(\d+)_", re.I)


# --------------------------------------------------------------- loading ----
def _rows(pattern, **kw):
    p = tsv_source.newest(pattern, **kw)
    return rng76.read_tsv(p)


def _fid(link):
    """'0063127D:QuestReward_...:GMRW' -> ('0063127D', 'QuestReward_...', 'GMRW')."""
    parts = (link or "").strip().split(":")
    if len(parts) < 3 or not re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return None
    return parts[0].upper(), parts[1], parts[-1].upper()


def _clean_text(s):
    s = _PLACEHOLDER.sub("", s or "").strip()
    return re.sub(r"\s{2,}", " ", s)


def _all_formids(node, out=None):
    """Every "formid" value anywhere in a reward page (all of its pools)."""
    out = set() if out is None else out
    if isinstance(node, dict):
        for k, v in node.items():
            if k.lower() == "formid" and isinstance(v, str) and re.fullmatch(r"[0-9A-Fa-f]{8}", v):
                out.add(v.upper())
            else:
                _all_formids(v, out)
    elif isinstance(node, list):
        for v in node:
            _all_formids(v, out)
    return out


def _key(s):
    s = re.sub(r"[^a-z0-9]", "", (s or "").lower().replace("&", "and"))
    return s[3:] if s.startswith("the") else s


def _norm(s):
    s = (s or "").lower().replace("’", "'")
    s = re.sub(r"^(daily|event|activity|quest)\s*:\s*", "", s)
    return re.sub(r"[^a-z0-9]", "", s)


class Data:
    def __init__(self):
        # quests
        self.quests = {}            # edid -> {fid, edid, name, type}
        self.quest_by_fid = {}
        for r in _rows("QUEST_Export_*.tsv"):
            edid = (r.get("EDID") or "").strip()
            if not edid:
                continue
            q = {"fid": (r.get("FormID") or "").upper(), "edid": edid,
                 "name": (r.get("FULL - Name") or "").strip(),
                 "type": (r.get("Quest Type") or "").strip()}
            self.quests[edid] = q
            self.quest_by_fid[q["fid"]] = q

        # objectives and reward stages per quest
        self.objectives = collections.defaultdict(dict)    # edid -> {idx: text}
        self.reward_stage = collections.defaultdict(dict)  # edid -> {gmrw fid: stage}
        for r in _rows("QUST2_Export_*_RewardQuests.tsv"):
            edid = (r.get("QuestEDID") or "").strip()
            kind = (r.get("RowKind") or "").strip()
            try:
                idx = int(float(r.get("Index") or ""))
            except ValueError:
                continue
            if kind == "Objective":
                self.objectives[edid].setdefault(idx, (r.get("Text") or "").strip())
            elif kind == "Reward":
                f = _fid(r.get("Link"))
                if f and f[2] == "GMRW":
                    self.reward_stage[edid][f[0]] = idx

        # reward records
        self.gmrw = collections.defaultdict(list)          # gmrw fid -> rows
        self.gmrw_edid = {}
        self.gmrw_by_quest = collections.defaultdict(set)  # quest edid -> gmrw fids
        for r in _rows("GMRW_Export_*.tsv"):
            g = (r.get("FormID") or "").upper()
            if not g:
                continue
            self.gmrw[g].append(r)
            self.gmrw_edid[g] = (r.get("EDID") or "").strip()
            pq = _fid(r.get("ParentQuestLink"))
            if pq:
                self.gmrw_by_quest[pq[1]].add(g)

        # unique gear a quest has you make at a workbench (COBJ referenced by the quest)
        self.cobj_by_quest = collections.defaultdict(list)  # quest edid -> [(fid, edid, sig?)]
        for r in _rows("COBJ_Export_*.tsv"):
            cnam = (r.get("CNAM_FormID") or "").upper()
            if not cnam:
                continue
            refs = [_fid(x) for x in (r.get("ReferencedBy_Flat") or "").split("|")]
            for ref in refs:
                if ref and ref[2] == "QUST":
                    self.cobj_by_quest[ref[1]].append(
                        (cnam, (r.get("CNAM_EDID") or "").strip(), (r.get("CNAM_FULL") or "").strip(),
                         (r.get("COBJ_EDID") or "").strip()))

        # titles
        self.plyt = {}
        for r in _rows("PLYT_Export_*.tsv"):
            e = rng76.pick(r, "EDID - Editor ID", "EDID")
            if e:
                self.plyt[e] = rng76.pick(r, "ANAM - Male Title", "ANAM")
        self.cmpt = {}
        for r in _rows("CMPT_Export_*.tsv"):
            e = rng76.pick(r, "EDID")
            if e:
                self.cmpt[e] = rng76.pick(r, "ANAM - Title", "ANAM")

        # leveled lists, globals, curves, item names
        self.rng = rng76.Rng76Data.from_tsv_root("tsv")
        self.R = self.rng.resolver
        self._deep = {}
        self._pool = {}
        self._named = {}

        # classifiers built by other builders
        with open(os.path.join("dist", "apparel_formids.json"), encoding="utf-8") as f:
            self.apparel = {x.upper() for x in json.load(f)["formids"]}
        # Outfits built on armour (the Garb of Mysteries) carry ObjectTypeArmor, so the
        # apparel list leaves them out. A costume keyword makes them apparel here.
        for r in _rows("ARMO_Export_*_ARMOUR.tsv"):
            kws = " ".join(v for k, v in r.items() if k and k.startswith("KW") and v)
            if ":ClothingTypeCostume:" in kws and ":ArmorTypePower:" not in kws:
                self.apparel.add((r.get("ARMO_FormID") or "").upper())
        with open(os.path.join("dist", "unique_weapons_armour", "unique_weapons_armour.json"),
                  encoding="utf-8") as f:
            uwa = json.load(f)
        cut_names = set()
        try:
            with open(os.path.join("dist", "unique_weapons_armour", "cut_uniques.json"),
                      encoding="utf-8") as f:
                cut_names = {(x.get("name") or "").lower() for x in json.load(f).get("items", [])}
        except FileNotFoundError:
            pass
        self.unique_names = sorted(
            {x["name"] for x in uwa.get("items", [])
             if x.get("name") and not x.get("isCut") and x["name"].lower() not in cut_names},
            key=len, reverse=True)

        with open(os.path.join("dist", "guide-index.json"), encoding="utf-8") as f:
            self.guides = json.load(f).get("guides", [])
        self.activity_names = set()
        self.activity_items = collections.defaultdict(set)   # formid -> activity pages it is on
        idx_path = os.path.join("dist", "activities", "by_page", "_index.json")
        if os.path.exists(idx_path):
            with open(idx_path, encoding="utf-8") as f:
                pages = json.load(f).get("pages", {})
            for fn in set(pages.values()):
                p = os.path.join("dist", "activities", "by_page", fn)
                try:
                    with open(p, encoding="utf-8") as f:
                        pg = json.load(f)
                except (OSError, ValueError):
                    continue
                for fid in _all_formids(pg):
                    self.activity_items[fid].add("page:" + fn)
                pg = pg.get("page") or pg
                t = pg.get("title") or pg.get("name") or (pg.get("activityData") or {}).get("name")
                if t:
                    self.activity_names.add(_norm(t))

    def deep(self, lvli):
        if lvli not in self._deep:
            self._deep[lvli] = self.R.resolve_deep(lvli)
        return self._deep[lvli]

    def named_sublists(self, root):
        """{sub-list FormID: unique name} for named unique weapons / armour in a reward list.

        A named unique is often a leveled list holding the base weapon plus the object
        template that names it (BS01_MQ02_Invention_LL_Weapon_Ranged_TheFactFinder), so
        its leaf is just "Plasma". The list's EditorID says which unique it is."""
        if root in self._named:
            return self._named[root]
        out, seen, todo = {}, set(), [root]
        L = self.rng.lvli
        while todo:
            fid = todo.pop()
            if fid in seen:
                continue
            seen.add(fid)
            if fid != root:
                nm = self._unique_from_edid(L.edid_for(fid) or "")
                if nm:
                    out[fid] = nm
                    continue
            for e in L.entries_by_list.get(fid, []):
                m = L.math_by_entry.get((fid, e.get("EntryIndex"))) or {}
                sub = (m.get("SubLVLI_FormID") or "").strip()
                if sub:
                    todo.append(sub)
        self._named[root] = out
        return out

    def _unique_from_edid(self, edid):
        """The unique whose name is in a leveled list's EditorID, else None."""
        key = _key(edid)
        for u in self.unique_names:      # longest names first
            k = _key(u)
            if len(k) >= 6 and k in key:
                return u
        return None

    def pool_items(self, root, armour=False):
        """FormIDs a reward list reaches through shared world-loot lists (SHARED_POOL).
        armour=True lets the one-piece armour lists through (ARMOUR_PIECE_LIST)."""
        if (root, armour) in self._pool:
            return self._pool[(root, armour)]
        out, seen, todo = set(), set(), [root]
        L = self.rng.lvli
        while todo:
            fid = todo.pop()
            if fid in seen:
                continue
            seen.add(fid)
            edid = L.edid_for(fid) or ""
            if fid != root and self._unique_from_edid(edid):
                continue                    # a named unique's own list is not a shared pool
            if armour and fid != root and ARMOUR_PIECE_LIST.match(edid):
                pass                        # a quest's own armour piece, any material
            elif SHARED_POOL.match(edid):
                out.update((x.get("formid") or "").upper() for x in self.deep(fid))
                continue
            for e in L.entries_by_list.get(fid, []):
                m = L.math_by_entry.get((fid, e.get("EntryIndex"))) or {}
                sub = (m.get("SubLVLI_FormID") or "").strip()
                if sub:
                    todo.append(sub)
        self._pool[(root, armour)] = out
        return out

    def is_armour(self, fid, sig):
        """Armour or a power armour set (not apparel): counted only as a quest's own reward."""
        sig = (sig or "").upper()
        return sig == "PACH" or (sig == "ARMO" and (fid or "").upper() not in self.apparel)


# ------------------------------------------------------------ conditions ----
def _op(bits):
    """xEdit CTDA type bits (low bit first) -> 0 !=, 1 ==, 2 >, 3 >=, 4 <, 5 <=."""
    return int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2])


def simplify_condition(s):
    """Raw LVLI entry condition -> the plain words the reward pages use, or ''."""
    s = (s or "").strip()
    m = re.search(r"\)\s+([01]{8})\s+(-?\d+(?:\.\d+)?)\s*$", s) or \
        re.search(r"\b([01]{8})\s+(-?\d+(?:\.\d+)?)\s*$", s)
    op = _op(m.group(1)) if m else None
    num = float(m.group(2)) if m else None
    if "GetLevel" in s and op in (2, 3) and num is not None:
        lvl = int(num) + (1 if op == 2 else 0)
        return f"Requires player level {lvl}+"
    if "HasLearnedRecipe" in s:
        must = (op == 1 and num >= 1) or (op == 0 and num < 1) or op in (2, 3)
        return "Requires the base plan to be learned" if must else \
            "Won’t drop if you’ve already learned it"
    if "GetIsRace" in s and "Ghoul" in s:
        return "Ghoul character only" if (op == 1 and num >= 1) else "Human character only"
    return ""


def simplify(conds):
    out = []
    for c in conds or []:
        t = simplify_condition(c)
        if t and t not in out:
            out.append(t)
    return out


# --------------------------------------------------------------- uniques ----
def _book_title(d, edid):
    base = edid.replace("_Recipe_", "_")
    s = base.replace("Title_", "Titles_")
    if s in d.plyt and d.plyt[s]:
        return "Player Title: " + d.plyt[s]
    for cand in (re.sub(r"(?i)CampTitle_", "CAMPTitles_", base),
                 re.sub(r"(?i)^camptitles_", "CAMPTitles_", s)):
        if cand in d.cmpt and d.cmpt[cand]:
            return "Camp Title: " + d.cmpt[cand]
    return None


def unique_name(d, fid, edid, sig, name):
    """Display name when the item counts as a unique reward, else None."""
    if not fid or plan_sources.is_cut_content(edid):
        return None
    name = (name or "").strip()
    if not name or re.fullmatch(r"[0-9A-Fa-f]{8}", name) or name.lower().startswith(("zzz", "cut", "post")):
        return None
    sig = (sig or "").upper()
    if sig == "UNIQUE":          # a named unique found through its own leveled list
        return name
    if sig == "BOOK":
        t = _book_title(d, edid)
        if t:
            return t
        if name.startswith(("Plan:", "Recipe:")):
            return name
        return None
    if sig == "PACH":
        return name or None
    if sig in ("ARMO", "WEAP"):
        if sig == "ARMO":
            # apparel, or armour that got this far as the quest's own reward
            # (gmrw_items has already dropped armour that came from a shared pool)
            return name
        low = name.lower()
        for u in d.unique_names:
            if low.startswith(u.lower()):
                return name
    return None


# --------------------------------------------------------------- rewards ----
def gmrw_items(d, gfid):
    """[(fid, edid, sig, name, rate 0-1, conditions, from_list)] for YOUR tiers of a reward record."""
    out = []
    for r in d.gmrw.get(gfid, []):
        item = _fid(r.get("RewardedItem"))
        if not item:
            continue
        conds = r.get("Conditions") or ""
        if _TEAMMATE.search(conds):
            continue
        mult = 1.0
        extra = []
        if "GetRandomPercent" in conds:
            g = _fid(r.get("ConditionGlobs") or "")
            v = d.rng.globs.value(g[0]) if g else None
            m = re.search(r"GetRandomPercent\s+([01]{8})\s*(-?\d+(?:\.\d+)?)?", conds)
            if v is None and m and m.group(2):
                v = float(m.group(2))
            if v is not None:
                op = _op(m.group(1)) if m else 5
                mult = max(0.0, min(1.0, (v + (1 if op == 5 else 0)) / 100.0)) if op in (4, 5) else mult
        m = re.search(r"GetItemCount\s+([01]{8})\s+(-?\d+(?:\.\d+)?)", conds)
        if m and _op(m.group(1)) == 1 and float(m.group(2)) == 0:
            extra.append("Only if you don’t already have it")
        fid, edid, sig = item
        if sig == "LVLI":
            pool = d.pool_items(fid)
            armour_pool = d.pool_items(fid, armour=True)
            # a quest's own armour list (MTNL01_LL_Armor_Raider_ArmLeft): its chance-based
            # pieces count; elsewhere only a guaranteed armour piece does
            armour_list = bool(re.search(r"armou?r", edid or "", re.I))
            named_leaves = set()
            for sub, uname in d.named_sublists(fid).items():
                leaves = [x for x in d.deep(sub) if (x.get("sig") or "").upper() in ("WEAP", "ARMO")]
                if not leaves:
                    continue
                lf = {(x.get("formid") or "").upper() for x in leaves}
                named_leaves |= lf
                rate = d.R.pick_rate(fid, lf) * mult
                if rate <= 0 or (rate < 1.0 and lf & pool):
                    continue
                ncond = simplify([c for x in leaves for c in (x.get("conditions") or [])])
                out.append((sorted(lf)[0], d.rng.lvli.edid_for(sub) or "", "UNIQUE", uname, rate, ncond, True))
            for leaf in d.deep(fid):
                rate = float(leaf.get("dropRate") or 0.0) * mult
                if rate <= 0:
                    continue
                lfid = (leaf.get("formid") or "").upper()
                if lfid in named_leaves:
                    continue
                if d.is_armour(lfid, leaf.get("sig")):
                    if lfid in armour_pool:
                        continue            # armour from a shared pool is everyday loot
                    if rate < 1.0 and not armour_list:
                        continue            # a chance at armour from a quest's general grab bag
                elif rate < 1.0 and lfid in pool:
                    continue
                lsig = (leaf.get("sig") or "").upper()
                lname = pack_in_name(leaf.get("edid")) if lsig == "PACH" else (leaf.get("name") or "")
                out.append((lfid, leaf.get("edid") or "", lsig, lname, rate,
                            simplify(leaf.get("conditions")), True))
        else:
            name = pack_in_name(edid) if sig == "PACH" else d.rng.names.resolve(fid, edid)
            out.append((fid, edid, sig, name, mult, extra, False))
    return out


_PA_NAMES = {"T45": "T-45", "T51": "T-51", "T60": "T-60", "T65": "T-65", "X01": "X-01"}


def pack_in_name(edid):
    """PAC_PowerArmor_Raider_Full -> 'Raider Power Armor' (pack-ins have no in-game name)."""
    m = re.match(r"(?i)^PAC_PowerArmor_([A-Za-z0-9]+)", edid or "")
    if not m:
        return ""
    t = m.group(1)
    t = _PA_NAMES.get(t.upper()) or re.sub(r"([a-z])([A-Z])", r"\1 \2", t)
    return t + " Power Armor"


def quest_sources(d, edid):
    """{stage or None: [items]} for one named quest, helpers folded in."""
    by_stage = collections.defaultdict(list)
    gm = dict(d.reward_stage.get(edid, {}))
    for g in d.gmrw_by_quest.get(edid, ()):
        if g not in gm:
            m = _STAGE_IN_EDID.search(d.gmrw_edid.get(g, ""))
            gm[g] = int(m.group(1)) if m else None
    for g, stage in gm.items():
        by_stage[stage].extend(gmrw_items(d, g))
    final = []
    for helper, owner in HELPER_QUESTS.items():
        if owner == edid:
            for g in d.gmrw_by_quest.get(helper, ()):
                final.extend(gmrw_items(d, g))
            for c in d.cobj_by_quest.get(helper, ()):
                final.append((c[0], c[1], _sig_of(d, c[0]), c[2] or d.rng.names.resolve(c[0], c[1]),
                              1.0, [], False))
    for c in d.cobj_by_quest.get(edid, ()):
        final.append((c[0], c[1], _sig_of(d, c[0]), c[2] or d.rng.names.resolve(c[0], c[1]),
                      1.0, [], False))
    for fid in SCRIPT_REWARDS.get(edid, ()):
        final.append((fid, "", _sig_of(d, fid), d.rng.names.resolve(fid), 1.0, [], False))
    if final:
        stages = [s for s in by_stage if s is not None]
        by_stage[max(stages) if stages else None].extend(final)
    return by_stage


_SIG = {}


def _sig_of(d, fid):
    """Record type (ARMO / WEAP / BOOK) of an item given without one (workbench and script rewards)."""
    return _SIG.get((fid or "").upper(), "")


def _load_sigs(d):
    """FormID -> record type from the item exports."""
    for pattern, sig, cols in (("ARMO_Export_*_ARMOUR.tsv", "ARMO", ("FormID", "ARMO_FormID")),
                               ("WEAP_Export_*_Base.tsv", "WEAP", ("FormID", "WEAP_FormID")),
                               ("BOOK_Export_*.tsv", "BOOK", ("FormID", "BOOK_FormID"))):
        p = tsv_source.newest(pattern, exclude=("_Locations",) if sig == "BOOK" else None, required=False)
        if not p:
            continue
        for r in rng76.read_tsv(p):
            f = rng76.pick(r, *cols).upper()
            if f:
                _SIG.setdefault(f, sig)


# ------------------------------------------------------------ part names ----
def _usable_objectives(d, edid):
    out = {}
    for idx, text in d.objectives.get(edid, {}).items():
        t = _clean_text(text)
        if not t or "<" in (text or "") or t.lower().startswith("(optional)"):
            continue
        out[idx] = t
    return out


def objective_name(d, edid, stage):
    """The in-game objective the player has just finished when `stage` gives its reward."""
    objs = _usable_objectives(d, edid)
    if stage is None or not objs:
        return None
    shown = [i for i in objs if i <= stage]
    if not shown:
        return None
    b = max(shown)
    if stage - b < 10:                  # the reward fires with the stage that shows objective b,
        earlier = sorted(i for i in objs if i < b)   # so the one just finished is the one before it
        while earlier and _TRAVEL.match(objs[earlier[-1]]) and len(earlier) > 1:
            earlier.pop()               # ...unless that was only a walk to the next place
        if earlier:
            return objs[earlier[-1]]
    return objs[b]


def quest_display(q):
    return _clean_text(q["name"])


def is_named_quest(q):
    e, f = q["edid"], q["name"]
    if not f.strip() or f.startswith("[") or not _clean_text(f):
        return False
    if e.startswith(("zzz", "ZZZ", "CUT", "DEL", "test_", "Test_", "Debug")) or "Template" in e:
        return False
    if "_RE_" in e or e.startswith("RE_") or "HubRE" in e:
        return False
    return not plan_sources.is_cut_content(e)


# ----------------------------------------------------------------- build ----
def build():
    d = Data()
    _load_sigs(d)

    # which quests go in, and how they group
    line_of = {}
    for i, ql in enumerate(QUESTLINES):
        for e in ql["quests"]:
            line_of[e] = i
    entries = collections.OrderedDict()   # key -> {"name", "aliases", "quests": [edid]}
    skipped_activity = []
    for edid, q in d.quests.items():
        if edid in HELPER_QUESTS:
            continue
        if edid in line_of:
            ql = QUESTLINES[line_of[edid]]
            entries.setdefault("line:" + ql["name"], {"name": ql["name"], "aliases": list(ql["aliases"]),
                                                      "quests": [], "line": True})
            continue
        if q["type"] not in KEEP_TYPES or not is_named_quest(q):
            continue
        name = quest_display(q)
        if _norm(name) in d.activity_names:
            skipped_activity.append(name)
            continue
        ent = entries.setdefault("q:" + name.lower(), {"name": name, "aliases": [], "quests": [], "line": False})
        ent["quests"].append(edid)
    for ql in QUESTLINES:
        entries["line:" + ql["name"]]["quests"] = [e for e in ql["quests"] if e in d.quests]

    # every item's parts, before the everyday-loot rule
    raw = {}
    spread = collections.defaultdict(set)
    for key, ent in entries.items():
        parts = []
        for edid in ent["quests"]:
            q = d.quests[edid]
            by_stage = quest_sources(d, edid)
            if not by_stage:
                if ent["line"]:
                    parts.append((quest_display(q), []))
                continue
            stages = sorted(by_stage, key=lambda s: (s is None, s or 0))
            last = stages[-1]
            for s in stages:
                items = by_stage[s]
                for it in items:
                    if it[6]:
                        spread[it[0]].add(key)
                single = len(stages) == 1
                if ent["line"]:
                    if s == last or single:
                        pname = quest_display(q)
                    else:
                        obj = objective_name(d, edid, s)
                        pname = f"{quest_display(q)}: {obj}" if obj else quest_display(q)
                else:
                    pname = quest_display(q) if single else (objective_name(d, edid, s) or quest_display(q))
                parts.append((pname, items))
        raw[key] = parts

    out_quests = []
    for key, ent in entries.items():
        parts_out = collections.OrderedDict()
        for pname, items in raw.get(key, []):
            best = parts_out.setdefault(pname, {})
            for fid, edid, sig, name, rate, conds, from_list in items:
                if from_list and rate < 1.0 and not d.is_armour(fid, sig) and \
                        len(spread.get(fid, set()) | d.activity_items.get(fid, set())) > SPREAD_LIMIT:
                    continue
                dn = unique_name(d, fid, edid, sig or _sig_of(d, fid), name)
                if not dn:
                    continue
                cur = best.get(fid)
                if cur is None or rate > cur["_rate"]:
                    best[fid] = {"name": dn, "formid": fid, "dropRate": round(min(rate, 1.0) * 100, 4),
                                 "conditions": conds, "_rate": rate}
        if not parts_out:
            continue
        parts = []
        for pname, best in parts_out.items():
            rewards = sorted(best.values(), key=lambda r: r["name"].lower())
            for r in rewards:
                r.pop("_rate", None)
                if float(r["dropRate"]).is_integer():
                    r["dropRate"] = int(r["dropRate"])
            parts.append({"name": pname, "rewards": rewards})
        aliases = []
        part_quests = [quest_display(d.quests[e]) for e in ent["quests"]] if ent["line"] else []
        for a in [ent["name"]] + ent["aliases"] + part_quests:
            for v in (a.lower(), re.sub(r"^(the|daily:)\s+", "", a.lower())):
                v = v.strip()
                if v and v not in aliases:
                    aliases.append(v)
        out_quests.append({"name": ent["name"], "aliases": aliases, "url": guide_url(d, ent["name"], aliases),
                           "parts": parts})
    out_quests.sort(key=lambda q: q["name"].lower())
    return d, out_quests, skipped_activity


def guide_url(d, name, aliases):
    want = {_norm(name)} | {_norm(a) for a in aliases}
    for g in d.guides:
        if (g.get("type") or "") != "guide":
            continue
        t = re.sub(r"(?i)\s*guide$", "", g.get("title") or "")
        t = re.sub(r"(?i)^quests\s*-\s*", "", t)
        if _norm(t) in want and "/quests/" in (g.get("url") or ""):
            return g["url"]
    return None


def main():
    check = "--check" in sys.argv
    d, quests, skipped = build()
    payload = {"schema": 1, "generated": None, "quests": quests}
    old = None
    if os.path.exists(OUT):
        try:
            with open(OUT, encoding="utf-8") as f:
                old = json.load(f)
        except (OSError, ValueError):
            old = None
    if old and old.get("quests") == quests and old.get("generated"):
        payload["generated"] = old["generated"]          # nothing moved: keep the date
    else:
        payload["generated"] = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).date().isoformat()

    with_rewards = [q for q in quests if any(p["rewards"] for p in q["parts"])]
    print(f"[quest rewards] {len(quests)} quests / questlines, {len(with_rewards)} with unique rewards, "
          f"{sum(len(p['rewards']) for q in quests for p in q['parts'])} reward rows")
    if skipped:
        print(f"[quest rewards] left to their activity reward page: {', '.join(sorted(set(skipped)))}")
    if check:
        print("[quest rewards] --check: not written")
        return
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"[quest rewards] wrote {OUT}")


if __name__ == "__main__":
    main()
