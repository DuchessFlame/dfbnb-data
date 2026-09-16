#!/usr/bin/env python3
r"""
uwa_obtain.py — the nine How to Obtain routes for Unique Weapons and Armour.

WHY THIS EXISTS
===============
The page used to carry one free-text ``howToObtain`` line per item, which
cannot say the thing a reader actually needs to know: some of these weapons
drop ready-made, some only ever drop as a plan you then craft, and some do
both. "Survival mode reward (legacy)" said none of that, and worse, the plan
row was inherited from the BASE weapon — every legacy Survival weapon read
"Plan: Laser Gun / Craftable: Yes" when its own recipe is
``recipe_Dummy_Uncraftable_Item_NOCRAFT``, i.e. repair only.

So each item now carries the same fixed nine-route array the CAMP pages use
(camp-item-expands, "How to Obtain"), and each populated route says in its
first line WHAT you get there — the made item, the plan, or both.

WHAT DROPS WHAT
===============
Two different FormIDs have to be chased for every item, and they land in
different lists:

    the WEAP/ARMO record   -> ready-made drops, quest rewards, NPC loot
    the plan BOOK (COBJ    -> vendor stock, plan-only reward pools
    GNAM of its recipe)       (Minerva, Mortimer, Daily Ops chase, mutated
                               event ultra-rares)

Face Breaker is the clean example of why both are needed: the weapon comes
from the BS02 Penance quest reward list, while the PLAN is sold by Mortimer at
Neighborly and drops from the Daily Ops untradeable chase pool. Reading only
one of the two FormIDs loses half the page.

CUT REWARDS ARE NOT ROUTES
==========================
``ChallengeReward_CUT_*`` and ``*_DONOTUSE_*`` GMRW rows still name the sixteen
Survival-mode weapons — that is where the old "Survival mode reward (legacy)"
line came from. They are kept out of the routes (a dead weekly challenge is not
somewhere you can go) and reported instead through ``legacy_only``, which the
builder turns into the Note on an otherwise empty item.

USAGE
-----
    import uwa_obtain
    idx = uwa_obtain.index(channel="live")
    idx.apply(items)          # sets obtainRoutes / craftable / planName
"""

import csv
import json
import os
import re
import sys
from collections import defaultdict

import tsv_source
import gold_vendor

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

# The fixed nine, in the order camp-item-expands pins them to.
ROUTES = ["Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
          "Limited Time Bundle", "Events & Activities", "Quests", "Challenges"]

# Where the existing free-text summary lands when the exports yield no route of
# their own. That line is hand-won knowledge (the SUPP_OBTAIN table, the
# match_obtain_fallback table) and is worth more than a row of N/A — but it is
# only ever a fallback, never allowed to overwrite a generated route.
SOURCE_TYPE_ROUTE = {
    "quest": "Quests", "event": "Events & Activities",
    "public event": "Events & Activities", "expedition": "Events & Activities",
    "daily ops": "Events & Activities", "world drop": "Events & Activities",
    "score": "Scoreboard", "vendor": "Caps", "purveyor": "Events & Activities",
}

# A recipe pointing here is repair-only: the item has no plan and cannot be
# crafted from scratch, whatever the base weapon's own recipe says.
NOCRAFT = "recipe_Dummy_Uncraftable_Item_NOCRAFT"

# Bethesda's own roster of every named unique weapon, kept in the game files
# for their data validation. Where it speaks, it beats anything inferred from
# list names here.
OFFICIAL_LIST = "WeaponsUniqueNamedList"

# Noise that wraps a source EDID: list-type prefixes and reward suffixes.
_SRC_PREFIX = re.compile(r"^(cr|zzz_?|LLI?_|LLS_|LLV_|LL_|crLLI_)+", re.I)
# The same list-type tokens turn up mid-EDID (MTNS04_LL_RareRewards_QuestReward,
# E09C_LL_LoveTunnel_...), where they both read as noise and stop the EDID
# matching its quest.
_SRC_NOISE = {"ll", "lli", "lls", "llv", "lvli", "cr", "lld"}
_SRC_SUFFIX = re.compile(
    r"(_?(LL|LLI|LVLI|GMRW|NPC_|WEAP)|_Quest_?Rewards?|_QuestReward(s)?"
    r"|_Rewards?|_Weapons?|_Stage\d+(_\d+)?)+$", re.I)

DEAD_REWARD = re.compile(r"(^|_)(CUT|DONOTUSE|zzz)", re.I)

# What an owning leveled list / reward record says about where it sits.
# First match wins, so the specific patterns come before the generic ones.
LIST_ROUTES = [
    (re.compile(r"DailyOps", re.I),
     "Events & Activities", "Daily Ops"),
    (re.compile(r"MutatedEvents?", re.I),
     "Events & Activities", "Mutated public events"),
    (re.compile(r"BountyHunt|BountyTarget", re.I),
     "Events & Activities", "Bounty hunt target"),
    (re.compile(r"Expedition|MTNW|Atlantic|Skyline", re.I),
     "Events & Activities", "Expedition"),
    (re.compile(r"SCORE_|Season|Scoreboard", re.I),
     "Scoreboard", ""),
    (re.compile(r"ATX_|AtomShop", re.I),
     "Atom Shop", ""),
    (re.compile(r"Bundle", re.I),
     "Limited Time Bundle", ""),
    (re.compile(r"Challenge", re.I),
     "Challenges", ""),
    (re.compile(r"Quest|_MQ\d|COMP_|QDL|Lesson", re.I),
     "Quests", ""),
    (re.compile(r"Event", re.I),
     "Events & Activities", ""),
]


def _q(v):
    return str(v or "").strip().strip('"').strip()


def _fid(v):
    return _q(v).split(":")[0].upper().zfill(8) if _q(v) else ""


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _rows(path):
    if not path:
        return []
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def blank_routes():
    """The nine routes, every one present and empty.

    Every route is emitted even when it is empty: a dimmed N/A route is the
    page's way of saying we checked and the item genuinely isn't available
    there. A missing row would read as "not looked at yet".
    """
    return [{"route": r, "populated": False, "lines": [],
             "tradeable": None, "dropRate": None} for r in ROUTES]


def _put(routes, name, lines, tradeable=None, drop=None):
    """Write or extend one route. Lines already present are not repeated."""
    for entry in routes:
        if entry["route"] != name:
            continue
        for ln in lines:
            if ln not in entry["lines"]:
                entry["lines"].append(ln)
        entry["populated"] = bool(entry["lines"])
        if tradeable is not None:
            entry["tradeable"] = tradeable
        if drop is not None:
            entry["dropRate"] = drop
        return True
    return False


class ObtainIndex:
    def __init__(self, channel="live"):
        self.channel = channel
        self._load()

    # -- exports ---------------------------------------------------------
    def _pick(self, pattern, exclude=None):
        p = tsv_source.newest(pattern, channel=self.channel, exclude=exclude,
                              required=False)
        if p is None and self.channel == "pts":
            p = tsv_source.newest(pattern, channel="live", exclude=exclude,
                                  required=False)
        return p

    def _load(self):
        # name -> the record FormIDs that carry it. Several items have more
        # than one record (the legacy Survival copy plus a re-issued one), and
        # the re-issue is usually the one with a live source, so both are
        # walked rather than picking one.
        self.rec_by_name = defaultdict(set)
        for path, fid_col, full_col in (
                (self._pick("WEAP_Export_*_Base.tsv"), "WEAP_FormID", "WEAP_FULL"),
                (self._pick("ARMO_Export_*_ARMOUR.tsv"), "ARMO_FormID", "ARMO_FULL")):
            for r in _rows(path):
                nm = _norm(r.get(full_col))
                if nm:
                    self.rec_by_name[nm].add(_fid(r.get(fid_col)))

        # COBJ: made item -> its plan BOOK. NOCRAFT recipes are recorded as
        # "exists but repair only" so the builder can clear the inherited plan.
        self.plan_of = {}
        self.repair_only = set()
        for r in _rows(self._pick("COBJ_Export_*.tsv")):
            cnam = _fid(r.get("CNAM_FormID"))
            if not cnam:
                continue
            if _q(r.get("GNAM_EDID")) == NOCRAFT:
                self.repair_only.add(cnam)
            elif _q(r.get("GNAM_EDID")).lower().startswith("recipe_"):
                self.plan_of[cnam] = {
                    "fid": _fid(r.get("GNAM_FormID")),
                    "name": re.sub(r"^\s*plan\s*:\s*", "",
                                   _q(r.get("GNAM_FULL")), flags=re.I),
                }

        # LVLI: what contains what, and what contains that.
        self.owners = defaultdict(set)      # entry fid -> {(list fid, edid)}
        edid_of = {}
        for r in _rows(self._pick("LVLI_Export_*_LVLI_Entries.tsv")):
            lf, le = _fid(r.get("LVLI_FormID")), _q(r.get("LVLI_EDID"))
            edid_of[lf] = le
            ref = _fid(r.get("LVLO_Reference"))
            if ref:
                self.owners[ref].add((lf, le))

        self.parents = defaultdict(set)     # list fid -> {referencing edid}
        for r in _rows(self._pick("LVLI_Export_*_LVLI_Refs.tsv")):
            lf = _fid(r.get("LVLI_FormID"))
            for k, v in r.items():
                if k and k.startswith("Ref") and _q(v) and ":" in _q(v):
                    self.parents[lf].add(_q(v).split(":", 1)[1])

        # GMRW: quest and challenge rewards, live ones only.
        self.rewards = defaultdict(list)    # item fid -> [(edid, quest)]
        self.dead_rewards = defaultdict(list)
        for r in _rows(self._pick("GMRW_Export_*.tsv")):
            item = _fid(r.get("RewardedItem"))
            if not item:
                continue
            ed = _q(r.get("EDID"))
            bucket = self.dead_rewards if DEAD_REWARD.search(ed) else self.rewards
            bucket[item].append((ed, _q(r.get("ParentQuestDisplay"))))

        # Quest display names, so a route says "Penance" and not
        # BS02_MQ01_Penance_LL_Quest_Rewards. Indexed by EDID; the lookup walks
        # the source EDID's leading tokens from longest to shortest.
        # QUEST first, then QUST2 on top: the QUEST export carries every quest
        # but almost no display names, while the QUST2 walkthrough export
        # carries the names for the ones that have them ("Event: Eviction
        # Notice"). Neither alone is enough.
        self.quest_name = {}
        for pattern, col in (("QUEST_Export_*.tsv", "FULL - Name"),
                             ("QUST2_Export_*_Quests.tsv", "FULL")):
            for r in _rows(self._pick(pattern)):
                ed, full = _q(r.get("EDID")), _q(r.get(col))
                # "[Not Playable]", "[Dialogue quest for …]" — bracketed names
                # are the writers' notes to each other, not a quest a reader
                # can go and do.
                if ed and full and full.lower() != "none" \
                        and not full.startswith("["):
                    self.quest_name[ed] = full

        # Bethesda's own roster: FLST WeaponsUniqueNamedList, "Unique Named
        # Weapons for Data Validation". Each entry is either the unique's WEAP
        # record or the leveled list that hands it out — which makes it the
        # authoritative item -> source-list pairing, rather than one inferred
        # from list names. Used to confirm a source and to flag page items the
        # list has never heard of.
        self.official = {}       # normalised name -> {(sig, fid, edid)}
        for r in _rows(self._pick("FLST_Export_*_Entries.tsv")):
            if _q(r.get("FLST_EDID")) != OFFICIAL_LIST:
                continue
            sig, efid = _q(r.get("Entry_Sig")), _fid(r.get("Entry_FormID"))
            eedid, efull = _q(r.get("Entry_EDID")), _q(r.get("Entry_FULL"))
            # An LVLI entry names the unique in its own EDID
            # (LL_Weapon_Ranged_MissileLauncher_BunkerBuster); the WEAP record
            # inside it is the plain base gun, so the list EDID is the name.
            key = _norm(efull) if sig == "WEAP" else _norm(eedid.split("_")[-1])
            if key:
                self.official.setdefault(key, set()).add((sig, efid, eedid))

        # Hand-maintained table, same pattern as data/camp/<page>.json: the
        # handful of facts the exports cannot join up on their own.
        self.aliases, self.excluded = {}, {}
        table = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "data", "unique_weapons_armour.json")
        if os.path.exists(table):
            with open(table, encoding="utf-8") as fh:
                cfg = json.load(fh)
            self.aliases = cfg.get("source_aliases") or {}
            self.excluded = cfg.get("excluded") or {}

        self.gv = gold_vendor.index(channel=self.channel)

    def _quest_name(self, edid):
        """Longest leading-token match against the quest names.

        Exact first, then a prefix match — the list EDID is usually the quest's
        with an extra word on it (E08B_Eviction_LL_QuestReward vs the quest
        E08B_EvictionNotice). A prefix of one token is not enough to identify a
        quest: "E09A" alone would match half a content drop.
        """
        parts = [p for p in re.split(r"[_:]", _q(edid)) if p
                 and p.lower() not in _SRC_NOISE]
        clean = "_".join(parts)
        # Hand-written alias first — it exists precisely because nothing in the
        # exports joins these two records up.
        for key in sorted(self.aliases, key=len, reverse=True):
            if key.replace("_", "").lower() in clean.replace("_", "").lower():
                return self.aliases[key]
        # Any contiguous run of tokens, longest first: the quest code often
        # sits in the middle (QuestReward_MoM02B_Stage100_01 -> MoM02B), so a
        # leading-prefix search alone never finds it.
        for n in range(len(parts), 0, -1):
            for i in range(0, len(parts) - n + 1):
                hit = self.quest_name.get("_".join(parts[i:i + n]))
                if hit:
                    return hit
        # Only a LEADING run may match loosely, and only two tokens or more:
        # the list EDID is usually the quest's with a word added on the end.
        for n in range(len(parts), 1, -1):
            stem = "_".join(parts[:n])
            for ed, full in self.quest_name.items():
                if ed.startswith(stem):
                    return full
        return ""

    # -- classification --------------------------------------------------
    @staticmethod
    def _classify(edid):
        for pat, route, label in LIST_ROUTES:
            if pat.search(edid or ""):
                return route, label
        return "", ""

    def _pretty(self, edid):
        """A source EDID as something a reader can act on.

        The quest name wins when one matches; otherwise the record-keeping
        wrapper comes off and the CamelCase tail is split, which turns
        E09C_LL_LoveTunnel_QuestReward_Weapons into "Love Tunnel".
        """
        quest = self._quest_name(edid)
        if quest:
            return quest
        tail = _q(edid).split(":")[0]
        tail = _SRC_SUFFIX.sub("", _SRC_PREFIX.sub("", tail))
        # Drop a leading content code (E09C_, BS02_, MTNS04_) — it means
        # nothing to a reader and the words after it are the actual name.
        tail = re.sub(r"^[A-Z]{1,4}\d{2,3}[A-Z]?_", "", tail)
        tail = " ".join(p for p in tail.split("_")
                        if p and p.lower() not in _SRC_NOISE).strip()
        tail = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", tail)
        return re.sub(r"\s+", " ", tail).strip() or _q(edid)

    def _source_label(self, list_fid, list_edid):
        """Route + human label for a leveled list, looking one level up when
        the list itself is anonymous (``crLLI_..._MedicalMalpractice`` says
        nothing; its parent NPC record says Quack Doctor)."""
        route, label = self._classify(list_edid)
        if not route:
            for parent in self.parents.get(list_fid, ()):
                route, label = self._classify(parent)
                if route:
                    list_edid = parent
                    break
        if not route:
            return "", "", list_edid
        return route, label, list_edid

    def _drop_lines(self, fid, what):
        """Every populated route a FormID's leveled lists imply.

        `what` is "weapon" or "plan" — the distinction the page exists to make.
        """
        out = []
        for list_fid, list_edid in sorted(self.owners.get(fid, ())):
            route, label, shown = self._source_label(list_fid, list_edid)
            if not route:
                continue
            out.append((route, label or self._pretty(shown), what))
        return out

    def _reward_lines(self, fid, what):
        out = []
        for ed, quest in self.rewards.get(fid, ()):
            route, label = self._classify(ed)
            if not route:
                route = "Quests"
            out.append((route, quest or label or self._pretty(ed), what))
        return out

    # -- the item --------------------------------------------------------
    def routes_for(self, item):
        """(routes, meta) for one page item. meta carries the corrected plan
        and craftable flags plus a legacy-only marker."""
        routes = blank_routes()
        key = _norm(item.get("name"))
        recs = sorted(self.rec_by_name.get(key, ()))
        meta = {"planFormId": "", "planName": "", "craftable": False,
                "repairOnly": False, "legacyOnly": False, "recordIds": recs}

        plan = None
        for fid in recs:
            if fid in self.plan_of:
                plan = self.plan_of[fid]
                break
        if plan:
            meta.update(planFormId=plan["fid"], planName=plan["name"],
                        craftable=True)
        elif any(f in self.repair_only for f in recs):
            meta["repairOnly"] = True

        hits = []
        for fid in recs:
            hits += self._drop_lines(fid, "weapon")
            hits += self._reward_lines(fid, "weapon")
        if plan:
            hits += self._drop_lines(plan["fid"], "plan")
            hits += self._reward_lines(plan["fid"], "plan")

        # Group by route so "weapon and plan" reads as one entry rather than
        # the same source printed twice.
        grouped = defaultdict(lambda: {"sources": [], "what": set()})
        for route, label, what in hits:
            g = grouped[route]
            if label not in g["sources"]:
                g["sources"].append(label)
            g["what"].add(what)

        for route, g in grouped.items():
            lines = [_drops_line(g["what"], item.get("kind"))]
            for src in g["sources"]:
                lines.append(f"Source: {src}")
            if plan and "plan" in g["what"]:
                lines.append(f"Plan: {plan['name']}")
            _put(routes, route, lines,
                 tradeable=item.get("tradeable"), drop="N/A")

        if plan:
            gold = self.gv.route_for_plan(plan["fid"])
            if gold:
                _put(routes, "Gold Bullion",
                     ["Drops: Plan only"] + gold,
                     tradeable=item.get("tradeable"), drop="N/A")

        if not any(r["populated"] for r in routes):
            # A cut reward record proves it; the old summary saying "legacy"
            # is the other half — Nuclear Winter left no reward record behind
            # at all, so Old Guard had neither a route nor an explanation.
            meta["legacyOnly"] = bool(
                any(f in self.dead_rewards for f in recs)
                or "legacy" in _q(item.get("howToObtain")).lower())
        return routes, meta

    def apply(self, items, verbose=True):
        filled = legacy = 0
        for it in items:
            routes, meta = self.routes_for(it)
            it["obtainRoutes"] = routes
            it["recordIds"] = meta["recordIds"]
            # The inherited base-weapon plan is worse than no plan: it sends a
            # reader to a workbench that will never list the unique.
            if meta["planName"]:
                it["planName"] = meta["planName"]
                it["planFormId"] = meta["planFormId"]
                it["craftable"] = True
            elif meta["repairOnly"]:
                it["planName"] = ""
                it["planFormId"] = ""
                it["craftable"] = False
                it["repairOnly"] = True
            if not any(r["populated"] for r in routes):
                self._fallback(it, routes)
            if any(r["populated"] for r in routes):
                filled += 1
            elif meta["legacyOnly"]:
                legacy += 1
                was = _q(it.get("howToObtain")).replace(" (legacy)", "")
                _put(routes, "Events & Activities",
                     ["Drops: No longer obtainable",
                      f"Source: {was} — retired" if was else "Source: retired",
                      "Note: The challenge or mode that granted it is cut from "
                      "the game files. Player trading only."],
                     tradeable=it.get("tradeable"), drop="N/A")
        if verbose:
            print(f"  obtain routes: {filled} item(s) with a live route, "
                  f"{legacy} legacy-only")
        return filled


    @staticmethod
    def _fallback(item, routes):
        """Put the old one-line summary in its route rather than nine N/As."""
        how = _q(item.get("howToObtain"))
        if not how or "unconfirmed" in how.lower() or "legacy" in how.lower():
            return False
        route = SOURCE_TYPE_ROUTE.get(_q(item.get("sourceType")).lower())
        if not route:
            return False
        # "Quest: Skyline Valley (Vault 63)" -> "Skyline Valley (Vault 63)";
        # the route label already says it is a quest.
        detail = how.split(":", 1)[1].strip() if ":" in how else how
        lines = [_drops_line({"weapon"}, item.get("kind")),
                 f"Source: {detail}"]
        if _q(item.get("planName")):
            lines.append("Plan: " + _q(item.get("planName")))
        return _put(routes, route, lines,
                    tradeable=item.get("tradeable"), drop="N/A")


def _drops_line(what, kind):
    """What this route actually hands you.

    The whole reason the page moved to routes: some of these drop ready-made,
    some only ever drop as a plan you then craft, and a few do both. Saying
    which is the first line of every populated route.
    """
    made = "armour" if str(kind or "").lower().startswith("armour") else "weapon"
    if what == {"weapon", "plan"}:
        return f"Drops: Ready-made {made} and its plan"
    if what == {"plan"}:
        return "Drops: Plan only"
    return f"Drops: Ready-made {made}"


def index(channel="live"):
    return ObtainIndex(channel)


if __name__ == "__main__":
    import json
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    items = json.load(open(os.path.join(
        root, "dist/unique_weapons_armour/unique_weapons_armour.json")))["items"]
    idx = index()
    idx.apply(items)
    for it in items:
        pop = [r for r in it["obtainRoutes"] if r["populated"]]
        if pop:
            print(f"### {it['name']}")
            for r in pop:
                print(f"    [{r['route']}]")
                for ln in r["lines"]:
                    print(f"        {ln}")
