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
import plan_sources
import uwa_trace

_RECIPE = re.compile(r"(^|_)recipe_", re.I)

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

# Bethesda's unique rosters — catalogues for data validation, not sources.
ROSTER_LIST = re.compile(r"^LL_(Weapon|Armor)_Unique_All$", re.I)

NPC_COPY = re.compile(r"^(cr[A-Z0-9]|DEL_|XPD_|MTR)|_cr[A-Z]")

# Leading EditorID token of a retired copy -> the mode it belonged to.
LEGACY_MODES = {"survival": "Survival mode", "nw": "Nuclear Winter",
                "nuclearwinter": "Nuclear Winter"}


def is_legacy_record(edid):
    """A retired copy: a known retired mode's prefix, or DEPRECATED_. A bare
    repair-only record is NOT enough — Grand Finale's E09A_ record is just
    the copy its bounty target carries."""
    return bool(legacy_mode(edid)) or bool(re.match(r"DEPRECATED_", edid or "", re.I))


def legacy_mode(edid):
    """'Survival_AssaultRifle_WhistleInTheDark' -> 'Survival mode'."""
    head = re.split(r"[_]", edid or "", 1)[0].lower()
    return LEGACY_MODES.get(head, "")


DEAD_REWARD = re.compile(r"(^|_)(CUT|DONOTUSE|zzz)", re.I)

# What an owning leveled list / reward record says about where it sits.
# First match wins, so the specific patterns come before the generic ones.
LIST_ROUTES = [
    (re.compile(r"DailyOps", re.I),
     "Events & Activities", "Daily Ops"),
    (re.compile(r"MutatedEvents?", re.I),
     "Events & Activities", "Mutated public events"),
    # BountyDrop: what a bounty target NPC drops (SDOW_LL_BountyDrop_BIG,
    # the Slasher's — Slasher Axe sits in it at 5%).
    (re.compile(r"BountyHunt|BountyTarget|BountyDrop", re.I),
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
    # Pint-Sized Phantoms infestations (HTO = Hostile Takeover; the quest
    # export calls the activity "Infestation"). Without this the parent list
    # leaked as "HTO cr LLS Rewards Special Events SDOW".
    (re.compile(r"(^|_)HTO_|Infestation", re.I),
     "Events & Activities", "Infestations"),
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
        self.rec_edid = {}
        for path, fid_col, full_col, edid_col in (
                (self._pick("WEAP_Export_*_Base.tsv"), "WEAP_FormID", "WEAP_FULL", "WEAP_EDID"),
                (self._pick("ARMO_Export_*_ARMOUR.tsv"), "ARMO_FormID", "ARMO_FULL", "ARMO_EDID")):
            for r in _rows(path):
                nm = _norm(r.get(full_col))
                # The NPC copy shares the name (cr10mm_CircuitBreaker is the
                # Indebted Gambler's gun). Walking it printed "Enemy drop:
                # Indebted Gambler" and "Bounty hunt target" for a weapon those
                # enemies only carry.
                if nm and not uwa_trace.is_npc_copy(_q(r.get(edid_col))):
                    self.rec_by_name[nm].add(_fid(r.get(fid_col)))
                    self.rec_edid[_fid(r.get(fid_col))] = _q(r.get(edid_col))

        # COBJ: made item -> its plan BOOK. NOCRAFT recipes are recorded as
        # "exists but repair only" so the builder can clear the inherited plan.
        self.plan_of = {}
        self.repair_only = set()
        # A re-issued unique is often crafted from a plan whose recipe makes
        # the BASE weapon (co_Weapon_Ranged_AssaultRifle_WhistleInTheDark ->
        # AssaultRifle, "Plan: Whistle in the Dark"), so the record lookup
        # above never finds it. The plan's own name does.
        self.plan_by_name = {}
        self.plan_edid = {}
        for r in _rows(self._pick("COBJ_Export_*.tsv")):
            if _q(r.get("GNAM_FormID")):
                self.plan_edid[_fid(r.get("GNAM_FormID"))] = _q(r.get("GNAM_EDID"))
            cnam = _fid(r.get("CNAM_FormID"))
            gfull = _q(r.get("GNAM_FULL"))
            # Plans are named Recipe_… but often with a content prefix:
            # SCORE_S12_Recipe_Coldshoulder_StampVendor, RD01_Recipe_…, SSE_Recipe_…
            # (Cold Shoulder lost its plan route to the startswith test).
            if _RECIPE.search(_q(r.get("GNAM_EDID"))) \
                    and re.match(r"^\s*plan\s*:", gfull, flags=re.I):
                nm = re.sub(r"^\s*plan\s*:\s*", "", gfull, flags=re.I)
                self.plan_by_name.setdefault(_norm(nm), {
                    "fid": _fid(r.get("GNAM_FormID")), "name": nm})
            if not cnam:
                continue
            if _q(r.get("GNAM_EDID")) == NOCRAFT:
                self.repair_only.add(cnam)
            elif _RECIPE.search(_q(r.get("GNAM_EDID"))):
                self.plan_of[cnam] = {
                    "fid": _fid(r.get("GNAM_FormID")),
                    "name": re.sub(r"^\s*plan\s*:\s*", "",
                                   _q(r.get("GNAM_FULL")), flags=re.I),
                }

        # LVLI: what contains what, and what contains that.
        self.owners = defaultdict(set)      # entry fid -> {(list fid, edid)}
        self.list_size = {}                 # list fid -> number of entries
        edid_of = {}
        self.list_fid = {}                  # list edid -> fid
        for r in _rows(self._pick("LVLI_Export_*_LVLI_Entries.tsv")):
            lf, le = _fid(r.get("LVLI_FormID")), _q(r.get("LVLI_EDID"))
            edid_of[lf] = le
            self.list_fid[le] = lf
            ref = _fid(r.get("LVLO_Reference"))
            if ref:
                self.owners[ref].add((lf, le))
                self.list_size[lf] = self.list_size.get(lf, 0) + 1

        self.list_edid = edid_of
        self.parents = defaultdict(set)     # list fid -> {referencing edid}
        for r in _rows(self._pick("LVLI_Export_*_LVLI_Refs.tsv")):
            lf = _fid(r.get("LVLI_FormID"))
            for k, v in r.items():
                if k and k.startswith("Ref") and _q(v) and ":" in _q(v):
                    self.parents[lf].add(_q(v).split(":", 1)[1])

        # Packages you open (ALCH): a leveled list rolled by a magic effect
        # (MutatedEvents_Package_SingleMutatedEvent_Effect) that an ALCH uses
        # ("Mutated Package"). MGEF Refs name the ALCH; ALCH FULL is its name.
        alch_full = {}
        for r in _rows(self._pick("ALCH_Export_*.tsv", exclude="_Effects")):
            full = _q(r.get("FULL"))
            if full and "<" not in full and "[" not in full \
                    and not DEAD_REWARD.search(_q(r.get("ALCH_EDID"))):
                alch_full[_fid(r.get("ALCH_FormID"))] = full
        self.mgef_alch = defaultdict(list)   # MGEF edid -> [ALCH FULL]
        for r in _rows(self._pick("MGEF_Export_*.tsv")):
            ed = _q(r.get("EDID"))
            if DEAD_REWARD.search(ed):
                continue
            for k, v in r.items():
                v = _q(v)
                if k and k.startswith("Ref") and v.endswith(":ALCH"):
                    nm = alch_full.get(_fid(v))
                    if nm and nm not in self.mgef_alch[ed]:
                        self.mgef_alch[ed].append(nm)

        # GMRW: quest and challenge rewards, live ones only.
        self.rewards = defaultdict(list)    # item fid -> [(edid, quest)]
        self.dead_rewards = defaultdict(list)
        # A challenge reward (Burn_ChallengeReward_..._Abraxodyne_...) has no
        # ParentQuestDisplay; the challenge it belongs to is a CHAL in its Ref
        # columns, and the CHAL's own FULL is what the player sees in game
        # ("Find more Abraxodyne Intel").
        chal_full = {}
        for r in _rows(self._pick("CHAL_Export_*.tsv")):
            full = _q(r.get("FULL"))
            if full and "<" not in full and "[" not in full:
                chal_full[_fid(r.get("FormID"))] = full
        self.reward_chal = {}               # GMRW edid -> challenge name
        for r in _rows(self._pick("GMRW_Export_*.tsv")):
            item = _fid(r.get("RewardedItem"))
            if not item:
                continue
            ed = _q(r.get("EDID"))
            bucket = self.dead_rewards if DEAD_REWARD.search(ed) else self.rewards
            bucket[item].append((ed, _q(r.get("ParentQuestDisplay"))))
            for k, v in r.items():
                v = _q(v or "")
                if (k or "").startswith("Ref") and v.endswith(":CHAL"):
                    name = chal_full.get(_fid(v.split(":")[0]))
                    if name:
                        self.reward_chal[ed] = name
                        break

        # Vendor names as the plan pages already resolved them (the NPC who
        # sells, not the stock list: "Splint (Burning Springs Gun, Settlers
        # vendor)", "Windy (Cryptid Hunter vendor)"). Keyed by every list
        # FormID the plan route came through. One naming for both pages.
        self.vendor_label = {}
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for parts in ((("dist", "pts", "plan_master.json"),) if self.channel == "pts" else ()) \
                + (("dist", "plan_master.json"),):
            path = os.path.join(root, *parts)
            if not os.path.exists(path):
                continue
            try:
                pm = json.load(open(path, encoding="utf-8"))
            except (OSError, ValueError):
                continue
            for row in pm.get("items") or []:
                for ro in row.get("obtain_routes") or []:
                    if ro.get("source_type") != "vendor" or not ro.get("route"):
                        continue
                    for lf in ro.get("lvli") or []:
                        self.vendor_label.setdefault(_fid(lf), ro["route"])
            if self.vendor_label:
                break

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
        # A bare content code (MTNS05) names a quest only when exactly ONE
        # quest carries it: MTNS05_LL_PipeSyringer_Vox_QuestItem is the
        # "Someone To Talk To" daily. "E09A" matches many, so it stays unnamed.
        if parts and re.match(r"^[A-Za-z]{1,4}\d{2,3}[A-Za-z]?$", parts[0]):
            hits = {full for ed, full in self.quest_name.items()
                    if ed.split("_")[0] == parts[0]}
            if len(hits) == 1:
                return hits.pop()
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
            for parent in sorted(self.parents.get(list_fid, ())):
                route, label = self._classify(parent)
                if route:
                    list_edid = parent
                    break
        if not route:
            return "", "", list_edid
        return route, label, list_edid

    def _packages(self, list_fid, depth=5):
        """Names of the packages (ALCH) that roll this list, walking up the
        lists that hold it. Minty Breather sits in LL_MutatedEvents_Rewards_
        Weapons, which the Mutated Package's magic effect rolls — so the
        place you get it is "Mutated Package", not a vague event label."""
        found, seen, todo = [], set(), [(list_fid, 0)]
        while todo:
            lf, d = todo.pop(0)
            if lf in seen or d > depth:
                continue
            seen.add(lf)
            for ref in sorted(self.parents.get(lf, ())):
                ed, _, sig = ref.rpartition(":")
                if DEAD_REWARD.search(ed) or ROSTER_LIST.search(ed):
                    continue
                if sig == "MGEF":
                    for nm in self.mgef_alch.get(ed, ()):
                        if nm not in found:
                            found.append(nm)
                elif sig == "LVLI" and ed in self.list_fid:
                    todo.append((self.list_fid[ed], d + 1))
        return sorted(found, key=package_order)

    def _edid_of_list(self, list_fid):
        return self.list_edid.get(list_fid, "")

    def _drop_lines(self, fid, what):
        """Every populated route a FormID's leveled lists imply.

        `what` is "weapon" or "plan" — the distinction the page exists to make.
        """
        out = []
        for list_fid, list_edid in sorted(self.owners.get(fid, ())):
            # A cut leveled list (zzz / CUT / DONOTUSE) is not somewhere you can
            # go, same rule as the dead GMRW rewards. Without this Nailer showed
            # "Optional Success Rare Rewards" off zzzMTNS04_LL_OptionalSuccess_
            # RareRewards_QuestReward, a list nothing rolls any more.
            if DEAD_REWARD.search(list_edid or ""):
                continue
            # A vendor the plan pages already named (Giuseppe, Minerva…) is a
            # vendor route, never "Expedition" read off the list name.
            vlab = self.vendor_label.get(list_fid)
            if vlab or re.search(r"(^|_)LLV(_|$)", list_edid or "", re.I):
                if not vlab and re.search(r"GoldVendor|Gold_Vendor|GoldBullion", list_edid or "", re.I):
                    continue      # the Gold Bullion route below names it (gold_vendor.py)
                vlab = vlab or plan_sources.source_label(list_edid) or self._pretty(list_edid)
                money = (vlab + " " + list_edid + " " + self.rec_edid.get(fid, "") + " "
                         + self.plan_edid.get(fid, "")).lower()
                route = ("Gold Bullion" if "gold" in money and "bullion" in money or "goldvendor" in money
                         else "Stamps" if "stamp" in money else "Caps")
                out.append((route, vlab, what, list_fid, fid))
                continue
            route, label, shown = self._source_label(list_fid, list_edid)
            if route == "Challenges" and not label and re.search(r"ChallengeReward", shown or "", re.I):
                label = self._challenge_name(shown)
            pk = self._packages(list_fid) if route == "Events & Activities" else []
            if pk:
                label = "Opening a " + PK_SEP.join(pk)
            if not route:
                continue
            out.append((route, label or self._pretty(shown), what, list_fid, fid))
        return out

    def _challenge_name(self, ed):
        """The challenge's own title, or — when its CHAL is not in the export
        yet (RTSV_ChallengeReward_Challenge_Lifetime_Brain_1 on PTS) — the
        kind of challenge it is. Never the EditorID."""
        ed = (ed or "").split(":")[0]
        if self.reward_chal.get(ed):
            return self.reward_chal[ed]
        kind = re.search(r"Challenge_(Lifetime|Daily|Weekly|Seasonal|Event)", ed, re.I)
        return ((kind.group(1).title() + " challenge") if kind else "Challenge") \
            + " (name not in the game files yet)"

    def _reward_lines(self, fid, what):
        out = []
        for ed, quest in self.rewards.get(fid, ()):
            route, label = self._classify(ed)
            chal = self.reward_chal.get(ed)
            if chal:
                route, quest = "Challenges", quest or chal
            if not route:
                route = "Quests"
            # The quest's own name can say it is an event ("Event: A Feast Fit
            # for a King", QDLBarbara_Lesson06): file it there, name it once.
            if route == "Challenges" and not (quest or label):
                quest = self._challenge_name(ed)
            shown = quest or label or self._pretty(ed)
            if re.match(r"^\s*Event:\s*", shown or ""):
                route, quest = "Events & Activities", re.sub(r"^\s*Event:\s*", "", shown)
            # A GMRW hands the item over directly: no leveled-list entry, so
            # no list to read conditions from (list_fid None).
            out.append((route, quest or label or self._pretty(ed), what, None, fid))
        return out

    def _own_list_lines(self, list_fid):
        """Routes that hand out a unique's OWN one-item list directly.

        LL_Weapon_Ranged_AbraxoGun holds one thing (a Plasma Gun built as the
        Meadow Breeze Sprayer), so whatever rewards that list, or stocks it,
        gives exactly this unique: the "Find more Abraxodyne Intel" challenge
        reward and Splint's gun stock. Only ONE level is read, and only for a
        list with a single entry. Walking further up reaches shared unique
        pools that several events roll (the 35 wrong renames of 11 Oct 2026);
        a pool with several entries is left to the stale-only walk below.
        """
        if self.list_size.get(list_fid) != 1:
            return []
        # Path (own list, own list): the list's OWN conditions still apply
        # when a reward rolls it (Last Bastion's list only pays out after One
        # of Us) — a (None, ...) path would read as "handed over, no gate".
        out = [(r, l, w, list_fid, list_fid)
               for r, l, w, _none, _t in self._reward_lines(list_fid, "weapon")]
        # A quest that holds the list itself (alias / script, no GMRW):
        # Meteorite Sword <- MTR05_Mother_Breach "Activity: Breach and Clear".
        # Bracketed names ("[Quest for MoleMiner Mystery Crate]") are the
        # writers' notes, not something a reader can go and do — skipped.
        for ref in sorted(self.parents.get(list_fid, ())):
            ed, _, sig = ref.rpartition(":")
            if sig != "QUST" or DEAD_REWARD.search(ed):
                continue
            name = self.quest_name.get(ed, "")
            if not name or "<" in name:
                continue
            m = re.match(r"^\s*(Event|Activity)\s*:\s*(.+)$", name)
            route, label = ("Events & Activities", m.group(2)) if m else ("Quests", name)
            out.append((route, label, "weapon", list_fid, list_fid))
        for pf, pe in sorted(self.owners.get(list_fid, ())):
            if ROSTER_LIST.search(pe) or DEAD_REWARD.search(pe):
                continue
            label = self.vendor_label.get(pf)
            if not label:
                for ped in sorted(self.parents.get(pf, ())):
                    # Refs are "EDID:LVLI"; list_fid is keyed on the bare EDID.
                    label = self.vendor_label.get(self.list_fid.get(ped.split(":")[0], ""))
                    if label:
                        break
            if not label:
                continue          # not a vendor the plan pages know — skip
            low = label.lower()
            route = ("Gold Bullion" if "gold bullion" in low
                     else "Stamps" if "stamp" in low else "Caps")
            out.append((route, label, "weapon", pf, list_fid))
            out.append((route, label, "weapon", list_fid, list_fid))
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
        if not plan:
            plan = self.plan_by_name.get(key)
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
        # The unique's OWN leveled list (Bethesda's roster pairs it with the
        # item: DailyOps_LL_Weapon_Ranged_WhistleInTheDark holds a plain
        # AssaultRifle). Its record walk above finds nothing, because the list
        # holds the base gun, not the named record. The roster itself
        # (LL_Weapon_Unique_All) is a catalogue, not a source.
        # ONLY for an item that would otherwise read "retired" or have no
        # source at all: many own lists are shared pools (Treasure Hunter,
        # Dangerous Pastimes), and walking up from them names AN event, not
        # THE event — doing it for every item renamed ~60 good sources.
        how = _q(item.get("howToObtain")).lower()
        # A retired copy beside the record (Survival_LeverGun_SoleSurvivor)
        # means the item was re-issued, and the re-issue lives in the own list.
        reissued = any(is_legacy_record(self.rec_edid.get(f, "")) for f in recs)
        stale = (not how or "legacy" in how or "retired" in how
                 or "unconfirmed" in how or reissued)
        has_weapon = any(h[2] == "weapon" for h in hits)
        for le in (item.get("_rosterLists") or []) if (stale and not has_weapon) else []:
            lf = self.list_fid.get(le)
            if lf:
                hits += [h for h in self._drop_lines(lf, "weapon")
                         if not ROSTER_LIST.search(self._edid_of_list(h[3]))]

        # The unique's own one-item list, read directly and ALWAYS — also
        # when an older hand line exists. Meadow Breeze Sprayer and
        # Prototype ABX03 carried "Quest: Shadow over Appalachia", copied from
        # their base Plasma Gun, because this read only ran for items with no
        # source at all (Duchess, 12 Oct 2026).
        own_hits = []
        for le in item.get("_rosterLists") or []:
            lf = self.list_fid.get(le)
            if lf:
                own_hits += [h for h in self._own_list_lines(lf)
                             if (h[0], h[1]) not in {(x[0], x[1]) for x in hits + own_hits}]
        hits += own_hits
        meta["ownListRoutes"] = [(h[0], h[1]) for h in own_hits]

        # Group by route so "weapon and plan" reads as one entry rather than
        # the same source printed twice.
        grouped = defaultdict(lambda: {"sources": [], "what": set(), "paths": [],
                                       "by_label": defaultdict(list)})
        for route, label, what, list_fid, target in hits:
            g = grouped[route]
            if label not in g["sources"]:
                g["sources"].append(label)
            g["what"].add(what)
            # (list, record) pairs this route came from — read later for
            # Drop Conditions (build_unique_weapons_armour_json).
            g["paths"].append([list_fid, target])
            # ...and per source line, for its drop rate (uwa_rates).
            g["by_label"][label].append([list_fid, target])

        for route, g in grouped.items():
            lines = [_drops_line(g["what"], item.get("kind"))]
            for src in g["sources"]:
                lines.append(f"Source: {src}")
            if plan and "plan" in g["what"]:
                lines.append(f"Plan: {plan['name']}")
            _put(routes, route, lines,
                 tradeable=item.get("tradeable"), drop="N/A")
            for entry in routes:
                if entry["route"] == route:
                    entry["paths"] = g["paths"]
                    lp = entry.setdefault("_linePaths", {})
                    for src, ps in g["by_label"].items():
                        lp.setdefault(f"Source: {src}", []).extend(ps)

        if plan:
            gold = self.gv.route_for_plan(plan["fid"])
            if gold:
                _put(routes, "Gold Bullion",
                     ["Drops: Plan only"] + gold,
                     tradeable=item.get("tradeable"), drop="N/A")

        # A legacy copy next to a live re-issue (Survival_AssaultRifle_
        # WhistleInTheDark beside the Daily Ops version): repair-only, nothing
        # hands it out. Said in the box beside the live sources, because both
        # versions are still out there in players' hands.
        live = [r for r in routes if r["populated"]]
        if live:
            # NPC copies (crE09A_Broadsider_GrandFinale) are the enemy's
            # weapon, not a version a player ever owned.
            legacy = [f for f in recs if f in self.repair_only
                      and not self.owners.get(f) and not self.rewards.get(f)
                      and is_legacy_record(self.rec_edid.get(f, ""))]
            modes = [legacy_mode(self.rec_edid.get(f, "")) for f in legacy]
            # No separate record, but the old summary said the item was a
            # retired mode's reward (Old Guard: Nuclear Winter) and the files
            # now hand it out again — the old version is still the legacy one.
            if not legacy and stale and ("legacy" in how or "retired" in how):
                old = re.sub(r"\s*\((legacy|retired)\)|\s*[—-]\s*retired", "",
                             _q(item.get("howToObtain")), flags=re.I)
                old = re.sub(r"^\s*[^:]*:\s*", "", old).strip()
                modes = [old.replace(" reward", "") or ""]
            # Beside the live source lines, not on a vendor row.
            home = next((r for r in live if any(l.startswith("Source:") for l in r["lines"])),
                        live[0])
            for mode in modes:
                line = (f"Legacy: {mode} version — no longer obtainable, player trading only"
                        if mode else
                        "Legacy: an older version — no longer obtainable, player trading only")
                if line not in home["lines"]:
                    home["lines"].append(line)
            meta["legacyRecords"] = legacy

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
        self.replaced, self.base_plans_cleared, self.hand_only = [], [], []
        for it in items:
            routes, meta = self.routes_for(it)
            it["obtainRoutes"] = routes
            it["recordIds"] = meta["recordIds"]
            self._summary_from_own_list(it, routes, meta)
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
            elif self._is_base_plan(it):
                # "Plan: Super Sledge" on All Rise came with the base weapon's
                # record: that plan builds a plain Super Sledge. The unique's
                # own plan (if it had one) was looked for above and not found.
                self.base_plans_cleared.append(it.get("name"))
                it["planName"], it["planFormId"], it["craftable"] = "", "", False
            if not any(r["populated"] for r in routes):
                if self._fallback(it, routes):
                    self.hand_only.append((it.get("name"), _q(it.get("howToObtain"))))
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
            # Every one-line source the game files overruled. A long list here
            # means the own-list read reached further than meant — read it.
            print(f"  base-weapon plans cleared (not the unique's plan): "
                  f"{len(self.base_plans_cleared)} {self.base_plans_cleared}")
            # Sources no game-file record backs: the old hand line is all the
            # page has. This is where an inherited or guessed source can hide
            # (both Abraxo guns sat here as "Shadow over Appalachia").
            print(f"  hand-written source only (no game-file route): {len(self.hand_only)}")
            for name, how in self.hand_only:
                print(f"    {name}: {how}")
            print(f"  sources replaced from the item's own list: {len(self.replaced)}")
            for name, old, new in self.replaced:
                print(f"    {name}: {old!r} -> {new!r}")
        return filled


    _SUMMARY_PREFIX = {"Challenges": ("Challenge", "Challenge"),
                       "Quests": ("Quest", "Quest"),
                       "Caps": ("Vendor", "Vendor"), "Stamps": ("Vendor", "Vendor"),
                       "Gold Bullion": ("Vendor", "Vendor"),
                       "Events & Activities": ("Event", "Event"),
                       "Scoreboard": ("Score", "Score")}

    @staticmethod
    def _is_base_plan(it):
        plan = _norm(re.sub(r"^\s*plan\s*:\s*", "", _q(it.get("planName")), flags=re.I))
        return bool(plan) and plan == _norm(it.get("base"))

    def _summary_from_own_list(self, it, routes, meta):
        """The one-line howToObtain must agree with the routes. When the own
        list gave routes and the old line names a source none of them shows,
        the old line was inherited (base weapon) or guessed — rebuild it from
        the routes and drop the base weapon's plan that came with it."""
        own = meta.get("ownListRoutes") or []
        how = _q(it.get("howToObtain"))
        if not own and (not how or "unconfirmed" in how.lower()):
            # No line left (blank, or dropped by the build as contradicted by
            # the files): say what the generated routes say.
            own = [(r["route"], l.split(":", 1)[1].strip()) for r in routes
                   if r["populated"] for l in r["lines"] if l.startswith("Source:")]
        if not own:
            return
        detail = (how.split(":", 1)[1] if ":" in how else how).strip().lower()
        shown = " ".join(l for r in routes if r["populated"] for l in r["lines"]).lower()
        if detail and all(part.strip() in shown for part in detail.split(" / ")):
            return
        # Own-list sources first, then whatever else the routes show
        # (Slug Buster: Buried Treasure, and the mutated events pool).
        own = list(own) + [(r["route"], l.split(":", 1)[1].strip()) for r in routes
                           if r["populated"] for l in r["lines"] if l.startswith("Source:")]
        parts, first = [], None
        for route, label in own:
            pre, stype = self._SUMMARY_PREFIX.get(route, ("Source", "Unknown"))
            line = f"{pre}: {label}"
            if line not in parts:
                parts.append(line)
            first = first or (stype, label, route)
        self.replaced.append((it.get("name"), how, " / ".join(parts)))
        it["howToObtain"] = " / ".join(parts)
        it["sourceType"], it["sourceRef"] = first[0], first[1]
        it["questName"] = first[1] if first[2] == "Quests" else ""
        # The plan line came from the same base-weapon copy ("Plan: Plasma
        # Gun"): building that plan makes a plain Plasma Gun, never this.

    @staticmethod
    def _fallback(item, routes):
        """Hand-written summaries are no longer shown (Duchess, 13 Oct 2026:
        "there should never have been hand written lines"). Every route comes
        from the game files; an item they cannot place says so."""
        return False

    @staticmethod
    def _old_fallback(item, routes):
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
            # planName already reads "Plan: X" — no "Plan: Plan: X".
            lines.append("Plan: " + re.sub(r"^\s*plan\s*:\s*", "", _q(item.get("planName")),
                                           flags=re.I))
        return _put(routes, route, lines,
                    tradeable=item.get("tradeable"), drop="N/A")


def package_order(name):
    """Mutated Package, Double Mutated Package, then the Party Packs."""
    low = name.lower()
    return ("party" in low, "double" in low, low)


PK_SEP = " \u00a6 "      # between package names until merge_package_lines


def package_text(names):
    """'Opening any Mutated Package or Mutated Party Pack'. A name that ends
    in another name on the list is a kind of it (Double Mutated Package,
    Dusty Mole Miner Pail), so only the plain ones are named, with "any"."""
    names = sorted(set(names), key=package_order)
    roots = [n for n in names
             if not any(o != n and n.lower().endswith(" " + o.lower()) for o in names)]
    lead = "Opening any " if len(roots) < len(names) else "Opening a "
    return lead + " or ".join(roots)


_OPENING = re.compile(r"Opening an? ((?:(?!\s/\s).)+?)(?=\s/\s|$)")


def merge_package_lines(items):
    """One readable "Opening ..." line per route (the list walk and the holder
    trace each name the packages, one magic effect at a time), and the same
    wording in the one-line summary."""
    def _names(txt):
        return [n.strip() for n in txt.split(PK_SEP.strip()) if n.strip()]
    for it in items:
        for r in it.get("obtainRoutes") or []:
            names, first, keep, merged_paths = [], None, [], []
            for ln in r.get("lines") or []:
                m = re.match(r"^Source: Opening an? (.+)$", ln)
                if not m:
                    keep.append(ln)
                    continue
                merged_paths += (r.get("_linePaths") or {}).get(ln, [])
                if first is None:
                    first = len(keep)
                    keep.append(None)
                names += [n for n in _names(m.group(1)) if n not in names]
            if first is not None:
                keep[first] = "Source: " + package_text(names)
                r["lines"] = keep
                if merged_paths:
                    r.setdefault("_linePaths", {})[keep[first]] = merged_paths
        how = it.get("howToObtain") or ""
        if PK_SEP.strip() in how or "Opening a" in how:
            parts, names, out = how.split(" / "), [], []
            # summary parts look like "Event: Opening a A ¦ B"
            for p in parts:
                m = re.match(r"^(.*?)Opening an? (.+)$", p)
                if m:
                    if not names:
                        out.append(m.group(1) + "\0")
                    names += [n for n in _names(m.group(2)) if n not in names]
                else:
                    out.append(p)
            it["howToObtain"] = " / ".join(out).replace("\0", package_text(names))
        ref = it.get("sourceRef") or ""
        m = re.search(r"Opening an?y? [^/]+", it.get("howToObtain") or "")
        if ref.startswith("Opening a") and m:
            it["sourceRef"] = m.group(0).strip()


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
