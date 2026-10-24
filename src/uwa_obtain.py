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


def _is_dead(edid):
    """A reward / list that is not somewhere you can go: this module's own
    dead words, or cut content by the site-wide test (plan_sources.
    is_cut_content - the Drifter, Milepost Zero... 21 Oct 2026)."""
    return bool(DEAD_REWARD.search(edid or "")) or plan_sources.is_cut_content(edid)

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
        self._cut_seen = set()
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
                    and not _is_dead(_q(r.get("ALCH_EDID"))):
                alch_full[_fid(r.get("ALCH_FormID"))] = full
        self.mgef_alch = defaultdict(list)   # MGEF edid -> [ALCH FULL]
        for r in _rows(self._pick("MGEF_Export_*.tsv")):
            ed = _q(r.get("EDID"))
            if _is_dead(ed):
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
                # The required count (TNAM) tells the tiers apart: nine
                # Abraxodyne challenges are all "Find more Abraxodyne Intel"
                # (Duchess, 24 Oct 2026: "Find more Abraxodyne Intel (145)").
                import chal_names
                chal_full[_fid(r.get("FormID"))] = chal_names.display(full, _q(r.get("TNAM")))
        quest_full = {}
        for r in _rows(self._pick("QUST2_Export_*_Quests.tsv")):
            t = _q(r.get("FULL"))
            if t and "<" not in t and "[" not in t:
                quest_full[_fid(r.get("FormID"))] = t
        # The quest whose own Reward stage links the reward record - the only
        # join that is always right (GMRW Ref columns can list a second quest:
        # Hells Eagles' reward row also points at Custodial Compulsions).
        gmrw_quest = {}
        rq = self._pick("QUST2_Export_*_RewardQuests.tsv")
        for r in (_rows(rq) if rq else []):
            if _q(r.get("RowKind")) == "Reward":
                g = _q(r.get("Link")).split(":")
                t = clean = _q(r.get("QuestName"))
                if len(g) > 1 and t and "<" not in t and "[" not in t:
                    gmrw_quest.setdefault(g[1], t)
        self.reward_chal = {}               # GMRW edid -> challenge name
        for r in _rows(self._pick("GMRW_Export_*.tsv")):
            item = _fid(r.get("RewardedItem"))
            if not item:
                continue
            ed = _q(r.get("EDID"))
            bucket = self.dead_rewards if _is_dead(ed) else self.rewards
            # The QUST the reward record belongs to (its Ref columns) names it;
            # ParentQuestDisplay can be another quest's title (Hells Eagles'
            # reward row says "Custodial Compulsions"; Dangerous Pastimes' says
            # "Neurological Warfare").
            disp = gmrw_quest.get(ed) or _q(r.get("ParentQuestDisplay"))
            for k, v in ([] if ed in gmrw_quest else r.items()):
                v = _q(v or "")
                if (k or "").startswith("Ref") and v.endswith(":QUST"):
                    t = quest_full.get(_fid(v.split(":")[0]))
                    if t:
                        disp = t
                        break
            bucket[item].append((ed, disp))
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
                if _is_dead(ed) or ROSTER_LIST.search(ed):
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

    def _cut(self, list_fid, list_edid):
        """True when this list is not a route. A list that is cut CONTENT (not
        just a dead zzz list) is remembered for the item being resolved, so an
        item whose every source is cut is called cut, not unreleased, and the
        Cut Content page can say where it used to come from."""
        if DEAD_REWARD.search(list_edid or ""):
            return True
        if plan_sources.is_cut_content(list_edid):
            try:
                label = self._source_label(list_fid, list_edid)[1]
            except Exception:                     # noqa: BLE001
                label = None
            self._cut_seen.add((label or plan_sources.cut_source_name(list_edid) or list_edid,
                                plan_sources.cut_source(list_edid) or "cut list"))
            return True
        return False

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
            if self._cut(list_fid, list_edid):
                continue
            # A list whose only holders are cut or catalogue lists is not a way
            # in (V63 Zweihaender: LL_Weapon_Stormcutter sits only in
            # zzzQuestRewards_LLS_Weapon_SkylineValley and read "Expedition -
            # 100%"). A list nobody references at all is left to the rules below.
            ups = self.parents.get(list_fid, ())
            # Nothing references it at all: it is an orphan, and naming it from
            # its EditorID ("DailyOps_LL_Weapon_Melee_TheGutter" -> "Daily Ops -
            # 100%") would be a guess (24 Oct 2026).
            if not ups:
                continue
            if ups and all(_is_dead(u.rpartition(":")[0]) or ROSTER_LIST.search(u.rpartition(":")[0])
                           or u.endswith(":FLST") for u in ups):
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
            # Rate from the list the game actually rolls, not the inner one the
            # record sits in: The Fixer's plan sits in E01B_Encryptid_LLS_Weapons
            # (1 in 2) but Encryptid rolls that list at 20% (E01B_Encryptid_LL_
            # Quest_Reward, ChanceNone 80) - the page said 50%, the files 10%.
            # Only when every way up stays under this same source label.
            tops = self._entry_tops(list_fid)
            named = {l2 for t in tops for _r2, l2, *_x in self._reward_lines(t, what)}
            usable = named and not any("<" in n or not n for n in named) and label != "Daily Ops"
            if tops and usable and len(named) > 1 and all(self._reward_lines(t, what) for t in tops):
                # The ways up are DIFFERENT quests: name each by its own reward
                # record (The Fixer's AC side-quest plan list is paid by Hells
                # Eagles, Custodial Compulsions and Regent of the Dead - not
                # "Atlantic City: The Human Condition", read off the EditorID).
                for t in tops:
                    for r2, l2, _w, _n, _t in self._reward_lines(t, what):
                        out.append((r2, l2, what, t, fid))
            elif tops and all(self._classify(self._edid_of_list(t))[1] in ("", label)
                              or self._source_label(t, self._edid_of_list(t))[1] == label
                              for t in tops):
                for t in tops:
                    out.append((route, label or self._pretty(shown), what, t, fid))
            else:
                out.append((route, label or self._pretty(shown), what, list_fid, fid))
        return out

    def _entry_tops(self, list_fid, depth=6):
        """The lists above list_fid that the game rolls directly (something other
        than a leveled list holds them - a reward record, NPC, container, magic
        effect, quest). [] when list_fid itself is one, or nothing live is above."""
        def live_ups(lf):
            return [r for r in sorted(self.parents.get(lf, ()))
                    if not _is_dead(r.rpartition(":")[0]) and not ROSTER_LIST.search(r.rpartition(":")[0])
                    and not r.endswith(":FLST")]
        if any(not r.endswith(":LVLI") for r in live_ups(list_fid)):
            return []
        tops, seen, todo = [], {list_fid}, [(list_fid, 0)]
        while todo:
            lf, d = todo.pop(0)
            ups = live_ups(lf)
            if lf != list_fid and any(not r.endswith(":LVLI") for r in ups):
                tops.append(lf)
                continue
            if d >= depth:
                continue
            for r in ups:
                ed = r.rpartition(":")[0]
                pf = self.list_fid.get(ed)
                if pf and pf not in seen:
                    seen.add(pf)
                    todo.append((pf, d + 1))
        return tops

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
            if sig != "QUST" or _is_dead(ed):
                continue
            name = self.quest_name.get(ed, "")
            if not name or "<" in name:
                continue
            m = re.match(r"^\s*(Event|Activity)\s*:\s*(.+)$", name)
            route, label = ("Events & Activities", m.group(2)) if m else ("Quests", name)
            # Held by the quest itself (a script property / alias, no reward
            # record): the quest's script decides IF it spawns, so the list's
            # own "100%" is not the chance (Meteorite Sword <- Breach and
            # Clear's LL_MTR05_RareWeapon, beside a 0.25 chance GLOB). No list
            # path, so no rate is printed; a note says why.
            out.append((route, label, "weapon", None, list_fid))
            self._script_spawn.add((route, label))
        for pf, pe in sorted(self.owners.get(list_fid, ())):
            if ROSTER_LIST.search(pe) or _is_dead(pe):
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
        self._cut_seen = set()
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
        self._script_spawn = set()
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
            for src in g["sources"]:
                if (route, src) in getattr(self, "_script_spawn", ()):
                    lines.append(SCRIPT_NOTE)
            _put(routes, route, lines,
                 tradeable=item.get("tradeable"), drop="N/A")
            for entry in routes:
                if entry["route"] == route:
                    entry["paths"] = g["paths"]
                    lp = entry.setdefault("_linePaths", {})
                    for src, ps in g["by_label"].items():
                        lp.setdefault(f"Source: {src}", []).extend(ps)

        if plan:
            # One entry per gold vendor (Face Breaker: Mortimer, then Minerva),
            # each with its own Vendor / Cost - gold_vendor.routes_for_plan.
            for n, gold in enumerate(self.gv.routes_for_plan(plan["fid"])):
                if n == 0:
                    _put(routes, "Gold Bullion", ["Drops: Plan only"] + gold,
                         tradeable=item.get("tradeable"), drop="N/A")
                    continue
                at = max(i for i, r in enumerate(routes) if r["route"] == "Gold Bullion")
                routes.insert(at + 1, {"route": "Gold Bullion", "populated": True,
                                       "lines": ["Drops: Plan only"] + gold,
                                       "tradeable": item.get("tradeable"), "dropRate": "N/A"})

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
            cut_seen = sorted(getattr(self, "_cut_seen", ()) or ())
            if cut_seen and not any(r["populated"] for r in routes):
                # Every source this item had is cut content: the item is cut.
                # No hand fallback - that would re-add the cut source by name.
                it["isCut"] = True
                it["_cutSources"] = cut_seen
                continue
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


def stamp_vendor_layout(items, idx):
    """Stamps routes read like the Gold Bullion block (Duchess, 24 Oct 2026):
    Drops / Plan / Vendor (name - place) / Cost, never "Source: Giuseppe
    (Expeditions vendor) - 100.0000%". The vendor and place come from
    build_stamps_json.STAMP_VENDORS (the Stamps page's own table, keyed on the
    vendor's leveled list); the price is the plan BOOK's DATA_Value, which is
    what a stamp vendor charges (build_stamps_json.index_book). Caps vendors
    are left alone on purpose: their price moves with Charisma and perks.
    Runs before uwa_rates, so the vendor line gets no "100%"."""
    try:
        import build_stamps_json as _bs
        vendors = _bs.STAMP_VENDORS
    except Exception:                                   # noqa: BLE001
        return 0
    books = (getattr(idx.gv, "_ctx", {}) or {}).get("book_by_id", {}) or {}
    by_name = {}
    for fid, b in books.items():
        nm = (b.get("FULL") or "").strip()
        if nm and not re.match(r"^(zzz|DEL_|CUT)", b.get("EDID") or "", re.I):
            by_name.setdefault(nm.lower(), b)
    done = 0
    for it in items:
        for r in it.get("obtainRoutes") or []:
            if r.get("route") != "Stamps" or not r.get("populated"):
                continue
            lp = r.get("_linePaths") or {}
            plan_ln = next((l for l in r["lines"] if l.startswith("Plan:")), "")
            book = by_name.get(plan_ln.split(":", 1)[1].strip().lower()) if plan_ln else None
            out, changed = [], False
            for ln in r["lines"]:
                if not ln.startswith("Source:"):
                    out.append(ln)
                    continue
                vend = None
                for lst, tgt in lp.get(ln, []):
                    vend = vendors.get(idx._edid_of_list(_fid(lst)) or "")
                    b = books.get(_fid(tgt)) if tgt else None
                    if b and re.search(r"recipe", b.get("EDID") or "", re.I):
                        book = b
                    if vend:
                        break
                if not vend:
                    out.append(ln)
                    continue
                cost = ""
                try:
                    cost = int(float((book or {}).get("DATA_Value") or 0))
                except (ValueError, TypeError):
                    cost = 0
                out.append(f"Vendor: {vend['name']} — {vend['location']}")
                if cost:
                    out.append(f"Cost: {cost:,} Stamps")
                lp.pop(ln, None)
                changed = True
            if changed:
                # Drops / Plan / Vendor / Cost, the Gold Bullion order
                head = [l for l in out if l.startswith("Drops:")]
                plan = [l for l in out if l.startswith("Plan:")]
                rest = [l for l in out if l not in head and l not in plan]
                r["lines"] = head + plan + rest
                r["dropRate"] = "N/A"
                done += 1
    return done


def prize_terminal_costs(items, idx, tsv_dir):
    """Prize terminal sources say what they cost (Duchess, 24 Oct 2026).

    Read from the terminal's own menu (TERM export: ITEM rows):
      "Camden Whacker - 50 tokens"  + condition GetItemCount(token) >= 50
          -> the row names the item: Cost: 50 Mr. Fuzzy Tokens
      "Jackpot Haul! - 100 Tokens" (Hornwright claim token exchange)
          -> the row names the haul whose leveled list holds the item
             (MTR08_LL_04_MineHaulJackpot): Cost: 100 Claim Tokens (Jackpot Haul!)
    The currency is the one the source line already names ("spend Claim Tokens").
    """
    import glob
    paths = [p for p in glob.glob(os.path.join(tsv_dir, "TERM_Export_*.tsv"))
             if "_Placements" not in p and "_Quests" not in p]
    if not paths:
        return 0
    menu = []                      # (term edid, text, cost)
    for r in _rows(max(paths, key=os.path.getmtime)):
        if "ITEM" not in (_q(r.get("Row")), _q(r.get("ItemType"))):
            continue
        text = _q(r.get("Text")) or _q(r.get("Display"))
        m = re.match(r"^(.+?)\s+-\s+([\d,]+)\s+tokens?\b", text, re.I)
        if not m:
            continue
        menu.append((_q(r.get("TERM_EDID")), m.group(1).strip(), int(m.group(2).replace(",", ""))))
    n = 0
    for it in items:
        names = {_norm(x) for x in [it.get("name")] + list(it.get("aka") or []) if x}
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            lp = r.get("_linePaths") or {}
            out = []
            for ln in r["lines"]:
                out.append(ln)
                m = re.match(r"^Source: Prize terminal \(spend (.+?)\)", ln)
                if not m:
                    continue
                cur = m.group(1)
                hit = next(((t, c) for _e, t, c in menu if _norm(t) in names), None)
                if not hit:
                    lists = {idx._edid_of_list(_fid(p[0])).lower() for p in lp.get(ln, []) if p and p[0]}
                    hit = next(((t, c) for _e, t, c in menu
                                if re.sub(r"[^a-z]", "", t.split()[0].lower())
                                and any(re.sub(r"[^a-z]", "", t.split()[0].lower()) in l for l in lists)),
                               None)
                    if hit:
                        out.append(f"Cost: {hit[1]:,} {cur} ({hit[0]})")
                        n += 1
                    continue
                out.append(f"Cost: {hit[1]:,} {cur}")
                n += 1
            r["lines"] = out
    return n


SCRIPT_NOTE = ("Note: Handed out by the quest's own script rather than a reward list, "
               "so the chance is set in that script and is not in the game files.")


def _script_chances(tsv_dir):
    """QUST2_Export_*_ScriptProps.tsv (tools/export_from_esm.py QUSTSP) ->
    {quest name: [(script, {prop: (fid, edid, sig, globvalue)})]}."""
    import csv
    import glob as _g
    files = [p for p in _g.glob(os.path.join(tsv_dir or "", "QUST2_Export_*_ScriptProps.tsv"))]
    out = {}
    if not files:
        return out
    with open(max(files, key=os.path.getmtime), encoding="utf-8", errors="replace", newline="") as f:
        by = {}
        for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
            if r.get("Type") != "obj":
                continue
            fid, ed, sig = (r.get("Value") or "::").split(":", 2)
            k = ((r.get("QuestName") or "").strip(), r.get("QuestEDID"), r.get("Script"))
            by.setdefault(k, {})[r.get("Property") or ""] = (fid, ed, sig, r.get("GlobValue") or "")
    for (qn, qe, sc), props in by.items():
        out.setdefault(qn, []).append((qe, sc, props))
    return out


def _words(s):
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s or "")
    return {w.lower() for w in re.split(r"[^A-Za-z]+", s) if len(w) > 2}


def script_chance(label, item_fids, rates, chances):
    """The chance a quest's own script hands an item out, read from the
    script's properties: the item's list (an LVLI property whose tree holds
    the item) and the chance GLOB beside it in the same script. A GLOB of 1
    or less is a fraction (MTR05_Breach_RareItemPercentChance 0.25 sits with
    MTR05_Breach_PercentContainersOpeningPerBatch 0.2 - the share of
    containers per batch - so these "percent" globals are fractions); above 1
    it is a percentage (SQ_UltraciteScorchedChance 25).
    Returns (rate 0-1, glob edid, glob value) or None when the files do not
    pin it to one list + one chance."""
    if not rates or not chances:
        return None
    found = []
    for qn, rows in chances.items():
        if not (qn == label or qn.endswith(": " + label)):
            continue
        for qe, sc, props in rows:
            lists = [(pn, v) for pn, v in props.items() if v[2] == "LVLI"
                     and item_fids & rates._leaves(v[0])]
            globs = [(pn, v) for pn, v in props.items() if v[2] == "GLOB"
                     and re.search(r"chance", v[1], re.I) and v[3]]
            if len(lists) != 1 or not globs:
                continue
            lp, lv = lists[0]
            if len(globs) > 1:
                lw = _words(lp) | _words(lv[1])
                globs = [g for g in globs if _words(g[1][1]) & lw - {"mtr", "chance", "percent"}]
            if len(globs) != 1:
                continue
            g = globs[0][1]
            try:
                c = float(g[3])
            except ValueError:
                continue
            c = c if c <= 1 else c / 100.0
            y = rates.yields(lv[0])
            found.append((c * y, g[1], g[3]))
    return found[0] if len(set(f[0] for f in found)) == 1 and found else None


def script_spawn_notes(items, rates=None, tsv_dir=None):
    """After uwa_rates: a source the quest's script hands out (uwa_trace
    _scriptQuests / ObtainIndex._script_spawn). When the script's own
    properties name the item's list and a chance GLOB (QUST2 _ScriptProps),
    the line gets that rate and a note saying where it comes from; otherwise
    no percentage and a note saying why. Runs last, so no later pass puts the
    list's own 100% back."""
    chances = _script_chances(tsv_dir) if tsv_dir else {}
    n = 0
    for it in items:
        labs = set(it.pop("_scriptQuests", None) or [])
        done = set()
        fids = {_fid(x) for x in (it.get("formId"), *(it.get("variantFormIds") or [])) if x}
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated") or not labs:
                continue
            if any(l.startswith("Drops: Unlocks") for l in r["lines"]):
                continue
            for i, ln in enumerate(list(r["lines"])):
                m = re.match(r"^Source: (.+?)(\s+—\s+[\d.]+%)?$", ln)
                if not (m and m.group(1) in labs):
                    continue
                sc = script_chance(m.group(1), fids, rates, chances)
                if not sc:
                    continue
                rate, ged, gval = sc
                r["lines"][i] = f"Source: {m.group(1)} — {rate * 100:.4f}%"
                note = (f"Note: Rolled by the {('activity' if 'Activity' in ln or r['route'].startswith('Events') else 'quest')}'s own script, not a reward list - "
                        f"{ged} = {gval} in the game files, a {rate * 100:g}% chance each time.")
                if SCRIPT_NOTE in r["lines"]:
                    r["lines"].remove(SCRIPT_NOTE)
                if note not in r["lines"]:
                    r["lines"].append(note)
                done.add(r["lines"][i])
                labs.discard(m.group(1))
                n += 1
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            if any(l.startswith("Drops: Unlocks") for l in r["lines"]):
                continue          # a crafting unlock is not a spawn
            hit = False
            for i, ln in enumerate(r["lines"]):
                if ln in done:
                    continue
                m = re.match(r"^Source: (.+?)(\s+—\s+[\d.]+%)?$", ln)
                if m and (m.group(1) in labs or SCRIPT_NOTE in r["lines"]):
                    if m.group(1) in labs or (SCRIPT_NOTE in r["lines"] and m.group(2)):
                        r["lines"][i] = f"Source: {m.group(1)}"
                        hit = True
            if hit and SCRIPT_NOTE not in r["lines"]:
                r["lines"].append(SCRIPT_NOTE)
            n += hit
    return n


def dedupe_same_source(items):
    """Two Source lines backed by exactly the same lists are one source named
    twice (Slasher Axe: "Bounty hunt target" and "Enemy drop: The Reborn
    Pint-Sized Slasher" - the Reborn Slasher IS the bounty target). Keep the
    line that names the enemy / place; drop the generic one. Before uwa_rates."""
    n = 0
    for it in items:
        for r in it.get("obtainRoutes") or []:
            lp = r.get("_linePaths") or {}
            src = [l for l in r.get("lines") or [] if l.startswith("Source:") and lp.get(l)]
            key = {}
            for l in src:
                k = tuple(sorted(tuple(map(str, p)) for p in lp[l]))
                key.setdefault(k, []).append(l)
            for k, ls in key.items():
                # Only a generic holder name beside the line that names the
                # same enemy. Lines that share lists on purpose stay (Daily Ops
                # ranks, Enclave activities sharing one pool, safes and trunks).
                named = [l for l in ls if l.startswith("Source: Enemy drop:")]
                if not named:
                    continue
                for l in ls:
                    if re.match(r"^Source: (Bounty hunt target|Legendary enemy drop)$", l):
                        r["lines"].remove(l)
                        lp.pop(l, None)
                        n += 1
    return n


def choice_reward_notes(items, idx, tsv_dir):
    """A quest that hands out ONE of several rewards depending on a choice
    (Field Testing: side with Colin -> Mind Over Matter, Marty -> The Farmhand)
    says so instead of a bare 100% on both (24 Oct 2026). Read from the reward
    record: its rewards are gated by GetValue tiers and each list is named for
    its choice (BS01_MQ03_FieldTesting_LL_Quest_Reward_Colin / _Marty)."""
    import glob
    paths = glob.glob(os.path.join(tsv_dir, "GMRW_Export_*.tsv"))
    if not paths:
        return 0
    by_gmrw = defaultdict(list)          # GMRW edid -> [(list edid, tier func)]
    list_gmrw = {}
    for r in _rows(max(paths, key=os.path.getmtime)):
        it = _q(r.get("RewardedItem")).split(":")
        if len(it) < 3 or it[2] != "LVLI":
            continue
        by_gmrw[_q(r.get("EDID"))].append((it[1], _q(r.get("TierConditionFunc"))))
        list_gmrw[it[1]] = _q(r.get("EDID"))
    n = 0
    for it in items:
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            lp = r.get("_linePaths") or {}
            notes = []
            for ln, ps in lp.items():
                quest = re.sub(r"\s+—\s+[\d.]+%$", "", ln[8:]) if ln.startswith("Source: ") else ""
                for p in ps:
                    if not p or not p[0]:
                        continue
                    # the line's OWN list and the lists right above it - not the
                    # unique's list, which every line of the item shares
                    own = idx._edid_of_list(_fid(p[0]))
                    cands = [own] + [u.rpartition(":")[0] for u in idx.parents.get(_fid(p[0]), ())]
                    for le in cands:
                        g = list_gmrw.get(le)
                        if not g:
                            continue
                        sibs = [(l, f) for l, f in by_gmrw[g] if f == "GetValue"]
                        if len(sibs) < 2 or le not in [l for l, _f in sibs]:
                            continue
                        who = le.rsplit("_", 1)[-1]
                        others = [l.rsplit("_", 1)[-1] for l, _f in sibs if l != le]
                        others = [o for o in others if o.lower() not in ("reward", "rewards")]
                        # a person's name (Colin, Marty) - not a tier or rank
                        # (Daily Ops Tier01/Tier02 use the same GetValue gate)
                        name_rx = r"^[A-Z][a-z]{2,}$"
                        if not others or who.lower() in ("reward", "rewards", "tier") \
                                or not re.match(name_rx, who) \
                                or not all(re.match(name_rx, o) for o in others):
                            continue
                        notes.append(f"Note: {quest} is a choice - you get this if you side with "
                                     f"{who}; siding with {', '.join(others)} gives a different reward.")
            for nt in dict.fromkeys(notes):
                if nt not in r["lines"]:
                    r["lines"].append(nt)
                    n += 1
    return n


def atom_shop_routes(items, atom_shop_path):
    """Atom Shop / Limited Time Bundle entries from dist/atom_shop.json (the
    Atom Shop page's own data): a store item or bundle that holds a unique's
    crafting entitlement (ENTM "..._ENTM_Weapons_<Name>") is a way to get it.
    Cold Shoulder: the "Cold Shoulder Starter Bundle" (1,000 Atoms, Add-ons
    store) holds SCORE_S12_ENTM_Weapons_ColdShoulder. Store listings and
    prices are not in the game files, so they live in the Atom Shop data and
    every page reads them from there (24 Oct 2026)."""
    import json as _json
    try:
        shop = _json.load(open(atom_shop_path, encoding="utf-8"))
    except (OSError, ValueError):
        return 0

    def edids(x):
        out = {x.get("edid") or ""}
        out |= set(x.get("relatedEdids") or [])
        for b in x.get("bundleItems") or x.get("items") or []:
            out.add(b.get("edid") or "")
        return {e for e in out if e}

    offers = [("Atom Shop", x, edids(x)) for x in shop.get("items") or []]
    offers += [("Limited Time Bundle", x, edids(x)) for x in shop.get("ltb") or []]
    n = 0
    for it in items:
        key = _norm(it.get("name"))
        if not key:
            continue
        rx = re.compile(r"_ENTM_(Weapons?|Armou?r|Outfit)_" + re.escape(key) + r"$", re.I)
        for route, offer, eds in offers:
            if not any(rx.search(_norm_keep_us(e)) for e in eds):
                continue
            r = next((r for r in it.get("obtainRoutes") or [] if r.get("route") == route), None)
            if r is None:
                continue
            bench = "Armor Workbench" if (it.get("kind") or "").lower().startswith("armo") else "Weapon Workbench"
            lines = [f"Drops: Unlocks crafting ({bench})", f"Source: {offer['name']}"]
            if offer.get("atoms"):
                lines.append(f"Cost: {int(offer['atoms']):,} Atoms")
            if offer.get("store"):
                lines.append(f"Note: In the Atom Shop's {offer['store']} store.")
            if route == "Limited Time Bundle":
                lines.append("Note: A real-money platform bundle (Steam / PlayStation / Xbox), not an Atom Shop item.")
            if r.get("populated") and any(l == f"Source: {offer['name']}" for l in r.get("lines") or []):
                continue
            if r.get("populated"):
                r["lines"] += lines
            else:
                r.update({"populated": True, "lines": lines, "tradeable": False,
                          "dropRate": "N/A", "conditions": [], "conditionsChecked": True})
            n += 1
    return n


def _norm_keep_us(s):
    """EditorID with only letters, digits and underscores, for the ENTM match."""
    return re.sub(r"[^A-Za-z0-9_]", "", str(s or ""))
