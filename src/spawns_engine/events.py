#!/usr/bin/env python3
r"""
spawns_engine.events — detect the EVENT / ACTIVITY reward level lists (LVLI) in an
item's closure, for the "Events & Activities" root expand on every "{Item} Spawn
Locations" page (spawn-guide skill §9k).

The shared engine already resolves the FULL LVLI closure (every list that can roll
the item). This module filters that closure down to the lists that are *event /
activity reward pools* — public events, seasonal events, daily ops, expeditions,
bounty hunts, treasure hunters, spooky scorched, meat week, loot bags, and
event-quest reward pools — so the renderer can surface them as a source group, the
same way Vendors / Resource Generators are.

Detection is two complementary passes, NO hardcoded FormIDs:

  1. EDID-keyword pass — a closure list whose EDID carries a reward / activity
     signal (and is NOT one of the other source families) is an event/activity
     pool. Catches event LOOT pools reached via containers/creatures/aliases
     during an event (fasnacht clusters, mothman cultist loot, bounty-hunt junk,
     spooky/meat-week/treasure-hunter rewards, lunchbox/caravan loot bags).

  2. QUEST registry pass — "anything on the QUEST tsv". The QUEST export lists,
     per quest, the GMRW reward records it hands out; each GMRW's RewardedItem may
     be a reward LVLI. We build {reward LVLI -> (quest name, quest type)} and flag
     any closure list in that registry. Authoritative for quest / public-event /
     daily / expedition COMPLETION reward pools, with real names + types.

Nested loot bags: FO76 loot bags are LVLI-based (a bag list nested inside the
event's reward list), so they are already in the item's up-closure. detect()
COLLAPSES a candidate that is nested under another candidate, keeping the OUTER
event/activity root; the rate is then resolved from that root with rng76
(resolve_event_rates), which walks the full nested tree DOWN to the item — so the
transitive "item <- loot bag <- activity/event" chance is chained automatically.
When nothing matches, detect() returns [] and the renderer shows the empty-state.
"""

import os
import re
import csv
import glob

# ── keyword vocabulary ───────────────────────────────────────────────────────
# Reward / activity signal (build_events_rewards_json.py prefix vocabulary + known
# public/seasonal event codes + loot-bag containers).
INCLUDE_RE = re.compile(
    r"(reward|\bquest|bountyhunt|bounty_hunt|treasure_?hunt|meatweek|meat_week|"
    r"fasnacht|invaders|mole_?miner|holiday|halloween|spooky|eviction|rumble|"
    r"expedition|daily_?op|score_s\d|seasonal|carnival|nukaworldontour|nwot|"
    r"equinox|campaign|lunchbox|loot_?bag|caravan|partycrasher|party_crasher)",
    re.I,
)

# Other source families — these own their own expands, never route them here.
EXCLUDE_RE = re.compile(
    r"(lld_creature|lls_creature|lle_creature|_creature|creature_|vendor|collectron|"
    r"slowroaster|morbidwell|cultistwell|atx_resource|resources?_|container_|dispenser)",
    re.I,
)

# Leading quest/event ID token to strip for a friendlier display name.
_ID_PREFIX_RE = re.compile(
    r"^(?:llq_|lls_|lle_|lld_|ll_|ra_)?"
    r"(?:e0\d+[a-z]?|ff\d+z?|ffz\d*|mtns?\d*|mtr\d*|cbz\d*|bs\d+|bo\d*|sr\d+|mq\d+|"
    r"tw\d+|twz\d+|burn|storm|nwot|moon|mn\d+|sse|d0\d+[a-z]?|v9\d+|w0\d+|p\d+)_",
    re.I,
)
_LIST_TOKEN_RE = re.compile(r"\b(?:LLQ|LLS|LLE|LLD|LL|RA|MQ\d+|LCP|LLI)\b", re.I)


# ── QUEST + GMRW reward registry (authoritative) ─────────────────────────────
_REGISTRY_CACHE = {}


def _newest(patterns, tsv_root):
    for pat in patterns:
        hits = sorted(glob.glob(os.path.join(tsv_root, pat)), key=os.path.getmtime,
                      reverse=True)
        if hits:
            return hits[0]
    return None


def _fid(token):
    return (token or "").split(":")[0].strip().upper()


def load_reward_registry(tsv_root=None):
    """{reward LVLI FormID -> (name, type)} from QUEST(GMRWRef) -> GMRW(RewardedItem).

    "Anything on the QUEST tsv": every reward LVLI a quest hands out (via its GMRW
    reward records), tagged with the quest's display name + Quest Type. Broadened
    to any GMRW whose EDID is a QuestReward_/ChallengeReward_ record. Cached by
    tsv_root. Returns {} when the exports are absent (keyword pass still runs)."""
    if tsv_root is None:
        here = os.path.dirname(os.path.abspath(__file__))
        tsv_root = os.path.join(os.path.dirname(os.path.dirname(here)), "tsv")
    if tsv_root in _REGISTRY_CACHE:
        return _REGISTRY_CACHE[tsv_root]

    reg = {}
    qpath = _newest(["QUEST_Export_*.tsv"], tsv_root)
    gpath = _newest(["GMRW_Export_*.tsv"], tsv_root)
    if not (qpath and gpath):
        _REGISTRY_CACHE[tsv_root] = reg
        return reg

    # quest -> its GMRW reward refs
    gmrw_quest = {}
    with open(qpath, encoding="utf-8", errors="replace") as f:
        rd = csv.DictReader(f, delimiter="\t")
        gcols = [c for c in (rd.fieldnames or []) if c and c.startswith("GMRWRef")]
        for row in rd:
            qname = (row.get("FULL - Name") or row.get("EDID") or "").strip()
            qtype = (row.get("Quest Type") or "").strip()
            for gc in gcols:
                g = _fid(row.get(gc))
                if g:
                    gmrw_quest.setdefault(g, (qname, qtype))

    # gmrw -> rewarded LVLIs (+ its own edid)
    gmrw_lvlis, gmrw_edid = {}, {}
    with open(gpath, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            g = (row.get("FormID") or "").strip().upper()
            if not g:
                continue
            gmrw_edid.setdefault(g, (row.get("EDID") or "").strip())
            ri = row.get("RewardedItem") or ""
            if ":LVLI" in ri.upper():
                gmrw_lvlis.setdefault(g, set()).add(_fid(ri))

    for g, (qname, qtype) in gmrw_quest.items():
        for lv in gmrw_lvlis.get(g, ()):
            reg.setdefault(lv, (qname, qtype))
    for g, ed in gmrw_edid.items():
        if re.match(r"(quest|challenge)reward", ed, re.I):
            for lv in gmrw_lvlis.get(g, ()):
                reg.setdefault(lv, (ed, ""))

    _REGISTRY_CACHE[tsv_root] = reg
    return reg


# ── name / type helpers ──────────────────────────────────────────────────────
def _event_type(edid: str) -> str:
    e = (edid or "").lower()
    if "bountyhunt" in e or "bounty_hunt" in e:
        return "Bounty Hunt"
    if "treasurehunt" in e or "treasure_hunt" in e:
        return "Treasure Hunter"
    if "expedition" in e:
        return "Expedition"
    if "dailyop" in e or "daily_op" in e:
        return "Daily Op"
    if "lunchbox" in e or "lootbag" in e or "loot_bag" in e or "caravan" in e:
        return "Loot Bag"
    if any(t in e for t in ("fasnacht", "meatweek", "meat_week", "mothman", "equinox",
                            "invaders", "moleminer", "mole_miner", "holiday", "halloween",
                            "spooky", "eviction", "rumble", "carnival", "nukaworldontour",
                            "nwot", "score_s", "seasonal")):
        return "Seasonal / Public Event"
    if "questreward" in e or "quest_reward" in e or "quest" in e:
        return "Quest Reward"
    return "Event / Activity"


def prettify_event_name(edid: str) -> str:
    """Best-effort friendly name from a reward-list EDID. The exact EDID is always
    shown alongside, so this only needs to read cleanly."""
    if not edid:
        return ""
    s = _ID_PREFIX_RE.sub("", edid)
    s = s.replace("_", " ")
    s = _LIST_TOKEN_RE.sub(" ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)   # de-camel
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        s = re.sub(r"_", " ", edid).strip()
    return s[:1].upper() + s[1:] if s else edid


def _ancestors(x, c2p):
    """All LVLI FormIDs reachable UP from x via child->parent (excludes x)."""
    seen, stack = set(), [x]
    while stack:
        n = stack.pop()
        for p in c2p.get(n, ()):
            if p not in seen:
                seen.add(p)
                stack.append(p)
    return seen


# ── public API ───────────────────────────────────────────────────────────────
def detect(closure, parent_edid, c2p=None, tsv_root=None, registry=None):
    """Return the event/activity reward ROOTS in a closure, as
        [{"list_id", "edid", "name", "type"}], sorted by display name.

    closure     : iterable of LVLI FormIDs (get_sources()'s 'lvli_closure').
    parent_edid : {LVLI FormID -> EDID} (tables['parent_edid']).
    c2p         : {child FormID -> set(parent FormIDs)} (tables['c2p']). When given,
                  a candidate nested under another candidate is collapsed away so
                  only the OUTER event/activity root is reported (nested loot bags).
    registry    : precomputed QUEST reward registry; loaded (cached) when None.
    """
    if registry is None:
        registry = load_reward_registry(tsv_root)

    cand = {}
    for lv in closure:
        ed = parent_edid.get(lv, "") or ""
        if EXCLUDE_RE.search(ed):
            continue
        in_registry = lv in registry
        if in_registry or (ed and INCLUDE_RE.search(ed)):
            cand[lv] = (ed, in_registry)

    candset = set(cand)
    roots = candset
    if c2p:
        roots = {lv for lv in candset if not (_ancestors(lv, c2p) & (candset - {lv}))}

    out = []
    for lv in roots:
        ed, in_registry = cand[lv]
        if in_registry:
            qname, qtype = registry[lv]
            name = qname or prettify_event_name(ed)
            typ = qtype or _event_type(ed)
        else:
            name, typ = prettify_event_name(ed), _event_type(ed)
        out.append({"list_id": lv, "edid": ed, "name": name, "type": typ})
    return sorted(out, key=lambda r: (r["name"].lower(), r["list_id"]))


def _fmt_pct(p: float) -> str:
    if p is None or p <= 0:
        return "0%"
    if p >= 0.9995:
        return "100%"
    if p * 100.0 < 0.01:            # positive but below display precision
        return "<0.01%"
    s = f"{p * 100.0:.2f}".rstrip("0").rstrip(".")
    return f"{s}%"


def resolve_event_rates(events, targets, appearance_fn):
    """Fill each event/activity root's chained drop rate + blurb, sort by chance
    desc, and drop pools with no real chance. `appearance_fn(list_id, targets)`
    returns the item's overall appearance probability resolved from that ROOT list
    (rng76 walks the nested tree — incl. loot bags — DOWN to the item, so the
    chance is chained).

    When appearance_fn is given, rows whose resolved chance is 0 are dropped: the
    closure over-approximates (structurally reachable) while rng76 gives the true
    chance, so a 0 means the item can't actually come from that pool. When
    appearance_fn is None (rng76 unavailable) NO rows are dropped — they list with
    a blank rate so the pools are still shown.
    """
    if not isinstance(events, list):
        return
    kept = []
    for row in events:
        if not isinstance(row, dict) or not row.get("list_id"):
            continue
        row.setdefault("note",
                       f"Chance the item is in the {row.get('name') or row.get('edid')} "
                       f"reward pool per event/activity payout.")
        if appearance_fn is None:
            row["rate_value"] = None
            row["rate_display"] = ""
            row["rate_lines"] = []
            kept.append(row)
            continue
        try:
            p = float(appearance_fn(row["list_id"], targets) or 0.0)
        except Exception:
            p = 0.0
        if p <= 0:
            continue  # not a real source per rng76 — drop the over-approximation
        disp = _fmt_pct(p)
        row["rate_value"] = round(p, 6)
        row["rate_display"] = disp
        row["rate_lines"] = [disp]
        kept.append(row)
    kept.sort(key=lambda r: (-(r.get("rate_value") or 0.0), (r.get("name") or "").lower()))
    events[:] = kept
