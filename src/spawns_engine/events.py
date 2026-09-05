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
import tsv_source          # one resolver for every export selection

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

# DO NOT ADD `mystery_?crate` HERE. The 41 MILE_LL_MysteryCrate_* lists (Milepost Zero)
# read event-like and sit in the closure of ~40 food / chem / drink items plus the
# Chainsaw — but the crates are CUT CONTENT and never shipped, so surfacing them would
# put a route players cannot take on every one of those pages. Checked Sep 2026.

# Other source families — these own their own expands, never route them here.
#
#   mystery_?machine : the Mystery Machines are CAMP resource generators. Their
#     season pools read event-like (SCORE_S23_LL_ChemMysteryMachine_Uncommon
#     matches both `score_s\d` and `seasonal`), but §9k routes `MysteryMachine`
#     to Resource Generators, which already renders them as producer cards.
#   treasure_?map    : the Treasure Maps expand (§9l) owns every dig-reward pool
#     and states the chance per dig per MAP. Listing LL_TreasureMap_Reward here
#     as well just duplicated that number under a meaningless name.
EXCLUDE_RE = re.compile(
    r"(lld_creature|lls_creature|lle_creature|_creature|creature_|vendor|collectron|"
    r"slowroaster|morbidwell|cultistwell|atx_resource|resources?_|container_|dispenser|"
    r"mystery_?machine|treasure_?map)",
    re.I,
)

# A pool nested under a VENDOR list is that vendor's stock, not an event payout —
# the Vendors expand (§9j) already names the merchant. Only the vendor family gets
# this ancestor test: event loot reached via containers/creatures/aliases DURING an
# event is a deliberate catch of the keyword pass, so those families must not be
# walked up. Example: NWOT_LL_Del_Consumable's only parents are NWOT_Del_VendorChest
# and NWOT_LL_Vendor_Del — Del is a Nuka-World on Tour merchant, and he already
# appears in Vendors, so the "Del Consumable" event row was a straight duplicate.
VENDOR_ANCESTOR_RE = re.compile(r"vendor", re.I)

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
        hit = tsv_source.newest(os.path.join(tsv_root, pat), required=False)
        if hit:
            return hit
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


# ── orphan reward pools (script-invoked, no owning event) ────────────────────
_REFCOUNT_CACHE = {}


def load_ref_counts(tsv_root=None):
    """{LVLI FormID -> ReferencedByCount} from the LVLI_Refs export.

    Used to spot a reward pool that NOTHING in the game data references — no
    parent list, no container, no activator, no quest. Such a pool is invoked
    from a script, so there is no event or activity name to put against it, and
    a row reading "Rewards General Aid Items" tells the reader nothing.

    The Refs export is ~6,900 columns wide (one RefN per referencing record), so
    read ONLY the two columns needed — reading it whole OOMs a local build.
    """
    if tsv_root is None:
        here = os.path.dirname(os.path.abspath(__file__))
        tsv_root = os.path.join(os.path.dirname(os.path.dirname(here)), "tsv")
    if tsv_root in _REFCOUNT_CACHE:
        return _REFCOUNT_CACHE[tsv_root]

    counts = {}
    path = _newest(["LVLI_Export_*_LVLI_Refs.tsv"], tsv_root)
    if path:
        try:
            import rng76
            for row in rng76.read_tsv_columns(path, ("LVLI_FormID", "ReferencedByCount")):
                fid = (row.get("LVLI_FormID") or "").strip().upper()
                if not fid:
                    continue
                try:
                    counts[fid] = int((row.get("ReferencedByCount") or "0").strip() or 0)
                except ValueError:
                    counts[fid] = 0
        except Exception:
            counts = {}          # export missing/unreadable -> drop nothing
    _REFCOUNT_CACHE[tsv_root] = counts
    return counts


def is_orphan_pool(list_id, edid, ref_counts, in_registry):
    """True when a candidate has no owning event and should not be listed.

    Self-healing by design: the ONLY thing that keeps a pool out is that nothing
    references it AND no quest hands it out. The moment Bethesda nests it under
    an event list, hangs it on a container/activator, or wires it to a QUEST/GMRW
    reward, refcount or registry membership flips and the row comes back on every
    item page automatically — with the real event name attached. Never add a
    FormID or EDID to a block list to achieve this.
    """
    if in_registry:
        return False
    if not ref_counts:                  # no Refs export -> can't tell, keep it
        return False
    fid = (list_id or "").strip().upper()
    if fid not in ref_counts:           # not in the export -> can't tell, keep it
        return False
    return ref_counts[fid] <= 0


# ── quest name by EDID prefix ────────────────────────────────────────────────
_QPREFIX_CACHE = {}
# Quest-ID shaped leading token: FF12, MQ04, E01, D03a, W05, BS02, TW01, MTNM03 …
_QUEST_ID_TOKEN_RE = re.compile(
    r"^(?:ff|mq|sq|e0|d0|w0|bs|bo|sr|tw|cb|v9|p|mtn[mr]?|mtr|mile|nwot|sdow)\d+[a-z]?$",
    re.I,
)


def load_quest_prefix_index(tsv_root=None):
    """{leading EDID token -> (quest FULL name, Quest Type)} from the QUEST export.

    Many reward lists carry their quest's ID as the first EDID token but are never
    wired to a GMRW, so the registry misses them and prettify_event_name() emits
    something useless: FF12_LL_QuestReward_Event_Basic became "Quest Reward Event
    Basic", when FF12 is FF12_Bell — "The Bell Tolls". Only tokens that look like a
    quest ID are indexed, and only when exactly ONE named quest claims the token,
    so an ambiguous prefix resolves to nothing rather than to the wrong quest.
    """
    if tsv_root is None:
        here = os.path.dirname(os.path.abspath(__file__))
        tsv_root = os.path.join(os.path.dirname(os.path.dirname(here)), "tsv")
    if tsv_root in _QPREFIX_CACHE:
        return _QPREFIX_CACHE[tsv_root]

    by_token = {}
    qpath = _newest(["QUEST_Export_*.tsv"], tsv_root)
    if qpath:
        with open(qpath, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                edid = (row.get("EDID") or "").strip()
                name = (row.get("FULL - Name") or "").strip()
                if not (edid and name) or "_" not in edid:
                    continue
                token = edid.split("_", 1)[0]
                if not _QUEST_ID_TOKEN_RE.match(token):
                    continue
                by_token.setdefault(token.upper(), set()).add(
                    (name, (row.get("Quest Type") or "").strip()))
    # Ambiguous tokens (two differently-named quests) resolve to nothing.
    index = {t: next(iter(v)) for t, v in by_token.items() if len(v) == 1}
    _QPREFIX_CACHE[tsv_root] = index
    return index


def quest_by_prefix(edid, index):
    """(name, type) for a reward list whose EDID leads with a quest ID, else None."""
    if not (edid and index and "_" in edid):
        return None
    return index.get(edid.split("_", 1)[0].upper())


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
    ref_counts = load_ref_counts(tsv_root)
    qprefix = load_quest_prefix_index(tsv_root)

    cand = {}
    for lv in closure:
        ed = parent_edid.get(lv, "") or ""
        if EXCLUDE_RE.search(ed):
            continue
        in_registry = lv in registry
        if not (in_registry or (ed and INCLUDE_RE.search(ed))):
            continue
        cand[lv] = (ed, in_registry)

    candset = set(cand)
    roots = candset
    if c2p:
        roots = {lv for lv in candset if not (_ancestors(lv, c2p) & (candset - {lv}))}
        # Vendor stock, not an event payout — the Vendors expand names the merchant.
        roots = {lv for lv in roots
                 if not any(VENDOR_ANCESTOR_RE.search(parent_edid.get(a, "") or "")
                            for a in _ancestors(lv, c2p))}

    # A ROOT with no owning event at all — nothing references it and no quest hands
    # it out. Filtered here rather than during candidate selection on purpose: drop
    # it earlier and its children stop being collapsed, so the equally nameless
    # sub-pool is promoted in its place (RA_LL_Rewards_General_AidItems giving way
    # to RA_LL_Rewards_General_AidItems_Rare). Removing the root drops the branch.
    roots = {lv for lv in roots
             if not is_orphan_pool(lv, cand[lv][0], ref_counts, cand[lv][1])}

    out = []
    for lv in roots:
        ed, in_registry = cand[lv]
        if in_registry:
            qname, qtype = registry[lv]
            name = qname or prettify_event_name(ed)
            typ = qtype or _event_type(ed)
        else:
            # Not wired to a GMRW, but the EDID may still lead with its quest's ID.
            hit = quest_by_prefix(ed, qprefix)
            if hit:
                qname, qtype = hit
                name = qname
                # "Miscellaneous" is the export's catch-all and reads as noise in
                # the Type column — fall back to the EDID's own signal instead.
                typ = qtype if qtype and qtype.lower() != "miscellaneous" else _event_type(ed)
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
        if not isinstance(row, dict):
            continue
        # Hand-authored rows (e.g. a quest dialogue hand-in) have no LVLI reward
        # edge, so rng76 can't resolve them. Keep them verbatim — never rng76-drop
        # or overwrite their rate — and let them sort last (rate_value None -> 0).
        if row.get("manual"):
            row.setdefault("rate_lines",
                           [row["rate_display"]] if row.get("rate_display") else [])
            row.setdefault("rate_value", None)
            kept.append(row)
            continue
        if not row.get("list_id"):
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
