"""
new_plans_sources.py — group the New Plans page by WHERE a plan comes from.

Duchess, 23 Sep 2026: the page reads better sorted by source (Daily Ops,
Minerva, Stamps, Quests, Events, Seasonal Events, Activities, Challenges ...)
than by item type (Apparel, CAMP ...). Each plan is listed ONCE, under its main
source; every other source still shows in its How to Obtain table. The item
type moves to the row pill (`type_label`), since the heading now says the source.

Main source = the same pick plan_source_pill.py makes for the row's one-word
pill: the route with the best resolved chance, where a non-loot unlock
(challenge reward, scrap to learn, quest reward) counts as 100%. Ties keep
the earlier route.

The buckets are decided from the resolved route label + source_type (the
strings the page already prints) and, for seasonal events, from the event
names in dist/seasonal_events/seasonal_events_rewards.json plus the names
their reward lists go by in the game files ("Scorched Spooky" is Halloween
Scorched). No per-plan list.

Order on the page = SOURCE_ORDER. Empty groups are not emitted. The renderer
holds no list of its own.
"""
import csv
import json
import os
import re

import tsv_source

SOURCE_ORDER = [
    ("daily-ops",   "Daily Ops"),
    ("minerva",     "Minerva"),
    ("stamps",      "Stamps"),
    ("quests",      "Quests"),
    ("events",      "Events"),
    ("seasonal",    "Seasonal Events"),
    ("activities",  "Activities"),
    ("maps",        "Maps"),
    ("infestations", "Infestations"),
    ("party-crashers", "Party Crashers"),
    ("bounty",      "Bounty Hunting"),
    ("mutated-packs", "Mutated Party Packs"),
    ("challenges",  "Challenges"),
    ("raids",       "Raids"),
    ("scoreboard",  "Scoreboard"),
    ("gold",        "Gold Bullion Vendors"),
    ("vendors",     "Vendors (Caps)"),
    ("enemies",     "Enemies"),
    ("scrap",       "Scrap to Learn"),
    ("atom",        "Atom Shop"),
    ("loot",        "World Loot"),
    ("other",       "No Source Found Yet"),
    ("cut",         "Cut Content"),
]
LABELS = dict(SOURCE_ORDER)

# Names the seasonal events' reward lists use in the game files that don't
# contain the event's page name. Extend here if a new event's lists read oddly.
SEASONAL_ALIASES = ["spooky", "halloween", "holiday", "festive", "fasnacht",
                    "invaders", "meat week", "mischief", "equinox",
                    "big bloom", "treasure hunter", "treasure hunt"]

# Own groups (Duchess, 23 Sep 2026), tested before the general ones:
#   Mutated Party Packs — every "Mutated Public Events - ..." list is the
#     contents of the two packs a Mutated Event hands out (Single / Double
#     Mutation, each with a Fallout 1st variant); the lists are opened by the
#     MutatedEvents_Package_* effects, not rolled by the event itself.
#   Bounty Hunting — Head Hunts and Grunt Hunts.
_RX_MUTATED = re.compile(r"mutated public events?|mutatedevents", re.I)
_RX_INFEST = re.compile(r"infestation", re.I)
_RX_CRASHER = re.compile(r"party crasher", re.I)
_RX_BOUNTY = re.compile(r"head hunt|grunt hunt|bounty", re.I)
# Maps (Duchess, 23 Sep 2026): treasure maps, grave digging and Lucky Strike maps.
_RX_MAPS = re.compile(r"treasure map|buried treasure|grave digging|lucky strike|dig site", re.I)
_ACTIVITY = re.compile(r"expedition|"
                       r"\bactivity\b|dig site|treasure map|caravan", re.I)
_RX_RAID = re.compile(r"\braids?\b", re.I)
_RX_QUEST = re.compile(r"\(quest\)|\bside quests?\b|\bquest reward|\bquests?\b", re.I)
_RX_EVENT = re.compile(r"^event:|public events?\b", re.I)


def _seasonal_rx(dist_dir):
    names = list(SEASONAL_ALIASES)
    path = os.path.join(dist_dir or "", "seasonal_events", "seasonal_events_rewards.json")
    try:
        with open(path, encoding="utf-8") as f:
            for e in json.load(f).get("events") or []:
                n = (e.get("name") or "").strip().lower()
                if n:
                    names.append(n)
    except (OSError, ValueError):
        pass
    return re.compile("|".join(re.escape(n) for n in sorted(set(names), key=len, reverse=True)), re.I)


def _public_event_rx(data_dir):
    """Names of every Public Event in the QUEST export ("Event: Dangerous
    Pastimes" -> "dangerous pastimes"), so a list labelled by region, like
    "Skyline Valley - Dangerous Pastimes", still files under Events."""
    names = set()
    path = tsv_source.newest(os.path.join(data_dir or "", "QUEST_Export_*.tsv"), required=False)
    if path:
        csv.field_size_limit(1 << 30)
        with open(path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                if (row.get("Quest Type") or "").strip().lower() != "public event":
                    continue
                n = re.sub(r"^event:\s*", "", (row.get("FULL - Name") or "").strip(), flags=re.I).lower()
                if len(n) >= 6 and "<" not in n and "[" not in n:
                    names.add(n)
    if not names:
        return None
    return re.compile(r"\b(" + "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True)) + r")\b", re.I)


def route_bucket(route, seasonal_rx, event_rx=None):
    label = route.get("route") or ""
    st = (route.get("source_type") or "").lower()
    low = label.lower()
    if "daily ops" in low:
        return "daily-ops"
    if "minerva" in low:
        return "minerva"
    if "stamp" in low:
        return "stamps"
    if "gold bullion" in low:
        return "gold"
    # Any other trader is a caps vendor, whatever place or content their label
    # names ("Expeditions - Giuseppe vendor" is a shop, not an expedition).
    if st == "vendor":
        return "vendors"
    if _RX_MUTATED.search(label):
        return "mutated-packs"
    if _RX_INFEST.search(label):
        return "infestations"
    if _RX_CRASHER.search(label):
        return "party-crashers"
    if _RX_BOUNTY.search(label):
        return "bounty"
    if _RX_MAPS.search(label):
        return "maps"
    if seasonal_rx.search(label):
        return "seasonal"
    if _RX_RAID.search(label):
        return "raids"
    # Events before activities: "Event: Seismic Activity" is an event whose
    # name happens to contain the word "activity".
    if _RX_EVENT.search(label) or (event_rx is not None and event_rx.search(label)):
        return "events"
    if _ACTIVITY.search(label):
        return "activities"
    if _RX_QUEST.search(label) or route.get("quest_reward"):
        return "quests"
    if st == "vendor":
        return "vendors"
    if st == "creature":
        return "enemies"
    return "loot"


def unlock_bucket(sentence):
    s = (sentence or "").lower()
    if s.startswith("scrap ") or "to learn this plan" in s:
        return "scrap"
    if "challenge" in s:
        return "challenges"
    if "stamp" in s:
        return "stamps"
    if "gold bullion" in s or "gold vendor" in s:
        return "gold"
    if "atom shop" in s:
        return "atom"
    if "season" in s or "scoreboard" in s:
        return "scoreboard"
    if "quest" in s:
        return "quests"
    return "other"


def main_source(item, seasonal_rx, event_rx=None):
    best = None      # (rate, -index, bucket)
    routes = item.get("obtain_routes") or []
    # A plan whose every route was retired as dead (prune_dead_routes.py) is
    # still filed where it is SUPPOSED to come from: Love Tap Paint under
    # Events (Tunnel of Love), Resolve Breaker Paint under Raids. It shows red
    # there, which is the point.
    if not routes and not (item.get("obtain_unlocks") or []):
        routes = item.get("retired_routes") or []
    for i, r in enumerate(routes):
        rate = r.get("rate")
        rate = float(rate) if isinstance(rate, (int, float)) else 0.0
        key = (rate, -i)
        if best is None or key > best[0]:
            best = (key, route_bucket(r, seasonal_rx, event_rx))
    for j, u in enumerate(item.get("obtain_unlocks") or []):
        key = (1.0, -(100 + j))           # a guaranteed unlock ranks as 100%
        if best is None or key > best[0]:
            best = (key, unlock_bucket(u))
    return best[1] if best else "other"


def _book_holders(data_dir, fids):
    """{BOOK FormID: [LVLI EditorIDs that hold it]} for the few rows with no
    resolved route at all (rng76 drops a route whose rate is 0%, so Resolve
    Breaker Paint arrives with nothing to file it by)."""
    out = {}
    if not fids:
        return out
    path = tsv_source.newest(os.path.join(data_dir or "", "BOOK_Export_*.tsv"), required=False)
    if not path:
        return out
    csv.field_size_limit(1 << 30)
    with open(path, encoding="utf-8", errors="replace") as f:
        r = csv.reader(f, delimiter="\t")
        head = next(r)
        ref_ix = [i for i, h in enumerate(head) if re.match(r"^Ref\d+$", h)]
        for row in r:
            fid = (row[0] if row else "").strip().upper()
            if fid not in fids:
                continue
            lists = []
            for i in ref_ix:
                if i < len(row) and row[i].endswith(":LVLI"):
                    lists.append(row[i].split(":")[1])
            out[fid] = lists
    return out


def all_sources(item, seasonal_rx, event_rx=None):
    """Every source bucket the plan has, best first (Duchess, 23 Sep 2026: a
    plan with several sources is listed under EACH of them). Falls back to the
    retired routes, like main_source, when nothing live is left."""
    routes = item.get("obtain_routes") or []
    unlocks = item.get("obtain_unlocks") or []
    if not routes and not unlocks:
        routes = item.get("retired_routes") or []
    scored = []
    for i, r in enumerate(routes):
        rate = r.get("rate")
        rate = float(rate) if isinstance(rate, (int, float)) else 0.0
        scored.append(((rate, -i), route_bucket(r, seasonal_rx, event_rx)))
    for j, u in enumerate(unlocks):
        scored.append(((1.0, -(100 + j)), unlock_bucket(u)))
    out = []
    for _, b in sorted(scored, key=lambda x: x[0], reverse=True):
        if b not in out:
            out.append(b)
    return out


def group_rows(rows, dist_dir, title_key, type_labels, data_dir=None):
    """rows -> ordered [{key,label,count,items}]. Sets `source_group` and
    `type_label` on each row (type from its old type group key)."""
    rx = _seasonal_rx(dist_dir)
    erx = _public_event_rx(data_dir)
    bare = {((r.get("plan_item") or {}).get("formid") or "").upper() for r in rows
            if not (r.get("obtain_routes") or r.get("obtain_unlocks") or r.get("retired_routes"))}
    holders = _book_holders(data_dir, bare - {""})
    try:
        import plan_sources
        _label = lambda e: plan_sources.source_label(e) or e
    except Exception:                                   # noqa: BLE001
        _label = lambda e: e
    for r in rows:
        if r.get("cut"):
            r["source_group"] = "cut"
        else:
            src = main_source(r, rx, erx)
            fid = ((r.get("plan_item") or {}).get("formid") or "").upper()
            if src == "other" and holders.get(fid):
                guess = main_source({"obtain_routes": [{"route": _label(e)} for e in holders[fid]]}, rx, erx)
                # only trust a guess that names something; an unrecognised list
                # name would otherwise fall through to World Loot
                if guess != "loot":
                    src = guess
            r["source_group"] = src
        # every bucket, main one first; the row is listed in each
        if r["source_group"] == "cut":
            r["source_groups"] = ["cut"]
        else:
            extra = [b for b in all_sources(r, rx, erx) if b != r["source_group"]]
            r["source_groups"] = [r["source_group"]] + extra
        t = r.get("type_group") or r.get("group")
        if t in type_labels:
            r["type_label"] = type_labels[t]
            r["type_group"] = t
        r["group"] = r["source_group"]
    groups = []
    for key, label in SOURCE_ORDER:
        members = sorted((r for r in rows if key in (r.get("source_groups") or [r["group"]])),
                         key=title_key)
        if members:
            groups.append({"key": key, "label": label, "count": len(members), "items": members})
    return groups
