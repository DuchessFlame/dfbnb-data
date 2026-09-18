#!/usr/bin/env python3
r"""
plan_source_pill.py — the short "where does this come from" tag on a plan row.

A plan's How to Obtain can run to twelve routes. The row itself has space for
one word, and the word a reader wants is the KIND of thing they have to go and
do: an op, an expedition, an event, a challenge. That is what this writes, as
`source_tag`, and it is what the export poster prints in its SOURCE column.

ONE TAG, THE BEST CHANCE. Duchess's call: the tag is the source most likely to
give the plan, not a list of everything that can. A vendor selling it outright
at 100% beats a 2% event drop, because that is where a reader should go first.
Ties are broken toward the thing you DO (Ops, Expos, Raids, Events...) over the
thing you buy from, since two sources at the same rate are equally likely and
the activity is the more useful half of the answer.

THE TAGS
--------
    Ops Expos Raids Events Activities   -- things you run
    CHAL Quest Workshop                 -- things you complete
    Gold Atom Vendor                    -- things you buy from
    Loot Enemy Fixed                    -- things you find

Short on purpose: they sit in a 20px-wide poster column and beside three other
pills on the row. "Ops" not "Daily Ops", "CHAL" not "Challenge", "Expos" not
"Expeditions" -- Duchess's wording.

The classification reads the ROUTE LABEL first and the route's `source_type`
second, because the label is what the builder already resolved to a real place
("Daily Ops - Chase", "Atlantic City Expedition - Mission") while source_type
only knows vendor / container / creature / fixed / event-quest, and
"event-quest" covers ops, expeditions, raids, events and daily activities all
at once.
"""

import re

# Order matters twice over: it is the tie-break when two routes share the best
# rate, and it is the order the label tests run in.
TAG_ORDER = ["Ops", "Expos", "Raids", "Events", "Activities",
             "CHAL", "Quest", "Workshop",
             "Gold", "Atom", "Vendor", "Loot", "Enemy", "Fixed"]
_RANK = {t: i for i, t in enumerate(TAG_ORDER)}

_LABEL_TESTS = [
    ("Ops",    re.compile(r"daily\s*ops", re.I)),
    ("Expos",  re.compile(r"expedition", re.I)),
    ("Raids",  re.compile(r"\braid(s|ing)?\b", re.I)),
    ("Events", re.compile(r"^event:|public\s*event|seasonal\s*event|meat\s*cook|"
                          r"mothman\s*equinox|fasnacht", re.I)),
    ("Atom",   re.compile(r"atom\s*shop", re.I)),
    ("Gold",   re.compile(r"gold\s*bullion", re.I)),
]

_BY_TYPE = {"vendor": "Vendor", "container": "Loot",
            "creature": "Enemy", "fixed": "Fixed",
            "event-quest": "Activities"}

# A guaranteed unlock is worth the same as a 100% route: you go and do the thing
# and the plan is yours.
_UNLOCK_TESTS = [
    ("CHAL",     re.compile(r"challenge", re.I)),
    ("Workshop", re.compile(r"workshop", re.I)),
    ("Atom",     re.compile(r"atom\s*shop", re.I)),
    ("Quest",    re.compile(r"quest|completing|reward for", re.I)),
]


def tag_for_route(route):
    """The tag one obtain_route belongs to."""
    label = str((route or {}).get("route") or "")
    for tag, rx in _LABEL_TESTS:
        if rx.search(label):
            return tag
    return _BY_TYPE.get(str((route or {}).get("source_type") or ""), "Activities")


def tag_for_unlock(sentence):
    text = str(sentence or "")
    for tag, rx in _UNLOCK_TESTS:
        if rx.search(text):
            return tag
    return None


def source_tag(item):
    """The one tag for a row, or None when nothing resolved."""
    best = None      # (rate, -rank, tag)
    for r in item.get("obtain_routes") or []:
        tag = tag_for_route(r)
        rate = r.get("rate")
        rate = float(rate) if isinstance(rate, (int, float)) else 0.0
        key = (rate, -_RANK.get(tag, 99))
        if best is None or key > best[0]:
            best = (key, tag)
    for u in item.get("obtain_unlocks") or []:
        tag = tag_for_unlock(u)
        if not tag:
            continue
        key = (1.0, -_RANK.get(tag, 99))   # guaranteed, so it ranks as 100%
        if best is None or key > best[0]:
            best = (key, tag)
    return best[1] if best else None


def attach(items, stats=None):
    """Write `source_tag` on every row. Idempotent."""
    stats = stats if stats is not None else {}
    counts = stats.setdefault("tags", {})
    stats.setdefault("rows", 0)
    stats.setdefault("untagged", 0)
    for it in items:
        tag = source_tag(it)
        stats["rows"] += 1
        if tag:
            it["source_tag"] = tag
            counts[tag] = counts.get(tag, 0) + 1
        else:
            it.pop("source_tag", None)
            stats["untagged"] += 1
    return stats


def report(stats, stream=None):
    import sys as _sys
    stream = stream or _sys.stdout
    tags = stats.get("tags") or {}
    order = sorted(tags.items(), key=lambda kv: -kv[1])
    print(f"  [source] {stats.get('rows',0)} rows, "
          f"{stats.get('untagged',0)} with no resolvable source", file=stream)
    print("    " + " · ".join(f"{t} {n}" for t, n in order), file=stream)
