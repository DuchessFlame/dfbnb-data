#!/usr/bin/env python3
r"""
plan_route_kinds.py — what KIND of place a resolved drop route is.

WHY
---
resolve_routes() files every quest AND event drop under one source_type,
"event-quest", and the How to Obtain ledger then printed all of them under
"Events & Activities". So A Grand Reopening, Custodial Compulsions, Hells Eagles
and Regent of the Dead — four Atlantic City QUESTS — sat in the events row on
every plan page (Duchess, 4 Oct 2026: "so wrong its not funny"). Creature drops
went the same way: a dead trick-or-treater at the Pumpkin House random
encounter read as an event called "Trick-or-Treater".

THE RULE — read off the records, never the label
-------------------------------------------------
event-quest  Walk UP the route's leveled lists (list -> parent list -> ...) to
             the GMRW reward records and QUST records that pay them out, and read
             each quest's own "Quest Type" in the QUEST export:
               Public Event / Event / Expedition / Raid / Daily Ops /
               Caravan / Server ...............................  event
               Primary / Secondary / Side Quest / Miscellaneous /
               Daily ..........................................  quest
             Any event in the chain wins (an event's reward pool is an event
             reward even if a quest shares it). Nothing reached -> left as an
             event, which is what it was before.
creature     The NPC holding the list. A Loot_Corpse NPC is a body lying in the
             world (the Pumpkin House trick-or-treaters), not something you
             fight -> "corpse", filed with world loot. Anything else -> "enemy".

CUT LISTS
---------
Double mutations are cut from the game — the Mutation Invasion rewards page
(build_mutated_events_json.py) already skips every double-gated pack. The plan
pipeline still printed "Mutated Public Events - Double Mutation" routes. A route
whose every leveled list is a double-mutation list is dropped here.

Used by build_plan_obtain_json.py on every build, and runnable on its own over a
finished plan_master.json (no rng76, no rate walk — seconds):

    python3 src/plan_route_kinds.py dist/plan_master.json
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import tsv_source            # noqa: E402
import plan_sources          # noqa: E402

csv.field_size_limit(1 << 30)

SCHEMA = 1

EVENT_TYPES = {"public event", "event", "expedition", "raid", "daily ops",
               "caravan", "server", "14"}
QUEST_TYPES = {"primary", "secondary", "side quest", "miscellaneous", "daily"}

# Leveled lists that are cut content even though nothing marks them zzz.
CUT_LIST_RX = re.compile(r"DoubleMutat", re.I)

_RX_CORPSE = re.compile(r"(?:^|_)Loot_?Corpse", re.I)
_RX_RE_CODE = re.compile(r"^(RE_[A-Za-z]+\d+)", re.I)


def _split_ref(ref):
    bits = (ref or "").strip().split(":")
    if len(bits) < 3:
        return (bits[0].strip().upper() if bits else "", "", "")
    return bits[0].strip().upper(), ":".join(bits[1:-1]).strip(), bits[-1].strip().upper()


def _newest(root, pat):
    return tsv_source.newest(os.path.join(root, pat), required=False)


class Ctx:
    """Everything classify() needs, read once per build."""

    def __init__(self, tsv_dir):
        self.lvli_edid = {}
        self.parents = collections.defaultdict(list)     # LVLI -> [(fid, edid, sig)]
        self.gmrw_quest = {}                              # GMRW -> quest EDID
        self.quest_type = {}                              # quest EDID -> type (lower)
        self.quest_full = {}                              # quest EDID -> FULL
        self.npc = {}                                     # NPC fid -> (edid, full)
        self._load(tsv_dir)

    def _load(self, root):
        p = _newest(root, "LVLI_Export_*_LVLI_List.tsv")
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    fid = (r.get("LVLI_FormID") or "").strip().upper()
                    if fid:
                        self.lvli_edid[fid] = (r.get("LVLI_EDID") or "").strip()
        p = _newest(root, "LVLI_Export_*_LVLI_Refs.tsv")
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                head = next(rd)
                ix = [i for i, h in enumerate(head) if re.match(r"^Ref_?\d+$", h)]
                for row in rd:
                    if not row:
                        continue
                    fid = row[0].strip().upper()
                    self.parents[fid] = [_split_ref(row[i]) for i in ix
                                         if i < len(row) and row[i].strip()]
        p = _newest(root, "GMRW_Export_*.tsv")
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    fid = (r.get("FormID") or "").strip().upper()
                    q = _split_ref(r.get("ParentQuestLink") or "")
                    if fid and q[1]:
                        self.gmrw_quest[fid] = q[1]
        p = _newest(root, "QUEST_Export_*.tsv")
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    e = (r.get("EDID") or "").strip()
                    if e:
                        self.quest_type[e] = (r.get("Quest Type") or "").strip().lower()
                        self.quest_full[e] = (r.get("FULL - Name") or "").strip()
        p = _newest(root, "NPC_Export_*.tsv")
        if p and not re.search(r"_(PRPS|Refs)\.tsv$", p):
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                next(rd, None)
                for row in rd:
                    if len(row) >= 3 and row[0].strip():
                        self.npc[row[0].strip().upper()] = (row[1].strip(), row[2].strip())

    # ------------------------------------------------------------------ quests
    def quest_types_for(self, lvli_fids, depth=5):
        """Quest Types of the NEAREST quests that pay these lists out.

        Level by level, stopping at the first level that reaches any quest: a
        quest's own reward list is often ALSO pulled into a bigger shared pool
        that some event rolls, and "any event anywhere up the chain" filed A
        Grand Reopening's own reward list as an event.
        """
        seen = set()
        frontier = [str(f).upper() for f in lvli_fids]
        for _ in range(depth):
            types = set()
            nxt = []
            for fid in frontier:
                if fid in seen:
                    continue
                seen.add(fid)
                for rf, redid, sig in self.parents.get(fid, ()):
                    if sig == "GMRW":
                        q = self.gmrw_quest.get(rf)
                        if q and q in self.quest_type:
                            types.add(self.quest_type[q])
                    elif sig == "QUST" and redid in self.quest_type:
                        types.add(self.quest_type[redid])
                    elif sig == "LVLI":
                        nxt.append(rf)
            if types:
                return types
            frontier = nxt
            if not frontier:
                break
        return set()

    def type_of_name(self, name):
        """Quest Type of a quest whose title IS this name, or None."""
        if not hasattr(self, "_by_full"):
            self._by_full = collections.defaultdict(set)
            for e, full in self.quest_full.items():
                if full:
                    self._by_full[full.lower()].add(self.quest_type.get(e, ""))
        ts = self._by_full.get((name or "").strip().lower())
        if ts and len(ts) == 1:
            return next(iter(ts))
        return None

    # ----------------------------------------------------------------- holders
    def npc_holders(self, lvli_fids):
        out = []
        for fid in lvli_fids:
            for rf, redid, sig in self.parents.get(str(fid).upper(), ()):
                if sig == "NPC_":
                    e, full = self.npc.get(rf, (redid, ""))
                    out.append((e or redid, full))
        return out


def is_cut_route(route, ctx):
    fids = route.get("lvli") or []
    eds = [ctx.lvli_edid.get(str(f).upper(), "") for f in fids]
    return bool(eds) and all(e and CUT_LIST_RX.search(e) for e in eds)


def corpse_label(edid, full, ctx):
    """'Random Encounter: Pumpkin House Revealed - Trick-or-Treater corpse'."""
    who = (full or "").strip() or None
    if who and re.search(r"\bcorpse\b", who, re.I):
        who = re.sub(r"\s*\bcorpse\b", "", who, flags=re.I).strip() or None
    m = _RX_RE_CODE.match(edid or "")
    where = None
    if m:
        q = m.group(1)
        name = ctx.quest_full.get(q, "")
        if name and plan_sources.usable_quest_name(name):
            where = f"Random Encounter: {name}"
    if where and who:
        return f"{where} - {who} corpse"
    if who:
        return f"{who} corpse"
    return where


def classify(route, ctx):
    """'quest' | 'event' | 'enemy' | 'corpse' | None (vendors, containers ...)."""
    st = (route.get("source_type") or "").lower()
    fids = route.get("lvli") or []
    if st == "creature":
        holders = ctx.npc_holders(fids)
        if holders and all(_RX_CORPSE.search(e or "") for e, _f in holders):
            return "corpse"
        return "enemy"
    if st == "event-quest":
        # The label IS a quest title ("A Grand Reopening", "Hells Eagles") —
        # the quest record says what it is. Labels listing several quests after
        # a region ("Atlantic City - Custodial Compulsions, Hells Eagles, ...")
        # count when every one of them is a quest.
        label = route.get("route") or ""
        head_t = ctx.type_of_name(label.split(" - ", 1)[0])
        if head_t in QUEST_TYPES:
            return "quest"            # "When the Rust Settles - Unique Items"
        if head_t in EVENT_TYPES:
            return "event"
        names = [label]
        if " - " in label:
            names = [n.strip() for n in label.split(" - ", 1)[1].split(",")]
        named = [ctx.type_of_name(n) for n in names]
        if named and all(t in QUEST_TYPES for t in named):
            return "quest"
        if named and all(t in EVENT_TYPES for t in named):
            return "event"
        types = ctx.quest_types_for(fids)
        if types & EVENT_TYPES:
            return "event"
        if types & QUEST_TYPES:
            return "quest"
        if route.get("quest_reward"):
            return "quest"
        return "event"
    return None


def apply_to_routes(routes, ctx, stats=None):
    """Drop cut routes, tag kinds, name corpse routes. Returns the new list."""
    stats = stats if stats is not None else collections.Counter()
    out = []
    for r in routes or []:
        if is_cut_route(r, ctx):
            stats["cut_routes_dropped"] += 1
            continue
        k = classify(r, ctx)
        if k:
            r["kind"] = k
            stats["kind_" + k] += 1
        else:
            r.pop("kind", None)
        if k == "corpse":
            for e, full in ctx.npc_holders(r.get("lvli") or []):
                lab = corpse_label(e, full, ctx)
                if lab:
                    r["route"] = lab
                    break
        out.append(r)
    return out


def apply(items, ctx):
    stats = collections.Counter()
    for it in items:
        if not it.get("obtain_routes"):
            continue
        before = len(it["obtain_routes"])
        it["obtain_routes"] = apply_to_routes(it["obtain_routes"], ctx, stats)
        if len(it["obtain_routes"]) != before:
            stats["items_lost_cut_routes"] += 1
        it["obtain_ledger"] = plan_sources.obtain_ledger(it)
    return dict(stats)


def enrich(path, tsv_dir):
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    ctx = Ctx(tsv_dir)
    stats = apply(doc.get("items") or [], ctx)
    doc["route_kinds_schema"] = SCHEMA
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    a = ap.parse_args()
    for p in a.paths:
        print(f"[plan_route_kinds] {p}: {enrich(p, a.tsv_dir)}")


if __name__ == "__main__":
    main()
