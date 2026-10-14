#!/usr/bin/env python3
r"""
plan_activity_regions.py — an activity / event only gives the region-locked
plans of the region it is played in.

WHY
---
Shared reward pools pick a regional list with location conditions, e.g. the
Battle Bots shared activity pool (0086A8C0) holds branches gated by

    Subject.GetInCurrentLocation(RegionMountainLocation) == 1   ...

Every activity that rolls the pool was listed on every plan in it, with the
pool's region gate as its Drop Condition. So a Forest-only activity
(Collision Course, Death Blossoms, Fertile Soil) showed up on a plan whose
only way in reads "Only in Cranberry Bog, The Mire or Ash Heap" — a source
that can never meet its own condition. Same problem plan_vendor_regions.py
solves for vendors; this is the activity / event half.

WHERE AN ACTIVITY IS PLAYED (every build, nothing per-activity is hardcoded)
---------------------------------------------------------------------------
The route label is the quest's own name ("Activity: Collision Course"), so the
quests are found by FULL in the QUEST export. Their places, game files first:

  1. QUEST LNAM (the quest's location) and every LCTN whose Refs export lists
     the quest. Each walked up PNAM_ParentLocation (plan_vendor_regions.Locations).
  2. When the game files give no place (most activities pick theirs through a
     Find-Matching alias the exports can't see — Fly Swatter, Death Blossoms):
     the region / location table the activity and event pages already show
     (tsv/events_region_location.tsv — "Region: Ash Heap" on Fly Swatter).
     Location names are matched to LCTN FULL inside that region; the region
     itself always counts.

An activity with no place from either is UNKNOWN and always kept (reported).

WHAT IT DOES TO A ROW
---------------------
Only event / activity / daily quest routes are judged. For every path from the
route's lists down to the plan, the location test on that path is read the
same way plan_vendor_regions reads it (GetInCurrentLocation / GetIsEditorLocation
on an LCTN; a group mixing location with anything else is not judged).

  * Region grab bags: QuestReward_LLS_AllRegions_GrabBag holds one bag per
    region (QuestReward_LLS_Mire_GrabBag ...) with NO written condition — the
    game picks the bag of the region you are in (the activity pages resolve it
    the same way). A path that steps from an "AllRegions" list into a list
    named after a region is read as "Only in <that region>". Region names come
    from the Region*Location LCTN records, not a list in this file.
  * a path with no location test         -> the route can always give the
                                            plan: kept as it is
  * no path passes for any of its places -> the route moves to
                                            `retired_routes`, retired_because
                                            ["activity is not played in that region"]
  * some paths pass                      -> kept; its Drop Conditions are
                                            re-read from the passing paths only,
                                            so "Only in ..." names the regions
                                            this activity can actually meet

No rate is computed or changed (plan-obtain-pipeline: this pipeline owns no
rate maths). Runs in build_plan_obtain_json.py after plan_vendor_regions.
Standalone over a finished file:

    python src/plan_activity_regions.py dist/plan_master.json --data-dir tsv --diff
    python src/plan_activity_regions.py dist/plan_master.json --data-dir tsv --write
"""
import argparse
import collections
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import plan_conditions            # noqa: E402
import plan_sources               # noqa: E402
import plan_vendor_regions as PV  # noqa: E402  (Locations, _path_rule, _passes)

csv.field_size_limit(1 << 30)

DROP_REASON = "activity is not played in that region"
REGION_TABLE = "events_region_location.tsv"
# Route kinds that are a quest the player is standing in when it pays out.
_QUEST_TYPES = {"event-quest"}


def _u(s):
    return (s or "").strip().upper()


def _key(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


class ActivityPlaces:
    """Route label -> list of LCTN chains (each chain = location + its parents)."""

    def __init__(self, tsv_dir):
        self.loc = PV.Locations(tsv_dir)
        self.by_name = collections.defaultdict(set)      # key(FULL) -> quest fids
        self.lnam = {}
        p = PV._newest("QUEST_Export_*.tsv", tsv_dir)
        if p:
            for r in _rows(p):
                fid = _u(r.get("FormID"))
                full = (r.get("FULL - Name") or "").strip()
                if fid and full:
                    self.by_name[_key(full)].add(fid)
                m = re.search(r"\[LCTN:([0-9A-F]{8})\]", r.get("LNAM - Location") or "", re.I)
                if m:
                    self.lnam[fid] = _u(m.group(1))
        self.refs = collections.defaultdict(set)         # quest fid -> LCTN fids
        p = PV._newest("LCTN_Export_*_Refs.tsv", tsv_dir)
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as fh:
                rd = csv.reader(fh, delimiter="\t")
                next(rd, None)
                for row in rd:
                    if not row:
                        continue
                    for cell in row[3:]:
                        parts = cell.split(":")
                        if len(parts) == 3 and parts[2] == "QUST":
                            self.refs[_u(parts[0])].add(_u(row[0]))
        # Page table: key(name) -> [(region, location)]
        self.table = collections.defaultdict(list)
        p = os.path.join(tsv_dir, REGION_TABLE)
        if not os.path.exists(p):
            p = os.path.join(os.path.dirname(HERE), "tsv", REGION_TABLE)
        if os.path.exists(p):
            for r in _rows(p):
                name = (r.get("Activity / Event Name") or "").strip()
                if name:
                    self.table[_key(name)].append(((r.get("Region") or "").strip(),
                                                   (r.get("Location / LCTN") or "").strip()))
        self.lctn_by_name = collections.defaultdict(set)
        for f, full in self.loc.full.items():
            if full:
                self.lctn_by_name[_key(full)].add(f)
        self._cache = {}

    def places(self, label):
        """(chains, via) — via is 'game files', 'page table' or None (unknown)."""
        k = _key(label)
        if k in self._cache:
            return self._cache[k]
        lctns = set()
        for q in self.by_name.get(k, ()):
            if q in self.lnam:
                lctns.add(self.lnam[q])
            lctns |= self.refs.get(q, set())
        # Only locations that sit inside a region say where the activity is.
        chains = [self.loc.chain(f) for f in sorted(lctns) if self.loc.region_of(f)]
        via = "game files" if chains else None
        if not chains:
            for region, place in self.table.get(k, ()):
                rf = self.loc.region_by_key.get(PV._region_key(region))
                if not rf:
                    continue
                hit = [f for f in self.lctn_by_name.get(_key(place), ()) if self.loc.region_of(f) == rf]
                chains += [self.loc.chain(f) for f in hit] or [self.loc.chain(rf)]
            via = "page table" if chains else None
        self._cache[k] = (chains, via)
        return chains, via


class Judge:
    def __init__(self, tsv_dir):
        self.places = ActivityPlaces(tsv_dir)
        self.idx = plan_conditions.ConditionIndex(tsv_dir, lambda pat, root: PV._newest(pat, root))
        self.unknown = collections.Counter()
        loc = self.places.loc
        # 'mire' / 'ashheap' / 'skylinevalley' ... -> Region*Location FormID
        self.region_keys = {_key(PV._region_key(loc.full.get(f))): f for f in loc.regions if loc.full.get(f)}
        self.lvli_edid = {}
        p = PV._newest("LVLI_Export_*_LVLI_List.tsv", tsv_dir)
        if p:
            for r in _rows(p):
                f = _u(r.get("LVLI_FormID"))
                if f:
                    self.lvli_edid[f] = r.get("LVLI_EDID") or ""

    def _bag_gates(self, path):
        """[(region LCTN fid)] for each AllRegions -> <Region> bag step on the path."""
        out = []
        for holder, e in path:
            if not re.search(r"allregions", self.lvli_edid.get(_u(holder), ""), re.I):
                continue
            child = _key(self.lvli_edid.get(_u(e.get("ref")), ""))
            hits = [f for k, f in self.region_keys.items() if k and k in child]
            if len(hits) == 1:
                out.append(hits[0])
        return out

    def _rule(self, path):
        rule = PV._path_rule(self.idx, path)
        return rule + [[(f, True)] for f in self._bag_gates(path)]

    def _lines_for(self, paths, own, above_by_list):
        """plan_conditions.for_lists, but over the given paths only."""
        out, seen = [], set()
        for lid, path in paths:
            above = list(above_by_list.get(lid, []))
            for f in self._bag_gates(path):
                above.append("Only in " + self.places.loc.name(f))
            for holder, e in path:
                lines, _miss = self.idx._entry_lines(holder, e, own)
                lines = above + lines
                above = []
                for ln in lines:
                    if ln.lower() not in seen:
                        seen.add(ln.lower())
                        out.append(ln)
        return plan_conditions.tidy(out)[:plan_conditions.MAX_SENTENCES]

    def apply(self, it):
        changes = []
        book = _u((it.get("plan_item") or {}).get("formid"))
        if not book:
            return changes
        own = {"recipe": (it.get("cobj") or {}).get("formid") or "", "book": book,
               "title": it.get("name") or ""}
        keep_routes, gone, touched = [], [], False
        for r in it.get("obtain_routes") or []:
            if r.get("source_type") not in _QUEST_TYPES or not r.get("lvli"):
                keep_routes.append(r)
                continue
            paths = [(_u(l), p) for l in r["lvli"]
                     for p in self.idx._paths(_u(l), book, 0, frozenset())]
            rules = [(lid, p, self._rule(p)) for lid, p in paths]
            if not rules or any(not ru for _l, _p, ru in rules):
                keep_routes.append(r)          # some way in has no location test
                continue
            chains, via = self.places.places(r.get("route"))
            if not chains:
                self.unknown[r.get("route")] += 1
                keep_routes.append(r)
                continue
            ok = [(lid, p) for lid, p, ru in rules if any(PV._passes(ru, ch) for ch in chains)]
            regions = sorted({self.places.loc.name(self.places.loc.region_of(ch[0])) for ch in chains})
            if not ok:
                gone.append(dict(r, retired_because=[DROP_REASON],
                                 region_detail={"played_in": regions, "via": via,
                                                "conditions": r.get("conditions") or []}))
                changes.append({"plan": it.get("name"), "route": r.get("route"), "after": None,
                                "played_in": regions, "via": via, "conditions": r.get("conditions")})
                touched = True
                continue
            # Re-read the Drop Conditions from the ways in this activity can meet
            # (plus the region a grab bag stands for, which no condition says).
            if len(ok) < len(rules) or any(self._bag_gates(p) for _l, p in ok):
                above = {lid: self.idx._ancestor_lines(lid, own) for lid, _p in ok}
                lines = self._lines_for(ok, own, above)
                if lines != (r.get("conditions") or []):
                    changes.append({"plan": it.get("name"), "route": r.get("route"),
                                    "after": lines, "before": r.get("conditions"),
                                    "played_in": regions, "via": via})
                    r = dict(r, conditions=lines)
                    touched = True
            keep_routes.append(r)
        if touched:
            it["obtain_routes"] = keep_routes
            if gone:
                it["retired_routes"] = (it.get("retired_routes") or []) + gone
            it["obtain_ledger"] = plan_sources.obtain_ledger(it)
        return changes


def attach(items, tsv_dir, judge=None):
    j = judge or Judge(tsv_dir)
    stats = collections.Counter()
    log = []
    for it in items:
        ch = j.apply(it)
        if ch:
            stats["plans"] += 1
            stats["routes_retired"] += sum(1 for c in ch if c["after"] is None)
            stats["conditions_trimmed"] += sum(1 for c in ch if c["after"] is not None)
            log.extend(ch)
    stats["_log"] = log
    stats["_unknown"] = sorted(j.unknown)
    return stats


def report(stats, stream=None):
    stream = stream or sys.stdout
    log = stats.pop("_log", [])
    unknown = stats.pop("_unknown", [])
    print(f"  activity regions: {stats.get('routes_retired', 0)} route(s) retired, "
          f"{stats.get('conditions_trimmed', 0)} condition list(s) trimmed, "
          f"on {stats.get('plans', 0)} plan(s)", file=stream)
    if unknown:
        print(f"    no place found for {len(unknown)} quest(s) (kept): {', '.join(unknown[:12])}"
              + (" ..." if len(unknown) > 12 else ""), file=stream)
    for c in log[:6]:
        what = "(retired)" if c["after"] is None else "; ".join(c["after"])
        print(f"    e.g. {c['plan']}: {c['route']} [{', '.join(c['played_in'])}] -> {what}",
              file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("plan_master")
    ap.add_argument("--data-dir", default="tsv")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--diff", action="store_true")
    a = ap.parse_args(argv)
    d = json.load(open(a.plan_master, encoding="utf-8"))
    stats = attach(d["items"], a.data_dir)
    log = list(stats.get("_log", []))
    report(stats)
    if a.diff:
        for c in log:
            what = "RETIRED" if c["after"] is None else "conditions -> " + "; ".join(c["after"])
            print(f"{c['plan']} | {c['route']} | played in {', '.join(c['played_in'])} ({c['via']}) | {what}")
    if a.write:
        with open(a.plan_master, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
