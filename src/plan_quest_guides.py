#!/usr/bin/env python3
r"""
plan_quest_guides.py — "how to do this quest" under every quest route.

WHY
===
A quest reward is not a loot drop. The plan pages printed

    Quests   The Slasher: Blood Will Have Blood (Head Hunts)   100%

and nothing else, so a reader could not tell that the plan is a guaranteed
quest reward, what starts the quest, or what to do in it (Duchess, 22 Oct
2026: "if it's not a simple drop, we need to explain how to obtain it").

Everything here comes from the game files — nothing is written by hand:

  QUST2_Export_<M>_<Y>_RewardQuests.tsv   (ExportQUST2ToTSV.pas, every quest
                                          with a reward stage)
      Objective   the in-game objective text            -> the steps
      Log         the in-game quest log text            -> "only available ..."
      StageNote   Bethesda's stage comment (SCFC)       -> what starts the quest
      Reward      the GMRW a stage pays out             -> route -> quest join
  QUEST_Export_*.tsv                                    -> prerequisite names
  GMRW_Export_*.tsv                                     -> reward list -> GMRW

OUTPUT
======
  plan_master["quest_guides"] = {
      "008F15C3": {"name": "The Slasher: Blood Will Have Blood",
                   "requires": ["The Slasher: Masked Truth",
                                "The Slasher: Secrets to the Grave",
                                "The Slasher: Out of the Shadows"],
                   "availability": "Only available for the duration of The
                                    Slasher Seasonal Content",
                   "steps": ["Listen to \"A Warning\" broadcast", ...]}}
  route["quest_guide"] = "008F15C3"          (on routes with kind == "quest")

One copy per quest at the top level, routes point at it: 10,000 quest routes
each carrying their own copy would add megabytes to a file the browser
downloads. The renderer (df-bnb-plan-checklists.js, obtQuestGuideNode) draws
the guide under the route's rate line.

RULES
=====
* Steps = objectives in index order. "(Hint) ...", "(Optional) ...",
  "Optional: ..." and divider lines ("---FISHING OBJECTIVES--") are left out,
  duplicates dropped. Over MAX_STEPS: the first MAX_STEPS plus "more_steps".
* Counters: "<Variable=X>/5" -> "(5)"; a counter with no fixed total is dropped.
  Runtime-filled names ("Kill one <Alias=CreatureName02>") become "the marked
  target" / "the marked location" (alias name says Loc/Location/Dungeon).
* Requires: the stage comment's "started by completing <EDID>" / "has completed
  <EDID>", walked back up the chain (oldest first), named from the QUEST export.
  Comments that name a script, trigger or poster are not followed — only a
  completed quest is a step the reader can take.
* Availability: the "(This quest is only available ...)" note Bethesda puts on
  the quest log.
* A route joins a quest by name first ("... - shared rewards" and a trailing
  "(...)" ignored), then by its reward list -> GMRW -> the quest whose stage
  pays it, when exactly one quest does.
* No export (an old tsv folder) = no guides, never an error.

Runnable on a finished plan_master.json (seconds, no rates):

    python3 src/plan_quest_guides.py dist/plan_master.json --tsv-dir tsv
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

csv.field_size_limit(1 << 30)

SCHEMA = {
    "quest_guides": "quest FormID -> {name, requires[], availability, steps[]} "
                    "(src/plan_quest_guides.py)",
    "route.quest_guide": "key into quest_guides, on quest routes",
}

MAX_STEPS = 12
MAX_CHAIN = 8

_RX_TOKEN_NAME = re.compile(r"<[^<>]*>")
_RX_VAR_TOTAL = re.compile(r"\s*\(?<Variable=[^>]+>\s*/\s*(\d+)\)?", re.I)
_RX_VAR_PAIR = re.compile(r"\s*\(?<Variable=[^>]+>\s*/\s*<Variable=[^>]+>\)?", re.I)
_RX_VAR = re.compile(r"\s*<Variable=[^>]+>", re.I)
_RX_ALIAS = re.compile(r"<Alias(?:\.[A-Za-z]+)?=([^>]+)>", re.I)
_RX_STARTED = re.compile(
    r"(?:started by completing|has completed|after completing|upon completing)\s+([A-Za-z0-9_]+)",
    re.I)
_RX_AVAIL = re.compile(r"\(\s*This quest is (only available[^()]*)\)", re.I)
# Not a step the reader must do: hints, optional extras, and Bethesda's own
# divider lines inside the objective list ("---FISHING OBJECTIVES--").
_RX_SKIP = re.compile(r"^(?:\(\s*(?:hint|optional)\s*\)|(?:hint|optional)\s*:|[-=*_]{2,}.*[-=*_]{2,}$)", re.I)
_RX_HEX = re.compile(r"^(?:[0-9A-Fa-f]{2} )+[0-9A-Fa-f]{2}$")


def _newest(root, pat):
    return tsv_source.newest(os.path.join(root, pat), required=False)


def _rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)


def _split_ref(ref):
    bits = (ref or "").strip().split(":")
    return bits[0].strip().upper() if bits else ""


def _text(s):
    """A cell as plain text. FO76Edit shows an undefined subrecord as hex."""
    s = (s or "").strip()
    if _RX_HEX.match(s):
        try:
            s = bytes.fromhex(s.replace(" ", "")).rstrip(b"\0").decode("utf-8", "replace")
        except ValueError:
            return ""
    return " ".join(s.split())


def clean_name(name):
    """Quest title without runtime tokens ("Vault Mission: Washout<Alias=...>")."""
    return " ".join(_RX_TOKEN_NAME.sub("", name or "").split()).strip(" -:")


def _alias_word(m):
    a = m.group(1).lower()
    return "the marked location" if re.search(r"loc|dungeon|area|place", a) else "the marked target"


def clean_step(text):
    t = _text(text)
    t = _RX_VAR_PAIR.sub("", t)
    t = _RX_VAR_TOTAL.sub(lambda m: f" ({m.group(1)})", t)
    t = _RX_VAR.sub("", t)
    t = _RX_ALIAS.sub(_alias_word, t)
    t = _RX_TOKEN_NAME.sub("", t)
    return " ".join(t.split()).strip()


def route_key(name):
    n = (name or "").strip()
    n = re.sub(r"\s+-\s+shared(?: activity)? rewards$", "", n, flags=re.I)
    n = re.sub(r"\s*\([^()]*\)$", "", n)
    return clean_name(n).lower()


class Guides:
    def __init__(self, tsv_dir):
        self.ok = False
        self.quests = {}                       # fid -> dict(edid,name,obj,logs,notes,rewards)
        self.edid_to_fid = {}
        self.full_by_edid = {}
        self.gmrw_by_list = collections.defaultdict(set)    # LVLI -> {GMRW}
        self.quest_by_gmrw = collections.defaultdict(set)   # GMRW -> {quest fid}
        self._load(tsv_dir)

    def _load(self, root):
        p = _newest(root, "QUST2_Export_*_RewardQuests.tsv")
        if not p:
            return
        for r in _rows(p):
            fid = (r.get("QuestFormID") or "").strip().upper()
            if not fid:
                continue
            q = self.quests.setdefault(fid, {
                "edid": (r.get("QuestEDID") or "").strip(),
                "name": clean_name(r.get("QuestName")),
                "obj": [], "logs": [], "notes": [], "rewards": set()})
            kind = (r.get("RowKind") or "").strip()
            idx = (r.get("Index") or "").strip()
            try:
                n = int(re.match(r"\d+", idx).group(0)) if re.match(r"\d+", idx) else 0
            except (TypeError, ValueError):
                n = 0
            if kind == "Objective":
                q["obj"].append((n, r.get("Text") or ""))
            elif kind == "Log":
                q["logs"].append((n, _text(r.get("Text"))))
            elif kind == "StageNote":
                q["notes"].append((n, _text(r.get("Text"))))
            elif kind == "Reward":
                g = _split_ref(r.get("Link"))
                if g:
                    q["rewards"].add(g)
                    self.quest_by_gmrw[g].add(fid)
        for fid, q in self.quests.items():
            if q["edid"]:
                self.edid_to_fid[q["edid"]] = fid
        p = _newest(root, "QUEST_Export_*.tsv")
        if p:
            for r in _rows(p):
                e = (r.get("EDID") or "").strip()
                if e:
                    self.full_by_edid[e] = clean_name(r.get("FULL - Name"))
        p = _newest(root, "GMRW_Export_*.tsv")
        if p:
            for r in _rows(p):
                g = (r.get("FormID") or "").strip().upper()
                lst = _split_ref(r.get("RewardedItem"))
                if g and lst:
                    self.gmrw_by_list[lst].add(g)
        self.ok = bool(self.quests)
        self.by_name = collections.defaultdict(set)
        for fid, q in self.quests.items():
            if q["name"]:
                self.by_name[q["name"].lower()].add(fid)

    # ------------------------------------------------------------------ parts
    def prereq_edid(self, fid):
        q = self.quests.get(fid)
        if not q:
            return None
        for _, note in sorted(q["notes"]):
            m = _RX_STARTED.search(note)
            if m:
                return m.group(1)
        return None

    def requires(self, fid):
        chain, seen, cur = [], {fid}, fid
        e = self.prereq_edid(cur)
        while e and len(chain) < MAX_CHAIN:
            name = self.full_by_edid.get(e) or (self.quests.get(self.edid_to_fid.get(e, ""), {}) or {}).get("name")
            if not name:
                break
            chain.append(name)
            nxt = self.edid_to_fid.get(e)
            if not nxt or nxt in seen:
                break
            seen.add(nxt)
            e = self.prereq_edid(nxt)
        return list(reversed(chain))

    def availability(self, fid):
        for _, log in sorted(self.quests[fid]["logs"]):
            m = _RX_AVAIL.search(log)
            if m:
                s = " ".join(m.group(1).split())
                return s[0].upper() + s[1:]
        return None

    def steps(self, fid):
        out, seen = [], set()
        for _, raw in sorted(self.quests[fid]["obj"], key=lambda x: x[0]):
            t = clean_step(raw)
            if not t or _RX_SKIP.match(t):
                continue
            if t.lower() in seen:
                continue
            seen.add(t.lower())
            out.append(t)
        return out

    def guide(self, fid):
        q = self.quests.get(fid)
        if not q or not q["name"]:
            return None
        steps = self.steps(fid)
        if not steps:
            return None
        g = {"name": q["name"], "steps": steps[:MAX_STEPS]}
        if len(steps) > MAX_STEPS:
            g["more_steps"] = len(steps) - MAX_STEPS
        req = self.requires(fid)
        if req:
            g["requires"] = req
        av = self.availability(fid)
        if av:
            g["availability"] = av
        return g

    # ------------------------------------------------------------------ joins
    def quest_for_route(self, r):
        hits = self.by_name.get(route_key(r.get("route")), set())
        if len(hits) == 1:
            return next(iter(hits))
        cands = set()
        for L in (r.get("lvli") or []):
            for g in self.gmrw_by_list.get(str(L).upper(), ()):
                cands |= self.quest_by_gmrw.get(g, set())
        if hits:
            cands &= hits
        return next(iter(cands)) if len(cands) == 1 else None


def attach(items, tsv_dir, out=None):
    """Adds route["quest_guide"]; returns {fid: guide} (also put in out)."""
    g = Guides(tsv_dir)
    stats = {"export": g.ok, "routes": 0, "matched": 0, "quests": 0}
    guides = {}
    if not g.ok:
        if out is not None:
            out["quest_guides"] = {}
        return stats
    cache = {}
    for it in items:
        for key in ("obtain_routes", "retired_routes"):
            for r in it.get(key) or []:
                r.pop("quest_guide", None)
                if r.get("kind") != "quest":
                    continue
                stats["routes"] += 1
                fid = g.quest_for_route(r)
                if not fid:
                    continue
                if fid not in cache:
                    cache[fid] = g.guide(fid)
                if cache[fid]:
                    r["quest_guide"] = fid
                    guides[fid] = cache[fid]
                    stats["matched"] += 1
    stats["quests"] = len(guides)
    if out is not None:
        out["quest_guides"] = dict(sorted(guides.items()))
        out["quest_guides_schema"] = SCHEMA
    return stats


def subset(out, master):
    """Copies the quest guides a derived page (New Plans, Underarmour) points at
    into its own JSON - those pages copy plan_master rows verbatim, routes and
    their quest_guide keys included, but not plan_master's top level."""
    guides = (master or {}).get("quest_guides") or {}
    keys = set()

    def walk(o):
        if isinstance(o, dict):
            k = o.get("quest_guide")
            if isinstance(k, str):
                keys.add(k)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(out)
    out["quest_guides"] = {k: guides[k] for k in sorted(keys) if k in guides}
    return len(out["quest_guides"])


def report(stats):
    if not stats.get("export"):
        print("  quest guides: no QUST2 _RewardQuests export in this tsv folder - skipped "
              "(run ExportQUST2ToTSV.pas)")
        return
    print(f"  quest guides: {stats['matched']}/{stats['routes']} quest routes explained, "
          f"{stats['quests']} quests")


def enrich(path, tsv_dir):
    with open(path, encoding="utf-8") as f:
        master = json.load(f)
    stats = attach(master.get("items") or [], tsv_dir, master)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    a = ap.parse_args()
    for p in a.paths:
        st = enrich(p, a.tsv_dir)
        print(f"[plan_quest_guides] {p}:")
        report(st)


if __name__ == "__main__":
    main()
