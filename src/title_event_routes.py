#!/usr/bin/env python3
"""
title_event_routes.py - which event reward hands out a title, under what
conditions, and at what chance. Read from the game files every build.

Added 13 Oct 2026 (Duchess: "Oh they were hard static? Then yes definitely
change that to read from the files and generative"). Project Paradise's four
titles were typed by hand in build_titles_json.py and two were wrong
(Researcher showed 10% when it is guaranteed; Zookeeper and Tamer ignored the
roll they share).

HOW IT WORKS
  1. Every GMRW reward row hands out one leveled list, and its own conditions
     say WHEN (Project Paradise: SFS09_Habitat_MainframePower = 1 and the
     quest's NumAlive = 3 gives SFS09_Habitat_LL_Quest_Reward_3).
  2. For each title BOOK, every reward row whose list reaches the BOOK is a
     route. The chance is rng76's appearance_prob() - the one drop-rate
     engine. Nothing here does rate maths except the reward row's own
     GetRandomPercent, which wraps the whole list.
  3. A title sharing a roll with other titles, each "until you learn it"
     (Zookeeper / Tamer), gets a second figure: its chance once you know the
     others. rng76 works it out on a copy of the lists with those entries
     taken out - the engine itself is not touched.
  4. Conditions in words, from the files:
       - a quest counter by the quest's own objective text
         ("Friendly Animals Alive: <Variable=NumAlive>/3" -> "... 3/3");
         with no objective to name it, "Reward tier 3"
       - a 0/1 switch global by its EditorID minus the quest code
         ("Mainframe Power: On"); a count global as a number
         ("Num Pyres Destroyed: 0"); seasonal Update01_Quest_<Event> flags
         through plan_conditions ("Only while the X event is running")
       - entry conditions on the way down to the title through
         plan_conditions ("Only after you learn Plan: ...")
  The GMRW export gained a ConditionsFull column on 13 Oct 2026 (the old
  Conditions column drops every parameter and sometimes the value). Until a
  new export lands, a global is found from the GLOB export's references to the
  reward record, and a counter from the quest's only "<Variable=X>/N"
  objective.

  precompute() builds everything once and frees the engine before the titles
  build loads its own data - together they do not fit in 4 GB.
"""
import copy
import csv
import os
import re
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import rng76                       # noqa: E402
import plan_conditions as _pc      # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

DEAD = re.compile(r"(^|_)(zzz|CUT|DONOTUSE|DEL)(_|$)|^zzz|^DEL_", re.I)
_OLD_COND = re.compile(r"^\s*(?P<fn>\w+)\s+(?P<bits>[01]{8})(?:\s+(?P<value>-?\d+\.\d+))?\s*$")
_COUNTISH = re.compile(r"(Num|Count|Ct\b|Tier|Index|Stage|Score|Amount)", re.I)


def _rows(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _newest(tsv_root, pattern, exclude=None):
    try:
        return rng76.newest(os.path.join(tsv_root, pattern), exclude)
    except FileNotFoundError:
        return None


def _q(v):
    return (v or "").strip().strip('"')


def _fid(v):
    m = re.match(r"\s*([0-9A-Fa-f]{8})", v or "")
    return m.group(1).upper() if m else ""


def _pct(x):
    return rng76.fmt_pct(x * 100.0)


_SWITCH_QUEST = re.compile(
    r"^\[?\s*(?:Shut\s*Down|Turn\s+(?:On|Off)|Power\s+(?:Up|Down)|Activate|Deactivate|Restore Power to)\s+(?P<what>[^\]]+?)\s*\]?$",
    re.I)


def _humanize(edid, quest_code=""):
    s = edid or ""
    if quest_code and s.lower().startswith(quest_code.lower() + "_"):
        s = s[len(quest_code) + 1:]
    s = re.sub(r"^[A-Z]{1,5}\d{2,3}[A-Za-z]?_", "", s)          # SFS09_
    s = re.sub(r"^Global_?", "", s)
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s.replace("_", " "))
    return re.sub(r"\s+", " ", s).strip()


def _num(v):
    return f"{v:g}"


def _op_phrase(op, v):
    """'3', '3 or more', ... for a counter compared with an operator."""
    if op == _pc.OP_EQ:
        return _num(v)
    if op == _pc.OP_GE:
        return f"{_num(v)} or more"
    if op == _pc.OP_GT:
        return f"more than {_num(v)}"
    if op == _pc.OP_LE:
        return f"{_num(v)} or less"
    if op == _pc.OP_LT:
        return f"less than {_num(v)}"
    if op == _pc.OP_NE:
        return f"not {_num(v)}"
    return _num(v)


def _event_label(name):
    """'Uplink: <Alias=Location>' -> 'Uplink'; bracketed writer notes -> ''."""
    name = _q(name)
    if "<" in name:
        name = name.split("<", 1)[0].rstrip(" :")
    if not name or "[" in name:
        return ""
    return name


class TitleEventRoutes:
    def __init__(self, tsv_root):
        # The engine without its name index (not needed for a chance; ~1 GB).
        d = rng76.Rng76Data()
        d.lvli.load_from_tsv_root(tsv_root)
        d.globs.load_from_tsv_root(tsv_root)
        d.curvs.load_from_tsv_root(tsv_root)
        d.resolver = rng76.Rng76Resolver(d.lvli, d.globs, d.names, d.curvs)
        d.lvli.entry_rows = d.lvli.math_rows = d.lvli.list_rows = []
        self.data = d
        self.res = d.resolver
        self.parents = defaultdict(set)
        for lid, entries in d.lvli.entries_by_list.items():
            for e in entries:
                ref = _fid(e.get("LVLO_Reference"))
                if ref:
                    self.parents[ref].add(lid)
        self._ancestors = {}

        # The same entry-condition reader the plan pages and uniques use.
        self.conds = _pc.ConditionIndex(tsv_root, lambda pat, d=None: _newest(tsv_root, pat))

        all_gmrw = _rows(_newest(tsv_root, "GMRW_Export_*.tsv"))
        self.payout_globs = defaultdict(set)
        for r in all_gmrw:
            for col in ("NAM7_XPGlobal", "NAM8_CapsGlobal"):
                g = _fid(r.get(col))
                if g:
                    self.payout_globs[_fid(r.get("FormID"))].add(g)
        self.gmrw_by_list = defaultdict(list)
        for r in all_gmrw:
            item = _q(r.get("RewardedItem"))
            if item.endswith(":LVLI") and not DEAD.search(_q(r.get("EDID"))) \
                    and not DEAD.search(item.split(":")[1] if ":" in item else ""):
                self.gmrw_by_list[_fid(item)].append(r)

        self.quest_full, self.quest_edid, self.switch_target = {}, {}, {}
        for r in _rows(_newest(tsv_root, "QUEST_Export_*.tsv")):
            f = _fid(r.get("FormID"))
            self.quest_full[f] = _q(r.get("FULL - Name"))
            self.quest_edid[f] = _q(r.get("EDID"))
            # "[Shut Down ARIC-4]" style misc quests: what a switch on the
            # event's own quest actually turns on/off, in player words.
            m = _SWITCH_QUEST.match(self.quest_full[f])
            if m:
                self.switch_target[self.quest_edid[f].lower()] = m.group("what").strip()
        self.objectives = defaultdict(list)
        for r in _rows(_newest(tsv_root, "QUST2_Export_*_Objectives.tsv")):
            text = _q(r.get("ObjectiveText"))
            qf = _fid(r.get("QuestFormID"))
            if "<Variable=" in text and text not in self.objectives[qf]:
                self.objectives[qf].append(text)

        # Globals, streamed (thousands of Ref columns); only :GMRW refs kept.
        self.glob_val, self.glob_edid, self.glob_by_ref = {}, {}, defaultdict(list)
        gp = _newest(tsv_root, "GLOB_Export_*.tsv")
        if gp:
            with open(gp, encoding="utf-8", errors="replace", newline="") as fh:
                rd = csv.reader(fh, delimiter="\t")
                next(rd, None)
                for vals in rd:
                    if len(vals) < 3:
                        continue
                    f = _fid(vals[0])
                    self.glob_edid[f] = _q(vals[1])
                    try:
                        self.glob_val[f] = float(_q(vals[2]))
                    except ValueError:
                        pass
                    for v in vals[4:]:
                        if v.endswith(":GMRW"):
                            self.glob_by_ref[_fid(v)].append(f)

        # Title recipes: the hidden *Title*_co_CondProxy_* recipe a title
        # checks -> the BOOK that teaches it, and that BOOK's display name.
        self.book_of_cobj, self.title_books, self.book_name = {}, set(), {}
        self.proxy_of_book = {}
        for r in _rows(_newest(tsv_root, "COBJ_Export_*.tsv", ["COBJ2"])):
            c, b = _fid(r.get("COBJ_FormID")), _fid(r.get("GNAM_FormID"))
            ed = _q(r.get("COBJ_EDID")).lower()
            if c and b:
                self.book_of_cobj[c] = b
                if "condproxy" in ed and "title" in ed:
                    self.title_books.add(b)
                    self.proxy_of_book[b] = c
        for r in _rows(_newest(tsv_root, "BOOK_Export_*.tsv", ["_Locations", "BOOK2"])):
            b = _fid(r.get("FormID"))
            full = _q(r.get("FULL"))
            if b in self.title_books and full:
                self.book_name[b] = re.sub(r"^[^:]*:\s*", "", full)

    # -- graph helpers -------------------------------------------------------
    def _lists_above(self, fid):
        if fid in self._ancestors:
            return self._ancestors[fid]
        seen, todo = set(), [fid]
        while todo:
            x = todo.pop()
            for p in self.parents.get(x, ()):
                if p not in seen:
                    seen.add(p)
                    todo.append(p)
        self._ancestors[fid] = seen
        return seen

    def _without(self, keep):
        """Resolver over a copy of the lists with every OTHER title's entry
        taken out - the player knows them, so their "until learned" gate
        removes them from the roll."""
        others = self.title_books - {keep}
        lv = copy.copy(self.data.lvli)
        lv.entries_by_list = {
            lid: [e for e in ents if _fid(e.get("LVLO_Reference")) not in others]
            for lid, ents in self.data.lvli.entries_by_list.items()}
        return rng76.Rng76Resolver(lv, self.data.globs, self.data.names, self.data.curvs)

    def _entry_lines(self, list_fid, book, name):
        """Conditions on the entries from the reward list down to the title
        (plan_conditions' reader, without the ancestor gates above the list -
        the reward row already says how the list is reached)."""
        # its own hidden recipe too, so "until you learn it" is not repeated
        own = {"book": book, "recipe": self.proxy_of_book.get(book, ""), "title": name}
        out = []
        try:
            for path in self.conds._paths(list_fid, book, 0, frozenset()):
                for holder, e in path:
                    lines, _miss = self.conds._entry_lines(holder, e, own)
                    for ln in lines:
                        if ln not in out and not ln.lower().startswith("stops dropping"):
                            out.append(ln)
        except Exception:
            pass
        return out

    # -- reward-row conditions in words ------------------------------------
    def _row_conditions(self, row):
        """[(fn, op, value, parsed-or-None)] from ConditionsFull, else the old
        Conditions column read strictly (an item with no value is skipped -
        the old export drops it on GetRandomPercent and others)."""
        full = [c for c in _q(row.get("ConditionsFull")).split("|") if c.strip()]
        out = []
        if full:
            for c in full:
                p = _pc.parse_condition(c)
                if p:
                    out.append((p["fn"], p["op"], p.get("number"), p))
            return out
        raw = (row.get("Conditions") or "").strip()
        for c in (next(csv.reader([raw]), []) if raw else []):
            m = _OLD_COND.match(c)
            if not m or m.group("value") is None:
                continue
            out.append((m.group("fn"), _pc._op(m.group("bits")), float(m.group("value")), None))
        return out

    def _cond_words(self, row):
        quest_fid = _fid(row.get("ParentQuestLink"))
        quest_code = self.quest_edid.get(quest_fid, "")
        words, mult = [], 1.0
        counters = [o for o in self.objectives.get(quest_fid, [])
                    if re.search(r"<Variable=\w+>/\d+", o)]
        for fn, op, value, p in self._row_conditions(row):
            if value is None:
                continue
            if fn == "GetRandomPercent":
                if op in (_pc.OP_LE, _pc.OP_LT):
                    mult *= max(0.0, min(1.0, value / 100.0))
                continue
            if fn == "GetPublicEventHasMutation":
                yes = (value != 0) == (op != _pc.OP_NE)
                words.append("Mutated event" if yes else "Not mutated")
                continue
            if fn == "GetGlobalValue":
                if p:
                    s = _pc.sentence(p)
                    if s:
                        words.append(s)
                        continue
                    gfid = ((p.get("refs") or [{}])[0].get("fid") or "").upper()
                else:
                    gm = _fid(row.get("FormID"))
                    cands = [g for g in self.glob_by_ref.get(gm, [])
                             if g not in self.payout_globs.get(gm, set())
                             and not re.match(r"^(XP|Caps)_", self.glob_edid.get(g, ""), re.I)]
                    gfid = cands[0] if len(cands) == 1 else ""
                edid = self.glob_edid.get(gfid, "")
                m = re.match(r"^Update01_Quest_(\w+)$", edid, re.I)
                if m:
                    yes = (value != 0) == (op != _pc.OP_NE)
                    ev = _humanize(m.group(1))
                    words.append(f"Only while the {ev} event is running" if yes
                                 else f"Only while the {ev} event is not running")
                    continue
                name = _humanize(edid, quest_code)
                if not name:
                    continue                      # unnamed switch: say nothing
                boolean = self.glob_val.get(gfid) in (0.0, 1.0) and value in (0.0, 1.0) \
                    and not _COUNTISH.search(edid) and op in (_pc.OP_EQ, _pc.OP_NE)
                if boolean:
                    on = (value == 1.0) == (op == _pc.OP_EQ)
                    # A switch named after machinery ("Mainframe Power") reads
                    # as the thing the event's own misc quest switches, when
                    # exactly one sibling quest names it ("ARIC-4 switch").
                    if quest_code and re.search(r"power|switch|mainframe|generator|reactor", edid, re.I):
                        pre = quest_code.lower() + "_"
                        targets = {t for q, t in self.switch_target.items() if q.startswith(pre)}
                        if len(targets) == 1:
                            name = f"{targets.pop()} switch"
                    words.append(f"{name}: {'On' if on else 'Off'}")
                else:
                    words.append(f"{name}: {_op_phrase(op, value)}")
                continue
            if fn.startswith("GetVMQuestVariable"):
                line = ""
                if p:
                    raw = p.get("raw") or ""
                    for o in self.objectives.get(quest_fid, []):
                        v = re.search(r"<Variable=(\w+)>", o).group(1)
                        if re.search(rf"\b{re.escape(v)}\b", raw):
                            line = o
                            break
                if not line and len(counters) == 1:
                    line = counters[0]
                if line:
                    words.append(re.sub(r"<Variable=\w+>", _op_phrase(op, value), line, count=1))
                elif value <= 9:
                    words.append(f"Reward tier {_op_phrase(op, value)}")
                else:
                    words.append(f"Event score {_op_phrase(op, value)}")
                continue
            if p:
                s = _pc.sentence(p)
                if s:
                    words.append(s)
        return words, mult

    def _event_name(self, row):
        """The reward's quest, as the player sees it. The row's own :QUST Ref
        first (ParentQuestDisplay can name the region boss instead), then the
        ParentQuest columns. A name built around an alias ("Uplink:
        <Alias=Location>") keeps its first part, led by the quest family from
        its EditorID ("DailyOps_Mode01_Quest" -> "Daily Ops: Uplink")."""
        quests = [_fid(v) for k, v in row.items() if (k or "").startswith("Ref")
                  and _q(v).endswith(":QUST")] + [_fid(row.get("ParentQuestLink"))]
        cands = [self.quest_full.get(q, "") for q in quests if q] + [row.get("ParentQuestDisplay")]
        for c in cands:
            c = _q(c)
            if not c or "[" in c:
                continue
            if "<" in c:
                label = _event_label(c)
                q = next((q for q in quests if q and self.quest_full.get(q) == c), "")
                fam = re.split(r"_", self.quest_edid.get(q, ""))[0] if q else ""
                fam = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", fam)
                if ":" in label or not fam:
                    return label
                return f"{fam}: {label}"
            return c
        return ""

    # -- the public calls ------------------------------------------------------
    def routes(self, book):
        book = (book or "").upper()
        name = self.book_name.get(book, "")
        rows = [r for lid in self._lists_above(book) for r in self.gmrw_by_list.get(lid, [])]
        out, solo = [], None
        for r in rows:
            lid = _fid(r.get("RewardedItem"))
            p = self.res.appearance_prob(lid, book)
            if p <= 0:
                continue
            words, mult = self._cond_words(r)
            words = words + [w for w in self._entry_lines(lid, book, name) if w not in words]
            rate = p * mult
            sibs = sorted(b for b in self.title_books
                          if b != book and lid in self._lists_above(b))
            once = None
            if sibs:
                if solo is None:
                    solo = self._without(book)
                once = solo.appearance_prob(lid, book) * mult
                if abs(once - rate) < 1e-9:
                    once = None
            out.append({"event": self._event_name(r), "conditions": words,
                        "rate": rate, "rateOnceOthers": once,
                        "others": [self.book_name.get(b, b) for b in sibs] if once is not None else []})
        return out

    def for_cobj(self, cobj_fid):
        book = self.book_of_cobj.get((cobj_fid or "").upper())
        if not book:
            return None, None
        return describe(self.routes(book))


# -- page text ------------------------------------------------------------------
def _merge_counters(routes):
    """Same event, same chance, conditions differing in one counter only
    ("Friendly Animals Alive: 1/3", "2/3", "3/3") -> one route."""
    groups, order = {}, []
    for rt in routes:
        key = (rt["event"], round(rt["rate"], 9),
               None if rt["rateOnceOthers"] is None else round(rt["rateOnceOthers"], 9),
               tuple(rt["others"]), len(rt["conditions"]))
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(rt)
    out = []
    for key in order:
        g = groups[key]
        uniq = []
        for rt in g:
            if rt["conditions"] not in [u["conditions"] for u in uniq]:
                uniq.append(rt)
        if len(uniq) == 1:
            out.append(uniq[0])
            continue
        n = key[-1]
        diff = [i for i in range(n) if len({rt["conditions"][i] for rt in uniq}) > 1]
        if len(diff) != 1:
            out.extend(uniq)
            continue
        i = diff[0]
        vals = [rt["conditions"][i] for rt in uniq]
        ms = [re.match(r"^(.*?)(\d+(?:\.\d+)?)(/\d+)?$", v) for v in vals]
        if all(ms) and len({m.group(1) for m in ms}) == 1 and len({m.group(3) for m in ms}) == 1:
            nums = sorted({m.group(2) for m in ms}, key=float)
            merged = f"{ms[0].group(1)}{', '.join(nums[:-1])} or {nums[-1]}{ms[0].group(3) or ''}"
        else:
            merged = " or ".join(dict.fromkeys(vals))
        rt = dict(uniq[0])
        rt["conditions"] = rt["conditions"][:i] + [merged] + rt["conditions"][i + 1:]
        out.append(rt)
    return out


def _complete(events):
    if not events:
        return ""
    if len(events) == 1:
        return f"Complete the {events[0]}" if ":" in events[0] else f"Complete {events[0]}"
    pre = {e.split(":", 1)[0] for e in events if ":" in e}
    if len(pre) == 1 and all(":" in e for e in events):
        kind = pre.pop().strip()
        names = [e.split(":", 1)[1].strip() for e in events]
        kind = kind if kind.endswith("s") else kind + "s"
        if " " not in kind:
            kind = kind.lower()
        return f"Complete any of these {kind}: " + ", ".join(names)
    return "Complete any of: " + ", ".join(events)


def _rate_text(rt):
    r = _pct(rt["rate"])
    if rt["rateOnceOthers"] is not None:
        r += f" ({_pct(rt['rateOnceOthers'])} once you have {', '.join(rt['others'])})"
    return r


def describe(routes):
    """(howToObtain, dropRate) for the titles page, in its existing style:
    one source and no conditions reads exactly as before ("Complete the Event:
    Radiation Rumble" / "10%")."""
    routes = _merge_counters(routes)
    if not routes:
        return None, None
    # routes with identical conditions + chance across several events -> one
    blocks, order = {}, []
    for rt in routes:
        key = (tuple(rt["conditions"]), round(rt["rate"], 9),
               None if rt["rateOnceOthers"] is None else round(rt["rateOnceOthers"], 9),
               tuple(rt["others"]))
        if key not in blocks:
            blocks[key] = {"rt": rt, "events": []}
            order.append(key)
        if rt["event"] and rt["event"] not in blocks[key]["events"]:
            blocks[key]["events"].append(rt["event"])
    how, rate = [], []
    one_block = len(order) == 1
    for key in order:
        b = blocks[key]
        rt, events = b["rt"], b["events"]
        head = _complete(events)
        cond = ", ".join(rt["conditions"])
        if head and head not in how:
            how.append(head)
        if cond:
            how.append(f"Condition: {cond}")
        if one_block:
            rate.append(_rate_text(rt))
        else:
            label = cond or (", ".join(events) if events else "Any finish")
            rate.append(f"{label} - {_rate_text(rt)}")
    return "\n".join(how), "\n".join(dict.fromkeys(rate))


def precompute(tsv_root):
    """{CondProxy recipe FormID: (howToObtain, dropRate)} for every title an
    event reward hands out. Built before the titles build loads its own data,
    and the engine is freed afterwards."""
    import gc
    r = TitleEventRoutes(tsv_root)
    out = {}
    for cobj, book in r.book_of_cobj.items():
        if book in r.title_books:
            how, dr = r.for_cobj(cobj)
            if how:
                out[cobj] = (how, dr)
    del r
    gc.collect()
    return out
