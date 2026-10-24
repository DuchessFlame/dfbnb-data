"""Drop rates for the Unique Weapons & Armour page (Duchess, 17 Oct 2026).

Every "Source:" line on the page came from a way into the game files: a
leveled list that holds the unique, or that holds the unique's OWN list
(uwa_obtain / uwa_trace record these as [list, target] pairs per line in
route["_linePaths"]). This module asks rng76 how likely that list is to give
the unique when it is rolled once, and writes the answer on the line:

    Source: Public Event: Neurological Warfare — 2.5000%

No rate maths lives here. Every number is rng76.Rng76Resolver.appearance_prob
(drop-rate-engine). rng76.py is not edited. The one thing done outside it:

  A unique's own list usually holds the PLAIN base weapon (built as the
  unique by its template keyword), so asking "does the base weapon appear?"
  would also count every plain copy elsewhere in a big pool. Instead the rate
  is split in two, both answered by rng76:
    reach  = P(the own list is reached from the source list), asked with the
             own list's entries swapped, for that one call, for a single
             stand-in entry the walk cannot find anywhere else;
    yields = P(the own list gives anything) - its own ChanceNone etc.
  rate = reach x yields.

What is NOT in the number (same as the plan pages): a reward record's own
GetRandomPercent (GMRW), and drop conditions - those are shown on the
route's Conditions line. A source with no leveled list behind it (a quest
or reward handing the item over directly, a vendor, the Atom Shop) gets no
number rather than a guessed one.
"""
import os
import re

import rng76

_SENTINEL = "FFFFFFFE"


def _f(v):
    return re.sub(r"[^0-9A-Fa-f]", "", str(v or "").split(":")[0])[-8:].upper()


class Rates:
    def __init__(self, tsv_root, log=print):
        self.log = log
        self.data = rng76.Rng76Data.from_tsv_root(tsv_root)
        self.res = self.data.resolver
        self.lvli = self.data.lvli
        self._cache = {}
        self._gate = None
        # the LVLI_List export beside the entries (list-level conditions);
        # set by the builder from the same pick() as the entries
        self.list_path = None

    def is_list(self, fid):
        return bool(fid) and fid in self.lvli.list_by_formid

    def _leaves(self, fid, seen=None):
        """Every leaf FormID inside a list's tree."""
        seen = seen or set()
        if fid in seen:
            return set()
        seen.add(fid)
        out = set()
        for e in self.lvli.entries_by_list.get(fid, []):
            m = self.lvli.math_by_entry.get((fid, e.get("EntryIndex"))) or {}
            sub = (m.get("SubLVLI_FormID") or "").strip()
            if sub:
                out |= self._leaves(sub, seen)
            else:
                f = _f(e.get("LVLO_Reference"))
                if f:
                    out.add(f)
        return out

    def yields(self, own):
        """P(the own list gives anything when rolled once)."""
        leaves = self._leaves(own)
        return self.res.appearance_prob(own, leaves) if leaves else 0.0

    def reach(self, src, own):
        """P(the own list is reached when src is rolled once)."""
        if src == own:
            return 1.0
        ents = self.lvli.entries_by_list.get(own)
        saved_math = {k: v for k, v in self.lvli.math_by_entry.items() if k[0] == own}
        stand_in = {"EntryIndex": "0", "LVLO_Reference": f"{_SENTINEL}:UWA_STAND_IN:WEAP"}
        try:
            self.lvli.entries_by_list[own] = [stand_in]
            for k in saved_math:
                del self.lvli.math_by_entry[k]
            # non-empty on purpose: the walk skips an entry with no math row
            self.lvli.math_by_entry[(own, "0")] = {"EntryAprioriChance_NoSublist": "1"}
            return self.res.appearance_prob(src, _SENTINEL)
        finally:
            del self.lvli.math_by_entry[(own, "0")]
            self.lvli.math_by_entry.update(saved_math)
            if ents is None:
                self.lvli.entries_by_list.pop(own, None)
            else:
                self.lvli.entries_by_list[own] = ents

    def rate(self, path, item=None):
        """Rate for one [list, target] pair, or None when there is no list.

        `item`: the FormIDs of the thing the page is about. When the target is
        a SHARED pool (a list with other things in it too - The Dragon's
        LegendaryItems_LL_Weapons_Ranged_All holds 58 ranged weapons), the
        rate is reaching that pool times the pool giving THIS item, not the
        pool giving anything (that printed 82.76% for a 1-in-58 pick)."""
        src, tgt = _f(path[0]), _f(path[1])
        hit = frozenset()
        if item and self.is_list(tgt):
            leaves = self._leaves(tgt)
            h = frozenset(_f(x) for x in item) & leaves
            if h and leaves - h:
                hit = h
        key = (src, tgt, hit)
        if key in self._cache:
            return self._cache[key]
        val = None
        if self.is_list(src) and tgt:
            if self.is_list(tgt):
                val = self.reach(src, tgt) * (self.res.appearance_prob(tgt, set(hit)) if hit
                                              else self.yields(tgt))
            else:
                val = self.res.appearance_prob(src, tgt)
        self._cache[key] = val
        return val


    def gate(self, fid):
        """A list's own list-level GetRandomPercent gate (0-1), via rng76's
        extract_grp_chance. 1.0 when the list has none."""
        if self._gate is None:
            import csv
            self._gate = {}
            if self.list_path:
                with open(self.list_path, encoding="utf-8", errors="replace", newline="") as fh:
                    for r in csv.DictReader(fh, delimiter="\t"):
                        conds = [v for k, v in r.items()
                                 if k and re.match(r"^ListCond\d+$", k) and v]
                        if conds:
                            c = self.res.extract_grp_chance(conds)
                            if c is not None:
                                self._gate[_f(r.get("LVLI_FormID"))] = c
        return self._gate.get(_f(fid), 1.0)

    def combo(self, parts, item=None):
        """P(at least one of several independent rolls gives the item): each
        part is a [list, target] rolled once, behind its own list gate."""
        miss = 1.0
        for lst, tgt in parts:
            p = self.rate([lst, tgt], item)
            if p:
                miss *= 1.0 - self.gate(lst) * p
        return 1.0 - miss


# Plain words for the parts of a list EditorID that tell two lists behind ONE
# source apart (The Dragon: "Safe" is pre-war safes at 0.62% and raider safes
# at 0.52%). Phrases first, then single tokens. A token with no entry here
# stops the split and the line keeps its range (logged), so an editor word
# never reaches the page.
_SPLIT_PHRASES = [
    ("Weapons_Ranged_All", "ranged-weapon pool"),
    ("Weapons_Melee_All", "melee-weapon pool"),
    ("Weapons_Any", "any-weapon pool"),
    ("Special_AllItems", "special any-item pool"),
    ("Creatures_AllItems", "creature any-item pool"),
    ("Armor_All", "armour pool"),
    ("Burn_RE", "random encounter"),
]
_SPLIT_DROP = {"container", "loot", "priority", "legendaryitems", "ll", "lli", "llc"}
_SPLIT_WORDS = {
    "prewar": "pre-war", "raider": "raider", "trap": "trapped", "boss": "boss",
    "poi": "at a point of interest", "safe": "safe", "trunk": "trunk", "re": "random encounter",
    "cooler": "cooler", "toolbox": "toolbox",
}


def _split_desc(edid):
    """EditorID -> list of plain words, or None when a word is unknown."""
    e = edid or ""
    words = []
    for ph, w in _SPLIT_PHRASES:
        if ph.lower() in e.lower():
            words.append(w)
            e = re.sub(re.escape(ph), "", e, flags=re.I)
    for t in [t for t in re.split(r"_+", e) if t]:
        tl = t.lower()
        if tl in _SPLIT_DROP:
            continue
        if tl not in _SPLIT_WORDS:
            return None
        words.append(_SPLIT_WORDS[tl])
    return words


def split_by_list(rates, paths, item):
    """One Source line, several lists, different rates -> [(label, rate)] per
    rate, each label naming what its lists have that the others don't. None
    when the lists can't be told apart in plain words."""
    rated = []
    for p in paths:
        v = rates.rate(p, item)
        if v is None or v <= 0:
            continue
        d = _split_desc(rates.lvli.edid_for(_f(p[0])))
        if d is None:
            return None
        rated.append((round(v, 8), d))
    if len({v for v, _ in rated}) < 2:
        return None
    common = set.intersection(*(set(d) for _v, d in rated))
    groups = {}
    for v, d in rated:
        desc = " ".join(w for w in d if w not in common)
        groups.setdefault(v, [])
        if desc not in groups[v]:
            groups[v].append(desc)
    out = []
    for v in sorted(groups, reverse=True):
        lab = " / ".join(x for x in groups[v] if x)
        out.append((lab, v))
    if len({l for l, _ in out}) != len(out):
        return None
    return out


def _fmt(p):
    return f"{p * 100:.4f}%"


def apply(items, rates, log=print):
    """Write the rate on every Source line that has a list behind it."""
    stats = {"lines": 0, "rated": 0, "zero": []}
    for it in items:
        item = [x for x in (it.get("formId"), it.get("planFormId"),
                            *(it.get("variantFormIds") or [])) if x]
        for r in it.get("obtainRoutes") or []:
            lp = r.pop("_linePaths", None) or {}
            combos = r.pop("_combos", None) or {}
            if not r.get("populated"):
                continue
            any_rated = False
            zero_note = False
            pool_note = False
            new_lines = []
            for ln in r.get("lines") or []:
                if not ln.startswith("Source:"):
                    new_lines.append(ln)
                    continue
                stats["lines"] += 1
                if ln in combos:
                    vals = [rates.combo(combos[ln], item)]
                else:
                    vals = [v for v in (rates.rate(p, item) for p in lp.get(ln, [])) if v is not None]
                raw = list(vals)
                vals = sorted({round(v, 8) for v in vals if v > 0})
                if not vals:
                    if lp.get(ln):
                        stats["zero"].append(f"{it.get('name')}: {ln[8:]}")
                    if raw and all(v == 0 for v in raw):
                        # Wired up, but the files give it nothing (Nailer: One
                        # Violent Night hands its rare list out at count 0).
                        new_lines.append(f"{ln} — 0%")
                        zero_note = True
                    else:
                        new_lines.append(ln)
                    continue
                if len(vals) > 1 and _fmt(vals[0]) != _fmt(vals[-1]) and ln not in combos:
                    # House rule: no rate ranges. Different lists behind one
                    # source name -> one line each, named in plain words.
                    parts = split_by_list(rates, lp.get(ln, []), item)
                    if parts:
                        # words the source name already says ("Pre-War Safe
                        # (pre-war)") are not repeated
                        low = set(re.findall(r"[a-z'-]+", ln[8:].lower()))
                        def _trim(seg):
                            t = " ".join(w for w in seg.split(" ") if w.lower() not in low or "pool" in seg)
                            # all said by the name already: the plain kind
                            return t or ("standard" if seg else "")
                        parts = [(" / ".join(_trim(seg) for seg in lab.split(" / ")), v) for lab, v in parts]
                        if any("pool" in lab for lab, _v in parts):
                            pool_note = True
                        for lab, v in parts:
                            new_lines.append(f"{ln} ({lab}) — {_fmt(v)}" if lab else f"{ln} — {_fmt(v)}")
                        stats["rated"] += 1
                        any_rated = True
                        continue
                    stats.setdefault("ranges", []).append(f"{it.get('name')}: {ln[8:]}")
                txt = _fmt(vals[0]) if len(vals) == 1 or _fmt(vals[0]) == _fmt(vals[-1]) \
                    else f"{_fmt(vals[0])} – {_fmt(vals[-1])}"
                new_lines.append(f"{ln} — {txt}")
                stats["rated"] += 1
                any_rated = True
            if pool_note:
                new_lines.append("Note: A legendary drop first picks a pool (ranged weapons, any weapon, "
                                 "any item...); each rate is the chance of this item once that pool is picked.")
            if zero_note:
                new_lines.append("Note: 0% in the game files - this source is still wired up "
                                 "but hands nothing out, so it does not drop from here.")
            r["lines"] = new_lines
            if any_rated:
                # The rate now sits on each source line; a route-wide
                # "Drop: N/A" pill beside it would contradict it.
                r["dropRate"] = ""
    log(f"  drop rates: {stats['rated']} of {stats['lines']} source lines rated")
    if stats.get("ranges"):
        log(f"    rate ranges left (lists not nameable in plain words - add to _SPLIT_WORDS): {len(stats['ranges'])}")
        for z in stats["ranges"][:40]:
            log(f"      {z}")
    if stats["zero"]:
        log(f"    lists found but rate 0 (left blank, check): {len(stats['zero'])}")
        for z in stats["zero"][:40]:
            log(f"      {z}")
    return stats


def strip(items):
    for it in items:
        for r in it.get("obtainRoutes") or []:
            r.pop("_linePaths", None)
            r.pop("_combos", None)

