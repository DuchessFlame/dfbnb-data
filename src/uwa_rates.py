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

    def rate(self, path):
        """Rate for one [list, target] pair, or None when there is no list."""
        src, tgt = _f(path[0]), _f(path[1])
        key = (src, tgt)
        if key in self._cache:
            return self._cache[key]
        val = None
        if self.is_list(src) and tgt:
            if self.is_list(tgt):
                val = self.reach(src, tgt) * self.yields(tgt)
            else:
                val = self.res.appearance_prob(src, tgt)
        self._cache[key] = val
        return val


def _fmt(p):
    return f"{p * 100:.4f}%"


def apply(items, rates, log=print):
    """Write the rate on every Source line that has a list behind it."""
    stats = {"lines": 0, "rated": 0, "zero": []}
    for it in items:
        for r in it.get("obtainRoutes") or []:
            lp = r.pop("_linePaths", None) or {}
            if not r.get("populated"):
                continue
            any_rated = False
            new_lines = []
            for ln in r.get("lines") or []:
                if not ln.startswith("Source:"):
                    new_lines.append(ln)
                    continue
                stats["lines"] += 1
                vals = [v for v in (rates.rate(p) for p in lp.get(ln, [])) if v is not None]
                vals = sorted({round(v, 8) for v in vals if v > 0})
                if not vals:
                    if lp.get(ln):
                        stats["zero"].append(f"{it.get('name')}: {ln[8:]}")
                    new_lines.append(ln)
                    continue
                txt = _fmt(vals[0]) if len(vals) == 1 or _fmt(vals[0]) == _fmt(vals[-1]) \
                    else f"{_fmt(vals[0])} – {_fmt(vals[-1])}"
                new_lines.append(f"{ln} — {txt}")
                stats["rated"] += 1
                any_rated = True
            r["lines"] = new_lines
            if any_rated:
                # The rate now sits on each source line; a route-wide
                # "Drop: N/A" pill beside it would contradict it.
                r["dropRate"] = ""
    log(f"  drop rates: {stats['rated']} of {stats['lines']} source lines rated")
    if stats["zero"]:
        log(f"    lists found but rate 0 (left blank, check): {len(stats['zero'])}")
        for z in stats["zero"][:40]:
            log(f"      {z}")
    return stats


def strip(items):
    for it in items:
        for r in it.get("obtainRoutes") or []:
            r.pop("_linePaths", None)
