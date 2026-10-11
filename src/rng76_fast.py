#!/usr/bin/env python3
"""
rng76_fast.py — skip loot-list branches that cannot contain the item.

rng76.py IS NOT EDITED. This module subclasses its resolver and changes one
thing: before `appearance_prob()` walks a list, it checks a precomputed index
of every FormID reachable from that list. If none of the targets can be reached,
the list's answer is 0, so the walk is skipped and 0.0 is returned.

Why that cannot change a number: when no target is reachable, every leaf in
the walk contributes 0.0, every sub-list returns 0.0, and every combination
rng76 applies to them (sum, max over contexts, 1 - Π(1 - 0), 1 - (1 - 0)^N)
is exactly 0.0. The skip returns the same 0.0 without doing the arithmetic.

The index is deliberately a SUPERSET: every LVLO reference and every sub-list
of every entry, followed through every list, whatever its flags or conditions.
A superset can only ever cause a walk that was not needed — never a skip that
was. Matching mirrors rng76 exactly (`fid.endswith(target)`).

Opt-in only: build_plan_obtain_json uses it when DFBNB_FAST_RATES=1. Proof
before switching it on: run_equivalence() compares old and new on every
(list, plan) pair the build asks for — all must be bit-identical.
"""

import rng76


def _fid(ref):
    ref = (ref or "").strip()
    return ref.split(":")[0].upper() if ":" in ref else ref.upper()


def build_reach(lvli):
    """{list FormID: frozenset(every FormID reachable below it)}."""
    direct, subs = {}, {}
    for lid, entries in lvli.entries_by_list.items():
        d, s = set(), set()
        for e in entries:
            f = _fid(e.get("LVLO_Reference"))
            if f:
                d.add(f)
            idx = e.get("EntryIndex")
            m = lvli.math_by_entry.get((lid, idx)) or {}
            sub = (m.get("SubLVLI_FormID") or "").strip()
            if sub:
                s.add(sub)
                d.add(sub.upper())
            if f and f in lvli.entries_by_list:
                s.add(f)
        direct[lid], subs[lid] = d, s
    reach = {lid: set(d) for lid, d in direct.items()}
    changed = True
    while changed:                       # fixed point — cycles are fine
        changed = False
        for lid, ss in subs.items():
            r = reach[lid]
            before = len(r)
            for sub in ss:
                child = reach.get(sub) or reach.get(sub.upper())
                if child:
                    r |= child
            if len(r) != before:
                changed = True
    return {lid: frozenset(r) for lid, r in reach.items()}


class PrunedResolver(rng76.Rng76Resolver):
    reach = None

    def _can_reach(self, list_id, targets):
        r = self.reach.get(list_id)
        if r is None:
            return True                  # unknown list: never skip
        for t in targets:
            if not t:
                continue
            if len(t) == 8:
                if t in r:
                    return True
            elif any(f.endswith(t) for f in r):
                return True
        return False

    def appearance_prob(self, list_id, target_formid, depth=0, seen=None):
        if isinstance(target_formid, (list, set, tuple, frozenset)):
            targets = {str(t).upper() for t in target_formid if t}
        else:
            targets = {(target_formid or "").upper()}
        if list_id and not self._can_reach(list_id, targets):
            return 0.0
        return super().appearance_prob(list_id, target_formid, depth, seen)


def enable(data):
    """Swap a loaded Rng76Data's resolver for the pruned one, in place."""
    res = data.resolver
    res.__class__ = PrunedResolver
    res.reach = build_reach(data.lvli)
    return res
