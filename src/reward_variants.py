#!/usr/bin/env python3
"""
reward_variants.py - which named version of a weapon or armour piece a reward
list actually drops.

WHY THIS EXISTS
===============
A named weapon is usually the plain base record (Alien Blaster, 000FF995) plus
an ObjectTemplate combination that adds the unique mod. One base record can
carry several of these: the Alien Blaster has both Zeta's Revenge and The
V.A.T.S. Unknown. The reward builders used to pick "the combination with the
most legendary slots" from the FormID alone, so every Alien Blaster reward was
labelled Zeta's Revenge - including the Invaders from Beyond drop, which the
game files hand out through LL_Weapon_Ranged_AlienBlaster_TheVATSUnknown
(Duchess, 19 Oct 2026).

The leveled list that holds the base record names the version: Bethesda wraps
each unique in its own one-item list (LL_Weapon_Ranged_AlienBlaster_TheVATSUnknown,
SCORE_S8_LL_AlienBlaster_ZetasRevenge). This module finds those holder lists
under a reward list and matches their EditorIDs against a weapon's named
combinations.

No rate maths here: it only decides a NAME and which combination's mod slots
to show. Rates stay with rng76 (drop-rate-engine).

DO NOT TRUST Combination_FULL ON ITS OWN
========================================
Bethesda's combination names can be out of step with the combinations. On the
Alien Blaster (Oct 2026 export) combination 6 is NAMED "Zeta's Revenge" but
carries mod_Custom_TheVATSUnknown and the template keyword
if_tmp_TheVATSUnknown; the real Zeta's Revenge preset is combination 7
(if_tmp_ZetasRevenge, no name). The game picks a combination by its template
keyword and the unique mod is what the player sees, so a combination is
identified by its mod_Custom_* mod and its if_tmp_* keyword. Combination_FULL
is only used when it agrees with one of those.

USAGE
-----
    import reward_variants as rv
    named   = rv.named_combos(WEAP_OT, "WEAP_FormID")    # once per builder
    holders = rv.holder_edids(root_fid, item_fid, entries_by_list, edid_of)
    hit     = rv.pick(holders, rv.variants_for(named, item_fid), base_name="Alien Blaster")
    # hit is (combination index, display name) of the ONE version the holder
    # lists name, else None (None = keep the builder's old behaviour).
"""

import re

MIN_KEY = 5   # shorter keys ("ace", "the") would match half the lists in the game


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _fid(s):
    return (s or "").strip().strip('"').upper()


def _ref(row):
    """LVLO_Reference "000FF995:AlienBlaster:WEAP" -> (fid, edid, sig)."""
    parts = (row.get("LVLO_Reference") or "").strip().strip('"').split(":")
    if len(parts) < 3:
        return _fid(parts[0]) if parts else "", "", ""
    return _fid(parts[0]), parts[1], parts[-1].upper()


def holder_edids(root_fid, target_fid, entries_by_list, edid_of=None, max_depth=50):
    """EditorIDs of every leveled list reachable from root_fid (itself included)
    that holds target_fid as a direct entry.

    entries_by_list: {list FormID: [entry rows with LVLO_Reference]} - the dict
    each builder already loads. edid_of: {list FormID: EditorID} or a callable;
    a list's EditorID is also read from its own entry rows (LVLI_EDID)."""
    root, target = _fid(root_fid), _fid(target_fid)
    if not root or not target:
        return []
    by_list = {}
    for k, v in entries_by_list.items():
        by_list.setdefault(_fid(k), v)

    def edid(fid, rows):
        if callable(edid_of):
            e = edid_of(fid)
        elif edid_of:
            e = edid_of.get(fid) or edid_of.get(fid.lower()) or ""
        else:
            e = ""
        if not e and rows:
            e = (rows[0].get("LVLI_EDID") or "").strip().strip('"')
        return e or ""

    out, seen, stack = [], set(), [(root, 0)]
    while stack:
        fid, depth = stack.pop()
        if fid in seen or depth > max_depth:
            continue
        seen.add(fid)
        rows = by_list.get(fid) or []
        for r in rows:
            rfid, _redid, rsig = _ref(r)
            if rfid == target:
                e = edid(fid, rows)
                if e and e not in out:
                    out.append(e)
            elif rsig == "LVLI" and rfid:
                stack.append((rfid, depth + 1))
    return out


def pick(holders, variants, base_name=""):
    """The payload of the single variant whose key appears in a holder list's
    EditorID, or None.

    variants: [(iterable of names/keys, payload)]. Keys are normalised here.
    A key that is (part of) the base item's own name never counts - a generic
    LL_Weapon_Ranged_AlienBlaster must not pick a version called "Alien Blaster".
    Two different versions named by the holders -> None (ambiguous, leave it)."""
    base = norm(base_name)
    hs = [norm(h) for h in holders or [] if h]
    if not hs or not variants:
        return None
    hit = {}
    for i, (keys, payload) in enumerate(variants):
        for k in keys or ():
            k = norm(k)
            if len(k) < MIN_KEY or (base and k in base):
                continue
            if any(k in h for h in hs):
                hit[i] = payload
                break
    if len(hit) != 1:
        return None
    return next(iter(hit.values()))


_CUSTOM_MOD = re.compile(r'^\s*"?(mod_custom_[A-Za-z0-9_]+)"?\s*(?:"([^"]*)")?', re.I)
_TMP_KW = re.compile(r"if_tmp_([A-Za-z0-9_]+)", re.I)


def _split_camel(s):
    s = re.sub(r"_+", " ", s or "")
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s).strip()


_LABEL_TAIL = re.compile(r"\s+(paint|custom mod|custom|effect|mod)$", re.I)


def clean_label(name):
    """A unique mod's name as a weapon name: "Red Terror Paint" -> "Red Terror",
    "Daisycutter Custom Mod" -> "Daisycutter", "custom HolyFire Effect" -> ""."""
    n = (name or "").strip()
    if not n or n.lower().startswith("custom ") or "_" in n:
        return ""
    prev = None
    while prev != n:
        prev, n = n, _LABEL_TAIL.sub("", n).strip()
    return n


def named_combos(ot_rows, fid_col):
    """{record FormID: [(names, combination index, display name)]} for every
    ObjectTemplate combination that adds a unique mod (mod_Custom_*).

    names: the unique mod's name and EditorID tail, the combination's
    if_tmp_* keyword tails, and Combination_FULL only when it agrees with one
    of those. display name: Combination_FULL when it agrees, else the unique
    mod's name, else "" (the builder keeps its own naming)."""
    meta = {}
    for r in ot_rows or []:
        fid = _fid(r.get(fid_col) or r.get("FormID"))
        if not fid:
            continue
        try:
            ci = int(float((r.get("CombinationIndex") or "0").strip().strip('"') or 0))
        except ValueError:
            ci = 0
        m = meta.setdefault((fid, ci), {"custom": [], "kw": set(), "full": ""})
        mod = (r.get("Include_Mod") or r.get("Mod") or "").strip()
        cm = _CUSTOM_MOD.match(mod)
        if cm:
            m["custom"].append((cm.group(1), (cm.group(2) or "").strip()))
        for k in _TMP_KW.findall(r.get("OBTS_Keywords") or ""):
            m["kw"].add(k)
        full = (r.get("Combination_FULL") or "").strip().strip('"')
        if full and not m["full"]:
            m["full"] = full
    out = {}
    for (fid, ci), m in meta.items():
        if not m["custom"]:
            continue
        names = set(m["kw"])
        for ed, nm in m["custom"]:
            names.add(ed[len("mod_custom_"):])
            if nm:
                names.add(nm)
        known = {norm(n) for n in names}
        nf = norm(m["full"])
        # Combination_FULL counts only when it agrees with the mod / keyword
        # ("Red Terror" vs RedTerror_Paint) - never on its own.
        full = m["full"] if len(nf) >= MIN_KEY and any(nf in k or k in nf for k in known
                                                       if len(k) >= MIN_KEY) else ""
        if full:
            names.add(full)
        label = full or next((c for c in (clean_label(nm) for _ed, nm in m["custom"])
                              if c and c.lower() != "cursed"), "")
        out.setdefault(fid, []).append((sorted(names), (ci, label)))
    return out


def variants_for(named, fid):
    return named.get(_fid(fid)) or []
