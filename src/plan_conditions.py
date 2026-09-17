#!/usr/bin/env python3
r"""
plan_conditions.py — "Drop Conditions" for a plan's How to Obtain.

A rate says how often a source gives a plan. It does not say when the source
gives it AT ALL. Those are two different questions and the game answers the
second one in the leveled-list entry's CONDITIONS, which this repo has never
read: "the mod plans only drop once you know the base weapon plan", "level 10
and above", "only while the event is switched on".

WHERE THE DATA IS
-----------------
`LVLI_Export_*_LVLI_Entries.tsv`, columns `Cond1..Cond10` (plus `CondCount`),
one row per entry. The export writes each condition as

    Subject.HasLearnedRecipe(00 00 00, 64 43, co_Weapon_Melee_DeathclawGauntlet
        [COBJ:0010A08A], 00 00 00 00, -1) 00000000 1.000000
    <run on>.<function>(<params>) <TYPE BITS> <comparison value>

READING THE TYPE BITS
---------------------
xEdit prints a flags field LSB-first (bit 0 leftmost), so the comparison
operator is the FIRST THREE characters read low-bit-first:

    op = bits[0] + 2*bits[1] + 4*bits[2]     0 =  1 !=  2 >  3 >=  4 <  5 <=

This is not a guess -- it is exactly how `rng76.parse_grp_condition()` reads the
same strings, verified against real lists (a First Match list's
`GetRandomPercent <= 20 / <= 38` comes out 20% / 18% / 62%). Two more bits are
used here:

    bit 3  OR   -- this condition is OR-ed with the next one (region lists)
    bit 5       -- the comparison value is a GLOB reference, not a number

Reading the bits the other way round flips the meaning of every sentence on the
page ("only before you learn X" vs "only after"), so it is worth stating plainly
that rng76 is the authority for it. **Nothing here touches rng76 or any rate.**

WHAT IS PUBLISHED
-----------------
Only conditions that can be stated in plain English. An unrecognised function is
counted, not printed -- a page that says "None" when the game has a condition is
worse than one that says some conditions could not be read, so where any were
skipped the row ends with a single honest line saying so.

`GetRandomPercent` is deliberately IGNORED: it is a dice roll, not a condition,
and it is already inside the rate the row prints next to the source.
"""

import csv
import glob
import os
import re

MAX_DEPTH = 6          # entry-graph walk from a source list down to the plan
MAX_SENTENCES = 6      # per route, before "and other conditions"

# Operator codes, read off the first three TYPE bits low-bit-first exactly as
# rng76.parse_grp_condition() does:  op = bits[0] + 2*bits[1] + 4*bits[2].
#
# The ORDER OF 0 AND 1 IS NOT THE FALLOUT 4 DOCUMENTED ORDER, and it was settled
# against the game rather than against the documentation. The Deathclaw Gauntlet
# chain is unambiguous in game -- the base plan drops until you learn it, the
# Hook mod plan only starts dropping once you know the base -- and it is written
# as (LLE_Creature_Deathclaw):
#
#   base plan entry : HasLearnedRecipe(gauntlet)  00000000 1   -> must be NOT learned
#   hook plan entry : HasLearnedRecipe(gauntlet)  10000000 1   -> must BE learned
#                     HasLearnedRecipe(hook)      00000000 1   -> must be NOT learned
#
# so code 1 is "equal to" and code 0 is "not equal to". The Vault 94 gold-vendor
# set says the same thing twice over: GetItemCount(the plan) 10000000 0 with
# HasLearnedRecipe(its recipe) 00000000 1 -- "you don't have it and haven't
# learned it", which is what a vendor offering set pieces should check.
#
# rng76 maps these two codes the other way round. It only ever uses them for
# GetRandomPercent, where <= and >= (codes 5 and 3, unaffected) carry every real
# list, so no rate is wrong -- but it is worth Duchess confirming in xEdit.
OP_NE, OP_EQ, OP_GT, OP_GE, OP_LT, OP_LE = range(6)

_COND_RE = re.compile(
    r"^(?:(?P<runon>\w+)\.)?(?P<fn>\w+)\((?P<params>.*)\)\s+"
    r"(?P<bits>[01]{8})\s+(?P<value>.*)$")

# EDID "Display Name" [SIG:FORMID]   /   EDID [SIG:FORMID]
_REF_RE = re.compile(r'(?P<edid>[A-Za-z0-9_\-]+)(?:\s+"(?P<full>[^"]*)")?\s*'
                     r'\[(?P<sig>[A-Z_]{4}):(?P<fid>[0-9A-Fa-f]{8})\]')

_NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def _op(bits):
    return int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2])


def _is_or(bits):
    return bits[3] == "1"


def parse_condition(text):
    """One condition string -> dict, or None when it is not in that shape."""
    m = _COND_RE.match((text or "").strip())
    if not m:
        return None
    refs = [mm.groupdict() for mm in _REF_RE.finditer(m.group("params"))]
    value = (m.group("value") or "").strip()
    vref = _REF_RE.search(value)
    return {
        "fn": m.group("fn"),
        "run_on": (m.group("runon") or "").lower(),
        "op": _op(m.group("bits")),
        "or_next": _is_or(m.group("bits")),
        "refs": refs,
        "value": value,
        "value_ref": vref.groupdict() if vref else None,
        "number": float(value) if _NUM_RE.match(value) else None,
        "raw": text,
    }


# ── naming ───────────────────────────────────────────────────────────────────
# Same rule the rest of this pipeline follows: the game's own display name wins,
# an EditorID is only humanised as a fallback, and an editor-only record is
# never prettified into something that reads like a real place or item.
try:
    import plan_sources as _ps
except Exception:                                    # pragma: no cover
    _ps = None


def _humanize(edid):
    if not edid:
        return None
    if _ps is not None:
        if _ps.is_dev_record(edid):
            return None
        try:
            return _ps.humanize(edid)
        except Exception:
            pass
    s = re.sub(r"[_]+", " ", str(edid))
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    return s.strip() or None


def _ref_name(ref, names=None):
    """Display name for one `EDID "FULL" [SIG:FID]` parameter."""
    if not ref:
        return None
    full = (ref.get("full") or "").strip()
    if full:
        return full
    fid = (ref.get("fid") or "").upper()
    if names and fid in names:
        return names[fid]
    return _humanize(ref.get("edid"))


def _first_ref_name(cond, names=None):
    for ref in cond.get("refs") or []:
        n = _ref_name(ref, names)
        if n:
            return n
    return None


def _truthy(cond):
    """Does a yes/no condition mean YES? None when it cannot be read."""
    n = cond.get("number")
    if n is None:
        return None
    op = cond["op"]
    yes = n != 0
    if op == OP_EQ:
        return yes
    if op == OP_NE:
        return not yes
    if op in (OP_GE, OP_GT):
        return True
    if op in (OP_LT, OP_LE):
        return False
    return None


def _level_phrase(cond):
    n = cond.get("number")
    if n is None:
        return None
    n = int(n)
    op = cond["op"]
    if op in (OP_GE, OP_GT):
        return f"Level {n if op == OP_GE else n + 1} and above"
    if op in (OP_LT, OP_LE):
        return f"Level {n if op == OP_LE else n - 1} and below"
    if op == OP_EQ:
        return f"Only at level {n}"
    return None


# fn -> (yes sentence, no sentence). {} is the named record.
_YES_NO = {
    "HasEntitlement":      ("Requires the {} entitlement",
                            "Only while you do not own {}"),
    "HasActiveChallenge":  ("Only while the challenge {} is active",
                            "Only while the challenge {} is not active"),
    "IsFishingBaitSelected": ("Only with {} on the rod", None),
    "WornHasKeyword":      ("Only while wearing {}", "Only while not wearing {}"),
    "HasKeyword":          ("Only from {}", None),
    "GetEquipped":         ("Only with {} equipped", None),
    "GetLocked":           ("Only from a locked container",
                            "Only from an unlocked container"),
    "GetIsInRegion":       ("Only in {}", "Not in {}"),
    "GetInCurrentLocation": ("Only in {}", "Not in {}"),
    "GetIsEditorLocation": ("Only in {}", "Not in {}"),
    "LocationHierarchyHasKeyword": ("Only in {}", "Not in {}"),
    "EditorLocationHasKeyword":    ("Only in {}", "Not in {}"),
    "GetPublicEventHasMutation":   ("Only when the event has the {} mutation", None),
}

# Functions that are never printed. GetRandomPercent is the dice roll that is
# already inside the rate; the rest are plumbing a player cannot act on.
_IGNORED = {"GetRandomPercent", "IsTrueForConditionForm", "GetRandomPercentGlobal",
            # Interior-vs-exterior and raw actor values: engine plumbing with
            # nothing for a player to act on.
            "GetWorldType", "GetValue", "GetActorValueForCurrentLocation"}


def is_wiring(cond):
    """A condition whose silence is expected, so it must not be counted as
    "unreadable" and must not make a route claim that something was missed.

    Nearly all of these are GLOBs: 1,054 of them reach the plan routes and they
    are switches with editor names ("BS02 Special Vendor Inventory Index"), most
    permanently on. Only the seasonal Update01_Quest_<Event> flags say anything
    a player can act on, and those DO get a sentence.
    """
    fn = cond.get("fn")
    if fn in _IGNORED:
        return True
    if fn == "GetGlobalValue":
        edid = ((cond.get("refs") or [{}])[0].get("edid") or "")
        return not re.match(r"^Update01_Quest_\w+$", edid, re.I)
    return False


def _is_own(ref_fid, name, own):
    """Is this condition talking about the plan the row is for?

    Two tests, because the FormID link is not always right: 221 rows had no
    recipe linked at all before add_cobj_link, and the Deathclaw Gauntlet Extra
    Claw row is linked to the BASE gauntlet's recipe rather than its own. The
    name comparison catches what the id misses.
    """
    if not own:
        return False
    if ref_fid and ref_fid in {(own.get("recipe") or "").upper(),
                               (own.get("book") or "").upper()}:
        return True
    title = re.sub(r"^(plan|recipe):\s*", "", (own.get("title") or ""), flags=re.I)
    return bool(title and name and title.strip().lower() == name.strip().lower())


def sentence(cond, names=None, own=None):
    """Plain-English line for one parsed condition, or None if not readable."""
    fn = cond["fn"]
    if fn in _IGNORED:
        return None

    if fn == "GetLevel":
        return _level_phrase(cond)

    if fn == "HasLearnedRecipe":
        name = _first_ref_name(cond, names)
        yes = _truthy(cond)
        if yes is None:
            return None
        ref_fid = ((cond.get("refs") or [{}])[0].get("fid") or "").upper()
        if not yes and _is_own(ref_fid, name, own):
            # The plan's own recipe -- the ordinary "a plan you know stops
            # showing up", which Technical already carries as a flag.
            return "Stops dropping once you learn it"
        if not name:
            return None
        return (f"Only after you learn {name}" if yes
                else f"Only until you learn {name}")

    if fn == "GetItemCount":
        name = _first_ref_name(cond, names)
        yes = _truthy(cond)
        if yes is None:
            return None
        ref_fid = ((cond.get("refs") or [{}])[0].get("fid") or "").upper()
        if _is_own(ref_fid, name, own):
            return ("Only while you already have one" if yes
                    else "Only while you do not already have one")
        if not name:
            return None
        return (f"Only while you are carrying {name}" if yes
                else f"Only while you do not have {name}")

    if fn in ("GetQuestCompleted", "IsActiveQuest", "HasCompletedChallenge"):
        name = _first_ref_name(cond, names)
        yes = _truthy(cond)
        if not name or yes is None:
            return None
        if fn == "IsActiveQuest":
            return (f"Only while {name} is active" if yes
                    else f"Only while {name} is not active")
        word = "the challenge " if fn == "HasCompletedChallenge" else ""
        return (f"Only after completing {word}{name}" if yes
                else f"Only before completing {word}{name}")

    if fn == "GetLockLevel":
        name = _first_ref_name(cond, names) or ""
        lock = re.sub(r"^LockLevel[_ ]?", "", (cond.get("value_ref") or {}).get("edid") or "", flags=re.I)
        lock = _humanize(lock) if lock else ""
        return (f"Only from a {lock}-locked container" if lock
                else "Only from a locked container")

    if fn == "GetNumTimesCompletedQuest":
        name = _first_ref_name(cond, names)
        yes = _truthy(cond)
        if not name or yes is None:
            return None
        return (f"Only after completing {name}" if yes
                else f"Only before completing {name}")

    if fn == "GetGlobalValue":
        # A GLOB is a switch with an editor name, and most of them are wiring a
        # reader can do nothing with -- "BS02 Special Vendor Inventory Index"
        # published 171 times before this guard. Only the seasonal-event enable
        # flags say something a player can act on, and they are all named
        # Update01_Quest_<Event>.
        yes = _truthy(cond)
        edid = ((cond.get("refs") or [{}])[0].get("edid") or "")
        m = re.match(r"^Update01_Quest_(?P<event>\w+)$", edid, re.I)
        if not m or yes is None:
            return None
        name = _humanize(m.group("event"))
        if not name:
            return None
        return (f"Only while the {name} event is running" if yes
                else f"Only while the {name} event is not running")

    if fn in _YES_NO:
        yes = _truthy(cond)
        if yes is None:
            return None
        tpl = _YES_NO[fn][0 if yes else 1]
        if not tpl:
            return None
        if "{}" not in tpl:
            return tpl
        name = _first_ref_name(cond, names)
        return tpl.format(name) if name else None

    return None


# ── OR groups ────────────────────────────────────────────────────────────────
def _merge_or(parsed, names, own):
    """Sentences for one entry, OR-runs folded into a single line.

    Region and location lists are written as a chain of OR-ed conditions -- one
    per region -- and printing them as separate lines reads as "all of these at
    once", the opposite of what the game does.
    """
    out, skipped = [], 0
    i = 0
    while i < len(parsed):
        run = [parsed[i]]
        while run[-1]["or_next"] and i + 1 < len(parsed):
            i += 1
            run.append(parsed[i])
        i += 1

        lines = []
        for c in run:
            s = sentence(c, names, own)
            if s:
                lines.append(s)
            elif not is_wiring(c):
                skipped += 1
        if not lines:
            continue
        if len(lines) == 1:
            out.append(lines[0])
            continue
        # Same opening words on every branch ("Only in X" / "Only in Y") reads
        # best as one line listing the alternatives.
        heads = {ln.rsplit(" ", 1)[0] if " " in ln else ln for ln in lines}
        prefix = os.path.commonprefix(lines).rstrip()
        if len(heads) > 1 and len(prefix) > 6:
            tails = [ln[len(prefix):].strip() for ln in lines]
            out.append(f"{prefix} " + ", ".join(tails[:-1]) + f" or {tails[-1]}"
                       if len(tails) > 2 else f"{prefix} {tails[0]} or {tails[1]}")
        else:
            out.append(" or ".join(lines))
    return out, skipped


# ── the index ────────────────────────────────────────────────────────────────
class ConditionIndex:
    """Entry conditions for every leveled list, and the walk down to a plan."""

    def __init__(self, tsv_dir="tsv", newest=None):
        self.entries = {}       # LVLI FormID -> [entry dicts]
        self.lists = {}         # LVLI FormID -> {"flags", "conds"}
        self.names = {}         # FormID -> display name (COBJ -> what it makes)
        self.export = ""
        self._load_entries(tsv_dir, newest)
        self._load_lists(tsv_dir, newest)
        self.parents = {}       # LVLI FormID -> [(parent FormID, entry)]
        for lid, es in self.entries.items():
            for e in es:
                if e["sig"] == "LVLI" and e["ref"]:
                    self.parents.setdefault(e["ref"], []).append((lid, e))
        self._anc_cache = {}
        self._load_cobj_names(tsv_dir, newest)

    # -- loading ------------------------------------------------------------
    def _pick(self, pattern, tsv_dir, newest):
        if newest:
            p = newest(pattern, tsv_dir)
            if p:
                return p
        found = sorted(glob.glob(os.path.join(tsv_dir, pattern)))
        return found[-1] if found else None

    def _load_entries(self, tsv_dir, newest):
        path = self._pick("LVLI_Export_*_LVLI_Entries.tsv", tsv_dir, newest)
        if not path:
            return
        self.export = os.path.basename(path)
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                lid = (row.get("LVLI_FormID") or "").strip().upper()
                if not lid:
                    continue
                ref = (row.get("LVLO_Reference") or "").split(":")
                rfid = (ref[0] if ref else "").strip().upper()
                rsig = (ref[2] if len(ref) > 2 else "").strip().upper()
                conds = []
                for i in range(1, 11):
                    c = (row.get(f"Cond{i}") or "").strip()
                    if not c:
                        continue
                    p = parse_condition(c)
                    if p:
                        conds.append(p)
                lvl = (row.get("LVLV_MinimumLevel") or "").strip()
                try:
                    lvl = float(lvl) if lvl else 0.0
                except ValueError:
                    lvl = 0.0
                bucket = self.entries.setdefault(lid, [])
                bucket.append({"ref": rfid, "sig": rsig, "conds": conds,
                               "minlvl": lvl, "idx": len(bucket)})

    def _load_lists(self, tsv_dir, newest):
        """Per-LIST flags and conditions.

        Two things live here that the entry rows do not have:

        * `LVLF_Flags` -- bit 6 is First Match, which is what makes the level
          complement below sound (see `_below_from_siblings`).
        * `ListCond1..N` -- a condition on the WHOLE list. 168 of them exist and
          they carry real rules: the Mk5 underarmour mod list only pays out once
          you have learned Mk2, several regional loot lists only apply in their
          own region.
        """
        path = self._pick("LVLI_Export_*_LVLI_List.tsv", tsv_dir, newest)
        if not path:
            return
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                lid = (row.get("LVLI_FormID") or "").strip().upper()
                if not lid:
                    continue
                conds = []
                for i in range(1, 11):
                    c = (row.get(f"ListCond{i}") or "").strip()
                    if not c:
                        continue
                    pc = parse_condition(c)
                    if pc:
                        conds.append(pc)
                self.lists[lid] = {"flags": (row.get("LVLF_Flags") or "").strip(),
                                   "conds": conds}

    def _is_first_match(self, lid):
        flags = (self.lists.get((lid or "").upper()) or {}).get("flags") or ""
        return len(flags) > 6 and flags[6] == "1"

    def _load_cobj_names(self, tsv_dir, newest):
        """COBJ FormID -> the name of the thing it crafts.

        A HasLearnedRecipe condition points at a COBJ, and `co_Weapon_Melee_
        DeathclawGauntlet` is not what a player calls it.
        """
        path = self._pick("COBJ_Export_*.tsv", tsv_dir, newest)
        if not path:
            return
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                fid = (row.get("COBJ_FormID") or "").strip().upper()
                full = (row.get("CNAM_FULL") or "").strip()
                if fid and full:
                    self.names[fid] = full

    # -- the walk -----------------------------------------------------------
    def _paths(self, list_fid, target_fid, depth, seen):
        """Yield the entry chain(s) from `list_fid` down to `target_fid`."""
        lid = (list_fid or "").upper()
        if depth > MAX_DEPTH or lid in seen or lid not in self.entries:
            return
        seen = seen | {lid}
        for e in self.entries[lid]:
            if e["ref"] == target_fid:
                yield [(lid, e)]
            elif e["sig"] == "LVLI" and e["ref"]:
                for tail in self._paths(e["ref"], target_fid, depth + 1, seen):
                    yield [(lid, e)] + tail

    def _below_from_siblings(self, lid, entry):
        """"Level N and below", inferred from a First Match list.

        Daily Ops is the case that needs it, and it is written like this:

            LL_DailyOps_Rewards_Chase_Tier01   (First Match)
              entry 0 -> ...Chase_HighLVL   [GetLevel >= 50]
              entry 1 -> ...Chase_LowLVL    (no condition at all)

        A First Match list rolls once and takes the FIRST entry whose condition
        matches, so entry 1 is only ever reached by a player the level gate
        above it did not catch. The low-level pool therefore is "level 49 and
        below" even though nothing in it says so, and printing "None" there
        would tell a max-level reader to go and farm something they can never
        get.

        Deliberately narrow: First Match only, GetLevel only, and only when the
        entry we took carries no level condition of its own. Where several gates
        sit above us the SMALLEST one binds -- that is the first branch that
        catches a player on the way down.
        """
        if not self._is_first_match(lid):
            return None
        if any(c["fn"] == "GetLevel" for c in entry["conds"]):
            return None
        floor = None
        for sib in self.entries.get((lid or "").upper(), ()):
            if sib["idx"] >= entry["idx"]:
                break
            for c in sib["conds"]:
                if c["fn"] != "GetLevel" or c.get("number") is None:
                    continue
                if c["op"] in (OP_GE, OP_GT):
                    n = int(c["number"]) + (1 if c["op"] == OP_GT else 0)
                    floor = n if floor is None else min(floor, n)
        return f"Level {floor - 1} and below" if floor and floor > 1 else None

    def _entry_lines(self, holder, entry, own):
        """Every sentence one entry contributes: the holding list's own
        conditions, the entry's, and the First Match level complement."""
        conds = list((self.lists.get(holder) or {}).get("conds") or [])
        conds += entry["conds"]
        lines, skipped = _merge_or(conds, self.names, own)
        below = self._below_from_siblings(holder, entry)
        if below:
            lines.append(below)
        return lines, skipped

    def _ancestor_lines(self, lid, own, depth=3, seen=frozenset()):
        """Conditions that EVERY way into this list agrees on.

        A route is named after one list, and the walk down from it cannot see a
        gate that sits above it. Daily Ops is exactly that shape: the route
        "Daily Ops - Low Chase Uncommon" is a list whose only way in is the
        low-level branch of a First Match tier list, so every plan in it is
        level 49 and below -- and nothing inside the list says so.

        Only UNANIMOUS conditions are taken. A list reachable two ways, one of
        them ungated, is not gated, and claiming otherwise would be worse than
        saying nothing.
        """
        lid = (lid or "").upper()
        key = (lid, (own or {}).get("book"), depth)
        if key in self._anc_cache:
            return self._anc_cache[key]
        out = []
        if depth > 0 and lid not in seen:
            parents = self.parents.get(lid) or ()
            per_parent = []
            for plid, e in parents:
                lines, _ = self._entry_lines(plid, e, own)
                lines += self._ancestor_lines(plid, own, depth - 1, seen | {lid})
                per_parent.append(set(lines))
            if per_parent:
                common = set.intersection(*per_parent)
                out = sorted(common)
        self._anc_cache[key] = out
        return out

    def for_lists(self, list_fids, target_fid, own=None):
        """(sentences, unreadable_count) for every path from these lists.

        Conditions on every entry along the path count: a condition on the entry
        that points at the sub-list gates everything inside it just as firmly as
        one sitting on the plan's own entry.
        """
        target = (target_fid or "").upper()
        out, skipped, seen_txt = [], 0, set()
        for lid in list_fids or ():
            # What gates the way INTO this source, before anything inside it.
            above = self._ancestor_lines(lid, own)
            for path in self._paths(lid, target, 0, frozenset()):
                for holder, e in path:
                    lines, miss = self._entry_lines(holder, e, own)
                    skipped += miss
                    lines = above + lines
                    above = []
                    # LVLV_MinimumLevel is NOT a player-level gate. On a
                    # creature list it is the level of the thing carrying the
                    # loot -- the Deathclaw entry reads 91 -- and publishing it
                    # as "Level 91 and above" tells a reader to come back at a
                    # level that does not exist for most of them. Only an
                    # explicit GetLevel condition is a statement about the
                    # player, and that is the only one this prints. (Same trap
                    # the drop-rate engine documents for MinLvl GLOBs.)
                    for ln in lines:
                        k = ln.lower()
                        if k not in seen_txt:
                            seen_txt.add(k)
                            out.append(ln)
        return tidy(out)[:MAX_SENTENCES], skipped


# ── attaching to plan_master rows ────────────────────────────────────────────
UNREADABLE_LINE = ("Other in-game conditions apply that could not be put into "
                   "words from the game files")


def attach(items, tsv_dir="tsv", newest=None, stats=None, index=None):
    """Write `conditions` onto every route of every row. Idempotent.

    Each route carries the leveled lists it was named after (`lvli`, written by
    build_plan_obtain_json.resolve_routes), and the conditions are read off the
    entries on the path from those lists down to the plan.
    """
    stats = stats if stats is not None else {}
    idx = index or ConditionIndex(tsv_dir, newest)
    stats["export"] = idx.export
    stats.setdefault("routes", 0)
    stats.setdefault("with_conditions", 0)
    stats.setdefault("unreadable", 0)
    stats.setdefault("no_lists", 0)
    for it in items:
        book = ((it.get("plan_item") or {}).get("formid") or "").upper()
        own = {"recipe": (it.get("cobj") or {}).get("formid") or "",
               "book": book,
               "title": it.get("name") or ""}
        for r in it.get("obtain_routes") or []:
            stats["routes"] += 1
            lists = r.get("lvli") or []
            if not lists:
                stats["no_lists"] += 1
                r["conditions"] = []
                continue
            lines, skipped = idx.for_lists(lists, book, own)
            if skipped and not lines:
                lines = [UNREADABLE_LINE]
                stats["unreadable"] += 1
            elif skipped:
                stats["unreadable"] += 1
            if lines:
                stats["with_conditions"] += 1
            r["conditions"] = lines
    return stats


def report(stats, stream=None):
    import sys as _sys
    stream = stream or _sys.stdout
    print(f"  [conditions] entries from {stats.get('export','?')}", file=stream)
    for k in ("routes", "with_conditions", "unreadable", "no_lists"):
        if k in stats:
            print(f"    {k:18s} {stats[k]}", file=stream)


# ── tidying a route's lines ──────────────────────────────────────────────────
# Conditions reach a route from several entries along the path, so the same
# sentence shape arrives several times over: one "Only in <region>" per region a
# container list covers, or a level gate from two different lists. Four separate
# lines saying "Only in ..." read as four rules that must ALL hold, which is the
# opposite of what the game does.
_MERGE_PREFIXES = ("Only in ", "Not in ", "Only after you learn ",
                   "Only until you learn ", "Only with ", "Only while wearing ")
_LEVEL_RE = re.compile(r"^Level (\d+) and (above|below)$")


def tidy(lines):
    out, groups, levels = [], {}, {}
    for ln in lines:
        m = _LEVEL_RE.match(ln)
        if m:
            n, side = int(m.group(1)), m.group(2)
            # Two floors mean the higher one decides; two ceilings, the lower.
            keep = max if side == "above" else min
            levels[side] = keep(levels.get(side, n), n)
            if side not in groups:
                groups[side] = len(out)
                out.append(ln)
            continue
        for pre in _MERGE_PREFIXES:
            if ln.startswith(pre):
                if pre not in groups:
                    groups[pre] = len(out)
                    out.append(ln)
                else:
                    tail = ln[len(pre):]
                    cur = out[groups[pre]]
                    parts = cur[len(pre):].replace(" or ", ", ").split(", ")
                    if tail not in parts:
                        parts.append(tail)
                    out[groups[pre]] = (pre + ", ".join(parts[:-1]) + " or " + parts[-1]
                                        if len(parts) > 1 else pre + parts[0])
                break
        else:
            out.append(ln)
    for side, n in levels.items():
        out[groups[side]] = f"Level {n} and {side}"
    return out
