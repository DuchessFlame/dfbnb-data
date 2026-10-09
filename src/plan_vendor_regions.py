#!/usr/bin/env python3
r"""
plan_vendor_regions.py — a vendor only sells the regional plans of the region it
stands in.

WHY
---
Vendor plan stock is regional. LL_Recipes_Mods_Armor_AllRegions_Vendor
(003EC605) picks ONE region list per restock with

    Subject.GetInCurrentLocation(RegionToxicValleyLocation) == 1
    Subject.GetInCurrentLocation(RegionMountainLocation)    == 1   ...

so a vendor can only ever sell plans from the list of the region it is in.
The plan pages still listed every faction vendor on every regional plan:
Plan: Pocketed Metal Armor Chest showed the Whitespring bots with "Only in
Toxic Valley, Skyline Valley or Burning Springs" — a shop that can never meet
its own condition.

HOW A VENDOR'S PLACE IS FOUND (every build, nothing per-vendor is hardcoded)
---------------------------------------------------------------------------
Inputs: NPC2_Vendors_<date>.tsv (NPC -> merchant chest),
NPC2_Vendors_<date>_Placements.tsv (where each vendor NPC stands: cell, X/Y,
Location), LCTN_Export_*_LCTN.tsv (PNAM parent chain) and _LCSR.tsv (which
locations own refs in each exterior grid cell), data/mappalachia_geo.json
(region outlines) and data/vendors/geo_cache.json (Mappalachia's interior ->
outside-door lookup), data/vendors/vendor_overrides.tsv.

For each placement, in this order — the game's own data first, because the
condition IS the game's location check:

  1. The placement's Location, walked up PNAM_ParentLocation. Every location on
     the way counts (a condition can name a sub-location, not only a region).
  2. Exterior with no Location: the exterior grid cell (X,Y / 4096) -> the
     locations that own refs there (LCSR) -> their regions. Used only when
     that gives exactly ONE region.
  3. Still no region: Mappalachia. Exterior -> the region outline the point
     sits in; interior -> the region of its outside door (geo_cache.json).
     Marked `via: mappalachia` in the report: the game's own chain did not
     reach a region, so whether its check passes in game is less certain
     (The Whitespring is the case: Mall -> The Whitespring -> Appalachia).

When steps 1/2 and the Mappalachia outline disagree, the game's answer is used
and the disagreement is reported (Duncan & Duncan Robotics: chain says The
Forest, Mappalachia's door says Cranberry Bog; The Forest was checked in game).

RULES, NOT NAME LISTS
---------------------
  * Placed in 76HoldingCellCompanion          -> OPEN (camp ally / companion:
                                                 stands wherever the camp is)
  * Placed in 76HoldingCellRE                 -> OPEN (random encounter:
                                                 spawns near the player)
  * Chest WorkshopVendorChestBaseMisc         -> not a shop (workshop corpse)
  * QA / test / DELETED_ / zzz / debug records -> ignored
  * Any other holding cell                    -> the override file's region,
    if it has one; otherwise UNKNOWN (kept on the page, reported).

Overrides (data/vendors/vendor_overrides.tsv) apply ONLY while the vendor has
no real placement. If an overridden vendor turns up standing in the world, the
data wins and the build warns that the override line is stale.

WHAT IT DOES TO A ROW
---------------------
Only vendor routes whose path down to the plan carries a location condition
are judged. For each stock list on the route, the vendors whose chest holds it
are checked against the conditions on every path; a list is kept if ANY of its
vendors can pass ANY path. OPEN and UNKNOWN vendors always pass.

  * some lists dropped -> `lvli` loses them and a grouped label
    ("Responders vendors (Camden Park, ...)") loses those places
  * every list dropped -> the route moves to `retired_routes` with
    retired_because ["no vendor stands in that region"], like prune_dead_routes

No rate is computed or changed (plan-obtain-pipeline: this pipeline owns no
rate maths). Runs in build_plan_obtain_json.py after prune_dead_routes.
Standalone over a finished file:

    python src/plan_vendor_regions.py dist/plan_master.json --data-dir tsv --diff
    python src/plan_vendor_regions.py dist/plan_master.json --data-dir tsv --write
    python src/plan_vendor_regions.py --vendors --data-dir tsv   # where is every vendor
"""
import argparse
import collections
import csv
import json
import math
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import tsv_source            # noqa: E402
import plan_conditions       # noqa: E402
import plan_sources          # noqa: E402

csv.field_size_limit(1 << 30)

OPEN, EXCLUDED, IGNORED, UNKNOWN, PLACED = "open", "excluded", "ignored", "unknown", "placed"

# Location-check functions whose parameter is an LCTN. GetIsInRegion takes a
# REGN (weather/map region), not a location, and is left alone.
_LOC_FNS = {"GetInCurrentLocation", "GetIsEditorLocation"}

# Holding-cell rules. Matched on the cell EditorID, case-insensitive.
_OPEN_CELLS = (re.compile(r"^76HoldingCellCompanion", re.I),
               re.compile(r"^76HoldingCellRE$", re.I))
_TEST_CELLS = re.compile(r"(^76QA|QASmoke|test|^Warehouse|debug)", re.I)
_HOLDING = re.compile(r"HoldingCell", re.I)
_CORPSE_CHEST = re.compile(r"^WorkshopVendorChestBaseMisc$", re.I)
# zzz is NOT here: Bethesda ships live records with it (the Season 5 allies
# Daphne and Maul sit in the companion cell as zzzSCORE_S5_*). Cut records are
# prune_dead_routes' job, not this module's.
_DEV_NPC = re.compile(r"^(DELETED_|QA_|test_|Debug|TEMP_)", re.I)

DROP_REASON = "no vendor stands in that region"


def _newest(pat, tsv_dir):
    return tsv_source.newest(os.path.join(tsv_dir, pat), required=False)


def _rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


def _u(s):
    return (s or "").strip().upper()


def _region_key(name):
    """'The Forest' / 'Forest' -> 'forest' so Mappalachia, the override file and
    LCTN FULL names line up."""
    n = re.sub(r"^the\s+", "", (name or "").strip(), flags=re.I).lower()
    return n


class Locations:
    """LCTN parent chains and the Region*Location records."""

    def __init__(self, tsv_dir):
        self.parent, self.edid, self.full = {}, {}, {}
        p = _newest("LCTN_Export_*_LCTN.tsv", tsv_dir)
        if not p:
            raise FileNotFoundError("LCTN_Export_*_LCTN.tsv")
        for r in _rows(p):
            fid = _u(r.get("LCTN_FormID"))
            self.edid[fid] = r.get("LCTN_EDID") or ""
            self.full[fid] = r.get("LCTN_FULL") or ""
            self.parent[fid] = _u((r.get("PNAM_ParentLocation") or "").split(":")[0])
        self.regions = {f for f, e in self.edid.items()
                        if re.match(r"^Region\w+Location$", e)}
        self.region_by_key = {_region_key(self.full[f]): f for f in self.regions}
        # exterior grid cell -> owning locations (LCSR)
        self.grid = collections.defaultdict(set)
        p = _newest("LCTN_Export_*_LCSR.tsv", tsv_dir)
        if p:
            for r in _rows(p):
                if "APPALACHIA" in (r.get("WorldCell") or ""):
                    self.grid[(r.get("GridX"), r.get("GridY"))].add(_u(r.get("LCTN_FormID")))

    def chain(self, fid):
        out, fid = [], _u(fid)
        while fid and fid not in out:
            out.append(fid)
            fid = self.parent.get(fid, "")
        return out

    def region_of(self, fid):
        for f in self.chain(fid):
            if f in self.regions:
                return f
        return None

    def name(self, fid):
        return self.full.get(fid) or self.edid.get(fid) or fid


class Mappalachia:
    """Region outlines + Mappalachia's interior door lookup (cached JSON)."""

    def __init__(self):
        geo = os.path.join(REPO, "data", "mappalachia_geo.json")
        self.rings = {}
        if os.path.exists(geo):
            with open(geo, encoding="utf-8") as fh:
                g = json.load(fh)
            for reg, rings in (g.get("rings") or {}).items():
                rs = rings if rings and isinstance(rings[0][0], list) else [rings]
                self.rings[reg] = rs
        cache = os.path.join(REPO, "data", "vendors", "geo_cache.json")
        self.door = {}
        if os.path.exists(cache):
            with open(cache, encoding="utf-8") as fh:
                self.door = json.load(fh)

    @staticmethod
    def _inside(x, y, ring):
        c = False
        for i in range(len(ring)):
            x1, y1 = ring[i]
            x2, y2 = ring[i - 1]
            if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
                c = not c
        return c

    def outline(self, x, y):
        hits = [reg for reg, rs in self.rings.items() if any(self._inside(x, y, r) for r in rs)]
        return hits[0] if len(hits) == 1 else None

    def by_ref(self, ref_fid):
        try:
            g = self.door.get(str(int(ref_fid, 16)))
        except (TypeError, ValueError):
            return None
        return (g or {}).get("region")


def load_overrides():
    path = os.path.join(REPO, "data", "vendors", "vendor_overrides.tsv")
    out = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#") or line.startswith("edid\t") or not line.strip():
                continue
            f = (line.rstrip("\n").split("\t") + [""] * 5)[:5]
            out[f[0].strip()] = {"exclude": f[1].strip().lower() == "x",
                                 "marker": f[2].strip(), "region": f[3].strip(),
                                 "note": f[4].strip()}
    return out


class VendorRegions:
    """Where every vendor stands, per merchant chest."""

    def __init__(self, tsv_dir):
        self.tsv_dir = tsv_dir
        self.loc = Locations(tsv_dir)
        self.map = Mappalachia()
        self.overrides = load_overrides()
        self.warnings = []
        # NPC -> chest base, chest base -> NPCs
        self.npc_edid, self.chest_npcs = {}, collections.defaultdict(set)
        vend = _newest("NPC2_Vendors_*.tsv", tsv_dir)
        if not vend:
            raise FileNotFoundError("NPC2_Vendors_*.tsv")
        vend = vend.replace("_Placements", "")
        for r in _rows(vend):
            n, c = _u(r.get("NPC_FormID")), _u(r.get("MerchantContainerBase_FormID"))
            self.npc_edid[n] = r.get("NPC_EDID") or ""
            if c:
                self.chest_npcs[c].add(n)
        pl = vend.replace(".tsv", "_Placements.tsv")
        self.placements = collections.defaultdict(list)
        for r in _rows(pl):
            self.placements[_u(r.get("NPC_FormID"))].append(r)
        self.vend_path, self.place_path = vend, pl
        # Interior cell -> the region Mappalachia gives ANY ref placed in it, so a
        # vendor whose own ref is not in the door cache (Giuseppe's Refuge stool,
        # XPD_Hub_Furniture_Giuseppe) still gets the cell's answer.
        self._cell_door = {}
        for rs in self.placements.values():
            for r in rs:
                if (r.get("Worldspace_EDID") or "") or not r.get("Cell_FormID"):
                    continue
                reg = self.map.by_ref(r.get("Ref_FormID"))
                if reg:
                    self._cell_door.setdefault(_u(r.get("Cell_FormID")), reg)
        self._npc_cache = {}
        # LVLI -> holders, for stock list -> chest
        self.refs = collections.defaultdict(list)
        p = _newest("LVLI_Export_*_LVLI_Refs.tsv", tsv_dir)
        self.lvli_edid = {}
        for r in _rows(p):
            lid = _u(r.get("LVLI_FormID"))
            self.lvli_edid[lid] = r.get("LVLI_EDID") or ""
            for k, v in r.items():
                if k and k.startswith("Ref") and k != "ReferencedByCount" and v:
                    bits = v.split(":")
                    if len(bits) >= 3:
                        self.refs[lid].append((_u(bits[0]), bits[1], bits[-1].strip().upper()))

    # -- one placement -----------------------------------------------------
    def _place(self, p):
        """(status, chain-of-LCTN-FormIDs, region FormID, via, detail)."""
        cell = p.get("Cell_EDID") or ""
        npc = p.get("NPC_EDID") or ""
        if _CORPSE_CHEST.match(p.get("Ref_EDID") or ""):
            return EXCLUDED, [], None, "rule", "workshop corpse chest"
        if any(rx.match(cell) for rx in _OPEN_CELLS) and not _DEV_NPC.match(npc):
            return OPEN, [], None, "rule", cell
        if _DEV_NPC.match(npc) or _TEST_CELLS.search(cell):
            return IGNORED, [], None, "rule", "test / deleted record"
        if _HOLDING.search(cell) and not p.get("Location_FormID"):
            return UNKNOWN, [], None, "holding cell", cell
        chain = self.loc.chain(p.get("Location_FormID")) if p.get("Location_FormID") else []
        reg = next((f for f in chain if f in self.loc.regions), None)
        via = "location" if reg else ""
        ext = (p.get("Worldspace_EDID") or "").upper() == "APPALACHIA"
        x = y = None
        if ext:
            try:
                x, y = float(p.get("X")), float(p.get("Y"))
            except (TypeError, ValueError):
                ext = False
        if not reg and ext:
            g = (str(math.floor(x / 4096)), str(math.floor(y / 4096)))
            regs = {self.loc.region_of(f) for f in self.loc.grid.get(g, ())} - {None}
            if len(regs) == 1:
                reg, via = regs.pop(), "grid"
        # Mappalachia: fallback, and a cross-check
        m_name = ((self.map.outline(x, y) if ext else None)
                  or self.map.by_ref(p.get("Ref_FormID"))
                  or (None if ext else self._cell_door.get(_u(p.get("Cell_FormID")))))
        m_reg = self.loc.region_by_key.get(_region_key(m_name)) if m_name else None
        if reg and m_reg and m_reg != reg:
            self.warnings.append(
                f"region disagrees for {npc} ({cell or 'exterior'}): game data "
                f"{self.loc.name(reg)}, Mappalachia {self.loc.name(m_reg)} — using game data")
        if not reg and m_reg:
            reg, via = m_reg, "mappalachia"
        if not reg:
            return UNKNOWN, chain, None, "no region", cell or "exterior"
        if reg not in chain:
            chain = chain + [reg]
        return PLACED, chain, reg, via, cell or "exterior"

    # -- one NPC -----------------------------------------------------------
    def npc(self, npc_fid):
        """{'status', 'places': [(chain, region, via, detail)], 'edid'}"""
        npc_fid = _u(npc_fid)
        if npc_fid in self._npc_cache:
            return self._npc_cache[npc_fid]
        edid = self.npc_edid.get(npc_fid, "")
        places, flags = [], set()
        for p in self.placements.get(npc_fid, ()):
            st, chain, reg, via, detail = self._place(p)
            flags.add(st)
            if st == PLACED:
                places.append((chain, reg, via, detail))
        ov = self.overrides.get(edid)
        if places:
            status = PLACED
            if ov and (ov["region"] or ov["exclude"]):
                self.warnings.append(f"stale override: {edid} now stands in "
                                     f"{', '.join(sorted({self.loc.name(r) for _c, r, _v, _d in places}))}"
                                     f" — the data wins; remove its line from vendor_overrides.tsv")
        elif OPEN in flags:
            status = OPEN
        elif ov and ov["exclude"]:
            status = EXCLUDED
        elif ov and ov["region"]:
            reg = self.loc.region_by_key.get(_region_key(ov["region"]))
            if reg:
                status = PLACED
                places = [([reg], reg, "override", ov["marker"] or ov["region"])]
            else:
                status = UNKNOWN
                self.warnings.append(f"override region not recognised for {edid}: {ov['region']!r}")
        elif EXCLUDED in flags and not (flags - {EXCLUDED, IGNORED}):
            status = EXCLUDED
        elif flags and not (flags - {IGNORED, EXCLUDED}):
            status = IGNORED
        elif not flags and _DEV_NPC.match(edid):
            status = IGNORED
        else:
            status = UNKNOWN
        out = {"status": status, "places": places, "edid": edid}
        self._npc_cache[npc_fid] = out
        return out

    # -- stock list -> chests ---------------------------------------------
    def chests_for_list(self, lid, depth=0, seen=None):
        seen = seen if seen is not None else set()
        lid = _u(lid)
        if depth > 4 or lid in seen:
            return set()
        seen.add(lid)
        found = {rf for rf, _e, rs in self.refs.get(lid, ()) if rs == "CONT" and rf in self.chest_npcs}
        if found:
            return found
        for rf, _e, rs in self.refs.get(lid, ()):
            if rs == "LVLI":
                found |= self.chests_for_list(rf, depth + 1, seen)
        return found


# ── the conditions ───────────────────────────────────────────────────────────
def _path_rule(idx, path):
    """The location test one path from a stock list down to the plan applies.
    A list of OR-groups that must all hold; each group is [(lctn_fid, want)].
    Empty list = no location condition on this path."""
    conds = []
    for holder, e in path:
        conds += list((idx.lists.get(holder) or {}).get("conds") or [])
        conds += e["conds"]
    groups, cur = [], []
    for c in conds:
        cur.append(c)
        if not c.get("or_next"):
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    out = []
    for g in groups:
        tests = []
        for c in g:
            if c["fn"] not in _LOC_FNS:
                continue
            ref = next((r for r in c.get("refs") or [] if r.get("sig") == "LCTN"), None)
            want = plan_conditions._truthy(c)
            if ref and want is not None:
                tests.append((_u(ref["fid"]), want))
        # A group that mixes a location test with something else cannot be
        # judged on location alone: leave it out (it can only make us KEEP more).
        if tests and len(tests) == len(g):
            out.append(tests)
    return out


def _passes(rule, chain):
    s = set(chain)
    return all(any((fid in s) == want for fid, want in grp) for grp in rule)


# ── the rows ─────────────────────────────────────────────────────────────────
_RX_GROUP = re.compile(r"^(?P<qual>.*?\bvendors)\s*\((?P<places>.*)\)$")
_RX_SINGLE = re.compile(r"^(?P<head>.*?)\s*\((?:(?P<place>[^(),]+),\s*)?(?P<qual>[^(),]*\bvendor)\)$")


class Judge:
    def __init__(self, tsv_dir):
        self.vr = VendorRegions(tsv_dir)
        self.idx = plan_conditions.ConditionIndex(tsv_dir, lambda pat, root: _newest(pat, root))
        self._place_names = {}

    def _list_verdict(self, lid, book):
        """(keep?, judged?, detail). judged False = no location condition."""
        rules = [_path_rule(self.idx, path) for path in self.idx._paths(_u(lid), book, 0, frozenset())]
        if not rules or any(not r for r in rules):
            return True, False, "no location condition"
        chests = self.vr.chests_for_list(lid)
        if not chests:
            return True, False, "no vendor chest found"
        npcs = set().union(*(self.vr.chest_npcs[c] for c in chests))
        statuses = []
        for n in npcs:
            v = self.vr.npc(n)
            statuses.append(v)
            if v["status"] in (OPEN, UNKNOWN):
                return True, True, v["status"]
            if v["status"] == PLACED and any(_passes(r, ch) for r in rules for ch, *_ in v["places"]):
                return True, True, "in region"
        return False, True, "; ".join(sorted({
            f"{v['edid']}: " + (", ".join(sorted({self.vr.loc.name(rg) for _c, rg, _v, _d in v['places']}))
                                 if v["places"] else v["status"]) for v in statuses}))

    def place_of_list(self, lid):
        """The place word(s) a grouped label uses for this stock list's vendor.

        Named by the build's OWN functions (build_plan_obtain_json: the chest
        label, then the place half of it exactly as group_vendor_rows() reads
        it), so the words match the label character for character — including
        Bethesda's own spellings ("Plesant Valley Station", "Rand Station")."""
        lid = _u(lid)
        if lid in self._place_names:
            return self._place_names[lid]
        B = _build_module(self.vr.tsv_dir)
        ce = B.chest_edids()
        out = set()
        for c in self.vr.chests_for_list(lid):
            name = B.better_vendor_label(B.source_label(ce.get(c, "")),
                                         B.source_label(self.vr.lvli_edid.get(lid, "")))
            label = B.vendor_names().label(c, name) if name else None
            m = B._RX_VENDOR_ROW.match(label or "")
            if m:
                out.add((m.group("place") or m.group("head") or "").strip())
        self._place_names[lid] = out
        return out

    def apply(self, it):
        """Mutates one plan row. Returns a list of change dicts."""
        changes = []
        book = _u((it.get("plan_item") or {}).get("formid"))
        if not book:
            return changes
        keep_routes, gone = [], []
        for r in it.get("obtain_routes") or []:
            if r.get("source_type") != "vendor" or not r.get("lvli"):
                keep_routes.append(r)
                continue
            verdicts = {l: self._list_verdict(l, book) for l in r["lvli"]}
            dropped = [l for l, (k, _j, _d) in verdicts.items() if not k]
            if not dropped:
                keep_routes.append(r)
                continue
            kept = [l for l in r["lvli"] if l not in dropped]
            why = {l: verdicts[l][2] for l in dropped}
            if not kept:
                gone.append(dict(r, retired_because=[DROP_REASON], region_detail=why))
                changes.append({"plan": it.get("name"), "before": r["route"], "after": None,
                                "why": why})
                continue
            new = dict(r, lvli=kept)
            new["route"] = self.relabel(r["route"], kept, dropped)
            changes.append({"plan": it.get("name"), "before": r["route"], "after": new["route"],
                            "why": why})
            keep_routes.append(new)
        if gone or changes:
            it["obtain_routes"] = keep_routes
            if gone:
                it["retired_routes"] = (it.get("retired_routes") or []) + gone
            it["obtain_ledger"] = plan_sources.obtain_ledger(it)
        return changes

    def relabel(self, label, kept, dropped):
        """Take the dropped vendors' places out of a grouped label. A place is
        removed only when every list behind it was dropped."""
        m = _RX_GROUP.match(label or "")
        if not m:
            return label
        places = [p.strip() for p in m.group("places").split(",") if p.strip()]
        keep = set().union(*(self.place_of_list(l) for l in kept)) if kept else set()
        drop = set().union(*(self.place_of_list(l) for l in dropped)) if dropped else set()
        unmatched = drop - set(places)
        if unmatched:
            self.vr.warnings.append(f"label place not found for {sorted(unmatched)} in {label!r}")
        left = [p for p in places if not (p in drop and p not in keep)]
        if not left or len(left) == len(places):
            return label
        qual = m.group("qual")
        if len(left) == 1:
            return f"{left[0]} ({qual[:-1]})"
        return f"{qual} ({', '.join(left)})"


_BUILD = None


def _build_module(tsv_dir):
    """build_plan_obtain_json, pointed at this export root. Imported lazily:
    the build imports THIS module, and when the build runs as __main__ the
    import here is a second copy whose TSV must be set explicitly."""
    global _BUILD
    if _BUILD is None:
        import build_plan_obtain_json as B
        _BUILD = B
    if os.path.abspath(_BUILD.TSV) != os.path.abspath(tsv_dir):
        _BUILD.TSV = os.path.abspath(tsv_dir)
    return _BUILD


def _squash(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def attach(items, tsv_dir, judge=None):
    j = judge or Judge(tsv_dir)
    stats = collections.Counter()
    log = []
    for it in items:
        ch = j.apply(it)
        if ch:
            stats["plans"] += 1
            stats["routes_dropped"] += sum(1 for c in ch if c["after"] is None)
            stats["routes_relabelled"] += sum(1 for c in ch if c["after"] is not None)
            log.extend(ch)
    stats["_log"] = log
    stats["_warnings"] = sorted(set(j.vr.warnings))
    return stats


def report(stats, stream=None):
    stream = stream or sys.stdout
    log = stats.pop("_log", [])
    warns = stats.pop("_warnings", [])
    print(f"  vendor regions: {stats.get('routes_dropped', 0)} route(s) retired, "
          f"{stats.get('routes_relabelled', 0)} trimmed, on {stats.get('plans', 0)} plan(s)",
          file=stream)
    for w in warns:
        print(f"    WARNING: {w}", file=stream)
    for c in log[:6]:
        print(f"    e.g. {c['plan']}: {c['before']} -> {c['after'] or '(retired)'}", file=stream)


def vendor_table(tsv_dir):
    vr = VendorRegions(tsv_dir)
    out = []
    for n in sorted(vr.placements, key=lambda f: vr.npc_edid.get(f, "")):
        v = vr.npc(n)
        regs = sorted({f"{vr.loc.name(r)} [{via}]" for _c, r, via, _d in v["places"]})
        out.append((v["edid"], v["status"], ", ".join(regs)))
    return out, vr.warnings


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("plan_master", nargs="?")
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--diff", action="store_true", help="print before/after, write nothing")
    ap.add_argument("--write", action="store_true", help="write the file back")
    ap.add_argument("--vendors", action="store_true", help="print every vendor's region")
    ap.add_argument("--diff-out", default="", help="also save the diff as TSV here")
    a = ap.parse_args(argv)
    if a.vendors:
        rows, warns = vendor_table(a.data_dir)
        for r in rows:
            print("\t".join(r))
        for w in sorted(set(warns)):
            print("WARNING:", w)
        return 0
    with open(a.plan_master, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [], a.data_dir)
    log = list(stats["_log"])
    if a.diff or a.diff_out:
        lines = ["plan\tbefore\tafter\twhy"]
        for c in log:
            lines.append("\t".join([c["plan"] or "", c["before"], c["after"] or "(removed)",
                                    " | ".join(f"{k}: {v}" for k, v in c["why"].items())]))
        if a.diff_out:
            with open(a.diff_out, "w", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        if a.diff:
            print("\n".join(lines))
    report(stats)
    if a.write:
        with open(a.plan_master, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
