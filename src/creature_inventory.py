#!/usr/bin/env python3
r"""
creature_inventory.py — the Inventory and Creatures root expands for creature
pages (Farming - Meat, Farming - Insects, Farming - Critters).

  Inventory   everything the page's creature drops when you kill it, one column
              per creature variant whose drops differ (Scorchbeast / Uninvited
              Scorchbeast / Scorchbeast Queen), in four groups, A–Z inside each:
                Meat & Bits   raw meat (ALCH MealTypeRaw + IngredientTypeMeat) and
                              the creature bits listed in data/creature_parts.tsv
                Plans         every BOOK (Plan: / Recipe: / Player Title)
                Junk          every MISC that is not a loose mod or a bit
                Ammo          every AMMO (contextual — matches the equipped weapon)
                Legendary     the legendary rolls (LGDI), "3★ Legendary Ranged Weapon"
              and, in sub-expands: Ammo, Armour, Chems, Food & Drink, Junk,
              Loose Mods, Weapons (and Other for anything else).

  Creatures   every creature in the game whose death list can drop THIS page's own
              meat and bits — including other species (Blue Devil drops Mongrel Dog
              and Wolf meat, Thunder Crab drops Hermit Crab meat, …).

Rates are rng76 only (drop-rate-engine): appearance_prob = chance the item is in
the body at least once per kill. Plan rates are joined from dist/plan_master.json
so a plan reads the same here as on its plan page; a plan the plan pipeline does
not list falls back to rng76. Nothing is typed: columns come from the NPC export
(INAM death list -> NPC FULL name), groups from record type + keywords.

Cell notes come from the leveled-list path to the item:
  * a MinLvl_*_ECON GLOB on the path   -> "Level N+"
  * a GetInCurrentLocation region test -> "Only in {Region}" (when every path to
    the item has one)
"""
import rate_format
import csv
import collections
import json
import os
import re

csv.field_size_limit(10 ** 9)

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
PARTS_TSV = os.path.join(REPO, "data", "creature_parts.tsv")

# Shown open, at the top of the expand, in this order.
GROUPS = ["Meat & Bits", "Plans", "Legendary"]
# Long lists, each in its own sub-expand below them. "Other Plans" = plans that
# also drop from somewhere else (Duchess, Oct 2026): only the plans that come
# from this creature alone stay open in "Plans".
SUB_GROUPS = ["Other Plans", "Ammo", "Armour", "Chems", "Food & Drink", "Junk", "Loose Mods",
              "Weapons", "Other"]
GROUP_NOTES = {
    "Ammo": "Ammo drops match the weapon you have equipped.",
}
_REGION_RE = re.compile(r'GetInCurrentLocation\([^"]*"([^"]+)"')
# The game's location names -> the site's region names.
_REGION_NAMES = {"The Forest": "Forest"}
# The eight Appalachia regions a region-locked loot list can name.
MAIN_REGIONS = ["Ash Heap", "Burning Springs", "Cranberry Bog", "Forest",
                "Savage Divide", "Skyline Valley", "The Mire", "Toxic Valley"]
_SKIP_PLAN_CONDS = {"Stops dropping once you learn it"}


def _pick(pattern, exclude=()):
    """Newest export by its name's date (tsv_source, never mtime)."""
    import glob
    import tsv_source
    files = [f for f in glob.glob(os.path.join(TSV, pattern))
             if not any(x in os.path.basename(f) for x in exclude)]
    if not files:
        return None
    return sorted(files, key=tsv_source.export_key)[-1]


def _cols(path, wanted):
    """Read only the wanted columns (some exports are thousands of columns wide)."""
    out = []
    if not path:
        return out
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter="\t")
        h = next(r)
        idx = {w: h.index(w) for w in wanted if w in h}
        for row in r:
            out.append({w: (row[i].strip() if i < len(row) else "") for w, i in idx.items()})
    return out


def pct(rate):
    """Display a 0-1 rate. Uses rate_format.fmt_rate, the one formatter for the
    whole site (up to 4 decimals, trailing zeros trimmed, <0.0001% floor).
    Duchess, 31 Oct 2026: this file had its own 2-decimal / 4-decimal / <0.01%
    rules, so one Inventory table mixed 0.63% with 0.0378%."""
    if rate is None:
        return ""
    return rate_format.fmt_rate(rate)


class Ctx:
    """Everything the two expands need, loaded once for a whole build."""

    def __init__(self, data):
        self.data = data
        self.res = data.resolver
        self.lvli = data.lvli
        self.globs = data.globs
        # ALCH keywords + names
        self.alch = {}
        for r in _cols(_pick("ALCH_Export_*.tsv", ["_Effects"]),
                       ["ALCH_FormID", "FULL", "Keywords_Flat"]):
            self.alch[r["ALCH_FormID"].upper()] = (r.get("FULL", ""), r.get("Keywords_Flat", ""))
        self.misc = {}
        for r in _cols(_pick("MISC_Export_*.tsv"), ["FormID", "FULL", "Keywords"]):
            self.misc[r["FormID"].upper()] = (r.get("FULL", ""), r.get("Keywords", ""))
        self.book = {}
        for r in _cols(_pick("BOOK_Export_*.tsv", ["_Locations", "BOOK2"]),
                       ["FormID", "FULL"]):
            self.book[r["FormID"].upper()] = r.get("FULL", "")
        # There is no AMMO export; the Tinker's Bench recipes name every ammo type.
        self.ammo = {}
        for r in _cols(_pick("COBJ_Export_*.tsv", ["COBJ2"]),
                       ["CNAM_FormID", "CNAM_FULL"]):
            if r.get("CNAM_FULL"):
                self.ammo.setdefault(r["CNAM_FormID"].upper(), r["CNAM_FULL"])
        # Legendary rolls: FULL is "¬¬¬ Legendary Ranged Weapon" (the stars
        # arrive as ¬). Site style: "3★ Legendary Ranged Weapon".
        self.lgdi = {}
        for r in _cols(_pick("LGDI_Export_*.tsv", ["_Mods"]), ["LGDI_FormID", "LGDI_EDID", "FULL"]):
            full = r.get("FULL", "")
            m = re.search(r"Rank(\d)", r.get("LGDI_EDID", ""))
            stars = int(m.group(1)) if m else len(re.match(r"^[¬★\*]*", full).group(0))
            rest = re.sub(r"^[¬★\*\s]+", "", full).replace("Armor", "Armour")
            rest = re.sub(r"^Legendary\s+", "", rest)
            if rest:
                self.lgdi[r["LGDI_FormID"].upper()] = (f"{stars}★ Legendary {rest}" if stars
                                                      else f"Legendary {rest}")
        self.parts = {}
        if os.path.exists(PARTS_TSV):
            for ln in open(PARTS_TSV, encoding="utf-8"):
                if ln.startswith("#") or ln.startswith("form_id") or not ln.strip():
                    continue
                p = ln.rstrip("\n").split("\t")
                self.parts[p[0].upper()] = {"name": p[3], "owner": p[4] if len(p) > 4 else ""}
        # NPC names per death list (INAM), dev/test NPCs ignored
        self.list_names = collections.defaultdict(collections.Counter)
        self.list_races = collections.defaultdict(collections.Counter)
        for r in _cols(_pick("NPC_Export_*.tsv", ["_Refs", "_PRPS"]),
                       ["EDID", "FULL", "INAM_FormID", "RNAM_Name"]):
            if re.match(r"(?i)(test|debug|zzz|audiotemplate)", r.get("EDID", "")):
                continue
            if r.get("INAM_FormID") and r.get("FULL"):
                self.list_names[r["INAM_FormID"].upper()][r["FULL"]] += 1
                if r.get("RNAM_Name"):
                    self.list_races[r["INAM_FormID"].upper()][r["RNAM_Name"]] += 1
        # Plan rates by death list, from the plan pipeline
        self.plan_rates = collections.defaultdict(dict)
        self.plan_sources = {}
        pm = os.path.join(DIST, "plan_master.json")
        if os.path.exists(pm):
            for p in json.load(open(pm, encoding="utf-8")).get("items", []):
                fid = ((p.get("plan_item") or {}).get("formid") or "").upper()
                if not fid:
                    continue
                # Every source the plan has (any kind), as the leveled lists behind
                # each one: a plan whose sources are ALL this page's creature is
                # "unique" to it (open Plans group); anything else is Other Plans.
                self.plan_sources[fid] = [{x.upper() for x in (rt.get("lvli") or [])}
                                          for rt in (p.get("obtain_routes") or [])]
                for rt in p.get("obtain_routes") or []:
                    if rt.get("source_type") != "creature":
                        continue
                    conds = [c for c in (rt.get("conditions") or []) if c not in _SKIP_PLAN_CONDS]
                    for lid in rt.get("lvli") or []:
                        self.plan_rates[lid.upper()][fid] = (rt.get("rate"), conds)
        self._walk_cache = {}
        self._appear = {}
        self._lld = None

    # ── naming / grouping ───────────────────────────────────────────────
    def list_name(self, lid, base=""):
        """The creature name for a death list: the page's own creature name when an
        NPC using the list carries it, else the shortest name that contains it
        ("Mole Rat Brood Mother"), else the name most NPCs on the list use. A list
        no named NPC uses falls back to its EDID, made readable."""
        c = self.list_names.get(lid.upper())
        if not c:
            ed = self.lvli.edid_for(lid.upper()) or lid
            ed = re.sub(r"^(\w+_)?LLD_Creature_", "", ed)
            return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", ed.replace("_", " ")).strip()
        if base:
            b = base.lower()
            if any(n.lower() == b for n in c):
                return next(n for n in c if n.lower() == b)
            rx = re.compile(r"\b" + re.escape(b) + r"\b", re.I)
            hits = sorted((n for n in c if rx.search(n)), key=len)
            if hits:
                return hits[0]
        # The name the others are built on ("Deathclaw" under "Glowing Deathclaw",
        # "Alpha Deathclaw"), else the shortest.
        names = list(c)
        return sorted(names, key=lambda n: (-sum(n.lower() in m.lower() for m in names),
                                            len(n), n))[0]

    def item_name(self, sig, fid, fallback=""):
        if sig == "ALCH":
            return self.alch.get(fid, (fallback, ""))[0] or fallback
        if sig == "MISC":
            return self.misc.get(fid, (fallback, ""))[0] or fallback
        if sig == "BOOK":
            return self.book.get(fid) or fallback
        if sig == "AMMO":
            return self.ammo.get(fid) or fallback
        if sig == "LGDI":
            return self.lgdi.get(fid) or fallback
        n = self.data.names.resolve(fid) if self.data.names else ""
        return n if n and n != fid else fallback

    def is_meat(self, fid):
        kw = self.alch.get(fid, ("", ""))[1]
        return "MealTypeRaw" in kw and "IngredientTypeMeat" in kw

    def group(self, sig, fid):
        if fid in self.parts or (sig == "ALCH" and self.is_meat(fid)):
            return "Meat & Bits"
        if sig == "BOOK":
            return "Plans"
        if sig == "LGDI":
            return "Legendary"
        if sig == "AMMO":
            return "Ammo"
        if sig == "ARMO":
            return "Armour"
        if sig == "WEAP":
            return "Weapons"
        if sig == "ALCH":
            return "Chems" if "ObjectTypeChem" in self.alch.get(fid, ("", ""))[1] else "Food & Drink"
        if sig == "MISC":
            return "Loose Mods" if "ObjectTypeLooseMod" in self.misc.get(fid, ("", ""))[1] else "Junk"
        return "Other"

    # ── leveled-list walk: leaf -> path notes ───────────────────────────
    def walk(self, lid, depth=0, ctx=(frozenset(), 0), seen=frozenset()):
        """{fid: (sig, [(regions, minlvl), ...])} for every leaf under lid."""
        out = {}
        if lid in seen or depth > 50:
            return out
        seen = seen | {lid}
        for e in self.lvli.entries_by_list.get(lid, []):
            ref = (e.get("LVLO_Reference") or "").strip()
            if ":" not in ref:
                continue
            parts = ref.split(":")
            fid, sig = parts[0].upper(), parts[-1]
            regions, minlvl = ctx
            for i in range(1, 11):
                m = _REGION_RE.search(e.get(f"Cond{i}") or "")
                if m:
                    regions = regions | {_REGION_NAMES.get(m.group(1), m.group(1))}
            for col in ("LVOG_ChanceNoneGlobal", "LVLG_MinimumLevelGlobal"):
                raw = e.get(col) or ""
                if "MinLvl" in raw:
                    g = self.globs.value(raw.split(":")[0])
                    if g:
                        minlvl = max(minlvl, int(g))
            c2 = (regions, minlvl)
            if sig == "LVLI":
                for k, (s, paths) in self.walk(fid, depth + 1, c2, seen).items():
                    out.setdefault(k, (s, []))[1].extend(paths)
            else:
                out.setdefault(fid, (sig, []))[1].append(c2)
        return out

    def appear(self, lid, fid):
        k = (lid, fid)
        if k not in self._appear:
            self._appear[k] = self.res.appearance_prob(lid, fid)
        return self._appear[k]

    def leaves(self, lid):
        if lid not in self._walk_cache:
            self._walk_cache[lid] = self.walk(lid)
        return self._walk_cache[lid]

    @staticmethod
    def path_note(paths):
        notes = []
        if paths and all(p[0] for p in paths):
            regs = sorted(set().union(*[p[0] for p in paths]))
            missing = sorted(r for r in MAIN_REGIONS if r not in regs)
            # A plan sold in six of eight regions reads better as the two it skips.
            if len(regs) > len(MAIN_REGIONS) / 2 and missing \
                    and set(regs) <= set(MAIN_REGIONS):
                notes.append("Not in " + " / ".join(missing))
            elif set(MAIN_REGIONS) <= set(regs):
                pass
            else:
                notes.append("Only in " + " / ".join(regs))
        lv = [p[1] for p in paths]
        if lv and min(lv) > 1:
            notes.append(f"Level {min(lv)}+")
        return "; ".join(notes)

    # ── one death list -> {fid: cell} ───────────────────────────────────
    def column(self, lid):
        """{row key: cell} for one death list. Records with the same name in the
        same group (a weapon at five level bands) become ONE row whose rate is
        the chance any of them drops."""
        lid = lid.upper()
        by_key = {}
        for fid, (sig, paths) in self.leaves(lid).items():
            g = self.group(sig, fid)
            if not g:
                continue
            name = self.item_name(sig, fid, fid)
            k = by_key.setdefault((g, name), {"fids": [], "paths": [], "sig": sig})
            k["fids"].append(fid)
            k["paths"].extend(paths)
        cells = {}
        for (g, name), k in by_key.items():
            note = self.path_note(k["paths"])
            fid = k["fids"][0]
            if g == "Plans" and len(k["fids"]) == 1 and fid in self.plan_rates.get(lid, {}):
                rate, conds = self.plan_rates[lid][fid]
                note = "; ".join([note] + conds if note else conds)
            else:
                rate = self.appear(lid, fid if len(k["fids"]) == 1 else frozenset(k["fids"]))
            if not rate or rate <= 0:
                continue
            cells[(g, name)] = {"group": g, "name": name, "form_id": fid, "rate": rate,
                                "rate_display": pct(rate), "note": note}
        return cells

    # ── every creature death list in the game ───────────────────────────
    def all_death_lists(self):
        if self._lld is None:
            self._lld = [f for f, e in self.lvli.edid_by_formid.items()
                         if re.match(r"(?i)^(\w+_)?LLD_Creature", e or "")
                         and not re.match(r"(?i)^zzz|.*test", e or "")
                         # only lists a live, named NPC uses: a pre-placed corpse
                         # is looted like a container, so it belongs in Containers
                         and self.list_names.get(f.upper())]
        return self._lld


def build_inventory(ictx, lists, event_note=None, base_name=""):
    """lists: [{form_id, edid}] — the page creature's death lists."""
    cols = []
    for l in lists:
        lid = (l.get("form_id") or "").upper()
        if not lid:
            continue
        name = ictx.list_name(lid, base_name) or l.get("edid") or lid
        if event_note:
            ev = event_note(l.get("edid") or "")
            if ev:
                name = f"{name} ({ev})"
        cols.append({"name": name, "lists": [lid], "cells": ictx.column(lid)})
    seen_names = collections.Counter(c["name"] for c in cols)
    for c in cols:
        if seen_names[c["name"]] > 1:
            ed = ictx.lvli.edid_for(c["lists"][0]) or ""
            ed = re.sub(r"^(\w+_)?LLD_Creature_", "", ed)
            ed = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", ed.replace("_", " ")).strip()
            c["name"] = f"{c['name']} ({ed})"
    page = {x for c in cols for x in c["lists"]}
    return _assemble(ictx, cols, base_name, page)


def _assemble(ictx, cols, base_name, page_lists):
    """cols: [{name, lists, cells}] -> {"columns", "groups"} (shared by the
    creature Inventory and the container loot tables)."""
    # Variants whose drops are identical share one column ("A / B").
    merged = []
    for c in cols:
        sig = {k: (v["rate_display"], v["note"]) for k, v in c["cells"].items()}
        for m in merged:
            if m["sig"] == sig:
                if c["name"] not in m["names"]:
                    m["names"].append(c["name"])
                break
        else:
            merged.append({"names": [c["name"]], "sig": sig, "cells": c["cells"]})
    # The page's own creature first, the other variants A–Z after it.
    merged.sort(key=lambda m: (base_name not in m["names"], " / ".join(m["names"]).lower()))
    columns = [" / ".join(m["names"]) for m in merged]
    rows = collections.defaultdict(dict)
    for i, m in enumerate(merged):
        for fid, cell in m["cells"].items():
            rows[fid][i] = cell
    def unique_plan(fid):
        src = ictx.plan_sources.get(fid)
        return bool(src) and all(x and x <= page_lists for x in src)

    groups = []
    for g in GROUPS + SUB_GROUPS:
        out = []
        for key, by_col in rows.items():
            first = next(iter(by_col.values()))
            kg = key[0]
            if kg == "Plans" and not unique_plan(first["form_id"]):
                kg = "Other Plans"
            if kg != g:
                continue
            cells = [({"rate_display": by_col[i]["rate_display"], "note": by_col[i]["note"]}
                      if i in by_col else None) for i in range(len(merged))]
            # A note every column shares is said once, under the item name.
            notes = {c["note"] for c in cells if c}
            row_note = ""
            if len(notes) == 1:
                row_note = notes.pop()
                for c in cells:
                    if c:
                        c["note"] = ""
            out.append({"name": first["name"], "form_id": first["form_id"],
                        "note": row_note, "cells": cells})
        out.sort(key=lambda r: r["name"].lower())
        if out:
            groups.append({"name": g, "note": GROUP_NOTES.get(g, ""), "rows": out,
                           "sub": g in SUB_GROUPS})
    return {"columns": columns, "groups": groups}


def own_items(ictx, slug, meat_items):
    """The page's own meat + bits: its meat_items plus the parts it owns."""
    own = {(m.get("form_id") or "").upper() for m in meat_items or [] if m.get("form_id")}
    own |= {fid for fid, p in ictx.parts.items() if p["owner"] == slug}
    return own


def build_creatures(ictx, items):
    """Every creature whose death list can drop one of `items`, with the chance per
    kill. Lists with the same creature name and the same rate merge into one row."""
    rows, seen = [], set()
    for lid in ictx.all_death_lists():
        leaves = ictx.leaves(lid)
        for fid in items:
            if fid not in leaves:
                continue
            rate = ictx.appear(lid, fid)
            if not rate or rate <= 0:
                continue
            sig = leaves[fid][0]
            # Named after the creature's race ("Hermit Crab" -> "Giant Hermit Crab",
            # not whichever named NPC happens to be shortest).
            race = ictx.list_races.get(lid.upper())
            race = race.most_common(1)[0][0] if race else ""
            row = {"creature": ictx.list_name(lid, race), "item": ictx.item_name(sig, fid, fid),
                   "rate": rate, "rate_display": pct(rate),
                   "note": ictx.path_note(leaves[fid][1])}
            key = (row["creature"], row["item"], row["rate_display"], row["note"])
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    rows.sort(key=lambda r: (r["item"].lower(), -r["rate"], r["creature"].lower()))
    for r in rows:
        r.pop("rate")
    return rows


# ── Creature containers (nests, corpses, guano piles) ───────────────────────
def _cont_index(ictx):
    """{CONT FormID: (FULL, EDID, [leveled lists it holds])} — from the LVLI
    Refs export (a CONT that references a list holds it) and the CONT names."""
    if getattr(ictx, "_conts", None) is not None:
        return ictx._conts
    names = {}
    for r in _cols(_pick("CONT_Export_*.tsv"), ["FormID", "EDID", "FULL"]):
        names[r["FormID"].upper()] = (r.get("FULL", ""), r.get("EDID", ""))
    by = collections.defaultdict(list)
    path = _pick("LVLI_Export_*_LVLI_Refs.tsv")
    if path:
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            rd = csv.reader(f, delimiter="\t")
            next(rd)
            for row in rd:
                for c in row[3:]:
                    if c.endswith(":CONT"):
                        by[c.split(":")[0].upper()].append(row[0].upper())
    ictx._conts = {fid: (names.get(fid, ("", ""))[0], names.get(fid, ("", ""))[1], lists)
                   for fid, lists in by.items()}
    return ictx._conts


def _edid_has(edid, squash):
    """True when a run of whole EDID words spells the creature ("ScorchBeast",
    "Scorchbeast_Nest") — never a fragment ("cat" inside "Location")."""
    toks = [t.lower() for t in re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+", edid or "")]
    for i in range(len(toks)):
        run = ""
        for t in toks[i:]:
            run += t
            if run == squash:
                return True
            if len(run) >= len(squash):
                break
    return False


def container_loot(ictx, creature):
    """Everything that can come out of the creature's own containers — the ones
    whose name carries the creature's name (Scorchbeast Nest, Scorchbeast Corpse,
    Scorchbeast Guano Pile). One column per container name; a container that
    holds several lists rolls each of them, so an item's chance is
    1 - product(1 - chance in each list). Laid out like the Inventory."""
    word = re.compile(r"\b" + re.escape(creature) + r"\b", re.I)
    squash = re.sub(r"\s+", "", creature).lower()
    by_name = collections.OrderedDict()
    for fid, (full, edid, lists) in sorted(_cont_index(ictx).items(), key=lambda x: x[1][0]):
        if not full or re.match(r"(?i)(test|qa|debug|zzz|atx_|score_)", edid or ""):
            continue
        # The container's own name must carry the creature ("Molerat Corpse"
        # counts for Mole Rat); a "Box" with the creature only in its EDID doesn't.
        if not (word.search(full) or (_edid_has(edid, squash)
                                      and squash in re.sub(r"\s+", "", full).lower())):
            continue
        by_name.setdefault(full, set()).update(lists)
    cols = []
    for name, lists in by_name.items():
        cells = {}
        for L in sorted(lists):
            for key, cell in ictx.column(L).items():
                if key in cells:
                    a = cells[key]
                    r = 1 - (1 - a["rate"]) * (1 - cell["rate"])
                    a.update(rate=r, rate_display=pct(r))
                else:
                    cells[key] = dict(cell)
        if cells:
            cols.append({"name": name, "lists": sorted(lists), "cells": cells})
    if not cols:
        return None
    return _assemble(ictx, cols, "", {x for c in cols for x in c["lists"]})

