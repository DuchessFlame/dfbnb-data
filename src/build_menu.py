#!/usr/bin/env python3
"""
build_menu.py - which CAMP Build Menu section ("Utility > Player Buffs") an item is in.

Source: tsv/WORKSHOP_Export_<Month>_<Year>.json, the game's own
misc/workshop/workshop.json (see extract_workshop_menu.py for where it lives and
why the ESM can't tell you this). Fully generative:

    workshop.json  sub-category -> recipes (COBJ FormIDs)
    COBJ export    recipe -> CNAM (the object, or a workshop_LL_* LVLI)
    LVLI entries   LVLI -> every object it can place (recursive)
    COBJ2 export   recipe conditions -> the ENTM (Atom Shop entitlement) that unlocks it
    KYWD export    category keyword -> FULL display name

So any FormID a CAMP builder already carries for an item - recipe, built object
(FURN / ACTI / CONT / STAT / MISC), LVLI, or ENTM - resolves to its section.
A FormID can sit in more than one section (e.g. a vending machine that also
counts as an appliance); every match is returned, in menu order.

Use:
    import build_menu
    bm = build_menu.load()
    bm.paths("00006F48", "0050CD11")          # -> [{"main": "Utility", "sub": "Instruments", ...}]
    bm.label("00006F48")                       # -> "Utility > Instruments"
    bm.members("Workshop2_SubCategory_Resources_Water")   # -> set of FormIDs
"""
import csv, json, os, re, sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source  # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
_FID = re.compile(r"\b([0-9A-Fa-f]{8})\b")
_COND_ENTM = re.compile(r"\[ENTM:([0-9A-Fa-f]{8})\]")
_COND_CNDF = re.compile(r"\[CNDF:([0-9A-Fa-f]{8})\]")


def _rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


def _fid(v):
    m = _FID.search(v or "")
    return m.group(1).upper() if m else ""


class BuildMenu:
    def __init__(self, channel="live"):
        menu_path = tsv_source.newest("WORKSHOP_Export_*.json", channel=channel)
        self.source = os.path.basename(menu_path)
        menu = json.load(open(menu_path, encoding="utf-8"))

        # keyword EDID -> in-game name (KYWD FULL), falling back to the EDID tail
        names = {}
        for r in _rows(tsv_source.newest("KYWD_Export_*.tsv", channel=channel, exclude=["_Refs"])):
            if (r.get("EDID") or "").startswith("Workshop2_"):
                names[r["EDID"]] = (r.get("FULL_Name") or "").strip()

        def nice(edid):
            if names.get(edid):
                return names[edid]
            tail = edid.split("Category_", 1)[-1].split("_")[-1]
            return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tail)

        # recipe FormID -> list of section dicts (menu order kept)
        self.sections = []
        recipe_sections = defaultdict(list)
        for ci, cat in enumerate(menu):
            main_edid = cat["CategoryKeyword"]["FormEditorID"]
            for si, sub in enumerate(cat.get("SubCategories") or []):
                sub_edid = sub["CategoryKeyword"]["FormEditorID"]
                sec = {"main": nice(main_edid), "sub": nice(sub_edid),
                       "mainEdid": main_edid, "subEdid": sub_edid, "order": (ci, si)}
                self.sections.append(sec)
                for rec in sub.get("Recipes") or []:
                    recipe_sections["%08X" % int(rec["FormID"])].append(sec)

        # Unlock conditions name the ENTM directly, or wrap it in a condition form
        # (CNDF, e.g. SCORE_S14_COBJ_AutoMiner_Condition) - open those up too.
        cndf_entm = defaultdict(set)
        for r in _rows(tsv_source.newest("CNDF_Export_*.tsv", channel=channel)):
            for k, v in r.items():
                if k and k.startswith("Cond") and v:
                    cndf_entm[_fid(r.get("FormID"))].update(e.upper() for e in _COND_ENTM.findall(v))

        def cond_entms(text):
            out = {e.upper() for e in _COND_ENTM.findall(text or "")}
            for c in _COND_CNDF.findall(text or ""):
                out |= cndf_entm.get(c.upper(), set())
            return out

        # recipe -> CNAM, and ENTM unlock conditions
        cobj_cnam = {}
        for r in _rows(tsv_source.newest("COBJ_Export_*.tsv", channel=channel)):
            cobj_cnam[_fid(r.get("COBJ_FormID"))] = _fid(r.get("CNAM_FormID"))
        cobj_entm = defaultdict(set)
        for r in _rows(tsv_source.newest("COBJ2_Export_*.tsv", channel=channel)):
            cobj_entm[_fid(r.get("COBJ_FormID"))] |= cond_entms(r.get("Conditions"))

        # LVLI -> children, for workshop_LL_* recipes that place several objects
        # Each entry can carry its own HasEntitlement condition: that ENTM is the
        # Atom Shop item the entry places, so it belongs to the same section.
        kids = defaultdict(list)
        entry_entm = defaultdict(set)
        for r in _rows(tsv_source.newest("LVLI_Export_*_LVLI_Entries.tsv", channel=channel)):
            c = _fid(r.get("LVLO_Reference"))
            if c:
                kids[_fid(r.get("LVLI_FormID"))].append(c)
                for k, v in r.items():
                    if k and k.startswith("Cond") and v and ("ENTM:" in v or "CNDF:" in v):
                        entry_entm[c] |= cond_entms(v)

        def expand(fid, seen):
            if fid in seen or len(seen) > 400:
                return
            seen.add(fid)
            seen.update(entry_entm.get(fid, ()))
            for c in kids.get(fid, ()):
                expand(c, seen)

        self._by_fid = defaultdict(list)
        self._recipes_of = defaultdict(set)
        for recipe, secs in recipe_sections.items():
            reach = {recipe}
            if cobj_cnam.get(recipe):
                expand(cobj_cnam[recipe], reach)
            reach |= cobj_entm.get(recipe, set())
            for fid in reach:
                self._recipes_of[fid].add(recipe)
                for s in secs:
                    if s not in self._by_fid[fid]:
                        self._by_fid[fid].append(s)
        for lst in self._by_fid.values():
            lst.sort(key=lambda s: s["order"])

        # Fallbacks for items a builder keys by something other than a placed
        # object - both still read straight off the exports:
        #   keyword  (BedTypeSleepingBag) -> every in-menu object carrying it
        #   RESO     (WaterResource)      -> every in-menu object whose properties
        #                                    carry the RESO's actor value (Water)
        obj_kw, obj_av = defaultdict(set), defaultdict(set)
        for pattern, fid_col in (("ACTI_Export_*_ACTI.tsv", "ACTI_FormID"),
                                 ("FURN_Export_*_FURN.tsv", "FURN_FormID"),
                                 ("CONT_Export_*.tsv", "FormID"),
                                 ("FLOR_Export_*.tsv", "FLOR_FormID")):
            path = tsv_source.newest(pattern, channel=channel, required=False)
            if not path:
                continue
            for r in _rows(path):
                fid = _fid(r.get(fid_col))
                if fid not in self._by_fid:
                    continue
                for k, v in r.items():
                    if not k or not v:
                        continue
                    if re.fullmatch(r"KW_?\d+", k):
                        obj_kw[_fid(v)].add(fid)
                    elif re.fullmatch(r"Prop_\d+_AV", k):
                        obj_av[v.split()[0].upper()].add(fid)
        self._kw_objects = obj_kw
        self._av_objects = obj_av
        #   RESO produce list (NAM2) -> any in-menu FLOR with the same Produce
        #                                    list (the SSE potted plants)
        produce_objects = defaultdict(set)
        flor = tsv_source.newest("FLOR_Export_*.tsv", channel=channel, required=False)
        for r in _rows(flor) if flor else ():
            fid = _fid(r.get("FLOR_FormID"))
            if fid in self._by_fid and _fid(r.get("Produce")):
                produce_objects[_fid(r.get("Produce"))].add(fid)
        self._reso_av, self._reso_produce = {}, {}
        for r in _rows(tsv_source.newest("RESO_Export_*.tsv", channel=channel)):
            rid = _fid(r.get("FormID"))
            av = (r.get("NAM1_ActorValue") or "").split()
            if av:
                self._reso_av[rid] = av[0].upper()
            prod = _fid(r.get("NAM2_Produce"))
            if prod in produce_objects:
                self._reso_produce[rid] = produce_objects[prod]

    # ---- queries -------------------------------------------------------
    def paths(self, *fids):
        """Sections for any of the given FormIDs (recipe, object, LVLI or ENTM).
        Falls back to keyword / RESO actor-value matches only when no direct
        FormID is in the menu."""
        out = []

        def add(f):
            for s in self._by_fid.get(f, ()):
                if s not in out:
                    out.append(s)
        ids = [_fid(f) for f in fids if _fid(f)]
        for f in ids:
            add(f)
        if not out:
            for f in ids:
                for o in self._kw_objects.get(f, ()):
                    add(o)
                av = self._reso_av.get(f)
                for o in self._av_objects.get(av, ()) if av else ():
                    add(o)
                for o in self._reso_produce.get(f, ()):
                    add(o)
        return sorted(out, key=lambda s: s["order"])

    def label(self, *fids, sep=" > "):
        return ", ".join(f"{s['main']}{sep}{s['sub']}" for s in self.paths(*fids))

    def public(self, *fids):
        """JSON-ready list for a dist item:
        [{"main": "Utility", "sub": "Instruments", "order": 4012}].
        `order` is the section's place in the game's Build Menu (category * 1000
        + sub-category), so a page can list headings in menu order."""
        return [{"main": s["main"], "sub": s["sub"],
                 "order": s["order"][0] * 1000 + s["order"][1]}
                for s in self.paths(*fids)]

    def in_sub(self, sub_edid_suffix, *fids):
        return any(s["subEdid"].endswith(sub_edid_suffix) for s in self.paths(*fids))

    def members(self, sub_edid):
        return {f for f, secs in self._by_fid.items() if any(s["subEdid"] == sub_edid for s in secs)}


_CACHE = {}


def load(channel="live"):
    if channel not in _CACHE:
        _CACHE[channel] = BuildMenu(channel)
    return _CACHE[channel]


if __name__ == "__main__":
    bm = load(tsv_source.channel_of())
    print(bm.source, len(bm.sections), "sections,", len(bm._by_fid), "FormIDs resolved")
    for a in sys.argv[1:]:
        print(a, "->", bm.label(a) or "(not in the Build Menu)")
