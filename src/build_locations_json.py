#!/usr/bin/env python3
r"""
build_locations_json.py — Data Mining - Fallout 76 List of All Locations

Every map marker in Appalachia, grouped by region, with the challenges and
quests that start at or visit each one. Feeds df-bnb-locations.js.

    python src/build_locations_json.py           # live  -> dist/locations/all_locations.json
    python src/build_locations_json.py --pts     # PTS   -> dist/pts/locations/all_locations.json

WHERE EACH PIECE COMES FROM (nothing hand-typed)
================================================
Markers   Mappalachia DB  MapMarker (Appalachia worldspace): label, icon, x, y.
          The DB is local-only (~480 MB), so a DB-backed run writes the markers,
          their cell -> Location links and the challenge REFR positions it needs
          into data/locations/mappalachia_markers.json. CI reads that committed
          cache — same pattern as data/mappalachia_geo.json.
Region    1) the LocRegion* keyword on the marker's own LCTN (walking PNAM
             parents) — the game's own tag, see crossref_mappalachia_markers;
          2) point-in-polygon against the Mappalachia SubRegion tilings;
          3) nearest polygon, then MARKER_REGION_OVERRIDES.
Marker -> LCTN
          the LCTN whose FULL name matches the marker label; ties (and the seven
          "Fissure Site"s) are broken by Mappalachia's Cell table, which says
          which Location owns each exterior cell. No name match -> the Location
          that owns the marker's cell. Then every child LCTN (PNAM) that no
          other marker claims is folded in, so interiors count too.
Quests    QUEST export (FULL, Quest Type, LNAM) + every QUST that references
          one of the marker's LCTNs (LCTN_Export_*_Refs.tsv).
Challenges
          every CHAL that references one of the marker's LCTNs (Refs export),
          every CHAL whose conditions go through a CNDF that does, and every
          CHAL with a GetWithinDistance on a placed REFR whose Mappalachia
          position is nearest this marker. Type / required count / cut flag /
          parent come from dist/challenges/challenges.json (same labels as the
          farming "Used For" lists).

PTS caveat: Mappalachia is built from the LIVE game, so the marker list is the
live one on both channels. The PTS build re-links quests/challenges from the PTS
exports; a brand-new PTS-only marker cannot appear until Mappalachia has it.
"""

from __future__ import annotations

import csv
import datetime
import json
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import tsv_source                                   # noqa: E402
import crossref_mappalachia_markers as xref         # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
DIST = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST, "locations", "all_locations.json")

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
MARKER_CACHE = os.path.join(REPO, "data", "locations", "mappalachia_markers.json")
WORLDSPACE = xref.WORLDSPACE
CELL = 4096.0

# Region order on the page — the order a new character meets them.
REGION_ORDER = ["Forest", "Toxic Valley", "Ash Heap", "Savage Divide", "The Mire",
                "Cranberry Bog", "Skyline Valley", "Burning Springs"]

IMAGE_BASE = "/wp-content/uploads/guide-images/data-mining/all-locations/"

# Pip-Boy quest categories. Anything not listed (None / Module / Server /
# dialogue scaffolding) is engine plumbing, not a quest a player sees.
QUEST_TYPES = {
    "Primary": "Main Quest",
    "Secondary": "Side Quest",
    "Side Quest": "Side Quest",
    "Miscellaneous": "Misc Quest",
    "Daily": "Daily Quest",
    "Event": "Event",
    "Public Event": "Event",
    "Expedition": "Expedition",
    "Daily Ops": "Daily Ops",
    "Raid": "Raid",
    "Caravan": "Caravan",
}
QUEST_TYPE_ORDER = ["Main Quest", "Side Quest", "Misc Quest", "Daily Quest", "Event",
                    "Activity", "Expedition", "Daily Ops", "Raid", "Caravan"]
CHAL_TYPE_ORDER = ["Daily", "Weekly", "Mini Season", "Event", "Monthly", "Lifetime"]

_DEAD_EDID = re.compile(r"^(zzz|test|debug|deprecated|temp_)|(^|_)(cut|test|debug)(_|$)", re.I)


# ── small helpers ────────────────────────────────────────────────────────────
def slugify(s):
    s = (s or "").lower().replace("&", " and ").replace("'", "").replace("\u2019", "")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "location"


def norm(s):
    s = re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()
    return re.sub(r"^the ", "", s)


def hexid(v):
    v = (v or "").strip().upper()
    return v.zfill(8) if re.fullmatch(r"[0-9A-F]{1,8}", v) else ""


def read_tsv(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


# A few FO76 map icons still carry their Fallout 4 asset names. The type pill
# describes the ICON, so name what the icon is, not the FO4 place it came from.
ICON_LABELS = {
    "BoSMarker": "Brotherhood of Steel",
    "SancHillsMarker": "Monument",
    "GoodneighborMarker": "Town",
    "LibertaliaMarker": "Settlement",
    "SkullRingMarker": "Raider Camp",
    "TrainTrackMark": "Trainyard",
    "WhitespringResort": "Resort",
    "NukaColaQuantumPlant": "Nuka-Cola Plant",
    "PalaceWindingPathMarker": "Palace",
    "TopOfTheWorldMarker": "Ski Lodge",
    "TeapotMarker": "Landmark",
    "PumpkinMarker": "Landmark",
    "CowSpotsCreameryMarker": "Creamery",
    "LegendaryPurveyorMarker": "Legendary Purveyor",
    "PondLakeMarker": "Lake",
}


def icon_label(icon):
    """'PublicWorkshopMarker' -> 'Public Workshop'."""
    if icon in ICON_LABELS:
        return ICON_LABELS[icon]
    if re.match(r"Vault\d*Marker$", icon or ""):
        return "Vault"
    s = re.sub(r"(Marker|Mark)$", "", icon or "")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", s).strip()
    return s or "Location"


# ── markers (Mappalachia DB, or the committed cache) ─────────────────────────
REFR_RE = re.compile(r"\[REFR:([0-9A-F]{8})\]", re.I)


def chal_refr_ids():
    """REFR FormIDs used by GetWithinDistance in any CHAL export, BOTH channels, so
    one cache serves live and PTS CI."""
    ids = set()
    for ch in ("live", "pts"):
        p = tsv_source.newest("CHAL_Export_*.tsv", channel=ch, required=False)
        if not p:
            continue
        for row in read_tsv(p):
            for k, v in row.items():
                if k and k.startswith("Cond") and v and "GetWithinDistance" in v:
                    ids.update(m.upper() for m in REFR_RE.findall(v))
    return ids


def load_markers():
    if os.path.isfile(MAPPALACHIA_DB):
        con = sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)
        cur = con.cursor()
        ver = dict(cur.execute("SELECT key, value FROM Meta").fetchall()).get("GameVersion", "")
        cells = defaultdict(list)
        for lf, x, y in cur.execute("SELECT locationFormID, x, y FROM Cell WHERE spaceFormID=?",
                                    (WORLDSPACE,)):
            cells[(x, y)].append(f"{lf:08X}")
        markers = []
        for x, y, label, icon in cur.execute(
                "SELECT x, y, label, icon FROM MapMarker WHERE spaceFormID=? AND label<>'' "
                "ORDER BY label, x, y", (WORLDSPACE,)):
            cx, cy = math.floor(x / CELL), math.floor(y / CELL)
            near = []
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    near += cells.get((cx + dx, cy + dy), [])
            markers.append({"label": label, "icon": icon, "x": round(x, 1), "y": round(y, 1),
                            "cell_locs": sorted(set(cells.get((cx, cy), []))),
                            "near_locs": sorted(set(near))})
        refr = {}
        want = chal_refr_ids()
        if want:
            ids = [int(h, 16) for h in want]
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = (f"SELECT instanceFormID, x, y FROM Position WHERE spaceFormID=? "
                     f"AND instanceFormID IN ({','.join('?' * len(chunk))})")
                for fid, x, y in cur.execute(q, [WORLDSPACE] + chunk):
                    refr[f"{fid:08X}"] = [round(x, 1), round(y, 1)]
        con.close()
        os.makedirs(os.path.dirname(MARKER_CACHE), exist_ok=True)
        with open(MARKER_CACHE, "w", encoding="utf-8") as f:
            json.dump({"_note": "Map markers, their cell->Location links and challenge REFR "
                                "positions, extracted from mappalachia.db so CI can build "
                                "the All Locations page. Regenerated by any local run of "
                                "build_locations_json.py that can see the DB.",
                       "game_version": ver, "markers": markers, "refr_positions": refr},
                      f, ensure_ascii=False, indent=1)
        print(f"[locations] Mappalachia DB {ver}: {len(markers)} markers, "
              f"{len(refr)}/{len(want)} challenge REFRs placed — cache refreshed.")
        return markers, refr, ver
    if not os.path.exists(MARKER_CACHE):
        raise SystemExit(f"No Mappalachia DB at {MAPPALACHIA_DB} and no cache at {MARKER_CACHE}. "
                         "Run once locally with the DB.")
    d = json.load(open(MARKER_CACHE, encoding="utf-8"))
    print(f"[locations] Mappalachia DB not found — using committed cache "
          f"({len(d['markers'])} markers, game {d.get('game_version', '?')}).")
    return d["markers"], d.get("refr_positions", {}), d.get("game_version", "")


# ── LCTN records, regions and references ─────────────────────────────────────
def load_lctn():
    path = tsv_source.newest("LCTN_Export_*_LCTN.tsv", channel=CHANNEL)
    recs = {}
    for row in read_tsv(path):
        fid = hexid(row.get("LCTN_FormID"))
        if not fid:
            continue
        regs = []
        for k, v in row.items():
            if k and re.fullmatch(r"KW_\d+", k):
                m = re.search(r"LocRegion([A-Za-z]+)", v or "")
                if m and m.group(1) in xref.LOCREGION_KEYWORD_TO_REGION:
                    regs.append(m.group(1))
        pn = (row.get("PNAM_ParentLocation") or "").split(":")[0].strip()
        recs[fid] = {"edid": row.get("LCTN_EDID") or "", "full": row.get("LCTN_FULL") or "",
                     "regs": regs, "pnam": hexid(pn)}
    return recs


def lctn_region(recs, fid, depth=0):
    r = recs.get(fid)
    if not r or depth > 8:
        return ""
    for kw in xref.LOCREGION_PRIORITY:
        if kw in r["regs"]:
            return xref.LOCREGION_KEYWORD_TO_REGION[kw]
    return lctn_region(recs, r["pnam"], depth + 1) if r["pnam"] else ""


def is_region_level(rec):
    e = rec["edid"]
    return bool(re.match(r"(SubRegion|Region|LocRegion)", e)) or e.endswith("RegionLocation")


def load_lctn_refs():
    """{lctn: {'QUST': set, 'CHAL': set, 'CNDF': set}} from the Refs export."""
    path = tsv_source.newest("LCTN_Export_*_Refs.tsv", channel=CHANNEL)
    out = defaultdict(lambda: defaultdict(set))
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        rd = csv.reader(f, delimiter="\t")
        next(rd, None)
        for row in rd:
            if not row:
                continue
            fid = hexid(row[0])
            for cell in row[3:]:
                parts = cell.split(":")
                if len(parts) == 3 and parts[2] in ("QUST", "CHAL", "CNDF"):
                    out[fid][parts[2]].add(hexid(parts[0]))
    return out


# ── quests ───────────────────────────────────────────────────────────────────
def load_quests():
    path = tsv_source.newest("QUEST_Export_*.tsv", channel=CHANNEL)
    cut = set()
    cj = os.path.join(DIST, "challenges", "challenges.json")
    if os.path.exists(cj):
        d = json.load(open(cj, encoding="utf-8"))
        for page in (d.get("quest_pages") or {}).values():
            for it in _iter_dicts(page):
                if it.get("is_cut") and it.get("form_id"):
                    cut.add(it["form_id"].upper())
    quests = {}
    for row in read_tsv(path):
        fid = hexid(row.get("FormID"))
        edid = row.get("EDID") or ""
        full = (row.get("FULL - Name") or "").strip()
        qt = (row.get("Quest Type") or "").strip()
        lnam = re.search(r"\[LCTN:([0-9A-F]{8})\]", row.get("LNAM - Location") or "", re.I)
        q = {"form_id": fid, "edid": edid, "lnam": lnam.group(1).upper() if lnam else ""}
        typ = QUEST_TYPES.get(qt, "")
        if re.match(r"^Activity\s*:", full) and typ == "Event":
            typ = "Activity"
        # Pip-Boy prefixes become the type pill; runtime alias tokens
        # (<Alias=Location>) are filled in-game and mean nothing on a page.
        name = re.sub(r"^(Event|Public Event|Activity)\s*:\s*", "", full)
        name = re.sub(r"\s*<Alias[^>]*>\s*", " ", name)
        name = re.sub(r"\s+", " ", name).strip(" :-")
        ok = (fid and name and typ and not _DEAD_EDID.search(edid)
              and not re.search(r"template|moduletest", edid, re.I)
              and not name.startswith("[") and not re.search(r"\bMisc Obj\b", name, re.I)
              and fid not in cut and "(cut)" not in name.lower())
        q.update(ok=bool(ok), name=name, type=typ)
        quests[fid] = q
    return quests


def _iter_dicts(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _iter_dicts(v)
    elif isinstance(o, list):
        for v in o:
            yield from _iter_dicts(v)


def load_event_locations():
    """{quest FormID: [(lctn fid or '', location name, region)]} from the activity /
    event reward pages' regionLocations (themselves built from the QUEST export +
    events_region_location.tsv). Catches events whose quest only reaches its
    location through a Find-Matching alias, which the LCTN Refs export can't see."""
    out = defaultdict(list)
    for rel in ("activities/activities_rewards_by_page.json", "events/events_rewards_by_page.json",
                "seasonal_events/seasonal_events_rewards_by_page.json"):
        p = os.path.join(DIST, rel)
        if not os.path.exists(p):
            p = os.path.join(REPO, "dist", rel)
        if not os.path.exists(p):
            continue
        d = json.load(open(p, encoding="utf-8"))
        for v in (d.get("byPage") or {}).values():
            fid = hexid(v.get("questFormID") or "")
            for rl in v.get("regionLocations") or []:
                loc = rl.get("location") or ""
                m = re.search(r"\[LCTN:([0-9A-F]{8})\]", loc, re.I)
                name = re.sub(r"\s*\[LCTN:[0-9A-F]+\]\s*", "", loc, flags=re.I).strip()
                if fid and name:
                    t = (m.group(1).upper() if m else "", norm(name), rl.get("region") or "")
                    if t not in out[fid]:
                        out[fid].append(t)
    return out


# ── challenges ───────────────────────────────────────────────────────────────
def load_challenges():
    """{fid: {...}} for every live (non-cut, named) CHAL, plus the condition links."""
    meta, by_edid = {}, {}
    cj = os.path.join(DIST, "challenges", "challenges.json")
    if not os.path.exists(cj):
        cj = os.path.join(REPO, "dist", "challenges", "challenges.json")
    if os.path.exists(cj):
        d = json.load(open(cj, encoding="utf-8"))
        for page, val in (d.get("pages") or {}).items():
            for it in _iter_dicts(val):
                fid = (it.get("form_id") or "").upper()
                if not fid or "scope" not in it:
                    continue
                typ = "Mini Season" if page.startswith("season:") else \
                      (it.get("scope") or it.get("group") or page.replace("-", " ").title())
                prev = meta.get(fid)
                if not prev or (typ == "Mini Season" and prev["type"] != "Mini Season"):
                    meta[fid] = {"type": typ, "required": it.get("required"),
                                 "cut": bool(it.get("is_cut")), "parent": it.get("parent_edid") or "",
                                 "full": it.get("full") or ""}
                if it.get("edid"):
                    by_edid[it["edid"]] = it.get("full") or ""

    path = tsv_source.newest("CHAL_Export_*.tsv", channel=CHANNEL)
    chals, via_lctn, via_cndf, via_refr = {}, defaultdict(set), defaultdict(set), defaultdict(set)
    for row in read_tsv(path):
        fid = hexid(row.get("FormID"))
        edid = row.get("EDID") or ""
        full = (row.get("FULL") or "").strip()
        m = meta.get(fid, {})
        cut = m.get("cut") or edid.upper().startswith("CUT_") or bool(_DEAD_EDID.search(edid))
        parent = m.get("parent") or ""
        if not parent:
            a = re.match(r"(\S+)\s", row.get("ANAM") or "")
            parent = a.group(1) if a else ""
        req = m.get("required")
        if req is None:
            try:
                req = int(float(row.get("TNAM") or 0)) or None
            except ValueError:
                req = None
        chals[fid] = {"form_id": fid, "edid": edid, "name": full,
                      "type": m.get("type") or (row.get("CNAM") or "").strip() or "Challenge",
                      "required": req, "parent": by_edid.get(parent, ""),
                      "ok": bool(fid and full and not cut)}
        for k, v in row.items():
            if not (k and k.startswith("Cond") and v):
                continue
            for x in re.findall(r"\[LCTN:([0-9A-F]{8})\]", v, re.I):
                via_lctn[x.upper()].add(fid)
            for x in re.findall(r"\[CNDF:([0-9A-F]{8})\]", v, re.I):
                via_cndf[x.upper()].add(fid)
            if "GetWithinDistance" in v:
                for x in REFR_RE.findall(v):
                    via_refr[x.upper()].add(fid)
    return chals, via_lctn, via_cndf, via_refr


# ── marker -> LCTN ───────────────────────────────────────────────────────────
def match_markers(markers, recs):
    by_name = defaultdict(list)
    for fid, r in recs.items():
        if r["full"] and not is_region_level(r):
            by_name[norm(r["full"])].append(fid)
    label_count = defaultdict(int)
    for m in markers:
        label_count[norm(m["label"])] += 1

    claimed = {}
    for i, m in enumerate(markers):
        cands = list(by_name.get(norm(m["label"]), []))
        # Map labels say "Site Alpha"; the LCTN says "Missile Silo Alpha".
        sm = re.match(r"site (\w+)$", norm(m["label"]))
        if sm:
            cands += by_name.get(f"missile silo {sm.group(1)}", [])
        near = set(m.get("near_locs") or [])
        cell = set(m.get("cell_locs") or [])
        pick = [c for c in cands if c in cell] or [c for c in cands if c in near]
        how = "name+cell" if pick else ""
        if not pick and cands and label_count[norm(m["label"])] == 1:
            pick, how = cands, "name"
        if not pick:
            pick = [c for c in sorted(cell) if c in recs and not is_region_level(recs[c])]
            how = "cell" if pick else "none"
        m["_lctn"], m["_how"] = pick, how
        for c in pick:
            claimed.setdefault(c, i)

    # Walk UP one level when the parent is a real place that only this marker
    # reaches (Charleston North -> Charleston, Athens exec -> Athens Ruins).
    parent_of = defaultdict(set)
    for i, m in enumerate(markers):
        for c in m["_lctn"]:
            p = recs.get(c, {}).get("pnam")
            if p and p in recs and not is_region_level(recs[p]) and p not in claimed:
                parent_of[p].add(i)
    for p, who in parent_of.items():
        same = [i for i in who if norm(markers[i]["label"]) == norm(recs[p]["full"])]
        if len(same) == 1:
            who = set(same)
        if len(who) == 1:
            i = next(iter(who))
            markers[i]["_lctn"] = markers[i]["_lctn"] + [p]
            claimed[p] = i

    children = defaultdict(list)
    for fid, r in recs.items():
        if r["pnam"]:
            children[r["pnam"]].append(fid)
    for i, m in enumerate(markers):
        seen, stack = set(m["_lctn"]), list(m["_lctn"])
        while stack:
            for ch in children.get(stack.pop(), []):
                if ch in seen or is_region_level(recs[ch]):
                    continue
                if claimed.get(ch, i) != i:      # another marker owns it
                    continue
                seen.add(ch)
                stack.append(ch)
        m["_all"] = seen


def main():
    markers, refr_pos, game_ver = load_markers()
    rings, _ = xref.load_mappalachia()
    recs = load_lctn()
    refs = load_lctn_refs()
    quests = load_quests()
    event_locs = load_event_locations()
    chals, chal_lctn, chal_cndf, chal_refr = load_challenges()

    match_markers(markers, recs)

    # CNDF -> CHAL through the LCTN refs export (a CNDF that names the LCTN).
    # Challenges placed by a REFR distance check go to the nearest marker.
    refr_marker = {}
    for ref, (x, y) in refr_pos.items():
        best, bd = None, 1e30
        for i, m in enumerate(markers):
            dd = (m["x"] - x) ** 2 + (m["y"] - y) ** 2
            if dd < bd:
                best, bd = i, dd
        if best is not None and bd <= (2.5 * CELL) ** 2:
            refr_marker[ref] = best

    linked_q, linked_c = set(), set()
    regions = defaultdict(list)
    dup = defaultdict(int)
    ids_seen = defaultdict(int)
    for m in sorted(markers, key=lambda m: (m["label"].lower(), m["x"], m["y"])):
        ids_seen[slugify(m["label"])] += 1

    for idx, m in enumerate(markers):
        region = ""
        for fid in m["_lctn"]:
            region = lctn_region(recs, fid)
            if region:
                break
        if region not in REGION_ORDER:
            region = xref.region_for_xy(rings, m["x"], m["y"])
        if not region:
            region = xref.MARKER_REGION_OVERRIDES.get(m["label"]) or \
                     xref.region_for_xy(rings, m["x"], m["y"], nearest=True)
        m["_region"] = region

        qset, cset = set(), set()
        for fid in m["_all"]:
            r = refs.get(fid, {})
            qset |= r.get("QUST", set())
            cset |= r.get("CHAL", set()) | chal_lctn.get(fid, set())
            for cn in r.get("CNDF", set()):
                cset |= chal_cndf.get(cn, set())
        for q in quests.values():
            if q["lnam"] and q["lnam"] in m["_all"]:
                qset.add(q["form_id"])
        lab = norm(m["label"])
        for qfid, locs in event_locs.items():
            for lf, lname, lreg in locs:
                if (lf and lf in m["_all"]) or (lname == lab and (not lreg or lreg == region)):
                    qset.add(qfid)
                    break
        for ref, mi in refr_marker.items():
            if mi == idx:
                cset |= chal_refr.get(ref, set())

        ql = [quests[q] for q in qset if q in quests and quests[q]["ok"]]
        cl = [chals[c] for c in cset if c in chals and chals[c]["ok"]]
        linked_q.update(q["form_id"] for q in ql)
        linked_c.update(c["form_id"] for c in cl)

        # dedupe by visible name+type (Bethesda reuses names across variants)
        def dd(items, keyf):
            out, seen = [], set()
            for it in items:
                k = keyf(it)
                if k not in seen:
                    seen.add(k)
                    out.append(it)
            return out
        ql = dd(sorted(ql, key=lambda q: (QUEST_TYPE_ORDER.index(q["type"]), q["name"].lower(), q["form_id"])),
                lambda q: (q["type"], q["name"].lower()))
        cl = dd(sorted(cl, key=lambda c: (CHAL_TYPE_ORDER.index(c["type"]) if c["type"] in CHAL_TYPE_ORDER
                                          else 99, c["name"].lower(), c["form_id"])),
                lambda c: (c["type"], c["name"].lower(), c["parent"].lower()))

        base = slugify(m["label"])
        dup[base] += 1
        lid = base if ids_seen[base] == 1 else f"{base}-{dup[base]}"
        regions[region].append({
            "id": lid,
            "name": m["label"],
            "type": icon_label(m["icon"]),
            "icon": m["icon"],
            "x": m["x"], "y": m["y"],
            "image": f"{IMAGE_BASE}{lid}.avif",
            "lctn": [{"form_id": f, "edid": recs[f]["edid"], "name": recs[f]["full"]}
                     for f in sorted(m["_lctn"]) if f in recs],
            "match": m["_how"],
            "quests": [{"name": q["name"], "type": q["type"], "edid": q["edid"],
                        "form_id": q["form_id"]} for q in ql],
            "challenges": [{"name": c["name"], "type": c["type"], "required": c["required"],
                            "parent": c["parent"], "edid": c["edid"], "form_id": c["form_id"]}
                           for c in cl],
        })

    out_regions = []
    for name in REGION_ORDER + sorted(r for r in regions if r not in REGION_ORDER):
        locs = regions.get(name)
        if not locs:
            continue
        locs.sort(key=lambda l: (l["name"].lower(), l["id"]))
        out_regions.append({"name": name, "slug": slugify(name), "count": len(locs), "locations": locs})

    # What could not be tied to a marker — reported, never invented.
    quests_with_loc = {q["form_id"] for q in quests.values() if q["lnam"]}
    for r in refs.values():
        quests_with_loc |= r.get("QUST", set())
    unl_q = sorted(({"name": q["name"], "type": q["type"], "edid": q["edid"], "form_id": q["form_id"],
                     "reason": ("location outside Appalachia / not on a map marker"
                                if (q["form_id"] in quests_with_loc or q["form_id"] in event_locs)
                                else "no location data in the game files")}
                    for q in quests.values() if q["ok"] and q["form_id"] not in linked_q),
                   key=lambda q: (q["type"], q["name"].lower()))
    # Challenges that name a place at all: an LCTN directly, a CNDF that names
    # an LCTN, or a placed REFR. Anything else (kill X, build Y) never had one.
    loc_chals = set()
    for ref in refr_pos:
        loc_chals |= chal_refr.get(ref, set())
    for ids in chal_lctn.values():
        loc_chals |= ids
    for r in refs.values():
        loc_chals |= r.get("CHAL", set())
        for cn in r.get("CNDF", set()):
            loc_chals |= chal_cndf.get(cn, set())
    chal_to_lctn = defaultdict(set)
    for lf, ids in chal_lctn.items():
        for c in ids:
            chal_to_lctn[c].add(lf)
    for lf, r in refs.items():
        for c in r.get("CHAL", set()):
            chal_to_lctn[c].add(lf)

    def chal_reason(c):
        lfs = [lf for lf in chal_to_lctn.get(c, ()) if lf in recs]
        if lfs and all(is_region_level(recs[lf]) for lf in lfs):
            return "region-wide (whole region, not one marker)"
        if any("workshop" in recs[lf]["edid"].lower() or "camp" in recs[lf]["edid"].lower()
               or "shelter" in recs[lf]["edid"].lower() for lf in lfs):
            return "C.A.M.P. / Workshop / Shelter (not one marker)"
        return "location outside Appalachia / not on a map marker"

    unl_c = sorted(({"name": chals[c]["name"], "type": chals[c]["type"], "edid": chals[c]["edid"],
                     "form_id": c, "reason": chal_reason(c)}
                    for c in loc_chals if c in chals and chals[c]["ok"] and c not in linked_c),
                   key=lambda c: (c["reason"], c["type"], c["name"].lower()))
    total = sum(r["count"] for r in out_regions)
    doc = {
        "version": 1,
        "channel": CHANNEL,
        "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "observed": tsv_source.observed(),
        "mappalachia_game_version": game_ver,
        "title": "Fallout 76 List of All Locations",
        "image_base": IMAGE_BASE,
        "count": total,
        "regions": out_regions,
        "unlinked": {
            "quests": unl_q,
            "challenges": unl_c,
            "markers_without_lctn": sorted(m["label"] for m in markers if not m["_lctn"]),
        },
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)

    print(f"[locations] {CHANNEL}: {total} locations -> {os.path.relpath(OUT, REPO)}")
    for r in out_regions:
        nq = sum(1 for l in r["locations"] if l["quests"] or l["challenges"])
        print(f"   {r['name']:<16} {r['count']:>4}   ({nq} with quests/challenges)")
    print(f"   quests linked {len(linked_q)}, unlinked {len(unl_q)}; "
          f"challenges linked {len(linked_c)}, unlinked {len(unl_c)}; markers with no LCTN "
          f"{len(doc['unlinked']['markers_without_lctn'])}")
    if total < 400:
        raise SystemExit(f"[locations] only {total} locations — refusing to publish a short page.")


if __name__ == "__main__":
    main()
