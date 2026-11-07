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
import plan_sources   # site-wide cut test (is_cut_content)
import crossref_mappalachia_markers as xref         # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
DIST = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST, "locations", "all_locations.json")

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
MARKER_CACHE = os.path.join(REPO, "data", "locations", "mappalachia_markers.json")
WORLDSPACE = xref.WORLDSPACE
WORLDSPACE_EDID = "APPALACHIA"
CELL = 4096.0

# Region order on the page — A to Z, with "The Mire" filed under M (Duchess, 6 Nov 2026).
# df-bnb-locations.js sorts the same way, so the page is right even before a rebuild.
REGION_ORDER = ["Ash Heap", "Burning Springs", "Cranberry Bog", "Forest", "The Mire",
                "Savage Divide", "Skyline Valley", "Toxic Valley"]

# Maps are NOT duplicated for PTS (Duchess, 7 Nov 2026): the PTS page uses the live map of
# every place live already has, and only places new on the PTS (and regions whose
# markers changed) get their own map in .../all-locations/pts/ (PC: "01 Location Maps
# PTS", "02 Region Maps PTS"; PTS region map files end "-pts" so they can never be
# mistaken for, or overwrite, the live ones).
IMAGE_ROOT = "/wp-content/uploads/guide-images/data-mining/all-locations/"
IMAGE_BASE = IMAGE_ROOT + ("pts/" if PTS else "")
TODAY = datetime.date.today().isoformat()

# Every place the page has ever listed, per channel, with the date it first showed up
# (Duchess, 7 Nov 2026). It gives the NEW pill its date (the page shows NEW for the rest
# of the season a place arrived in), keeps a place's id when Bethesda renames it (ticks
# are saved by id), and remembers places taken out of the game so they stay on the list
# as Cut. "baseline" = was already there when the ledger started.
LEDGER = os.path.join(REPO, "data", "locations", "ledger.json")


def load_ledger():
    try:
        with open(LEDGER, encoding="utf-8") as f:
            return json.load(f).get("locations", {}), False
    except (OSError, ValueError):
        return {}, True


def save_ledger(led):
    os.makedirs(os.path.dirname(LEDGER), exist_ok=True)
    with open(LEDGER, "w", encoding="utf-8") as f:
        json.dump({"_note": "Written by build_locations_json.py. live/pts = date the place first "
                            "showed on that channel ('baseline' = before the ledger started).",
                   "locations": dict(sorted(led.items()))}, f, ensure_ascii=False, indent=1)


# History notes (Duchess, 7 Nov 2026): every place keeps a log of when it was added,
# cut or renamed, shown on its row as a note. A change the build spots is dated by the
# update it came in: live = the newest patch (tsv/fallout76_patches.tsv) released on or
# before the marker export; PTS = the PTS export's date. Changes from before the log
# started, or that need better wording, go in data/locations/location_history.tsv
# (id, channel, event, date, update, note) - a row there wins over the build's guess.
HISTORY_TSV = os.path.join(REPO, "data", "locations", "location_history.tsv")
EXPORT_PATH = ""        # the marker export this build read (set by load_export_markers)


def _patches():
    p = os.path.join(REPO, "tsv", "fallout76_patches.tsv")
    try:
        return sorted(((r.get("release_date") or "", r) for r in read_tsv(p) if r.get("release_date")),
                      key=lambda t: t[0])
    except OSError:
        return []


def change_stamp():
    """(date, update) a change spotted in this build is put down to."""
    exp = tsv_source.export_date(EXPORT_PATH).isoformat() if EXPORT_PATH else TODAY
    if exp.startswith("0001"):
        exp = TODAY
    if PTS:
        return exp, "PTS"
    best = None
    for d, r in _patches():
        if d <= exp:
            best = (d, f"{r.get('name', '').strip()} {(r.get('type') or 'update').strip().lower()}".strip())
    return best or (exp, "")


def load_history_overrides():
    out = {}
    try:
        for r in read_tsv(HISTORY_TSV):
            lid, ev = (r.get("id") or "").strip(), (r.get("event") or "").strip().lower()
            if lid and ev and not lid.startswith("#"):
                out[(lid, (r.get("channel") or "live").strip().lower(), ev)] = {
                    k: (r.get(k) or "").strip() for k in ("date", "update", "note")}
    except OSError:
        pass
    return out


def log_event(led, event, stamp, **extra):
    """Add an event to a place's history once (same channel + event + from)."""
    hist = led.setdefault("history", [])
    if any(h["channel"] == CHANNEL and h["event"] == event and h.get("from") == extra.get("from")
           for h in hist):
        return
    hist.append(dict(channel=CHANNEL, event=event, date=stamp[0], update=stamp[1], **extra))


def _nice_date(d):
    try:
        x = datetime.date.fromisoformat(d)
        return f"{x.day} {x.strftime('%b %Y')}"
    except (TypeError, ValueError):
        return ""


def history_for(lid, led, overrides):
    """-> (notes for the page, date it was added for the NEW pill). The live page shows
    live history; the PTS page shows both."""
    hist = [dict(h) for h in led.get("history", [])]
    for (oid, ch, ev), o in overrides.items():
        if oid != lid:
            continue
        h = next((h for h in hist if h["channel"] == ch and h["event"] == ev), None)
        if h is None:
            h = {"channel": ch, "event": ev}
            hist.append(h)
        h.update({k: v for k, v in o.items() if v})
    hist = [h for h in hist if CHANNEL == "pts" or h["channel"] == "live"]
    # PTS page: a PTS event the live game already has (same change) is shown once, as live
    live_keys = {(h["event"], h.get("from")) for h in hist if h["channel"] == "live"}
    hist = [h for h in hist if h["channel"] == "live" or (h["event"], h.get("from")) not in live_keys]
    notes, added = [], ""
    for h in sorted(hist, key=lambda h: h.get("date") or ""):
        when = ", ".join(x for x in (h.get("update") if h.get("update") != "PTS" else "",
                                     _nice_date(h.get("date"))) if x)
        when = f" ({when})" if when else ""
        if h.get("note"):
            notes.append(h["note"])
        elif h["event"] == "added":
            notes.append(f"Added on the PTS{when}." if h["channel"] == "pts" else f"Added to the game{when}.")
        elif h["event"] == "cut":
            notes.append(f"Taken off the PTS map{when}." if h["channel"] == "pts" else
                         f"Cut from the game{when} \u2014 its map marker no longer shows on the in-game map.")
        elif h["event"] == "renamed":
            notes.append(f"Renamed from {h.get('from', '')}{when}.")
        if h["event"] == "added" and h.get("date") and h["channel"] == CHANNEL:
            added = h["date"]
        elif h["event"] == "added" and h.get("date") and not added:
            added = h["date"]
    return notes, added


def season_starts():
    """ISO start dates of every season (dist/seasons.json, D/M/YYYY). The page works out
    the current season itself, so the NEW pill goes off when the season ends with no
    rebuild."""
    try:
        with open(os.path.join(REPO, "dist", "seasons.json"), encoding="utf-8") as f:
            ss = json.load(f).get("seasons", {})
    except (OSError, ValueError):
        return []
    out = set()
    for v in (ss.values() if isinstance(ss, dict) else ss):
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})$", (v or {}).get("startDate") or "")
        if m:
            out.add(f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}")
    return sorted(out)

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
# Local Legend fish (Duchess, 7 Nov 2026): each one bites at ONE place, set by the
# GetInCurrentLocation condition on its entry in Fishing_LLS_FishCollection_LocalLegends
# (LVLI 00804F7F). Read from the game files every build — nothing typed here. The
# seasonal Local Legends sit in the same list and also carry a Seasonal_<Season> FISH
# keyword. Plain seasonal / regional / common fish roll anywhere in a region, so they
# are deliberately NOT tied to a location.
LEGEND_LIST_EDID = "Fishing_LLS_FishCollection_LocalLegends"
_LCTN_COND = re.compile(r"GetInCurrentLocation\(.*?\[LCTN:([0-9A-Fa-f]{8})\]")


def load_local_legends():
    """LCTN formid -> [{name, edid, form_id, season}] from the Local Legend list."""
    path = tsv_source.newest("LVLI_Export_*_LVLI_Entries.tsv", channel=CHANNEL, required=False)
    if not path:
        return {}
    fish_info = {}
    try:
        with open(os.path.join(DIST, "fishing.json"), encoding="utf-8") as f:
            for x in json.load(f).get("fish", []):
                fish_info[x.get("edid") or ""] = x
    except (OSError, ValueError):
        pass
    out = defaultdict(list)
    for row in read_tsv(path):
        if (row.get("LVLI_EDID") or "") != LEGEND_LIST_EDID:
            continue
        ref = (row.get("LVLO_Reference") or "").split(":")
        if len(ref) < 3 or ref[2] != "FISH":
            continue
        conds = " ".join(v for k, v in row.items() if k and k.startswith("Cond") and v)
        m = _LCTN_COND.search(conds)
        if not m:
            continue
        info = fish_info.get(ref[1], {})
        out[m.group(1).upper()].append({
            "name": info.get("name") or ref[1],
            "edid": ref[1], "form_id": ref[0].upper(),
            "season": info.get("season") or "",
        })
    return out


def legend_fish_for(lctn_ids, legends, recs):
    """Local Legends that bite at this marker: the legend's LCTN is one of the marker's
    own, or a child of one (e.g. the Organ Cave interior sits under Organ Cave South)."""
    mine = set(lctn_ids)
    found = []
    for lf, fish in legends.items():
        cur, seen = lf, set()
        while cur and cur not in seen:
            if cur in mine:
                found.extend(fish)
                break
            seen.add(cur)
            cur = (recs.get(cur) or {}).get("pnam")
    return sorted(found, key=lambda f: f["name"].lower())


# Vendors at each map marker (Duchess, 7 Nov 2026) — joined from dist/vendors.json
# (build_vendors_json.py: NPC2 export + Mappalachia geo cache + vendor_overrides.tsv).
# Names only; no stock list. A vendor placed inside an interior cell carries that
# cell's Mappalachia name, which is not a map marker, so those few are pointed at the
# marker you walk in from.
VENDOR_MARKER_ALIASES = {
    "Highway Town Interior": "Highway Town",
    "The Crater Core": "The Crater",
    "Whitespring Mall": "The Whitespring Resort",
    "Whitespring Refuge": "The Whitespring Resort",
    "Gold Vault Operations": "Vault 79",
}
# Faction word in the vendor faction EDID -> the group players know.
VENDOR_GROUPS = [("Responders", "Responders"), ("Raider", "Raiders"), ("BoS", "Brotherhood of Steel"),
                 ("Brotherhood", "Brotherhood of Steel"), ("FreeStates", "Free States"),
                 ("Settler", "Settlers"), ("Wayward", "The Wayward"), ("Whitespring", "Whitespring"),
                 ("Fishing", "Fishing"), ("MoleMiner", "Mole Miners"), ("NWOT", "Nuka-World on Tour"),
                 ("Caravan", "Caravan"), ("Showmen", "Showmen"), ("Mobster", "Mobsters"),
                 ("_Muni_", "Municipality")]


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def load_vendors_by_marker():
    """normalised marker name -> [{name, group}] (one row per distinct name+group)."""
    try:
        with open(os.path.join(DIST, "vendors.json"), encoding="utf-8") as f:
            vendors = json.load(f).get("vendors", [])
    except (OSError, ValueError):
        return {}
    out = defaultdict(list)
    for v in vendors:
        marker = VENDOR_MARKER_ALIASES.get(v.get("marker") or "", v.get("marker") or "")
        if not marker or not v.get("name"):
            continue
        fac = v.get("faction") or ""
        group = next((g for tok, g in VENDOR_GROUPS if tok.lower() in fac.lower()), "")
        key = group.replace("The ", "").split()[0].rstrip("s") if group else ""
        if key and _norm(key) in _norm(v["name"]):
            group = ""                                   # "Responders Vendor" already says it
        row = {"name": v["name"], "group": group}
        if row not in out[_norm(marker)]:
            out[_norm(marker)].append(row)
    for k in out:
        out[k].sort(key=lambda r: r["name"].lower())
    return out


# Map markers straight from the game files (Duchess, 7 Nov 2026). Mappalachia only
# knows the markers of the game version it was built for, so new ones (PTS, or a
# patch Mappalachia hasn't caught up with yet) were missing — e.g. Skyline Blockade
# and Vault 63 Immortality Sector on the PTS. The REFR placements export, run on the
# STAT MapMarker [00000010], lists every placed marker with its name, icon type,
# position and cell Location. When that export exists for the channel it is the
# authority: new markers are added, and (once it clearly holds the whole set)
# markers the game no longer has are kept as Cut. Mappalachia still supplies icons and
# the cell->Location table for the markers it knows.
MAPMARKER_BASE = "00000010"
# Markers Bethesda has taken out of the game but which still show in Mappalachia's
# copy (Duchess, 7 Nov 2026). They stay on the page with a greyed-out checkbox and a
# Cut pill rather than vanishing. Only needed until the marker export exists: its
# MarkerFlags column says when a marker is disabled, and the export is then the list.
MARKERS_REMOVED = {"Milepost Zero"}
RENAME_DIST = 0.35 * CELL   # same spot, new name = Bethesda renamed it (Colonel Kelly -> Kelley)


def marker_hidden(flags):
    """True when the export's MarkerFlags say the marker is not on the in-game map:
    the REFR is initially disabled (Milepost Zero, and the Fissure Sites that are
    unmarked locations - Duchess, 7 Nov 2026). The FNAM byte is NOT a Visible flag
    in 76: The Whitespring Resort and Vault 79 have 0, most markers 8, region labels
    3 - so it is never used to hide a marker."""
    return "disabled" in (flags or "").lower()


def load_export_markers():
    path = tsv_source.newest("REFR_Placements_*.tsv", channel=CHANNEL, required=False)
    if not path:
        return [], ""
    global EXPORT_PATH
    EXPORT_PATH = path
    out = []
    for row in read_tsv(path):
        if hexid(row.get("BaseFormID")) != MAPMARKER_BASE:
            continue
        if (row.get("WorldspaceEDID") or "").upper() != WORLDSPACE_EDID:
            continue
        name = (row.get("MarkerName") or "").strip()
        if marker_hidden(row.get("MarkerFlags")):
            continue
        # "$REGION_ASH_HEAP" etc.: the region name labels on the map (an interface
        # string key, not a place). Not locations - leave them out.
        if name.startswith("$"):
            continue
        try:
            x, y = float(row.get("X") or ""), float(row.get("Y") or "")
        except ValueError:
            continue
        if name:
            out.append({"label": name, "type": (row.get("MarkerType") or "").strip(),
                        "x": round(x, 1), "y": round(y, 1),
                        "loc_edid": (row.get("LocationEDID") or "").strip()})
    return out, os.path.basename(path)


def merge_export_markers(markers, recs):
    """-> (markers on the map now, cut markers). Cut = in Mappalachia's copy but gone
    from (or disabled in) the game's own marker list, or listed in MARKERS_REMOVED."""
    exp, src = load_export_markers()
    if not exp:
        return ([m for m in markers if m["label"] not in MARKERS_REMOVED],
                [dict(m, cut=True) for m in markers if m["label"] in MARKERS_REMOVED])
    by_edid = {r["edid"]: fid for fid, r in recs.items()}
    known_icons = {m["icon"] for m in markers if m.get("icon")}
    seen, added, pending = set(), [], []
    type_icons = defaultdict(lambda: defaultdict(int))   # learnt: marker type -> icon
    for e in exp:
        hit = None
        for i, m in enumerate(markers):
            if norm(m["label"]) == norm(e["label"]) and \
                    (m["x"] - e["x"]) ** 2 + (m["y"] - e["y"]) ** 2 <= (1.5 * CELL) ** 2:
                hit = i
                break
        if hit is not None:
            seen.add(hit)
            if e["type"] and markers[hit].get("icon"):
                type_icons[e["type"]][markers[hit]["icon"]] += 1
            continue
        pending.append(e)
    # Renamed in place: an unmatched export marker sitting on an unmatched Mappalachia
    # marker is the same place under a new name (Hocking Hills Train Station -> Station).
    still, renamed = [], []
    for e in pending:
        hit = None
        for i, m in enumerate(markers):
            if i not in seen and (m["x"] - e["x"]) ** 2 + (m["y"] - e["y"]) ** 2 <= RENAME_DIST ** 2:
                hit = i
                break
        if hit is None:
            still.append(e)
            continue
        seen.add(hit)
        renamed.append(f"{markers[hit]['label']} -> {e['label']}")
        markers[hit] = dict(markers[hit], label=e["label"], old_label=markers[hit]["label"])
    pending = still
    for e in pending:
        # Icon: what markers of the same type already use (works whether the export
        # writes xEdit's type text or the raw number), else a name guess.
        learnt = type_icons.get(e["type"])
        guess = re.sub(r"[^A-Za-z0-9]", "", e["type"]) + "Marker"
        icon = (max(learnt, key=learnt.get) if learnt else
                guess if guess in known_icons else (e["type"] if e["type"] in known_icons else ""))
        lf = by_edid.get(e["loc_edid"])
        added.append({"label": e["label"], "icon": icon, "x": e["x"], "y": e["y"], "type_text": e["type"],
                      "cell_locs": [lf] if lf else [], "near_locs": [lf] if lf else []})
    cut = []
    if len(exp) >= 300:          # the whole marker set was exported, so it is authoritative
        keep = [m for i, m in enumerate(markers) if i in seen]
        here = {norm(m["label"]) for m in keep + added}
        for i, m in enumerate(markers):
            # a name still on the map (the many "Fissure Site"s, some disabled) is not cut
            if i not in seen and norm(m["label"]) not in here and \
                    norm(m["label"]) not in {norm(c["label"]) for c in cut}:
                cut.append(dict(m, cut=True))
        markers = keep
    else:
        cut = [dict(m, cut=True) for m in markers if m["label"] in MARKERS_REMOVED]
        markers = [m for m in markers if m["label"] not in MARKERS_REMOVED]
    print(f"[locations] map markers from {src}: {len(exp)} named; "
          f"{len(added)} new ({', '.join(a['label'] for a in added) or 'none'}); "
          f"{len(renamed)} renamed ({', '.join(renamed) or 'none'}); "
          f"{len(cut)} cut ({', '.join(c['label'] for c in cut) or 'none'})")
    return markers + added, cut


# The Pitt and Atlantic City (Duchess, 7 Nov 2026). These expedition areas have no map
# markers and are not in Mappalachia's Appalachia map, so each named area is listed by
# its LCTN record instead — the names Duchess picked from the game files. Quests,
# challenges, vendors and fish link through these LCTNs exactly like a marker's. Maps
# for them are made by hand, so they carry no x/y.
EXPEDITION_LOCATIONS = [
    # (region, display name, [LCTN EDIDs — the area plus its Daily Ops copy if any])
    ("The Pitt", "The Pitt \u2013 Industrial", ["XPD_Pitt01_Exterior_Location"]),
    ("The Pitt", "The Foundry", ["XPD_Pitt01_Dungeon_Location", "XPD_Pitt01_DailyOpsDungeon_Location"]),
    ("The Pitt", "Steel Tower", ["XPD_RttP01_SteelTower_Location"]),
    ("The Pitt", "The Trench", ["XPD_Pitt02_Exterior_Location"]),
    ("The Pitt", "Sanctum", ["XPD_Pitt02_Dungeon_Location"]),
    ("The Pitt", "Purgatory", ["XPD_Pitt02_ACCJ_Location"]),
    ("Atlantic City", "The Boardwalk", ["XPD_AC02_Boardwalk_Location"]),
    ("Atlantic City", "Showman's Pier", ["XPD_AC02_Pier_Location"]),
    ("Atlantic City", "Aquarium of the Atlantic", ["XPD_AC02_Aquarium_Location",
                                                  "XPD_AC02_Aquarium_DailyOpsDungeon_Location"]),
    ("Atlantic City", "Casino Quarter", ["XPD_AC03_Entertainment_District_Location"]),
    ("Atlantic City", "The Neapolitan Casino", ["XPD_AC03_Casino_Location"]),
    ("Atlantic City", "Quentino's Night Club", ["XPD_AC03_Night_Club_Location"]),
    ("Atlantic City", "Flooded City Center", ["XPD_AC02_Flooded_City_Location"]),
    ("Atlantic City", "City Hall", ["XPD_AC02_City_Hall_Location"]),
    ("Atlantic City", "Community Center", ["XPD_AC02_Community_Center_Location",
                                          "XPD_AC02_Community_Center_DailyOpsDungeon_Location"]),
]


def expedition_markers(recs):
    """Pseudo-markers for the Pitt / Atlantic City areas (no map position)."""
    by_edid = {r["edid"]: fid for fid, r in recs.items()}
    out = []
    for region, name, edids in EXPEDITION_LOCATIONS:
        fids = [by_edid[e] for e in edids if e in by_edid]
        if not fids:
            print(f"[locations] WARNING: no LCTN for {name} ({edids}) — left off")
            continue
        out.append({"label": name, "icon": "", "x": None, "y": None, "cell_locs": [], "near_locs": [],
                    "_lctn": fids, "_how": "expedition", "_all": set(fids), "_fixed_region": region})
    return out


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
              and not plan_sources.is_cut_content(edid)   # site-wide cut test
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
        cut = (m.get("cut") or edid.upper().startswith("CUT_") or bool(_DEAD_EDID.search(edid))
               or plan_sources.is_cut_content(edid))
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
    ledger, seeding = load_ledger()
    rings, _ = xref.load_mappalachia()
    recs = load_lctn()
    legends = load_local_legends()
    vendors_at = load_vendors_by_marker()
    refs = load_lctn_refs()
    quests = load_quests()
    event_locs = load_event_locations()
    chals, chal_lctn, chal_cndf, chal_refr = load_challenges()

    markers, cut_markers = merge_export_markers(markers, recs)
    match_markers(markers, recs)
    markers += expedition_markers(recs)
    for m in cut_markers:              # cut: listed, but nothing links to them any more
        m.update(_lctn=[], _all=set(), _how="cut")
    markers += cut_markers

    # CNDF -> CHAL through the LCTN refs export (a CNDF that names the LCTN).
    # Challenges placed by a REFR distance check go to the nearest marker.
    refr_marker = {}
    for ref, (x, y) in refr_pos.items():
        best, bd = None, 1e30
        for i, m in enumerate(markers):
            if m["x"] is None or m.get("cut"):
                continue
            dd = (m["x"] - x) ** 2 + (m["y"] - y) ** 2
            if dd < bd:
                best, bd = i, dd
        if best is not None and bd <= (2.5 * CELL) ** 2:
            refr_marker[ref] = best

    linked_q, linked_c = set(), set()
    regions = defaultdict(list)
    dup = defaultdict(int)
    ids_seen = defaultdict(int)
    for m in sorted(markers, key=lambda m: (m["label"].lower(), m["x"] or 0, m["y"] or 0)):
        ids_seen[slugify(m["label"])] += 1
    led_by_name, _cnt = {}, defaultdict(int)
    for lid_, e in ledger.items():
        _cnt[norm(e.get("name"))] += 1
        led_by_name[norm(e.get("name"))] = lid_
    led_by_name = {k: v for k, v in led_by_name.items() if _cnt[k] == 1}
    emitted = set()
    stamp = change_stamp()
    hist_over = load_history_overrides()
    for e in ledger.values():            # older ledger format
        e.pop("cut_live", None); e.pop("cut_pts", None)

    for idx, m in enumerate(markers):
        region = m.get("_fixed_region", "")
        for fid in ([] if region else m["_lctn"]):
            region = lctn_region(recs, fid)
            if region:
                break
        if region not in REGION_ORDER and not m.get("_fixed_region"):
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

        if m.get("cut"):
            qset, cset = set(), set()
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
        if ids_seen[base] == 1:
            # keep the id a place already had (ticks are saved by id) when it is renamed
            lid = led_by_name.get(norm(m["label"])) or \
                (slugify(m["old_label"]) if m.get("old_label") else lid)
        led = ledger.setdefault(lid, {})
        if not led.get(CHANNEL):
            if seeding:
                led[CHANNEL] = "baseline"
            elif CHANNEL == "pts" and led.get("live"):
                led[CHANNEL] = led["live"]
            else:
                led[CHANNEL] = stamp[0]
        if led[CHANNEL] != "baseline" and not (CHANNEL == "pts" and led.get("live")):
            log_event(led, "added", (led[CHANNEL], stamp[1]) if led[CHANNEL] == stamp[0] else
                      (led[CHANNEL], "PTS" if PTS else ""))
        old = m.get("old_label") or (led.get("name") if led.get("name") and led["name"] != m["label"] else "")
        if old and old != m["label"]:
            log_event(led, "renamed", stamp, **{"from": old})
        led.update(name=m["label"], icon=m["icon"], x=m["x"], y=m["y"], region=region)
        if m.get("cut"):
            log_event(led, "cut", stamp)
        emitted.add(lid)
        notes, added = history_for(lid, led, hist_over)
        on_live = CHANNEL == "live" or bool(led.get("live"))
        regions[region].append({
            "id": lid,
            "name": m["label"],
            "cut": bool(m.get("cut")),
            "added": added,
            "notes": notes,
            "type": icon_label(m["icon"]) or m.get("type_text", ""),
            "icon": m["icon"],
            "x": m["x"], "y": m["y"],
            # one-off: location maps are AVIF (Duchess, 7 Nov 2026); region maps stay JPEG.
            # PTS uses the live map unless the place is new on the PTS.
            "image": f"{IMAGE_ROOT if on_live else IMAGE_BASE}{lid}.avif",
            "lctn": [{"form_id": f, "edid": recs[f]["edid"], "name": recs[f]["full"]}
                     for f in sorted(m["_lctn"]) if f in recs],
            "match": m["_how"],
            "quests": [{"name": q["name"], "type": q["type"], "edid": q["edid"],
                        "form_id": q["form_id"]} for q in ql],
            "challenges": [{"name": c["name"], "type": c["type"], "required": c["required"],
                            "parent": c["parent"], "edid": c["edid"], "form_id": c["form_id"]}
                           for c in cl],
            "fish": [] if m.get("cut") else legend_fish_for(m["_lctn"], legends, recs),
            "vendors": [] if m.get("cut") else vendors_at.get(_norm(m["label"]), []),
        })

    # Places this channel listed before that are gone now (and not under another
    # name): still listed, as Cut, from what the ledger remembers of them.
    here = {norm(l["name"]) for locs in regions.values() for l in locs}
    for lid, e in ledger.items():
        if lid in emitted or not e.get(CHANNEL) or norm(e.get("name")) in here or not e.get("region"):
            continue
        log_event(e, "cut", stamp)
        notes, added = history_for(lid, e, hist_over)
        regions[e["region"]].append({
            "id": lid, "name": e["name"], "cut": True,
            "added": added, "notes": notes,
            "type": icon_label(e.get("icon")) if e.get("icon") else "", "icon": e.get("icon", ""),
            "x": e.get("x"), "y": e.get("y"),
            "image": f"{IMAGE_ROOT if (CHANNEL == 'live' or e.get('live')) else IMAGE_BASE}{lid}.avif",
            "lctn": [], "match": "cut", "quests": [], "challenges": [], "fish": [], "vendors": [],
        })
    save_ledger(ledger)

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
    ncut = sum(1 for r in out_regions for l in r["locations"] if l["cut"])
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
        # Region map tiles (Maps root expand). Drawn locally by render_all_locations_maps.py,
        # which also writes the list; the page HEAD-checks each one before showing it.
        "region_maps": _region_maps(),
        # NEW pill: a place whose "added" date is on/after the start of the season the
        # page is viewed in (Duchess, 7 Nov 2026) — the page picks the season from these.
        "season_starts": season_starts(),
        "unlinked": {
            "quests": unl_q,
            "challenges": unl_c,
            "markers_without_lctn": sorted(m["label"] for m in markers if not m["_lctn"]),
        },
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)

    print(f"[locations] {CHANNEL}: {total} locations ({ncut} cut) -> {os.path.relpath(OUT, REPO)}")
    for r in out_regions:
        nq = sum(1 for l in r["locations"] if l["quests"] or l["challenges"])
        print(f"   {r['name']:<16} {r['count']:>4}   ({nq} with quests/challenges)")
    print(f"   quests linked {len(linked_q)}, unlinked {len(unl_q)}; "
          f"challenges linked {len(linked_c)}, unlinked {len(unl_c)}; markers with no LCTN "
          f"{len(doc['unlinked']['markers_without_lctn'])}")
    if total - ncut < 400:
        raise SystemExit(f"[locations] only {total} locations — refusing to publish a short page.")


def _region_maps():
    """Live: data/locations/region_maps.json. PTS: the live tiles, with any region whose
    markers changed on the PTS swapped for its "-pts" tiles (region_maps_pts.json, written
    by render_all_locations_maps.py). Every tile carries its own url."""
    def rd(name):
        try:
            with open(os.path.join(REPO, "data", "locations", name), encoding="utf-8") as f:
                return json.load(f).get("maps", [])
        except (OSError, ValueError):
            return []
    live = [dict(m, url=IMAGE_ROOT + m["file"]) for m in rd("region_maps.json")]
    if not PTS:
        return live
    pts = [dict(m, url=IMAGE_ROOT + "pts/" + m["file"]) for m in rd("region_maps_pts.json")]
    changed = {m["region"] for m in pts}
    out = [m for m in live if m["region"] not in changed] + pts
    order = {r: i for i, r in enumerate(m["region"] for m in live)}
    return sorted(out, key=lambda m: (order.get(m["region"], 99), m.get("part", 1)))


if __name__ == "__main__":
    main()
