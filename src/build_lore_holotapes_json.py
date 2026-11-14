#!/usr/bin/env python3
r"""
build_lore_holotapes_json.py - the Lore Holotape Checklist
(/df/collectables/holotapes/checklist/, Duchess 14 Nov 2026).

Every holotape in the game (NOTE records), not the handful the old BOOK/MISC
guess found. Same JSON shape and renderer as the lore checklists
(df-bnb-collectables.js renderSlasherLore), with two ROOT expands:

  Collectable    lying in the world: placed, or in a placed container /
                 corpse / terminal (NOTE2 placements; before the first NOTE2
                 run: Referenced By holds a REFR / CONT / NPC_ / TERM / LVLI / ACTI)
  Environmental  only handed out by quests, dialogue or rewards

and region groups inside each (A-Z, Other/Unknown last). Region = point in
polygon on the NOTE2 coordinates (same engine as the Notes checklist), else the
quest's location (QUEST LNAM) for Environmental tapes.

Per holotape: How to Obtain (place text + map-marker / in-the-world picture
slots + route ledger), Transcript (ALLYDLG _Lore lines of the scene the tape
plays), Technical. Left out: holotape games (Program - own page), cut / test
records (plan_sources.is_cut_content + QA / debug / Babylon-only), and records
nothing in the game hands out.

Pictures: guide-images/collectables/Lore-Holotapes/<slug>-map.avif and
<slug>-location.avif, linked only when on data/server_listing.tsv.

    python src/build_lore_holotapes_json.py          # live -> dist/
    python src/build_lore_holotapes_json.py --pts    # PTS  -> dist/pts/
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

import tsv_source            # noqa: E402
import shared_images         # noqa: E402
import plan_sources          # noqa: E402
from build_slasher_lore_json import _rows, _ref, _slug, on_server   # noqa: E402

csv.field_size_limit(1 << 30)

OUT_NAME = "collectables_lore_holotapes.json"
IMG_DIR = "/wp-content/uploads/guide-images/collectables/Lore-Holotapes/"

REGIONS_AZ = ["Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
              "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley"]
OTHER = "Other/Unknown"
ROOTS = [("collectable", "Collectable", "Holotapes lying in the world: on the ground, in a container, on a body or in a terminal."),
         ("environmental", "Environmental", "Holotapes you only get from quests, characters or rewards.")]

# Referenced By types that put a holotape in the world / hand it out
WORLD_REFS = {"REFR", "CONT", "NPC_", "TERM", "LVLI", "ACTI"}
GIVE_REFS = {"QUST", "INFO", "GMRW", "SCEN", "BOOK"}
JUNK_REF = re.compile(r"(?i)^(qa|test|debug|zzz|babylon|del_|deleted|cut_|temp)")
TEST_CELL = re.compile(r"(?i)(^|[^a-z])(qa|test|debug|zzz|cut|temp)|testcell|^76qa|outsource")
CHALLENGE_LIST = "Challenge_Holotapes_All_Audio"
TYPE_WORDS = {"Voice": "Audio", "Terminal": "Terminal", "Sound": "Audio"}


def newest(root, pat):
    return tsv_source.newest(str(Path(root) / pat), required=False)


def name_map(path, fid_col, name_cols):
    out = {}
    for r in (_rows(path) if path else []):
        fid = (r.get(fid_col) or "").strip().upper()
        for c in name_cols:
            v = (r.get(c) or "").strip()
            if v:
                out[fid] = v
                break
    return out


def refs_of(r):
    out = []
    n = 1
    while ("Ref_%d" % n) in r:
        t = _ref(r.get("Ref_%d" % n))
        if t:
            out.append(t)
        n += 1
    return out


def quest_places(root):
    """quest FormID -> (name, LNAM location display name)."""
    out = {}
    for r in (_rows(newest(root, "QUEST_Export_*.tsv")) if newest(root, "QUEST_Export_*.tsv") else []):
        fid = (r.get("FormID") or "").strip().upper()
        nm = (r.get("FULL - Name") or "").strip()
        loc = (r.get("LNAM - Location") or "").strip()
        m = re.search(r'"([^"]+)"', loc)
        out[fid] = (nm, m.group(1) if m else "", (r.get("EDID") or "").strip())
    return out


def load_placements(root):
    """NOTE2 rows per holotape FormID (empty until the first NOTE2 run)."""
    p = newest(root, "NOTE2_Export_*.tsv")
    per, test_only = {}, set()
    for r in (_rows(p) if p else []):
        test_only.add((r.get("item_formid") or "").strip().upper())
        if TEST_CELL.search(r.get("cell") or "") or TEST_CELL.search(r.get("worldspace") or "") \
                or (r.get("loc_name") or "").strip().lower() == "quick test cell":
            continue                       # QA / debug / cut test cells
        per.setdefault((r.get("item_formid") or "").strip().upper(), []).append(r)
    return per, p, test_only - set(per)


def region_tools(root):
    try:
        import crossref_mappalachia_markers as xref
    except Exception as e:            # never fatal: everything -> Other/Unknown
        print("  WARNING: region resolver unavailable (%s)" % e, file=sys.stderr)
        return None
    try:
        rings, markers = xref.load_mappalachia()
    except BaseException:
        rings = markers = None
    try:
        by_name, by_core = xref.load_lctn_regions(str(root))
    except Exception:
        by_name, by_core = {}, {}
    # map marker name -> region, for interiors the LCTN names don't cover
    # ("Morgantown Airport Terminal" -> marker "Morgantown Airport")
    global MARKER_REGION
    MARKER_REGION = {}
    for nm, mx, my in (markers or []):
        try:
            reg = xref.region_for_xy(rings, mx, my, nearest=True)
        except Exception:
            reg = ""
        if reg and nm:
            MARKER_REGION[nm.lower()] = reg
    return xref, rings, markers, xref.load_manual_regions(), xref.load_geo_cache(), by_name, by_core


MARKER_REGION = {}


def marker_region(name):
    """Region of the map marker this place name belongs to: exact, else the
    longest marker name the place starts with ("AMS Headquarters Basement Lab")."""
    n = (name or "").lower().replace("near ", "", 1).strip()
    if not n:
        return ""
    if n in MARKER_REGION:
        return MARKER_REGION[n]
    best = ""
    for m in MARKER_REGION:
        if len(m) >= 5 and n.startswith(m) and len(m) > len(best):
            best = m
    return MARKER_REGION.get(best, "")


def build(root):
    note_p = newest(root, "NOTE_Export_*.tsv")
    if not note_p:
        sys.exit("no NOTE_Export_*.tsv in %s" % root)
    lore_p = newest(root, "ALLYDLG_Export_*_Lore.tsv")
    conts = name_map(newest(root, "CONT_Export_*.tsv"), "FormID", ["FULL"])
    npcs = name_map(newest(root, "NPC_Export_*[0-9].tsv"), "FormID", ["FULL"])
    terms = {}
    for r in (_rows(newest(root, "TERM_Export_*[0-9].tsv")) if newest(root, "TERM_Export_*[0-9].tsv") else []):
        if r.get("Row") == "HEADER" and r.get("Text"):
            terms.setdefault((r.get("TERM_FormID") or "").upper(), r["Text"].strip())
    quests = quest_places(root)
    placed, note2, test_only = load_placements(root)
    rt = region_tools(root)

    lore = {}
    for r in (_rows(lore_p) if lore_p else []):
        h = (r.get("HolotapeFormID") or "").upper()
        if h:
            lore.setdefault(h, []).append(((int(r.get("SceneOrder") or 0), int(r.get("LineOrder") or 0)),
                                           {"speaker": (r.get("SpeakerName") or "").strip(),
                                            "text": (r.get("Text") or "").strip()}))

    items, slugs, skipped = [], Counter(), Counter()
    for r in _rows(note_p):
        fid = (r.get("NOTE_FormID") or "").strip().upper()
        edid = (r.get("NOTE_EDID") or "").strip()
        name = (r.get("FULL") or "").strip()
        typ = (r.get("DNAM_Type") or "").strip()
        if typ == "Program":
            skipped["holotape game"] += 1; continue
        if not name or plan_sources.is_cut_content(edid) or JUNK_REF.match(edid):
            skipped["cut / test"] += 1; continue
        # only ever placed in the developers' test rooms: not in the game (Duchess, 14 Nov 2026)
        if fid in test_only:
            skipped["test rooms only"] += 1; continue
        all_refs = refs_of(r)
        # on the game's own "collect every holotape" challenge list: it counts in
        # game, so it stays even when the files don't show where it is yet
        on_list = any(t[1] == CHALLENGE_LIST for t in all_refs)
        refs = [t for t in all_refs if t[2] != "FLST" and not JUNK_REF.match(t[1] or "")]
        world = [t for t in refs if t[2] in WORLD_REFS]
        give = [t for t in refs if t[2] in GIVE_REFS]
        rows2 = placed.get(fid, [])
        if note2:
            is_world = bool(rows2)
        else:
            # a terminal that names a quest tape usually just accepts it (Air Data,
            # Ecological Balance), so TERM alone doesn't make a quest tape "lying around"
            is_world = any(t[2] != "TERM" for t in world) or (bool(world) and not give)
        if not is_world and not give and on_list:
            is_world = True
        if not is_world and not give:
            skipped["nothing hands it out"] += 1; continue

        routes, seen = [], set()

        def add(row, nm):
            if nm and (row, nm) not in seen:
                seen.add((row, nm)); routes.append({"row": row, "name": nm, "conditions": []})
        qlocs = []
        for f, ed, sig in refs:
            if sig == "QUST":
                q = quests.get(f, ("", "", ed))
                add("Quests", q[0] or q[2] or ed)
                if q[1]:
                    qlocs.append(q[1])
            elif sig == "CONT":
                add("Containers", conts.get(f) or ed)
            elif sig == "NPC_":
                add("Enemies", "On " + (npcs.get(f) or ed))
            elif sig == "TERM":
                add("Terminals", terms.get(f) or "A terminal")
            elif sig == "LVLI":
                add("Containers", "Random loot (" + ed + ")")
            elif sig == "GMRW":
                add("Events & Activities", ed)
        # place names: interior = the cell's name; outdoors = the nearest map marker
        places, regs = [], []
        for p in rows2:
            ws = (p.get("worldspace") or "").strip()
            ln = (p.get("loc_name") or "").strip()
            src = (p.get("loc_source") or "").strip()
            reg = nm = ""
            if rt:
                xref, rings, markers, manual, cache, by_name, by_core = rt
                if ws:            # outdoors (or an instanced worldspace): coordinates / manual map
                    try:
                        reg, nm, _h = xref.resolve(p, rings, markers, manual, cache)
                    except Exception:
                        reg, nm = "", ""
                    nm = ("near " + nm) if nm and src == "WRLDName" else (nm or "")
                if not ws or not reg:   # interior: coords are cell-local, so go by the cell's name
                    for cand in (ln, nm.replace("near ", "", 1)):
                        if not reg and cand:
                            reg = xref.lctn_region_for(by_name, by_core, display_name=cand) or marker_region(cand)
            if src != "WRLDName" and ln:
                nm = ln
            if nm and nm not in places:
                places.append(nm)
            if reg in REGIONS_AZ:
                regs.append(reg)
        if is_world and (rows2 and not any(p.get("holder") for p in rows2) or (not rows2 and any(t[2] == "REFR" for t in world))):
            add("World Spawns", ", ".join(places[:3]) + (" …" if len(places) > 3 else "") or "Lying in the world")

        # region
        region = Counter(regs).most_common(1)[0][0] if regs else ""
        if rt and not region:
            xref, rings, markers, manual, cache, by_name, by_core = rt
            for nm in qlocs:
                reg = xref.lctn_region_for(by_name, by_core, display_name=nm)
                if reg in REGIONS_AZ:
                    region = reg; break
        region = region or OTHER

        slug = _slug(name)
        slugs[slug] += 1
        if slugs[slug] > 1:
            slug += "-" + fid.lower()
        if is_world:
            more = len(places) - 4
            shown = ", ".join(places[:4])
            loc_text = (("Found " + ("" if shown.startswith("near ") else "at ") + shown) + (" and %d more places" % more if more > 0 else "") + "." if places else
                        "Location not added yet." if (world or rows2) else
                        "Counts towards the holotape challenge, but the game files don't show where it is placed. Location not added yet.")
            lead = "A holotape lying in the world" + (" — it can also come from a quest." if give else ".")
        else:
            qn = [x["name"] for x in routes if x["row"] == "Quests"]
            loc_text = ("Given during " + " / ".join(qn[:3]) + "." if qn else "Handed to you during a quest or event.")
            lead = "A holotape you are given, not one lying in the world."
        tr = [l for _, l in sorted(lore.get(fid, []), key=lambda x: x[0])]
        items.append({
            "formId": fid, "edid": edid, "name": name, "kind": "Holotape",
            "tapeType": TYPE_WORDS.get(typ, typ),
            "root": "collectable" if is_world else "environmental",
            "region": region,
            "location": {"text": loc_text, "fromFiles": True, "noFixedPlace": not is_world,
                         "mapImage": on_server(IMG_DIR + slug + "-map.avif"),
                         "locationImage": on_server(IMG_DIR + slug + "-location.avif")},
            "routes": routes,
            "lead": lead,
            "transcript": tr,
            "contents": "" if tr else ("A terminal holotape: load it into your Pip-Boy or a terminal to read it."
                                       if typ == "Terminal" else "Transcript not found in the game files."),
            "image": shared_images.HOLOTAPE_IMAGE,
            "imageSlug": slug,
        })

    groups = []
    for key, label, sub in ROOTS:
        for reg in REGIONS_AZ + [OTHER]:
            its = sorted((i for i in items if i["root"] == key and i["region"] == reg),
                         key=lambda i: (i["name"].lower(), i["formId"]))
            if its:
                groups.append({"key": key + ":" + _slug(reg), "root": key, "label": reg, "items": its})
    roots = [{"key": k, "label": l, "sub": s,
              "count": sum(1 for i in items if i["root"] == k)} for k, l, s in ROOTS]
    stats = {"items": len(items), "collectable": roots[0]["count"], "environmental": roots[1]["count"],
             "withTranscript": sum(1 for i in items if i["transcript"]),
             "regionKnown": sum(1 for i in items if i["region"] != OTHER),
             "placementsFile": Path(note2).name if note2 else None, "skipped": dict(skipped)}
    return {"generatedAt": datetime.now(timezone.utc).isoformat(), "type": "holotapes",
            "count": len(items), "roots": roots, "groups": groups, "stats": stats}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data-dir")
    ap.add_argument("--out-dir")
    a = ap.parse_args(argv)
    root = Path(a.data_dir) if a.data_dir else REPO / "tsv" / ("pts" if a.pts else "")
    out = Path(a.out_dir) if a.out_dir else REPO / "dist" / ("pts" if a.pts else "")
    out.mkdir(parents=True, exist_ok=True)
    data = build(root)
    (out / OUT_NAME).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Wrote %s: %s" % (out / OUT_NAME, json.dumps(data["stats"])))


if __name__ == "__main__":
    main()
