#!/usr/bin/env python3
r"""
render_random_encounter_maps.py - Random Encounter location maps, watermarked.

WHERE THE POINTS COME FROM
==========================
Every random encounter in Appalachia fires from a placed trigger activator, and the
trigger's BASE tells you the encounter TYPE:

    RETriggerObject / RETriggerScene / RETriggerTravel / RETriggerCamp /
    RETriggerAssault / RETriggerWhitespringAssault / RETriggerMining

The expansion regions carry their OWN trigger bases, prefixed by the region's
editor family — Skyline Valley is `Storm_RETrigger<Type>` and Burning Springs is
`Burn_RETrigger<Type>`. Matching only the bare `RETrigger*` names silently drops
both regions (that shipped once: Skyline Valley came out with no encounters).
Any `<Prefix>_RETrigger<Type>` is picked up by suffix, so a future region works
with no edit here. (StormEncounterMarker is a different system — the Storm
Encounters, Storm_SE## — and is deliberately NOT a random encounter trigger.)

Those are read straight out of the Mappalachia DB (Position x Entity), so no export
is needed. The type list is the same one the quest export uses — the RE quests are
named [<Prefix>_]RE_Object*, RE_Scene*, … (W05_, BS_, Burn_, Storm_ …) — and --check prints the quest count per type beside the
trigger count so the two can be compared.

Only the APPALACHIA worldspace is drawn. (One RETriggerAssault sits inside the
RollinsLaborCamp01 interior; it has no exterior position and is reported, not drawn.)

OUTPUT (under --out, default "Guides and Stuff\.Random Encounters")
======
  <root>/01 Full Maps (4096)/random_encounters_all.jpg        colour per type
  <root>/02 Numbered Maps (4096)/random_encounters_all_numbered.jpg
  <root>/03 Region Tiles/<Region>_all.jpg
  <root>/<Type>/01 Full Maps (4096)/random_encounters_<type>.jpg
  <root>/<Type>/02 Numbered Maps (4096)/random_encounters_<type>_numbered.jpg
  <root>/<Type>/03 Region Tiles/<Region>_<type>.jpg

Every image goes through map_watermark.apply() — standing rule, no map leaves
unmarked. Scene maps (full, numbered and every Scene tile) carry the note that
tameable pets turn up at Scene encounter locations.

Usage:
  python render_random_encounter_maps.py
  python render_random_encounter_maps.py --types scene object
  python render_random_encounter_maps.py --check        # counts only, no images

Env: MAPPALACHIA_DIR / MAPPALACHIA_DB / GUIDES_ROOT / RE_MAPS_OUT
"""

import argparse
import os
import re
import sys
from collections import Counter, defaultdict

from PIL import Image, ImageDraw

Image.MAX_IMAGE_PIXELS = None

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import map_watermark                      # standing rule: every map is watermarked
import render_slasher_maps as rsm         # same background, dots, legend, fonts
from render_spawn_maps import region_bboxes   # the fixed one (matches <Family>SubRegion##)

S = rsm.S
APPALACHIA_SPACE = rsm.APPALACHIA_SPACE
TILE_WIDTH = rsm.TILE_WIDTH

GUIDES_ROOT = os.environ.get(
    "GUIDES_ROOT", os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff"))
DEFAULT_OUT = os.environ.get("RE_MAPS_OUT", os.path.join(GUIDES_ROOT, ".Random Encounters"))

PET_NOTE = "Tameable pets can be found at Scene encounter locations."

# key -> (folder / legend label, trigger base EDID, quest EDID prefix, dot colour)
# Order is the legend order on the all-encounters map.
TYPES = {
    "object":      ("Object",              "RETriggerObject",             "RE_Object",             (255, 193, 7)),
    "scene":       ("Scene",               "RETriggerScene",              "RE_Scene",              (224, 64, 224)),
    "travel":      ("Travel",              "RETriggerTravel",             "RE_Travel",             (41, 182, 246)),
    "camp":        ("CAMP",                "RETriggerCamp",               "RE_Camp",               (102, 187, 106)),
    "assault":     ("Assault",             "RETriggerAssault",            "RE_Assault",            (229, 57, 53)),
    "whitespring": ("Whitespring Assault", "RETriggerWhitespringAssault", "RE_WhitespringAssault", (255, 152, 0)),
    "mining":      ("Mining",              "RETriggerMining",             "RE_Mining",             (236, 236, 236)),
}


# ---------------------------------------------------------------- data

RE_BASE = re.compile(r"^(?:[A-Za-z0-9]+_)?RETrigger[A-Z]")

def load_triggers(conn):
    """{type_key: [{ref, x, y}]} for every RE trigger in the Appalachia worldspace."""
    base_to_key = {v[1]: k for k, v in TYPES.items()}
    q = ("SELECT e.editorID, p.referenceFormID, p.x, p.y, p.spaceFormID "
         "FROM Position p JOIN Entity e ON e.entityFormID = p.referenceFormID "
         "WHERE e.editorID LIKE '%RETrigger%'")
    out = defaultdict(list)
    skipped = Counter()
    for eid, ref, x, y, space in conn.execute(q):
        # bare `RETriggerScene` or region-prefixed `Storm_RETriggerScene`
        # The LIKE is loose on purpose, so gate on the real shape here:
        # `MTR07_EarthIgnitionCoreTrigger` contains "coRETrigger" and is not one.
        if not RE_BASE.match(eid):
            continue
        key = base_to_key.get(eid) or base_to_key.get(eid.split("_", 1)[-1])
        if not key:
            print(f"  (unknown trigger base {eid} — add it to TYPES)", file=sys.stderr)
            continue
        if space != APPALACHIA_SPACE:
            skipped[key] += 1
            continue
        out[key].append({"ref": "%08X" % ref if isinstance(ref, int) else str(ref),
                         "x": float(x), "y": float(y), "type": key})
    return out, skipped


def quest_counts():
    """RE quest count per type from the newest QUEST export (for --check only)."""
    # tsv_source picks the export — never glob + sort, and never mtime.
    import tsv_source
    p = tsv_source.newest("QUEST_Export_*.tsv", required=False)
    if not p:
        return {}
    paths = [p]
    counts = Counter()
    prefixes = sorted(((v[2], k) for k, v in TYPES.items()), key=lambda t: -len(t[0]))
    with open(paths[-1], encoding="utf-8", errors="replace") as fh:
        fh.readline()
        for line in fh:
            cols = line.split("\t")
            if len(cols) < 2:
                continue
            eid = cols[1]
            if eid[:3].lower() in ("zzz", "del", "cut") or eid.startswith("COMP_"):
                continue                  # cut / deleted / compatibility records
            if "_RE_" in eid:
                eid = "RE_" + eid.split("_RE_", 1)[1]   # Storm_RE_Scene01 -> RE_Scene01
            for pre, k in prefixes:          # longest prefix first: WhitespringAssault
                if eid.startswith(pre):      # must not count as plain Assault
                    counts[k] += 1
                    break
    return counts


def locate(points, rings, markers):
    import crossref_mappalachia_markers as xref
    for p in points:
        p["region"] = xref.region_for_xy(rings, p["x"], p["y"], nearest=True) or "Unknown"
        m = xref.nearest_marker(markers, p["x"], p["y"])
        p["marker"] = (m[0] if isinstance(m, (list, tuple)) else m) or "Unmarked"


# ---------------------------------------------------------------- drawing

def draw_note_box(img, text, anchor_y=None):
    """A legend-style box holding one line of text, under the legend (full maps)
    or top-left (tiles)."""
    d = ImageDraw.Draw(img)
    size = 64 if img.size[0] >= 4000 else 48
    f = rsm._font(size)
    tw = d.textlength(text, font=f)
    x0 = rsm.LEGEND_PAD
    y0 = anchor_y if anchor_y is not None else rsm.LEGEND_PAD
    box = [x0, y0, x0 + int(tw) + 64, y0 + size + 44]
    d.rectangle(box, fill=rsm.LEGEND_BG, outline=rsm.LEGEND_BORDER,
                width=rsm.LEGEND_BORDER_W)
    d.text((x0 + 32, y0 + 18), text, font=f, fill=(255, 255, 255))
    return img


def legend_height(n_rows):
    return int(96 + 26 + n_rows * 84 + 24)


def render(name, title, layers, out_dir, to_px, boxes, note=None):
    """layers = [(type_key, points)] ; writes full / numbered / region tiles."""
    bg = os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    base = Image.open(bg).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    rows, legend = [], []
    for key, pts in layers:
        fill = TYPES[key][3]
        for p in pts:
            p["px"], p["py"] = to_px(p["x"], p["y"])
            rsm.draw_marker(d, p["px"], p["py"], fill, "circle")
        legend.append((TYPES[key][0], len(pts), fill, "circle"))
        rows.extend(pts)
    rsm.draw_legend(plain, title, legend)
    if note:
        draw_note_box(plain, note, rsm.LEGEND_PAD + legend_height(len(legend)) + 24)

    # numbering: region A-Z, then nearest marker, then position — stable run to run
    rows.sort(key=lambda r: (r["region"], r["marker"].lower(), round(r["x"]), round(r["y"])))
    numbered = plain.copy()
    dn = ImageDraw.Draw(numbered)
    f = rsm._font(56)
    for i, r in enumerate(rows, 1):
        r["n"] = i
        rsm.draw_outlined_text(dn, (r["px"] + rsm.DOT_D, r["py"] - 34), str(i), f, w=3)

    full_dir = os.path.join(out_dir, "01 Full Maps (4096)")
    num_dir = os.path.join(out_dir, "02 Numbered Maps (4096)")
    tile_dir = os.path.join(out_dir, "03 Region Tiles")
    for p in (full_dir, num_dir, tile_dir):
        os.makedirs(p, exist_ok=True)
    map_watermark.apply(plain).save(os.path.join(full_dir, f"random_encounters_{name}.jpg"),
                                    "JPEG", quality=88, optimize=True)
    map_watermark.apply(numbered).save(
        os.path.join(num_dir, f"random_encounters_{name}_numbered.jpg"),
        "JPEG", quality=88, optimize=True)

    by_region = defaultdict(list)
    for r in rows:
        by_region[r["region"]].append(r)
    tiles = 0
    for region, rrows in sorted(by_region.items()):
        box = boxes.get(region)
        if box:
            x0, y0 = to_px(box[0], box[3])
            x1, y1 = to_px(box[2], box[1])
        else:
            xs = [r["px"] for r in rrows]; ys = [r["py"] for r in rrows]
            x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        pad = 120
        x0 = max(0, int(x0 - pad)); y0 = max(0, int(y0 - pad))
        x1 = min(S, int(x1 + pad)); y1 = min(S, int(y1 + pad))
        if x1 - x0 < 200 or y1 - y0 < 200:
            cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
            x0, x1 = max(0, cx - 400), min(S, cx + 400)
            y0, y1 = max(0, cy - 400), min(S, cy + 400)
        crop = numbered.crop((x0, y0, x1, y1))
        w, h = crop.size
        crop = crop.resize((TILE_WIDTH, max(1, int(h * TILE_WIDTH / w))), Image.LANCZOS)
        if note:
            draw_note_box(crop, note)
        crop = map_watermark.apply(crop, corner="bottom-right")
        crop.save(os.path.join(tile_dir, f"{region.replace(' ', '')}_{name}.jpg"),
                  "JPEG", quality=88, optimize=True)
        tiles += 1
    print(f"  {name:12} points={len(rows):4} regions={tiles}")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--types", nargs="+", choices=list(TYPES), default=list(TYPES))
    ap.add_argument("--no-all", action="store_true", help="skip the all-encounters map")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true", help="print counts, render nothing")
    args = ap.parse_args()

    conn = rsm._db()
    trig, skipped = load_triggers(conn)
    qc = quest_counts()
    print(f"{'type':22} triggers  quests")
    for k, v in TYPES.items():
        extra = f"  (+{skipped[k]} interior, not drawn)" if skipped[k] else ""
        print(f"{v[0]:22} {len(trig[k]):8}  {qc.get(k, 0):6}{extra}")
    if args.check:
        return

    import crossref_mappalachia_markers as xref
    rings, markers = xref.load_mappalachia()[:2]
    for pts in trig.values():
        locate(pts, rings, markers)

    to_px = rsm.projector(rsm.load_space(conn))
    boxes = region_bboxes(conn)
    os.makedirs(args.out, exist_ok=True)

    if not args.no_all:
        render("all", "Random Encounters",
               [(k, [dict(p) for p in trig[k]]) for k in TYPES],
               args.out, to_px, boxes)
    for k in args.types:
        label = TYPES[k][0]
        render(k, f"{label} Random Encounters", [(k, [dict(p) for p in trig[k]])],
               os.path.join(args.out, label), to_px, boxes,
               note=PET_NOTE if k == "scene" else None)


if __name__ == "__main__":
    main()
