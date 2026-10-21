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
<<<<<<< Updated upstream
POOLS (Duchess, 21 Oct 2026). Skyline Valley and Burning Springs run their OWN
encounter pools - a Storm_RETrigger* point only ever fires a Skyline Valley
encounter, a Burn_RETrigger* point only a Burning Springs one, and a bare RETrigger*
point only an Appalachia one (launch, Wastelanders, Steel Dawn BS_, Blue Moon ...).
Putting them on one map sends players to Burning Springs for an Appalachia-only
encounter, so every map is ONE pool. The pool is read from the trigger's base EDID.
=======
The local layout MIRRORS the server folder one-for-one, so each folder is dragged
straight across in FileZilla and every page links the ONE hosted copy
(/wp-content/uploads/guide-images/random-encounters/...). Never copy these maps into
another guide's folder - point the page at the server path below instead.

  <root>/random-encounters-full-map.jpg          all types, colour per type
  <root>/random-encounters-numbered-map.jpg
  <root>/<region>-spawn-map.jpg                  e.g. the-mire-spawn-map.jpg
  <root>/<folder>/<folder>-full-map.jpg          folder = SERVER_DIR below
  <root>/<folder>/<folder>-numbered-map.jpg      (object, scene, travel, camp,
  <root>/<folder>/<region>-spawn-map.jpg          assault, whitesprings, mining)

Server: guide-images/random-encounters/  (+ /<folder>/ for each type)
>>>>>>> Stashed changes

The local layout MIRRORS the server folder one-for-one
(/wp-content/uploads/guide-images/random-encounters/...), so a folder is dragged
straight across in FileZilla and every page links the ONE hosted copy. Never copy
these maps into another guide's folder - point the page at the server path.

  Server layout (Duchess, 21 Oct 2026) - Appalachia is the default pool, so a TYPE
  folder holds Appalachia; the two expansion pools get a folder each holding every type:

  <root>/random-encounters-appalachia-full-map.jpg         all Appalachia types, colour
  <root>/random-encounters-appalachia-numbered-map.jpg      per type, + <region>-spawn-map.jpg
  <root>/<type>/maps/<type>-appalachia-full-map.jpg
  <root>/<type>/maps/<type>-appalachia-numbered-map.jpg
  <root>/<type>/maps/<region>-spawn-map.jpg                 region tiles (Appalachia only)
  <root>/<pool>/maps/random-encounters-<pool>-full-map.jpg  pool = skyline-valley,
  <root>/<pool>/maps/<type>-<pool>-full-map.jpg                    burning-springs
  <root>/<pool>/maps/<type>-<pool>-numbered-map.jpg         (already zoomed - no tiles)
  <folder>/location-photos/   <type>-<NN>-marker.jpg + <type>-<NN>-location.jpg
                              (NN = the number on that type's numbered map)
  <folder>/encounter-photos/  <encounter-slug>-1.png, -2 ...
  <folder>/photo-list.tsv     what to photograph; photos already there are ticked
  (<folder> = <type> for Appalachia, <pool> for Skyline Valley / Burning Springs)

  type = object, scene, travel, camp, assault, whitespring (server: whitesprings), mining
  pool = appalachia, skyline-valley, burning-springs  (only pools with points are written)

Every image goes through map_watermark.apply() - standing rule, no map leaves
unmarked. Scene maps carry the note that tameable pets turn up at Scene encounters.

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

# type key -> folder name, local AND on the server (guide-images/random-encounters/<x>/)
SERVER_DIR = {"object": "object", "scene": "scene", "travel": "travel", "camp": "camp",
              "assault": "assault", "whitespring": "whitesprings", "mining": "mining"}


def slug(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


<<<<<<< Updated upstream
# trigger EDID prefix -> pool (folder name, title)
POOLS = {"": ("appalachia", "Appalachia"),
         "Storm": ("skyline-valley", "Skyline Valley"),
         "Burn": ("burning-springs", "Burning Springs")}
POOL_ORDER = ["appalachia", "skyline-valley", "burning-springs"]
POOL_REGIONS = {"Skyline Valley", "Burning Springs"}   # regions that ARE a pool


def pool_of(eid):
    pre = eid.split("_", 1)[0] if "_RETrigger" in eid else ""
    return POOLS.get(pre, POOLS[""])


=======
>>>>>>> Stashed changes
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
                         "x": float(x), "y": float(y), "type": key,
                         "pool": pool_of(eid)[0]})
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


def _base_map():
    bg = os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    base = Image.open(bg).convert("RGB")
    return base.resize((S, S), Image.LANCZOS) if base.size != (S, S) else base


def _crop_box(pts, pad=170, top=380, min_side=900):
    """The pool's own points plus padding - extra at the top so the legend never
    sits on a dot. Each side at least min_side so a tight cluster isn't blown up."""
    xs = [p["px"] for p in pts]; ys = [p["py"] for p in pts]
    x0, x1, y0, y1 = min(xs) - pad, max(xs) + pad, min(ys) - top, max(ys) + pad
    if x1 - x0 < min_side:
        c = (x0 + x1) / 2; x0, x1 = c - min_side / 2, c + min_side / 2
    if y1 - y0 < min_side:
        c = (y0 + y1) / 2; y0, y1 = c - min_side / 2, c + min_side / 2
    return (max(0, int(x0)), max(0, int(y0)), min(S, int(x1)), min(S, int(y1)))


def _save(img, path, corner="bottom-left"):
    map_watermark.apply(img, corner=corner).save(path, "JPEG", quality=88, optimize=True)


LISTS_ONLY = False


def render(stem, title, layers, out_dir, to_px, boxes, note=None, zoom=False, tiles=True):
    """layers = [(type_key, points)] for ONE pool. Writes <stem>-full-map.jpg,
    <stem>-numbered-map.jpg and (tiles) <region>-spawn-map.jpg. zoom=True crops both
    maps to the pool's own area (Skyline Valley / Burning Springs). Returns the
    numbered rows (n, region, marker, x, y)."""
    if LISTS_ONLY:      # same numbering as the drawn maps, nothing written
        rows = [p for _, pts in layers for p in pts]
        rows.sort(key=lambda r: (r["region"], r["marker"].lower(), round(r["x"]), round(r["y"])))
        for i, r in enumerate(rows, 1):
            r["n"] = i
        return rows
    os.makedirs(out_dir, exist_ok=True)
    plain = _base_map()
    d = ImageDraw.Draw(plain)
    rows, legend = [], []
    for key, pts in layers:
        fill = TYPES[key][3]
        for p in pts:
            p["px"], p["py"] = to_px(p["x"], p["y"])
            rsm.draw_marker(d, p["px"], p["py"], fill, "circle")
        legend.append((TYPES[key][0], len(pts), fill, "circle"))
        rows.extend(pts)

    # numbering: region A-Z, then nearest marker, then position - stable run to run
    rows.sort(key=lambda r: (r["region"], r["marker"].lower(), round(r["x"]), round(r["y"])))
    numbered = plain.copy()
    dn = ImageDraw.Draw(numbered)
    f = rsm._font(56)
    for i, r in enumerate(rows, 1):
        r["n"] = i
        rsm.draw_outlined_text(dn, (r["px"] + rsm.DOT_D, r["py"] - 34), str(i), f, w=3)
    numbered_raw = numbered.copy()          # tiles crop this: no legend, not zoomed

<<<<<<< Updated upstream
    if zoom:
        box = _crop_box(rows)
        outs = []
        for img in (plain, numbered):
            c = img.crop(box)
            w, h = c.size
            c = c.resize((TILE_WIDTH, max(1, int(h * TILE_WIDTH / w))), Image.LANCZOS)
            outs.append(c)
        plain, numbered = outs
    for img in (plain, numbered):
        rsm.draw_legend(img, title, legend)
        if note:
            draw_note_box(img, note, rsm.LEGEND_PAD + legend_height(len(legend)) + 24)
    _save(plain, os.path.join(out_dir, f"{stem}-full-map.jpg"))
    _save(numbered, os.path.join(out_dir, f"{stem}-numbered-map.jpg"))

    n_tiles = 0
    if tiles:
        # Appalachia region tiles. A point sitting just over a pool region's border
        # (an Appalachia trigger inside the Burning Springs outline) joins the nearest
        # Appalachia region's tile, which grows to hold it.
        app_boxes = {k: v for k, v in boxes.items() if k not in POOL_REGIONS}
        by_region = defaultdict(list)
        for r in rows:
            reg = r["region"]
            if reg in POOL_REGIONS or reg not in app_boxes:
                reg = min(app_boxes, key=lambda k: (
                    max(app_boxes[k][0] - r["x"], 0, r["x"] - app_boxes[k][2]) ** 2 +
                    max(app_boxes[k][1] - r["y"], 0, r["y"] - app_boxes[k][3]) ** 2))
            by_region[reg].append(r)
        for region, rrows in sorted(by_region.items()):
            bx = app_boxes[region]
            x0, y0 = to_px(bx[0], bx[3])
            x1, y1 = to_px(bx[2], bx[1])
            xs = [x0, x1] + [r["px"] for r in rrows]
            ys = [y0, y1] + [r["py"] for r in rrows]
            pad = 120
            x0 = max(0, int(min(xs) - pad)); y0 = max(0, int(min(ys) - pad))
            x1 = min(S, int(max(xs) + pad)); y1 = min(S, int(max(ys) + pad))
            crop = numbered_raw.crop((x0, y0, x1, y1))
            w, h = crop.size
            crop = crop.resize((TILE_WIDTH, max(1, int(h * TILE_WIDTH / w))), Image.LANCZOS)
            if note:
                draw_note_box(crop, note)
            _save(crop, os.path.join(out_dir, f"{slug(region)}-spawn-map.jpg"), corner="bottom-right")
            n_tiles += 1
    print(f"  {stem:34} points={len(rows):4} tiles={n_tiles}")
    return rows


# ---------------------------------------------------------------- photo lists

def encounters_by_pool():
    """{(type_key, pool): [{name, edid, form_id, content}]} from the built random
    encounter pages (dist/random_encounters/random_encounters.json). The pool comes
    from the encounter's own trigger, exactly as for the map points."""
    path = os.path.join(REPO, "dist", "random_encounters", "random_encounters.json")
    out = defaultdict(dict)
    if not os.path.exists(path):
        return out
    import json
    label_to_key = {v[0].lower(): k for k, v in TYPES.items()}
    acc = []

    def walk(o):
        if isinstance(o, dict):
            if "edid" in o and "trigger" in o and "type" in o:
                acc.append(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(json.load(open(path, encoding="utf-8")))
    for e in acc:
        key = label_to_key.get((e.get("type") or "").lower())
        if not key or not e.get("trigger"):
            continue
        pool = pool_of(e["trigger"])[0]
        out[(key, pool)].setdefault(e["edid"], {
            "name": e.get("name") or e.get("game_name") or e["edid"],
            "edid": e["edid"], "form_id": e.get("form_id", ""),
            "content": e.get("content", "")})
    return {k: sorted(v.values(), key=lambda x: x["name"].lower()) for k, v in out.items()}


def write_photo_list(folder, sets):
    """photo-list.tsv + the two photo folders for one server folder. sets =
    [(type_dir, rows, encounters)] - a type folder has one set, a pool folder one per
    type. Photos already dropped in are ticked; a photo is never touched."""
    loc_dir = os.path.join(folder, "location-photos")
    enc_dir = os.path.join(folder, "encounter-photos")
    os.makedirs(loc_dir, exist_ok=True)
    os.makedirs(enc_dir, exist_ok=True)
    exts = (".jpg", ".jpeg", ".png", ".avif", ".webp")
    files = {d: [f.lower() for f in os.listdir(d)] for d in (loc_dir, enc_dir)}
    # <stem>.jpg, or numbered shots <stem>-1.png, <stem>-2.png ...
    have = lambda d, stem: any(f.endswith(exts) and (os.path.splitext(f)[0] == stem or
                               re.match(re.escape(stem) + r"-\d+$", os.path.splitext(f)[0]))
                               for f in files[d])
    with open(os.path.join(folder, "photo-list.tsv"), "w", encoding="utf-8", newline="") as fh:
        fh.write("type\tsection\tnumber\tname\tregion\tnearest location\tphoto files\tdone\n")
        for tdir, rows, encounters in sets:
            for r in rows:
                n = f"{tdir}-{r['n']:02d}"
                done = have(loc_dir, f"{n}-marker") and have(loc_dir, f"{n}-location")
                fh.write(f"{tdir}\tlocation\t{r['n']:02d}\t\t{r['region']}\t{r['marker']}\t"
                         f"location-photos/{n}-marker, location-photos/{n}-location\t"
                         f"{'yes' if done else ''}\n")
            for e in encounters:
                stem = slug(e["name"])
                fh.write(f"{tdir}\tencounter\t\t{e['name']}\t\t\tencounter-photos/{stem}\t"
                         f"{'yes' if have(enc_dir, stem) else ''}\n")
=======
    # flat, server-named files (see OUTPUT above): one folder = one server folder
    os.makedirs(out_dir, exist_ok=True)
    stem = "random-encounters" if name == "all" else SERVER_DIR[name]
    map_watermark.apply(plain).save(os.path.join(out_dir, f"{stem}-full-map.jpg"),
                                    "JPEG", quality=88, optimize=True)
    map_watermark.apply(numbered).save(
        os.path.join(out_dir, f"{stem}-numbered-map.jpg"),
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
        crop.save(os.path.join(out_dir, f"{slug(region)}-spawn-map.jpg"),
                  "JPEG", quality=88, optimize=True)
        tiles += 1
    print(f"  {name:12} points={len(rows):4} regions={tiles}")
>>>>>>> Stashed changes


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--types", nargs="+", choices=list(TYPES), default=list(TYPES))
    ap.add_argument("--no-all", action="store_true", help="skip the all-encounters maps")
    ap.add_argument("--only-all", action="store_true", help="only the all-encounters maps")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true", help="print counts, render nothing")
    ap.add_argument("--lists-only", action="store_true",
                    help="refresh every photo-list.tsv (all types) without redrawing a map")
    args = ap.parse_args()
    global LISTS_ONLY
    LISTS_ONLY = args.lists_only
    if args.lists_only:
        args.types, args.no_all = list(TYPES), True

    conn = rsm._db()
    trig, skipped = load_triggers(conn)
    qc = quest_counts()
    print(f"{'type':22} {'pool':16} triggers")
    for k, v in TYPES.items():
        for pool in POOL_ORDER:
            n = sum(1 for p in trig[k] if p["pool"] == pool)
            if n:
                print(f"{v[0]:22} {pool:16} {n:8}")
        if skipped[k]:
            print(f"{v[0]:22} (+{skipped[k]} interior, not drawn)")
    if args.check:
        return

    import crossref_mappalachia_markers as xref
    rings, markers = xref.load_mappalachia()[:2]
    for pts in trig.values():
        locate(pts, rings, markers)

    to_px = rsm.projector(rsm.load_space(conn))
    boxes = region_bboxes(conn)
    os.makedirs(args.out, exist_ok=True)
    pool_title = {v[0]: v[1] for v in POOLS.values()}

    def folder_of(pool, type_dir=None):
        """Appalachia lives in the TYPE folder; a pool folder holds every type."""
        if pool == "appalachia":
            return os.path.join(args.out, type_dir) if type_dir else args.out
        return os.path.join(args.out, pool)

    if not args.no_all:
        for pool in POOL_ORDER:
            layers = [(k, [dict(p) for p in trig[k] if p["pool"] == pool]) for k in TYPES]
            layers = [l for l in layers if l[1]]
            if layers:
                out = folder_of(pool) if pool == "appalachia" else os.path.join(folder_of(pool), "maps")
                render(f"random-encounters-{pool}", f"{pool_title[pool]} Random Encounters",
                       layers, out, to_px, boxes,
                       zoom=pool != "appalachia", tiles=pool == "appalachia")
    if args.only_all:
        return
    encs = encounters_by_pool()
    photo_sets = defaultdict(list)
    for k in TYPES:
        if k not in args.types:
            continue
        label = TYPES[k][0]
<<<<<<< Updated upstream
        for pool in POOL_ORDER:
            pts = [dict(p) for p in trig[k] if p["pool"] == pool]
            if not pts:
                continue
            folder = folder_of(pool, SERVER_DIR[k])
            rows = render(f"{SERVER_DIR[k]}-{pool}", f"{pool_title[pool]} {label} Encounters",
                          [(k, pts)], os.path.join(folder, "maps"), to_px, boxes,
                          note=PET_NOTE if k == "scene" else None,
                          zoom=pool != "appalachia", tiles=pool == "appalachia")
            photo_sets[folder].append((SERVER_DIR[k], rows, encs.get((k, pool), [])))
    for folder, sets in photo_sets.items():
        # a pool folder's list is rewritten whole, so only write it when every type ran
        if os.path.basename(folder) in ("skyline-valley", "burning-springs") and \
                set(args.types) != set(TYPES):
            print(f"  (photo list for {os.path.basename(folder)} left as is - run all types to refresh it)")
            continue
        write_photo_list(folder, sets)
=======
        render(k, f"{label} Random Encounters", [(k, [dict(p) for p in trig[k]])],
               os.path.join(args.out, SERVER_DIR[k]), to_px, boxes,
               note=PET_NOTE if k == "scene" else None)

>>>>>>> Stashed changes

if __name__ == "__main__":
    main()
