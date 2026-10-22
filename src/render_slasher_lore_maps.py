#!/usr/bin/env python3
r"""
render_slasher_lore_maps.py - maps for /df/collectables/slasher-lore/checklist/.

Where the Slasher notes and holotapes turn up, read from the game files:

  * NOT the masks: the Article Clippings and the children's book page come out of
    the Pint-Sized Slasher Masks (SDOW_MQ01_Bodies hands one out per batch of masks -
    its stage notes say so), and the masks have their own spawn guide, so they are
    left off (Duchess, 22 Oct 2026). The page says so in words.
  * The quest spots: every alias in SPOT_ALIASES is resolved in SeventySix.esm -
    a Location Ref Type alias (ALRT) to the one placed ref carrying that type, a
    forced-ref alias (ALFR) to its ref. Nothing positional is typed here.

The Disturbed Graves are left off (Duchess, 22 Oct 2026 - they have their own maps
on the grave pages).

Outputs (default: Guides and Stuff\.Collectables\Slasher Lore):
  01 Full Maps\slasher-lore-map.jpg
  03 Region Tiles\<region-slug>-lore-map.jpg   (only regions with a spot)
  slasher-lore_coords.csv
Server folder: /wp-content/uploads/guide-images/collectables/Slasher-Lore/ (JPEG -
maps never go up as AVIF). build_slasher_lore_json.py links whichever of them are
on data/server_listing.tsv.

Run on the PC (needs the game files and the Mappalachia DB + background):
  python src\render_slasher_lore_maps.py
Env: MAPPALACHIA_DIR, FO76_DATA (game Data folder), SLASHER_LORE_OUT
"""

import argparse, csv, os, struct, sys
from collections import defaultdict

from PIL import Image, ImageDraw

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("DFBNB_REPO") or os.path.dirname(HERE)
for p in (HERE, os.path.join(REPO, "src"), os.path.join(REPO, "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)

import render_slasher_maps as RSM       # masks loader, projector, Space table, fonts
import render_spawn_maps as RSP          # region boxes (SubRegion tilings), label placement
import map_watermark

S = RSM.S
DOT_D = RSM.DOT_D
MASK_FILL = RSM.MASK_FILL               # amber, same as the mask maps
SPOT_FILL = (255, 193, 7)               # amber - the map-marker icons are pale blue, so cyan blended in
SPOT_D = 60
OFFSET = 52                             # lettered square sits up-right of its spot

DEFAULT_OUT = os.environ.get(
    "SLASHER_LORE_OUT",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff",
                 ".Collectables", "Slasher Lore"))


# (quest EDID, alias name) -> what the reader finds there. The position comes from
# the alias in the game files; only the words are ours. Words follow the quests'
# own objectives / logs (QUST2 _RewardQuests).
SPOT_ALIASES = [
    ("SDOW_MQ01_Bodies", "Marker_DogHouse",
     "Dog kennel", "We're Coming For You (note, in the bloody mess)"),
    ("SDOW_MQ02_Graves", "Marker_Clown",
     "Smiling Clown, Tyler County Fairgrounds", "Invite Me (holotape)"),
    ("SDOW_MQ04_Infestations", "Door_RobCo",
     "RobCo Research Center", "Phantoms' Orders (notes, inside)"),
    ("SDOW_MQ04_Infestations", "Door_GauleyMine",
     "Gauley Mine", "Phantoms' Orders (notes, inside)"),
    ("SDOW_MQ04_Infestations", "Door_OrganCave",
     "Organ Cave", "The Summoning (holotape, on the Phantom Tinkerer inside)"),
]

TITLE = "The Slasher Lore - Notes & Holotapes"


# ---------------------------------------------------------------- game files

def _aliases(rec):
    """{alias name: {'ALFR': fid, 'ALRT': fid}} for one QUST record."""
    out, cur = {}, None
    for k, v in rec.subs:
        if k in ("ALST", "ALLS", "ALCS"):
            cur = {}
        elif cur is None:
            continue
        elif k == "ALID":
            out[v.rstrip(b"\0").decode("latin1")] = cur
        elif k in ("ALFR", "ALRT") and len(v) >= 4:
            cur[k] = struct.unpack_from("<I", v)[0]
        elif k == "ALED":
            cur = None
    return out


def load_spots(data_dir):
    import esm_reader as R
    import esm_refs
    esm = R.Esm(data_dir, ["QUST", "LCRT"])
    quests = {r.edid: r for r in esm.records["QUST"]}
    want = []
    for q, alias, place, finds in SPOT_ALIASES:
        rec = quests.get(q)
        if rec is None:
            raise SystemExit("quest %s is not in the game files" % q)
        a = _aliases(rec).get(alias)
        if a is None:
            raise SystemExit("alias %s is not on %s" % (alias, q))
        want.append((q, alias, place, finds, a))
    lcrts = {a["ALRT"] for *_, a in want if a.get("ALRT")}
    refby = esm_refs.referenced_by(esm, lcrts, referrers={"REFR"})[0] if lcrts else {}
    spots = []
    for q, alias, place, finds, a in want:
        if a.get("ALFR"):
            refs = [a["ALFR"]]
        else:
            refs = refby.get(a.get("ALRT"), [])
        if len(refs) != 1:
            raise SystemExit("%s %s: expected one placed ref, found %d" % (q, alias, len(refs)))
        w = esm.world_record(refs[0])
        sig, rec, cell, wrld = w
        if wrld is None:
            raise SystemExit("%s %s: ref %08X is inside an interior - pick its door alias"
                             % (q, alias, refs[0]))
        x, y, _z = struct.unpack_from("<3f", rec.first("DATA"))
        spots.append({"ref": "%08X" % refs[0], "place": place, "finds": finds,
                      "quest": q, "alias": alias, "x": x, "y": y})
    return spots


# ---------------------------------------------------------------- drawing

def draw_square(d, px, py, fill, size=SPOT_D):
    r = size / 2.0
    d.rectangle([px - r, py - r, px + r, py + r], fill=fill,
                outline=RSM.DOT_OUTLINE, width=4)


def draw_legend_box(img, spots):
    """Legend as a box ON the map, top-left - where the mask maps keep theirs
    (Duchess, 22 Oct 2026: not a strip hanging under the map). Moves to top-right
    only if a lettered spot would sit under it. Sized to the image (4096 map or
    2600 tile)."""
    k = img.width / 4096.0
    f_title, f_row, f_sub = RSM._font(int(84 * k)), RSM._font(int(60 * k)), RSM._font(int(46 * k))
    d = ImageDraw.Draw(img)
    rows = [("%s  %s" % (s["n"], s["place"]), s["finds"]) for s in spots]
    row_h, pad_in = int(126 * k), int(30 * k)
    sw = int(44 * k)
    tw = d.textlength(TITLE, font=f_title)
    rw = max(max(d.textlength(a, font=f_row), d.textlength(b, font=f_sub)) for a, b in rows)
    box_w = int(max(tw, rw + sw + 28 * k) + 2 * pad_in)
    box_h = int(pad_in + 96 * k + 14 * k + len(rows) * row_h + pad_in / 2)
    m = int(RSM.LEGEND_PAD * k)
    def clear(x0):
        return not any(x0 - SPOT_D <= s["lx"] <= x0 + box_w + SPOT_D and
                       s["ly"] <= m + box_h + SPOT_D for s in spots)
    x0 = m if clear(m) else img.width - m - box_w
    y0 = m
    d.rectangle([x0, y0, x0 + box_w, y0 + box_h], fill=RSM.LEGEND_BG,
                outline=RSM.LEGEND_BORDER, width=max(2, int(RSM.LEGEND_BORDER_W * k)))
    d.text((x0 + pad_in, y0 + pad_in * 0.6), TITLE, font=f_title, fill=(255, 255, 255))
    y = y0 + pad_in + 96 * k + 14 * k
    for a_, b_ in rows:
        draw_square(d, x0 + pad_in + sw / 2, y + 32 * k, SPOT_FILL, size=sw)
        d.text((x0 + pad_in + sw + 22 * k, y), a_, font=f_row, fill=(240, 240, 240))
        d.text((x0 + pad_in + sw + 22 * k, y + 64 * k), b_, font=f_sub, fill=(190, 190, 190))
        y += row_h


def draw_spot(d, s, f_spot, k=1.0):
    """Lettered square up-right of the spot, with a short line back to it, so the
    location's own map-marker icon underneath stays visible."""
    size = SPOT_D * k
    d.line([(s["dx"], s["dy"]), (s["lx"], s["ly"])],
           fill=(15, 15, 15), width=max(3, int(6 * k)))
    d.line([(s["dx"], s["dy"]), (s["lx"], s["ly"])], fill=SPOT_FILL, width=max(2, int(3 * k)))
    draw_square(d, s["lx"], s["ly"], SPOT_FILL, size=size)
    tb = d.textbbox((0, 0), s["n"], font=f_spot)
    d.text((s["lx"] - (tb[2] - tb[0]) / 2 - tb[0], s["ly"] - (tb[3] - tb[1]) / 2 - tb[1]),
           s["n"], font=f_spot, fill=(0, 0, 0))


def render(out_root, data_dir):
    conn = RSM._db()
    to_px = RSM.projector(RSM.load_space(conn))
    boxes = RSP.region_bboxes(conn)

    import crossref_mappalachia_markers as xref
    rings = xref.load_mappalachia()[0]

    spots = load_spots(data_dir)
    for i, s in enumerate(spots):
        s["n"] = chr(ord("A") + i)
        s["region"] = xref.region_for_xy(rings, s["x"], s["y"], nearest=True) or "Unknown"
        s["px"], s["py"] = to_px(s["x"], s["y"])

    bg = os.path.join(RSM.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    base = Image.open(bg).convert("RGB")
    if base.size != (S, S):
        base = base.resize((S, S), Image.LANCZOS)

    # The in-game map-marker icons (Mappalachia img/mapmarker/*.svg at its MapMarker
    # positions) - the same layer the Treasure Hunter route maps use. A map without
    # them is just terrain (Duchess, 22 Oct 2026). Needs cairosvg.
    import render_event_swap_maps as ESW
    marks = ESW.map_marker_icons()
    for x, y, _label, im in marks:
        px, py = to_px(x, y)
        base.paste(im, (int(px - im.width / 2), int(py - im.height / 2)), im)

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    f_spot = RSM._font(64)
    for s in spots:
        s["dx"], s["dy"] = s["px"], s["py"]
        s["lx"], s["ly"] = s["px"] + OFFSET, s["py"] - OFFSET
        draw_spot(d, s, f_spot)
    draw_legend_box(plain, spots)

    os.makedirs(os.path.join(out_root, "01 Full Maps"), exist_ok=True)
    p_plain = os.path.join(out_root, "01 Full Maps", "slasher-lore-map.jpg")
    map_watermark.apply(plain).save(p_plain, "JPEG", quality=88, optimize=True)

    # one close-up per region that has a spot, with its own legend
    tiles_dir = os.path.join(out_root, "03 Region Tiles")
    os.makedirs(tiles_dir, exist_ok=True)
    by_region = defaultdict(list)
    for s in spots:
        by_region[s["region"]].append(s)
    made = []
    for region, rows in sorted(by_region.items()):
        box = boxes.get(region)
        if box:
            x0, y0 = to_px(box[0], box[3]); x1, y1 = to_px(box[2], box[1])
        else:
            xs = [r["px"] for r in rows]; ys = [r["py"] for r in rows]
            x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        for r in rows:
            x0 = min(x0, r["px"] - SPOT_D); y0 = min(y0, r["py"] - SPOT_D)
            x1 = max(x1, r["px"] + SPOT_D); y1 = max(y1, r["py"] + SPOT_D)
        pad = 120
        x0 = max(0, int(x0 - pad)); y0 = max(0, int(y0 - pad))
        x1 = min(S, int(x1 + pad)); y1 = min(S, int(y1 + pad))
        x1 = min(S, max(x1, max(r["lx"] for r in rows) + SPOT_D + pad))
        y0 = max(0, min(y0, min(r["ly"] for r in rows) - SPOT_D - pad))
        # crop the map BEFORE its legend went on, then give the tile its own legend
        tile_src = base.copy()
        dt = ImageDraw.Draw(tile_src)
        for s in rows:
            draw_spot(dt, s, f_spot)
        crop = tile_src.crop((x0, y0, x1, y1))
        w, h = crop.size
        crop = crop.resize((RSM.TILE_WIDTH, max(1, int(h * RSM.TILE_WIDTH / w))), Image.LANCZOS)
        name = "%s-lore-map.jpg" % RSP._region_slug(region)
        sc = RSM.TILE_WIDTH / float(w)
        trows = [dict(r, lx=(r["lx"] - x0) * sc, ly=(r["ly"] - y0) * sc) for r in rows]
        draw_legend_box(crop, trows)
        map_watermark.apply(crop, corner="bottom-right").save(
            os.path.join(tiles_dir, name), "JPEG", quality=88, optimize=True)
        made.append((region, name, len(rows)))

    with open(os.path.join(out_root, "slasher-lore_coords.csv"), "w", newline="",
              encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "place", "finds", "ref_formid", "game_x", "game_y"])
        for s in spots:
            w_.writerow([s["n"], s["region"], s["place"], s["finds"], s["ref"],
                         round(s["x"], 1), round(s["y"], 1)])

    print("  spots=%d  map icons=%d  logo=%s" % (len(spots), len(marks), "yes" if map_watermark._logo() is not None else "NO"))
    for s in spots:
        print("   %s %-42s %-14s %s" % (s["n"], s["place"], s["region"], s["ref"]))
    print("  " + p_plain)
    for region, name, n in made:
        print("  tile %-16s %-30s %d spot(s)" % (region, name, n))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--data", default=os.environ.get("FO76_DATA"),
                    help="game Data folder (default: the live install esm_reader knows)")
    a = ap.parse_args()
    import esm_reader as R
    render(a.out, a.data or R.LIVE_DATA)


if __name__ == "__main__":
    main()
