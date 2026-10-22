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
SPOT_FILL = (64, 224, 255)              # bright cyan square - reads on every region's ground
SPOT_D = 60

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


def with_legend(img, n_masks, spots):
    """The map with its legend in a black strip UNDER it. Every corner of this map
    has dots (Burning Springs and Tyler County Fairgrounds masks top-left, more in
    the others), so a legend box on the map always hid some - it goes below instead.
    Two columns: masks + the first spots left, the rest right."""
    f_title, f_row, f_sub = RSM._font(96), RSM._font(64), RSM._font(52)
    rows = [("circle", "Pint-Sized Slasher Masks  (%d)" % n_masks,
             "the Article Clippings and the children's book page come out of these")] if n_masks else []
    rows += [("square", "%s  %s" % (s["n"], s["place"]), s["finds"]) for s in spots]
    # two columns on the 4096 maps; one on the 2600 region tiles, where two touch
    per_col = (len(rows) + 1) // 2 if img.width >= 3500 else len(rows)
    row_h, top = 140, 40 + 96 + 30
    band_h = top + per_col * row_h + 30
    out = Image.new("RGB", (img.width, img.height + band_h), RSM.LEGEND_BG)
    out.paste(img, (0, 0))
    d = ImageDraw.Draw(out)
    y0 = img.height
    d.rectangle([0, y0, img.width - 1, y0 + band_h - 1], outline=RSM.LEGEND_BORDER,
                width=RSM.LEGEND_BORDER_W)
    d.text((48, y0 + 30), TITLE, font=f_title, fill=(255, 255, 255))
    col_w = img.width // 2
    for i, (shape, a_, b_) in enumerate(rows):
        x = 48 + (i // per_col) * col_w
        y = y0 + top + (i % per_col) * row_h
        if shape == "circle":
            RSM.draw_marker(d, x + 24, y + 36, MASK_FILL, "circle")
        else:
            draw_square(d, x + 24, y + 36, SPOT_FILL, size=48)
        d.text((x + 72, y), a_, font=f_row, fill=(240, 240, 240))
        d.text((x + 72, y + 70), b_, font=f_sub, fill=(190, 190, 190))
    return out


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

    plain = base.copy()
    d = ImageDraw.Draw(plain)
    f_spot = RSM._font(64)
    for s in spots:
        draw_square(d, s["px"], s["py"], SPOT_FILL)
        tb = d.textbbox((0, 0), s["n"], font=f_spot)
        d.text((s["px"] - (tb[2] - tb[0]) / 2 - tb[0], s["py"] - (tb[3] - tb[1]) / 2 - tb[1]),
               s["n"], font=f_spot, fill=(0, 0, 0))

    os.makedirs(os.path.join(out_root, "01 Full Maps"), exist_ok=True)
    p_plain = os.path.join(out_root, "01 Full Maps", "slasher-lore-map.jpg")
    with_legend(map_watermark.apply(plain), 0, spots).save(p_plain, "JPEG", quality=88, optimize=True)

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
        crop = plain.crop((x0, y0, x1, y1))
        w, h = crop.size
        crop = crop.resize((RSM.TILE_WIDTH, max(1, int(h * RSM.TILE_WIDTH / w))), Image.LANCZOS)
        name = "%s-lore-map.jpg" % RSP._region_slug(region)
        with_legend(map_watermark.apply(crop, corner="bottom-right"), 0, rows).save(
            os.path.join(tiles_dir, name), "JPEG", quality=88, optimize=True)
        made.append((region, name, len(rows)))

    with open(os.path.join(out_root, "slasher-lore_coords.csv"), "w", newline="",
              encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["n", "region", "place", "finds", "ref_formid", "game_x", "game_y"])
        for s in spots:
            w_.writerow([s["n"], s["region"], s["place"], s["finds"], s["ref"],
                         round(s["x"], 1), round(s["y"], 1)])

    print("  spots=%d  logo=%s" % (len(spots), "yes" if map_watermark._logo() is not None else "NO"))
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
