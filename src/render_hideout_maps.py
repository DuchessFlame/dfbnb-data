#!/usr/bin/env python3
r"""
render_hideout_maps.py - maps for /df/hideouts/hideout-locations/.

Reads data/hideouts/locations_<channel>.tsv (esm_scan_placements.py --names) and
draws, in the house style (render_spawn_maps.py dots + labels, watermark):
  01 Full Maps (4096)/hideout_locations.jpg   every Hideout with a marker, labelled
  03 Region Tiles/<hideout-slug>-map.jpg       one zoomed tile per Hideout
Default --out: Guides and Stuff\.Hideouts\Hideout Locations
Upload both to /wp-content/uploads/guide-images/hideouts/ (names match the JSON).
"""
import argparse, csv, os, re, sys
from PIL import Image
HERE = os.path.dirname(os.path.abspath(__file__)); REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import render_spawn_maps as rsm
import map_watermark
from render_brain_in_a_jar_maps import draw_crop

DEFAULT_OUT = os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff", ".Hideouts", "Hideout Locations")


def slug(t):
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="pts", choices=["pts", "live"])
    ap.add_argument("--out", default=DEFAULT_OUT)
    a = ap.parse_args()
    with open(os.path.join(REPO, "data", "hideouts", f"locations_{a.channel}.tsv"), encoding="utf-8") as fh:
        pts = [r for r in csv.DictReader(fh, delimiter="\t") if r.get("name") and r["name"] != "Vault 63"]
    conn = rsm._db(); spaces = rsm.load_spaces(conn); to_px = rsm.projector(spaces[rsm.APPALACHIA_SPACE])
    rows = []
    for i, r in enumerate(pts, 1):
        px, py = to_px(float(r["X"]), float(r["Y"]))
        rows.append({"n": i, "name": r["name"], "px": px, "py": py})
    bg = Image.open(os.path.join(rsm.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")).convert("RGB")
    if bg.size != (rsm.S, rsm.S):
        bg = bg.resize((rsm.S, rsm.S), Image.LANCZOS)
    full = draw_crop(bg, rows, (0, 0, rsm.S, rsm.S), title="Hideout Locations", legend="Hideout", label=lambda r: r["name"])
    os.makedirs(os.path.join(a.out, "01 Full Maps (4096)"), exist_ok=True)
    full.save(os.path.join(a.out, "01 Full Maps (4096)", "hideout_locations.jpg"), "JPEG", quality=88, optimize=True)
    tdir = os.path.join(a.out, "03 Region Tiles"); os.makedirs(tdir, exist_ok=True)
    for r in rows:
        x, y = r["px"], r["py"]
        box = (max(0, int(x - 520)), max(0, int(y - 330)), min(rsm.S, int(x + 520)), min(rsm.S, int(y + 330)))
        img = draw_crop(bg, [r], box, context=[o for o in rows if o is not r], title=r["name"], legend="Hideout",
                        label=lambda q: q["name"])
        img.save(os.path.join(tdir, f"{slug(r['name'])}-map.jpg"), "JPEG", quality=88, optimize=True)
    print(f"{len(rows)} Hideouts -> {a.out}")


if __name__ == "__main__":
    main()
