#!/usr/bin/env python3
r"""fix_interior_maps.py — correct maps for spawns in INTERIOR cells.

Mappalachia resolves interior-cell placements to their surface region, so interior
spawns were kept in the docs and (bug) plotted on the Appalachia surface map at
wrong positions. This:
  1. Regenerates the surface map with ONLY Appalachia-worldspace points.
  2. Writes "Interior Spawn Locations.txt" listing every interior spawn (cell +
     marker + count) so none are silently dropped.
  3. Generates a per-interior-cell map for CREATURE pages (meat/cryptid) using
     img/cell/<edid>.jpg + the cell's own Space transform. (Item pages have spawns
     in 100+ cells each — thousands of images — so those get the list only.)
Resumable: a page whose folder already has the interior list is skipped.
"""
import json, os, glob, math, re
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont
Image.MAX_IMAGE_PIXELS = None

REPO = "/sessions/elegant-dazzling-faraday/mnt/GitHub/dfbnb-data"
MAPP = "/sessions/elegant-dazzling-faraday/mnt/Mappalachia"
G    = "/sessions/elegant-dazzling-faraday/mnt/Guides and Stuff"
APP  = 2480661
CX, CY, MR = -500.0, 135.0, 582550.0
RES = 2048
DOT = 8
CELLS = json.load(open(f"{REPO}/data/interior_cells.json"))
FONTP = os.path.join(MAPP, "font", sorted(os.listdir(os.path.join(MAPP, "font")))[0])
_SURFBASE = Image.open(f"{MAPP}/img/wrld/APPALACHIA.jpg").convert("RGB").resize((RES, RES))
_F = ImageFont.truetype(FONTP, 44)

def safe(n): return re.sub(r"[:*?\"<>|/\\]+", " ", n).replace("  ", " ").strip()
def surf_px(x, y): return ((x-CX)/MR+1)/2*RES, (1-(y-CY)/MR)/2*RES
def cell_px(x, y, c, res):
    dx, dy = x-c["cx"], y-c["cy"]; a = math.radians(-c["north"])
    rx = dx*math.cos(a) - dy*math.sin(a); ry = dx*math.sin(a) + dy*math.cos(a)
    return (rx/c["mr"]+1)/2*res, (1-ry/c["mr"]/1)/2*res

def draw(base, pts, title, pxfn, out):
    im = base.copy(); dr = ImageDraw.Draw(im, "RGBA")
    for x, y in pts:
        X, Y = pxfn(x, y)
        dr.ellipse([X-DOT, Y-DOT, X+DOT, Y+DOT], fill=(230,30,30,235), outline=(255,255,255,255), width=2)
    dr.text((30, 30), title, fill=(255,255,255,255), stroke_width=3, stroke_fill=(0,0,0,255), font=_F)
    im.save(out, "PNG", optimize=True)

def spacemap(kind, slug):
    m = {}
    try:
        if kind in ("meat", "insects"):
            sub = "meat_spawns" if kind == "meat" else "insect_spawns"
            for k, v in json.load(open(f"{REPO}/data/{sub}/{slug}.json")).items():
                m[int(k.split(":")[-1])] = v.get("space")
        elif kind == "cryptid":
            for k, v in json.load(open(f"{REPO}/data/cryptid_spawns/geo_cache.json")).items():
                if v.get("page") == slug: m[int(k)] = v.get("space")
        elif kind == "farming":
            p = f"{REPO}/data/farming_spawns/geo_cache_{slug.replace('-','_')}.json"
            if os.path.exists(p):
                for k, v in json.load(open(p)).items():
                    try: m[int(k)] = v.get("space")
                    except: pass
    except Exception: pass
    return m

def page_spawns(doc, kind):
    """yield (ref_int, x, y, marker) for guaranteed spawns."""
    regions = doc.get("fixed_spawns", {}).get("regions") if kind in ("meat","cryptid","insects") else doc.get("regions", [])
    EXCL = {"vendor","container","loot-list","quest-reward"}
    for r in regions or []:
        for l in r["locations"]:
            for sp in (l.get("spawns") or []):
                if kind not in ("meat","cryptid","insects") and sp.get("source_type") in EXCL: continue
                c = sp.get("coords")
                if not c or c[0] is None: continue
                ref = int(sp["ref"], 16) if sp.get("ref") else None
                yield ref, c[0], c[1], l.get("marker","")

def process(name, family, kind, doc, slug):
    folder = os.path.join(G, family, safe(name)); os.makedirs(folder, exist_ok=True)
    sm = spacemap(kind, slug)
    surf, interior = [], defaultdict(list)   # cell_space -> [(x,y,marker)]
    for ref, x, y, marker in page_spawns(doc, kind):
        s = sm.get(ref, APP)
        if s == APP: surf.append((x, y))
        else: interior[s].append((x, y, marker))
    if not interior:
        return 0, 0   # nothing interior; leave existing surface map alone
    # 1) regenerate surface map (Appalachia only). OneDrive locks existing files
    # against delete, so overwrite in place via PIL; fall back to a corrected-name
    # copy if the original is locked.
    if surf:
        target = os.path.join(folder, f"{safe(name)} - Fixed Spawn Map.png")
        try:
            draw(_SURFBASE, surf, f"{name} - Fixed Spawn Locations ({len(surf)}) [surface only]",
                 surf_px, target)
        except Exception:
            draw(_SURFBASE, surf, f"{name} - Fixed Spawn Locations ({len(surf)}) [surface only]",
                 surf_px, os.path.join(folder, f"{safe(name)} - Fixed Spawn Map (corrected surface).png"))
    # 2) interior locations list
    lines = [f"{name} — interior (indoor) guaranteed spawns\n",
             "These are inside interior cells (mines, buildings, vaults) and cannot plot on "
             "the Appalachia surface map. Listed by interior cell:\n"]
    total_int = 0
    for sp, items in sorted(interior.items(), key=lambda kv: CELLS.get(str(kv[0]), {}).get("name","")):
        c = CELLS.get(str(sp)); cname = c["name"] if c else f"cell {sp}"
        markers = defaultdict(int)
        for _x, _y, mk in items: markers[mk or cname] += 1
        total_int += len(items)
        lines.append(f"  {cname} — {len(items)} spawn(s): " +
                     ", ".join(f"{m} x{n}" if n > 1 else m for m, n in sorted(markers.items())))
    open(os.path.join(folder, "Interior Spawn Locations.txt"), "w", encoding="utf-8").write("\n".join(lines) + "\n")
    # 3) creature-page interior cell maps
    made_cell = 0
    if kind in ("meat", "cryptid", "insects"):
        for sp, items in interior.items():
            c = CELLS.get(str(sp))
            if not c: continue
            img = os.path.join(MAPP, "img/cell", (c["edid"] or "") + ".jpg")
            if not os.path.exists(img): continue
            base = Image.open(img).convert("RGB")
            w = base.size[0]
            pts = [(x, y) for (x, y, _m) in items]
            draw(base, pts, f"{name} - {c['name']} (interior, {len(pts)})",
                 lambda X, Y, cc=c, ww=w: cell_px(X, Y, cc, ww),
                 os.path.join(folder, f"{safe(name)} - Interior - {safe(c['name'])}.png"))
            made_cell += 1
    return len(surf), total_int, made_cell

def main():
    done_pages = 0; done_cells = 0; item_pages = 0
    jobs = []
    for f in sorted(glob.glob(f"{REPO}/dist/meat/*.json")):
        if f.endswith("/meat.json"): continue
        d = json.load(open(f))
        if d.get("name"): jobs.append((d["name"], ".Farming - Meat", "meat", d, d["slug"]))
    for f in sorted(glob.glob(f"{REPO}/dist/insects/*.json")):
        if f.endswith("/insects.json"): continue
        d = json.load(open(f))
        if d.get("name"): jobs.append((d["name"], ".Farming - Insects", "insects", d, d["slug"]))
    for f in sorted(glob.glob(f"{REPO}/dist/cryptids/*.json")):
        d = json.load(open(f))
        if d.get("name"): jobs.append((d["name"], ".Cryptids", "cryptid", d, d["slug"]))
    for f in sorted(glob.glob(f"{REPO}/dist/farming_spawns/*_spawns.json")):
        d = json.load(open(f)); base = os.path.basename(f)[:-len("_spawns.json")]
        if not d.get("name"): continue
        fam = ".Farming - Chems" if base.startswith("chems-") else \
              ".Farming - Eggs" if base.endswith("-egg") else ".Farming - Non Perishable"
        jobs.append((d["name"], fam, "farming", d, base))
    for name, fam, kind, d, slug in jobs:
        folder = os.path.join(G, fam, safe(name))
        if os.path.exists(os.path.join(folder, "Interior Spawn Locations.txt")):
            continue   # resumable
        res = process(name, fam, kind, d, slug)
        if res and (res[1] if len(res) > 1 else 0):
            done_pages += 1
            if len(res) > 2: done_cells += res[2]
            if kind == "farming": item_pages += 1
    print(f"[fix_interior] pages fixed: {done_pages}  interior cell-maps: {done_cells}  (item pages: {item_pages})")

if __name__ == "__main__":
    main()
