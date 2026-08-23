#!/usr/bin/env python3
r"""gen_spawn_maps.py — per-page fixed-spawn maps into the "Guides and Stuff" folders.

Plots each page's GUARANTEED fixed spawns (spawn-guide tiering) on the in-game
Mappalachia background (APPALACHIA.jpg — NOT the satellite/menu art), one PNG per
page in a per-page subfolder under the matching category folder. Pages with no
guaranteed spawns get THERE ARE NO FIXED SPAWN FOR THIS ITEM.txt instead.

Coordinate transform = Mappalachia's Space params (Appalachia): pixel =
((world - center)/maxRange + 1)/2 * res, y flipped. Source coords come from the
clean dist docs (creature docs are already guaranteed-tiered; item docs exclude
vendor; drinks keep direct/machine). Mothman also gets a night-spawn map from its
weighted placements; Flatwoods (no coords) gets the txt + relies on its page note.
"""
import json, os, re, glob, sys
from PIL import Image, ImageDraw, ImageFont
Image.MAX_IMAGE_PIXELS = None

REPO = "/sessions/elegant-dazzling-faraday/mnt/GitHub/dfbnb-data"
MAPP = "/sessions/elegant-dazzling-faraday/mnt/Mappalachia"
G    = "/sessions/elegant-dazzling-faraday/mnt/Guides and Stuff"
BG   = os.path.join(MAPP, "img/wrld/APPALACHIA.jpg")
CX, CY, MR = -500.0, 135.0, 582550.0
RES = 2048
DOT = 8
NOFIX = "THERE ARE NO FIXED SPAWN FOR THIS ITEM.txt"
FONTP = os.path.join(MAPP, "font", sorted(os.listdir(os.path.join(MAPP, "font")))[0])

_BASE = Image.open(BG).convert("RGB").resize((RES, RES))
_FONT = ImageFont.truetype(FONTP, 44)

def px(x, y):
    return ((x - CX) / MR + 1) / 2 * RES, (1 - (y - CY) / MR) / 2 * RES

def safe(name):
    return re.sub(r"[:*?\"<>|/\\]+", " ", name).replace("  ", " ").strip()

def render_map(pts, title, out_png):
    im = _BASE.copy(); dr = ImageDraw.Draw(im, "RGBA")
    for x, y in pts:
        X, Y = px(x, y)
        dr.ellipse([X-DOT, Y-DOT, X+DOT, Y+DOT], fill=(230,30,30,235),
                   outline=(255,255,255,255), width=2)
    dr.text((30, 30), title, fill=(255,255,255,255), stroke_width=3,
            stroke_fill=(0,0,0,255), font=_FONT)
    im.save(out_png, "PNG", optimize=True)

def coords_creature(doc):
    out = []
    for r in doc.get("fixed_spawns", {}).get("regions", []):
        for l in r["locations"]:
            sp = l.get("spawns") or []
            if sp:
                out += [tuple(s["coords"]) for s in sp if s.get("coords")]
            elif l.get("coords"):
                out.append(tuple(l["coords"]))
    return out

def coords_item(doc, keep):
    out = []
    for r in doc.get("regions", []):
        for l in r["locations"]:
            for s in (l.get("spawns") or []):
                if s.get("source_type") in keep and s.get("coords"):
                    out.append(tuple(s["coords"]))
    return out

def write(folder, files):
    os.makedirs(folder, exist_ok=True)
    return folder

def main():
    made_maps = made_txt = 0
    folders = []
    skipped = []

    def do_page(name, family_dir, coords, slug=""):
        nonlocal made_maps, made_txt
        folder = os.path.join(G, family_dir, safe(name))
        os.makedirs(folder, exist_ok=True)
        folders.append(folder)
        # Resumable: skip a page already done (has a map PNG or the no-fix txt).
        if os.path.exists(os.path.join(folder, NOFIX)) or \
           glob.glob(os.path.join(folder, "*Fixed Spawn Map.png")):
            return folder
        pts = [(x, y) for (x, y) in coords if x is not None and y is not None]
        # drop the no-fix txt if it exists and we now have spawns
        txt = os.path.join(folder, NOFIX)
        if pts:
            render_map(pts, f"{name} - Fixed Spawn Locations ({len(pts)})",
                       os.path.join(folder, f"{safe(name)} - Fixed Spawn Map.png"))
            if os.path.exists(txt): os.remove(txt)
            made_maps += 1
        else:
            open(txt, "w", encoding="utf-8").write(
                "There are no fixed (guaranteed) spawn locations for this item in the "
                "game data.\n")
            made_txt += 1
        return folder

    # CRYPTIDS
    for f in sorted(glob.glob(f"{REPO}/dist/cryptids/*.json")):
        d = json.load(open(f))
        if not d.get("name"): continue
        folder = do_page(d["name"], ".Cryptids", coords_creature(d))
        # Mothman night map (weighted placements from geo cache)
        if d["slug"] == "mothman":
            gc = json.load(open(f"{REPO}/data/cryptid_spawns/geo_cache.json"))
            night = [(v["x"], v["y"]) for v in gc.values()
                     if v.get("page") == "mothman" and v.get("source_type") not in ("ambush","nest")
                     and v.get("x") is not None]
            if night:
                render_map(night, f"Mothman - Night Spawns ({len(night)}) - after ~6pm",
                           os.path.join(folder, "Mothman - Night Spawns Map.png"))
                made_maps += 1

    # MEAT
    for f in sorted(glob.glob(f"{REPO}/dist/meat/*.json")):
        if f.endswith("/meat.json"): continue
        d = json.load(open(f))
        if not d.get("name"): continue
        do_page(d["name"], ".Farming - Meat", coords_creature(d))

    # INSECTS (same creature-seeded doc shape as meat)
    for f in sorted(glob.glob(f"{REPO}/dist/insects/*.json")):
        if f.endswith("/insects.json"): continue
        d = json.load(open(f))
        if not d.get("name"): continue
        do_page(d["name"], ".Farming - Insects", coords_creature(d))

    # FARMING_SPAWNS (chems / eggs / non-perishable)
    ITEM_KEEP = {"direct","static","static-raw","static-cracked","nest","harvestable",
                 "flora","enlightened-flora","loot-list"}
    for f in sorted(glob.glob(f"{REPO}/dist/farming_spawns/*_spawns.json")):
        d = json.load(open(f))
        slug = d.get("set") or ""
        name = d.get("name")
        if not name: continue
        base = os.path.basename(f)[:-len("_spawns.json")]
        if base.startswith("chems-"): fam = ".Farming - Chems"
        elif base.endswith("-egg"):   fam = ".Farming - Eggs"
        else:                          fam = ".Farming - Non Perishable"
        do_page(name, fam, coords_item(d, ITEM_KEEP))

    # DRINKS (nuka cola)
    for f in sorted(glob.glob(f"{REPO}/dist/nuka_cola_spawns_*.json")):
        if f.endswith("manifest.json"): continue
        d = json.load(open(f))
        if not d.get("name"): continue
        do_page(d["name"], ".Farming - Nuka Cola", coords_item(d, {"direct","machine"}))

    print(f"[maps] folders touched: {len(set(folders))}  maps: {made_maps}  no-fix txt: {made_txt}")

if __name__ == "__main__":
    main()
