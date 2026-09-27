#!/usr/bin/env python3
r"""
finish_npc_spawn_maps.py — the step after render_spawn_maps.py for the DF Score
Challenges enemy pages (src/build_npc_spawn_guides.py).

For every page it reads what was ACTUALLY rendered under
  <GUIDES_ROOT>\.Score Challenges\<Folder>\
and then:

  1. writes the upload set into  <Folder>\AVIF for Upload\  named exactly as the
     page expects under map_base (/wp-content/uploads/guide-images/score-challenges/<slug>/):
        <region-slug>-spawn-map.avif    03 Region Tiles  (Fixed Spawn map links)
        <region-slug>-chance-map.avif   05 Chance Maps   (Chance to Spawn map links)
        <region-slug>-<marker-slug>-cell-map.avif   04 Interior Maps
        <slug>-spawn-map-4k.jpg         01 Full Maps     (the 4K download link)
  2. stamps the doc so it only links to files that exist:
        map_regions               regions that have a spawn tile
        chance_spawns.map_regions regions that have a chance tile
        full_map                  only when the page has exterior spawns
        image_bottom              an interior marker's cell map (only if empty — a
                                  hand-placed image is never overwritten)
     in dist/farming_spawns/ and the dist/pts/ twin.

Maps are rendered (and watermarked) by render_spawn_maps.py; this step only
converts and renames, so the watermark carries through. AVIF quality 58 matches the
rest of the site's guide images.

  python src/finish_npc_spawn_maps.py [slug ...]
"""
import csv
import json
import os
import re
import sys

from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import build_npc_spawn_guides as N   # noqa: E402

GUIDES_ROOT = os.environ.get("GUIDES_ROOT", r"C:\Users\Duche\OneDrive\Guides and Stuff")
ROOT = os.path.join(GUIDES_ROOT, ".Score Challenges")
QUALITY = 58

# page slug -> folder under .Score Challenges
FOLDERS = {
    "blood-eagle": "Blood Eagles", "cultist": "Cultists", "fanatic": "Fanatics",
    "communist": "Communists", "rust-raider": "Rust Raiders",
    "pint-sized-phantom": "Pint-Sized Phantoms", "glowing-one": "Glowing Ones",
    "lost": "Lost", "feral-ghoul": "Feral Ghouls", "super-mutant": "Super Mutants",
    "mole-miner": "Mole Miners", "scorched": "Scorched", "floater": "Floaters",
    "overgrown": "Overgrown", "trog": "Trogs", "assaultron": "Assaultrons",
    "sentry-bot": "Sentry Bots", "protectron": "Protectrons", "eyebot": "Eyebots",
    "liberator": "Liberators", "robobrain": "Robobrains",
    "mr-handy": "Mr Handy and Other Robots",
}


# render_spawn_maps.CELL_DISPLAY_OVERRIDES retitles a few cells for the egg page
# (whose markers are renamed to match). The enemy pages keep Mappalachia's own
# marker name, so the cell map is matched on either form.
CELL_ALIASES = {
    "Abandoned Waste Dump (Cavern)": "Cavern",
    "Transmission Station 1AT-U03 (Enclave Research Facility)": "Enclave Research Facility",
    "Dark Hollow Manor (Vault 63 > Atrium Upper Level)": "Atrium Upper Level",
}


def slugify(s):
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", str(s or "").lower()).lstrip("-"))


def to_avif(src, dst):
    if os.path.exists(dst) and os.path.getmtime(dst) >= os.path.getmtime(src):
        return False
    Image.open(src).convert("RGB").save(dst, "AVIF", quality=QUALITY)
    return True


def finish(slug):
    folder = os.path.join(ROOT, FOLDERS[slug])
    up = os.path.join(folder, "AVIF for Upload")
    os.makedirs(up, exist_ok=True)
    doc_paths = [os.path.join(N.OUT_DIR, f"npc-{slug}_spawns.json"),
                 os.path.join(N.PTS_OUT_DIR, f"npc-{slug}_spawns.json")]
    doc = json.load(open(doc_paths[0], encoding="utf-8"))
    base = doc.get("map_base") or ""
    made = 0

    # A tile older than this render's full map is left over from an earlier run
    # (the region no longer has spawns) and must not be linked.
    full = os.path.join(folder, "01 Full Maps (4096)", f"{slug}.jpg")
    fresh = os.path.getmtime(full) - 120 if os.path.exists(full) else 0

    def tiles(sub, suffix):
        d = os.path.join(folder, sub)
        out = []
        if os.path.isdir(d):
            for fn in sorted(os.listdir(d)):
                if fn.endswith(suffix + ".jpg") and os.path.getmtime(os.path.join(d, fn)) >= fresh:
                    made_ = to_avif(os.path.join(d, fn), os.path.join(up, fn[:-4] + ".avif"))
                    out.append((fn[:-len(suffix + ".jpg")], made_))
        return out

    spawn_tiles = tiles("03 Region Tiles", "-spawn-map")
    chance_tiles = tiles("05 Chance Maps", "-chance-map")
    made += sum(1 for _, m in spawn_tiles + chance_tiles if m)
    by_slug = {slugify(r): r for r in N.ALL_REGIONS}
    doc["map_regions"] = [by_slug[s] for s, _ in spawn_tiles if s in by_slug]
    cs = doc.get("chance_spawns") or {}
    if not cs.get("no_maps"):
        cs["map_regions"] = [by_slug[s] for s, _ in chance_tiles if s in by_slug]

    # 4K download: only when something was plotted on the exterior map.
    if doc["map_regions"] and os.path.exists(full):
        dst = os.path.join(up, f"{slug}-spawn-map-4k.jpg")
        if not os.path.exists(dst) or os.path.getmtime(dst) < os.path.getmtime(full):
            with open(full, "rb") as a, open(dst, "wb") as b:
                b.write(a.read())
            made += 1
        doc["full_map"] = base + f"{slug}-spawn-map-4k.jpg"
    else:
        doc["full_map"] = ""

    # Interior cell maps -> the marker of the same name.
    wired = 0
    csv_p = os.path.join(folder, "04 Interior Maps", "interior_cells.csv")
    if os.path.exists(csv_p):
        for row in csv.DictReader(open(csv_p, encoding="utf-8")):
            src = os.path.join(folder, "04 Interior Maps", f"{row['cell_edid']}_{slug}.jpg")
            if not os.path.exists(src):
                continue
            name = f"{slugify(row['region'])}-{slugify(row['display_name'])}-cell-map.avif"
            if to_avif(src, os.path.join(up, name)):
                made += 1
            for reg in doc.get("regions", []):
                if reg.get("region") != row["region"]:
                    continue
                for loc in reg.get("locations", []):
                    names = {row["display_name"], CELL_ALIASES.get(row["display_name"], "")}
                    if loc.get("marker") in names and not loc.get("image_bottom"):
                        loc["image_bottom"] = base + name
                        wired += 1

    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    for p in doc_paths:
        open(p, "w", encoding="utf-8").write(txt)
    print(f"  {slug:<20} spawn-tiles:{len(spawn_tiles):>2} chance-tiles:{len(chance_tiles):>2} "
          f"cell-maps-wired:{wired:>2} new-files:{made:>3} full-map:{'yes' if doc['full_map'] else 'no'}")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    want = [a for a in argv if not a.startswith("-")] or list(FOLDERS)
    for s in want:
        finish(s)


if __name__ == "__main__":
    main()
