#!/usr/bin/env python3
r"""
convert_deathclaw_egg_photos.py — convert the hand-shot Deathclaw Egg guide
photos to AVIF and name them to the paths the spawn JSON expects.

Sources live in the guide working folder, NOT the repo:
    OneDrive\Guides and Stuff\Deathclaw Meat and Egg Guide\DC Eggs\
      *.jpg          annotated Dec-2025 set (yellow arrow)
      Done\*.jpg     older 2022-23 walkthrough set (red arrow)
Output:
    OneDrive\Guides and Stuff\.Farming - Eggs\Deathclaw Egg\AVIF for Upload\

Two kinds of shot, because that is how they were taken:
  MARKER MAP  — one Pip-Boy map screenshot per marker, named
                {region}-{marker}-map.avif
  SPAWN ITEM  — one in-world shot per individual spawn, named to that spawn's
                `image_bottom_planned` value from the spawn JSON, i.e.
                {region}-{marker}-{label}-item.avif

Single-spawn markers get both halves under the per-spawn name, since there the
marker and the spawn are the same thing.

Encoder: pillow-avif (libavif) q=72 speed=6. ImageMagick's AVIF encoder is
avoided — it gets OOM-killed on tall images.

Run:  python tools/convert_deathclaw_egg_photos.py [--dry-run]
"""
import json, os, sys

try:
    import pillow_avif  # noqa: F401  (registers the AVIF plugin)
except ImportError:
    sys.exit("pillow-avif-plugin missing:  pip install pillow-avif-plugin")
from PIL import Image

Image.MAX_IMAGE_PIXELS = None

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GUIDES = os.environ.get(
    "DFBNB_GUIDES",
    os.path.expanduser("~/OneDrive/Guides and Stuff"),
)
SRC = os.path.join(GUIDES, "Deathclaw Meat and Egg Guide")
DEST = os.path.join(GUIDES, ".Farming - Eggs", "Deathclaw Egg", "AVIF for Upload")
SPAWNS = os.path.join(REPO, "dist", "farming_spawns", "deathclaw-egg_spawns.json")

QUALITY, SPEED = 72, 6

# ── Marker map shots (Pip-Boy screenshots) ──────────────────────────────────
# marker -> source file, relative to SRC
MARKER_MAPS = {
    "Ash Cave":             "DC Eggs/Ash Cave  .jpg",
    "The Rust Kingdom":     "DC Eggs/The Rust Kingdom.jpg",
    "Dino Peaks Mini Golf": "DC Eggs/Dino Peaks Mini Golf.jpg",
    "Deathclaw Island":     "DC Eggs/Done/deathclaw egg locations (1).jpg",
    "Arktos Pharma":        "DC Eggs/Done/deathclaw egg locations (10).jpg",
    "Monorail Elevator":    "DC Eggs/Done/deathclaw egg locations (11).jpg",
    "Abandoned Waste Dump": "DC Eggs/Done/deathclaw egg locations (17).jpg",
    "Hopewell Cave":        "DC Eggs/Done/deathclaw egg locations (32).jpg",
    "Tunnel of Love":       "DC Eggs/Done/tunnel of love.jpg",
    "Ella Ames' Bunker":    "DC Meat/Done/ella ames bunker.jpg",
    "Thunder Mt. Power Plant Yard": "DC Meat/Done/thunder mountain (2).jpg",
}

# ── Per-spawn in-world shots, keyed by REF ──────────────────────────────────
# Ref keying survives the relabel/re-sort that inject_deathclaw_egg_directions
# performs, and survives a rebuild.
SPAWN_ITEMS = {
    # Ash Cave / Rust Kingdom — single-spawn markers, Dec-2025 annotated set
    "80F840": "DC Eggs/Ash Cave spawn.jpg",
    "84B014": "DC Eggs/The Rust Kingdom spawn.jpg",

    # Dino Peaks Mini Golf — photos are numbered in WALK order, which is the
    # order the JSON now uses. Nest 1 shows the HOLE 3 / STEGOSAURUS STEPS sign.
    "82A3B2": "DC Eggs/Dino Peaks Nest 1.jpg",   # Nest #1 — Hole 3
    "7F0A84": "DC Eggs/Dino Peaks Nest 2.jpg",   # Nest #2
    "80391B": "DC Eggs/Dino Peaks Nest 3.jpg",   # Nest #3
    "82A3B3": "DC Eggs/Dino Peaks Nest 4.jpg",   # Nest #4 — Hole 5
    "7F0A81": "DC Eggs/Dino Peaks Nest 5.jpg",   # Nest #5
    "82A3B4": "DC Eggs/Dino Peaks Nest 6.jpg",   # Nest #6 — Hole 6

    # Hopewell Cave — order confirmed by the author:
    # 1 = egg on the nest, 2 = behind the Firecracker Berry bush,
    # 3 = behind the sleeping bag by the campfire.
    "62A86C": "DC Eggs/hopewell cave 1.jpg",
    "62A86F": "DC Eggs/hopewell cave 2.jpg",
    "62A873": "DC Eggs/hopewell cave 3.jpg",

    # Monorail Elevator — the picnic scene with the egg (TAKE / EAT prompt).
    "4F4CC1": "DC Eggs/Done/deathclaw egg locations (13).jpg",

    # Abandoned Waste Dump — the loose egg beside the carcass at the truck cab,
    # and the nest behind the dead brahmin with its inventory open.
    "3DC447": "DC Eggs/Done/deathclaw egg locations (19).jpg",
    "3DC3DD": "DC Eggs/Done/deathclaw egg locations (22).jpg",

    # Ella Ames' Bunker and Thunder Mt. — from the MEAT guide's Done set;
    # both show the nest with eggs, arrowed.
    "2C089D": "DC Meat/Done/ella ames nest.jpg",
    "37BE5C": "DC Meat/Done/thunder mountain eggs.jpg",

    # Deathclaw Island — the composite showing the egg in the nest dirt.
    "0638D1": "DC Eggs/Done/deathclaw egg locations (2).jpg",
}


def slug(s):
    import re
    s = (s or "").replace("'", "").replace("’", "")
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-").lower()
    return re.sub(r"-{2,}", "-", s)


def encode(src_rel, out_name, dry):
    src = os.path.join(SRC, src_rel.replace("/", os.sep))
    if not os.path.exists(src):
        return ("MISSING", src_rel, out_name, 0, 0)
    out = os.path.join(DEST, out_name)
    a = os.path.getsize(src)
    if dry:
        return ("dry", src_rel, out_name, a, 0)
    im = Image.open(src).convert("RGB")
    im.save(out, format="AVIF", quality=QUALITY, speed=SPEED)
    return ("ok", src_rel, out_name, a, os.path.getsize(out))


def main():
    dry = "--dry-run" in sys.argv
    if not dry:
        os.makedirs(DEST, exist_ok=True)
    doc = json.load(open(SPAWNS, encoding="utf-8"))

    rows, used = [], set()
    for reg in doc.get("regions", []):
        region = reg.get("region", "")
        for loc in reg.get("locations", []):
            mk = loc.get("marker", "")
            spawns = loc.get("spawns") or []

            # Filenames are derived here, not read from image_*_planned — the
            # inject tool clears those fields once a photo goes live, so reading
            # them would make this script stop finding its own outputs.
            if mk in MARKER_MAPS:
                if len(spawns) == 1:
                    name = "%s-%s-%s-map.avif" % (
                        slug(region), slug(mk), slug(spawns[0].get("label", "")))
                else:
                    name = "%s-%s-map.avif" % (slug(region), slug(mk))
                rows.append(encode(MARKER_MAPS[mk], name, dry))
                used.add(mk)

            # per-spawn item shots
            for sp in spawns:
                ref = (sp.get("ref") or "").upper()
                if ref not in SPAWN_ITEMS:
                    continue
                name = "%s-%s-%s-item.avif" % (
                    slug(region), slug(mk), slug(sp.get("label", "")))
                rows.append(encode(SPAWN_ITEMS[ref], name, dry))
                used.add(ref)

    ok = [r for r in rows if r[0] != "MISSING"]
    miss = [r for r in rows if r[0] == "MISSING"]
    for st, s, o, a, b in sorted(ok, key=lambda r: r[2]):
        print("  %-64s  %5.2fMB -> %5.2fMB" % (o, a / 1e6, b / 1e6))
    print("\n  %d written to %s" % (len(ok), DEST))
    if ok:
        ta, tb = sum(r[3] for r in ok), sum(r[4] for r in ok)
        print("  %.1fMB -> %.1fMB" % (ta / 1e6, tb / 1e6))
    for st, s, o, a, b in miss:
        print("  !! source not found: %s" % s)

    unused = (set(MARKER_MAPS) | set(SPAWN_ITEMS)) - used
    if unused:
        print("  !! mapped but never matched a marker/ref: %s" % sorted(unused))
    return 1 if miss else 0


if __name__ == "__main__":
    sys.exit(main())
