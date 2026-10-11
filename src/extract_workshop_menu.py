#!/usr/bin/env python3
r"""
extract_workshop_menu.py - pull the CAMP Build Menu layout out of the game files.

WHERE THE BUILD MENU LIVES
==========================
Not in SeventySix.esm. The Workshop2 category keywords
(Workshop2_MainCategory_* / Workshop2_SubCategory_*) are KYWD records there, but
no recipe, object, FLST or keyword in the ESM points an item at them — they only
appear in a few challenge conditions. A full scan of all 5.6M ESM records in
Oct 2026 confirmed it; COBJ HNAM is a per-recipe ID, not a category.

The game reads the menu from a JSON file shipped inside the archives:

    misc/workshop/workshop.json
        in  SeventySix - Startup.ba2     (also an identical copy in MiscClient.ba2)

Shape: [ {CategoryKeyword:{FormEditorID,FormID,FormIDHex},
          SubCategories:[ {CategoryKeyword:{...}, Recipes:[{FormEditorID,FormID,FormIDHex}]} ]} ]
Recipes are COBJ records. Follow COBJ CNAM (often a workshop_LL_* LVLI) to the
objects each recipe builds. Display names come from the KYWD FULL in the KYWD export.

USAGE (run on the PC with the game installed, after each patch)
==============================================================
    python src/extract_workshop_menu.py                      # default Steam path, month = today
    python src/extract_workshop_menu.py --data "D:\steamlibrary\steamapps\common\Fallout76\Data" --month October_2026

Writes tsv/WORKSHOP_Export_<Month>_<Year>.json so tsv_source picks the newest
one like every other export. Read it with build_menu.py, never directly.
"""
import argparse, datetime, json, os, struct, sys, zlib

DEFAULT_DATA = r"D:\steamlibrary\steamapps\common\Fallout76\Data"
ARCHIVES = ("SeventySix - Startup.ba2", "SeventySix - MiscClient.ba2")
INNER = "misc/workshop/workshop.json"
HERE = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(os.path.dirname(HERE), "tsv")


def read_ba2_file(path, inner):
    """Return the bytes of one file inside a GNRL BA2 (Fallout 76 = version 1)."""
    with open(path, "rb") as f:
        magic, ver, typ, count, name_off = struct.unpack("<4sI4sIQ", f.read(24))
        if magic != b"BTDX" or typ != b"GNRL":
            raise ValueError(f"{path}: not a general BA2 ({magic!r} {typ!r})")
        f.seek(24 if ver == 1 else 32)
        recs = [struct.unpack("<I4sIIQIII", f.read(36)) for _ in range(count)]
        f.seek(name_off)
        names = []
        for _ in range(count):
            n = struct.unpack("<H", f.read(2))[0]
            names.append(f.read(n).decode("latin1").replace("\\", "/").lower())
        if inner not in names:
            return None
        _, _, _, _, off, packed, unpacked, _ = recs[names.index(inner)]
        f.seek(off)
        data = f.read(packed or unpacked)
        return zlib.decompress(data) if packed else data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=DEFAULT_DATA)
    ap.add_argument("--month", default=datetime.date.today().strftime("%B_%Y"),
                    help="label for the output file, e.g. October_2026")
    a = ap.parse_args()

    data = None
    for name in ARCHIVES:
        p = os.path.join(a.data, name)
        if os.path.exists(p):
            data = read_ba2_file(p, INNER)
            if data:
                print(f"read {INNER} from {name} ({len(data):,} bytes)")
                break
    if not data:
        sys.exit(f"{INNER} not found in {ARCHIVES} under {a.data}")

    menu = json.loads(data)
    subs = sum(len(c.get("SubCategories") or []) for c in menu)
    recipes = sum(len(s.get("Recipes") or []) for c in menu for s in c.get("SubCategories") or [])
    if recipes < 500:
        sys.exit(f"only {recipes} recipes - refusing to write a thin Build Menu")
    out = os.path.join(TSV_DIR, f"WORKSHOP_Export_{a.month}.json")
    with open(out, "wb") as f:
        f.write(data)
    print(f"{len(menu)} categories, {subs} sub-categories, {recipes} recipes -> {out}")


if __name__ == "__main__":
    main()
