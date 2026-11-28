#!/usr/bin/env python3
r"""
build_secrets_guides_json.py - the Quests > Secrets & Easter Eggs guide pages
(/df/quests/secrets-and-easter-eggs/<slug>/) built on the collectable-guide
template. 28 Nov 2026. (Captain Bee's Diary has its own builder,
build_captain_bees_diary_guide_json.py, and stays there.)

Prose:  data/secrets/<slug>/guide.json (hand-written; edit that file)
Output: dist/secrets_<slug>_guide.json, mounted by df-bnb-collectable-guide.js
Maps:   src/render_secrets_maps.py <slug> (PC, needs Mappalachia)

Placeholders filled from the game files:
  {note:EDID}   the note's full text where you pick it up (BOOK export)
  {loot:EDID}   a container's loot in plain words (CONTITEMS); notes left out
  {name:EDID}   a record's in-game name
  {cages_table} Dusty's Treasure Hunt: what's behind each locked door. The doors
                (REFR 00807B39 / 00807B63 / 00807B68, locked to KEYM Burn_RK_Cage1-3)
                and the containers behind them were read from SeventySix.esm on
                28 Nov 2026 (cell 002655C2); the containers' names and loot lists
                come from the exports, so a change there shows up here.
"""
from __future__ import annotations

import argparse
import html as _h
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import collectable_guide as cg  # noqa: E402

PAGES = ["pioneer-treasure-hunt", "dustys-treasure-hunt", "odd-looking-rock",
         "tnt-dome-2-key", "tnt-dome-3-key", "tnt-dome-7-key", "tnt-dome-8-key"]
IMG = "/wp-content/uploads/guide-images/quests/secrets-and-easter-eggs/{slug}/"
# The four TNT dome pages share one folder (Duchess made it 28 Nov 2026); names carry dome-N-.
IMG_FOR = {s: "/wp-content/uploads/guide-images/quests/tnt-dome-keys/"
           for s in ("tnt-dome-2-key", "tnt-dome-3-key", "tnt-dome-7-key", "tnt-dome-8-key")}

# Loot lists in plain words. A list not named here stops the build, so a game-file
# name never reaches the page.
LISTS = {
    "LLS_Loot_Junk_Small_Basic": "small junk",
    "LLS_Loot_Junk_Rare_All": "rare junk",
    "LLS_Ammo_Loot_Contextual": "ammo",
    "Container_Loot_Priority_Raider_Safe": "safe loot",
    "Container_Loot_Priority_Suitcase_Armor": "armour",
    "Container_Loot_Priority_DuffleBag_Guns": "weapons",
    "LL_PrewarMoney_Reward_Large": "pre-war money",
    "Container_Loot_Furniture_Small_ChanceEmpty": "a small loot roll (can be empty)",
    "Container_Loot_ToolBox_ChanceEmpty": "tools and junk (can be empty)",
    "Container_Loot_Locker_ChanceEmpty": "locker loot (can be empty)",
    "Container_Loot_Priority_Cooler_Chems_Raider": "chems",
}
SKIP = {"LL_Perk_Scrounger", "LL_Perk_PharmaFarma"}   # perk-only extra rolls

# Dusty's cages: key -> containers behind that door (counts), from the ESM read above.
CAGES = [
    ("Dusty's Key 1", [("LootPriority_Raider_Safe", 2, "locked"), ("LootPriority_Raider_Suitcase_Armor", 1, "")]),
    ("Dusty's Key 2", [("LootPrewar_Locker_Wall02", 1, ""), ("LootPrewar_Locker_01", 1, ""),
                       ("LootPriority_DuffleBag_PreWarLoot", 1, ""), ("LootPriority_DuffleBag_Guns", 1, "")]),
    ("Dusty's Key 3", [("Loot_Toolbox_Grey", 1, ""), ("LootPriority_Raider_Cooler_Chems", 1, "")]),
]


class Files:
    def __init__(self, tsv):
        self.tsv = tsv
        self.items, self.cont = {}, {}
        for r in cg.rows(cg.newest(tsv, "CONTITEMS_Export_*.tsv")):
            self.items.setdefault(r["CONT_EDID"], []).append(r)
        for r in cg.rows(cg.newest(tsv, "CONT_Export_*.tsv")):
            self.cont[r.get("EDID")] = r.get("FULL") or ""

    def name(self, edid):
        return self.cont.get(edid) or cg.record_name(
            self.tsv, edid, ("BOOK_Export_*.tsv", "ALCH_Export_*.tsv", "MISC_Export_*.tsv", "KEYM_Export_*.tsv",
                             "ARMO_Export_*.tsv", "SPEL_Export_*_HEADER.tsv")) or ""

    def loot(self, cont, words_only=False):
        if cont not in self.items:
            raise SystemExit(f"build_secrets_guides_json: container {cont} not in CONTITEMS")
        out = []
        for r in self.items[cont]:
            e, sig, n = r["Item_EDID"], r["Item_Sig"], int(r.get("Count") or 1)
            if e in SKIP:
                continue
            if sig == "LVLI":
                if e not in LISTS:
                    raise SystemExit(f"build_secrets_guides_json: no plain name for {e} - add it to LISTS")
                w = LISTS[e]
            else:
                w = self.name(e) or e
                if sig == "BOOK" and not re.match(r"(Plan|Recipe):", w):
                    continue                       # notes are shown in their step instead
            out.append(f"{n}&times; {_h.escape(w)}" if n > 1 else _h.escape(w))
        return out


def cages_table(f):
    rows = []
    for key, conts in CAGES:
        bits = []
        for c, n, extra in conts:
            nm = f.name(c) or c
            lab = (f"{n}&times; " if n > 1 else "") + _h.escape(nm) + (f" ({extra})" if extra else "")
            bits.append(f"{lab}: {', '.join(f.loot(c))}")
        rows.append(f"<tr><td>{_h.escape(key)}</td><td>{'<br>'.join(bits)}</td></tr>")
    return ('<table class="abg-table abg-table--handin"><thead><tr><th>Door</th><th>Behind it</th></tr></thead>'
            f"<tbody>{''.join(rows)}</tbody></table>")


def build(slug, tsv, f):
    src = ROOT / "data" / "secrets" / slug / "guide.json"
    out = ROOT / "dist" / f"secrets_{slug.replace('-', '_')}_guide.json"
    if not src.exists():
        print(f"build_secrets_guides_json: no {src}; skipped")
        return
    def fill(t):
        t = cg.fill_notes(t, tsv)
        t = re.sub(r"\{loot:([A-Za-z0-9_]+)\}", lambda m: cg.and_list(f.loot(m.group(1))), t)
        t = re.sub(r"\{name:([A-Za-z0-9_]+)\}", lambda m: _h.escape(f.name(m.group(1))), t)
        if "{cages_table}" in t:
            t = t.replace("{cages_table}", cages_table(f))
        return t
    doc = cg.build(src, out, fill, IMG_FOR.get(slug) or IMG.format(slug=slug))
    print(f"{out.name}: {len(doc['sections'])} sections, {len(doc['missing_images'])} pictures not on the server yet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "tsv"))
    ap.add_argument("slugs", nargs="*", default=PAGES)
    a = ap.parse_args()
    f = Files(a.data_dir)
    for slug in a.slugs:
        build(slug, a.data_dir, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
