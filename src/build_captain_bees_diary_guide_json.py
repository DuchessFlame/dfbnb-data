#!/usr/bin/env python3
r"""
build_captain_bees_diary_guide_json.py - /df/quests/secrets-and-easter-eggs/captain-bees-diary/

Captain Bee's Diary is an UNMARKED trail in Skyline Valley: there is no quest
record, just six Steel Jack statues pointing at six containers, each holding a
page of Bee's Diary, plus Bee's Final Note by a skeleton near the last one.
Built on the collectable-guide template (Duchess, 28 Nov 2026), so the page uses
df-bnb-collectable-guide.js like the collectable guides.

Prose:  data/captain_bees_diary/guide.json (hand-written; edit that file).
Output: dist/captain_bees_diary_guide.json.

From the game files:
  {page_count}    how many Bee's Diary pages exist (BOOK Storm_SteelJackNote_0N)
  {final_note}    BOOK Storm_Bees_finalNote name ("Bee's Final Note")
  {note:EDID}     the note's full text in the step where you find it (BOOK export)
  {loot_table}    Page | Place | Container | Loot, from CONTITEMS (container per page)
                  and CONT (container name). Wix said "no reward"; the files win.
Maps: src/render_captain_bees_diary_maps.py (run on the PC, needs Mappalachia).
"""
from __future__ import annotations

import argparse
import html as _h
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import collectable_guide as cg  # noqa: E402

SRC = ROOT / "data" / "captain_bees_diary" / "guide.json"
OUT = ROOT / "dist" / "captain_bees_diary_guide.json"
IMG = "/wp-content/uploads/guide-images/quests/secrets-and-easter-eggs/captain-bees-diary/"

# Page -> the place players know it by (nearest map marker / fast travel start;
# Duchess 28 Nov 2026: stop 2 = High Knob Lookout, Research Site Rhineland is
# slightly nearer as the crow flies but her route starts at High Knob).
PLACES = {1: "Shenandoah Visitor Center", 2: "High Knob Lookout", 3: "Naked Creek",
          4: "Old Rag Lookout", 5: "Abandoned Convoy", 6: "South River Bridge overlook"}

# Loot lists in plain words. A list not named here stops the build so a new one
# never shows as a game-file name.
LISTS = {
    "Container_Loot_Priority_Medkit": "Medkit loot",
    "Container_Loot_Priority_ToolBox": "Toolbox loot",
    "Container_Loot_Priority_Cooler_PreWar": "Pre-war cooler loot",
    "Container_Loot_CapsStash_Medium": "Caps stash (medium)",
    "Container_Loot_CapsStash_High": "Caps stash (high)",
}


def pages(tsv):
    """{n: (page name, container EDID)} for Storm_SteelJackNote_0N, plus the final note's name."""
    out, final = {}, ""
    for r in cg.rows(cg.newest(tsv, "CONTITEMS_Export_*.tsv")):
        e = r.get("Item_EDID") or ""
        if e.startswith("Storm_SteelJackNote_0"):
            out[int(e[-1])] = r["CONT_EDID"]
    names = {}
    for r in cg.rows(cg.newest(tsv, "BOOK_Export_*.tsv", skip=("_Locations.tsv",))):
        e = r.get("EDID") or ""
        if e.startswith("Storm_SteelJackNote_0"):
            names[int(e[-1])] = r.get("FULL") or ""
        elif e == "Storm_Bees_finalNote":
            final = r.get("FULL") or ""
    return out, names, final


def loot_table(tsv, conts):
    cont_name = {r.get("EDID"): r.get("FULL") for r in cg.rows(cg.newest(tsv, "CONT_Export_*.tsv"))}
    loot = {}
    for r in cg.rows(cg.newest(tsv, "CONTITEMS_Export_*.tsv")):
        c = r.get("CONT_EDID")
        if c not in conts.values() or (r.get("Item_EDID") or "").startswith("Storm_SteelJackNote_"):
            continue
        e, sig, n = r["Item_EDID"], r["Item_Sig"], int(r.get("Count") or 1)
        if sig == "LVLI":
            if e not in LISTS:
                raise SystemExit(f"build_captain_bees_diary_guide_json: no plain name for {e} - add it to LISTS")
            loot.setdefault(c, []).append(LISTS[e])
        else:
            name = cg.record_name(tsv, e, ("ALCH_Export_*.tsv", "MISC_Export_*.tsv", "BOOK_Export_*.tsv")) or e
            loot.setdefault(c, []).append(f"{n}&times; {_h.escape(name)}" if n > 1 else _h.escape(name))
    body = "".join(
        f"<tr><td>{i}</td><td>{_h.escape(PLACES.get(i, ''))}</td>"
        f"<td>{_h.escape(cont_name.get(conts[i]) or '')}</td><td>{', '.join(loot.get(conts[i], [])) or '&ndash;'}</td></tr>"
        for i in sorted(conts))
    return ("<table class=\"abg-table abg-table--handin\"><thead><tr>"
            "<th>Page</th><th>Place</th><th>Container</th><th>Loot (besides the page)</th></tr></thead>"
            f"<tbody>{body}</tbody></table>")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(ROOT / "tsv"))
    tsv = ap.parse_args().data_dir
    conts, names, final = pages(tsv)
    if len(conts) < 6:
        print(f"build_captain_bees_diary_guide_json: only {len(conts)} diary pages found; dist left as is")
        return 0
    table = loot_table(tsv, conts)
    base_fill = lambda t: (t.replace("{page_count}", str(len(conts)))
                       .replace("{final_note}", _h.escape(final or "Bee's Final Note"))
                       .replace("{loot_table}", table))
    fill = lambda t: base_fill(cg.fill_notes(t, tsv))   # {note:EDID}: full note text in its step
    doc = cg.build(SRC, OUT, fill, IMG)
    print(f"{OUT.name}: {len(conts)} pages + {final!r}, "
          f"{len(doc['missing_images'])} pictures not on the server yet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
