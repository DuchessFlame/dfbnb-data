#!/usr/bin/env python3
r"""
render_event_swap_maps.py - maps of where seasonal "swap-in" enemies can appear.

Four seasonal events swap a normal enemy for an event enemy while they run. The game
files (SeventySix.esm, read Sept 2026) put them in TWO pools, not one:

  Pool A - Treasure Hunters + Slashers (+ the Mothman Equinox cultist)
    LChar_MainAll [0051C55B] is a first-match list. Entry 2 is LChar_Event_Actors
    [005A7559], taken when:
        GetRandomPercent < TreasureHunt_SpawnChance        (Treasure Hunters)
     OR GetRandomPercent < LCP_E07A_Mothman_SpecialEncounter_SpawnChance
     OR GetRandomPercent < LCP_SDOW_Global_SlasherMinionSpawnRate  (Slashers)
     AND outdoors (IsInInterior == 0)
     AND in an event region (EventActor_Regions CNDF: Forest, Savage Divide, Ash Heap,
         The Mire, Cranberry Bog, Toxic Valley, Whitespring) OR EventActor_GlobalSpawn
    So the pool = every OUTDOOR placed LvlMain* actor (the NPC_ records that template
    off LChar_MainAll) outside Skyline Valley / Burning Springs. LvlSub* actors use
    LChar_SubAll, which has no event entry, so they are NOT in this pool.

  Pool B - Spooky Scorched + Holiday Scorched
    LCharScorched_InvType_* [005A03B2-B8] each lead with
        LvlScorchedFestive  if Festive_Holiday_Enabled and GetRandomPercent <= Festive_ScorchedSpawnChance
        LvlScorchedSpooky   if Spooky_EventEnabled     and GetRandomPercent <= Spooky_ScorchedSpawnChance
    both outdoors only. So the pool = every OUTDOOR scorched spawn: dedicated scorched
    placements, plus LvlMain*/LvlSub* placements Mappalachia resolves to Scorched.

The chance globals are 0 in the ESM except Spooky (10); the live values are pushed by
the server during each event.

Outputs (under --out):
  01 Full Maps (4096)/<slug>.jpg      02 Numbered Maps (4096)/<slug>_numbered.jpg
  03 Region Tiles/<slug>/<region-slug>-spawn-map.jpg (+ coords csv)
  <slug>_exterior_coords.csv

Env: MAPPALACHIA_DIR / MAPPALACHIA_DB, GUIDES_ROOT (watermark).
"""
import argparse, csv, os, pickle, sqlite3, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import render_spawn_maps as R
from spawns_engine.geo import Geo

import event_swap_spawns as ESS   # pool A (actor bases + event regions) from the game files

R.TYPE_LABELS["event-swap"] = "Possible swap spawn"
R.TYPE_COLOURS["event-swap"] = (255, 193, 7)       # amber
R.TYPE_LABELS["scorched-swap"] = "Possible swap spawn"
R.TYPE_COLOURS["scorched-swap"] = (255, 138, 30)   # orange

SETS = {
    # Names are the in-game FULLs: LvlMoleMinerTreasureHunt = "Treasure Hunter",
    # CultistHighPriest = "Cultist High Priest"; SDOW_LvlSlasherFanMelee has no FULL,
    # its SDOW_LCharSlasherFan members are all "Pint-Sized Phantom <role>".
    ESS.MAP_SLUG:
        ("Treasure Hunters, Pint-Sized Phantoms and Cultist High Priests", "event-swap"),
    "event-swap-spooky-holiday-scorched":
        ("Spooky & Holiday Scorched", "scorched-swap"),
}


def points(conn, geo, which, scorched_npcs):
    q = ("SELECT p.instanceFormID, p.x, p.y, e.editorID FROM Position p "
         "JOIN Entity e ON e.entityFormID = p.referenceFormID "
         "WHERE p.spaceFormID = ? AND e.signature = 'NPC_' AND e.editorID IN (%s)")
    out = []
    if which == "event-swap":
        # Same points the Treasure Hunter Farming Map page lists, so its dot
        # numbers match (event_swap_spawns.numbered mirrors cluster_by_marker).
        return ESS.pool_points(R.MAPPALACHIA_DB, geo, ESS.pool_npcs(), ESS.event_regions())
    else:
        lvl = [e for e in scorched_npcs if e.startswith(("LvlMain", "LvlSub"))]
        ded = [e for e in scorched_npcs if e not in lvl]
        rows = conn.execute(q % ",".join("?" * len(ded)), [R.APPALACHIA_SPACE] + ded).fetchall()
        rows += conn.execute(
            "SELECT DISTINCT p.instanceFormID, p.x, p.y, e.editorID FROM Position p "
            "JOIN Entity e ON e.entityFormID = p.referenceFormID "
            "JOIN NPC n ON n.instanceFormID = p.instanceFormID "
            "WHERE p.spaceFormID = ? AND n.npcName = 'Scorched' AND n.spawnWeight > 0 "
            "AND e.editorID IN (%s)" % ",".join("?" * len(lvl)),
            [R.APPALACHIA_SPACE] + lvl).fetchall()
    seen = set()
    for inst, x, y, eid in rows:
        if inst in seen:
            continue
        seen.add(inst)
        region, marker, _ = geo.resolve(R.APPALACHIA_SPACE, x, y)
        out.append({"ref": f"{inst:08X}", "x": x, "y": y, "space": R.APPALACHIA_SPACE,
                    "region": region or "Unknown", "marker": marker or "Unmarked",
                    "source_type": which, "base": eid})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--scorched-npcs", required=True,
                    help="text file, one scorched NPC_ EDID per line (from the ESM walk)")
    ap.add_argument("--only", help="render just this set slug")
    a = ap.parse_args()
    scorched = [l.strip() for l in open(a.scorched_npcs, encoding="utf-8") if l.strip()]
    conn = sqlite3.connect(R.MAPPALACHIA_DB)
    spaces = R.load_spaces(conn)
    boxes = R.region_bboxes(conn)
    geo = Geo(R.MAPPALACHIA_DB)
    to_px = R.projector(spaces[R.APPALACHIA_SPACE])
    bg = os.path.join(R.MAPPALACHIA, "img", "wrld", "Appalachia_menu.jpg")
    for slug, (title, which) in SETS.items():
        if a.only and slug != a.only:
            continue
        pts = points(conn, geo, which, scorched)
        rows = R.cluster_by_marker(pts, to_px)
        p_plain = os.path.join(a.out, "01 Full Maps (4096)", f"{slug}.jpg")
        p_num = os.path.join(a.out, "02 Numbered Maps (4096)", f"{slug}_numbered.jpg")
        plain, numbered = R.render_exterior(bg, rows, title, p_plain, p_num)
        tiles = R.render_region_tiles(numbered, rows, boxes, to_px,
                                      os.path.join(a.out, "03 Region Tiles", slug), slug,
                                      name_fn=lambda r: f"{R._region_slug(r)}-spawn-map.jpg")
        with open(os.path.join(a.out, f"{slug}_exterior_coords.csv"), "w",
                  newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["n", "region", "marker", "count", "game_x", "game_y", "refs"])
            for r in rows:
                w.writerow([r["n"], r["region"], r["marker"], r["count"],
                            round(r["x"], 1), round(r["y"], 1), " ".join(r["refs"])])
        by = {}
        for p in pts:
            by[p["region"]] = by.get(p["region"], 0) + 1
        print(slug, "points", len(pts), "markers", len(rows), "tiles", len(tiles), by)


if __name__ == "__main__":
    main()
