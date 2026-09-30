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


# Route map (Treasure Hunter Farming Map page, "Suggested Route"): the loop from
# event_swap_spawns.route() drawn over the plain Appalachia map - one colour per
# region, a numbered stop at each map-marker icon, arrows showing the direction.
ROUTE_COLOURS = ESS.ROUTE_COLOURS
ROUTE_STOP_D = 60
ROUTE_LINE_W = 12


def _arrow(d, a, b, colour):
    import math
    (x0, y0), (x1, y1) = a, b
    L = math.hypot(x1 - x0, y1 - y0)
    if L < ROUTE_STOP_D * 2.4:
        return
    ux, uy = (x1 - x0) / L, (y1 - y0) / L
    mx, my = (x0 + x1) / 2 + ux * 18, (y0 + y1) / 2 + uy * 18
    s = 30
    tip = (mx + ux * s, my + uy * s)
    l = (mx - ux * s - uy * s * 0.9, my - uy * s + ux * s * 0.9)
    r = (mx - ux * s + uy * s * 0.9, my - uy * s - ux * s * 0.9)
    d.polygon([tip, l, r], fill=colour, outline=(10, 10, 10), width=4)


def _bow(a, b, k, n=40):
    """Points on a quadratic curve from a to b, bowed sideways by k * length."""
    (x0, y0), (x1, y1) = a, b
    mx, my = (x0 + x1) / 2 - (y1 - y0) * k, (y0 + y1) / 2 + (x1 - x0) * k
    return [((1 - t) ** 2 * x0 + 2 * (1 - t) * t * mx + t * t * x1,
             (1 - t) ** 2 * y0 + 2 * (1 - t) * t * my + t * t * y1)
            for t in (i / n for i in range(n + 1))]


def _arrow_at(d, a, b, colour):
    """Arrowhead at a, pointing along a->b."""
    import math
    (x0, y0), (x1, y1) = a, b
    L = math.hypot(x1 - x0, y1 - y0) or 1
    ux, uy = (x1 - x0) / L, (y1 - y0) / L
    s = 30
    tip = (x0 + ux * s, y0 + uy * s)
    l = (x0 - ux * s - uy * s * 0.9, y0 - uy * s + ux * s * 0.9)
    r = (x0 - ux * s + uy * s * 0.9, y0 - uy * s - ux * s * 0.9)
    d.polygon([tip, l, r], fill=colour, outline=(10, 10, 10), width=4)


# Route maps are drawn on the in-game map (Mappalachia's Appalachia_menu.jpg) with
# its map-marker icons on top. Not the satellite map (Duchess, 30 Sep 2026).
ROUTE_BG = "Appalachia_menu.jpg"
ROUTE_BG_DIM = 0.0
ROUTE_ICON_PX = 46          # map-marker icon size on the 4096 canvas
ROUTE_BADGE_OFF = (34, -34)  # stop number sits up-right of its location's icon


def map_marker_icons(size=ROUTE_ICON_PX):
    """The in-game map-marker icons Mappalachia ships (img/mapmarker/*.svg), placed
    from its MapMarker table: [(x, y, label, RGBA icon)]. Needs cairosvg."""
    import io
    import cairosvg
    from PIL import Image
    con = sqlite3.connect("file:{}?mode=ro".format(R.MAPPALACHIA_DB), uri=True)
    rows = con.execute("SELECT x, y, label, icon FROM MapMarker WHERE spaceFormID = ?",
                       [R.APPALACHIA_SPACE]).fetchall()
    con.close()
    cache, out = {}, []
    for x, y, label, icon in rows:
        if icon not in cache:
            svg = os.path.join(R.MAPPALACHIA, "img", "mapmarker", icon + ".svg")
            if not os.path.exists(svg):
                cache[icon] = None
            else:
                png = cairosvg.svg2png(url=svg, output_height=size * 4)
                im = Image.open(io.BytesIO(png)).convert("RGBA")
                im.thumbnail((size, size), Image.LANCZOS)
                cache[icon] = im
        if cache[icon] is not None:
            out.append((x, y, label, cache[icon]))
    return out


def _icon_background(bg_path, to_px):
    """The background with every map-marker icon pasted on, written to a temp
    JPEG (render_exterior takes a path). Falls back to the plain background."""
    import tempfile
    from PIL import Image
    try:
        marks = map_marker_icons()
    except Exception as e:
        print("  [WARN] map-marker icons skipped ({})".format(e))
        return bg_path
    base = Image.open(bg_path).convert("RGB")
    if base.size != (R.S, R.S):
        base = base.resize((R.S, R.S), Image.LANCZOS)
    for x, y, _, im in marks:
        px, py = to_px(x, y)
        base.paste(im, (int(px - im.width / 2), int(py - im.height / 2)), im)
    out = os.path.join(tempfile.gettempdir(), "event_swap_bg_icons.jpg")
    base.save(out, "JPEG", quality=95)
    return out


def render_route(legs, bg_path, to_px, out_path, title="Suggested Treasure Hunter Route",
                 icons=True, dim=ROUTE_BG_DIM, closed=False, crop=False, legend=None):
    """legs = [{region, stops: [{n, x, y}]}] in route order. closed = draw the hop
    from the last stop back to the first. crop = cut the map down to the route and
    put the legend in a band above it (else the legend sits top-left on the full
    4096 map). legend = [(label, text, rgb)] rows, default region + stop range."""
    from PIL import Image, ImageDraw, ImageEnhance
    base = Image.open(bg_path).convert("RGB")
    if base.size != (R.S, R.S):
        base = base.resize((R.S, R.S), Image.LANCZOS)
    if dim:
        base = ImageEnhance.Brightness(base).enhance(1.0 - dim)
    d = ImageDraw.Draw(base)
    marks = map_marker_icons() if icons else []
    stops = []
    for leg in legs:
        col = ROUTE_COLOURS.get(leg["region"], (255, 255, 255))
        for s in leg["stops"]:
            px, py = to_px(s["x"], s["y"])
            stops.append((px, py, col, s["n"]))
    hop_pairs = [(i - 1, i) for i in range(1, len(stops)) if stops[i - 1][2] != stops[i][2]]
    line_pairs = [(i - 1, i) for i in range(1, len(stops)) if stops[i - 1][2] == stops[i][2]]
    if closed and len(stops) > 2:
        (line_pairs if stops[-1][2] == stops[0][2] else hop_pairs).append((len(stops) - 1, 0))
    # Lines inside a region: solid, in the region's colour (dark casing under it).
    segs = [(stops[i][:2], stops[j][:2], stops[j][2]) for i, j in line_pairs]
    for a, b, _ in segs:
        d.line([a, b], fill=(10, 10, 10), width=ROUTE_LINE_W + 8)
    for a, b, col in segs:
        d.line([a, b], fill=col, width=ROUTE_LINE_W)
    for a, b, col in segs:
        _arrow(d, a, b, col)
    # Region to region: a dashed white curve (a fast-travel hop). It bows to the
    # side that keeps it furthest from other stops, so a hop never looks like it
    # runs through a stop it doesn't visit.
    for i0, i in hop_pairs:
        a, b = stops[i0][:2], stops[i][:2]
        others = [p[:2] for j, p in enumerate(stops) if j not in (i0, i)]
        best = None
        for k in (0.0, 0.12, -0.12, 0.2, -0.2, 0.3, -0.3):
            pts = _bow(a, b, k)
            clear = min((min(((x - ox) ** 2 + (y - oy) ** 2) ** 0.5 for ox, oy in others)
                         for x, y in pts[3:-3]), default=1e9)
            score = (clear >= ROUTE_STOP_D * 1.2, -abs(k) if clear >= ROUTE_STOP_D * 1.2 else clear)
            if best is None or score > best[0]:
                best = (score, pts)
        pts = best[1]
        for j in range(0, len(pts) - 1, 2):
            d.line([pts[j], pts[j + 1]], fill=(10, 10, 10), width=16)
        for j in range(0, len(pts) - 1, 2):
            d.line([pts[j], pts[j + 1]], fill=(255, 255, 255), width=8)
        m = len(pts) // 2
        _arrow_at(d, pts[m], pts[m + 1], (255, 255, 255))
    f = R._font(40)
    r = ROUTE_STOP_D / 2
    if marks:
        # Every map marker, over the lines, so each stop's own icon shows where
        # the line meets it; the stop number sits beside the icon.
        for x, y, _, im in marks:
            px, py = to_px(x, y)
            base.paste(im, (int(px - im.width / 2), int(py - im.height / 2)), im)
        stop_xy = [(px + ROUTE_BADGE_OFF[0], py + ROUTE_BADGE_OFF[1]) for px, py, _, _ in stops]
        for (px, py, col, _), (bx, by) in zip(stops, stop_xy):
            d.line([(px, py), (bx, by)], fill=(10, 10, 10), width=6)
    else:
        stop_xy = [(px, py) for px, py, _, _ in stops]
    for (px, py), (_, _, col, n) in zip(stop_xy, stops):
        d.ellipse([px - r, py - r, px + r, py + r], fill=col, outline=(10, 10, 10), width=5)
        t = str(n)
        tw = d.textlength(t, font=f)
        d.text((px - tw / 2, py - 24), t, font=f, fill=(10, 10, 10))
    # START flag beside stop 1.
    if stops:
        fs = R._font(56)
        R.draw_outlined_text(d, (stop_xy[0][0] + r + 14, stop_xy[0][1] - 34), "START", fs, w=4)
    # Legend: region colour + its stop range, in route order (or the caller's rows).
    if legend is None:
        legend = []
        for leg in legs:
            if leg["stops"]:
                a_, b_ = leg["stops"][0]["n"], leg["stops"][-1]["n"]
                legend.append((leg["region"], "{}-{}".format(a_, b_),
                               ROUTE_COLOURS.get(leg["region"], (255, 255, 255))))
    if crop:
        xs = [p[0] for p in stop_xy]; ys = [p[1] for p in stop_xy]
        pad = 260
        x0, y0 = max(0, int(min(xs) - pad)), max(0, int(min(ys) - pad))
        x1, y1 = min(R.S, int(max(xs) + pad)), min(R.S, int(max(ys) + pad))
        mapimg = base.crop((x0, y0, x1, y1))
        band_h = R.LEGEND_PAD * 2 + 96 + 26 + len(legend) * 84 + 24 + 120
        img = Image.new("RGB", (mapimg.width, mapimg.height + band_h), R.LEGEND_BG)
        img.paste(mapimg, (0, band_h))
    else:
        img = base
    R.draw_legend(img, title, legend)
    d = ImageDraw.Draw(img)
    # Key for the dashed hop, under the legend box.
    fk = R._font(60)
    ky = R.LEGEND_PAD + 96 + 26 + len(legend) * 84 + 24 + 30
    kx = R.LEGEND_PAD
    d.rectangle([kx, ky, kx + 620, ky + 90], fill=R.LEGEND_BG, outline=R.LEGEND_BORDER, width=R.LEGEND_BORDER_W)
    for j in range(4):
        d.line([(kx + 28 + j * 40, ky + 45), (kx + 48 + j * 40, ky + 45)], fill=(255, 255, 255), width=8)
    d.text((kx + 200, ky + 12), "Next region", font=fk, fill=(230, 230, 230))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    R.map_watermark.apply(img).save(out_path, "JPEG", quality=86, optimize=True)
    return out_path


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
        # The spawn maps carry the in-game map-marker icons too (under the dots),
        # same as the route maps, so readers can find each spot by its icon.
        bg_icons = _icon_background(bg, to_px)
        plain, numbered = R.render_exterior(bg_icons, rows, title, p_plain, p_num)
        # Tiles go straight into "03 Region Tiles" - --out is already the item's
        # own Maps folder, so a <slug> subfolder only made a second copy of each.
        tiles = R.render_region_tiles(numbered, rows, boxes, to_px,
                                      os.path.join(a.out, "03 Region Tiles"), slug,
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
        if which == "event-swap":
            # Same page_regions -> route the farming-map page builds, so the stop
            # numbers on the map match the page's route list.
            from spawns_configs.cryptids import ALL_REGIONS
            clusters = ESS.numbered(pts)
            try:
                workshops = ESS.workshop_markers(R.MAPPALACHIA_DB)
            except Exception:
                workshops = []
            pr = ESS.page_regions(clusters, geo, ALL_REGIONS, ESS.event_regions(), workshops)
            legs = ESS.route(pr, geo)
            rbg = os.path.join(R.MAPPALACHIA, "img", "wrld", ROUTE_BG)
            p_route = os.path.join(a.out, "04 Route Map", f"{slug}-route-map.jpg")
            render_route(legs, rbg, to_px, p_route)
            print(slug, "route", sum(len(l["stops"]) for l in legs), "stops ->", p_route)
            rec = ESS.recommended_route(pr)
            if rec:
                tally = {}
                for st in rec["stops"]:
                    tally[st["region"]] = tally.get(st["region"], 0) + 1
                rows = [(rg, "{} {}".format(k, "stop" if k == 1 else "stops"),
                         ROUTE_COLOURS.get(rg, (255, 255, 255))) for rg, k in tally.items()]
                p_rec = os.path.join(a.out, "04 Route Map", f"{slug}-recommended-route-map.jpg")
                render_route(ESS.legs_of(rec["stops"]), rbg, to_px, p_rec,
                             title="Recommended {}-Stop Route".format(len(rec["stops"])),
                             closed=True, crop=True, legend=rows)
                print(slug, "recommended", len(rec["stops"]), "stops,", rec["spawns"], "spawns ->", p_rec)


if __name__ == "__main__":
    main()
