#!/usr/bin/env python3
r"""
locate_map_screenshot.py — read an in-game FO76 world-map screenshot and work out
exactly where the player is standing, in game coordinates.

Why: the grave / mask spawn photo sets are named per spawn, so every screenshot has
to be tied to the right spawn. Eyeballing "that dot looks about here" is guesswork
once two spawns share a marker (Kanawha County Cemetery has three graves). This
solves it from the pixels instead.

How it works
------------
The FO76 world map is an orthographic, north-up render, so game -> screen is a
uniform scale + translation:

    px = (gx - Ox) / s          py = (Oy - gy) / s

No anchor marker is needed — the fit is derived from the marker constellation:

1. Detect the pale-blue map-marker icons as pixel blobs.
2. Scan every plausible scale s. For each, every (icon, marker) pairing implies an
   origin, so the modal origin over all pairings is the candidate translation.
3. Score that (s, origin) by how many icons land on a real marker. Best score wins,
   then refine s locally.
4. Detect the gold player arrow and invert the transform.

Doing it constellation-wide, rather than anchoring on the labelled POI, matters
because on the "player is standing on it" shot the POI icon is usually HIDDEN
UNDERNEATH the player arrow — so an anchor-based fit fails exactly when you need
the answer most.

Usage
-----
  python locate_map_screenshot.py shot.jpg [more.jpg ...] [--graves] [--debug]
  python locate_map_screenshot.py "Savage Divide"/*.jpg --graves

Env: MAPPALACHIA_DB   default D:\Mappalachia\data\mappalachia.db
"""

import argparse, csv, math, os, sqlite3, sys
from collections import defaultdict, Counter

import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
APPALACHIA_SPACE = 2480661
GRAVE_TSV = os.path.join(REPO, "tsv", "phantom_grave_sites.tsv")

# HUD furniture to ignore. (left, top, right, bottom) as fractions of the frame.
HUD_BOXES = [
    (0.00, 0.00, 0.36, 0.26),   # LB MENU + C.A.M.P. slots
    (0.00, 0.62, 0.28, 1.00),   # World Activity
    (0.60, 0.00, 1.00, 0.22),   # Social + Challenges
    (0.86, 0.52, 1.00, 1.00),   # weight / caps / atoms column
    (0.00, 0.90, 1.00, 1.00),   # button legend
    (0.53, 0.04, 0.70, 0.22),   # nuke / event roundel
]

SCALE_MIN, SCALE_MAX, SCALE_STEP = 25.0, 200.0, 0.25
ORIGIN_CELL = 900.0          # game units, origin-vote grid
INLIER_TOL_PX = 8.0          # an icon this close to a projected marker counts as a hit


# ---------------------------------------------------------------- data

def load_markers():
    conn = sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)
    rows = [(l, float(x), float(y)) for x, y, l, sp in
            conn.execute("SELECT x, y, label, spaceFormID FROM MapMarker")
            if sp == APPALACHIA_SPACE and l]
    conn.close()
    return rows


def load_graves():
    if not os.path.exists(GRAVE_TSV):
        return []
    out = []
    with open(GRAVE_TSV, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r.get("x") and r.get("y"):
                out.append((r.get("site_number") or "?", r.get("ref_formid", ""),
                            r.get("closest_fast_travel", ""),
                            float(r["x"]), float(r["y"])))
    return out


# ---------------------------------------------------------------- vision

def _hud_mask(shape):
    h, w = shape
    m = np.zeros(shape, bool)
    for l, t, r, b in HUD_BOXES:
        m[int(t * h):int(b * h), int(l * w):int(r * w)] = True
    return m


def _blobs(mask, min_px, max_px=None):
    ys, xs = np.nonzero(mask)
    occ = set(zip(xs.tolist(), ys.tolist()))
    seen, out = set(), []
    for p in occ:
        if p in seen:
            continue
        stack, comp = [p], []
        seen.add(p)
        while stack:
            x, y = stack.pop()
            comp.append((x, y))
            for dx in (-2, -1, 0, 1, 2):
                for dy in (-2, -1, 0, 1, 2):
                    q = (x + dx, y + dy)
                    if q in occ and q not in seen:
                        seen.add(q)
                        stack.append(q)
        n = len(comp)
        if n < min_px or (max_px and n > max_px):
            continue
        xs_ = [c[0] for c in comp]
        ys_ = [c[1] for c in comp]
        out.append({"x": sum(xs_) / n, "y": sum(ys_) / n, "n": n,
                    "w": max(xs_) - min(xs_) + 1, "h": max(ys_) - min(ys_) + 1})
    return out


def detect_icons(img):
    a = np.asarray(img, dtype=int)
    R, G, B = a[:, :, 0], a[:, :, 1], a[:, :, 2]
    m = (B > 150) & (B - R > 25) & (G > 120) & (R > 90)
    m &= ~_hud_mask(m.shape)
    return _blobs(m, 70)


def detect_player(img):
    """The gold player arrow: a solid, roughly square, saturated-gold blob."""
    a = np.asarray(img, dtype=int)
    R, G, B = a[:, :, 0], a[:, :, 1], a[:, :, 2]
    # The arrow is a BRIGHT pale gold (~243,220,135). Sunlit red-brown terrain
    # (Cranberry Bog, ~155,138,91) also reads "warm", so the brightness floor is
    # what separates them — not the warmth alone.
    m = (R > 195) & (G > 170) & (B < 180) & (R - B > 70) & (G - B > 55)
    m &= ~_hud_mask(m.shape)
    # A marker icon drawn over the arrow can split it into fragments that each fall
    # under the size floor. Dilate so the pieces rejoin before blobbing.
    d = m.copy()
    for sx in (-2, -1, 1, 2):
        d |= np.roll(m, sx, axis=1)
    m2 = d.copy()
    for sy in (-2, -1, 1, 2):
        m2 |= np.roll(d, sy, axis=0)
    m = m2
    h, w = m.shape
    ccx, ccy = w / 2.0, h / 2.0
    cands = []
    for b in _blobs(m, 90, 1600):
        ar = b["w"] / max(1.0, b["h"])
        fill = b["n"] / max(1.0, b["w"] * b["h"])
        # The arrow is a compact chevron, 25-60px, squarish, solidly filled.
        if not (0.55 <= ar <= 1.8 and 0.22 <= fill <= 0.85):
            continue
        if not (22 <= b["w"] <= 80 and 22 <= b["h"] <= 80):
            continue
        # Opening the map centres it on the player, so the arrow is at or near the
        # middle of the frame. A selected-POI highlight ring reads warm and roughly
        # circular too, but sits wherever the POI is — usually far off centre.
        d = math.hypot(b["x"] - ccx, b["y"] - ccy)
        if d > 0.22 * w:
            continue
        cands.append((d, b))
    if not cands:
        return None
    cands.sort(key=lambda t: t[0])
    return cands[0][1]


# ---------------------------------------------------------------- fit

def fit_transform(icons, markers, s_min=SCALE_MIN, s_max=SCALE_MAX, step=SCALE_STEP):
    """Scan every plausible scale; for each, vote for the origin and count how many
    icons land on a real marker. Best inlier count wins.

    An earlier version pre-filtered scales by a translation-invariant offset vote.
    That vote saturated — with a 600-unit offset grid over 458 markers, essentially
    every scale scored the same, so the pre-filter discarded correct scales at random.
    The inlier check is the only thing that actually discriminates, so just run it.
    """
    bx = np.array([b["x"] for b in icons])
    by = np.array([b["y"] for b in icons])
    gx = np.array([m[1] for m in markers])
    gy = np.array([m[2] for m in markers])

    best = None
    for s in np.arange(s_min, s_max + step, step):
        # every (icon, marker) pairing implies an origin; the modal one wins
        Ox = gx[None, :] - bx[:, None] * s
        Oy = gy[None, :] + by[:, None] * s
        keys = (np.round(Ox / ORIGIN_CELL).astype(np.int64) * 100003
                + np.round(Oy / ORIGIN_CELL).astype(np.int64))
        vals, counts = np.unique(keys.ravel(), return_counts=True)
        k = vals[counts.argmax()]
        sel = keys.ravel() == k
        ox = float(Ox.ravel()[sel].mean())
        oy = float(Oy.ravel()[sel].mean())

        mx = (gx - ox) / s
        my = (oy - gy) / s
        d = np.hypot(bx[:, None] - mx[None, :], by[:, None] - my[None, :])
        n = int((d.min(axis=1) <= INLIER_TOL_PX).sum())
        if best is None or n > best[0]:
            best = (n, float(s), ox, oy)
    return best






# ---------------------------------------------------------------- driver

def analyse(path, markers, offset_table, graves, debug=False, force_map=False):
    img = Image.open(path).convert("RGB")
    name = os.path.basename(path)
    icons = detect_icons(img)
    if len(icons) < 4 and not force_map:
        print(f"  {name:26} IN-WORLD PHOTO  ({len(icons)} icon-ish blobs)")
        return {"file": name, "kind": "photo"}

    # Verify every candidate scale by actually fitting an origin and counting how
    # many icons land on a real marker. The raw vote alone favours small scales,
    # where the coarse offset grid makes almost anything "agree".
    best = fit_transform(icons, markers)
    if best is None:
        print(f"  {name:26} map · fit failed")
        return {"file": name, "kind": "map"}
    # refine around the winner
    best = fit_transform(icons, markers, best[1] - 0.5, best[1] + 0.5, 0.05) or best
    fit, s, Ox, Oy = best
    # Sky and foliage in an in-world shot can pass the icon colour test. A real map
    # has most of its icons landing on real markers; a photo never does.
    if not force_map and (fit < 4 or fit / len(icons) < 0.45):
        print(f"  {name:26} IN-WORLD PHOTO  (only {fit}/{len(icons)} blobs fit a map)")
        return {"file": name, "kind": "photo"}

    # Opening the world map centres it on the player, so the middle of the frame is
    # the player's position whether or not the arrow blob is detectable. On dark or
    # sparse maps the arrow often isn't, so this is the more dependable read; the
    # arrow only matters when the map has been panned away from the player.
    h, w = np.asarray(img).shape[:2]
    cgx, cgy = Ox + (w / 2.0) * s, Oy - (h / 2.0) * s
    cm = min(markers, key=lambda m: math.hypot(m[1] - cgx, m[2] - cgy))
    print(f"  {name:26} MAP  scale {s:6.2f} · {fit}/{len(icons)} icons · "
          f"centre ({cgx:>8.0f},{cgy:>8.0f}) · {cm[0]} +{math.hypot(cm[1]-cgx, cm[2]-cgy):.0f}u")
    if graves:
        g = min(graves, key=lambda r: math.hypot(r[3] - cgx, r[4] - cgy))
        gd = math.hypot(g[3] - cgx, g[4] - cgy)
        flag = "" if gd <= 4000 else "   <-- centre is not on a known grave"
        print(f"  {'':26}     -> nearest grave #{g[0]} ({g[1]}) @ {g[2]} — {gd:.0f}u{flag}")

    player = detect_player(img)
    tag = f"scale {s:6.2f} · {fit}/{len(icons)} icons"

    if not player:
        print(f"  {name:26} MAP-OVERVIEW  {tag} · no player arrow")
        return {"file": name, "kind": "overview", "scale": s, "fit": fit}

    gxp = Ox + player["x"] * s
    gyp = Oy - player["y"] * s
    nm = min(markers, key=lambda m: math.hypot(m[1] - gxp, m[2] - gyp))
    nmd = math.hypot(nm[1] - gxp, nm[2] - gyp)
    out = {"file": name, "kind": "player", "scale": s, "fit": fit,
           "game": (gxp, gyp), "marker": nm[0], "marker_dist": nmd}
    print(f"  {name:26} MAP-PLAYER    {tag} · at ({gxp:>8.0f},{gyp:>8.0f}) "
          f"· {nm[0]} +{nmd:.0f}u")
    if graves:
        g = min(graves, key=lambda r: math.hypot(r[3] - gxp, r[4] - gyp))
        gd = math.hypot(g[3] - gxp, g[4] - gyp)
        out["grave"], out["grave_dist"] = g, gd
        flag = "" if gd <= 4000 else "   <-- no grave in the data here"
        print(f"  {'':26}              -> grave #{g[0]} ({g[1]}) @ {g[2]} "
              f"— {gd:.0f}u{flag}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("images", nargs="+")
    ap.add_argument("--graves", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--force-map", action="store_true",
                    help="skip the map/photo guard — treat every input as a map screenshot. "
                         "Use when you already know these are maps; the guard trips on "
                         "sparse maps (few icons) and on foliage-heavy photos alike.")
    args = ap.parse_args()

    if not os.path.exists(MAPPALACHIA_DB):
        raise SystemExit(f"Mappalachia DB not found at {MAPPALACHIA_DB}")
    markers = load_markers()
    graves = load_graves() if args.graves else []
    print(f"{len(markers)} markers · {len(graves)} graves\n")
    for p in sorted(args.images):
        try:
            analyse(p, markers, None, graves, args.debug, args.force_map)
        except Exception as e:
            print(f"  !! {os.path.basename(p)}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
