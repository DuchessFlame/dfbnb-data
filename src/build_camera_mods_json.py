#!/usr/bin/env python3
"""
build_camera_mods_json.py
-------------------------
Data feed for /df/plan-checklists/camera-mod/ — the ProSnap Deluxe camera's
lenses and paints.

WHY THIS PAGE NEEDS ITS OWN BUILD
---------------------------------
Only FOUR of the thirteen camera mods are learned from a plan, so plan_master
cannot carry this page. The fifth lens (Standard) is fitted by default and has
no plan, and all eight paints are Atom Shop or Season 14 scoreboard unlocks
whose OMODs exist with no recipe behind them at all. Selecting on plan_master
would have shipped a page that was four lenses and an empty Skins section.

So the roster comes from the OMOD export — every mod that attaches to the
camera, whether or not a plan grants it — and the four rows that DO have a plan
copy that plan's row out of plan_master verbatim, so their How to Obtain ledger
and drop rates are byte-identical to the same plan elsewhere and cannot drift.

Two modes, same shape as every other twin-channel builder:
  (default / live)  reads tsv/      -> dist/camera_mods.json
  --pts             reads tsv/pts/  -> dist/pts/camera_mods.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.

Usage:
    python3 src/build_camera_mods_json.py
    python3 src/build_camera_mods_json.py --pts
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(SCRIPT_DIR)
PTS = "--pts" in sys.argv
TSV_DIR = os.path.join(REPO, "tsv", "pts") if PTS else os.path.join(REPO, "tsv")
LIVE_TSV_DIR = os.path.join(REPO, "tsv")
DIST_DIR = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST_DIR, "camera_mods.json")
PLAN_MASTER = os.path.join(DIST_DIR, "plan_master.json")

SCHEMA = 1
IMAGE_BASE = "/wp-content/uploads/guide-images/plan-checklist/camera/"

# Every mod that attaches to the ProSnap Deluxe. Anchored on `_Camera_` with the
# laser weapons excluded by name: a Laser Gun's "Gyro Compensating Lens" is
# mod_LaserGun_muzzle_Camera_Base, which matches the word but is not a camera
# part. CUT_ and zzz_ records are editor leftovers and never render.
CAMERA_RX = re.compile(r"(^|_)mod_Camera_", re.I)
NOT_CAMERA_RX = re.compile(r"LaserGun|LaserMusket|UltraciteLaser|BackPack", re.I)
DEAD_RX = re.compile(r"^(CUT_|zzz)", re.I)

# The attach point is what separates a lens from a paint, and it is the game's
# own answer rather than a guess from the name: a lens changes what the camera
# does (ap_gun_Barrel), a paint changes only how it looks (ap_gun_Appearance).
GROUPS = [
    ("mod",  "Mods",  "ap_gun_Barrel",
     "The lens fitted to the camera. Each one changes what you can see through "
     "the viewfinder — zoom, night vision, or the targeting HUD."),
    ("skin", "Skins", "ap_gun_Appearance",
     "Paint jobs. Cosmetic only — a paint never changes what the camera does."),
]
ATTACH_TO_GROUP = {ap: key for key, _label, ap, _blurb in GROUPS}


# ── TSV ─────────────────────────────────────────────────────────────────────

def newest(pattern, exclude=""):
    hits = [h for h in glob.glob(os.path.join(TSV_DIR, pattern))
            if not (exclude and re.search(exclude, os.path.basename(h), re.I))]
    if not hits:
        raise SystemExit(f"[camera-mods] no TSV matching {pattern} in {TSV_DIR}")
    # Month in the filename decides, mtime only breaks a tie — a re-downloaded
    # older export shares its timestamp with the newer one.
    months = {m: i for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
    def stamp(path):
        m = re.search(r"_([A-Za-z]{3,9})_(\d{4})", os.path.basename(path))
        return (int(m.group(2)), months.get(m.group(1)[:3].lower(), 0)) if m else (0, 0)
    return sorted(hits, key=lambda h: (stamp(h), os.path.getmtime(h)), reverse=True)[0]


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            yield r


# ── How to Obtain ───────────────────────────────────────────────────────────
# The EditorID prefix is the source, the same convention every other DF/BNB
# builder reads. Each returns the plan_master ledger shape so the renderer's
# obtainLedgerBody prints these rows exactly like a real plan's.

SEASON_RX = re.compile(r"^SCORE_S(\d+)_", re.I)


def season_names():
    path = os.path.join(LIVE_TSV_DIR, "fallout76_seasons.tsv")
    out = {}
    if not os.path.isfile(path):
        return out
    for r in rows(path):
        num = (r.get("SeasonNumber") or "").strip()
        name = (r.get("SeasonName") or "").strip()
        if num.isdigit() and name:
            out[int(num)] = name
    return out


def obtain_for(edid, seasons):
    """(obtain sentence, obtain_unlocks, obtain_ledger) for a mod with no plan."""
    if edid.upper().startswith("ATX_"):
        line = "Bought from the Atom Shop with Atoms, on its own or in a bundle."
        return ("Not learned from a plan — it unlocks on your account.",
                [line], [{"label": "Atom Shop", "unlocks": [0], "drop": "N/A"}])
    m = SEASON_RX.match(edid)
    if m:
        n = int(m.group(1))
        name = seasons.get(n)
        line = (f"Claimed from the {name} Scoreboard (Season {n})." if name
                else f"Claimed from the Season {n} Scoreboard.")
        return ("Not learned from a plan — it unlocks on your account.",
                [line], [{"label": "Scoreboard", "unlocks": [0], "drop": "N/A"}])
    return ("Fitted by default — every ProSnap Deluxe comes with it, so there is "
            "nothing to unlock.", [], [])


# ── Output & Effects ────────────────────────────────────────────────────────
# Read off the OMOD's own properties. Deliberately short: the camera has no
# damage or resistance ladder, so there is no level curve to print — a lens
# either changes the view or it does not.

ZOOM_RX = re.compile(r"ZM_Camera_([A-Za-z0-9]+)")
ENCH_RX = re.compile(r'\[?([A-Za-z0-9_]+)\s+""(.+?)""')

ZOOM_LABEL = {
    "105mm":       "105mm — a moderate zoom",
    "200mm":       "200mm — the longest zoom",
    "NightVision": "Night vision",
    "Targeting":   "Targeting",
}


def effects_for(edid, props, is_skin):
    """The Output & Effects block, in the shape penEffectsBody expects."""
    if is_skin:
        return {"summary": "Changes the camera's appearance and nothing else. "
                           "Paints carry no stats, and fitting one never changes "
                           "what you can photograph.",
                "rows": []}
    out = []
    summary_bits = []
    for p in props:
        name = (p.get("PropertyName") or "").strip()
        v1 = (p.get("Value1") or "").strip()
        if name == "ZoomData":
            m = ZOOM_RX.search(v1)
            if m:
                out.append({"label": "View", "value": ZOOM_LABEL.get(m.group(1), m.group(1))})
        elif name == "Enchantments":
            m = ENCH_RX.search(v1)
            if m and m.group(2):
                out.append({"label": "Grants", "value": m.group(2)})
                summary_bits.append(m.group(2).lower())
        elif name == "AimModelBaseStability":
            out.append({"label": "Aim stability", "value": "Steadier while sighted"})
        elif name == "SightedTransitionSeconds":
            out.append({"label": "Raise time", "value": "Faster to bring up"})
    if not out:
        return None
    # What the lens DOES first, then how it handles. A reader opening this box
    # wants "what does this change" before "is it a bit steadier".
    order = {"View": 0, "Grants": 1, "Aim stability": 2, "Raise time": 3}
    out.sort(key=lambda r: order.get(r["label"], 9))
    summary = ("The standard lens every camera starts with." if edid.endswith("_Standard")
               else "Swaps what you see through the viewfinder"
                    + (f", and grants the {summary_bits[0]}." if summary_bits else "."))
    return {"summary": summary, "rows": out}


# ── row assembly ────────────────────────────────────────────────────────────

def image_name(edid):
    tail = re.sub(r"^(ATX_|SCORE_S\d+_)", "", edid, flags=re.I).split("_")[-1]
    return f"{tail}.avif" if tail else ""


def build():
    seasons = season_names()

    omod_path = newest("OMOD_Export_*.tsv", exclude=r"_Properties\.tsv$")
    prop_path = newest("OMOD_Export_*_Properties.tsv")

    props_by_fid = {}
    for r in rows(prop_path):
        props_by_fid.setdefault((r.get("OMOD_FormID") or "").strip(), []).append(r)

    mods = []
    for r in rows(omod_path):
        edid = (r.get("OMOD_EDID") or "").strip()
        if not CAMERA_RX.search(edid) or NOT_CAMERA_RX.search(edid) or DEAD_RX.match(edid):
            continue
        ap = (r.get("AttachPoint_EDID") or "").strip()
        if ap not in ATTACH_TO_GROUP:
            continue
        mods.append({
            "formid": (r.get("OMOD_FormID") or "").strip(),
            "edid":   edid,
            "name":   (r.get("FULL") or "").strip() or edid,
            "group":  ATTACH_TO_GROUP[ap],
            "attach": ap,
        })

    # The four lenses with a plan: copy the plan_master row so their ledger and
    # drop rates are the same object the rest of the site shows.
    by_cnam = {}
    if os.path.exists(PLAN_MASTER):
        with open(PLAN_MASTER, encoding="utf-8") as fh:
            for it in (json.load(fh).get("items") or []):
                cn = (it.get("cnam") or {})
                if cn.get("sig") == "OMOD" and cn.get("formid"):
                    by_cnam[cn["formid"]] = it
    else:
        print(f"[camera-mods] WARNING: {PLAN_MASTER} missing — rows will have no plan ledger",
              file=sys.stderr)

    groups = []
    for key, label, _ap, blurb in GROUPS:
        items = []
        for m in sorted([x for x in mods if x["group"] == key], key=lambda x: x["name"]):
            plan = by_cnam.get(m["formid"])
            fx = effects_for(m["edid"], props_by_fid.get(m["formid"], []), key == "skin")
            if plan:
                row = dict(plan)
                row["effects"] = fx or plan.get("effects")
            else:
                obtain, unlocks, ledger = obtain_for(m["edid"], seasons)
                row = {
                    "kind": "plan", "brand": "df", "type": "camera-mod",
                    "id": f"CAMERAMOD_{m['formid']}",
                    "name": m["name"],
                    "has_image_box": True,
                    "image_dir": "camera",
                    "obtain": obtain,
                    "category_label": "Camera Lens" if key == "mod" else "Camera Paint",
                    "obtain_routes": [],
                    "obtain_unlocks": unlocks,
                    "obtain_ledger": ledger,
                    "plan_item": None,
                    "cobj": None,
                    "cnam": {"formid": m["formid"], "edid": m["edid"], "sig": "OMOD"},
                    "tradeable": False,
                    "stops_dropping": None,
                    "effects": fx,
                    "cut": False,
                    "cut_reason": None,
                    "images": [],
                    "image_source": "",
                }
            # Row title: the in-game name, with the shared prefix trimmed. Every
            # lens is called "ProSnap Deluxe <something> Lens", and a column of
            # rows that all start the same way is a column you cannot scan.
            row["display_name"] = re.sub(r"^Plan:\s*", "", row.get("name") or "")
            row["display_name"] = re.sub(r"^ProSnap Deluxe\s+", "", row["display_name"])
            if not row.get("images"):
                row["images"] = [IMAGE_BASE + image_name(m["edid"])]
                row["image_source"] = "camera"
            items.append(row)
        groups.append({"key": key, "label": label, "blurb": blurb,
                       "count": len(items), "items": items})

    doc = {
        "schemaVersion": SCHEMA,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": PTS,
        "title": "Camera Mod",
        "source_files": [os.path.basename(omod_path), os.path.basename(prop_path)],
        "groups": groups,
    }
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)
    total = sum(g["count"] for g in groups)
    print(f"[camera-mods] wrote {OUT} — {total} mods "
          + ", ".join(f"{g['label']}={g['count']}" for g in groups))
    print(f"  exports: {', '.join(doc['source_files'])}")
    print(f"  with a plan: {sum(1 for g in groups for i in g['items'] if i.get('plan_item'))}")


if __name__ == "__main__":
    build()
