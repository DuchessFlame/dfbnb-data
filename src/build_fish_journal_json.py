#!/usr/bin/env python3
"""Build dist/fish_journal.json (+ pts twin) for the Field Journal of Appalachia page.

Parses the newest FISH_Export TSV, assigns each catchable fish to a single cascade
tier, derives size / max-length / fight / region / season, and attaches an image
path where a known asset exists. The renderer (df-bnb-fishing.js -> renderFieldJournal)
consumes this feed and computes the live "in season" state from seasonRule.rolloverDates.
"""
import csv, re, json, os, glob, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                 # dfbnb-data/
TSV_DIR = os.path.join(ROOT, "tsv")
DIST = os.path.join(ROOT, "dist")

IMG_BASE = "/wp-content/uploads/guide-images/fishing"
CASC_FOLDER = {
    "season": "seasonal-fish-images", "legend": "local-legends", "glow": "glowing-fish",
    "axolotl": "axolotl-images", "uncommon": "uncommon-fish", "common": "common-fish",
    "generic": "generic-fish", "junk": None, "gift": None,
}
# Confirmed server filenames (display name -> file, no extension). Extend as folders fill.
IMAGE_OVERRIDES = {
    "Ryl-Tkannoth, Maw-Begotten": "local-legends/mudskipper_mawbegotten",
    "Wavy Willard": "local-legends/suckerfish_wavywillard",
    "Organ Grinder": "local-legends/Organ Grinder",
    "Glass Ghost": "local-legends/Glass Ghost",
    "Fernskipper": "seasonal-fish-images/Fernskipper",
    "Dotted Axolotl": "axolotl-images/axolotl_dotted", "Purple Axolotl": "axolotl-images/axolotl_purple",
    "Stone Axolotl": "axolotl-images/axolotl_stone", "Clay Axolotl": "axolotl-images/axolotl_clay",
    "Striped Axolotl": "axolotl-images/axolotl_striped", "Scaled Axolotl": "axolotl-images/axolotl_scaled",
    "Shadow Axolotl": "axolotl-images/axolotl_shadow", "Speckled Axolotl": "axolotl-images/axolotl_speckled",
    "Spotted Axolotl": "axolotl-images/axolotl_spotted",
    "Charcoal Axolotl": "axolotl-images/axolotl_charcoal", "Banded Axolotl": "axolotl-images/axolotl_banded",
    "Pink Axolotl": "axolotl-images/axolotl_pink",
}
# Records to drop from the journal: leftover/duplicate game-data entries that are
# not part of the live monthly rotation. Slot 02 is Pink on live; "Peach Axolotl"
# is a cut twin of that slot that still lingers in the FISH export.
DROP_NAMES = {"Peach Axolotl"}
CASCADES = [
    ("season",  "Seasonal Fish",     "Rotates with the in-game season. Each is catchable only during its window before the next swaps in."),
    ("legend",  "Local Legend",      "Unique named bosses at fixed spots. Top of the cascade — first pick of the roll, with max fight and length."),
    ("glow",    "Glowing",           "Irradiated variants, one per region. A rare shimmer that rolls just below the legends."),
    ("axolotl", "Axolotl",           "Twelve colour morphs of the prize amphibian, one per calendar month. Only rolls on Improved or Superb bait."),
    ("uncommon","Uncommon",          "Region signature fish, two per region. The middle of the cascade."),
    ("common",  "Common",            "One sawgill per region — the reliable everyday catch that fills most casts."),
    ("generic", "Generic",           "Plain fish found in any water. Bottom of the cascade, right before the junk."),
    ("junk",    "Junk",              "Old-world rubbish snagged off the bottom — not a fish. No fight, no fillet."),
    ("gift",    "Waterlogged Gifts", "Weekend event drops. A 15% pre-cascade chance on each cast; open it for a reward bundle."),
]
REGION_KW = {"Forest": "The Forest", "AshHeap": "Ash Heap", "CranberryBog": "Cranberry Bog",
    "Mire": "The Mire", "ToxicValley": "Toxic Valley", "SavageDivide": "Savage Divide",
    "SkylineValley": "Skyline Valley", "BurningSprings": "Burning Springs"}
# Seasonal members + their season (curated cross-cut; legends keep their Local Legend tab too).
SEASON_OF = {"Orange Overseer": "Spring", "Fernskipper": "Summer", "Glass Ghost": "Summer",
    "Fester Koi": "Fall", "Sludge Eye": "Fall", "Bog Sucker": "Winter"}

def newest_tsv(directory=TSV_DIR, pts=False):
    """Newest FISH_Export in `directory`. Live names are '<Month>_<Year>'; PTS names
    are 'PTS_<YYYY-MM-DD>_<HHMM>'."""
    months = {"Jan":1,"Feb":2,"March":3,"Apr":4,"May":5,"June":6,"July":7,"Aug":8,
              "Sep":9,"Oct":10,"Nov":11,"Dec":12}
    best, bestkey = None, (-1, -1, -1, -1)
    for p in glob.glob(os.path.join(directory, "FISH_Export_*.tsv")):
        b = os.path.basename(p)
        if pts:
            m = re.search(r"FISH_Export_PTS_(\d{4})-(\d{2})-(\d{2})_(\d{4})", b)
            if not m: continue
            key = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        else:
            if "PTS" in b: continue
            m = re.search(r"FISH_Export_([A-Za-z]+)_(\d{4})", b)
            if not m: continue
            key = (int(m.group(2)), months.get(m.group(1), 0), 0, 0)
        if key > bestkey: bestkey, best = key, p
    return best

def load_axolotl_months(guide_filename):
    """Map axolotl display name -> active month name, from a built axolotl guide feed."""
    p = os.path.join(DIST, guide_filename)
    out = {}
    try:
        d = json.load(open(p, encoding="utf-8"))
        for v in d.get("variants", []):
            if v.get("name") and v.get("monthName"):
                out[v["name"]] = v["monthName"]
    except (IOError, ValueError):
        pass
    return out

def kws(r): return [r.get(k, "") for k in ("KW1","KW2","KW3","KW4","KW5")]
def has_kw(r, t): return any("FishType_%s:" % t in v for v in kws(r))
def disp(r):
    m = re.search(r'"([^"]+)"', r.get("FIRI", "")); return m.group(1) if m else r.get("EDID", "")
def size_of(r):
    for v in kws(r):
        if "FishSize_" in v: return v.split("FishSize_")[1].split(":")[0]
    return "Unknown"
def colls(r):
    out = []
    for i in range(1, 7):
        m = re.search(r":([^:]+):LVLI", r.get("Ref%d" % i, ""))
        if m: out.append(m.group(1))
    return out
def region_of(r):
    for k, v in REGION_KW.items():
        if has_kw(r, k): return v
    if has_kw(r, "Generic"): return "All regions"
    return "Special"
def cascade_of(r):
    cl, nm = colls(r), disp(r)
    if has_kw(r, "LocalLegend") or any("LocalLegends" in c for c in cl): return "legend"
    if any("WaterLoggedGifts" in c for c in cl) or "Gift" in nm: return "gift"
    if any("Junk" in c for c in cl): return "junk"
    if has_kw(r, "Glowing") or nm.startswith("Glowing"): return "glow"
    if has_kw(r, "Axolotl") or any("Axolotls" in c for c in cl): return "axolotl"
    if any(c.endswith("_Uncommon") or "SeasonalFish" in c for c in cl): return "uncommon"
    if any(c.endswith("_Common") for c in cl): return "common"
    if any("Generic" in c for c in cl): return "generic"
    return None
def image_for(name, cid):
    rel = IMAGE_OVERRIDES.get(name)
    return "%s/%s.avif" % (IMG_BASE, rel) if rel else None  # null -> renderer draws the styled SVG glyph

def load_rollovers():
    for cand in ("seasonal-fish.json", "seasonal_fish_guide.json"):
        p = os.path.join(DIST, cand)
        if os.path.exists(p):
            try:
                d = json.load(open(p, encoding="utf-8"))
            except (ValueError, IOError):
                continue  # skip a malformed candidate (e.g. trailing data) and try the next
            roll = (d.get("seasonRule") or {}).get("rolloverDates")
            if roll: return roll
    return []

def build(tsv_path, month_map=None):
    month_map = month_map or {}
    rows = list(csv.DictReader(open(tsv_path, encoding="utf-8"), delimiter="\t"))
    bucket = {cid: [] for cid, _, _ in CASCADES}
    seen = {cid: set() for cid, _, _ in CASCADES}
    for r in rows:
        nm = disp(r)
        # Skip cut content: Bethesda prefixes deprecated/removed records' EDIDs with
        # "zzz_" (the display name stays clean, so check the EDID too). e.g. the Golden
        # Axolotl was a live PTS record on 2026-06-21 but cut by the 2026-06-27 build.
        if r.get("EDID", "").startswith("zzz"): continue
        if nm.startswith("zzz") or nm.startswith("Fishing_"): continue
        if nm in DROP_NAMES: continue
        cid = cascade_of(r)
        if not cid: continue
        def rec(c):
            if nm in seen[c]: return None
            seen[c].add(nm)
            o = {"name": nm, "size": size_of(r), "maxLengthCm": int(float(r["FIHA"])),
                 "fight": int(float(r["FIHS"])), "region": region_of(r), "image": image_for(nm, c)}
            if nm in SEASON_OF: o["season"] = SEASON_OF[nm]
            if c == "axolotl":
                mo = month_map.get(nm)
                if mo: o["month"] = mo
            return o
        m = rec(cid)
        if m: bucket[cid].append(m)
        # also surface seasonal members in the Seasonal tab
        if nm in SEASON_OF:
            m2 = rec("season")
            if m2:
                bucket["season"].append(m2)
    out = {
        "generated": datetime.datetime.utcnow().isoformat() + "Z",
        "source": os.path.basename(tsv_path),
        "seasonRule": {"rolloverDates": load_rollovers()},
        "cascades": [{"id": cid, "label": lab, "blurb": bl, "fish": bucket[cid]}
                     for cid, lab, bl in CASCADES],
    }
    return out

def main():
    os.makedirs(DIST, exist_ok=True)
    os.makedirs(os.path.join(DIST, "pts"), exist_ok=True)

    # ---- Live feed: newest live FISH export, months from the live axolotl guide ----
    tsv = newest_tsv()
    if not tsv: raise SystemExit("No FISH_Export TSV found in %s" % TSV_DIR)
    live_data = build(tsv, load_axolotl_months("axolotl_guide.json"))
    live = os.path.join(DIST, "fish_journal.json")
    json.dump(live_data, open(live, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    # ---- PTS feed: newest PTS FISH export (includes the Golden Axolotl) ----
    pts_dir = os.path.join(TSV_DIR, "pts")
    pts_tsv = newest_tsv(pts_dir, pts=True)
    pts_months = load_axolotl_months("axolotl_guide_pts.json") or load_axolotl_months("axolotl_guide.json")
    pts_data = build(pts_tsv, pts_months) if pts_tsv else live_data
    pts = os.path.join(DIST, "pts", "fish_journal.json")
    json.dump(pts_data, open(pts, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    data = live_data  # for the summary print below
    tot = sum(len(c["fish"]) for c in data["cascades"])
    print("Source:", data["source"])
    for c in data["cascades"]:
        print("  %-9s %2d" % (c["id"], len(c["fish"])))
    print("Total records:", tot)
    print("Wrote:", live, "and", pts)

if __name__ == "__main__":
    main()
