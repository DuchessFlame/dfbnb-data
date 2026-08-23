#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_curves_json.py  (v1 — curve tables + perk-card cross-ref)

Reads game-data TSVs and outputs enriched JSON consumed by
df-bnb-curves.js for:
    /bnb/curve-tables/          — all curves, filterable by category
    /bnb/curve-tables/perk-cards/ — perk-card specific curves

Outputs:
    dist/curves/index.json
    dist/curves/meta.json
    dist/curves/perk_cards.json
    dist/curves/chunks/<category>/<category>.<n>.json

Cross-references:
    CURV  — curve table records (X/Y point data, Ref columns)
    PCRD  — perk card records (RankPERK FormIDs)
    PERK  — perk records (EffectLinks → SPEL/ENCH FormIDs)

Generative: overwrites dist/curves/ completely on every run.
"""

import csv
import glob
import json
import math
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import tsv_source          # one resolver for every export selection

# ==================================================================
# Paths — run from repo root (dfbnb-data/)
# ==================================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist" / "curves"
CHUNK_MAX_CURVES = 200

# ==================================================================
# Helpers
# ==================================================================

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def name_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)


def newest(pattern):
    """
    Return the newest file matching a glob pattern.
    Prefers files with the latest date in the filename (e.g. March_2026 > Feb_2026).
    Falls back to modification time if no date can be parsed.
    """
    files = glob.glob(str(pattern))
    if not files:
        return None
    files.sort(key=lambda x: (name_date_key(x), os.path.basename(x)))
    return files[-1]


def read_tsv(path):
    """Read a TSV file into a list of dicts, handling BOM and encoding quirks."""
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="latin-1", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


def to_num(v):
    """Convert a value to float, returning None if invalid."""
    try:
        n = float(v)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def normalize_form_id(form_id):
    """Normalize a FormID to uppercase 8-char hex."""
    if not form_id:
        return ""
    s = str(form_id).strip().upper()
    if s.startswith("0X"):
        s = s[2:]
    if re.match(r"^[0-9A-F]+$", s):
        s = s.zfill(8)
    return s


def safe_category_from_json_path(json_path):
    """Extract a category slug from the curve's JsonPath field."""
    if not json_path:
        return "other"
    norm = json_path.replace("\\", "/").lower()
    idx = norm.find("/json/")
    if idx == -1:
        return "other"
    rest = norm[idx + len("/json/"):]
    parts = [p for p in rest.split("/") if p]
    if not parts:
        return "other"
    folder = re.sub(r"[^a-z0-9_-]", "", parts[0])
    return folder or "other"


def is_rejected_edid(edid):
    """Filter out CUT / DEL / ZZZ records."""
    s = str(edid or "").strip()
    if not s:
        return False
    u = s.upper()
    return u.startswith("DEL") or u.startswith("CUT") or u.startswith("ZZZ")


def extract_form_ids_from_ref(ref_text):
    """
    Extract FormIDs from various reference text formats:
      Format A: '0089EA90:Something:GLOB'
      Format B: 'Name [GLOB:0085AD24]'
      Format C: 'SPEL:AbPerkFoo[007ACE71]'  (bare FormID in brackets)
    Returns de-duped list of uppercase FormIDs.
    """
    s = str(ref_text or "").strip()
    if not s:
        return []

    out = set()

    # Format A: starts with 8 hex chars + colon
    m = re.match(r"^([0-9A-Fa-f]{8}):", s)
    if m:
        out.add(m.group(1).upper())

    # Format B: [TYPE:FormID]
    for m in re.finditer(r"\[[A-Z0-9_]+:([0-9A-Fa-f]{8})\]", s):
        out.add(m.group(1).upper())

    # Format C: bare [FormID] (8 hex in brackets)
    for m in re.finditer(r"\[([0-9A-Fa-f]{8})\]", s):
        out.add(m.group(1).upper())

    return list(out)


def clamp_min_max(curve):
    """Compute xMin/xMax/yMin/yMax for a curve from its points."""
    pts = curve["points"]
    if not pts:
        curve.update({"xMin": 0, "xMax": 0, "yMin": 0, "yMax": 0})
        return curve
    xs = [p["x"] for p in pts]
    ys = [p["y"] for p in pts]
    curve["xMin"] = min(xs)
    curve["xMax"] = max(xs)
    curve["yMin"] = min(ys)
    curve["yMax"] = max(ys)
    return curve


# ==================================================================
# Title-case category mapping
# ==================================================================

CATEGORY_TITLES = {
    "legendaryperks": "Legendary Perks",
    "legendarymods": "Legendary Mods",
    "itemcondition": "Item Condition",
    "encounterwave": "Encounter Wave",
    "perkcardpacks": "Perk Card Packs",
    "fasttravelcostcurvedistancejson": "Fast Travel Cost (Distance)",
    "fasttravelcostmultcurvejson": "Fast Travel Cost (Multiplier)",
    "fasttraveloverencumberedcostmultcurvejson": "Fast Travel (Over-Encumbered)",
    "movecampcostcurvejson": "Move Camp Cost",
    "xp_curvejson": "XP Curve",
}


def title_case_category(cat_id):
    if cat_id in CATEGORY_TITLES:
        return CATEGORY_TITLES[cat_id]
    clean = re.sub(r"json$", "", cat_id, flags=re.IGNORECASE)
    words = re.split(r"[-_]", clean)
    return " ".join(w.capitalize() for w in words if w)


# ==================================================================
# Description generator
# ==================================================================

METRIC_WORDS = [
    "BaseDMG", "DMG", "Damage", "DR", "ER", "Health", "HP",
    "Bonus", "Scale", "Mult", "Cost", "Rate", "Speed", "Weight",
    "Chance", "Duration", "Radius", "Range", "Regen", "Resist",
    "Armor", "AP", "Stagger", "Bleed", "Reload", "Bash", "DPS",
    "Tier", "Level", "Count", "Offset", "Min", "Max",
]


def parse_edid(edid):
    s = str(edid or "").strip()
    if not s:
        return {"raw": s, "subject": "", "metric": "", "prefix": ""}

    work = s
    prefix = ""
    pm = re.match(r"^(CT_|cr|ab|Ab|Ench|ench|Weap_|PA_)", work)
    if pm:
        prefix = pm.group(1)
        work = work[len(prefix):]

    # Split on underscores and CamelCase
    work2 = re.sub(r"([a-z])([A-Z])", r"\1_\2", work)
    work2 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", work2)
    parts = [p for p in work2.split("_") if p]

    metric = ""
    subject = " ".join(parts)

    for i in range(len(parts) - 1, -1, -1):
        p = parts[i].lower()
        if any(m.lower() in p for m in METRIC_WORDS):
            metric = " ".join(parts[i:])
            subject = " ".join(parts[:i])
            break

    return {"raw": s, "subject": subject or work, "metric": metric, "prefix": prefix}


def humanize(s):
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"(\d+)", r" \1 ", s)
    return re.sub(r"\s+", " ", s).strip()


def infer_x_label(category, x_min, x_max):
    if category == "special":
        return "SPECIAL Stat Value"
    if category == "perkcardpacks":
        return "Player Level"
    if category == "legendaryperks":
        return "Legendary Perk Rank"
    if x_max <= 5 and x_min >= 0:
        return "Rank"
    if x_max <= 10 and x_min >= 1:
        return "Star Rating / Rank"
    if x_max <= 15 and x_min >= 1:
        return "SPECIAL Stat Value"
    if x_max <= 500:
        return "Player Level"
    return "Input Value"


def infer_y_label(category, edid, y_min, y_max):
    e = (edid or "").lower()
    if re.search(r"basedmg|_dmg|damage", e, re.I):
        return "Base Damage"
    if re.search(r"\bdr\b|damageresist", e, re.I):
        return "Damage Resistance"
    if re.search(r"\ber\b|energyresist", e, re.I):
        return "Energy Resistance"
    if re.search(r"health|hp\b", e, re.I):
        return "Health"
    if re.search(r"armor(?!ed)", e, re.I) and not re.search(r"bonus", e, re.I):
        return "Armor Value"
    if re.search(r"regen|aprecovery", e, re.I):
        return "Regeneration Rate"
    if re.search(r"reload", e, re.I):
        return "Reload Speed Modifier"
    if re.search(r"bash", e, re.I):
        return "Bash Damage"
    if re.search(r"stagger", e, re.I):
        return "Stagger Chance"
    if re.search(r"bleed", e, re.I):
        return "Bleed Damage"
    if re.search(r"chance", e, re.I):
        return "Chance (%)"
    if re.search(r"duration", e, re.I):
        return "Duration (seconds)"
    if re.search(r"cost", e, re.I):
        return "Cost (Caps)"
    if re.search(r"xp|experience", e, re.I):
        return "XP Required"
    if re.search(r"weight", e, re.I):
        return "Weight"
    if re.search(r"speed", e, re.I):
        return "Speed Modifier"
    if re.search(r"resist", e, re.I):
        return "Resistance Value"

    if y_min >= 0 and y_max <= 1.5:
        return "Multiplier"
    if y_min >= 0 and y_max <= 100 and category != "creatures":
        return "Bonus Value"

    cat_defaults = {
        "weapons": "Damage / Stat Value",
        "armor": "Armor / Resistance Value",
        "creatures": "Stat Value",
        "player": "Player Stat Value",
        "perks": "Perk Bonus",
        "spells": "Effect Magnitude",
        "enchantments": "Enchantment Value",
        "legendary": "Legendary Bonus",
        "legendarymods": "Mod Value",
        "itemcondition": "Condition Factor",
        "bobbleheads": "Bobblehead Bonus",
        "mutations": "Mutation Effect",
        "econ": "Economy Value",
        "crafting": "Crafting Multiplier",
        "cobj": "Construction Value",
        "workshop": "Workshop Value",
        "brewing": "Brewing Effect",
        "encounterwave": "Encounter Value",
        "vendors": "Vendor Value",
    }
    return cat_defaults.get(category, "Output Value")


KNOWN_DESCS = {
    "xp_curvejson": "XP required to reach each player level.",
    "fasttravelcostcurvedistancejson": "Fast travel caps cost based on distance traveled.",
    "fasttravelcostmultcurvejson": "Multiplier applied to fast travel cost.",
    "fasttraveloverencumberedcostmultcurvejson": "Extra fast travel cost when over-encumbered.",
    "movecampcostcurvejson": "Caps cost to move your C.A.M.P. based on distance.",
}


def generate_description(edid, category, x_min, x_max, y_min, y_max):
    parsed = parse_edid(edid)
    subj = humanize(parsed["subject"])
    cat = category or "other"

    if cat in KNOWN_DESCS:
        return KNOWN_DESCS[cat]

    if cat == "perks" and re.search(r"bonus", edid or "", re.I):
        perk_name = re.sub(r"\s*Bonus.*$", "", subj, flags=re.I).strip()
        if y_max <= 1.5 and y_min >= 0:
            return f"{perk_name} perk card bonus multiplier, scaling by player level. A value of 1.0 means no change; higher means a stronger effect."
        return f"{perk_name} perk card bonus value, scaling by player level."

    if cat == "weapons":
        if re.search(r"basedmg|_dmg", edid or "", re.I):
            return f"Base damage for {subj}, scaling by player level. Higher levels deal more damage."
        return f"{subj} weapon stat, scaling by level."

    if cat == "armor":
        if re.search(r"\bdr\b", edid or "", re.I):
            return f"Damage resistance for {subj} armor, scaling by level."
        if re.search(r"\ber\b", edid or "", re.I):
            return f"Energy resistance for {subj} armor, scaling by level."
        if re.search(r"health", edid or "", re.I):
            return f"Health/durability for {subj} armor, scaling by level."
        return f"{subj} armor stat, scaling by level."

    if cat == "creatures":
        tier_m = re.search(r"tier\s*(\d+)", edid or "", re.I)
        tier_str = f" (Tier {tier_m.group(1)})" if tier_m else ""
        if re.search(r"armor", edid or "", re.I):
            return f"Creature armor value{tier_str}, scaling by creature level."
        if re.search(r"health|hp", edid or "", re.I):
            return f"Creature health{tier_str}, scaling by creature level."
        if re.search(r"dmg|damage", edid or "", re.I):
            return f"Creature damage{tier_str}, scaling by creature level."
        return f"Creature stat{tier_str} for {subj}, scaling by level."

    if cat == "player":
        return f"Player {subj.lower()} stat, scaling by level."
    if cat == "itemcondition":
        return f"{subj} condition/durability factor, scaling by item level."
    if cat == "legendarymods":
        return f"Legendary mod: {subj} effect value, scaling by star rating."
    if cat == "legendary":
        return f"Legendary {subj.lower()} value, scaling by legendary rank."
    if cat == "spells":
        return f"Spell effect: {subj}, scaling by caster level."
    if cat == "enchantments":
        return f"Enchantment: {subj} effect magnitude, scaling by level."
    if cat == "encounterwave":
        return f"Encounter wave: {subj} scaling by player level or zone."
    if cat == "bobbleheads":
        return f"Bobblehead {subj.lower()} bonus, scaling by player level."
    if cat == "mutations":
        return f"Mutation: {subj} effect value."
    if cat == "econ":
        return f"Economy: {subj} value, scaling by level or rank."
    if cat in ("crafting", "cobj"):
        return f"Crafting: {subj} scaling value."
    if cat == "workshop":
        return f"Workshop: {subj} value, scaling by level."
    if cat == "brewing":
        return f"Brewing: {subj} effect value."
    if cat == "perkcardpacks":
        return f"Perk card pack: {subj} probability or count, by player level."
    if cat == "vendors":
        return f"Vendor: {subj} value."

    return f"{subj} curve table value, scaling by input."


# ==================================================================
# Soft cap detection
# ==================================================================

def detect_soft_cap(points):
    """
    Detect soft cap: the X value where Y gains start diminishing significantly.
    Returns {x, y, ratio} or None.
    """
    if not points or len(points) < 4:
        return None

    x_range = points[-1]["x"] - points[0]["x"]
    y_range = abs(points[-1]["y"] - points[0]["y"])
    if x_range <= 0 or y_range < 0.001:
        return None

    slopes = []
    for i in range(1, len(points)):
        dx = points[i]["x"] - points[i - 1]["x"]
        if dx <= 0:
            continue
        dy = points[i]["y"] - points[i - 1]["y"]
        slopes.append({
            "x": (points[i]["x"] + points[i - 1]["x"]) / 2,
            "slope": dy / dx,
            "idx": i,
        })

    if len(slopes) < 3:
        return None

    abs_slopes = [abs(s["slope"]) for s in slopes]

    half_idx = len(slopes) // 2
    peak_slope = max(abs_slopes[:half_idx + 1]) if half_idx >= 0 else 0
    if peak_slope < 0.0001:
        return None

    threshold = peak_slope * 0.25
    sustain_threshold = peak_slope * 0.35

    for i in range(1, len(slopes) - 1):
        if abs_slopes[i] < threshold:
            stays_low = True
            look_ahead = min(i + 3, len(slopes))
            for j in range(i + 1, look_ahead):
                if abs_slopes[j] > sustain_threshold:
                    stays_low = False
                    break
            if stays_low:
                sc_idx = slopes[i]["idx"]
                return {
                    "x": points[sc_idx]["x"],
                    "y": points[sc_idx]["y"],
                    "ratio": abs_slopes[i] / peak_slope,
                }

    return None


# ==================================================================
# Display table (pre-calculated interpolated values)
# ==================================================================

def build_display_table(points, soft_cap):
    if not points:
        return []

    x_min = points[0]["x"]
    x_max = points[-1]["x"]
    rng = x_max - x_min

    if rng <= 0:
        return [{"x": x_min, "y": points[0]["y"]}]

    # Step size
    if rng <= 100:
        step = 1
    elif rng <= 500:
        step = 5
    elif rng <= 1000:
        step = 10
    elif rng <= 5000:
        step = 50
    else:
        step = 500

    x_values = set()
    x = math.ceil(x_min)
    while x <= x_max:
        x_values.add(x)
        x += step

    x_values.add(x_min)
    x_values.add(x_max)
    for p in points:
        x_values.add(p["x"])
    if soft_cap:
        x_values.add(soft_cap["x"])

    sorted_x = sorted(x_values)

    # Linear interpolation
    table = []
    for xv in sorted_x:
        if xv <= points[0]["x"]:
            y = points[0]["y"]
        elif xv >= points[-1]["x"]:
            y = points[-1]["y"]
        else:
            lo, hi = 0, len(points) - 1
            for i in range(len(points) - 1):
                if points[i]["x"] <= xv <= points[i + 1]["x"]:
                    lo, hi = i, i + 1
                    break
            dx = points[hi]["x"] - points[lo]["x"]
            if dx == 0:
                y = points[lo]["y"]
            else:
                t = (xv - points[lo]["x"]) / dx
                y = points[lo]["y"] + t * (points[hi]["y"] - points[lo]["y"])

        table.append({"x": xv, "y": round(y, 4)})

    # Cap at 500 rows
    if len(table) > 500:
        table_step = max(1, len(table) // 500)
        resampled = [table[i] for i in range(0, len(table), table_step)]
        if resampled[-1]["x"] != table[-1]["x"]:
            resampled.append(table[-1])
        return resampled

    return table


# ==================================================================
# Enrichment
# ==================================================================

def enrich_curve(curve):
    """Add description, labels, soft cap, and display table to a curve."""
    curve["desc"] = generate_description(
        curve["edid"], curve["category"],
        curve["xMin"], curve["xMax"],
        curve["yMin"], curve["yMax"],
    )
    curve["xLabel"] = infer_x_label(curve["category"], curve["xMin"], curve["xMax"])
    curve["yLabel"] = infer_y_label(curve["category"], curve["edid"], curve["yMin"], curve["yMax"])
    soft_cap = detect_soft_cap(curve["points"])
    curve["displayTable"] = build_display_table(curve["points"], soft_cap)
    if soft_cap:
        curve["softCap"] = {"x": soft_cap["x"], "y": round(soft_cap["y"], 4)}
    return curve


# ==================================================================
# Write JSON helper
# ==================================================================

def write_json(file_path, data):
    p = Path(file_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ==================================================================
# Main build
# ==================================================================

def build():
    print("[curves] Starting curve tables build...")

    # ---- Locate TSVs ----
    curv_points_tsv = newest(str(TSV_DIR / "CURV_Export_*_POINTS.tsv"))
    # Header TSV should NOT be the POINTS one — use a filtered glob
    _curv_all = glob.glob(str(TSV_DIR / "CURV_Export_*.tsv"))
    _curv_hdr_candidates = [f for f in _curv_all if "_POINTS" not in f]
    if _curv_hdr_candidates:
        _curv_hdr_candidates.sort(key=lambda x: (name_date_key(x), os.path.basename(x)))
        curv_hdr_tsv = _curv_hdr_candidates[-1]
    else:
        curv_hdr_tsv = None

    pcrd_tsv = newest(str(TSV_DIR / "PCRD_Export_*.tsv"))
    perk_tsv = newest(str(TSV_DIR / "PERK_Export_*.tsv"))

    if not curv_points_tsv:
        print("[curves] ERROR: No CURV_Export_*_POINTS.tsv found in tsv/")
        return

    print(f"  CURV points: {os.path.basename(curv_points_tsv)}")
    print(f"  CURV header: {os.path.basename(curv_hdr_tsv) if curv_hdr_tsv else 'MISSING'}")
    print(f"  PCRD:        {os.path.basename(pcrd_tsv) if pcrd_tsv else 'MISSING'}")
    print(f"  PERK:        {os.path.basename(perk_tsv) if perk_tsv else 'MISSING'}")

    # ---- Wipe dist/curves/ for clean regeneration ----
    if DIST_DIR.exists():
        print(f"  Cleaning {DIST_DIR} ...")
        try:
            shutil.rmtree(DIST_DIR)
        except (PermissionError, OSError):
            # On some systems (mounted folders, etc.) rmtree can fail.
            # Fall back to removing individual files then dirs.
            for root, dirs, files in os.walk(DIST_DIR, topdown=False):
                for fn in files:
                    try:
                        os.remove(os.path.join(root, fn))
                    except OSError:
                        pass
                for dn in dirs:
                    try:
                        os.rmdir(os.path.join(root, dn))
                    except OSError:
                        pass
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Parse CURV points ----
    print("[curves] Parsing CURV points...")
    rows = read_tsv(curv_points_tsv)
    print(f"  {len(rows)} point rows")

    curves_map = {}
    for r in rows:
        form_id = normalize_form_id(r.get("FormID") or r.get("formid") or r.get("formId") or "")
        edid = (r.get("EDID") or r.get("edid") or "").strip()
        x = to_num(r.get("X") or r.get("x"))
        y = to_num(r.get("Y") or r.get("y"))
        json_path = (r.get("JsonPath") or r.get("jsonpath") or r.get("Path") or "").strip()

        if not form_id or x is None or y is None:
            continue
        if is_rejected_edid(edid):
            continue

        if form_id not in curves_map:
            curves_map[form_id] = {
                "id": form_id,
                "edid": edid,
                "jsonPath": json_path,
                "category": safe_category_from_json_path(json_path),
                "points": [],
            }

        c = curves_map[form_id]
        if not c["edid"] and edid:
            c["edid"] = edid
        if not c["jsonPath"] and json_path:
            c["jsonPath"] = json_path
        c["points"].append({"x": x, "y": y})

    # Sort points, compute min/max
    curves = []
    for c in curves_map.values():
        c["points"].sort(key=lambda p: (p["x"], p["y"]))
        c["pointsCount"] = len(c["points"])
        clamp_min_max(c)
        curves.append(c)

    curves.sort(key=lambda c: (c["category"], c.get("edid", ""), c["id"]))

    # Enrich all curves
    print("[curves] Enriching curves (descriptions, soft caps, display tables)...")
    for c in curves:
        enrich_curve(c)

    # ---- Build index stubs ----
    index_curves = []
    for c in curves:
        stub = {
            "id": c["id"],
            "edid": c["edid"],
            "category": c["category"],
            "points": c["pointsCount"],
            "xMin": c["xMin"], "xMax": c["xMax"],
            "yMin": c["yMin"], "yMax": c["yMax"],
            "desc": c.get("desc", ""),
            "xLabel": c.get("xLabel", ""),
            "yLabel": c.get("yLabel", ""),
        }
        if "softCap" in c:
            stub["softCap"] = c["softCap"]
        index_curves.append(stub)

    # ---- Categories ----
    cat_counts = defaultdict(int)
    for c in index_curves:
        cat_counts[c["category"]] += 1

    categories = sorted(
        [{"id": cid, "title": title_case_category(cid), "count": cnt}
         for cid, cnt in cat_counts.items()],
        key=lambda x: x["id"],
    )

    # ---- Write chunks ----
    print("[curves] Writing chunks...")
    chunks_root = DIST_DIR / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)

    by_cat = defaultdict(list)
    for c in curves:
        by_cat[c["category"]].append(c)

    chunk_index = {}
    for cat, cat_curves in sorted(by_cat.items()):
        cat_dir = chunks_root / cat
        cat_dir.mkdir(parents=True, exist_ok=True)

        files = []
        chunk_num = 0

        for i in range(0, len(cat_curves), CHUNK_MAX_CURVES):
            chunk_slice = cat_curves[i:i + CHUNK_MAX_CURVES]
            chunk_data = []
            for c in chunk_slice:
                entry = {
                    "id": c["id"],
                    "edid": c["edid"],
                    "category": c["category"],
                    "xMin": c["xMin"], "xMax": c["xMax"],
                    "yMin": c["yMin"], "yMax": c["yMax"],
                    "desc": c.get("desc", ""),
                    "xLabel": c.get("xLabel", ""),
                    "yLabel": c.get("yLabel", ""),
                    "points": c["points"],
                    "displayTable": c.get("displayTable", []),
                }
                if "softCap" in c:
                    entry["softCap"] = c["softCap"]
                chunk_data.append(entry)

            file_name = f"{cat}.{chunk_num}.json"
            write_json(cat_dir / file_name, {
                "category": cat,
                "chunk": chunk_num,
                "count": len(chunk_data),
                "curves": chunk_data,
            })
            files.append(f"chunks/{cat}/{file_name}")
            chunk_num += 1

        chunk_index[cat] = files

    # ---- Meta ----
    total_points = sum(c["pointsCount"] for c in curves)
    meta = {
        "buildTimeUTC": datetime.now(timezone.utc).isoformat(),
        "input": os.path.basename(curv_points_tsv),
        "curves": len(index_curves),
        "points": total_points,
        "chunkMaxCurves": CHUNK_MAX_CURVES,
    }

    write_json(DIST_DIR / "meta.json", meta)
    write_json(DIST_DIR / "index.json", {
        "meta": meta,
        "categories": categories,
        "chunks": chunk_index,
        "curves": index_curves,
    })

    print(f"[curves] index.json: {len(index_curves)} curves, {len(categories)} categories")

    # ==============================================================
    # PERK CARDS INDEX
    # Chain: CURV refs → SPEL/PERK/ENCH → PERK EffectLinks → PCRD
    # ==============================================================

    if curv_hdr_tsv and pcrd_tsv and perk_tsv:
        print("[curves] Building perk_cards.json...")

        pcrd_rows = read_tsv(pcrd_tsv)
        perk_rows = read_tsv(perk_tsv)
        curv_hdr_rows = read_tsv(curv_hdr_tsv)

        print(f"  PCRD: {len(pcrd_rows)} rows")
        print(f"  PERK: {len(perk_rows)} rows")
        print(f"  CURV hdr: {len(curv_hdr_rows)} rows")

        curve_stub_by_id = {c["id"]: c for c in index_curves}

        # Step 1: refFormId → Set<CURV_FormID> from CURV header Ref columns
        ref_to_curvs = defaultdict(set)
        for r in curv_hdr_rows:
            curv_id = normalize_form_id(r.get("CURV_FormID", ""))
            if not curv_id or curv_id not in curve_stub_by_id:
                continue
            for key, val in r.items():
                if not re.match(r"^Ref\d+$", key):
                    continue
                v = str(val or "").strip()
                if not v:
                    continue
                ids = extract_form_ids_from_ref(v)
                for ref_id in ids:
                    ref_to_curvs[ref_id].add(curv_id)

        print(f"  CURV ref index: {len(ref_to_curvs)} unique referencing FormIDs")

        # Step 2: PERK_FormID → Set<EffectLink FormIDs>
        perk_to_links = {}
        for r in perk_rows:
            perk_id = normalize_form_id(r.get("PERK_FormID", ""))
            if not perk_id:
                continue

            link_ids = set()
            for i in range(1, 31):
                el = str(r.get(f"EffectLink_{i}", "") or "").strip()
                if not el:
                    continue
                ids = extract_form_ids_from_ref(el)
                link_ids.update(ids)

            # Also grab Spell_FormID and CurveTable_FormID
            spell_id = normalize_form_id(r.get("Spell_FormID", ""))
            if spell_id:
                link_ids.add(spell_id)
            ct_id = normalize_form_id(r.get("CurveTable_FormID", ""))
            if ct_id:
                link_ids.add(ct_id)

            if link_ids:
                perk_to_links[perk_id] = link_ids

        print(f"  PERK link index: {len(perk_to_links)} perks with links")

        # Step 3: PCRD → PERKs → links → CURVs
        perk_groups = []
        for r in pcrd_rows:
            pcrd_fid = normalize_form_id(r.get("PCRD_FormID", ""))
            pcrd_edid = str(r.get("PCRD_EDID", "") or "").strip()
            pcrd_name = str(r.get("MNAM_Name", "") or r.get("PCRD_EDID", "") or "").strip()

            if not pcrd_fid:
                continue

            rank_perk_ids = set()
            for i in range(1, 13):
                pid = normalize_form_id(r.get(f"RankPERK_{i}_FormID", ""))
                if pid:
                    rank_perk_ids.add(pid)

            curve_ids_set = set()
            for perk_id in rank_perk_ids:
                # Direct CURV refs to this PERK
                direct = ref_to_curvs.get(perk_id, set())
                curve_ids_set.update(direct)

                # Via EffectLinks
                links = perk_to_links.get(perk_id, set())
                for link_id in links:
                    linked_curvs = ref_to_curvs.get(link_id, set())
                    curve_ids_set.update(linked_curvs)

            if not curve_ids_set:
                continue

            perk_curves = sorted(
                [curve_stub_by_id[cid] for cid in curve_ids_set if cid in curve_stub_by_id],
                key=lambda c: (c.get("edid", ""), c["id"]),
            )

            perk_groups.append({
                "pcrdFormId": pcrd_fid,
                "pcrdEdid": pcrd_edid,
                "name": pcrd_name or pcrd_edid or pcrd_fid,
                "curves": perk_curves,
            })

        perk_groups.sort(key=lambda g: g.get("name", ""))

        # Limit chunk refs to used categories
        used_cats = set()
        for g in perk_groups:
            for c in g["curves"]:
                used_cats.add(c["category"])

        perk_chunks = {cat: chunk_index.get(cat, []) for cat in used_cats}

        write_json(DIST_DIR / "perk_cards.json", {
            "meta": meta,
            "perks": perk_groups,
            "chunks": perk_chunks,
        })

        print(f"[curves] perk_cards.json: {len(perk_groups)} perks, {len(used_cats)} chunk categories")
    else:
        missing = []
        if not curv_hdr_tsv:
            missing.append("CURV header")
        if not pcrd_tsv:
            missing.append("PCRD")
        if not perk_tsv:
            missing.append("PERK")
        print(f"[curves] perk_cards.json skipped (missing: {', '.join(missing)})")

    # ---- Summary ----
    print()
    print(f"[curves] BUILD COMPLETE")
    print(f"  curves: {meta['curves']}")
    print(f"  points: {meta['points']}")
    print(f"  output: {DIST_DIR}")
    print(f"  chunks: {sum(len(v) for v in chunk_index.values())} files")


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    build()
