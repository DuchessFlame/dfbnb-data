#!/usr/bin/env python3
"""
build_perk_cards_json.py
========================
Builds the data for the Perk Cards curve-tables page:

    dist/curves/perk_cards.json       (LIVE channel)
    dist/curves/perk_cards.pts.json   (PTS channel)

Each output is the FULL perk-card roster (every PCRD record), classified and
enriched so the website renderer (df-bnb-curve-tables.js, perk-cards branch)
can draw an expand per card with a curve chart + value table, or a
"no curve table" note when a card has no backing curve.

DATA SOURCES (all already in the repo)
--------------------------------------
  tsv/PCRD_Export_*.tsv            roster: name, SPECIAL, race restriction
  tsv/CURV_Export_*_POINTS.tsv     curve points (x,y) by curve EDID
  dist/curves/index.json           per-curve labels/desc/min-max (from build_curves_json.py)
  dist/curves/perk_cards.json      EXISTING file = correct PCRD->curve linkage
                                   (produced by build_curves_json.py's PCRD->PERK->CURV chain)
  data/perk_cards_meta.json        hand overrides: former names, explain, axis labels

  PTS channel reads the same file types from tsv/pts/ . Curve POINTS are
  LIVE-only today (no PTS CURV export yet), so the PTS build reuses the LIVE
  curve linkage/points and flags isPtsFallback=true while still using the PTS
  roster + PTS SPECIAL/race classification.

CLASSIFICATION RULES (confirmed with the site owner)
----------------------------------------------------
  Section   : EDID prefix  GHL_ -> Ghoul, LGN_ -> Legendary, else Human.
  SPECIAL   : DATA_Special column (Strength..Luck / Unknown).
  HumanOnly : DATA_RaceRestriction == "Human"  -> renders a "Human Only" pill.
              "None" works on humans AND ghouls (no pill); "Ghoul" is a ghoul card.
  Ordering  : Legendary = A-Z. Human / Ghoul = SPECIAL then A-Z.

Usage:
  python build_perk_cards_json.py                 # builds both channels
  python build_perk_cards_json.py --channel live
  python build_perk_cards_json.py --channel pts
"""

import argparse, csv, glob, json, os, re, sys
from pathlib import Path
import tsv_source          # one resolver for every export selection

SRC_DIR    = Path(__file__).resolve().parent
REPO_ROOT  = SRC_DIR.parent
TSV_DIR    = REPO_ROOT / "tsv"
PTS_DIR    = TSV_DIR / "pts"
DIST_DIR   = REPO_ROOT / "dist" / "curves"
META_PATH  = REPO_ROOT / "data" / "perk_cards_meta.json"

SECTION_ORDER = ["Legendary Perk Cards", "Human Perk Cards", "Ghoul Perk Cards"]
SPECIAL_ORDER = ["Strength", "Perception", "Endurance",
                 "Charisma", "Intelligence", "Agility", "Luck", "Unknown"]
MAX_TABLE_ROWS = 200

def strip_quotes(s):
    return str(s or "").strip().strip('"').strip()

def is_cut(edid):
    u = strip_quotes(edid).upper()
    return u.startswith("ZZZ") or u.startswith("CUT") or u.startswith("DEL") or u.startswith("ZZZ_")

def to_num(v):
    try: return float(str(v).strip())
    except (TypeError, ValueError): return None

def newest(pattern):
    return tsv_source.newest(pattern, required=False)

def read_tsv(path):
    with open(path, "r", encoding="latin-1", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def col(row, *names):
    for n in names:
        if n in row and str(row[n]).strip() != "":
            return str(row[n]).strip()
    return ""

def section_for(edid):
    u = strip_quotes(edid).upper()
    # Legendary takes priority: ghoul-legendary perks (GHL_LGN_*) are Legendary
    # cards, not Ghoul cards, so check for the LGN marker before the GHL prefix.
    if u.startswith("LGN_") or "_LGN_" in u: return "Legendary Perk Cards"
    if u.startswith("GHL_"): return "Ghoul Perk Cards"
    return "Human Perk Cards"

def normalize_special(s):
    s = strip_quotes(s).title()
    return s if s in SPECIAL_ORDER else "Unknown"

def humanize_name(s):
    s = strip_quotes(s)
    if not s or " " in s: return s
    s = re.sub(r"^(LGN|GHL)_?", "", s)
    s = re.sub(r"Card$", "", s)
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    s = s.replace("_", " ")
    return re.sub(r"\s+", " ", s).strip() or strip_quotes(s)

def norm_name(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())

def resolve_card_name(row, edid, meta_card):
    """Return (display_name, former).

    Display name = the card's CURRENT in-game perk name (Rank 1 perk FULL) when
    present, else the humanized card record name (MNAM). `former` = the humanized
    record name, but ONLY when it genuinely differs from the current name
    (punctuation/case differences are ignored, so "Can Do" -> "Can Do!" is not a
    rename). A hand override in perk_cards_meta.json (displayName / former) always
    wins. This is what auto-detects renamed cards (e.g. Archer -> Hat Trick)
    straight from the PCRD export, instead of hand-maintaining each one.
    """
    record = humanize_name(col(row, "MNAM_MaleName", "MNAM_Name") or edid)
    current_raw = strip_quotes(col(row, "RankPERK_1_FULL", "Rank_1_MalePerk_FULL"))
    current = current_raw or record
    display = meta_card.get("displayName") or current
    if meta_card.get("former"):
        former = meta_card["former"]
    elif current_raw and norm_name(record) != norm_name(current):
        former = record
    else:
        former = ""
    return display, former

def interp_y(points, x):
    if x <= points[0][0]: return points[0][1]
    if x >= points[-1][0]: return points[-1][1]
    for i in range(len(points) - 1):
        ax, ay = points[i]; bx, by = points[i + 1]
        if ax <= x <= bx:
            if bx == ax: return ay
            return ay + (by - ay) * (x - ax) / (bx - ax)
    return points[-1][1]

def build_table(points):
    if not points: return []
    xs = [p[0] for p in points]
    x_min, x_max = int(round(min(xs))), int(round(max(xs)))
    if x_max <= x_min: return [{"x": x_min, "y": round(points[0][1], 6)}]
    span = x_max - x_min
    step = max(1, (span + MAX_TABLE_ROWS - 1) // MAX_TABLE_ROWS)
    out = []; x = x_min
    while x <= x_max:
        out.append({"x": x, "y": round(interp_y(points, x), 6)}); x += step
    if out[-1]["x"] != x_max:
        out.append({"x": x_max, "y": round(interp_y(points, x_max), 6)})
    return out

def default_unit_decimals(y_min, y_max):
    if y_max is not None and y_max <= 1.5 and (y_min is None or y_min >= 0): return "x", 3
    return "%", 2

def default_explain(name, y_min, y_max):
    return f"The {name} perk's value scales with its input, ranging from {y_min} to {y_max}."

def load_points_by_edid(points_tsv):
    out = {}
    if not points_tsv: return out
    for r in read_tsv(points_tsv):
        edid = (r.get("EDID") or r.get("edid") or "").strip()
        fid  = (r.get("FormID") or r.get("formid") or "").strip().upper().replace("0X", "")
        x = to_num(r.get("X") or r.get("x")); y = to_num(r.get("Y") or r.get("y"))
        if not edid or x is None or y is None or is_cut(edid): continue
        d = out.setdefault(edid, {"id": fid[-8:] if fid else "", "points": []})
        d["points"].append((x, y))
    for d in out.values(): d["points"].sort(key=lambda p: (p[0], p[1]))
    return out

def load_curve_labels(index_path):
    out = {}
    if not index_path or not os.path.exists(index_path): return out
    idx = json.load(open(index_path, encoding="utf-8"))
    for c in idx.get("curves", []): out[c.get("edid", "")] = c
    return out

def load_linkage(perk_cards_path):
    out = {}
    if not perk_cards_path or not os.path.exists(perk_cards_path): return out
    try: existing = json.load(open(perk_cards_path, encoding="utf-8"))
    except (json.JSONDecodeError, OSError): return out
    for p in existing.get("perks", []):
        key = strip_quotes(p.get("pcrdEdid") or p.get("name"))
        edids = [c.get("edid") for c in p.get("curves", []) if c.get("edid")]
        if key: out[key] = edids
    return out

def load_meta():
    if META_PATH.exists():
        try: return json.load(open(META_PATH, encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"  WARN: {META_PATH} is not valid JSON ({e}); ignoring overrides.")
    return {}

def make_curve(curve_edid, points_by_edid, labels, meta_curves):
    pdata = points_by_edid.get(curve_edid)
    if not pdata or not pdata["points"]: return None
    lab = labels.get(curve_edid, {}); pts = pdata["points"]
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
    x_min, x_max = min(xs), max(xs); y_min, y_max = min(ys), max(ys)
    unit, decimals = default_unit_decimals(y_min, y_max)
    over = (meta_curves or {}).get(curve_edid, {})
    def num(v): return int(v) if isinstance(v, float) and v.is_integer() else v
    return {
        "id": over.get("id") or lab.get("id") or pdata["id"], "edid": curve_edid,
        "explain": over.get("explain") or lab.get("desc") or default_explain(curve_edid, num(y_min), num(y_max)),
        "xLabel": over.get("xLabel") or lab.get("xLabel") or "Input",
        "yLabel": over.get("yLabel") or lab.get("yLabel") or "Value",
        "unit": over.get("unit", unit), "decimals": over.get("decimals", decimals),
        "xMin": num(x_min), "xMax": num(x_max), "yMin": num(y_min), "yMax": num(y_max),
        "table": build_table(pts),
    }

def load_class_index(*pcrd_paths):
    idx = {}
    for p in pcrd_paths:
        if not p or not os.path.exists(p): continue
        for r in read_tsv(p):
            edid = strip_quotes(col(r, "PCRD_EDID"))
            sp = col(r, "DATA_Special")
            rc = col(r, "DATA_RaceRestriction", "DATA_SpeciesText", "DATA_Species")
            if edid and sp and edid not in idx:
                idx[edid] = {"special": sp, "race": rc or "None"}
    return idx

def build_channel(channel, pcrd_tsv, points_by_edid, labels, linkage, meta,
                  pts_fallback=False, class_index=None):
    class_index = class_index or {}
    pcrd_rows = read_tsv(pcrd_tsv); perks = []
    counts = {"with_curve": 0, "no_curve": 0, "cut": 0}
    for r in pcrd_rows:
        edid = col(r, "PCRD_EDID")
        if not edid or is_cut(edid): counts["cut"] += 1; continue
        key = strip_quotes(edid)
        special = normalize_special(col(r, "DATA_Special"))
        race = col(r, "DATA_RaceRestriction", "DATA_SpeciesText", "DATA_Species") or "None"
        if special == "Unknown" and key in class_index:
            special = normalize_special(class_index[key]["special"])
            if race in ("", "None"): race = class_index[key]["race"]
        section = section_for(edid); meta_card = meta.get(key, {})
        disp_name, former_name = resolve_card_name(r, key, meta_card)
        curve_edids = list(meta_card.get("curveEdids") or linkage.get(key, []))
        curves = []
        for ce in curve_edids:
            cobj = make_curve(ce, points_by_edid, labels, meta_card.get("curves"))
            if cobj: curves.append(cobj)
        if curves: counts["with_curve"] += 1
        else: counts["no_curve"] += 1
        perks.append({
            "pcrdFormId": col(r, "PCRD_FormID"), "pcrdEdid": key,
            "name": disp_name, "former": former_name,
            "section": section, "special": special, "minLevel": col(r, "DATA_MinLevel"),
            "humanOnly": (race.strip().lower() == "human"), "raceRestriction": race, "curves": curves,
        })
    perks.sort(key=lambda p: (
        SECTION_ORDER.index(p["section"]) if p["section"] in SECTION_ORDER else 99,
        0 if p["section"] == "Legendary Perk Cards"
          else (SPECIAL_ORDER.index(p["special"]) if p["special"] in SPECIAL_ORDER else 99),
        p["name"].lower(),
    ))
    out = {"meta": {"channel": channel, "source": os.path.basename(pcrd_tsv),
           "perkCount": len(perks), "withCurve": counts["with_curve"],
           "noCurve": counts["no_curve"], "isPtsFallback": bool(pts_fallback)},
           "sectionOrder": SECTION_ORDER, "specialOrder": SPECIAL_ORDER, "perks": perks}
    return out, counts

def run(channel):
    DIST_DIR.mkdir(parents=True, exist_ok=True); meta = load_meta()
    live_points_tsv = newest(str(TSV_DIR / "CURV_Export_*_POINTS.tsv"))
    points_by_edid = load_points_by_edid(live_points_tsv)
    labels = load_curve_labels(DIST_DIR / "index.json")
    linkage = load_linkage(DIST_DIR / "perk_cards.json")
    if not linkage:
        print("  WARN: no curve linkage found (run build_curves_json.py first); cards will render without curves.")
    class_index = load_class_index(
        newest(str(TSV_DIR / "PCRD_Export_*.tsv")), newest(str(PTS_DIR / "PCRD_Export_*.tsv")))
    targets = []
    if channel in ("live", "both"):
        pcrd = newest(str(TSV_DIR / "PCRD_Export_*.tsv"))
        if pcrd and "_PTS_" in os.path.basename(pcrd):
            pcrd = newest(str(TSV_DIR / "PCRD_Export_[!P]*.tsv")) or pcrd
        targets.append(("live", pcrd, "perk_cards.json", False))
    if channel in ("pts", "both"):
        pcrd_pts = newest(str(PTS_DIR / "PCRD_Export_*.tsv"))
        targets.append(("pts", pcrd_pts, "perk_cards.pts.json", True))
    for ch, pcrd_tsv, outname, fallback in targets:
        if not pcrd_tsv: print(f"[{ch}] SKIP — no PCRD export found."); continue
        data, counts = build_channel(ch, pcrd_tsv, points_by_edid, labels,
                                      linkage, meta, pts_fallback=fallback, class_index=class_index)
        out_path = DIST_DIR / outname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[{ch}] {outname}: {data['meta']['perkCount']} cards "
              f"({counts['with_curve']} with curves, {counts['no_curve']} without, "
              f"{counts['cut']} cut) <- {os.path.basename(pcrd_tsv)}"
              + ("  [PTS fallback to LIVE curves]" if fallback else ""))

# ══════════════════════════════════════════════════════════════════════════════
#  ANIMATED PERK CARD CHECKLIST — dist/perk_cards.json + .pts.json
# ══════════════════════════════════════════════════════════════════════════════
CHECKLIST_DIR = REPO_ROOT / "dist"
RARITY_MAP = {
    "PerkCard_Caps_Common": "Common", "PerkCard_Caps_Uncommon": "Uncommon",
    "PerkCard_Caps_Rare": "Rare", "PerkCard_Caps_Unique": "Unique",
}

def _friendly_name(male_raw, female_raw, edid, meta_card):
    if meta_card.get("displayName"): return meta_card["displayName"]
    male = strip_quotes(male_raw); female = strip_quotes(female_raw)
    if male and female and male != female:
        return f"{humanize_name(male)} / {humanize_name(female)}"
    if male: return humanize_name(male)
    if female: return humanize_name(female)
    return humanize_name(edid)

def _load_perk_descs(perk_tsv):
    out = {}
    if not perk_tsv or not os.path.exists(perk_tsv): return out
    for r in read_tsv(perk_tsv):
        edid = (r.get("PERK_EDID") or "").strip()
        desc = (r.get("DESC") or "").strip()
        if edid and desc: out[edid] = desc
    return out

def build_checklist_channel(channel, pcrd_tsv, perk_descs, meta,
                            class_index=None, anim_index=None):
    class_index = class_index or {}; anim_index = anim_index or set()
    pcrd_rows = read_tsv(pcrd_tsv); cards = []; total_ranks = 0; cut_count = 0
    for r in pcrd_rows:
        edid = col(r, "PCRD_EDID")
        if not edid: continue
        key = strip_quotes(edid)
        if is_cut(edid): cut_count += 1; continue
        meta_card = meta.get(key, {})
        name, former = resolve_card_name(r, key, meta_card)
        special = normalize_special(col(r, "DATA_Special"))
        race = col(r, "DATA_RaceRestriction", "DATA_SpeciesText", "DATA_Species") or "None"
        if special == "Unknown" and key in class_index:
            special = normalize_special(class_index[key]["special"])
            if race in ("", "None"): race = class_index[key].get("race", "None")
        section = section_for(edid)
        # A perk card has an animated (gold-back) copy IFF it carries a PCDV
        # "Perk Card Value" rarity GLOB (PerkCard_Caps_Common/Uncommon/Rare/
        # Unique). Cards without one (e.g. all Legendary perk cards) have no
        # animated version. NOTE: ANAM_PerkCardFlags is NOT the animated flag
        # and must not be used here.
        rarity_edid = col(r, "PCDV_GLOB_EDID")
        rarity = RARITY_MAP.get(rarity_edid, "")
        animated = bool(rarity)
        rank_count_raw = col(r, "RankCount")
        try: rank_count = int(rank_count_raw)
        except (TypeError, ValueError): rank_count = 1
        if rank_count < 1: rank_count = 1
        ranks = []
        for rn in range(1, rank_count + 1):
            perk_edid = col(r, f"RankPERK_{rn}_EDID", f"Rank_{rn}_MalePerk_EDID")
            perk_name = col(r, f"RankPERK_{rn}_FULL", f"Rank_{rn}_MalePerk_FULL")
            perk_desc = perk_descs.get(perk_edid, "")
            ranks.append({"rank": rn, "perkEdid": perk_edid, "perkName": perk_name, "desc": perk_desc})
        if not ranks or not ranks[0].get("perkEdid"):
            ranks = [{"rank": rn, "perkEdid": "", "perkName": "", "desc": ""} for rn in range(1, rank_count + 1)]
        total_ranks += rank_count
        min_level = col(r, "DATA_MinLevel")
        try: min_level = int(min_level)
        except (TypeError, ValueError): min_level = 0
        cards.append({
            "id": key, "formId": col(r, "PCRD_FormID").upper().replace("0X", "")[-8:],
            "name": name, "former": former, "desc": (col(r, "DESC") or "").strip(),
            "section": section, "special": special, "minLevel": min_level,
            "rarity": rarity, "rarityEdid": rarity_edid, "animated": animated,
            "humanOnly": (race.strip().lower() == "human"),
            "rankCount": rank_count, "ranks": ranks,
        })
    cards.sort(key=lambda c: (
        SECTION_ORDER.index(c["section"]) if c["section"] in SECTION_ORDER else 99,
        SPECIAL_ORDER.index(c["special"]) if c["special"] in SPECIAL_ORDER else 99,
        c["name"].lower(),
    ))
    from datetime import datetime, timezone
    return {
        "meta": {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "channel": channel, "source": os.path.basename(pcrd_tsv),
                 "type": "animated-checklist", "totalCards": len(cards),
                 "totalRanks": total_ranks, "cutSkipped": cut_count},
        "sectionOrder": SECTION_ORDER, "specialOrder": SPECIAL_ORDER, "cards": cards,
    }

def _build_anim_index(*pcrd_paths):
    out = set()
    for p in pcrd_paths:
        if not p or not os.path.exists(p): continue
        for r in read_tsv(p):
            edid = strip_quotes(col(r, "PCRD_EDID"))
            if edid and col(r, "ANAM_PerkCardFlags") == "1": out.add(edid)
    return out

def run_checklist(channel):
    CHECKLIST_DIR.mkdir(parents=True, exist_ok=True); meta = load_meta()
    class_index = load_class_index(
        newest(str(TSV_DIR / "PCRD_Export_*.tsv")), newest(str(PTS_DIR / "PCRD_Export_*.tsv")))
    anim_index = _build_anim_index(
        newest(str(TSV_DIR / "PCRD_Export_*.tsv")), newest(str(PTS_DIR / "PCRD_Export_*.tsv")))
    targets = []
    if channel in ("live", "both"):
        pcrd = newest(str(TSV_DIR / "PCRD_Export_*.tsv"))
        if pcrd and "_PTS_" in os.path.basename(pcrd):
            pcrd = newest(str(TSV_DIR / "PCRD_Export_[!P]*.tsv")) or pcrd
        perk = newest(str(TSV_DIR / "PERK_Export_*.tsv"))
        targets.append(("live", pcrd, perk, "perk_cards.json"))
    if channel in ("pts", "both"):
        pcrd_pts = newest(str(PTS_DIR / "PCRD_Export_*.tsv"))
        perk_pts = newest(str(PTS_DIR / "PERK_Export_*.tsv"))
        targets.append(("pts", pcrd_pts, perk_pts, "perk_cards.pts.json"))
    for ch, pcrd_tsv, perk_tsv, outname in targets:
        if not pcrd_tsv: print(f"[checklist:{ch}] SKIP — no PCRD export found."); continue
        perk_descs = _load_perk_descs(perk_tsv)
        data = build_checklist_channel(ch, pcrd_tsv, perk_descs, meta,
                                       class_index=class_index, anim_index=anim_index)
        out_path = CHECKLIST_DIR / outname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        m = data["meta"]
        print(f"[checklist:{ch}] {outname}: {m['totalCards']} cards, "
              f"{m['totalRanks']} rank slots, {m['cutSkipped']} cut <- {os.path.basename(pcrd_tsv)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=["live", "pts", "both"], default="both")
    args = ap.parse_args()
    os.chdir(REPO_ROOT)
    run(args.channel)
    run_checklist(args.channel)

if __name__ == "__main__":
    main()
