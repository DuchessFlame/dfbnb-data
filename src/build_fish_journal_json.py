#!/usr/bin/env python3
"""Build dist/fish_journal.json (+ pts twin) for the Field Journal of Appalachia page.

Parses the newest FISH_Export TSV, assigns each catchable fish to a single cascade
tier, and derives a full "field journal entry" for every fish by joining several
exports:

  FISH  -> the fish record: size / region / season / 12 fight stats / keywords /
           splash / FormID / EDID, and the raw-meal reference (FIRI).
  ALCH  -> raw-meal value (the "value" stat) + the cooked meals' food effects,
           reached through the FIRI reference.
  COBJ  -> the recipe chain (raw fish -> Filet -> prepared meals) with ingredients,
           crafting station, and any required challenge/recipe.
  CHAL  -> challenges that reward catching the fish, linked through the CNDF
           condition forms referenced by the raw meal, plus the season-wide
           "Catch a <Season> Seasonal Fish" challenges.
  GLOB  -> numeric magnitude / duration tiers used to render food effects.

The renderer (df-bnb-fishing-journal.js -> renderFieldJournal) consumes this feed
and computes the live "in season" state from seasonRule.rolloverDates.

Everything the entry card shows is generated here so the page is fully data-driven;
the only curated input is TEMPLATE_OVERRIDES (the specific fishing-hole name and the
flavour description, which are not present in the game data).
"""
import csv, re, json, os, glob, datetime

# Some reference columns (ALCH especially) are extremely wide.
csv.field_size_limit(10 * 1024 * 1024)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                 # dfbnb-data/
TSV_DIR = os.path.join(ROOT, "tsv")
DIST = os.path.join(ROOT, "dist")

IMG_BASE = "/wp-content/uploads/guide-images/fishing"

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

# Curated, non-game-data fields. The hole name and the flavour description aren't in
# the export; everything else on the card is generated. Keyed by display name.
TEMPLATE_OVERRIDES = {
    "Glass Ghost": {
        "hole": "Glassed Cavern",
        "description": "Can only be caught at <b>Glassed Cavern</b> in the Cranberry Bog, and only "
                       "while <b>Summer</b> is in season. Once the season turns, the Glass Ghost "
                       "vanishes until next summer.",
    },
}

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
SEASON_OF = {"Orange Overseer": "Spring", "Fernskipper": "Summer", "Glass Ghost": "Summer",
    "Fester Koi": "Fall", "Sludge Eye": "Fall", "Bog Sucker": "Winter"}

# Fish Bits yield by size (not in the export; the in-game scrap yield scales with size).
FISHBITS_BY_SIZE = {"Small": 1, "Medium": 3, "Large": 5}

# Axolotls fillet for a flat 25 Fish Bits regardless of their Small size class.
# The COBJ that makes Fish Bits from a raw axolotl is in the export, but xEdit's
# created-object count (NNAM) is not one of the exported columns, so the yield
# cannot yet be read from the game files — it is pinned here. If a future COBJ
# export adds the create count, source it from that recipe instead.
AXOLOTL_FISHBITS = 25

# Region keyword -> friendly name (same mapping as build_axolotl_guide_json.py).
LOC_KEYWORD_MAP = {
    "LocRegionBurningSprings":   "Burning Springs",
    "LocRegionMountain":         "Savage Divide",
    "LocRegionCranberryBog":     "Cranberry Bog",
    "LocRegionForestFloodlands": "Forest",
    "LocRegionStorm":            "Skyline Valley",
    "LocRegionSwampForest":      "Mire",
    "LocRegionMTR":              "Ash Heap",
    "LocRegionToxicValley":      "Toxic Valley",
}
MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

# Display names for crafting components whose EDID has no FULL anywhere in the exports.
COMPONENT_NAMES = {
    "c_Wood": "Wood", "Wood": "Wood", "CookingOil": "Cooking Oil",
    "WaterBoiled": "Boiled Water", "WaterDirty": "Dirty Water", "WaterPurified": "Purified Water",
    "DeathclawEgg": "Deathclaw Egg", "CarrotVegetable": "Carrot",
    "Cooking_Razorgrain_Flour": "Razorgrain Flour", "RazorgrainFlour": "Razorgrain Flour",
    "Salt": "Salt", "CookingFlavor_Salt": "Salt", "Sugar": "Sugar", "Flour": "Flour",
}

SPECIAL = {"Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"}

# ---------------------------------------------------------------- generic helpers

def newest(directory, prefix, pts=False):
    """Newest export matching prefix in `directory`. Live names embed '<Month>_<Year>';
    PTS names embed '_PTS_<YYYY-MM-DD>_<HHMM>'. Returns a path or None."""
    months = {"Jan":1,"Feb":2,"March":3,"Mar":3,"Apr":4,"May":5,"June":6,"Jun":6,"July":7,"Jul":7,
              "Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    best, bestkey = None, (-1, -1, -1, -1)
    for p in glob.glob(os.path.join(directory, prefix + "*.tsv")):
        b = os.path.basename(p)
        if pts:
            m = re.search(r"_PTS_(\d{4})-(\d{2})-(\d{2})_(\d{4})", b)
            if not m: continue
            key = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        else:
            if "PTS" in b: continue
            m = re.search(r"_([A-Za-z]+)_(\d{4})", b)
            if not m: continue
            key = (int(m.group(2)), months.get(m.group(1), 0), 0, 0)
        if key > bestkey: bestkey, best = key, p
    return best

def read_rows(path):
    if not path or not os.path.exists(path): return []
    # Some exports carry stray non-UTF-8 bytes; decode leniently so one bad byte
    # in a wide reference column never aborts the whole build.
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))

FORMID_RE = re.compile(r"\[(?:ALCH|FISH|COBJ|CNDF|MISC|RESO|KYWD|EXPL|SNDR|GLOB):([0-9A-Fa-f]{6,8})\]")
BRACKET_FORMID_RE = re.compile(r":([0-9A-Fa-f]{6,8})\]")
COLON_FORMID_RE = re.compile(r"^([0-9A-Fa-f]{6,8}):")

def first_formid(s):
    m = FORMID_RE.search(s or "")
    return m.group(1).upper() if m else None

def all_formids(s):
    return [x.upper() for x in re.findall(r"([0-9A-Fa-f]{6,8})(?=:[A-Za-z]+\b|\])", s or "")]

# ---------------------------------------------------------------- index builders

def build_name_map(alch, misc, cobj):
    """edid -> display name, from every export that carries a FULL/display column."""
    out = dict(COMPONENT_NAMES)
    for r in alch:
        e, n = r.get("ALCH_EDID"), r.get("FULL")
        if e and n: out.setdefault(e, n)
    for r in misc:
        e = r.get("EDID") or r.get("MISC_EDID")
        n = r.get("FULL")
        if e and n: out.setdefault(e, n)
    for r in cobj:
        e, n = r.get("CNAM_EDID"), r.get("CNAM_FULL")
        if e and n: out.setdefault(e, n)
    return out

def prettify(edid, name_map):
    if not edid: return ""
    if edid in name_map: return name_map[edid]
    s = edid
    for pre in ("c_", "Cooking_", "Cooking", "SeasonalFish_Meal_", "Meal_", "co_meal_"):
        if s.startswith(pre): s = s[len(pre):]
    for suf in ("Vegetable", "Fruit", "Herb", "Meat", "Flower"):
        if s.endswith(suf) and len(s) > len(suf): s = s[:-len(suf)]
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).replace("_", " ").strip()
    return s or edid

def build_glob(glob_rows):
    out = {}
    for r in glob_rows:
        e = r.get("EDID")
        v = r.get("FLTV")
        if e and v not in (None, ""):
            try: out[e] = float(v)
            except ValueError: pass
    return out

def build_alch_index(alch_rows):
    """formid -> {value, edid, name, cndf:[formids], cobj:[formids]}"""
    out = {}
    for r in alch_rows:
        fid = (r.get("ALCH_FormID") or "").upper()
        if not fid: continue
        refs = r.get("Refs_Flat", "") or ""
        cndf = [m for m in all_formids(refs) if ":CNDF" in refs[refs.find(m):refs.find(m)+30]]
        # Simpler: scan the flat refs string for "<id>:<edid>:CNDF"
        cndf = re.findall(r"([0-9A-Fa-f]{6,8}):[^:|]+:CNDF", refs)
        cobj = re.findall(r"([0-9A-Fa-f]{6,8}):[^:|]+:COBJ", refs)
        out[fid] = {
            "value": _intval(r.get("Value")),
            "edid": r.get("ALCH_EDID", ""),
            "name": r.get("FULL", ""),
            "cndf": [x.upper() for x in cndf],
            "cobj": [x.upper() for x in cobj],
        }
    return out

def build_alch_effects(rows, glob_map):
    """edid -> rendered effect string, e.g. '+4 Intelligence for 60 min'."""
    by_edid = {}
    for r in rows:
        by_edid.setdefault(r.get("ALCH_EDID", ""), []).append(r)
    out = {}
    for edid, effs in by_edid.items():
        parts, diet = [], None
        for r in effs:
            mg_name = r.get("MGEF_FULL") or r.get("MGEF_EDID") or ""
            # magnitude: literal EFIT_Magnitude, else the MAGG global tier value
            mag = _floatval(r.get("EFIT_Magnitude"))
            if not mag:
                mag = glob_map.get(r.get("MAGG_GLOB_EDID", ""), 0.0)
            dur = _floatval(r.get("EFIT_Duration"))
            if not dur:
                dur = glob_map.get(r.get("DURG_GLOB_EDID", ""), 0.0)
            txt = render_effect(mg_name, mag, dur)
            if txt: parts.append(txt)
        out[edid] = parts
    return out

def render_effect(mg_name, mag, dur):
    n = mg_name or ""
    # Skip the plumbing effects that aren't player-facing buffs.
    if any(k in n for k in ("SURV_Food_Effect", "Eating:", "Restore Health", "RestoreHealthFood",
                            "Radiation Damage", "DamageRadiation")):
        return None
    # SPECIAL buffs: "Fortify Intelligence Food" -> "+4 Intelligence"
    stat = next((s for s in SPECIAL if s in n), None)
    label = stat if stat else re.sub(r"\b(Food|Fortify|Restore)\b", "", n).strip()
    sign = "+" if (mag and mag > 0) else ""
    magtxt = ("%g" % mag) if mag else ""
    out = (sign + magtxt + " " + label).strip()
    if dur and dur >= 1:
        mins = int(round(dur / 60.0)) if dur >= 60 else None
        out += " for " + (("%d min" % mins) if mins else ("%ds" % int(dur)))
    return out or None

def build_cobj_index(cobj_rows):
    """Two lookups over recipes:
       by_output_edid : output meal EDID -> recipe dict
       by_component   : ingredient EDID  -> [recipe dicts that consume it]
    """
    recipes, by_output, by_component = [], {}, {}
    for r in cobj_rows:
        comps = []
        for chunk in (r.get("FVPA", "") or "").split("|"):
            chunk = chunk.strip()
            if not chunk: continue
            bits = chunk.split(":")
            edid = bits[0]
            qty = bits[1] if len(bits) > 1 and bits[1] else "1"
            comps.append({"edid": edid, "qty": _intval(qty) or 1})
        rec = {
            "out_edid": r.get("CNAM_EDID", ""),
            "out_name": r.get("CNAM_FULL", ""),
            "station": r.get("BNAM_FULL", ""),
            "req_edid": r.get("GNAM_EDID", ""),
            "req_name": r.get("GNAM_FULL", ""),
            "req_fid": (r.get("GNAM_FormID") or "").upper(),
            "kw": r.get("FNAM_Keywords", "") or "",
            "comps": comps,
        }
        recipes.append(rec)
        if rec["out_edid"]:
            by_output.setdefault(rec["out_edid"], rec)
        for c in comps:
            by_component.setdefault(c["edid"], []).append(rec)
    return recipes, by_output, by_component

def build_chal_index(chal_rows):
    """List of challenges with the CNDF condition formids they reference."""
    out = []
    for r in chal_rows:
        edid = r.get("EDID", "")
        if edid.startswith("zzz"): continue
        conds = []
        for k, v in r.items():
            if k and k.startswith("Cond") and v:
                conds += re.findall(r"\[CNDF:([0-9A-Fa-f]{6,8})\]", v)
        out.append({
            "fid": (r.get("FormID") or "").upper(),
            "edid": edid,
            "name": r.get("FULL", ""),
            "cndf": [c.upper() for c in conds],
        })
    return out

def _intval(s):
    try: return int(float(s))
    except (TypeError, ValueError): return None

def _floatval(s):
    try: return float(s)
    except (TypeError, ValueError): return 0.0

# ---------------------------------------------------------------- FISH parsing

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

def keywords_of(r):
    out = []
    for v in kws(r):
        if not v: continue
        parts = v.split(":")
        if len(parts) >= 2:
            out.append({"id": parts[0].upper(), "name": parts[1]})
    return out

def stats_of(r):
    def fv(k):
        try: return round(float(r.get(k, "")), 4)
        except (TypeError, ValueError): return None
    def iv(k):
        try: return int(float(r.get(k, "")))
        except (TypeError, ValueError): return None
    return {
        "startProgress": fv("FISP"), "drawInSpeed": fv("FIDS"), "escapeSpeed": fv("FIES"),
        "maxStamina": iv("FIHS"), "minStamina": iv("FILS"),
        "maxJumpDelay": fv("FIHD"), "minJumpDelay": fv("FILD"),
        "jumpSpeed": iv("FIJS"),
        "maxJumpDistance": iv("FIHJ"), "minJumpDistance": iv("FILJ"),
        "minInitialAngle": iv("FILA"), "maxInitialAngle": iv("FIHA"),
    }

def splash_of(r):
    s = r.get("FIEX", "") or ""
    return s.split("[")[0].strip() or None

def is_legendary(r):
    return "Legendary" in (r.get("FISU", "") or "")

def image_for(name):
    rel = IMAGE_OVERRIDES.get(name)
    return "%s/%s.avif" % (IMG_BASE, rel) if rel else None

def baits_for(cid):
    if cid == "axolotl": return ["Improved", "Superior"]
    if cid == "junk":    return ["Basic"]
    return ["Basic", "Improved", "Superior"]

# All three weather buckets exist for every fish; weather changes the rate, not the
# availability. Displayed as the three in-game fishing weather states.
WEATHER = ["Clear", "Rain", "Rad Storm"]

# ---------------------------------------------------------------- recipe + challenge resolution

def recipe_dict(rec, name_map, eff_map):
    ings = []
    for c in rec["comps"]:
        ings.append({"item": prettify(c["edid"], name_map), "qty": c["qty"]})
    out_edid = rec["out_edid"]
    effs = eff_map.get(out_edid, [])
    return {
        "name": rec["out_name"] or prettify(out_edid, name_map),
        "station": rec["station"] or "",
        "effect": " · ".join(effs) if effs else "",
        "ingredients": ings,
    }

def resolve_recipes(raw_meal_edid, by_output, by_component, name_map, eff_map):
    """Follow raw fish -> Filet -> prepared meals, in cascade order, de-duplicated."""
    out, seen = [], set()
    def add(rec):
        key = rec["out_edid"] or rec["out_name"]
        if not key or key in seen: return None
        seen.add(key)
        out.append(recipe_dict(rec, name_map, eff_map))
        return rec
    if not raw_meal_edid:
        return out
    # 1) the Filet recipe(s) that consume the raw fish
    filet_recs = list(by_component.get(raw_meal_edid, []))
    filet_edids = []
    for fr in filet_recs:
        if add(fr) and fr["out_edid"]:
            filet_edids.append(fr["out_edid"])
    # 2) prepared meals that consume each Filet
    for fe in filet_edids:
        for pr in by_component.get(fe, []):
            add(pr)
    return out

def resolve_challenges(cndf_ids, season, chal_list, by_output, name_map):
    """Challenges whose condition references one of this fish's CNDF forms, plus the
       season-wide 'Catch a <Season> Seasonal Fish' challenges. Reward is the recipe
       a challenge unlocks (a COBJ requiring that challenge), else '—'."""
    out, seen = [], set()
    cndf_set = set(cndf_ids or [])

    def reward_for(chal_fid):
        for rec in by_output.values():
            if rec["req_fid"] and rec["req_fid"] == chal_fid:
                nm = rec["out_name"] or prettify(rec["out_edid"], name_map)
                return "Recipe: " + nm
        return "—"

    for ch in chal_list:
        hit = bool(cndf_set & set(ch["cndf"]))
        if not hit and season:
            # season-wide summary challenge, e.g. "Catch A Summer Seasonal Fish"
            if season.lower() in ch["name"].lower() and "seasonal fish" in ch["name"].lower():
                hit = True
        if not hit: continue
        if ch["name"] in seen: continue
        seen.add(ch["name"])
        out.append({"name": ch["name"], "reward": reward_for(ch["fid"])})
    return out

# ---------------------------------------------------------------- season rule

def load_rollovers():
    for cand in ("seasonal-fish.json", "seasonal_fish_guide.json"):
        p = os.path.join(DIST, cand)
        if os.path.exists(p):
            try:
                d = json.load(open(p, encoding="utf-8"))
            except (ValueError, IOError):
                continue
            roll = (d.get("seasonRule") or {}).get("rolloverDates")
            if roll: return roll
    return []

# ---- Axolotl monthly rotation, read straight from the game LVLI export --------
# The Fishing_LLS_FishCollection_Axolotls leveled list encodes, per entry, the
# fish it points at plus the CTDA conditions that gate it: a MonthlyIndex check
# (which calendar month the colour is active) and two LocationHierarchyHasKeyword
# checks (the two regions it spawns in that month). Both facts are parsed here
# generatively, so the field journal no longer depends on dist/axolotl_guide.json.

def newest_lvli_entries(directory, pts=False):
    """Newest LVLI_Export_*_LVLI_Entries.tsv in `directory`, honouring the same
    live/PTS filename conventions as newest()."""
    months = {"Jan":1,"Feb":2,"March":3,"Mar":3,"Apr":4,"May":5,"June":6,"Jun":6,
              "July":7,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    best, bestkey = None, (-1, -1, -1, -1)
    for p in glob.glob(os.path.join(directory, "LVLI_Export_*_LVLI_Entries.tsv")):
        b = os.path.basename(p)
        if pts:
            m = re.search(r"_PTS_(\d{4})-(\d{2})-(\d{2})_(\d{4})", b)
            if not m: continue
            key = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        else:
            if "PTS" in b: continue
            m = re.search(r"_([A-Za-z]+)_(\d{4})", b)
            if not m: continue
            key = (int(m.group(2)), months.get(m.group(1), 0), 0, 0)
        if key > bestkey: bestkey, best = key, p
    return best

def parse_axolotl_rotation(path, list_edid="Fishing_LLS_FishCollection_Axolotls"):
    """month-number (1-12) -> {refFormId, regions[]} from a fishing axolotl LVLI."""
    out = {}
    for r in read_rows(path):
        if (r.get("LVLI_EDID") or "") != list_edid: continue
        ref = r.get("LVLO_Reference", "") or ""
        ref_fid = ref.split(":")[0].upper() if ref else ""
        regions, month = [], None
        for k, v in r.items():
            if not k or not k.startswith("Cond") or not v: continue
            mreg = re.search(r"LocationHierarchyHasKeyword\(.+?,\s*(LocRegion\w+)\s*\[", v)
            if mreg: regions.append(LOC_KEYWORD_MAP.get(mreg.group(1), mreg.group(1)))
            mmon = re.search(r"MonthlyIndex.+?\)\s+\S+\s+([\d.]+)", v)
            if mmon: month = int(float(mmon.group(1)))
        if month: out[month] = {"refFormId": ref_fid, "regions": regions}
    return out

def load_axolotl_rotation(directory, pts=False):
    """FormID (upper) -> {'month': 'June', 'regions': [...]} for each axolotl,
    derived from the newest LVLI entries export in `directory`. Returns an empty
    map (and the caller falls back) if no LVLI entries export is present."""
    path = newest_lvli_entries(directory, pts)
    out = {}
    if path:
        for month, rec in parse_axolotl_rotation(path).items():
            if rec["refFormId"]:
                out[rec["refFormId"]] = {"month": MONTH_NAMES[month - 1],
                                         "regions": rec["regions"]}
    return out

# ---------------------------------------------------------------- build

def build(tsv_path, ctx, axolotl_map=None):
    axolotl_map = axolotl_map or {}
    rows = read_rows(tsv_path)
    bucket = {cid: [] for cid, _, _ in CASCADES}
    seen = {cid: set() for cid, _, _ in CASCADES}

    for r in rows:
        nm = disp(r)
        if r.get("EDID", "").startswith("zzz"): continue
        if nm.startswith("zzz") or nm.startswith("Fishing_"): continue
        if nm in DROP_NAMES: continue
        cid = cascade_of(r)
        if not cid: continue

        def rec(c):
            if nm in seen[c]: return None
            seen[c].add(nm)
            casc_label = next((lab for cc, lab, _ in CASCADES if cc == c), c)
            raw_meal_fid = first_formid(r.get("FIRI", ""))
            alch = ctx["alch"].get(raw_meal_fid) if raw_meal_fid else None
            raw_meal_edid = alch["edid"] if alch else None
            season = SEASON_OF.get(nm)
            recipes = resolve_recipes(raw_meal_edid, ctx["cobj_out"], ctx["cobj_comp"],
                                      ctx["names"], ctx["effects"])
            challenges = resolve_challenges(alch["cndf"] if alch else [], season,
                                            ctx["chal"], ctx["cobj_out"], ctx["names"])
            o = {
                "id": (r.get("FormID") or "").upper(),
                "edid": r.get("EDID", ""),
                "name": nm,
                # Junk and waterlogged gifts carry the in-game Small size keyword,
                # but they are not fish — they don't fillet, so they have no fish
                # size class and yield no Fish Bits. (The raw keyword is still shown
                # in the Technical section via keywords_of.)
                "size": (None if c in ("junk", "gift") else size_of(r)),
                "region": region_of(r),
                "image": image_for(nm),
                "value": (alch["value"] if alch else None),
                "fishBits": (None if c in ("junk", "gift") else FISHBITS_BY_SIZE.get(size_of(r))),
                "legendary": is_legendary(r),
                "splash": splash_of(r),
                "stats": stats_of(r),
                "keywords": keywords_of(r),
                "recipes": recipes,
                "challenges": challenges,
                "weather": list(WEATHER),
                "bait": baits_for(c),
            }
            if season: o["season"] = season
            if c == "axolotl":
                info = axolotl_map.get((r.get("FormID") or "").upper()) or {}
                if info.get("month"): o["month"] = info["month"]
                if info.get("regions"): o["regions"] = info["regions"]
                # Fixed 25 Fish Bits on fillet (not in the export; see AXOLOTL_FISHBITS).
                o["fishBits"] = AXOLOTL_FISHBITS
            ov = TEMPLATE_OVERRIDES.get(nm)
            if ov:
                if ov.get("hole"): o["hole"] = ov["hole"]
                if ov.get("description"): o["description"] = ov["description"]
            return o

        m = rec(cid)
        if m: bucket[cid].append(m)
        if nm in SEASON_OF:
            m2 = rec("season")
            if m2: bucket["season"].append(m2)

    return {
        "generated": datetime.datetime.utcnow().isoformat() + "Z",
        "source": os.path.basename(tsv_path),
        "seasonRule": {"rolloverDates": load_rollovers()},
        "cascades": [{"id": cid, "label": lab, "blurb": bl, "fish": bucket[cid]}
                     for cid, lab, bl in CASCADES],
    }

def make_ctx(directory, pts=False):
    glob_map = build_glob(read_rows(newest(directory, "GLOB_Export_", pts)))
    alch_main = read_rows(newest(directory, "ALCH_Export_", pts))
    # ALCH main vs effects share the prefix; the effects file ends in _Effects.
    alch_main = [r for r in alch_main if "ALCH_FormID" in r]
    eff_path = None
    for p in glob.glob(os.path.join(directory, "ALCH_Export_*Effects*.tsv")):
        if pts == ("PTS" in os.path.basename(p)):
            eff_path = p
    misc = read_rows(newest(directory, "MISC_Export_", pts))
    cobj = read_rows(newest(directory, "COBJ_Export_", pts))
    chal = read_rows(newest(directory, "CHAL_Export_", pts))
    names = build_name_map(alch_main, misc, cobj)
    recipes, cobj_out, cobj_comp = build_cobj_index(cobj)
    return {
        "alch": build_alch_index(alch_main),
        "effects": build_alch_effects(read_rows(eff_path), glob_map),
        "names": names,
        "cobj_out": cobj_out,
        "cobj_comp": cobj_comp,
        "chal": build_chal_index(chal),
    }

def main():
    os.makedirs(DIST, exist_ok=True)
    os.makedirs(os.path.join(DIST, "pts"), exist_ok=True)

    # Live FISH export must avoid the PTS naming; reuse the same logic as the others.
    tsv = newest(TSV_DIR, "FISH_Export_")
    if not tsv: raise SystemExit("No FISH_Export TSV found in %s" % TSV_DIR)
    live_ctx = make_ctx(TSV_DIR)
    live_axo = load_axolotl_rotation(TSV_DIR)
    live_data = build(tsv, live_ctx, live_axo)
    live = os.path.join(DIST, "fish_journal.json")
    json.dump(live_data, open(live, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    # PTS twin (best-effort; falls back to live if no PTS exports exist).
    pts_dir = os.path.join(TSV_DIR, "pts")
    pts_tsv = newest(pts_dir, "FISH_Export_", pts=True)
    if pts_tsv:
        pts_ctx = make_ctx(pts_dir, pts=True)
        # PTS context may be sparse; fall back to live indices where empty.
        for k in ("alch", "effects", "names", "cobj_out", "cobj_comp", "chal"):
            if not pts_ctx.get(k): pts_ctx[k] = live_ctx[k]
        # PTS axolotl rotation from the PTS LVLI export; fall back to live if absent.
        pts_axo = load_axolotl_rotation(pts_dir, pts=True) or live_axo
        pts_data = build(pts_tsv, pts_ctx, pts_axo)
    else:
        pts_data = live_data
    pts = os.path.join(DIST, "pts", "fish_journal.json")
    json.dump(pts_data, open(pts, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    tot = sum(len(c["fish"]) for c in live_data["cascades"])
    print("Source:", live_data["source"])
    for c in live_data["cascades"]:
        print("  %-9s %2d" % (c["id"], len(c["fish"])))
    print("Total records:", tot)
    print("Wrote:", live, "and", pts)

if __name__ == "__main__":
    main()
